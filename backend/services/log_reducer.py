"""
Log Reduction Service
=====================
Implements a Chain-of-Responsibility pipeline where each Handler reads,
filters, or aggregates Spark event log data.

Design Patterns used:
  - Chain of Responsibility: processing steps chained via BaseHandler
  - Strategy: output rendering (MarkdownRenderer / JsonRenderer)
  - Iterator: events streamed line-by-line from the ZIP
  - Factory: StageMetrics built from raw dicts in _build_stage()
"""
from __future__ import annotations

import io
import json
import re
import random
import zipfile
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional

from backend.models.job import AppSummary, StageMetrics
from backend.utils.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Callback type: (percent 0-100, stage_key str) -> None
ProgressCallback = Optional[Callable[[int, str], None]]

# Maximum samples kept per stage for approximate p95 via reservoir sampling
_RESERVOIR_SIZE = 10_000


def _resource_amount(value: object) -> int:
    """Best-effort conversion for Spark resource Amount fields."""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


# ─── Iterator ────────────────────────────────────────────────────────────────

def _iter_events(
    zip_bytes: bytes,
    progress_cb: ProgressCallback = None,
) -> Iterator[dict]:
    """Yield parsed JSON events from every file inside the ZIP.

    Progress is emitted from 5 % (first file) to 60 % (last file), proportional
    to file index within the archive.  For desktop local-path mode the
    uncompressed-size limit is intentionally not enforced here — the caller
    (route handler) controls whether to apply it.
    """
    logger.info("Processing ZIP with %d bytes", len(zip_bytes))

    file_count = 0
    uncompressed_size = 0

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        namelist = [n for n in zf.namelist() if not n.endswith("/")]
        total_files = len(namelist)

        if total_files > settings.max_files_in_zip:
            raise ValueError(
                f"ZIP contains too many files: {total_files} > {settings.max_files_in_zip}"
            )

        for file_idx, name in enumerate(namelist):
            file_count += 1

            # Emit progress proportional to file position: 5 % → 60 %
            if progress_cb:
                pct = 5 + int((file_idx / max(total_files, 1)) * 55)
                progress_cb(pct, "reading_file")

            with zf.open(name) as fh:
                file_data = fh.read()
                uncompressed_size += len(file_data)

                # ZIP-bomb guard: per-file compression ratio only
                compressed_size = zf.getinfo(name).compress_size
                if compressed_size > 0:
                    ratio = len(file_data) / compressed_size
                    if ratio > settings.compression_ratio_limit:
                        raise ValueError(
                            f"File {name} has suspicious compression ratio: "
                            f"{ratio:.1f} > {settings.compression_ratio_limit}"
                        )

                for raw_line in io.BytesIO(file_data):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        if not isinstance(event, dict) or "Event" not in event:
                            logger.warning("Skipping non-Spark event in %s", name)
                            continue
                        yield event
                    except json.JSONDecodeError as exc:
                        logger.warning("Skipping malformed JSON line in %s: %s", name, exc)

    uncompressed_mb = uncompressed_size / (1024 * 1024)
    logger.info(
        "Processed %d files, total uncompressed size: %.2f MB",
        file_count,
        uncompressed_mb,
    )


# ─── StageAccumulator — memory-efficient per-stage aggregator ────────────────

@dataclass
class StageAccumulator:
    """Accumulates task metrics for one stage without storing raw event dicts.

    Scalar aggregations (sum/min/max/count) are updated incrementally.
    Task durations for p95 use reservoir sampling capped at _RESERVOIR_SIZE
    items, giving an approximate p95 with fixed memory cost (~80 KB per stage).
    """
    # Reservoir for approximate p95
    _reservoir: list = field(default_factory=list)
    _total_seen: int = 0

    # Running aggregates — all integers to match StageMetrics types
    count: int = 0
    dur_sum: int = 0
    dur_min: int = 0
    dur_max: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read: int = 0
    shuffle_write: int = 0
    gc_time: int = 0
    memory_spill: int = 0
    disk_spill: int = 0
    shuffle_write_time: int = 0
    fetch_wait: int = 0
    remote_to_disk: int = 0
    peak_exec_mem: int = 0
    shuffle_read_records: int = 0
    shuffle_write_records: int = 0

    def add(
        self,
        duration: int,
        input_b: int,
        output_b: int,
        shuffle_r: int,
        shuffle_w: int,
        gc: int,
        mem_spill: int,
        disk_spill: int,
        sw_time: int,
        fetch_wait: int,
        remote_disk: int,
        peak_mem: int,
        sr_records: int,
        sw_records: int,
    ) -> None:
        # Scalar aggregates
        self.count += 1
        self.dur_sum += duration
        if self.count == 1:
            self.dur_min = duration
            self.dur_max = duration
        else:
            if duration < self.dur_min:
                self.dur_min = duration
            if duration > self.dur_max:
                self.dur_max = duration
        self.input_bytes += input_b
        self.output_bytes += output_b
        self.shuffle_read += shuffle_r
        self.shuffle_write += shuffle_w
        self.gc_time += gc
        self.memory_spill += mem_spill
        self.disk_spill += disk_spill
        self.shuffle_write_time += sw_time
        self.fetch_wait += fetch_wait
        self.remote_to_disk += remote_disk
        if peak_mem > self.peak_exec_mem:
            self.peak_exec_mem = peak_mem
        self.shuffle_read_records += sr_records
        self.shuffle_write_records += sw_records

        # Reservoir sampling for p95
        self._total_seen += 1
        if len(self._reservoir) < _RESERVOIR_SIZE:
            self._reservoir.append(duration)
        else:
            j = random.randrange(self._total_seen)
            if j < _RESERVOIR_SIZE:
                self._reservoir[j] = duration

    @property
    def dur_avg(self) -> float:
        return self.dur_sum / self.count if self.count else 0.0

    @property
    def dur_p95(self) -> int:
        if not self._reservoir:
            return 0
        s = sorted(self._reservoir)
        return s[int(len(s) * 0.95)]

    @property
    def skew_ratio(self) -> float:
        avg = self.dur_avg
        return self.dur_max / avg if avg > 0 else 0.0


# ─── Chain of Responsibility ──────────────────────────────────────────────────

class BaseHandler(ABC):
    """Abstract handler in the pipeline chain."""

    def __init__(self):
        self._next: Optional[BaseHandler] = None

    def set_next(self, handler: "BaseHandler") -> "BaseHandler":
        self._next = handler
        return handler

    def handle(self, ctx: dict) -> dict:
        result = self.process(ctx)
        if self._next:
            return self._next.handle(result)
        return result

    @abstractmethod
    def process(self, ctx: dict) -> dict:
        ...


# ── SQL execution smart-selection ────────────────────────────────────────────

def _count_plan_nodes(node: dict) -> int:
    """Recursively count nodes in a sparkPlanInfo tree."""
    if not isinstance(node, dict):
        return 0
    return 1 + sum(_count_plan_nodes(c) for c in (node.get("children") or []))


def _select_sql_executions(all_execs: list, max_kept: int = 30) -> list:
    """Smart-select the most informative SQL executions to send to the UI.

    Priority order:
    1. Always keep write/insert executions (final output — most valuable).
    2. Always keep executions with many nodes (complex compute plans).
    3. Deduplicate identical root types — keep at most 3 per root node name.
    4. Sort result by executionId (chronological order).
    """
    annotated = []
    for ex in all_execs:
        plan = ex.get("sparkPlanInfo") or {}
        root_name = plan.get("nodeName", "") if isinstance(plan, dict) else ""
        nc = _count_plan_nodes(plan)
        is_write = bool(
            re.search(r"Insert|Write", root_name, re.I)
            and "Create" not in root_name
        )
        annotated.append({**ex, "_nc": nc, "_root": root_name, "_write": is_write})

    selected: list = []
    seen_root: dict[str, int] = {}

    # Pass 1 — always keep write/insert and large plans (>100 nodes)
    for ex in sorted(annotated, key=lambda x: -x["_nc"]):
        if ex["_write"] or ex["_nc"] > 100:
            selected.append(ex)

    # Pass 2 — fill remainder with deduplicated samples of smaller plans
    for ex in sorted(annotated, key=lambda x: -x["_nc"]):
        if len(selected) >= max_kept:
            break
        if ex in selected:
            continue
        root = ex["_root"]
        if seen_root.get(root, 0) < 3:
            seen_root[root] = seen_root.get(root, 0) + 1
            selected.append(ex)

    # Strip internal annotations, sort chronologically
    selected.sort(key=lambda x: x.get("executionId", 0))
    return [{k: v for k, v in ex.items() if not k.startswith("_")} for ex in selected[:max_kept]]


class SinglePassHandler(BaseHandler):
    """Reads the ZIP once, dispatching events by type on the fly.

    Replaces the old three-handler sequence
    (EventLoaderHandler → AppMetaHandler → StageAggregationHandler).
    Memory footprint is O(stages × _RESERVOIR_SIZE) regardless of total
    task count, instead of O(total_tasks).
    """

    def process(self, ctx: dict) -> dict:
        zip_bytes: bytes = ctx["zip_bytes"]
        progress_cb: ProgressCallback = ctx.get("progress_cb")

        # ── app-level accumulators
        app_start: dict = {}
        app_end: dict = {}
        env_update: dict = {}
        executor_count: int = 0
        resource_profile: dict = {}
        sql_execution_count: int = 0
        sql_plan_root: dict = {}
        sql_executions_raw: list = []   # collect ALL then smart-select

        # ── stage accumulators
        by_stage: dict[int, StageAccumulator] = {}
        stage_names: dict[int, str] = {}
        stage_info: dict[int, dict] = {}   # sid → Stage Info dict from StageCompleted

        for ev in _iter_events(zip_bytes, progress_cb):
            etype = ev.get("Event", "")

            if etype == "SparkListenerApplicationStart":
                app_start = ev
            elif etype == "SparkListenerApplicationEnd":
                app_end = ev
            elif etype == "SparkListenerEnvironmentUpdate":
                env_update = ev
            elif etype == "SparkListenerExecutorAdded":
                executor_count += 1
            elif etype == "SparkListenerResourceProfileAdded":
                # Keep the first/default profile (id=0 in typical Spark logs).
                if not resource_profile:
                    resource_profile = ev
            elif etype.endswith("SparkListenerSQLExecutionStart"):
                sql_execution_count += 1
                plan = ev.get("sparkPlanInfo")
                if isinstance(plan, dict):
                    sql_executions_raw.append({
                        "executionId": ev.get("executionId", sql_execution_count - 1),
                        "description": ev.get("description", ""),
                        "sparkPlanInfo": plan,
                    })
                    if not sql_plan_root:
                        sql_plan_root = plan
            elif etype == "SparkListenerStageCompleted":
                si = ev.get("Stage Info", {})
                sid = si.get("Stage ID", -1)
                stage_names[sid] = si.get("Stage Name", f"Stage {sid}")
                stage_info[sid] = si
            elif etype == "SparkListenerTaskEnd":
                sid = ev.get("Stage ID", -1)
                acc = by_stage.get(sid)
                if acc is None:
                    acc = StageAccumulator()
                    by_stage[sid] = acc

                ti = ev.get("Task Info", {})
                tm = ev.get("Task Metrics", {})
                sr = tm.get("Shuffle Read Metrics", {})
                sw = tm.get("Shuffle Write Metrics", {})

                acc.add(
                    duration=ti.get("Finish Time", 0) - ti.get("Launch Time", 0),
                    input_b=tm.get("Input Metrics", {}).get("Bytes Read", 0),
                    output_b=tm.get("Output Metrics", {}).get("Bytes Written", 0),
                    shuffle_r=sr.get("Total Bytes Read", 0),
                    shuffle_w=sw.get("Shuffle Bytes Written", 0),
                    gc=tm.get("JVM GC Time", 0),
                    mem_spill=tm.get("Memory Bytes Spilled", 0),
                    disk_spill=tm.get("Disk Bytes Spilled", 0),
                    sw_time=sw.get("Shuffle Write Time", 0) // 1_000_000,  # ns → ms
                    fetch_wait=sr.get("Fetch Wait Time", 0),
                    remote_disk=sr.get("Remote Bytes Read To Disk", 0),
                    peak_mem=tm.get("Peak Execution Memory", 0),
                    sr_records=sr.get("Total Records Read", 0),
                    sw_records=sw.get("Shuffle Records Written", 0),
                )

        if progress_cb:
            progress_cb(62, "aggregating_stages")

        # ── build app_meta
        spark_props = {
            k: v
            for k, v in (env_update.get("Spark Properties", {}) or {}).items()
        }
        ctx["app_meta"] = {
            "app_id": app_start.get("App ID", "unknown"),
            "app_name": app_start.get("App Name", "unknown"),
            "spark_version": app_start.get(
                "Spark Version", spark_props.get("spark.version", "unknown")
            ),
            "start_time_ms": app_start.get("Timestamp", 0),
            "end_time_ms": app_end.get("Timestamp", 0),
            "executor_count": executor_count,
        }

        exec_res = resource_profile.get("Executor Resource Requests", {}) or {}
        ctx["app_meta"].update(
            {
                "executor_memory_mb": _resource_amount(
                    (exec_res.get("memory", {}) or {}).get("Amount", 0)
                ),
                "executor_memory_overhead_mb": _resource_amount(
                    (exec_res.get("memoryOverhead", {}) or {}).get("Amount", 0)
                ),
                "executor_offheap_mb": _resource_amount(
                    (exec_res.get("offHeap", {}) or {}).get("Amount", 0)
                ),
                "executor_cores": _resource_amount(
                    (exec_res.get("cores", {}) or {}).get("Amount", 0)
                ),
            }
        )
        ctx["sql_execution_count"] = sql_execution_count
        # Keep the raw plan tree (Spark's sparkPlanInfo shape) for UI rendering.
        ctx["sql_plan_tree"] = sql_plan_root if sql_plan_root else None
        # Smart-select executions: always include writes/inserts + largest plans.
        ctx["sql_executions"] = _select_sql_executions(sql_executions_raw) or None

        # ── build StageMetrics list from accumulators
        stages: list[StageMetrics] = []
        for sid, acc in by_stage.items():
            si = stage_info.get(sid, {})
            name = stage_names.get(sid, f"Stage {sid}")
            stages.append(
                StageMetrics(
                    stage_id=sid,
                    name=name,
                    num_tasks=acc.count,
                    duration_ms=si.get("Completion Time", 0) - si.get("Submission Time", 0),
                    input_bytes=acc.input_bytes,
                    output_bytes=acc.output_bytes,
                    shuffle_read_bytes=acc.shuffle_read,
                    shuffle_write_bytes=acc.shuffle_write,
                    gc_time_ms=acc.gc_time,
                    task_duration_min_ms=acc.dur_min if acc.count else None,
                    task_duration_avg_ms=round(acc.dur_avg, 1) if acc.count else None,
                    task_duration_max_ms=acc.dur_max if acc.count else None,
                    task_duration_p95_ms=acc.dur_p95 if acc.count else None,
                    skew_ratio=round(acc.skew_ratio, 2) if acc.count else None,
                    memory_bytes_spilled=acc.memory_spill,
                    disk_bytes_spilled=acc.disk_spill,
                    shuffle_write_time_ms=acc.shuffle_write_time,
                    fetch_wait_time_ms=acc.fetch_wait,
                    remote_bytes_read_to_disk=acc.remote_to_disk,
                    peak_execution_memory_bytes=acc.peak_exec_mem,
                    shuffle_read_records=acc.shuffle_read_records,
                    shuffle_write_records=acc.shuffle_write_records,
                )
            )

        stages.sort(key=lambda s: s.stage_id)
        ctx["stages"] = stages

        if progress_cb:
            progress_cb(70, "stages_ready")

        return ctx


class SummaryBuilderHandler(BaseHandler):
    """Assembles the final AppSummary from all gathered data."""

    def process(self, ctx: dict) -> dict:
        progress_cb: ProgressCallback = ctx.get("progress_cb")
        meta = ctx.get("app_meta", {})
        stages: list[StageMetrics] = ctx.get("stages", [])

        if progress_cb:
            progress_cb(75, "building_summary")

        total_dur = meta.get("end_time_ms", 0) - meta.get("start_time_ms", 0)

        ctx["summary"] = AppSummary(
            app_id=meta.get("app_id", "unknown"),
            app_name=meta.get("app_name", "unknown"),
            spark_version=meta.get("spark_version", "unknown"),
            start_time_ms=meta.get("start_time_ms", 0),
            end_time_ms=meta.get("end_time_ms", 0),
            total_duration_ms=total_dur,
            num_stages=len(stages),
            num_tasks=sum(s.num_tasks for s in stages),
            executor_count=meta.get("executor_count", 0),
            executor_memory_mb=meta.get("executor_memory_mb", 0),
            executor_memory_overhead_mb=meta.get("executor_memory_overhead_mb", 0),
            executor_offheap_mb=meta.get("executor_offheap_mb", 0),
            executor_cores=meta.get("executor_cores", 0),
            total_input_bytes=sum(s.input_bytes for s in stages),
            total_output_bytes=sum(s.output_bytes for s in stages),
            total_shuffle_read_bytes=sum(s.shuffle_read_bytes for s in stages),
            total_shuffle_write_bytes=sum(s.shuffle_write_bytes for s in stages),
            sql_execution_count=ctx.get("sql_execution_count", 0),
            sql_plan_mermaid=None,
            sql_plan_tree=ctx.get("sql_plan_tree"),
            sql_executions=ctx.get("sql_executions"),
            stages=stages,
        )
        return ctx


# ─── Strategy: Output Renderers ──────────────────────────────────────────────

class BaseRenderer(ABC):
    @abstractmethod
    def render(self, summary: AppSummary) -> str:
        ...


class MarkdownRenderer(BaseRenderer):
    def render(self, summary: AppSummary) -> str:
        def fmt_bytes(b: int) -> str:
            for unit in ("B", "KB", "MB", "GB", "TB"):
                if b < 1024:
                    return f"{b:.1f} {unit}"
                b /= 1024
            return f"{b:.1f} PB"

        def fmt_ms(ms: int) -> str:
            if ms < 1000:
                return f"{ms} ms"
            if ms < 60_000:
                return f"{ms/1000:.1f} s"
            return f"{ms/60000:.1f} min"

        lines = [
            f"# Spark Log Report — {summary.app_name}",
            "",
            "## Application Overview",
            f"| Field | Value |",
            f"|---|---|",
            f"| App ID | `{summary.app_id}` |",
            f"| Spark Version | {summary.spark_version} |",
            f"| Total Duration | {fmt_ms(summary.total_duration_ms)} |",
            f"| Stages | {summary.num_stages} |",
            f"| Tasks | {summary.num_tasks} |",
            f"| Executors | {summary.executor_count} |",
            f"| Executor Memory | {summary.executor_memory_mb:,} MB on-heap "
            f"+ {summary.executor_memory_overhead_mb:,} MB overhead |",
            f"| Executor Cores | {summary.executor_cores} vcores/executor |",
            f"| SQL Executions | {summary.sql_execution_count} |",
            f"| Input | {fmt_bytes(summary.total_input_bytes)} |",
            f"| Output | {fmt_bytes(summary.total_output_bytes)} |",
            f"| Shuffle Read | {fmt_bytes(summary.total_shuffle_read_bytes)} |",
            f"| Shuffle Write | {fmt_bytes(summary.total_shuffle_write_bytes)} |",
            "",
            "## Stage Breakdown",
            "| Stage | Name | Tasks | Duration | Input | Shuffle R | Shuffle W | SW Time | Fetch Wait | Spill Mem | Spill Disk | Peak Mem | Skew |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for s in summary.stages:
            skew_flag = " ⚠️" if s.has_skew else ""
            spill_disk_flag = " 💾" if s.has_spill else ""
            lines.append(
                f"| {s.stage_id} | {s.name[:35]} | {s.num_tasks} "
                f"| {fmt_ms(s.duration_ms)} | {fmt_bytes(s.input_bytes)} "
                f"| {fmt_bytes(s.shuffle_read_bytes)} | {fmt_bytes(s.shuffle_write_bytes)} "
                f"| {fmt_ms(s.shuffle_write_time_ms)} | {fmt_ms(s.fetch_wait_time_ms)} "
                f"| {fmt_bytes(s.memory_bytes_spilled)} | {fmt_bytes(s.disk_bytes_spilled)}{spill_disk_flag} "
                f"| {fmt_bytes(s.peak_execution_memory_bytes)} "
                f"| {s.skew_ratio}{skew_flag} |"
            )

        if summary.sql_plan_tree:
            lines += [
                "",
                "## SQL Physical Plan (Structured)",
                "(interactive rendering available in desktop UI)",
            ]

        skewed = [s for s in summary.stages if s.has_skew]
        if skewed:
            lines += ["", "## ⚠️ Skewed Stages Detected"]
            for s in skewed:
                lines.append(f"- **Stage {s.stage_id}** ({s.name}): skew ratio = {s.skew_ratio}×")

        spilled = [s for s in summary.stages if s.has_spill]
        if spilled:
            lines += ["", "## 💾 Stages with Disk Spill"]
            for s in spilled:
                disk = fmt_bytes(s.disk_bytes_spilled)
                mem = fmt_bytes(s.memory_bytes_spilled)
                lines.append(f"- **Stage {s.stage_id}** ({s.name}): disk spill = {disk}, memory spill = {mem}")

        heavy_shuffle = [s for s in summary.stages if s.has_heavy_shuffle]
        if heavy_shuffle:
            lines += ["", "## 🔀 Stages with Heavy Shuffle"]
            for s in heavy_shuffle:
                lines.append(
                    f"- **Stage {s.stage_id}** ({s.name}): "
                    f"shuffle read = {fmt_bytes(s.shuffle_read_bytes)}, "
                    f"shuffle write = {fmt_bytes(s.shuffle_write_bytes)}, "
                    f"SW time = {fmt_ms(s.shuffle_write_time_ms)}, "
                    f"fetch wait = {fmt_ms(s.fetch_wait_time_ms)}"
                )

        return "\n".join(lines)


class CompactMarkdownRenderer(MarkdownRenderer):
    """Strategy variant: shorter output, top-5 stages only."""
    def render(self, summary: AppSummary) -> str:
        full = super().render(summary)
        return full[:3000] + "\n\n*(truncated — compact mode)*" if len(full) > 3000 else full


class JsonRenderer(BaseRenderer):
    def render(self, summary: AppSummary) -> str:
        return summary.model_dump_json(indent=2)


# ─── Reducer Facade ──────────────────────────────────────────────────────────

class LogReducer:
    """
    Facade that wires the CoR pipeline and delegates to the chosen renderer.
    """

    RENDERERS = {
        ("md", False): MarkdownRenderer,
        ("md", True): CompactMarkdownRenderer,
        ("json", False): JsonRenderer,
        ("json", True): JsonRenderer,
    }

    def __init__(self, output_format: str = "md", compact: bool = False):
        self._renderer: BaseRenderer = self.RENDERERS.get(
            (output_format, compact), MarkdownRenderer
        )()
        # Build the chain: single-pass replaces the old three-handler sequence
        single = SinglePassHandler()
        builder = SummaryBuilderHandler()
        single.set_next(builder)
        self._chain = single

    def reduce(
        self,
        zip_bytes: bytes,
        progress_cb: ProgressCallback = None,
    ) -> tuple[AppSummary, str]:
        ctx = self._chain.handle({"zip_bytes": zip_bytes, "progress_cb": progress_cb})
        if progress_cb:
            progress_cb(80, "rendering_report")
        summary: AppSummary = ctx["summary"]
        report = self._renderer.render(summary)
        if progress_cb:
            progress_cb(85, "report_ready")
        return summary, report
