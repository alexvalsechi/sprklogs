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

from backend.models.job import AppSummary, StageGroup, StageMetrics
from backend.utils.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Publicly re-export group_stages and _normalize_stage_name for tests
__all__ = [
    "LogReducer",
    "SinglePassHandler",
    "SummaryBuilderHandler",
    "StageAccumulator",
    "MarkdownRenderer",
    "CompactMarkdownRenderer",
    "JsonRenderer",
    "group_stages",
    "_normalize_stage_name",
    "_iter_events",
]

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

    # ── Executor-level fields ──────────────────────────────────────────────
    cpu_time_ms: int = 0           # Executor CPU Time summed, nanoseconds→ms
    deserialize_time_ms: int = 0   # Executor Deserialize Time summed, ms
    result_size_bytes: int = 0     # Result Size summed, bytes
    minor_gc_count: int = 0        # MinorGCCount from Task Executor Metrics
    major_gc_count: int = 0        # MajorGCCount from Task Executor Metrics
    total_gc_time_tem_ms: int = 0  # TotalGCTime from Task Executor Metrics, ms

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
        # executor-level params
        cpu_time_ns: int = 0,
        deserialize_ms: int = 0,
        result_size: int = 0,
        minor_gc: int = 0,
        major_gc: int = 0,
        total_gc_tem_ms: int = 0,
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

        # Executor-level aggregates
        self.cpu_time_ms += cpu_time_ns // 1_000_000
        self.deserialize_time_ms += deserialize_ms
        self.result_size_bytes += result_size
        self.minor_gc_count += minor_gc
        self.major_gc_count += major_gc
        self.total_gc_time_tem_ms += total_gc_tem_ms

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
        # Use bisect to maintain sorted order efficiently
        # For small lists, sorted() is fast enough; for large ones,
        # we could use a sorted list with bisect.insort
        if len(self._reservoir) <= 1000:
            s = sorted(self._reservoir)
        else:
            # For large reservoirs, use approximate selection
            import random
            # Sample and sort for better performance
            sample = random.sample(self._reservoir, min(1000, len(self._reservoir)))
            s = sorted(sample)
        return s[int(len(s) * 0.95)]

    @property
    def skew_ratio(self) -> float:
        avg = self.dur_avg
        return self.dur_max / avg if avg > 0 else 0.0

    # ── Derived executor-level properties ──────────────────────────────────

    @property
    def cpu_efficiency(self) -> float:
        """CPU time / wall-clock run time.  <0.3 often indicates I/O or shuffle wait."""
        return self.cpu_time_ms / self.dur_sum if self.dur_sum > 0 else 0.0

    @property
    def gc_overhead_pct(self) -> float:
        """GC time as % of total run time (JVM GC Time)."""
        return (self.gc_time / self.dur_sum * 100) if self.dur_sum > 0 else 0.0

    @property
    def deserialize_overhead_pct(self) -> float:
        """Deserialize time as % of total run time."""
        return (self.deserialize_time_ms / self.dur_sum * 100) if self.dur_sum > 0 else 0.0

    @property
    def avg_result_size_kb(self) -> float:
        return (self.result_size_bytes / self.count / 1024) if self.count > 0 else 0.0


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

    # Sort once by node count (descending)
    sorted_execs = sorted(annotated, key=lambda x: -x["_nc"])

    # Pass 1 — always keep write/insert and large plans (>100 nodes)
    for ex in sorted_execs:
        if ex["_write"] or ex["_nc"] > 100:
            selected.append(ex)

    # Pass 2 — fill remainder with deduplicated samples of smaller plans
    for ex in sorted_execs:
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

        # ── executor registry and per-executor task stats
        executor_registry: dict[str, dict] = {}   # ex_id → {host, cores}
        executor_task_stats: dict[str, dict] = {}  # ex_id → aggregated counters

        # ── stage accumulators
        by_stage: dict[int, StageAccumulator] = {}
        stage_names: dict[int, str] = {}
        stage_info: dict[int, dict] = {}   # sid → Stage Info dict from StageCompleted

        # Cache events for reuse by Sparklens (avoids double-pass)
        events_cache: list[dict] = []

        for ev in _iter_events(zip_bytes, progress_cb):
            events_cache.append(ev)
            etype = ev.get("Event", "")

            if etype == "SparkListenerApplicationStart":
                app_start = ev
            elif etype == "SparkListenerApplicationEnd":
                app_end = ev
            elif etype == "SparkListenerEnvironmentUpdate":
                env_update = ev
            elif etype == "SparkListenerExecutorAdded":
                executor_count += 1
                ex_id = str(ev.get("Executor ID", ""))
                ei = ev.get("Executor Info", {}) or {}
                executor_registry[ex_id] = {
                    "host": ei.get("Host", "unknown"),
                    "cores": ei.get("Total Cores", 0),
                }
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
                tem = ev.get("Task Executor Metrics", {}) or {}
                sr = tm.get("Shuffle Read Metrics", {})
                sw = tm.get("Shuffle Write Metrics", {})

                # ── executor-level raw values
                cpu_time_ns = tm.get("Executor CPU Time", 0)
                deserialize_ms = tm.get("Executor Deserialize Time", 0)
                result_size = tm.get("Result Size", 0)
                minor_gc = tem.get("MinorGCCount", 0)
                major_gc = tem.get("MajorGCCount", 0)
                total_gc_tem_ms = tem.get("TotalGCTime", 0)

                task_dur = ti.get("Finish Time", 0) - ti.get("Launch Time", 0)

                acc.add(
                    duration=task_dur,
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
                    cpu_time_ns=cpu_time_ns,
                    deserialize_ms=deserialize_ms,
                    result_size=result_size,
                    minor_gc=minor_gc,
                    major_gc=major_gc,
                    total_gc_tem_ms=total_gc_tem_ms,
                )

                # Aggregate per-executor stats
                ex_id = str(ti.get("Executor ID", ""))
                if ex_id not in executor_task_stats:
                    executor_task_stats[ex_id] = {
                        "tasks": 0, "run_ms": 0, "gc_ms": 0, "cpu_ms": 0
                    }
                es = executor_task_stats[ex_id]
                es["tasks"] += 1
                es["run_ms"] += task_dur
                es["gc_ms"] += tm.get("JVM GC Time", 0)
                es["cpu_ms"] += cpu_time_ns // 1_000_000

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

        # ── build executor summary
        executor_summary = []
        for ex_id in sorted(executor_task_stats.keys()):
            es = executor_task_stats[ex_id]
            info = executor_registry.get(ex_id, {})
            run_ms = es["run_ms"]
            executor_summary.append({
                "executor_id": ex_id,
                "host": info.get("host", "unknown"),
                "cores": info.get("cores", 0),
                "tasks": es["tasks"],
                "gc_ms": es["gc_ms"],
                "gc_pct": round(es["gc_ms"] / run_ms * 100, 2) if run_ms else 0.0,
                "cpu_efficiency": round(es["cpu_ms"] / run_ms, 3) if run_ms else 0.0,
            })
        ctx["executor_summary"] = executor_summary

        # ── job-level efficiency aggregates
        total_run_ms = sum(es["run_ms"] for es in executor_task_stats.values())
        total_cpu_ms = sum(es["cpu_ms"] for es in executor_task_stats.values())
        total_gc_ms = sum(es["gc_ms"] for es in executor_task_stats.values())
        total_deser_ms = sum(acc.deserialize_time_ms for acc in by_stage.values())
        ctx["job_efficiency_meta"] = {
            "cpu_efficiency": round(total_cpu_ms / total_run_ms, 3) if total_run_ms else 0.0,
            "gc_overhead_pct": round(total_gc_ms / total_run_ms * 100, 2) if total_run_ms else 0.0,
            "deserialize_overhead_pct": round(total_deser_ms / total_run_ms * 100, 2) if total_run_ms else 0.0,
        }

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
                    cpu_efficiency=round(acc.cpu_efficiency, 3),
                    gc_overhead_pct=round(acc.gc_overhead_pct, 2),
                    deserialize_time_ms=acc.deserialize_time_ms,
                    minor_gc_count=acc.minor_gc_count,
                    major_gc_count=acc.major_gc_count,
                    avg_result_size_kb=round(acc.avg_result_size_kb, 1),
                )
            )

        stages.sort(key=lambda s: s.stage_id)
        ctx["stages"] = stages

        # Cache events for reuse (avoids double-pass in Sparklens)
        ctx["_events_cache"] = events_cache

        if progress_cb:
            progress_cb(70, "stages_ready")

        return ctx


# ─── Stage Grouping ──────────────────────────────────────────────────────────

# Regex to strip trailing " at ClassName.method:lineNo" or similar source refs
_RE_AT_SOURCE = re.compile(r"\s+at\s+\S+$")
# Regex to strip trailing numeric partition/attempt suffixes like " (4)"
_RE_TRAILING_PAREN_NUM = re.compile(r"\s*\(\d+\)\s*$")


def _normalize_stage_name(name: str) -> str:
    """Normalize a Spark stage name for grouping purposes.

    Strips source-reference suffixes (`` at Foo.java:42``), trailing
    parenthesized numbers (``(3)``), and collapses whitespace.
    """
    n = _RE_AT_SOURCE.sub("", name)
    n = _RE_TRAILING_PAREN_NUM.sub("", n)
    return n.strip()


def group_stages(stages: list[StageMetrics]) -> list[StageGroup]:
    """Group stages by normalized name and compute per-group aggregates.

    Returns groups ordered by first stage_id in each group.
    """
    if not stages:
        return []

    from collections import OrderedDict

    buckets: OrderedDict[str, list[StageMetrics]] = OrderedDict()
    for s in stages:
        key = _normalize_stage_name(s.name)
        buckets.setdefault(key, []).append(s)

    groups: list[StageGroup] = []
    for gname, members in buckets.items():
        stage_ids = [m.stage_id for m in members]
        count = len(members)

        skew_vals = [m.skew_ratio for m in members if m.skew_ratio is not None]
        gc_vals = [m.gc_overhead_pct for m in members if m.gc_overhead_pct is not None]
        cpu_vals = [m.cpu_efficiency for m in members if m.cpu_efficiency is not None]

        # Detect anomalies within the group
        anomalies: list[str] = []
        for m in members:
            if m.has_skew:
                anomalies.append(f"skew={m.skew_ratio:.1f}x in stage {m.stage_id}")
            if m.has_spill:
                disk_mb = m.disk_bytes_spilled / (1024 * 1024)
                anomalies.append(f"disk_spill={disk_mb:.0f}MB in stage {m.stage_id}")
            if (m.gc_overhead_pct or 0) > 5.0:
                anomalies.append(f"gc={m.gc_overhead_pct:.1f}% in stage {m.stage_id}")
            if m.cpu_efficiency is not None and m.cpu_efficiency < 0.1 and m.num_tasks >= 10:
                anomalies.append(f"low_cpu={m.cpu_efficiency:.3f} in stage {m.stage_id}")

        groups.append(StageGroup(
            group_name=gname,
            stage_ids=stage_ids,
            count=count,
            total_tasks=sum(m.num_tasks for m in members),
            total_duration_ms=sum(m.duration_ms for m in members),
            total_input_bytes=sum(m.input_bytes for m in members),
            total_output_bytes=sum(m.output_bytes for m in members),
            total_shuffle_read_bytes=sum(m.shuffle_read_bytes for m in members),
            total_shuffle_write_bytes=sum(m.shuffle_write_bytes for m in members),
            total_disk_spill_bytes=sum(m.disk_bytes_spilled for m in members),
            total_memory_spill_bytes=sum(m.memory_bytes_spilled for m in members),
            total_gc_time_ms=sum(m.gc_time_ms for m in members),
            skew_ratio_min=round(min(skew_vals), 2) if skew_vals else None,
            skew_ratio_avg=round(sum(skew_vals) / len(skew_vals), 2) if skew_vals else None,
            skew_ratio_max=round(max(skew_vals), 2) if skew_vals else None,
            worst_gc_overhead_pct=round(max(gc_vals), 2) if gc_vals else 0.0,
            worst_cpu_efficiency=round(min(cpu_vals), 3) if cpu_vals else None,
            worst_disk_spill_bytes=max(m.disk_bytes_spilled for m in members),
            peak_execution_memory_bytes=max(m.peak_execution_memory_bytes for m in members),
            anomalies=anomalies,
        ))

    return groups


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
            executor_summary=ctx.get("executor_summary", []),
            job_efficiency_meta=ctx.get("job_efficiency_meta", {}),
            stages=stages,
            stage_groups=group_stages(stages),
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
        ]

        # ── Job-Level Efficiency summary (3 KPIs)
        jem = getattr(summary, "job_efficiency_meta", {}) or {}
        if jem:
            lines += [
                "",
                "## Job-Level Efficiency",
                "| Metric | Value | Interpretation |",
                "|---|---|---|",
                f"| CPU Efficiency | {jem.get('cpu_efficiency', 0):.3f} "
                f"| ratio CPU time / wall-clock run time (1.0 = fully compute-bound) |",
                f"| GC Overhead | {jem.get('gc_overhead_pct', 0):.2f}% "
                f"| % of task wall-clock spent in GC (>5% is concerning) |",
                f"| Deserialize Overhead | {jem.get('deserialize_overhead_pct', 0):.2f}% "
                f"| % of task wall-clock spent deserializing inputs |",
            ]

        # ── Executor-Level Metrics — concise table
        exec_summary = getattr(summary, "executor_summary", []) or []
        if exec_summary:
            lines += [
                "",
                "## Executor-Level Metrics",
                "| Executor | Cores | Tasks | GC ms | GC% | CPU Efficiency |",
                "|---|---|---|---|---|---|",
            ]
            for ex in exec_summary:
                lines.append(
                    f"| {ex['executor_id']} | {ex['cores']} | {ex['tasks']} "
                    f"| {ex['gc_ms']:,} | {ex['gc_pct']:.2f}% | {ex['cpu_efficiency']:.3f} |"
                )
            outliers = [
                ex for ex in exec_summary
                if ex["gc_pct"] > 5.0 or ex["cpu_efficiency"] < 0.05
            ]
            if outliers:
                lines += ["", "**Outlier executors** (GC > 5% or CPU efficiency < 0.05):"]
                for ex in outliers:
                    lines.append(
                        f"- Executor {ex['executor_id']} ({ex['host']}): "
                        f"gc={ex['gc_pct']:.2f}%, cpu_eff={ex['cpu_efficiency']:.3f}"
                    )

        # ── Stage Groups — collapsed view for multi-stage groups
        groups = summary.stage_groups or []
        multi_groups = [g for g in groups if g.count > 1]
        solo_stages_in_groups = {
            g.stage_ids[0] for g in groups if g.count == 1
        }

        if multi_groups:
            lines += ["", "## Stage Groups (repeated operations)"]
            for g in multi_groups:
                ids_str = ",".join(str(i) for i in g.stage_ids)
                flags = ""
                if g.anomalies:
                    flags = " ⚠️"
                lines += [
                    f"### \"{g.group_name}\" ×{g.count} stages (IDs: {ids_str}){flags}",
                    f"| Metric | Total | Per-Stage Avg |",
                    f"|---|---|---|",
                    f"| Tasks | {g.total_tasks:,} | {g.total_tasks // g.count:,} |",
                    f"| Duration | {fmt_ms(g.total_duration_ms)} | {fmt_ms(g.total_duration_ms // g.count)} |",
                    f"| Input | {fmt_bytes(g.total_input_bytes)} | {fmt_bytes(g.total_input_bytes // g.count)} |",
                    f"| Shuffle Read | {fmt_bytes(g.total_shuffle_read_bytes)} | {fmt_bytes(g.total_shuffle_read_bytes // g.count)} |",
                    f"| Shuffle Write | {fmt_bytes(g.total_shuffle_write_bytes)} | {fmt_bytes(g.total_shuffle_write_bytes // g.count)} |",
                ]
                if g.total_disk_spill_bytes > 0:
                    lines.append(
                        f"| Disk Spill | {fmt_bytes(g.total_disk_spill_bytes)} "
                        f"| {fmt_bytes(g.total_disk_spill_bytes // g.count)} |"
                    )
                if g.skew_ratio_avg is not None:
                    lines.append(
                        f"| Skew Ratio | min={g.skew_ratio_min}× avg={g.skew_ratio_avg}× "
                        f"max={g.skew_ratio_max}× | — |"
                    )
                if g.worst_gc_overhead_pct > 5.0:
                    lines.append(
                        f"| Worst GC Overhead | {g.worst_gc_overhead_pct:.2f}% | — |"
                    )
                if g.anomalies:
                    lines.append("")
                    lines.append(f"**Anomalies ({len(g.anomalies)}):**")
                    for a in g.anomalies[:10]:  # cap displayed anomalies
                        lines.append(f"- {a}")
                lines.append("")

        # ── Individual Stage Breakdown for solo stages
        solo_stages = [s for s in summary.stages if s.stage_id in solo_stages_in_groups]
        # If no groups were formed (e.g., all unique names), show all stages
        if not groups:
            solo_stages = summary.stages

        if solo_stages:
            lines += [
                "",
                "## Stage Breakdown",
                "| Stage | Name | Tasks | Duration | Input | Shuffle R | Shuffle W "
                "| SW Time | Fetch Wait | Spill Mem | Spill Disk | Peak Mem "
                "| Skew | CPU Eff | GC% | Deser ms |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
            ]
            for s in solo_stages:
                skew_flag = " ⚠️" if s.has_skew else ""
                spill_disk_flag = " 💾" if s.has_spill else ""
                cpu_eff = getattr(s, "cpu_efficiency", None)
                gc_pct = getattr(s, "gc_overhead_pct", None)
                deser_ms = getattr(s, "deserialize_time_ms", None)
                lines.append(
                    f"| {s.stage_id} | {s.name[:35]} | {s.num_tasks} "
                    f"| {fmt_ms(s.duration_ms)} | {fmt_bytes(s.input_bytes)} "
                    f"| {fmt_bytes(s.shuffle_read_bytes)} | {fmt_bytes(s.shuffle_write_bytes)} "
                    f"| {fmt_ms(s.shuffle_write_time_ms)} | {fmt_ms(s.fetch_wait_time_ms)} "
                    f"| {fmt_bytes(s.memory_bytes_spilled)} | {fmt_bytes(s.disk_bytes_spilled)}{spill_disk_flag} "
                    f"| {fmt_bytes(s.peak_execution_memory_bytes)} "
                    f"| {s.skew_ratio}{skew_flag} "
                    f"| {cpu_eff if cpu_eff is not None else 'n/a'} "
                    f"| {gc_pct if gc_pct is not None else 'n/a'} "
                    f"| {deser_ms if deser_ms is not None else 'n/a'} |"
                )

        if summary.sql_plan_tree:
            lines += [
                "",
                "## SQL Physical Plan (Structured)",
                "(interactive rendering available in desktop UI)",
            ]

        # ── Anomaly summary — consolidated from all stages (grouped + solo)
        all_anomalies: list[str] = []
        for g in groups:
            all_anomalies.extend(g.anomalies)

        skewed = [s for s in summary.stages if s.has_skew]
        spilled = [s for s in summary.stages if s.has_spill]
        heavy_shuffle = [s for s in summary.stages if s.has_heavy_shuffle]
        low_cpu = [
            s for s in summary.stages
            if getattr(s, "cpu_efficiency", None) is not None
            and s.cpu_efficiency < 0.1
            and s.num_tasks >= 10
        ]

        if skewed or spilled or heavy_shuffle or low_cpu:
            lines += ["", "## ⚠️ Anomaly Summary"]
            if skewed:
                lines.append(f"- **Skewed stages** ({len(skewed)}): "
                    + ", ".join(f"Stage {s.stage_id} ({s.skew_ratio}×)" for s in skewed))
            if spilled:
                lines.append(f"- **Disk spill** ({len(spilled)}): "
                    + ", ".join(f"Stage {s.stage_id} ({fmt_bytes(s.disk_bytes_spilled)})" for s in spilled))
            if heavy_shuffle:
                lines.append(f"- **Heavy shuffle** ({len(heavy_shuffle)}): "
                    + ", ".join(f"Stage {s.stage_id}" for s in heavy_shuffle))
            if low_cpu:
                lines.append(f"- **Low CPU efficiency** ({len(low_cpu)}): "
                    + ", ".join(f"Stage {s.stage_id} ({s.cpu_efficiency:.3f})" for s in low_cpu))

        return "\n".join(lines)


class CompactMarkdownRenderer(MarkdownRenderer):
    """Strategy variant: shows only groups and top-5 anomalous stages."""
    def render(self, summary: AppSummary) -> str:
        full = super().render(summary)
        # Keep full output if reasonably sized; truncate intelligently otherwise
        if len(full) <= 5000:
            return full
        # Find the Anomaly Summary section and keep it
        anomaly_idx = full.find("## ⚠️ Anomaly Summary")
        if anomaly_idx > 0:
            header = full[:3000]
            anomaly_section = full[anomaly_idx:]
            return header + "\n\n...\n\n" + anomaly_section
        return full[:5000] + "\n\n*(truncated — compact mode)*"


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
        self._cached_events: list[dict] | None = None

    def reduce(
        self,
        zip_bytes: bytes,
        progress_cb: ProgressCallback = None,
    ) -> tuple[AppSummary, str]:
        ctx = self._chain.handle({"zip_bytes": zip_bytes, "progress_cb": progress_cb})
        # Cache the ctx for potential reuse (e.g. Sparklens report)
        self._cached_events = ctx.get("_events_cache")
        if progress_cb:
            progress_cb(80, "rendering_report")
        summary: AppSummary = ctx["summary"]
        report = self._renderer.render(summary)
        if progress_cb:
            progress_cb(85, "report_ready")
        return summary, report
