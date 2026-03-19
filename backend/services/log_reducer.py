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
import zipfile
import statistics
import logging
from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional

from backend.models.job import AppSummary, StageMetrics
from backend.utils.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# ─── Iterator ────────────────────────────────────────────────────────────────

def _iter_events(zip_bytes: bytes) -> Iterator[dict]:
    """Yield parsed JSON events from every file inside the ZIP."""
    logger.info(f"Processing ZIP with {len(zip_bytes)} bytes")
    
    # ZIP bomb protection
    uncompressed_size = 0
    file_count = 0
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Check number of files
        namelist = zf.namelist()
        if len(namelist) > settings.max_files_in_zip:
            raise ValueError(f"ZIP contains too many files: {len(namelist)} > {settings.max_files_in_zip}")
        
        for name in namelist:
            if name.endswith("/"):
                continue
            file_count += 1
            
            # Check individual file size
            with zf.open(name) as fh:
                file_data = fh.read()
                uncompressed_size += len(file_data)
                
                # Check compression ratio
                compressed_size = zf.getinfo(name).compress_size
                if compressed_size > 0:
                    ratio = len(file_data) / compressed_size
                    if ratio > settings.compression_ratio_limit:
                        raise ValueError(f"File {name} has suspicious compression ratio: {ratio:.1f} > {settings.compression_ratio_limit}")
                
                # Process lines
                for raw_line in io.BytesIO(file_data):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        # Basic content validation - check for Spark event structure
                        if not isinstance(event, dict) or "Event" not in event:
                            logger.warning(f"Skipping non-Spark event in {name}: {event}")
                            continue
                        yield event
                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping malformed JSON line in {name}: {e}")
                        pass  # skip malformed lines
        
        # Check total uncompressed size
        uncompressed_mb = uncompressed_size / (1024 * 1024)
        if uncompressed_mb > settings.max_uncompressed_mb:
            raise ValueError(f"Uncompressed ZIP size too large: {uncompressed_mb:.2f} MB > {settings.max_uncompressed_mb} MB")
    
    logger.info(f"Processed {file_count} files, total uncompressed size: {uncompressed_mb:.2f} MB, yielded events")


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


class EventLoaderHandler(BaseHandler):
    """Reads all events from the ZIP and stores them grouped by type."""

    def process(self, ctx: dict) -> dict:
        zip_bytes: bytes = ctx["zip_bytes"]
        events_by_type: dict[str, list[dict]] = {}
        for ev in _iter_events(zip_bytes):
            etype = ev.get("Event", "Unknown")
            events_by_type.setdefault(etype, []).append(ev)
        ctx["events"] = events_by_type
        logger.debug("Loaded event types: %s", list(events_by_type.keys()))
        return ctx


class AppMetaHandler(BaseHandler):
    """Extracts application-level metadata."""

    def process(self, ctx: dict) -> dict:
        events = ctx.get("events", {})
        start_ev = (events.get("SparkListenerApplicationStart") or [{}])[0]
        end_ev = (events.get("SparkListenerApplicationEnd") or [{}])[0]
        env_ev = (events.get("SparkListenerEnvironmentUpdate") or [{}])[0]
        executor_evs = events.get("SparkListenerExecutorAdded", [])

        spark_props = {
            k: v
            for k, v in (env_ev.get("Spark Properties", {}) or {}).items()
        }

        ctx["app_meta"] = {
            "app_id": start_ev.get("App ID", "unknown"),
            "app_name": start_ev.get("App Name", "unknown"),
            "spark_version": start_ev.get("Spark Version", spark_props.get("spark.version", "unknown")),
            "start_time_ms": start_ev.get("Timestamp", 0),
            "end_time_ms": end_ev.get("Timestamp", 0),
            "executor_count": len(executor_evs),
        }
        return ctx


class StageAggregationHandler(BaseHandler):
    """
    Aggregates TaskEnd events per stage into statistical summaries.
    Factory sub-pattern: _build_stage() constructs StageMetrics objects.
    """

    def process(self, ctx: dict) -> dict:
        events = ctx.get("events", {})
        task_events: list[dict] = events.get("SparkListenerTaskEnd", [])
        stage_info_evs: list[dict] = events.get("SparkListenerStageCompleted", [])

        # Build name map from StageCompleted
        stage_names: dict[int, str] = {}
        stage_completed: dict[int, dict] = {}
        for ev in stage_info_evs:
            si = ev.get("Stage Info", {})
            sid = si.get("Stage ID", -1)
            stage_names[sid] = si.get("Stage Name", f"Stage {sid}")
            stage_completed[sid] = si

        # Group task durations/metrics by stage
        by_stage: dict[int, dict[str, list]] = {}
        for ev in task_events:
            sid = ev.get("Stage ID", -1)
            tm = ev.get("Task Metrics", {})
            entry = by_stage.setdefault(sid, {
                "durations": [], "input": [], "output": [],
                "shuffle_read": [], "shuffle_write": [], "gc": [],
                "memory_spill": [], "disk_spill": [],
                "shuffle_write_time": [], "fetch_wait": [], "remote_to_disk": [],
                "peak_exec_mem": [], "shuffle_read_records": [], "shuffle_write_records": [],
            })
            entry["durations"].append(ev.get("Task Info", {}).get("Finish Time", 0) - ev.get("Task Info", {}).get("Launch Time", 0))
            entry["input"].append(tm.get("Input Metrics", {}).get("Bytes Read", 0))
            entry["output"].append(tm.get("Output Metrics", {}).get("Bytes Written", 0))
            sr = tm.get("Shuffle Read Metrics", {})
            sw = tm.get("Shuffle Write Metrics", {})
            entry["shuffle_read"].append(sr.get("Total Bytes Read", 0))
            entry["shuffle_write"].append(sw.get("Shuffle Bytes Written", 0))
            entry["gc"].append(tm.get("JVM GC Time", 0))
            entry["memory_spill"].append(tm.get("Memory Bytes Spilled", 0))
            entry["disk_spill"].append(tm.get("Disk Bytes Spilled", 0))
            entry["shuffle_write_time"].append(sw.get("Shuffle Write Time", 0) // 1_000_000)  # ns → ms
            entry["fetch_wait"].append(sr.get("Fetch Wait Time", 0))
            entry["remote_to_disk"].append(sr.get("Remote Bytes Read To Disk", 0))
            entry["peak_exec_mem"].append(tm.get("Peak Execution Memory", 0))
            entry["shuffle_read_records"].append(sr.get("Total Records Read", 0))
            entry["shuffle_write_records"].append(sw.get("Shuffle Records Written", 0))

        stages: list[StageMetrics] = []
        for sid, data in by_stage.items():
            si = stage_completed.get(sid, {}).get("Stage Info", {})
            stages.append(self._build_stage(sid, stage_names.get(sid, f"Stage {sid}"), data, si))

        stages.sort(key=lambda s: s.stage_id)
        ctx["stages"] = stages
        return ctx

    @staticmethod
    def _build_stage(sid: int, name: str, data: dict, si: dict) -> StageMetrics:
        """Factory: build a StageMetrics from raw aggregated data."""
        durations = data["durations"]
        n = len(durations)
        d_sorted = sorted(durations)
        p95 = d_sorted[int(n * 0.95)] if n else 0
        avg = statistics.mean(durations) if durations else 0
        skew = (max(durations) / avg) if avg > 0 else 0.0

        return StageMetrics(
            stage_id=sid,
            name=name,
            num_tasks=n,
            duration_ms=si.get("Completion Time", 0) - si.get("Submission Time", 0),
            input_bytes=sum(data["input"]),
            output_bytes=sum(data["output"]),
            shuffle_read_bytes=sum(data["shuffle_read"]),
            shuffle_write_bytes=sum(data["shuffle_write"]),
            gc_time_ms=sum(data["gc"]),
            task_duration_min_ms=min(durations) if durations else None,
            task_duration_avg_ms=round(avg, 1) if durations else None,
            task_duration_max_ms=max(durations) if durations else None,
            task_duration_p95_ms=p95 if durations else None,
            skew_ratio=round(skew, 2) if durations else None,
            memory_bytes_spilled=sum(data["memory_spill"]),
            disk_bytes_spilled=sum(data["disk_spill"]),
            shuffle_write_time_ms=sum(data["shuffle_write_time"]),
            fetch_wait_time_ms=sum(data["fetch_wait"]),
            remote_bytes_read_to_disk=sum(data["remote_to_disk"]),
            peak_execution_memory_bytes=max(data["peak_exec_mem"]) if data["peak_exec_mem"] else 0,
            shuffle_read_records=sum(data["shuffle_read_records"]),
            shuffle_write_records=sum(data["shuffle_write_records"]),
        )


class SummaryBuilderHandler(BaseHandler):
    """Assembles the final AppSummary from all gathered data."""

    def process(self, ctx: dict) -> dict:
        meta = ctx.get("app_meta", {})
        stages: list[StageMetrics] = ctx.get("stages", [])

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
            total_input_bytes=sum(s.input_bytes for s in stages),
            total_output_bytes=sum(s.output_bytes for s in stages),
            total_shuffle_read_bytes=sum(s.shuffle_read_bytes for s in stages),
            total_shuffle_write_bytes=sum(s.shuffle_write_bytes for s in stages),
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
        # Build the chain
        loader = EventLoaderHandler()
        meta = AppMetaHandler()
        agg = StageAggregationHandler()
        builder = SummaryBuilderHandler()
        loader.set_next(meta).set_next(agg).set_next(builder)
        self._chain = loader

    def reduce(self, zip_bytes: bytes) -> tuple[AppSummary, str]:
        ctx = self._chain.handle({"zip_bytes": zip_bytes})
        summary: AppSummary = ctx["summary"]
        report = self._renderer.render(summary)
        return summary, report
