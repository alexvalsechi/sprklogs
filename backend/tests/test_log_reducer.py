"""
Unit tests for LogReducer pipeline.
Uses fixtures and DI — no real ZIP files needed.
"""
from __future__ import annotations

import io
import json
import zipfile
import pytest

from backend.services.log_reducer import (
    LogReducer,
    EventLoaderHandler,
    AppMetaHandler,
    StageAggregationHandler,
    SummaryBuilderHandler,
    MarkdownRenderer,
    JsonRenderer,
)
from backend.models.job import AppSummary, StageMetrics


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _make_zip(events: list[dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        content = "\n".join(json.dumps(e) for e in events)
        zf.writestr("app-20240101-logs/events", content)
    return buf.getvalue()


SAMPLE_EVENTS = [
    {"Event": "SparkListenerApplicationStart", "App ID": "app-001", "App Name": "TestJob", "Spark Version": "3.4.0", "Timestamp": 1_000_000},
    {"Event": "SparkListenerApplicationEnd", "Timestamp": 1_060_000},
    {"Event": "SparkListenerExecutorAdded"},
    {"Event": "SparkListenerExecutorAdded"},
    {"Event": "SparkListenerStageCompleted", "Stage Info": {
        "Stage ID": 0, "Stage Name": "count at main.py:10",
        "Submission Time": 1_001_000, "Completion Time": 1_020_000,
    }},
    *[
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 0,
            "Task Info": {"Launch Time": 1_001_000, "Finish Time": 1_001_000 + (i * 100)},
            "Task Metrics": {
                "Input Metrics": {"Bytes Read": 1024 * i},
                "Output Metrics": {"Bytes Written": 512 * i},
                "Shuffle Read Metrics": {"Total Bytes Read": 256 * i},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 128 * i},
                "JVM GC Time": 50 * i,
            },
        }
        for i in range(1, 11)
    ],
]


@pytest.fixture
def sample_zip() -> bytes:
    return _make_zip(SAMPLE_EVENTS)


@pytest.fixture
def sample_summary() -> AppSummary:
    reducer = LogReducer()
    summary, _ = reducer.reduce(_make_zip(SAMPLE_EVENTS))
    return summary


# ─── Tests ───────────────────────────────────────────────────────────────────

class TestEventLoader:
    def test_loads_events(self, sample_zip):
        handler = EventLoaderHandler()
        ctx = handler.process({"zip_bytes": sample_zip})
        assert "SparkListenerApplicationStart" in ctx["events"]
        assert "SparkListenerTaskEnd" in ctx["events"]
        assert len(ctx["events"]["SparkListenerTaskEnd"]) == 10

    def test_handles_malformed_lines(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("log", '{"Event": "SparkListenerApplicationStart"}\nnot json\n{"Event": "SparkListenerApplicationEnd", "Timestamp": 0}')
        ctx = EventLoaderHandler().process({"zip_bytes": buf.getvalue()})
        assert "SparkListenerApplicationStart" in ctx["events"]


class TestStageAggregation:
    def test_aggregates_tasks_by_stage(self, sample_zip):
        ctx: dict = {"zip_bytes": sample_zip}
        ctx = EventLoaderHandler().process(ctx)
        ctx = AppMetaHandler().process(ctx)
        ctx = StageAggregationHandler().process(ctx)

        assert len(ctx["stages"]) == 1
        stage = ctx["stages"][0]
        assert stage.stage_id == 0
        assert stage.num_tasks == 10
        assert stage.skew_ratio is not None
        assert stage.task_duration_max_ms > stage.task_duration_avg_ms

    def test_skew_detection(self):
        """A stage where one task takes 100× longer should flag as skewed."""
        stage = StageMetrics(
            stage_id=0, name="test", num_tasks=10, duration_ms=5000,
            input_bytes=0, output_bytes=0, shuffle_read_bytes=0,
            shuffle_write_bytes=0, gc_time_ms=0, skew_ratio=5.0,
        )
        assert stage.has_skew is True

    def test_no_skew(self):
        stage = StageMetrics(
            stage_id=1, name="test", num_tasks=10, duration_ms=1000,
            input_bytes=0, output_bytes=0, shuffle_read_bytes=0,
            shuffle_write_bytes=0, gc_time_ms=0, skew_ratio=1.2,
        )
        assert stage.has_skew is False


class TestLogReducer:
    def test_full_pipeline_returns_summary_and_report(self, sample_zip):
        reducer = LogReducer()
        summary, report = reducer.reduce(sample_zip)
        assert summary.app_id == "app-001"
        assert summary.num_tasks == 10
        assert "TestJob" in report

    def test_json_renderer(self, sample_zip):
        import json as _json
        reducer = LogReducer(output_format="json")
        summary, report = reducer.reduce(sample_zip)
        data = _json.loads(report)
        assert data["app_id"] == "app-001"

    def test_compact_renderer_truncates(self, sample_zip):
        reducer = LogReducer(output_format="md", compact=True)
        _, report = reducer.reduce(sample_zip)
        assert len(report) <= 3100  # compact adds tiny suffix


class TestMarkdownRenderer:
    def test_renders_without_error(self, sample_summary):
        r = MarkdownRenderer()
        md = r.render(sample_summary)
        assert "# Spark Log Report" in md
        assert "Stage Breakdown" in md


class TestZipBombProtection:
    def test_rejects_too_many_files(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for i in range(2000):  # More than max_files_in_zip (1000)
                zf.writestr(f"file{i}.json", '{"Event": "SparkListenerApplicationStart"}')
        zip_bytes = buf.getvalue()
        
        from backend.services.log_reducer import _iter_events
        with pytest.raises(ValueError, match="too many files"):
            list(_iter_events(zip_bytes))

    def test_rejects_high_compression_ratio(self):
        # Create a file with high compression ratio (simulate ZIP bomb)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            # Write a large amount of compressible data
            large_data = b'0' * 1000000  # 1 MB of zeros, compresses to ~1 KB
            zf.writestr("bomb.txt", large_data)
        zip_bytes = buf.getvalue()
        
        from backend.services.log_reducer import _iter_events
        with pytest.raises(ValueError, match="suspicious compression ratio"):
            list(_iter_events(zip_bytes))

    def test_json_renderer(self, sample_summary):
        r = JsonRenderer()
        out = r.render(sample_summary)
        import json
        data = json.loads(out)
        assert "stages" in data


class TestReduceScriptJsonOutput:
    """Validates the JSON contract that reduce_log.py emits on stdout.

    reduce_log.py prints json.dumps(summary.model_dump()) so Electron
    can capture structured summary data without re-parsing markdown.
    """

    def test_summary_model_dump_is_json_serializable(self, sample_summary):
        """summary.model_dump() must round-trip through JSON without error."""
        import json
        data = sample_summary.model_dump()
        serialized = json.dumps(data)
        deserialized = json.loads(serialized)
        assert deserialized["app_name"] == "TestJob"
        assert deserialized["num_tasks"] == 10
        assert isinstance(deserialized["stages"], list)
        assert len(deserialized["stages"]) == 1

    def test_summary_stages_have_renderer_required_fields(self, sample_summary):
        """Every stage dict must include all fields read by renderResults() in the renderer."""
        data = sample_summary.model_dump()
        stages = data["stages"]
        assert stages, "Expected at least one stage in summary"
        required = {
            "stage_id", "name", "num_tasks", "duration_ms",
            "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
        }
        for stage in stages:
            missing = required - stage.keys()
            assert not missing, f"Stage {stage.get('stage_id')} missing renderer fields: {missing}"

    def test_summary_top_level_fields_present(self, sample_summary):
        """Top-level summary fields expected by the KPI cards must be present."""
        data = sample_summary.model_dump()
        required = {
            "app_name", "spark_version", "total_duration_ms",
            "num_stages", "num_tasks", "executor_count",
            "total_input_bytes", "total_shuffle_read_bytes",
        }
        missing = required - data.keys()
        assert not missing, f"AppSummary missing KPI fields: {missing}"
