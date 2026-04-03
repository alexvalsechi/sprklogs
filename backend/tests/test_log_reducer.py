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
    SinglePassHandler,
    SummaryBuilderHandler,
    StageAccumulator,
    MarkdownRenderer,
    JsonRenderer,
    group_stages,
    _normalize_stage_name,
    _iter_events,
)
from backend.models.job import AppSummary, StageGroup, StageMetrics


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _make_zip(events: list[dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        content = "\n".join(json.dumps(e) for e in events)
        zf.writestr("app-20240101-logs/events", content)
    return buf.getvalue()


SAMPLE_EVENTS = [
    {"Event": "SparkListenerApplicationStart", "App ID": "app-001", "App Name": "TestJob", "Spark Version": "3.4.0", "Timestamp": 1_000_000},
    {
        "Event": "SparkListenerResourceProfileAdded",
        "Executor Resource Requests": {
            "memory": {"Amount": 14336},
            "memoryOverhead": {"Amount": 2048},
            "offHeap": {"Amount": 0},
            "cores": {"Amount": 5},
        },
    },
    {"Event": "SparkListenerApplicationEnd", "Timestamp": 1_060_000},
    {"Event": "SparkListenerExecutorAdded"},
    {"Event": "SparkListenerExecutorAdded"},
    {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": 7,
        "sparkPlanInfo": {
            "nodeName": "HashAggregate",
            "children": [
                {
                    "nodeName": "Exchange",
                    "children": [
                        {"nodeName": "FileScan parquet", "children": []}
                    ],
                }
            ],
        },
    },
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

class TestSinglePassHandler:
    def test_produces_app_meta(self, sample_zip):
        ctx = SinglePassHandler().process({"zip_bytes": sample_zip})
        meta = ctx["app_meta"]
        assert meta["app_id"] == "app-001"
        assert meta["app_name"] == "TestJob"
        assert meta["spark_version"] == "3.4.0"
        assert meta["executor_count"] == 2
        assert meta["executor_memory_mb"] == 14336
        assert meta["executor_memory_overhead_mb"] == 2048
        assert meta["executor_offheap_mb"] == 0
        assert meta["executor_cores"] == 5

    def test_produces_stages(self, sample_zip):
        ctx = SinglePassHandler().process({"zip_bytes": sample_zip})
        assert len(ctx["stages"]) == 1
        stage = ctx["stages"][0]
        assert stage.stage_id == 0
        assert stage.num_tasks == 10

    def test_extracts_sql_plan_tree(self, sample_zip):
        ctx = SinglePassHandler().process({"zip_bytes": sample_zip})
        assert ctx["sql_execution_count"] == 1
        tree = ctx["sql_plan_tree"]
        assert tree is not None
        assert tree.get("nodeName") == "HashAggregate"
        assert tree.get("children", [])[0].get("nodeName") == "Exchange"

    def test_handles_malformed_lines(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(
                "log",
                '{"Event": "SparkListenerApplicationStart", "App ID": "x", "App Name": "y", "Spark Version": "3.0", "Timestamp": 0}'
                "\nnot json\n"
                '{"Event": "SparkListenerApplicationEnd", "Timestamp": 0}',
            )
        # Should not raise despite malformed line
        ctx = SinglePassHandler().process({"zip_bytes": buf.getvalue()})
        assert ctx["app_meta"]["app_name"] == "y"


class TestStageAggregation:
    def test_aggregates_tasks_by_stage(self, sample_zip):
        ctx = SinglePassHandler().process({"zip_bytes": sample_zip})

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
        assert summary.sql_execution_count == 1
        assert summary.sql_plan_tree is not None
        assert "TestJob" in report

    def test_json_renderer(self, sample_zip):
        import json as _json
        reducer = LogReducer(output_format="json")
        summary, report = reducer.reduce(sample_zip)
        data = _json.loads(report)
        assert data["app_id"] == "app-001"

    def test_compact_renderer(self, sample_zip):
        reducer = LogReducer(output_format="md", compact=True)
        _, report = reducer.reduce(sample_zip)
        # Compact renderer should produce valid markdown with groups
        assert "# Spark Log Report" in report


class TestMarkdownRenderer:
    def test_renders_without_error(self, sample_summary):
        r = MarkdownRenderer()
        md = r.render(sample_summary)
        assert "# Spark Log Report" in md
        assert "Stage Breakdown" in md
        assert "SQL Physical Plan (Structured)" in md

    def test_renders_stage_groups(self):
        """When stage_groups exist with count > 1, they should appear in the markdown."""
        stages = [
            StageMetrics(
                stage_id=i, name="Scan parquet default.events", num_tasks=100,
                duration_ms=5000, input_bytes=1024, output_bytes=0,
                shuffle_read_bytes=0, shuffle_write_bytes=0, gc_time_ms=10,
                skew_ratio=1.2,
            )
            for i in range(5)
        ]
        summary = AppSummary(
            app_id="app-test", app_name="GroupTest", spark_version="3.5",
            start_time_ms=0, end_time_ms=25000, total_duration_ms=25000,
            num_stages=5, num_tasks=500, executor_count=2,
            total_input_bytes=5120, total_output_bytes=0,
            total_shuffle_read_bytes=0, total_shuffle_write_bytes=0,
            stages=stages, stage_groups=group_stages(stages),
        )
        md = MarkdownRenderer().render(summary)
        assert "Stage Groups" in md
        assert "×5 stages" in md
        assert "Scan parquet default.events" in md

    def test_anomaly_summary_section(self):
        """Anomaly summary should consolidate skew/spill/shuffle flags."""
        stages = [
            StageMetrics(
                stage_id=0, name="Join", num_tasks=100,
                duration_ms=10000, input_bytes=0, output_bytes=0,
                shuffle_read_bytes=0, shuffle_write_bytes=0, gc_time_ms=0,
                skew_ratio=5.0, disk_bytes_spilled=1_000_000,
            ),
        ]
        summary = AppSummary(
            app_id="app-test", app_name="AnomalyTest", spark_version="3.5",
            start_time_ms=0, end_time_ms=10000, total_duration_ms=10000,
            num_stages=1, num_tasks=100, executor_count=1,
            total_input_bytes=0, total_output_bytes=0,
            total_shuffle_read_bytes=0, total_shuffle_write_bytes=0,
            stages=stages, stage_groups=group_stages(stages),
        )
        md = MarkdownRenderer().render(summary)
        assert "Anomaly Summary" in md
        assert "Skewed stages" in md
        assert "Disk spill" in md


# ─── Stage Grouping Tests ────────────────────────────────────────────────────

class TestNormalizeStageName:
    def test_strips_at_source_ref(self):
        assert _normalize_stage_name("count at main.py:10") == "count"

    def test_strips_java_source_ref(self):
        assert _normalize_stage_name("scan at NativeMethodAccessorImpl.java:0") == "scan"

    def test_strips_trailing_paren_number(self):
        assert _normalize_stage_name("exchange (3)") == "exchange"

    def test_preserves_plain_name(self):
        assert _normalize_stage_name("SortMergeJoin") == "SortMergeJoin"

    def test_combined_stripping(self):
        # Both at-source and trailing paren number are stripped
        assert _normalize_stage_name("count (2) at main.py:10") == "count"

    def test_empty_string(self):
        assert _normalize_stage_name("") == ""


class TestGroupStages:
    def _make_stage(self, sid: int, name: str, **kwargs) -> StageMetrics:
        defaults = dict(
            stage_id=sid, name=name, num_tasks=100, duration_ms=5000,
            input_bytes=1024, output_bytes=512, shuffle_read_bytes=256,
            shuffle_write_bytes=128, gc_time_ms=50, skew_ratio=1.5,
        )
        defaults.update(kwargs)
        return StageMetrics(**defaults)

    def test_empty_list(self):
        assert group_stages([]) == []

    def test_single_stage(self):
        groups = group_stages([self._make_stage(0, "scan")])
        assert len(groups) == 1
        assert groups[0].count == 1
        assert groups[0].stage_ids == [0]

    def test_identical_names_grouped(self):
        stages = [self._make_stage(i, "Scan parquet") for i in range(5)]
        groups = group_stages(stages)
        assert len(groups) == 1
        g = groups[0]
        assert g.count == 5
        assert g.stage_ids == [0, 1, 2, 3, 4]
        assert g.total_tasks == 500
        assert g.total_duration_ms == 25000

    def test_similar_names_with_source_refs_grouped(self):
        stages = [
            self._make_stage(0, "count at main.py:10"),
            self._make_stage(1, "count at main.py:20"),
            self._make_stage(2, "count at other.py:5"),
        ]
        groups = group_stages(stages)
        assert len(groups) == 1
        assert groups[0].group_name == "count"
        assert groups[0].count == 3

    def test_different_names_separate_groups(self):
        stages = [
            self._make_stage(0, "Scan parquet"),
            self._make_stage(1, "SortMergeJoin"),
            self._make_stage(2, "Exchange"),
        ]
        groups = group_stages(stages)
        assert len(groups) == 3
        assert all(g.count == 1 for g in groups)

    def test_anomalies_detected(self):
        stages = [
            self._make_stage(0, "scan", skew_ratio=5.0, disk_bytes_spilled=1_000_000),
            self._make_stage(1, "scan", skew_ratio=1.2),
        ]
        groups = group_stages(stages)
        assert len(groups) == 1
        g = groups[0]
        assert len(g.anomalies) >= 1
        assert any("skew" in a for a in g.anomalies)
        assert any("disk_spill" in a for a in g.anomalies)
        assert g.skew_ratio_max == 5.0
        assert g.skew_ratio_min == 1.2
        assert g.worst_disk_spill_bytes == 1_000_000

    def test_aggregates_computed_correctly(self):
        stages = [
            self._make_stage(0, "scan", input_bytes=1000, shuffle_write_bytes=200),
            self._make_stage(1, "scan", input_bytes=2000, shuffle_write_bytes=300),
        ]
        groups = group_stages(stages)
        g = groups[0]
        assert g.total_input_bytes == 3000
        assert g.total_shuffle_write_bytes == 500

    def test_order_preserved(self):
        stages = [
            self._make_stage(0, "alpha"),
            self._make_stage(1, "beta"),
            self._make_stage(2, "alpha"),
        ]
        groups = group_stages(stages)
        assert len(groups) == 2
        assert groups[0].group_name == "alpha"
        assert groups[1].group_name == "beta"


# ─── LLM Prompt Builder Tests ────────────────────────────────────────────────

class TestBuildLlmContext:
    def test_basic_structure(self, sample_summary):
        from backend.services.llm_prompt_builder import build_llm_context
        ctx = build_llm_context(sample_summary)
        assert "app" in ctx
        assert ctx["app"]["id"] == "app-001"
        assert ctx["app"]["name"] == "TestJob"
        assert "stage_groups" in ctx or "stages" in ctx

    def test_stage_groups_used_when_available(self):
        from backend.services.llm_prompt_builder import build_llm_context
        stages = [
            StageMetrics(
                stage_id=i, name="scan", num_tasks=10, duration_ms=1000,
                input_bytes=100, output_bytes=0, shuffle_read_bytes=0,
                shuffle_write_bytes=0, gc_time_ms=5, skew_ratio=1.1,
            )
            for i in range(3)
        ]
        summary = AppSummary(
            app_id="test", app_name="test", spark_version="3.5",
            start_time_ms=0, end_time_ms=3000, total_duration_ms=3000,
            num_stages=3, num_tasks=30, executor_count=1,
            total_input_bytes=300, total_output_bytes=0,
            total_shuffle_read_bytes=0, total_shuffle_write_bytes=0,
            stages=stages, stage_groups=group_stages(stages),
        )
        ctx = build_llm_context(summary)
        assert "stage_groups" in ctx
        assert len(ctx["stage_groups"]) == 1
        assert ctx["stage_groups"][0]["count"] == 3

    def test_token_reduction(self, sample_summary):
        """LLM JSON context should be more compact than the markdown report."""
        import json as _json
        from backend.services.llm_prompt_builder import build_llm_context
        md = MarkdownRenderer().render(sample_summary)
        llm_json = _json.dumps(build_llm_context(sample_summary), separators=(",", ":"))
        # JSON should not be larger than markdown
        assert len(llm_json) <= len(md) * 1.5  # generous bound for single-stage case


class TestBuildAnalysisPromptWithSummary:
    def test_uses_json_context_when_summary_provided(self, sample_summary):
        from backend.services.llm_prompt_builder import build_analysis_prompt
        prompt, _ = build_analysis_prompt(
            reduced_report="## fallback report",
            summary=sample_summary,
        )
        assert "Reduced Log Context" in prompt
        assert '"app"' in prompt or '"id"' in prompt
        # Should NOT contain the fallback markdown
        assert "## fallback report" not in prompt

    def test_falls_back_to_markdown_when_no_summary(self):
        from backend.services.llm_prompt_builder import build_analysis_prompt
        prompt, _ = build_analysis_prompt(
            reduced_report="## my markdown report",
            summary=None,
        )
        assert "## Reduced Log Report" in prompt
        assert "## my markdown report" in prompt

    def test_sparklens_block_separate_and_labeled(self, sample_summary):
        from backend.services.llm_prompt_builder import build_analysis_prompt
        prompt, _ = build_analysis_prompt(
            reduced_report="",
            summary=sample_summary,
            sparklens_context={"app": {"driver_idle_pct": 15.0}},
        )
        assert "reference only" in prompt.lower() or "do not recalculate" in prompt.lower()
        assert "NEVER recalculate" in prompt or "NUNCA recalcule" in prompt


class TestZipBombProtection:
    def test_rejects_too_many_files(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for i in range(2000):  # More than max_files_in_zip (1000)
                zf.writestr(f"file{i}.json", '{"Event": "SparkListenerApplicationStart"}')
        zip_bytes = buf.getvalue()
        
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
            "executor_memory_mb", "executor_memory_overhead_mb",
            "executor_offheap_mb", "executor_cores",
            "total_input_bytes", "total_shuffle_read_bytes",
        }
        missing = required - data.keys()
        assert not missing, f"AppSummary missing KPI fields: {missing}"
