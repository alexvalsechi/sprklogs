"""
Unit tests for LLM adapters.
Uses a mock adapter - no real API calls.
"""
from __future__ import annotations

import pytest

from backend.adapters.llm_adapters import BaseLLMAdapter, LLMClientFactory, NoOpAdapter
from backend.models.job import AppSummary
from backend.services.llm_analyzer import LLMAnalyzer


class EchoAdapter(BaseLLMAdapter):
    """Test double that echoes the prompt length."""

    def _complete(self, prompt: str) -> str:
        return f"Echo: {len(prompt)} chars"


@pytest.fixture
def minimal_summary() -> AppSummary:
    return AppSummary(
        app_id="app-test",
        app_name="UnitTestJob",
        spark_version="3.5.0",
        start_time_ms=0,
        end_time_ms=10000,
        total_duration_ms=10000,
        num_stages=1,
        num_tasks=5,
        executor_count=2,
        total_input_bytes=0,
        total_output_bytes=0,
        total_shuffle_read_bytes=0,
        total_shuffle_write_bytes=0,
        stages=[],
    )


class TestNoOpAdapter:
    def test_returns_notice(self):
        adapter = NoOpAdapter()
        result = adapter.complete("anything")
        assert "not configured" in result.lower()


class TestLLMClientFactory:
    def test_noop_when_no_provider(self):
        adapter = LLMClientFactory.get()
        assert isinstance(adapter, NoOpAdapter)

    def test_noop_when_unknown_provider(self):
        adapter = LLMClientFactory.get(provider="cohere", api_key="key123")
        assert isinstance(adapter, NoOpAdapter)

    def test_singleton_same_key(self):
        a1 = LLMClientFactory.get(provider="unknown_test", api_key="abc")
        a2 = LLMClientFactory.get(provider="unknown_test", api_key="abc")
        assert a1 is a2


class TestLLMAnalyzer:
    def test_calls_adapter_with_report(self, minimal_summary):
        analyzer = LLMAnalyzer(adapter=EchoAdapter())
        result = analyzer.analyze(
            reduced_report="# Report\n\nSome data",
            summary=minimal_summary,
        )
        assert "Echo:" in result

    def test_includes_py_files_in_prompt(self, minimal_summary):
        prompts = []

        class CapturingAdapter(BaseLLMAdapter):
            def _complete(self, prompt: str) -> str:
                prompts.append(prompt)
                return "ok"

        analyzer = LLMAnalyzer(adapter=CapturingAdapter())
        analyzer.analyze(
            reduced_report="# Report",
            summary=minimal_summary,
            py_files={"job.py": b"df = spark.read.parquet('s3://bucket/data')"},
        )
        assert "job.py" in prompts[0]
        assert "parquet" in prompts[0]

    def test_language_switch(self, minimal_summary):
        """Prompt should reflect requested language."""
        prompts = []

        class CapturingAdapter(BaseLLMAdapter):
            def _complete(self, prompt: str) -> str:
                prompts.append(prompt)
                return "ok"

        analyzer = LLMAnalyzer(adapter=CapturingAdapter())
        analyzer.analyze("# Report", minimal_summary, language="pt")

        assert "Você analisa logs da Spark UI" in prompts[0]
        assert "You analyze Spark UI logs" not in prompts[0]

    def test_retry_on_failure(self, minimal_summary):
        call_count = {"n": 0}

        class FlakyAdapter(BaseLLMAdapter):
            MAX_RETRIES = 3
            RETRY_DELAY = 0

            def _complete(self, prompt: str) -> str:
                call_count["n"] += 1
                if call_count["n"] < 3:
                    raise RuntimeError("transient error")
                return "recovered"

        analyzer = LLMAnalyzer(adapter=FlakyAdapter())
        result = analyzer.analyze("# r", minimal_summary)
        assert result == "recovered"
        assert call_count["n"] == 3

    def test_py_files_are_not_reduced(self, minimal_summary):
        prompts = []

        class CapturingAdapter(BaseLLMAdapter):
            def _complete(self, prompt: str) -> str:
                prompts.append(prompt)
                return "{}"

        analyzer = LLMAnalyzer(adapter=CapturingAdapter())
        repeated = "same_line\n" * 20
        analyzer.analyze(
            reduced_report="# Report",
            summary=minimal_summary,
            py_files={"job.py": repeated.encode("utf-8")},
        )

        assert "[... repeated line omitted" not in prompts[0]
        assert repeated.strip() in prompts[0]

    def test_reconciles_code_link_line_numbers(self, minimal_summary):
        class JsonAdapter(BaseLLMAdapter):
            def _complete(self, _prompt: str) -> str:
                import json as _json

                payload = {
                    "meta": {
                        "mode": "B",
                        "job_file": "job.py",
                        "log_file": "x",
                        "analyzed_at": "t",
                    },
                    "summary": {
                        "score": 80,
                        "verdict": "ok",
                        "estimated_gain_min": 1,
                        "kpis": {
                            "duration_total_min": 1,
                            "input_volume_gb": 0,
                            "total_tasks": 1,
                            "avg_data_per_task_kb": 1,
                            "avg_data_per_task_critical": False,
                            "stages_with_skew": 0,
                            "disk_spill_total_gb": 0,
                            "memory_spill_total_gb": 0,
                            "shuffle_write_total_gb": 0,
                            "stages_with_failure_or_retry": 0,
                        },
                    },
                    "stages": [],
                    "bottlenecks": [
                        {
                            "id": "B1",
                            "severity": "high",
                            "type": "other",
                            "title": "x",
                            "stages_affected": [],
                            "operator": None,
                            "duration_observed_s": 1,
                            "duration_expected_s": 1,
                            "evidence": "e",
                            "root_cause": "r",
                            "observed_effect": "o",
                            "code_link": {
                                "line_start": 999,
                                "line_end": 999,
                                "function_name": "target_fn",
                                "snippet": "return x + 1",
                                "explanation": "e",
                            },
                        }
                    ],
                    "action_plan": {
                        "cluster_configs": [],
                        "code_fixes": [
                            {
                                "bottleneck_id": "B1",
                                "title": "fix",
                                "line_start": 999,
                                "line_end": 999,
                                "function_name": "target_fn",
                                "problem_explanation": "p",
                                "before_code": "return x + 1",
                                "after_code": "return x + 2",
                                "expected_gain": "g",
                            }
                        ],
                    },
                    "limitations": "l",
                }
                return _json.dumps(payload)

        source = "\n".join([
            "def other():",
            "    return 0",
            "",
            "def target_fn(x):",
            "    return x + 1",
            "",
        ])

        analyzer = LLMAnalyzer(adapter=JsonAdapter())
        result = analyzer.analyze(
            reduced_report="# r",
            summary=minimal_summary,
            py_files={"job.py": source.encode("utf-8")},
        )

        import json as _json

        data = _json.loads(result)
        link = data["bottlenecks"][0]["code_link"]
        fix = data["action_plan"]["code_fixes"][0]
        assert link["line_start"] == 5
        assert link["line_end"] == 5
        assert fix["line_start"] == 5
        assert fix["line_end"] == 5
