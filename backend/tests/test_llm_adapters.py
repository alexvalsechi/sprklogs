"""
Unit tests for LLM adapters.
Uses a mock adapter — no real API calls.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from backend.adapters.llm_adapters import NoOpAdapter, LLMClientFactory, BaseLLMAdapter
from backend.services.llm_analyzer import LLMAnalyzer
from backend.models.job import AppSummary


class EchoAdapter(BaseLLMAdapter):
    """Test double that echoes the prompt length."""
    def _complete(self, prompt: str) -> str:
        return f"Echo: {len(prompt)} chars"


@pytest.fixture
def minimal_summary() -> AppSummary:
    return AppSummary(
        app_id="app-test", app_name="UnitTestJob", spark_version="3.5.0",
        start_time_ms=0, end_time_ms=10000, total_duration_ms=10000,
        num_stages=1, num_tasks=5, executor_count=2,
        total_input_bytes=0, total_output_bytes=0,
        total_shuffle_read_bytes=0, total_shuffle_write_bytes=0,
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
        adapter = EchoAdapter()
        analyzer = LLMAnalyzer(adapter=adapter)
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

        # Validate Portuguese prompt selection without coupling to stale wording.
        assert "Você analisa logs da Spark UI" in prompts[0]
        assert "You analyze Spark UI logs" not in prompts[0]

    def test_retry_on_failure(self, minimal_summary):
        """Adapter that fails twice then succeeds should still return result."""
        call_count = {"n": 0}

        class FlakyAdapter(BaseLLMAdapter):
            MAX_RETRIES = 3
            RETRY_DELAY = 0  # no sleep in tests

            def _complete(self, prompt: str) -> str:
                call_count["n"] += 1
                if call_count["n"] < 3:
                    raise RuntimeError("transient error")
                return "recovered"

        analyzer = LLMAnalyzer(adapter=FlakyAdapter())
        result = analyzer.analyze("# r", minimal_summary)
        assert result == "recovered"
        assert call_count["n"] == 3
