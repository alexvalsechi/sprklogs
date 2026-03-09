"""
LLM Analysis Service
====================
Builds the prompt, calls the adapter, and parses the response.
"""
from __future__ import annotations

import logging
from typing import Optional

from models.job import AppSummary
from adapters.llm_adapters import LLMClientFactory, BaseLLMAdapter

logger = logging.getLogger(__name__)

_SYSTEM_INSTRUCTIONS = """
You are an expert Apache Spark performance engineer.
Analyze the following reduced Spark event log report and:

1. Identify the TOP 3 performance bottlenecks (data skew, GC pressure, shuffle overhead, etc.).
2. For each bottleneck, explain WHY it is a problem and the IMPACT on job duration.
3. Provide actionable, specific recommendations (with PySpark code snippets where relevant).
4. If PySpark source code is provided, suggest concrete code changes.

Be concise and technical. Use markdown formatting.
""".strip()


class LLMAnalyzer:
    """
    Orchestrates prompt construction and calls the LLM adapter.
    Dependency-injected adapter makes this fully testable with mocks.
    """

    def __init__(self, adapter: Optional[BaseLLMAdapter] = None):
        self._adapter = adapter  # injected; resolved lazily if None

    def _get_adapter(
        self,
        provider: Optional[str],
        api_key: Optional[str],
    ) -> BaseLLMAdapter:
        if self._adapter:
            return self._adapter
        return LLMClientFactory.get(provider=provider, api_key=api_key)

    def analyze(
        self,
        reduced_report: str,
        summary: AppSummary,
        py_files: Optional[dict[str, bytes]] = None,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> str:
        adapter = self._get_adapter(provider, api_key)

        prompt_parts = [
            _SYSTEM_INSTRUCTIONS,
            "",
            "## Reduced Log Report",
            reduced_report[:6000],  # guard context window
        ]

        if py_files:
            prompt_parts.append("\n## PySpark Source Files")
            for fname, content in py_files.items():
                try:
                    text = content.decode("utf-8", errors="replace")[:2000]
                    prompt_parts.append(f"\n### {fname}\n```python\n{text}\n```")
                except Exception:
                    pass

        prompt = "\n".join(prompt_parts)
        logger.info("Calling LLM (%s) for analysis…", adapter.__class__.__name__)
        result = adapter.complete(prompt)
        return result
