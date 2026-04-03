"""
LLM Analysis Service.
"""
from __future__ import annotations

import logging
from typing import Optional

from backend.adapters.llm_adapters import BaseLLMAdapter, LLMClientFactory
from backend.models.job import AppSummary
from backend.services.llm_code_link_reconciler import reconcile_code_links
from backend.services.llm_prompt_builder import build_analysis_prompt

logger = logging.getLogger(__name__)


class LLMAnalyzer:
    """
    Orchestrates prompt construction and calls the LLM adapter.
    Dependency-injected adapter makes this fully testable with mocks.
    """

    def __init__(self, adapter: Optional[BaseLLMAdapter] = None):
        self._adapter = adapter

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
        sparklens_context: Optional[dict] = None,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
        language: str = "en",
    ) -> str:
        adapter = self._get_adapter(provider, api_key)
        prompt, py_files_provided = build_analysis_prompt(
            reduced_report=reduced_report,
            summary=summary,
            py_files=py_files,
            sparklens_context=sparklens_context,
            language=language,
        )

        logger.info(
            "Calling LLM (%s) for analysis... [Mode: %s, report_chars=%s, prompt_chars=%s]",
            adapter.__class__.__name__,
            "B" if py_files_provided else "A",
            len(reduced_report),
            len(prompt),
        )

        result = adapter.complete(prompt)
        if py_files_provided:
            result = reconcile_code_links(result, py_files or {})
        return result
