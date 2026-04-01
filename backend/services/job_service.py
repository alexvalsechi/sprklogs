"""
Job Service
===========
Facade that wires together LogReducer and LLMAnalyzer.
get_job_service() provides a DI-friendly factory.
"""
from __future__ import annotations

import logging
from typing import Optional

from backend.models.job import JobResult, JobStatus
from backend.services.llm_analyzer import LLMAnalyzer
from backend.utils.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class JobService:
    def __init__(
        self,
        analyzer: Optional[LLMAnalyzer] = None,
    ):
        self._analyzer = analyzer or LLMAnalyzer()

    def process_reduced(
        self,
        reduced_report: str,
        py_files: dict[str, bytes],
        sparklens_context: Optional[dict] = None,
        compact: bool = False,
        llm_provider: Optional[str] = None,
        api_key: Optional[str] = None,
        language: str = "en",
    ) -> JobResult:
        # Resolve provider/key (form > env)
        provider = llm_provider or settings.llm_provider
        key = api_key or settings.llm_api_key

        logger.info("Starting LLM analysis from pre-reduced log (provider=%s, compact=%s)…", provider, compact)
        llm_analysis = self._analyzer.analyze(
            reduced_report=reduced_report,
            summary=None,
            py_files=py_files,
            sparklens_context=sparklens_context,
            provider=provider,
            api_key=key,
            language=language,
        )

        return JobResult(
            job_id="",  # filled by route layer
            status=JobStatus.DONE,
            summary=None,
            reduced_report=reduced_report,
            llm_analysis=llm_analysis,
        )


# ─── DI factory ──────────────────────────────────────────────────────────────

def get_job_service() -> JobService:
    """Default factory used by FastAPI routes."""
    return JobService()
