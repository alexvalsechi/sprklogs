"""
Job Service
===========
Facade that wires together LogReducer and LLMAnalyzer.
get_job_service() provides a DI-friendly factory.
"""
from __future__ import annotations

import logging
from typing import Optional

from models.job import JobResult, JobStatus
from services.log_reducer import LogReducer
from services.llm_analyzer import LLMAnalyzer
from utils.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class JobService:
    def __init__(
        self,
        reducer: Optional[LogReducer] = None,
        analyzer: Optional[LLMAnalyzer] = None,
    ):
        self._reducer = reducer
        self._analyzer = analyzer or LLMAnalyzer()

    def process(
        self,
        zip_bytes: bytes,
        py_files: dict[str, bytes],
        compact: bool = False,
        llm_provider: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> JobResult:
        # Resolve provider/key (form > env)
        provider = llm_provider or settings.llm_provider
        key = api_key or settings.llm_api_key

        logger.info("Starting log reduction (compact=%s)…", compact)
        reducer = self._reducer or LogReducer(output_format="md", compact=compact)
        summary, reduced_report = reducer.reduce(zip_bytes)

        logger.info("Starting LLM analysis (provider=%s)…", provider)
        llm_analysis = self._analyzer.analyze(
            reduced_report=reduced_report,
            summary=summary,
            py_files=py_files,
            provider=provider,
            api_key=key,
        )

        return JobResult(
            job_id="",  # filled by route layer
            status=JobStatus.DONE,
            summary=summary,
            reduced_report=reduced_report,
            llm_analysis=llm_analysis,
        )


# ─── DI factory ──────────────────────────────────────────────────────────────

def get_job_service() -> JobService:
    """Default factory used by FastAPI routes."""
    return JobService()
