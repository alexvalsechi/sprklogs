"""
Local job runner for desktop mode.

Runs reduced-report analysis in background threads and updates the in-memory
job store used by API polling endpoints.
"""
from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock

from backend.models.job import JobStatus
from backend.services.job_store import InMemoryJobStore
from backend.services.job_service import get_job_service

logger = logging.getLogger(__name__)


class LocalReducedJobRunner:
    def __init__(self, max_workers: int = 2):
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="local-reduced")
        self._futures: dict[str, Future[None]] = {}
        self._lock = Lock()

    def submit_reduced(
        self,
        job_id: str,
        job_store: InMemoryJobStore,
        reduced_report: str,
        py_files: dict[str, bytes],
        compact: bool,
        llm_provider: str | None,
        api_key: str | None,
        language: str,
    ) -> None:
        """Submit a reduced-report analysis job to run asynchronously."""

        def _run() -> None:
            job = job_store.get(job_id)
            if not job:
                logger.warning("Local reduced job %s disappeared before execution", job_id)
                return
            job.status = JobStatus.RUNNING
            try:
                service = get_job_service()
                result = service.process_reduced(
                    reduced_report=reduced_report,
                    py_files=py_files,
                    compact=compact,
                    llm_provider=llm_provider,
                    api_key=api_key,
                    language=language,
                )
                job.status = JobStatus.DONE
                # Desktop already keeps the reduced markdown locally.
                job.reduced_report = None
                job.llm_analysis = result.llm_analysis
                job.summary = result.summary
                logger.info("Local reduced job finished: %s", job_id)
            except Exception as exc:
                logger.exception("Local reduced job failed: %s", job_id)
                job.status = JobStatus.ERROR
                job.error = str(exc)

        future = self._executor.submit(_run)
        with self._lock:
            self._futures[job_id] = future

        def _cleanup(_future: Future[None]) -> None:
            with self._lock:
                self._futures.pop(job_id, None)

        future.add_done_callback(_cleanup)
