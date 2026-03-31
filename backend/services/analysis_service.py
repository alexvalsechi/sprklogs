"""
Application service for reduced-report analysis jobs.
"""
from __future__ import annotations

import logging
import uuid

from fastapi import HTTPException, UploadFile

from backend.models.job import JobResult
from backend.services.job_store import InMemoryJobStore
from backend.services.local_job_runner import LocalReducedJobRunner

logger = logging.getLogger(__name__)


class AnalysisService:
    def __init__(
        self,
        job_store: InMemoryJobStore,
        local_runner: LocalReducedJobRunner,
    ):
        self._job_store = job_store
        self._local_runner = local_runner

    async def submit_reduced_log(
        self,
        reduced_report: str,
        pyspark_files: list[UploadFile],
        compact: bool,
        user_id: str | None,
        llm_provider: str | None,
        api_key: str | None,
        language: str,
    ) -> dict[str, str]:
        if not reduced_report or not reduced_report.strip():
            raise HTTPException(status_code=422, detail="reduced_report is required")

        reduced_size_mb = len(reduced_report.encode("utf-8")) / (1024 * 1024)
        if reduced_size_mb > 50:
            raise HTTPException(status_code=413, detail="reduced_report too large. Maximum size is 50 MB")

        job_id = str(uuid.uuid4())
        self._job_store.create_pending(job_id)

        py_files: dict[str, bytes] = {}
        for file in pyspark_files:
            if file.filename:
                py_files[file.filename] = await file.read()

        self._local_runner.submit_reduced(
            job_id=job_id,
            job_store=self._job_store,
            reduced_report=reduced_report,
            py_files=py_files,
            compact=compact,
            llm_provider=llm_provider,
            api_key=api_key,
            language=language,
        )

        logger.info(
            "Queued local reduced job %s (user_id=%s, provider=%s, py_files=%s)",
            job_id,
            user_id,
            llm_provider,
            len(py_files),
        )
        return {"job_id": job_id, "status": "pending"}

    def get_job(self, job_id: str) -> JobResult:
        job = self._job_store.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job
