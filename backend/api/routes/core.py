"""
API Routes — thin controller layer.
Delegates all logic to services; handles HTTP concerns only.
"""
from __future__ import annotations

import uuid
import logging
from typing import Optional

from fastapi import APIRouter, File, Form, UploadFile, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.models.job import JobResult, JobStatus
from backend.services.local_job_runner import LocalReducedJobRunner
from backend.utils.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

limiter = Limiter(key_func=get_remote_address)

# In-memory job store (replace with Redis for production)
_jobs: dict[str, JobResult] = {}
_local_runner = LocalReducedJobRunner()


@router.post("/upload-reduced", response_model=dict, status_code=202)
@limiter.limit("20/hour")
async def upload_reduced_log(
    request: Request,
    reduced_report: str = Form(..., description="Pre-reduced Spark report generated locally in Electron"),
    pyspark_files: list[UploadFile] = File(default=[], description="Optional .py job files"),
    compact: bool = Form(default=False),
    user_id: Optional[str] = Form(default=None),
    provider: Optional[str] = Form(default=None),
    llm_provider: Optional[str] = Form(default=None),
    api_key: Optional[str] = Form(default=None),
    language: str = Form(default="en"),
):
    """Accept a pre-reduced report from desktop and enqueue only LLM analysis."""
    client_ip = request.client.host if request.client else "unknown"
    logger.info(
        "Reduced upload request received: report_len=%s, py_files=%s, ip=%s",
        len(reduced_report),
        len(pyspark_files),
        client_ip,
    )

    if not reduced_report or not reduced_report.strip():
        raise HTTPException(status_code=422, detail="reduced_report is required")

    # Keep payload bounded for API safety. Desktop can handle large ZIPs locally,
    # but this endpoint should still guard inbound report size.
    reduced_size_mb = len(reduced_report.encode("utf-8")) / (1024 * 1024)
    if reduced_size_mb > 50:
        raise HTTPException(status_code=413, detail="reduced_report too large. Maximum size is 50 MB")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = JobResult(job_id=job_id, status=JobStatus.PENDING)

    py_files: dict[str, bytes] = {}
    for f in pyspark_files:
        if f.filename:
            py_files[f.filename] = await f.read()

    resolved_provider = provider or llm_provider

    _local_runner.submit_reduced(
        job_id=job_id,
        jobs=_jobs,
        reduced_report=reduced_report,
        py_files=py_files,
        compact=compact,
        llm_provider=resolved_provider,
        api_key=api_key,
        language=language,
    )

    logger.info(
        "Queued local reduced job %s (user_id=%s, provider=%s, py_files=%s)",
        job_id,
        user_id,
        resolved_provider,
        len(py_files),
    )

    return {"job_id": job_id, "status": "pending"}


@router.get("/status/{job_id}")
def get_status(job_id: str):
    """Poll for job completion."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return job


@router.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "healthy"}
