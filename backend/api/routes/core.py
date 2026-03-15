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

from backend.tasks import process_reduced_task
from backend.models.job import JobStatus, JobResult
from backend.utils.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

limiter = Limiter(key_func=get_remote_address)

# In-memory job store (replace with Redis for production)
_jobs: dict[str, JobResult] = {}


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

    # Keep payload bounded for API safety. Desktop can handle larger files locally.
    reduced_size_mb = len(reduced_report.encode("utf-8")) / (1024 * 1024)
    if reduced_size_mb > 50:
        raise HTTPException(status_code=413, detail="reduced_report too large. Maximum size is 50 MB")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = JobResult(job_id=job_id, status=JobStatus.PENDING)

    py_files: dict[str, bytes] = {}
    for f in pyspark_files:
        if f.filename:
            py_files[f.filename] = await f.read()

    task = process_reduced_task.delay(
        reduced_report=reduced_report,
        py_files=py_files,
        compact=compact,
        user_id=user_id,
        provider=provider or llm_provider,
        api_key=api_key,
        language=language,
    )
    _jobs[job_id].task_id = task.id

    logger.info("Enqueued reduced Celery task %s for job %s (user_id=%s, provider=%s)", task.id, job_id, user_id, provider)
    return {"job_id": job_id, "status": "pending"}


@router.get("/status/{job_id}")
def get_status(job_id: str):
    """Poll for job completion."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Check Celery task status if we have task_id
    if job.task_id:
        try:
            from ...celery_app import celery_app
        except ImportError:
            from backend.celery_app import celery_app
        task_result = celery_app.AsyncResult(job.task_id)
        if task_result.state == "PENDING":
            job.status = JobStatus.PENDING
        elif task_result.state in ("PROGRESS", "STARTED"):
            job.status = JobStatus.RUNNING
        elif task_result.state == "SUCCESS":
            result = task_result.result
            job.status = JobStatus.DONE
            # reduced_report is NOT stored server-side — the desktop already
            # holds the original locally and builds the final Markdown there.
            job.reduced_report = None
            job.llm_analysis = result["llm_analysis"]
            if result.get("summary"):
                from backend.models.job import AppSummary
                job.summary = AppSummary(**result["summary"])
        elif task_result.state == "FAILURE":
            job.status = JobStatus.ERROR
            job.error = str(task_result.info)

    return job


@router.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "healthy"}
