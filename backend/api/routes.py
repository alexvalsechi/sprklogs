"""
API Routes — thin controller layer.
Delegates all logic to services; handles HTTP concerns only.
"""
from __future__ import annotations

import uuid
import asyncio
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, UploadFile, HTTPException
from fastapi.responses import FileResponse, JSONResponse

try:
    from ..tasks import process_log_task
except ImportError:
    from tasks import process_log_task
from models.job import JobStatus, JobResult
from utils.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

# In-memory job store (replace with Redis for production)
_jobs: dict[str, JobResult] = {}


@router.post("/upload", response_model=dict, status_code=202)
async def upload_log(
    log_zip: UploadFile = File(..., description="Spark event log ZIP file"),
    pyspark_files: list[UploadFile] = File(default=[], description="Optional .py job files"),
    compact: bool = Form(default=False),
    llm_provider: Optional[str] = Form(default=None),
    api_key: Optional[str] = Form(default=None),
):
    """Accept a ZIP of Spark logs, enqueue analysis, return job_id."""
    if not log_zip.filename.endswith(".zip"):
        raise HTTPException(status_code=422, detail="log_zip must be a .zip file")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = JobResult(job_id=job_id, status=JobStatus.PENDING)

    # Read file bytes
    zip_bytes = await log_zip.read()
    py_files: dict[str, bytes] = {}
    for f in pyspark_files:
        if f.filename:
            py_files[f.filename] = await f.read()

    # Enqueue Celery task
    task = process_log_task.delay(
        zip_bytes=zip_bytes,
        py_files=py_files,
        compact=compact,
        llm_provider=llm_provider,
        api_key=api_key,
    )
    _jobs[job_id].task_id = task.id  # Store Celery task ID

    logger.info("Enqueued Celery task %s for job %s", task.id, job_id)
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
            from ..celery_app import celery_app
        except ImportError:
            from celery_app import celery_app
        task_result = celery_app.AsyncResult(job.task_id)
        if task_result.state == "PENDING":
            job.status = JobStatus.PENDING
        elif task_result.state in ("PROGRESS", "STARTED"):
            job.status = JobStatus.RUNNING
        elif task_result.state == "SUCCESS":
            result = task_result.result
            job.status = JobStatus.DONE
            job.reduced_report = result["reduced_report"]
            job.llm_analysis = result["llm_analysis"]
            # TODO: Deserialize summary if needed
        elif task_result.state == "FAILURE":
            job.status = JobStatus.ERROR
            job.error = str(task_result.info)

    return job


@router.get("/download/{job_id}/{format}")
def download_report(job_id: str, format: str):
    """Download the reduced report as markdown or json."""
    job = _jobs.get(job_id)
    if not job or job.status != JobStatus.DONE:
        raise HTTPException(status_code=404, detail="Report not ready")

    tmp = Path("/tmp") / f"{job_id}.{format}"
    if format == "md":
        tmp.write_text(job.reduced_report or "")
        media = "text/markdown"
    elif format == "json":
        import json
        tmp.write_text(json.dumps({"reduced": job.reduced_report, "analysis": job.llm_analysis}, ensure_ascii=False, indent=2))
        media = "application/json"
    else:
        raise HTTPException(status_code=400, detail="format must be md or json")

    return FileResponse(str(tmp), media_type=media, filename=f"spark_report_{job_id}.{format}")
