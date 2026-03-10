"""
API Routes — thin controller layer.
Delegates all logic to services; handles HTTP concerns only.
"""
from __future__ import annotations

import uuid
import asyncio
import logging
import hashlib
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, UploadFile, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.tasks import process_log_task
from backend.models.job import JobStatus, JobResult
from backend.utils.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

limiter = Limiter(key_func=get_remote_address)

# In-memory job store (replace with Redis for production)
_jobs: dict[str, JobResult] = {}


@router.post("/upload", response_model=dict, status_code=202)
@limiter.limit("5/hour")
async def upload_log(
    request: Request,
    log_zip: UploadFile = File(..., description="Spark event log ZIP file"),
    pyspark_files: list[UploadFile] = File(default=[], description="Optional .py job files"),
    compact: bool = Form(default=False),
    user_id: Optional[str] = Form(default=None),
    provider: Optional[str] = Form(default=None),
    llm_provider: Optional[str] = Form(default=None),  # BYOK provider
    api_key: Optional[str] = Form(default=None),  # Still support legacy BYOK
    language: str = Form(default="en"),
):
    """
    Accept a ZIP of Spark logs, enqueue analysis, return job_id.
    
    Either provide:
    - OAuth: user_id + provider (token retrieved from session)
    - BYOK: llm_provider + api_key (legacy, less secure)
    """
    client_ip = request.client.host if request.client else "unknown"
    logger.info(f"Upload request received: filename={log_zip.filename}, size={getattr(log_zip, 'size', 'unknown')}, ip={client_ip}")
    
    if not log_zip.filename.endswith(".zip"):
        logger.error(f"Invalid file extension: {log_zip.filename}")
        raise HTTPException(status_code=422, detail="log_zip must be a .zip file")

    # Read file bytes
    zip_bytes = await log_zip.read()
    
    # Validate file size
    zip_size_mb = len(zip_bytes) / (1024 * 1024)
    if zip_size_mb > settings.max_zip_mb:
        logger.warning(f"ZIP file too large: {zip_size_mb:.2f} MB > {settings.max_zip_mb} MB, ip={client_ip}")
        raise HTTPException(status_code=413, detail=f"ZIP file too large. Maximum size is {settings.max_zip_mb} MB")
    
    # Validate ZIP magic number
    if len(zip_bytes) < 4 or zip_bytes[:4] != b'PK\x03\x04':
        logger.warning(f"Invalid ZIP file: magic number check failed, ip={client_ip}")
        raise HTTPException(status_code=422, detail="Invalid ZIP file format")
    
    # Log file hash for monitoring
    file_hash = hashlib.sha256(zip_bytes).hexdigest()[:16]
    logger.info(f"ZIP validated: size={zip_size_mb:.2f} MB, hash={file_hash}, ip={client_ip}")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = JobResult(job_id=job_id, status=JobStatus.PENDING)
    
    logger.info(f"Created job {job_id} for user {user_id} with provider {provider}")

    py_files: dict[str, bytes] = {}
    for f in pyspark_files:
        if f.filename:
            py_files[f.filename] = await f.read()

    # Enqueue Celery task
    task = process_log_task.delay(
        zip_bytes=zip_bytes,
        py_files=py_files,
        compact=compact,
        user_id=user_id,  # OAuth user ID
        provider=provider or llm_provider,  # OAuth provider or BYOK provider
        api_key=api_key,  # BYOK API key
        language=language,
    )
    _jobs[job_id].task_id = task.id  # Store Celery task ID

    logger.info("Enqueued Celery task %s for job %s (user_id=%s, provider=%s)", task.id, job_id, user_id, provider)
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
            if result.get("summary"):
                from backend.models.job import AppSummary
                job.summary = AppSummary(**result["summary"])
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

    tmp_dir = Path(settings.upload_tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / f"{job_id}.{format}"
    try:
        if format == "md":
            # Combine reduced report and LLM analysis
            content = job.reduced_report or ""
            if job.llm_analysis and job.llm_analysis.strip():
                content += "\n\n---\n\n## AI Analysis\n\n" + job.llm_analysis
            tmp.write_text(content)
            media = "text/markdown"
        elif format == "json":
            import json
            tmp.write_text(json.dumps({"reduced": job.reduced_report, "analysis": job.llm_analysis}, ensure_ascii=False, indent=2))
            media = "application/json"
        else:
            raise HTTPException(status_code=400, detail="format must be md or json")

        return FileResponse(str(tmp), media_type=media, filename=f"spark_report_{job_id}.{format}")
    finally:
        # Clean up temporary file after response
        if tmp.exists():
            tmp.unlink()


@router.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "healthy"}
