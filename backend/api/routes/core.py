"""
API Routes — thin controller layer.
Delegates all logic to services; handles HTTP concerns only.
"""
from __future__ import annotations

import os
import uuid
import logging
from typing import Optional

import json

from fastapi import APIRouter, File, Form, UploadFile, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.models.job import JobResult, JobStatus
from backend.services.log_reducer import LogReducer
from backend.services.local_job_runner import LocalReducedJobRunner
from backend.utils.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

limiter = Limiter(key_func=get_remote_address)

# In-memory job store (replace with Redis for production)
_jobs: dict[str, JobResult] = {}
_local_runner = LocalReducedJobRunner()

# In-memory progress store for local-path reductions (reduce_job_id → {percent, stage})
_reduce_progress: dict[str, dict] = {}


@router.post("/reduce-local", response_model=dict)
@limiter.limit("20/hour")
async def reduce_local_zip(
    request: Request,
    zip_file: UploadFile = File(..., description="Spark event log ZIP"),
    compact: bool = Form(default=False),
):
    """Reduce a local ZIP and return summary + markdown report synchronously."""
    if not zip_file.filename or not zip_file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=422, detail="zip_file must be a .zip")

    zip_bytes = await zip_file.read()
    if not zip_bytes:
        raise HTTPException(status_code=422, detail="zip_file is empty")

    try:
        reducer = LogReducer(output_format="md", compact=compact)
        summary, reduced_report = reducer.reduce(zip_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return {
        "summary": summary.model_dump(),
        "reduced_report": reduced_report,
    }


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


@router.post("/reduce-local-path")
def reduce_local_path(
    file_path: str = Form(..., description="Absolute path to the Spark event log ZIP on disk"),
    reduce_job_id: Optional[str] = Form(default=None, description="Client-supplied tracking ID"),
    compact: bool = Form(default=False),
):
    """Reduce a ZIP given its local filesystem path (desktop mode only).

    The file is read directly from disk — no upload transfer needed.
    Progress can be polled via GET /api/reduce-progress/{reduce_job_id}.
    """
    # Validate path: must be an existing .zip file.
    # No directory traversal risk here because this endpoint is only reachable
    # from the local Electron main process (loopback, no CORS from untrusted origins).
    if not file_path or not isinstance(file_path, str):
        raise HTTPException(status_code=422, detail="file_path is required")
    if not file_path.lower().endswith(".zip"):
        raise HTTPException(status_code=422, detail="file_path must be a .zip file")
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    rjid = reduce_job_id or str(uuid.uuid4())
    _reduce_progress[rjid] = {"percent": 2, "stage": "reading_zip"}

    def _progress(pct: int, stage: str) -> None:
        _reduce_progress[rjid] = {"percent": pct, "stage": stage}

    try:
        _progress(2, "reading_zip")
        with open(file_path, "rb") as fh:
            zip_bytes = fh.read()
        _progress(5, "zip_loaded")

        reducer = LogReducer(output_format="md", compact=compact)
        summary, reduced_report = reducer.reduce(zip_bytes, progress_cb=_progress)
    except ValueError as exc:
        _reduce_progress.pop(rjid, None)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except OSError as exc:
        _reduce_progress.pop(rjid, None)
        raise HTTPException(status_code=500, detail=f"Could not read file: {exc}") from exc
    except Exception as exc:
        _reduce_progress.pop(rjid, None)
        logger.exception("Unexpected error during local path reduction for %s", file_path)
        raise HTTPException(
            status_code=500,
            detail=f"Reduction failed: {type(exc).__name__}: {exc}",
        ) from exc
    finally:
        _reduce_progress.pop(rjid, None)

    # Serialise the summary; keep sql_executions outside Pydantic serialisation
    # to avoid issues with large / deeply-nested plan trees.
    sql_executions = summary.sql_executions
    try:
        summary_dict = summary.model_dump(exclude={"sql_executions"})
    except Exception as exc:
        logger.exception("model_dump failed for job %s", rjid)
        raise HTTPException(
            status_code=500,
            detail=f"Serialisation failed: {type(exc).__name__}: {exc}",
        ) from exc
    summary_dict["sql_executions"] = sql_executions

    # Use Response with raw json.dumps output to bypass pydantic-core's depth
    # limit: deeply-nested sparkPlanInfo trees (2000+ nodes) trigger
    # "Circular reference detected (depth exceeded)" in Pydantic's serializer.
    payload = json.dumps({
        "reduce_job_id": rjid,
        "summary": summary_dict,
        "reduced_report": reduced_report,
    })
    return Response(content=payload, media_type="application/json")


@router.get("/reduce-progress/{reduce_job_id}")
async def get_reduce_progress(reduce_job_id: str):
    """Return current reduction progress for the given tracking ID.

    Returns {"percent": 100, "stage": "done"} once the job is no longer tracked
    (i.e., it has finished or failed).
    """
    return _reduce_progress.get(reduce_job_id, {"percent": 100, "stage": "done"})


@router.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "healthy"}
