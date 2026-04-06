"""
API routes for local reduction and reduced-report analysis.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.services.analysis_service import AnalysisService
from backend.services.job_store import InMemoryJobStore, ReductionProgressStore
from backend.services.local_job_runner import LocalReducedJobRunner
from backend.services.reduction_service import ReductionService

router = APIRouter()
logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)

_job_store = InMemoryJobStore()
_progress_store = ReductionProgressStore()
_local_runner = LocalReducedJobRunner()
_analysis_service = AnalysisService(job_store=_job_store, local_runner=_local_runner)
_reduction_service = ReductionService(progress_store=_progress_store)


@router.post("/reduce-local", response_model=dict)
@limiter.limit("20/hour")
async def reduce_local_zip(
    request: Request,
    zip_file: UploadFile = File(..., description="Spark event log ZIP"),
    compact: bool = Form(default=False),
):
    if not zip_file.filename or not zip_file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=422, detail="zip_file must be a .zip")

    zip_bytes = await zip_file.read()
    return _reduction_service.reduce_uploaded_zip(zip_bytes=zip_bytes, compact=compact)


@router.post("/upload-reduced", response_model=dict, status_code=202)
@limiter.limit("20/hour")
async def upload_reduced_log(
    request: Request,
    reduced_report: str = Form(..., description="Pre-reduced Spark report generated locally in Electron"),
    pyspark_files: list[UploadFile] = File(default=[], description="Optional .py job files"),
    sparklens_context: Optional[str] = Form(default=None, description="Optional deterministic Spark metrics JSON"),
    compact: bool = Form(default=False),
    user_id: Optional[str] = Form(default=None),
    provider: Optional[str] = Form(default=None),
    llm_provider: Optional[str] = Form(default=None),
    api_key: Optional[str] = Form(default=None),
    language: str = Form(default="en"),
):
    client_ip = request.client.host if request.client else "unknown"
    logger.info(
        "Reduced upload request received: report_len=%s, py_files=%s, ip=%s",
        len(reduced_report),
        len(pyspark_files),
        client_ip,
    )

    return await _analysis_service.submit_reduced_log(
        reduced_report=reduced_report,
        pyspark_files=pyspark_files,
        sparklens_context=_parse_optional_json(sparklens_context, "sparklens_context"),
        compact=compact,
        user_id=user_id,
        llm_provider=provider or llm_provider,
        api_key=api_key,
        language=language,
    )


@router.get("/status/{job_id}")
def get_status(job_id: str):
    return _analysis_service.get_job(job_id)


@router.post("/reduce-local-path")
async def reduce_local_path(
    file_path: str = Form(..., description="Absolute path to the Spark event log ZIP on disk"),
    reduce_job_id: Optional[str] = Form(default=None, description="Client-supplied tracking ID"),
    compact: bool = Form(default=False),
):
    payload = await _reduction_service.reduce_local_path(
        file_path=file_path,
        compact=compact,
        reduce_job_id=reduce_job_id,
    )
    return Response(content=payload, media_type="application/json")


@router.get("/reduce-progress/{reduce_job_id}")
async def get_reduce_progress(reduce_job_id: str):
    return _reduction_service.get_progress(reduce_job_id)


@router.get("/health")
def health_check():
    return {"status": "healthy"}


def _parse_optional_json(payload: Optional[str], field_name: str) -> Optional[dict]:
    if payload is None or not payload.strip():
        return None
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"{field_name} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise HTTPException(status_code=422, detail=f"{field_name} must be a JSON object")
    return value
