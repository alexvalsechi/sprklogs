"""
Application service for local ZIP reduction flows.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Optional

from fastapi import HTTPException

from backend.analyzer.sparklens_metrics import (
    build_sparklens_report_from_bytes,
)
from backend.services.job_store import ReductionProgressStore
from backend.services.log_reducer import LogReducer


async def _async_read_file(path: str) -> bytes:
    """Read file asynchronously to avoid blocking the event loop."""
    try:
        import aiofiles
        async with aiofiles.open(path, "rb") as fh:
            return await fh.read()
    except ImportError:
        # Fallback to sync if aiofiles is not available
        with open(path, "rb") as fh:
            return fh.read()


class ReductionService:
    def __init__(self, progress_store: ReductionProgressStore):
        self._progress_store = progress_store

    def reduce_uploaded_zip(self, zip_bytes: bytes, compact: bool) -> dict:
        if not zip_bytes:
            raise HTTPException(status_code=422, detail="zip_file is empty")

        try:
            reducer = LogReducer(output_format="md", compact=compact)
            summary, reduced_report = reducer.reduce(zip_bytes)
            # Reuse already-parsed events from reducer instead of re-reading ZIP
            sparklens_report = build_sparklens_report_from_bytes(
                zip_bytes, events=reducer._cached_events
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        return {
            "summary": summary.model_dump(),
            "reduced_report": reduced_report,
            "sparklens_context": sparklens_report.get("llm_context"),
        }

    async def reduce_local_path(
        self,
        file_path: str,
        compact: bool,
        reduce_job_id: Optional[str] = None,
    ) -> str:
        if not file_path or not isinstance(file_path, str):
            raise HTTPException(status_code=422, detail="file_path is required")
        if not file_path.lower().endswith(".zip"):
            raise HTTPException(status_code=422, detail="file_path must be a .zip file")
        if not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

        resolved_job_id = reduce_job_id or str(uuid.uuid4())
        self._progress_store.start(resolved_job_id)

        def _progress(percent: int, stage: str) -> None:
            self._progress_store.update(resolved_job_id, percent=percent, stage=stage)

        try:
            _progress(2, "reading_zip")
            # Async file read to avoid blocking the event loop
            zip_bytes = await _async_read_file(file_path)
            _progress(5, "zip_loaded")

            reducer = LogReducer(output_format="md", compact=compact)
            summary, reduced_report = reducer.reduce(zip_bytes, progress_cb=_progress)
            _progress(86, "sparklens_metrics")
            # Reuse already-parsed events from reducer instead of re-reading ZIP
            sparklens_report = build_sparklens_report_from_bytes(
                zip_bytes, events=reducer._cached_events
            )
            summary_payload = self._serialize_summary(summary)
            return json.dumps({
                "reduce_job_id": resolved_job_id,
                "summary": summary_payload,
                "reduced_report": reduced_report,
                "sparklens_context": sparklens_report.get("llm_context"),
            })
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Could not read file: {exc}") from exc
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Reduction failed: {type(exc).__name__}: {exc}",
            ) from exc
        finally:
            self._progress_store.finish(resolved_job_id)

    def get_progress(self, reduce_job_id: str) -> dict[str, int | str]:
        return self._progress_store.get(reduce_job_id)

    @staticmethod
    def _serialize_summary(summary) -> dict:
        sql_executions = summary.sql_executions
        try:
            summary_dict = summary.model_dump(exclude={"sql_executions"})
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Serialisation failed: {type(exc).__name__}: {exc}",
            ) from exc
        summary_dict["sql_executions"] = sql_executions
        return summary_dict
