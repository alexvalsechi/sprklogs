"""
Celery Tasks
=============
Defines async tasks for log processing.
"""
try:
    from .celery_app import celery_app
except ImportError:
    from celery_app import celery_app
from services.job_service import JobService, get_job_service


@celery_app.task(bind=True)
def process_log_task(self, zip_bytes: bytes, py_files: dict[str, bytes], compact: bool, llm_provider: str | None, api_key: str | None):
    """Async task to process Spark log ZIP and return results."""
    service = get_job_service()
    result = service.process(
        zip_bytes=zip_bytes,
        py_files=py_files,
        compact=compact,
        llm_provider=llm_provider,
        api_key=api_key,
    )
    return {
        "reduced_report": result.reduced_report,
        "llm_analysis": result.llm_analysis,
        "summary": result.summary.model_dump() if result.summary else None,
    }