"""
Celery Tasks
=============
Defines async tasks for log processing.
"""
try:
    from .celery_app import celery_app
except ImportError:
    from celery_app import celery_app
from backend.services.job_service import get_job_service
from backend.api.routes.auth import TokenManager
import redis as redis_lib
from backend.utils.config import get_settings
import logging

logger = logging.getLogger(__name__)
settings = get_settings()

# OAuth token manager
try:
    redis_client = redis_lib.Redis.from_url(settings.celery_broker_url)
    token_manager = TokenManager(redis_client, settings.secret_key)
except Exception as e:
    logger.warning(f"Token manager unavailable: {e}")
    token_manager = None


@celery_app.task(bind=True)
def process_reduced_task(
    self,
    reduced_report: str,
    py_files: dict[str, bytes],
    compact: bool,
    user_id: str | None = None,
    provider: str | None = None,
    api_key: str | None = None,
    language: str = "en",
):
    """Async task to process a pre-reduced report and run only LLM analysis."""
    logger.info("Starting reduced task %s with report length: %s", self.request.id, len(reduced_report))
    logger.info(
        "Parameters: compact=%s, user_id=%s, provider=%s, api_key=%s",
        compact,
        user_id,
        provider,
        "***" if api_key else None,
    )

    service = get_job_service()

    # Resolve API key: prefer OAuth token
    resolved_api_key = api_key
    resolved_provider = provider

    if user_id and provider and token_manager:
        try:
            token_data = token_manager.get_token(user_id, provider)
            if token_data:
                resolved_api_key = token_data.get("access_token")
                logger.info("Using OAuth token for %s:%s", user_id, provider)
        except Exception as e:
            logger.error("Failed to retrieve OAuth token: %s", e)

    result = service.process_reduced(
        reduced_report=reduced_report,
        py_files=py_files,
        compact=compact,
        llm_provider=resolved_provider,
        api_key=resolved_api_key,
        language=language,
    )

    logger.info(
        "Reduced task %s completed. Report length: %s, Analysis length: %s",
        self.request.id,
        len(result.reduced_report or ""),
        len(result.llm_analysis or ""),
    )

    return {
        "reduced_report": result.reduced_report,
        "llm_analysis": result.llm_analysis,
        "summary": result.summary.model_dump() if result.summary else None,
    }