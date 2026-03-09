"""
Celery App Configuration
========================
Initializes Celery with Redis as broker and result backend.
"""
from celery import Celery
from utils.config import get_settings

settings = get_settings()

celery_app = Celery(
    "spark_analyzer",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["backend.tasks"],  # Import tasks module
)

# Celery config
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,  # Retry failed tasks
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # Fair queueing
)