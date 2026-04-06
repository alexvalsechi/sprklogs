"""
Thread-safe in-memory stores for local desktop job orchestration.
"""
from __future__ import annotations

import time
from threading import Lock

from backend.models.job import JobResult, JobStatus


class InMemoryJobStore:
    """In-memory job store with TTL-based cleanup to prevent memory leaks."""

    # Jobs older than this are automatically cleaned up
    DEFAULT_TTL_SECONDS = 3600  # 1 hour

    def __init__(self, ttl_seconds: int = DEFAULT_TTL_SECONDS):
        self._jobs: dict[str, JobResult] = {}
        self._created_at: dict[str, float] = {}
        self._ttl = ttl_seconds
        self._lock = Lock()

    def create_pending(self, job_id: str) -> JobResult:
        job = JobResult(job_id=job_id, status=JobStatus.PENDING)
        with self._lock:
            self._jobs[job_id] = job
            self._created_at[job_id] = time.time()
        return job

    def get(self, job_id: str) -> JobResult | None:
        with self._lock:
            # Cleanup expired jobs on access
            self._cleanup_expired()
            return self._jobs.get(job_id)

    def cleanup_expired(self) -> int:
        """Remove all expired jobs. Returns number of removed jobs."""
        with self._lock:
            return self._cleanup_expired()

    def _cleanup_expired(self) -> int:
        """Must be called with lock held."""
        now = time.time()
        expired = [
            jid for jid, created in self._created_at.items()
            if now - created > self._ttl
        ]
        for jid in expired:
            self._jobs.pop(jid, None)
            self._created_at.pop(jid, None)
        return len(expired)


class ReductionProgressStore:
    def __init__(self):
        self._progress: dict[str, dict[str, int | str]] = {}
        self._lock = Lock()

    def start(self, reduce_job_id: str, percent: int = 2, stage: str = "reading_zip") -> None:
        self.update(reduce_job_id, percent=percent, stage=stage)

    def update(self, reduce_job_id: str, percent: int, stage: str) -> None:
        with self._lock:
            self._progress[reduce_job_id] = {"percent": percent, "stage": stage}

    def finish(self, reduce_job_id: str) -> None:
        with self._lock:
            self._progress.pop(reduce_job_id, None)

    def get(self, reduce_job_id: str) -> dict[str, int | str]:
        with self._lock:
            return dict(self._progress.get(reduce_job_id, {"percent": 100, "stage": "done"}))
