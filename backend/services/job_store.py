"""
Thread-safe in-memory stores for local desktop job orchestration.
"""
from __future__ import annotations

from threading import Lock

from backend.models.job import JobResult, JobStatus


class InMemoryJobStore:
    def __init__(self):
        self._jobs: dict[str, JobResult] = {}
        self._lock = Lock()

    def create_pending(self, job_id: str) -> JobResult:
        job = JobResult(job_id=job_id, status=JobStatus.PENDING)
        with self._lock:
            self._jobs[job_id] = job
        return job

    def get(self, job_id: str) -> JobResult | None:
        with self._lock:
            return self._jobs.get(job_id)


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
