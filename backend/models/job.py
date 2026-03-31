"""
Domain models for job lifecycle.
"""
from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"


class StageMetrics(BaseModel):
    stage_id: int
    name: str
    num_tasks: int
    duration_ms: int
    input_bytes: int
    output_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    gc_time_ms: int
    cpu_util_pct: Optional[float] = None
    task_duration_min_ms: Optional[int] = None
    task_duration_avg_ms: Optional[float] = None
    task_duration_max_ms: Optional[int] = None
    task_duration_p95_ms: Optional[int] = None
    skew_ratio: Optional[float] = None
    memory_bytes_spilled: int = 0
    disk_bytes_spilled: int = 0
    shuffle_write_time_ms: int = 0
    fetch_wait_time_ms: int = 0
    remote_bytes_read_to_disk: int = 0
    peak_execution_memory_bytes: int = 0
    shuffle_read_records: int = 0
    shuffle_write_records: int = 0
    # Executor-level metrics
    cpu_efficiency: Optional[float] = None
    gc_overhead_pct: Optional[float] = None
    deserialize_time_ms: int = 0
    minor_gc_count: int = 0
    major_gc_count: int = 0
    avg_result_size_kb: Optional[float] = None

    @property
    def has_skew(self) -> bool:
        return (self.skew_ratio or 0.0) > 3.0

    @property
    def has_spill(self) -> bool:
        return self.disk_bytes_spilled > 0

    @property
    def has_heavy_shuffle(self) -> bool:
        """Flag stages where total shuffle (read + write) exceeds 500 MB."""
        return (self.shuffle_read_bytes + self.shuffle_write_bytes) > 524_288_000


class AppSummary(BaseModel):
    app_id: str
    app_name: str
    spark_version: str
    start_time_ms: int
    end_time_ms: int
    total_duration_ms: int
    num_stages: int
    num_tasks: int
    executor_count: int
    executor_memory_mb: int = 0
    executor_memory_overhead_mb: int = 0
    executor_offheap_mb: int = 0
    executor_cores: int = 0
    total_input_bytes: int
    total_output_bytes: int
    total_shuffle_read_bytes: int
    total_shuffle_write_bytes: int
    sql_execution_count: int = 0
    sql_plan_mermaid: Optional[str] = None
    sql_plan_tree: Optional[dict] = None
    sql_executions: Optional[list] = None
    executor_summary: list = Field(default_factory=list)
    job_efficiency_meta: dict = Field(default_factory=dict)
    stages: list[StageMetrics] = Field(default_factory=list)


class JobResult(BaseModel):
    job_id: str
    status: JobStatus = JobStatus.PENDING
    summary: Optional[AppSummary] = None
    reduced_report: Optional[str] = None
    llm_analysis: Optional[str] = None
    error: Optional[str] = None
