from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

from backend.analyzer.sparklens_metrics import build_sparklens_report, build_sparklens_report_from_bytes


def _build_zip_bytes(events: list[dict]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("eventlog.json", "\n".join(json.dumps(event) for event in events))
    return buffer.getvalue()


def test_build_sparklens_report_from_bytes_computes_expected_metrics():
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.3.1"},
        {"Event": "SparkListenerResourceProfileAdded", "Executor Resource Requests": {"cores": {"Amount": 2}, "memory": {"Amount": 4096}, "memoryOverhead": {"Amount": 512}}},
        {"Event": "SparkListenerApplicationStart", "App Name": "demo-app", "App ID": "app-1", "Timestamp": 1000},
        {"Event": "SparkListenerExecutorAdded", "Timestamp": 1000, "Executor ID": "1", "Executor Info": {"Host": "host-a", "Total Cores": 2}},
        {"Event": "SparkListenerExecutorAdded", "Timestamp": 1000, "Executor ID": "2", "Executor Info": {"Host": "host-b", "Total Cores": 2}},
        {"Event": "SparkListenerJobStart", "Job ID": 1, "Submission Time": 1100, "Stage Infos": [{"Stage ID": 10, "Number of Tasks": 4}, {"Stage ID": 11, "Number of Tasks": 2}]},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 10, "Stage Name": "read", "Number of Tasks": 4, "Submission Time": 1100, "Parent IDs": []}},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 11, "Stage Name": "write", "Number of Tasks": 2, "Submission Time": 1400, "Parent IDs": [10]}},
        {"Event": "SparkListenerTaskEnd", "Stage ID": 10, "Task Info": {"Task ID": 1, "Executor ID": "1", "Host": "host-a", "Launch Time": 1100, "Finish Time": 1300, "Failed": False}, "Task Metrics": {"Executor Run Time": 200, "JVM GC Time": 10, "Memory Bytes Spilled": 0, "Disk Bytes Spilled": 0, "Shuffle Read Metrics": {"Remote Bytes Read": 50, "Local Bytes Read": 50, "Fetch Wait Time": 20, "Total Records Read": 5}, "Shuffle Write Metrics": {"Shuffle Bytes Written": 100, "Shuffle Write Time": 30000000, "Shuffle Records Written": 5}, "Input Metrics": {"Bytes Read": 1000, "Records Read": 5}, "Output Metrics": {"Bytes Written": 200, "Records Written": 5}}},
        {"Event": "SparkListenerTaskEnd", "Stage ID": 10, "Task Info": {"Task ID": 2, "Executor ID": "2", "Host": "host-b", "Launch Time": 1100, "Finish Time": 1600, "Failed": False}, "Task Metrics": {"Executor Run Time": 500, "JVM GC Time": 20, "Memory Bytes Spilled": 0, "Disk Bytes Spilled": 10, "Shuffle Read Metrics": {"Remote Bytes Read": 30, "Local Bytes Read": 20, "Fetch Wait Time": 30, "Total Records Read": 2}, "Shuffle Write Metrics": {"Shuffle Bytes Written": 0, "Shuffle Write Time": 0, "Shuffle Records Written": 0}, "Input Metrics": {"Bytes Read": 500, "Records Read": 2}, "Output Metrics": {"Bytes Written": 100, "Records Written": 2}}},
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 10, "Stage Name": "read", "Number of Tasks": 4, "Submission Time": 1100, "Completion Time": 1600, "Parent IDs": []}},
        {"Event": "SparkListenerTaskEnd", "Stage ID": 11, "Task Info": {"Task ID": 3, "Executor ID": "1", "Host": "host-a", "Launch Time": 1400, "Finish Time": 1800, "Failed": False}, "Task Metrics": {"Executor Run Time": 400, "JVM GC Time": 60, "Memory Bytes Spilled": 0, "Disk Bytes Spilled": 0, "Shuffle Read Metrics": {"Remote Bytes Read": 200, "Local Bytes Read": 100, "Fetch Wait Time": 50, "Total Records Read": 10}, "Shuffle Write Metrics": {"Shuffle Bytes Written": 600, "Shuffle Write Time": 60000000, "Shuffle Records Written": 10}, "Input Metrics": {"Bytes Read": 0, "Records Read": 0}, "Output Metrics": {"Bytes Written": 300, "Records Written": 10}}},
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 11, "Stage Name": "write", "Number of Tasks": 2, "Submission Time": 1400, "Completion Time": 1800, "Parent IDs": [10]}},
        {"Event": "SparkListenerJobEnd", "Job ID": 1, "Completion Time": 1800},
        {"Event": "SparkListenerExecutorRemoved", "Timestamp": 2000, "Executor ID": "1"},
        {"Event": "SparkListenerExecutorRemoved", "Timestamp": 2000, "Executor ID": "2"},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 2000},
    ]

    zip_bytes = _build_zip_bytes(events)
    report = build_sparklens_report_from_bytes(zip_bytes)
    zip_path = Path(__file__).parent / "_tmp_sparklens_demo.zip"
    zip_path.write_bytes(zip_bytes)
    try:
        report_from_path = build_sparklens_report(str(zip_path))
    finally:
        zip_path.unlink(missing_ok=True)

    assert report["app"]["id"] == "app-1"
    assert report["app"]["spark_version"] == "3.3.1"
    assert report["cluster"]["executor_count"] == 2
    assert report["cluster"]["cores_per_executor"] == 2
    assert report["cluster"]["total_cores"] == 4
    assert report["cluster"]["available_core_hours"] == 0.001
    assert report["cluster"]["used_core_hours"] == 0.0
    assert len(report["jobs"]) == 1
    assert len(report["stages"]) == 2
    assert report["stages"][0]["stage_id"] == 10
    assert report["stages"][0]["task_run_ms_total"] == 700
    assert report["stages"][0]["one_core_hours"]["used_pct"] == 35.0
    assert report["stages"][0]["skew"]["task_skew"] == 1.0
    assert report["stages"][1]["time_distribution"]["gc_pct"] == 15.0
    assert report["stages"][1]["time_distribution"]["shuffle_write_pct"] == 15.0
    assert report["stages"][1]["time_distribution"]["shuffle_read_fetch_pct"] == 12.5
    assert report["stages"][1]["io"]["oi_ratio"] == 3.0
    assert report["host_analysis"]["host-a"]["task_count"] == 2
    assert report["job_timelines"][0]["parallel_groups"][0]["stages"] == [10]
    assert report["job_timelines"][0]["parallel_groups"][1]["stages"] == [11]
    assert report["top_bottlenecks"][0]["stage_id"] == 10
    assert report["llm_context"]["app"]["id"] == "app-1"
    assert report_from_path["cluster"] == report["cluster"]
