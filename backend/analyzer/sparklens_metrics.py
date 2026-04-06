"""
Deterministic Spark event-log metrics inspired by Sparklens.
"""
from __future__ import annotations

import functools
import heapq
import io
import json
import zipfile
from collections import defaultdict

TEST_PERCENTAGES = [10, 20, 50, 80, 100, 110, 120, 150, 200, 300, 400, 500]


def _round_float(value: float, digits: int = 3) -> float:
    return round(float(value), digits)


def _safe_pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator * 100.0 / denominator


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _resource_amount(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _iter_events_from_zipfile(zf: zipfile.ZipFile):
    names = [name for name in zf.namelist() if not name.endswith("/")]
    if not names:
        return
    with zf.open(names[0]) as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                yield event


def _parse_events_from_zip_file(zip_path: str):
    with zipfile.ZipFile(zip_path) as zf:
        yield from _iter_events_from_zipfile(zf)


def _parse_events_from_zip_bytes(zip_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        yield from _iter_events_from_zipfile(zf)


def get_max_concurrent_executors(executor_events: list[dict]) -> int:
    events = sorted(
        executor_events,
        key=lambda item: (item["ts"], 0 if item["type"] == "add" else 1),
    )
    current = 0
    peak = 0
    for event in events:
        if event["type"] == "add":
            current += 1
            peak = max(peak, current)
        else:
            current = max(0, current - 1)
    return peak


@functools.lru_cache(maxsize=1024)
def estimate_stage_wall_clock(task_run_times_tuple: tuple[int, ...], new_total_cores: int) -> int:
    task_run_times_ms = list(task_run_times_tuple)
    if not task_run_times_ms:
        return 0
    if new_total_cores <= 0:
        return sum(task_run_times_ms)

    cores = min(new_total_cores, len(task_run_times_ms))
    heap = [0] * cores
    heapq.heapify(heap)
    for task_ms in sorted(task_run_times_ms, reverse=True):
        earliest_free = heapq.heappop(heap)
        heapq.heappush(heap, earliest_free + task_ms)
    return max(heap, default=0)


def _critical_path(stage_estimates: dict[int, int], stages: dict[int, dict], stage_ids: list[int]) -> int:
    earliest_finish: dict[int, int] = {}

    def finish_time(stage_id: int) -> int:
        if stage_id in earliest_finish:
            return earliest_finish[stage_id]
        if stage_id not in stage_estimates:
            return 0
        parents = [parent for parent in stages.get(stage_id, {}).get("parent_ids", []) if parent in stage_estimates]
        parent_finish = max((finish_time(parent) for parent in parents), default=0)
        earliest_finish[stage_id] = parent_finish + stage_estimates[stage_id]
        return earliest_finish[stage_id]

    for stage_id in stage_ids:
        finish_time(stage_id)
    return max(earliest_finish.values(), default=0)


def estimate_app_wall_clock(
    jobs: dict[int, dict],
    stages: dict[int, dict],
    app_ms: int,
    job_total_ms: int,
    current_executor_count: int,
    cores_per_executor: int,
    target_executor_count: int,
) -> int:
    del current_executor_count
    new_cores = max(1, target_executor_count) * max(1, cores_per_executor)
    driver_ms = max(0, app_ms - job_total_ms)
    estimated_job_total = 0
    for job in jobs.values():
        stage_estimates: dict[int, int] = {}
        for stage_id in job.get("stage_ids", []):
            stage = stages.get(stage_id)
            if not stage:
                continue
            stage_estimates[stage_id] = estimate_stage_wall_clock(tuple(stage.get("task_run_times", [])), new_cores)
        estimated_job_total += _critical_path(stage_estimates, stages, job.get("stage_ids", []))
    return driver_ms + estimated_job_total


def _build_job_timelines(jobs: dict[int, dict], stages: dict[int, dict]) -> list[dict]:
    timelines: list[dict] = []
    for job_id in sorted(jobs):
        stage_ids = [stage_id for stage_id in jobs[job_id].get("stage_ids", []) if stage_id in stages]
        if not stage_ids:
            timelines.append({"job_id": job_id, "parallel_groups": []})
            continue

        levels: dict[int, int] = {}

        def level_of(stage_id: int) -> int:
            if stage_id in levels:
                return levels[stage_id]
            parents = [parent for parent in stages[stage_id].get("parent_ids", []) if parent in stage_ids]
            levels[stage_id] = 0 if not parents else 1 + max(level_of(parent) for parent in parents)
            return levels[stage_id]

        for stage_id in stage_ids:
            level_of(stage_id)

        groups_by_level: dict[int, list[int]] = defaultdict(list)
        for stage_id, level in levels.items():
            groups_by_level[level].append(stage_id)

        parallel_groups = []
        for level in sorted(groups_by_level):
            group_stage_ids = sorted(groups_by_level[level])
            start_ms = min(stages[stage_id].get("start_ms", 0) for stage_id in group_stage_ids)
            end_ms = max(stages[stage_id].get("end_ms", 0) for stage_id in group_stage_ids)
            parallel_groups.append(
                {
                    "stages": group_stage_ids,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "duration_ms": max(0, end_ms - start_ms),
                }
            )
        timelines.append({"job_id": job_id, "parallel_groups": parallel_groups})
    return timelines


def _build_llm_context(report: dict) -> dict:
    return {
        "app": report["app"],
        "cluster": report["cluster"],
        "driver_analysis": report["driver_analysis"],
        "environment": report["environment"],
        "scalability_simulation": report["scalability_simulation"],
        "top_bottlenecks": report["top_bottlenecks"],
        "stages_summary": [
            {key: value for key, value in stage.items() if key != "task_run_times"}
            for stage in report["stages"]
        ],
        "host_analysis": report["host_analysis"],
        "job_timelines": report["job_timelines"],
    }


def _build_state() -> dict:
    return {
        "app": {"id": "", "name": "", "spark_version": "unknown", "start_ms": 0, "end_ms": 0},
        "resource_profile": {},
        "executors": {},
        "executor_events": [],
        "jobs": {},
        "stage_to_job": {},
        "stages": {},
        "stage_attempts": {},  # (stage_id, attempt_id) -> status
        "environment": {},  # relevant spark properties
        "host_analysis": defaultdict(
            lambda: {
                "executor_ids": set(),
                "task_count": 0,
                "total_run_ms": 0,
                "max_task_ms": 0,
                "total_gc_ms": 0,
                "total_shuffle_read_bytes": 0,
                "total_shuffle_write_bytes": 0,
                "total_input_bytes": 0,
                "total_spill_disk_bytes": 0,
            }
        ),
    }


def _get_stage(state: dict, stage_id: int) -> dict:
    if stage_id not in state["stages"]:
        state["stages"][stage_id] = {
            "stage_id": stage_id,
            "name": f"Stage {stage_id}",
            "num_tasks": 0,
            "parent_ids": [],
            "start_ms": 0,
            "end_ms": 0,
            "wall_ms": 0,
            "task_run_ms_total": 0,
            "task_run_times": [],
            "task_count_accepted": 0,
            "task_count_filtered": 0,
            "input_bytes": 0,
            "output_bytes": 0,
            "shuffle_read_bytes": 0,
            "shuffle_write_bytes": 0,
            "spill_memory_bytes": 0,
            "spill_disk_bytes": 0,
            "gc_ms_total": 0,
            "shuffle_write_time_ns_total": 0,
            "shuffle_read_fetch_wait_ms_total": 0,
            "peak_execution_memory": 0,
            "executor_cpu_time_ns_total": 0,
            "executor_deserialize_time_ms_total": 0,
            "result_serialization_time_ms_total": 0,
            "result_size_bytes_total": 0,
        }
    return state["stages"][stage_id]


def _process_events(events) -> dict:
    state = _build_state()

    for event in events:
        event_type = event.get("Event")
        if not event_type:
            continue

        if event_type == "SparkListenerLogStart":
            state["app"]["spark_version"] = event.get("Spark Version", state["app"]["spark_version"])
            continue
        if event_type == "SparkListenerApplicationStart":
            state["app"]["id"] = event.get("App ID", state["app"]["id"])
            state["app"]["name"] = event.get("App Name", state["app"]["name"])
            state["app"]["start_ms"] = int(event.get("Timestamp", 0) or 0)
            continue
        if event_type == "SparkListenerApplicationEnd":
            state["app"]["end_ms"] = int(event.get("Timestamp", 0) or 0)
            continue
        if event_type == "SparkListenerEnvironmentUpdate":
            spark_props = event.get("Spark Properties", {}) or {}
            relevant_keys = [
                "spark.executor.instances", "spark.executor.memory",
                "spark.executor.cores", "spark.driver.memory",
                "spark.sql.autoBroadcastJoinThreshold", "spark.scheduler.mode",
                "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                "spark.dynamicAllocation.enabled",
            ]
            for key in relevant_keys:
                if key in spark_props:
                    state["environment"][key] = spark_props[key]
            continue
        if event_type == "SparkListenerResourceProfileAdded" and not state["resource_profile"]:
            state["resource_profile"] = event
            continue
        if event_type == "SparkListenerExecutorAdded":
            executor_id = str(event.get("Executor ID", ""))
            timestamp = int(event.get("Timestamp", 0) or 0)
            info = event.get("Executor Info", {}) or {}
            state["executors"][executor_id] = {
                "start_ms": timestamp,
                "end_ms": 0,
                "host": info.get("Host", "unknown"),
                "cores": int(info.get("Total Cores", 0) or 0),
            }
            state["executor_events"].append({"ts": timestamp, "type": "add", "executor_id": executor_id})
            continue
        if event_type == "SparkListenerExecutorRemoved":
            executor_id = str(event.get("Executor ID", ""))
            timestamp = int(event.get("Timestamp", 0) or 0)
            executor = state["executors"].setdefault(
                executor_id,
                {"start_ms": timestamp, "end_ms": 0, "host": "unknown", "cores": 0},
            )
            executor["end_ms"] = timestamp
            state["executor_events"].append({"ts": timestamp, "type": "remove", "executor_id": executor_id})
            continue
        if event_type == "SparkListenerJobStart":
            job_id = int(event.get("Job ID", -1))
            if job_id < 0:
                continue
            stage_ids = []
            for stage_info in event.get("Stage Infos", []) or []:
                stage_id = int(stage_info.get("Stage ID", -1))
                if stage_id < 0:
                    continue
                stage_ids.append(stage_id)
                state["stage_to_job"][stage_id] = job_id
                stage = _get_stage(state, stage_id)
                if stage_info.get("Number of Tasks") is not None:
                    stage["num_tasks"] = int(stage_info.get("Number of Tasks", 0) or 0)
            state["jobs"][job_id] = {
                "job_id": job_id,
                "start_ms": int(event.get("Submission Time", 0) or 0),
                "end_ms": 0,
                "stage_ids": sorted(set(stage_ids)),
            }
            continue
        if event_type == "SparkListenerJobEnd":
            job_id = int(event.get("Job ID", -1))
            if job_id in state["jobs"]:
                state["jobs"][job_id]["end_ms"] = int(event.get("Completion Time", 0) or 0)
            continue
        if event_type in {"SparkListenerStageSubmitted", "SparkListenerStageCompleted"}:
            stage_info = event.get("Stage Info", {}) or {}
            stage_id = int(stage_info.get("Stage ID", -1))
            if stage_id < 0:
                continue
            attempt_id = int(stage_info.get("Stage Attempt ID", 0) or 0)
            # Track stage attempts; for multiple attempts, keep only the latest
            prev_attempt = state["stage_attempts"].get(stage_id, -1)
            if attempt_id > prev_attempt:
                # Reset stage data for the new attempt
                if prev_attempt >= 0 and stage_id in state["stages"]:
                    old = _get_stage(state, stage_id)
                    old["task_run_ms_total"] = 0
                    old["task_run_times"] = []
                    old["task_count_accepted"] = 0
                    old["task_count_filtered"] = 0
                    old["input_bytes"] = 0
                    old["output_bytes"] = 0
                    old["shuffle_read_bytes"] = 0
                    old["shuffle_write_bytes"] = 0
                    old["spill_memory_bytes"] = 0
                    old["spill_disk_bytes"] = 0
                    old["gc_ms_total"] = 0
                    old["shuffle_write_time_ns_total"] = 0
                    old["shuffle_read_fetch_wait_ms_total"] = 0
                    old["peak_execution_memory"] = 0
                    old["executor_cpu_time_ns_total"] = 0
                    old["executor_deserialize_time_ms_total"] = 0
                    old["result_serialization_time_ms_total"] = 0
                    old["result_size_bytes_total"] = 0
                state["stage_attempts"][stage_id] = attempt_id
            stage = _get_stage(state, stage_id)
            stage["name"] = stage_info.get("Stage Name", stage["name"])
            stage["num_tasks"] = int(stage_info.get("Number of Tasks", stage["num_tasks"]) or 0)
            stage["parent_ids"] = list(stage_info.get("Parent IDs", []) or [])
            submission_time = int(stage_info.get("Submission Time", 0) or 0)
            completion_time = int(stage_info.get("Completion Time", 0) or 0)
            if submission_time > 0:
                stage["start_ms"] = submission_time
            if completion_time > 0:
                stage["end_ms"] = completion_time
            if stage["start_ms"] > 0 and stage["end_ms"] > 0:
                stage["wall_ms"] = max(0, stage["end_ms"] - stage["start_ms"])
            continue
        if event_type != "SparkListenerTaskEnd":
            continue

        stage_id = int(event.get("Stage ID", -1))
        if stage_id < 0:
            continue
        # Filter tasks from older stage attempts
        task_attempt_id = int(event.get("Stage Attempt ID", 0) or 0)
        current_attempt = state["stage_attempts"].get(stage_id, 0)
        if task_attempt_id < current_attempt:
            continue
        stage = _get_stage(state, stage_id)
        task_info = event.get("Task Info", {}) or {}
        task_metrics = event.get("Task Metrics", {}) or {}

        # Filter failed and speculative tasks (instruction #3)
        if task_info.get("Failed", False):
            stage["task_count_filtered"] += 1
            continue
        if task_info.get("Speculative", False):
            stage["task_count_filtered"] += 1
            continue

        shuffle_read = task_metrics.get("Shuffle Read Metrics", {}) or {}
        shuffle_write = task_metrics.get("Shuffle Write Metrics", {}) or {}
        input_metrics = task_metrics.get("Input Metrics", {}) or {}
        output_metrics = task_metrics.get("Output Metrics", {}) or {}

        executor_run_ms = int(task_metrics.get("Executor Run Time", 0) or 0)
        stage["task_run_ms_total"] += executor_run_ms
        stage["task_run_times"].append(executor_run_ms)
        stage["task_count_accepted"] += 1
        stage["input_bytes"] += int(input_metrics.get("Bytes Read", 0) or 0)
        stage["output_bytes"] += int(output_metrics.get("Bytes Written", 0) or 0)
        stage["shuffle_read_bytes"] += int(
            shuffle_read.get("Total Bytes Read")
            or (int(shuffle_read.get("Remote Bytes Read", 0) or 0) + int(shuffle_read.get("Local Bytes Read", 0) or 0))
        )
        stage["shuffle_write_bytes"] += int(shuffle_write.get("Shuffle Bytes Written", 0) or 0)
        stage["spill_memory_bytes"] += int(task_metrics.get("Memory Bytes Spilled", 0) or 0)
        stage["spill_disk_bytes"] += int(task_metrics.get("Disk Bytes Spilled", 0) or 0)
        stage["gc_ms_total"] += int(task_metrics.get("JVM GC Time", 0) or 0)
        stage["shuffle_write_time_ns_total"] += int(shuffle_write.get("Shuffle Write Time", 0) or 0)
        stage["shuffle_read_fetch_wait_ms_total"] += int(shuffle_read.get("Fetch Wait Time", 0) or 0)
        # New quantitative fields
        peak_mem = int(task_metrics.get("Peak Execution Memory", 0) or 0)
        if peak_mem > stage["peak_execution_memory"]:
            stage["peak_execution_memory"] = peak_mem
        stage["executor_cpu_time_ns_total"] += int(task_metrics.get("Executor CPU Time", 0) or 0)
        stage["executor_deserialize_time_ms_total"] += int(task_metrics.get("Executor Deserialize Time", 0) or 0)
        stage["result_serialization_time_ms_total"] += int(task_metrics.get("Result Serialization Time", 0) or 0)
        stage["result_size_bytes_total"] += int(task_metrics.get("Result Size", 0) or 0)

        launch_ms = int(task_info.get("Launch Time", 0) or 0)
        finish_ms = int(task_info.get("Finish Time", 0) or 0)
        if launch_ms > 0 and (stage["start_ms"] == 0 or launch_ms < stage["start_ms"]):
            stage["start_ms"] = launch_ms
        if finish_ms > stage["end_ms"]:
            stage["end_ms"] = finish_ms
        if stage["start_ms"] > 0 and stage["end_ms"] > 0:
            stage["wall_ms"] = max(0, stage["end_ms"] - stage["start_ms"])

        host = task_info.get("Host") or state["executors"].get(str(task_info.get("Executor ID", "")), {}).get("host") or "unknown"
        executor_id = str(task_info.get("Executor ID", ""))
        host_entry = state["host_analysis"][host]
        host_entry["executor_ids"].add(executor_id)
        host_entry["task_count"] += 1
        host_entry["total_run_ms"] += executor_run_ms
        host_entry["max_task_ms"] = max(host_entry["max_task_ms"], executor_run_ms)
        host_entry["total_gc_ms"] += int(task_metrics.get("JVM GC Time", 0) or 0)
        host_entry["total_shuffle_read_bytes"] += int(
            shuffle_read.get("Total Bytes Read")
            or (int(shuffle_read.get("Remote Bytes Read", 0) or 0) + int(shuffle_read.get("Local Bytes Read", 0) or 0))
        )
        host_entry["total_shuffle_write_bytes"] += int(shuffle_write.get("Shuffle Bytes Written", 0) or 0)
        host_entry["total_input_bytes"] += int(input_metrics.get("Bytes Read", 0) or 0)
        host_entry["total_spill_disk_bytes"] += int(task_metrics.get("Disk Bytes Spilled", 0) or 0)

    return _finalize_report(state)


def _finalize_report(state: dict) -> dict:
    app_start_ms = state["app"]["start_ms"]
    app_end_ms = state["app"]["end_ms"]
    app_duration_ms = max(0, app_end_ms - app_start_ms)

    for executor in state["executors"].values():
        if executor["end_ms"] == 0:
            executor["end_ms"] = app_end_ms

    peak_executors = get_max_concurrent_executors(state["executor_events"])
    if peak_executors == 0:
        peak_executors = len(state["executors"])

    resource_requests = state["resource_profile"].get("Executor Resource Requests", {}) or {}
    resource_cores = _resource_amount((resource_requests.get("cores", {}) or {}).get("Amount", 0))
    fallback_cores = next((executor.get("cores", 0) for executor in state["executors"].values() if executor.get("cores", 0) > 0), 0)
    cores_per_executor = resource_cores or fallback_cores or 1
    total_cores = peak_executors * cores_per_executor
    memory_mb = _resource_amount((resource_requests.get("memory", {}) or {}).get("Amount", 0))
    overhead_mb = _resource_amount((resource_requests.get("memoryOverhead", {}) or {}).get("Amount", 0))

    available_core_ms = 0
    for executor in state["executors"].values():
        duration_ms = max(0, executor["end_ms"] - executor["start_ms"])
        available_core_ms += duration_ms * (executor.get("cores", 0) or cores_per_executor)
    if available_core_ms == 0 and app_duration_ms > 0 and total_cores > 0:
        available_core_ms = app_duration_ms * total_cores

    jobs_output = []
    total_job_ms = 0
    for job_id in sorted(state["jobs"]):
        job = state["jobs"][job_id]
        duration_ms = max(0, job["end_ms"] - job["start_ms"])
        total_job_ms += duration_ms
        jobs_output.append(
            {
                "job_id": job_id,
                "start_ms": job["start_ms"],
                "end_ms": job["end_ms"],
                "duration_ms": duration_ms,
                "stage_ids": sorted(job["stage_ids"]),
            }
        )

    total_stage_runtime_ms = sum(stage["task_run_ms_total"] for stage in state["stages"].values())
    total_io_bytes = sum(
        stage["input_bytes"] + stage["output_bytes"] + stage["shuffle_read_bytes"] + stage["shuffle_write_bytes"]
        for stage in state["stages"].values()
    )
    top_wall_stage_ids = {stage["stage_id"] for stage in sorted(state["stages"].values(), key=lambda item: item["wall_ms"], reverse=True)[:3]}
    stages_output = []
    bottleneck_candidates = []

    # ── Driver vs Executor time decomposition ──────────────────────────────
    # Build sorted list of (start, end) intervals for all stages to find gaps
    stage_intervals = []
    for stage in state["stages"].values():
        if stage["start_ms"] > 0 and stage["end_ms"] > 0:
            stage_intervals.append((stage["start_ms"], stage["end_ms"]))
    stage_intervals.sort()

    # Merge overlapping stage intervals
    merged_stage_intervals = []
    for start, end in stage_intervals:
        if merged_stage_intervals and start <= merged_stage_intervals[-1][1]:
            merged_stage_intervals[-1] = (merged_stage_intervals[-1][0], max(merged_stage_intervals[-1][1], end))
        else:
            merged_stage_intervals.append((start, end))

    # Driver time = app duration minus time when any stage was active
    total_stage_active_ms = sum(end - start for start, end in merged_stage_intervals)
    driver_time_ms = max(0, app_duration_ms - total_stage_active_ms)
    executor_time_ms = total_stage_active_ms
    driver_pct = _safe_pct(driver_time_ms, app_duration_ms)

    # Build driver time intervals (gaps between merged stage intervals)
    driver_intervals = []
    if merged_stage_intervals and app_start_ms < merged_stage_intervals[0][0]:
        driver_intervals.append({
            "start_ms": app_start_ms,
            "end_ms": merged_stage_intervals[0][0],
            "duration_ms": merged_stage_intervals[0][0] - app_start_ms,
            "label": "pre_first_stage",
        })
    for i in range(len(merged_stage_intervals) - 1):
        gap_start = merged_stage_intervals[i][1]
        gap_end = merged_stage_intervals[i + 1][0]
        if gap_end > gap_start:
            driver_intervals.append({
                "start_ms": gap_start,
                "end_ms": gap_end,
                "duration_ms": gap_end - gap_start,
                "label": "inter_stage_gap",
            })
    if merged_stage_intervals and merged_stage_intervals[-1][1] < app_end_ms:
        driver_intervals.append({
            "start_ms": merged_stage_intervals[-1][1],
            "end_ms": app_end_ms,
            "duration_ms": app_end_ms - merged_stage_intervals[-1][1],
            "label": "post_last_stage",
        })

    for stage_id in sorted(state["stages"]):
        stage = state["stages"][stage_id]
        wall_ms = stage["wall_ms"]
        available_ms = total_cores * wall_ms
        used_pct = _safe_pct(stage["task_run_ms_total"], available_ms)
        wasted_pct = max(0.0, 100.0 - used_pct) if available_ms > 0 else 0.0
        sorted_task_times = sorted(stage["task_run_times"])
        task_count = len(sorted_task_times)
        min_task_ms = sorted_task_times[0] if sorted_task_times else 0
        median_task_ms = sorted_task_times[task_count // 2] if sorted_task_times else 0
        max_task_ms = sorted_task_times[-1] if sorted_task_times else 0
        avg_task_ms = (stage["task_run_ms_total"] / task_count) if task_count > 0 else 0.0
        task_skew = _safe_ratio(max_task_ms, median_task_ms) if median_task_ms > 0 else 0.0
        # Skew per spec: max/avg (Sparklens definition)
        skew_avg_ratio = _safe_ratio(max_task_ms, avg_task_ms) if avg_task_ms > 0 else 0.0
        stage_skew = _safe_ratio(max_task_ms, wall_ms) if wall_ms > 0 else 0.0
        p_ratio = _safe_ratio(stage["num_tasks"], total_cores) if total_cores > 0 else 0.0
        input_total = stage["input_bytes"] + stage["shuffle_read_bytes"]
        output_total = stage["output_bytes"] + stage["shuffle_write_bytes"]
        stage_io_total = stage["input_bytes"] + stage["output_bytes"] + stage["shuffle_read_bytes"] + stage["shuffle_write_bytes"]
        oi_ratio = _safe_ratio(output_total, input_total) if input_total > 0 else 0.0
        gc_pct = _safe_pct(stage["gc_ms_total"], stage["task_run_ms_total"])
        shuffle_write_pct = (
            stage["shuffle_write_time_ns_total"] * 100.0 / stage["task_run_ms_total"] / 1_000_000.0
            if stage["task_run_ms_total"] > 0
            else 0.0
        )
        shuffle_read_fetch_pct = _safe_pct(stage["shuffle_read_fetch_wait_ms_total"], stage["task_run_ms_total"])
        ideal_wall_ms = int(stage["task_run_ms_total"] / total_cores) if total_cores > 0 else stage["task_run_ms_total"]
        # Per-stage percentages relative to app totals
        wall_clock_pct = _safe_pct(wall_ms, app_duration_ms)
        task_runtime_pct = _safe_pct(stage["task_run_ms_total"], total_stage_runtime_ms)
        io_pct = _safe_pct(stage_io_total, total_io_bytes)
        # CPU utilization: executor CPU time (ns) / executor run time (ms * 1e6)
        cpu_pct = (
            stage["executor_cpu_time_ns_total"] * 100.0 / (stage["task_run_ms_total"] * 1_000_000.0)
            if stage["task_run_ms_total"] > 0
            else 0.0
        )

        stages_output.append(
            {
                "stage_id": stage_id,
                "job_id": state["stage_to_job"].get(stage_id, -1),
                "name": stage["name"],
                "num_tasks": stage["num_tasks"],
                "task_count_accepted": stage["task_count_accepted"],
                "task_count_filtered": stage["task_count_filtered"],
                "parent_ids": sorted(stage["parent_ids"]),
                "wall_ms": wall_ms,
                "start_ms": stage["start_ms"],
                "end_ms": stage["end_ms"],
                "task_run_ms_total": stage["task_run_ms_total"],
                "task_run_times": list(stage["task_run_times"]),
                "one_core_hours": {
                    "available": _round_float(available_ms / 3_600_000.0),
                    "used": _round_float(stage["task_run_ms_total"] / 3_600_000.0),
                    "used_pct": _round_float(used_pct),
                    "wasted_pct": _round_float(wasted_pct),
                },
                "skew": {
                    "min_task_ms": min_task_ms,
                    "median_task_ms": median_task_ms,
                    "avg_task_ms": _round_float(avg_task_ms),
                    "max_task_ms": max_task_ms,
                    "task_skew": _round_float(task_skew),
                    "skew_avg_ratio": _round_float(skew_avg_ratio),
                    "stage_skew": _round_float(stage_skew),
                    "p_ratio": _round_float(p_ratio),
                },
                "io": {
                    "input_bytes": stage["input_bytes"],
                    "output_bytes": stage["output_bytes"],
                    "shuffle_read_bytes": stage["shuffle_read_bytes"],
                    "shuffle_write_bytes": stage["shuffle_write_bytes"],
                    "spill_memory_bytes": stage["spill_memory_bytes"],
                    "spill_disk_bytes": stage["spill_disk_bytes"],
                    "oi_ratio": _round_float(oi_ratio),
                },
                "time_distribution": {
                    "gc_ms_total": stage["gc_ms_total"],
                    "gc_pct": _round_float(gc_pct),
                    "shuffle_write_pct": _round_float(shuffle_write_pct),
                    "shuffle_read_fetch_pct": _round_float(shuffle_read_fetch_pct),
                    "cpu_pct": _round_float(cpu_pct),
                },
                "memory": {
                    "peak_execution_memory": stage["peak_execution_memory"],
                    "spill_memory_bytes": stage["spill_memory_bytes"],
                    "spill_disk_bytes": stage["spill_disk_bytes"],
                },
                "percentages": {
                    "wall_clock_pct": _round_float(wall_clock_pct),
                    "task_runtime_pct": _round_float(task_runtime_pct),
                    "io_pct": _round_float(io_pct),
                },
                "ideal_wall_ms": ideal_wall_ms,
            }
        )

        # ── Bottleneck detection with spec thresholds ──────────────────────
        if stage_id in top_wall_stage_ids:
            bottleneck_candidates.append({"type": "long_stage", "stage_id": stage_id, "metric_value": float(wall_ms), "score": float(wall_ms), "description": f"Stage {stage_id} is one of the longest stages by wall clock."})
        if stage_id in top_wall_stage_ids and used_pct < 50.0:
            bottleneck_candidates.append({"type": "low_parallelism", "stage_id": stage_id, "metric_value": float(used_pct), "score": 100.0 - used_pct, "description": f"Stage {stage_id} used only {used_pct:.1f}% of available core time."})
        # Skew: >2x attention, >5x critical (using max/avg per spec)
        if skew_avg_ratio > 2.0:
            severity = "critical" if skew_avg_ratio > 5.0 else "attention"
            bottleneck_candidates.append({"type": "skew", "stage_id": stage_id, "metric_value": float(skew_avg_ratio), "score": float(skew_avg_ratio) * (2.0 if severity == "critical" else 1.0), "description": f"Stage {stage_id} has {'severe' if severity == 'critical' else 'moderate'} task skew (max/avg={skew_avg_ratio:.2f}x)."})
        # GC: >5% attention, >15% critical
        if gc_pct > 5.0:
            severity = "critical" if gc_pct > 15.0 else "attention"
            bottleneck_candidates.append({"type": "gc_pressure", "stage_id": stage_id, "metric_value": float(gc_pct), "score": float(gc_pct) * (2.0 if severity == "critical" else 1.0), "description": f"Stage {stage_id} spent {gc_pct:.1f}% of executor time in JVM GC ({severity})."})
        # Shuffle bound: fetch wait >10% attention, >25% critical
        if shuffle_read_fetch_pct > 10.0:
            severity = "critical" if shuffle_read_fetch_pct > 25.0 else "attention"
            bottleneck_candidates.append({"type": "shuffle_bound", "stage_id": stage_id, "metric_value": float(shuffle_read_fetch_pct), "score": float(shuffle_read_fetch_pct) * (2.0 if severity == "critical" else 1.0), "description": f"Stage {stage_id} spent {shuffle_read_fetch_pct:.1f}% of executor time waiting on shuffle fetches ({severity})."})
        shuffle_heavy_pct = shuffle_write_pct + shuffle_read_fetch_pct
        if shuffle_heavy_pct > 30.0:
            bottleneck_candidates.append({"type": "shuffle_heavy", "stage_id": stage_id, "metric_value": float(shuffle_heavy_pct), "score": float(shuffle_heavy_pct), "description": f"Stage {stage_id} spent {shuffle_heavy_pct:.1f}% of executor time in shuffle waits/writes."})
        # Spill: any disk spill >0 = attention, >1 GB = critical
        if stage["spill_disk_bytes"] > 0:
            spill_gb = stage["spill_disk_bytes"] / (1024 ** 3)
            severity = "critical" if spill_gb > 1.0 else "attention"
            bottleneck_candidates.append({"type": "spill", "stage_id": stage_id, "metric_value": float(spill_gb), "score": float(spill_gb) * 10.0 + 5.0, "description": f"Stage {stage_id} spilled {spill_gb:.2f} GB to disk ({severity})."})
        # Wasted% thresholds: >30% attention, >70% critical
        if wasted_pct > 30.0:
            severity = "critical" if wasted_pct > 70.0 else "attention"
            bottleneck_candidates.append({"type": "low_utilization", "stage_id": stage_id, "metric_value": float(wasted_pct), "score": float(wasted_pct), "description": f"Stage {stage_id} wasted {wasted_pct:.1f}% of OneCoreComputeHours ({severity})."})

    # Driver overhead bottleneck: >20% attention, >40% critical
    if driver_pct > 20.0:
        severity = "critical" if driver_pct > 40.0 else "attention"
        bottleneck_candidates.append({"type": "driver_overhead", "stage_id": -1, "metric_value": float(driver_pct), "score": float(driver_pct) * (2.0 if severity == "critical" else 1.0), "description": f"Driver (non-executor) time is {driver_pct:.1f}% of total application time ({severity}). Adding executors has limited benefit."})

    host_output = {}
    for host in sorted(state["host_analysis"]):
        data = state["host_analysis"][host]
        host_output[host] = {
            "executor_ids": sorted(data["executor_ids"]),
            "task_count": data["task_count"],
            "total_run_ms": data["total_run_ms"],
            "avg_task_ms": int(data["total_run_ms"] / data["task_count"]) if data["task_count"] > 0 else 0,
            "max_task_ms": data["max_task_ms"],
            "total_gc_ms": data["total_gc_ms"],
            "gc_pct": _round_float(_safe_pct(data["total_gc_ms"], data["total_run_ms"])),
            "total_shuffle_read_bytes": data["total_shuffle_read_bytes"],
            "total_shuffle_write_bytes": data["total_shuffle_write_bytes"],
            "total_input_bytes": data["total_input_bytes"],
            "total_spill_disk_bytes": data["total_spill_disk_bytes"],
        }

    scalability_simulation = []
    for pct in TEST_PERCENTAGES:
        target_executors = max(1, peak_executors * pct // 100) if peak_executors > 0 else 1
        estimated_ms = estimate_app_wall_clock(
            jobs=state["jobs"],
            stages=state["stages"],
            app_ms=app_duration_ms,
            job_total_ms=total_job_ms,
            current_executor_count=peak_executors,
            cores_per_executor=cores_per_executor,
            target_executor_count=target_executors,
        )
        denominator = estimated_ms * target_executors * max(1, cores_per_executor)
        scalability_simulation.append(
            {
                "executor_count": target_executors,
                "pct_of_current": pct,
                "estimated_ms": estimated_ms,
                "estimated_min": round(estimated_ms / 60_000.0, 1),
                "cluster_utilization_pct": _round_float(_safe_pct(total_stage_runtime_ms, denominator)),
            }
        )

    bottleneck_candidates.sort(key=lambda item: item["score"], reverse=True)
    top_bottlenecks = [
        {"rank": index, "type": candidate["type"], "stage_id": candidate["stage_id"], "metric_value": _round_float(candidate["metric_value"]), "description": candidate["description"]}
        for index, candidate in enumerate(bottleneck_candidates[:5], start=1)
    ]

    report = {
        "app": {
            "id": state["app"]["id"],
            "name": state["app"]["name"],
            "spark_version": state["app"]["spark_version"],
            "start_ms": app_start_ms,
            "end_ms": app_end_ms,
            "duration_ms": app_duration_ms,
            "duration_min": round(app_duration_ms / 60_000.0, 1),
            "driver_idle_pct": round(_safe_pct(max(0, app_duration_ms - total_job_ms), app_duration_ms), 1),
        },
        "cluster": {
            "executor_count": peak_executors,
            "cores_per_executor": cores_per_executor,
            "total_cores": total_cores,
            "memory_mb_per_executor": memory_mb,
            "overhead_mb": overhead_mb,
            "available_core_hours": _round_float(available_core_ms / 3_600_000.0),
            "used_core_hours": _round_float(total_stage_runtime_ms / 3_600_000.0),
            "cluster_utilization_pct": _round_float(_safe_pct(total_stage_runtime_ms, available_core_ms)),
        },
        "driver_analysis": {
            "driver_time_ms": driver_time_ms,
            "executor_time_ms": executor_time_ms,
            "driver_pct": _round_float(driver_pct),
            "executor_pct": _round_float(100.0 - driver_pct),
            "driver_intervals": driver_intervals,
        },
        "environment": state["environment"],
        "jobs": jobs_output,
        "stages": stages_output,
        "scalability_simulation": scalability_simulation,
        "job_timelines": _build_job_timelines(state["jobs"], state["stages"]),
        "host_analysis": host_output,
        "top_bottlenecks": top_bottlenecks,
    }
    report["llm_context"] = _build_llm_context(report)
    return report


def build_sparklens_report(zip_path: str) -> dict:
    return _process_events(_parse_events_from_zip_file(zip_path))


def build_sparklens_report_from_bytes(zip_bytes: bytes, events=None) -> dict:
    """Build Sparklens report from ZIP bytes.

    If *events* is provided (an iterable of already-parsed event dicts),
    they are used directly instead of re-parsing the ZIP.  This avoids a
    full second pass when the caller has already iterated the events (e.g.
    the LogReducer single-pass handler).
    """
    if events is not None:
        return _process_events(events)
    return _process_events(_parse_events_from_zip_bytes(zip_bytes))
