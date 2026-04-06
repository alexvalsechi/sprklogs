"""
Prompt assembly helpers for LLMAnalyzer.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from backend.models.job import AppSummary
from backend.services.llm_prompt_templates import SYSTEM_INSTRUCTIONS

logger = logging.getLogger(__name__)

SPARKLENS_GUIDANCE = {
    "en": [
        "## Deterministic Sparklens Rules",
        "- IMPORTANT: The Sparklens Metrics below are pre-computed AUTHORITATIVE deterministic calculations. NEVER recalculate, override, or reinterpret these values with your own estimates.",
        "- When Sparklens provides a metric, cite the exact numeric value verbatim. Do not approximate or round differently.",
        "- The Reduced Log Context above is raw aggregated data for your analysis. The Sparklens Metrics below are the final arithmetic truth.",
        "- Use Sparklens numbers as the primary basis for bottleneck severity, duration_expected_s, estimated_gain_min, and cluster recommendations.",
        "- Prefer exact fields when available: driver_analysis.driver_pct, cluster_utilization_pct, one_core_hours.used_pct, skew.skew_avg_ratio, skew.task_skew, time_distribution.gc_pct, time_distribution.shuffle_write_pct, time_distribution.shuffle_read_fetch_pct, time_distribution.cpu_pct, io.spill_disk_bytes, io.spill_memory_bytes, memory.peak_execution_memory, percentages.wall_clock_pct, percentages.task_runtime_pct, percentages.io_pct, and scalability_simulation estimated_min.",
        "- The driver_analysis section decomposes total app time into driver (serial) vs executor (parallel) time with specific intervals. Use driver_analysis.driver_pct to assess scalability limits per Amdahl's law.",
        "- The skew.skew_avg_ratio field (max/avg task time) is the Sparklens-standard skew metric. Values >2x indicate attention, >5x indicate severe skew.",
        "- Do not ignore Sparklens bottlenecks because of softer wording in the reduced report. Reconcile both inputs and prefer deterministic math over generic statements.",
        "- When a Sparklens metric clearly indicates skew, low parallelism, GC pressure, shuffle overhead, spill, driver overhead, or weak scaling, surface it explicitly in the response JSON with the exact numeric values.",
        "- The environment section contains actual Spark configuration properties. Reference these when recommending configuration changes.",
    ],
    "pt": [
        "## Regras Deterministicas do Sparklens",
        "- IMPORTANTE: As Metricas do Sparklens abaixo sao calculos deterministicos AUTORITATIVOS pre-computados. NUNCA recalcule, sobrescreva ou reinterprete esses valores com suas proprias estimativas.",
        "- Quando o Sparklens fornecer uma metrica, cite o valor numerico exato literalmente. Nao aproxime ou arredonde diferente.",
        "- O Contexto do Log Reduzido acima sao dados brutos agregados para sua analise. As Metricas do Sparklens abaixo sao a verdade aritmetica final.",
        "- Use os numeros do Sparklens como base principal para severidade dos gargalos, duration_expected_s, estimated_gain_min e recomendacoes de cluster.",
        "- Prefira campos exatos quando existirem: driver_analysis.driver_pct, cluster_utilization_pct, one_core_hours.used_pct, skew.skew_avg_ratio, skew.task_skew, time_distribution.gc_pct, time_distribution.shuffle_write_pct, time_distribution.shuffle_read_fetch_pct, time_distribution.cpu_pct, io.spill_disk_bytes, io.spill_memory_bytes, memory.peak_execution_memory, percentages.wall_clock_pct, percentages.task_runtime_pct, percentages.io_pct e scalability_simulation estimated_min.",
        "- A secao driver_analysis decompoe o tempo total em driver (serial) vs executor (paralelo) com intervalos especificos. Use driver_analysis.driver_pct para avaliar limites de escalabilidade pela Lei de Amdahl.",
        "- O campo skew.skew_avg_ratio (max/avg task time) e a metrica padrao do Sparklens para skew. Valores >2x indicam atencao, >5x indicam skew severo.",
        "- Nao ignore gargalos do Sparklens por causa de descricoes mais suaves no reduced report. Concilie as duas fontes e prefira calculo deterministico a linguagem generica.",
        "- Quando uma metrica do Sparklens indicar skew, baixo paralelismo, pressao de GC, overhead de shuffle, spill, overhead de driver ou baixo ganho de escala, isso deve aparecer explicitamente no JSON final com os valores numericos exatos.",
        "- A secao environment contem propriedades reais de configuracao do Spark. Referencie-as ao recomendar mudancas de configuracao.",
    ],
}


def collapse_repetitive_lines(text: str, keep: int = 2) -> str:
    """Collapse only consecutive duplicate lines to reduce prompt noise."""
    lines = text.splitlines()
    if not lines:
        return text

    out: list[str] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        j = i + 1
        while j < n and lines[j] == line:
            j += 1

        run_len = j - i
        if run_len <= keep:
            out.extend(lines[i:j])
        else:
            out.extend([line] * keep)
            out.append(f"[... repeated line omitted {run_len - keep} times ...]")
        i = j

    return "\n".join(out)


def build_llm_context(summary: AppSummary) -> dict:
    """Convert an AppSummary into a token-efficient dict for LLM consumption.

    This is the "analyze this" block — raw aggregated data the LLM should
    interpret.  Sparklens metrics are intentionally excluded; they go in a
    separate authoritative block that the LLM must NOT recalculate.
    """
    ctx: dict = {
        "app": {
            "id": summary.app_id,
            "name": summary.app_name,
            "spark_version": summary.spark_version,
            "duration_ms": summary.total_duration_ms,
            "num_stages": summary.num_stages,
            "num_tasks": summary.num_tasks,
            "executors": summary.executor_count,
            "executor_memory_mb": summary.executor_memory_mb,
            "executor_memory_overhead_mb": summary.executor_memory_overhead_mb,
            "executor_cores": summary.executor_cores,
            "total_input_bytes": summary.total_input_bytes,
            "total_output_bytes": summary.total_output_bytes,
            "total_shuffle_read_bytes": summary.total_shuffle_read_bytes,
            "total_shuffle_write_bytes": summary.total_shuffle_write_bytes,
            "sql_execution_count": summary.sql_execution_count,
        },
    }

    # Job-level efficiency KPIs
    jem = summary.job_efficiency_meta or {}
    if jem:
        ctx["job_efficiency"] = jem

    # Executor summary — collapse when homogeneous
    exec_summary = summary.executor_summary or []
    if exec_summary:
        gc_vals = [e["gc_pct"] for e in exec_summary]
        cpu_vals = [e["cpu_efficiency"] for e in exec_summary]
        gc_mean = sum(gc_vals) / len(gc_vals) if gc_vals else 0
        cpu_mean = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0
        # Check if all executors are within ±10% of mean (homogeneous)
        homogeneous = all(
            abs(e["gc_pct"] - gc_mean) <= max(gc_mean * 0.1, 0.5)
            and abs(e["cpu_efficiency"] - cpu_mean) <= max(cpu_mean * 0.1, 0.05)
            for e in exec_summary
        )
        if homogeneous and len(exec_summary) > 2:
            ctx["executor_summary"] = {
                "count": len(exec_summary),
                "uniform": True,
                "avg_gc_pct": round(gc_mean, 2),
                "avg_cpu_efficiency": round(cpu_mean, 3),
                "total_tasks": sum(e["tasks"] for e in exec_summary),
            }
        else:
            # Show individual executors, only outlier-relevant fields
            ctx["executor_summary"] = [
                {
                    "id": e["executor_id"],
                    "tasks": e["tasks"],
                    "gc_pct": e["gc_pct"],
                    "cpu_eff": e["cpu_efficiency"],
                }
                for e in exec_summary
            ]

    # Stage groups — the core token-saving structure
    stage_groups = summary.stage_groups or []
    if stage_groups:
        ctx["stage_groups"] = []
        for g in stage_groups:
            gd: dict = {
                "name": g.group_name,
                "count": g.count,
                "stage_ids": g.stage_ids,
                "total_tasks": g.total_tasks,
                "total_duration_ms": g.total_duration_ms,
                "total_input_bytes": g.total_input_bytes,
                "total_output_bytes": g.total_output_bytes,
                "total_shuffle_read_bytes": g.total_shuffle_read_bytes,
                "total_shuffle_write_bytes": g.total_shuffle_write_bytes,
            }
            if g.total_disk_spill_bytes > 0:
                gd["total_disk_spill_bytes"] = g.total_disk_spill_bytes
                gd["total_memory_spill_bytes"] = g.total_memory_spill_bytes
            if g.skew_ratio_avg is not None:
                gd["skew_ratio"] = {
                    "min": g.skew_ratio_min,
                    "avg": g.skew_ratio_avg,
                    "max": g.skew_ratio_max,
                }
            if g.worst_gc_overhead_pct > 5.0:
                gd["worst_gc_pct"] = g.worst_gc_overhead_pct
            if g.worst_cpu_efficiency is not None and g.worst_cpu_efficiency < 0.1:
                gd["worst_cpu_eff"] = g.worst_cpu_efficiency
            if g.peak_execution_memory_bytes > 0:
                gd["peak_mem_bytes"] = g.peak_execution_memory_bytes
            if g.anomalies:
                gd["anomalies"] = g.anomalies
            ctx["stage_groups"].append(gd)
    else:
        # Fallback: list individual stages if no groups were computed
        ctx["stages"] = [
            {
                "id": s.stage_id,
                "name": s.name,
                "tasks": s.num_tasks,
                "duration_ms": s.duration_ms,
                "input_bytes": s.input_bytes,
                "output_bytes": s.output_bytes,
                "shuffle_read_bytes": s.shuffle_read_bytes,
                "shuffle_write_bytes": s.shuffle_write_bytes,
                "disk_spill_bytes": s.disk_bytes_spilled,
                "skew_ratio": s.skew_ratio,
                "gc_pct": s.gc_overhead_pct,
                "cpu_eff": s.cpu_efficiency,
            }
            for s in summary.stages
        ]

    return ctx


def build_analysis_prompt(
    reduced_report: str,
    summary: Optional[AppSummary] = None,
    py_files: dict[str, bytes] | None = None,
    sparklens_context: dict | None = None,
    language: str = "en",
) -> tuple[str, bool]:
    py_files_provided = bool(py_files)
    instructions = SYSTEM_INSTRUCTIONS.get(language, SYSTEM_INSTRUCTIONS["en"])
    mode_indicator = (
        "**[OPERATION MODE ACTIVATED: MODE B - Log + Python Code]**"
        if py_files_provided
        else "**[OPERATION MODE ACTIVATED: MODE A - Log Only]**"
    )

    prompt_parts = [
        instructions,
        "",
        mode_indicator,
    ]

    # Prefer structured LLM context when summary is available
    if summary is not None:
        llm_ctx = build_llm_context(summary)
        prompt_parts.extend([
            "",
            "## Reduced Log Context (analyze this data)",
            "```json",
            json.dumps(llm_ctx, separators=(",", ":")),
            "```",
        ])
    else:
        # Fallback: use the markdown report directly (legacy path)
        report_for_prompt = collapse_repetitive_lines(reduced_report)
        prompt_parts.extend([
            "",
            "## Reduced Log Report",
            report_for_prompt,
        ])

    if sparklens_context:
        sparklens_rules = SPARKLENS_GUIDANCE.get(language, SPARKLENS_GUIDANCE["en"])
        prompt_parts.extend(
            [
                "",
                *sparklens_rules,
                "",
                "## Deterministic Sparklens Metrics (reference only — do not recalculate)",
                "```json",
                json.dumps(sparklens_context, indent=2, sort_keys=True),
                "```",
            ]
        )

    if py_files_provided:
        prompt_parts.append("\n## PySpark Source Files")
        for fname, content in (py_files or {}).items():
            try:
                text = content.decode("utf-8", errors="replace")
                line_count = text.count("\n") + 1
                # Truncate large files to avoid excessive token usage
                MAX_LINES = 500
                if line_count > MAX_LINES:
                    lines = text.split("\n")[:MAX_LINES]
                    text = "\n".join(lines)
                    text += f"\n# ... (truncated, showing first {MAX_LINES} of {line_count} lines)"
                logger.info(
                    "Embedding py_file in prompt: %s — %d bytes, %d lines%s",
                    fname,
                    len(content),
                    min(line_count, MAX_LINES),
                    " (truncated)" if line_count > MAX_LINES else "",
                )
                prompt_parts.append(f"\n### {fname}\n```python\n{text}\n```")
            except Exception:
                continue

    return "\n".join(prompt_parts), py_files_provided
