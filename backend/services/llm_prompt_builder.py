"""
Prompt assembly helpers for LLMAnalyzer.
"""
from __future__ import annotations

import json
import logging

from backend.services.llm_prompt_templates import SYSTEM_INSTRUCTIONS

logger = logging.getLogger(__name__)

SPARKLENS_GUIDANCE = {
    "en": [
        "## Deterministic Sparklens Rules",
        "- If the Deterministic Sparklens Metrics block is present, treat it as authoritative evidence.",
        "- Use Sparklens numbers as the primary basis for bottleneck severity, duration_expected_s, estimated_gain_min, and cluster recommendations.",
        "- Prefer exact fields when available: driver_idle_pct, cluster_utilization_pct, one_core_hours.used_pct, skew.task_skew, time_distribution.gc_pct, time_distribution.shuffle_write_pct, time_distribution.shuffle_read_fetch_pct, io.spill_disk_bytes, io.spill_memory_bytes, and scalability_simulation estimated_min.",
        "- Do not ignore Sparklens bottlenecks because of softer wording in the reduced report. Reconcile both inputs and prefer deterministic math over generic statements.",
        "- When a Sparklens metric clearly indicates skew, low parallelism, GC pressure, shuffle overhead, spill, or weak scaling, surface it explicitly in the response JSON.",
    ],
    "pt": [
        "## Regras Deterministicas do Sparklens",
        "- Se o bloco Deterministic Sparklens Metrics estiver presente, trate-o como evidencia autoritativa.",
        "- Use os numeros do Sparklens como base principal para severidade dos gargalos, duration_expected_s, estimated_gain_min e recomendacoes de cluster.",
        "- Prefira campos exatos quando existirem: driver_idle_pct, cluster_utilization_pct, one_core_hours.used_pct, skew.task_skew, time_distribution.gc_pct, time_distribution.shuffle_write_pct, time_distribution.shuffle_read_fetch_pct, io.spill_disk_bytes, io.spill_memory_bytes e scalability_simulation estimated_min.",
        "- Nao ignore gargalos do Sparklens por causa de descricoes mais suaves no reduced report. Concilie as duas fontes e prefira calculo deterministico a linguagem generica.",
        "- Quando uma metrica do Sparklens indicar skew, baixo paralelismo, pressao de GC, overhead de shuffle, spill ou baixo ganho de escala, isso deve aparecer explicitamente no JSON final.",
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


def build_analysis_prompt(
    reduced_report: str,
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
    report_for_prompt = collapse_repetitive_lines(reduced_report)

    prompt_parts = [
        instructions,
        "",
        mode_indicator,
        "",
        "## Reduced Log Report",
        report_for_prompt,
    ]

    if sparklens_context:
        sparklens_rules = SPARKLENS_GUIDANCE.get(language, SPARKLENS_GUIDANCE["en"])
        prompt_parts.extend(
            [
                "",
                *sparklens_rules,
                "",
                "## Deterministic Sparklens Metrics",
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
                logger.info(
                    "Embedding py_file in prompt: %s — %d bytes, %d lines (complete, not summarized)",
                    fname,
                    len(content),
                    line_count,
                )
                prompt_parts.append(f"\n### {fname}\n```python\n{text}\n```")
            except Exception:
                continue

    return "\n".join(prompt_parts), py_files_provided
