"""
Prompt assembly helpers for LLMAnalyzer.
"""
from __future__ import annotations

import logging

from backend.services.llm_prompt_templates import SYSTEM_INSTRUCTIONS

logger = logging.getLogger(__name__)


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
