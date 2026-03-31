"""
Helpers to align LLM-produced code references with uploaded source files.
"""
from __future__ import annotations

import json
import re


def _find_snippet_line_range(source: str, snippet: str) -> tuple[int, int] | None:
    if not snippet:
        return None

    src = source.replace("\r\n", "\n")
    snp = snippet.replace("\r\n", "\n").strip("\n")
    if not snp:
        return None

    idx = src.find(snp)
    if idx >= 0:
        start = src.count("\n", 0, idx) + 1
        end = start + snp.count("\n")
        return start, end

    src_lines = src.split("\n")
    snp_lines = [ln.strip() for ln in snp.split("\n") if ln.strip()]
    if not snp_lines:
        return None

    first = snp_lines[0]
    for i in range(len(src_lines)):
        if src_lines[i].strip() != first:
            continue
        j = i
        k = 0
        while j < len(src_lines) and k < len(snp_lines):
            if src_lines[j].strip() == snp_lines[k]:
                j += 1
                k += 1
            else:
                break
        if k == len(snp_lines):
            return i + 1, j

    for i in range(len(src_lines)):
        if src_lines[i].strip() != first:
            continue
        if len(snp_lines) > 1:
            window_end = min(i + len(snp_lines) + 3, len(src_lines))
            if not any(src_lines[j].strip() == snp_lines[1] for j in range(i + 1, window_end)):
                continue
        return i + 1, i + len(snp_lines)

    min_substr = 15
    for i, src_line in enumerate(src_lines):
        stripped_line = src_line.strip()
        if len(stripped_line) < min_substr:
            continue
        for snippet_token in snp_lines:
            if len(snippet_token) < min_substr:
                continue
            if stripped_line in snippet_token or snippet_token[:50] in stripped_line:
                return i + 1, i + max(len(snp_lines), 1)
    return None


def _find_function_start_line(source: str, function_name: str) -> int | None:
    if not function_name:
        return None
    pattern = rf"^\s*(?:async\s+def|def)\s+{re.escape(function_name)}\s*\("
    match = re.search(pattern, source, flags=re.MULTILINE)
    if not match:
        return None
    return source.count("\n", 0, match.start()) + 1


def reconcile_code_links(llm_text: str, py_files: dict[str, bytes]) -> str:
    if not llm_text or not py_files:
        return llm_text

    try:
        parsed = json.loads(llm_text)
    except Exception:
        return llm_text

    if not isinstance(parsed, dict):
        return llm_text

    decoded_sources = {
        name: content.decode("utf-8", errors="replace")
        for name, content in py_files.items()
    }
    meta = parsed.get("meta")
    preferred_file = meta.get("job_file") if isinstance(meta, dict) else None

    def resolve_range(snippet: str | None, function_name: str | None) -> tuple[int | None, int | None]:
        ordered_items = list(decoded_sources.items())
        if preferred_file and preferred_file in decoded_sources:
            ordered_items = [(preferred_file, decoded_sources[preferred_file])] + [
                (name, source) for name, source in ordered_items if name != preferred_file
            ]

        if snippet:
            for _, source in ordered_items:
                found = _find_snippet_line_range(source, snippet)
                if found:
                    return found

        if function_name:
            for _, source in ordered_items:
                start = _find_function_start_line(source, function_name)
                if start:
                    return start, start

        return None, None

    for bottleneck in (parsed.get("bottlenecks") or []):
        if not isinstance(bottleneck, dict):
            continue
        link = bottleneck.get("code_link")
        if not isinstance(link, dict):
            continue
        start, end = resolve_range(link.get("snippet"), link.get("function_name"))
        link["line_start"] = start
        link["line_end"] = end

    action_plan = parsed.get("action_plan")
    if isinstance(action_plan, dict):
        for fix in (action_plan.get("code_fixes") or []):
            if not isinstance(fix, dict):
                continue
            start, end = resolve_range(fix.get("before_code"), fix.get("function_name"))
            fix["line_start"] = start
            fix["line_end"] = end

    try:
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        return llm_text
