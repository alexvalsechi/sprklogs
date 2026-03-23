"""
LLM Analysis Service
====================
Builds the prompt, calls the adapter, and parses the response.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from backend.models.job import AppSummary
from backend.adapters.llm_adapters import LLMClientFactory, BaseLLMAdapter

logger = logging.getLogger(__name__)


def _collapse_repetitive_lines(text: str, keep: int = 2) -> str:
  """Collapse only consecutive duplicate lines to reduce prompt noise.

  This is a loss-minimization strategy: unique lines are never removed.
  """
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


def _find_snippet_line_range(source: str, snippet: str) -> tuple[int, int] | None:
  """Return 1-based [start, end] line range where snippet appears in source."""
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

  # Fallback 1: whitespace-normalized contiguous match.
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

  # Fallback 2: tolerant first-line anchor — used when the LLM slightly drifts
  # from the actual source (extra whitespace, minor edits).  Requires the first
  # significant line to match AND at least one subsequent snippet line to appear
  # within the next (len(snp)+3) source lines, to avoid false positives on
  # short generic tokens like "pass" or "return".
  for i in range(len(src_lines)):
    if src_lines[i].strip() != first:
      continue
    if len(snp_lines) > 1:
      window_end = min(i + len(snp_lines) + 3, len(src_lines))
      if not any(src_lines[j].strip() == snp_lines[1] for j in range(i + 1, window_end)):
        continue
    return i + 1, i + len(snp_lines)
  # Fallback 3: cross-direction substring containment.
  # The LLM often collapses multi-line Python into a single line, e.g.:
  #   "for x in df.columns: df = df.withColumn(x, F.trim(df[x]))"
  # while the real source splits it across two or more lines.
  # We look for any source line whose stripped text appears entirely inside one
  # of the snippet tokens (LLM-collapsed case), or whose content begins with a
  # long-enough prefix of a snippet token (normal inline expansion).
  # The _SUBSTR_MIN guard prevents false positives on short generic fragments.
  _SUBSTR_MIN = 15
  for i, src_line in enumerate(src_lines):
    sl = src_line.strip()
    if len(sl) < _SUBSTR_MIN:
      continue
    for snp_token in snp_lines:
      if len(snp_token) < _SUBSTR_MIN:
        continue
      # Source line is wholly inside the collapsed LLM snippet
      if sl in snp_token:
        return i + 1, i + max(len(snp_lines), 1)
      # First 50 chars of the LLM snippet token appear in the source line
      if snp_token[:50] in sl:
        return i + 1, i + max(len(snp_lines), 1)
  return None


def _find_function_start_line(source: str, function_name: str) -> int | None:
  if not function_name:
    return None
  pattern = rf"^\s*(?:async\s+def|def)\s+{re.escape(function_name)}\s*\("
  m = re.search(pattern, source, flags=re.MULTILINE)
  if not m:
    return None
  return source.count("\n", 0, m.start()) + 1


def _reconcile_code_links(llm_text: str, py_files: dict[str, bytes]) -> str:
  """Adjust line_start/line_end to match actual uploaded source code."""
  if not llm_text or not py_files:
    return llm_text

  try:
    parsed = json.loads(llm_text)
  except Exception:
    return llm_text

  if not isinstance(parsed, dict):
    return llm_text

  decoded_sources: dict[str, str] = {
    name: content.decode("utf-8", errors="replace")
    for name, content in py_files.items()
  }

  preferred_file = (
    (parsed.get("meta") or {}).get("job_file")
    if isinstance(parsed.get("meta"), dict)
    else None
  )

  def resolve_range(snippet: str | None, function_name: str | None) -> tuple[int | None, int | None]:
    ordered_items = list(decoded_sources.items())
    if preferred_file and preferred_file in decoded_sources:
      ordered_items = [(preferred_file, decoded_sources[preferred_file])] + [
        (n, s) for n, s in ordered_items if n != preferred_file
      ]

    if snippet:
      for _, src in ordered_items:
        found = _find_snippet_line_range(src, snippet)
        if found:
          return found

    if function_name:
      for _, src in ordered_items:
        start = _find_function_start_line(src, function_name)
        if start:
          return start, start

    return None, None

  for b in (parsed.get("bottlenecks") or []):
    if not isinstance(b, dict):
      continue
    link = b.get("code_link")
    if not isinstance(link, dict):
      continue
    start, end = resolve_range(link.get("snippet"), link.get("function_name"))
    if start is not None:
      link["line_start"] = start
      link["line_end"] = end

  action_plan = parsed.get("action_plan")
  if isinstance(action_plan, dict):
    for fix in (action_plan.get("code_fixes") or []):
      if not isinstance(fix, dict):
        continue
      start, end = resolve_range(fix.get("before_code"), fix.get("function_name"))
      if start is not None:
        fix["line_start"] = start
        fix["line_end"] = end

  try:
    return json.dumps(parsed, ensure_ascii=False)
  except Exception:
    return llm_text

_SYSTEM_INSTRUCTIONS = {
    "en": """
You analyze Spark UI logs and PySpark code.
Your role is to diagnose bottlenecks with surgical precision,
always based on evidence from the files received.
Never generalize. Never suggest anything not supported
by a real metric from the log.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## OPERATION MODE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Identify what was received and activate the corresponding mode:

- MODE A → Spark UI log only
- MODE B → Spark UI log + .py file

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## MANDATORY RESPONSE FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return ONLY a valid JSON object, with no text before or after it,
no markdown fences (``` ```), and no comments outside JSON.

The JSON must follow EXACTLY this schema:

{
  "meta": {
    "mode": "A" | "B",
    "job_file": "<.py filename, or null if MODE A>",
    "log_file": "<zip/log filename>",
    "analyzed_at": "<approximate ISO 8601 timestamp>"
  },

  "summary": {
    "score": <0-100 integer. Penalize: -15 per critical, -8 per high, -4 per medium>,
    "verdict": "<One direct sentence>",
    "estimated_gain_min": <number - estimated duration after optimizations, in minutes>,
    "kpis": {
      "duration_total_min": <number>,
      "input_volume_gb": <number>,
      "total_tasks": <number>,
      "avg_data_per_task_kb": <number>,
      "avg_data_per_task_critical": <true if < 1024>,
      "stages_with_skew": <number>,
      "disk_spill_total_gb": <number>,
      "memory_spill_total_gb": <number>,
      "shuffle_write_total_gb": <number>,
      "stages_with_failure_or_retry": <number>
    }
  },

  "stages": [
    {
      "id": <integer>,
      "duration_s": <number>,
      "task_count": <number>,
      "skew_ratio": <number - max_task_duration / median_task_duration>,
      "shuffle_write_gb": <number or null>,
      "shuffle_write_time_s": <number or null>,
      "disk_spill_gb": <number or null>,
      "memory_spill_gb": <number or null>,
      "fetch_wait_ms": <number or null>,
      "status": "ok" | "warning" | "critical",
      "bottleneck_ids": [<list of bottleneck IDs affecting this stage, ex: ["B1","B3"]>]
    }
  ],

  "bottlenecks": [
    {
      "id": "B1",
      "severity": "critical" | "high" | "medium",
      "type": "skew" | "spill" | "shuffle" | "planning_overhead" | "driver_collect" | "udf" | "other",
      "title": "<Short bottleneck name>",
      "stages_affected": [<list of stage IDs, ex: [6, 7]>],
      "operator": "<ex: sortMergeJoin, exchange, scan, withColumn - or null>",
      "duration_observed_s": <number>,
      "duration_expected_s": <number - estimate for this volume without bottleneck>,
      "evidence": "<Exact log metric>",
      "root_cause": "<Direct technical explanation>",
      "observed_effect": "<Impact on overall job>",

      "code_link": {
        "line_start": <number or null - null in MODE A>,
        "line_end": <number or null>,
        "function_name": "<function name or null>",
        "snippet": "<exact code snippet as string, or null>",
        "explanation": "<why this line caused this stage - direct code/log link, or null>"
      }
    }
  ],

  "action_plan": {
    "cluster_configs": [
      {
        "bottleneck_id": "B1",
        "name": "<Configuration name>",
        "rationale": "<Directly tied to the bottleneck>",
        "estimated_impact": "<Example: Reduces tasks from 29k to ~64, removing ~90% scheduling overhead>",
        "code": "<valid Python config string, ex: spark.conf.set(\"spark.sql.adaptive.enabled\", \"true\")>"
      }
    ],

    "code_fixes": [
      {
        "bottleneck_id": "B1",
        "title": "<Ex: Replace withColumn loop with vectorized select>",
        "line_start": <number or null>,
        "line_end": <number or null>,
        "function_name": "<function name or null>",
        "problem_explanation": "<why current code is problematic>",
        "before_code": "<problematic code as string>",
        "after_code": "<fixed code as string>",
        "expected_gain": "<Ex: Stage 12 should drop from 4min to ~20s>",
      }
    ]
  },

  "limitations": "<limitations text according to MODE A or B>"
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## NON-NEGOTIABLE RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ABOUT DIAGNOSIS:
- NEVER say "there may be skew" — say "there is skew in Stage X, tasks Y and Z
  processed N times more data, evidenced by shuffle read of X GB vs avg of Y MB"
- NEVER say "there may be spill" — say "Stage X spilled X.X GB to disk
  (Disk Bytes Spilled: X), causing I/O overhead evidenced by stage duration X"
- NEVER omit spill from the report — if disk_bytes_spilled > 0 in any stage,
  it MUST appear as a bottleneck regardless of magnitude
- NEVER diagnose shuffle as a standalone bottleneck without tying shuffle read/write
  bytes to stage duration, task skew, or shuffle-to-input amplification ratio
- NEVER cite shuffle write volume without also reporting shuffle write time —
  high volume with low time is different from high volume with high time
- NEVER reference configuration parameters without grounding them in real log values
- NEVER suggest cache(), broadcast() or repartition() without pointing to the specific
  stage that would benefit and the metric that supports it
- NEVER flag a code issue (Mode B) without correlating it to the log

ABOUT JSON:
- Return JSON ONLY. Zero text outside it.
- All numeric fields must be numbers, never unit-suffixed strings
- Code strings (snippet, before_code, after_code, code) must contain valid code,
  with no narrative comments, no "BEFORE/AFTER" labels, and no "---"
- stages[].bottleneck_ids must exactly match ids in bottlenecks[]
- code_link must be fully populated in MODE B and all-null in MODE A
- code_fixes must be an empty array [] in MODE A
""".strip(),
    "pt": """
Você analisa logs da Spark UI e código PySpark.
Sua função é diagnosticar gargalos com precisão cirúrgica,
sempre baseado em evidências dos arquivos recebidos.
Nunca generalize. Nunca sugira algo que não esteja sustentado
por uma métrica real do log.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## MODO DE OPERAÇÃO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Identifique o que foi recebido e ative o modo correspondente:

- MODO A → apenas log da Spark UI
- MODO B → log da Spark UI + arquivo .py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## FORMATO OBRIGATÓRIO DE RESPOSTA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Retorne EXCLUSIVAMENTE um objeto JSON válido, sem texto antes ou depois,
sem markdown fences (``` ```), sem comentários fora do JSON.

O JSON deve seguir EXATAMENTE este schema:

{
  "meta": {
    "mode": "A" | "B",
    "job_file": "<nome do arquivo .py, ou null se MODO A>",
    "log_file": "<nome do arquivo zip/log>",
    "analyzed_at": "<ISO 8601 timestamp aproximado>"
  },

  "summary": {
    "score": <0-100 inteiro. Penalize: -15 por cada critico, -8 por alto, -4 por medio>,
    "verdict": "<Uma frase direta>",
    "estimated_gain_min": <numero - duracao estimada pos-otimizacoes, em minutos>,
    "kpis": {
      "duration_total_min": <numero>,
      "input_volume_gb": <numero>,
      "total_tasks": <numero>,
      "avg_data_per_task_kb": <numero>,
      "avg_data_per_task_critical": <true se < 1024>,
      "stages_with_skew": <numero>,
      "disk_spill_total_gb": <numero>,
      "memory_spill_total_gb": <numero>,
      "shuffle_write_total_gb": <numero>,
      "stages_with_failure_or_retry": <numero>
    }
  },

  "stages": [
    {
      "id": <numero inteiro>,
      "duration_s": <numero>,
      "task_count": <numero>,
      "skew_ratio": <numero - max_task_duration / median_task_duration>,
      "shuffle_write_gb": <numero ou null>,
      "shuffle_write_time_s": <numero ou null>,
      "disk_spill_gb": <numero ou null>,
      "memory_spill_gb": <numero ou null>,
      "fetch_wait_ms": <numero ou null>,
      "status": "ok" | "warning" | "critical",
      "bottleneck_ids": [<lista de IDs de gargalos que afetam este stage, ex: ["B1","B3"]>]
    }
  ],

  "bottlenecks": [
    {
      "id": "B1",
      "severity": "critical" | "high" | "medium",
      "type": "skew" | "spill" | "shuffle" | "planning_overhead" | "driver_collect" | "udf" | "other",
      "title": "<Nome curto do gargalo>",
      "stages_affected": [<lista de IDs de stage, ex: [6, 7]>],
      "operator": "<ex: sortMergeJoin, exchange, scan, withColumn - ou null>",
      "duration_observed_s": <numero>,
      "duration_expected_s": <numero - estimativa para este volume sem o gargalo>,
      "evidence": "<Metrica exata do log>",
      "root_cause": "<Explicacao tecnica direta>",
      "observed_effect": "<O que isso causou no job como um todo>",

      "code_link": {
        "line_start": <numero ou null - null se MODO A>,
        "line_end": <numero ou null>,
        "function_name": "<nome da funcao ou null>",
        "snippet": "<trecho de codigo exato como string, ou null>",
        "explanation": "<por que esta linha causou este stage - link direto entre codigo e log, ou null>"
      }
    }
  ],

  "action_plan": {
    "cluster_configs": [
      {
        "bottleneck_id": "B1",
        "name": "<Nome da configuracao>",
        "rationale": "<Liga diretamente ao gargalo>",
        "estimated_impact": "<Ex: Reduz tasks de 29k para ~64, eliminando ~90% do overhead>",
        "code": "<configuracao como string Python valida, ex: spark.conf.set(\"spark.sql.adaptive.enabled\", \"true\")>"
      }
    ],

    "code_fixes": [
      {
        "bottleneck_id": "B1",
        "title": "<Ex: Substituir loop de withColumn por select vetorizado>",
        "line_start": <numero ou null>,
        "line_end": <numero ou null>,
        "function_name": "<nome da funcao ou null>",
        "problem_explanation": "<Por que o codigo atual e problematico>",
        "before_code": "<codigo problematico como string>",
        "after_code": "<codigo corrigido como string>",
        "expected_gain": "<Ex: Stage 12 deve cair de 4min para ~20s>",
      }
    ]
  },

  "limitations": "<Texto de limitacoes conforme MODO A ou B>"
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## REGRAS INVIOLÁVEIS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SOBRE DIAGNÓSTICO:
- NUNCA diga "pode haver skew" — diga "há skew no Stage X, tasks Y e Z processaram
  N vezes mais dados, evidenciado por shuffle read de X GB vs média de Y MB"
- NUNCA diga "pode haver spill" — diga "Stage X despejou X.X GB em disco
  (Disk Bytes Spilled: X), causando overhead de I/O evidenciado pela duração do stage X"
- NUNCA omita spill do relatório — se disk_bytes_spilled > 0 em qualquer stage,
  isso DEVE aparecer como gargalo independentemente da magnitude
- NUNCA diagnostique shuffle como gargalo isolado sem relacionar os bytes de
  shuffle read/write à duração do stage, skew de tasks ou razão de amplificação shuffle/input
- NUNCA cite shuffle write sem informar também o shuffle write time —
  volume alto com tempo baixo é diferente de volume alto com tempo alto
- NUNCA cite parâmetros de configuração sem basear nos valores reais do log
- NUNCA sugira cache(), broadcast() ou repartition() sem apontar o stage específico
  que seria beneficiado e a métrica que sustenta isso
- NUNCA aponte um problema de código (Modo B) sem correlacionar com o log

SOBRE O JSON:
- Retorne APENAS o JSON. Zero texto fora dele.
- Todos os campos numericos devem ser numeros, nunca strings com unidade
- Strings de codigo (snippet, before_code, after_code, code) devem conter
  codigo valido, sem comentarios narrativos, sem "ANTES/DEPOIS", sem "---"
- bottleneck_ids nos stages devem corresponder exatamente aos ids em bottlenecks[]
- code_link deve ser preenchido em MODO B e ter todos os campos null em MODO A
- code_fixes deve ser array vazio [] em MODO A
""".strip()
}


class LLMAnalyzer:
    """
    Orchestrates prompt construction and calls the LLM adapter.
    Dependency-injected adapter makes this fully testable with mocks.
    """

    def __init__(self, adapter: Optional[BaseLLMAdapter] = None):
        self._adapter = adapter  # injected; resolved lazily if None

    def _get_adapter(
        self,
        provider: Optional[str],
        api_key: Optional[str],
    ) -> BaseLLMAdapter:
        if self._adapter:
            return self._adapter
        return LLMClientFactory.get(provider=provider, api_key=api_key)

    def analyze(
        self,
        reduced_report: str,
        summary: AppSummary,
        py_files: Optional[dict[str, bytes]] = None,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
        language: str = "en",
    ) -> str:
      adapter = self._get_adapter(provider, api_key)

      instr = _SYSTEM_INSTRUCTIONS.get(language, _SYSTEM_INSTRUCTIONS["en"])

      # Auto-detect operation mode based on presence of Python files.
      py_files_provided = bool(py_files and len(py_files) > 0)
      mode_indicator = (
        "**[OPERATION MODE ACTIVATED: MODE B - Log + Python Code]**"
        if py_files_provided
        else "**[OPERATION MODE ACTIVATED: MODE A - Log Only]**"
      )

      report_for_prompt = _collapse_repetitive_lines(reduced_report)

      prompt_parts = [
        instr,
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
            # Keep source files intact to preserve line mapping accuracy.
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

      prompt = "\n".join(prompt_parts)
      logger.info(
        "Calling LLM (%s) for analysis... [Mode: %s, report_chars=%s, prompt_chars=%s]",
        adapter.__class__.__name__,
        "B" if py_files_provided else "A",
        len(report_for_prompt),
        len(prompt),
      )

      result = adapter.complete(prompt)
      if py_files_provided:
        result = _reconcile_code_links(result, py_files or {})
      return result
