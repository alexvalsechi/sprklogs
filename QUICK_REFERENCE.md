# 🚀 QUICK REFERENCE — Sistema Condicional de Prompt

## TL;DR

O prompt agora **detecta automaticamente o tipo de input** (log vs log+código) e ativa dois modos:

```
Upload: LOG APENAS    → MODO A: Análise precisa de configuração
Upload: LOG + CÓDIGO  → MODO B: Análise integrada + correções
```

---

## Arquivos Principais

| Arquivo | Função | Mudança |
|---------|--------|--------|
| `llm_analyzer.py` | Sistema de prompts | ⭐ Novo prompt aprimorado |
| `routes.py` | API upload | ✅ Já suporta `pyspark_files` |
| `job_service.py` | Orquestração | ✅ Passa `py_files` corretamente |
| `tasks.py` | Celery task | ✅ Já implementado |

---

## Como Usar

### MODO A (Log Apenas)
```bash
curl -X POST http://localhost:8000/api/upload \
  -F "log_zip=@spark_log.zip" \
  -F "provider=openai" \
  -F "api_key=sk-..."
```

**Retorna:**
- ✅ Análise precisa do log
- ✅ Diagnóstico por stage/task  
- ✅ Plano de ação (cluster config)
- ⚠️ Aviso: código não pode ser analisado

### MODO B (Log + Python)
```bash
curl -X POST http://localhost:8000/api/upload \
  -F "log_zip=@spark_log.zip" \
  -F "pyspark_files=@job.py" \
  -F "pyspark_files=@config.py" \
  -F "provider=openai" \
  -F "api_key=sk-..."
```

**Retorna:**
- ✅ Análise integrada (log + código)
- ✅ Rastreamento linha-a-linha
- ✅ Diffs com correções
- ✅ Priorização: código vs configuração

---

## Estrutura de Resposta

### MODO A
```
1. Resumo Executivo          ({texto})
2. Diagnóstico por Stage/Task ({etapa: tempo, causa, evidência})
3. Gargalos Identificados    (CRÍTICO/ALTO/MÉDIO: {métrica→causa→efeito})
4. Plano de Ação             ({parâmetros}{hardware}{estratégia})
5. ⚠️ Aviso de Limitação      ({boilerplate})
```

### MODO B
```
1. Resumo Executivo          ({texto})
2. Diagnóstico + Código      ({etapa}→{linha exata do .py}→{correlação log-código})
3. Análise Linha-a-Linha     ({linha, o que faz, por que é ruim, versão corrigida})
4. Gargalos Identificados    (idem MODO A)
5. Plano de Ação Completo    ({diffs antes/depois, priorização})
```

---

## Exemplos Rápidos

### MODO A — Resposta Esperada
```
## Resumo Executivo
- Duração: 18min 32s
- Stages: 6
- Falhas: 0

## Diagnóstico por Stage/Task
[Stage 3 — Shuffle — 5min 20s]
Causa: shuffle write 18GB, 10x desequilíbrio de dados
Evidência: Task 5 processou 8.5GB vs avg 1.2GB
→ Data skew em 'city_id' (40% dos dados em 3 valores)

## Gargalos
🔴 CRÍTICO: Data Skew Stage 3 → -15% duração total
🟠 ALTO: GC Pressure Stage 2 → -3.5% duração

## Plano de Ação
spark.sql.shuffle.partitions = 320  (aumentar de 200)
spark.executor.memory = 28g  (aumentar de 16g)
→ Esperado: 18:32 → 10:00 (-46%)

⚠️ Código-fonte não pode ser avaliado sem .py
```

### MODO B — Resposta Esperada
```
## Diagnóstico por Stage/Task
[Stage 3 — Shuffle — 5min 20s]
Causa: shuffle write 18GB, desequilíbrio
Origem no código: linha 48 em etl_job.py
  df.groupBy('city_id', 'product_category').agg(...)
→ GroupBy sem reparticionamento prévio

## Análise Linha-a-Linha
Linha 48: df.groupBy('city_id').agg(...)
O que faz: agrupa sem reparticionar antes
Por que é ruim: 'city_id' tem 40% skew (evidência do log)
Versão corrigida:
  df.repartition(320, 'city_id').groupBy(...).agg(...)

## Plano de Ação
Fase 1: Adicionar repartition (linha 48)
  → Stage 3: 320s → 160s (-50%)
Fase 2: Aumentar memory + ativar AQE
  → Stage 2: 225s → 130s (-42%)
Total: 18:32 → 10:00 (-46%)
```

---

## Regras Invioláveis

```
❌ NÃO fazer:                         ✅ FAZER:
---------------------------------------  ----------------
"Considere usar cache()"               "cache() economiza 45s em Stage 3"
"Pode haver skew"                      "Há skew: 8.5GB vs 1.2GB (10x)"
"Usar 28GB memory"                     "28GB memory (espill de 8GB observado)"
"Correlacionar sem evidência"          "linha 48 causa Stage 3 skew (log mostra...)"
```

---

## Detecção Automática (Internals)

```python
# No llm_analyzer.py, método analyze()

py_files_provided = bool(py_files and len(py_files) > 0)
mode = "B" if py_files_provided else "A"

# Injeta instrução no prompt:
prompt_start = f"[OPERATION MODE ACTIVATED: MODE {mode}]"
```

**Quando MODO A:**
- `py_files` é `{}` ou `None`
- LLM recebe só o log reduzido
- Resposta segue estrutura MODO A

**Quando MODO B:**
- `py_files` tem ≥1 arquivo
- LLM recebe log + código (truncado em 2KB por arquivo)
- Resposta segue estrutura MODO B

---

## FAQ Rápido

**P: Meu upload tem log + código, mas resposta é MODO A?**  
R: Arquivo `.py` não foi lido corretamente. Verifique:
- Formato: `.py` (não `.ipynb` ou `.txt`)
- Tamanho: <500KB total
- Encoding: UTF-8

**P: Posso misturar múltiplos arquivos `.py`?**  
R: ✅ Sim! Passar múltiplos `pyspark_files` na mesma requisição.

**P: A resposta MODO B é sempre melhor?**  
R: ✅ Sim, para diagnóstico de código. Mas MODO A ainda é útil se só log disponível.

**P: Qual é o prompt exato enviado ao LLM?**  
R: Veja em `backend/services/llm_analyzer.py`, linhas 10–220.

**P: Pode fazer A/B testing entre prompts?**  
R: Sim, criar nova função `_SYSTEM_INSTRUCTIONS_V2` e usar com flag.

---

## Documentação Completa

| Doc | Propósito |
|-----|-----------|
| [PROMPT_UPGRADE.md](PROMPT_UPGRADE.md) | Arquitetura + detalhes técnicos |
| [UPGRADE_SUMMARY.md](UPGRADE_SUMMARY.md) | Resumo executivo |
| [RESPONSE_EXAMPLES.md](RESPONSE_EXAMPLES.md) | Exemplos reais de respostas |
| [CHECKLIST.md](CHECKLIST.md) | Validação de implementação |
| Este arquivo | Quick reference |

---

## Status

✅ Implementado e testado  
✅ Backward compatible  
✅ Pronto para produção  
✅ Multilingue (PT + EN)  

---

**v2.1.0 — March 9, 2026**
