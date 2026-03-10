# 📝 Exemplos de Respostas Esperadas

## MODO A — Exemplo de Resposta (Log Apenas)

```markdown
# Análise de Log Spark — Job de ETL de Vendas

## Resumo Executivo
- **Duração Total:** 18 minutos 32 segundos
- **Stages:** 6
- **Total de Tasks:** 587
- **Tasks com Falha:** 0 (reexecuções: 23)
- **Status:** Completado com aviso de performance

## Diagnóstico por Stage/Task

### [Stage 1 — Scan External (Parquet) — 2min 15s]
**Tempo esperado:** ~90-120s  
**Tempo real:** 135s (+12% acima do esperado)

**Causa raiz no log:**
- Leitura de 342 partições Parquet (12GB total)
- Task 0: 98s (normal)
- Task 5: 134s (desequilíbrio: 37% mais lento)
- Shuffle read: 0B (esperado, é scan puro)

**Evidência direta:**
```
executor 1:   [Partition 0] 45s
executor 2:   [Partition 5] 98s  ← Tardio demais
executor 3:   [Partition 10] 52s
Locality: RACK_LOCAL (75%), NODE_LOCAL (20%), ANY (5%)
```

**Diagnóstico:** Partição 5 tem ~40% mais registros que média  
**Impacto:** Atrasa leitura em 45s, cascateia para Stage 2

---

### [Stage 2 — Filter + Map — 3min 45s]
**Tempo esperado:** ~2min (base: 12GB / 100MB/s = 120s)  
**Tempo real:** 225s (+87% acima!)

**Causa raiz no log:**
- Garbage Collection excessiva
  - Young GC: 12 ocorrências, 2.3s cada = 27.6s lost
  - Full GC: 2 ocorrências, 8.5s = 17s lost
  - Total GC: 44.6s = 19.8% do tempo do stage

**Evidência direta:**
```
GC_Pressure:
  GC_TIME_MILLIS: 44600ms
  GC_COUNT: 14
  Max Heap Used: 28.4GB (limite: 30GB)
  Max Heap Free: 1.6GB
  
Memory allocation rate: 850 MB/s
```

**Diagnóstico:** Heap pressure extrema durante transformações  
**Impacto:** Cada GC pausa toda a execução

---

### [Stage 3 — Shuffle (GroupBy) — 5min 20s]
**Tempo esperado:** ~3min 30s  
**Tempo real:** 320s (+53% acima!)

**Causa raiz no log:**
- **Data Skew severo no GroupBy**
  - Total shuffle write: 18GB
  - Task 0: 1.2GB (shuffle write 120ms, tempo total 12s)
  - Task 5: 8.5GB (shuffle write 2.1s, tempo total 125s) ← 10.4x mais lento
  - Task 15: 0.3GB (tempo total 2s)
  - "city_id" tem distribuição: 40% dos dados em 3 valores

**Evidência direta:**
```
Shuffle metrics:
  Total shuffle write: 18.2GB
  By task:
    Task 0: 1200MB, write_time: 120ms, read_time: 340ms
    Task 5: 8500MB, write_time: 2100ms, read_time: 8900ms ← Gargalo
    Task 15: 300MB, write_time: 45ms, read_time: 210ms

Peak shuffle memory: 7.2GB (executor memory 16GB)
Spill to disk: 2.1GB (20% do shuffle escreveu em disco!)
```

**Diagnóstico:** GroupBy em coluna skewed + partition count default (200)  
→ 3 partições concentram 70% dos dados

**Impacto:** Task 5 processa 10x mais dados, gargalo crítico

---

## Gargalos Identificados (Ordenados por Impacto)

### **🔴 CRÍTICO — Data Skew no Stage 3**
- **Métrica:** Shuffle write de 8.5GB em 1 task vs 1.2GB em outra
- **Causa:** GroupBy sem reparticionamento prévio em coluna skewed
- **Efeito:** Atraso de 113s cascateia para Stage 4
- **Impacto total na duração:** ~15% (+ 2min 45s)

### **🟠 ALTO — GC Pressure no Stage 2**
- **Métrica:** 19.8% do tempo perdido em GC (44.6s em 225s)
- **Causa:** Executor memory insuficiente (16GB) para volume de dados
- **Efeito:** Pausa de execução repetida
- **Impacto total na duração:** ~3.5% (+ 38s)

### **🟠 ALTO — Desequilíbrio de Partições no Stage 1**
- **Métrica:** Partição 5 tem +40% de registros vs média
- **Causa:** Dados originais não foram re-particionados ao salvar
- **Efeito:** Task de leitura 45% mais lenta
- **Impacto total na duração:** ~2% (+ 18s)

### **🟡 MÉDIO — Shuffle Read ineficiente no Stage 4**
- **Métrica:** 4.2GB lido, mas valor esperado ~3.8GB (10% overhead)
- **Causa:** Overhead de serialização entre stages
- **Efeito:** +22s para leitura de shuffle
- **Impacto total na duração:** ~1% (+ 12s)

---

## Plano de Ação — Configuração de Cluster e Máquina

### 1. **Resolver Data Skew do GroupBy (Prioridade 1)**

**Configuração Spark:**
```
spark.sql.shuffle.partitions = 320
# Aumentar de 200 para distribuir melhor (diminui 
# dados por partição: 56.9MB → 56.9MB, mas cores 
# aumentadas disponíveis = menos concentração)

spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
# AQE vai detectar skew e repartição dinamicamente
```

**Ajuste de Hardware:**
- Aumentar `spark.executor.cores` de 4 para 8 por executor
- Resultado: paralelismo dentro da task prejudicada por skew
- Esperado: Stage 3 de 320s → 160s (-50%)

---

### 2. **Resolver GC Pressure no Stage 2 (Prioridade 2)**

**Configuração Spark:**
```
spark.executor.memory = 28g
# Aumentar de 16GB para 28GB (deixar 2GB para SO)

spark.executor.memoryOverhead = 3g
# Off-heap para shuffle: 3GB

spark.sql.inMemoryColumnarStorage.batchSize = 8192
# Reduzir batch size → menos alocação simultânea

spark.memory.storageFraction = 0.4
# Dar menos espaço para cache, mais para execução
```

**Ajuste de Hardware:**
- Aumentar RAM por executor de 16GB para 32GB
- Esperado: Stage 2 de 225s → 130s (-42%)

---

### 3. **Reparticionamento Preventivo (Prioridade 3)**

**Configuração Spark:**
```
spark.sql.files.maxRecordsPerFile = 5000000
# Limitar registros por arquivo ao salvar
# Garante distribuição mais uniforme de futuros leitores

spark.sql.hive.verifyPartitionPath = true
spark.sql.hive.dynamicPartitionPruning = true
# Melhor pruning de partições
```

**Impacto esperado para próximas execuções:**
- Stage 1: 135s → 110s (-19%)
- Acumula benefício com skew fix do Stage 3

---

### 4. **Otimizar Shuffle Read (Prioridade 4)**

**Configuração Spark:**
```
spark.reducer.maxSizeInFlight = 100m
spark.shuffle.compress = true
spark.shuffle.spill.compress = true
# Comprimir shuffle → menos dados em rede/disco
```

**Impacto esperado:**
- Stage 4: ~22s → 18s (-18%)

---

## Resumo de Configuração Recomendada

```properties
# Cluster Configuration
spark.executor.instances = 8           # (aumentar de 4)
spark.executor.memory = 28g            # (aumentar de 16g)
spark.executor.cores = 8               # (aumentar de 4)
spark.executor.memoryOverhead = 3g     # (novo)

# SQL Optimization
spark.sql.shuffle.partitions = 320     # (aumentar de 200)
spark.sql.adaptive.enabled = true      # (novo)
spark.sql.adaptive.skewJoin.enabled = true  # (novo)

# Memory & GC
spark.memory.storageFraction = 0.4     # (reduzir de 0.6)
spark.sql.inMemoryColumnarStorage.batchSize = 8192

# Shuffle
spark.reducer.maxSizeInFlight = 100m   # (novo)
spark.shuffle.compress = true          # (novo)
```

**Impacto esperado total:**
- Tempo do job: 18:32 → ~10:00 (-46%)
- GC pressure: reduz > 80%
- Spill to disk: elimina

---

## ⚠️ Aviso de Limitação de Análise

Esta análise cobre **configuração de cluster e comportamento de execução** baseado exclusivamente no log de eventos Spark.

**Não podem ser avaliados sem o arquivo .py correspondente:**
- ❌ Lógica de transformações (e.g., se o Filter no Stage 2 é eficiente)
- ❌ Uso de UDFs (muito mais lentos que funções nativas Spark SQL)
- ❌ collect() ou ações desnecessárias
- ❌ Repartiçõesmal planejadas no código
- ❌ Ineficiência de JOINs (ordem, hints, broadcast)

**Para uma análise completa, forneça o arquivo job.py ou etl.py**

---

---

## MODO B — Exemplo de Resposta (Log + Código)

```markdown
# Análise Integrada: Spark Log + Código PySpark

## Resumo Executivo
- **Duração Total:** 18 minutos 32 segundos
- **Stages:** 6
- **Taxa de Falha:** 0/587 (23 reexecuções)
- **Gargalo Principal:** Data skew no Stage 3 (correlacionado com código)

---

## Diagnóstico por Stage/Task (Com Rastreamento ao Código)

### [Stage 3 — Shuffle (GroupBy) — 5min 20s]

**Causa no log:**
- Shuffle write de 18.2GB distribuído de forma skewed
- Task 5 concentra 8.5GB (10.4x mais que Task 0)
- Spill to disk: 2.1GB

**Origem no código:** 📍 Arquivo `etl_job.py`, linha 47–52

```python
# ❌ PROBLEMA
result = df \
    .groupBy('city_id', 'product_category') \  # Linha 48: GroupBy
    .agg(
        F.sum('amount').alias('total_sales'),
        F.count('*').alias('transaction_count')
    ) \
    .write.mode('overwrite').parquet(output_path)
```

**Correlação:**
1. Dados brutos têm distribuição skewed em 'city_id' (40% em 3 cidades)
2. Código faz GroupBy direto sem reparticionamento
3. 200 partições (default) recebem volumes despropor cionais
4. Spark aloca uma task por partição → Task 5 fica com 8.5GB

**Taxa de impacto:** -53% na duração esperada (+320s)

---

## Análise Linha a Linha dos Trechos Problemáticos

### **Problema 1: GroupBy sem Reparticionamento (Linha 48)**

**Arquivo:** `etl_job.py`  
**Linha:** 48  
**Código:**
```python
result = df \
    .groupBy('city_id', 'product_category') \
    .agg(...)
```

**O que faz:**
- Agrupa todos os registros por combinação de `city_id` + `product_category`
- Usa 200 partições default do cluster
- Shuffle escreve agregações no disco local de cada executor

**Por que é problemático:**
- Log evidencia que `city_id` tem 40% dos dados concentrado em 3 valores
  - Exemplo: 'city_id'=1001 = 4M registros / 10M total = 40%
  - 'city_id'=1002 = 2.5M registros
  - 'city_id'=1003 = 2.1M registros
  - Restante (200+ cidades) = 1.4M registros

Esta distribuição é mapeada para 200 partições:
  - Partição 5 (hash('city_id'=1001) % 200 = 5) → 4M + repartições cruzadas
  - Partição 80 (hash('city_id'=1002) % 200) → 2.5M
  - Partição 140 (hash('city_id'=1003) % 200) → 2.1M
  - Partições 0–199 restantes → ~1.4M dividido (7KB cada)

**Resultado no log:**
```
Partition 5:
  Shuffle write: 8.5GB (dados + metadados)
  Duration: 125s
  Peak memory: 6.2GB
  GC collections: 8 (total 15s lost)

Partition 80:
  Shuffle write: 4.2GB
  Duration: 68s

Partition 140:
  Shuffle write: 2.1GB
  Duration: 35s

Partition 0–199 (excluding 5,80,140):
  Shuffle write: 3.4GB (distributed)
  Duration: 2–12s each
```

**Impacto:** A task mais lenta (Partição 5) atrasa TODAS as demais  
→ Toda a Stage 3 espera 125s quando deveria ser ~65s

---

### **Problema 2: Sem Broadcast Hint para DF Menor (Linha 45–52)**

**Arquivo:** `etl_job.py`  
**Linha:** 45–52

**Análise:**
```python
# Anterior (não mostrado no snippet, mas análise)
df = spark.read.parquet(input_path)  # 10M registros, 12GB

# Depois do GroupBy, resultado é menor:
result = df.groupBy('city_id', 'product_category').agg(...)
# Resultado: ~15K registros (cidades × categorias)

# Se houver STAGE 4 (JOIN com resultado):
# ❌ Sem broadcast de result
final = df.join(result, on='city_id', how='inner')
# → Shuffle de 15K registros + 12GB = ineficiente
```

**Evidência do log:**
- Se Stage 4 existe, mostrará shuffle read/write desnecessário
- Broadcast evitaria mover este pequeno resultado

---

### **Problema 3: Sem Reparticionamento Preventivo (Linha 42–45)**

**Arquivo:** `etl_job.py`  
**Linha:** 42–45 (não mostrado, mas inferível)

**Análise:**
```python
df = spark.read.parquet(input_path)
# ❌ Direto para GroupBy, sem reparticionamento

# ✅ Versão corrigida:
df = spark.read.parquet(input_path) \
    .repartition(320, 'city_id') \  # Aumentar partições ANTES de GroupBy
    .groupBy('city_id', 'product_category').agg(...)
```

**Por que esta fix é melhor:**
- Repartitioning 'city_id' com 320 partições força redistribuição
- Cada partição recebe ~31K registros base em vez de concentração
- GroupBy depois trabalha sobre distribuição já uniforme
- Reduz shuffle write de 18.2GB → ~12.1GB (34% less)

**Evidência de impacto esperado:**
- Stage 3 duration: 320s → 180s (-44%)

---

## Gargalos Identificados (Com Priorização)

| Prioridade | Gargalo | Código | Log | Impacto |
|------------|---------|--------|-----|---------|
| 1️⃣ CRÍTICO | Data skew GroupBy | Linha 48 | Stage 3: 8.5GB em 1 task | -53% duração |
| 2️⃣ ALTO | Sem reparticionamento prévio | Implícito linha 42 | 18.2GB shuffle write | -34% shuffle |
| 3️⃣ MÉDIO | Sem broadcast hint | Pouco evidente (Stage 4?) | Shuffle overhead 10% | -1% duração |
| 4️⃣ MÉDIO | GC pressure | Linha 48 (volume) | Stage 2: 19.8% lost | -3.5% duração |

---

## Plano de Ação Completo

### **FASE 1: FIX CRÍTICO** (Implementar primeira)

**Arquivo:** `etl_job.py` — Linhas 47–52

**Antes:**
```python
result = df \
    .groupBy('city_id', 'product_category') \
    .agg(
        F.sum('amount').alias('total_sales'),
        F.count('*').alias('transaction_count')
    ) \
    .write.mode('overwrite').parquet(output_path)
```

**Depois:**
```python
result = df \
    .repartition(320, 'city_id') \  # ← Nova linha: Repartition antes de GroupBy
    .groupBy('city_id', 'product_category') \
    .agg(
        F.sum('amount').alias('total_sales'),
        F.count('*').alias('transaction_count')
    ) \
    .write.mode('overwrite').parquet(output_path)
```

**Diff:**
```diff
  result = df \
+     .repartition(320, 'city_id') \
      .groupBy('city_id', 'product_category') \
      .agg(
          F.sum('amount').alias('total_sales'),
          F.count('*').alias('transaction_count')
      ) \
      .write.mode('overwrite').parquet(output_path)
```

**Justificativa:**
- Aumenta partições de 200 → 320 (melhor fitting com cores disponíveis)
- Força `repartition()` antes de GroupBy para redistribuir dados uniformemente
- Reduz concentração de dados por partição
- **Impacto esperado:** Stage 3: 320s → 160s (-50%)

**Comandos de teste:**
```bash
# Antes do fix
spark-submit --conf spark.executor.memory=16g etl_job.py
# Esperado: ~18min 30s

# Depois do fix
spark-submit --conf spark.executor.memory=16g etl_job.py  
# Esperado: ~14min 00s
```

---

### **FASE 2: CONFIG CLUSTER** (Paralelizar com FASE 1)

**Arquivo:** `spark-submit.sh` ou `spark-defaults.conf`

```properties
# Aumentar executor memory (correlacionado com GC pressure no log)
spark.executor.memory=28g             # ← Aumentar de 16g
spark.executor.memoryOverhead=3g      # ← Novo

# Aumentar shuffle partitions (já feito em código, mas garantir globalmente)
spark.sql.shuffle.partitions=320      # ← Aumentar de 200

# Ativar Adaptive Query Execution (ajusta dinamicamente)
spark.sql.adaptive.enabled=true       # ← Novo
spark.sql.adaptive.skewJoin.enabled=true  # ← Novo
```

**Comandos:**
```bash
spark-submit \
  --executor-memory 28g \
  --conf spark.sql.shuffle.partitions=320 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  etl_job.py
```

**Impacto esperado:**
- Stage 2 (GC): 225s → 130s (-42%)
- Stage 3 (Skew) + FASE 1: 160s → 95s (-40% adicional)
- **Total:** 18:32 → 9:45 (-47%)

---

### **FASE 3: REFACTOR ESTRUTURAL** (Implementar depois)

Se análise posterior mostrar joins problemáticos, adicionar:

**Arquivo:** `etl_job.py` — Nova seção antes de GroupBy

```python
# 1. Cache resultado de GroupBy se reutilizado (não foi visto no log, mas prudência)
result = df.repartition(320, 'city_id') \
    .groupBy('city_id', 'product_category') \
    .agg(...) \
    .cache()  # ← Se reutilizado depois: reduz recompute

# 2. Se houver Stage 4 (join com result), usar broadcast
if result.count() < 100_000:  # ~50MB
    final = df.join(
        F.broadcast(result),
        on='city_id',
        how='inner'
    )
```

---

## Priorização: O que Resolver Primeiro e Por Quê

```
🔴 URGENTE: Linha 48 repartition
   Razão: -50% duração com 1 linha
   Tempo de implementação: 5 minutos
   
🟠 IMPORTANTE: Configuração cluster (26g memory, AQE)
   Razão: -42% GC pressure
   Tempo: 10 minutos
   
🟡 OPCIONAL: Broadcast hints (se Stage 4 existe)
   Razão: Melhora marginal, -1% duração
   Tempo: 15 minutos
```

---

## Resumo de Impacto (Antes vs. Depois)

| Métrica | Antes | Depois | Melhoria |
|---------|-------|--------|----------|
| Duração total | 18:32 | 9:45 | -47% |
| Stage 3 | 5:20 | 1:35 | -70% |
| Shuffle write | 18.2GB | 12.1GB | -34% |
| GC pressure | 19.8% | 4.2% | -78% |
| Spill to disk | 2.1GB | 180MB | -91% |

---

fim da análise
```

---

## Comparação: Modo A vs B

| Elemento | Modo A | Modo B |
|----------|--------|--------|
| Profundidade log | ✅✅✅ | ✅✅✅ |
| Rastreamento código | ❌ | ✅✅✅ |
| Linha-a-linha | ❌ | ✅✅✅ |
| Diffs/correções | ❌ | ✅✅ |
| Actionabilidade | ✅✅ | ✅✅✅ |
| Tempo de implementação | ~1 semana | ~2 horas |

---

**Estes exemplos representam respostas esperadas com o novo prompt aprimorado.**
