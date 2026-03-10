# 🚀 Upgrade do Prompt — Sistema Condicional Inteligente

## Resumo das Mudanças

O prompt do `LLMAnalyzer` foi completamente reformulado para **detectar automaticamente o contexto** e ativar **dois modos de operação distintos**:

### MODO A — Somente Log da Spark UI
- ✅ Análise precisa do log, sem especulações
- ✅ Diagnóstico por stage/task com evidências diretas
- ✅ Plano de ação focado em configuração de cluster e máquina
- ⚠️ Avisos de limitação (código-fonte não disponível)

### MODO B — Log + Arquivo Python
- ✅ Análise **integrada**: comportamento do log + código-fonte
- ✅ Rastreamento linha-a-linha dos problemas
- ✅ Diff antes/depois com correções específicas
- ✅ Priorização de resolver código vs configuração

---

## Arquitetura da Detecção Automática

### Fluxo de Dados

```
Upload (Log + opcionalmente .py)
    ↓
Job Service (job_service.py)
    ↓
LLM Analyzer (llm_analyzer.py)
    ├─ Detecta: py_files está preenchido?
    ├─ SIM → MODO B (adiciona instrução explícita)
    └─ NÃO → MODO A (análise log-only)
    ↓
LLM Backend (OpenAI/Claude)
    ↓
Resposta Estruturada Conforme Modo
```

### Implementação

No arquivo `backend/services/llm_analyzer.py`:

```python
# Auto-detect operation mode based on presence of Python files
py_files_provided = bool(py_files and len(py_files) > 0)
mode_indicator = (
    "**[OPERATION MODE ACTIVATED: MODE B — Log + Python Code]**"
    if py_files_provided
    else "**[OPERATION MODE ACTIVATED: MODE A — Log Only]**"
)
```

A LLM recebe uma instrução explícita no topo do prompt indicando qual modo ativar, garantindo comportamento consistente.

---

## Regras Invioláveis Implementadas

1. **NUNCA sugerir cache() sem apontar impacto no log**
2. **NUNCA citar parâmetros sem basear em valores reais**
3. **NUNCA dizer "pode haver skew" — dizer com números**
4. **No MODO B, NUNCA correlacionar problema de código sem evidência do log**

---

## Estrutura de Resposta Esperada

### MODO A (Log Only)

```
## Resumo Executivo
- Duração do job
- Número de stages
- Falhas/retentativas

## Diagnóstico por Stage/Task
[Stage X — Operador — Tempo]
  Causa no log: ...
  Evidência: ...

## Gargalos Identificados
- CRÍTICO: ...
- ALTO: ...

## Plano de Ação
- Parâmetros spark.conf
- Ajustes de hardware

⚠️ Aviso de Limitação
```

### MODO B (Log + Python)

```
## Resumo Executivo
[mesmo que MODO A]

## Diagnóstico por Stage/Task
[Stage X — Operador — Tempo]
  Causa no log: ...
  Origem no código: linha 47 — df.join(...)
  Correlação: ...

## Análise Linha a Linha
Linha 47: df.join(df_large, ...)
  O que faz: ...
  Por que é problemático: [evidência do log]
  Versão corrigida: ...

## Gargalos Identificados
[mesmo que MODO A]

## Plano de Ação Completo
- Correções de código (com diff)
- Ajustes de configuração
- Priorização
```

---

## Como as Mudanças Afetam o Fluxo

### Antes (Genérico)
```
Log Reduzido → LLM → "Você pode tentar cache()"
```

### Depois (Inteligente)
```
Log Reduzido → Detecção de Modo → LLM recebe instrução explícita
   ↓
   Caso A: "Dê diagnóstico PRECISO do log"
   Caso B: "Corrija código E configuração, correlacionando com log"
```

---

## Exemplos de Uso

### Cenário 1: Usuário envia apenas LOG

```bash
curl -X POST http://localhost:8000/api/upload \
  -F "log_zip=@eventos.zip" \
  -F "provider=openai" \
  -F "api_key=sk-..."
```

**Resultado:** Prompt ativa **MODO A**
- Diagnóstico detalhado do log
- Aviso final: "Padrões de código não podem ser avaliados sem .py"

### Cenário 2: Usuário envia LOG + CÓDIGO

```bash
curl -X POST http://localhost:8000/api/upload \
  -F "log_zip=@eventos.zip" \
  -F "pyspark_files=@job.py" \
  -F "provider=openai" \
  -F "api_key=sk-..."
```

**Resultado:** Prompt ativa **MODO B**
- Rastreamento linha-a-linha do código
- Sugestões de correção com diff
- Priorização integrada

---

## Instruções para Manutenção

### Adicionar Novas Regras ao Prompt

Edite `backend/services/llm_analyzer.py` na seção `_SYSTEM_INSTRUCTIONS`:

```python
_SYSTEM_INSTRUCTIONS = {
    "pt": """
    ... (instruções existentes) ...
    
    ## NOVA REGRA
    
    - NUNCA fazer X sem fazer Y
    """.strip()
}
```

### Testar Novo Prompt

1. **Teste com log somente** (Modo A)
   - Sintaxe: Shell script ou curl
   - Validar: Resposta segue estrutura MODO A

2. **Teste com log + código** (Modo B)
   - Sintaxe: Shell script ou curl
   - Validar: Resposta correlaciona código com log

3. **Edge cases**:
   - Código vazio
   - Múltiplos arquivos .py
   - Arquivo .py muito grande (truncado em 2KB no prompt)

---

## Compatibilidade

- ✅ Suporta múltiplas linguagens (en, pt, ...)
- ✅ Compatível com OpenAI (GPT-4o) e Claude
- ✅ Backward compatible (logs antigos continuam funcionando)
- ✅ Não quebra integração com OAuth/BYOK

---

## Próximos Passos (Opcional)

1. **Persistência de Modo Detectado**
   - Salvar qual modo foi ativado no JobResult
   - Útil para analytics e auditoria

2. **Refinamento Dinâmico**
   - Ajustar comprimento de contexto por modo
   - MODO A: mais espaço para diagnóstico
   - MODO B: mais espaço para código

3. **Versioning de Prompts**
   - Permitir múltiplas versões de prompt
   - A/B testing entre versões
