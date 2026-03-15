# 3. Rascunho de Politica de Privacidade + Termos de Uso

## Politica de Privacidade (LGPD) — Rascunho

### 1. Controlador

Este software e operado por [PREENCHER RAZAO SOCIAL], CNPJ [PREENCHER], com sede em [PREENCHER], contato: [PREENCHER EMAIL/TELEFONE/DPO].

### 2. Finalidade do tratamento

Tratamos dados para:

- reduzir e analisar logs de execucao Spark
- gerar diagnostico tecnico de performance
- exibir resultado analitico e relatorios para o proprio usuario

### 3. Dados tratados

Conforme o fluxo do sistema, podem ser tratados:

- arquivo ZIP de event log Spark (dependendo do caminho de execucao habilitado)
- arquivos `.py` opcionais enviados pelo usuario
- parametros tecnicos da analise (idioma, provedor LLM, modo compacto)
- credenciais de integracao (API key enviada manualmente) ou token OAuth
- metadados tecnicos de processamento (status do job, tamanho de payload, logs operacionais)

### 4. Processamento local e minimizacao

O codigo do desktop contem funcionalidade de reducao local do ZIP (`apps/desktop/main/ipc/compress.handler.ts` + `apps/desktop/main/scripts/reduce_log.py`) que permite enviar ao backend apenas relatorio reduzido.

No entanto, o comportamento efetivo depende do fluxo de UI ativo. No estado atual do renderer, ha envio HTTP para endpoint de upload (`/api/upload`). Portanto, a alegacao de "processamento sempre local" nao deve ser feita sem validacao/ajuste de fluxo.

### 5. Compartilhamento com terceiros

Quando ha analise por IA, o sistema envia prompt textual para provedores externos configurados:

- OpenAI
- Anthropic
- Google Gemini

O prompt pode conter trecho do relatorio reduzido e trechos de codigo `.py` (quando fornecidos).

### 6. Armazenamento e retencao

No codigo atual:

- jobs sao mantidos em memoria de processo (`_jobs`)
- arquivos de download sao temporarios e removidos ao final da resposta
- tokens OAuth sao armazenados em Redis com TTL
- historico local de analises fica em `localStorage` no cliente

Nao foi identificado, no codigo, mecanismo completo de politica de retencao com prazo formal para todos os artefatos.

### 7. Bases legais (LGPD)

Bases legais tipicamente aplicaveis, a serem validadas juridicamente:

- execucao de contrato ou procedimentos preliminares
- legitimo interesse para seguranca e melhoria tecnica
- consentimento, quando exigido para integracoes especificas

[PREENCHER COM ENQUADRAMENTO JURIDICO OFICIAL]

### 8. Direitos do titular

Titulares podem solicitar, nos termos da LGPD:

- confirmacao e acesso
- correcao
- anonimização, bloqueio ou eliminacao quando cabivel
- informacao sobre compartilhamentos
- revisao de decisoes automatizadas, quando aplicavel

Canal de atendimento: [PREENCHER].

### 9. Seguranca

Medidas tecnicas observadas no codigo incluem:

- isolamento de contexto no Electron (`contextIsolation: true`, `sandbox: true`)
- limites de tamanho para processamento de logs
- armazenamento de token OAuth com TTL em Redis

Recomendacao obrigatoria de producao: configurar segredo forte (`SECRET_KEY`) e revisar exposicao de API keys.

### 10. Alteracoes desta politica

Esta politica pode ser atualizada. Data da ultima atualizacao: [PREENCHER].

---

## Termos de Uso — Rascunho

### 1. Aceite

Ao utilizar o software, o usuario concorda com estes Termos.

### 2. Uso permitido

E permitido usar o software para analise tecnica de performance de jobs Spark em ambiente autorizado pelo usuario.

E vedado:

- uso para atividade ilicita
- envio de dados sem autorizacao legal ou contratual
- tentativa de engenharia reversa para explorar vulnerabilidades do servico

### 3. Responsabilidades do usuario

O usuario e responsavel por:

- garantir legitimidade dos dados enviados
- revisar politicas internas antes de compartilhar logs/codigo com provedores externos de IA
- proteger credenciais e chaves de API inseridas na ferramenta

### 4. Limitacao de responsabilidade

A analise gerada por IA e apoio tecnico e nao substitui validacao humana em ambiente de teste/homologacao.

O operador do software nao garante:

- ausencia total de erros nas recomendacoes
- adequacao a um fim especifico sem validacao tecnica adicional
- disponibilidade ininterrupta de provedores externos de IA

### 5. Retencao de dados de log

No desenho atual:

- nao ha banco dedicado de historico de logs no backend
- resultados de job ficam em memoria de processo durante o ciclo de execucao
- arquivos temporarios de download sao removidos ao final da entrega

Se houver implantacao com persistencia adicional, os Termos devem ser atualizados.

### 6. Integracoes de terceiros

Recursos de IA dependem de APIs de terceiros (OpenAI, Anthropic, Google Gemini), sujeitas a disponibilidade e politicas proprias desses provedores.

### 7. Propriedade intelectual

[PREENCHER CLAUSULA DE PROPRIEDADE INTELECTUAL]

### 8. Foro e legislacao

Estes Termos sao regidos pela legislacao brasileira.
Foro: [PREENCHER COMARCA].

### 9. Contato

Canal oficial para duvidas, solicitacoes e privacidade: [PREENCHER].