# 2. Documento de Arquitetura de Privacidade (1 pagina)

## Objetivo do software

O sistema analisa logs de execucao do Apache Spark e produz diagnostico tecnico com apoio de LLM. A interface desktop (Electron) permite enviar ZIP de event log e, opcionalmente, arquivos `.py` do job para enriquecer a analise.

## Tratamento de dados do cliente

Pelo codigo atual do renderer, o envio ocorre para backend via endpoint HTTP (`/api/upload`). Nesse caminho, o backend executa processamento de reducao do log e posterior analise por LLM.

No repositorio tambem existe um caminho de processamento local (desktop) ja implementado:

- IPC `reduce-zip-locally` em `apps/desktop/main/ipc/compress.handler.ts`
- script local `apps/desktop/main/scripts/reduce_log.py`
- uso de `LogReducer` para extrair resumo

Esse caminho local reduz o ZIP no computador do usuario e depois envia somente relatorio reduzido para `/api/upload-reduced`. Entretanto, no estado atual, o renderer nao chama esse IPC diretamente.

## O que trafega externamente

No backend, o envio para provedores externos ocorre somente na etapa LLM (`backend/adapters/llm_adapters.py`). O payload enviado e um prompt textual montado em `backend/services/llm_analyzer.py` com:

- trecho do relatorio reduzido (`reduced_report[:6000]`)
- opcionalmente, trechos de codigo `.py` (`[:2000]` por arquivo)

Provedores externos suportados:

- OpenAI
- Anthropic
- Google Gemini

A autenticacao pode ocorrer por API key recebida no formulario ou por token OAuth recuperado do Redis.

## Retorno ao usuario

O frontend consulta `/api/status/{job_id}` ate concluido e renderiza:

- resumo/KPIs
- tabela de stages
- texto de analise AI
- downloads em Markdown/JSON

Isso ocorre em `renderResults()` no `apps/desktop/renderer/features/spark-analyzer/index.html`.

## Armazenamento e retencao no codigo atual

- Memoria de jobs em RAM (`_jobs`), sem persistencia duravel dedicada.
- Arquivos de download gerados temporariamente e removidos no `finally` do endpoint.
- Tokens OAuth armazenados no Redis (`oauth_token:{user_id}:{provider}`).
- Historico de execucoes no frontend em `localStorage`.

## Por que a arquitetura pode ser privacy-by-architecture

A base do projeto contem um desenho tecnicamente favoravel a minimizacao de dados:

- ha componente de reducao local no desktop
- ha limite/tamanho para payload reduzido no backend
- o prompt ao LLM usa truncamento de conteudo

Contudo, para afirmar processamento local como comportamento efetivo padrao, e necessario que o renderer utilize o fluxo IPC local (`reduce-zip-locally` + `/upload-reduced`) de forma obrigatoria ou por feature flag ativa.

## Lacunas que exigem alinhamento antes de declaracoes formais

- Divergencia entre endpoint chamado pela UI (`/api/upload`) e rota implementada observada (`/upload-reduced`).
- Fluxo local existente no Electron ainda nao conectado ao renderer atual.
- Ausencia, no codigo, de politica explicita de retencao com prazos para `_jobs` e `localStorage`.

Sem esses ajustes, qualquer garantia de "nunca envia ZIP bruto" ou "100% local" nao e sustentada de forma universal pelo estado atual do repositorio.