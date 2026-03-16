# 2. Documento de Arquitetura e Privacidade

## Escopo deste documento

Este documento descreve o comportamento atual da experiencia desktop do SprkLogs, revisada com base na implementacao do app Electron, do preload, dos handlers IPC e do backend FastAPI embarcado. O foco aqui e o fluxo de analise local do ZIP com envio apenas do relatorio reduzido para a etapa de IA.

## Objetivo do software

O SprkLogs recebe logs de execucao do Apache Spark em formato ZIP, reduz o material para um resumo tecnico e gera um diagnostico assistido por LLM. O app desktop tambem aceita arquivos `.py` opcionais para correlacionar comportamento do job com o codigo enviado pelo usuario.

## Componentes ativos no desktop

### 1. Renderer Electron

- Exibe a UI, recebe o ZIP, arquivos `.py`, idioma e provedor LLM.
- Nao acessa Node.js diretamente.
- Armazena localmente idioma, aba ativa e historico das 10 analises mais recentes em `localStorage`.

### 2. Preload bridge

- Expoe somente uma API restrita para o renderer.
- Principais chamadas: `reduceZipLocally`, `submitReducedForAnalysis`, `saveReportToDisk`, `getBackendUrl` e `getAppVersion`.

### 3. Processo principal Electron

- Cria a janela com `contextIsolation: true`, `sandbox: true` e `nodeIntegration: false`.
- Sobe o backend local em `127.0.0.1` com porta dinamica.
- Intercepta navegacao externa e abre links HTTP/HTTPS fora da janela principal.

### 4. Backend local FastAPI

- Executa no mesmo dispositivo do usuario.
- Expoe `/api/reduce-local`, `/api/upload-reduced`, `/api/status/{job_id}` e `/api/health`.
- Mantem o estado de jobs em memoria (`_jobs`) e processa analises em background com `ThreadPoolExecutor`.

## Fluxo real de dados

### Etapa A. Reducao local obrigatoria

1. O usuario seleciona um ZIP no renderer.
2. O renderer chama `window.desktopApi.reduceZipLocally(...)`.
3. O processo principal envia o ZIP ao backend local via `/api/reduce-local`.
4. O `LogReducer` gera `summary` e `reduced_report` localmente.

Resultado: o ZIP original permanece no dispositivo; o que avanca no pipeline e um relatorio textual reduzido.

### Etapa B. Analise por IA

1. O renderer envia `reduced_report` para `submitReducedForAnalysis(...)`.
2. O processo principal monta `FormData` com:
	- relatorio reduzido
	- idioma
	- provedor LLM selecionado
	- chave de API informada pelo usuario, quando houver
	- arquivos `.py` opcionais
3. O backend local recebe em `/api/upload-reduced` e cria um job em memoria.
4. O `LocalReducedJobRunner` executa a analise assincrona.
5. O renderer faz polling em `/api/status/{job_id}` ate o status `done` ou `error`.

## Dados que podem ser transmitidos para terceiros

No fluxo principal atual, os provedores externos recebem apenas o prompt montado a partir de:

- trecho do relatorio reduzido
- instrucoes de sistema do analisador
- eventualmente trechos relevantes de arquivos `.py` enviados pelo usuario

Provedores suportados pelo codigo:

- OpenAI
- Anthropic
- Google Gemini

## Dados que permanecem locais

- ZIP de event log original
- caminho local do arquivo selecionado
- relatorio reduzido mantido na sessao do renderer para exibicao e exportacao
- historico local salvo em `localStorage`
- arquivo Markdown exportado, quando o usuario escolhe gravar em disco

## Armazenamento e retencao observados

| Dado | Local | Comportamento atual |
|---|---|---|
| ZIP original | Dispositivo do usuario | Nao enviado ao fluxo LLM no desktop atual |
| Jobs e status | RAM do backend local | Persistem enquanto o processo do backend estiver em execucao |
| Historico da UI | `localStorage` do renderer | Limitado a 10 analises |
| Chave de API digitada | Campo de formulario em memoria | Nao ha persistencia em `localStorage` observada |
| Relatorio exportado | Disco local | So existe quando o usuario aciona a exportacao |

## Autenticacao e identidade no estado atual

- O uso principal do desktop hoje e BYOK, sem login obrigatorio.
- Existem handlers `login/getSession/logout` no Electron, mas eles implementam apenas sessao local simplificada e nao autenticacao corporativa completa.
- Ha codigo de OAuth no backend, porem ele nao e o mecanismo central do fluxo de analise atualmente exposto na UI desktop.

## Controles de seguranca relevantes

- Isolamento de contexto no Electron.
- Sandbox do renderer.
- Backend local preso a loopback (`127.0.0.1`).
- Limite de tamanho no endpoint `/api/upload-reduced`.
- Job store sem banco persistente por padrao no desktop atual.
- Abertura de links externos fora da WebView principal.

## Limites desta arquitetura

- O historico local em `localStorage` nao possui politica automatica de expiracao por tempo; apenas limitacao por quantidade.
- O backend em memoria nao implementa, neste fluxo, limpeza temporal formal de jobs concluidos.
- Se uma implantacao futura habilitar OAuth real, telemetria, sincronizacao em nuvem ou persistencia adicional, este documento deve ser revisado.

## Conclusao

No estado atual do projeto, a afirmacao tecnicamente sustentada e: o desktop reduz o ZIP localmente e envia para a etapa de IA somente o relatorio reduzido, mais anexos `.py` opcionais fornecidos pelo usuario. Isso reduz exposicao de dados em relacao a um upload bruto do event log, mas nao elimina a necessidade de governanca sobre o conteudo resumido e sobre eventuais arquivos de codigo anexados.