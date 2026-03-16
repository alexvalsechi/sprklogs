# 3. Politica de Privacidade + Termos de Uso

> Modelo revisado para a experiencia desktop atual do SprkLogs. O texto abaixo foi ajustado para um projeto open-source mantido por pessoa fisica e colaboradores, sem exigir CNPJ ou estrutura corporativa formal. Em caso de distribuicao comercial ou institucional, recomenda-se revisao juridica especifica.

## Politica de Privacidade

### 1. Quem opera o software

O SprkLogs e um projeto open-source mantido por Alex Valsechi e colaboradores.

Na ausencia de pessoa juridica dedicada, referencias ao "operador" neste documento devem ser entendidas como referencia ao mantenedor do repositorio e a quem publicar distribuicoes oficiais do projeto.

Canal principal de contato do projeto:

- repositorio publico do projeto
- pagina de Issues/discussoes, quando disponivel
- contato publico informado pelo mantenedor

### 2. Finalidade do tratamento

Os dados tratados pela aplicacao sao usados para:

- reduzir logs de execucao do Apache Spark
- gerar diagnostico tecnico de performance com apoio de IA
- exibir resultados, historico local e relatorios exportados ao proprio usuario
- manter a operacao tecnica do aplicativo desktop e do backend local embarcado

### 3. Categorias de dados tratadas

Dependendo do uso feito pelo usuario, o aplicativo pode tratar:

- arquivo ZIP de event log Spark selecionado localmente
- arquivos `.py` opcionais anexados para analise conjunta
- relatorio reduzido gerado a partir do ZIP
- parametros de analise, como idioma e provedor LLM
- chave de API informada manualmente no modo BYOK
- metadados operacionais, como status do job e mensagens de erro
- historico local de resultados salvo no navegador embarcado do Electron

### 4. Como os dados sao processados no desktop atual

No fluxo principal atual do app Electron:

1. o ZIP e lido localmente no dispositivo do usuario;
2. a reducao inicial acontece no backend local iniciado pelo proprio aplicativo;
3. o ZIP original nao integra o payload enviado para a etapa LLM;
4. somente o relatorio reduzido e, quando aplicavel, anexos `.py` seguem para a analise de IA.

Esse desenho reduz a exposicao de dados em comparacao com o envio do log bruto, mas o relatorio reduzido ainda pode conter informacoes tecnicas relevantes do ambiente analisado.

### 5. Compartilhamento com terceiros

Quando o usuario solicita analise por IA, o aplicativo pode compartilhar dados com o provedor selecionado entre os suportados pelo codigo atual:

- OpenAI
- Anthropic
- Google Gemini

O compartilhamento pode incluir:

- relatorio reduzido em formato textual
- trechos ou arquivos `.py` anexados pelo usuario
- dados tecnicos necessarios para autenticar a chamada ao provedor, como a chave de API informada manualmente

### 6. Armazenamento e retencao

No estado atual do desktop:

- o ZIP original permanece no dispositivo do usuario;
- o backend local mantem jobs em memoria de processo, sem banco persistente por padrao;
- o historico da interface e salvo localmente em `localStorage`, limitado a 10 analises;
- a chave de API digitada no campo BYOK nao e gravada em `localStorage` pelo fluxo observado;
- arquivos Markdown exportados so sao criados quando o usuario escolhe salvá-los em disco.

Nao existe, neste fluxo, uma politica automatica de expiracao temporal para o historico local alem da limitacao por quantidade.

### 7. Autenticacao

O fluxo principal atual da analise desktop nao exige login obrigatorio. O codigo do Electron contem handlers de sessao local simplificada, mas eles nao equivalem a um mecanismo formal de identidade corporativa. Se a distribuicao futura habilitar OAuth real ou login externo, esta politica devera ser atualizada.

### 8. Bases legais

Quando aplicavel sob a LGPD e legislacao correlata, as bases legais podem incluir:

- execucao de contrato ou de procedimentos preliminares solicitados pelo usuario
- legitimo interesse para operacao tecnica, seguranca e melhoria controlada do servico
- consentimento, quando exigido para integracoes opcionais ou contextos especificos

O enquadramento final deve ser validado pelo operador do software para o contexto concreto de uso.

### 9. Direitos dos titulares

Observada a legislacao aplicavel, o titular pode solicitar, quando cabivel:

- confirmacao do tratamento
- acesso aos dados
- correcao de dados incompletos ou desatualizados
- eliminacao, anonimização, bloqueio ou restricao
- informacoes sobre compartilhamento com terceiros
- revisao de decisoes automatizadas, quando juridicamente pertinente

Canal de atendimento: canais publicos do repositorio do projeto e demais contatos publicamente divulgados pelo mantenedor.

### 10. Seguranca

Medidas tecnicas observadas no projeto incluem:

- `contextIsolation: true`
- `sandbox: true`
- `nodeIntegration: false`
- backend local em `127.0.0.1`
- API restrita exposta via preload
- limite de tamanho para `reduced_report` no backend local

Em ambientes de producao, o operador deve complementar esses controles com governanca de secrets, hardening do endpoint local, revisao de logs e politica organizacional para o uso de provedores externos de IA.

### 11. Alteracoes desta politica

Esta politica pode ser revisada para refletir mudancas tecnicas, regulatórias ou operacionais.

Data da ultima revisao deste texto: 2026-03-16.

---

## Termos de Uso

### 1. Aceite

Ao instalar, acessar ou utilizar o SprkLogs, o usuario declara que leu e concorda com estes Termos.

### 2. Uso permitido

O software destina-se a analise tecnica de performance de workloads Spark, para ambientes, dados e codigos cujo uso e compartilhamento sejam autorizados pelo proprio usuario ou por sua organizacao.

Nao e permitido:

- utilizar a aplicacao para atividade ilicita ou abusiva
- submeter dados sem base legal, contratual ou autorizacao adequada
- usar o software para tentar comprometer terceiros, explorar vulnerabilidades ou burlar controles tecnicos

### 3. Responsabilidades do usuario

O usuario e responsavel por:

- verificar se pode compartilhar o conteudo analisado com provedores externos de IA
- revisar ZIPs, anexos `.py` e demais insumos antes do envio
- proteger as chaves de API inseridas no modo BYOK
- validar tecnicamente as recomendacoes antes de aplicá-las em producao

### 4. Natureza das respostas geradas por IA

As respostas do SprkLogs constituem apoio tecnico automatizado. Elas nao substituem avaliacao humana, testes controlados, revisao de arquitetura, parecer juridico ou decisao operacional do usuario.

### 5. Disponibilidade e dependencias externas

Parte da funcionalidade depende de bibliotecas locais, do backend embarcado e de APIs de terceiros. O operador do software nao garante disponibilidade continua de:

- OpenAI
- Anthropic
- Google Gemini
- conectividade de rede necessaria para a etapa de analise externa

### 6. Retencao e historico

No fluxo desktop atual:

- nao ha banco persistente de historico de jobs por padrao;
- resultados de job podem permanecer em memoria enquanto o backend local estiver em execucao;
- o historico visivel ao usuario e salvo localmente em `localStorage`;
- exportacoes em Markdown dependem de acao explicita do usuario.

### 7. Propriedade intelectual e licenciamento

O codigo-fonte do projeto e distribuido sob a licenca GPL-3.0. Componentes de terceiros permanecem sujeitos as respectivas licencas. O uso de marcas, nomes comerciais, credenciais de provedores e conteudo analisado continua sujeito aos direitos de seus respectivos titulares.

### 8. Limitacao de responsabilidade

Na maxima extensao permitida pela legislacao aplicavel, o software e fornecido no estado em que se encontra. O operador nao garante que:

- as recomendacoes geradas sejam isentas de erro
- o software atenda a finalidade especifica do usuario sem validacao adicional
- provedores externos respondam dentro de prazo, custo ou qualidade previsiveis

### 9. Atualizacoes futuras

Se o produto vier a incorporar login obrigatorio, sincronizacao em nuvem, persistencia adicional, telemetria ou novos fluxos de integracao, estes Termos e a Politica de Privacidade deverao ser revisados.

### 10. Lei aplicavel e resolucao de conflitos

Estes Termos devem ser interpretados de acordo com a legislacao aplicavel ao mantenedor ou distribuidor que publicar a versao utilizada pelo usuario, sem prejuizo de normas obrigatorias de protecao de dados, consumidor ou ordem publica aplicaveis no local de uso.

Como se trata de projeto open-source sem pessoa juridica dedicada neste documento, nao ha definicao de foro exclusivo. Sempre que possivel, duvidas e conflitos devem ser tratados inicialmente pelos canais publicos do projeto.

### 11. Contato

Canal oficial do projeto para suporte, privacidade e comunicacoes relacionadas a esta documentacao: repositorio publico do SprkLogs e canais publicamente indicados pelo mantenedor.