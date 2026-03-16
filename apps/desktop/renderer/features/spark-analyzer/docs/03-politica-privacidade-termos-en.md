# 3. Privacy Policy + Terms of Use

> Revised template for the current SprkLogs desktop experience. The text below has been adapted for an open-source project maintained by an individual maintainer and contributors, without requiring a corporate entity or formal registration. For commercial or institutional distribution, a specific legal review is still recommended.

## Privacy Policy

### 1. Who operates the software

SprkLogs is an open-source project maintained by Alex Valsechi and contributors.

Where no dedicated legal entity exists, references to the "operator" in this document should be read as references to the repository maintainer and to whoever publishes official project distributions.

Primary project contact channels include:

- the public project repository
- the Issues/discussions page, when available
- any public contact channel disclosed by the maintainer

### 2. Purpose of processing

The application processes data in order to:

- reduce Apache Spark execution logs
- generate AI-assisted technical performance diagnostics
- display results, local history, and exported reports to the user
- operate the desktop app and its bundled local backend

### 3. Categories of data processed

Depending on how the user operates the app, the software may process:

- locally selected Spark event log ZIP files
- optional `.py` files attached for joint analysis
- the reduced report generated from the ZIP
- analysis parameters such as language and LLM provider
- API keys manually entered in BYOK mode
- operational metadata such as job status and error messages
- locally stored UI history in the Electron embedded browser context

### 4. How data is processed in the current desktop flow

In the current main Electron workflow:

1. the ZIP is read locally on the user's device;
2. the initial reduction runs in the local backend started by the app itself;
3. the original ZIP is not part of the payload sent into the LLM stage;
4. only the reduced report and, when applicable, optional `.py` attachments are forwarded to AI analysis.

This design reduces exposure compared with raw log upload, but the reduced report may still contain meaningful technical information about the analyzed environment.

### 5. Sharing with third parties

When the user requests AI analysis, the app may share data with the selected provider supported by the current codebase:

- OpenAI
- Anthropic
- Google Gemini

Shared content may include:

- the reduced text report
- attached `.py` files or excerpts derived from them
- technical authentication data needed to call the selected provider, such as a manually entered API key

### 6. Storage and retention

In the current desktop implementation:

- the original ZIP remains on the user's device;
- the local backend keeps jobs in process memory, with no persistent database by default;
- UI history is stored locally in `localStorage`, limited to 10 analyses;
- the BYOK API key typed into the form is not observed to be persisted in `localStorage`;
- exported Markdown files are created only when the user explicitly saves them to disk.

This flow does not currently implement time-based expiration for local history beyond the item-count cap.

### 7. Authentication

The main desktop analysis flow does not require mandatory login. The Electron codebase includes lightweight local session handlers, but they are not equivalent to formal enterprise identity verification. If a future distribution enables real OAuth or external login, this policy must be updated.

### 8. Legal bases

Where applicable under data protection law, possible legal bases may include:

- performance of a contract or user-requested pre-contractual steps
- legitimate interests in technical operation, security, and controlled service improvement
- consent, where required for optional integrations or specific contexts

The final legal basis assessment must be validated by the software operator for the actual deployment context.

### 9. Data subject rights

Subject to applicable law, data subjects may request, where relevant:

- confirmation of processing
- access to their data
- correction of inaccurate or incomplete data
- erasure, anonymization, blocking, or restriction
- information about third-party sharing
- review of automated decisions, where legally applicable

Contact channel: the project's public repository channels and any other public contact disclosed by the maintainer.

### 10. Security

Technical measures present in the project include:

- `contextIsolation: true`
- `sandbox: true`
- `nodeIntegration: false`
- local backend bound to `127.0.0.1`
- narrow preload API exposure
- size guardrails on `reduced_report` submissions

In production, the software operator should complement these controls with secret management, local endpoint hardening, log review, and organizational rules governing use of external AI providers.

### 11. Changes to this policy

This policy may be updated to reflect technical, regulatory, or operational changes.

Last revised: 2026-03-16.

---

## Terms of Use

### 1. Acceptance

By installing, accessing, or using SprkLogs, the user agrees to these Terms.

### 2. Permitted use

The software is intended for technical Spark workload performance analysis in environments, datasets, and codebases that the user or the user's organization is authorized to process and share.

The following are not permitted:

- unlawful or abusive use
- submitting data without proper legal, contractual, or internal authorization
- using the software to attack, probe, or compromise third parties

### 3. User responsibilities

The user is responsible for:

- verifying whether the analyzed content may be shared with external AI providers
- reviewing ZIP files, `.py` attachments, and other inputs before submission
- protecting API keys entered in BYOK mode
- validating recommendations before applying them in production

### 4. Nature of AI-generated output

SprkLogs provides automated technical assistance. It does not replace human review, controlled testing, architecture assessment, legal advice, or production decision-making by the user.

### 5. Availability and external dependencies

Part of the product depends on local libraries, the bundled backend, and third-party APIs. The operator does not guarantee uninterrupted availability of:

- OpenAI
- Anthropic
- Google Gemini
- network connectivity required for the external analysis step

### 6. Retention and history

In the current desktop flow:

- there is no persistent backend history database by default;
- job results may remain in memory while the local backend process is running;
- visible history is stored locally in `localStorage`;
- Markdown exports depend on explicit user action.

### 7. Intellectual property and licensing

The project source code is distributed under the GPL-3.0 license. Third-party components remain subject to their own licenses. Use of trademarks, provider credentials, and analyzed content remains subject to the rights of their respective owners.

### 8. Limitation of liability

To the maximum extent permitted by applicable law, the software is provided as-is. The operator does not guarantee that:

- recommendations will be error-free
- the software will fit a specific purpose without additional validation
- third-party providers will respond within predictable cost, time, or quality constraints

### 9. Future updates

If the product later adds mandatory login, cloud synchronization, extra persistence, telemetry, or new integration flows, these Terms and the Privacy Policy must be revised.

### 10. Governing law and dispute handling

These Terms should be interpreted in accordance with the law applicable to the maintainer or distributor that published the version used by the user, without limiting any mandatory data protection, consumer, or public-order rules applicable where the software is used.

Because this is an open-source project and no dedicated legal entity is designated in this document, no exclusive jurisdiction is defined here. Where possible, questions and disputes should first be raised through the project's public channels.

### 11. Contact

Official project channel for support, privacy, and communications related to this documentation: the public SprkLogs repository and any public channels identified by the maintainer.
