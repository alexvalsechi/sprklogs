# 3. Privacy Policy + Terms of Use (Draft)

## Privacy Policy — Draft

### 1. Controller

This software is operated by [FILL IN COMPANY NAME], registration number [FILL IN], headquartered at [FILL IN], contact: [FILL IN EMAIL/PHONE/DPO].

### 2. Purpose of processing

We process data in order to:

- reduce and analyze Spark execution logs
- generate technical performance diagnostics
- display analytical results and reports to the user

### 3. Data processed

Depending on the configuration in use, the following data may be processed:

- Spark event log ZIP file (processed locally on the user's device before any transmission)
- Optional `.py` files provided by the user
- Technical analysis parameters (language, LLM provider, compact mode)
- Integration credentials (manually entered API key) or OAuth token
- Technical processing metadata (job status, payload size, operational logs)

### 4. Local processing and data minimization

The desktop application reduces the log ZIP locally before any network transmission. Only the resulting aggregated metrics report is sent to the backend. The original log file never leaves the user's device.

### 5. Sharing with third parties

When AI analysis is requested, the system sends a text prompt to the configured external provider:

- OpenAI
- Anthropic
- Google Gemini

The prompt may contain a portion of the reduced metrics report and excerpts of `.py` files (when provided by the user).

### 6. Storage and retention

Under the current implementation:

- Job results are held in server process memory only and are not persisted to a database
- Temporary download files are removed immediately after delivery
- OAuth tokens are stored in Redis with a time-to-live (TTL)
- Local analysis history is stored in `localStorage` on the client device

### 7. Legal bases

Applicable legal bases, to be validated with legal counsel:

- Performance of a contract or pre-contractual steps
- Legitimate interests in security and technical improvement
- Consent, where required for specific integrations

[FILL IN OFFICIAL LEGAL BASIS]

### 8. Data subject rights

Users may request, as applicable under data protection law:

- Confirmation and access
- Rectification
- Erasure or restriction where applicable
- Information about data sharing
- Review of automated decisions, where applicable

Contact channel: [FILL IN].

### 9. Security

Technical measures implemented include:

- Electron context isolation (`contextIsolation: true`, `sandbox: true`)
- Size limits for log processing payloads
- OAuth token storage with TTL in Redis

Production recommendation: configure a strong `SECRET_KEY` and review API key exposure.

### 10. Changes to this policy

This policy may be updated. Date of last update: [FILL IN].

---

## Terms of Use — Draft

### 1. Acceptance

By using the software, the user agrees to these Terms.

### 2. Permitted use

The software may be used for technical performance analysis of Spark jobs in environments authorized by the user.

Prohibited uses include:

- Use for unlawful activities
- Submitting data without legal or contractual authorization
- Attempting to reverse-engineer the service to exploit vulnerabilities

### 3. User responsibilities

The user is responsible for:

- Ensuring the legitimacy of submitted data
- Reviewing internal policies before sharing logs or code with external AI providers
- Protecting credentials and API keys entered into the tool

### 4. Limitation of liability

AI-generated analysis is technical support and does not replace human validation in test or staging environments.

The software operator does not guarantee:

- The absence of errors in recommendations
- Fitness for a specific purpose without additional technical validation
- Uninterrupted availability of external AI providers

### 5. Log data retention

Under the current design:

- There is no dedicated log history database on the backend
- Job results remain in server process memory during the execution cycle
- Temporary download files are removed upon delivery

If additional persistence is deployed, these Terms must be updated accordingly.

### 6. Third-party integrations

AI features depend on third-party APIs (OpenAI, Anthropic, Google Gemini), subject to the availability and policies of those providers.

### 7. Intellectual property

[FILL IN INTELLECTUAL PROPERTY CLAUSE]

### 8. Governing law and jurisdiction

These Terms are governed by applicable law.
Jurisdiction: [FILL IN].

### 9. Contact

Official channel for inquiries, requests, and privacy matters: [FILL IN].
