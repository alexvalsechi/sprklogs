# 2. Privacy Architecture Document

## Purpose of the software

SprkLogs analyzes Apache Spark execution logs and produces technical diagnostics with AI support. The desktop interface (Electron) enables users to submit an event log ZIP and, optionally, job `.py` files to enrich the analysis.

## How client data is handled

The original ZIP file is processed entirely on the user's device before any network transmission occurs. A local reduction pipeline extracts only aggregated performance metrics — execution times, memory usage, task counts, and stage summaries — and discards all raw events and business data.

Only the resulting metrics report is sent to the backend for AI analysis. The backend never receives the original log file.

## What is transmitted externally

The backend forwards a text prompt to the configured AI provider (OpenAI, Anthropic, or Google Gemini). That prompt contains:

- A portion of the reduced metrics report
- Optionally, excerpts of user-provided `.py` files

The prompt does not contain raw log events, business data, or any information beyond what the user explicitly provided for analysis.

## What is returned to the user

After the AI analysis completes, the backend returns:

- Performance KPIs and stage breakdown
- AI-generated diagnosis in Markdown format

Results are displayed in the application and can optionally be saved to disk by the user. No data is stored on the server after delivery.

## Data storage and retention

| Data | Where stored | Retention |
|------|-------------|-----------|
| Job results | Server RAM only | Discarded when server restarts |
| OAuth tokens | Redis (server) | Time-limited (TTL per provider) |
| Analysis history | User's localStorage | Local only, last 10 analyses |
| Original log file | User's device only | Never transmitted |

## Privacy-by-design characteristics

- Local reduction is the first and mandatory step — the ZIP never leaves the device
- The server processes only pre-reduced, aggregated data
- No persistent database of logs or analysis results on the server
- The user provides and controls their own AI provider API key
