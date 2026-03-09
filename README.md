# Spark Log Analyzer

> Reduce gigabyte-scale Apache Spark event logs to actionable insights — with optional AI-powered bottleneck diagnostics.

![Version](https://img.shields.io/badge/version-2.0.0-orange)
![Python](https://img.shields.io/badge/python-3.12+-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green)

---

## Overview

Spark applications generate massive event logs that are nearly impossible to inspect manually. This tool:

1. **Reduces** a ZIP of Spark event logs to a structured summary (stage metrics, task statistics, skew detection).
2. **Analyzes** the reduced report with an LLM (OpenAI or Anthropic) to surface bottlenecks and recommend PySpark fixes.
3. **Presents** everything in a clean web interface with download options.

No CLI. Everything is driven through the browser.

---

## Project Structure

```
log-sparkui/
├── backend/
│   ├── app.py                   # FastAPI entrypoint
│   ├── api/
│   │   └── routes.py            # Controllers (thin layer, delegates to services)
│   ├── services/
│   │   ├── log_reducer.py       # CoR pipeline + Strategy renderers
│   │   ├── llm_analyzer.py      # LLM prompt + response handling
│   │   └── job_service.py       # Orchestration facade
│   ├── adapters/
│   │   └── llm_adapters.py      # OpenAI / Anthropic adapters (Factory + Singleton)
│   ├── models/
│   │   └── job.py               # Pydantic domain models
│   ├── utils/
│   │   ├── config.py            # Settings (pydantic-settings, Singleton via lru_cache)
│   │   └── logging_config.py
│   ├── tests/
│   │   ├── conftest.py
│   │   ├── test_log_reducer.py
│   │   └── test_llm_adapters.py
│   └── requirements.txt
├── frontend/
│   └── index.html               # Single-file SPA
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

---

## Design Patterns Applied

| Pattern | Where | Purpose |
|---|---|---|
| **Chain of Responsibility** | `log_reducer.py` — `*Handler` classes | Each step (load → parse meta → aggregate → build summary) passes a context dict down the chain |
| **Strategy** | `MarkdownRenderer`, `CompactMarkdownRenderer`, `JsonRenderer` | Swap output format without changing pipeline logic |
| **Iterator** | `_iter_events()` in `log_reducer.py` | Streams JSON events line-by-line from ZIP — memory efficient |
| **Factory** | `StageAggregationHandler._build_stage()`, `LLMClientFactory` | Construct complex objects in one place |
| **Singleton** | `get_settings()` via `lru_cache`, `LLMClientFactory._instances` | One config object and one LLM client per (provider, key) pair |
| **Adapter** | `OpenAIAdapter`, `AnthropicAdapter`, `NoOpAdapter` | Uniform `complete(prompt)` interface across providers |
| **Facade** | `LogReducer`, `JobService` | Hide pipeline complexity behind simple `.reduce()` / `.process()` calls |
| **Dependency Injection** | `LLMAnalyzer(adapter=...)`, `JobService(reducer=..., analyzer=...)` | Services accept injected dependencies — fully mockable in tests |

---

## Quick Start

### Local (Python)

```bash
# 1. Clone and set up environment
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure (optional — skip for no-LLM mode)
cp ../.env.example .env
# Edit .env with your API key

# 3. Run
uvicorn app:app --reload --port 8000

# 4. Open browser
open http://localhost:8000
```

### Docker

```bash
# Copy and configure
cp .env.example .env
# Edit .env

# Build & run
docker compose up --build

# Open
open http://localhost:8000
```

---

## Usage

1. **Upload** your Spark event log ZIP (produced by `spark.eventLog.enabled=true`).
2. *(Optional)* Upload `.py` source files for code-level recommendations.
3. *(Optional)* Select an LLM provider and enter your API key (or set env vars).
4. Click **Analyze →** and wait for processing.
5. Review the KPI cards, stage table, and AI analysis panel.
6. **Download** the report as Markdown or JSON.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/upload` | Submit ZIP + options; returns `job_id` |
| `GET` | `/api/status/{job_id}` | Poll for job status and results |
| `GET` | `/api/download/{job_id}/{format}` | Download report (`md` or `json`) |

### Upload form fields

| Field | Type | Required | Description |
|---|---|---|---|
| `log_zip` | file | ✅ | `.zip` containing Spark event log files |
| `pyspark_files` | file[] | — | `.py` job source files |
| `compact` | bool | — | Generate shorter report (default: false) |
| `llm_provider` | string | — | `openai` or `anthropic` |
| `api_key` | string | — | API key (overrides env var) |

---

## Running Locally

### Prerequisites

- Python 3.12+
- Redis (for async task queue)

### Setup

```bash
# Install dependencies
cd backend
pip install -r requirements.txt

# Start Redis (via Docker or system)
docker run -d -p 6379:6379 redis:7-alpine

# Set environment variables (optional)
export OPENAI_API_KEY=sk-...
export CELERY_BROKER_URL=redis://localhost:6379/0
export CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Start the web server
uvicorn app:app --reload --host 0.0.0.0 --port 8000

# In another terminal, start Celery workers
celery -A backend.celery_app worker --loglevel=info
```

Access http://localhost:8000

---

## Docker (Recommended)

```bash
# Build and run all services (FastAPI + Redis + Celery workers)
docker-compose up --build

# Or run in background
docker-compose up -d
```

This starts:
- **Redis** on port 6379
- **FastAPI app** on port 8000
- **Celery workers** for async processing

---

## Running Tests

## Configuration Reference

| Environment Variable | Default | Description |
|---|---|---|
| `OPENAI_API_KEY` | — | OpenAI API key (auto-selects `openai` as provider) |
| `ANTHROPIC_API_KEY` | — | Anthropic API key (auto-selects `anthropic` as provider) |
| `LLM_PROVIDER` | — | Explicit override: `openai` or `anthropic` |
| `LLM_API_KEY` | — | Unified key (lower priority than above) |
| `MAX_ZIP_MB` | `500` | Maximum ZIP size accepted |
| `CORS_ORIGINS` | `["*"]` | Allowed CORS origins (JSON array string) |

---

## Possible Extensions

- **Redis job store** — replace the in-memory `_jobs` dict for multi-process deployments.
- **Celery workers** — move `_run_job` to a task queue for better scalability.
- **Additional log formats** — add new `BaseHandler` subclasses for Flink, Databricks runtime logs.
- **New LLM providers** — add a `GeminiAdapter` or `BedrockAdapter`; register in `LLMClientFactory._build()`.
- **Authentication** — add OAuth2/JWT middleware to the FastAPI app.
- **Persistent storage** — swap in PostgreSQL or S3 for job results.
- **Streaming responses** — use SSE to stream LLM output token-by-token to the browser.
- **Comparison view** — diff two runs side-by-side to detect regressions.

---

## License

MIT — see `LICENSE`.
