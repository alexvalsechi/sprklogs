# 2. Privacy Architecture Document

## Scope of this document

This document describes the current SprkLogs desktop behavior based on the Electron app, preload bridge, IPC handlers, and bundled FastAPI backend. Its focus is the local ZIP reduction flow followed by LLM analysis of the reduced report only.

## Purpose of the software

SprkLogs receives Apache Spark event log ZIP files, reduces them into a technical summary, and generates an AI-assisted diagnosis. The desktop app may also accept optional `.py` files so the analysis can correlate runtime behavior with the code supplied by the user.

## Active components in the desktop application

### 1. Electron renderer

- Displays the UI and collects the ZIP, optional `.py` files, language, and provider choice.
- Does not access Node.js APIs directly.
- Stores language, active tab, and up to 10 recent analyses in `localStorage`.

### 2. Preload bridge

- Exposes a narrow API surface to the renderer.
- Main methods: `reduceZipLocally`, `submitReducedForAnalysis`, `saveReportToDisk`, `getBackendUrl`, and `getAppVersion`.

### 3. Electron main process

- Creates the window with `contextIsolation: true`, `sandbox: true`, and `nodeIntegration: false`.
- Starts the bundled backend on `127.0.0.1` using a dynamic port.
- Prevents in-app navigation to external HTTP/HTTPS pages and opens them outside the main window.

### 4. Local FastAPI backend

- Runs on the same device as the user.
- Exposes `/api/reduce-local`, `/api/upload-reduced`, `/api/status/{job_id}`, and `/api/health`.
- Keeps job state in memory (`_jobs`) and runs reduced-report analyses in background threads.

## Actual data flow

### Step A. Mandatory local reduction

1. The user selects a ZIP file in the renderer.
2. The renderer calls `window.desktopApi.reduceZipLocally(...)`.
3. The main process forwards the ZIP to the local backend through `/api/reduce-local`.
4. `LogReducer` produces `summary` and `reduced_report` locally.

Result: the original ZIP stays on the device; the next stage receives a reduced text report.

### Step B. AI analysis

1. The renderer calls `submitReducedForAnalysis(...)`.
2. The main process builds `FormData` containing:
	- the reduced report
	- language
	- selected LLM provider
	- user-entered API key, when provided
	- optional `.py` files
3. The local backend receives the request at `/api/upload-reduced` and creates an in-memory job.
4. `LocalReducedJobRunner` executes the analysis asynchronously.
5. The renderer polls `/api/status/{job_id}` until it reaches `done` or `error`.

## Data that may be transmitted to third parties

In the current main flow, external providers receive only the prompt assembled from:

- a portion of the reduced report
- analyzer system instructions
- optionally, relevant `.py` content supplied by the user

Providers supported in the codebase:

- OpenAI
- Anthropic
- Google Gemini

## Data that remains local

- original Spark ZIP file
- local path to the selected file
- reduced report kept in renderer session for display and export
- UI history stored in `localStorage`
- Markdown file exported to disk when the user chooses to save it

## Observed storage and retention behavior

| Data | Location | Current behavior |
|---|---|---|
| Original ZIP | User device | Not sent to the LLM flow in the current desktop path |
| Jobs and status | Local backend RAM | Persist while the local backend process is running |
| UI history | Renderer `localStorage` | Limited to 10 analyses |
| Typed API key | In-memory form state | No observed `localStorage` persistence |
| Exported report | Local disk | Created only when the user explicitly saves it |

## Authentication and identity in the current state

- The main desktop workflow is BYOK and does not require mandatory login.
- Electron includes `login/getSession/logout` handlers, but they implement only a lightweight local session and not full enterprise authentication.
- OAuth-related backend code exists in the repository, but it is not the central mechanism currently used by the desktop analysis flow.

## Relevant security controls

- Electron context isolation.
- Renderer sandboxing.
- Local backend bound to loopback (`127.0.0.1`).
- Size guard on `/api/upload-reduced`.
- No persistent job database by default in the current desktop flow.
- External links are opened outside the main app surface.

## Limitations of the current architecture

- `localStorage` history has no time-based expiration policy; it is capped only by count.
- The in-memory backend job store does not implement formal time-based cleanup in this flow.
- If a future release adds real OAuth, telemetry, cloud sync, or extra persistence, this document must be updated.

## Conclusion

The statement that is technically supported by the current implementation is: the desktop app reduces the ZIP locally and sends only the reduced report, plus optional user-supplied `.py` files, into the AI analysis path. This materially reduces exposure compared with raw event-log upload, but governance is still required for the reduced report and for any attached source files.
