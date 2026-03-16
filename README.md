<div align="center">
  <img src="apps/web/favicon-96x96.png" alt="SprkLogs" width="96" />
  <h1>SprkLogs</h1>
  <p><strong>LLMs can't read your Spark logs. SprkLogs can.</strong></p>

  [![Platform](https://img.shields.io/badge/platform-Windows-0078d4?logo=windows)](https://github.com/alexvalsechi/sprklogs/releases)
  [![Electron](https://img.shields.io/badge/Electron-31-47848f?logo=electron)](https://www.electronjs.org/)
  [![Python](https://img.shields.io/badge/Python-3.11+-ffd343?logo=python)](https://www.python.org/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
</div>

---

Spark production logs can reach 1 GB. Sending that directly to an LLM blows up the context window or generates an absurd token bill. **SprkLogs processes the log locally first** — extracting and compressing only what matters — then sends a lean diagnostic report to the LLM of your choice.

You bring the ZIP. SprkLogs delivers the diagnosis.

---

## Demo

<div align="center">
  <video src="apps/web/sprklogs-how-to-use.mp4" autoplay loop muted playsinline width="100%"></video>
</div>

---

## Quick Start

**Download the installer (Windows):**

1. Go to [Releases](https://github.com/alexvalsechi/sprklogs/releases)
2. Download `SprkLogs-setup-vX.X.X.exe`
3. Run the installer — no configuration required

**Or run from source:**

```bash
git clone https://github.com/alexvalsechi/sprklogs.git
cd sprklogs

# Backend
pip install -r backend/requirements.txt
python -m backend.app

# Desktop (separate terminal)
cd apps/desktop
npm install
npm start
```

---

## How it works

| Step | What happens |
|---|---|
| **1** | Drag a Spark event log ZIP (even 1 GB) into SprkLogs |
| **2** | The log is processed **locally** — nothing is uploaded to any cloud |
| **3** | A compressed diagnostic report is generated on your machine |
| **4** | Only the report is sent to the LLM (OpenAI, Gemini, or Anthropic) |
| **5** | You receive a full technical diagnosis: bottlenecks by stage, root cause, and actionable recommendations |

---

## Features

- Local ZIP processing — file size is no longer a limitation
- Multi-provider LLM support (OpenAI · Google Gemini · Anthropic)
- BYOK — bring your own API key, no subscription required
- Stage-by-stage breakdown table with sorting
- AI-generated diagnosis with bottlenecks, root cause, and recommendations
- Export analysis as Markdown report
- Analysis history stored locally
- Dark theme, bilingual interface (EN / PT)
- Privacy-first: no telemetry, no cloud storage, no account required

---

## Tech Stack

| Layer | Stack |
|---|---|
| Desktop | Electron 31, TypeScript |
| Renderer | HTML, CSS, vanilla JS |
| Backend | Python, FastAPI |
| Log processing | Python (local, in-process) |
| LLM providers | OpenAI, Google Gemini, Anthropic |
| Packaging | electron-builder, NSIS |
| Monorepo | Turborepo |

---

## License

<details>
<summary><strong>MIT License</strong></summary>

```
MIT License

Copyright (c) 2024 Alex Valsechi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

</details>

<details>
<summary><strong>Third-party licenses</strong></summary>

| Dependency | License |
|---|---|
| [Electron](https://github.com/electron/electron) | MIT |
| [FastAPI](https://github.com/tiangolo/fastapi) | MIT |
| [Python](https://www.python.org/) | PSF |
| [Turborepo](https://github.com/vercel/turbo) | MIT |
| [electron-builder](https://github.com/electron-userland/electron-builder) | MIT |

</details>

---

<div align="center">
  Made by <a href="https://www.linkedin.com/in/alex-valsechi/">Alex Valsechi</a> &nbsp;·&nbsp; <a href="https://alexvalsechi.github.io/sprklogs/">sprklogs website</a>
</div>
