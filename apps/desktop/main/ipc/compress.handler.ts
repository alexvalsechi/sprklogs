const { app, dialog, ipcMain } = require('electron')
import fs from 'fs/promises'
import os from 'os'
import path from 'path'
import crypto from 'crypto'
import { capture } from '../analytics'

type IpcMainInvokeEvent = Electron.IpcMainInvokeEvent

type ReducePayload = {
  zipPath: string
  compact?: boolean
}

type SubmitPayload = {
  apiBaseUrl?: string
  reducedReport: string
  pyFilePaths?: string[]
  llmProvider?: string
  apiKey?: string
  userId?: string
  provider?: string
  language?: string
}

type SavePayload = {
  content: string
  suggestedName?: string
}

type ReduceResponse = {
  reduced_report: string
  summary: unknown
  reduce_job_id?: string
}

/** Send the ZIP path to the backend for local-disk reduction (no file transfer). */
async function reduceZipViaPath(
  apiBaseUrl: string,
  zipPath: string,
  compact: boolean,
  reduceJobId: string,
): Promise<ReduceResponse> {
  const form = new FormData()
  form.append('file_path', zipPath)
  form.append('reduce_job_id', reduceJobId)
  if (compact) form.append('compact', 'true')

  const res = await fetch(`${apiBaseUrl}/api/reduce-local-path`, {
    method: 'POST',
    body: form,
  })

  if (!res.ok) {
    const errText = await res.text()
    throw new Error(`reduce-local-path failed (${res.status}): ${errText}`)
  }

  return (await res.json()) as ReduceResponse
}

/** Poll /api/reduce-progress/{id} and forward updates to the renderer. */
function startProgressPoll(
  apiBaseUrl: string,
  reduceJobId: string,
  sender: Electron.WebContents,
): ReturnType<typeof setInterval> {
  return setInterval(async () => {
    try {
      const res = await fetch(`${apiBaseUrl}/api/reduce-progress/${reduceJobId}`)
      if (!res.ok) return
      const data = (await res.json()) as { percent: number; stage: string }
      sender.send('reduce-progress', data)
    } catch {
      // swallow transient errors — the poll will retry
    }
  }, 500)
}

export function registerCompressHandlers(pyBaseUrl: string): void {
  ipcMain.handle('reduce-zip-locally', async (event: IpcMainInvokeEvent, payload: ReducePayload) => {
    const zipPath = payload?.zipPath
    const compact = !!payload?.compact

    if (!zipPath) {
      throw new Error('zipPath is required')
    }

    const reduceJobId = crypto.randomUUID()

    // Start polling progress in background, forwarding events to the renderer
    const poll = startProgressPoll(pyBaseUrl, reduceJobId, event.sender)
    const startMs = Date.now()
    let fileSizeBytes = 0
    try { fileSizeBytes = (await fs.stat(zipPath)).size } catch { /* best effort */ }

    try {
      const reduced = await reduceZipViaPath(pyBaseUrl, zipPath, compact, reduceJobId)

      // Spark SQL plan trees can be thousands of nodes deep.
      // summary.sql_plan_tree (unused in renderer) and the sparkPlanInfo trees
      // inside sql_executions can exceed Electron contextBridge's 1000-level
      // recursion limit on large ZIPs.  Fix: drop sql_plan_tree entirely and
      // pass sql_executions as a serialised JSON string; the renderer parses it
      // back after the IPC call completes.
      const summary = (reduced.summary ?? null) as Record<string, unknown> | null
      let sqlExecutionsJson: string | null = null
      if (summary) {
        delete summary.sql_plan_tree // not consumed by the renderer
        if (summary.sql_executions != null) {
          sqlExecutionsJson = JSON.stringify(summary.sql_executions)
          delete summary.sql_executions
        }
      }
      capture('zip_reduced', {
        success:         true,
        duration_ms:     Date.now() - startMs,
        file_size_bytes: fileSizeBytes,
      })
      return {
        reducedReport: reduced.reduced_report,
        summary,
        sqlExecutionsJson,
      }
    } catch (err) {
      capture('zip_reduced', {
        success:         false,
        duration_ms:     Date.now() - startMs,
        error:           err instanceof Error ? err.message : String(err),
      })
      throw err
    } finally {
      clearInterval(poll)
      // Emit 100 % so the renderer progress bar reaches the end
      try { event.sender.send('reduce-progress', { percent: 100, stage: 'report_ready' }) } catch { /* closed */ }
    }
  })

  ipcMain.handle('get-backend-url', async () => pyBaseUrl)
  ipcMain.handle('get-app-version', async () => app.getVersion())

  ipcMain.handle('submit-reduced-for-analysis', async (_event: IpcMainInvokeEvent, payload: SubmitPayload) => {
    // Always use the locally-managed backend URL — do NOT trust the renderer-supplied
    // apiBaseUrl which defaults to http://localhost:8000 and would point at the wrong port.
    const apiBaseUrl = pyBaseUrl
    const reducedReport = payload?.reducedReport
    const pyFilePaths = payload?.pyFilePaths || []
    const llmProvider = payload?.llmProvider
    const apiKey = payload?.apiKey
    const userId = payload?.userId
    const provider = payload?.provider
    const language = payload?.language || 'en'

    if (!reducedReport || !String(reducedReport).trim()) {
      throw new Error('reducedReport is required')
    }

    const form = new FormData()
    form.append('reduced_report', reducedReport)
    form.append('language', language)
    if (llmProvider) form.append('llm_provider', llmProvider)
    if (apiKey) form.append('api_key', apiKey)
    if (userId) form.append('user_id', userId)
    if (provider) form.append('provider', provider)

    for (const filePath of pyFilePaths) {
      const fileBuffer = await fs.readFile(filePath)
      const fileName = path.basename(filePath)
      form.append('pyspark_files', new Blob([fileBuffer]), fileName)
    }

    const res = await fetch(`${apiBaseUrl}/api/upload-reduced`, {
      method: 'POST',
      body: form,
    })

    if (!res.ok) {
      const errText = await res.text()
      throw new Error(`upload-reduced failed (${res.status}): ${errText}`)
    }

    const result = await res.json()
    capture('analysis_submitted', { llm_provider: llmProvider ?? provider ?? 'unknown', language })
    return result
  })

  ipcMain.handle('save-report-to-disk', async (_event: IpcMainInvokeEvent, payload: SavePayload) => {
    const { content, suggestedName } = payload || {}
    if (!content || typeof content !== 'string') {
      throw new Error('content is required')
    }

    const defaultName = suggestedName || `spark_report_${Date.now()}.md`

    const { filePath, canceled } = await dialog.showSaveDialog({
      title: 'Salvar relatorio Markdown',
      defaultPath: path.join(app.getPath('documents'), defaultName),
      filters: [{ name: 'Markdown', extensions: ['md'] }],
    })

    if (canceled || !filePath) {
      return { saved: false }
    }

    await fs.writeFile(filePath, content, 'utf-8')
    capture('report_exported', { saved: true })
    return { saved: true, filePath }
  })

  ipcMain.handle('compressFile', async (_event: IpcMainInvokeEvent, filePath: string) => {
    const originalSize = (await fs.stat(filePath)).size
    const outputPath = path.join(os.tmpdir(), `spark_reduced_${Date.now()}.md`)
    const reduceJobId = crypto.randomUUID()
    const reduced = await reduceZipViaPath(pyBaseUrl, filePath, false, reduceJobId)
    await fs.writeFile(outputPath, reduced.reduced_report, 'utf-8')

    const outputSize = (await fs.stat(outputPath)).size
    return {
      outputPath,
      savedBytes: Math.max(0, originalSize - outputSize),
    }
  })

  ipcMain.handle('getCompressionStatus', async () => {
    return { status: 'done', progress: 100 }
  })
}
