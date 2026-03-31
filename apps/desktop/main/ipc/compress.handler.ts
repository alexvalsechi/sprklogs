const { app, dialog, ipcMain } = require('electron')
import fs from 'fs/promises'
import os from 'os'
import path from 'path'
import crypto from 'crypto'
import type {
  ReduceZipPayload,
  ReduceZipResult,
  SaveReportPayload,
  SubmitReducedAnalysisPayload,
} from '@log-sparkui/ipc-types'
import { capture } from '../analytics'
import { reduceZipViaPath, startProgressPoll, submitReducedAnalysis } from './backend-client'

type IpcMainInvokeEvent = Electron.IpcMainInvokeEvent

export function registerCompressHandlers(pyBaseUrl: string): void {
  ipcMain.handle('reduce-zip-locally', async (event: IpcMainInvokeEvent, payload: ReduceZipPayload): Promise<ReduceZipResult> => {
    const zipPath = payload?.zipPath
    const compact = !!payload?.compact

    if (!zipPath) {
      throw new Error('zipPath is required')
    }

    const reduceJobId = crypto.randomUUID()
    const poll = startProgressPoll(pyBaseUrl, reduceJobId, event.sender)
    const startMs = Date.now()
    let fileSizeBytes = 0
    try { fileSizeBytes = (await fs.stat(zipPath)).size } catch { /* best effort */ }

    try {
      const reduced = await reduceZipViaPath(pyBaseUrl, zipPath, compact, reduceJobId)
      const summary = (reduced.summary ?? null) as Record<string, unknown> | null
      let sqlExecutionsJson: string | null = null
      if (summary) {
        delete summary.sql_plan_tree
        if (summary.sql_executions != null) {
          sqlExecutionsJson = JSON.stringify(summary.sql_executions)
          delete summary.sql_executions
        }
      }

      capture('zip_reduced', {
        success: true,
        duration_ms: Date.now() - startMs,
        file_size_bytes: fileSizeBytes,
      })
      return {
        reducedReport: reduced.reduced_report,
        summary,
        sqlExecutionsJson,
      }
    } catch (err) {
      capture('zip_reduced', {
        success: false,
        duration_ms: Date.now() - startMs,
        error: err instanceof Error ? err.message : String(err),
      })
      throw err
    } finally {
      clearInterval(poll)
      try { event.sender.send('reduce-progress', { percent: 100, stage: 'report_ready' }) } catch { /* closed */ }
    }
  })

  ipcMain.handle('get-backend-url', async () => pyBaseUrl)
  ipcMain.handle('get-app-version', async () => app.getVersion())

  ipcMain.handle('submit-reduced-for-analysis', async (_event: IpcMainInvokeEvent, payload: SubmitReducedAnalysisPayload) => {
    const result = await submitReducedAnalysis(pyBaseUrl, payload)
    capture('analysis_submitted', {
      llm_provider: payload?.llmProvider ?? 'unknown',
      language: payload?.language || 'en',
    })
    return result
  })

  ipcMain.handle('save-report-to-disk', async (_event: IpcMainInvokeEvent, payload: SaveReportPayload) => {
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
