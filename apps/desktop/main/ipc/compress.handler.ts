const { app, dialog, ipcMain } = require('electron')
import fs from 'fs/promises'
import os from 'os'
import path from 'path'
import { execFile } from 'child_process'

type IpcMainInvokeEvent = unknown

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

function runFile(command: string, args: string[], cwd: string): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    execFile(command, args, { cwd }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`Command failed: ${stderr || error.message}`))
        return
      }
      resolve({ stdout, stderr })
    })
  })
}

export function registerCompressHandlers(pyBaseUrl: string): void {
  ipcMain.handle('reduce-zip-locally', async (_event: IpcMainInvokeEvent, payload: ReducePayload) => {
    const zipPath = payload?.zipPath
    const compact = !!payload?.compact

    if (!zipPath) {
      throw new Error('zipPath is required')
    }

    const outputFile = path.join(os.tmpdir(), `spark_reduced_${Date.now()}.md`)
    const scriptPath = path.join(__dirname, '../scripts/reduce_log.py')
    const workspaceRoot = path.resolve(__dirname, '../../../..')

    const { stdout } = await runFile('python', [scriptPath, '--zip', zipPath, '--out', outputFile, ...(compact ? ['--compact'] : [])], workspaceRoot)

    const reducedReport = await fs.readFile(outputFile, 'utf-8')
    await fs.unlink(outputFile).catch(() => {})

    let summary: unknown = null
    try {
      summary = JSON.parse(stdout.trim())
    } catch {
      summary = null
    }

    return { reducedReport, summary }
  })

  ipcMain.handle('submit-reduced-for-analysis', async (_event: IpcMainInvokeEvent, payload: SubmitPayload) => {
    const apiBaseUrl = payload?.apiBaseUrl || pyBaseUrl
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

    return res.json()
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
    return { saved: true, filePath }
  })

  ipcMain.handle('compressFile', async (_event: IpcMainInvokeEvent, filePath: string) => {
    const originalSize = (await fs.stat(filePath)).size
    const outputPath = path.join(os.tmpdir(), `spark_reduced_${Date.now()}.md`)

    await runFile(
      'python',
      [path.join(__dirname, '../scripts/reduce_log.py'), '--zip', filePath, '--out', outputPath],
      path.resolve(__dirname, '../../../..'),
    )

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
