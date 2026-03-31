import fs from 'fs/promises'
import path from 'path'
import type { ReduceProgressData, SubmitReducedAnalysisPayload, SubmitReducedAnalysisResult } from '@log-sparkui/ipc-types'

type ReduceBackendResponse = {
  reduced_report: string
  summary: unknown
  reduce_job_id?: string
}

export async function reduceZipViaPath(
  apiBaseUrl: string,
  zipPath: string,
  compact: boolean,
  reduceJobId: string,
): Promise<ReduceBackendResponse> {
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

  return (await res.json()) as ReduceBackendResponse
}

export function startProgressPoll(
  apiBaseUrl: string,
  reduceJobId: string,
  sender: Electron.WebContents,
): ReturnType<typeof setInterval> {
  return setInterval(async () => {
    try {
      const res = await fetch(`${apiBaseUrl}/api/reduce-progress/${reduceJobId}`)
      if (!res.ok) return
      const data = (await res.json()) as ReduceProgressData
      sender.send('reduce-progress', data)
    } catch {
      // Swallow transient polling errors; the next tick retries.
    }
  }, 500)
}

export async function submitReducedAnalysis(
  apiBaseUrl: string,
  payload: SubmitReducedAnalysisPayload,
): Promise<SubmitReducedAnalysisResult> {
  const reducedReport = payload?.reducedReport
  const pyFilePaths = payload?.pyFilePaths || []
  const llmProvider = payload?.llmProvider
  const apiKey = payload?.apiKey
  const userId = payload?.userId
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

  return (await res.json()) as SubmitReducedAnalysisResult
}
