export interface UpdateCheckResult {
  hasUpdate: boolean
  latestVersion: string
  currentVersion: string
  releaseUrl: string
}

export interface ReduceProgressData {
  percent: number
  stage: string
}

export interface SessionUser {
  id: string
  email: string
}

export type Session = { user: SessionUser } | null

export interface ReduceZipPayload {
  zipPath: string
  compact?: boolean
}

export interface ReduceZipResult {
  reducedReport: string
  summary: Record<string, unknown> | null
  sqlExecutionsJson: string | null
}

export interface SubmitReducedAnalysisPayload {
  reducedReport: string
  pyFilePaths?: string[]
  llmProvider?: string
  apiKey?: string
  userId?: string
  language?: string
}

export interface SubmitReducedAnalysisResult {
  job_id: string
  status: 'pending' | 'running' | 'done' | 'error'
}

export interface SaveReportPayload {
  content: string
  suggestedName?: string
}

export interface SaveReportResult {
  saved: boolean
  filePath?: string
}

export interface CompressionResult {
  outputPath: string
  savedBytes: number
}

export interface CompressionStatus {
  status: 'pending' | 'running' | 'done' | 'error'
  progress: number
}

export interface IpcApi {
  compressFile: (filePath: string) => Promise<CompressionResult>
  getCompressionStatus: (jobId: string) => Promise<CompressionStatus>
  getAppVersion: () => Promise<string>
  checkForUpdates: () => Promise<UpdateCheckResult>
  login: (credentials: { email: string; password: string }) => Promise<{ token: string }>
  logout: () => Promise<void>
  getSession: () => Promise<Session>
  getBackendUrl: () => Promise<string>
  reduceZipLocally: (payload: ReduceZipPayload) => Promise<ReduceZipResult>
  submitReducedForAnalysis: (payload: SubmitReducedAnalysisPayload) => Promise<SubmitReducedAnalysisResult>
  saveReportToDisk: (payload: SaveReportPayload) => Promise<SaveReportResult>
  onReduceProgress: (callback: (event: unknown, data: ReduceProgressData) => void) => () => void
  trackEvent: (event: string, props?: Record<string, unknown>) => Promise<void>
  setAnalyticsOptOut: (optOut: boolean) => Promise<void>
  getAnalyticsOptOut: () => Promise<boolean>
}

export type IpcChannel = keyof IpcApi
