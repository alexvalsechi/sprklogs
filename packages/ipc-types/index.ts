export interface IpcApi {
  compressFile: (filePath: string) => Promise<{ outputPath: string; savedBytes: number }>
  getCompressionStatus: (jobId: string) => Promise<{ status: 'pending' | 'running' | 'done' | 'error'; progress: number }>
  getAppVersion: () => Promise<string>

  login: (credentials: { email: string; password: string }) => Promise<{ token: string }>
  logout: () => Promise<void>
  getSession: () => Promise<{ user: { id: string; email: string } } | null>
  getBackendUrl: () => Promise<string>

  reduceZipLocally: (payload: { zipPath: string; compact?: boolean }) => Promise<{ reducedReport: string; summary: unknown | null }>
  submitReducedForAnalysis: (payload: {
    apiBaseUrl?: string
    reducedReport: string
    pyFilePaths?: string[]
    llmProvider?: string
    apiKey?: string
    userId?: string
    provider?: string
    language?: string
  }) => Promise<unknown>
  saveReportToDisk: (payload: { content: string; suggestedName?: string }) => Promise<{ saved: boolean; filePath?: string }>
}

export type IpcChannel = keyof IpcApi
