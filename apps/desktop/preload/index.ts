import { contextBridge, ipcRenderer } from 'electron'
import type { IpcApi } from '@log-sparkui/ipc-types'

const api: IpcApi = {
  compressFile: (filePath) => ipcRenderer.invoke('compressFile', filePath),
  getCompressionStatus: (jobId) => ipcRenderer.invoke('getCompressionStatus', jobId),
  login: (credentials) => ipcRenderer.invoke('login', credentials),
  logout: () => ipcRenderer.invoke('logout'),
  getSession: () => ipcRenderer.invoke('getSession'),
  getBackendUrl: () => ipcRenderer.invoke('get-backend-url'),
  getAppVersion: () => ipcRenderer.invoke('get-app-version'),
  reduceZipLocally: (payload) => ipcRenderer.invoke('reduce-zip-locally', payload),
  submitReducedForAnalysis: (payload) => ipcRenderer.invoke('submit-reduced-for-analysis', payload),
  saveReportToDisk: (payload) => ipcRenderer.invoke('save-report-to-disk', payload),
}

contextBridge.exposeInMainWorld('api', api)
contextBridge.exposeInMainWorld('desktopApi', {
  reduceZipLocally: api.reduceZipLocally,
  submitReducedForAnalysis: api.submitReducedForAnalysis,
  saveReportToDisk: api.saveReportToDisk,
})

declare global {
  interface Window {
    api: IpcApi
    desktopApi: Pick<IpcApi, 'reduceZipLocally' | 'submitReducedForAnalysis' | 'saveReportToDisk'>
  }
}
