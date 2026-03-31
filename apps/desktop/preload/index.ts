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
  checkForUpdates: () => ipcRenderer.invoke('check-for-updates'),
  reduceZipLocally: (payload) => ipcRenderer.invoke('reduce-zip-locally', payload),
  submitReducedForAnalysis: (payload) => ipcRenderer.invoke('submit-reduced-for-analysis', payload),
  saveReportToDisk: (payload) => ipcRenderer.invoke('save-report-to-disk', payload),
  onReduceProgress: (callback) => {
    ipcRenderer.on('reduce-progress', callback)
    return () => ipcRenderer.removeListener('reduce-progress', callback)
  },

  // Analytics
  trackEvent: (event, props) => ipcRenderer.invoke('track-event', event, props),
  setAnalyticsOptOut: (optOut) => ipcRenderer.invoke('set-analytics-opt-out', optOut),
  getAnalyticsOptOut: () => ipcRenderer.invoke('get-analytics-opt-out'),
}

contextBridge.exposeInMainWorld('api', api)
contextBridge.exposeInMainWorld('desktopApi', {
  reduceZipLocally: api.reduceZipLocally,
  submitReducedForAnalysis: api.submitReducedForAnalysis,
  saveReportToDisk: api.saveReportToDisk,
  onReduceProgress: api.onReduceProgress,
})

declare global {
  interface Window {
    api: IpcApi
    desktopApi: Pick<IpcApi, 'reduceZipLocally' | 'submitReducedForAnalysis' | 'saveReportToDisk' | 'onReduceProgress'>
  }
}
