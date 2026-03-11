const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('desktopApi', {
  reduceZipLocally: (payload) => ipcRenderer.invoke('reduce-zip-locally', payload),
  submitReducedForAnalysis: (payload) => ipcRenderer.invoke('submit-reduced-for-analysis', payload),
  saveReportToDisk: (payload) => ipcRenderer.invoke('save-report-to-disk', payload),
});
