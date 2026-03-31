import { app, BrowserWindow, shell, dialog, Menu } from 'electron'
import path from 'path'
import { startPython, stopPython } from './python-manager'
import { registerAllHandlers } from './ipc/register'
import { initAnalytics, capture, shutdownAnalytics } from './analytics'

let mainWindow: BrowserWindow | null = null
let pyBaseUrl: string

function isHttpUrl(rawUrl: string): boolean {
  try {
    const parsed = new URL(rawUrl)
    return parsed.protocol === 'http:' || parsed.protocol === 'https:'
  } catch {
    return false
  }
}

function createWindow(): void {
  mainWindow = new BrowserWindow({
    title: 'SprkLogs',
    icon: path.join(__dirname, '../../renderer/features/spark-analyzer/web-app-manifest-512x512.png'),
    width: 1280,
    height: 800,
    minWidth: 980,
    minHeight: 680,
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, '../preload/index.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  })

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (isHttpUrl(url)) {
      shell.openExternal(url)
      return { action: 'deny' }
    }
    return { action: 'allow' }
  })

  mainWindow.webContents.on('will-navigate', (event, url) => {
    if (isHttpUrl(url)) {
      event.preventDefault()
      shell.openExternal(url)
    }
  })

  if (process.env.NODE_ENV === 'development') {
    mainWindow.loadURL('http://localhost:5173')
  } else {
    mainWindow.loadFile(path.join(__dirname, '../../renderer/features/spark-analyzer/index.html'))
  }
}

app.whenReady().then(async () => {
  Menu.setApplicationMenu(null)
  initAnalytics()
  try {
    pyBaseUrl = await startPython()
    console.log('[main] Local backend running at', pyBaseUrl)
    registerAllHandlers(pyBaseUrl)
    createWindow()
    capture('app_opened')
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error)
    dialog.showErrorBox('Backend startup error', `Could not start bundled backend.\n\n${detail}`)
    app.quit()
    return
  }

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow()
    }
  })
})

let _isQuitting = false
app.on('before-quit', async (event) => {
  if (_isQuitting) return
  event.preventDefault()
  _isQuitting = true
  capture('app_closed')
  stopPython()
  await shutdownAnalytics()
  app.quit()
})
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})
