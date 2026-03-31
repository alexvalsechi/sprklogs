import { ipcMain } from 'electron'
import { capture, setOptOut, getOptOut } from '../analytics'

type IpcMainInvokeEvent = Electron.IpcMainInvokeEvent

export function registerAnalyticsHandlers(): void {
  ipcMain.handle('track-event', (_event: IpcMainInvokeEvent, event: string, props?: Record<string, unknown>) => {
    capture(event, props ?? {})
  })

  ipcMain.handle('set-analytics-opt-out', (_event: IpcMainInvokeEvent, optOut: boolean) => {
    setOptOut(optOut)
  })

  ipcMain.handle('get-analytics-opt-out', () => {
    return getOptOut()
  })
}
