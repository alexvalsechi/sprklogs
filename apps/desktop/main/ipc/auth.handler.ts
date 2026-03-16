const { ipcMain } = require('electron')

type Session = { user: { id: string; email: string } } | null
type IpcMainInvokeEvent = unknown

let session: Session = null

export function registerAuthHandlers(_pyBaseUrl: string): void {
  ipcMain.handle('login', async (_event: IpcMainInvokeEvent, credentials: { email: string; password: string }) => {
    const email = credentials?.email || ''
    const password = credentials?.password || ''

    if (!email || !password) {
      throw new Error('email and password are required')
    }

    session = {
      user: {
        id: Buffer.from(email).toString('base64url'),
        email,
      },
    }

    return { token: Buffer.from(`${email}:${Date.now()}`).toString('base64url') }
  })

  ipcMain.handle('logout', async () => {
    session = null
  })

  ipcMain.handle('getSession', async () => {
    return session
  })
}
