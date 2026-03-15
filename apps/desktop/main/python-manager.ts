import { spawn, ChildProcess } from 'child_process'
import path from 'path'
import net from 'net'
import fs from 'fs'

const { app } = require('electron')

let pyProcess: ChildProcess | null = null
let pyPort = 8765

async function findFreePort(): Promise<number> {
  return new Promise((resolve) => {
    const srv = net.createServer()
    srv.listen(0, () => {
      const port = (srv.address() as net.AddressInfo).port
      srv.close(() => resolve(port))
    })
  })
}

function waitForPort(port: number, timeout = 10_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    const retry = () => {
      const sock = net.connect(port, '127.0.0.1')
      sock.on('connect', () => {
        sock.destroy()
        resolve()
      })
      sock.on('error', () => {
        if (Date.now() - start > timeout) {
          reject(new Error('Python backend timeout'))
        } else {
          setTimeout(retry, 200)
        }
      })
    }
    retry()
  })
}

export async function startPython(): Promise<string> {
  pyPort = await findFreePort()

  const packagedBin = process.platform === 'win32' ? 'server.exe' : 'server'
  const resourcesPath = (process as NodeJS.Process & { resourcesPath?: string }).resourcesPath || ''
  const bin = app.isPackaged
    ? path.join(resourcesPath, 'backend', packagedBin)
    : 'python'

  if (app.isPackaged && !fs.existsSync(bin)) {
    throw new Error(`Packaged backend executable not found at: ${bin}`)
  }

  const repoRoot = path.join(__dirname, '../../../..')
  const args = app.isPackaged
    ? ['--port', String(pyPort)]
    : ['-m', 'backend.app', '--port', String(pyPort)]
  const spawnOpts = app.isPackaged
    ? { stdio: 'pipe' as const }
    : { stdio: 'pipe' as const, cwd: repoRoot }

  pyProcess = spawn(bin, args, spawnOpts)
  pyProcess.stderr?.on('data', (d) => console.error('[python]', d.toString()))
  pyProcess.on('exit', (code) => console.log('[python] exited with code', code))

  await Promise.race([
    waitForPort(pyPort),
    new Promise<void>((_, reject) => {
      pyProcess?.once('error', (err) => reject(err))
      pyProcess?.once('exit', (code) => reject(new Error(`Python backend exited early with code ${code}`)))
    }),
  ])
  return `http://localhost:${pyPort}`
}

export function stopPython(): void {
  pyProcess?.kill()
  pyProcess = null
}
