import { spawn, ChildProcess } from 'child_process'
import path from 'path'
import net from 'net'
import fs from 'fs'

const { app } = require('electron')

let pyProcess: ChildProcess | null = null
let pyPort = 8765

type BackendLaunchConfig = {
  command: string
  args: string[]
  cwd?: string
}

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

  const resolveLaunchCandidates = (): BackendLaunchConfig[] => {
    if (!app.isPackaged) {
      const repoRoot = path.join(__dirname, '../../../..')
      return [{ command: 'python', args: ['-m', 'backend.app', '--port', String(pyPort)], cwd: repoRoot }]
    }

    const packagedBackendRoot = path.join(resourcesPath, 'backend')
    const packagedPyEntrypoint = path.join(packagedBackendRoot, 'app.py')
    const packagedExe = path.join(packagedBackendRoot, packagedBin)

    const candidates: BackendLaunchConfig[] = []

    // Prefer Python source backend when available. This keeps local packaged builds
    // aligned with current backend code and avoids stale server.exe regressions.
    if (fs.existsSync(packagedPyEntrypoint)) {
      candidates.push({
        command: 'python',
        args: ['-m', 'backend.app', '--port', String(pyPort)],
        cwd: resourcesPath,
      })
    }

    if (fs.existsSync(packagedExe)) {
      candidates.push({
        command: packagedExe,
        args: ['--port', String(pyPort)],
      })
    }

    return candidates
  }

  const candidates = resolveLaunchCandidates()
  if (candidates.length === 0) {
    throw new Error('No bundled backend launcher found (expected backend Python files or server executable).')
  }

  const errors: string[] = []

  for (const candidate of candidates) {
    const spawnOpts = candidate.cwd
      ? { stdio: 'pipe' as const, cwd: candidate.cwd }
      : { stdio: 'pipe' as const }

    pyProcess = spawn(candidate.command, candidate.args, spawnOpts)
    pyProcess.stderr?.on('data', (d) => console.error('[python]', d.toString()))
    pyProcess.on('exit', (code) => console.log('[python] exited with code', code))

    try {
      await Promise.race([
        waitForPort(pyPort),
        new Promise<void>((_, reject) => {
          pyProcess?.once('error', (err) => reject(err))
          pyProcess?.once('exit', (code) => reject(new Error(`Python backend exited early with code ${code}`)))
        }),
      ])

      return `http://localhost:${pyPort}`
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err)
      errors.push(`${candidate.command} ${candidate.args.join(' ')} => ${detail}`)
      pyProcess?.kill()
      pyProcess = null
    }
  }

  throw new Error(`Could not start backend. Attempts: ${errors.join(' | ')}`)
}

export function stopPython(): void {
  pyProcess?.kill()
  pyProcess = null
}
