import { spawn, ChildProcess } from 'child_process'
import path from 'path'
import net from 'net'
import fs from 'fs'

const { app } = require('electron')

let pyProcess: ChildProcess | null = null
let pyPort = 8765

const DEV_BACKEND_STARTUP_TIMEOUT_MS = 10_000
const PACKAGED_BACKEND_STARTUP_TIMEOUT_MS = 60_000
const OUTPUT_BUFFER_LIMIT = 8_000

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
          reject(new Error(`Python backend timeout after ${timeout}ms`))
        } else {
          setTimeout(retry, 200)
        }
      })
    }
    retry()
  })
}

function appendOutput(buffer: string[], chunk: string): void {
  if (!chunk) {
    return
  }

  buffer.push(chunk)
  while (buffer.join('').length > OUTPUT_BUFFER_LIMIT) {
    buffer.shift()
  }
}

function formatCapturedOutput(stdout: string[], stderr: string[]): string {
  const sections: string[] = []

  const combinedStdout = stdout.join('').trim()
  if (combinedStdout) {
    sections.push(`stdout: ${combinedStdout}`)
  }

  const combinedStderr = stderr.join('').trim()
  if (combinedStderr) {
    sections.push(`stderr: ${combinedStderr}`)
  }

  return sections.length > 0 ? ` | ${sections.join(' | ')}` : ''
}

function getBackendStartupTimeoutMs(isPackaged: boolean): number {
  const rawValue = process.env.SPRK_BACKEND_STARTUP_TIMEOUT_MS
  const envValue = rawValue ? Number.parseInt(rawValue, 10) : Number.NaN

  if (Number.isFinite(envValue) && envValue > 0) {
    return envValue
  }

  return isPackaged ? PACKAGED_BACKEND_STARTUP_TIMEOUT_MS : DEV_BACKEND_STARTUP_TIMEOUT_MS
}

export async function startPython(): Promise<string> {
  pyPort = await findFreePort()
  const startupTimeoutMs = getBackendStartupTimeoutMs(app.isPackaged)

  const packagedBin = process.platform === 'win32' ? 'server.exe' : 'server'
  const resourcesPath = (process as NodeJS.Process & { resourcesPath?: string }).resourcesPath || ''

  const resolveLaunchCandidates = (): BackendLaunchConfig[] => {
    if (!app.isPackaged) {
      const repoRoot = path.join(__dirname, '../../../..')
      return [{ command: 'python', args: ['-m', 'backend.app', '--port', String(pyPort)], cwd: repoRoot }]
    }

    const packagedBackendRoot = path.join(resourcesPath, 'backend')
    const packagedExe = path.join(packagedBackendRoot, packagedBin)

    if (!fs.existsSync(packagedExe)) {
      throw new Error(`Bundled backend executable not found at ${packagedExe}`)
    }

    // In packaged mode we only allow the bundled executable to guarantee
    // end users do not need Python installed on their machine.
    return [{ command: packagedExe, args: ['--port', String(pyPort)] }]
  }

  const candidates = resolveLaunchCandidates()
  if (candidates.length === 0) {
    throw new Error('No backend launcher candidates found.')
  }

  const errors: string[] = []

  for (const candidate of candidates) {
    const stdoutChunks: string[] = []
    const stderrChunks: string[] = []
    const spawnOpts = candidate.cwd
      ? { stdio: 'pipe' as const, cwd: candidate.cwd, windowsHide: true }
      : { stdio: 'pipe' as const, windowsHide: true }

    pyProcess = spawn(candidate.command, candidate.args, spawnOpts)
    pyProcess.stdout?.on('data', (d) => appendOutput(stdoutChunks, d.toString()))
    pyProcess.stderr?.on('data', (d) => {
      const chunk = d.toString()
      appendOutput(stderrChunks, chunk)
      console.error('[python]', chunk)
    })
    pyProcess.on('exit', (code) => console.log('[python] exited with code', code))

    try {
      await Promise.race([
        waitForPort(pyPort, startupTimeoutMs),
        new Promise<void>((_, reject) => {
          pyProcess?.once('error', (err) => reject(err))
          pyProcess?.once('exit', (code) => reject(new Error(`Python backend exited early with code ${code}`)))
        }),
      ])

      return `http://localhost:${pyPort}`
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err)
      const outputDetail = formatCapturedOutput(stdoutChunks, stderrChunks)
      errors.push(`${candidate.command} ${candidate.args.join(' ')} => ${detail}${outputDetail}`)
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
