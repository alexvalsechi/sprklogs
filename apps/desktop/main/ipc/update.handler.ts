import { ipcMain, app } from 'electron'
import https from 'https'

const GITHUB_OWNER = 'alexvalsechi'
const GITHUB_REPO  = 'sprklogs'
const RELEASES_API = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/latest`
const RELEASES_URL = 'https://alexvalsechi.github.io/sprklogs/'

export interface UpdateCheckResult {
  hasUpdate:      boolean
  latestVersion:  string
  currentVersion: string
  releaseUrl:     string
}

function fetchLatestTag(): Promise<string> {
  return new Promise((resolve, reject) => {
    const req = https.get(
      RELEASES_API,
      {
        headers: {
          'User-Agent': `${GITHUB_REPO}/${app.getVersion()} Electron update-check`,
          'Accept': 'application/vnd.github+json',
        },
      },
      (res) => {
        if (res.statusCode === 404) {
          reject(new Error('No releases found'))
          return
        }
        if (!res.statusCode || res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`GitHub API returned ${res.statusCode}`))
          return
        }
        let body = ''
        res.on('data', (chunk: Buffer) => { body += chunk.toString() })
        res.on('end', () => {
          try {
            const json = JSON.parse(body) as { tag_name?: string }
            if (!json.tag_name) {
              reject(new Error('tag_name missing in GitHub response'))
              return
            }
            resolve(json.tag_name)
          } catch (e) {
            reject(e)
          }
        })
      },
    )
    req.setTimeout(10_000, () => {
      req.destroy()
      reject(new Error('GitHub API request timed out'))
    })
    req.on('error', reject)
  })
}

/** Strip a leading "v" so "v1.2.3" and "1.2.3" compare equal. */
function normalise(tag: string): string {
  return tag.trim().replace(/^v/i, '')
}

function parseVersion(tag: string): [number, number, number] | null {
  const match = normalise(tag).match(/^(\d+)\.(\d+)\.(\d+)$/)
  if (!match) return null
  return [Number(match[1]), Number(match[2]), Number(match[3])]
}

function isNewerVersion(latest: string, current: string): boolean {
  const latestParts = parseVersion(latest)
  const currentParts = parseVersion(current)
  if (!latestParts || !currentParts) return false

  return latestParts.some((part, index) => {
    const currentPart = currentParts[index]
    if (part === currentPart) return false
    return part > currentPart &&
      latestParts.slice(0, index).every((value, previousIndex) =>
        value === currentParts[previousIndex],
      )
  })
}

export function registerUpdateHandlers(): void {
  ipcMain.handle('check-for-updates', async (): Promise<UpdateCheckResult> => {
    const currentVersion = app.getVersion()
    const latestTag      = await fetchLatestTag()
    const latestVersion  = normalise(latestTag)
    const hasUpdate      = isNewerVersion(latestVersion, currentVersion)
    return { hasUpdate, latestVersion, currentVersion, releaseUrl: RELEASES_URL }
  })
}
