/**
 * analytics.ts — PostHog singleton for the main process.
 *
 * All events are fired from the Node/main side so the project API key
 * never reaches the sandboxed renderer. The renderer sends events via IPC.
 *
 * Token is injected at build time via POSTHOG_TOKEN env var (CI secret).
 * Local / fork builds have no token → all calls are silent no-ops.
 */
import { app } from 'electron'
import path from 'path'
import fs from 'fs'
import crypto from 'crypto'
import { PostHog } from 'posthog-node'

const TOKEN   = process.env.POSTHOG_TOKEN ?? ''
const API_HOST = 'https://us.i.posthog.com'

let client:     PostHog | null = null
let distinctId: string  | null = null
let optedOut = false

// ─── Config / ID persistence ─────────────────────────────────────

function configPath(): string {
  return path.join(app.getPath('userData'), 'analytics-config.json')
}
function idPath(): string {
  return path.join(app.getPath('userData'), 'analytics-id.json')
}

function readOptOut(): boolean {
  try {
    const raw = fs.readFileSync(configPath(), 'utf-8')
    return (JSON.parse(raw) as { optOut?: boolean }).optOut === true
  } catch {
    return false
  }
}

function writeOptOut(value: boolean): void {
  try { fs.writeFileSync(configPath(), JSON.stringify({ optOut: value }), 'utf-8') } catch { /* best effort */ }
}

function getOrCreateDistinctId(): string {
  try {
    const raw = fs.readFileSync(idPath(), 'utf-8')
    const parsed = JSON.parse(raw) as { id?: string }
    if (parsed?.id) return parsed.id
  } catch { /* will create */ }
  const id = crypto.randomUUID()
  try { fs.writeFileSync(idPath(), JSON.stringify({ id }), 'utf-8') } catch { /* best effort */ }
  return id
}

// ─── Public API ───────────────────────────────────────────────────

export function initAnalytics(): void {
  if (!TOKEN) return   // dev / fork builds — silent no-op

  distinctId = getOrCreateDistinctId()
  optedOut   = readOptOut()

  client = new PostHog(TOKEN, {
    host:          API_HOST,
    flushAt:       5,
    flushInterval: 10_000,
  })
}

export function capture(event: string, props: Record<string, unknown> = {}): void {
  if (!client || !distinctId || optedOut) return
  client.capture({
    distinctId,
    event,
    properties: {
      app_version: app.getVersion(),
      platform:    process.platform,
      ...props,
    },
  })
}

export function setOptOut(value: boolean): void {
  optedOut = value
  writeOptOut(value)
}

export function getOptOut(): boolean {
  return optedOut
}

export async function shutdownAnalytics(): Promise<void> {
  if (!client) return
  try { await client.shutdown() } catch { /* best effort */ }
  client = null
}
