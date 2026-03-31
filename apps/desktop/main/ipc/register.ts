import { registerCompressHandlers } from './compress.handler'
import { registerAuthHandlers } from './auth.handler'
import { registerUpdateHandlers } from './update.handler'
import { registerAnalyticsHandlers } from './analytics.handler'

export function registerAllHandlers(pyBaseUrl: string): void {
  registerCompressHandlers(pyBaseUrl)
  registerAuthHandlers(pyBaseUrl)
  registerUpdateHandlers()
  registerAnalyticsHandlers()
}
