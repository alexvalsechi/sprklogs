import { registerCompressHandlers } from './compress.handler'
import { registerAuthHandlers } from './auth.handler'

export function registerAllHandlers(pyBaseUrl: string): void {
  registerCompressHandlers(pyBaseUrl)
  registerAuthHandlers(pyBaseUrl)
}
