import { randomUUID } from 'node:crypto'
import { Context, Service } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import {
  EXECUTION_PROTOCOL_VERSION,
  MAX_RPC_RESPONSE_BYTES,
  parseExecutionResponse,
  type ExecutionOperation,
  type ExecutionRequest,
} from '@dsh-cloud/execution-protocol'
import type { RunAuthority } from '@dsh-cloud/run-context'

declare module '@deepseek-ai/cordis' {
  interface Context {
    cloudExecution: CloudExecutionClient
  }
}

export interface Config {
  brokerUrl?: string
  internalToken?: string
  requestTimeoutMs?: number
}

interface ResolvedConfig {
  brokerUrl: string
  internalToken: string
  requestTimeoutMs: number
}

async function readBounded(response: Response): Promise<Buffer> {
  if (response.body === null) return Buffer.alloc(0)
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const chunk of response.body) {
    const value = Buffer.from(chunk)
    bytes += value.byteLength
    if (bytes > MAX_RPC_RESPONSE_BYTES) {
      await response.body.cancel().catch(() => undefined)
      throw new Error('execution response exceeded its byte limit')
    }
    chunks.push(value)
  }
  return Buffer.concat(chunks)
}

export class ExecutionRemoteError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message)
    this.name = 'ExecutionRemoteError'
  }
}

/** Trusted HTTP client for one current RunAttempt's remote execution world. */
class CloudExecutionClient extends Service {
  static inject = ['cloudRunContext']
  static Config: z<Config> = z.object({
    brokerUrl: z.string(),
    internalToken: z.string(),
    requestTimeoutMs: z.number().default(30_000),
  })

  private readonly config: ResolvedConfig

  constructor(ctx: Context, config: Config = {}) {
    super(ctx, 'cloudExecution')
    const brokerUrl = config.brokerUrl ?? process.env['DSH_CLOUD_TOOL_BROKER_URL'] ?? ''
    const internalToken = config.internalToken ?? process.env['DSH_CLOUD_TOOL_BROKER_TOKEN'] ?? ''
    const requestTimeoutMs = config.requestTimeoutMs ?? 30_000
    if (!/^https?:\/\//.test(brokerUrl)) throw new Error('cloud execution requires an HTTP(S) Tool Broker URL')
    if (internalToken.length < 32) throw new Error('cloud execution requires a Tool Broker token of at least 32 characters')
    if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 100 || requestTimeoutMs > 300_000) {
      throw new Error('cloud execution request timeout is invalid')
    }
    this.config = { brokerUrl: brokerUrl.replace(/\/$/, ''), internalToken, requestTimeoutMs }
  }

  currentAuthority(): RunAuthority { return this.ctx.cloudRunContext.require() }

  async call<T>(operation: ExecutionOperation, signal?: AbortSignal, authority: RunAuthority = this.currentAuthority()): Promise<T> {
    const operationId = randomUUID()
    const request: ExecutionRequest = {
      protocolVersion: EXECUTION_PROTOCOL_VERSION,
      operationId,
      authority,
      operation,
    }
    const timeout = AbortSignal.timeout(this.config.requestTimeoutMs)
    const combined = signal === undefined ? timeout : AbortSignal.any([signal, timeout])
    const response = await fetch(`${this.config.brokerUrl}/v1/execute`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${this.config.internalToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify(request),
      signal: combined,
    })
    const bytes = await readBounded(response)
    if (!response.ok) throw new Error(`Tool Broker rejected execution with HTTP ${response.status}`)
    let decoded: unknown
    try { decoded = JSON.parse(bytes.toString('utf8')) as unknown } catch { throw new Error('Tool Broker returned invalid JSON') }
    const parsed = parseExecutionResponse(decoded, operationId)
    if (!parsed.ok) throw new ExecutionRemoteError(parsed.error.code, parsed.error.message, parsed.error.retryable)
    return parsed.result as T
  }
}

export default CloudExecutionClient
