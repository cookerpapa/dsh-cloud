import { timingSafeEqual } from 'node:crypto'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import {
  EXECUTION_PROTOCOL_VERSION,
  MAX_RPC_REQUEST_BYTES,
  operationFailure,
  parseExecutionRequest,
  type ExecutionResponse,
} from '@dsh-cloud/execution-protocol'
import { ExecutionEngine } from './engine.js'

function constantTimeEqual(left: string, right: string): boolean {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.byteLength === b.byteLength && timingSafeEqual(a, b)
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const chunk of request) {
    const value = Buffer.from(chunk)
    bytes += value.byteLength
    if (bytes > MAX_RPC_REQUEST_BYTES) throw Object.assign(new Error('request exceeds the byte limit'), { code: 'EFBIG' })
    chunks.push(value)
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown
}

function sendJson(response: ServerResponse, status: number, value: unknown): void {
  const body = Buffer.from(JSON.stringify(value))
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(body.byteLength),
    'cache-control': 'no-store',
  })
  response.end(body)
}

export interface ExecutionAgentOptions {
  readonly engine: ExecutionEngine
  readonly initialBindingSecret?: string
}

/** Create the in-VM credential-free execution service. */
export function createExecutionAgent(options: ExecutionAgentOptions): Server {
  let binding: { activationId: string; secret: string; writerFence: number } | undefined = options.initialBindingSecret === undefined
    ? undefined
    : { activationId: 'local', secret: options.initialBindingSecret, writerFence: 0 }

  return createServer(async (request, response) => {
    try {
      if (request.method === 'GET' && request.url === '/health/live') {
        sendJson(response, 200, { status: 'live', bound: binding !== undefined })
        return
      }
      if (request.method === 'POST' && request.url === '/v1/bind') {
        if (binding !== undefined) {
          sendJson(response, 409, { error: 'execution agent is already bound' })
          return
        }
        const input = await readJson(request) as Record<string, unknown>
        if (typeof input['activationId'] !== 'string' || typeof input['secret'] !== 'string' || !Number.isSafeInteger(input['writerFence'])) {
          sendJson(response, 400, { error: 'binding is invalid' })
          return
        }
        binding = { activationId: input['activationId'], secret: input['secret'], writerFence: input['writerFence'] as number }
        sendJson(response, 200, { status: 'bound' })
        return
      }
      if (request.method === 'POST' && request.url === '/v1/rebind') {
        if (binding === undefined) {
          sendJson(response, 409, { error: 'execution agent is not bound' })
          return
        }
        const currentSecret = request.headers['x-dsh-execution-secret']
        const activationId = request.headers['x-dsh-activation-id']
        const input = await readJson(request) as Record<string, unknown>
        if (
          typeof currentSecret !== 'string' || !constantTimeEqual(currentSecret, binding.secret) ||
          activationId !== binding.activationId || typeof input['secret'] !== 'string' ||
          !Number.isSafeInteger(input['writerFence']) || (input['writerFence'] as number) <= binding.writerFence
        ) {
          sendJson(response, 403, { error: 'execution rebind authority is stale or invalid' })
          return
        }
        binding = { ...binding, secret: input['secret'], writerFence: input['writerFence'] as number }
        sendJson(response, 200, { status: 'rebound' })
        return
      }
      if (request.method !== 'POST' || request.url !== '/v1/execute') {
        sendJson(response, 404, { error: 'not found' })
        return
      }
      if (binding === undefined) {
        sendJson(response, 503, { error: 'execution agent is not bound' })
        return
      }
      const secret = request.headers['x-dsh-execution-secret']
      const activationId = request.headers['x-dsh-activation-id']
      const fence = Number(request.headers['x-dsh-writer-fence'])
      if (typeof secret !== 'string' || !constantTimeEqual(secret, binding.secret) || activationId !== binding.activationId || fence !== binding.writerFence) {
        sendJson(response, 403, { error: 'execution authority is stale or invalid' })
        return
      }
      const input = await readJson(request)
      let operationId = 'invalid'
      let result: ExecutionResponse
      try {
        const parsed = parseExecutionRequest(input)
        operationId = parsed.operationId
        if (parsed.authority.writerFence !== binding.writerFence) throw Object.assign(new Error('request carries a stale writer fence'), { code: 'ESTALE' })
        result = {
          protocolVersion: EXECUTION_PROTOCOL_VERSION,
          operationId,
          ok: true,
          result: await options.engine.execute(parsed.operation),
        }
      } catch (error: unknown) {
        result = operationFailure(operationId, error)
      }
      sendJson(response, 200, result)
    } catch (error: unknown) {
      sendJson(response, 500, { error: error instanceof Error ? error.message : 'execution agent failure' })
    }
  })
}
