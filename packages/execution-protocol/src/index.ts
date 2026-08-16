import type { RunAuthority } from '@dsh-cloud/run-context'

export const EXECUTION_PROTOCOL_VERSION = 1 as const
export const MAX_RPC_REQUEST_BYTES = 8 * 1024 * 1024
export const MAX_RPC_RESPONSE_BYTES = 8 * 1024 * 1024
export const MAX_PROCESS_OUTPUT_BYTES = 16 * 1024 * 1024

export type FileKind = 'file' | 'directory' | 'symlink' | 'other'

export interface RemoteFileInfo {
  readonly path: string
  readonly name: string
  readonly kind: FileKind
  readonly size: number
  readonly mode: number
  readonly modifiedMs: number
  readonly symlinkTarget?: string
}

export interface RemoteProcessResult {
  readonly pid: number
  readonly exitCode: number | null
  readonly signal: NodeJS.Signals | null
  readonly stdout: string
  readonly stderr: string
  readonly stdoutTruncated: boolean
  readonly stderrTruncated: boolean
}

export interface RemoteProcessSnapshot {
  readonly pid: number
  readonly running: boolean
  readonly stdout: string
  readonly stderr: string
  readonly stdoutOffset: number
  readonly stderrOffset: number
  readonly stdoutLossy: boolean
  readonly stderrLossy: boolean
  readonly exitCode?: number | null
  readonly signal?: NodeJS.Signals | null
}

export interface RemoteTerminalSnapshot {
  readonly pid: number
  readonly running: boolean
  readonly outputBase64: string
  readonly outputOffset: number
  readonly outputLossy: boolean
  readonly exitCode?: number | null
  readonly signal?: NodeJS.Signals | null
  readonly foregroundProcessGroupId?: number
  readonly inputWaiting?: boolean
}

export type ExecutionOperation =
  | { readonly kind: 'fs.resolve'; readonly path: string; readonly cwd?: string }
  | { readonly kind: 'fs.stat'; readonly path: string; readonly follow: boolean }
  | { readonly kind: 'fs.read'; readonly path: string; readonly maxBytes: number }
  | { readonly kind: 'fs.list'; readonly path: string }
  | { readonly kind: 'fs.mkdir'; readonly path: string }
  | { readonly kind: 'fs.write'; readonly path: string; readonly dataBase64: string }
  | { readonly kind: 'fs.rename'; readonly from: string; readonly to: string }
  | { readonly kind: 'fs.remove'; readonly path: string }
  | { readonly kind: 'process.resolve'; readonly command: string; readonly env?: Record<string, string> }
  | {
      readonly kind: 'process.start'
      readonly argv: readonly string[]
      readonly cwd: string
      readonly env?: Record<string, string | undefined>
      readonly stdin: 'ignore' | 'pipe' | { readonly data: string }
      readonly maximumOutputBytes: number
    }
  | { readonly kind: 'process.poll'; readonly pid: number; readonly stdoutOffset: number; readonly stderrOffset: number }
  | { readonly kind: 'process.stdin'; readonly pid: number; readonly dataBase64: string; readonly close: boolean }
  | { readonly kind: 'process.terminate'; readonly pid: number; readonly graceMs: number }
  | { readonly kind: 'process.list' }
  | {
      readonly kind: 'terminal.start'
      readonly argv: readonly string[]
      readonly cwd: string
      readonly env?: Record<string, string>
      readonly rows: number
      readonly cols: number
      readonly maximumOutputBytes: number
    }
  | { readonly kind: 'terminal.poll'; readonly pid: number; readonly outputOffset: number }
  | { readonly kind: 'terminal.input'; readonly pid: number; readonly dataBase64: string }
  | { readonly kind: 'terminal.resize'; readonly pid: number; readonly rows: number; readonly cols: number }
  | { readonly kind: 'terminal.signal'; readonly pid: number; readonly signal: string }
  | { readonly kind: 'terminal.terminate'; readonly pid: number }

export interface ExecutionRequest {
  readonly protocolVersion: typeof EXECUTION_PROTOCOL_VERSION
  readonly operationId: string
  readonly authority: RunAuthority
  readonly operation: ExecutionOperation
}

export interface ExecutionSuccess {
  readonly protocolVersion: typeof EXECUTION_PROTOCOL_VERSION
  readonly operationId: string
  readonly ok: true
  readonly result: unknown
}

export interface ExecutionFailure {
  readonly protocolVersion: typeof EXECUTION_PROTOCOL_VERSION
  readonly operationId: string
  readonly ok: false
  readonly error: {
    readonly code: string
    readonly message: string
    readonly retryable: boolean
  }
}

export type ExecutionResponse = ExecutionSuccess | ExecutionFailure

function record(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`)
  }
  return value as Record<string, unknown>
}

function boundedString(value: unknown, label: string, maximum = 4096): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > maximum || value.includes('\0')) {
    throw new TypeError(`${label} is invalid`)
  }
  return value
}

function safeInteger(value: unknown, label: string, minimum = 0, maximum = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    throw new TypeError(`${label} is invalid`)
  }
  return value as number
}

export function parseExecutionRequest(value: unknown): ExecutionRequest {
  const input = record(value, 'execution request')
  if (input['protocolVersion'] !== EXECUTION_PROTOCOL_VERSION) throw new TypeError('execution protocol version is unsupported')
  boundedString(input['operationId'], 'operation id', 200)
  const authority = record(input['authority'], 'run authority')
  for (const key of ['tenantId', 'workspaceId', 'sessionId', 'runId', 'attemptId']) {
    boundedString(authority[key], `authority ${key}`, 200)
  }
  safeInteger(authority['writerFence'], 'writer fence')
  const operation = record(input['operation'], 'execution operation')
  boundedString(operation['kind'], 'operation kind', 64)
  return value as ExecutionRequest
}

export function parseExecutionResponse(value: unknown, operationId: string): ExecutionResponse {
  const input = record(value, 'execution response')
  if (input['protocolVersion'] !== EXECUTION_PROTOCOL_VERSION || input['operationId'] !== operationId) {
    throw new TypeError('execution response identity is invalid')
  }
  if (input['ok'] !== true && input['ok'] !== false) throw new TypeError('execution response status is invalid')
  if (input['ok'] === false) {
    const error = record(input['error'], 'execution error')
    boundedString(error['code'], 'execution error code', 100)
    boundedString(error['message'], 'execution error message', 2048)
    if (typeof error['retryable'] !== 'boolean') throw new TypeError('execution retryability is invalid')
  }
  return value as ExecutionResponse
}

export function operationFailure(operationId: string, error: unknown): ExecutionFailure {
  const candidate = error as NodeJS.ErrnoException
  const knownCode = typeof candidate.code === 'string' && candidate.code.length <= 100
    ? candidate.code
    : 'EXECUTION_FAILED'
  const message = error instanceof Error ? error.message : 'execution operation failed'
  return {
    protocolVersion: EXECUTION_PROTOCOL_VERSION,
    operationId,
    ok: false,
    error: {
      code: knownCode,
      message: message.replace(/[\u0000-\u001f\u007f]/g, ' ').slice(0, 2048) || 'execution operation failed',
      retryable: knownCode === 'ETIMEDOUT' || knownCode === 'ECONNRESET',
    },
  }
}
