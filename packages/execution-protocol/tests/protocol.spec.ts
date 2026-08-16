import { describe, expect, it } from 'vitest'
import { cloudIdentifier, writerFence } from '@dsh-cloud/run-context'
import { EXECUTION_PROTOCOL_VERSION, parseExecutionRequest } from '../src/index.js'

const authority = {
  tenantId: cloudIdentifier('TenantId', 'tenant'),
  workspaceId: cloudIdentifier('WorkspaceId', 'workspace'),
  sessionId: cloudIdentifier('SessionId', 'session'),
  runId: cloudIdentifier('RunId', 'run'),
  attemptId: cloudIdentifier('AttemptId', 'attempt'),
  writerFence: writerFence(1),
}

function request(operation: unknown): unknown {
  return { protocolVersion: EXECUTION_PROTOCOL_VERSION, operationId: 'operation', authority, operation }
}

describe('execution protocol validation', () => {
  it('accepts empty file and stdin payloads used by the upstream providers', () => {
    expect(parseExecutionRequest(request({ kind: 'fs.write', path: '/workspace/empty', dataBase64: '' })))
      .toMatchObject({ operation: { kind: 'fs.write' } })
    expect(parseExecutionRequest(request({ kind: 'process.stdin', pid: 1, dataBase64: '', close: true })))
      .toMatchObject({ operation: { kind: 'process.stdin' } })
  })

  it('rejects unknown and structurally incomplete operations at the VM boundary', () => {
    expect(() => parseExecutionRequest(request({ kind: 'host.mount', path: '/' }))).toThrow(/unsupported/)
    expect(() => parseExecutionRequest(request({ kind: 'process.start', argv: [], cwd: '/workspace' }))).toThrow(/argv/)
    expect(() => parseExecutionRequest(request({ kind: 'terminal.resize', pid: 1, rows: 0, cols: 80 }))).toThrow(/rows/)
  })
})
