import type { ClaimedRun } from '@dsh-cloud/control-store'
import type { RunExecutionBackend } from '@dsh-cloud/run-queue'

interface ClientRequestEnvelope { type: 'client-request'; rpcId: string; method: string; payload: unknown }

function requestEnvelope(value: unknown): ClientRequestEnvelope {
  if (value === null || typeof value !== 'object') throw new TypeError('Run request is not an RPC envelope')
  const candidate = value as Record<string, unknown>
  if (candidate['type'] !== 'client-request' || candidate['method'] !== 'session.prompt' || candidate['payload'] === null || typeof candidate['payload'] !== 'object') {
    throw new TypeError('Run request must be a session.prompt client request')
  }
  if (typeof candidate['rpcId'] !== 'string' || candidate['rpcId'].length === 0) throw new TypeError('Run request rpcId is invalid')
  return { type: 'client-request', rpcId: candidate['rpcId'], method: 'session.prompt', payload: candidate['payload'] }
}

/** Dispatches durable Runs into one trusted, process-local official DSH Host. */
export class DshHttpRunBackend implements RunExecutionBackend {
  private readonly baseUrl: string

  constructor(baseUrl: string) { this.baseUrl = new URL(baseUrl).toString().replace(/\/$/, '') }

  async dispatch(run: ClaimedRun, signal: AbortSignal): Promise<unknown> {
    const envelope = requestEnvelope(run.request)
    const response = await fetch(`${this.baseUrl}/api/session.prompt`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(envelope), signal,
    })
    if (!response.ok) throw Object.assign(new Error(`DSH Host rejected prompt with HTTP ${response.status}`), { code: 'host_transport_error' })
    const result = await response.json() as { result?: { ok?: boolean; error?: { code?: string; message?: string } } }
    if (result.result?.ok !== true) throw Object.assign(new Error(result.result?.error?.message ?? 'DSH Host rejected prompt'), { code: result.result?.error?.code ?? 'host_prompt_rejected' })
    return result
  }

  async cancel(run: ClaimedRun): Promise<'accepted' | 'absent'> {
    const response = await fetch(`${this.baseUrl}/api/session.cancel`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'client-request', rpcId: `cancel-${run.runId}`, method: 'session.cancel', payload: { sessionId: run.sessionId } }),
      signal: AbortSignal.timeout(10_000),
    })
    if (!response.ok) throw new Error(`DSH Host cancellation failed with HTTP ${response.status}`)
    const result = await response.json() as { result?: { ok?: boolean; error?: { code?: string; message?: string } } }
    if (result.result?.ok === true) return 'accepted'
    if (result.result?.error?.code === 'session-not-found') return 'absent'
    throw new Error(result.result?.error?.message ?? 'DSH Host rejected cancellation')
  }
}
