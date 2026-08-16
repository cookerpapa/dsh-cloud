import { createHash } from 'node:crypto'
import { decodeStorageRecord, type SessionEvent, type StorageRecord } from '@deepseek-ai/dsh-session'

export const SESSION_EVENT_ENVELOPE_VERSION = 1

export interface SessionEventEnvelope {
  schemaVersion: typeof SESSION_EVENT_ENVELOPE_VERSION
  namespace: string
  sessionId: string
  seq: number
  seqEnd: number
  records: StorageRecord[]
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

export function parseSessionEventEnvelope(value: unknown): SessionEventEnvelope {
  const candidate = record(value)
  if (candidate?.['schemaVersion'] !== SESSION_EVENT_ENVELOPE_VERSION
    || typeof candidate['namespace'] !== 'string'
    || typeof candidate['sessionId'] !== 'string'
    || !Number.isSafeInteger(candidate['seq'])
    || !Number.isSafeInteger(candidate['seqEnd'])
    || !Array.isArray(candidate['records'])) {
    throw new TypeError('Session event envelope is invalid')
  }
  const records = structuredClone(candidate['records']) as StorageRecord[]
  const events = records.flatMap(item => decodeStorageRecord(item))
  const seq = candidate['seq'] as number
  const seqEnd = candidate['seqEnd'] as number
  if (events[0]?.seq !== seq || events.at(-1)?.seq !== seqEnd) {
    throw new TypeError('Session event envelope range is invalid')
  }
  for (let index = 0; index < events.length; index++) {
    if (events[index]?.seq !== seq + index) throw new TypeError('Session event envelope is not contiguous')
  }
  return {
    schemaVersion: SESSION_EVENT_ENVELOPE_VERSION,
    namespace: candidate['namespace'],
    sessionId: candidate['sessionId'],
    seq,
    seqEnd,
    records,
  }
}

export function envelopeEvents(envelope: SessionEventEnvelope): SessionEvent[] {
  return envelope.records.flatMap(item => decodeStorageRecord(item))
}

export function sessionEventEnvelopeDigest(envelope: SessionEventEnvelope): string {
  return createHash('sha256').update(canonicalJson(envelope)).digest('hex')
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    const encoded = JSON.stringify(value)
    if (encoded === undefined) throw new TypeError('Session event envelope contains an unsupported value')
    return encoded
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, item]) => item !== undefined)
    .sort(([left], [right]) => left.localeCompare(right))
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(',')}}`
}
