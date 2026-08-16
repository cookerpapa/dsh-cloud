import { createHash } from 'node:crypto'
import { Context, Service } from '@deepseek-ai/cordis'
import { decodeStorageRecord, type SessionEvent, type StorageRecord } from '@deepseek-ai/dsh-session'

export const SESSION_EVENT_ENVELOPE_VERSION = 2

export interface SessionEventWriterAuthority {
  readonly runId: string
  readonly attemptId: string
  readonly writerFence: number
}

/** One contiguous native DSH event batch before Turn sealing. */
export interface SessionEventEnvelope {
  readonly schemaVersion: typeof SESSION_EVENT_ENVELOPE_VERSION
  readonly namespace: string
  readonly sessionId: string
  readonly seq: number
  readonly seqEnd: number
  readonly records: StorageRecord[]
  readonly authority?: SessionEventWriterAuthority
}

/** Opaque durable address returned by a live-log Provider. */
export interface SessionLiveLocation {
  readonly seq: number
  readonly seqEnd: number
  /** Provider-owned JSON value; SessionPersistence stores but never interprets it. */
  readonly locator: Readonly<Record<string, string | number | boolean>>
  readonly digest: string
}

export interface SessionLiveLogProvisioning {
  readonly partitions: number
  readonly replicationFactor: number
  readonly retentionMs: number
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    sessionLiveLog: SessionLiveLog
    sessionLiveProjection: SessionLiveProjection
  }
}

/**
 * Provider-neutral durable suffix of a DSH Session.
 *
 * A Provider may use Kafka, Pulsar, JetStream, or another ordered log. The
 * SessionPersistence plugin stores only the returned opaque location and never
 * relies on provider-specific clients.
 */
export abstract class SessionLiveLog extends Service {
  constructor(ctx: Context) {
    super(ctx, 'sessionLiveLog')
  }

  abstract provision(options: SessionLiveLogProvisioning): Promise<void>
  abstract publish(envelope: SessionEventEnvelope): Promise<SessionLiveLocation>
  abstract read(
    namespace: string,
    sessionId: string,
    locations: readonly SessionLiveLocation[],
    signal?: AbortSignal,
  ): Promise<SessionEventEnvelope[]>
  abstract checkHealth(): Promise<void>
}

/** Rebuildable low-latency projection used only after the durable log ACKs. */
export abstract class SessionLiveProjection extends Service {
  constructor(ctx: Context) {
    super(ctx, 'sessionLiveProjection')
  }

  abstract watermark(namespace: string, sessionId: string): Promise<number | undefined>
  abstract reset(namespace: string, sessionId: string, through: number): Promise<void>
  abstract append(envelope: SessionEventEnvelope, digest: string): Promise<void>
  abstract checkHealth(): Promise<void>
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
  const authority = candidate['authority']
  const authorityRecord = authority === undefined ? undefined : record(authority)
  if (authority !== undefined && (
    authorityRecord === undefined
    || typeof authorityRecord['runId'] !== 'string'
    || typeof authorityRecord['attemptId'] !== 'string'
    || !Number.isSafeInteger(authorityRecord['writerFence'])
    || (authorityRecord['writerFence'] as number) < 0
  )) {
    throw new TypeError('Session event envelope authority is invalid')
  }
  return {
    schemaVersion: SESSION_EVENT_ENVELOPE_VERSION,
    namespace: candidate['namespace'],
    sessionId: candidate['sessionId'],
    seq,
    seqEnd,
    records,
    ...(authority === undefined ? {} : {
      authority: {
        runId: authorityRecord!['runId'] as string,
        attemptId: authorityRecord!['attemptId'] as string,
        writerFence: authorityRecord!['writerFence'] as number,
      },
    }),
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
