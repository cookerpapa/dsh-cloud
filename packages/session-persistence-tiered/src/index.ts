import { createHash, randomUUID } from 'node:crypto'
import { gzipSync, gunzipSync } from 'node:zlib'
import { Context } from '@deepseek-ai/cordis'
import {
  SESSION_EVENT_ENVELOPE_VERSION,
  envelopeEvents,
  type SessionEventEnvelope,
  type SessionLiveLocation,
  type SessionLiveLog,
  type SessionLiveProjection,
} from '@dsh-cloud/session-live'
import {
  Session,
  decodeStorageRecord,
  createSessionRuntimeBaseline,
  packChunkRuns,
  type SessionEvent,
  type SessionHeader,
  type SessionId,
  type SessionPreparation,
  type SessionRuntimeBaseline,
  type StorageRecord,
} from '@deepseek-ai/dsh-session'
import {
  DEFAULT_PREPARED_SESSION_CACHE_SIZE,
  DEFAULT_WRITE_BATCH_MAX_DELAY_MS,
  MAX_WRITE_BATCH_DELAY_MS,
  PersistenceCoordinator,
  SessionPersistence,
  SessionPersistenceRevision,
  type PersistenceBackend,
  type SessionInspection,
  type SessionLocation,
  type SessionPersistenceSnapshot,
  type StoredPrefix,
  type StoredSuffix,
} from '@deepseek-ai/dsh-session-persistence'
import z from '@deepseek-ai/schemastery'
import { Pool, type PoolClient } from 'pg'
import type CloudRunContext from '@dsh-cloud/run-context'
import type { RunAuthority } from '@dsh-cloud/run-context'
export {
  envelopeEvents,
  parseSessionEventEnvelope,
  sessionEventEnvelopeDigest,
  type SessionEventEnvelope,
} from '@dsh-cloud/session-live'

const SCHEMA_VERSION = 7
const SQL_SCHEMA = 'dsh_cloud'
const DEFAULT_NAMESPACE = 'local'
const SEGMENT_CODEC = 'dsh-storage-records+json+gzip-v1'
const RUNTIME_BASELINE_CODEC = 'dsh-runtime-baseline+json+gzip-v1'

interface SessionRow {
  header: SessionHeader
  incarnation: string
  revision: string
  next_seq: string
  sealed_through: string
  projected_through: string
  writer_fence: string
  writer_attempt_id: string | null
}

interface EventBatchRow {
  seq: string
  seq_end: string
  provider_location: unknown
  digest: string
}

interface PackedEventRow {
  seq: string
  seq_end: string
  event: StorageRecord
}

interface SegmentRow {
  seq: string
  seq_end: string
  codec: string
  payload: Buffer
  sha256: string
}

interface EncodedSegment {
  seq: number
  seqEnd: number
  payload: Buffer
  sha256: string
}

interface RuntimeBaselineRow {
  through_seq: string
  event_count: string
  codec: string
  payload: Buffer
  sha256: string
}

interface SessionMarker {
  seq: number
  type: 'user/message' | 'turn/end'
  rpcId?: string
  reason?: unknown
}

interface WriterAuthority {
  readonly fence: number
  readonly attemptId?: string
  readonly cloud?: RunAuthority
}

/** Configuration for the tiered SessionPersistence provider. */
export interface Config {
  /** PostgreSQL URL; credentials remain in the trusted Cloud Host. */
  connectionString: string
  /** Isolates one deployment while request-scoped tenancy is added at the host plane. */
  namespace?: string
  /** Fail closed unless an orchestrator has installed RunAttempt authority. */
  requireWriterAuthority?: boolean
  /** Validate the Run/Attempt lease in the cloud control schema in the append transaction. */
  validateControlAuthority?: boolean
  /** Shared RunAttempt lease used by Worker reconciliation and execution admission. */
  attemptLeaseSeconds?: number
  /** Maximum connections held by this Host process. */
  maxPoolSize?: number
  /** Maximum number of prepared cold Sessions retained by the upstream coordinator. */
  preparedSessionCacheSize?: number
  /** Fixed event coalescing window used before an append batch is committed. */
  writeBatchMaxDelayMs?: number
  /** Partition count passed to the injected durable live-log Provider. */
  liveLogPartitions?: number
  /** Replication factor passed to the injected durable live-log Provider. */
  liveLogReplicationFactor?: number
  /** Retention for unsealed Session tails and bounded live replay. */
  liveLogRetentionMs?: number
}

/** A stale or conflicting Worker attempted to publish Session state. */
export class StaleSessionWriterError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'StaleSessionWriterError'
  }
}

/** Convert a PostgreSQL bigint while refusing silent precision loss. */
function safeInteger(value: string, field: string): number {
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`stored ${field} is not a non-negative safe integer`)
  }
  return parsed
}

function safeWatermark(value: string, field: string): number {
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < -1) {
    throw new Error(`stored ${field} is not a safe event watermark`)
  }
  return parsed
}

/** The revision token used by both full and lightweight reads. */
function postgresRevision(
  storeId: string,
  namespace: string,
  row: Pick<SessionRow, 'incarnation' | 'revision'>,
): ReturnType<typeof SessionPersistenceRevision> {
  return SessionPersistenceRevision(
    `postgres:${storeId}:namespace:${namespace}:incarnation:${row.incarnation}:revision:${row.revision}`,
  )
}

/** Reject malformed identifiers before they can cross an SQL namespace boundary. */
function deploymentNamespace(input: string | undefined): string {
  const value = input?.trim() || DEFAULT_NAMESPACE
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(value)) {
    throw new TypeError('namespace must be 1-128 characters from [a-zA-Z0-9._-] and start with an alphanumeric')
  }
  return value
}

/** Assert that an event batch is non-gapped before one SQL statement publishes it. */
function assertContiguous(events: readonly SessionEvent[], expected: number): void {
  for (let index = 0; index < events.length; index++) {
    const event = events[index]
    if (event === undefined || event.seq !== expected + index) {
      throw new Error(
        `session append is not contiguous: expected seq ${expected + index}, got ${String(event?.seq)}`,
      )
    }
  }
}

/**
 * Validate a detached row sequence and identify an uncommitted physical tail.
 * PostgreSQL transactions should make such a tail impossible; retaining the
 * check keeps the official cold-recovery contract fail-safe under manual data
 * damage or an interrupted administrative import.
 */
function storageRows(events: readonly SessionEvent[]): Array<{ seq: number; seqEnd: number; record: StorageRecord }> {
  return packChunkRuns(events).map(record => {
    const expanded = decodeStorageRecord(record)
    const first = expanded[0]
    const last = expanded.at(-1)
    if (first === undefined || last === undefined) throw new Error('packed session record contained no events')
    return { seq: first.seq, seqEnd: last.seq, record }
  })
}

function encodeSegment(events: readonly SessionEvent[]): EncodedSegment {
  const first = events[0]
  const last = events.at(-1)
  if (first === undefined || last === undefined) throw new Error('cannot encode an empty Session segment')
  assertContiguous(events, first.seq)
  const payload = gzipSync(Buffer.from(JSON.stringify(storageRows(events).map(row => row.record))), { level: 6 })
  return {
    seq: first.seq,
    seqEnd: last.seq,
    payload,
    sha256: createHash('sha256').update(payload).digest('hex'),
  }
}

function decodeSegment(row: SegmentRow): SessionEvent[] {
  if (row.codec !== SEGMENT_CODEC) throw new Error(`unsupported Session segment codec "${row.codec}"`)
  const digest = createHash('sha256').update(row.payload).digest('hex')
  if (digest !== row.sha256) throw new Error('corrupt Session segment digest')
  let records: unknown
  try {
    records = JSON.parse(gunzipSync(row.payload).toString('utf8'))
  } catch (error) {
    throw new Error('corrupt compressed Session segment', { cause: error })
  }
  if (!Array.isArray(records)) throw new Error('corrupt Session segment record collection')
  const events = records.flatMap(record => decodeStorageRecord(record as StorageRecord))
  const seq = safeInteger(row.seq, 'segment seq')
  const seqEnd = safeInteger(row.seq_end, 'segment end seq')
  if (events[0]?.seq !== seq || events.at(-1)?.seq !== seqEnd) {
    throw new Error(`corrupt Session segment range ${seq}-${seqEnd}`)
  }
  assertContiguous(events, seq)
  return events
}

function encodeRuntimeBaseline(events: readonly SessionEvent[]): EncodedSegment {
  const baseline = createSessionRuntimeBaseline(events)
  const payload = gzipSync(Buffer.from(JSON.stringify(baseline)), { level: 6 })
  return {
    seq: 0,
    seqEnd: baseline.throughSeq,
    payload,
    sha256: createHash('sha256').update(payload).digest('hex'),
  }
}

function decodeRuntimeBaseline(row: RuntimeBaselineRow): SessionRuntimeBaseline {
  if (row.codec !== RUNTIME_BASELINE_CODEC) {
    throw new Error(`unsupported Session runtime baseline codec "${row.codec}"`)
  }
  const digest = createHash('sha256').update(row.payload).digest('hex')
  if (digest !== row.sha256) throw new Error('corrupt Session runtime baseline digest')
  let value: unknown
  try {
    value = JSON.parse(gunzipSync(row.payload).toString('utf8'))
  } catch (error) {
    throw new Error('corrupt compressed Session runtime baseline', { cause: error })
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('corrupt Session runtime baseline object')
  }
  const baseline = value as SessionRuntimeBaseline
  const throughSeq = safeInteger(row.through_seq, 'runtime baseline through seq')
  const eventCount = safeInteger(row.event_count, 'runtime baseline event count')
  if (eventCount !== throughSeq + 1 || baseline.throughSeq !== throughSeq) {
    throw new Error(`corrupt Session runtime baseline range through ${throughSeq}`)
  }
  return baseline
}

function completedCompaction(events: readonly SessionEvent[]): boolean {
  return events.some(event => {
    // Compaction events declaration-merge into SessionEventMap only when the
    // optional compaction plugin is installed. Persistence remains plugin-
    // agnostic and recognizes the stable wire type without importing it.
    if (String(event.type) !== 'compaction/end') return false
    const data = object(event.data)
    return data !== undefined && data['error'] === undefined
  })
}

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function markers(events: readonly SessionEvent[]): SessionMarker[] {
  const output: SessionMarker[] = []
  for (const event of events) {
    if (event.type === 'user/message') {
      const rpcId = object(object(event.data)?.['source'])?.['rpcId']
      output.push({
        seq: event.seq,
        type: 'user/message',
        ...(typeof rpcId === 'string' && rpcId.length > 0 ? { rpcId } : {}),
      })
    } else if (event.type === 'turn/end') {
      output.push({ seq: event.seq, type: 'turn/end', reason: object(event.data)?.['reason'] ?? null })
    }
  }
  return output
}

function scanRows(rows: readonly PackedEventRow[], expectedStart = 0): { events: SessionEvent[]; tornFrom?: number } {
  const expanded: SessionEvent[] = []
  for (const row of rows) {
    const seq = safeInteger(row.seq, 'storage row seq')
    const seqEnd = safeInteger(row.seq_end, 'storage row end seq')
    const events = decodeStorageRecord(row.event)
    if (events[0]?.seq !== seq || events.at(-1)?.seq !== seqEnd) {
      throw new Error(`corrupt session log: packed row range ${seq}-${seqEnd} is inconsistent`)
    }
    expanded.push(...events)
  }
  let lastTurnEnd = -1
  for (let index = expanded.length - 1; index >= 0; index--) {
    if (expanded[index]?.type === 'turn/end') {
      lastTurnEnd = index
      break
    }
  }

  const events: SessionEvent[] = []
  for (let index = 0; index < expanded.length; index++) {
    const event = expanded[index]
    const seq = event?.seq ?? -1
    const validEnvelope = event !== null
      && typeof event === 'object'
      && Number.isSafeInteger(event.seq)
      && event.seq === seq
      && typeof event.type === 'string'
    if (!validEnvelope || seq !== expectedStart + index) {
      if (index <= lastTurnEnd) {
        throw new Error(
          `corrupt session log: expected seq ${expectedStart + index}, got ${String(seq)} (${String(event?.type)})`,
        )
      }
      return { events, tornFrom: expectedStart + index }
    }
    events.push(structuredClone(event))
  }
  return { events }
}

/** Tiered implementation of DSH's append-only Session log. */
class TieredSessionPersistence extends SessionPersistence implements PersistenceBackend<number> {
  static inject = ['sessions', 'sessionLiveLog', 'sessionLiveProjection']

  static Config: z<Config> = z.object({
    connectionString: z.string().required(),
    namespace: z.string().default(DEFAULT_NAMESPACE),
    requireWriterAuthority: z.boolean().default(false),
    validateControlAuthority: z.boolean().default(false),
    attemptLeaseSeconds: z.number().step(1).min(1).max(3600).default(20),
    maxPoolSize: z.number().step(1).min(1).max(200).default(10),
    preparedSessionCacheSize: z.number().step(1).min(1).default(DEFAULT_PREPARED_SESSION_CACHE_SIZE),
    writeBatchMaxDelayMs: z.number().step(1).min(1).max(MAX_WRITE_BATCH_DELAY_MS)
      .default(DEFAULT_WRITE_BATCH_MAX_DELAY_MS),
    liveLogPartitions: z.number().step(1).min(1).max(10_000).default(12),
    liveLogReplicationFactor: z.number().step(1).min(1).max(9).default(1),
    liveLogRetentionMs: z.number().step(1).min(60_000).default(2_592_000_000),
  })

  override readonly supportsRawArtifacts = false
  override readonly name = 'session-persistence-tiered'

  private readonly pool: Pool
  private readonly namespace: string
  private readonly requireWriterAuthority: boolean
  private readonly validateControlAuthority: boolean
  private readonly attemptLeaseSeconds: number
  private readonly ready: Promise<void>
  private readonly coordinator: PersistenceCoordinator<number>
  private readonly tail: SessionLiveLog
  private readonly projection: SessionLiveProjection
  private readonly boundAuthorities = new Map<string, Readonly<RunAuthority>>()
  private storeId = ''

  constructor(ctx: Context, readonly config: Config) {
    super(ctx)
    if (config.connectionString.trim().length === 0) {
      throw new TypeError('connectionString must not be empty')
    }
    this.namespace = deploymentNamespace(config.namespace)
    this.requireWriterAuthority = config.requireWriterAuthority ?? false
    this.validateControlAuthority = config.validateControlAuthority ?? false
    this.attemptLeaseSeconds = config.attemptLeaseSeconds ?? 20
    this.pool = new Pool({
      connectionString: config.connectionString,
      max: config.maxPoolSize ?? 10,
      application_name: 'dsh-cloud-session-persistence',
    })
    this.tail = ctx.sessionLiveLog
    this.projection = ctx.sessionLiveProjection
    this.ready = this.initialize()
    this.coordinator = new PersistenceCoordinator<number>(ctx, this, {
      preparedSessionCacheSize: config.preparedSessionCacheSize
        ?? DEFAULT_PREPARED_SESSION_CACHE_SIZE,
      writeBatchMaxDelayMs: config.writeBatchMaxDelayMs
        ?? DEFAULT_WRITE_BATCH_MAX_DELAY_MS,
    })
  }

  locate(_meta: SessionHeader): SessionLocation | undefined {
    return undefined
  }

  create(meta: SessionHeader): Promise<void> {
    return this.coordinator.create(meta)
  }

  append(id: SessionId, events: readonly SessionEvent[]): Promise<void> {
    return this.coordinator.append(id, events)
  }

  /**
   * Carry the latest RunAttempt identity into DSH's detached write-behind task.
   * This does not authorize a write: appendBatch validates the Lease/Fence in
   * PostgreSQL again before committing any event.
   */
  bindRunAuthority(authority: RunAuthority): void {
    this.rememberAuthority(String(authority.sessionId), authority)
  }

  private rememberAuthority(sessionId: string, authority: RunAuthority): void {
    const existing = this.boundAuthorities.get(sessionId)
    // A detached PersistenceCoordinator task can still carry the
    // AsyncLocalStorage context of the previous Run.  Never let that older
    // task move the process-local hint backwards after a follow-up Run has
    // already installed a higher Workspace fence.
    if (existing !== undefined && existing.writerFence > authority.writerFence) return
    if (existing !== undefined
      && existing.writerFence === authority.writerFence
      && existing.attemptId !== authority.attemptId) return
    this.boundAuthorities.delete(sessionId)
    this.boundAuthorities.set(sessionId, Object.freeze({ ...authority }))
    while (this.boundAuthorities.size > 10_000) {
      const oldest = this.boundAuthorities.keys().next().value as string | undefined
      if (oldest === undefined) break
      this.boundAuthorities.delete(oldest)
    }
  }

  override prepare(id: SessionId, signal?: AbortSignal): Promise<SessionPreparation> {
    return this.coordinator.prepare(id, signal)
  }

  async load(id: SessionId): Promise<SessionInspection> {
    const projected = await this.coordinator.load(id)
    return this.canonicalInspection(id, projected)
  }

  async inspect(id: SessionId, signal?: AbortSignal): Promise<SessionInspection> {
    const projected = await this.coordinator.inspect(id, signal)
    return this.canonicalInspection(id, projected, signal)
  }

  readFrom(
    id: SessionId,
    fromSeq: number,
    signal?: AbortSignal,
  ): Promise<{ meta: SessionHeader; events: SessionEvent[] }> {
    return this.coordinator.readFrom(id, fromSeq, signal)
  }

  async loadStored(id: SessionId, signal?: AbortSignal): Promise<StoredPrefix<number> | undefined> {
    signal?.throwIfAborted()
    await this.ready
    return this.readTransaction(async (client) => {
      signal?.throwIfAborted()
      const row = await this.sessionRow(client, id)
      if (row === undefined) return undefined
      const prepared = await this.loadRuntimePreparation(client, id, row.header)
      signal?.throwIfAborted()
      const expectedStart = prepared.runtimeBaseline === undefined
        ? 0
        : prepared.runtimeBaseline.throughSeq + 1
      const scanned = scanRows(storageRows(prepared.events).map(item => ({
        seq: String(item.seq),
        seq_end: String(item.seqEnd),
        event: item.record,
      })), expectedStart)
      return {
        meta: structuredClone(row.header),
        events: scanned.events,
        revision: postgresRevision(this.storeId, this.namespace, row),
        ...prepared.runtimeBaseline === undefined
          ? {}
          : { runtimeBaseline: prepared.runtimeBaseline },
        ...scanned.tornFrom === undefined ? {} : { tornMarker: scanned.tornFrom },
      }
    })
  }

  async readStoredRevision(
    id: SessionId,
    signal?: AbortSignal,
  ): Promise<ReturnType<typeof SessionPersistenceRevision> | undefined> {
    signal?.throwIfAborted()
    await this.ready
    const result = await this.pool.query<Pick<SessionRow, 'incarnation' | 'revision'>>(`
      SELECT incarnation::text, revision::text
      FROM ${SQL_SCHEMA}.sessions
      WHERE namespace = $1 AND id = $2
    `, [this.namespace, id])
    signal?.throwIfAborted()
    const row = result.rows[0]
    return row === undefined ? undefined : postgresRevision(this.storeId, this.namespace, row)
  }

  async loadStoredFrom(
    id: SessionId,
    fromSeq: number,
    signal?: AbortSignal,
  ): Promise<StoredSuffix | undefined> {
    signal?.throwIfAborted()
    await this.ready
    return this.readTransaction(async (client) => {
      const row = await this.sessionRow(client, id)
      if (row === undefined) return undefined
      const nextSeq = safeInteger(row.next_seq, 'next seq')
      const events = (await this.loadPhysicalEvents(client, id, fromSeq)).filter(event => event.seq >= fromSeq)
      signal?.throwIfAborted()
      for (let index = 0; index < events.length; index++) {
        if (events[index]?.seq !== fromSeq + index) throw new Error(`corrupt session log: suffix gap at seq ${fromSeq + index}`)
      }
      if (fromSeq < nextSeq && events.length !== nextSeq - fromSeq) {
        throw new Error(`corrupt session log: suffix ends before stored next seq ${nextSeq}`)
      }
      return { meta: structuredClone(row.header), events }
    })
  }

  async appendBatch(
    meta: SessionHeader,
    events: readonly SessionEvent[],
    isMaterialized: boolean,
  ): Promise<void> {
    if (events.length === 0) return
    await this.ready
    const authority = this.writerAuthority(meta.id)
    await this.writeTransaction(async (client) => {
      await this.assertControlAuthority(client, authority, meta)
      let row: SessionRow
      if (!isMaterialized) {
        const inserted = await client.query<SessionRow>(`
          INSERT INTO ${SQL_SCHEMA}.sessions
            (namespace, id, header, incarnation, revision, next_seq, writer_fence, writer_attempt_id)
          VALUES ($1, $2, $3::jsonb, $4, 0, 0, $5, $6)
          ON CONFLICT (namespace, id) DO NOTHING
          RETURNING header, incarnation::text, revision::text, next_seq::text,
                    sealed_through::text, projected_through::text,
                    writer_fence::text, writer_attempt_id
        `, [
          this.namespace,
          meta.id,
          JSON.stringify(meta),
          randomUUID(),
          authority.fence,
          authority.attemptId ?? null,
        ])
        const created = inserted.rows[0]
        if (created === undefined) {
          throw new Error(`session "${meta.id}" was concurrently materialized`)
        }
        row = created
      } else {
        const locked = await client.query<SessionRow>(`
          SELECT header, incarnation::text, revision::text, next_seq::text,
                 sealed_through::text, projected_through::text,
                 writer_fence::text, writer_attempt_id
          FROM ${SQL_SCHEMA}.sessions
          WHERE namespace = $1 AND id = $2
          FOR UPDATE
        `, [this.namespace, meta.id])
        const existing = locked.rows[0]
        if (existing === undefined) throw new Error(`session "${meta.id}" is not materialized`)
        this.assertWriter(existing, authority, meta.id)
        row = existing
      }

      const expected = safeInteger(row.next_seq, 'next seq')
      const first = events[0]!.seq
      const last = events.at(-1)!.seq
      if (first < expected) {
        if (last >= expected) throw new Error('Session append partially overlaps committed history')
        const stored = await this.loadPhysicalEvents(client, meta.id, first)
        const replay = stored.filter(event => event.seq >= first && event.seq <= last)
        if (JSON.stringify(replay) !== JSON.stringify(events)) {
          throw new Error('Session append conflicts with already committed history')
        }
        return
      }
      assertContiguous(events, expected)
      const priorSealed = safeWatermark(row.sealed_through, 'sealed through')
      const priorProjected = safeWatermark(row.projected_through, 'projected through')
      await this.reconcileProjection(client, meta.id, priorSealed, priorProjected)
      await this.publishLiveBatches(client, meta.id, events, authority)
      await this.assertControlAuthority(client, authority, meta)
      const sealedThrough = await this.sealCompletedTurns(
        client,
        meta.id,
        priorSealed,
      )
      await client.query(`
        UPDATE ${SQL_SCHEMA}.sessions
        SET next_seq = $3,
            revision = revision + 1,
            writer_fence = $4,
            writer_attempt_id = $5,
            sealed_through = $6,
            projected_through = $7
        WHERE namespace = $1 AND id = $2
      `, [
        this.namespace,
        meta.id,
        expected + events.length,
        authority.fence,
        authority.attemptId ?? null,
        sealedThrough,
        expected + events.length - 1,
      ])
      if (completedCompaction(events)) {
        // This row is an accelerator, never part of the native Session commit.
        // A cache construction failure must not turn a valid DSH Compaction
        // into a persistence failure or leave the transaction aborted.
        await client.query('SAVEPOINT dsh_restore_checkpoint')
        try {
          await this.writeRuntimeBaseline(client, meta.id, expected + events.length)
          await client.query('RELEASE SAVEPOINT dsh_restore_checkpoint')
        } catch (error) {
          await client.query('ROLLBACK TO SAVEPOINT dsh_restore_checkpoint')
          await client.query('RELEASE SAVEPOINT dsh_restore_checkpoint')
          this.ctx.logger.warn(
            `session "${meta.id}": runtime baseline construction failed; canonical Compaction remains authoritative: ${String(error)}`,
          )
        }
      }
      await client.query(`SELECT pg_notify('dsh_cloud_session_projection',$1)`, [String(meta.id)])
    })
  }

  async commitRepair(
    meta: SessionHeader,
    tornMarker: number | undefined,
    closers: readonly SessionEvent[],
  ): Promise<void> {
    if (tornMarker === undefined && closers.length === 0) return
    await this.ready
    const authority = this.writerAuthority(meta.id)
    await this.writeTransaction(async (client) => {
      await this.assertControlAuthority(client, authority, meta)
      const locked = await client.query<SessionRow>(`
        SELECT header, incarnation::text, revision::text, next_seq::text,
               sealed_through::text, projected_through::text,
               writer_fence::text, writer_attempt_id
        FROM ${SQL_SCHEMA}.sessions
        WHERE namespace = $1 AND id = $2
        FOR UPDATE
      `, [this.namespace, meta.id])
      const row = locked.rows[0]
      if (row === undefined) throw new Error(`session "${meta.id}" is not materialized`)
      this.assertWriter(row, authority, meta.id)
      const storedNext = safeInteger(row.next_seq, 'next seq')
      const sealedThrough = safeWatermark(row.sealed_through, 'sealed through')
      const projectedThrough = safeWatermark(row.projected_through, 'projected through')
      const appendAt = tornMarker ?? storedNext
      if (appendAt > storedNext) throw new Error('repair marker exceeds the stored session length')
      assertContiguous(closers, appendAt)

      if (tornMarker !== undefined) {
        throw new Error('Kafka Session batches are atomic and cannot expose a repairable torn tail')
      }
      await this.reconcileProjection(client, meta.id, sealedThrough, projectedThrough)
      if (closers.length > 0) {
        await this.publishLiveBatches(client, meta.id, closers, authority)
      }
      await this.assertControlAuthority(client, authority, meta)
      const repairedSealedThrough = await this.sealCompletedTurns(client, meta.id, sealedThrough)
      await client.query(`
        UPDATE ${SQL_SCHEMA}.sessions
        SET next_seq = $3,
            revision = revision + 1,
            writer_fence = $4,
            writer_attempt_id = $5,
            sealed_through = $6,
            projected_through = $7
        WHERE namespace = $1 AND id = $2
      `, [
        this.namespace,
        meta.id,
        appendAt + closers.length,
        authority.fence,
        authority.attemptId ?? null,
        repairedSealedThrough,
        appendAt + closers.length - 1,
      ])
      await client.query(`SELECT pg_notify('dsh_cloud_session_projection',$1)`, [String(meta.id)])
    })
  }

  async list(signal?: AbortSignal): Promise<SessionHeader[]> {
    signal?.throwIfAborted()
    await this.ready
    const result = await this.pool.query<{ header: SessionHeader }>(`
      SELECT header
      FROM ${SQL_SCHEMA}.sessions
      WHERE namespace = $1
      ORDER BY (header->>'createdAt')::bigint DESC, id ASC
    `, [this.namespace])
    signal?.throwIfAborted()
    return result.rows.map(row => structuredClone(row.header))
  }

  async listSnapshots(signal?: AbortSignal): Promise<SessionPersistenceSnapshot[]> {
    signal?.throwIfAborted()
    await this.ready
    const result = await this.pool.query<Pick<SessionRow, 'header' | 'incarnation' | 'revision'>>(`
      SELECT header, incarnation::text, revision::text
      FROM ${SQL_SCHEMA}.sessions
      WHERE namespace = $1
      ORDER BY (header->>'createdAt')::bigint DESC, id ASC
    `, [this.namespace])
    signal?.throwIfAborted()
    return result.rows.map(row => ({
      header: structuredClone(row.header),
      revision: postgresRevision(this.storeId, this.namespace, row),
    }))
  }

  async close(): Promise<void> {
    await this.ready.catch(() => undefined)
    await this.pool.end()
  }

  private writerAuthority(sessionId: SessionId): WriterAuthority {
    const runContext = this.ctx.get('cloudRunContext') as CloudRunContext | undefined
    const inherited = runContext?.current()
    const bound = this.boundAuthorities.get(String(sessionId))
    // DSH's write-behind coordinator is intentionally asynchronous.  Its
    // promise chain may inherit Run N's AsyncLocal context even after Run N+1
    // has been admitted and bound for this Session.  Select the newest fenced
    // authority instead of blindly preferring the inherited context.
    const current = inherited === undefined
      ? bound
      : bound === undefined || inherited.writerFence >= bound.writerFence
        ? inherited
        : bound
    if (current === undefined) {
      if (this.requireWriterAuthority) {
        throw new Error('session persistence mutation requires active RunAttempt authority')
      }
      return { fence: 0 }
    }
    // One-shot DSH subagents own independent native Session logs, but execute
    // synchronously inside the root RunAttempt.  Preserve the root authority
    // for detached coordinator flushes; appendBatch validates the target's
    // durable parentSession chain before accepting any child event.
    if (String(current.sessionId) !== String(sessionId)) {
      this.rememberAuthority(String(sessionId), current)
    }
    return { fence: current.writerFence, attemptId: current.attemptId, cloud: current }
  }

  private async assertControlAuthority(client: PoolClient, incoming: WriterAuthority, meta: SessionHeader): Promise<void> {
    if (incoming.cloud === undefined) return
    const authority = incoming.cloud
    const rootSessionId = String(authority.sessionId)
    const targetSessionId = String(meta.id)
    if (this.validateControlAuthority) {
      const result = await client.query(`
      SELECT 1
      WHERE EXISTS (
        SELECT 1
          FROM dsh_cloud_control.runs run
          JOIN dsh_cloud_control.run_attempts attempt
            ON attempt.namespace=run.namespace AND attempt.id=run.current_attempt_id AND attempt.run_id=run.id
         WHERE run.namespace=$1 AND run.id::text=$2 AND run.tenant_id::text=$3
           AND run.workspace_id::text=$4 AND run.session_id=$5
           AND attempt.id::text=$6 AND run.writer_fence=$7 AND attempt.writer_fence=$7
           AND run.status IN ('claimed','dispatching','dispatched','running','cancel_requested')
           AND attempt.status IN ('claimed','running')
           AND attempt.heartbeat_at>now()-make_interval(secs=>$8)
      ) OR EXISTS (
        SELECT 1
          FROM dsh_cloud_control.session_commands command
         WHERE command.namespace=$1 AND command.command_id::text=$2
           AND command.tenant_id::text=$3 AND command.workspace_id::text=$4
           AND command.session_id=$5 AND command.attempt_id::text=$6
           AND command.writer_fence=$7 AND command.expires_at>now()
      )
      `, [
        this.namespace,
        authority.runId,
        authority.tenantId,
        authority.workspaceId,
        rootSessionId,
        authority.attemptId,
        authority.writerFence,
        this.attemptLeaseSeconds,
      ])
      if (result.rowCount !== 1) {
        throw new StaleSessionWriterError(`session "${targetSessionId}" rejected inactive or expired RunAttempt authority`)
      }
    }
    if (targetSessionId === rootSessionId) return
    if (meta.origin !== 'subagent' || meta.parentSession === undefined) {
      throw new StaleSessionWriterError(
        `RunAttempt authority for session "${rootSessionId}" cannot write unrelated session "${targetSessionId}"`,
      )
    }

    // The target may not have a row yet, so begin at its declared parent and
    // walk only already-committed immutable headers.  A bounded recursive CTE
    // makes nested one-shot Workflow children valid without trusting a
    // model-provided target id or accepting an arbitrary cross-session write.
    const lineage = await client.query(`
      WITH RECURSIVE ancestors(id, parent_session, depth) AS (
        SELECT session.id, session.header->>'parentSession', 0
          FROM ${SQL_SCHEMA}.sessions session
         WHERE session.namespace=$1 AND session.id=$2
        UNION ALL
        SELECT parent.id, parent.header->>'parentSession', ancestors.depth + 1
          FROM ancestors
          JOIN ${SQL_SCHEMA}.sessions parent
            ON parent.namespace=$1 AND parent.id=ancestors.parent_session
         WHERE ancestors.depth < 63
      )
      SELECT 1 FROM ancestors WHERE id=$3 LIMIT 1
    `, [this.namespace, meta.parentSession, rootSessionId])
    if (lineage.rowCount !== 1) {
      throw new StaleSessionWriterError(
        `subagent session "${targetSessionId}" is not a descendant of Run session "${rootSessionId}"`,
      )
    }
  }

  private assertWriter(row: SessionRow, incoming: WriterAuthority, id: SessionId): void {
    const storedFence = safeInteger(row.writer_fence, 'writer fence')
    if (incoming.fence < storedFence) {
      throw new StaleSessionWriterError(
        `session "${id}" rejected stale writer fence ${incoming.fence}; current fence is ${storedFence}`,
      )
    }
    if (incoming.fence === storedFence
      && row.writer_attempt_id !== null
      && incoming.attemptId !== row.writer_attempt_id) {
      throw new StaleSessionWriterError(
        `session "${id}" rejected a different Attempt at writer fence ${storedFence}`,
      )
    }
  }

  private async loadPhysicalEvents(
    client: PoolClient,
    id: SessionId,
    fromSeq: number,
  ): Promise<SessionEvent[]> {
    const [segments, batches] = await Promise.all([
      client.query<SegmentRow>(`
        SELECT seq::text, seq_end::text, codec, payload, sha256
        FROM ${SQL_SCHEMA}.session_segments segment
        WHERE namespace = $1 AND session_id = $2 AND seq_end >= $3
        ORDER BY segment.seq
      `, [this.namespace, id, fromSeq]),
      client.query<EventBatchRow>(`
        SELECT seq::text, seq_end::text, provider_location, digest
        FROM ${SQL_SCHEMA}.session_event_batches batch
        WHERE namespace = $1 AND session_id = $2 AND seq_end >= $3
        ORDER BY batch.seq
      `, [this.namespace, id, fromSeq]),
    ])
    const locations = batches.rows.map(row => this.batchLocation(row))
    const tail = await this.tail.read(this.namespace, String(id), locations)
    const events = [
      ...segments.rows.flatMap(decodeSegment),
      ...tail.flatMap(envelopeEvents),
    ].filter(event => event.seq >= fromSeq)
    events.sort((left, right) => left.seq - right.seq)
    return events
  }

  /**
   * Return canonical history to UI/export callers even when the attached
   * runtime was prepared from a compact baseline containing local gap records.
   * Live coordinator-only closers or pending events are merged after the last
   * durable event; baseline gaps themselves never cross this service boundary.
   */
  private async canonicalInspection(
    id: SessionId,
    projected: SessionInspection,
    signal?: AbortSignal,
  ): Promise<SessionInspection> {
    signal?.throwIfAborted()
    await this.ready
    return this.readTransaction(async (client) => {
      const row = await this.sessionRow(client, id)
      if (row === undefined) throw new Error(`session "${id}" not found`)
      const events = await this.loadPhysicalEvents(client, id, 0)
      const nextSeq = safeInteger(row.next_seq, 'next seq')
      assertContiguous(events, 0)
      if (events.length !== nextSeq) {
        throw new Error(`corrupt session log: canonical history ends before stored next seq ${nextSeq}`)
      }
      const liveSuffix = projected.events.filter(event => event.seq >= nextSeq)
      assertContiguous(liveSuffix, nextSeq)
      signal?.throwIfAborted()
      return Object.freeze({
        meta: structuredClone(projected.meta),
        events: Object.freeze([...events, ...structuredClone(liveSuffix)]),
      })
    })
  }

  /**
   * Prepare only the effective runtime surface from the latest verified
   * baseline plus the canonical physical suffix. Canonical segments and Kafka
   * locations remain the authority for history, export and recovery fallback.
   */
  private async loadRuntimePreparation(
    client: PoolClient,
    id: SessionId,
    header: SessionHeader,
  ): Promise<{ runtimeBaseline?: SessionRuntimeBaseline; events: SessionEvent[] }> {
    const result = await client.query<RuntimeBaselineRow>(`
      SELECT through_seq::text, event_count::text, codec, payload, sha256
      FROM ${SQL_SCHEMA}.session_runtime_baselines
      WHERE namespace = $1 AND session_id = $2
    `, [this.namespace, id])
    const checkpoint = result.rows[0]
    if (checkpoint === undefined) return { events: await this.loadPhysicalEvents(client, id, 0) }

    try {
      const runtimeBaseline = decodeRuntimeBaseline(checkpoint)
      const suffix = await this.loadPhysicalEvents(client, id, runtimeBaseline.throughSeq + 1)
      assertContiguous(suffix, runtimeBaseline.throughSeq + 1)
      // Validate the complete contract here so a damaged derived baseline can
      // fall back to canonical history before the coordinator sees it.
      Session.fromRuntimeBaseline(id, runtimeBaseline, suffix, header)
      return { runtimeBaseline, events: suffix }
    } catch (error) {
      // Runtime baselines are derived acceleration data. A damaged cache must
      // never make the canonical Session unavailable while its source log is sound.
      this.ctx.logger.warn(
        `session "${id}": runtime baseline rejected; falling back to canonical log: ${String(error)}`,
      )
      return { events: await this.loadPhysicalEvents(client, id, 0) }
    }
  }

  /** Materialize an effective runtime baseline after successful compaction. */
  private async writeRuntimeBaseline(
    client: PoolClient,
    id: SessionId,
    expectedNextSeq: number,
  ): Promise<void> {
    const events = await this.loadPhysicalEvents(client, id, 0)
    if (events.length !== expectedNextSeq) {
      throw new Error(
        `Session runtime baseline expected ${expectedNextSeq} events, loaded ${events.length}`,
      )
    }
    assertContiguous(events, 0)
    const checkpoint = encodeRuntimeBaseline(events)
    await client.query(`
      INSERT INTO ${SQL_SCHEMA}.session_runtime_baselines
        (namespace, session_id, through_seq, event_count, codec, payload, sha256)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (namespace, session_id) DO UPDATE
      SET through_seq = EXCLUDED.through_seq,
          event_count = EXCLUDED.event_count,
          codec = EXCLUDED.codec,
          payload = EXCLUDED.payload,
          sha256 = EXCLUDED.sha256,
          created_at = now()
      WHERE ${SQL_SCHEMA}.session_runtime_baselines.through_seq < EXCLUDED.through_seq
    `, [
      this.namespace,
      id,
      checkpoint.seqEnd,
      events.length,
      RUNTIME_BASELINE_CODEC,
      checkpoint.payload,
      checkpoint.sha256,
    ])
  }

  private async publishLiveBatches(
    client: PoolClient,
    id: SessionId,
    events: readonly SessionEvent[],
    authority: WriterAuthority,
  ): Promise<void> {
    let start = 0
    for (let index = 0; index < events.length; index++) {
      if (events[index]?.type !== 'turn/end' && index !== events.length - 1) continue
      const end = index + 1
      const batch = events.slice(start, end)
      start = end
      const records = storageRows(batch)
      const envelope: SessionEventEnvelope = {
        schemaVersion: SESSION_EVENT_ENVELOPE_VERSION,
        namespace: this.namespace,
        sessionId: String(id),
        seq: batch[0]!.seq,
        seqEnd: batch.at(-1)!.seq,
        records: records.map(item => item.record),
        ...(authority.cloud === undefined ? {} : {
          authority: {
            runId: authority.cloud.runId,
            attemptId: authority.cloud.attemptId,
            writerFence: authority.cloud.writerFence,
          },
        }),
      }
      const location = await this.tail.publish(envelope)
      await this.projection.append(envelope, location.digest)
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.session_event_batches
          (namespace, session_id, seq, seq_end, provider_location, digest)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
      `, [
        this.namespace,
        id,
        location.seq,
        location.seqEnd,
        JSON.stringify(location.locator),
        location.digest,
      ])
    }

    const semantic = markers(events)
    if (semantic.length > 0) {
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.session_event_markers
          (namespace, session_id, seq, type, rpc_id, reason)
        SELECT $1, $2, (value->>'seq')::bigint, value->>'type',
               NULLIF(value->>'rpcId', ''), value->'reason'
        FROM jsonb_array_elements($3::jsonb) AS value
      `, [this.namespace, id, JSON.stringify(semantic)])
    }

  }

  private async sealCompletedTurns(
    client: PoolClient,
    id: SessionId,
    sealedThrough: number,
  ): Promise<number> {
    const batches = await client.query<EventBatchRow>(`
      SELECT seq::text, seq_end::text, provider_location, digest
      FROM ${SQL_SCHEMA}.session_event_batches batch
      WHERE namespace = $1 AND session_id = $2
      ORDER BY batch.seq
    `, [this.namespace, id])
    if (batches.rows.length === 0) return sealedThrough
    const expectedStart = sealedThrough + 1
    const envelopes = await this.tail.read(
      this.namespace,
      String(id),
      batches.rows.map(row => this.batchLocation(row)),
    )
    const events = envelopes.flatMap(envelopeEvents)
    assertContiguous(events, expectedStart)
    let segmentStart = 0
    let nextSealedThrough = sealedThrough
    for (let index = 0; index < events.length; index++) {
      if (events[index]?.type !== 'turn/end') continue
      const segment = encodeSegment(events.slice(segmentStart, index + 1))
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.session_segments
          (namespace, session_id, seq, seq_end, codec, payload, sha256)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `, [
        this.namespace,
        id,
        segment.seq,
        segment.seqEnd,
        SEGMENT_CODEC,
        segment.payload,
        segment.sha256,
      ])
      nextSealedThrough = segment.seqEnd
      segmentStart = index + 1
    }
    if (nextSealedThrough > sealedThrough) {
      const crossing = batches.rows.some((row) => {
        const seq = safeInteger(row.seq, 'hot row seq')
        const seqEnd = safeInteger(row.seq_end, 'hot row end seq')
        return seq <= nextSealedThrough && seqEnd > nextSealedThrough
      })
      if (crossing) throw new Error('turn boundary intersects one packed hot Session row')
      await client.query(`
        DELETE FROM ${SQL_SCHEMA}.session_event_batches
        WHERE namespace = $1 AND session_id = $2 AND seq_end <= $3
      `, [this.namespace, id, nextSealedThrough])
    }
    return nextSealedThrough
  }

  private batchLocation(row: EventBatchRow): SessionLiveLocation {
    if (row.provider_location === null
      || typeof row.provider_location !== 'object'
      || Array.isArray(row.provider_location)) {
      throw new Error('stored Session live-log location is invalid')
    }
    if (!/^[0-9a-f]{64}$/.test(row.digest)) throw new Error('stored Session live-log digest is invalid')
    return {
      seq: safeInteger(row.seq, 'batch seq'),
      seqEnd: safeInteger(row.seq_end, 'batch end seq'),
      locator: structuredClone(row.provider_location) as Record<string, string | number | boolean>,
      digest: row.digest,
    }
  }

  private async reconcileProjection(
    client: PoolClient,
    id: SessionId,
    sealedThrough: number,
    projectedThrough: number,
  ): Promise<void> {
    const current = await this.projection.watermark(this.namespace, String(id)) ?? -1
    if (current === projectedThrough) return
    await this.projection.reset(this.namespace, String(id), sealedThrough)
    const rows = await client.query<EventBatchRow>(`
      SELECT seq::text, seq_end::text, provider_location, digest
      FROM ${SQL_SCHEMA}.session_event_batches
      WHERE namespace=$1 AND session_id=$2 AND seq>$3
      ORDER BY seq
    `, [this.namespace, id, sealedThrough])
    const envelopes = await this.tail.read(
      this.namespace,
      String(id),
      rows.rows.map(row => this.batchLocation(row)),
    )
    for (let index = 0; index < envelopes.length; index++) {
      const envelope = envelopes[index]!
      await this.projection.append(envelope, rows.rows[index]!.digest)
    }
    const rebuilt = await this.projection.watermark(this.namespace, String(id)) ?? -1
    const expected = rows.rows.length === 0 ? sealedThrough : safeInteger(rows.rows.at(-1)!.seq_end, 'batch end seq')
    if (expected !== projectedThrough || rebuilt !== projectedThrough) {
      throw new Error('Valkey Session projection could not be rebuilt to the committed watermark')
    }
  }

  private async initialize(): Promise<void> {
    await this.tail.provision({
      partitions: this.config.liveLogPartitions ?? 12,
      replicationFactor: this.config.liveLogReplicationFactor ?? 1,
      retentionMs: this.config.liveLogRetentionMs ?? 2_592_000_000,
    })
    await this.projection.checkHealth()
    await this.writeTransaction(async (client) => {
      // PostgreSQL's CREATE SCHEMA IF NOT EXISTS can still race at the catalog
      // unique index when several fresh Workers boot simultaneously. Serialize
      // only schema initialization; normal Session traffic never takes this lock.
      await client.query(`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, [
        'dsh_cloud.session-persistence:migration',
      ])
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${SQL_SCHEMA}`)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.schema_state (
          singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
          version integer NOT NULL
        )
      `)
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.schema_state (singleton, version)
        VALUES (true, $1)
        ON CONFLICT (singleton) DO NOTHING
      `, [SCHEMA_VERSION])
      const schema = await client.query<{ version: number }>(`
        SELECT version FROM ${SQL_SCHEMA}.schema_state WHERE singleton = true
      `)
      const storedVersion = schema.rows[0]?.version
      if (storedVersion !== SCHEMA_VERSION) {
        throw new Error(
          `PostgreSQL session schema version ${String(storedVersion)} is incompatible with ${SCHEMA_VERSION}; reset this pre-production database`,
        )
      }
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.persistence_state (
          namespace text PRIMARY KEY,
          store_id uuid NOT NULL
        )
      `)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.sessions (
          namespace text NOT NULL REFERENCES ${SQL_SCHEMA}.persistence_state(namespace),
          id text NOT NULL,
          header jsonb NOT NULL,
          incarnation uuid NOT NULL,
          revision bigint NOT NULL CHECK (revision >= 0),
          next_seq bigint NOT NULL CHECK (next_seq >= 0),
          sealed_through bigint NOT NULL DEFAULT -1 CHECK (sealed_through >= -1),
          projected_through bigint NOT NULL DEFAULT -1 CHECK (projected_through >= -1),
          writer_fence bigint NOT NULL CHECK (writer_fence >= 0),
          writer_attempt_id text,
          PRIMARY KEY (namespace, id)
        )
      `)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.session_segments (
          namespace text NOT NULL,
          session_id text NOT NULL,
          seq bigint NOT NULL CHECK (seq >= 0),
          seq_end bigint NOT NULL CHECK (seq_end >= seq),
          codec text NOT NULL,
          payload bytea NOT NULL,
          sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (namespace, session_id, seq),
          FOREIGN KEY (namespace, session_id)
            REFERENCES ${SQL_SCHEMA}.sessions(namespace, id)
            ON DELETE CASCADE
        )
      `)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.session_event_markers (
          namespace text NOT NULL,
          session_id text NOT NULL,
          seq bigint NOT NULL CHECK (seq >= 0),
          type text NOT NULL CHECK (type IN ('user/message', 'turn/end')),
          rpc_id text,
          reason jsonb,
          PRIMARY KEY (namespace, session_id, seq),
          FOREIGN KEY (namespace, session_id)
            REFERENCES ${SQL_SCHEMA}.sessions(namespace, id)
            ON DELETE CASCADE
        )
      `)
      await client.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS session_event_markers_rpc
        ON ${SQL_SCHEMA}.session_event_markers(namespace, session_id, rpc_id)
        WHERE rpc_id IS NOT NULL
      `)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.session_event_batches (
          namespace text NOT NULL,
          session_id text NOT NULL,
          seq bigint NOT NULL CHECK (seq >= 0),
          seq_end bigint NOT NULL CHECK (seq_end >= seq),
          provider_location jsonb NOT NULL CHECK (jsonb_typeof(provider_location) = 'object'),
          digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (namespace, session_id, seq),
          FOREIGN KEY (namespace, session_id)
            REFERENCES ${SQL_SCHEMA}.sessions(namespace, id)
            ON DELETE CASCADE
        )
      `)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.session_runtime_baselines (
          namespace text NOT NULL,
          session_id text NOT NULL,
          through_seq bigint NOT NULL CHECK (through_seq >= 0),
          event_count bigint NOT NULL CHECK (event_count = through_seq + 1),
          codec text NOT NULL,
          payload bytea NOT NULL,
          sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (namespace, session_id),
          FOREIGN KEY (namespace, session_id)
            REFERENCES ${SQL_SCHEMA}.sessions(namespace, id)
            ON DELETE CASCADE
        )
      `)
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.persistence_state (namespace, store_id)
        VALUES ($1, $2)
        ON CONFLICT (namespace) DO NOTHING
      `, [this.namespace, randomUUID()])
      const state = await client.query<{ store_id: string }>(`
        SELECT store_id::text FROM ${SQL_SCHEMA}.persistence_state WHERE namespace = $1
      `, [this.namespace])
      const storeId = state.rows[0]?.store_id
      if (storeId === undefined) throw new Error('PostgreSQL persistence namespace has no store identity')
      this.storeId = storeId
    })
  }

  private async sessionRow(client: PoolClient, id: SessionId): Promise<SessionRow | undefined> {
    const result = await client.query<SessionRow>(`
      SELECT header, incarnation::text, revision::text, next_seq::text,
             sealed_through::text, projected_through::text,
             writer_fence::text, writer_attempt_id
      FROM ${SQL_SCHEMA}.sessions
      WHERE namespace = $1 AND id = $2
    `, [this.namespace, id])
    return result.rows[0]
  }

  private async readTransaction<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
      const value = await operation(client)
      await client.query('COMMIT')
      return value
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async writeTransaction<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const value = await operation(client)
      await client.query('COMMIT')
      return value
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }
}

export default TieredSessionPersistence
