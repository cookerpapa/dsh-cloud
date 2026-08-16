import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import {
  decodeStorageRecord,
  packChunkRuns,
  type SessionEvent,
  type SessionHeader,
  type SessionId,
  type SessionPreparation,
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

const SCHEMA_VERSION = 2
const SQL_SCHEMA = 'dsh_cloud'
const DEFAULT_NAMESPACE = 'local'

interface SessionRow {
  header: SessionHeader
  incarnation: string
  revision: string
  next_seq: string
  writer_fence: string
  writer_attempt_id: string | null
}

interface EventRow {
  seq: string
  seq_end: string
  event: StorageRecord
}

interface WriterAuthority {
  readonly fence: number
  readonly attemptId?: string
  readonly cloud?: RunAuthority
}

/** Configuration for the PostgreSQL SessionPersistence provider. */
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

function scanRows(rows: readonly EventRow[], expectedStart = 0): { events: SessionEvent[]; tornFrom?: number } {
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
        throw new Error(`corrupt session log: seq gap or invalid event in committed region at ${expectedStart + index}`)
      }
      return { events, tornFrom: expectedStart + index }
    }
    events.push(structuredClone(event))
  }
  return { events }
}

/** PostgreSQL-native implementation of DSH's append-only Session log. */
class PostgresSessionPersistence extends SessionPersistence implements PersistenceBackend<number> {
  static inject = ['sessions']

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
  })

  override readonly supportsRawArtifacts = false
  override readonly name = 'session-persistence-postgres'

  private readonly pool: Pool
  private readonly namespace: string
  private readonly requireWriterAuthority: boolean
  private readonly validateControlAuthority: boolean
  private readonly attemptLeaseSeconds: number
  private readonly ready: Promise<void>
  private readonly coordinator: PersistenceCoordinator<number>
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

  override prepare(id: SessionId, signal?: AbortSignal): Promise<SessionPreparation> {
    return this.coordinator.prepare(id, signal)
  }

  load(id: SessionId): Promise<SessionInspection> {
    return this.coordinator.load(id)
  }

  inspect(id: SessionId, signal?: AbortSignal): Promise<SessionInspection> {
    return this.coordinator.inspect(id, signal)
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
      const result = await client.query<EventRow>(`
        SELECT seq::text, seq_end::text, event
        FROM ${SQL_SCHEMA}.session_events
        WHERE namespace = $1 AND session_id = $2
        ORDER BY session_events.seq ASC
      `, [this.namespace, id])
      signal?.throwIfAborted()
      const scanned = scanRows(result.rows)
      return {
        meta: structuredClone(row.header),
        events: scanned.events,
        revision: postgresRevision(this.storeId, this.namespace, row),
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
      const result = await client.query<EventRow>(`
        SELECT seq::text, seq_end::text, event
        FROM ${SQL_SCHEMA}.session_events
        WHERE namespace = $1 AND session_id = $2 AND seq_end >= $3
        ORDER BY session_events.seq ASC
      `, [this.namespace, id, fromSeq])
      signal?.throwIfAborted()
      const firstStoredSeq = result.rows[0] === undefined ? fromSeq : safeInteger(result.rows[0].seq, 'storage row seq')
      const events = scanRows(result.rows, firstStoredSeq).events.filter(event => event.seq >= fromSeq)
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
      await this.assertControlAuthority(client, authority, meta.id)
      let row: SessionRow
      if (!isMaterialized) {
        const inserted = await client.query<SessionRow>(`
          INSERT INTO ${SQL_SCHEMA}.sessions
            (namespace, id, header, incarnation, revision, next_seq, writer_fence, writer_attempt_id)
          VALUES ($1, $2, $3::jsonb, $4, 0, 0, $5, $6)
          ON CONFLICT (namespace, id) DO NOTHING
          RETURNING header, incarnation::text, revision::text, next_seq::text,
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
      assertContiguous(events, expected)
      const records = storageRows(events)
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.session_events (namespace, session_id, seq, seq_end, event)
        SELECT $1, $2, (value->>'seq')::bigint, (value->>'seqEnd')::bigint, value->'record'
        FROM jsonb_array_elements($3::jsonb) AS value
      `, [this.namespace, meta.id, JSON.stringify(records)])
      await client.query(`
        UPDATE ${SQL_SCHEMA}.sessions
        SET next_seq = $3,
            revision = revision + 1,
            writer_fence = $4,
            writer_attempt_id = $5
        WHERE namespace = $1 AND id = $2
      `, [
        this.namespace,
        meta.id,
        expected + events.length,
        authority.fence,
        authority.attemptId ?? null,
      ])
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
      await this.assertControlAuthority(client, authority, meta.id)
      const locked = await client.query<SessionRow>(`
        SELECT header, incarnation::text, revision::text, next_seq::text,
               writer_fence::text, writer_attempt_id
        FROM ${SQL_SCHEMA}.sessions
        WHERE namespace = $1 AND id = $2
        FOR UPDATE
      `, [this.namespace, meta.id])
      const row = locked.rows[0]
      if (row === undefined) throw new Error(`session "${meta.id}" is not materialized`)
      this.assertWriter(row, authority, meta.id)
      const storedNext = safeInteger(row.next_seq, 'next seq')
      const appendAt = tornMarker ?? storedNext
      if (appendAt > storedNext) throw new Error('repair marker exceeds the stored session length')
      assertContiguous(closers, appendAt)

      if (tornMarker !== undefined) {
        const crossing = await client.query(`
          SELECT 1 FROM ${SQL_SCHEMA}.session_events
          WHERE namespace=$1 AND session_id=$2 AND seq<$3 AND seq_end>=$3 LIMIT 1
        `, [this.namespace, meta.id, tornMarker])
        if (crossing.rowCount !== 0) throw new Error('repair marker intersects one packed storage row')
        await client.query(`
          DELETE FROM ${SQL_SCHEMA}.session_events
          WHERE namespace = $1 AND session_id = $2 AND seq >= $3
        `, [this.namespace, meta.id, tornMarker])
      }
      if (closers.length > 0) {
        const records = storageRows(closers)
        await client.query(`
          INSERT INTO ${SQL_SCHEMA}.session_events (namespace, session_id, seq, seq_end, event)
          SELECT $1, $2, (value->>'seq')::bigint, (value->>'seqEnd')::bigint, value->'record'
          FROM jsonb_array_elements($3::jsonb) AS value
        `, [this.namespace, meta.id, JSON.stringify(records)])
      }
      await client.query(`
        UPDATE ${SQL_SCHEMA}.sessions
        SET next_seq = $3,
            revision = revision + 1,
            writer_fence = $4,
            writer_attempt_id = $5
        WHERE namespace = $1 AND id = $2
      `, [
        this.namespace,
        meta.id,
        appendAt + closers.length,
        authority.fence,
        authority.attemptId ?? null,
      ])
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
    const current = runContext?.current()
    if (current === undefined) {
      if (this.requireWriterAuthority) {
        throw new Error('session persistence mutation requires active RunAttempt authority')
      }
      return { fence: 0 }
    }
    if (String(current.sessionId) !== String(sessionId)) {
      throw new StaleSessionWriterError(
        `RunAttempt authority for session "${current.sessionId}" cannot write session "${sessionId}"`,
      )
    }
    return { fence: current.writerFence, attemptId: current.attemptId, cloud: current }
  }

  private async assertControlAuthority(client: PoolClient, incoming: WriterAuthority, id: SessionId): Promise<void> {
    if (!this.validateControlAuthority || incoming.cloud === undefined) return
    const authority = incoming.cloud
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
           AND run.status IN ('claimed','dispatched','running','cancel_requested')
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
      id,
      authority.attemptId,
      authority.writerFence,
      this.attemptLeaseSeconds,
    ])
    if (result.rowCount !== 1) {
      throw new StaleSessionWriterError(`session "${id}" rejected inactive or expired RunAttempt authority`)
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

  private async initialize(): Promise<void> {
    await this.writeTransaction(async (client) => {
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
          writer_fence bigint NOT NULL CHECK (writer_fence >= 0),
          writer_attempt_id text,
          PRIMARY KEY (namespace, id)
        )
      `)
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.session_events (
          namespace text NOT NULL,
          session_id text NOT NULL,
          seq bigint NOT NULL CHECK (seq >= 0),
          seq_end bigint NOT NULL CHECK (seq_end >= seq),
          event jsonb NOT NULL,
          PRIMARY KEY (namespace, session_id, seq),
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

export default PostgresSessionPersistence
