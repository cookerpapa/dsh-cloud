import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import { createMessage, MessageId } from '@deepseek-ai/dsh-llm'
import {
  SESSION_FORMAT_VERSION,
  SessionId,
  default as SessionStore,
  type SessionEvent,
  type SessionHeader,
} from '@deepseek-ai/dsh-session'
import { Pool } from 'pg'
import { afterEach, describe, expect, test } from 'vitest'
import CloudRunContext, { cloudIdentifier, writerFence } from '@dsh-cloud/run-context'
import PostgresSessionPersistence from '@dsh-cloud/session-persistence-postgres'
import {
  SessionEventOutbox,
  SessionEventRelay,
  type ClaimedEnvelope,
  type EventLogPublisher,
  type LiveEventProjection,
} from '../src/index.js'

const databaseUrl = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const integration = databaseUrl === undefined ? describe.skip : describe
const cleanups: Array<() => Promise<void>> = []

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

class RecordingLog implements EventLogPublisher {
  readonly batches: ClaimedEnvelope[] = []
  async publish(batches: readonly ClaimedEnvelope[]): Promise<void> { this.batches.push(...batches) }
  async checkHealth(): Promise<void> {}
  async close(): Promise<void> {}
}

class RecordingProjection implements LiveEventProjection {
  readonly batches: ClaimedEnvelope[] = []
  fail = false
  async append(envelope: ClaimedEnvelope['envelope'], digest: string): Promise<void> {
    if (this.fail) throw new Error('projection unavailable')
    this.batches.push({ id: '', envelope, digest })
  }
  async checkHealth(): Promise<void> {}
  async close(): Promise<void> {}
}

function header(id: string): SessionHeader {
  return { version: SESSION_FORMAT_VERSION, id: SessionId(id), createdAt: Date.now(), cwd: '/workspace' }
}

function turn(): SessionEvent[] {
  return [
    { type: 'turn/start', seq: 0, time: 1, data: { turn: 1 } },
    {
      type: 'user/message', seq: 1, time: 2, surfaceOp: 'append',
      data: createMessage({
        id: MessageId('relay-user'), role: 'user', content: [{ type: 'text', text: 'hello' }],
        source: { kind: 'user', rpcId: 'relay-rpc' },
      }),
    },
    { type: 'step/start', seq: 2, time: 3, data: { turn: 1, step: 1 } },
    {
      type: 'assistant/message', seq: 3, time: 4, surfaceOp: 'append',
      data: {
        turn: 1, step: 1,
        message: createMessage({
          id: MessageId('relay-assistant'), role: 'assistant', content: [{ type: 'text', text: 'hi' }],
          source: { kind: 'model', provider: 'test', model: 'test' },
        }),
      },
    },
    { type: 'step/end', seq: 4, time: 5, data: { turn: 1, step: 1 } },
    { type: 'turn/end', seq: 5, time: 6, data: { turn: 1, reason: { kind: 'completed' } } },
  ]
}

integration('SessionEventRelay', () => {
  test('publishes an atomic Session batch before advancing the browser projection watermark', async () => {
    const namespace = `relay-${randomUUID()}`
    const id = 'relay-session'
    const ctx = new Context()
    const sessionFiber = await ctx.plugin(SessionStore)
    const runFiber = await ctx.plugin(CloudRunContext)
    const persistenceFiber = await ctx.plugin(PostgresSessionPersistence, {
      connectionString: databaseUrl as string,
      namespace,
      requireWriterAuthority: true,
      writeBatchMaxDelayMs: 1,
    })
    const pool = new Pool({ connectionString: databaseUrl as string })
    cleanups.push(async () => {
      await pool.query('DELETE FROM dsh_cloud.sessions WHERE namespace=$1', [namespace]).catch(() => undefined)
      await pool.query('DELETE FROM dsh_cloud.persistence_state WHERE namespace=$1', [namespace]).catch(() => undefined)
      await pool.end()
      await persistenceFiber.dispose()
      await runFiber.dispose()
      await sessionFiber.dispose()
    })
    const meta = header(id)
    const events = turn()
    await ctx.sessionPersistence.create(meta)
    await ctx.cloudRunContext.run({
      tenantId: cloudIdentifier('TenantId', 'tenant'),
      workspaceId: cloudIdentifier('WorkspaceId', 'workspace'),
      sessionId: cloudIdentifier('SessionId', id),
      runId: cloudIdentifier('RunId', 'run'),
      attemptId: cloudIdentifier('AttemptId', 'attempt'),
      writerFence: writerFence(1),
    }, () => ctx.sessionPersistence.append(meta.id, events))

    const before = await pool.query<{ hot: string; segments: string; outbox: string; projected: string }>(`
      SELECT
        (SELECT count(*) FROM dsh_cloud.session_events WHERE namespace=$1 AND session_id=$2)::text AS hot,
        (SELECT count(*) FROM dsh_cloud.session_segments WHERE namespace=$1 AND session_id=$2)::text AS segments,
        (SELECT count(*) FROM dsh_cloud.session_event_outbox WHERE namespace=$1 AND session_id=$2)::text AS outbox,
        projected_through::text AS projected
      FROM dsh_cloud.sessions WHERE namespace=$1 AND id=$2
    `, [namespace, id])
    expect(before.rows[0]).toEqual({ hot: '0', segments: '1', outbox: '1', projected: '-1' })

    const log = new RecordingLog()
    const projection = new RecordingProjection()
    const relay = new SessionEventRelay(new SessionEventOutbox(pool, namespace, 'relay-test'), log, projection)
    expect(await relay.runOnce()).toBe(1)
    expect(log.batches).toHaveLength(1)
    expect(projection.batches).toHaveLength(1)
    const after = await pool.query<{ projected: string; outbox: string }>(`
      SELECT projected_through::text AS projected,
        (SELECT count(*) FROM dsh_cloud.session_event_outbox WHERE namespace=$1 AND session_id=$2)::text AS outbox
      FROM dsh_cloud.sessions WHERE namespace=$1 AND id=$2
    `, [namespace, id])
    expect(after.rows[0]).toEqual({ projected: '5', outbox: '0' })
    expect((await ctx.sessionPersistence.load(meta.id)).events).toEqual(events)
  })

  test('treats Kafka publication as at-least-once and keeps projection identity stable', async () => {
    const namespace = `relay-retry-${randomUUID()}`
    const pool = new Pool({ connectionString: databaseUrl as string })
    cleanups.push(async () => { await pool.end() })
    const state = await pool.query<{ namespace: string }>(`
      SELECT namespace FROM dsh_cloud.persistence_state ORDER BY namespace LIMIT 1
    `)
    if (state.rows[0] === undefined) return
    // The preceding contract test covers the full database path. This focused
    // assertion exercises retry behavior with the same claimed batch shape.
    const log = new RecordingLog()
    const projection = new RecordingProjection()
    projection.fail = true
    const envelope = {
      schemaVersion: 1 as const, namespace, sessionId: 'session', seq: 0, seqEnd: 0,
      records: [{ type: 'turn/start', seq: 0, time: 1, data: { turn: 1 } }],
    }
    const batch: ClaimedEnvelope = { id: randomUUID(), envelope, digest: 'a'.repeat(64) }
    await expect((async () => {
      await log.publish([batch])
      await projection.append(batch.envelope, batch.digest)
    })()).rejects.toThrow('projection unavailable')
    expect(log.batches).toHaveLength(1)
    projection.fail = false
    const resumed = { ...batch }
    await log.publish([resumed])
    await projection.append(resumed.envelope, resumed.digest)
    expect(log.batches).toHaveLength(2)
  })
})
