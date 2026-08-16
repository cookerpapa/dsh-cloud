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
import { KafkaJS } from '@confluentinc/kafka-javascript'
import { Valkey } from 'iovalkey'
import { Pool } from 'pg'
import { describe, expect, test } from 'vitest'
import CloudRunContext, { cloudIdentifier, writerFence } from '@dsh-cloud/run-context'
import PostgresSessionPersistence from '@dsh-cloud/session-persistence-postgres'
import {
  KafkaSessionEventLog,
  SessionEventOutbox,
  SessionEventRelay,
  ValkeySessionEventProjection,
} from '../src/index.js'

const databaseUrl = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const brokers = process.env['DSH_CLOUD_TEST_KAFKA_BROKERS']?.split(',').filter(Boolean)
const valkeyUrl = process.env['DSH_CLOUD_TEST_VALKEY_URL']
const enabled = databaseUrl !== undefined && brokers?.length && valkeyUrl !== undefined ? describe : describe.skip

enabled('real Session event publication', () => {
  test('crosses PostgreSQL Outbox, Kafka acks=all, Valkey, and the Gateway watermark', async () => {
    const namespace = `real-relay-${randomUUID()}`
    const sessionId = `session-${randomUUID()}`
    const topic = `dsh-cloud-test-${randomUUID()}`
    const kafka = new KafkaJS.Kafka({ 'bootstrap.servers': brokers!.join(','), 'client.id': 'dsh-cloud-real-relay-test' })
    const admin = kafka.admin()
    await admin.connect()
    await admin.createTopics({ topics: [{ topic, numPartitions: 3, replicationFactor: 1 }] })

    const ctx = new Context()
    const sessionFiber = await ctx.plugin(SessionStore)
    const runFiber = await ctx.plugin(CloudRunContext)
    const persistenceFiber = await ctx.plugin(PostgresSessionPersistence, {
      connectionString: databaseUrl!, namespace, requireWriterAuthority: true, writeBatchMaxDelayMs: 1,
    })
    const pool = new Pool({ connectionString: databaseUrl! })
    const log = new KafkaSessionEventLog({ brokers: brokers!, topic, clientId: `relay-${randomUUID()}` })
    const projection = new ValkeySessionEventProjection({ url: valkeyUrl!, ttlSeconds: 600 })
    const relay = new SessionEventRelay(new SessionEventOutbox(pool, namespace, 'real-relay-test'), log, projection)
    try {
      const meta: SessionHeader = {
        version: SESSION_FORMAT_VERSION, id: SessionId(sessionId), createdAt: Date.now(), cwd: '/workspace',
      }
      const events: SessionEvent[] = [
        { type: 'turn/start', seq: 0, time: 1, data: { turn: 1 } },
        {
          type: 'user/message', seq: 1, time: 2, surfaceOp: 'append',
          data: createMessage({
            id: MessageId('real-relay-user'), role: 'user', content: [{ type: 'text', text: 'hello' }],
            source: { kind: 'user', rpcId: 'real-relay-rpc' },
          }),
        },
        { type: 'step/start', seq: 2, time: 3, data: { turn: 1, step: 1 } },
        { type: 'step/end', seq: 3, time: 4, data: { turn: 1, step: 1 } },
        { type: 'turn/end', seq: 4, time: 5, data: { turn: 1, reason: { kind: 'completed' } } },
      ]
      await ctx.sessionPersistence.create(meta)
      await ctx.cloudRunContext.run({
        tenantId: cloudIdentifier('TenantId', 'tenant'),
        workspaceId: cloudIdentifier('WorkspaceId', 'workspace'),
        sessionId: cloudIdentifier('SessionId', sessionId),
        runId: cloudIdentifier('RunId', 'run'),
        attemptId: cloudIdentifier('AttemptId', 'attempt'),
        writerFence: writerFence(1),
      }, () => ctx.sessionPersistence.append(meta.id, events))

      expect(await relay.runOnce()).toBe(1)
      const stored = await pool.query<{ through: string; outbox: string; segments: string }>(`
        SELECT projected_through::text AS through,
          (SELECT count(*) FROM dsh_cloud.session_event_outbox WHERE namespace=$1 AND session_id=$2)::text AS outbox,
          (SELECT count(*) FROM dsh_cloud.session_segments WHERE namespace=$1 AND session_id=$2)::text AS segments
        FROM dsh_cloud.sessions WHERE namespace=$1 AND id=$2
      `, [namespace, sessionId])
      expect(stored.rows[0]).toEqual({ through: '4', outbox: '0', segments: '1' })
      const offsets = await admin.fetchTopicOffsets(topic)
      expect(offsets.some(offset => BigInt(offset.high) > 0n)).toBe(true)
      const identity = Buffer.from(`${namespace}\0${sessionId}`).toString('base64url')
      const valkey = new Valkey(valkeyUrl!)
      expect(await valkey.get(`dsh-cloud:events:{${identity}}:watermark`)).toBe('4')
      valkey.disconnect()
      expect((await ctx.sessionPersistence.load(meta.id)).events).toEqual(events)
    } finally {
      await relay.close()
      await pool.query('DELETE FROM dsh_cloud.sessions WHERE namespace=$1', [namespace]).catch(() => undefined)
      await pool.query('DELETE FROM dsh_cloud.persistence_state WHERE namespace=$1', [namespace]).catch(() => undefined)
      await pool.end()
      await persistenceFiber.dispose()
      await runFiber.dispose()
      await sessionFiber.dispose()
      await admin.deleteTopics({ topics: [topic] }).catch(() => undefined)
      await admin.disconnect()
    }
  }, 30_000)
})
