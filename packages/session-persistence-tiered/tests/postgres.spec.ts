import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import {
  SESSION_FORMAT_VERSION,
  Session,
  SessionId,
  default as SessionStore,
  type SessionEvent,
  type SessionHeader,
} from '@deepseek-ai/dsh-session'
import { MessageId, createMessage } from '@deepseek-ai/dsh-llm'
import { Pool } from 'pg'
import { afterEach, describe, expect, it } from 'vitest'
import CloudRunContext, {
  cloudIdentifier,
  writerFence,
  type RunAuthority,
} from '@dsh-cloud/run-context'
import KafkaSessionLiveLog from '@dsh-cloud/session-live-kafka'
import ValkeySessionLiveProjection from '@dsh-cloud/session-live-valkey'
import TieredSessionPersistence, { StaleSessionWriterError } from '../src/index.ts'

const databaseUrl = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const kafkaBrokers = process.env['DSH_CLOUD_TEST_KAFKA_BROKERS']
const valkeyUrl = process.env['DSH_CLOUD_TEST_VALKEY_URL']
const integration = databaseUrl === undefined || kafkaBrokers === undefined || valkeyUrl === undefined
  ? describe.skip
  : describe
const disposers: Array<() => Promise<void>> = []

afterEach(async () => {
  for (const dispose of disposers.splice(0).reverse()) await dispose()
})

function header(id: string): SessionHeader {
  return {
    version: SESSION_FORMAT_VERSION,
    id: SessionId(id),
    createdAt: Date.now(),
    cwd: '/workspace',
  }
}

function completedTurn(startSeq = 0, turn = 1): SessionEvent[] {
  return [
    { type: 'turn/start', seq: startSeq, time: 1, data: { turn } },
    {
      type: 'user/message',
      seq: startSeq + 1,
      time: 2,
      data: createMessage({
        id: MessageId(`user-${turn}`),
        role: 'user',
        content: [{ type: 'text', text: `turn ${turn}` }],
        source: { kind: 'user' },
      }),
      surfaceOp: 'append',
    },
    { type: 'step/start', seq: startSeq + 2, time: 3, data: { turn, step: 1 } },
    {
      type: 'assistant/message',
      seq: startSeq + 3,
      time: 4,
      data: {
        turn,
        step: 1,
        message: createMessage({
          id: MessageId(`assistant-${turn}`),
          role: 'assistant',
          content: [{ type: 'text', text: `done ${turn}` }],
          source: { kind: 'model', provider: 'test', model: 'test' },
        }),
      },
      surfaceOp: 'append',
    },
    { type: 'step/end', seq: startSeq + 4, time: 5, data: { turn, step: 1 } },
    { type: 'turn/end', seq: startSeq + 5, time: 6, data: { turn, reason: { kind: 'completed' } } },
  ]
}

function compaction(
  startSeq: number,
  shadowedSeqs: number[],
  summary: string,
  ordinal = 1,
): SessionEvent[] {
  const first = shadowedSeqs[0]
  const last = shadowedSeqs.at(-1)
  if (first === undefined || last === undefined) throw new Error('compaction needs a surface range')
  const compactionId = `compaction-${ordinal}`
  return [
    {
      type: 'compaction/start',
      seq: startSeq,
      time: startSeq + 1,
      data: { compactionId, turn: null },
    },
    {
      type: 'compaction/summary',
      seq: startSeq + 1,
      time: startSeq + 2,
      data: {
        compactionId,
        summary: [{ type: 'text', text: summary }],
        shadowedRange: { start: first, end: last },
        shadowedSeqs,
        shadowedTokenCount: 100,
        provider: 'test',
        model: 'test',
      },
    },
    {
      type: 'user/message',
      seq: startSeq + 2,
      time: startSeq + 3,
      data: createMessage({
        id: MessageId(`compact-checkpoint-${ordinal}`),
        role: 'user',
        content: [{ type: 'text', text: `<summary>${summary}</summary>` }],
        source: { kind: 'plugin', plugin: 'compact', compactionId },
      }),
      surfaceOp: { op: 'replace', start: first, end: last },
      sourceEventSeqs: [startSeq, startSeq + 1, ...shadowedSeqs],
    },
    {
      type: 'compaction/end',
      seq: startSeq + 3,
      time: startSeq + 4,
      data: { compactionId, turn: null },
    },
  ] as SessionEvent[]
}

function authority(sessionId: string, fence: number, attempt = `attempt-${fence}`): RunAuthority {
  return {
    tenantId: cloudIdentifier('TenantId', 'tenant-a'),
    workspaceId: cloudIdentifier('WorkspaceId', 'workspace-a'),
    sessionId: cloudIdentifier('SessionId', sessionId),
    runId: cloudIdentifier('RunId', `run-${fence}`),
    attemptId: cloudIdentifier('AttemptId', attempt),
    writerFence: writerFence(fence),
  }
}

async function backend(namespace: string): Promise<{
  ctx: Context
  dispose: () => Promise<void>
}> {
  const ctx = new Context()
  const sessionFiber = await ctx.plugin(SessionStore)
  const runFiber = await ctx.plugin(CloudRunContext)
  const liveLogFiber = await ctx.plugin(KafkaSessionLiveLog, {
    brokers: kafkaBrokers as string,
    topic: `dsh-cloud-session-test-${namespace}`,
    clientId: 'dsh-cloud-session-test',
  })
  const projectionFiber = await ctx.plugin(ValkeySessionLiveProjection, {
    url: valkeyUrl as string,
    retentionSeconds: 3600,
  })
  const persistenceFiber = await ctx.plugin(TieredSessionPersistence, {
    connectionString: databaseUrl as string,
    namespace,
    requireWriterAuthority: true,
    writeBatchMaxDelayMs: 1,
    liveLogPartitions: 3,
    liveLogReplicationFactor: 1,
  })
  const dispose = async (): Promise<void> => {
    await persistenceFiber.dispose()
    await projectionFiber.dispose()
    await liveLogFiber.dispose()
    await runFiber.dispose()
    await sessionFiber.dispose()
  }
  disposers.push(dispose)
  return { ctx, dispose }
}

integration('TieredSessionPersistence', () => {
  it('round-trips native DSH events through the official persistence contract', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = { ...header('round-trip'), agentPreset: 'code' }
    const log = completedTurn()
    await ctx.sessionPersistence.create(meta)
    await ctx.cloudRunContext.run(authority(meta.id, 1), () =>
      ctx.sessionPersistence.append(meta.id, log))

    const loaded = await ctx.sessionPersistence.load(meta.id)
    expect(loaded.meta).toEqual(meta)
    expect(loaded.events).toEqual(log)
    expect((await ctx.sessionPersistence.readFrom(meta.id, 3)).events.map(event => event.seq))
      .toEqual([3, 4, 5])
  })

  it('retains Session authority for detached DSH write-behind flushes', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('detached-write-behind')
    const log = completedTurn()
    await ctx.sessionPersistence.create(meta)
    const runAuthority = authority(meta.id, 2)
    ;(ctx.sessionPersistence as TieredSessionPersistence).bindRunAuthority(runAuthority)
    expect(ctx.cloudRunContext.current()).toBeUndefined()

    await ctx.sessionPersistence.append(meta.id, log)

    expect((await ctx.sessionPersistence.load(meta.id)).events).toEqual(log)
  })

  it('uses the newest bound authority when a follow-up Turn inherits stale async context', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('follow-up-stale-context')
    await ctx.sessionPersistence.create(meta)
    const first = authority(meta.id, 1)
    await ctx.cloudRunContext.run(first, () =>
      ctx.sessionPersistence.append(meta.id, completedTurn()))

    const followUp = authority(meta.id, 2)
    ;(ctx.sessionPersistence as TieredSessionPersistence).bindRunAuthority(followUp)
    // PersistenceCoordinator work created by the previous Turn can retain
    // fence 1 in AsyncLocalStorage.  The Session binding for fence 2 must win.
    await ctx.cloudRunContext.run(first, () =>
      ctx.sessionPersistence.append(meta.id, completedTurn(6, 2)))

    const pool = new Pool({ connectionString: databaseUrl })
    const stored = await pool.query<{
      writer_fence: string
      writer_attempt_id: string
    }>(`
      SELECT writer_fence::text,writer_attempt_id
       FROM dsh_cloud.sessions
       WHERE namespace=$1 AND id=$2
    `, [namespace, meta.id])
    await pool.end()
    expect(stored.rows[0]).toEqual({
      writer_fence: '2',
      writer_attempt_id: String(followUp.attemptId),
    })
  })

  it('does not let a late stale binding replace a newer Session authority', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('monotonic-bound-authority')
    await ctx.sessionPersistence.create(meta)
    const current = authority(meta.id, 3)
    const stale = authority(meta.id, 2)
    const persistence = ctx.sessionPersistence as TieredSessionPersistence
    persistence.bindRunAuthority(current)
    persistence.bindRunAuthority(stale)
    await ctx.sessionPersistence.append(meta.id, completedTurn())

    const pool = new Pool({ connectionString: databaseUrl })
    const stored = await pool.query<{ writer_fence: string; writer_attempt_id: string }>(`
      SELECT writer_fence::text,writer_attempt_id
        FROM dsh_cloud.sessions
       WHERE namespace=$1 AND id=$2
    `, [namespace, meta.id])
    await pool.end()
    expect(stored.rows[0]).toEqual({
      writer_fence: '3',
      writer_attempt_id: String(current.attemptId),
    })
  })

  it('rejects a stale Worker after a higher writer fence has committed', async () => {
    const namespace = `test-${randomUUID()}`
    const first = await backend(namespace)
    const meta = header('fenced')
    await first.ctx.sessionPersistence.create(meta)
    await first.ctx.cloudRunContext.run(authority(meta.id, 4), () =>
      first.ctx.sessionPersistence.append(meta.id, completedTurn()))
    await first.dispose()
    disposers.splice(disposers.indexOf(first.dispose), 1)

    const resumed = await backend(namespace)
    await resumed.ctx.sessionPersistence.load(meta.id)
    await expect(resumed.ctx.cloudRunContext.run(authority(meta.id, 3), () =>
      resumed.ctx.sessionPersistence.append(meta.id, completedTurn(6, 2))))
      .rejects.toBeInstanceOf(StaleSessionWriterError)

    // A rejected persistence batch poisons that in-memory coordinator by design;
    // a fenced-out Worker is discarded rather than reused for another Attempt.
    await resumed.dispose()
    disposers.splice(disposers.indexOf(resumed.dispose), 1)
    const current = await backend(namespace)
    await current.ctx.sessionPersistence.load(meta.id)

    await current.ctx.cloudRunContext.run(authority(meta.id, 5), () =>
      current.ctx.sessionPersistence.append(meta.id, completedTurn(6, 2)))
    expect((await current.ctx.sessionPersistence.load(meta.id)).events).toHaveLength(12)
  })

  it('does not let authority for one Session publish another Session', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('authority-bound')
    await ctx.sessionPersistence.create(meta)
    await expect(ctx.cloudRunContext.run(authority('another-session', 1), () =>
      ctx.sessionPersistence.append(meta.id, completedTurn())))
      .rejects.toThrow(/unrelated session/)
  })

  it('persists nested one-shot subagent Sessions under the root RunAttempt fence', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const root = header('multi-agent-root')
    const child: SessionHeader = {
      ...header('multi-agent-child'),
      parentSession: root.id,
      origin: 'subagent',
      delegationDepth: 1,
    }
    const grandchild: SessionHeader = {
      ...header('multi-agent-grandchild'),
      parentSession: child.id,
      origin: 'subagent',
      delegationDepth: 2,
    }
    const rootAuthority = authority(root.id, 12)
    const rootEvents = completedTurn()
    const childEvents = completedTurn()
    const grandchildEvents = completedTurn()

    await ctx.cloudRunContext.run(rootAuthority, async () => {
      await ctx.sessionPersistence.create(root)
      await ctx.sessionPersistence.append(root.id, rootEvents)
      await ctx.sessionPersistence.create(child)
      await ctx.sessionPersistence.append(child.id, childEvents)
      await ctx.sessionPersistence.create(grandchild)
      await ctx.sessionPersistence.append(grandchild.id, grandchildEvents)
    })

    expect((await ctx.sessionPersistence.load(child.id)).meta).toEqual(child)
    expect((await ctx.sessionPersistence.load(grandchild.id)).events).toEqual(grandchildEvents)

    // DSH may flush a child batch after the AsyncLocal prompt callback has
    // returned. The backend retains only the root authority and revalidates
    // the durable lineage on every commit.
    await ctx.sessionPersistence.append(child.id, completedTurn(6, 2))
    expect((await ctx.sessionPersistence.load(child.id)).events).toHaveLength(12)
  })

  it('rejects a forged subagent lineage outside the active root Session', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const root = header('lineage-root')
    const unrelated = header('lineage-unrelated')
    const forged: SessionHeader = {
      ...header('lineage-forged'),
      parentSession: unrelated.id,
      origin: 'subagent',
      delegationDepth: 1,
    }
    await ctx.cloudRunContext.run(authority(unrelated.id, 2), async () => {
      await ctx.sessionPersistence.create(unrelated)
      await ctx.sessionPersistence.append(unrelated.id, completedTurn())
    })
    await ctx.cloudRunContext.run(authority(root.id, 3), async () => {
      await ctx.sessionPersistence.create(root)
      await ctx.sessionPersistence.append(root.id, completedTurn())
      await ctx.sessionPersistence.create(forged)
      await expect(ctx.sessionPersistence.append(forged.id, completedTurn()))
        .rejects.toThrow(/not a descendant/)
    })
  })

  it('durably closes an interrupted turn when a new Worker resumes it', async () => {
    const namespace = `test-${randomUUID()}`
    const first = await backend(namespace)
    const meta = header('interrupted')
    await first.ctx.sessionPersistence.create(meta)
    await first.ctx.cloudRunContext.run(authority(meta.id, 8), async () => {
      await first.ctx.sessionPersistence.append(meta.id, completedTurn())
      await first.ctx.sessionPersistence.append(meta.id, [
        { type: 'turn/start', seq: 6, time: 7, data: { turn: 2 } },
        { type: 'step/start', seq: 7, time: 8, data: { turn: 2, step: 1 } },
      ])
    })

    const pool = new Pool({ connectionString: databaseUrl as string })
    try {
      const shape = await pool.query<{
        activeBatches: string
        segments: string
        legacyEvents: string | null
        legacyOutbox: string | null
        payloadColumns: string
      }>(`
        SELECT
          (SELECT count(*) FROM dsh_cloud.session_event_batches
            WHERE namespace=$1 AND session_id=$2)::text AS "activeBatches",
          (SELECT count(*) FROM dsh_cloud.session_segments
            WHERE namespace=$1 AND session_id=$2)::text AS segments,
          to_regclass('dsh_cloud.session_events')::text AS "legacyEvents",
          to_regclass('dsh_cloud.session_event_outbox')::text AS "legacyOutbox",
          (SELECT count(*) FROM information_schema.columns
            WHERE table_schema='dsh_cloud' AND table_name='session_event_batches'
              AND column_name IN ('payload','event','records'))::text AS "payloadColumns"
      `, [namespace, meta.id])
      expect(shape.rows[0]).toEqual({
        activeBatches: '1',
        segments: '1',
        legacyEvents: null,
        legacyOutbox: null,
        payloadColumns: '0',
      })
    } finally {
      await pool.end()
    }

    // Valkey is a projection, not authority. Simulate total projection loss;
    // the next Worker must rebuild it from PostgreSQL locators + Kafka bytes.
    await first.ctx.sessionLiveProjection.reset(namespace, String(meta.id), -1)
    await first.dispose()
    disposers.splice(disposers.indexOf(first.dispose), 1)

    const resumed = await backend(namespace)
    const loaded = await resumed.ctx.cloudRunContext.run(authority(meta.id, 9), () =>
      resumed.ctx.sessionPersistence.load(meta.id))
    expect(loaded.events.slice(-2).map(event => event.type)).toEqual(['step/end', 'turn/end'])
    const final = loaded.events.at(-1)
    expect(final?.type === 'turn/end' ? final.data.reason : undefined)
      .toEqual({ kind: 'interrupted' })
  })

  it('keeps token-sized deltas out of PostgreSQL rows after sealing the native DSH Turn', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('packed-chunks')
    const chunks: SessionEvent[] = Array.from({ length: 50 }, (_, index) => ({
      type: 'assistant/chunk',
      seq: index + 3,
      time: 10 + index,
      data: {
        turn: 1,
        step: 1,
        chunk: { type: 'reasoning-delta', index: 0, text: `token-${index}` },
      },
    }))
    const log: SessionEvent[] = [
      { type: 'turn/start', seq: 0, time: 1, data: { turn: 1 } },
      {
        type: 'user/message',
        seq: 1,
        time: 2,
        data: createMessage({
          id: MessageId('user-packed'),
          role: 'user',
          content: [{ type: 'text', text: 'stream a long answer' }],
          source: { kind: 'user' },
        }),
        surfaceOp: 'append',
      },
      { type: 'step/start', seq: 2, time: 3, data: { turn: 1, step: 1 } },
      ...chunks,
      {
        type: 'assistant/message',
        seq: 53,
        time: 61,
        surfaceOp: 'append',
        sourceEventSeqs: chunks.map(event => event.seq),
        data: {
          turn: 1,
          step: 1,
          message: createMessage({
            id: MessageId('assistant-packed'),
            role: 'assistant',
            content: [{ type: 'reasoning', text: chunks.map(event =>
              event.type === 'assistant/chunk' && 'text' in event.data.chunk
                ? event.data.chunk.text
                : '').join('') }],
            source: { kind: 'model', provider: 'test', model: 'test' },
          }),
        },
      },
      { type: 'step/end', seq: 54, time: 62, data: { turn: 1, step: 1 } },
      { type: 'turn/end', seq: 55, time: 63, data: { turn: 1, reason: { kind: 'completed' } } },
    ]

    await ctx.sessionPersistence.create(meta)
    await ctx.cloudRunContext.run(authority(meta.id, 1), () =>
      ctx.sessionPersistence.append(meta.id, log))

    expect((await ctx.sessionPersistence.load(meta.id)).events).toEqual(log)
    expect((await ctx.sessionPersistence.readFrom(meta.id, 20)).events).toEqual(log.slice(20))

    const pool = new Pool({ connectionString: databaseUrl as string })
    try {
      const rows = await pool.query<{ activeBatches: string; segments: string }>(`
        SELECT
          (SELECT count(*) FROM dsh_cloud.session_event_batches
            WHERE namespace=$1 AND session_id=$2)::text AS "activeBatches",
          (SELECT count(*) FROM dsh_cloud.session_segments
            WHERE namespace=$1 AND session_id=$2)::text AS segments
      `, [namespace, meta.id])
      expect(rows.rows[0]).toEqual({ activeBatches: '0', segments: '1' })
    } finally {
      await pool.end()
    }
  })

  it('does not reread the Kafka hot tail for non-terminal streaming batches', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('streaming-hot-path')
    const log = completedTurn()
    let reads = 0
    const originalRead = ctx.sessionLiveLog.read.bind(ctx.sessionLiveLog)
    ctx.sessionLiveLog.read = async (...args) => {
      reads += 1
      return originalRead(...args)
    }

    await ctx.sessionPersistence.create(meta)
    await ctx.cloudRunContext.run(authority(meta.id, 1), () =>
      ctx.sessionPersistence.append(meta.id, log.slice(0, -3)))
    expect(reads).toBe(0)

    await ctx.cloudRunContext.run(authority(meta.id, 1), () =>
      ctx.sessionPersistence.append(meta.id, log.slice(-3)))
    expect(reads).toBeGreaterThan(0)
    expect((await ctx.sessionPersistence.load(meta.id)).events).toEqual(log)
  })

  it('restores the effective runtime surface from a compaction baseline without reading its physical prefix', async () => {
    const namespace = `test-${randomUUID()}`
    const first = await backend(namespace)
    const meta = header('checkpoint-suffix')
    const compacted = [...completedTurn(), ...compaction(6, [1, 3], 'turn one summary')]
    const suffix = completedTurn(10, 2)
    await first.ctx.sessionPersistence.create(meta)
    await first.ctx.cloudRunContext.run(authority(meta.id, 1), () =>
      first.ctx.sessionPersistence.append(meta.id, compacted))
    await first.ctx.cloudRunContext.run(authority(meta.id, 1), () =>
      first.ctx.sessionPersistence.append(meta.id, suffix))
    await first.dispose()
    disposers.splice(disposers.indexOf(first.dispose), 1)

    const pool = new Pool({ connectionString: databaseUrl as string })
    try {
      const checkpoint = await pool.query<{ throughSeq: string; eventCount: string }>(`
        SELECT through_seq::text AS "throughSeq", event_count::text AS "eventCount"
        FROM dsh_cloud.session_runtime_baselines
        WHERE namespace=$1 AND session_id=$2
      `, [namespace, meta.id])
      expect(checkpoint.rows[0]).toEqual({ throughSeq: '9', eventCount: '10' })

      // Damage a canonical segment wholly below the baseline boundary. Runtime
      // preparation must not read it, proving the physical prefix is skipped.
      await pool.query(`
        UPDATE dsh_cloud.session_segments SET payload=$3
        WHERE namespace=$1 AND session_id=$2 AND seq_end < 10
      `, [namespace, meta.id, Buffer.from('deliberately unreadable old segment')])
    } finally {
      await pool.end()
    }

    const resumed = await backend(namespace)
    const expected = Session.create(meta.id, [...compacted, ...suffix], meta).deriveMessages()
    const prepared = await resumed.ctx.cloudRunContext.run(authority(meta.id, 2), () =>
      resumed.ctx.sessionPersistence.prepare(meta.id))
    try {
      expect(prepared.session.deriveMessages()).toEqual(expected)
      expect(prepared.session.events.some(event => event.type === 'session/runtime-gap')).toBe(true)
      expect(prepared.session.events.filter(event => event.type !== 'session/runtime-gap').length)
        .toBeLessThan(compacted.length + suffix.length)
    } finally {
      prepared[Symbol.dispose]()
    }
  })

  it('repairs an interrupted suffix after restoring a compaction checkpoint', async () => {
    const namespace = `test-${randomUUID()}`
    const first = await backend(namespace)
    const meta = header('checkpoint-interrupted')
    const compacted = [...completedTurn(), ...compaction(6, [1, 3], 'stable summary')]
    await first.ctx.sessionPersistence.create(meta)
    await first.ctx.cloudRunContext.run(authority(meta.id, 3), async () => {
      await first.ctx.sessionPersistence.append(meta.id, compacted)
      await first.ctx.sessionPersistence.append(meta.id, [
        { type: 'turn/start', seq: 10, time: 20, data: { turn: 2 } },
        { type: 'step/start', seq: 11, time: 21, data: { turn: 2, step: 1 } },
      ])
    })
    await first.dispose()
    disposers.splice(disposers.indexOf(first.dispose), 1)

    const resumed = await backend(namespace)
    const loaded = await resumed.ctx.cloudRunContext.run(authority(meta.id, 4), () =>
      resumed.ctx.sessionPersistence.load(meta.id))
    expect(loaded.events.slice(0, compacted.length)).toEqual(compacted)
    expect(loaded.events.slice(-2).map(event => event.type)).toEqual(['step/end', 'turn/end'])
    const final = loaded.events.at(-1)
    expect(final?.type === 'turn/end' ? final.data.reason : undefined)
      .toEqual({ kind: 'interrupted' })
  })

  it('atomically advances one runtime baseline across repeated compactions', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('checkpoint-repeated')
    const firstCompaction = [...completedTurn(), ...compaction(6, [1, 3], 'first summary')]
    const secondTurn = completedTurn(10, 2)
    const secondCompaction = compaction(16, [8, 11, 13], 'second summary', 2)
    await ctx.sessionPersistence.create(meta)
    await ctx.cloudRunContext.run(authority(meta.id, 5), async () => {
      await ctx.sessionPersistence.append(meta.id, firstCompaction)
      await ctx.sessionPersistence.append(meta.id, secondTurn)
      await ctx.sessionPersistence.append(meta.id, secondCompaction)
    })

    const pool = new Pool({ connectionString: databaseUrl as string })
    try {
      const checkpoints = await pool.query<{ count: string; throughSeq: string }>(`
        SELECT count(*)::text AS count, max(through_seq)::text AS "throughSeq"
        FROM dsh_cloud.session_runtime_baselines
        WHERE namespace=$1 AND session_id=$2
      `, [namespace, meta.id])
      expect(checkpoints.rows[0]).toEqual({ count: '1', throughSeq: '19' })
    } finally {
      await pool.end()
    }
    expect((await ctx.sessionPersistence.load(meta.id)).events)
      .toEqual([...firstCompaction, ...secondTurn, ...secondCompaction])
  })

  it('falls back to the canonical log when derived runtime baseline bytes are damaged', async () => {
    const namespace = `test-${randomUUID()}`
    const first = await backend(namespace)
    const meta = header('checkpoint-fallback')
    const log = [...completedTurn(), ...compaction(6, [1, 3], 'fallback summary')]
    await first.ctx.sessionPersistence.create(meta)
    await first.ctx.cloudRunContext.run(authority(meta.id, 6), () =>
      first.ctx.sessionPersistence.append(meta.id, log))
    await first.dispose()
    disposers.splice(disposers.indexOf(first.dispose), 1)

    const pool = new Pool({ connectionString: databaseUrl as string })
    try {
      await pool.query(`
        UPDATE dsh_cloud.session_runtime_baselines SET payload=$3
        WHERE namespace=$1 AND session_id=$2
      `, [namespace, meta.id, Buffer.from('damaged derived checkpoint')])
    } finally {
      await pool.end()
    }

    const resumed = await backend(namespace)
    const loaded = await resumed.ctx.cloudRunContext.run(authority(meta.id, 7), () =>
      resumed.ctx.sessionPersistence.load(meta.id))
    expect(loaded.events).toEqual(log)
  })
})
