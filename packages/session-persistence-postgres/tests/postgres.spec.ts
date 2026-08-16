import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import {
  SESSION_FORMAT_VERSION,
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
import PostgresSessionPersistence, { StaleSessionWriterError } from '../src/index.ts'

const databaseUrl = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const integration = databaseUrl === undefined ? describe.skip : describe
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
  const persistenceFiber = await ctx.plugin(PostgresSessionPersistence, {
    connectionString: databaseUrl as string,
    namespace,
    requireWriterAuthority: true,
    writeBatchMaxDelayMs: 1,
  })
  const dispose = async (): Promise<void> => {
    await persistenceFiber.dispose()
    await runFiber.dispose()
    await sessionFiber.dispose()
  }
  disposers.push(dispose)
  return { ctx, dispose }
}

integration('PostgresSessionPersistence', () => {
  it('round-trips native DSH events through the official persistence contract', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('round-trip')
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

    await resumed.ctx.cloudRunContext.run(authority(meta.id, 5), () =>
      resumed.ctx.sessionPersistence.append(meta.id, completedTurn(6, 2)))
    expect((await resumed.ctx.sessionPersistence.load(meta.id)).events).toHaveLength(12)
  })

  it('does not let authority for one Session publish another Session', async () => {
    const namespace = `test-${randomUUID()}`
    const { ctx } = await backend(namespace)
    const meta = header('authority-bound')
    await ctx.sessionPersistence.create(meta)
    await expect(ctx.cloudRunContext.run(authority('another-session', 1), () =>
      ctx.sessionPersistence.append(meta.id, completedTurn())))
      .rejects.toThrow(/cannot write session/)
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

  it('packs token-sized assistant deltas without changing the native DSH log', async () => {
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
      const rows = await pool.query<{ count: string }>(`
        SELECT count(*)::text AS count
        FROM dsh_cloud.session_events
        WHERE namespace=$1 AND session_id=$2
      `, [namespace, meta.id])
      expect(Number(rows.rows[0]?.count)).toBeLessThan(log.length / 5)
    } finally {
      await pool.end()
    }
  })
})
