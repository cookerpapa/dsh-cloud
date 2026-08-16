import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import { createMessage, MessageId } from '@deepseek-ai/dsh-llm'
import SessionStore, { SESSION_FORMAT_VERSION, SessionId, type SessionEvent, type SessionHeader } from '@deepseek-ai/dsh-session'
import { Pool } from 'pg'
import { afterEach, describe, expect, test } from 'vitest'
import { ControlStore, type ClaimedRun } from '@dsh-cloud/control-store'
import CloudRunContext, { cloudIdentifier, writerFence } from '@dsh-cloud/run-context'
import { PostgresRunWorker, type RunExecutionBackend } from '@dsh-cloud/run-queue'
import PostgresSessionPersistence, { StaleSessionWriterError } from '@dsh-cloud/session-persistence-postgres'
import { EXECUTION_PROTOCOL_VERSION, type ExecutionRequest, type ExecutionResponse } from '@dsh-cloud/execution-protocol'
import { SandboxManager, type SandboxBinding, type SandboxHandle, type SandboxProvider } from '@dsh-cloud/sandbox-manager'

const connectionString = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const integration = connectionString === undefined ? describe.skip : describe
const cleanups: Array<() => Promise<void>> = []

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

async function eventually<T>(read: () => Promise<T>, accept: (value: T) => boolean, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = await read()
    if (accept(value)) return value
    await new Promise(resolve => setTimeout(resolve, 25))
  }
  throw new Error('condition did not become true before its deadline')
}

integration('cloud failure semantics', () => {
  test('rejects tool calls as soon as their RunAttempt lease is stale', async () => {
    const namespace = `fence-${randomUUID()}`
    const pool = new Pool({ connectionString, max: 10 })
    const store = new ControlStore(pool, namespace)
    await store.initialize()
    const principal = await store.register('Fence Tenant', `${randomUUID()}@example.test`, 'correct horse battery staple')
    const workspace = await store.createWorkspace(principal.tenantId, 'Fence Workspace')
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId: principal.tenantId, workspaceId: workspace.id })
    await store.heartbeatWorker({ id: 'fence-worker', baseUrl: 'http://127.0.0.1:49100', maximumRuns: 1 })
    const rpcId = randomUUID()
    const admitted = await store.enqueueRun({
      tenantId: principal.tenantId,
      sessionId,
      clientRpcId: rpcId,
      idempotencyKey: rpcId,
      request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } },
    })
    const claimed = await store.claimNext('fence-worker')
    expect(claimed.kind).toBe('claimed')
    if (claimed.kind !== 'claimed') return
    let executions = 0
    const provider: SandboxProvider = {
      async create(): Promise<SandboxHandle> { return { provider: 'fake', sandboxId: randomUUID(), endpoint: 'http://unused' } },
      async bind(): Promise<void> {},
      async execute(_handle: SandboxHandle, _binding: SandboxBinding, request: ExecutionRequest): Promise<ExecutionResponse> {
        executions++
        return { protocolVersion: EXECUTION_PROTOCOL_VERSION, operationId: request.operationId, ok: true, result: [] }
      },
      async destroy(): Promise<void> {},
      async inspect(): Promise<'ready'> { return 'ready' },
    }
    const manager = new SandboxManager({
      pool,
      namespace,
      internalToken: 'fault-test-manager-token-with-32-characters',
      encryptionKey: Buffer.alloc(32, 7),
      provider,
      attemptLeaseSeconds: 20,
    })
    await manager.initialize()
    cleanups.push(async () => {
      await pool.query('DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
      await pool.end()
    })
    const request: ExecutionRequest = {
      protocolVersion: EXECUTION_PROTOCOL_VERSION,
      operationId: randomUUID(),
      authority: {
        tenantId: cloudIdentifier('TenantId', claimed.run.tenantId),
        workspaceId: cloudIdentifier('WorkspaceId', claimed.run.workspaceId),
        sessionId: cloudIdentifier('SessionId', claimed.run.sessionId),
        runId: cloudIdentifier('RunId', claimed.run.runId),
        attemptId: cloudIdentifier('AttemptId', claimed.run.attemptId),
        writerFence: writerFence(claimed.run.writerFence),
      },
      operation: { kind: 'fs.list', path: '/workspace' },
    }
    await expect(manager.execute(request)).resolves.toMatchObject({ ok: true })
    await pool.query(`UPDATE dsh_cloud_control.runs SET status='cancel_requested' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.runId])
    await expect(manager.execute({ ...request, operationId: randomUUID() })).rejects.toMatchObject({ code: 'ECANCELED' })
    await pool.query(`UPDATE dsh_cloud_control.runs SET status='claimed' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.runId])
    await pool.query(`UPDATE dsh_cloud_control.run_attempts SET heartbeat_at=now()-interval '1 minute' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.attemptId])
    await expect(manager.execute({ ...request, operationId: randomUUID() })).rejects.toMatchObject({ code: 'ESTALE' })
    expect(executions).toBe(1)
    expect((await store.runResponse(admitted.runId))?.status).toBe('claimed')
  })

  test('does not replay a Run after its user prompt crossed the durable boundary', async () => {
    const namespace = `fault-${randomUUID()}`
    const pool = new Pool({ connectionString, max: 10 })
    const store = new ControlStore(pool, namespace)
    await store.initialize()
    const principal = await store.register('Failure Tenant', `${randomUUID()}@example.test`, 'correct horse battery staple')
    const workspace = await store.createWorkspace(principal.tenantId, 'Failure Workspace')
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId: principal.tenantId, workspaceId: workspace.id })
    await store.heartbeatWorker({ id: 'crashed-worker', baseUrl: 'http://127.0.0.1:49101', maximumRuns: 1 })

    const ctx = new Context()
    const fibers = [
      await ctx.plugin(SessionStore),
      await ctx.plugin(CloudRunContext),
      await ctx.plugin(PostgresSessionPersistence, {
        connectionString: connectionString as string,
        namespace,
        requireWriterAuthority: true,
        validateControlAuthority: true,
        attemptLeaseSeconds: 20,
        writeBatchMaxDelayMs: 1,
      }),
    ]
    const dispose = async (): Promise<void> => {
      for (const fiber of fibers.reverse()) await fiber.dispose()
      await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud.sessions WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud.persistence_state WHERE namespace=$1', [namespace])
      await pool.end()
    }
    cleanups.push(dispose)

    const rpcId = randomUUID()
    const request = { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } }
    const admitted = await store.enqueueRun({ tenantId: principal.tenantId, sessionId, clientRpcId: rpcId, idempotencyKey: rpcId, request })
    const claimed = await store.claimNext('crashed-worker')
    expect(claimed.kind).toBe('claimed')
    if (claimed.kind !== 'claimed') return

    const header: SessionHeader = { version: SESSION_FORMAT_VERSION, id: SessionId(sessionId), createdAt: Date.now(), cwd: '/workspace' }
    await ctx.sessionPersistence.create(header)
    const authority = {
      tenantId: cloudIdentifier('TenantId', claimed.run.tenantId),
      workspaceId: cloudIdentifier('WorkspaceId', claimed.run.workspaceId),
      sessionId: cloudIdentifier('SessionId', claimed.run.sessionId),
      runId: cloudIdentifier('RunId', claimed.run.runId),
      attemptId: cloudIdentifier('AttemptId', claimed.run.attemptId),
      writerFence: writerFence(claimed.run.writerFence),
    }
    const prompt = createMessage({
      id: MessageId(`prompt-${rpcId}`),
      role: 'user',
      content: [{ type: 'text', text: 'change the workspace' }],
      source: { kind: 'user', rpcId } as never,
    })
    const events = [
      { type: 'turn/start', seq: 0, time: Date.now(), data: { turn: 1 } },
      { type: 'user/message', seq: 1, time: Date.now(), data: prompt, surfaceOp: 'append' },
      { type: 'step/start', seq: 2, time: Date.now(), data: { turn: 1, step: 1 } },
    ] as SessionEvent[]
    await ctx.cloudRunContext.run(authority, () => ctx.sessionPersistence.append(header.id, events))
    expect(await store.promptPersisted(admitted.runId)).toBe(true)
    await pool.query(`UPDATE dsh_cloud_control.run_attempts SET heartbeat_at=now()-interval '1 minute' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.attemptId])
    expect(await store.reconcileExpiredAttempts(20)).toEqual({ requeued: 0, failed: 1 })
    expect((await store.runResponse(admitted.runId))?.status).toBe('failed')
    expect((await store.claimNext('crashed-worker')).kind).toBe('idle')
    await expect(ctx.cloudRunContext.run(authority, () => ctx.sessionPersistence.append(header.id, [
      { type: 'step/end', seq: 3, time: Date.now(), data: { turn: 1, step: 1 } },
    ]))).rejects.toBeInstanceOf(StaleSessionWriterError)
  })

  test('propagates cancellation to the active backend and commits one terminal Run state', async () => {
    const namespace = `cancel-${randomUUID()}`
    const pool = new Pool({ connectionString, max: 10 })
    const store = new ControlStore(pool, namespace)
    await store.initialize()
    const principal = await store.register('Cancel Tenant', `${randomUUID()}@example.test`, 'correct horse battery staple')
    const workspace = await store.createWorkspace(principal.tenantId, 'Cancel Workspace')
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId: principal.tenantId, workspaceId: workspace.id })
    const rpcId = randomUUID()
    const request = { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } }
    const admitted = await store.enqueueRun({ tenantId: principal.tenantId, sessionId, clientRpcId: rpcId, idempotencyKey: rpcId, request })

    let cancelled = false
    const backend: RunExecutionBackend = {
      async dispatch(run: ClaimedRun): Promise<unknown> {
        await pool.query(`
          INSERT INTO dsh_cloud.persistence_state(namespace,store_id) VALUES($1,$2)
          ON CONFLICT(namespace) DO NOTHING
        `, [namespace, randomUUID()])
        await pool.query(`
          INSERT INTO dsh_cloud.sessions(namespace,id,header,incarnation,revision,next_seq,writer_fence,writer_attempt_id)
          VALUES($1,$2,$3,$4,1,1,$5,$6)
        `, [namespace, sessionId, JSON.stringify({ version: SESSION_FORMAT_VERSION, id: sessionId, createdAt: Date.now(), cwd: '/workspace' }), randomUUID(), run.writerFence, run.attemptId])
        await pool.query(`
          INSERT INTO dsh_cloud.session_events(namespace,session_id,seq,seq_end,event) VALUES($1,$2,0,0,$3)
        `, [namespace, sessionId, JSON.stringify({ type: 'user/message', seq: 0, time: Date.now(), data: { source: { kind: 'user', rpcId } } })])
        return { accepted: true }
      },
      async cancel(run: ClaimedRun): Promise<'accepted'> {
        cancelled = true
        await pool.query(`INSERT INTO dsh_cloud.session_events(namespace,session_id,seq,seq_end,event) VALUES($1,$2,1,1,$3)`, [namespace, sessionId, JSON.stringify({ type: 'turn/end', seq: 1, time: Date.now(), data: { turn: 1, reason: { kind: 'interrupted' } } })])
        await pool.query(`UPDATE dsh_cloud.sessions SET next_seq=2,revision=revision+1 WHERE namespace=$1 AND id=$2`, [namespace, sessionId])
        return 'accepted'
      },
    }
    const worker = new PostgresRunWorker({
      store,
      notificationConnectionString: connectionString as string,
      identity: 'cancel-worker',
      baseUrl: 'http://127.0.0.1:49102',
      maximumConcurrentRuns: 1,
      pollIntervalMs: 25,
      backend,
    })
    cleanups.push(async () => {
      await worker.stop()
      await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud.sessions WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud.persistence_state WHERE namespace=$1', [namespace])
      await pool.end()
    })
    await worker.start()
    await eventually(() => store.runResponse(admitted.runId), value => value?.status === 'running')
    expect(await store.requestCancellation(principal.tenantId, admitted.runId)).toBe(true)
    const terminal = await eventually(() => store.runResponse(admitted.runId), value => value?.status === 'cancelled')
    expect(terminal?.status).toBe('cancelled')
    expect(cancelled).toBe(true)
  })

  test('does not report a durable model-error turn as a completed Run', async () => {
    const namespace = `model-error-${randomUUID()}`
    const pool = new Pool({ connectionString, max: 10 })
    const store = new ControlStore(pool, namespace)
    await store.initialize()
    const principal = await store.register('Model Error Tenant', `${randomUUID()}@example.test`, 'correct horse battery staple')
    const workspace = await store.createWorkspace(principal.tenantId, 'Model Error Workspace')
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId: principal.tenantId, workspaceId: workspace.id })
    const rpcId = randomUUID()
    const request = { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } }
    const admitted = await store.enqueueRun({ tenantId: principal.tenantId, sessionId, clientRpcId: rpcId, idempotencyKey: rpcId, request })
    const backend: RunExecutionBackend = {
      async dispatch(run: ClaimedRun): Promise<unknown> {
        await pool.query(`INSERT INTO dsh_cloud.persistence_state(namespace,store_id) VALUES($1,$2) ON CONFLICT(namespace) DO NOTHING`, [namespace, randomUUID()])
        await pool.query(`
          INSERT INTO dsh_cloud.sessions(namespace,id,header,incarnation,revision,next_seq,writer_fence,writer_attempt_id)
          VALUES($1,$2,$3,$4,1,2,$5,$6)
        `, [namespace, sessionId, JSON.stringify({ version: SESSION_FORMAT_VERSION, id: sessionId, createdAt: Date.now(), cwd: '/workspace' }), randomUUID(), run.writerFence, run.attemptId])
        await pool.query(`
          INSERT INTO dsh_cloud.session_events(namespace,session_id,seq,seq_end,event) VALUES
            ($1,$2,0,0,$3),($1,$2,1,1,$4)
        `, [
          namespace,
          sessionId,
          JSON.stringify({ type: 'user/message', seq: 0, time: Date.now(), data: { source: { kind: 'user', rpcId } } }),
          JSON.stringify({ type: 'turn/end', seq: 1, time: Date.now(), data: { turn: 1, reason: { kind: 'error', error: { message: 'provider transport failed', code: 'TRANSPORT' } } } }),
        ])
        return { accepted: true }
      },
      async cancel(): Promise<'absent'> { return 'absent' },
    }
    const worker = new PostgresRunWorker({
      store,
      notificationConnectionString: connectionString as string,
      identity: 'model-error-worker',
      baseUrl: 'http://127.0.0.1:49103',
      maximumConcurrentRuns: 1,
      pollIntervalMs: 25,
      backend,
    })
    cleanups.push(async () => {
      await worker.stop()
      await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud.sessions WHERE namespace=$1', [namespace])
      await pool.query('DELETE FROM dsh_cloud.persistence_state WHERE namespace=$1', [namespace])
      await pool.end()
    })
    await worker.start()
    const terminal = await eventually(() => store.runResponse(admitted.runId), value => value?.status === 'failed')
    expect(terminal?.status).toBe('failed')
    const stored = await pool.query<{ error_code: string }>(`SELECT error_code FROM dsh_cloud_control.runs WHERE namespace=$1 AND id=$2`, [namespace, admitted.runId])
    expect(stored.rows[0]?.error_code).toBe('TRANSPORT')
  })
})
