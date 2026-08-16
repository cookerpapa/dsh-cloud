import { randomUUID } from 'node:crypto'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { Pool } from 'pg'
import { ControlStore } from '../src/index.js'

const connectionString = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const enabled = connectionString === undefined ? describe.skip : describe

enabled('PostgreSQL control authority', () => {
  const namespace = `test-${randomUUID()}`
  const pool = new Pool({ connectionString, max: 10 })
  const store = new ControlStore(pool, namespace)
  let tenantId = ''
  let workspaceId = ''

  beforeAll(async () => {
    await pool.query(`
      BEGIN;
      SELECT pg_advisory_xact_lock(hashtextextended('dsh_cloud.session-persistence:migration',0));
      CREATE SCHEMA IF NOT EXISTS dsh_cloud;
      CREATE TABLE IF NOT EXISTS dsh_cloud.schema_state (
        singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), version integer NOT NULL
      );
      INSERT INTO dsh_cloud.schema_state(singleton,version) VALUES(true,5)
        ON CONFLICT(singleton) DO NOTHING;
      CREATE TABLE IF NOT EXISTS dsh_cloud.persistence_state(namespace text PRIMARY KEY,store_id uuid NOT NULL);
      CREATE TABLE IF NOT EXISTS dsh_cloud.sessions (
        namespace text NOT NULL REFERENCES dsh_cloud.persistence_state(namespace),id text NOT NULL,header jsonb NOT NULL,
        incarnation uuid NOT NULL,revision bigint NOT NULL CHECK(revision>=0),next_seq bigint NOT NULL CHECK(next_seq>=0),
        sealed_through bigint NOT NULL DEFAULT -1 CHECK(sealed_through>=-1),
        projected_through bigint NOT NULL DEFAULT -1 CHECK(projected_through>=-1),
        writer_fence bigint NOT NULL CHECK(writer_fence>=0),writer_attempt_id text,PRIMARY KEY(namespace,id)
      );
      CREATE TABLE IF NOT EXISTS dsh_cloud.session_event_markers (
        namespace text NOT NULL,session_id text NOT NULL,seq bigint NOT NULL CHECK(seq>=0),
        type text NOT NULL CHECK(type IN ('user/message','turn/end')),rpc_id text,reason jsonb,
        PRIMARY KEY(namespace,session_id,seq),
        FOREIGN KEY(namespace,session_id) REFERENCES dsh_cloud.sessions(namespace,id) ON DELETE CASCADE
      );
      COMMIT;
    `)
    await store.initialize()
    const principal = await store.register('Tenant A', 'owner@example.test', 'correct horse battery staple')
    tenantId = principal.tenantId
    workspaceId = (await store.createWorkspace(tenantId, 'Main')).id
    await Promise.all([
      store.heartbeatWorker({ id: 'worker-a', baseUrl: 'http://127.0.0.1:4101', maximumRuns: 4 }),
      store.heartbeatWorker({ id: 'worker-b', baseUrl: 'http://127.0.0.1:4102', maximumRuns: 4 }),
    ])
  })

  afterAll(async () => {
    await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1', [namespace])
    await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
    await pool.end()
  })

  test('auth tokens do not expose passwords and are revocable', async () => {
    const principal = await store.login('OWNER@example.test', 'correct horse battery staple')
    expect(principal?.tenantId).toBe(tenantId)
    const token = await store.issueAuthSession(principal!.userId, 60)
    expect((await store.authenticate(token))?.email).toBe('owner@example.test')
    await store.revokeAuthSession(token)
    expect(await store.authenticate(token)).toBeUndefined()
  })

  test('balances new users across healthy Workers while preserving an existing route', async () => {
    const first = await store.login('owner@example.test', 'correct horse battery staple')
    // Display names are not identities: different tenants may legitimately choose the same name.
    const second = await store.register('Tenant A', 'second@example.test', 'correct horse battery staple')
    const firstWorker = await store.routeWorker(first!.userId)
    const secondWorker = await store.routeWorker(second.userId)
    expect(firstWorker?.id).toBeDefined()
    expect(secondWorker?.id).toBeDefined()
    expect(secondWorker?.id).not.toBe(firstWorker?.id)
    expect((await store.routeWorker(first!.userId))?.id).toBe(firstWorker?.id)
  })

  test('admission is idempotent and tenant scoped', async () => {
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId, workspaceId })
    const request = { type: 'client-request', rpcId: randomUUID(), method: 'session.prompt', payload: { sessionId } }
    const first = await store.enqueueRun({ tenantId, sessionId, clientRpcId: request.rpcId, idempotencyKey: 'idem-a', request })
    const duplicate = await store.enqueueRun({ tenantId, sessionId, clientRpcId: request.rpcId, idempotencyKey: 'idem-a', request })
    expect(first.created).toBe(true)
    expect(duplicate).toMatchObject({ runId: first.runId, created: false })
    await expect(store.enqueueRun({ tenantId: randomUUID(), sessionId, clientRpcId: randomUUID(), idempotencyKey: 'foreign', request })).rejects.toMatchObject({ code: 'EACCES' })
    expect(await store.requestCancellation(tenantId, first.runId)).toBe(true)
  })

  test('workers serialize one Session while different Sessions run concurrently', async () => {
    const sessionA = randomUUID()
    const sessionB = randomUUID()
    const workspaceB = (await store.createWorkspace(tenantId, `Other-${sessionB}`)).id
    await store.registerSession({ sessionId: sessionA, tenantId, workspaceId })
    await store.registerSession({ sessionId: sessionB, tenantId, workspaceId: workspaceB })
    for (const [key, sessionId] of [['a1', sessionA], ['a2', sessionA], ['b1', sessionB]] as const) {
      const rpcId = randomUUID()
      await store.enqueueRun({ tenantId, sessionId, clientRpcId: rpcId, idempotencyKey: key, request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } } })
    }
    const first = await store.claimNext('worker-a')
    const second = await store.claimNext('worker-b')
    expect(first.kind).toBe('claimed')
    expect(second.kind).toBe('claimed')
    if (first.kind !== 'claimed' || second.kind !== 'claimed') return
    expect(first.run.sessionId).not.toBe(second.run.sessionId)
    const occupiedSession = first.run.sessionId
    expect((await store.claimNext('worker-a')).kind).toBe('idle')
    await store.finishRun(first.run.runId, first.run.attemptId, 'completed')
    const next = await store.claimNext('worker-a')
    expect(next.kind).toBe('claimed')
    if (next.kind === 'claimed') {
      expect(next.run.sessionId).toBe(occupiedSession)
      expect(next.run.writerFence).toBeGreaterThan(first.run.writerFence)
      await store.finishRun(next.run.runId, next.run.attemptId, 'completed')
    }
    await store.finishRun(second.run.runId, second.run.attemptId, 'completed')
  })

  test('allocates one monotonic writer fence across Sessions sharing a Workspace', async () => {
    const sharedWorkspace = (await store.createWorkspace(tenantId, `Shared-${randomUUID()}`)).id
    const firstSession = randomUUID()
    const secondSession = randomUUID()
    await store.registerSession({ sessionId: firstSession, tenantId, workspaceId: sharedWorkspace })
    await store.registerSession({ sessionId: secondSession, tenantId, workspaceId: sharedWorkspace })

    const submit = async (sessionId: string) => {
      const rpcId = randomUUID()
      await store.enqueueRun({
        tenantId,
        sessionId,
        clientRpcId: rpcId,
        idempotencyKey: `shared-${rpcId}`,
        request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } },
      })
      const claimed = await store.claimNext('worker-a')
      expect(claimed.kind).toBe('claimed')
      if (claimed.kind !== 'claimed') throw new Error('expected a claimed Run')
      return claimed.run
    }

    const first = await submit(firstSession)
    await store.finishRun(first.runId, first.attemptId, 'completed')
    const second = await submit(secondSession)
    expect(second.writerFence).toBeGreaterThan(first.writerFence)
    await store.finishRun(second.runId, second.attemptId, 'completed')
  })

  test('an expired pre-prompt claim is requeued and releases capacity', async () => {
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId, workspaceId, preferredWorkerId: 'worker-a' })
    const rpcId = randomUUID()
    await store.enqueueRun({ tenantId, sessionId, clientRpcId: rpcId, idempotencyKey: `expiry-${rpcId}`, request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } } })
    const claimed = await store.claimNext('worker-a')
    expect(claimed.kind).toBe('claimed')
    if (claimed.kind !== 'claimed') return
    await pool.query(`UPDATE dsh_cloud_control.run_attempts SET heartbeat_at=now()-interval '1 minute' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.attemptId])
    expect(await store.reconcileExpiredAttempts(20)).toEqual({ requeued: 1, failed: 0 })
    const reclaimed = await store.claimNext('worker-a')
    expect(reclaimed.kind).toBe('claimed')
    if (reclaimed.kind === 'claimed') {
      expect(reclaimed.run.runId).toBe(claimed.run.runId)
      expect(reclaimed.run.writerFence).toBeGreaterThan(claimed.run.writerFence)
      await expect(store.finishRun(claimed.run.runId, claimed.run.attemptId, 'completed'))
        .rejects.toMatchObject({ code: 'ESTALE' })
      await store.finishRun(reclaimed.run.runId, reclaimed.run.attemptId, 'completed')
    }
  })

  test('does not replay a prompt after crossing the dispatch boundary', async () => {
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId, workspaceId, preferredWorkerId: 'worker-a' })
    const rpcId = randomUUID()
    const admitted = await store.enqueueRun({
      tenantId,
      sessionId,
      clientRpcId: rpcId,
      idempotencyKey: `dispatching-expiry-${rpcId}`,
      request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } },
    })
    const claimed = await store.claimNext('worker-a')
    expect(claimed.kind).toBe('claimed')
    if (claimed.kind !== 'claimed') return
    await store.markDispatching(claimed.run.runId, claimed.run.attemptId)
    await pool.query(`UPDATE dsh_cloud_control.run_attempts SET heartbeat_at=now()-interval '1 minute' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.attemptId])

    expect(await store.reconcileExpiredAttempts(20)).toEqual({ requeued: 0, failed: 1 })
    expect(await store.runResponse(admitted.runId)).toMatchObject({ status: 'failed' })
    const result = await pool.query<{ error_code: string }>(`SELECT error_code FROM dsh_cloud_control.runs WHERE namespace=$1 AND id=$2`, [namespace, admitted.runId])
    expect(result.rows[0]?.error_code).toBe('prompt_dispatch_unknown')
  })

  test('settles a dirty Workspace when a post-prompt Worker lease expires', async () => {
    const sessionId = randomUUID()
    const expiredWorkspace = (await store.createWorkspace(tenantId, `Expired-${sessionId}`)).id
    await store.registerSession({ sessionId, tenantId, workspaceId: expiredWorkspace, preferredWorkerId: 'worker-a' })
    const rpcId = randomUUID()
    const admitted = await store.enqueueRun({
      tenantId,
      sessionId,
      clientRpcId: rpcId,
      idempotencyKey: `dirty-expiry-${rpcId}`,
      request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } },
    })
    const claimed = await store.claimNext('worker-a')
    expect(claimed.kind).toBe('claimed')
    if (claimed.kind !== 'claimed') return
    await pool.query(`INSERT INTO dsh_cloud.persistence_state(namespace,store_id) VALUES($1,$2) ON CONFLICT(namespace) DO NOTHING`, [namespace, randomUUID()])
    await pool.query(`
      INSERT INTO dsh_cloud.sessions(namespace,id,header,incarnation,revision,next_seq,writer_fence,writer_attempt_id)
      VALUES($1,$2,$3,$4,1,1,$5,$6)
    `, [namespace, sessionId, JSON.stringify({ version: 3, id: sessionId, createdAt: Date.now(), cwd: '/workspace' }), randomUUID(), claimed.run.writerFence, claimed.run.attemptId])
    await pool.query(`
      INSERT INTO dsh_cloud.session_event_markers(namespace,session_id,seq,type,rpc_id)
      VALUES($1,$2,0,'user/message',$3)
    `, [namespace, sessionId, rpcId])
    await pool.query(`UPDATE dsh_cloud_control.workspaces SET dirty_fence=$3 WHERE namespace=$1 AND id=$2`, [namespace, expiredWorkspace, claimed.run.writerFence])
    await pool.query(`UPDATE dsh_cloud_control.run_attempts SET heartbeat_at=now()-interval '1 minute' WHERE namespace=$1 AND id=$2`, [namespace, claimed.run.attemptId])

    expect(await store.reconcileExpiredAttempts(20)).toEqual({ requeued: 0, failed: 1 })
    expect((await store.runResponse(admitted.runId))?.status).toBe('failed')
    const workspace = await pool.query<{ revision: string; dirty_fence: string }>(`
      SELECT revision::text,dirty_fence::text FROM dsh_cloud_control.workspaces
       WHERE namespace=$1 AND id=$2
    `, [namespace, expiredWorkspace])
    expect(workspace.rows[0]).toEqual({ revision: '1', dirty_fence: '0' })
    await pool.query('DELETE FROM dsh_cloud.sessions WHERE namespace=$1 AND id=$2', [namespace, sessionId])
  })

  test('idle metadata commands receive a fenced, short-lived writer grant', async () => {
    const sessionId = randomUUID()
    await store.registerSession({ sessionId, tenantId, workspaceId })
    const rpcId = randomUUID()
    await store.issueSessionCommand(tenantId, sessionId, rpcId)
    const authority = await store.authorityForRpcId(rpcId)
    expect(authority).toMatchObject({ tenantId, workspaceId, sessionId })
    expect(authority!.writerFence).toBeGreaterThan(0)
    await store.issueSessionCommand(tenantId, sessionId, rpcId)
    expect((await store.authorityForRpcId(rpcId))?.writerFence).toBe(authority!.writerFence)
  })

  test('steering joins the active Run authority without invalidating its fence', async () => {
    const sessionId = randomUUID()
    const steeringWorkspace = (await store.createWorkspace(tenantId, `Steering-${sessionId}`)).id
    await store.registerSession({ sessionId, tenantId, workspaceId: steeringWorkspace })
    const promptRpcId = randomUUID()
    await store.enqueueRun({
      tenantId,
      sessionId,
      clientRpcId: promptRpcId,
      idempotencyKey: `steering-${promptRpcId}`,
      request: { type: 'client-request', rpcId: promptRpcId, method: 'session.prompt', payload: { sessionId } },
    })
    const claimed = await store.claimNext('worker-a')
    expect(claimed.kind).toBe('claimed')
    if (claimed.kind !== 'claimed') return
    const steeringRpcId = randomUUID()
    await store.issueSessionCommand(tenantId, sessionId, steeringRpcId, true)
    expect(await store.authorityForRpcId(steeringRpcId)).toMatchObject({
      runId: claimed.run.runId,
      attemptId: claimed.run.attemptId,
      writerFence: claimed.run.writerFence,
    })
    await expect(store.issueSessionCommand(tenantId, sessionId, randomUUID())).rejects.toMatchObject({ code: 'EBUSY' })
    await store.finishRun(claimed.run.runId, claimed.run.attemptId, 'completed')
  })

  test('advances Workspace revision only when the execution world may have mutated it', async () => {
    const sessionId = randomUUID()
    const revisionWorkspace = (await store.createWorkspace(tenantId, `Revision-${sessionId}`)).id
    await store.registerSession({ sessionId, tenantId, workspaceId: revisionWorkspace })
    const submit = async (suffix: string) => {
      const rpcId = randomUUID()
      await store.enqueueRun({
        tenantId,
        sessionId,
        clientRpcId: rpcId,
        idempotencyKey: `revision-${suffix}-${rpcId}`,
        request: { type: 'client-request', rpcId, method: 'session.prompt', payload: { sessionId } },
      })
      const claim = await store.claimNext('worker-a')
      expect(claim.kind).toBe('claimed')
      if (claim.kind !== 'claimed') throw new Error('expected a claimed Run')
      return claim.run
    }
    const chat = await submit('chat')
    await store.finishRun(chat.runId, chat.attemptId, 'completed')
    expect((await store.listWorkspaces(tenantId)).find(item => item.id === revisionWorkspace)?.revision).toBe(0)

    const coding = await submit('coding')
    await pool.query(`UPDATE dsh_cloud_control.workspaces SET dirty_fence=$3 WHERE namespace=$1 AND id=$2`, [namespace, revisionWorkspace, coding.writerFence])
    await store.finishRun(coding.runId, coding.attemptId, 'failed', 'model_failed_after_tool')
    expect((await store.listWorkspaces(tenantId)).find(item => item.id === revisionWorkspace)?.revision).toBe(1)
  })
})
