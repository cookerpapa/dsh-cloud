import { createCipheriv, createDecipheriv, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { Pool } from 'pg'
import {
  MAX_RPC_REQUEST_BYTES,
  operationFailure,
  parseExecutionRequest,
  type ExecutionRequest,
  type ExecutionResponse,
} from '@dsh-cloud/execution-protocol'
import type { SandboxBinding, SandboxHandle, SandboxProvider } from './provider.js'

interface ActivationRow {
  tenant_id: string
  workspace_id: string
  activation_id: string
  attempt_id: string
  writer_fence: string
  handle_json: SandboxHandle
  binding_ciphertext: string
  status: 'creating' | 'ready' | 'failed' | 'destroying'
}

const WORKSPACE_MUTATIONS = new Set([
  'fs.mkdir', 'fs.write', 'fs.rename', 'fs.remove',
  'process.start', 'process.stdin', 'terminal.start', 'terminal.input',
])
const CANCELLATION_CLEANUP = new Set([
  'process.poll', 'process.terminate', 'terminal.poll', 'terminal.terminate',
])

export interface SandboxManagerOptions {
  readonly pool: Pool
  readonly namespace: string
  readonly internalToken: string
  readonly encryptionKey: Buffer
  readonly provider: SandboxProvider
  /** Test seam; production uses the PostgreSQL RunAttempt lease verifier. */
  readonly authorityVerifier?: (request: ExecutionRequest) => Promise<void>
  readonly attemptLeaseSeconds?: number
}

function equal(left: string, right: string): boolean {
  const a = Buffer.from(left)
  const b = Buffer.from(right)
  return a.byteLength === b.byteLength && timingSafeEqual(a, b)
}

async function body(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    const value = Buffer.from(chunk)
    size += value.byteLength
    if (size > MAX_RPC_REQUEST_BYTES) throw new Error('request exceeded its byte limit')
    chunks.push(value)
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown
}

function send(response: ServerResponse, status: number, value: unknown): void {
  const output = Buffer.from(JSON.stringify(value))
  response.writeHead(status, { 'content-type': 'application/json', 'content-length': String(output.byteLength), 'cache-control': 'no-store' })
  response.end(output)
}

export class SandboxManager {
  private readonly key: Buffer

  constructor(private readonly options: SandboxManagerOptions) {
    if (options.internalToken.length < 32) throw new Error('Sandbox Manager internal token must have at least 32 characters')
    if (options.encryptionKey.byteLength !== 32) throw new Error('Sandbox Manager encryption key must contain exactly 32 bytes')
    this.key = Buffer.from(options.encryptionKey)
  }

  async initialize(): Promise<void> {
    const client = await this.options.pool.connect()
    try {
      await client.query(`SELECT pg_advisory_lock(hashtextextended($1,0))`, ['dsh_cloud_sandbox.activations:migration'])
      await client.query('CREATE SCHEMA IF NOT EXISTS dsh_cloud_sandbox')
      await client.query(`CREATE TABLE IF NOT EXISTS dsh_cloud_sandbox.schema_state (
        singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
        version integer NOT NULL CHECK (version >= 1)
      )`)
      const state = await client.query<{ version: number }>('SELECT version FROM dsh_cloud_sandbox.schema_state WHERE singleton=true')
      const version = state.rows[0]?.version ?? 0
      if (version > 1) throw new Error(`Sandbox Manager schema version ${version} is newer than this binary supports`)
      if (version < 1) {
        await client.query(`
      CREATE TABLE IF NOT EXISTS dsh_cloud_sandbox.activations (
        namespace text NOT NULL,
        tenant_id text NOT NULL,
        workspace_id text NOT NULL,
        activation_id text NOT NULL,
        attempt_id text NOT NULL,
        writer_fence bigint NOT NULL CHECK (writer_fence >= 0),
        handle_json jsonb NOT NULL,
        binding_ciphertext text NOT NULL,
        status text NOT NULL CHECK (status IN ('creating', 'ready', 'failed', 'destroying')),
        last_activity_at timestamptz NOT NULL DEFAULT now(),
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, workspace_id),
        UNIQUE (namespace, activation_id)
      );
      INSERT INTO dsh_cloud_sandbox.schema_state(singleton,version) VALUES(true,1)
      ON CONFLICT(singleton) DO UPDATE SET version=EXCLUDED.version
        `)
      }
    } finally {
      await client.query(`SELECT pg_advisory_unlock(hashtextextended($1,0))`, ['dsh_cloud_sandbox.activations:migration']).catch(() => undefined)
      client.release()
    }
  }

  createServer(): Server {
    return createServer(async (request, response) => {
      if (request.method === 'GET' && request.url === '/health/ready') {
        try { await this.options.pool.query('SELECT 1'); send(response, 200, { status: 'ready' }) } catch { send(response, 503, { status: 'not_ready' }) }
        return
      }
      if (request.method === 'GET' && request.url === '/metrics') {
        const metrics = await this.options.pool.query<{ status: string; count: string }>(`SELECT status,count(*)::text AS count FROM dsh_cloud_sandbox.activations WHERE namespace=$1 GROUP BY status`, [this.options.namespace])
        const lines = ['# TYPE dsh_cloud_sandbox_activations gauge',...metrics.rows.map(row => `dsh_cloud_sandbox_activations{state="${row.status}"} ${row.count}`),'']
        const output=Buffer.from(lines.join('\n'));response.writeHead(200,{'content-type':'text/plain; version=0.0.4','content-length':String(output.byteLength)});response.end(output);return
      }
      const authorization = request.headers.authorization
      if (authorization === undefined || !authorization.startsWith('Bearer ') || !equal(authorization.slice(7), this.options.internalToken)) {
        send(response, 401, { error: 'unauthorized' })
        return
      }
      if (request.method === 'POST' && request.url === '/v1/workspaces/destroy') {
        try {
          const input = await body(request) as Record<string, unknown>
          if (typeof input['tenantId'] !== 'string' || typeof input['workspaceId'] !== 'string') {
            throw Object.assign(new Error('Workspace deletion identity is invalid'), { code: 'EINVAL' })
          }
          await this.destroyWorkspace(input['tenantId'], input['workspaceId'])
          send(response, 200, { deleted: true })
        } catch (error: unknown) {
          send(response, 409, { error: error instanceof Error ? error.message : 'Workspace deletion failed' })
        }
        return
      }
      if (request.method !== 'POST' || request.url !== '/v1/execute') { send(response, 404, { error: 'not found' }); return }
      let operationId = 'invalid'
      const controller = new AbortController()
      const abort = (): void => controller.abort()
      request.once('aborted', abort)
      response.once('close', () => { if (!response.writableEnded) abort() })
      try {
        const parsed = parseExecutionRequest(await body(request))
        operationId = parsed.operationId
        const result = await this.execute(parsed, controller.signal)
        if (!response.destroyed) send(response, 200, result)
      } catch (error: unknown) {
        if (!response.destroyed) send(response, 200, operationFailure(operationId, error))
      }
    })
  }

  async execute(request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResponse> {
    await this.verifyAuthority(request)
    const resolved = await this.ensureActivation(request, signal)
    await this.verifyAuthority(request)
    if (this.mayMutateWorkspace(request)) {
      await this.options.pool.query(`
        UPDATE dsh_cloud_control.workspaces
           SET dirty_fence=GREATEST(dirty_fence,$4),updated_at=now()
         WHERE namespace=$1 AND tenant_id::text=$2 AND id::text=$3
      `, [this.options.namespace, request.authority.tenantId, request.authority.workspaceId, request.authority.writerFence])
    }
    try {
      return await this.options.provider.execute(resolved.handle, resolved.binding, request, signal)
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code === 'SANDBOX_UNAVAILABLE') {
        await this.invalidateActivation(request, resolved).catch(() => undefined)
      }
      throw error
    }
  }

  /** Destroy an empty, control-plane-reserved Workspace and its physical Cube state. */
  async destroyWorkspace(tenantId: string, workspaceId: string): Promise<void> {
    if (tenantId.trim().length === 0 || workspaceId.trim().length === 0) {
      throw Object.assign(new Error('Workspace deletion identity is invalid'), { code: 'EINVAL' })
    }
    if (this.options.provider.destroyWorkspace === undefined) {
      throw new Error('Sandbox provider does not support Workspace deletion')
    }

    const client = await this.options.pool.connect()
    let activation: { activationId: string; handle: SandboxHandle } | undefined
    try {
      await client.query('BEGIN')
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [`${this.options.namespace}:${workspaceId}`])
      const workspace = await client.query(`
        SELECT 1 FROM dsh_cloud_control.workspaces workspace
         WHERE workspace.namespace=$1 AND workspace.tenant_id::text=$2
           AND workspace.id::text=$3 AND workspace.lifecycle='deleting'
           AND NOT EXISTS(
             SELECT 1 FROM dsh_cloud_control.sessions session
              WHERE session.namespace=workspace.namespace AND session.workspace_id=workspace.id
           )
         FOR UPDATE
      `, [this.options.namespace, tenantId, workspaceId])
      if (workspace.rowCount !== 1) throw Object.assign(new Error('Workspace is not reserved for deletion'), { code: 'ESTALE' })
      const found = await client.query<{ activation_id: string; handle_json: SandboxHandle }>(`
        SELECT activation_id,handle_json FROM dsh_cloud_sandbox.activations
         WHERE namespace=$1 AND workspace_id=$2 FOR UPDATE
      `, [this.options.namespace, workspaceId])
      const row = found.rows[0]
      if (row !== undefined) {
        await client.query(`
          UPDATE dsh_cloud_sandbox.activations SET status='destroying'
           WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3
        `, [this.options.namespace, workspaceId, row.activation_id])
        activation = { activationId: row.activation_id, handle: row.handle_json }
      }
      await client.query('COMMIT')
    } catch (error: unknown) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }

    try {
      if (activation?.handle.sandboxId !== undefined) {
        await this.options.provider.destroy(activation.handle)
      }
      await this.options.provider.destroyWorkspace({ tenantId, workspaceId })
      const removed = await this.options.pool.query(`
        WITH activation AS (
          DELETE FROM dsh_cloud_sandbox.activations
           WHERE namespace=$1 AND workspace_id=$3
        )
        DELETE FROM dsh_cloud_control.workspaces workspace
         WHERE workspace.namespace=$1 AND workspace.tenant_id::text=$2
           AND workspace.id::text=$3 AND workspace.lifecycle='deleting'
           AND NOT EXISTS(
             SELECT 1 FROM dsh_cloud_control.sessions session
              WHERE session.namespace=workspace.namespace AND session.workspace_id=workspace.id
           )
      `, [this.options.namespace, tenantId, workspaceId])
      if (removed.rowCount !== 1) throw Object.assign(new Error('Workspace deletion lost its control-plane reservation'), { code: 'ESTALE' })
    } catch (error: unknown) {
      if (activation !== undefined) {
        await this.options.pool.query(`
          UPDATE dsh_cloud_sandbox.activations SET status='ready',last_activity_at=now()
           WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='destroying'
        `, [this.options.namespace, workspaceId, activation.activationId]).catch(() => undefined)
      }
      throw error
    }
  }

  private async verifyAuthority(request: ExecutionRequest): Promise<void> {
    if (this.options.authorityVerifier !== undefined) {
      await this.options.authorityVerifier(request)
      return
    }
    const authority = request.authority
    const leaseSeconds = this.options.attemptLeaseSeconds ?? 20
    const result = await this.options.pool.query<{ status: string }>(`
      SELECT run.status
        FROM dsh_cloud_control.runs run
        JOIN dsh_cloud_control.run_attempts attempt
          ON attempt.namespace=run.namespace AND attempt.id=run.current_attempt_id AND attempt.run_id=run.id
       WHERE run.namespace=$1 AND run.id::text=$2 AND run.tenant_id::text=$3
         AND run.workspace_id::text=$4 AND run.session_id=$5
         AND attempt.id::text=$6 AND run.writer_fence=$7 AND attempt.writer_fence=$7
         AND run.status IN ('claimed','dispatched','running','cancel_requested')
         AND attempt.status IN ('claimed','running')
         AND attempt.heartbeat_at>now()-make_interval(secs=>$8)
    `, [
      this.options.namespace,
      authority.runId,
      authority.tenantId,
      authority.workspaceId,
      authority.sessionId,
      authority.attemptId,
      authority.writerFence,
      leaseSeconds,
    ])
    const state = result.rows[0]?.status
    if (state === undefined) throw Object.assign(new Error('execution authority is stale or expired'), { code: 'ESTALE' })
    if (state === 'cancel_requested' && !CANCELLATION_CLEANUP.has(request.operation.kind)) {
      throw Object.assign(new Error('Run cancellation has revoked new Tool effects'), { code: 'ECANCELED' })
    }
  }

  private mayMutateWorkspace(request: ExecutionRequest): boolean {
    return WORKSPACE_MUTATIONS.has(request.operation.kind)
  }

  /** Reconcile physical Cube inventory and reap warm activations after their idle TTL. */
  async reconcile(idleMilliseconds: number): Promise<{ destroyed: number; missing: number; orphaned: number }> {
    if (!Number.isSafeInteger(idleMilliseconds) || idleMilliseconds < 1_000) throw new TypeError('idle TTL is invalid')
    const candidates = await this.options.pool.query<{ workspace_id: string; activation_id: string; handle_json: SandboxHandle }>(`
      SELECT workspace_id,activation_id,handle_json FROM dsh_cloud_sandbox.activations
      WHERE namespace=$1 AND status='ready' AND last_activity_at<now()-make_interval(secs=>$2::double precision/1000)
      ORDER BY last_activity_at LIMIT 100
    `, [this.options.namespace, idleMilliseconds])
    let destroyed = 0; let missing = 0; let orphaned = 0
    for (const candidate of candidates.rows) {
      const reserved = await this.options.pool.query(`UPDATE dsh_cloud_sandbox.activations SET status='destroying' WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='ready'`, [this.options.namespace, candidate.workspace_id, candidate.activation_id])
      if (reserved.rowCount !== 1) continue
      try {
        const state = await this.options.provider.inspect(candidate.handle_json)
        if (state !== 'absent') { await this.options.provider.destroy(candidate.handle_json); destroyed++ } else missing++
        await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='destroying'`, [this.options.namespace, candidate.workspace_id, candidate.activation_id])
      } catch (error: unknown) {
        await this.options.pool.query(`UPDATE dsh_cloud_sandbox.activations SET status='ready',last_activity_at=now() WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='destroying'`, [this.options.namespace, candidate.workspace_id, candidate.activation_id])
        throw error
      }
    }
    const staleCreating = await this.options.pool.query<{ activation_id: string }>(`
      SELECT activation_id FROM dsh_cloud_sandbox.activations
       WHERE namespace=$1 AND status='creating' AND created_at<now()-interval '5 minutes'
    `, [this.options.namespace])
    if (this.options.provider.listManaged !== undefined) {
      const inventory = await this.options.provider.listManaged()
      const records = await this.options.pool.query<{ activation_id: string; sandbox_id: string | null; status: string }>(`
        SELECT activation_id,NULLIF(handle_json->>'sandboxId','') AS sandbox_id,status
          FROM dsh_cloud_sandbox.activations WHERE namespace=$1
      `, [this.options.namespace])
      const known = new Map(records.rows.map(row => [row.activation_id, row]))
      const stale = new Set(staleCreating.rows.map(row => row.activation_id))
      for (const instance of inventory) {
        const record = known.get(instance.activationId)
        const isOrphan = record === undefined || stale.has(instance.activationId) ||
          (record.sandbox_id !== null && record.sandbox_id !== instance.handle.sandboxId)
        if (!isOrphan) continue
        await this.options.provider.destroy(instance.handle)
        orphaned++
      }
    }
    await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1 AND status='creating' AND created_at<now()-interval '5 minutes'`, [this.options.namespace])
    return { destroyed, missing, orphaned }
  }

  private async ensureActivation(request: ExecutionRequest, signal?: AbortSignal): Promise<{ handle: SandboxHandle; binding: SandboxBinding }> {
    const authority = request.authority
    const client = await this.options.pool.connect()
    let create: { activationId: string; binding: SandboxBinding } | undefined
    let replace: { handle: SandboxHandle; activationId: string } | undefined
    let rebind: { handle: SandboxHandle; previous: SandboxBinding; binding: SandboxBinding; activationId: string } | undefined
    let pending: { activationId: string } | undefined
    try {
      await client.query('BEGIN')
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [`${this.options.namespace}:${authority.workspaceId}`])
      const result = await client.query<ActivationRow>(`
        SELECT tenant_id, workspace_id, activation_id, attempt_id, writer_fence, handle_json,
               binding_ciphertext, status
          FROM dsh_cloud_sandbox.activations
         WHERE namespace = $1 AND workspace_id = $2
         FOR UPDATE
      `, [this.options.namespace, authority.workspaceId])
      const row = result.rows[0]
      if (row === undefined) {
        const activationId = randomUUID()
        const binding = { activationId, secret: randomBytes(32).toString('base64url'), writerFence: authority.writerFence }
        await client.query(`
          INSERT INTO dsh_cloud_sandbox.activations
            (namespace, tenant_id, workspace_id, activation_id, attempt_id, writer_fence, handle_json, binding_ciphertext, status)
          VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, $7, 'creating')
        `, [this.options.namespace, authority.tenantId, authority.workspaceId, activationId, authority.attemptId, authority.writerFence, this.encrypt(binding)])
        create = { activationId, binding }
        await client.query('COMMIT')
      } else {
        if (row.tenant_id !== authority.tenantId) throw Object.assign(new Error('workspace belongs to another tenant'), { code: 'EACCES' })
        const currentFence = Number(row.writer_fence)
        if (authority.writerFence < currentFence || (authority.writerFence === currentFence && row.attempt_id !== authority.attemptId)) {
          throw Object.assign(new Error('execution request carries stale writer authority'), { code: 'ESTALE' })
        }
        if (row.status === 'creating' && authority.writerFence === currentFence && row.attempt_id === authority.attemptId) {
          pending = { activationId: row.activation_id }
          await client.query('COMMIT')
        } else if (row.status !== 'ready') {
          throw Object.assign(new Error('sandbox activation is not ready'), { code: 'EAGAIN' })
        }
        if (pending === undefined) {
          const handle = row.handle_json
          const previous = this.decrypt(row.binding_ciphertext)
          let binding = previous
          if (authority.writerFence > currentFence) {
          const previousAttempt = await client.query<{ status: string }>(`
            SELECT status FROM dsh_cloud_control.run_attempts WHERE namespace=$1 AND id::text=$2
          `, [this.options.namespace, row.attempt_id])
          if (previousAttempt.rows[0]?.status !== 'completed') {
            await client.query(`UPDATE dsh_cloud_sandbox.activations SET status='destroying' WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3`, [this.options.namespace, authority.workspaceId, row.activation_id])
            await client.query('COMMIT')
            replace = { handle, activationId: row.activation_id }
          } else {
            binding = { activationId: row.activation_id, secret: randomBytes(32).toString('base64url'), writerFence: authority.writerFence }
            await client.query(`
            UPDATE dsh_cloud_sandbox.activations
               SET status = 'creating'
             WHERE namespace = $1 AND workspace_id = $2
          `, [this.options.namespace, authority.workspaceId])
            rebind = { handle, previous, binding, activationId: row.activation_id }
          }
          } else {
            await client.query(`UPDATE dsh_cloud_sandbox.activations SET last_activity_at = now() WHERE namespace = $1 AND workspace_id = $2`, [this.options.namespace, authority.workspaceId])
          }
          if (replace === undefined && rebind === undefined) {
            await client.query('COMMIT')
            return { handle, binding }
          }
          if (rebind !== undefined) await client.query('COMMIT')
        }
      }
    } catch (error: unknown) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally { client.release() }

    if (replace !== undefined) {
      await this.options.provider.destroy(replace.handle).catch(() => undefined)
      await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='destroying'`, [this.options.namespace, authority.workspaceId, replace.activationId])
      return this.ensureActivation(request, signal)
    }

    if (rebind !== undefined) {
      try {
        if (await this.options.provider.inspect(rebind.handle) !== 'ready') {
          await this.options.provider.destroy(rebind.handle).catch(() => undefined)
          await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='creating'`, [this.options.namespace, authority.workspaceId, rebind.activationId])
          return this.ensureActivation(request, signal)
        }
        await this.options.provider.bind(rebind.handle, rebind.binding, rebind.previous)
        const finalized = await this.options.pool.query(`
          UPDATE dsh_cloud_sandbox.activations
             SET attempt_id=$4,writer_fence=$5,binding_ciphertext=$6,status='ready',last_activity_at=now()
           WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='creating'
        `, [this.options.namespace, authority.workspaceId, rebind.activationId, authority.attemptId, authority.writerFence, this.encrypt(rebind.binding)])
        if (finalized.rowCount !== 1) throw new Error('sandbox rebind lost ownership')
        return { handle: rebind.handle, binding: rebind.binding }
      } catch (error: unknown) {
        await this.options.pool.query(`UPDATE dsh_cloud_sandbox.activations SET status='destroying' WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='creating'`, [this.options.namespace, authority.workspaceId, rebind.activationId]).catch(() => undefined)
        await this.options.provider.destroy(rebind.handle).catch(() => undefined)
        await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3`, [this.options.namespace, authority.workspaceId, rebind.activationId]).catch(() => undefined)
        throw error
      }
    }

    if (pending !== undefined) return this.waitForActivation(request, pending.activationId, signal)

    if (create === undefined) throw new Error('sandbox activation reservation was lost')
    let handle: SandboxHandle | undefined
    try {
      handle = await this.options.provider.create({ activationId: create.activationId, tenantId: authority.tenantId, workspaceId: authority.workspaceId, writerFence: authority.writerFence })
      await this.options.provider.bind(handle, create.binding)
      const finalized = await this.options.pool.query(`
        UPDATE dsh_cloud_sandbox.activations
           SET handle_json = $4::jsonb, status = 'ready', last_activity_at = now()
         WHERE namespace = $1 AND workspace_id = $2 AND activation_id = $3 AND status = 'creating'
      `, [this.options.namespace, authority.workspaceId, create.activationId, JSON.stringify(handle)])
      if (finalized.rowCount !== 1) throw new Error('sandbox activation lost ownership during creation')
      return { handle, binding: create.binding }
    } catch (error: unknown) {
      if (handle !== undefined) await this.options.provider.destroy(handle).catch(() => undefined)
      await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace = $1 AND workspace_id = $2 AND activation_id = $3 AND status = 'creating'`, [this.options.namespace, authority.workspaceId, create.activationId])
      throw error
    }
  }

  private async waitForActivation(
    request: ExecutionRequest,
    activationId: string,
    signal?: AbortSignal,
  ): Promise<{ handle: SandboxHandle; binding: SandboxBinding }> {
    const deadline = Date.now() + 180_000
    while (Date.now() < deadline) {
      signal?.throwIfAborted()
      const result = await this.options.pool.query<ActivationRow>(`
        SELECT tenant_id,workspace_id,activation_id,attempt_id,writer_fence,handle_json,
               binding_ciphertext,status
          FROM dsh_cloud_sandbox.activations
         WHERE namespace=$1 AND workspace_id=$2
      `, [this.options.namespace, request.authority.workspaceId])
      const row = result.rows[0]
      if (row === undefined) return this.ensureActivation(request, signal)
      if (row.activation_id !== activationId || row.attempt_id !== request.authority.attemptId ||
          Number(row.writer_fence) !== request.authority.writerFence) {
        throw Object.assign(new Error('sandbox activation ownership changed while waiting'), { code: 'ESTALE' })
      }
      if (row.status === 'ready') {
        return { handle: row.handle_json, binding: this.decrypt(row.binding_ciphertext) }
      }
      if (row.status === 'failed' || row.status === 'destroying') {
        throw Object.assign(new Error(`sandbox activation entered ${row.status}`), { code: 'SANDBOX_UNAVAILABLE' })
      }
      await delay(50, undefined, { signal })
    }
    throw Object.assign(new Error('sandbox activation did not become ready before its deadline'), { code: 'ETIMEDOUT' })
  }

  private async invalidateActivation(
    request: ExecutionRequest,
    resolved: { handle: SandboxHandle; binding: SandboxBinding },
  ): Promise<void> {
    const reserved = await this.options.pool.query(`
      UPDATE dsh_cloud_sandbox.activations
         SET status='destroying'
       WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3
         AND writer_fence=$4 AND status='ready'
    `, [this.options.namespace, request.authority.workspaceId, resolved.binding.activationId, resolved.binding.writerFence])
    if (reserved.rowCount !== 1) return
    await this.options.provider.destroy(resolved.handle).catch(() => undefined)
    await this.options.pool.query(`DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1 AND workspace_id=$2 AND activation_id=$3 AND status='destroying'`, [this.options.namespace, request.authority.workspaceId, resolved.binding.activationId])
  }

  private encrypt(value: SandboxBinding): string {
    const nonce = randomBytes(12)
    const cipher = createCipheriv('aes-256-gcm', this.key, nonce)
    const body = Buffer.concat([cipher.update(JSON.stringify(value)), cipher.final()])
    return Buffer.concat([nonce, cipher.getAuthTag(), body]).toString('base64url')
  }

  private decrypt(value: string): SandboxBinding {
    const bytes = Buffer.from(value, 'base64url')
    if (bytes.byteLength < 29) throw new Error('sandbox binding ciphertext is invalid')
    const decipher = createDecipheriv('aes-256-gcm', this.key, bytes.subarray(0, 12))
    decipher.setAuthTag(bytes.subarray(12, 28))
    return JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()]).toString('utf8')) as SandboxBinding
  }
}

export type { SandboxProvider, SandboxHandle, SandboxBinding, ManagedSandbox } from './provider.js'
