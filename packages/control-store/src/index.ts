import { createHash, randomBytes, randomUUID, scrypt as scryptCallback, timingSafeEqual } from 'node:crypto'
import { promisify } from 'node:util'
import { Pool } from 'pg'

const scrypt = promisify(scryptCallback)
const SQL_SCHEMA = 'dsh_cloud_control'

export interface Principal {
  readonly userId: string
  readonly tenantId: string
  readonly email: string
  readonly role: 'admin' | 'member'
}

export interface WorkerRecord {
  readonly id: string
  readonly baseUrl: string
  readonly maximumRuns: number
  readonly activeRuns: number
}

export interface EnqueuedRun {
  readonly runId: string
  readonly created: boolean
  readonly status: string
}

export interface WorkspaceRecord {
  readonly id: string
  readonly name: string
  readonly revision: number
  readonly sessionIds: readonly string[]
  readonly createdAt: string
  readonly updatedAt: string
}

export interface OperationalMetrics {
  readonly queuedRuns: number
  readonly activeRuns: number
  readonly failedRuns: number
  readonly healthyWorkers: number
  readonly registeredTenants: number
}

export interface RunTurnOutcome {
  readonly kind: string
  readonly errorCode?: string
}

export interface ClaimedRun {
  readonly runId: string
  readonly tenantId: string
  readonly sessionId: string
  readonly workspaceId: string
  readonly attemptId: string
  readonly writerFence: number
  readonly worker: WorkerRecord
  readonly request: unknown
}

export type ClaimResult = { readonly kind: 'claimed'; readonly run: ClaimedRun }
  | { readonly kind: 'idle' }

interface UserRow { id: string; tenant_id: string; email: string; password_hash: string; role: Principal['role'] }
interface WorkerRow { id: string; base_url: string; maximum_runs: number; active_runs: number }
interface RunRow {
  id: string
  tenant_id: string
  session_id: string
  workspace_id: string
  status: string
  request_json: unknown
}

function normalizedEmail(value: string): string {
  const email = value.trim().toLowerCase()
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 320) throw new TypeError('email address is invalid')
  return email
}

function passwordPolicy(value: string): void {
  if (value.length < 10 || value.length > 200) throw new TypeError('password must contain 10-200 characters')
}

async function passwordHash(password: string): Promise<string> {
  passwordPolicy(password)
  const salt = randomBytes(16)
  const derived = await scrypt(password, salt, 32) as Buffer
  return `scrypt-v1.${salt.toString('base64url')}.${derived.toString('base64url')}`
}

async function passwordMatches(stored: string, password: string): Promise<boolean> {
  const [version, saltValue, hashValue] = stored.split('.')
  if (version !== 'scrypt-v1' || saltValue === undefined || hashValue === undefined) return false
  const expected = Buffer.from(hashValue, 'base64url')
  const actual = await scrypt(password, Buffer.from(saltValue, 'base64url'), expected.byteLength) as Buffer
  return actual.byteLength === expected.byteLength && timingSafeEqual(actual, expected)
}

function tokenDigest(token: string): Buffer {
  return createHash('sha256').update(token).digest()
}

function worker(row: WorkerRow): WorkerRecord {
  return { id: row.id, baseUrl: row.base_url, maximumRuns: row.maximum_runs, activeRuns: row.active_runs }
}

/** PostgreSQL authority for authentication, tenancy, Run admission, and Worker capacity. */
export class ControlStore {
  constructor(readonly pool: Pool, readonly namespace = 'default') {
    if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(namespace)) throw new TypeError('control namespace is invalid')
  }

  async initialize(): Promise<void> {
    const client = await this.pool.connect()
    try {
      await client.query(`SELECT pg_advisory_lock(hashtextextended($1,0))`, [`${SQL_SCHEMA}:migration`])
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${SQL_SCHEMA}; CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.schema_state(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),version integer NOT NULL)`)
      const state = await client.query<{ version: number }>(`SELECT version FROM ${SQL_SCHEMA}.schema_state WHERE singleton=true`)
      if (state.rows[0]?.version === 6) return
      if (state.rows[0] !== undefined) throw new Error(`control schema version ${state.rows[0].version} is unsupported; reset this pre-production database`)
      await client.query(`
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.tenants (
        namespace text NOT NULL, id uuid NOT NULL, name text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
        last_scheduled_at timestamptz NOT NULL DEFAULT '-infinity', maximum_concurrent_runs integer NOT NULL DEFAULT 4 CHECK(maximum_concurrent_runs>0),
        PRIMARY KEY (namespace, id)
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.users (
        namespace text NOT NULL, id uuid NOT NULL, tenant_id uuid NOT NULL, email text NOT NULL,
        password_hash text NOT NULL, role text NOT NULL CHECK (role IN ('admin', 'member')),
        preferred_worker_id text,
        created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (namespace, id),
        UNIQUE (namespace, email), FOREIGN KEY (namespace, tenant_id) REFERENCES ${SQL_SCHEMA}.tenants(namespace, id) ON DELETE CASCADE
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.auth_sessions (
        namespace text NOT NULL, token_hash bytea NOT NULL, user_id uuid NOT NULL, expires_at timestamptz NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (namespace, token_hash),
        FOREIGN KEY (namespace, user_id) REFERENCES ${SQL_SCHEMA}.users(namespace, id) ON DELETE CASCADE
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.workers (
        namespace text NOT NULL, id text NOT NULL, base_url text NOT NULL, maximum_runs integer NOT NULL CHECK (maximum_runs > 0),
        active_runs integer NOT NULL DEFAULT 0 CHECK (active_runs >= 0), draining boolean NOT NULL DEFAULT false,
        heartbeat_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (namespace, id)
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.workspaces (
        namespace text NOT NULL, id uuid NOT NULL, tenant_id uuid NOT NULL, name text NOT NULL,
        revision bigint NOT NULL DEFAULT 0, next_fence bigint NOT NULL DEFAULT 0,
        dirty_fence bigint NOT NULL DEFAULT 0,
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, id), UNIQUE (namespace, tenant_id, name),
        FOREIGN KEY (namespace, tenant_id) REFERENCES ${SQL_SCHEMA}.tenants(namespace, id) ON DELETE CASCADE
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.sessions (
        namespace text NOT NULL, id text NOT NULL, tenant_id uuid NOT NULL, workspace_id uuid NOT NULL,
        preferred_worker_id text, created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, id),
        FOREIGN KEY (namespace, tenant_id) REFERENCES ${SQL_SCHEMA}.tenants(namespace, id) ON DELETE CASCADE,
        FOREIGN KEY (namespace, workspace_id) REFERENCES ${SQL_SCHEMA}.workspaces(namespace, id) ON DELETE RESTRICT
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.runs (
        namespace text NOT NULL, id uuid NOT NULL, tenant_id uuid NOT NULL, session_id text NOT NULL, workspace_id uuid NOT NULL,
        client_rpc_id text NOT NULL, idempotency_key text NOT NULL, status text NOT NULL
          CHECK (status IN ('queued','claimed','dispatched','running','completed','failed','cancel_requested','cancelled','timed_out')),
        request_json jsonb NOT NULL, response_json jsonb, error_code text, worker_id text, current_attempt_id uuid,
        writer_fence bigint, available_at timestamptz NOT NULL DEFAULT now(),
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, id), UNIQUE (namespace, tenant_id, idempotency_key),
        FOREIGN KEY (namespace, session_id) REFERENCES ${SQL_SCHEMA}.sessions(namespace, id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS dsh_cloud_runs_session_queue ON ${SQL_SCHEMA}.runs(namespace, session_id, created_at, id);
      CREATE INDEX IF NOT EXISTS dsh_cloud_runs_ready_queue ON ${SQL_SCHEMA}.runs(namespace,status,available_at,created_at);
      CREATE UNIQUE INDEX IF NOT EXISTS dsh_cloud_runs_rpc_id ON ${SQL_SCHEMA}.runs(namespace, client_rpc_id);
      CREATE UNIQUE INDEX IF NOT EXISTS dsh_cloud_one_active_run_per_session_v2 ON ${SQL_SCHEMA}.runs(namespace, session_id)
        WHERE status IN ('claimed','dispatched','running','cancel_requested');
      CREATE UNIQUE INDEX IF NOT EXISTS dsh_cloud_one_active_run_per_workspace_v2 ON ${SQL_SCHEMA}.runs(namespace, workspace_id)
        WHERE status IN ('claimed','dispatched','running','cancel_requested');
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.run_attempts (
        namespace text NOT NULL, id uuid NOT NULL, run_id uuid NOT NULL, worker_id text NOT NULL, writer_fence bigint NOT NULL,
        status text NOT NULL CHECK (status IN ('claimed','running','completed','failed','superseded','cancelled')),
        heartbeat_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
        PRIMARY KEY (namespace, id), FOREIGN KEY (namespace, run_id) REFERENCES ${SQL_SCHEMA}.runs(namespace, id) ON DELETE CASCADE
      );
      CREATE TABLE IF NOT EXISTS ${SQL_SCHEMA}.session_commands (
        namespace text NOT NULL, rpc_id text NOT NULL, tenant_id uuid NOT NULL, session_id text NOT NULL,
        workspace_id uuid NOT NULL, command_id uuid NOT NULL, run_id uuid, attempt_id uuid,
        writer_fence bigint NOT NULL, expires_at timestamptz NOT NULL,
        PRIMARY KEY(namespace,rpc_id), UNIQUE(namespace,command_id),
        FOREIGN KEY(namespace,session_id) REFERENCES ${SQL_SCHEMA}.sessions(namespace,id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS dsh_cloud_runs_tenant_status ON ${SQL_SCHEMA}.runs(namespace,tenant_id,status);
      CREATE OR REPLACE FUNCTION ${SQL_SCHEMA}.notify_run_queue() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_notify('dsh_cloud_run_queue', NEW.id::text);
        RETURN NEW;
      END $$;
      DROP TRIGGER IF EXISTS notify_run_queue ON ${SQL_SCHEMA}.runs;
      CREATE TRIGGER notify_run_queue AFTER INSERT ON ${SQL_SCHEMA}.runs FOR EACH ROW
        WHEN (NEW.status='queued') EXECUTE FUNCTION ${SQL_SCHEMA}.notify_run_queue();
      INSERT INTO ${SQL_SCHEMA}.schema_state(singleton,version) VALUES(true,6) ON CONFLICT(singleton) DO UPDATE SET version=excluded.version;
      `)
    } finally {
      await client.query(`SELECT pg_advisory_unlock(hashtextextended($1,0))`, [`${SQL_SCHEMA}:migration`]).catch(() => undefined)
      client.release()
    }
  }

  async register(name: string, emailValue: string, password: string): Promise<Principal> {
    const tenantId = randomUUID()
    const userId = randomUUID()
    const email = normalizedEmail(emailValue)
    const hash = await passwordHash(password)
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      await client.query(`INSERT INTO ${SQL_SCHEMA}.tenants(namespace,id,name) VALUES($1,$2,$3)`, [this.namespace, tenantId, name.trim() || email])
      await client.query(`INSERT INTO ${SQL_SCHEMA}.users(namespace,id,tenant_id,email,password_hash,role) VALUES($1,$2,$3,$4,$5,'admin')`, [this.namespace, userId, tenantId, email, hash])
      await client.query('COMMIT')
      return { userId, tenantId, email, role: 'admin' }
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async login(emailValue: string, password: string): Promise<Principal | undefined> {
    const email = normalizedEmail(emailValue)
    const result = await this.pool.query<UserRow>(`SELECT id,tenant_id,email,password_hash,role FROM ${SQL_SCHEMA}.users WHERE namespace=$1 AND email=$2`, [this.namespace, email])
    const row = result.rows[0]
    if (row === undefined || !(await passwordMatches(row.password_hash, password))) return undefined
    return { userId: row.id, tenantId: row.tenant_id, email: row.email, role: row.role }
  }

  async issueAuthSession(userId: string, ttlSeconds = 86_400): Promise<string> {
    const token = randomBytes(32).toString('base64url')
    await this.pool.query(`INSERT INTO ${SQL_SCHEMA}.auth_sessions(namespace,token_hash,user_id,expires_at) VALUES($1,$2,$3,now()+make_interval(secs=>$4))`, [this.namespace, tokenDigest(token), userId, ttlSeconds])
    return token
  }

  async authenticate(token: string): Promise<Principal | undefined> {
    if (token.length < 32) return undefined
    const result = await this.pool.query<UserRow>(`
      SELECT u.id,u.tenant_id,u.email,u.password_hash,u.role FROM ${SQL_SCHEMA}.auth_sessions s
      JOIN ${SQL_SCHEMA}.users u ON u.namespace=s.namespace AND u.id=s.user_id
      WHERE s.namespace=$1 AND s.token_hash=$2 AND s.expires_at>now()
    `, [this.namespace, tokenDigest(token)])
    const row = result.rows[0]
    return row === undefined ? undefined : { userId: row.id, tenantId: row.tenant_id, email: row.email, role: row.role }
  }

  async revokeAuthSession(token: string): Promise<void> {
    await this.pool.query(`DELETE FROM ${SQL_SCHEMA}.auth_sessions WHERE namespace=$1 AND token_hash=$2`, [this.namespace, tokenDigest(token)])
  }

  async createWorkspace(tenantId: string, name: string): Promise<{ id: string; name: string }> {
    const id = randomUUID()
    const value = name.trim() || 'Workspace'
    await this.pool.query(`INSERT INTO ${SQL_SCHEMA}.workspaces(namespace,id,tenant_id,name) VALUES($1,$2,$3,$4)`, [this.namespace, id, tenantId, value])
    return { id, name: value }
  }

  async ensureDefaultWorkspace(tenantId: string): Promise<{ id: string; name: string }> {
    const result = await this.pool.query<{ id: string; name: string }>(`
      INSERT INTO ${SQL_SCHEMA}.workspaces(namespace,id,tenant_id,name) VALUES($1,$2,$3,'Workspace')
      ON CONFLICT(namespace,tenant_id,name) DO UPDATE SET name=excluded.name RETURNING id,name
    `, [this.namespace, randomUUID(), tenantId])
    const row = result.rows[0]
    if (row === undefined) throw new Error('default workspace was not returned')
    return row
  }

  async listWorkspaces(tenantId: string): Promise<WorkspaceRecord[]> {
    const result = await this.pool.query<{ id: string; name: string; revision: string; created_at: string; updated_at: string; session_ids: string[] }>(`
      SELECT workspace.id,workspace.name,workspace.revision::text,workspace.created_at::text,workspace.updated_at::text,
        COALESCE(array_agg(session.id ORDER BY session.created_at) FILTER (WHERE session.id IS NOT NULL),'{}') AS session_ids
      FROM ${SQL_SCHEMA}.workspaces workspace LEFT JOIN ${SQL_SCHEMA}.sessions session
        ON session.namespace=workspace.namespace AND session.workspace_id=workspace.id
      WHERE workspace.namespace=$1 AND workspace.tenant_id=$2
      GROUP BY workspace.namespace,workspace.id ORDER BY workspace.created_at,workspace.id
    `, [this.namespace, tenantId])
    return result.rows.map(row => ({ id: row.id, name: row.name, revision: Number(row.revision), sessionIds: row.session_ids, createdAt: row.created_at, updatedAt: row.updated_at }))
  }

  async renameWorkspace(tenantId: string, workspaceId: string, name: string): Promise<WorkspaceRecord | undefined> {
    const value = name.trim()
    if (value.length === 0 || value.length > 200) throw new TypeError('workspace name is invalid')
    const updated = await this.pool.query(`UPDATE ${SQL_SCHEMA}.workspaces SET name=$4,updated_at=now() WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, tenantId, workspaceId, value])
    if (updated.rowCount !== 1) return undefined
    return (await this.listWorkspaces(tenantId)).find(item => item.id === workspaceId)
  }

  async deleteWorkspace(tenantId: string, workspaceId: string): Promise<boolean> {
    const result = await this.pool.query(`DELETE FROM ${SQL_SCHEMA}.workspaces workspace WHERE namespace=$1 AND tenant_id=$2 AND id=$3 AND NOT EXISTS(SELECT 1 FROM ${SQL_SCHEMA}.sessions session WHERE session.namespace=workspace.namespace AND session.workspace_id=workspace.id)`, [this.namespace, tenantId, workspaceId])
    return result.rowCount === 1
  }

  async workspaceOwned(tenantId: string, workspaceId: string): Promise<boolean> {
    const result = await this.pool.query(`SELECT 1 FROM ${SQL_SCHEMA}.workspaces WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, tenantId, workspaceId])
    return result.rowCount === 1
  }

  async sessionPlacement(tenantId: string, sessionId: string): Promise<{ workspaceId: string; preferredWorkerId?: string } | undefined> {
    const result = await this.pool.query<{ workspace_id: string; preferred_worker_id: string | null }>(`SELECT workspace_id,preferred_worker_id FROM ${SQL_SCHEMA}.sessions WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, tenantId, sessionId])
    const row = result.rows[0]
    return row === undefined ? undefined : { workspaceId: row.workspace_id, ...(row.preferred_worker_id === null ? {} : { preferredWorkerId: row.preferred_worker_id }) }
  }

  async preferSessionWorker(tenantId: string, sessionId: string, workerId: string): Promise<boolean> {
    const result = await this.pool.query(`UPDATE ${SQL_SCHEMA}.sessions SET preferred_worker_id=$4 WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, tenantId, sessionId, workerId])
    return result.rowCount === 1
  }

  async registerSession(input: { sessionId: string; tenantId: string; workspaceId: string; preferredWorkerId?: string }): Promise<void> {
    await this.pool.query(`
      INSERT INTO ${SQL_SCHEMA}.sessions(namespace,id,tenant_id,workspace_id,preferred_worker_id)
      VALUES($1,$2,$3,$4,$5)
      ON CONFLICT(namespace,id) DO UPDATE SET preferred_worker_id=COALESCE(${SQL_SCHEMA}.sessions.preferred_worker_id,excluded.preferred_worker_id)
      WHERE ${SQL_SCHEMA}.sessions.tenant_id=excluded.tenant_id
    `, [this.namespace, input.sessionId, input.tenantId, input.workspaceId, input.preferredWorkerId ?? null])
  }

  async ownsSession(tenantId: string, sessionId: string): Promise<boolean> {
    const result = await this.pool.query(`SELECT 1 FROM ${SQL_SCHEMA}.sessions WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, tenantId, sessionId])
    return result.rowCount === 1
  }

  async listSessionIds(tenantId: string): Promise<Set<string>> {
    const result = await this.pool.query<{ id: string }>(`SELECT id FROM ${SQL_SCHEMA}.sessions WHERE namespace=$1 AND tenant_id=$2`, [this.namespace, tenantId])
    return new Set(result.rows.map(row => row.id))
  }

  async sessionDurableThrough(sessionId: string): Promise<number> {
    const result = await this.pool.query<{ next_seq: string }>(`SELECT next_seq::text FROM dsh_cloud.sessions WHERE namespace=$1 AND id=$2`, [this.namespace, sessionId])
    const next = Number(result.rows[0]?.next_seq ?? '0')
    return Number.isSafeInteger(next) && next > 0 ? next - 1 : -1
  }

  async heartbeatWorker(input: { id: string; baseUrl: string; maximumRuns: number }): Promise<void> {
    new URL(input.baseUrl)
    await this.pool.query(`
      INSERT INTO ${SQL_SCHEMA}.workers(namespace,id,base_url,maximum_runs,heartbeat_at) VALUES($1,$2,$3,$4,now())
      ON CONFLICT(namespace,id) DO UPDATE SET base_url=excluded.base_url,maximum_runs=excluded.maximum_runs,draining=false,heartbeat_at=now()
    `, [this.namespace, input.id, input.baseUrl.replace(/\/$/, ''), input.maximumRuns])
  }

  async setWorkerDraining(workerId: string, draining: boolean): Promise<void> {
    await this.pool.query(`UPDATE ${SQL_SCHEMA}.workers SET draining=$3,heartbeat_at=now() WHERE namespace=$1 AND id=$2`, [this.namespace, workerId, draining])
  }

  async workerLoad(workerId: string): Promise<{ activeRuns: number; maximumRuns: number; draining: boolean }> {
    const result = await this.pool.query<{ active_runs: number; maximum_runs: number; draining: boolean }>(`
      SELECT active_runs,maximum_runs,draining FROM ${SQL_SCHEMA}.workers WHERE namespace=$1 AND id=$2
    `, [this.namespace, workerId])
    const row = result.rows[0]
    return row === undefined ? { activeRuns: 0, maximumRuns: 0, draining: true } : { activeRuns: row.active_runs, maximumRuns: row.maximum_runs, draining: row.draining }
  }

  async healthyWorker(preferredId?: string): Promise<WorkerRecord | undefined> {
    const result = await this.pool.query<WorkerRow>(`
      SELECT id,base_url,maximum_runs,active_runs FROM ${SQL_SCHEMA}.workers
      WHERE namespace=$1 AND NOT draining AND heartbeat_at>now()-interval '15 seconds' AND active_runs<maximum_runs
      ORDER BY CASE WHEN id=$2 THEN 0 ELSE 1 END, active_runs::float/maximum_runs, id LIMIT 1
    `, [this.namespace, preferredId ?? ''])
    return result.rows[0] === undefined ? undefined : worker(result.rows[0])
  }

  async routeWorker(userId: string): Promise<WorkerRecord | undefined> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const user = await client.query<{ preferred_worker_id: string | null }>(`
        SELECT preferred_worker_id FROM ${SQL_SCHEMA}.users WHERE namespace=$1 AND id=$2 FOR UPDATE
      `, [this.namespace, userId])
      if (user.rows[0] === undefined) { await client.query('COMMIT'); return undefined }
      const selected = await client.query<WorkerRow>(`
        SELECT id,base_url,maximum_runs,active_runs FROM ${SQL_SCHEMA}.workers
        WHERE namespace=$1 AND NOT draining AND heartbeat_at>now()-interval '15 seconds'
        ORDER BY CASE WHEN id=$2 THEN 0 ELSE 1 END,
          active_runs::float/maximum_runs,
          (SELECT count(*) FROM ${SQL_SCHEMA}.users assigned
            WHERE assigned.namespace=${SQL_SCHEMA}.workers.namespace AND assigned.preferred_worker_id=${SQL_SCHEMA}.workers.id),
          id
        LIMIT 1
      `, [this.namespace, user.rows[0].preferred_worker_id ?? ''])
      const row = selected.rows[0]
      if (row !== undefined) await client.query(`UPDATE ${SQL_SCHEMA}.users SET preferred_worker_id=$3 WHERE namespace=$1 AND id=$2`, [this.namespace, userId, row.id])
      await client.query('COMMIT')
      return row === undefined ? undefined : worker(row)
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async enqueueRun(input: { tenantId: string; sessionId: string; clientRpcId: string; idempotencyKey: string; request: unknown }): Promise<EnqueuedRun> {
    const session = await this.pool.query<{ workspace_id: string }>(`SELECT workspace_id FROM ${SQL_SCHEMA}.sessions WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, input.tenantId, input.sessionId])
    const row = session.rows[0]
    if (row === undefined) throw Object.assign(new Error('session is not owned by this tenant'), { code: 'EACCES' })
    const id = randomUUID()
    const result = await this.pool.query<{ id: string; status: string; inserted: boolean }>(`
      INSERT INTO ${SQL_SCHEMA}.runs(namespace,id,tenant_id,session_id,workspace_id,client_rpc_id,idempotency_key,status,request_json)
      VALUES($1,$2,$3,$4,$5,$6,$7,'queued',$8::jsonb)
      ON CONFLICT(namespace,tenant_id,idempotency_key) DO UPDATE SET id=${SQL_SCHEMA}.runs.id
      RETURNING id,status,(id=$2::uuid) AS inserted
    `, [this.namespace, id, input.tenantId, input.sessionId, row.workspace_id, input.clientRpcId, input.idempotencyKey, JSON.stringify(input.request)])
    const returned = result.rows[0]
    if (returned === undefined) throw new Error('run admission returned no row')
    return { runId: returned.id, created: returned.inserted, status: returned.status }
  }

  /** Compete for one ready Run using the calling Worker's real local capacity. */
  async claimNext(workerId: string): Promise<ClaimResult> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const workerResult = await client.query<WorkerRow>(`
        SELECT id,base_url,maximum_runs,active_runs FROM ${SQL_SCHEMA}.workers
        WHERE namespace=$1 AND id=$2 AND NOT draining AND heartbeat_at>now()-interval '15 seconds'
        FOR UPDATE
      `, [this.namespace, workerId])
      const selected = workerResult.rows[0]
      if (selected === undefined || selected.active_runs >= selected.maximum_runs) {
        await client.query('COMMIT')
        return { kind: 'idle' }
      }
      const result = await client.query<RunRow>(`
        SELECT run.id,run.tenant_id,run.session_id,run.workspace_id,run.status,run.request_json
        FROM ${SQL_SCHEMA}.runs run
        JOIN ${SQL_SCHEMA}.sessions session ON session.namespace=run.namespace AND session.id=run.session_id
        JOIN ${SQL_SCHEMA}.tenants tenant ON tenant.namespace=run.namespace AND tenant.id=run.tenant_id
        WHERE run.namespace=$1 AND run.status='queued' AND run.available_at<=now()
          AND (session.preferred_worker_id IS NULL OR session.preferred_worker_id=$2 OR NOT EXISTS (
            SELECT 1 FROM ${SQL_SCHEMA}.workers preferred
            WHERE preferred.namespace=run.namespace AND preferred.id=session.preferred_worker_id
              AND NOT preferred.draining AND preferred.heartbeat_at>now()-interval '15 seconds'
          ))
          AND (SELECT count(*) FROM ${SQL_SCHEMA}.runs tenant_active
            WHERE tenant_active.namespace=run.namespace AND tenant_active.tenant_id=run.tenant_id
              AND tenant_active.status IN ('claimed','dispatched','running','cancel_requested')) < tenant.maximum_concurrent_runs
          AND NOT EXISTS (
            SELECT 1 FROM ${SQL_SCHEMA}.runs active
            WHERE active.namespace=run.namespace AND active.workspace_id=run.workspace_id
              AND active.status IN ('claimed','dispatched','running','cancel_requested')
          )
          AND NOT EXISTS (
            SELECT 1 FROM ${SQL_SCHEMA}.runs earlier
            WHERE earlier.namespace=run.namespace AND earlier.session_id=run.session_id AND earlier.status='queued'
              AND (earlier.created_at,earlier.id)<(run.created_at,run.id)
          )
        ORDER BY CASE WHEN session.preferred_worker_id=$2 THEN 0 ELSE 1 END,
                 tenant.last_scheduled_at ASC, run.created_at ASC, run.id ASC
        FOR UPDATE OF run SKIP LOCKED LIMIT 1
      `, [this.namespace, workerId])
      const row = result.rows[0]
      if (row === undefined) { await client.query('COMMIT'); return { kind: 'idle' } }
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [`${this.namespace}:${row.workspace_id}`])
      const session = await client.query<{ preferred_worker_id: string | null }>(`SELECT preferred_worker_id FROM ${SQL_SCHEMA}.sessions WHERE namespace=$1 AND id=$2 FOR UPDATE`, [this.namespace, row.session_id])
      const sessionRow = session.rows[0]
      if (sessionRow === undefined) throw new Error('run session disappeared')
      const workspace = await client.query<{ next_fence: string }>(`
        SELECT next_fence::text FROM ${SQL_SCHEMA}.workspaces
        WHERE namespace=$1 AND id=$2 AND tenant_id=$3 FOR UPDATE
      `, [this.namespace, row.workspace_id, row.tenant_id])
      const workspaceRow = workspace.rows[0]
      if (workspaceRow === undefined) throw new Error('run workspace disappeared')
      const writerFence = Number(workspaceRow.next_fence) + 1
      const attemptId = randomUUID()
      await client.query(`UPDATE ${SQL_SCHEMA}.workspaces SET next_fence=$3 WHERE namespace=$1 AND id=$2`, [this.namespace, row.workspace_id, writerFence])
      await client.query(`UPDATE ${SQL_SCHEMA}.sessions SET preferred_worker_id=$3 WHERE namespace=$1 AND id=$2`, [this.namespace, row.session_id, workerId])
      await client.query(`UPDATE ${SQL_SCHEMA}.workers SET active_runs=active_runs+1 WHERE namespace=$1 AND id=$2`, [this.namespace, workerId])
      await client.query(`UPDATE ${SQL_SCHEMA}.tenants SET last_scheduled_at=now() WHERE namespace=$1 AND id=$2`, [this.namespace, row.tenant_id])
      await client.query(`UPDATE ${SQL_SCHEMA}.runs SET status='claimed',worker_id=$3,current_attempt_id=$4,writer_fence=$5,updated_at=now() WHERE namespace=$1 AND id=$2`, [this.namespace, row.id, workerId, attemptId, writerFence])
      await client.query(`INSERT INTO ${SQL_SCHEMA}.run_attempts(namespace,id,run_id,worker_id,writer_fence,status) VALUES($1,$2,$3,$4,$5,'claimed')`, [this.namespace, attemptId, row.id, workerId, writerFence])
      await client.query('COMMIT')
      return { kind: 'claimed', run: { runId: row.id, tenantId: row.tenant_id, sessionId: row.session_id, workspaceId: row.workspace_id, attemptId, writerFence, worker: worker(selected), request: row.request_json } }
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async markDispatched(runId: string, attemptId: string, response: unknown): Promise<void> {
    await this.pool.query(`UPDATE ${SQL_SCHEMA}.runs SET status='dispatched',response_json=$4::jsonb,updated_at=now() WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3 AND status='claimed'`, [this.namespace, runId, attemptId, JSON.stringify(response)])
    await this.pool.query(`UPDATE ${SQL_SCHEMA}.run_attempts SET heartbeat_at=now() WHERE namespace=$1 AND id=$2 AND run_id=$3`, [this.namespace, attemptId, runId])
  }

  async promptPersisted(runId: string): Promise<boolean> {
    const result = await this.pool.query<{ persisted: boolean }>(`
      SELECT EXISTS(
        SELECT 1 FROM dsh_cloud.session_events event
        WHERE event.namespace=$1 AND event.session_id=run.session_id AND event.event->>'type'='user/message'
          AND event.event#>>'{data,source,rpcId}'=run.client_rpc_id
      ) AS persisted FROM ${SQL_SCHEMA}.runs run WHERE run.namespace=$1 AND run.id=$2
    `, [this.namespace, runId])
    return result.rows[0]?.persisted ?? false
  }

  async markRunning(runId: string, attemptId: string): Promise<void> {
    const result = await this.pool.query(`UPDATE ${SQL_SCHEMA}.runs SET status='running',updated_at=now() WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3 AND status IN ('claimed','dispatched')`, [this.namespace, runId, attemptId])
    if (result.rowCount !== 1) throw Object.assign(new Error('Run lost writer ownership before start'), { code: 'ESTALE' })
    await this.pool.query(`UPDATE ${SQL_SCHEMA}.run_attempts SET status='running',heartbeat_at=now() WHERE namespace=$1 AND id=$2 AND run_id=$3 AND status='claimed'`, [this.namespace, attemptId, runId])
  }

  async heartbeatAttempt(runId: string, attemptId: string): Promise<boolean> {
    const result = await this.pool.query(`UPDATE ${SQL_SCHEMA}.run_attempts SET heartbeat_at=now() WHERE namespace=$1 AND id=$2 AND run_id=$3 AND status IN ('claimed','running')`, [this.namespace, attemptId, runId])
    return result.rowCount === 1
  }

  async attemptCount(runId: string): Promise<number> {
    const result = await this.pool.query<{ count: string }>(`SELECT count(*)::text AS count FROM ${SQL_SCHEMA}.run_attempts WHERE namespace=$1 AND run_id=$2`, [this.namespace, runId])
    return Number(result.rows[0]?.count ?? '0')
  }

  async requeueBeforeStart(runId: string, attemptId: string, delayMs: number): Promise<void> {
    if (await this.promptPersisted(runId)) throw new Error('a Run with a durable prompt cannot be requeued')
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const result = await client.query<{ worker_id: string | null }>(`
        WITH owned AS (
          SELECT worker_id FROM ${SQL_SCHEMA}.runs
          WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3 AND status IN ('claimed','dispatched') FOR UPDATE
        ), updated AS (
          UPDATE ${SQL_SCHEMA}.runs SET status='queued',worker_id=NULL,current_attempt_id=NULL,response_json=NULL,
            available_at=now()+make_interval(secs=>$4::double precision/1000),updated_at=now()
          WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3 AND status IN ('claimed','dispatched') RETURNING 1
        ) SELECT worker_id FROM owned WHERE EXISTS(SELECT 1 FROM updated)
      `, [this.namespace, runId, attemptId, Math.max(0, delayMs)])
      const workerId = result.rows[0]?.worker_id
      if (workerId !== undefined && workerId !== null) await client.query(`UPDATE ${SQL_SCHEMA}.workers SET active_runs=GREATEST(0,active_runs-1) WHERE namespace=$1 AND id=$2`, [this.namespace, workerId])
      await client.query(`UPDATE ${SQL_SCHEMA}.run_attempts SET status='superseded',finished_at=now(),heartbeat_at=now() WHERE namespace=$1 AND id=$2 AND run_id=$3`, [this.namespace, attemptId, runId])
      await client.query(`SELECT pg_notify('dsh_cloud_run_queue',$1)`, [runId])
      await client.query('COMMIT')
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async runResponse(runId: string): Promise<{ status: string; clientRpcId: string; response?: unknown; workerId?: string } | undefined> {
    const result = await this.pool.query<{ status: string; client_rpc_id: string; response_json: unknown | null; worker_id: string | null }>(`SELECT status,client_rpc_id,response_json,worker_id FROM ${SQL_SCHEMA}.runs WHERE namespace=$1 AND id=$2`, [this.namespace, runId])
    const row = result.rows[0]
    return row === undefined ? undefined : { status: row.status, clientRpcId: row.client_rpc_id, ...(row.response_json === null ? {} : { response: row.response_json }), ...(row.worker_id === null ? {} : { workerId: row.worker_id }) }
  }

  async turnOutcome(runId: string): Promise<RunTurnOutcome | undefined> {
    const result = await this.pool.query<{ reason: unknown }>(`
      SELECT terminal.event#>'{data,reason}' AS reason
      FROM ${SQL_SCHEMA}.runs run
      JOIN dsh_cloud.session_events terminal
        ON terminal.namespace=run.namespace AND terminal.session_id=run.session_id
      WHERE run.namespace=$1 AND run.id=$2 AND terminal.event->>'type'='turn/end'
        AND terminal.seq > COALESCE((SELECT MIN(prompt.seq) FROM dsh_cloud.session_events prompt
          WHERE prompt.namespace=run.namespace AND prompt.session_id=run.session_id
            AND prompt.event->>'type'='user/message'
            AND prompt.event#>>'{data,source,rpcId}'=run.client_rpc_id), 9223372036854775807)
      ORDER BY terminal.seq LIMIT 1
    `, [this.namespace, runId])
    const reason = result.rows[0]?.reason
    if (reason === null || typeof reason !== 'object' || Array.isArray(reason)) return undefined
    const record = reason as Record<string, unknown>
    if (typeof record['kind'] !== 'string' || record['kind'].length === 0) return undefined
    const failure = record['error']
    const code = failure !== null && typeof failure === 'object' && !Array.isArray(failure)
      ? (failure as Record<string, unknown>)['code']
      : undefined
    return { kind: record['kind'], ...(typeof code === 'string' && code.length > 0 ? { errorCode: code } : {}) }
  }

  async turnCompleted(runId: string): Promise<boolean> {
    return await this.turnOutcome(runId) !== undefined
  }

  async finishRun(runId: string, attemptId: string, status: 'completed' | 'failed' | 'cancelled' | 'timed_out', errorCode?: string): Promise<void> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const updated = await client.query<{ worker_id: string | null; workspace_id: string; session_id: string; writer_fence: string }>(`UPDATE ${SQL_SCHEMA}.runs SET status=$4,error_code=$5,updated_at=now() WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3 AND status IN ('claimed','dispatched','running','cancel_requested') RETURNING worker_id,workspace_id,session_id,writer_fence::text`, [this.namespace, runId, attemptId, status, errorCode ?? null])
      const workerId = updated.rows[0]?.worker_id
      if (workerId !== undefined && workerId !== null) await client.query(`UPDATE ${SQL_SCHEMA}.workers SET active_runs=GREATEST(0,active_runs-1) WHERE namespace=$1 AND id=$2`, [this.namespace, workerId])
      const workspace = updated.rows[0]
      if (workspace !== undefined) {
        const current = await client.query(`
          UPDATE ${SQL_SCHEMA}.workspaces workspace
             SET revision=revision+1,dirty_fence=0,updated_at=now()
           WHERE workspace.namespace=$1 AND workspace.id=$2
             AND workspace.dirty_fence=$4 AND workspace.next_fence=$4
             AND EXISTS(SELECT 1 FROM ${SQL_SCHEMA}.sessions session
               WHERE session.namespace=$1 AND session.id=$3 AND session.workspace_id=$2)
        `, [this.namespace, workspace.workspace_id, workspace.session_id, workspace.writer_fence])
        if (current.rowCount === 0) {
          const fence = await client.query(`SELECT 1 FROM ${SQL_SCHEMA}.workspaces WHERE namespace=$1 AND id=$2 AND next_fence=$3`, [this.namespace, workspace.workspace_id, workspace.writer_fence])
          if (fence.rowCount !== 1) throw Object.assign(new Error('Workspace fence changed before Run settlement'), { code: 'ESTALE' })
        }
      }
      await client.query(`UPDATE ${SQL_SCHEMA}.run_attempts SET status=$4,finished_at=now(),heartbeat_at=now() WHERE namespace=$1 AND id=$2 AND run_id=$3`, [this.namespace, attemptId, runId, status === 'completed' ? 'completed' : status === 'cancelled' ? 'cancelled' : 'failed'])
      await client.query('COMMIT')
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async requestCancellation(tenantId: string, runId: string): Promise<boolean> {
    const result = await this.pool.query(`UPDATE ${SQL_SCHEMA}.runs SET status=CASE WHEN status='queued' THEN 'cancelled' ELSE 'cancel_requested' END,updated_at=now() WHERE namespace=$1 AND tenant_id=$2 AND id=$3 AND status IN ('queued','claimed','dispatched','running')`, [this.namespace, tenantId, runId])
    await this.pool.query(`SELECT pg_notify('dsh_cloud_run_queue',$1)`, [runId])
    return result.rowCount === 1
  }

  async requestSessionCancellation(tenantId: string, sessionId: string): Promise<boolean> {
    const result = await this.pool.query<{ id: string }>(`
      UPDATE ${SQL_SCHEMA}.runs SET status=CASE WHEN status='queued' THEN 'cancelled' ELSE 'cancel_requested' END,updated_at=now()
      WHERE namespace=$1 AND tenant_id=$2 AND session_id=$3 AND status IN ('queued','claimed','dispatched','running') RETURNING id
    `, [this.namespace, tenantId, sessionId])
    for (const row of result.rows) await this.pool.query(`SELECT pg_notify('dsh_cloud_run_queue',$1)`, [row.id])
    return result.rowCount !== null && result.rowCount > 0
  }

  /** Issue short-lived authority for a Session mutation; steering may join the current Run. */
  async issueSessionCommand(tenantId: string, sessionId: string, rpcId: string, joinActiveRun = false): Promise<void> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const existing = await client.query(`SELECT 1 FROM ${SQL_SCHEMA}.session_commands WHERE namespace=$1 AND rpc_id=$2 AND tenant_id=$3 AND expires_at>now()`, [this.namespace, rpcId, tenantId])
      if (existing.rowCount === 1) { await client.query('COMMIT'); return }
      const session = await client.query<{ workspace_id: string }>(`
        SELECT workspace_id FROM ${SQL_SCHEMA}.sessions session
        WHERE namespace=$1 AND tenant_id=$2 AND id=$3
      `, [this.namespace, tenantId, sessionId])
      const row = session.rows[0]
      if (row === undefined) throw Object.assign(new Error('Session is unavailable'), { code: 'ENOENT' })
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [`${this.namespace}:${row.workspace_id}`])
      const workspace = await client.query<{ next_fence: string }>(`
        SELECT next_fence::text FROM ${SQL_SCHEMA}.workspaces
        WHERE namespace=$1 AND tenant_id=$2 AND id=$3 FOR UPDATE
      `, [this.namespace, tenantId, row.workspace_id])
      const workspaceRow = workspace.rows[0]
      if (workspaceRow === undefined) throw Object.assign(new Error('Workspace is unavailable'), { code: 'ENOENT' })
      const active = await client.query<{ id: string; session_id: string; current_attempt_id: string; writer_fence: string }>(`
        SELECT id,session_id,current_attempt_id,writer_fence::text FROM ${SQL_SCHEMA}.runs
        WHERE namespace=$1 AND workspace_id=$2 AND status IN ('claimed','dispatched','running','cancel_requested')
        FOR UPDATE
      `, [this.namespace, row.workspace_id])
      const owner = active.rows[0]
      if (owner !== undefined && (!joinActiveRun || owner.session_id !== sessionId)) {
        throw Object.assign(new Error('Session is busy'), { code: 'EBUSY' })
      }
      const commandId = randomUUID()
      const fence = owner === undefined ? Number(workspaceRow.next_fence) + 1 : Number(owner.writer_fence)
      if (owner === undefined) {
        await client.query(`UPDATE ${SQL_SCHEMA}.workspaces SET next_fence=$4 WHERE namespace=$1 AND tenant_id=$2 AND id=$3`, [this.namespace, tenantId, row.workspace_id, fence])
      }
      await client.query(`
        INSERT INTO ${SQL_SCHEMA}.session_commands
          (namespace,rpc_id,tenant_id,session_id,workspace_id,command_id,run_id,attempt_id,writer_fence,expires_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,now()+interval '2 minutes')
      `, [this.namespace, rpcId, tenantId, sessionId, row.workspace_id, commandId, owner?.id ?? commandId, owner?.current_attempt_id ?? commandId, fence])
      await client.query('COMMIT')
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async cancellationRequested(runId: string, attemptId: string): Promise<boolean> {
    const result = await this.pool.query(`SELECT 1 FROM ${SQL_SCHEMA}.runs WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3 AND status='cancel_requested'`, [this.namespace, runId, attemptId])
    return result.rowCount === 1
  }

  async authorityForRpcId(rpcId: string): Promise<{ tenantId: string; workspaceId: string; sessionId: string; runId: string; attemptId: string; writerFence: number } | undefined> {
    const result = await this.pool.query<{ tenant_id: string; workspace_id: string; session_id: string; id: string; current_attempt_id: string; writer_fence: string }>(`SELECT tenant_id,workspace_id,session_id,id,current_attempt_id,writer_fence::text FROM ${SQL_SCHEMA}.runs WHERE namespace=$1 AND client_rpc_id=$2 AND status IN ('claimed','dispatched','running') ORDER BY created_at DESC LIMIT 1`, [this.namespace, rpcId])
    const row = result.rows[0]
    if (row !== undefined) return { tenantId: row.tenant_id, workspaceId: row.workspace_id, sessionId: row.session_id, runId: row.id, attemptId: row.current_attempt_id, writerFence: Number(row.writer_fence) }
    const command = await this.pool.query<{ tenant_id: string; workspace_id: string; session_id: string; command_id: string; run_id: string | null; attempt_id: string | null; writer_fence: string }>(`SELECT tenant_id,workspace_id,session_id,command_id,run_id,attempt_id,writer_fence::text FROM ${SQL_SCHEMA}.session_commands WHERE namespace=$1 AND rpc_id=$2 AND expires_at>now()`, [this.namespace, rpcId])
    const item = command.rows[0]
    return item === undefined ? undefined : { tenantId: item.tenant_id, workspaceId: item.workspace_id, sessionId: item.session_id, runId: item.run_id ?? item.command_id, attemptId: item.attempt_id ?? item.command_id, writerFence: Number(item.writer_fence) }
  }

  /** Settle expired ownership without replaying a prompt that may already have side effects. */
  async reconcileExpiredAttempts(leaseSeconds = 20): Promise<{ requeued: number; failed: number }> {
    const client = await this.pool.connect()
    let requeued = 0
    let failed = 0
    try {
      await client.query('BEGIN')
      const expired = await client.query<{ run_id: string; attempt_id: string; worker_id: string; prompt_persisted: boolean }>(`
        SELECT r.id AS run_id,a.id AS attempt_id,a.worker_id,
          EXISTS(SELECT 1 FROM dsh_cloud.session_events e WHERE e.namespace=r.namespace AND e.session_id=r.session_id
            AND e.event->>'type'='user/message' AND e.event#>>'{data,source,rpcId}'=r.client_rpc_id) AS prompt_persisted
        FROM ${SQL_SCHEMA}.run_attempts a JOIN ${SQL_SCHEMA}.runs r
          ON r.namespace=a.namespace AND r.id=a.run_id AND r.current_attempt_id=a.id
        WHERE a.namespace=$1 AND a.status IN ('claimed','running')
          AND a.heartbeat_at < now()-make_interval(secs=>$2)
        FOR UPDATE OF a,r SKIP LOCKED
      `, [this.namespace, leaseSeconds])
      for (const row of expired.rows) {
        if (row.prompt_persisted) {
          await client.query(`UPDATE ${SQL_SCHEMA}.runs SET status='failed',error_code='worker_lease_expired',updated_at=now() WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3`, [this.namespace, row.run_id, row.attempt_id])
          failed++
        } else {
          await client.query(`UPDATE ${SQL_SCHEMA}.runs SET status='queued',worker_id=NULL,current_attempt_id=NULL,response_json=NULL,available_at=now(),updated_at=now() WHERE namespace=$1 AND id=$2 AND current_attempt_id=$3`, [this.namespace, row.run_id, row.attempt_id])
          requeued++
        }
        await client.query(`UPDATE ${SQL_SCHEMA}.run_attempts SET status='superseded',finished_at=now() WHERE namespace=$1 AND id=$2`, [this.namespace, row.attempt_id])
        await client.query(`UPDATE ${SQL_SCHEMA}.workers SET active_runs=GREATEST(0,active_runs-1) WHERE namespace=$1 AND id=$2`, [this.namespace, row.worker_id])
      }
      if (requeued > 0) await client.query(`SELECT pg_notify('dsh_cloud_run_queue','reconcile')`)
      await client.query('COMMIT')
      return { requeued, failed }
    } catch (error) { await client.query('ROLLBACK').catch(() => undefined); throw error } finally { client.release() }
  }

  async operationalMetrics(): Promise<OperationalMetrics> {
    const result = await this.pool.query<{ queued: string; active: string; failed: string; workers: string; tenants: string }>(`
      SELECT
        (SELECT count(*) FROM ${SQL_SCHEMA}.runs WHERE namespace=$1 AND status='queued')::text AS queued,
        (SELECT count(*) FROM ${SQL_SCHEMA}.runs WHERE namespace=$1 AND status IN ('claimed','dispatched','running','cancel_requested'))::text AS active,
        (SELECT count(*) FROM ${SQL_SCHEMA}.runs WHERE namespace=$1 AND status='failed')::text AS failed,
        (SELECT count(*) FROM ${SQL_SCHEMA}.workers WHERE namespace=$1 AND NOT draining AND heartbeat_at>now()-interval '15 seconds')::text AS workers,
        (SELECT count(*) FROM ${SQL_SCHEMA}.tenants WHERE namespace=$1)::text AS tenants
    `, [this.namespace])
    const row = result.rows[0]
    return { queuedRuns:Number(row?.queued??0),activeRuns:Number(row?.active??0),failedRuns:Number(row?.failed??0),healthyWorkers:Number(row?.workers??0),registeredTenants:Number(row?.tenants??0) }
  }
}
