import { mkdtemp, rm } from 'node:fs/promises'
import { randomBytes, randomUUID } from 'node:crypto'
import { createServer as createHttpServer } from 'node:http'
import { createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import { Pool } from 'pg'
import { afterEach, describe, expect, it } from 'vitest'
import { ControlStore } from '@dsh-cloud/control-store'
import CloudRunContext, { cloudIdentifier, writerFence, type RunAuthority } from '@dsh-cloud/run-context'
import CloudExecutionClient from '@dsh-cloud/execution-client'
import RemoteFileSystem from '@dsh-cloud/fs-remote'
import RemoteSubprocessRuntime from '@dsh-cloud/subprocess-remote'
import { ExecutionEngine } from '@dsh-cloud/execution-agent/engine'
import { createExecutionAgent } from '@dsh-cloud/execution-agent'
import { parseExecutionResponse, type ExecutionRequest, type ExecutionResponse } from '@dsh-cloud/execution-protocol'
import { ToolBroker } from '../src/index.js'
import CubeSandboxProvider from '../src/cube-provider.js'
import type { ManagedSandbox, SandboxBinding, SandboxHandle, SandboxProvider } from '../src/provider.js'

const databaseUrl = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const integration = databaseUrl === undefined ? describe.skip : describe
const cleanups: Array<() => Promise<void>> = []

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

async function port(): Promise<number> {
  const server = createNetServer()
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve) })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('test server did not receive a port')
  await new Promise<void>((resolve, reject) => server.close(error => error === undefined ? resolve() : reject(error)))
  return address.port
}

class AgentProvider implements SandboxProvider {
  constructor(private readonly endpoint: string, private readonly reset: () => Promise<void>) {}
  async create(input: Readonly<{ activationId: string }>): Promise<SandboxHandle> {
    return { provider: 'test-agent', sandboxId: input.activationId, endpoint: this.endpoint }
  }
  async bind(handle: SandboxHandle, binding: SandboxBinding, previous?: SandboxBinding): Promise<void> {
    const response = await fetch(`${handle.endpoint}${previous === undefined ? '/v1/bind' : '/v1/rebind'}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        connection: 'close',
        ...(previous === undefined ? {} : { 'x-dsh-execution-secret': previous.secret, 'x-dsh-activation-id': previous.activationId }),
      },
      body: JSON.stringify({ activationId: binding.activationId, secret: binding.secret, writerFence: binding.writerFence }),
    })
    if (!response.ok) throw new Error(`test execution bind failed with ${response.status}`)
  }
  async execute(handle: SandboxHandle, binding: SandboxBinding, request: ExecutionRequest): Promise<ExecutionResponse> {
    const response = await fetch(`${handle.endpoint}/v1/execute`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        connection: 'close',
        'x-dsh-execution-secret': binding.secret,
        'x-dsh-activation-id': binding.activationId,
        'x-dsh-writer-fence': String(binding.writerFence),
      },
      body: JSON.stringify(request),
    })
    return parseExecutionResponse(await response.json(), request.operationId)
  }
  async destroy(): Promise<void> { await this.reset() }
  async inspect(): Promise<'ready'> { return 'ready' }
}

class InventoryProvider implements SandboxProvider {
  readonly destroyed: string[] = []

  constructor(readonly inventory: readonly ManagedSandbox[]) {}
  async create(): Promise<SandboxHandle> { throw new Error('not used') }
  async bind(): Promise<void> { throw new Error('not used') }
  async execute(): Promise<ExecutionResponse> { throw new Error('not used') }
  async destroy(handle: SandboxHandle): Promise<void> { this.destroyed.push(handle.sandboxId) }
  async inspect(): Promise<'ready'> { return 'ready' }
  async listManaged(): Promise<readonly ManagedSandbox[]> { return this.inventory }
}

class SlowProvider implements SandboxProvider {
  creates = 0
  deleteFailuresRemaining = 0
  readonly deletedWorkspaces: Array<{ tenantId: string; workspaceId: string }> = []

  async create(input: Readonly<{ activationId: string }>): Promise<SandboxHandle> {
    this.creates++
    await new Promise(resolve => setTimeout(resolve, 75))
    return { provider: 'slow', sandboxId: input.activationId, endpoint: 'http://unused' }
  }
  async bind(): Promise<void> {}
  async execute(_handle: SandboxHandle, _binding: SandboxBinding, request: ExecutionRequest): Promise<ExecutionResponse> {
    return { protocolVersion: 1, operationId: request.operationId, ok: true, result: [] }
  }
  async destroy(): Promise<void> {}
  async inspect(): Promise<'ready'> { return 'ready' }
  async destroyWorkspace(input: Readonly<{ tenantId: string; workspaceId: string }>): Promise<void> {
    if (this.deleteFailuresRemaining > 0) {
      this.deleteFailuresRemaining--
      throw new Error('injected persistent storage failure')
    }
    this.deletedWorkspaces.push(input)
  }
}

function authority(workspace: string, fence: number, attempt = `attempt-${fence}`): RunAuthority {
  return {
    tenantId: cloudIdentifier('TenantId', 'tenant-a'),
    workspaceId: cloudIdentifier('WorkspaceId', workspace),
    sessionId: cloudIdentifier('SessionId', 'session-a'),
    runId: cloudIdentifier('RunId', `run-${fence}`),
    attemptId: cloudIdentifier('AttemptId', attempt),
    writerFence: writerFence(fence),
  }
}

async function setup(): Promise<{
  ctx: Context
  workspace: string
  brokerUrl: string
  dispose: () => Promise<void>
}> {
  const root = await mkdtemp(join(tmpdir(), 'dsh-cloud-execution-'))
  const workspace = join(root, 'workspace')
  const runtime = join(root, 'runtime')
  const agentPort = await port()
  let engine!: ExecutionEngine
  let agent!: ReturnType<typeof createExecutionAgent>
  const startAgent = async (): Promise<void> => {
    engine = new ExecutionEngine(workspace, runtime)
    await engine.initialize()
    agent = createExecutionAgent({ engine })
    await new Promise<void>((resolve, reject) => { agent.once('error', reject); agent.listen(agentPort, '127.0.0.1', resolve) })
  }
  const resetAgent = async (): Promise<void> => {
    await new Promise<void>(resolve => agent.close(() => resolve()))
    await engine.dispose()
    await startAgent()
  }
  await startAgent()

  const pool = new Pool({ connectionString: databaseUrl, max: 4 })
  const namespace = `execution-${randomUUID()}`
  const token = 'integration-tool-broker-token-32-characters'
  const broker = new ToolBroker({
    pool,
    namespace,
    internalToken: token,
    encryptionKey: randomBytes(32),
    provider: new AgentProvider(`http://127.0.0.1:${agentPort}`, resetAgent),
    authorityVerifier: async () => undefined,
  })
  await broker.initialize()
  const brokerServer = broker.createServer()
  const brokerPort = await port()
  await new Promise<void>((resolve, reject) => { brokerServer.once('error', reject); brokerServer.listen(brokerPort, '127.0.0.1', resolve) })

  const ctx = new Context()
  const fibers = [
    await ctx.plugin(CloudRunContext),
    await ctx.plugin(CloudExecutionClient, { brokerUrl: `http://127.0.0.1:${brokerPort}`, internalToken: token }),
    await ctx.plugin(RemoteFileSystem, { workspaceRoot: workspace }),
    await ctx.plugin(RemoteSubprocessRuntime),
  ]
  const dispose = async (): Promise<void> => {
    for (const fiber of fibers.reverse()) await fiber.dispose()
    await new Promise<void>(resolve => brokerServer.close(() => resolve()))
    await new Promise<void>(resolve => agent.close(() => resolve()))
    await engine.dispose()
    await pool.query('DELETE FROM dsh_cloud_sandbox.activations WHERE namespace = $1', [namespace])
    await pool.end()
    await rm(root, { recursive: true, force: true })
  }
  cleanups.push(dispose)
  return { ctx, workspace, brokerUrl: `http://127.0.0.1:${brokerPort}`, dispose }
}

describe('Cube provider contract', () => {
  it('uses Cube official API-key authentication for Volume lifecycle calls', async () => {
    const serverPort = await port()
    const requests: Array<{ method: string | undefined; headers: Record<string, string | string[] | undefined> }> = []
    const server = createHttpServer((request, response) => {
      requests.push({ method: request.method, headers: request.headers })
      response.writeHead(204).end()
    })
    await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(serverPort, '127.0.0.1', resolve) })
    try {
      const provider = new CubeSandboxProvider({
        namespace: 'test',
        apiUrl: `http://127.0.0.1:${serverPort}`,
        apiKey: 'cube-contract-key',
        templateId: 'template',
        proxyNodeIp: '127.0.0.1',
        proxyPort: serverPort,
        proxyScheme: 'http',
        sandboxDomain: 'cube.test',
        egressProxyIp: '192.0.2.1',
      })
      await provider.destroyWorkspace({ tenantId: 'tenant', workspaceId: 'workspace' })
      expect(requests.map(request => request.method)).toEqual(['GET', 'DELETE'])
      expect(requests.every(request => request.headers['x-api-key'] === undefined)).toBe(true)
      expect(requests.every(request => request.headers['authorization'] === 'Bearer cube-contract-key')).toBe(true)
    } finally {
      await new Promise<void>((resolve, reject) => server.close(error => error === undefined ? resolve() : reject(error)))
    }
  })

  it('treats an absent persistent Volume as an idempotent Workspace deletion', async () => {
    const serverPort = await port()
    const methods: string[] = []
    const server = createHttpServer((request, response) => {
      methods.push(request.method ?? '')
      response.writeHead(404).end()
    })
    await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(serverPort, '127.0.0.1', resolve) })
    try {
      const provider = new CubeSandboxProvider({
        namespace: 'test',
        apiUrl: `http://127.0.0.1:${serverPort}`,
        apiKey: 'cube-contract-key',
        templateId: 'template',
        proxyNodeIp: '127.0.0.1',
        proxyPort: serverPort,
        proxyScheme: 'http',
        sandboxDomain: 'cube.test',
        egressProxyIp: '192.0.2.1',
      })
      await provider.destroyWorkspace({ tenantId: 'tenant', workspaceId: 'workspace' })
      expect(methods).toEqual(['GET'])
    } finally {
      await new Promise<void>((resolve, reject) => server.close(error => error === undefined ? resolve() : reject(error)))
    }
  })
})

integration('remote DSH execution plane', () => {
  it('single-flights concurrent first Tool calls through one durable activation', async () => {
    const pool = new Pool({ connectionString: databaseUrl, max: 4 })
    const namespace = `singleflight-${randomUUID()}`
    const provider = new SlowProvider()
    const broker = new ToolBroker({
      pool,
      namespace,
      internalToken: 'integration-tool-broker-token-32-characters',
      encryptionKey: randomBytes(32),
      provider,
      authorityVerifier: async () => undefined,
    })
    await broker.initialize()
    const runAuthority = authority('workspace-singleflight', 1)
    const operation = { kind: 'fs.list', path: '/workspace' } as const
    try {
      const responses = await Promise.all([1, 2].map(index => broker.execute({
        protocolVersion: 1,
        operationId: `parallel-${index}`,
        authority: runAuthority,
        operation,
      })))
      expect(responses.every(response => response.ok)).toBe(true)
      expect(provider.creates).toBe(1)
    } finally {
      await pool.query('DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1', [namespace])
      await pool.end()
    }
  })

  it('deletes the persistent provider state before removing a reserved Workspace', async () => {
    const pool = new Pool({ connectionString: databaseUrl, max: 4 })
    const namespace = `workspace-delete-${randomUUID()}`
    const store = new ControlStore(pool, namespace)
    await store.initialize()
    const principal = await store.register('Deletion Tenant', `${randomUUID()}@example.test`, 'correct horse battery staple')
    const workspace = await store.createWorkspace(principal.tenantId, 'Disposable')
    expect(await store.beginWorkspaceDeletion(principal.tenantId, workspace.id)).toBe(true)
    const provider = new SlowProvider()
    const broker = new ToolBroker({
      pool,
      namespace,
      internalToken: 'integration-tool-broker-token-32-characters',
      encryptionKey: randomBytes(32),
      provider,
      authorityVerifier: async () => undefined,
    })
    await broker.initialize()
    try {
      await broker.destroyWorkspace(principal.tenantId, workspace.id)
      expect(provider.deletedWorkspaces).toEqual([{ tenantId: principal.tenantId, workspaceId: workspace.id }])
      expect(await store.workspaceOwned(principal.tenantId, workspace.id)).toBe(false)
    } finally {
      await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
      await pool.end()
    }
  })

  it('keeps an ambiguous Workspace deletion reserved until the physical operation can be retried', async () => {
    const pool = new Pool({ connectionString: databaseUrl, max: 4 })
    const namespace = `workspace-delete-retry-${randomUUID()}`
    const store = new ControlStore(pool, namespace)
    await store.initialize()
    const principal = await store.register('Retry Tenant', `${randomUUID()}@example.test`, 'correct horse battery staple')
    const workspace = await store.createWorkspace(principal.tenantId, 'Retry deletion')
    expect(await store.beginWorkspaceDeletion(principal.tenantId, workspace.id)).toBe(true)
    const provider = new SlowProvider()
    provider.deleteFailuresRemaining = 1
    const broker = new ToolBroker({
      pool,
      namespace,
      internalToken: 'integration-tool-broker-token-32-characters',
      encryptionKey: randomBytes(32),
      provider,
      authorityVerifier: async () => undefined,
    })
    await broker.initialize()
    try {
      await expect(broker.destroyWorkspace(principal.tenantId, workspace.id)).rejects.toThrow(/injected/)
      expect(await store.workspaceOwned(principal.tenantId, workspace.id)).toBe(false)
      await expect(broker.destroyWorkspace(principal.tenantId, workspace.id)).resolves.toBeUndefined()
      expect(provider.deletedWorkspaces).toEqual([{ tenantId: principal.tenantId, workspaceId: workspace.id }])
    } finally {
      await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1', [namespace])
      await pool.end()
    }
  })

  it('reconciles a physical Cube that has no PostgreSQL activation record', async () => {
    const pool = new Pool({ connectionString: databaseUrl, max: 2 })
    const namespace = `orphan-${randomUUID()}`
    const provider = new InventoryProvider([{
      activationId: randomUUID(),
      handle: { provider: 'cubesandbox-kvm', sandboxId: 'orphan-cube', endpoint: '' },
    }])
    const broker = new ToolBroker({
      pool,
      namespace,
      internalToken: 'integration-tool-broker-token-32-characters',
      encryptionKey: randomBytes(32),
      provider,
      authorityVerifier: async () => undefined,
    })
    await broker.initialize()
    try {
      await expect(broker.reconcile(60_000)).resolves.toEqual({ destroyed: 0, missing: 0, orphaned: 1 })
      expect(provider.destroyed).toEqual(['orphan-cube'])
    } finally {
      await pool.query('DELETE FROM dsh_cloud_sandbox.activations WHERE namespace=$1', [namespace])
      await pool.end()
    }
  })

  it('keeps filesystem and subprocess tools in one fenced execution world', async () => {
    const { ctx, workspace } = await setup()
    await ctx.cloudRunContext.run(authority('workspace-a', 1), async () => {
      const source = await ctx.fs.resolve('source.txt', { cwd: workspace })
      await ctx.fs.writeText(source, 'from-filesystem\n')
      const command = ctx.subprocess.spawn({
        argv: ['/bin/bash', '-c', 'cat source.txt > from-bash.txt; printf ok'],
        cwd: workspace,
        stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } },
        graceMs: 500,
      })
      await expect(command.done).resolves.toEqual({ exitCode: 0, signal: null })
      expect(command.collected.stdout?.readFrom(0).text).toBe('ok')
      const produced = await ctx.fs.resolve('from-bash.txt', { cwd: workspace })
      await expect(ctx.fs.readText(produced)).resolves.toBe('from-filesystem\n')
    })
  })

  it('rejects path escapes, scrubs credentials, and fences stale attempts', async () => {
    const { ctx, workspace } = await setup()
    process.env['DSH_CLOUD_SENTINEL_TOKEN'] = 'must-not-leak'
    try {
      await ctx.cloudRunContext.run(authority('workspace-b', 2), async () => {
        const probe = ctx.subprocess.spawn({
          argv: ['/bin/bash', '-c', 'printf "<%s>" "$DSH_CLOUD_SENTINEL_TOKEN"'],
          cwd: workspace,
          stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } },
          graceMs: 500,
        })
        await probe.done
        expect(probe.collected.stdout?.readFrom(0).text).toBe('<>')
        const link = ctx.subprocess.spawn({
          argv: ['/bin/ln', '-s', '/etc/passwd', 'escape'], cwd: workspace,
          stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } }, graceMs: 500,
        })
        await link.done
        await expect(ctx.fs.resolve('escape', { cwd: workspace })).rejects.toMatchObject({ code: 'FS_PERMISSION_DENIED' })
      })
      await expect(ctx.cloudRunContext.run(authority('workspace-b', 1), async () => {
        const target = await ctx.fs.resolve('stale.txt', { cwd: workspace })
        return ctx.fs.writeText(target, 'stale')
      })).rejects.toThrow(/stale writer authority/i)
      await expect(ctx.cloudRunContext.run(authority('workspace-b', 3), async () => {
        const target = await ctx.fs.resolve('fresh.txt', { cwd: workspace })
        return ctx.fs.writeText(target, 'fresh')
      })).resolves.toMatchObject({ operation: 'create' })
    } finally { delete process.env['DSH_CLOUD_SENTINEL_TOKEN'] }
  })

  it('terminates a remote process tree on cancellation', async () => {
    const { ctx, workspace } = await setup()
    const process = await ctx.cloudRunContext.run(authority('workspace-c', 1), async () => {
      const handle = ctx.subprocess.spawn({
        argv: ['/bin/bash', '-c', 'sleep 100 & wait'], cwd: workspace,
        stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } }, graceMs: 100,
      })
      await new Promise(resolve => setTimeout(resolve, 100))
      return handle
    })
    // Cancellation is initiated by the Run worker after the prompt context has
    // unwound, so the handle must retain the authority of the operation it owns.
    process.terminate()
    await expect(process.done).resolves.toMatchObject({ exitCode: null })
    await expect(process.waitForExit()).resolves.toBe(true)
  })
})
