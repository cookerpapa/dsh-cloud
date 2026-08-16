import { createHash } from 'node:crypto'
import { Agent, buildConnector, fetch, type Dispatcher } from 'undici'
import { MAX_RPC_RESPONSE_BYTES, parseExecutionResponse, type ExecutionRequest, type ExecutionResponse } from '@dsh-cloud/execution-protocol'
import type { ManagedSandbox, SandboxBinding, SandboxHandle, SandboxProvider } from './provider.js'

const EXECUTION_PORT = 49_984

interface CubeInstance {
  readonly sandboxID: string
  readonly metadata?: Record<string, unknown>
  readonly domain?: string
  readonly trafficAccessToken?: string
  readonly state?: string
}

export interface CubeSandboxProviderOptions {
  readonly namespace: string
  readonly apiUrl: string
  readonly apiKey: string
  readonly templateId: string
  readonly proxyNodeIp: string
  readonly proxyPort: number
  readonly proxyScheme: 'http' | 'https'
  readonly sandboxDomain: string
  readonly egressProxyIp: string
  readonly volumeDriver?: string
  readonly timeoutSeconds?: number
}

async function boundedBody(response: Awaited<ReturnType<typeof fetch>>, maximum = MAX_RPC_RESPONSE_BYTES): Promise<Buffer> {
  if (response.body === null) return Buffer.alloc(0)
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const chunk of response.body) {
    const value = Buffer.from(chunk)
    bytes += value.byteLength
    if (bytes > maximum) throw new Error('Cube response exceeded its byte limit')
    chunks.push(value)
  }
  return Buffer.concat(chunks)
}

function parseJson(bytes: Buffer): unknown {
  try { return JSON.parse(bytes.toString('utf8')) as unknown } catch { throw new Error('Cube returned invalid JSON') }
}

/** CubeSandbox KVM provider. No Docker/runc fallback exists in this production adapter. */
class CubeSandboxProvider implements SandboxProvider {
  private readonly apiUrl: string
  private readonly dispatcher: Dispatcher
  private readonly timeoutSeconds: number

  constructor(private readonly options: CubeSandboxProviderOptions) {
    this.apiUrl = new URL(options.apiUrl).toString().replace(/\/$/, '')
    if (options.apiKey.length === 0 || options.templateId.length === 0) throw new Error('Cube API key and template are required')
    this.timeoutSeconds = options.timeoutSeconds ?? 3600
    const connect = buildConnector({ timeout: 30_000 })
    this.dispatcher = new Agent({
      connect(connection, callback) {
        connect({ ...connection, hostname: options.proxyNodeIp, port: String(options.proxyPort) }, callback)
      },
    })
  }

  /**
   * Fail closed when this trusted Broker is accidentally pointed at another
   * product's Cube credential or path policy. Cube maps every callback denial
   * to HTTP 401, while an authorized nonexistent resource returns 404.
   */
  async verifyAuthorizationBoundary(): Promise<void> {
    const own = await this.controlRaw(`/volumes/dsh-${'0'.repeat(48)}`)
    await own.body?.cancel().catch(() => undefined)
    if (own.status !== 200 && own.status !== 404) {
      throw new Error(`Cube authorization does not admit DSH Cloud Volume identities (HTTP ${own.status})`)
    }

    const foreign = await this.controlRaw(`/volumes/adw-${'0'.repeat(48)}`)
    await foreign.body?.cancel().catch(() => undefined)
    if (foreign.status !== 401) {
      throw new Error('Cube credential is not isolated to the DSH Cloud API policy')
    }
  }

  async create(input: Readonly<{ activationId: string; tenantId: string; workspaceId: string; writerFence: number }>): Promise<SandboxHandle> {
    const volumeId = this.volumeId(input.tenantId, input.workspaceId)
    await this.ensureVolume(volumeId)
    const response = await this.control('/sandboxes', {
      method: 'POST',
      body: JSON.stringify({
        templateID: this.options.templateId,
        timeout: this.timeoutSeconds,
        metadata: {
          'dsh-cloud.managed': 'true',
          'dsh-cloud.namespace': this.options.namespace,
          'dsh-cloud.activation': input.activationId,
          'dsh-cloud.tenant': input.tenantId,
          'dsh-cloud.workspace': input.workspaceId,
        },
        allow_internet_access: true,
        network: { allowPublicTraffic: false, allowOut: [`${this.options.egressProxyIp}/32`], denyOut: ['0.0.0.0/0'] },
        lifecycle: { on_timeout: 'kill', auto_resume: false },
        volumeMounts: [{ name: volumeId, path: '/workspace' }],
      }),
    })
    const instance = parseJson(await boundedBody(response, 256 * 1024)) as CubeInstance
    if (typeof instance.sandboxID !== 'string' || typeof instance.trafficAccessToken !== 'string') {
      throw new Error('Cube did not return a private-ingress sandbox identity')
    }
    return {
      provider: 'cubesandbox-kvm',
      sandboxId: instance.sandboxID,
      endpoint: `${this.options.proxyScheme}://${EXECUTION_PORT}-${instance.sandboxID}.${instance.domain ?? this.options.sandboxDomain}`,
      trafficToken: instance.trafficAccessToken,
      volumeId,
    }
  }

  async bind(handle: SandboxHandle, binding: SandboxBinding, previous?: SandboxBinding): Promise<void> {
    const path = previous === undefined ? '/v1/bind' : '/v1/rebind'
    const response = await this.data(handle, path, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        ...(previous === undefined ? {} : {
          'x-dsh-execution-secret': previous.secret,
          'x-dsh-activation-id': previous.activationId,
        }),
      },
      body: JSON.stringify({ activationId: binding.activationId, secret: binding.secret, writerFence: binding.writerFence }),
    })
    await response.body?.cancel().catch(() => undefined)
  }

  async execute(handle: SandboxHandle, binding: SandboxBinding, request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResponse> {
    const response = await this.data(handle, '/v1/execute', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-dsh-execution-secret': binding.secret,
        'x-dsh-activation-id': binding.activationId,
        'x-dsh-writer-fence': String(binding.writerFence),
      },
      body: JSON.stringify(request),
      ...(signal === undefined ? {} : { signal }),
    })
    return parseExecutionResponse(parseJson(await boundedBody(response)), request.operationId)
  }

  async destroy(handle: SandboxHandle): Promise<void> {
    const response = await this.control(`/sandboxes/${encodeURIComponent(handle.sandboxId)}`, { method: 'DELETE' }, true)
    await response.body?.cancel().catch(() => undefined)
  }

  async inspect(handle: SandboxHandle): Promise<'ready' | 'absent' | 'failed'> {
    const response = await this.control(`/sandboxes/${encodeURIComponent(handle.sandboxId)}`, {}, true)
    if (response.status === 404) return 'absent'
    const instance = parseJson(await boundedBody(response, 256 * 1024)) as CubeInstance
    return instance.state === 'running' || instance.state === 'ready' ? 'ready' : 'failed'
  }

  async listManaged(): Promise<readonly ManagedSandbox[]> {
    const response = await this.control('/v2/sandboxes?limit=1000')
    const value = parseJson(await boundedBody(response, 4 * 1024 * 1024))
    const instances = Array.isArray(value)
      ? value
      : typeof value === 'object' && value !== null && Array.isArray((value as { sandboxes?: unknown }).sandboxes)
        ? (value as { sandboxes: unknown[] }).sandboxes
        : undefined
    if (instances === undefined || instances.length > 1_000) throw new Error('Cube inventory response was invalid')
    const managed: ManagedSandbox[] = []
    for (const candidate of instances) {
      if (typeof candidate !== 'object' || candidate === null) throw new Error('Cube inventory entry was invalid')
      const instance = candidate as CubeInstance
      const metadata = instance.metadata
      if (
        typeof instance.sandboxID !== 'string' ||
        metadata?.['dsh-cloud.managed'] !== 'true' ||
        metadata['dsh-cloud.namespace'] !== this.options.namespace
      ) continue
      const activationId = metadata['dsh-cloud.activation']
      if (typeof activationId !== 'string') throw new Error('Managed Cube inventory entry lacked an activation identity')
      managed.push({
        activationId,
        handle: {
          provider: 'cubesandbox-kvm',
          sandboxId: instance.sandboxID,
          endpoint: '',
        },
      })
    }
    return managed
  }

  async destroyWorkspace(input: Readonly<{ tenantId: string; workspaceId: string }>): Promise<void> {
    const path = `/volumes/${encodeURIComponent(this.volumeId(input.tenantId, input.workspaceId))}`
    const existing = await this.control(path, {}, true)
    await existing.body?.cancel().catch(() => undefined)
    if (existing.status === 404) return
    const response = await this.control(path, { method: 'DELETE' }, true)
    await response.body?.cancel().catch(() => undefined)
  }

  private volumeId(tenantId: string, workspaceId: string): string {
    return `dsh-${createHash('sha256').update(`${tenantId}\0${workspaceId}`).digest('hex').slice(0, 48)}`
  }

  private async ensureVolume(volumeId: string): Promise<void> {
    const encoded = encodeURIComponent(volumeId)
    const existing = await this.control(`/volumes/${encoded}`, {}, true)
    if (existing.status !== 404) { await existing.body?.cancel().catch(() => undefined); return }
    await existing.body?.cancel().catch(() => undefined)
    try {
      const created = await this.control('/volumes', { method: 'POST', body: JSON.stringify({ name: volumeId, driver: this.options.volumeDriver ?? 'dsh-cloud-posix' }) })
      await created.body?.cancel().catch(() => undefined)
    } catch (error: unknown) {
      const raced = await this.control(`/volumes/${encoded}`, {}, true)
      if (raced.status === 404) { await raced.body?.cancel().catch(() => undefined); throw error }
      await raced.body?.cancel().catch(() => undefined)
    }
  }

  private async control(path: string, init: { method?: 'GET' | 'POST' | 'DELETE'; body?: string } = {}, allowNotFound = false): Promise<Awaited<ReturnType<typeof fetch>>> {
    const response = await this.controlRaw(path, init)
    if (!response.ok && !(allowNotFound && response.status === 404)) {
      await response.body?.cancel().catch(() => undefined)
      throw new Error(`Cube API request failed with HTTP ${response.status}`)
    }
    return response
  }

  private controlRaw(path: string, init: { method?: 'GET' | 'POST' | 'DELETE'; body?: string } = {}): Promise<Awaited<ReturnType<typeof fetch>>> {
    return fetch(`${this.apiUrl}${path}`, {
      method: init.method ?? 'GET',
      headers: {
        authorization: `Bearer ${this.options.apiKey}`,
        ...(init.body === undefined ? {} : { 'content-type': 'application/json' }),
      },
      ...(init.body === undefined ? {} : { body: init.body }),
      signal: AbortSignal.timeout(60_000),
    })
  }

  private async data(handle: SandboxHandle, path: string, init: {
    method: 'POST'
    headers: Record<string, string>
    body: string
    signal?: AbortSignal
  }): Promise<Awaited<ReturnType<typeof fetch>>> {
    if (handle.trafficToken === undefined) throw new Error('Cube traffic token is unavailable')
    const response = await fetch(`${handle.endpoint}${path}`, {
      method: init.method,
      body: init.body,
      headers: {
        ...init.headers,
        'cube-traffic-access-token': handle.trafficToken,
        'e2b-traffic-access-token': handle.trafficToken,
      },
      dispatcher: this.dispatcher,
      signal: init.signal ?? AbortSignal.timeout(300_000),
    })
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined)
      const error = new Error(`Cube execution agent request failed with HTTP ${response.status}`) as NodeJS.ErrnoException
      if ([404, 502, 503, 504].includes(response.status)) error.code = 'SANDBOX_UNAVAILABLE'
      throw error
    }
    return response
  }
}

export default CubeSandboxProvider
