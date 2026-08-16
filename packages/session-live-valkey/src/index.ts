import { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import { Valkey } from 'iovalkey'
import { SessionLiveProjection, type SessionEventEnvelope } from '@dsh-cloud/session-live'

export interface Config {
  url: string
  retentionSeconds?: number
}

function integer(value: number, name: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} is invalid`)
  }
  return value
}

const PROJECT_SCRIPT = String.raw`
local stream = KEYS[1]
local watermark = KEYS[2]
local digests = KEYS[3]
local first = tonumber(ARGV[1])
local last = tonumber(ARGV[2])
local digest = ARGV[3]
local payload = ARGV[4]
local ttl = tonumber(ARGV[5])
local field = tostring(first) .. ':' .. tostring(last)
local stored = redis.call('GET', watermark)
local current = stored and tonumber(stored) or first - 1
if current >= last then
  local prior = redis.call('HGET', digests, field)
  if prior == digest then return current end
  return redis.error_reply('dsh_projection_conflicting_replay')
end
if current + 1 ~= first then return redis.error_reply('dsh_projection_sequence_gap') end
redis.call('XADD', stream, tostring(last + 1) .. '-0', 'sha256', digest, 'event', payload)
redis.call('HSET', digests, field, digest)
redis.call('SET', watermark, tostring(last), 'EX', ttl)
redis.call('EXPIRE', stream, ttl)
redis.call('EXPIRE', digests, ttl)
return last
`

/** Valkey Provider for the rebuildable Session live-projection capability. */
class ValkeySessionLiveProjection extends SessionLiveProjection {
  static Config: z<Config> = z.object({
    url: z.string().required(),
    retentionSeconds: z.number().step(1).min(60).max(2_592_000).default(86_400),
  })

  private readonly client: Valkey
  private readonly retentionSeconds: number

  constructor(ctx: Context, config: Config) {
    super(ctx)
    const endpoint = new URL(config.url)
    if (!['redis:', 'rediss:'].includes(endpoint.protocol)) throw new TypeError('Valkey URL is invalid')
    this.retentionSeconds = integer(config.retentionSeconds ?? 86_400, 'Valkey retention', 60, 2_592_000)
    this.client = new Valkey(endpoint.href, {
      lazyConnect: true,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 2,
      connectTimeout: 5_000,
      commandTimeout: 10_000,
    })
    ctx.effect(() => () => {
      if (this.client.status !== 'end') this.client.disconnect()
    }, 'Valkey Session live-projection teardown')
  }

  async watermark(namespace: string, sessionId: string): Promise<number | undefined> {
    await this.connect()
    const value = await this.client.get(this.keys(namespace, sessionId)[1])
    if (value === null) return undefined
    const parsed = Number(value)
    if (!Number.isSafeInteger(parsed) || parsed < -1) throw new Error('Valkey Session projection watermark is invalid')
    return parsed
  }

  async reset(namespace: string, sessionId: string, through: number): Promise<void> {
    integer(through, 'projection reset watermark', -1, Number.MAX_SAFE_INTEGER)
    await this.connect()
    const keys = this.keys(namespace, sessionId)
    const transaction = this.client.multi().del(...keys)
    if (through >= 0) transaction.set(keys[1], String(through), 'EX', this.retentionSeconds)
    await transaction.exec()
  }

  async append(envelope: SessionEventEnvelope, digest: string): Promise<void> {
    await this.connect()
    await this.client.eval(
      PROJECT_SCRIPT,
      3,
      ...this.keys(envelope.namespace, envelope.sessionId),
      String(envelope.seq),
      String(envelope.seqEnd),
      digest,
      JSON.stringify(envelope),
      String(this.retentionSeconds),
    )
  }

  async checkHealth(): Promise<void> {
    await this.connect()
    await this.client.ping()
  }

  private keys(namespace: string, sessionId: string): [string, string, string] {
    const identity = Buffer.from(`${namespace}\0${sessionId}`).toString('base64url')
    const prefix = `dsh-cloud:events:{${identity}}`
    return [`${prefix}:stream`, `${prefix}:watermark`, `${prefix}:digests`]
  }

  private async connect(): Promise<void> {
    if (this.client.status === 'wait') await this.client.connect()
  }
}

export default ValkeySessionLiveProjection
