import { randomUUID } from 'node:crypto'
import { KafkaJS } from '@confluentinc/kafka-javascript'
import {
  parseSessionEventEnvelope,
  sessionEventEnvelopeDigest,
  type SessionEventEnvelope,
} from '@dsh-cloud/session-persistence-postgres'
import { Valkey } from 'iovalkey'
import { Pool, type PoolClient } from 'pg'

const { CompressionTypes, Kafka, logLevel } = KafkaJS

interface OutboxRow {
  id: string
  payload: unknown
  digest: string
}

export interface ClaimedEnvelope {
  readonly id: string
  readonly envelope: SessionEventEnvelope
  readonly digest: string
}

export interface EventLogPublisher {
  publish(batches: readonly ClaimedEnvelope[]): Promise<void>
  checkHealth(): Promise<void>
  close(): Promise<void>
}

export interface LiveEventProjection {
  append(envelope: SessionEventEnvelope, digest: string): Promise<void>
  checkHealth(): Promise<void>
  close(): Promise<void>
}

function bounded(value: string, name: string, maximum = 249): string {
  if (value.length < 1 || value.length > maximum || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new TypeError(`${name} is invalid`)
  }
  return value
}

function integer(value: number, name: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} is invalid`)
  }
  return value
}

function namespace(value: string): string {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(value)) throw new TypeError('namespace is invalid')
  return value
}

export class SessionEventOutbox {
  constructor(
    private readonly pool: Pool,
    private readonly deploymentNamespace: string,
    private readonly owner: string = randomUUID(),
    private readonly leaseSeconds = 60,
  ) {
    namespace(deploymentNamespace)
    integer(leaseSeconds, 'leaseSeconds', 1, 300)
  }

  async claim(limit = 32): Promise<ClaimedEnvelope[]> {
    integer(limit, 'claim limit', 1, 512)
    const rows = await this.transaction(async client => client.query<OutboxRow>(`
      WITH candidates AS (
        SELECT item.id
        FROM dsh_cloud.session_event_outbox item
        JOIN dsh_cloud.sessions session
          ON session.namespace=item.namespace AND session.id=item.session_id
        WHERE item.namespace=$1
          AND item.available_at<=now()
          AND item.seq<=session.projected_through+1
          AND (item.lease_owner IS NULL OR item.lease_expires_at<now())
          AND NOT EXISTS (
            SELECT 1 FROM dsh_cloud.session_event_outbox prior
            WHERE prior.namespace=item.namespace AND prior.session_id=item.session_id
              AND prior.seq<item.seq
          )
        ORDER BY item.created_at,item.id
        FOR UPDATE SKIP LOCKED
        LIMIT $2
      )
      UPDATE dsh_cloud.session_event_outbox item
      SET lease_owner=$3,lease_expires_at=now()+make_interval(secs=>$4),attempts=attempts+1,last_error=NULL
      FROM candidates
      WHERE item.id=candidates.id
      RETURNING item.id::text,item.payload,item.digest
    `, [this.deploymentNamespace, limit, this.owner, this.leaseSeconds]))
    return rows.rows.map(row => {
      const envelope = parseSessionEventEnvelope(row.payload)
      if (envelope.namespace !== this.deploymentNamespace
        || sessionEventEnvelopeDigest(envelope) !== row.digest) {
        throw new Error(`outbox envelope ${row.id} failed identity or digest validation`)
      }
      return { id: row.id, envelope, digest: row.digest }
    })
  }

  async failed(batch: ClaimedEnvelope, error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error)
    await this.pool.query(`
      UPDATE dsh_cloud.session_event_outbox
      SET lease_owner=NULL,lease_expires_at=NULL,
          available_at=now()+make_interval(secs=>LEAST(30,power(2,LEAST(attempts,5)))::double precision),
          last_error=$4
      WHERE id=$1 AND namespace=$2 AND lease_owner=$3
    `, [batch.id, this.deploymentNamespace, this.owner, message.slice(0, 2_000)])
  }

  async project(batch: ClaimedEnvelope): Promise<void> {
    await this.transaction(async (client) => {
      const owned = await client.query(`
        SELECT 1 FROM dsh_cloud.session_event_outbox
        WHERE id=$1 AND namespace=$2 AND lease_owner=$3 AND digest=$4
        FOR UPDATE
      `, [batch.id, this.deploymentNamespace, this.owner, batch.digest])
      if (owned.rowCount !== 1) throw new Error(`outbox lease for ${batch.id} was lost before projection commit`)
      const current = await client.query<{ projected_through: string }>(`
        SELECT projected_through::text
        FROM dsh_cloud.sessions
        WHERE namespace=$1 AND id=$2
        FOR UPDATE
      `, [this.deploymentNamespace, batch.envelope.sessionId])
      const row = current.rows[0]
      if (row === undefined) throw new Error('projected Session no longer exists')
      const through = Number(row.projected_through)
      if (!Number.isSafeInteger(through) || through < -1) throw new Error('projected Session watermark is corrupt')
      if (through < batch.envelope.seq - 1) throw new Error('Session projection has a sequence gap')
      if (through < batch.envelope.seqEnd) {
        await client.query(`
          UPDATE dsh_cloud.sessions SET projected_through=$3
          WHERE namespace=$1 AND id=$2 AND projected_through=$4
        `, [this.deploymentNamespace, batch.envelope.sessionId, batch.envelope.seqEnd, through])
      }
      await client.query(`
        DELETE FROM dsh_cloud.session_event_outbox
        WHERE namespace=$1 AND session_id=$2 AND seq_end<=$3
      `, [this.deploymentNamespace, batch.envelope.sessionId, batch.envelope.seqEnd])
      await client.query(`SELECT pg_notify('dsh_cloud_session_projection',$1)`, [batch.envelope.sessionId])
    })
  }

  async backlog(): Promise<number> {
    const result = await this.pool.query<{ count: string }>(`
      SELECT count(*)::text AS count FROM dsh_cloud.session_event_outbox WHERE namespace=$1
    `, [this.deploymentNamespace])
    return Number(result.rows[0]?.count ?? 0)
  }

  private async transaction<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const result = await operation(client)
      await client.query('COMMIT')
      return result
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }
}

export class KafkaSessionEventLog implements EventLogPublisher {
  private readonly producer
  private readonly topic: string
  private connected: Promise<void> | undefined

  constructor(options: { brokers: readonly string[]; topic: string; clientId: string }) {
    if (options.brokers.length < 1 || options.brokers.length > 64) throw new TypeError('Kafka brokers are invalid')
    this.topic = bounded(options.topic, 'Kafka topic')
    const kafka = new Kafka({
      'bootstrap.servers': options.brokers.map(value => bounded(value, 'Kafka broker', 512)).join(','),
      'client.id': bounded(options.clientId, 'Kafka client id'),
    })
    this.producer = kafka.producer({
      'allow.auto.create.topics': false,
      'enable.idempotence': true,
      'max.in.flight.requests.per.connection': 5,
      'request.timeout.ms': 10_000,
      'delivery.timeout.ms': 30_000,
      acks: -1,
      'compression.codec': CompressionTypes.GZIP,
    })
    this.producer.logger().setLogLevel(logLevel.NOTHING)
  }

  async publish(batches: readonly ClaimedEnvelope[]): Promise<void> {
    if (batches.length === 0) return
    this.connected ??= this.producer.connect()
    await this.connected
    await this.producer.send({
      topic: this.topic,
      messages: batches.map(batch => ({
          key: batch.envelope.sessionId,
          value: JSON.stringify(batch.envelope),
          headers: {
            'dsh-cloud-schema': 'native-session-events-v1',
            'dsh-cloud-digest': batch.digest,
            'dsh-cloud-outbox-id': batch.id,
          },
        })),
    })
  }

  async checkHealth(): Promise<void> {
    this.connected ??= this.producer.connect()
    await this.connected
  }

  async close(): Promise<void> {
    if (this.connected === undefined) return
    await this.connected
    await this.producer.disconnect()
  }
}

const PROJECT_SCRIPT = String.raw`
local stream = KEYS[1]
local watermark = KEYS[2]
local first = tonumber(ARGV[1])
local last = tonumber(ARGV[2])
local digest = ARGV[3]
local payload = ARGV[4]
local ttl = tonumber(ARGV[5])
local stored = redis.call('GET', watermark)
local current = stored and tonumber(stored) or first - 1
if current >= last then return current end
if current + 1 ~= first then return redis.error_reply('dsh_projection_sequence_gap') end
redis.call('XADD', stream, tostring(last + 1) .. '-0', 'sha256', digest, 'event', payload)
redis.call('SET', watermark, tostring(last), 'EX', ttl)
redis.call('EXPIRE', stream, ttl)
return last
`

export class ValkeySessionEventProjection implements LiveEventProjection {
  private readonly client: Valkey
  private readonly ttlSeconds: number

  constructor(options: { url: string; ttlSeconds?: number }) {
    const endpoint = new URL(options.url)
    if (!['redis:', 'rediss:'].includes(endpoint.protocol)) throw new TypeError('Valkey URL is invalid')
    this.ttlSeconds = integer(options.ttlSeconds ?? 86_400, 'Valkey retention', 60, 2_592_000)
    this.client = new Valkey(endpoint.href, {
      lazyConnect: true,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 2,
      connectTimeout: 5_000,
      commandTimeout: 10_000,
    })
  }

  async append(envelope: SessionEventEnvelope, digest: string): Promise<void> {
    if (this.client.status === 'wait') await this.client.connect()
    const identity = Buffer.from(`${envelope.namespace}\0${envelope.sessionId}`).toString('base64url')
    await this.client.eval(
      PROJECT_SCRIPT,
      2,
      `dsh-cloud:events:{${identity}}:stream`,
      `dsh-cloud:events:{${identity}}:watermark`,
      String(envelope.seq),
      String(envelope.seqEnd),
      digest,
      JSON.stringify(envelope),
      String(this.ttlSeconds),
    )
  }

  async checkHealth(): Promise<void> {
    if (this.client.status === 'wait') await this.client.connect()
    await this.client.ping()
  }

  async close(): Promise<void> {
    if (this.client.status !== 'end') this.client.disconnect()
  }
}

export class SessionEventRelay {
  private stopped = false

  constructor(
    private readonly outbox: SessionEventOutbox,
    private readonly log: EventLogPublisher,
    private readonly projection: LiveEventProjection,
    private readonly pollMs = 25,
  ) {
    integer(pollMs, 'pollMs', 1, 10_000)
  }

  async run(signal?: AbortSignal): Promise<void> {
    while (!this.stopped && !signal?.aborted) {
      let count = 0
      try {
        count = await this.runOnce()
      } catch {
        if (!signal?.aborted) await new Promise(resolve => setTimeout(resolve, Math.max(100, this.pollMs)))
      }
      if (count === 0) await new Promise(resolve => setTimeout(resolve, this.pollMs))
    }
  }

  async checkHealth(): Promise<void> {
    await Promise.all([this.log.checkHealth(), this.projection.checkHealth()])
  }

  async runOnce(): Promise<number> {
    const claimed = await this.outbox.claim()
    try {
      await this.log.publish(claimed)
    } catch (error) {
      await Promise.all(claimed.map(batch => this.outbox.failed(batch, error).catch(() => undefined)))
      throw error
    }
    await Promise.all(claimed.map(async (batch) => {
      try {
        await this.projection.append(batch.envelope, batch.digest)
        await this.outbox.project(batch)
      } catch (error) {
        await this.outbox.failed(batch, error).catch(() => undefined)
        throw error
      }
    }))
    return claimed.length
  }

  async close(): Promise<void> {
    this.stopped = true
    await Promise.all([this.log.close(), this.projection.close()])
  }
}
