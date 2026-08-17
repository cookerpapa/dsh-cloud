import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import kafkaPackage, { type Message } from '@confluentinc/kafka-javascript'
import {
  SessionLiveLog,
  parseSessionEventEnvelope,
  sessionEventEnvelopeDigest,
  type SessionEventEnvelope,
  type SessionLiveLocation,
  type SessionLiveLogProvisioning,
} from '@dsh-cloud/session-live'

const { KafkaConsumer, KafkaJS } = kafkaPackage
const { CompressionTypes, Kafka, logLevel } = KafkaJS

export interface Config {
  brokers: string
  topic?: string
  clientId?: string
}

function bounded(value: string, name: string, maximum = 512): string {
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

function kafkaOffset(value: string | undefined): string {
  if (value === undefined || !/^\d+$/.test(value)) throw new Error('Kafka did not return a durable record offset')
  return value
}

/** Kafka Provider for the provider-neutral durable Session live-log capability. */
class KafkaSessionLiveLog extends SessionLiveLog {
  static Config: z<Config> = z.object({
    brokers: z.string().required(),
    topic: z.string().default('dsh-cloud-session-events-v2'),
    clientId: z.string().default('dsh-cloud-session-live'),
  })

  private readonly producer
  private readonly brokers: string[]
  private readonly topic: string
  private connected: Promise<void> | undefined

  constructor(ctx: Context, config: Config) {
    super(ctx)
    this.brokers = config.brokers.split(',').map(value => bounded(value.trim(), 'Kafka broker')).filter(Boolean)
    if (this.brokers.length < 1 || this.brokers.length > 64) throw new TypeError('Kafka brokers are invalid')
    this.topic = bounded(config.topic ?? 'dsh-cloud-session-events-v2', 'Kafka topic', 249)
    const kafka = new Kafka({
      'bootstrap.servers': this.brokers.join(','),
      'client.id': bounded(`${config.clientId ?? 'dsh-cloud-session-live'}-${randomUUID()}`, 'Kafka client id', 249),
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
    ctx.effect(() => async () => {
      if (this.connected === undefined) return
      await this.connected
      await this.producer.disconnect()
    }, 'Kafka Session live-log teardown')
  }

  async publish(envelope: SessionEventEnvelope): Promise<SessionLiveLocation> {
    this.connected ??= this.producer.connect()
    await this.connected
    const digest = sessionEventEnvelopeDigest(envelope)
    const metadata = await this.producer.send({
      topic: this.topic,
      messages: [{
        key: envelope.sessionId,
        value: JSON.stringify(envelope),
        headers: {
          'dsh-cloud-schema': 'native-session-events-v2',
          'dsh-cloud-digest': digest,
        },
      }],
    })
    const stored = metadata[0]
    if (stored === undefined || stored.errorCode !== 0) throw new Error('Kafka did not acknowledge the Session event batch')
    return {
      seq: envelope.seq,
      seqEnd: envelope.seqEnd,
      locator: {
        partition: integer(stored.partition, 'Kafka partition', 0, 1_000_000),
        offset: kafkaOffset(stored.offset ?? stored.baseOffset),
      },
      digest,
    }
  }

  async provision(options: SessionLiveLogProvisioning): Promise<void> {
    integer(options.partitions, 'Kafka partition count', 1, 10_000)
    integer(options.replicationFactor, 'Kafka replication factor', 1, 9)
    integer(options.retentionMs, 'Kafka retention', 60_000, Number.MAX_SAFE_INTEGER)
    const admin = new Kafka({
      'bootstrap.servers': this.brokers.join(','),
      'client.id': `dsh-cloud-session-topic-${randomUUID()}`,
    }).admin()
    await admin.connect()
    try {
      await admin.createTopics({
        topics: [{
          topic: this.topic,
          numPartitions: options.partitions,
          replicationFactor: options.replicationFactor,
          configEntries: [
            { name: 'cleanup.policy', value: 'delete' },
            { name: 'retention.ms', value: String(options.retentionMs) },
          ],
        }],
      })
    } finally {
      await admin.disconnect()
    }
  }

  async read(
    namespace: string,
    sessionId: string,
    locations: readonly SessionLiveLocation[],
    signal?: AbortSignal,
  ): Promise<SessionEventEnvelope[]> {
    if (locations.length === 0) return []
    signal?.throwIfAborted()
    const consumer = new KafkaConsumer({
      'metadata.broker.list': this.brokers.join(','),
      'group.id': `dsh-cloud-tail-reader-${randomUUID()}`,
      'enable.auto.commit': false,
      'enable.partition.eof': false,
      'socket.timeout.ms': 10_000,
    }, { 'auto.offset.reset': 'error' })
    await new Promise<void>((resolve, reject) => {
      consumer.once('ready', () => resolve())
      consumer.once('event.error', (cause: unknown) => reject(cause))
      consumer.connect()
    })
    const expected = locations.map(location => {
      const partition = location.locator['partition']
      const offset = location.locator['offset']
      const numericOffset = typeof offset === 'string' && /^\d+$/.test(offset)
        ? Number(offset)
        : Number.NaN
      if (!Number.isSafeInteger(partition) || (partition as number) < 0
        || !Number.isSafeInteger(numericOffset) || numericOffset < 0) {
        throw new Error('Kafka Session location is invalid')
      }
      return {
        location,
        partition: partition as number,
        offset: String(offset),
        numericOffset,
      }
    })
    const envelopes: SessionEventEnvelope[] = []
    try {
      let start = 0
      while (start < expected.length) {
        signal?.throwIfAborted()
        const first = expected[start]!
        let end = start + 1
        let previousOffset = first.numericOffset
        while (end < expected.length && expected[end]!.partition === first.partition) {
          const nextOffset = expected[end]!.numericOffset
          if (nextOffset <= previousOffset) {
            throw new Error('Kafka Session locations are not strictly ordered')
          }
          previousOffset = nextOffset
          end += 1
        }
        // All records for one Session normally hash to one Kafka partition.
        // Seek once and scan forward, skipping interleaved records belonging to
        // other Sessions. Reassigning before every locator adds a large fixed
        // librdkafka cost and used to make Turn sealing linear in batch count.
        consumer.assign([{
          topic: this.topic,
          partition: first.partition,
          offset: first.numericOffset,
        }])
        let index = start
        while (index < end) {
          signal?.throwIfAborted()
          const target = expected[index]!
          const messages = await new Promise<Message[]>((resolve, reject) => {
            consumer.consume(1, (error, value) => error ? reject(error) : resolve(value))
          })
          const message = messages[0]
          if (message === undefined || message.value === null) {
            throw new Error(
              `Kafka active Session tail is missing offset ${target.partition}:${target.offset}`,
            )
          }
          if (message.partition !== target.partition) {
            throw new Error('Kafka returned a Session record from an unexpected partition')
          }
          const currentOffset = String(message.offset)
          const current = BigInt(currentOffset)
          const wanted = BigInt(target.offset)
          if (current < wanted) continue
          if (current > wanted) {
            throw new Error(
              `Kafka active Session tail is missing offset ${target.partition}:${target.offset}`,
            )
          }
          const envelope = parseSessionEventEnvelope(JSON.parse(message.value.toString()) as unknown)
          const location = target.location
          if (envelope.namespace !== namespace
            || envelope.sessionId !== sessionId
            || envelope.seq !== location.seq
            || envelope.seqEnd !== location.seqEnd
            || sessionEventEnvelopeDigest(envelope) !== location.digest) {
            throw new Error(`Kafka active Session tail failed identity or digest validation at seq ${location.seq}`)
          }
          envelopes.push(envelope)
          index += 1
        }
        start = end
      }
    } finally {
      await new Promise<void>(resolve => consumer.disconnect(() => resolve()))
    }
    return envelopes
  }

  async checkHealth(): Promise<void> {
    this.connected ??= this.producer.connect()
    await this.connected
  }
}

export default KafkaSessionLiveLog
