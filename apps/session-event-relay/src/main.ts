import { KafkaJS } from '@confluentinc/kafka-javascript'
import { createServer } from 'node:http'
import { Pool } from 'pg'
import {
  KafkaSessionEventLog,
  SessionEventOutbox,
  SessionEventRelay,
  ValkeySessionEventProjection,
} from './index.js'

function required(name: string): string {
  const value = process.env[name]?.trim()
  if (!value) throw new Error(`${name} is required`)
  return value
}

const databaseUrl = required('DSH_CLOUD_DATABASE_URL')
const deploymentNamespace = process.env['DSH_CLOUD_NAMESPACE']?.trim() || 'default'
const brokers = required('DSH_CLOUD_KAFKA_BROKERS').split(',').map(value => value.trim()).filter(Boolean)
const topic = process.env['DSH_CLOUD_KAFKA_SESSION_TOPIC']?.trim() || 'dsh-cloud-session-events-v1'
const retentionMs = Number(process.env['DSH_CLOUD_KAFKA_EVENT_RETENTION_MS'] ?? 172_800_000)
if (!Number.isSafeInteger(retentionMs) || retentionMs < 60_000) {
  throw new Error('DSH_CLOUD_KAFKA_EVENT_RETENTION_MS must be an integer of at least 60000')
}
const pool = new Pool({ connectionString: databaseUrl, max: Number(process.env['DSH_CLOUD_RELAY_PG_POOL'] ?? 10) })

const admin = new KafkaJS.Kafka({
  'bootstrap.servers': brokers.join(','),
  'client.id': 'dsh-cloud-session-relay-admin',
}).admin()
await admin.connect()
await admin.createTopics({
  topics: [{
    topic,
    numPartitions: Number(process.env['DSH_CLOUD_KAFKA_PARTITIONS'] ?? 12),
    replicationFactor: Number(process.env['DSH_CLOUD_KAFKA_REPLICATION_FACTOR'] ?? 1),
    configEntries: [
      { name: 'cleanup.policy', value: 'delete' },
      { name: 'retention.ms', value: String(retentionMs) },
    ],
  }],
})
await admin.disconnect()

const relay = new SessionEventRelay(
  new SessionEventOutbox(
    pool,
    deploymentNamespace,
    process.env['HOSTNAME'] || undefined,
    Number(process.env['DSH_CLOUD_RELAY_LEASE_SECONDS'] ?? 60),
  ),
  new KafkaSessionEventLog({ brokers, topic, clientId: process.env['HOSTNAME'] || 'dsh-cloud-session-relay' }),
  new ValkeySessionEventProjection({
    url: required('DSH_CLOUD_VALKEY_URL'),
    ttlSeconds: Number(process.env['DSH_CLOUD_LIVE_EVENT_RETENTION_SECONDS'] ?? 86_400),
  }),
  Number(process.env['DSH_CLOUD_RELAY_POLL_MS'] ?? 25),
)

const healthPort = Number(process.env['DSH_CLOUD_RELAY_HEALTH_PORT'] ?? 3091)
const healthServer = createServer(async (request, response) => {
  if (request.url === '/health/live') {
    response.writeHead(200).end('ok')
    return
  }
  if (request.url === '/health/ready') {
    try {
      await Promise.all([pool.query('SELECT 1'), relay.checkHealth()])
      response.writeHead(200).end('ready')
    } catch {
      response.writeHead(503).end('unavailable')
    }
    return
  }
  response.writeHead(404).end('not found')
})
await new Promise<void>((resolve, reject) => {
  healthServer.once('error', reject)
  healthServer.listen(healthPort, '0.0.0.0', resolve)
})

const abort = new AbortController()
for (const signal of ['SIGINT', 'SIGTERM'] as const) process.once(signal, () => abort.abort())
try {
  await relay.run(abort.signal)
} finally {
  await new Promise<void>(resolve => healthServer.close(() => resolve()))
  await relay.close()
  await pool.end()
}
