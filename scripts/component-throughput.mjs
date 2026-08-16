import { randomUUID } from 'node:crypto'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const kafkaRecords = integer('DSH_CLOUD_BENCH_KAFKA_RECORDS', 100_000, 1_000, 10_000_000)
const postgresSeconds = integer('DSH_CLOUD_BENCH_POSTGRES_SECONDS', 30, 5, 600)
const postgresClients = integer('DSH_CLOUD_BENCH_POSTGRES_CLIENTS', 16, 1, 256)
const suffix = randomUUID().replaceAll('-', '').slice(0, 12)
const topic = `dsh-cloud-bench-${suffix}`
const database = `dsh_cloud_bench_${suffix}`
const compose = ['compose', '-f', 'deploy/dev/compose.yaml']
const repository = fileURLToPath(new URL('..', import.meta.url))

function integer(name, fallback, minimum, maximum) {
  const value = Number(process.env[name] ?? fallback)
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new Error(`${name} must be an integer from ${minimum} to ${maximum}`)
  return value
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: repository, stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', chunk => { stdout += chunk })
    child.stderr.on('data', chunk => { stderr += chunk })
    child.once('error', reject)
    child.once('close', code => code === 0
      ? resolve({ stdout, stderr })
      : reject(new Error(`${command} ${args.join(' ')} failed with ${code}: ${stderr || stdout}`)))
  })
}

async function dockerExec(service, ...args) {
  return run('docker', [...compose, 'exec', '-T', service, ...args])
}

const report = { kafka: {}, postgres: {} }
try {
  await run('docker', [...compose, 'up', '-d', '--wait', 'postgres', 'kafka'])
  await dockerExec('kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--create', '--topic', topic, '--partitions', '12', '--replication-factor', '1')
  const kafka = await dockerExec(
    'kafka', '/opt/kafka/bin/kafka-producer-perf-test.sh',
    '--topic', topic,
    '--num-records', String(kafkaRecords),
    '--record-size', '1024',
    '--throughput', '-1',
    '--producer-props', 'bootstrap.servers=kafka:9092', 'acks=all', 'enable.idempotence=true', 'compression.type=gzip',
  )
  report.kafka = { records: kafkaRecords, output: kafka.stdout.trim() }

  await dockerExec('postgres', 'createdb', '-U', 'dsh_cloud', database)
  await dockerExec('postgres', 'pgbench', '-U', 'dsh_cloud', '-i', '-s', '10', database)
  const postgres = await dockerExec(
    'postgres', 'pgbench', '-U', 'dsh_cloud',
    '-c', String(postgresClients),
    '-j', String(Math.min(postgresClients, 8)),
    '-T', String(postgresSeconds),
    database,
  )
  report.postgres = { clients: postgresClients, durationSeconds: postgresSeconds, output: postgres.stdout.trim() }
} finally {
  await dockerExec('kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--delete', '--topic', topic).catch(() => undefined)
  await dockerExec('postgres', 'dropdb', '-U', 'dsh_cloud', '--if-exists', database).catch(() => undefined)
}

process.stdout.write(`${JSON.stringify(report, null, 2)}\n`)
