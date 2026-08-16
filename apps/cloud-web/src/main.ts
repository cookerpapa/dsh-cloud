import { spawn } from 'node:child_process'
import { createServer, type Server } from 'node:http'
import { lstat, mkdir, realpath, symlink, unlink } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import { Pool } from 'pg'
import { ControlStore } from '@dsh-cloud/control-store'
import { PostgresRunWorker } from '@dsh-cloud/run-queue'
import { DshHttpRunBackend } from './run-backend.js'
import { createWorkerControlRelay } from './worker-control.js'

const require = createRequire(import.meta.url)
const appDir = dirname(fileURLToPath(import.meta.url))
const repositoryRoot = resolve(appDir, '../../..')
const dshPackage = require.resolve('@deepseek-ai/dsh/package.json')
const dshEntry = join(dirname(dshPackage), 'lib', 'bin.js')
const cloudPatch = require.resolve('@dsh-cloud/web-bundle/cordis.patch.yml')
const dshHome = process.env['DSH_HOME'] ?? join(repositoryRoot, '.data', 'dsh-home')

/**
 * The DSH Loader resolves profile plugins from `$DSH_HOME/profiles/node_modules`.
 * Released DSH packages are healed there by DSH itself; this distribution owns
 * the out-of-tree Cloud plugins and publishes collision-safe links before
 * boot. Keeping this explicit makes an arbitrary external DSH_HOME work just
 * like the repository-local development home.
 */
async function exposeCloudPlugin(packageName: string): Promise<void> {
  const packageRoot = dirname(require.resolve(`${packageName}/package.json`))
  const link = join(dshHome, 'profiles', 'node_modules', ...packageName.split('/'))
  await mkdir(dirname(link), { recursive: true, mode: 0o700 })
  const assertTarget = async (): Promise<void> => {
    const [existing, expected] = await Promise.all([realpath(link), realpath(packageRoot)])
    if (existing !== expected) {
      throw new Error(`profile module ${packageName} already resolves to ${existing}, expected ${expected}`)
    }
  }
  try {
    await assertTarget()
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    try {
      await symlink(packageRoot, link, process.platform === 'win32' ? 'junction' : 'dir')
    } catch (linkError: unknown) {
      if ((linkError as NodeJS.ErrnoException).code !== 'EEXIST') throw linkError
      const existing = await lstat(link)
      if (!existing.isSymbolicLink()) {
        await assertTarget()
        return
      }
      await unlink(link)
      await symlink(packageRoot, link, process.platform === 'win32' ? 'junction' : 'dir')
    }
    await assertTarget()
  }
}

if (process.env['DSH_CLOUD_DATABASE_URL']?.trim() === '') {
  throw new Error('DSH_CLOUD_DATABASE_URL must not be empty')
}
if (process.env['DSH_CLOUD_DATABASE_URL'] === undefined) {
  throw new Error('DSH_CLOUD_DATABASE_URL is required')
}

await mkdir(dshHome, { recursive: true, mode: 0o700 })
await exposeCloudPlugin('@dsh-cloud/run-context')
await exposeCloudPlugin('@dsh-cloud/multi-agent-policy')
await exposeCloudPlugin('@dsh-cloud/agent-residency')
await exposeCloudPlugin('@dsh-cloud/run-admission')
await exposeCloudPlugin('@dsh-cloud/execution-client')
await exposeCloudPlugin('@dsh-cloud/subprocess-remote')
await exposeCloudPlugin('@dsh-cloud/fs-remote')
await exposeCloudPlugin('@dsh-cloud/sandbox-remote')
await exposeCloudPlugin('@dsh-cloud/session-live-kafka')
await exposeCloudPlugin('@dsh-cloud/session-live-valkey')
await exposeCloudPlugin('@dsh-cloud/session-persistence-tiered')

const host = process.env['DSH_CLOUD_HOST'] ?? '127.0.0.1'
const port = process.env['DSH_CLOUD_PORT'] ?? '3080'
const child = spawn(process.execPath, [
  dshEntry,
  '--profile', 'web',
  '--patch', cloudPatch,
  '--host', host,
  '--port', port,
  ...process.argv.slice(2),
], {
  cwd: repositoryRoot,
  env: { ...process.env, DSH_HOME: dshHome },
  stdio: 'inherit',
})

let runWorker: PostgresRunWorker | undefined
let controlPool: Pool | undefined
let stopping = false
let metricsServer: Server | undefined
let controlRelay: import('node:net').Server | undefined

async function waitForHost(baseUrl: string): Promise<void> {
  const deadline = Date.now() + 60_000
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error('DSH Host exited before becoming ready')
    try {
      const response = await fetch(baseUrl, { signal: AbortSignal.timeout(1000) })
      await response.body?.cancel()
      if (response.ok) return
    } catch { /* startup probe */ }
    await delay(100)
  }
  throw new Error('DSH Host did not become ready within 60 seconds')
}

async function startRunWorker(): Promise<void> {
  if (process.env['DSH_CLOUD_WORKER_ENABLED'] === '0') return
  const databaseUrl = process.env['DSH_CLOUD_DATABASE_URL']
  if (databaseUrl === undefined) throw new Error('DSH_CLOUD_DATABASE_URL is required')
  const localUrl = `http://${host === '0.0.0.0' || host === '::' ? '127.0.0.1' : host}:${port}`
  await waitForHost(localUrl)
  if (stopping) return
  const controlPort = process.env['DSH_CLOUD_WORKER_CONTROL_PORT']
  if (controlPort !== undefined) {
    controlRelay = await createWorkerControlRelay({
      listenHost: process.env['DSH_CLOUD_WORKER_CONTROL_HOST'] ?? '127.0.0.1',
      listenPort: Number(controlPort),
      targetHost: host,
      targetPort: Number(port),
    })
  }
  controlPool = new Pool({ connectionString: databaseUrl, max: 10, application_name: 'dsh-cloud-worker-control' })
  const store = new ControlStore(controlPool, process.env['DSH_CLOUD_NAMESPACE'] ?? 'default')
  await store.initialize()
  runWorker = new PostgresRunWorker({
    store,
    notificationConnectionString: databaseUrl,
    identity: process.env['DSH_CLOUD_WORKER_ID'] ?? `worker-${process.pid}`,
    baseUrl: process.env['DSH_CLOUD_WORKER_URL'] ?? (process.env['DSH_CLOUD_POD_IP'] === undefined
      ? `http://127.0.0.1:${controlPort ?? port}`
      : `http://${process.env['DSH_CLOUD_POD_IP']}:${controlPort ?? port}`),
    maximumConcurrentRuns: Number(process.env['DSH_CLOUD_WORKER_SLOTS'] ?? '4'),
    attemptLeaseSeconds: Number(process.env['DSH_CLOUD_RUN_LEASE_SECONDS'] ?? '20'),
    backend: new DshHttpRunBackend(localUrl),
    onError: error => console.error('DSH Cloud Worker:', error instanceof Error ? error.message : String(error)),
  })
  await runWorker.start()
  metricsServer = createServer((_request, response) => {
    const status = runWorker?.status() ?? { activeRuns: 0, maximumRuns: 0, draining: true }
    const output = [
      '# TYPE dsh_cloud_worker_active_runs gauge',
      `dsh_cloud_worker_active_runs ${status.activeRuns}`,
      '# TYPE dsh_cloud_worker_slots gauge',
      `dsh_cloud_worker_slots ${status.maximumRuns}`,
      '# TYPE dsh_cloud_worker_draining gauge',
      `dsh_cloud_worker_draining ${status.draining ? 1 : 0}`,
      '# TYPE dsh_cloud_worker_process_resident_bytes gauge',
      `dsh_cloud_worker_process_resident_bytes ${process.memoryUsage().rss}`,
      '',
    ].join('\n')
    response.writeHead(200, { 'content-type': 'text/plain; version=0.0.4', 'content-length': String(Buffer.byteLength(output)) })
    response.end(output)
  })
  metricsServer.listen(Number(process.env['DSH_CLOUD_WORKER_METRICS_PORT'] ?? '9090'), '0.0.0.0')
}

void startRunWorker().catch(error => {
  console.error('failed to start DSH Cloud Worker:', error instanceof Error ? error.message : String(error))
  child.kill('SIGTERM')
})

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    stopping = true
    const drainMs = Number(process.env['DSH_CLOUD_WORKER_DRAIN_TIMEOUT_MS'] ?? '540000')
    void Promise.resolve(runWorker?.drain(drainMs)).finally(() => {
      metricsServer?.close()
      controlRelay?.close()
      child.kill(signal)
    }).finally(() => controlPool?.end()).finally(() => {
      process.exitCode = signal === 'SIGINT' ? 130 : 143
      setImmediate(() => process.exit(process.exitCode))
    })
  })
}

child.once('error', (error) => {
  console.error('failed to start DSH Cloud Web:', error.message)
  process.exitCode = 1
})

child.once('exit', (code, signal) => {
  process.exitCode = code ?? (signal === null ? 1 : 128)
  if (!stopping) {
    stopping = true
    void Promise.resolve(runWorker?.stop()).finally(() => {
      metricsServer?.close()
      controlRelay?.close()
      return controlPool?.end()
    })
  }
})
