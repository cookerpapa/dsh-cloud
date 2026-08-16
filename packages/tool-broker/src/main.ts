import { readFile } from 'node:fs/promises'
import { Pool } from 'pg'
import CubeSandboxProvider from './cube-provider.js'
import { ToolBroker } from './index.js'

async function secret(name: string): Promise<string> {
  const file = process.env[`${name}_FILE`]
  const value = file === undefined ? process.env[name] : await readFile(file, 'utf8')
  if (value === undefined || value.trim().length === 0) throw new Error(`${name} is required`)
  return value.trim()
}

const pool = new Pool({ connectionString: await secret('DSH_CLOUD_DATABASE_URL'), max: 20 })
const namespace = process.env['DSH_CLOUD_NAMESPACE'] ?? 'default'
const provider = new CubeSandboxProvider({
  namespace,
  apiUrl: await secret('DSH_CLOUD_CUBE_API_URL'),
  apiKey: await secret('DSH_CLOUD_CUBE_API_KEY'),
  templateId: await secret('DSH_CLOUD_CUBE_TEMPLATE_ID'),
  proxyNodeIp: await secret('DSH_CLOUD_CUBE_PROXY_NODE_IP'),
  proxyPort: Number(process.env['DSH_CLOUD_CUBE_PROXY_PORT'] ?? '8080'),
  proxyScheme: process.env['DSH_CLOUD_CUBE_PROXY_SCHEME'] === 'https' ? 'https' : 'http',
  sandboxDomain: await secret('DSH_CLOUD_CUBE_DOMAIN'),
  egressProxyIp: await secret('DSH_CLOUD_CUBE_EGRESS_PROXY_IP'),
  volumeDriver: process.env['DSH_CLOUD_CUBE_VOLUME_DRIVER'] ?? 'dsh-cloud-posix',
})
const broker = new ToolBroker({
  pool,
  namespace,
  internalToken: await secret('DSH_CLOUD_TOOL_BROKER_TOKEN'),
  encryptionKey: Buffer.from(await secret('DSH_CLOUD_SANDBOX_ENCRYPTION_KEY'), 'base64'),
  provider,
  attemptLeaseSeconds: Number(process.env['DSH_CLOUD_RUN_LEASE_SECONDS'] ?? '20'),
})
await broker.initialize()
const server = broker.createServer()
server.listen(Number(process.env['DSH_CLOUD_TOOL_BROKER_PORT'] ?? '3090'), '0.0.0.0')
const idleMilliseconds = Number(process.env['DSH_CLOUD_SANDBOX_IDLE_TTL_MS'] ?? '1800000')
const reaper = setInterval(() => void broker.reconcile(idleMilliseconds).catch(error => console.error('Sandbox reaper:', error instanceof Error ? error.message : String(error))), 60_000)
reaper.unref()
for (const signal of ['SIGINT','SIGTERM'] as const) process.once(signal, () => { clearInterval(reaper); server.close(() => void pool.end()) })
