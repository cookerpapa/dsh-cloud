import { ExecutionEngine } from './engine.js'
import { createExecutionAgent } from './server.js'

const engine = new ExecutionEngine(
  process.env['DSH_CLOUD_WORKSPACE_ROOT'] ?? '/workspace',
  process.env['DSH_CLOUD_RUNTIME_ROOT'] ?? '/tmp/dsh-cloud-runtime',
)
await engine.initialize()

const server = createExecutionAgent({ engine })
const port = Number(process.env['DSH_CLOUD_EXECUTION_PORT'] ?? '49984')
server.listen(port, '0.0.0.0')

async function shutdown(): Promise<void> {
  server.close()
  await engine.dispose()
}

process.once('SIGINT', () => void shutdown())
process.once('SIGTERM', () => void shutdown())
