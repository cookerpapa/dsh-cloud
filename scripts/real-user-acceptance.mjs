import { chmod, readFile, writeFile } from 'node:fs/promises'
import { CloudAcceptanceClient, assistantText } from './lib/cloud-acceptance-client.mjs'

const baseUrl = process.env.DSH_CLOUD_ACCEPTANCE_URL || 'http://127.0.0.1:18080'
const statePath = process.env.DSH_CLOUD_ACCEPTANCE_STATE || '/tmp/dsh-cloud-real-user.json'
const prompt = process.env.DSH_CLOUD_ACCEPTANCE_PROMPT
const timeoutMs = Number(process.env.DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS ?? '300000')
if (!prompt) throw new Error('DSH_CLOUD_ACCEPTANCE_PROMPT is required')
if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 10_000 || timeoutMs > 1_800_000) {
  throw new Error('DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS must be an integer from 10000 to 1800000')
}

async function acceptanceState() {
  try {
    const stored = JSON.parse(await readFile(statePath, 'utf8'))
    if (typeof stored.cookie !== 'string' || typeof stored.workspaceId !== 'string' || typeof stored.sessionId !== 'string') {
      throw new Error('stored real-user acceptance state is invalid')
    }
    return { client: new CloudAcceptanceClient(baseUrl, stored.cookie), workspaceId: stored.workspaceId, sessionId: stored.sessionId }
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error
  }
  const client = await CloudAcceptanceClient.register(baseUrl)
  const { workspaceId, sessionId } = await client.createWorkspaceSession()
  await writeFile(statePath, JSON.stringify({ cookie: client.cookie, workspaceId, sessionId }), { mode: 0o600 })
  await chmod(statePath, 0o600)
  return { client, workspaceId, sessionId }
}

const acceptance = await acceptanceState()
const stream = await acceptance.client.connect(acceptance.sessionId)
try {
  const result = await stream.prompt(prompt, timeoutMs)
  const history = await stream.history(20)
  process.stdout.write(`${JSON.stringify({
    sessionId: acceptance.sessionId,
    workspaceId: acceptance.workspaceId,
    runId: result.runId,
    durationMs: result.durationMs,
    firstAssistantMs: result.firstAssistantMs,
    assistantChunkFlow: result.assistantChunkFlow,
    textDeltaFlow: result.textDeltaFlow,
    observedEvents: result.events.length,
    firstObservedSeq: result.events[0]?.seq,
    terminalSeq: result.terminal.seq,
    terminalReason: result.terminal.data?.reason,
    assistantText: assistantText(history)?.slice(0, 1_000),
  })}\n`)
} finally {
  stream.close()
}
