import { randomUUID } from 'node:crypto'
import { CloudAcceptanceClient, assistantText } from './lib/cloud-acceptance-client.mjs'

const baseUrl = process.env.DSH_CLOUD_ACCEPTANCE_URL || 'http://127.0.0.1:18080'
const timeoutMs = Number(process.env.DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS ?? '180000')
if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 10_000 || timeoutMs > 1_800_000) {
  throw new Error('DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS must be an integer from 10000 to 1800000')
}

const token = `DSH_REFRESH_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`
const client = await CloudAcceptanceClient.register(baseUrl, 'Refresh Reconnect Acceptance')
const { workspaceId, sessionId } = await client.createWorkspaceSession(`refresh-${randomUUID().slice(0, 8)}`)

const firstStream = await client.connect(sessionId)
let first
try {
  first = await firstStream.prompt(
    `Remember this exact token for the next Turn: ${token}. Reply briefly and include it exactly once.`,
    timeoutMs,
  )
} finally {
  firstStream.close()
}

// Reproduce a browser refresh after the follow-up was accepted but before its
// model response settled: submit on one connection, close it immediately, and
// recover only through a fresh connection plus canonical Session history.
const beforeRefresh = await client.connect(sessionId)
const rpcId = randomUUID()
const submittedAt = performance.now()
const submitted = await client.rpc('session.prompt', {
  sessionId,
  mode: 'queue',
  content: [{ type: 'text', text: 'What exact token did I ask you to remember? Reply with only that token.' }],
  clientTimeZone: 'Asia/Shanghai',
}, rpcId)
beforeRefresh.close()

const afterRefresh = await client.connect(sessionId)
try {
  const deadline = Date.now() + timeoutMs
  let history
  let terminal
  while (Date.now() < deadline) {
    history = await afterRefresh.history(80)
    const events = Array.isArray(history.value?.events)
      ? history.value.events.map(item => item?.event).filter(Boolean)
      : []
    const prompt = events.find(event =>
      event.type === 'user/message' && event.data?.source?.rpcId === rpcId)
    if (Number.isSafeInteger(prompt?.seq)) {
      terminal = events.find(event => event.type === 'turn/end' && event.seq > prompt.seq)
      if (terminal !== undefined) break
    }
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  if (terminal === undefined) throw new Error('follow-up Turn did not become visible in canonical history after reconnect')
  if (terminal.data?.reason?.kind !== 'completed') {
    throw new Error(`follow-up Turn ended with ${JSON.stringify(terminal.data?.reason)}`)
  }
  const answer = assistantText(history)
  if (answer?.trim() !== token) throw new Error(`reconnected history did not recover the exact token: ${JSON.stringify(answer)}`)
  process.stdout.write(`${JSON.stringify({
    sessionId,
    workspaceId,
    firstRunId: first.runId,
    followUpRunId: submitted.value?.runId,
    followUpDurationMs: Math.round(performance.now() - submittedAt),
    terminalSeq: terminal.seq,
    answer,
  })}\n`)
} finally {
  afterRefresh.close()
}
