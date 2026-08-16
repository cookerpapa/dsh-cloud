import { chmod, readFile, writeFile } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import WebSocket from 'ws'

const baseUrl = new URL(process.env.DSH_CLOUD_ACCEPTANCE_URL || 'http://127.0.0.1:18080')
const statePath = process.env.DSH_CLOUD_ACCEPTANCE_STATE || '/tmp/dsh-cloud-real-user.json'
const prompt = process.env.DSH_CLOUD_ACCEPTANCE_PROMPT
const timeoutMs = Number(process.env.DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS ?? '300000')
if (!prompt) throw new Error('DSH_CLOUD_ACCEPTANCE_PROMPT is required')
if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 10_000 || timeoutMs > 1_800_000) {
  throw new Error('DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS must be an integer from 10000 to 1800000')
}

async function request(path, init = {}) {
  const response = await fetch(new URL(path, baseUrl), init)
  const value = await response.json()
  if (!response.ok) throw new Error(`${path} failed: ${JSON.stringify(value)}`)
  return { response, value }
}

async function rpc(cookie, method, payload, rpcId = randomUUID()) {
  const { value } = await request(`/api/${method}`, {
    method: 'POST',
    headers: { cookie, 'content-type': 'application/json', 'idempotency-key': rpcId },
    body: JSON.stringify({ type: 'client-request', rpcId, method, payload }),
  })
  if (value?.result?.ok !== true) throw new Error(`${method} rejected: ${JSON.stringify(value?.result?.error)}`)
  return { rpcId, value: value.result.value }
}

async function state() {
  try {
    return JSON.parse(await readFile(statePath, 'utf8'))
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error
  }
  const email = `acceptance-${randomUUID()}@example.test`
  const password = `Acceptance-${randomUUID()}!`
  const registered = await request('/cloud/register', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'Real User Acceptance', email, password }),
  })
  const cookie = registered.response.headers.getSetCookie()[0]?.split(';', 1)[0]
  if (!cookie) throw new Error('registration did not return an auth cookie')
  const root = await rpc(cookie, 'host.listDirectory', {})
  if (root.value?.path !== '/workspaces') throw new Error('cloud Workspace directory root was not available')
  const directoryName = `real-user-${randomUUID().slice(0, 8)}`
  const directory = await rpc(cookie, 'host.createDirectory', { path: root.value.path, name: directoryName })
  if (directory.value?.path !== `/workspaces/${directoryName}`) throw new Error('cloud Workspace directory was not created')
  const workspace = await rpc(cookie, 'workspace.create', { path: directory.value.path })
  const workspaceId = workspace.value?.workspace?.workspaceId
  if (typeof workspaceId !== 'string') throw new Error('workspace.create did not return an id')
  const session = await rpc(cookie, 'session.create', { workspaceId })
  const sessionId = session.value?.sessionId
  if (typeof sessionId !== 'string') throw new Error('session.create did not return an id')
  const stored = { cookie, workspaceId, sessionId }
  await writeFile(statePath, JSON.stringify(stored), { mode: 0o600 })
  await chmod(statePath, 0o600)
  return stored
}

const acceptance = await state()
const websocketUrl = new URL('/api/events.mux', baseUrl)
websocketUrl.protocol = websocketUrl.protocol === 'https:' ? 'wss:' : 'ws:'
const socket = new WebSocket(websocketUrl, { headers: { cookie: acceptance.cookie, origin: baseUrl.origin } })
await new Promise((resolve, reject) => {
  socket.once('open', resolve)
  socket.once('error', reject)
})
const startedAt = performance.now()
const observed = []
let promptSeq = -1
const promptRpcId = randomUUID()
let resolveTerminal
let rejectTerminal
const terminalPromise = new Promise((resolve, reject) => {
  resolveTerminal = resolve
  rejectTerminal = reject
})
const timeout = setTimeout(() => {
  const tail = observed.slice(-8).map(event => `${event.seq}:${event.type}`).join(',')
  rejectTerminal(new Error(`real-user turn did not settle within ${timeoutMs}ms (events=${observed.length}, promptSeq=${promptSeq}, tail=${tail})`))
}, timeoutMs)
socket.on('message', data => {
  let message
  try { message = JSON.parse(data.toString()) } catch { return }
  const payload = message?.payload
  if (payload?.type !== 'session/event' || payload.sessionId !== acceptance.sessionId) return
  const event = payload.event
  if (!Number.isSafeInteger(event?.seq)) return
  observed.push(event)
  if (event.type === 'user/message' && event.data?.source?.rpcId === promptRpcId) promptSeq = event.seq
  if (promptSeq >= 0 && event.type === 'turn/end' && event.seq > promptSeq) {
    clearTimeout(timeout)
    resolveTerminal(event)
  }
})
socket.once('error', error => { clearTimeout(timeout); rejectTerminal(error) })
socket.once('close', () => { clearTimeout(timeout); rejectTerminal(new Error('event stream closed before turn settlement')) })

let submitted
try {
  submitted = await rpc(acceptance.cookie, 'session.prompt', {
    sessionId: acceptance.sessionId,
    mode: 'queue',
    content: [{ type: 'text', text: prompt }],
    clientTimeZone: 'Asia/Shanghai',
  }, promptRpcId)
} catch (error) {
  clearTimeout(timeout)
  socket.close()
  throw error
}
const terminal = await terminalPromise
socket.close()

const history = await rpc(acceptance.cookie, 'session.history', {
  sessionId: acceptance.sessionId,
  maxMessages: 20,
})
const eventRows = Array.isArray(history.value?.events) ? history.value.events : []
const assistant = [...eventRows].reverse().find(item => item?.event?.type === 'assistant/message')?.event
const text = assistant?.data?.message?.content?.filter(part => part?.type === 'text').map(part => part.text).join('')

process.stdout.write(`${JSON.stringify({
  sessionId: acceptance.sessionId,
  workspaceId: acceptance.workspaceId,
  runId: submitted?.value?.runId,
  durationMs: Math.round(performance.now() - startedAt),
  observedEvents: observed.length,
  firstObservedSeq: observed[0]?.seq,
  terminalSeq: terminal.seq,
  terminalReason: terminal.data?.reason,
  assistantText: typeof text === 'string' ? text.slice(0, 1_000) : undefined,
})}\n`)
