import { randomUUID } from 'node:crypto'
import WebSocket from 'ws'

export function percentile(values, fraction) {
  if (values.length === 0) return undefined
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1))]
}

function eventFlow(arrivals) {
  if (arrivals.length === 0) return { events: 0 }
  const gaps = arrivals.slice(1).map((value, index) => Math.round(value - arrivals[index]))
  return {
    events: arrivals.length,
    activeMs: Math.round(arrivals.at(-1) - arrivals[0]),
    ...(gaps.length === 0 ? {} : {
      gapMs: {
        p50: percentile(gaps, 0.50),
        p95: percentile(gaps, 0.95),
        maximum: Math.max(...gaps),
      },
      pausesOver100Ms: gaps.filter(value => value > 100).length,
      pausesOver250Ms: gaps.filter(value => value > 250).length,
    }),
  }
}

export class CloudAcceptanceClient {
  constructor(baseUrl, cookie) {
    this.baseUrl = new URL(baseUrl)
    this.cookie = cookie
  }

  static async register(baseUrl, label = 'Real User Acceptance') {
    const client = new CloudAcceptanceClient(baseUrl)
    const response = await client.request('/cloud/register', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: label,
        email: `acceptance-${randomUUID()}@example.test`,
        password: `Acceptance-${randomUUID()}!`,
      }),
    })
    const cookie = response.response.headers.getSetCookie()[0]?.split(';', 1)[0]
    if (!cookie) throw new Error('registration did not return an auth cookie')
    return new CloudAcceptanceClient(baseUrl, cookie)
  }

  async request(path, init = {}) {
    const response = await fetch(new URL(path, this.baseUrl), init)
    const value = await response.json()
    if (!response.ok) throw new Error(`${path} failed: ${JSON.stringify(value)}`)
    return { response, value }
  }

  async rpc(method, payload, rpcId = randomUUID()) {
    const { value } = await this.request(`/api/${method}`, {
      method: 'POST',
      headers: {
        cookie: this.cookie,
        'content-type': 'application/json',
        'idempotency-key': rpcId,
      },
      body: JSON.stringify({ type: 'client-request', rpcId, method, payload }),
    })
    if (value?.result?.ok !== true) {
      throw new Error(`${method} rejected: ${JSON.stringify(value?.result?.error)}`)
    }
    return { rpcId, value: value.result.value }
  }

  async createWorkspaceSession(name = `real-user-${randomUUID().slice(0, 8)}`) {
    const root = await this.rpc('host.listDirectory', {})
    if (root.value?.path !== '/workspaces') throw new Error('cloud Workspace directory root was not available')
    const directory = await this.rpc('host.createDirectory', { path: root.value.path, name })
    if (directory.value?.path !== `/workspaces/${name}`) throw new Error('cloud Workspace directory was not created')
    const workspace = await this.rpc('workspace.create', { path: directory.value.path })
    const workspaceId = workspace.value?.workspace?.workspaceId
    if (typeof workspaceId !== 'string') throw new Error('workspace.create did not return an id')
    const session = await this.rpc('session.create', { workspaceId })
    const sessionId = session.value?.sessionId
    if (typeof sessionId !== 'string') throw new Error('session.create did not return an id')
    return { workspaceId, sessionId }
  }

  async connect(sessionId) {
    const websocketUrl = new URL('/api/events.mux', this.baseUrl)
    websocketUrl.protocol = websocketUrl.protocol === 'https:' ? 'wss:' : 'ws:'
    const socket = new WebSocket(websocketUrl, {
      headers: { cookie: this.cookie, origin: this.baseUrl.origin },
    })
    await new Promise((resolve, reject) => {
      socket.once('open', resolve)
      socket.once('error', reject)
    })
    return new CloudSessionStream(this, socket, sessionId)
  }
}

class CloudSessionStream {
  constructor(client, socket, sessionId) {
    this.client = client
    this.socket = socket
    this.sessionId = sessionId
  }

  close() {
    this.socket.close()
  }

  async prompt(text, timeoutMs) {
    const rpcId = randomUUID()
    const observed = []
    const assistantChunkArrivals = []
    const textDeltaArrivals = []
    const startedAt = performance.now()
    let promptSeq = -1
    let userMessageAt
    let promptAcceptedAt
    let firstAssistantAt
    let resolveTerminal
    let rejectTerminal
    const terminal = new Promise((resolve, reject) => {
      resolveTerminal = resolve
      rejectTerminal = reject
    })
    const timeout = setTimeout(() => {
      const tail = observed.slice(-8).map(event => `${event.seq}:${event.type}`).join(',')
      rejectTerminal(new Error(`turn did not settle within ${timeoutMs}ms (events=${observed.length}, promptSeq=${promptSeq}, tail=${tail})`))
    }, timeoutMs)
    const onMessage = data => {
      let message
      try { message = JSON.parse(data.toString()) } catch { return }
      const payload = message?.payload
      if (payload?.type !== 'session/event' || payload.sessionId !== this.sessionId) return
      const event = payload.event
      if (!Number.isSafeInteger(event?.seq)) return
      observed.push(event)
      if (event.type === 'user/message' && event.data?.source?.rpcId === rpcId) {
        promptSeq = event.seq
        userMessageAt ??= performance.now()
      }
      if (promptSeq >= 0 && event.seq > promptSeq && event.type === 'assistant/chunk') {
        const arrivedAt = performance.now()
        assistantChunkArrivals.push(arrivedAt)
        if (event.data?.chunk?.type === 'text-delta') textDeltaArrivals.push(arrivedAt)
      }
      if (promptSeq >= 0 && event.seq > promptSeq && event.type === 'assistant/chunk' && firstAssistantAt === undefined) {
        firstAssistantAt = performance.now()
      }
      if (promptSeq >= 0 && event.type === 'turn/end' && event.seq > promptSeq) resolveTerminal(event)
    }
    const onError = error => rejectTerminal(error)
    const onClose = () => rejectTerminal(new Error('event stream closed before turn settlement'))
    this.socket.on('message', onMessage)
    this.socket.once('error', onError)
    this.socket.once('close', onClose)

    let submitted
    try {
      submitted = await this.client.rpc('session.prompt', {
        sessionId: this.sessionId,
        mode: 'queue',
        content: [{ type: 'text', text }],
        clientTimeZone: 'Asia/Shanghai',
      }, rpcId)
      promptAcceptedAt = performance.now()
      const terminalEvent = await terminal
      const endedAt = performance.now()
      return {
        runId: submitted.value?.runId,
        rpcId,
        events: observed,
        terminal: terminalEvent,
        durationMs: Math.round(endedAt - startedAt),
        userMessageMs: userMessageAt === undefined ? undefined : Math.round(userMessageAt - startedAt),
        promptAcceptedMs: Math.round(promptAcceptedAt - startedAt),
        firstAssistantMs: firstAssistantAt === undefined ? undefined : Math.round(firstAssistantAt - startedAt),
        assistantChunkFlow: eventFlow(assistantChunkArrivals),
        textDeltaFlow: eventFlow(textDeltaArrivals),
      }
    } finally {
      clearTimeout(timeout)
      this.socket.off('message', onMessage)
      this.socket.off('error', onError)
      this.socket.off('close', onClose)
    }
  }

  async history(maxMessages = 40) {
    return this.client.rpc('session.history', { sessionId: this.sessionId, maxMessages })
  }
}

export function assistantText(history) {
  const rows = Array.isArray(history?.value?.events) ? history.value.events : []
  const assistant = [...rows].reverse().find(item => item?.event?.type === 'assistant/message')?.event
  const content = assistant?.data?.message?.content
  if (!Array.isArray(content)) return undefined
  return content.filter(part => part?.type === 'text').map(part => part.text).join('')
}
