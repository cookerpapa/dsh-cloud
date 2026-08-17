import { randomUUID } from 'node:crypto'
import WebSocket, { type RawData } from 'ws'
import { ControlStore, type WorkerRecord } from '@dsh-cloud/control-store'

export type WorkerEventPath = '/api/events.mux' | '/api/events.host'

interface Subscriber {
  readonly tenantId: string
  readonly browser: WebSocket
  readonly allowedSessions: Set<string>
  readonly durableWatermark: Map<string, number>
  readonly deliveredThrough: Map<string, number>
  delivery: Promise<void>
}

interface WorkerStream {
  readonly worker: WorkerRecord
  readonly path: WorkerEventPath
  readonly socket: WebSocket
}

function streamKey(workerId: string, path: WorkerEventPath): string {
  return `${workerId}:${path}`
}

function frameBytes(data: RawData): string {
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8')
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8')
  return data.toString('utf8')
}

/**
 * Shared live outlet for every healthy Worker. Browser sockets subscribe to the
 * fleet, never to a durable user/Session placement. PostgreSQL remains the
 * tenant authority and the caller-provided barrier keeps visible events durable.
 */
export class WorkerEventHub {
  private readonly subscribers = new Map<WorkerEventPath, Set<Subscriber>>()
  private readonly streams = new Map<string, WorkerStream>()
  private readonly timer: NodeJS.Timeout
  private closed = false

  constructor(
    private readonly store: ControlStore,
    private readonly waitDurable: (sessionId: string, seq: number, watermarks: Map<string, number>) => Promise<void>,
  ) {
    this.timer = setInterval(() => void this.reconcile(), 1_000)
    this.timer.unref()
  }

  subscribe(path: WorkerEventPath, tenantId: string, allowedSessions: Set<string>, browser: WebSocket): void {
    const subscriber: Subscriber = {
      tenantId,
      browser,
      allowedSessions,
      durableWatermark: new Map(),
      deliveredThrough: new Map(),
      delivery: Promise.resolve(),
    }
    let group = this.subscribers.get(path)
    if (group === undefined) {
      group = new Set()
      this.subscribers.set(path, group)
      // A fresh browser cohort needs the upstream subscription handshake too;
      // reconnect instead of reusing a stream that emitted it while nobody listened.
      for (const [key, stream] of this.streams) {
        if (stream.path === path) {
          this.streams.delete(key)
          stream.socket.terminate()
        }
      }
    }
    group.add(subscriber)
    subscriber.delivery = this.sendSubscriptionBaseline(subscriber)
    browser.on('message', () => browser.close(1008, 'downlink only'))
    browser.once('close', () => {
      group!.delete(subscriber)
      if (group!.size === 0) this.subscribers.delete(path)
      void this.reconcile()
    })
    void this.reconcile()
  }

  /** Add a newly created/forked Session to already-open tenant outlets. */
  allowSession(tenantId: string, sessionId: string): void {
    for (const group of this.subscribers.values()) {
      for (const subscriber of group) {
        if (subscriber.tenantId !== tenantId || subscriber.allowedSessions.has(sessionId)) continue
        subscriber.allowedSessions.add(sessionId)
        subscriber.delivery = subscriber.delivery
          .then(() => this.sendSessionBaseline(subscriber, sessionId))
          .catch(() => subscriber.browser.close(1011, 'durability barrier failed'))
      }
    }
  }

  /** Publish a PostgreSQL-backed cloud Host projection to one tenant's browsers. */
  publishHost(tenantId: string, payload: Record<string, unknown>): void {
    const data=JSON.stringify({type:'server-request',rpcId:randomUUID(),method:String(payload['type']??''),payload})
    for(const subscriber of this.subscribers.get('/api/events.host')??[]){
      if(subscriber.tenantId!==tenantId)continue
      subscriber.delivery=subscriber.delivery
        .then(()=>{if(subscriber.browser.readyState===WebSocket.OPEN)subscriber.browser.send(data,{binary:false})})
        .catch(()=>subscriber.browser.close(1011,'cloud Host projection failed'))
    }
  }

  async close(): Promise<void> {
    this.closed = true
    clearInterval(this.timer)
    for (const stream of this.streams.values()) stream.socket.terminate()
    this.streams.clear()
    await Promise.allSettled(
      [...this.subscribers.values()].flatMap(group => [...group].map(subscriber => subscriber.delivery)),
    )
  }

  private async reconcile(): Promise<void> {
    if (this.closed) return
    const workers = await this.store.listHealthyWorkers().catch(() => [])
    const desired = new Set<string>()
    for (const path of this.subscribers.keys()) {
      for (const worker of workers) {
        const key = streamKey(worker.id, path)
        desired.add(key)
        if (!this.streams.has(key)) this.connect(worker, path, key)
      }
    }
    for (const [key, stream] of this.streams) {
      if (!desired.has(key)) {
        this.streams.delete(key)
        stream.socket.terminate()
      }
    }
  }

  private connect(worker: WorkerRecord, path: WorkerEventPath, key: string): void {
    const url = new URL(worker.baseUrl)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    url.pathname = path
    const socket = new WebSocket(url)
    const stream = { worker, path, socket }
    this.streams.set(key, stream)
    socket.on('message', (data, binary) => {
      if (!binary) this.broadcast(path, frameBytes(data))
    })
    const release = () => {
      if (this.streams.get(key) === stream) this.streams.delete(key)
    }
    socket.once('close', release)
    socket.once('error', release)
  }

  private broadcast(path: WorkerEventPath, data: string): void {
    for (const subscriber of this.subscribers.get(path) ?? []) {
      subscriber.delivery = subscriber.delivery
        .then(() => this.deliver(subscriber, data))
        .catch(() => subscriber.browser.close(1011, 'durability barrier failed'))
    }
  }

  private async sendSubscriptionBaseline(subscriber: Subscriber): Promise<void> {
    for (const sessionId of [...subscriber.allowedSessions].sort()) {
      await this.sendSessionBaseline(subscriber, sessionId)
    }
  }

  private async sendSessionBaseline(subscriber: Subscriber, sessionId: string): Promise<void> {
    if (subscriber.browser.readyState !== WebSocket.OPEN) return
    const lastSeq = await this.store.sessionDurableThrough(sessionId)
    subscriber.browser.send(JSON.stringify({
      type: 'server-request',
      rpcId: randomUUID(),
      method: 'session/subscribed',
      payload: { type: 'session/subscribed', sessionId, lastSeq },
    }))
  }

  private async deliver(subscriber: Subscriber, data: string): Promise<void> {
    if (subscriber.browser.readyState !== WebSocket.OPEN) return
    const value = JSON.parse(data) as Record<string, unknown>
    const payload = value['payload'] as Record<string, unknown> | undefined
    const sessionId = payload !== undefined && typeof payload['sessionId'] === 'string'
      ? payload['sessionId']
      : undefined
    // Fleet aggregation is wider than one user's old upstream socket. Only
    // Session-scoped frames have a tenant ownership key; cloud Workspace and
    // process-global Host frames are rebuilt through tenant-scoped HTTP APIs.
    if (sessionId === undefined) return
    if (!subscriber.allowedSessions.has(sessionId)) {
      if (!(await this.store.ownsSession(subscriber.tenantId, sessionId))) return
      subscriber.allowedSessions.add(sessionId)
    }
    const type = String(payload?.['type'] ?? '')
    if (type === 'session/subscribed') return
    // DSH emits these for process-local Agent/Session residency. Cloud Session
    // lifecycle belongs to PostgreSQL, so an idle handle disposal is not a user
    // deletion and a cold resume is not a newly created conversation.
    if (type === 'host/session-added' || type === 'host/session-removed') return
    if (type === 'host/remote-event' || type.startsWith('host/workspace-') || type === 'host/archived-sessions-changed') return
    if (type === 'session/event' && sessionId !== undefined) {
      const event = payload?.['event'] as Record<string, unknown> | undefined
      const seq = event?.['seq']
      if (!Number.isSafeInteger(seq)) return
      const sequence = seq as number
      if ((subscriber.deliveredThrough.get(sessionId) ?? -1) >= sequence) return
      await this.waitDurable(sessionId, sequence, subscriber.durableWatermark)
      subscriber.deliveredThrough.set(sessionId, sequence)
    }
    subscriber.browser.send(data, { binary: false })
  }
}
