import { setTimeout as delay } from 'node:timers/promises'
import { Client } from 'pg'
import { ControlStore, type ClaimedRun } from '@dsh-cloud/control-store'

export interface RunExecutionBackend {
  dispatch(run: ClaimedRun, signal: AbortSignal): Promise<unknown>
  cancel(run: ClaimedRun): Promise<'accepted' | 'absent'>
}

export interface PostgresRunWorkerOptions {
  readonly store: ControlStore
  readonly notificationConnectionString: string
  readonly identity: string
  readonly baseUrl: string
  readonly maximumConcurrentRuns: number
  readonly backend: RunExecutionBackend
  readonly pollIntervalMs?: number
  readonly maximumAttempts?: number
  readonly attemptLeaseSeconds?: number
  readonly promptPersistenceTimeoutMs?: number
  readonly onError?: (error: unknown) => void
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${name} must be a positive integer`)
  return value
}

/** Shared PostgreSQL queue consumer; LISTEN/NOTIFY is only a latency hint. */
export class PostgresRunWorker {
  private readonly active = new Map<string, { run: ClaimedRun; controller: AbortController; promise: Promise<void> }>()
  private readonly pollIntervalMs: number
  private readonly maximumAttempts: number
  private readonly attemptLeaseSeconds: number
  private readonly heartbeatIntervalMs: number
  private readonly promptPersistenceTimeoutMs: number
  private controller?: AbortController
  private listener?: Client
  private loop?: Promise<void>
  private drainPromise?: Promise<void>
  private wake: (() => void) | undefined

  constructor(private readonly options: PostgresRunWorkerOptions) {
    positive(options.maximumConcurrentRuns, 'maximumConcurrentRuns')
    this.pollIntervalMs = positive(options.pollIntervalMs ?? 1000, 'pollIntervalMs')
    this.maximumAttempts = positive(options.maximumAttempts ?? 3, 'maximumAttempts')
    this.attemptLeaseSeconds = positive(options.attemptLeaseSeconds ?? 20, 'attemptLeaseSeconds')
    this.heartbeatIntervalMs = Math.max(250, Math.min(5_000, Math.floor(this.attemptLeaseSeconds * 1_000 / 3)))
    this.promptPersistenceTimeoutMs = positive(options.promptPersistenceTimeoutMs ?? 30_000, 'promptPersistenceTimeoutMs')
    new URL(options.baseUrl)
  }

  async start(): Promise<void> {
    if (this.controller !== undefined) throw new Error('Run Worker can only start once')
    const controller = new AbortController()
    this.controller = controller
    await this.options.store.heartbeatWorker({ id: this.options.identity, baseUrl: this.options.baseUrl, maximumRuns: this.options.maximumConcurrentRuns })
    const listener = new Client({ connectionString: this.options.notificationConnectionString, application_name: `${this.options.identity}-run-queue` })
    listener.on('notification', message => { if (message.channel === 'dsh_cloud_run_queue') this.wake?.() })
    listener.on('error', error => this.observe(error))
    await listener.connect()
    await listener.query('LISTEN dsh_cloud_run_queue')
    this.listener = listener
    this.loop = this.run(controller.signal)
  }

  async stop(): Promise<void> {
    await this.drain(0)
  }

  /** Stop admission, let owned Runs settle, then abort only after the drain deadline. */
  async drain(timeoutMs: number): Promise<void> {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) throw new TypeError('drain timeout is invalid')
    if (this.drainPromise !== undefined) return this.drainPromise
    this.drainPromise = this.performDrain(timeoutMs)
    return this.drainPromise
  }

  private async performDrain(timeoutMs: number): Promise<void> {
    this.controller?.abort()
    this.wake?.()
    await this.loop
    await this.options.store.setWorkerDraining(this.options.identity, true).catch(error => this.observe(error))
    if (this.active.size > 0 && timeoutMs > 0) {
      await Promise.race([
        Promise.allSettled([...this.active.values()].map(item => item.promise)),
        delay(timeoutMs),
      ])
    }
    for (const item of this.active.values()) item.controller.abort()
    await Promise.allSettled([...this.active.values()].map(item => item.promise))
    await this.listener?.end().catch(() => undefined)
  }

  status(): { activeRuns: number; maximumRuns: number; draining: boolean } {
    return { activeRuns: this.active.size, maximumRuns: this.options.maximumConcurrentRuns, draining: this.controller?.signal.aborted ?? false }
  }

  private async run(signal: AbortSignal): Promise<void> {
    let nextHeartbeat = 0
    let nextReconciliation = 0
    while (!signal.aborted) {
      try {
        if (Date.now() >= nextHeartbeat) {
          await this.options.store.heartbeatWorker({ id: this.options.identity, baseUrl: this.options.baseUrl, maximumRuns: this.options.maximumConcurrentRuns })
          nextHeartbeat = Date.now() + 5000
        }
        if (Date.now() >= nextReconciliation) {
          await this.options.store.reconcileExpiredAttempts(this.attemptLeaseSeconds)
          nextReconciliation = Date.now() + 10_000
        }
        await this.fillCapacity()
      } catch (error) { this.observe(error) }
      await this.wait(signal)
    }
  }

  private async fillCapacity(): Promise<void> {
    while (this.active.size < this.options.maximumConcurrentRuns) {
      const claim = await this.options.store.claimNext(this.options.identity)
      if (claim.kind === 'idle') return
      const controller = new AbortController()
      const item = { run: claim.run, controller, promise: Promise.resolve() }
      item.promise = this.execute(claim.run, controller.signal).finally(() => {
        this.active.delete(claim.run.runId)
        this.wake?.()
      })
      this.active.set(claim.run.runId, item)
    }
  }

  private async execute(run: ClaimedRun, signal: AbortSignal): Promise<void> {
    let promptDurable = false
    let dispatchStarted = false
    let dispatchAccepted = false
    const lease = { nextHeartbeatAt: Date.now() + this.heartbeatIntervalMs }
    try {
      if (await this.options.store.cancellationRequested(run.runId, run.attemptId)) {
        await this.options.store.finishRun(run.runId, run.attemptId, 'cancelled', 'cancelled')
        return
      }
      await this.options.store.markDispatching(run.runId, run.attemptId)
      dispatchStarted = true
      const response = await this.options.backend.dispatch(run, signal)
      dispatchAccepted = true
      await this.options.store.markDispatched(run.runId, run.attemptId, response)
      const promptDeadline = Date.now() + this.promptPersistenceTimeoutMs
      while (!(promptDurable = await this.options.store.promptPersisted(run.runId))) {
        if (await this.options.store.cancellationRequested(run.runId, run.attemptId)) {
          await this.cancelAndAwaitSettlement(run, signal, lease)
          await this.options.store.finishRun(run.runId, run.attemptId, 'cancelled', 'cancelled')
          return
        }
        if (Date.now() >= promptDeadline) {
          await this.options.backend.cancel(run).catch(error => this.observe(error))
          throw Object.assign(
            new Error('DSH accepted the prompt but did not persist its user/message boundary before the deadline'),
            { code: 'prompt_persistence_timeout' },
          )
        }
        await this.heartbeatIfDue(run, lease)
        await delay(50, undefined, { signal })
      }
      await this.options.store.markRunning(run.runId, run.attemptId)
      let outcome = await this.options.store.turnOutcome(run.runId)
      while (outcome === undefined) {
        if (await this.options.store.cancellationRequested(run.runId, run.attemptId)) {
          await this.cancelAndAwaitSettlement(run, signal, lease)
          await this.options.store.finishRun(run.runId, run.attemptId, 'cancelled', 'cancelled')
          return
        }
        await this.heartbeatIfDue(run, lease)
        await delay(250, undefined, { signal })
        outcome = await this.options.store.turnOutcome(run.runId)
      }
      if (await this.options.store.cancellationRequested(run.runId, run.attemptId)) {
        await this.options.store.finishRun(run.runId, run.attemptId, 'cancelled', 'cancelled')
      } else if (outcome.kind === 'completed' || outcome.kind === 'blocked' || outcome.kind === 'max-tokens') {
        await this.options.store.finishRun(run.runId, run.attemptId, 'completed')
      } else {
        await this.options.store.finishRun(run.runId, run.attemptId, 'failed', outcome.errorCode ?? `turn_${outcome.kind}`)
      }
    } catch (error: unknown) {
      promptDurable ||= await this.options.store.promptPersisted(run.runId).catch(() => false)
      try {
        const cancelled = await this.options.store.cancellationRequested(run.runId, run.attemptId)
        if (cancelled && promptDurable) {
          await this.cancelAndAwaitSettlement(run, signal, lease)
          await this.options.store.finishRun(run.runId, run.attemptId, 'cancelled', 'cancelled')
        } else if (cancelled) await this.options.store.finishRun(run.runId, run.attemptId, 'cancelled', 'cancelled')
        else if (!dispatchStarted && !promptDurable && await this.options.store.attemptCount(run.runId) < this.maximumAttempts) await this.options.store.requeueBeforeStart(run.runId, run.attemptId, 500)
        else if (!promptDurable) await this.options.store.finishRun(
          run.runId,
          run.attemptId,
          'failed',
          dispatchAccepted && error instanceof Error && 'code' in error
            ? String(error.code)
            : dispatchStarted ? 'prompt_dispatch_unknown' : 'dispatch_attempts_exhausted',
        )
        else await this.options.store.finishRun(run.runId, run.attemptId, 'failed', error instanceof Error && 'code' in error ? String(error.code) : 'worker_execution_failed')
      } catch (settlementError) { this.observe(settlementError) }
      this.observe(error)
    }
  }

  private async heartbeat(run: ClaimedRun): Promise<void> {
    if (!(await this.options.store.heartbeatAttempt(run.runId, run.attemptId))) throw Object.assign(new Error('RunAttempt lease is stale'), { code: 'ESTALE' })
  }

  private async heartbeatIfDue(run: ClaimedRun, lease: { nextHeartbeatAt: number }): Promise<void> {
    if (Date.now() < lease.nextHeartbeatAt) return
    await this.heartbeat(run)
    lease.nextHeartbeatAt = Date.now() + this.heartbeatIntervalMs
  }

  private async cancelAndAwaitSettlement(run: ClaimedRun, signal: AbortSignal, lease: { nextHeartbeatAt: number }): Promise<void> {
    let accepted = false
    while (!(await this.options.store.turnCompleted(run.runId))) {
      if (!accepted) {
        try {
          const result = await this.options.backend.cancel(run)
          if (result === 'absent') return
          accepted = true
        } catch (error: unknown) {
          this.observe(error)
        }
      }
      await this.heartbeatIfDue(run, lease)
      await delay(250, undefined, { signal })
    }
  }

  private wait(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.resolve()
    return new Promise<void>(resolve => {
      let settled = false
      const timer = setTimeout(settle, this.pollIntervalMs)
      timer.unref()
      const abort = (): void => settle()
      this.wake = settle
      function settle(): void {
        if (settled) return
        settled = true
        clearTimeout(timer)
        signal.removeEventListener('abort', abort)
        resolve()
      }
      signal.addEventListener('abort', abort, { once: true })
    }).finally(() => { this.wake = undefined })
  }

  private observe(error: unknown): void {
    try { this.options.onError?.(error) } catch { /* observability is not queue authority */ }
  }
}
