import { Context } from '@deepseek-ai/cordis'
import type { Agent, AgentHandle, CreateAgentOptions, ResumeAgentOptions } from '@deepseek-ai/dsh-agent'
import z from '@deepseek-ai/schemastery'

export interface Config {
  /** Grace period for session.create/fork to finish attaching its Workspace. */
  initialIdleGraceMs?: number
  /** Small post-turn window in which a same-process follow-up may reuse the live Agent. */
  settledIdleGraceMs?: number
}

interface TrackedAgent {
  readonly handle: AgentHandle
  sawRunning: boolean
  timer?: ReturnType<typeof setTimeout>
  disposing: boolean
}

/**
 * Bounds process-local Agent residency so PostgreSQL SessionPersistence remains
 * the only cross-Run continuity mechanism. This deliberately uses DSH's public
 * AgentHandle lifecycle instead of modifying the upstream Agent Loop.
 */
class CloudAgentResidency {
  static inject = ['agents', 'sessions']
  static Config: z<Config> = z.object({
    initialIdleGraceMs: z.number().min(100).default(2_000),
    settledIdleGraceMs: z.number().min(0).default(25),
  })

  private readonly tracked = new Map<string, TrackedAgent>()
  private readonly initialIdleGraceMs: number
  private readonly settledIdleGraceMs: number

  constructor(ctx: Context, config: Config) {
    this.initialIdleGraceMs = config.initialIdleGraceMs ?? 2_000
    this.settledIdleGraceMs = config.settledIdleGraceMs ?? 25

    const create = ctx.agents.create.bind(ctx.agents)
    const resume = ctx.agents.resume.bind(ctx.agents)
    const wrappedCreate = async (options: CreateAgentOptions): Promise<AgentHandle> => this.capture(ctx, await create(options))
    const wrappedResume = async (options: ResumeAgentOptions): Promise<AgentHandle> => this.capture(ctx, await resume(options))
    ctx.agents.create = wrappedCreate
    ctx.agents.resume = wrappedResume

    const stopStatus = ctx.on('agent/status', ({ agent, status }) => {
      const entry = this.tracked.get(String(agent.id))
      if (entry === undefined || entry.handle.agent !== agent) return
      if (status === 'running') {
        entry.sawRunning = true
        this.clearTimer(entry)
      } else if (entry.sawRunning) {
        this.schedule(ctx, entry, this.settledIdleGraceMs)
      }
    }, { global: true })
    const stopDisposed = ctx.on('agent/disposed', ({ agent }) => {
      const id = String(agent.id)
      const entry = this.tracked.get(id)
      if (entry?.handle.agent !== agent) return
      this.clearTimer(entry)
      this.tracked.delete(id)
    }, { global: true })

    ctx.effect(() => async () => {
      if (ctx.agents.create === wrappedCreate) ctx.agents.create = create
      if (ctx.agents.resume === wrappedResume) ctx.agents.resume = resume
      stopStatus()
      stopDisposed()
      const entries = [...this.tracked.values()]
      this.tracked.clear()
      for (const entry of entries) this.clearTimer(entry)
      await Promise.allSettled(entries.map(entry => entry.handle.dispose()))
    }, 'cloudAgentResidency.lifecycle')
  }

  private capture(ctx: Context, handle: AgentHandle): AgentHandle {
    // Continuable subagents have a separate owner that deliberately retains its
    // AgentHandle. Cloud residency governs only ordinary user Sessions.
    if (handle.agent.session.header.origin === 'subagent') return handle
    const id = String(handle.agent.id)
    const previous = this.tracked.get(id)
    if (previous !== undefined && previous.handle !== handle) this.clearTimer(previous)
    const entry: TrackedAgent = { handle, sawRunning: handle.agent.status === 'running', disposing: false }
    this.tracked.set(id, entry)
    if (entry.sawRunning) return handle
    this.schedule(ctx, entry, this.initialIdleGraceMs)
    return handle
  }

  private schedule(ctx: Context, entry: TrackedAgent, delayMs: number): void {
    this.clearTimer(entry)
    entry.timer = setTimeout(() => { void this.release(ctx, entry) }, delayMs)
    entry.timer.unref()
  }

  private clearTimer(entry: TrackedAgent): void {
    if (entry.timer !== undefined) clearTimeout(entry.timer)
    delete entry.timer
  }

  private async release(ctx: Context, entry: TrackedAgent): Promise<void> {
    const agent: Agent = entry.handle.agent
    const id = String(agent.id)
    if (entry.disposing || this.tracked.get(id) !== entry || agent.status !== 'idle') return
    entry.disposing = true
    try {
      const participated = await ctx.sessions.flush(agent.session)
      if (!participated) throw new Error(`session "${id}" has no durability participant`)
      if (this.tracked.get(id) !== entry || agent.status !== 'idle') return
      await entry.handle.dispose()
    } catch (error: unknown) {
      ctx.logger.warn(`cloud Agent residency could not release session "${id}": ${String(error)}`)
      if (this.tracked.get(id) === entry && agent.status === 'idle') this.schedule(ctx, entry, this.initialIdleGraceMs)
    } finally {
      entry.disposing = false
    }
  }
}

export default CloudAgentResidency
