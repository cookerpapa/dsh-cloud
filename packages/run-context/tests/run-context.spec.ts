import { Context } from '@deepseek-ai/cordis'
import { describe, expect, it } from 'vitest'
import CloudRunContext, { cloudIdentifier, writerFence, type RunAuthority } from '../src/index.ts'

function authority(fence = 7): RunAuthority {
  return {
    tenantId: cloudIdentifier('TenantId', 'tenant-a'),
    workspaceId: cloudIdentifier('WorkspaceId', 'workspace-a'),
    sessionId: cloudIdentifier('SessionId', 'session-a'),
    runId: cloudIdentifier('RunId', 'run-a'),
    attemptId: cloudIdentifier('AttemptId', `attempt-${fence}`),
    writerFence: writerFence(fence),
  }
}

describe('CloudRunContext', () => {
  it('keeps authority across asynchronous boundaries and restores the parent scope', async () => {
    const ctx = new Context()
    const fiber = await ctx.plugin(CloudRunContext)

    expect(ctx.cloudRunContext.current()).toBeUndefined()
    await ctx.cloudRunContext.run(authority(), async () => {
      await Promise.resolve()
      expect(ctx.cloudRunContext.current()?.attemptId).toBe('attempt-7')
      await ctx.cloudRunContext.run(authority(8), async () => {
        await new Promise(resolve => setTimeout(resolve, 0))
        expect(ctx.cloudRunContext.current()?.writerFence).toBe(8)
      })
      expect(ctx.cloudRunContext.current()?.writerFence).toBe(7)
    })
    expect(ctx.cloudRunContext.current()).toBeUndefined()
    await fiber.dispose()
  })

  it('rejects invalid identifiers and fences', () => {
    expect(() => cloudIdentifier('RunId', '   ')).toThrow(/RunId/)
    expect(() => writerFence(-1)).toThrow(/non-negative safe integer/)
    expect(() => writerFence(1.5)).toThrow(/non-negative safe integer/)
  })
})

