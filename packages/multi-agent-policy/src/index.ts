import { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import '@deepseek-ai/dsh-tools'

const ROOT_DENY = new Set([
  // DSH rc.6 exposes these as continuable children. A Cloud continuation
  // needs its own durable RunAttempt; inheriting the parent's expiring fence
  // would let a background child outlive its authority.
  'subagent',
  'subagent_fork',
  'send_message',
  'list_agents',
])

const CHILD_DENY = new Set([
  // Parallel Workflow children currently share the root Workspace/Cube. They
  // may inspect it, but must not race filesystem or process side effects until
  // candidate Workspaces provide one isolated write branch per child.
  'bash',
  'write',
  'edit',
  // Keep delegation bounded to one cloud-governed Workflow generation.
  'subagent',
  'subagent_fork',
  'send_message',
  'list_agents',
  'workflow',
  'ralph',
])

function toolNames(agent: Agent): string[] {
  return agent.ctx.tools.schemas().map(schema => schema.name)
}

export function cloudMultiAgentDenyList(
  origin: 'subagent' | undefined,
  available: readonly string[],
): string[] {
  const policy = origin === 'subagent' ? CHILD_DENY : ROOT_DENY
  return available.filter(name => policy.has(name))
}

/**
 * Admit only the part of DSH's multi-agent runtime that has sound cloud
 * ownership today: a foreground Workflow and its one-shot, read-only child
 * Sessions execute under the root RunAttempt. Background continuations remain
 * model-invisible until they receive independent queue/lease/fence semantics.
 */
class CloudMultiAgentPolicy {
  static inject = ['agents', 'tools']

  constructor(ctx: Context) {
    ctx.on('agent/created', ({ agent }) => {
      const deny = cloudMultiAgentDenyList(agent.session.header.origin, toolNames(agent))
      if (deny.length > 0) agent.ctx.tools.restrict({ deny })
    }, { global: true })
  }
}

export default CloudMultiAgentPolicy
