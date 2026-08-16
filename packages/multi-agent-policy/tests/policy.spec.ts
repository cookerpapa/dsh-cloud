import { describe, expect, it } from 'vitest'
import { cloudMultiAgentDenyList } from '../src/index.ts'

const tools = ['read', 'write', 'edit', 'bash', 'web', 'subagent', 'subagent_fork', 'send_message', 'list_agents', 'workflow', 'ralph']

describe('Cloud multi-agent policy', () => {
  it('keeps foreground Workflow visible while hiding continuable root tools', () => {
    expect(cloudMultiAgentDenyList(undefined, tools)).toEqual([
      'subagent',
      'subagent_fork',
      'send_message',
      'list_agents',
    ])
  })

  it('makes Workflow children read-only and prevents recursive fan-out', () => {
    expect(cloudMultiAgentDenyList('subagent', tools)).toEqual([
      'write',
      'edit',
      'bash',
      'subagent',
      'subagent_fork',
      'send_message',
      'list_agents',
      'workflow',
      'ralph',
    ])
    expect(cloudMultiAgentDenyList('subagent', tools)).not.toContain('read')
    expect(cloudMultiAgentDenyList('subagent', tools)).not.toContain('web')
  })
})
