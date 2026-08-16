import type { Context } from '@deepseek-ai/cordis'
import { SandboxProvider } from '@deepseek-ai/dsh-sandbox'
import type { ConfinedArgv, SandboxPolicy } from '@deepseek-ai/dsh-sandbox'

/**
 * Declares that the subprocess capability itself crosses into a Cube KVM.
 * No same-host wrapper is added: filesystem containment, credentials, network
 * and process isolation are enforced by the execution plane around the argv.
 */
class RemoteSandboxProvider extends SandboxProvider {
  constructor(ctx: Context) { super(ctx) }

  override confine(argv: readonly string[], _policy: SandboxPolicy): ConfinedArgv {
    return {
      argv: [...argv],
      enforcement: 'full',
      denialSignatures: ['permission denied', 'read-only file system'],
      runnerFailureRules: [],
    }
  }
}

export default RemoteSandboxProvider
