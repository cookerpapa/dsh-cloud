import type { ExecutionRequest, ExecutionResponse } from '@dsh-cloud/execution-protocol'

export interface SandboxHandle {
  readonly provider: string
  readonly sandboxId: string
  readonly endpoint: string
  readonly trafficToken?: string
  readonly volumeId?: string
}

export interface SandboxBinding {
  readonly activationId: string
  readonly secret: string
  readonly writerFence: number
}

export interface ManagedSandbox {
  readonly handle: SandboxHandle
  readonly activationId: string
}

export interface SandboxProvider {
  create(input: Readonly<{ activationId: string; tenantId: string; workspaceId: string; writerFence: number }>): Promise<SandboxHandle>
  bind(handle: SandboxHandle, binding: SandboxBinding, previous?: SandboxBinding): Promise<void>
  execute(handle: SandboxHandle, binding: SandboxBinding, request: ExecutionRequest, signal?: AbortSignal): Promise<ExecutionResponse>
  destroy(handle: SandboxHandle): Promise<void>
  inspect(handle: SandboxHandle): Promise<'ready' | 'absent' | 'failed'>
  /** Physical inventory used to remove instances that outlived their DB record. */
  listManaged?(): Promise<readonly ManagedSandbox[]>
  destroyWorkspace?(input: Readonly<{ tenantId: string; workspaceId: string }>): Promise<void>
}
