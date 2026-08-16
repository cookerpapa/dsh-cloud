import { AsyncLocalStorage } from 'node:async_hooks'
import { Context, Service } from '@deepseek-ai/cordis'

declare const identifierBrand: unique symbol
declare const fenceBrand: unique symbol

/** A non-empty identifier whose meaning is fixed by its brand. */
export type CloudIdentifier<Name extends string> = string & { readonly [identifierBrand]: Name }

export type TenantId = CloudIdentifier<'TenantId'>
export type WorkspaceId = CloudIdentifier<'WorkspaceId'>
export type SessionId = CloudIdentifier<'SessionId'>
export type RunId = CloudIdentifier<'RunId'>
export type AttemptId = CloudIdentifier<'AttemptId'>
export type WriterFence = number & { readonly [fenceBrand]: true }

/** Trusted identity attached to one Agent RunAttempt. */
export interface RunAuthority {
  readonly tenantId: TenantId
  readonly workspaceId: WorkspaceId
  readonly sessionId: SessionId
  readonly runId: RunId
  readonly attemptId: AttemptId
  readonly writerFence: WriterFence
}

/** Validate an externally supplied cloud identifier before branding it. */
export function cloudIdentifier<Name extends string>(name: Name, value: string): CloudIdentifier<Name> {
  const normalized = value.trim()
  if (normalized.length === 0 || normalized.length > 200) {
    throw new TypeError(`${name} must contain between 1 and 200 non-whitespace characters`)
  }
  return normalized as CloudIdentifier<Name>
}

/** Validate the monotonic integer carried by a durable RunAttempt. */
export function writerFence(value: number): WriterFence {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`writer fence must be a non-negative safe integer, got ${String(value)}`)
  }
  return value as WriterFence
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    cloudRunContext: CloudRunContext
  }
}

/**
 * Process-local carrier for trusted RunAttempt authority.
 *
 * It is deliberately not a source of truth: an orchestrator issues the values,
 * and every durable or remote mutation validates the fence again at its own
 * commit boundary.
 */
export class CloudRunContext extends Service {
  private readonly storage = new AsyncLocalStorage<RunAuthority>()

  constructor(ctx: Context) {
    super(ctx, 'cloudRunContext')
  }

  /** Execute one asynchronous operation with immutable RunAttempt authority. */
  run<T>(authority: RunAuthority, operation: () => T): T {
    return this.storage.run(Object.freeze({ ...authority }), operation)
  }

  /** Return the authority visible to the current asynchronous call chain. */
  current(): RunAuthority | undefined {
    return this.storage.getStore()
  }

  /** Return current authority or fail before a cloud mutation can proceed. */
  require(): RunAuthority {
    const current = this.current()
    if (current === undefined) throw new Error('cloud mutation requires active RunAttempt authority')
    return current
  }
}

export default CloudRunContext

