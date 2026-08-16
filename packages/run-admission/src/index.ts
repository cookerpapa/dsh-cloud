import { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type { ApiProxy, RpcId, RpcResponse } from '@deepseek-ai/dsh-host-apiproxy/api'
import type { SessionId } from '@deepseek-ai/dsh-session'
import { Pool } from 'pg'
import { ControlStore } from '@dsh-cloud/control-store'
import { cloudIdentifier, writerFence } from '@dsh-cloud/run-context'

declare module '@deepseek-ai/cordis' {
  interface Context { apiProxy: ApiProxy }
}

export interface Config { connectionString: string; namespace?: string }

/** Installs PostgreSQL-issued Run authority around the upstream prompt admission call. */
export class RunAdmission {
  static inject = ['apiProxy', 'cloudRunContext']
  static Config: z<Config> = z.object({ connectionString: z.string().required(), namespace: z.string().default('default') })
  private readonly pool: Pool

  constructor(private readonly ctx: Context, config: Config) {
    this.pool = new Pool({ connectionString: config.connectionString, max: 5, application_name: 'dsh-cloud-run-admission' })
    const store = new ControlStore(this.pool, config.namespace)
    const prompt: ApiProxy['sessions']['prompt'] = ctx.apiProxy.sessions.prompt.bind(ctx.apiProxy.sessions)
    const rename: ApiProxy['sessions']['rename'] = ctx.apiProxy.sessions.rename.bind(ctx.apiProxy.sessions)
    const selectModel: ApiProxy['sessions']['selectModel'] = ctx.apiProxy.sessions.selectModel.bind(ctx.apiProxy.sessions)
    const updateQueue: ApiProxy['sessions']['updateQueue'] = ctx.apiProxy.sessions.updateQueue.bind(ctx.apiProxy.sessions)
    const run = async <T>(request: { rpcId: RpcId; payload: { sessionId: SessionId } }, operation: () => Promise<RpcResponse<T>>): Promise<RpcResponse<T>> => {
      const authority = await store.authorityForRpcId(String(request.rpcId))
      if (authority === undefined || authority.sessionId !== String(request.payload.sessionId)) {
        return {
          rpcId: request.rpcId,
          result: { ok: false, error: { code: 'internal', message: 'cloud Run authority is missing or stale', details: {} } },
        }
      }
      return ctx.cloudRunContext.run({
        tenantId: cloudIdentifier('TenantId', authority.tenantId),
        workspaceId: cloudIdentifier('WorkspaceId', authority.workspaceId),
        sessionId: cloudIdentifier('SessionId', authority.sessionId),
        runId: cloudIdentifier('RunId', authority.runId),
        attemptId: cloudIdentifier('AttemptId', authority.attemptId),
        writerFence: writerFence(authority.writerFence),
      }, operation)
    }
    ctx.apiProxy.sessions.prompt = request => run(request, () => prompt(request))
    ctx.apiProxy.sessions.rename = request => run(request, () => rename(request))
    ctx.apiProxy.sessions.selectModel = request => run(request, () => selectModel(request))
    ctx.apiProxy.sessions.updateQueue = request => run(request, () => updateQueue(request))
    ctx.effect(() => async () => {
      ctx.apiProxy.sessions.prompt = prompt
      ctx.apiProxy.sessions.rename = rename
      ctx.apiProxy.sessions.selectModel = selectModel
      ctx.apiProxy.sessions.updateQueue = updateQueue
      await this.pool.end()
    }, 'runAdmission.wrapPrompt')
  }
}

export default RunAdmission
