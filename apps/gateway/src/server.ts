import { randomUUID } from 'node:crypto'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { Duplex } from 'node:stream'
import { Pool, type PoolClient } from 'pg'
import { WebSocketServer } from 'ws'
import { ControlStore, type Principal, type WorkerRecord, type WorkspaceRecord } from '@dsh-cloud/control-store'
import { loginPage } from './login.js'
import { WorkerEventHub, type WorkerEventPath } from './worker-event-hub.js'

const MAX_BODY_BYTES = 160 * 1024 * 1024
const AUTH_COOKIE = 'dsh_cloud_session'
const CLOUD_WORKSPACE_ROOT = '/workspaces'
const PUBLIC_UI_ASSETS = new Set(['/manifest.webmanifest','/favicon.svg'])
const WELCOME_PREFERENCE_KEY = 'ui-onboarding.welcomeNoticeVersion'
const USER_SETTING_RULES = new Map<string,{field:string;valid:(value:unknown)=>boolean;preferenceKey:string}>([
  ['ui-onboarding',{field:'welcomeNoticeVersion',valid:value=>typeof value==='string'&&value.length<=100,preferenceKey:WELCOME_PREFERENCE_KEY}],
  ['ui-theme',{field:'preference',valid:value=>value==='light'||value==='dark'||value==='system',preferenceKey:'ui-theme.preference'}],
  ['locale',{field:'preference',valid:value=>value==='zh'||value==='en',preferenceKey:'locale.preference'}],
  ['ui-conversation',{field:'busyEnter',valid:value=>value==='queue'||value==='steer',preferenceKey:'ui-conversation.busyEnter'}],
])
const SESSION_METHODS = new Set(['session.history','session.models','session.selectModel','session.rename','session.attachment','session.updateQueue','session.cancel','session.prompt','session.fork','agentPreset.select'])
const READ_ONLY_HOST_METHODS = new Set([
  'host.describe',
  'skill.list',
  'agentPreset.list',
  'agentPreset.read',
  'settings.describe',
  'credentials.describe',
  'llm.providers',
  'llm.models',
  'llm.discoverModels',
])

interface Envelope { type: string; rpcId: string; method: string; payload: Record<string, unknown> }
interface ClientResponseEnvelope { type: 'client-response'; rpcId: string; result: { ok: boolean; value?: Record<string, unknown> } }
interface GatewayOptions {
  pool: Pool
  namespace: string
  publicOrigin?: string
  secureCookies: boolean
  eventProjectionTimeoutMs?: number
  promptAdmissionTimeoutMs?: number
  allowedAgentPresets?: readonly string[]
  toolBroker?: { url: string; token: string }
}

function cookies(request: IncomingMessage): Record<string, string> {
  const result: Record<string, string> = {}
  for (const part of (request.headers.cookie ?? '').split(';')) {
    const index = part.indexOf('=')
    if (index > 0) result[part.slice(0,index).trim()] = decodeURIComponent(part.slice(index+1).trim())
  }
  return result
}

async function bytes(request: IncomingMessage, maximum = MAX_BODY_BYTES): Promise<Buffer> {
  const output: Buffer[] = []; let size = 0
  for await (const chunk of request) { const value=Buffer.from(chunk); size+=value.byteLength; if(size>maximum) throw Object.assign(new Error('request too large'),{status:413}); output.push(value) }
  return Buffer.concat(output)
}

function json(response: ServerResponse, status: number, value: unknown, headers: Record<string,string> = {}): void {
  const body=Buffer.from(JSON.stringify(value)); response.writeHead(status,{'content-type':'application/json','content-length':String(body.byteLength),'cache-control':'no-store',...headers}); response.end(body)
}

function rpc(response: ServerResponse, envelope: Envelope, value: unknown): void { json(response,200,{type:'server-response',rpcId:envelope.rpcId,result:{ok:true,value}}) }
function rpcError(response: ServerResponse, envelope: Envelope, code: string, message: string, status=200): void { json(response,status,{type:'server-response',rpcId:envelope.rpcId,result:{ok:false,error:{code,message,details:{}}}}) }

function parseEnvelope(body: Buffer): Envelope {
  const value=JSON.parse(body.toString('utf8')) as Partial<Envelope>
  if(value.type!=='client-request'||typeof value.rpcId!=='string'||typeof value.method!=='string'||value.payload===null||typeof value.payload!=='object') throw Object.assign(new Error('invalid RPC envelope'),{status:400})
  return value as Envelope
}

function parseClientResponse(body: Buffer): ClientResponseEnvelope {
  const value=JSON.parse(body.toString('utf8')) as Partial<ClientResponseEnvelope>
  const result=value.result
  if(value.type!=='client-response'||typeof value.rpcId!=='string'||value.rpcId.length===0||result===null||typeof result!=='object'||typeof result.ok!=='boolean')throw Object.assign(new Error('invalid client response envelope'),{status:400})
  if(result.ok!==true||result.value===null||typeof result.value!=='object'||typeof result.value['sessionId']!=='string')throw Object.assign(new Error('client response has no Session authority'),{status:400})
  return value as ClientResponseEnvelope
}

function sessionId(envelope: Envelope): string | undefined { return typeof envelope.payload['sessionId']==='string' ? envelope.payload['sessionId'] : undefined }

function auxiliarySessionId(envelope:Envelope):string|undefined{
  if(envelope.method==='subagent.list'||envelope.method==='subagent.history')return typeof envelope.payload['parentSessionId']==='string'?envelope.payload['parentSessionId']:undefined
  if(envelope.method==='commands/list'){
    const args=envelope.payload['args']
    return args!==null&&typeof args==='object'&&typeof (args as Record<string,unknown>)['agentId']==='string'?(args as Record<string,unknown>)['agentId'] as string:undefined
  }
  if(envelope.method==='skill.list')return sessionId(envelope)
  return undefined
}

function cloudDirectoryPath(value: unknown): string | undefined {
  if(value===undefined)return CLOUD_WORKSPACE_ROOT
  if(typeof value!=='string')return undefined
  if(value===CLOUD_WORKSPACE_ROOT)return value
  const prefix=`${CLOUD_WORKSPACE_ROOT}/`
  const name=value.startsWith(prefix)?value.slice(prefix.length):''
  return name!==''&&!name.includes('/')&&!name.includes('\\')&&name!=='.'&&name!=='..'?value:undefined
}

async function copyResponse(upstream: Response, response: ServerResponse, transform?: (value: unknown)=>unknown): Promise<void> {
  const headers: Record<string,string>={}
  upstream.headers.forEach((value,key)=>{if(!['connection','transfer-encoding','content-length','set-cookie'].includes(key.toLowerCase())) headers[key]=value})
  if(transform!==undefined){const value=transform(await upstream.json()); json(response,upstream.status,value,headers);return}
  response.writeHead(upstream.status,headers)
  if(upstream.body===null){response.end();return}
  for await(const chunk of upstream.body){if(!response.write(chunk)) await new Promise<void>(resolve=>response.once('drain',resolve))}
  response.end()
}

function okValue(value: unknown): Record<string,unknown>|undefined {
  if(value===null||typeof value!=='object') return undefined
  const result=(value as Record<string,unknown>)['result']; if(result===null||typeof result!=='object'||(result as Record<string,unknown>)['ok']!==true)return undefined
  const answer=(result as Record<string,unknown>)['value']; return answer!==null&&typeof answer==='object' ? answer as Record<string,unknown> : undefined
}

function workspaceView(item:WorkspaceRecord):Record<string,unknown>{
  return {workspaceId:item.id,path:'/workspace',title:item.name,sessionIds:item.sessionIds,createdAt:item.createdAt,updatedAt:item.updatedAt}
}

export class CloudGateway {
  readonly store: ControlStore
  private readonly server: Server
  private readonly sockets = new WebSocketServer({ noServer: true })
  private readonly eventHub: WorkerEventHub
  private readonly allowedAgentPresets: ReadonlySet<string>
  private projectionClient: PoolClient | undefined
  private readonly projectionWaiters = new Map<string, Set<() => void>>()

  constructor(private readonly options: GatewayOptions) {
    if(options.eventProjectionTimeoutMs!==undefined&&(!Number.isSafeInteger(options.eventProjectionTimeoutMs)||options.eventProjectionTimeoutMs<1_000||options.eventProjectionTimeoutMs>300_000))throw new TypeError('eventProjectionTimeoutMs is invalid')
    if(options.promptAdmissionTimeoutMs!==undefined&&(!Number.isSafeInteger(options.promptAdmissionTimeoutMs)||options.promptAdmissionTimeoutMs<100||options.promptAdmissionTimeoutMs>300_000))throw new TypeError('promptAdmissionTimeoutMs is invalid')
    const configuredPresets=options.allowedAgentPresets??['standard','code']
    if(configuredPresets.some(id=>!/^[a-z0-9][a-z0-9-]*$/.test(id)))throw new TypeError('allowed Agent preset id is invalid')
    this.allowedAgentPresets=new Set(['standard',...configuredPresets])
    this.store=new ControlStore(options.pool,options.namespace)
    this.eventHub=new WorkerEventHub(this.store,(sessionId,seq,watermarks)=>this.waitDurable(sessionId,seq,watermarks))
    this.server=createServer((request,response)=>void this.handle(request,response).catch(error=>this.fail(response,error)))
    this.server.on('upgrade',(request,socket,head)=>void this.upgrade(request,socket,head))
  }

  async initialize(): Promise<void>{
    await this.store.initialize()
    this.projectionClient=await this.options.pool.connect()
    this.projectionClient.on('error',()=>{
      const failed=this.projectionClient;this.projectionClient=undefined;failed?.release(true)
      for(const waiters of this.projectionWaiters.values())for(const wake of waiters)wake()
      this.projectionWaiters.clear()
    })
    this.projectionClient.on('notification',message=>{
      if(message.channel!=='dsh_cloud_session_projection'||message.payload===undefined)return
      const waiters=this.projectionWaiters.get(message.payload);if(waiters===undefined)return
      this.projectionWaiters.delete(message.payload);for(const wake of waiters)wake()
    })
    await this.projectionClient.query('LISTEN dsh_cloud_session_projection')
  }
  listen(port:number,host:string):Promise<void>{return new Promise((resolve,reject)=>{this.server.once('error',reject);this.server.listen(port,host,()=>{this.server.off('error',reject);resolve()})})}
  address():{address:string;port:number}|undefined{const value=this.server.address();return value!==null&&typeof value==='object'?{address:value.address,port:value.port}:undefined}
  async close():Promise<void>{
    for(const socket of this.sockets.clients)socket.terminate()
    await this.eventHub.close()
    for(const waiters of this.projectionWaiters.values())for(const wake of waiters)wake()
    this.projectionWaiters.clear()
    if(this.projectionClient!==undefined){await this.projectionClient.query('UNLISTEN dsh_cloud_session_projection').catch(()=>undefined);this.projectionClient.release();this.projectionClient=undefined}
    await new Promise<void>((resolve,reject)=>this.server.close(error=>error===undefined?resolve():reject(error)))
  }

  private async principal(request:IncomingMessage):Promise<Principal|undefined>{const token=cookies(request)[AUTH_COOKIE];return token===undefined?undefined:this.store.authenticate(token)}
  private setCookie(token:string):string{return `${AUTH_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400${this.options.secureCookies?'; Secure':''}`}
  private clearCookie():string{return `${AUTH_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${this.options.secureCookies?'; Secure':''}`}
  private sameOrigin(request:IncomingMessage):boolean{const origin=request.headers.origin;if(origin===undefined)return true;const expected=this.options.publicOrigin??`${request.headers['x-forwarded-proto']??'http'}://${request.headers.host}`;return origin===expected}

  private async handle(request:IncomingMessage,response:ServerResponse):Promise<void>{
    const path=new URL(request.url??'/','http://gateway').pathname
    if(path==='/health/live'){json(response,200,{status:'live'});return}
    if(path==='/health/ready'){await this.options.pool.query('SELECT 1');json(response,200,{status:'ready'});return}
    if(path==='/metrics'){
      const metrics=await this.store.operationalMetrics();const body=[
        '# HELP dsh_cloud_runs Number of Runs by lifecycle class.','# TYPE dsh_cloud_runs gauge',
        `dsh_cloud_runs{state="queued"} ${metrics.queuedRuns}`,`dsh_cloud_runs{state="active"} ${metrics.activeRuns}`,`dsh_cloud_runs{state="failed"} ${metrics.failedRuns}`,
        '# TYPE dsh_cloud_healthy_workers gauge',`dsh_cloud_healthy_workers ${metrics.healthyWorkers}`,'# TYPE dsh_cloud_registered_tenants gauge',`dsh_cloud_registered_tenants ${metrics.registeredTenants}`,'',
      ].join('\n');response.writeHead(200,{'content-type':'text/plain; version=0.0.4','content-length':String(Buffer.byteLength(body))});response.end(body);return
    }
    if(path==='/cloud/login'||path==='/cloud/register'){
      if(request.method!=='POST'||!this.sameOrigin(request)){json(response,403,{error:'forbidden'});return}
      const input=JSON.parse((await bytes(request,64*1024)).toString('utf8')) as Record<string,unknown>
      const email=String(input['email']??''),password=String(input['password']??'')
      let principal:Principal|undefined
      if(path==='/cloud/register') principal=await this.store.register(String(input['name']??email),email,password)
      else principal=await this.store.login(email,password)
      if(principal===undefined){json(response,401,{error:'邮箱或密码错误'});return}
      const token=await this.store.issueAuthSession(principal.userId);json(response,200,{ok:true},{'set-cookie':this.setCookie(token)});return
    }
    const principal=await this.principal(request)
    if(path==='/cloud/logout'){
      if(request.method!=='POST'||!this.sameOrigin(request)){json(response,403,{error:'forbidden'});return}
      const token=cookies(request)[AUTH_COOKIE];if(token!==undefined)await this.store.revokeAuthSession(token);json(response,200,{ok:true},{'set-cookie':this.clearCookie()});return
    }
    if(path==='/cloud/me'){if(principal===undefined){json(response,401,{error:'unauthorized'});return}json(response,200,principal);return}
    if(principal===undefined&&request.method==='GET'&&PUBLIC_UI_ASSETS.has(path)){
      const worker=await this.store.selectWorker();if(worker===undefined){json(response,503,{error:'no healthy DSH Worker'});return}
      await this.proxy(worker,request,response,Buffer.alloc(0));return
    }
    if(principal===undefined){if(request.method==='GET'){response.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});response.end(loginPage)}else json(response,401,{error:'unauthorized'});return}
    if(path.startsWith('/api/')){
      const body=await bytes(request)
      try{await this.api(request,response,principal,path,body)}catch(error){
        console.error('Cloud Gateway RPC failed:',error instanceof Error?error.message:String(error))
        if(!response.headersSent){
          try{rpcError(response,parseEnvelope(body),'internal','Cloud Gateway request failed',500)}catch{this.fail(response,error)}
        }
      }
      return
    }
    if(request.method!=='GET'&&request.method!=='HEAD'){json(response,405,{error:'method not allowed'});return}
    const worker=await this.store.selectWorker()
    if(worker===undefined){json(response,503,{error:'no healthy DSH Worker'});return}
    await this.proxy(worker,request,response,await bytes(request))
  }

  private async api(request:IncomingMessage,response:ServerResponse,principal:Principal,path:string,body:Buffer):Promise<void>{
    if(request.method!=='POST'){json(response,405,{error:'method not allowed'});return}
    if(path==='/api/respond'){await this.respond(request,response,principal,body);return}
    const envelope=parseEnvelope(body)
    if(!this.sameOrigin(request)){rpcError(response,envelope,'bad-request','Request origin was rejected',403);return}
    if(path!==`/api/${envelope.method}`){rpcError(response,envelope,'bad-request','RPC method does not match its HTTP route',400);return}
    if(envelope.method.startsWith('workspace.')){await this.workspace(response,principal,envelope);return}
    if(envelope.method==='host.listDirectory'||envelope.method==='host.createDirectory'){this.cloudDirectory(response,envelope);return}
    if(envelope.method==='dynamicCordisRunner/inventory'){rpc(response,envelope,[]);return}
    if(envelope.method==='dynamicCordisRunner/syncInspectManifest'){rpc(response,envelope,null);return}
    const auxiliarySid=auxiliarySessionId(envelope)
    if(auxiliarySid!==undefined&&!await this.store.ownsSession(principal.tenantId,auxiliarySid)){rpcError(response,envelope,'session-not-found','Session was not found');return}
    // This cloud profile exposes no user-installed slash commands or Skills.
    // Returning an empty capability set makes the upstream UI hide the trigger
    // instead of advertising operations that bypass durable Run admission.
    if(envelope.method==='commands/list'){rpc(response,envelope,[]);return}
    if(envelope.method==='skill.list'){rpc(response,envelope,{skills:[]});return}
    const sid=sessionId(envelope)
    if(SESSION_METHODS.has(envelope.method)&&(sid===undefined||!await this.store.ownsSession(principal.tenantId,sid))){rpcError(response,envelope,'session-not-found','Session was not found');return}
    if(envelope.method==='agentPreset.select'){
      const agentPreset=envelope.payload['agentPreset']
      if(typeof agentPreset!=='string'||!this.allowedAgentPresets.has(agentPreset)){rpcError(response,envelope,'agent-preset-not-found','Agent preset is not enabled for this cloud deployment');return}
    }
    if(envelope.method==='session.prompt'){
      const enqueued=await this.store.enqueueRun({tenantId:principal.tenantId,sessionId:sid!,clientRpcId:envelope.rpcId,idempotencyKey:String(request.headers['idempotency-key']??envelope.rpcId),request:envelope})
      const admission=await this.waitForPromptAdmission(enqueued.runId,sid!)
      if(admission==='failed'){
        rpcError(response,envelope,'internal','The prompt could not be delivered. Your draft was kept; retry the message.')
        return
      }
      rpc(response,envelope,{accepted:true,runId:enqueued.runId});return
    }
    if(envelope.method==='session.cancel'){await this.store.requestSessionCancellation(principal.tenantId,sid!);rpc(response,envelope,{accepted:true});return}
    if(envelope.method==='session.rename'||envelope.method==='session.selectModel'||envelope.method==='agentPreset.select')await this.store.issueSessionCommand(principal.tenantId,sid!,envelope.rpcId)
    if(envelope.method==='session.updateQueue')await this.store.issueSessionCommand(principal.tenantId,sid!,envelope.rpcId,true)
    const worker=sid===undefined
      ?await this.store.selectWorker()
      :(await this.store.activeRunWorker(principal.tenantId,sid))??await this.store.selectWorker()
    if(worker===undefined){rpcError(response,envelope,'internal','No healthy DSH Worker is available',503);return}
    if(envelope.method==='settings.describe'||envelope.method==='settings.mutate'){await this.cloudSettings(response,principal,worker,envelope,request);return}
    if(envelope.method==='host.describe'){
      const upstream=await this.fetchWorker(worker,path,request,body)
      await copyResponse(upstream,response,value=>{const answer=okValue(value);if(answer!==undefined){answer['cwd']=CLOUD_WORKSPACE_ROOT;answer['canOpenPath']=false}return value});return
    }
    if((envelope.method==='subagent.list'||envelope.method==='subagent.history')&&auxiliarySid!==undefined){
      const prepared=await this.fetchWorker(worker,'/api/session.models',request,Buffer.from(JSON.stringify({type:'client-request',rpcId:randomUUID(),method:'session.models',payload:{sessionId:auxiliarySid}})))
      const preparation=await prepared.json()
      if(okValue(preparation)===undefined){json(response,prepared.status,preparation);return}
      await copyResponse(await this.fetchWorker(worker,path,request,body),response);return
    }
    if(envelope.method==='session.create'){
      const agentPreset=envelope.payload['agentPreset']
      if(agentPreset!==undefined&&(typeof agentPreset!=='string'||!this.allowedAgentPresets.has(agentPreset))){rpcError(response,envelope,'agent-preset-not-found','Agent preset is not enabled for this cloud deployment');return}
      const requested=typeof envelope.payload['workspaceId']==='string'&&await this.store.workspaceOwned(principal.tenantId,envelope.payload['workspaceId'])?envelope.payload['workspaceId']:undefined
      const workspaceId=requested??(await this.store.ensureDefaultWorkspace(principal.tenantId)).id
      const allocated=typeof envelope.payload['sessionId']==='string'?envelope.payload['sessionId']:randomUUID()
      const forwarded:Envelope={...envelope,payload:{...envelope.payload,sessionId:allocated,cwd:'/workspace'}};delete forwarded.payload['workspaceId']
      const upstream=await this.fetchWorker(worker,path,request,Buffer.from(JSON.stringify(forwarded)))
      const payload=await upstream.json();const value=okValue(payload)
      if(value!==undefined&&typeof value['sessionId']==='string'){
        await this.store.registerSession({sessionId:value['sessionId'],tenantId:principal.tenantId,workspaceId})
        this.eventHub.allowSession(principal.tenantId,value['sessionId'])
        await this.publishWorkspace(principal.tenantId,workspaceId)
      }
      json(response,upstream.status,payload);return
    }
    if(envelope.method==='session.fork'){
      const workspace=await this.store.sessionWorkspace(principal.tenantId,sid!)
      const upstream=await this.fetchWorker(worker,path,request,body);const payload=await upstream.json();const value=okValue(payload)
      if(workspace!==undefined&&value!==undefined&&typeof value['sessionId']==='string'){
        await this.store.registerSession({sessionId:value['sessionId'],tenantId:principal.tenantId,workspaceId:workspace.workspaceId})
        this.eventHub.allowSession(principal.tenantId,value['sessionId'])
        await this.publishWorkspace(principal.tenantId,workspace.workspaceId)
      }
      json(response,upstream.status,payload);return
    }
    if(envelope.method==='session.list'||envelope.method==='session.search'){
      const allowed=await this.store.listSessionIds(principal.tenantId)
      const upstream=await this.fetchWorker(worker,path,request,body)
      await copyResponse(upstream,response,value=>{const answer=okValue(value);if(answer!==undefined&&Array.isArray(answer['items']))answer['items']=(answer['items'] as Array<Record<string,unknown>>).filter(item=>typeof item['sessionId']==='string'&&allowed.has(item['sessionId']));return value});return
    }
    if(envelope.method==='agentPreset.select'){
      await copyResponse(await this.fetchWorker(worker,path,request,body),response)
      return
    }
    if(SESSION_METHODS.has(envelope.method)){
      await copyResponse(await this.fetchWorker(worker,path,request,body),response)
      return
    }
    if(envelope.method==='agentPreset.list'){
      const upstream=await this.fetchWorker(worker,path,request,body)
      await copyResponse(upstream,response,value=>{
        const answer=okValue(value)
        if(answer===undefined||!Array.isArray(answer['presets']))return value
        answer['presets']=(answer['presets'] as Array<Record<string,unknown>>).filter(preset=>preset['trust']==='system'&&typeof preset['id']==='string'&&this.allowedAgentPresets.has(preset['id'])).map(preset=>({...preset,description:preset['id']==='code'?'云端 PTC 编码模式；文件、命令和测试在隔离执行环境中运行。':'云端标准编码模式；对话可跨 Worker 恢复，工具调用进入隔离执行环境。'}))
        answer['authorable']=false
        answer['hasDocument']=false
        return value
      });return
    }
    if(envelope.method==='agentPreset.read'){
      const agentPreset=envelope.payload['agentPreset']
      if(typeof agentPreset!=='string'||!this.allowedAgentPresets.has(agentPreset)){rpcError(response,envelope,'agent-preset-not-found','Agent preset is not enabled for this cloud deployment');return}
    }
    if(!READ_ONLY_HOST_METHODS.has(envelope.method)){
      const code=envelope.method.startsWith('settings.')?'settings-not-exposed':envelope.method.startsWith('credentials.')?'credential-rejected':'bad-request'
      rpcError(response,envelope,code,'This cloud deployment does not expose that Host operation');return
    }
    await copyResponse(await this.fetchWorker(worker,path,request,body),response)
  }

  private async respond(request:IncomingMessage,response:ServerResponse,principal:Principal,body:Buffer):Promise<void>{
    if(!this.sameOrigin(request)){json(response,403,{accepted:false,reason:'bad-response'});return}
    let envelope:ClientResponseEnvelope
    try{envelope=parseClientResponse(body)}catch{json(response,200,{accepted:false,reason:'bad-response'});return}
    const sid=envelope.result.value!['sessionId'] as string
    if(!await this.store.ownsSession(principal.tenantId,sid)){json(response,200,{accepted:false,reason:'not-pending'});return}
    const worker=await this.store.activeRunWorker(principal.tenantId,sid)
    if(worker===undefined){json(response,200,{accepted:false,reason:'not-pending'});return}
    await copyResponse(await this.fetchWorker(worker,'/api/respond',request,body),response)
  }

  private cloudDirectory(response:ServerResponse,envelope:Envelope):void{
    if(envelope.method==='host.listDirectory'){
      const path=cloudDirectoryPath(envelope.payload['path'])
      if(path===undefined){rpcError(response,envelope,'directory-unreadable','Cloud Workspace paths must be direct children of /workspaces');return}
      const name=path===CLOUD_WORKSPACE_ROOT?'Workspaces':path.slice(CLOUD_WORKSPACE_ROOT.length+1)
      const crumbs=[{name:'Workspaces',path:CLOUD_WORKSPACE_ROOT,hidden:false}]
      if(path!==CLOUD_WORKSPACE_ROOT)crumbs.push({name,path,hidden:false})
      rpc(response,envelope,{path,home:CLOUD_WORKSPACE_ROOT,crumbs,entries:[],truncated:false});return
    }
    const parent=cloudDirectoryPath(envelope.payload['path'])
    const name=typeof envelope.payload['name']==='string'?envelope.payload['name'].trim():''
    if(parent!==CLOUD_WORKSPACE_ROOT||name===''||name==='.'||name==='..'||name.includes('/')||name.includes('\\')||name.length>120){
      rpcError(response,envelope,'directory-create-failed','Choose /workspaces and enter a single Workspace name of at most 120 characters');return
    }
    rpc(response,envelope,{path:`${CLOUD_WORKSPACE_ROOT}/${name}`})
  }

  private async cloudSettings(response:ServerResponse,principal:Principal,worker:WorkerRecord,envelope:Envelope,request:IncomingMessage):Promise<void>{
    if(envelope.method==='settings.mutate'){
      const operations=envelope.payload['ops']
      const operation=Array.isArray(operations)&&operations.length===1?operations[0] as Record<string,unknown>:undefined
      const path=operation?.['path']
      const value=operation?.['value']
      const expected=envelope.payload['expectedRevision']
      const ns=typeof envelope.payload['ns']==='string'?envelope.payload['ns']:''
      const rule=USER_SETTING_RULES.get(ns)
      if(rule===undefined||operation?.['op']!=='set'||!Array.isArray(path)||path.length!==1||path[0]!==rule.field||!rule.valid(value)||!(expected===undefined||Number.isSafeInteger(expected))){
        rpcError(response,envelope,'settings-not-exposed','Only safe per-user interface settings are writable in this cloud profile');return
      }
      const saved=await this.store.setUserPreference(principal.userId,rule.preferenceKey,value,expected as number|undefined)
      if(saved===undefined){rpcError(response,envelope,'settings-conflict','Interface settings were changed by another request');return}
      const view=await this.userSettingsView(worker,request,envelope,ns,saved)
      rpc(response,envelope,view);return
    }
    const upstream=await this.fetchSettingsDescription(worker,request,envelope)
    const payload=await upstream.json() as Record<string,unknown>
    const value=okValue(payload)
    if(value===undefined||!Array.isArray(value['namespaces']))throw new Error('Worker returned an invalid settings descriptor')
    const namespaces=(value['namespaces'] as Array<Record<string,unknown>>).filter(item=>typeof item['ns']==='string'&&USER_SETTING_RULES.has(item['ns']))
    for(const view of namespaces){
      const ns=view['ns'] as string
      const rule=USER_SETTING_RULES.get(ns)!
      this.applyUserPreference(view,rule,await this.store.userPreference(principal.userId,rule.preferenceKey))
    }
    value['namespaces']=namespaces
    value['writable']=true
    value['hasDocument']=false
    json(response,upstream.status,payload)
  }

  private async userSettingsView(worker:WorkerRecord,request:IncomingMessage,envelope:Envelope,ns:string,preference:{value:unknown;revision:number}):Promise<Record<string,unknown>>{
    const upstream=await this.fetchSettingsDescription(worker,request,envelope)
    const payload=await upstream.json()
    const value=okValue(payload)
    const view=value!==undefined&&Array.isArray(value['namespaces'])?(value['namespaces'] as Array<Record<string,unknown>>).find(item=>item['ns']===ns):undefined
    const rule=USER_SETTING_RULES.get(ns)
    if(view===undefined||rule===undefined)throw new Error(`Worker did not expose ${ns} settings`)
    this.applyUserPreference(view,rule,preference)
    return view
  }

  private fetchSettingsDescription(worker:WorkerRecord,request:IncomingMessage,envelope:Envelope):Promise<Response>{
    const forwarded:Envelope={type:'client-request',rpcId:envelope.rpcId,method:'settings.describe',payload:{}}
    return this.fetchWorker(worker,'/api/settings.describe',request,Buffer.from(JSON.stringify(forwarded)))
  }

  private applyUserPreference(view:Record<string,unknown>,rule:{field:string;valid:(value:unknown)=>boolean},preference:{value:unknown;revision:number}|undefined):void{
    if(preference===undefined){delete view['user'];view['revision']=0;return}
    const value=rule.valid(preference.value)?{[rule.field]:preference.value}:{}
    view['value']={...(view['value']!==null&&typeof view['value']==='object'?view['value'] as Record<string,unknown>:{}),...value}
    view['user']=value
    view['revision']=preference?.revision??0
  }

  private async workspace(response:ServerResponse,principal:Principal,envelope:Envelope):Promise<void>{
    if(envelope.method==='workspace.list'){const items=(await this.store.listWorkspaces(principal.tenantId)).map(workspaceView);rpc(response,envelope,{items,archivedSessionIds:await this.archivedSessions(principal)});return}
    if(envelope.method==='workspace.create'){const raw=String(envelope.payload['path']??'Workspace');const name=raw.split(/[\\/]/).filter(Boolean).at(-1)??'Workspace';const created=await this.store.createWorkspace(principal.tenantId,`${name}-${randomUUID().slice(0,6)}`);const item=(await this.store.listWorkspaces(principal.tenantId)).find(value=>value.id===created.id)!;const workspace=workspaceView(item);this.eventHub.publishHost(principal.tenantId,{type:'host/workspace-changed',workspace});rpc(response,envelope,{workspace,created:true});return}
    if(envelope.method==='workspace.rename'){const item=await this.store.renameWorkspace(principal.tenantId,String(envelope.payload['workspaceId']??''),String(envelope.payload['title']??''));if(item===undefined){rpcError(response,envelope,'workspace-not-found','Workspace was not found');return}const workspace=workspaceView(item);this.eventHub.publishHost(principal.tenantId,{type:'host/workspace-changed',workspace});rpc(response,envelope,{workspace});return}
    if(envelope.method==='workspace.delete'){
      const workspaceId=String(envelope.payload['workspaceId']??'')
      const reserved=await this.store.beginWorkspaceDeletion(principal.tenantId,workspaceId)
      if(!reserved){rpcError(response,envelope,'workspace-not-found','Workspace is not empty or was not found');return}
      await this.destroyWorkspace(principal.tenantId,workspaceId)
      this.eventHub.publishHost(principal.tenantId,{type:'host/workspace-removed',workspaceId})
      rpc(response,envelope,{deleted:true});return
    }
    if(envelope.method==='workspace.archiveSession'){
      const archived=String(envelope.payload['sessionId']??'')
      if(!await this.store.ownsSession(principal.tenantId,archived)){rpcError(response,envelope,'session-not-found','Session was not found');return}
      const archivedSessionIds=await this.mutateUserStringList(principal,'workspace.archived-session-ids',current=>current.includes(archived)?current:[...current,archived])
      this.eventHub.publishHost(principal.tenantId,{type:'host/archived-sessions-changed',archivedSessionIds})
      rpc(response,envelope,{archivedSessionIds});return
    }
    rpcError(response,envelope,'forbidden','Workspace ordering and archive operations are not available in this cloud profile')
  }

  private async publishWorkspace(tenantId:string,workspaceId:string):Promise<void>{
    const item=(await this.store.listWorkspaces(tenantId)).find(candidate=>candidate.id===workspaceId)
    if(item!==undefined)this.eventHub.publishHost(tenantId,{type:'host/workspace-changed',workspace:workspaceView(item)})
  }

  private async archivedSessions(principal:Principal):Promise<string[]>{
    const preference=await this.store.userPreference(principal.userId,'workspace.archived-session-ids')
    return Array.isArray(preference?.value)?preference.value.filter((value):value is string=>typeof value==='string'):[]
  }

  private async mutateUserStringList(principal:Principal,key:string,change:(current:string[])=>string[]):Promise<string[]>{
    for(let attempt=0;attempt<5;attempt++){
      const current=await this.store.userPreference(principal.userId,key)
      const values=Array.isArray(current?.value)?current.value.filter((value):value is string=>typeof value==='string'):[]
      const next=change(values)
      const saved=await this.store.setUserPreference(principal.userId,key,next,current?.revision??0)
      if(saved!==undefined)return saved.value as string[]
    }
    throw new Error('concurrent user Workspace preference update did not converge')
  }

  private async fetchWorker(worker:WorkerRecord,path:string,request:IncomingMessage,body:Buffer):Promise<Response>{
    const headers=new Headers();for(const [key,value] of Object.entries(request.headers)){if(value!==undefined&&!['host','cookie','connection','content-length','origin'].includes(key))headers.set(key,Array.isArray(value)?value.join(', '):value)}
    return fetch(`${worker.baseUrl}${path}`,{method:request.method??'GET',headers,...(body.byteLength===0?{}:{body}),signal:AbortSignal.timeout(180_000)})
  }
  private async proxy(worker:WorkerRecord,request:IncomingMessage,response:ServerResponse,body:Buffer):Promise<void>{await copyResponse(await this.fetchWorker(worker,new URL(request.url??'/','http://gateway').pathname+new URL(request.url??'/','http://gateway').search,request,body),response)}

  private async destroyWorkspace(tenantId:string,workspaceId:string):Promise<void>{
    const broker=this.options.toolBroker
    if(broker===undefined)throw Object.assign(new Error('Tool Broker is not configured for Workspace deletion'),{status:503})
    const response=await fetch(`${broker.url.replace(/\/$/,'')}/v1/workspaces/destroy`,{
      method:'POST',headers:{authorization:`Bearer ${broker.token}`,'content-type':'application/json'},
      body:JSON.stringify({tenantId,workspaceId}),signal:AbortSignal.timeout(120_000),
    })
    if(!response.ok){await response.body?.cancel();throw Object.assign(new Error('Tool Broker could not destroy the Workspace'),{status:503})}
    await response.body?.cancel()
  }

  private async upgrade(request:IncomingMessage,socket:Duplex,head:Buffer):Promise<void>{
    try{
      if(!this.sameOrigin(request))throw new Error('origin rejected')
      const principal=await this.principal(request);if(principal===undefined)throw new Error('unauthorized')
      const path=new URL(request.url??'/','http://gateway').pathname;if(path!=='/api/events.mux'&&path!=='/api/events.host')throw new Error('unknown websocket')
      const allowed=await this.store.listSessionIds(principal.tenantId)
      this.sockets.handleUpgrade(request,socket,head,browser=>{
        this.eventHub.subscribe(path as WorkerEventPath,principal.tenantId,allowed,browser)
      })
    }catch{if(!socket.destroyed)socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 9\r\n\r\nforbidden')}
  }
  private async waitDurable(sessionId:string,seq:number,watermarks:Map<string,number>):Promise<void>{
    if((watermarks.get(sessionId)??-1)>=seq)return
    const deadline=Date.now()+(this.options.eventProjectionTimeoutMs??90_000)
    while(Date.now()<deadline){const through=await this.store.sessionDurableThrough(sessionId);watermarks.set(sessionId,through);if(through>=seq)return;await this.waitProjectionSignal(sessionId,Math.min(100,deadline-Date.now()))}
    throw new Error('Session event did not cross the durable live-projection barrier')
  }
  private async waitForPromptAdmission(runId:string,sessionId:string):Promise<'persisted'|'pending'|'failed'>{
    const deadline=Date.now()+(this.options.promptAdmissionTimeoutMs??35_000)
    while(true){
      if(await this.store.promptPersisted(runId))return 'persisted'
      const run=await this.store.runResponse(runId)
      if(run===undefined||['failed','cancelled','timed_out'].includes(run.status))return 'failed'
      const remaining=deadline-Date.now()
      if(remaining<=0)return 'pending'
      await this.waitProjectionSignal(sessionId,Math.min(250,remaining))
    }
  }
  private waitProjectionSignal(sessionId:string,timeoutMs:number):Promise<void>{
    if(timeoutMs<=0)return Promise.resolve()
    return new Promise(resolve=>{
      let waiters=this.projectionWaiters.get(sessionId);if(waiters===undefined){waiters=new Set();this.projectionWaiters.set(sessionId,waiters)}
      let timer:NodeJS.Timeout
      const wake=()=>{clearTimeout(timer);waiters!.delete(wake);if(waiters!.size===0)this.projectionWaiters.delete(sessionId);resolve()}
      waiters.add(wake);timer=setTimeout(wake,timeoutMs)
    })
  }
  private fail(response:ServerResponse,error:unknown):void{if(response.headersSent){response.destroy();return}const status=typeof error==='object'&&error!==null&&'status'in error?Number((error as {status:unknown}).status):500;json(response,status,{error:status===500?'internal gateway error':error instanceof Error?error.message:'request failed'})}
}
