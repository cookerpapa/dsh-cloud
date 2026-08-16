import { randomUUID } from 'node:crypto'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import type { Duplex } from 'node:stream'
import { setTimeout as delay } from 'node:timers/promises'
import { Pool } from 'pg'
import WebSocket, { WebSocketServer } from 'ws'
import { ControlStore, type Principal, type WorkerRecord } from '@dsh-cloud/control-store'
import { loginPage } from './login.js'

const MAX_BODY_BYTES = 160 * 1024 * 1024
const AUTH_COOKIE = 'dsh_cloud_session'
const SESSION_METHODS = new Set(['session.history','session.models','session.selectModel','session.rename','session.attachment','session.updateQueue','session.cancel','session.prompt','session.fork'])
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
interface GatewayOptions {
  pool: Pool
  namespace: string
  publicOrigin?: string
  secureCookies: boolean
  sandboxManager?: { url: string; token: string }
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

function sessionId(envelope: Envelope): string | undefined { return typeof envelope.payload['sessionId']==='string' ? envelope.payload['sessionId'] : undefined }

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

export class CloudGateway {
  readonly store: ControlStore
  private readonly server: Server
  private readonly sockets = new WebSocketServer({ noServer: true })

  constructor(private readonly options: GatewayOptions) {
    this.store=new ControlStore(options.pool,options.namespace)
    this.server=createServer((request,response)=>void this.handle(request,response).catch(error=>this.fail(response,error)))
    this.server.on('upgrade',(request,socket,head)=>void this.upgrade(request,socket,head))
  }

  async initialize(): Promise<void>{await this.store.initialize()}
  listen(port:number,host:string):Promise<void>{return new Promise((resolve,reject)=>{this.server.once('error',reject);this.server.listen(port,host,()=>{this.server.off('error',reject);resolve()})})}
  address():{address:string;port:number}|undefined{const value=this.server.address();return value!==null&&typeof value==='object'?{address:value.address,port:value.port}:undefined}
  close():Promise<void>{for(const socket of this.sockets.clients)socket.terminate();return new Promise((resolve,reject)=>this.server.close(error=>error===undefined?resolve():reject(error)))}

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
    if(principal===undefined){if(request.method==='GET'){response.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});response.end(loginPage)}else json(response,401,{error:'unauthorized'});return}
    if(path.startsWith('/api/')){await this.api(request,response,principal,path,await bytes(request));return}
    if(request.method!=='GET'&&request.method!=='HEAD'){json(response,405,{error:'method not allowed'});return}
    const worker=await this.store.routeWorker(principal.userId)
    if(worker===undefined){json(response,503,{error:'no healthy DSH Worker'});return}
    await this.proxy(worker,request,response,await bytes(request))
  }

  private async api(request:IncomingMessage,response:ServerResponse,principal:Principal,path:string,body:Buffer):Promise<void>{
    if(request.method!=='POST'){json(response,405,{error:'method not allowed'});return}
    const envelope=parseEnvelope(body)
    if(path!==`/api/${envelope.method}`){rpcError(response,envelope,'invalid-request','RPC method does not match its HTTP route',400);return}
    if(envelope.method.startsWith('workspace.')){await this.workspace(response,principal,envelope);return}
    const worker=await this.store.routeWorker(principal.userId)
    if(worker===undefined){rpcError(response,envelope,'unavailable','No healthy DSH Worker is available',503);return}
    const sid=sessionId(envelope)
    if(SESSION_METHODS.has(envelope.method)&&(sid===undefined||!await this.store.ownsSession(principal.tenantId,sid))){rpcError(response,envelope,'session-not-found','Session was not found');return}
    if(envelope.method==='session.prompt'){
      await this.store.preferSessionWorker(principal.tenantId,sid!,worker.id)
      const enqueued=await this.store.enqueueRun({tenantId:principal.tenantId,sessionId:sid!,clientRpcId:envelope.rpcId,idempotencyKey:String(request.headers['idempotency-key']??envelope.rpcId),request:envelope})
      rpc(response,envelope,{accepted:true,runId:enqueued.runId});return
    }
    if(envelope.method==='session.cancel'){await this.store.requestSessionCancellation(principal.tenantId,sid!);rpc(response,envelope,{accepted:true});return}
    if(envelope.method==='session.rename'||envelope.method==='session.selectModel')await this.store.issueSessionCommand(principal.tenantId,sid!,envelope.rpcId)
    if(envelope.method==='session.updateQueue')await this.store.issueSessionCommand(principal.tenantId,sid!,envelope.rpcId,true)
    if(envelope.method==='session.create'){
      const requested=typeof envelope.payload['workspaceId']==='string'&&await this.store.workspaceOwned(principal.tenantId,envelope.payload['workspaceId'])?envelope.payload['workspaceId']:undefined
      const workspaceId=requested??(await this.store.ensureDefaultWorkspace(principal.tenantId)).id
      const allocated=typeof envelope.payload['sessionId']==='string'?envelope.payload['sessionId']:randomUUID()
      const forwarded:Envelope={...envelope,payload:{...envelope.payload,sessionId:allocated,cwd:'/workspace'}};delete forwarded.payload['workspaceId']
      const upstream=await this.fetchWorker(worker,path,request,Buffer.from(JSON.stringify(forwarded)))
      const payload=await upstream.json();const value=okValue(payload)
      if(value!==undefined&&typeof value['sessionId']==='string'){await this.store.registerSession({sessionId:value['sessionId'],tenantId:principal.tenantId,workspaceId,preferredWorkerId:worker.id})}
      json(response,upstream.status,payload);return
    }
    if(envelope.method==='session.fork'){
      const placement=await this.store.sessionPlacement(principal.tenantId,sid!)
      const upstream=await this.fetchWorker(worker,path,request,body);const payload=await upstream.json();const value=okValue(payload)
      if(placement!==undefined&&value!==undefined&&typeof value['sessionId']==='string'){await this.store.registerSession({sessionId:value['sessionId'],tenantId:principal.tenantId,workspaceId:placement.workspaceId,preferredWorkerId:worker.id})}
      json(response,upstream.status,payload);return
    }
    if(envelope.method==='session.list'||envelope.method==='session.search'){
      const allowed=await this.store.listSessionIds(principal.tenantId)
      const upstream=await this.fetchWorker(worker,path,request,body)
      await copyResponse(upstream,response,value=>{const answer=okValue(value);if(answer!==undefined&&Array.isArray(answer['items']))answer['items']=(answer['items'] as Array<Record<string,unknown>>).filter(item=>typeof item['sessionId']==='string'&&allowed.has(item['sessionId']));return value});return
    }
    if(SESSION_METHODS.has(envelope.method)){
      await copyResponse(await this.fetchWorker(worker,path,request,body),response)
      return
    }
    if(!READ_ONLY_HOST_METHODS.has(envelope.method)){rpcError(response,envelope,'forbidden','This cloud deployment does not expose that Host operation');return}
    await copyResponse(await this.fetchWorker(worker,path,request,body),response)
  }

  private async workspace(response:ServerResponse,principal:Principal,envelope:Envelope):Promise<void>{
    if(envelope.method==='workspace.list'){const items=(await this.store.listWorkspaces(principal.tenantId)).map(item=>({workspaceId:item.id,path:'/workspace',title:item.name,sessionIds:item.sessionIds,createdAt:item.createdAt,updatedAt:item.updatedAt}));rpc(response,envelope,{items,archivedSessionIds:[]});return}
    if(envelope.method==='workspace.create'){const raw=String(envelope.payload['path']??'Workspace');const name=raw.split(/[\\/]/).filter(Boolean).at(-1)??'Workspace';const created=await this.store.createWorkspace(principal.tenantId,`${name}-${randomUUID().slice(0,6)}`);const item=(await this.store.listWorkspaces(principal.tenantId)).find(value=>value.id===created.id)!;rpc(response,envelope,{workspace:{workspaceId:item.id,path:'/workspace',title:item.name,sessionIds:[],createdAt:item.createdAt,updatedAt:item.updatedAt},created:true});return}
    if(envelope.method==='workspace.rename'){const item=await this.store.renameWorkspace(principal.tenantId,String(envelope.payload['workspaceId']??''),String(envelope.payload['title']??''));if(item===undefined){rpcError(response,envelope,'workspace-not-found','Workspace was not found');return}rpc(response,envelope,{workspace:{workspaceId:item.id,path:'/workspace',title:item.name,sessionIds:item.sessionIds,createdAt:item.createdAt,updatedAt:item.updatedAt}});return}
    if(envelope.method==='workspace.delete'){
      const workspaceId=String(envelope.payload['workspaceId']??'')
      const reserved=await this.store.beginWorkspaceDeletion(principal.tenantId,workspaceId)
      if(!reserved){rpcError(response,envelope,'workspace-not-found','Workspace is not empty or was not found');return}
      await this.destroyWorkspace(principal.tenantId,workspaceId)
      rpc(response,envelope,{deleted:true});return
    }
    rpcError(response,envelope,'forbidden','Workspace ordering and archive operations are not available in this cloud profile')
  }

  private async fetchWorker(worker:WorkerRecord,path:string,request:IncomingMessage,body:Buffer):Promise<Response>{
    const headers=new Headers();for(const [key,value] of Object.entries(request.headers)){if(value!==undefined&&!['host','cookie','connection','content-length'].includes(key))headers.set(key,Array.isArray(value)?value.join(', '):value)}
    return fetch(`${worker.baseUrl}${path}`,{method:request.method??'GET',headers,...(body.byteLength===0?{}:{body}),signal:AbortSignal.timeout(180_000)})
  }
  private async proxy(worker:WorkerRecord,request:IncomingMessage,response:ServerResponse,body:Buffer):Promise<void>{await copyResponse(await this.fetchWorker(worker,new URL(request.url??'/','http://gateway').pathname+new URL(request.url??'/','http://gateway').search,request,body),response)}

  private async destroyWorkspace(tenantId:string,workspaceId:string):Promise<void>{
    const manager=this.options.sandboxManager
    if(manager===undefined)throw Object.assign(new Error('Sandbox Manager is not configured for Workspace deletion'),{status:503})
    const response=await fetch(`${manager.url.replace(/\/$/,'')}/v1/workspaces/destroy`,{
      method:'POST',headers:{authorization:`Bearer ${manager.token}`,'content-type':'application/json'},
      body:JSON.stringify({tenantId,workspaceId}),signal:AbortSignal.timeout(120_000),
    })
    if(!response.ok){await response.body?.cancel();throw Object.assign(new Error('Sandbox Manager could not destroy the Workspace'),{status:503})}
    await response.body?.cancel()
  }

  private async upgrade(request:IncomingMessage,socket:Duplex,head:Buffer):Promise<void>{
    try{
      if(!this.sameOrigin(request))throw new Error('origin rejected')
      const principal=await this.principal(request);if(principal===undefined)throw new Error('unauthorized')
      const path=new URL(request.url??'/','http://gateway').pathname;if(path!=='/api/events.mux'&&path!=='/api/events.host')throw new Error('unknown websocket')
      const worker=await this.store.routeWorker(principal.userId);if(worker===undefined)throw new Error('no worker')
      const allowed=await this.store.listSessionIds(principal.tenantId)
      this.sockets.handleUpgrade(request,socket,head,browser=>{
        const url=new URL(worker.baseUrl);url.protocol=url.protocol==='https:'?'wss:':'ws:';url.pathname=path
        const upstream=new WebSocket(url)
        const durableWatermark=new Map<string,number>()
        let delivery=Promise.resolve()
        upstream.on('message',(data,isBinary)=>{delivery=delivery.then(async()=>{if(isBinary||browser.readyState!==WebSocket.OPEN)return;try{const value=JSON.parse(data.toString()) as Record<string,unknown>;const payload=value['payload'] as Record<string,unknown>|undefined;const sid=payload&&typeof payload['sessionId']==='string'?payload['sessionId']:undefined;if(sid!==undefined&&!allowed.has(sid)){if(await this.store.ownsSession(principal.tenantId,sid))allowed.add(sid);else return}const type=String(payload?.['type']??'');if(type==='host/remote-event'||type.startsWith('host/workspace-')||type==='host/archived-sessions-changed')return;if(payload?.['type']==='session/event'&&sid!==undefined){const event=payload['event'] as Record<string,unknown>|undefined;if(event===undefined||!Number.isSafeInteger(event['seq']))return;await this.waitDurable(sid,event['seq'] as number,durableWatermark)}browser.send(data)}catch{browser.close(1011,'durability barrier failed')}})})
        upstream.on('close',()=>browser.close());upstream.on('error',()=>browser.close(1011,'upstream unavailable'));browser.on('close',()=>upstream.close());browser.on('message',()=>browser.close(1008,'downlink only'))
      })
    }catch{socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 9\r\n\r\nforbidden')}
  }
  private async waitDurable(sessionId:string,seq:number,watermarks:Map<string,number>):Promise<void>{
    if((watermarks.get(sessionId)??-1)>=seq)return
    const deadline=Date.now()+10_000
    while(Date.now()<deadline){const through=await this.store.sessionDurableThrough(sessionId);watermarks.set(sessionId,through);if(through>=seq)return;await delay(10)}
    throw new Error('Session event did not cross the PostgreSQL durability barrier')
  }
  private fail(response:ServerResponse,error:unknown):void{if(response.headersSent){response.destroy();return}const status=typeof error==='object'&&error!==null&&'status'in error?Number((error as {status:unknown}).status):500;json(response,status,{error:status===500?'internal gateway error':error instanceof Error?error.message:'request failed'})}
}
