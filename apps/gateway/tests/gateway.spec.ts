import { createServer, type Server } from 'node:http'
import { randomUUID } from 'node:crypto'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { Pool } from 'pg'
import WebSocket, { WebSocketServer } from 'ws'
import { CloudGateway } from '../src/server.js'

const connectionString=process.env['DSH_CLOUD_TEST_DATABASE_URL']
const enabled=connectionString===undefined?describe.skip:describe

function listen(server:Server):Promise<number>{return new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',()=>{server.off('error',reject);const value=server.address();if(value===null||typeof value==='string')reject(new Error('missing address'));else resolve(value.port)})})}
function close(server:Server):Promise<void>{return new Promise((resolve,reject)=>server.close(error=>error===undefined?resolve():reject(error)))}

enabled('multi-tenant Cloud Gateway',()=>{
  const namespace=`gateway-${randomUUID()}`
  const pool=new Pool({connectionString,max:10})
  let fake:Server,fakeSockets:WebSocketServer,fakeB:Server,fakeSocketsB:WebSocketServer,gateway:CloudGateway,baseUrl='',cookie='',ownedSession=''
  const workerOrigins=new Map<string,string|undefined>()
  const workerRequests=new Map<string,{method:string;payload:Record<string,unknown>}>()

  beforeAll(async()=>{
    fake=createServer(async(request,response)=>{
      if(request.url==='/'&&request.method==='GET'){response.writeHead(200,{'content-type':'text/html'});response.end('<main>official dsh ui</main>');return}
      if(request.url==='/v1/workspaces/destroy'&&request.method==='POST'){response.writeHead(200,{'content-type':'application/json'});response.end('{"deleted":true}');return}
      const chunks:Buffer[]=[];for await(const chunk of request)chunks.push(Buffer.from(chunk));const envelope=JSON.parse(Buffer.concat(chunks).toString('utf8')) as {rpcId:string;method:string;payload:Record<string,unknown>}
      workerOrigins.set(envelope.rpcId,request.headers.origin)
      workerRequests.set(envelope.rpcId,{method:envelope.method,payload:envelope.payload})
      let value:unknown={}
      if(envelope.method==='session.create')value={sessionId:envelope.payload['sessionId']}
      if(envelope.method==='agentPreset.list')value={presets:[
        {id:'standard',trust:'system',isDefault:true},
        {id:'code',trust:'system',isDefault:false},
        {id:'minimal',trust:'system',isDefault:false},
        {id:'cordis',trust:'system',isDefault:false},
        {id:'local-user-preset',trust:'user',isDefault:false},
      ],authorable:true,hasDocument:true}
      if(envelope.method==='agentPreset.select')value={agentPreset:envelope.payload['agentPreset']}
      if(envelope.method==='session.list')value={items:[{sessionId:ownedSession},{sessionId:'foreign-session'}]}
      if(envelope.method==='session.history')value={events:[],hasMore:false}
      if(envelope.method==='settings.describe')value={writable:true,hasDocument:true,namespaces:[{ns:'ui-onboarding',schema:{},value:{},applies:'live',secrets:[],revision:0}]}
      const body=JSON.stringify({type:'server-response',rpcId:envelope.rpcId,result:{ok:true,value}});response.writeHead(200,{'content-type':'application/json','content-length':String(Buffer.byteLength(body))});response.end(body)
    })
    fakeSockets=new WebSocketServer({noServer:true})
    fake.on('upgrade',(request,socket,head)=>{
      fakeSockets.handleUpgrade(request,socket,head,upstream=>{
        upstream.send(JSON.stringify({
          type:'server-request',rpcId:randomUUID(),
          payload:{type:'session/subscribed',sessionId:ownedSession,lastSeq:-1},
        }))
      })
    })
    fakeB=createServer((_,response)=>{response.writeHead(404);response.end()})
    fakeSocketsB=new WebSocketServer({noServer:true})
    fakeB.on('upgrade',(request,socket,head)=>fakeSocketsB.handleUpgrade(request,socket,head,()=>undefined))
    const workerPort=await listen(fake)
    const workerPortB=await listen(fakeB)
    gateway=new CloudGateway({pool,namespace,secureCookies:false,toolBroker:{url:`http://127.0.0.1:${workerPort}`,token:'test-tool-broker-token'}})
    await gateway.initialize()
    await gateway.store.heartbeatWorker({id:'worker-test',baseUrl:`http://127.0.0.1:${workerPort}`,maximumRuns:4})
    await gateway.store.heartbeatWorker({id:'worker-test-b',baseUrl:`http://127.0.0.1:${workerPortB}`,maximumRuns:4})
    await gateway.listen(0,'127.0.0.1');baseUrl=`http://127.0.0.1:${gateway.address()!.port}`
  })

  afterAll(async()=>{await gateway.close();await new Promise<void>(resolve=>fakeSockets.close(()=>resolve()));await new Promise<void>(resolve=>fakeSocketsB.close(()=>resolve()));await close(fake);await close(fakeB);await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1',[namespace]);await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1',[namespace]);await pool.end()})

  test('requires login, creates a tenant and serves the upstream UI',async()=>{
    expect(await (await fetch(baseUrl)).text()).toContain('登录后进入')
    const registered=await fetch(`${baseUrl}/cloud/register`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({name:'A',email:'a@example.test',password:'a secure password'})})
    expect(registered.status).toBe(200);cookie=registered.headers.get('set-cookie')!.split(';')[0]!
    expect(await (await fetch(baseUrl,{headers:{cookie}})).text()).toContain('official dsh ui')
  })

  test('registers cloud ownership and filters global Session lists',async()=>{
    const rpcId=randomUUID()
    const created=await fetch(`${baseUrl}/api/session.create`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({type:'client-request',rpcId,method:'session.create',payload:{}})})
    const result=await created.json() as {result:{value:{sessionId:string}}};ownedSession=result.result.value.sessionId
    expect(await gateway.store.ownsSession((await gateway.store.authenticate(cookie.split('=')[1]!))!.tenantId,ownedSession)).toBe(true)
    const listed=await fetch(`${baseUrl}/api/session.list`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({type:'client-request',rpcId:randomUUID(),method:'session.list',payload:{}})})
    const list=await listed.json() as {result:{value:{items:Array<{sessionId:string}>}}}
    expect(list.result.value.items.map(item=>item.sessionId)).toEqual([ownedSession])
  })

  test('offers only cloud-safe Harness profiles and pins one to a blank owned Session',async()=>{
    const listedEnvelope={type:'client-request',rpcId:randomUUID(),method:'agentPreset.list',payload:{}}
    const listed=await fetch(`${baseUrl}/api/agentPreset.list`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(listedEnvelope)})
    expect(await listed.json()).toMatchObject({result:{ok:true,value:{presets:[
      {id:'standard',trust:'system',isDefault:true},
      {id:'code',trust:'system',isDefault:false},
    ],authorable:false,hasDocument:false}}})

    const rpcId=randomUUID()
    const selectedEnvelope={type:'client-request',rpcId,method:'agentPreset.select',payload:{sessionId:ownedSession,agentPreset:'code'}}
    const selected=await fetch(`${baseUrl}/api/agentPreset.select`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(selectedEnvelope)})
    expect(await selected.json()).toMatchObject({result:{ok:true,value:{agentPreset:'code'}}})
    expect(workerRequests.get(rpcId)).toEqual({method:'agentPreset.select',payload:{sessionId:ownedSession,agentPreset:'code'}})

    const rejectedEnvelope={type:'client-request',rpcId:randomUUID(),method:'agentPreset.select',payload:{sessionId:ownedSession,agentPreset:'cordis'}}
    const rejected=await fetch(`${baseUrl}/api/agentPreset.select`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(rejectedEnvelope)})
    expect(await rejected.json()).toMatchObject({result:{ok:false,error:{code:'agent-preset-not-found'}}})
  })

  test('terminates browser Origin trust at the Gateway before invoking a Worker',async()=>{
    const rpcId=randomUUID()
    const created=await fetch(`${baseUrl}/api/session.create`,{method:'POST',headers:{cookie,'content-type':'application/json',origin:baseUrl},body:JSON.stringify({type:'client-request',rpcId,method:'session.create',payload:{}})})
    const result=await created.json() as {result:{ok:boolean;value:{sessionId:string}}}
    expect(result.result.ok).toBe(true)
    expect(workerOrigins.get(rpcId)).toBeUndefined()

    const rejected=await fetch(`${baseUrl}/api/host.describe`,{method:'POST',headers:{cookie,'content-type':'application/json',origin:'https://attacker.example'},body:JSON.stringify({type:'client-request',rpcId:randomUUID(),method:'host.describe',payload:{}})})
    expect(await rejected.json()).toMatchObject({result:{ok:false,error:{code:'bad-request'}}})
  })

  test('persists the loopback welcome acknowledgement per cloud user',async()=>{
    const describe=async()=>{
      const envelope={type:'client-request',rpcId:randomUUID(),method:'settings.describe',payload:{}}
      return (await (await fetch(`${baseUrl}/api/settings.describe`,{method:'POST',headers:{cookie,'content-type':'application/json',origin:baseUrl},body:JSON.stringify(envelope)})).json()) as {result:{value:{writable:boolean;namespaces:Array<{ns:string;value:Record<string,unknown>;revision:number}>}}}
    }
    expect((await describe()).result.value).toMatchObject({writable:false,namespaces:[{ns:'ui-onboarding',value:{},revision:0}]})
    const envelope={type:'client-request',rpcId:randomUUID(),method:'settings.mutate',payload:{ns:'ui-onboarding',ops:[{op:'set',path:['welcomeNoticeVersion'],value:'acceptance-v1'}]}}
    const saved=await fetch(`${baseUrl}/api/settings.mutate`,{method:'POST',headers:{cookie,'content-type':'application/json',origin:baseUrl},body:JSON.stringify(envelope)})
    expect(await saved.json()).toMatchObject({result:{ok:true,value:{ns:'ui-onboarding',value:{welcomeNoticeVersion:'acceptance-v1'},revision:1}}})
    expect((await describe()).result.value.namespaces[0]).toMatchObject({value:{welcomeNoticeVersion:'acceptance-v1'},revision:1})
  })

  test('admits prompts to PostgreSQL instead of invoking the Host directly',async()=>{
    const rpcId=randomUUID();const response=await fetch(`${baseUrl}/api/session.prompt`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({type:'client-request',rpcId,method:'session.prompt',payload:{sessionId:ownedSession,mode:'queue',content:[{type:'text',text:'hello'}]}})})
    const value=await response.json() as {result:{value:{accepted:boolean;runId:string}}};expect(value.result.value.accepted).toBe(true)
    expect((await gateway.store.runResponse(value.result.value.runId))?.status).toBe('queued')
  })

  test('rejects cross-tenant history access',async()=>{
    const second=await fetch(`${baseUrl}/cloud/register`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({name:'B',email:'b@example.test',password:'another secure password'})});const otherCookie=second.headers.get('set-cookie')!.split(';')[0]!
    const response=await fetch(`${baseUrl}/api/session.history`,{method:'POST',headers:{cookie:otherCookie,'content-type':'application/json'},body:JSON.stringify({type:'client-request',rpcId:randomUUID(),method:'session.history',payload:{sessionId:ownedSession}})})
    const value=await response.json() as {result:{ok:boolean;error:{code:string}}};expect(value.result).toMatchObject({ok:false,error:{code:'session-not-found'}})
  })

  test('forwards tenant-owned Session reads after the ownership check',async()=>{
    const response=await fetch(`${baseUrl}/api/session.history`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({type:'client-request',rpcId:randomUUID(),method:'session.history',payload:{sessionId:ownedSession}})})
    const value=await response.json() as {result:{ok:boolean;value:{events:unknown[]}}}
    expect(value.result).toMatchObject({ok:true,value:{events:[]}})
  })

  test('serves a cloud Workspace namespace instead of exposing Worker directories',async()=>{
    const listEnvelope={type:'client-request',rpcId:randomUUID(),method:'host.listDirectory',payload:{}}
    const listed=await fetch(`${baseUrl}/api/host.listDirectory`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(listEnvelope)})
    expect(await listed.json()).toMatchObject({result:{ok:true,value:{path:'/workspaces',home:'/workspaces',entries:[],truncated:false}}})

    const createEnvelope={type:'client-request',rpcId:randomUUID(),method:'host.createDirectory',payload:{path:'/workspaces',name:'sorting-lab'}}
    const created=await fetch(`${baseUrl}/api/host.createDirectory`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(createEnvelope)})
    expect(await created.json()).toMatchObject({result:{ok:true,value:{path:'/workspaces/sorting-lab'}}})

    const rejectedEnvelope={type:'client-request',rpcId:randomUUID(),method:'host.listDirectory',payload:{path:'/etc'}}
    const rejected=await fetch(`${baseUrl}/api/host.listDirectory`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(rejectedEnvelope)})
    expect(await rejected.json()).toMatchObject({result:{ok:false,error:{code:'directory-unreadable'}}})
  })

  test('opens the private Worker stream before the browser and preserves text frames',async()=>{
    const url=new URL('/api/events.mux',baseUrl);url.protocol='ws:'
    const client=new WebSocket(url,{headers:{cookie}})
    const frame=await new Promise<{binary:boolean;value:Record<string,unknown>}>((resolve,reject)=>{
      const timeout=setTimeout(()=>reject(new Error('Gateway event frame timed out')),5_000)
      const onMessage=(data:WebSocket.RawData,binary:boolean):void=>{
        const value=JSON.parse(data.toString()) as {payload?:{type?:string;sessionId?:string}}
        if(value.payload?.type!=='session/subscribed'||value.payload.sessionId!==ownedSession)return
        clearTimeout(timeout);client.off('message',onMessage);resolve({binary,value})
      }
      client.on('message',onMessage)
      client.once('error',error=>{clearTimeout(timeout);reject(error)})
    })
    expect(frame.binary).toBe(false)
    expect(frame.value).toMatchObject({payload:{type:'session/subscribed',sessionId:ownedSession}})
    client.close()
  })

  test('keeps one browser outlet while live events move between Workers',async()=>{
    const url=new URL('/api/events.mux',baseUrl);url.protocol='ws:'
    const client=new WebSocket(url,{headers:{cookie}})
    await new Promise<void>((resolve,reject)=>{
      const timeout=setTimeout(()=>reject(new Error('initial Worker frame timed out')),5_000)
      client.once('message',()=>{clearTimeout(timeout);resolve()})
      client.once('error',reject)
    })
    for(const socket of fakeSockets.clients)socket.terminate()
    await new Promise(resolve=>setTimeout(resolve,100))
    expect(client.readyState).toBe(WebSocket.OPEN)
    const moved=new Promise<Record<string,unknown>>((resolve,reject)=>{
      const timeout=setTimeout(()=>reject(new Error('replacement Worker frame timed out')),5_000)
      client.once('message',data=>{clearTimeout(timeout);resolve(JSON.parse(data.toString()) as Record<string,unknown>)})
    })
    for(const socket of fakeSocketsB.clients){
      socket.send(JSON.stringify({type:'server-request',rpcId:randomUUID(),payload:{type:'host/session-removed',sessionId:ownedSession}}))
      socket.send(JSON.stringify({type:'server-request',rpcId:randomUUID(),payload:{type:'host/session-status',sessionId:ownedSession,running:true}}))
    }
    expect(await moved).toMatchObject({payload:{type:'host/session-status',sessionId:ownedSession,running:true}})
    client.close()
  })

  test('adds a newly created Session to an already-open browser outlet',async()=>{
    const url=new URL('/api/events.mux',baseUrl);url.protocol='ws:'
    const client=new WebSocket(url,{headers:{cookie}})
    await new Promise<void>((resolve,reject)=>{client.once('open',()=>resolve());client.once('error',reject)})
    const allocated=randomUUID()
    const baseline=new Promise<Record<string,unknown>>((resolve,reject)=>{
      const timeout=setTimeout(()=>reject(new Error('new Session baseline timed out')),5_000)
      const onMessage=(data:WebSocket.RawData):void=>{
        const value=JSON.parse(data.toString()) as {payload?:{type?:string;sessionId?:string}}
        if(value.payload?.type!=='session/subscribed'||value.payload.sessionId!==allocated)return
        clearTimeout(timeout);client.off('message',onMessage);resolve(value)
      }
      client.on('message',onMessage)
    })
    const created=await fetch(`${baseUrl}/api/session.create`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({type:'client-request',rpcId:randomUUID(),method:'session.create',payload:{sessionId:allocated}})})
    expect(await created.json()).toMatchObject({result:{ok:true,value:{sessionId:allocated}}})
    expect(await baseline).toMatchObject({payload:{type:'session/subscribed',sessionId:allocated}})
    client.close()
  })

  test('denies upstream Host mutations that have no tenant-scoped cloud contract',async()=>{
    const envelope={type:'client-request',rpcId:randomUUID(),method:'settings.update',payload:{patch:{}}}
    const response=await fetch(`${baseUrl}/api/settings.update`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(envelope)})
    const value=await response.json() as {result:{ok:boolean;error:{code:string}}}
    expect(value.result).toMatchObject({ok:false,error:{code:'settings-not-exposed'}})
  })

  test('does not bypass RPC policy through another HTTP method',async()=>{
    const response=await fetch(`${baseUrl}/api/settings.update`,{headers:{cookie}})
    expect(response.status).toBe(405)
  })

  test('keeps Workspace lifecycle operations available while no Agent Worker is schedulable',async()=>{
    await gateway.store.setWorkerDraining('worker-test',true)
    try {
      const createEnvelope={type:'client-request',rpcId:randomUUID(),method:'workspace.create',payload:{path:'Disposable'}}
      const created=await fetch(`${baseUrl}/api/workspace.create`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(createEnvelope)})
      const createdBody=await created.json() as {result:{value:{workspace:{workspaceId:string}}}}
      expect(created.status).toBe(200)
      const workspaceId=createdBody.result.value.workspace.workspaceId
      const deleteEnvelope={type:'client-request',rpcId:randomUUID(),method:'workspace.delete',payload:{workspaceId}}
      const deleted=await fetch(`${baseUrl}/api/workspace.delete`,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(deleteEnvelope)})
      expect(await deleted.json()).toMatchObject({result:{ok:true,value:{deleted:true}}})
    } finally {
      await gateway.store.setWorkerDraining('worker-test',false)
    }
  })
})
