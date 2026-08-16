import { createServer, type Server } from 'node:http'
import { randomUUID } from 'node:crypto'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { Pool } from 'pg'
import { CloudGateway } from '../src/server.js'

const connectionString=process.env['DSH_CLOUD_TEST_DATABASE_URL']
const enabled=connectionString===undefined?describe.skip:describe

function listen(server:Server):Promise<number>{return new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',()=>{server.off('error',reject);const value=server.address();if(value===null||typeof value==='string')reject(new Error('missing address'));else resolve(value.port)})})}
function close(server:Server):Promise<void>{return new Promise((resolve,reject)=>server.close(error=>error===undefined?resolve():reject(error)))}

enabled('multi-tenant Cloud Gateway',()=>{
  const namespace=`gateway-${randomUUID()}`
  const pool=new Pool({connectionString,max:10})
  let fake:Server,gateway:CloudGateway,baseUrl='',cookie='',ownedSession=''

  beforeAll(async()=>{
    fake=createServer(async(request,response)=>{
      if(request.url==='/'&&request.method==='GET'){response.writeHead(200,{'content-type':'text/html'});response.end('<main>official dsh ui</main>');return}
      const chunks:Buffer[]=[];for await(const chunk of request)chunks.push(Buffer.from(chunk));const envelope=JSON.parse(Buffer.concat(chunks).toString('utf8')) as {rpcId:string;method:string;payload:Record<string,unknown>}
      let value:unknown={}
      if(envelope.method==='session.create')value={sessionId:envelope.payload['sessionId']}
      if(envelope.method==='session.list')value={items:[{sessionId:ownedSession},{sessionId:'foreign-session'}]}
      if(envelope.method==='session.history')value={events:[],hasMore:false}
      const body=JSON.stringify({type:'server-response',rpcId:envelope.rpcId,result:{ok:true,value}});response.writeHead(200,{'content-type':'application/json','content-length':String(Buffer.byteLength(body))});response.end(body)
    })
    const workerPort=await listen(fake)
    gateway=new CloudGateway({pool,namespace,secureCookies:false})
    await gateway.initialize()
    await gateway.store.heartbeatWorker({id:'worker-test',baseUrl:`http://127.0.0.1:${workerPort}`,maximumRuns:4})
    await gateway.listen(0,'127.0.0.1');baseUrl=`http://127.0.0.1:${gateway.address()!.port}`
  })

  afterAll(async()=>{await gateway.close();await close(fake);await pool.query('DELETE FROM dsh_cloud_control.workers WHERE namespace=$1',[namespace]);await pool.query('DELETE FROM dsh_cloud_control.tenants WHERE namespace=$1',[namespace]);await pool.end()})

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
})
