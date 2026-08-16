import { Pool } from 'pg'
import { CloudGateway } from './server.js'

const connectionString=process.env['DSH_CLOUD_DATABASE_URL']
if(connectionString===undefined||connectionString.trim()==='')throw new Error('DSH_CLOUD_DATABASE_URL is required')
const configuredOrigin=process.env['DSH_CLOUD_PUBLIC_ORIGIN']?.trim()
const pool=new Pool({connectionString,max:Number(process.env['DSH_CLOUD_GATEWAY_DB_POOL']??'20'),application_name:'dsh-cloud-gateway'})
const gateway=new CloudGateway({pool,namespace:process.env['DSH_CLOUD_NAMESPACE']??'default',...(configuredOrigin===undefined||configuredOrigin===''?{}:{publicOrigin:configuredOrigin}),secureCookies:process.env['DSH_CLOUD_SECURE_COOKIES']==='1'})
await gateway.initialize();await gateway.listen(Number(process.env['DSH_CLOUD_GATEWAY_PORT']??'8080'),process.env['DSH_CLOUD_GATEWAY_HOST']??'0.0.0.0')
for(const signal of ['SIGINT','SIGTERM'] as const)process.once(signal,()=>void gateway.close().finally(()=>pool.end()))
