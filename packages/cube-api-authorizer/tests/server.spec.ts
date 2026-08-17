import { once } from 'node:events'
import { afterEach, describe, expect, it } from 'vitest'
import { createCubeApiAuthorizerServer } from '../src/server.js'

const CREDENTIAL = 'a'.repeat(64)
const DSH_VOLUME = `dsh-${'a'.repeat(48)}`
const PI_VOLUME = `pcw-${'a'.repeat(48)}`

describe('Cube API callback protocol', () => {
  const servers: ReturnType<typeof createCubeApiAuthorizerServer>[] = []

  afterEach(async () => {
    await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve, reject) => {
      server.close(error => error === undefined ? resolve() : reject(error))
    })))
  })

  async function endpoint(): Promise<string> {
    const server = createCubeApiAuthorizerServer(CREDENTIAL)
    servers.push(server)
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('authorizer did not bind TCP')
    return `http://127.0.0.1:${address.port}`
  }

  it('admits DSH Volume paths and rejects Pi Volume paths', async () => {
    const base = await endpoint()
    const verify = (requestPath: string): Promise<Response> => fetch(`${base}/verify`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${CREDENTIAL}`,
        'x-request-method': 'GET',
        'x-request-path': requestPath,
      },
    })

    expect((await verify(`/volumes/${DSH_VOLUME}`)).status).toBe(200)
    expect((await verify(`/volumes/${PI_VOLUME}`)).status).toBe(403)
  })

  it('keeps health public but rejects a foreign API credential', async () => {
    const base = await endpoint()
    expect((await fetch(`${base}/health`)).status).toBe(200)
    expect((await fetch(`${base}/verify`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${'b'.repeat(64)}`,
        'x-request-method': 'GET',
        'x-request-path': `/volumes/${DSH_VOLUME}`,
      },
    })).status).toBe(401)
  })
})
