import { createServer } from 'node:http'
import { describe, expect, it } from 'vitest'
import { createWorkerControlRelay } from '../src/worker-control.js'

function listen(server: ReturnType<typeof createServer>): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      const address = server.address()
      if (address === null || typeof address === 'string') reject(new Error('server has no TCP address'))
      else resolve(address.port)
    })
  })
}

function stop(server: ReturnType<typeof createServer>): Promise<void> {
  return new Promise((resolve, reject) => server.close(error => error === undefined ? resolve() : reject(error)))
}

describe('Worker Control Relay', () => {
  it('keeps DSH on loopback and rewrites the trusted Host origin', async () => {
    const upstream = createServer((request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ host: request.headers.host, body: request.url }))
    })
    const targetPort = await listen(upstream)
    const reservation = createServer()
    const relayPort = await listen(reservation)
    await stop(reservation)
    const relay = await createWorkerControlRelay({ listenHost: '127.0.0.1', listenPort: relayPort, targetHost: '127.0.0.1', targetPort })
    try {
      const result = await fetch(`http://127.0.0.1:${relayPort}/trusted-control`, { headers: { host: 'worker-pod:3080' } })
      expect(await result.json()).toEqual({ host: `127.0.0.1:${targetPort}`, body: '/trusted-control' })
    } finally {
      await stop(relay)
      await stop(upstream)
    }
  })

  it('refuses to relay to a non-loopback DSH Host', async () => {
    await expect(createWorkerControlRelay({ listenHost: '127.0.0.1', listenPort: 1, targetHost: '10.0.0.1', targetPort: 3081 })).rejects.toThrow('loopback')
  })
})
