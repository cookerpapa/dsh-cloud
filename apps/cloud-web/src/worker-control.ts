import { createServer, request, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect } from 'node:net'
import type { Duplex } from 'node:stream'

function close(socket: Duplex): void {
  if (!socket.destroyed) socket.destroy()
}

function loopback(host: string): boolean {
  return host === '127.0.0.1' || host === '::1' || host === 'localhost'
}

function proxyHttp(targetHost: string, targetPort: number, downstream: IncomingMessage, response: ServerResponse): void {
  const upstream = request({
    host: targetHost,
    port: targetPort,
    method: downstream.method,
    path: downstream.url,
    headers: { ...downstream.headers, host: `${targetHost}:${targetPort}` },
  }, result => {
    response.writeHead(result.statusCode ?? 502, result.statusMessage, result.headers)
    result.pipe(response)
  })
  upstream.on('error', () => {
    if (!response.headersSent) response.writeHead(502, { 'content-type': 'text/plain; charset=utf-8' })
    response.end('Worker control upstream is unavailable')
  })
  downstream.pipe(upstream)
}

function proxyUpgrade(targetHost: string, targetPort: number, downstream: IncomingMessage, socket: Duplex, head: Buffer): void {
  const upstream = connect({ host: targetHost, port: targetPort })
  upstream.once('connect', () => {
    const lines = [`${downstream.method ?? 'GET'} ${downstream.url ?? '/'} HTTP/${downstream.httpVersion}`]
    for (let index = 0; index < downstream.rawHeaders.length; index += 2) {
      const name = downstream.rawHeaders[index]
      const value = downstream.rawHeaders[index + 1]
      if (name !== undefined && value !== undefined && name.toLowerCase() !== 'host') lines.push(`${name}: ${value}`)
    }
    lines.push(`Host: ${targetHost}:${targetPort}`, '', '')
    upstream.write(lines.join('\r\n'))
    if (head.byteLength > 0) upstream.write(head)
    socket.pipe(upstream).pipe(socket)
  })
  socket.on('error', () => close(upstream))
  upstream.on('error', () => close(socket))
  socket.on('close', () => close(upstream))
  upstream.on('close', () => close(socket))
}

/**
 * Trusted HTTP/WebSocket relay for the Gateway-to-Worker control channel.
 *
 * DSH intentionally refuses a non-loopback bind because its Host API can
 * execute Agent operations. The relay keeps DSH on loopback, rewrites the
 * Host header back to that trusted origin, and is reachable only through the
 * execution-plane NetworkPolicy.
 */
export async function createWorkerControlRelay(input: Readonly<{
  listenHost: string
  listenPort: number
  targetHost: string
  targetPort: number
}>): Promise<Server> {
  if (!Number.isSafeInteger(input.listenPort) || input.listenPort < 1 || input.listenPort > 65_535) throw new TypeError('Worker control port is invalid')
  if (!Number.isSafeInteger(input.targetPort) || input.targetPort < 1 || input.targetPort > 65_535) throw new TypeError('DSH Host port is invalid')
  if (!loopback(input.targetHost)) throw new TypeError('DSH Host target must remain on loopback')
  const server = createServer((incoming, response) => proxyHttp(input.targetHost, input.targetPort, incoming, response))
  server.on('upgrade', (incoming, socket, head) => proxyUpgrade(input.targetHost, input.targetPort, incoming, socket, head))
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(input.listenPort, input.listenHost, () => {
      server.off('error', reject)
      resolve()
    })
  })
  return server
}
