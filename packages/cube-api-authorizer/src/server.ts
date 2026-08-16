import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { authorizeCubeApiRequest } from './authorization.js'

function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name]
  return Array.isArray(value) ? undefined : value
}

function empty(response: ServerResponse, status: number): void {
  response.writeHead(status, { 'content-length': '0' }).end()
}

export function createCubeApiAuthorizerServer(credential: string): ReturnType<typeof createServer> {
  return createServer((request, response) => {
    if (request.method === 'GET' && request.url === '/health') {
      empty(response, 200)
      return
    }
    if (request.method !== 'POST' || request.url !== '/verify') {
      empty(response, 404)
      return
    }
    const authorization = header(request, 'authorization')
    const apiKey = header(request, 'x-api-key')
    const requestPath = header(request, 'x-request-path')
    const requestMethod = header(request, 'x-request-method')
    const result = authorizeCubeApiRequest(credential, {
      ...(authorization === undefined ? {} : { authorization }),
      ...(apiKey === undefined ? {} : { apiKey }),
      ...(requestPath === undefined ? {} : { requestPath }),
      ...(requestMethod === undefined ? {} : { requestMethod }),
    })
    empty(response, result === 'allow' ? 200 : result === 'invalid_credential' ? 401 : 403)
  })
}
