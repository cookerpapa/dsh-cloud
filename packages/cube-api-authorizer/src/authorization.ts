import { createHash, timingSafeEqual } from 'node:crypto'

export type CubeAuthorizationRequest = Readonly<{
  authorization?: string
  apiKey?: string
  requestPath?: string
  requestMethod?: string
}>

const RESOURCE_ID = '[A-Za-z0-9](?:[A-Za-z0-9-]{0,126}[A-Za-z0-9])?'
const SANDBOX_ITEM = new RegExp(`^/sandboxes/${RESOURCE_ID}$`)
const DSH_VOLUME_ITEM = /^\/volumes\/dsh-[a-f0-9]{48}$/

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest()
}

function credential(request: CubeAuthorizationRequest): string | undefined {
  if (request.authorization?.startsWith('Bearer ') === true) {
    const value = request.authorization.slice('Bearer '.length).trim()
    return value.length === 0 ? undefined : value
  }
  const value = request.apiKey?.trim()
  return value === undefined || value.length === 0 ? undefined : value
}

function boundedInventoryQuery(parsed: URL): boolean {
  if (parsed.search === '') return true
  if ([...parsed.searchParams.keys()].some(key => key !== 'limit')) return false
  const values = parsed.searchParams.getAll('limit')
  const limit = values[0]
  return values.length === 1 && limit !== undefined && /^(?:[1-9][0-9]{0,2}|1000)$/.test(limit)
}

/** Minimum Cube control operations used by DSH Cloud's trusted Tool Broker. */
export function isAllowedCubeApiOperation(path: string, method: string): boolean {
  let parsed: URL
  try {
    parsed = new URL(path, 'http://cube-api.internal')
  } catch {
    return false
  }
  if (parsed.origin !== 'http://cube-api.internal' || parsed.hash !== '') return false
  const normalizedMethod = method.toUpperCase()
  if (parsed.search === '') {
    if (normalizedMethod === 'POST' && parsed.pathname === '/sandboxes') return true
    if (normalizedMethod === 'POST' && parsed.pathname === '/volumes') return true
    if (
      (normalizedMethod === 'GET' || normalizedMethod === 'DELETE') &&
      (SANDBOX_ITEM.test(parsed.pathname) || DSH_VOLUME_ITEM.test(parsed.pathname))
    ) return true
  }
  return normalizedMethod === 'GET' && parsed.pathname === '/v2/sandboxes' && boundedInventoryQuery(parsed)
}

export function authorizeCubeApiRequest(
  expectedCredential: string,
  request: CubeAuthorizationRequest,
): 'allow' | 'invalid_credential' | 'operation_denied' {
  const supplied = credential(request)
  if (
    supplied === undefined ||
    supplied.length > 4_096 ||
    !timingSafeEqual(digest(supplied), digest(expectedCredential))
  ) return 'invalid_credential'

  if (
    request.requestPath === undefined ||
    request.requestMethod === undefined ||
    request.requestPath.length > 2_048 ||
    request.requestMethod.length > 16 ||
    !isAllowedCubeApiOperation(request.requestPath, request.requestMethod)
  ) return 'operation_denied'
  return 'allow'
}
