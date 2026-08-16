import { describe, expect, it } from 'vitest'
import { authorizeCubeApiRequest, isAllowedCubeApiOperation } from '../src/authorization.js'

const CREDENTIAL = 'a'.repeat(64)
const SANDBOX = 'sandbox-123'
const DSH_VOLUME = `dsh-${'a'.repeat(48)}`
const PI_VOLUME = `adw-${'a'.repeat(48)}`

describe('DSH Cloud Cube API authorizer', () => {
  it.each([
    ['POST', '/sandboxes'],
    ['GET', `/sandboxes/${SANDBOX}`],
    ['DELETE', `/sandboxes/${SANDBOX}`],
    ['GET', '/v2/sandboxes'],
    ['GET', '/v2/sandboxes?limit=1000'],
    ['POST', '/volumes'],
    ['GET', `/volumes/${DSH_VOLUME}`],
    ['DELETE', `/volumes/${DSH_VOLUME}`],
  ])('allows the required operation %s %s', (method, path) => {
    expect(isAllowedCubeApiOperation(path, method)).toBe(true)
    expect(authorizeCubeApiRequest(CREDENTIAL, {
      authorization: `Bearer ${CREDENTIAL}`,
      requestPath: path,
      requestMethod: method,
    })).toBe('allow')
  })

  it.each([
    ['GET', `/volumes/${PI_VOLUME}`],
    ['DELETE', `/volumes/${PI_VOLUME}`],
    ['GET', '/volumes'],
    ['GET', '/templates'],
    ['DELETE', `/sandboxes/${SANDBOX}/snapshots`],
    ['PATCH', `/sandboxes/${SANDBOX}`],
    ['GET', '/v2/sandboxes?limit=1001'],
    ['GET', '/v2/sandboxes?owner=foreign'],
    ['POST', '/sandboxes?unsafe=true'],
    ['GET', `/volumes/${DSH_VOLUME}?unsafe=true`],
  ])('denies the out-of-scope operation %s %s', (method, path) => {
    expect(isAllowedCubeApiOperation(path, method)).toBe(false)
    expect(authorizeCubeApiRequest(CREDENTIAL, {
      apiKey: CREDENTIAL,
      requestPath: path,
      requestMethod: method,
    })).toBe('operation_denied')
  })

  it('rejects missing and foreign credentials before evaluating policy', () => {
    expect(authorizeCubeApiRequest(CREDENTIAL, {
      requestPath: `/volumes/${DSH_VOLUME}`,
      requestMethod: 'GET',
    })).toBe('invalid_credential')
    expect(authorizeCubeApiRequest(CREDENTIAL, {
      authorization: `Bearer ${'b'.repeat(64)}`,
      requestPath: `/volumes/${DSH_VOLUME}`,
      requestMethod: 'GET',
    })).toBe('invalid_credential')
  })
})
