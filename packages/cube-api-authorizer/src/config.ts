import { readFile } from 'node:fs/promises'

export interface CubeApiAuthorizerConfig {
  readonly host: string
  readonly port: number
  readonly credential: string
}

async function secret(name: string): Promise<string> {
  const file = process.env[`${name}_FILE`]
  const value = file === undefined ? process.env[name] : await readFile(file, 'utf8')
  if (value === undefined || value.trim().length < 32 || value.trim().length > 4_096) {
    throw new Error(`${name} must contain 32-4096 characters`)
  }
  return value.trim()
}

export async function loadCubeApiAuthorizerConfig(): Promise<CubeApiAuthorizerConfig> {
  const port = Number(process.env['DSH_CLOUD_CUBE_API_AUTH_PORT'] ?? '8080')
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error('DSH_CLOUD_CUBE_API_AUTH_PORT must be a valid TCP port')
  }
  return {
    host: process.env['DSH_CLOUD_CUBE_API_AUTH_HOST'] ?? '0.0.0.0',
    port,
    credential: await secret('DSH_CLOUD_CUBE_API_AUTH_CREDENTIAL'),
  }
}
