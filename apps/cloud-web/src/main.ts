import { spawn } from 'node:child_process'
import { mkdir } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const require = createRequire(import.meta.url)
const appDir = dirname(fileURLToPath(import.meta.url))
const repositoryRoot = resolve(appDir, '../../..')
const dshPackage = require.resolve('@deepseek-ai/dsh/package.json')
const dshEntry = join(dirname(dshPackage), 'lib', 'bin.js')
const cloudPatch = require.resolve('@dsh-cloud/web-bundle/cordis.patch.yml')
const dshHome = process.env['DSH_HOME'] ?? join(repositoryRoot, '.data', 'dsh-home')

if (process.env['DSH_CLOUD_DATABASE_URL']?.trim() === '') {
  throw new Error('DSH_CLOUD_DATABASE_URL must not be empty')
}
if (process.env['DSH_CLOUD_DATABASE_URL'] === undefined) {
  throw new Error('DSH_CLOUD_DATABASE_URL is required')
}

await mkdir(dshHome, { recursive: true, mode: 0o700 })

const host = process.env['DSH_CLOUD_HOST'] ?? '127.0.0.1'
const port = process.env['DSH_CLOUD_PORT'] ?? '3080'
const child = spawn(process.execPath, [
  dshEntry,
  '--profile', 'web',
  '--patch', cloudPatch,
  '--host', host,
  '--port', port,
  ...process.argv.slice(2),
], {
  cwd: repositoryRoot,
  env: { ...process.env, DSH_HOME: dshHome },
  stdio: 'inherit',
})

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => child.kill(signal))
}

child.once('error', (error) => {
  console.error('failed to start DSH Cloud Web:', error.message)
  process.exitCode = 1
})

child.once('exit', (code, signal) => {
  process.exitCode = code ?? (signal === null ? 1 : 128)
})

