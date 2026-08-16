import { spawn } from 'node:child_process'
import { mkdir, realpath, symlink } from 'node:fs/promises'
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

/**
 * The DSH Loader resolves profile plugins from `$DSH_HOME/profiles/node_modules`.
 * Released DSH packages are healed there by DSH itself; this distribution owns
 * the two out-of-tree Cloud plugins and publishes collision-safe links before
 * boot. Keeping this explicit makes an arbitrary external DSH_HOME work just
 * like the repository-local development home.
 */
async function exposeCloudPlugin(packageName: string): Promise<void> {
  const packageRoot = dirname(require.resolve(`${packageName}/package.json`))
  const link = join(dshHome, 'profiles', 'node_modules', ...packageName.split('/'))
  await mkdir(dirname(link), { recursive: true, mode: 0o700 })
  const assertTarget = async (): Promise<void> => {
    const [existing, expected] = await Promise.all([realpath(link), realpath(packageRoot)])
    if (existing !== expected) {
      throw new Error(`profile module ${packageName} already resolves to ${existing}, expected ${expected}`)
    }
  }
  try {
    await assertTarget()
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    try {
      await symlink(packageRoot, link, process.platform === 'win32' ? 'junction' : 'dir')
    } catch (linkError: unknown) {
      if ((linkError as NodeJS.ErrnoException).code !== 'EEXIST') throw linkError
      await assertTarget()
    }
  }
}

if (process.env['DSH_CLOUD_DATABASE_URL']?.trim() === '') {
  throw new Error('DSH_CLOUD_DATABASE_URL must not be empty')
}
if (process.env['DSH_CLOUD_DATABASE_URL'] === undefined) {
  throw new Error('DSH_CLOUD_DATABASE_URL is required')
}

await mkdir(dshHome, { recursive: true, mode: 0o700 })
await exposeCloudPlugin('@dsh-cloud/run-context')
await exposeCloudPlugin('@dsh-cloud/session-persistence-postgres')

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
