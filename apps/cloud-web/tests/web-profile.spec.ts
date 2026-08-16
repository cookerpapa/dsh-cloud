import { spawn, type ChildProcess } from 'node:child_process'
import { mkdir, mkdtemp, realpath, rm, symlink } from 'node:fs/promises'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'

const databaseUrl = process.env['DSH_CLOUD_TEST_DATABASE_URL']
const integration = databaseUrl === undefined ? describe.skip : describe
const children = new Set<ChildProcess>()
const temporaryHomes: string[] = []

afterEach(async () => {
  for (const child of children) child.kill('SIGTERM')
  children.clear()
  for (const directory of temporaryHomes.splice(0)) {
    await rm(directory, { recursive: true, force: true })
  }
})

async function availablePort(): Promise<number> {
  const server = createServer()
  await new Promise<void>((resolvePromise, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolvePromise)
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('test server did not receive a TCP port')
  await new Promise<void>((resolvePromise, reject) => server.close(error => error === undefined
    ? resolvePromise()
    : reject(error)))
  return address.port
}

function waitForWeb(child: ChildProcess, timeoutMs: number): Promise<string> {
  return new Promise((resolvePromise, reject) => {
    let stdout = ''
    let stderr = ''
    const timer = setTimeout(() => {
      cleanup()
      reject(new Error(`DSH Cloud Web did not become ready\nstdout:\n${stdout}\nstderr:\n${stderr}`))
    }, timeoutMs)
    const cleanup = (): void => {
      clearTimeout(timer)
      child.stdout?.off('data', onStdout)
      child.stderr?.off('data', onStderr)
      child.off('exit', onExit)
    }
    const onStdout = (chunk: Buffer): void => {
      stdout += chunk.toString('utf8')
      const match = /dsh web: (http:\/\/[^\s]+)/.exec(stdout)
      if (match?.[1] !== undefined) {
        cleanup()
        resolvePromise(match[1])
      }
    }
    const onStderr = (chunk: Buffer): void => { stderr += chunk.toString('utf8') }
    const onExit = (code: number | null): void => {
      cleanup()
      reject(new Error(`DSH Cloud Web exited before readiness with ${String(code)}\n${stderr}`))
    }
    child.stdout?.on('data', onStdout)
    child.stderr?.on('data', onStderr)
    child.once('exit', onExit)
  })
}

integration('Cloud Web profile', () => {
  it('serves the official DSH frontend with PostgreSQL persistence mounted', async () => {
    const repositoryRoot = resolve(import.meta.dirname, '../../..')
    const home = await mkdtemp(join(tmpdir(), 'dsh-cloud-web-'))
    temporaryHomes.push(home)
    const stalePlugin = join(home, 'profiles', 'node_modules', '@dsh-cloud', 'run-admission')
    await mkdir(resolve(stalePlugin, '..'), { recursive: true })
    await symlink('/missing/previous-image/run-admission', stalePlugin)
    const port = await availablePort()
    const child = spawn(process.execPath, [join(repositoryRoot, 'apps/cloud-web/lib/main.js')], {
      cwd: repositoryRoot,
      env: {
        ...process.env,
        DSH_HOME: home,
        DSH_CLOUD_DATABASE_URL: databaseUrl as string,
        DSH_CLOUD_NAMESPACE: `web-smoke-${port}`,
        DSH_CLOUD_SANDBOX_MANAGER_URL: 'http://127.0.0.1:9',
        DSH_CLOUD_SANDBOX_MANAGER_TOKEN: 'web-smoke-manager-token-32-characters',
        DSH_CLOUD_HOST: '127.0.0.1',
        DSH_CLOUD_PORT: String(port),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    children.add(child)

    const url = await waitForWeb(child, 30_000)
    const response = await fetch(url)
    expect(response.status).toBe(200)
    const html = await response.text()
    expect(html).toContain('window.__DSH_BOOT__')
    expect(html).toContain('@deepseek-ai/dsh-client-ui-conversation')
    expect(await realpath(stalePlugin)).toContain('run-admission')

    child.kill('SIGTERM')
    await new Promise<void>(resolvePromise => child.once('exit', () => resolvePromise()))
    children.delete(child)
  }, 40_000)
})
