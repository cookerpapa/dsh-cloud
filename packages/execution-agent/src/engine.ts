import { constants } from 'node:fs'
import { access, lstat, mkdir, readFile, readdir, realpath, rename, rm, stat, writeFile } from 'node:fs/promises'
import { delimiter, dirname, isAbsolute, join, posix, relative, resolve, sep } from 'node:path'
import { PassThrough } from 'node:stream'
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import * as nodePty from 'node-pty'
import type {
  ExecutionOperation,
  RemoteFileInfo,
  RemoteProcessResult,
  RemoteProcessSnapshot,
  RemoteTerminalSnapshot,
} from '@dsh-cloud/execution-protocol'
import { MAX_PROCESS_OUTPUT_BYTES } from '@dsh-cloud/execution-protocol'

const SAFE_SIGNALS = new Set(['SIGINT', 'SIGTERM', 'SIGKILL', 'SIGTSTP', 'SIGHUP'])

class TailBuffer {
  private data = Buffer.alloc(0)
  private dropped = 0

  constructor(private readonly maximumBytes: number) {}

  append(chunk: Uint8Array): void {
    const combined = Buffer.concat([this.data, Buffer.from(chunk)])
    if (combined.byteLength <= this.maximumBytes) {
      this.data = combined
      return
    }
    const overflow = combined.byteLength - this.maximumBytes
    this.data = combined.subarray(overflow)
    this.dropped += overflow
  }

  read(offset: number): { bytes: Buffer; nextOffset: number; lossy: boolean } {
    const end = this.dropped + this.data.byteLength
    const start = Math.max(offset, this.dropped)
    return {
      bytes: this.data.subarray(start - this.dropped),
      nextOffset: end,
      lossy: offset < this.dropped,
    }
  }

  allText(): { text: string; truncated: boolean } {
    return { text: this.data.toString('utf8'), truncated: this.dropped > 0 }
  }
}

interface ManagedProcess {
  readonly child: ChildProcessWithoutNullStreams
  readonly stdout: TailBuffer
  readonly stderr: TailBuffer
  readonly done: Promise<RemoteProcessResult>
  result?: RemoteProcessResult
}

interface ManagedTerminal {
  readonly terminal: nodePty.IPty
  readonly output: TailBuffer
  readonly done: Promise<void>
  exitCode?: number | null
  signal?: NodeJS.Signals | null
}

function cleanEnvironment(explicit: Record<string, string | undefined> | undefined): NodeJS.ProcessEnv {
  const output: NodeJS.ProcessEnv = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined && !/KEY|PASSWORD|SECRET|TOKEN/i.test(key) && !key.toUpperCase().startsWith('DSH_')) {
      output[key] = value
    }
  }
  for (const [key, value] of Object.entries(explicit ?? {})) {
    if (value === undefined) delete output[key]
    else output[key] = value
  }
  return output
}

function toFileInfo(path: string, info: Awaited<ReturnType<typeof lstat>>): RemoteFileInfo {
  const kind = info.isSymbolicLink()
    ? 'symlink' as const
    : info.isFile()
      ? 'file' as const
      : info.isDirectory()
        ? 'directory' as const
        : 'other' as const
  return {
    path,
    name: path === '/' ? '/' : path.split('/').at(-1) ?? path,
    kind,
    size: Number(info.size),
    mode: Number(info.mode),
    modifiedMs: Number(info.mtimeMs),
  }
}

function signalName(signal: NodeJS.Signals | null): NodeJS.Signals | null {
  return signal
}

/**
 * Credential-free execution engine intended to run inside one Cube microVM.
 * The trusted Sandbox Manager owns identity and fencing; this class owns only
 * filesystem containment, process trees, bounded output and cancellation.
 */
export class ExecutionEngine {
  readonly workspaceRoot: string
  readonly runtimeRoot: string
  private readonly processes = new Map<number, ManagedProcess>()
  private readonly terminals = new Map<number, ManagedTerminal>()

  constructor(workspaceRoot = '/workspace', runtimeRoot = '/tmp/dsh-cloud-runtime') {
    if (!isAbsolute(workspaceRoot) || !isAbsolute(runtimeRoot)) throw new TypeError('execution roots must be absolute')
    this.workspaceRoot = resolve(workspaceRoot)
    this.runtimeRoot = resolve(runtimeRoot)
  }

  async initialize(): Promise<void> {
    await mkdir(this.workspaceRoot, { recursive: true, mode: 0o700 })
    await mkdir(this.runtimeRoot, { recursive: true, mode: 0o700 })
  }

  async execute(operation: ExecutionOperation): Promise<unknown> {
    switch (operation.kind) {
      case 'fs.resolve': return this.resolvePath(operation.path, operation.cwd)
      case 'fs.stat': return this.fileStat(operation.path, operation.follow)
      case 'fs.read': return this.read(operation.path, operation.maxBytes)
      case 'fs.list': return this.list(operation.path)
      case 'fs.mkdir': await mkdir(await this.safePath(operation.path, true), { recursive: true, mode: 0o700 }); return true
      case 'fs.write': return this.write(operation.path, operation.dataBase64)
      case 'fs.rename': return this.move(operation.from, operation.to)
      case 'fs.remove': await rm(await this.safePath(operation.path, false), { recursive: true, force: true }); return true
      case 'process.resolve': return this.resolveExecutable(operation.command, operation.env)
      case 'process.start': return this.startProcess(operation)
      case 'process.poll': return this.pollProcess(operation.pid, operation.stdoutOffset, operation.stderrOffset)
      case 'process.stdin': return this.processInput(operation.pid, operation.dataBase64, operation.close)
      case 'process.terminate': return this.terminateProcess(operation.pid, operation.graceMs)
      case 'process.list': return [...this.processes.entries()].filter(([, item]) => item.result === undefined).map(([pid]) => ({ pid }))
      case 'terminal.start': return this.startTerminal(operation)
      case 'terminal.poll': return this.pollTerminal(operation.pid, operation.outputOffset)
      case 'terminal.input': return this.terminalInput(operation.pid, operation.dataBase64)
      case 'terminal.resize': return this.resizeTerminal(operation.pid, operation.rows, operation.cols)
      case 'terminal.signal': return this.signalTerminal(operation.pid, operation.signal)
      case 'terminal.terminate': return this.terminateTerminal(operation.pid)
    }
  }

  async dispose(): Promise<void> {
    await Promise.allSettled([
      ...[...this.processes].map(([pid]) => this.terminateProcess(pid, 100)),
      ...[...this.terminals].map(([pid]) => this.terminateTerminal(pid)),
    ])
  }

  private isAllowed(path: string): boolean {
    return [this.workspaceRoot, this.runtimeRoot].some(root => path === root || (!relative(root, path).startsWith(`..${sep}`) && relative(root, path) !== '..' && !isAbsolute(relative(root, path))))
  }

  private async safePath(input: string, permitMissing: boolean): Promise<string> {
    if (!isAbsolute(input) || input.includes('\0')) throw Object.assign(new Error('path must be absolute'), { code: 'EINVAL' })
    const normalized = resolve(input)
    if (!this.isAllowed(normalized)) throw Object.assign(new Error('path escapes the execution roots'), { code: 'EACCES' })
    try {
      const canonical = await realpath(normalized)
      if (!this.isAllowed(canonical)) throw Object.assign(new Error('symlink escapes the execution roots'), { code: 'EACCES' })
      return canonical
    } catch (error: unknown) {
      if (!permitMissing || (error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
      const parent = dirname(normalized)
      if (parent === normalized) throw error
      const canonicalParent = await this.safePath(parent, true)
      const candidate = join(canonicalParent, normalized.slice(parent.length + (parent.endsWith(sep) ? 0 : 1)))
      if (!this.isAllowed(candidate)) throw Object.assign(new Error('path escapes the execution roots'), { code: 'EACCES' })
      return candidate
    }
  }

  private async resolvePath(path: string, cwd = this.workspaceRoot): Promise<string> {
    const candidate = isAbsolute(path) ? path : resolve(await this.safePath(cwd, false), path)
    return this.safePath(candidate, true)
  }

  private async fileStat(path: string, follow: boolean): Promise<RemoteFileInfo | undefined> {
    const safe = await this.safePath(path, false).catch((error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT') return undefined
      throw error
    })
    if (safe === undefined) return undefined
    try {
      const info = follow ? await stat(safe) : await lstat(safe)
      return toFileInfo(safe, info)
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
      throw error
    }
  }

  private async read(path: string, maximumBytes: number): Promise<{ dataBase64: string }> {
    if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 0 || maximumBytes > 64 * 1024 * 1024) throw Object.assign(new Error('read limit is invalid'), { code: 'EINVAL' })
    const safe = await this.safePath(path, false)
    const info = await stat(safe)
    if (!info.isFile()) throw Object.assign(new Error('path is not a regular file'), { code: 'EISDIR' })
    if (info.size > maximumBytes) throw Object.assign(new Error('file exceeds the read limit'), { code: 'EFBIG' })
    return { dataBase64: (await readFile(safe)).toString('base64') }
  }

  private async list(path: string): Promise<RemoteFileInfo[]> {
    const safe = await this.safePath(path, false)
    const entries = await readdir(safe, { withFileTypes: true })
    const result = await Promise.all(entries.sort((a, b) => a.name.localeCompare(b.name)).map(async entry => {
      const child = join(safe, entry.name)
      return toFileInfo(child, await lstat(child))
    }))
    return result
  }

  private async write(path: string, dataBase64: string): Promise<RemoteFileInfo> {
    const bytes = Buffer.from(dataBase64, 'base64')
    if (bytes.toString('base64') !== dataBase64 || bytes.byteLength > 8 * 1024 * 1024) throw Object.assign(new Error('file payload is invalid'), { code: 'EINVAL' })
    const safe = await this.safePath(path, true)
    await mkdir(dirname(safe), { recursive: true, mode: 0o700 })
    const temporary = join(dirname(safe), `.dsh-cloud-write-${process.pid}-${crypto.randomUUID()}`)
    await writeFile(temporary, bytes, { mode: 0o600, flag: 'wx' })
    await rename(temporary, safe)
    return toFileInfo(safe, await lstat(safe))
  }

  private async move(from: string, to: string): Promise<RemoteFileInfo> {
    const source = await this.safePath(from, false)
    const target = await this.safePath(to, true)
    await mkdir(dirname(target), { recursive: true, mode: 0o700 })
    await rename(source, target)
    return toFileInfo(target, await lstat(target))
  }

  private async resolveExecutable(command: string, explicit: Record<string, string> | undefined): Promise<string> {
    if (command.length === 0 || command.includes('\0')) throw Object.assign(new Error('executable is invalid'), { code: 'EINVAL' })
    if (!isAbsolute(command) && command.includes('/')) throw Object.assign(new Error('relative executable paths are forbidden'), { code: 'EINVAL' })
    const environment = cleanEnvironment(explicit)
    const candidates = isAbsolute(command)
      ? [command]
      : (environment.PATH ?? '/usr/local/bin:/usr/bin:/bin').split(delimiter).map(directory => resolve(directory, command))
    for (const candidate of candidates) {
      try {
        const info = await stat(candidate)
        if (info.isFile()) {
          await access(candidate, constants.X_OK)
          return candidate
        }
      } catch {
        // Continue through PATH candidates.
      }
    }
    throw Object.assign(new Error(`executable ${JSON.stringify(command)} was not found`), { code: 'ENOENT' })
  }

  private async startProcess(operation: Extract<ExecutionOperation, { kind: 'process.start' }>): Promise<{ pid: number }> {
    const executable = operation.argv[0]
    if (executable === undefined) throw Object.assign(new Error('argv must not be empty'), { code: 'EINVAL' })
    const cwd = await this.safePath(operation.cwd, false)
    const maximum = Math.min(Math.max(operation.maximumOutputBytes, 1024), MAX_PROCESS_OUTPUT_BYTES)
    const child = spawn(executable, [...operation.argv.slice(1)], {
      cwd,
      env: cleanEnvironment(operation.env),
      detached: true,
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    const stdout = new TailBuffer(maximum)
    const stderr = new TailBuffer(maximum)
    child.stdout.on('data', (chunk: Buffer) => stdout.append(chunk))
    child.stderr.on('data', (chunk: Buffer) => stderr.append(chunk))
    if (operation.stdin === 'ignore') child.stdin.end()
    else if (typeof operation.stdin === 'object') child.stdin.end(operation.stdin.data)
    let managed!: ManagedProcess
    const done = new Promise<RemoteProcessResult>((resolveResult, reject) => {
      child.once('error', reject)
      child.once('close', (exitCode, signal) => {
        const out = stdout.allText()
        const err = stderr.allText()
        const result: RemoteProcessResult = {
          pid: child.pid ?? -1,
          exitCode,
          signal: signalName(signal),
          stdout: out.text,
          stderr: err.text,
          stdoutTruncated: out.truncated,
          stderrTruncated: err.truncated,
        }
        managed.result = result
        resolveResult(result)
      })
    })
    if (child.pid === undefined) throw new Error('child process did not publish a pid')
    managed = { child, stdout, stderr, done }
    this.processes.set(child.pid, managed)
    void done.catch(() => {}).finally(() => setTimeout(() => this.processes.delete(child.pid!), 60_000).unref())
    return { pid: child.pid }
  }

  private pollProcess(pid: number, stdoutOffset: number, stderrOffset: number): RemoteProcessSnapshot {
    const managed = this.processes.get(pid)
    if (managed === undefined) throw Object.assign(new Error('process was not found'), { code: 'ESRCH' })
    const stdout = managed.stdout.read(stdoutOffset)
    const stderr = managed.stderr.read(stderrOffset)
    return {
      pid,
      running: managed.result === undefined,
      stdout: stdout.bytes.toString('utf8'),
      stderr: stderr.bytes.toString('utf8'),
      stdoutOffset: stdout.nextOffset,
      stderrOffset: stderr.nextOffset,
      stdoutLossy: stdout.lossy,
      stderrLossy: stderr.lossy,
      ...(managed.result === undefined ? {} : { exitCode: managed.result.exitCode, signal: managed.result.signal }),
    }
  }

  private processInput(pid: number, dataBase64: string, close: boolean): boolean {
    const managed = this.processes.get(pid)
    if (managed === undefined) throw Object.assign(new Error('process was not found'), { code: 'ESRCH' })
    const data = Buffer.from(dataBase64, 'base64')
    if (data.toString('base64') !== dataBase64) throw Object.assign(new Error('stdin payload is invalid'), { code: 'EINVAL' })
    if (data.byteLength > 0) managed.child.stdin.write(data)
    if (close) managed.child.stdin.end()
    return true
  }

  private async terminateProcess(pid: number, graceMs: number): Promise<boolean> {
    const managed = this.processes.get(pid)
    if (managed === undefined || managed.result !== undefined) return false
    try { process.kill(-pid, 'SIGTERM') } catch (error: unknown) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error }
    const timer = new Promise<'timer'>(resolveTimer => setTimeout(() => resolveTimer('timer'), Math.max(0, Math.min(graceMs, 30_000))))
    if (await Promise.race([managed.done.then(() => 'done' as const, () => 'done' as const), timer]) === 'timer') {
      try { process.kill(-pid, 'SIGKILL') } catch (error: unknown) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error }
      await managed.done.catch(() => undefined)
    }
    return true
  }

  private async startTerminal(operation: Extract<ExecutionOperation, { kind: 'terminal.start' }>): Promise<{ pid: number }> {
    const executable = operation.argv[0]
    if (executable === undefined) throw Object.assign(new Error('terminal argv must not be empty'), { code: 'EINVAL' })
    const cwd = await this.safePath(operation.cwd, false)
    const output = new TailBuffer(Math.min(Math.max(operation.maximumOutputBytes, 1024), MAX_PROCESS_OUTPUT_BYTES))
    const terminal = nodePty.spawn(executable, [...operation.argv.slice(1)], {
      cwd,
      env: cleanEnvironment(operation.env) as Record<string, string>,
      rows: operation.rows,
      cols: operation.cols,
      name: 'xterm-256color',
    })
    let managed!: ManagedTerminal
    terminal.onData(data => output.append(Buffer.from(data, 'utf8')))
    const done = new Promise<void>(resolveDone => terminal.onExit(({ exitCode, signal }) => {
      managed.exitCode = exitCode
      managed.signal = signal === 0 ? null : 'SIGTERM'
      resolveDone()
    }))
    managed = { terminal, output, done }
    this.terminals.set(terminal.pid, managed)
    void done.finally(() => setTimeout(() => this.terminals.delete(terminal.pid), 60_000).unref())
    return { pid: terminal.pid }
  }

  private pollTerminal(pid: number, offset: number): RemoteTerminalSnapshot {
    const managed = this.terminals.get(pid)
    if (managed === undefined) throw Object.assign(new Error('terminal was not found'), { code: 'ESRCH' })
    const output = managed.output.read(offset)
    return {
      pid,
      running: managed.exitCode === undefined,
      outputBase64: output.bytes.toString('base64'),
      outputOffset: output.nextOffset,
      outputLossy: output.lossy,
      ...(managed.exitCode === undefined ? {} : { exitCode: managed.exitCode, signal: managed.signal ?? null }),
      foregroundProcessGroupId: pid,
      inputWaiting: true,
    }
  }

  private terminalInput(pid: number, dataBase64: string): boolean {
    const managed = this.terminals.get(pid)
    if (managed === undefined) throw Object.assign(new Error('terminal was not found'), { code: 'ESRCH' })
    const data = Buffer.from(dataBase64, 'base64')
    if (data.toString('base64') !== dataBase64) throw Object.assign(new Error('terminal payload is invalid'), { code: 'EINVAL' })
    managed.terminal.write(data.toString('utf8'))
    return true
  }

  private resizeTerminal(pid: number, rows: number, cols: number): boolean {
    const managed = this.terminals.get(pid)
    if (managed === undefined) throw Object.assign(new Error('terminal was not found'), { code: 'ESRCH' })
    managed.terminal.resize(cols, rows)
    return true
  }

  private signalTerminal(pid: number, signal: string): number {
    if (!SAFE_SIGNALS.has(signal)) throw Object.assign(new Error('terminal signal is invalid'), { code: 'EINVAL' })
    const managed = this.terminals.get(pid)
    if (managed === undefined) throw Object.assign(new Error('terminal was not found'), { code: 'ESRCH' })
    process.kill(-pid, signal as NodeJS.Signals)
    return pid
  }

  private async terminateTerminal(pid: number): Promise<boolean> {
    const managed = this.terminals.get(pid)
    if (managed === undefined) return false
    try { managed.terminal.kill('SIGTERM') } catch { return false }
    await Promise.race([managed.done, new Promise(resolveTimer => setTimeout(resolveTimer, 500))])
    if (managed.exitCode === undefined) {
      try { managed.terminal.kill('SIGKILL') } catch { /* already gone */ }
      await managed.done
    }
    return true
  }
}
