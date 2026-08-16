import { PassThrough, Writable } from 'node:stream'
import type { Context } from '@deepseek-ai/cordis'
import { SubprocessRuntime } from '@deepseek-ai/dsh-subprocess'
import type {
  SubprocessCollect,
  SubprocessHandle,
  SubprocessOutcome,
  SubprocessOutputMode,
  SubprocessOutputRead,
  SubprocessSpawnSpec,
  SubprocessTerminalForeground,
  SubprocessTerminalHandle,
  SubprocessTerminalSignal,
  SubprocessTerminalSpawnSpec,
} from '@deepseek-ai/dsh-subprocess'
import type CloudExecutionClient from '@dsh-cloud/execution-client'
import type { RemoteProcessSnapshot, RemoteTerminalSnapshot } from '@dsh-cloud/execution-protocol'
import type { RunAuthority } from '@dsh-cloud/run-context'

const POLL_MS = 25

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void; reject: (error: unknown) => void } {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((onResolve, onReject) => { resolve = onResolve; reject = onReject })
  return { promise, resolve, reject }
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) return Promise.reject(signal.reason)
  return new Promise((resolve, reject) => {
    let timer: NodeJS.Timeout
    const onAbort = (): void => { clearTimeout(timer); signal?.removeEventListener('abort', onAbort); reject(signal?.reason) }
    const settle = (): void => { signal?.removeEventListener('abort', onAbort); resolve() }
    timer = setTimeout(settle, ms)
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

function collectMode(value: SubprocessOutputMode): value is SubprocessCollect {
  return value !== 'pipe' && value !== 'inherit'
}

class OutputReader {
  private data = Buffer.alloc(0)
  private dropped = 0
  private spillPath: string | undefined

  constructor(private readonly maximumBytes: number) {}

  append(text: string, lossy: boolean, spillPath?: string): void {
    const chunk = Buffer.from(text)
    if (lossy) this.dropped = Math.max(this.dropped, 1)
    const combined = Buffer.concat([this.data, chunk])
    if (combined.byteLength > this.maximumBytes) {
      const overflow = combined.byteLength - this.maximumBytes
      this.data = combined.subarray(overflow)
      this.dropped += overflow
    } else this.data = combined
    this.spillPath = spillPath
  }

  readFrom(offset: number): SubprocessOutputRead {
    const end = this.dropped + this.data.byteLength
    const start = Math.max(offset, this.dropped)
    return {
      text: this.data.subarray(start - this.dropped).toString('utf8'),
      nextOffset: end,
      lossy: offset < this.dropped,
      ...(this.spillPath === undefined ? {} : { spillPath: this.spillPath }),
    }
  }
}

class DeferredInput extends Writable {
  constructor(private readonly send: (data: Buffer, close: boolean) => Promise<void>) { super() }
  override _write(chunk: Buffer | string, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    void this.send(Buffer.from(chunk), false).then(() => callback(), error => callback(error as Error))
  }
  override _final(callback: (error?: Error | null) => void): void {
    void this.send(Buffer.alloc(0), true).then(() => callback(), error => callback(error as Error))
  }
}

class RemoteHandle implements SubprocessHandle {
  readonly stdout: PassThrough | undefined
  readonly stderr: PassThrough | undefined
  readonly stdin: Writable | undefined
  readonly collected: SubprocessHandle['collected']
  readonly done: Promise<SubprocessOutcome>
  private remotePid = -1
  private readonly ready = deferred<number>()
  private readonly stopped = new AbortController()
  private readonly stdoutReader: OutputReader | undefined
  private readonly stderrReader: OutputReader | undefined
  private readonly authority: RunAuthority
  private termination: Promise<void> | undefined

  constructor(private readonly client: CloudExecutionClient, private readonly spec: SubprocessSpawnSpec) {
    this.authority = client.currentAuthority()
    this.stdout = spec.stdio.stdout === 'pipe' ? new PassThrough() : undefined
    this.stderr = spec.stdio.stderr === 'pipe' ? new PassThrough() : undefined
    this.stdoutReader = collectMode(spec.stdio.stdout) ? new OutputReader(spec.stdio.stdout.maxBytes) : undefined
    this.stderrReader = collectMode(spec.stdio.stderr) ? new OutputReader(spec.stdio.stderr.maxBytes) : undefined
    this.collected = {
      ...(this.stdoutReader === undefined ? {} : { stdout: this.stdoutReader }),
      ...(this.stderrReader === undefined ? {} : { stderr: this.stderrReader }),
    }
    this.stdin = spec.stdio.stdin === 'pipe' ? new DeferredInput((data, close) => this.sendInput(data, close)) : undefined
    this.done = this.run()
    void this.done.catch(() => {})
    void this.done.catch(error => this.ready.reject(error))
    void this.ready.promise.catch(() => {})
    const abort = (): void => { this.terminate() }
    spec.signal?.addEventListener('abort', abort, { once: true })
    void this.done.finally(() => spec.signal?.removeEventListener('abort', abort)).catch(() => {})
  }

  get pid(): number { return this.remotePid }

  terminate(): void {
    if (this.termination !== undefined) return
    this.termination = this.ready.promise.then(async pid => {
      await this.client.call({ kind: 'process.terminate', pid, graceMs: this.spec.graceMs }, undefined, this.authority)
    })
    void this.termination.catch(() => {})
    this.stopped.abort(new Error('remote process termination requested'))
  }

  async waitForExit(signal?: AbortSignal): Promise<boolean> {
    if (signal?.aborted === true) return false
    const result = await Promise.race([
      this.done.then(() => true, () => true),
      ...(signal === undefined ? [] : [new Promise<false>(resolve => signal.addEventListener('abort', () => resolve(false), { once: true }))]),
    ])
    return result
  }

  private async run(): Promise<SubprocessOutcome> {
    const maximumOutputBytes = Math.min(16 * 1024 * 1024, Math.max(
      64 * 1024,
      ...[this.spec.stdio.stdout, this.spec.stdio.stderr].map(mode => collectMode(mode) ? (mode.spill?.maxBytes ?? mode.maxBytes) : 1024 * 1024),
    ))
    const started = await this.client.call<{ pid: number }>({
      kind: 'process.start',
      argv: this.spec.argv,
      cwd: this.spec.cwd,
      ...(this.spec.env === undefined ? {} : { env: this.spec.env }),
      stdin: this.spec.stdio.stdin,
      maximumOutputBytes,
    }, this.spec.signal, this.authority)
    this.remotePid = started.pid
    this.ready.resolve(started.pid)
    let stdoutOffset = 0
    let stderrOffset = 0
    try {
      while (true) {
        const snapshot = await this.client.call<RemoteProcessSnapshot>({ kind: 'process.poll', pid: started.pid, stdoutOffset, stderrOffset }, this.stopped.signal, this.authority)
        stdoutOffset = snapshot.stdoutOffset
        stderrOffset = snapshot.stderrOffset
        this.publish('stdout', snapshot.stdout, snapshot.stdoutLossy)
        this.publish('stderr', snapshot.stderr, snapshot.stderrLossy)
        if (!snapshot.running) return { exitCode: snapshot.exitCode ?? null, signal: snapshot.signal ?? null }
        await delay(POLL_MS, this.stopped.signal)
      }
    } catch (error: unknown) {
      if (!this.stopped.signal.aborted || this.termination === undefined) throw error
      await this.termination
      const snapshot = await this.client.call<RemoteProcessSnapshot>({ kind: 'process.poll', pid: started.pid, stdoutOffset, stderrOffset }, undefined, this.authority)
      this.publish('stdout', snapshot.stdout, snapshot.stdoutLossy)
      this.publish('stderr', snapshot.stderr, snapshot.stderrLossy)
      if (snapshot.running) throw new Error('remote process remained alive after termination')
      return { exitCode: snapshot.exitCode ?? null, signal: snapshot.signal ?? null }
    } finally {
      this.stdout?.end()
      this.stderr?.end()
    }
  }

  private publish(stream: 'stdout' | 'stderr', text: string, lossy: boolean): void {
    const mode = this.spec.stdio[stream]
    if (mode === 'pipe') (stream === 'stdout' ? this.stdout : this.stderr)?.write(text)
    else if (mode === 'inherit') (stream === 'stdout' ? process.stdout : process.stderr).write(text)
    else (stream === 'stdout' ? this.stdoutReader : this.stderrReader)?.append(text, lossy)
  }

  private async sendInput(data: Buffer, close: boolean): Promise<void> {
    const pid = await this.ready.promise
    await this.client.call({ kind: 'process.stdin', pid, dataBase64: data.toString('base64'), close }, undefined, this.authority)
  }
}

class RemoteTerminal implements SubprocessTerminalHandle {
  readonly output = new PassThrough()
  readonly done: Promise<SubprocessOutcome>
  private offset = 0
  private lastForeground: SubprocessTerminalForeground | undefined
  private terminated = false
  private readonly authority: RunAuthority

  constructor(readonly pid: number, private readonly client: CloudExecutionClient, signal?: AbortSignal) {
    this.authority = client.currentAuthority()
    this.done = this.poll()
    void this.done.catch(() => {})
    signal?.addEventListener('abort', () => void this.terminate(), { once: true })
  }

  async write(data: string): Promise<void> {
    await this.client.call({ kind: 'terminal.input', pid: this.pid, dataBase64: Buffer.from(data).toString('base64') }, undefined, this.authority)
  }
  async inspectForeground(): Promise<SubprocessTerminalForeground | undefined> { return this.lastForeground }
  async signalForeground(signal: SubprocessTerminalSignal): Promise<number> {
    return this.client.call<number>({ kind: 'terminal.signal', pid: this.pid, signal }, undefined, this.authority)
  }
  async terminate(): Promise<void> {
    if (this.terminated) return
    this.terminated = true
    await this.client.call({ kind: 'terminal.terminate', pid: this.pid }, undefined, this.authority).catch(() => undefined)
    await this.done.catch(() => undefined)
  }
  async resize(rows: number, cols: number): Promise<void> {
    await this.client.call({ kind: 'terminal.resize', pid: this.pid, rows, cols }, undefined, this.authority)
  }

  private async poll(): Promise<SubprocessOutcome> {
    try {
      while (true) {
        const snapshot = await this.client.call<RemoteTerminalSnapshot>({ kind: 'terminal.poll', pid: this.pid, outputOffset: this.offset }, undefined, this.authority)
        this.offset = snapshot.outputOffset
        if (snapshot.outputLossy) this.output.emit('error', new Error('remote terminal output was truncated'))
        const bytes = Buffer.from(snapshot.outputBase64, 'base64')
        if (bytes.byteLength > 0) this.output.write(bytes)
        this.lastForeground = snapshot.foregroundProcessGroupId === undefined ? undefined : {
          processGroupId: snapshot.foregroundProcessGroupId,
          inputWaiting: snapshot.inputWaiting ?? false,
        }
        if (!snapshot.running) return { exitCode: snapshot.exitCode ?? null, signal: snapshot.signal ?? null }
        await delay(POLL_MS)
      }
    } finally { this.output.end() }
  }
}

/** DSH subprocess provider backed by the same remote Cube world as RemoteFileSystem. */
class RemoteSubprocessRuntime extends SubprocessRuntime {
  static inject = ['cloudExecution']
  private readonly live = new Set<RemoteHandle>()
  private readonly terminals = new Set<RemoteTerminal>()

  constructor(ctx: Context) {
    super(ctx)
    ctx.effect(() => async () => {
      for (const handle of this.live) handle.terminate()
      await Promise.allSettled([...this.live].map(handle => handle.done))
      await Promise.allSettled([...this.terminals].map(terminal => terminal.terminate()))
    }, 'remote subprocess teardown')
  }

  override resolveExecutable(command: string, env?: Readonly<Record<string, string>>, signal?: AbortSignal): Promise<string> {
    return this.ctx.cloudExecution.call<string>({ kind: 'process.resolve', command, ...(env === undefined ? {} : { env: { ...env } }) }, signal)
  }

  override spawn(spec: SubprocessSpawnSpec): SubprocessHandle {
    const handle = new RemoteHandle(this.ctx.cloudExecution, spec)
    this.live.add(handle)
    void handle.done.finally(() => this.live.delete(handle)).catch(() => {})
    return handle
  }

  override async spawnTerminal(spec: SubprocessTerminalSpawnSpec): Promise<SubprocessTerminalHandle> {
    const started = await this.ctx.cloudExecution.call<{ pid: number }>({
      kind: 'terminal.start',
      argv: spec.argv,
      cwd: spec.cwd,
      ...(spec.env === undefined ? {} : { env: spec.env }),
      rows: spec.rows,
      cols: spec.cols,
      maximumOutputBytes: 4 * 1024 * 1024,
    }, spec.signal)
    const terminal = new RemoteTerminal(started.pid, this.ctx.cloudExecution, spec.signal)
    this.terminals.add(terminal)
    void terminal.done.finally(() => this.terminals.delete(terminal)).catch(() => {})
    return terminal
  }
}

export default RemoteSubprocessRuntime
