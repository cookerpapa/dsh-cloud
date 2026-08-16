import { createHash } from 'node:crypto'
import { posix } from 'node:path'
import { FileSystem, FsError, FsTargetKey, FsVersion } from '@deepseek-ai/dsh-fs'
import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type {
  FsDirEntry,
  FsEditOutcome,
  FsEditRequest,
  FsInfo,
  FsPathInfo,
  FsTarget,
  FsWriteIntent,
  FsWriteOutcome,
} from '@deepseek-ai/dsh-fs'
import type { SandboxExecutionPolicy, SandboxMode } from '@deepseek-ai/dsh-sandbox'
import type { RemoteFileInfo } from '@dsh-cloud/execution-protocol'
import { ExecutionRemoteError } from '@dsh-cloud/execution-client'

const MAX_TEXT_BYTES = 8 * 1024 * 1024

function version(info: RemoteFileInfo): ReturnType<typeof FsVersion> {
  return FsVersion(`cube:${createHash('sha256').update(JSON.stringify([info.path, info.kind, info.size, info.mode, info.modifiedMs, info.symlinkTarget])).digest('hex')}`)
}

function mapError(error: unknown, operation: string, path: string): FsError {
  if (error instanceof FsError) return error
  const code = error instanceof ExecutionRemoteError ? error.code : (error as NodeJS.ErrnoException).code
  if (code === 'ENOENT') return new FsError(`cannot ${operation} "${path}": not found`, 'FS_NOT_FOUND', { cause: error })
  if (code === 'EACCES' || code === 'EPERM') return new FsError(`cannot ${operation} "${path}": permission denied`, 'FS_PERMISSION_DENIED', { cause: error })
  if (code === 'EFBIG') return new FsError(`cannot ${operation} "${path}": file is too large`, 'FS_TOO_LARGE', { cause: error })
  if (code === 'EISDIR') return new FsError(`cannot ${operation} "${path}": not a regular file`, 'FS_NOT_REGULAR_FILE', { cause: error })
  return new FsError(`cannot ${operation} "${path}": ${error instanceof Error ? error.message : 'remote I/O failed'}`, 'FS_IO_ERROR', { cause: error })
}

function decodeText(bytes: Uint8Array, path: string): string {
  if (bytes.subarray(0, 8192).includes(0)) throw new FsError(`cannot read "${path}": binary file`, 'FS_NOT_TEXT')
  try { return new TextDecoder('utf-8', { fatal: true }).decode(bytes) } catch (error: unknown) {
    throw new FsError(`cannot read "${path}": invalid UTF-8`, 'FS_NOT_TEXT', { cause: error })
  }
}

function literalEdit(content: string, request: FsEditRequest, path: string): string {
  const normalized = content.replaceAll('\r\n', '\n')
  const oldString = request.oldString.replaceAll('\r\n', '\n')
  const newString = request.newString.replaceAll('\r\n', '\n')
  if (oldString.length === 0) throw new FsError(`cannot edit "${path}": old string is empty`, 'FS_EDIT_NOT_FOUND')
  const matches = normalized.split(oldString).length - 1
  if (matches === 0) throw new FsError(`cannot edit "${path}": old string was not found`, 'FS_EDIT_NOT_FOUND')
  if (!request.replaceAll && matches !== 1) throw new FsError(`cannot edit "${path}": old string matched ${matches} times`, 'FS_AMBIGUOUS_EDIT')
  return request.replaceAll ? normalized.split(oldString).join(newString) : normalized.replace(oldString, newString)
}

/** DSH filesystem provider whose complete execution world lives in Cube. */
class RemoteFileSystem extends FileSystem {
  static inject = ['cloudExecution']
  static Config = z.object({ workspaceRoot: z.string().default('/workspace') })
  private readonly locks = new Map<string, Promise<unknown>>()
  private readonly workspaceRoot: string

  constructor(ctx: Context, config: { workspaceRoot?: string } = {}) {
    super(ctx)
    this.workspaceRoot = config.workspaceRoot ?? '/workspace'
    if (!posix.isAbsolute(this.workspaceRoot)) throw new Error('remote filesystem workspace root must be absolute')
  }

  override get sandboxMode(): SandboxMode { return 'workspace-write' }

  override async resolve(path: string, opts?: { cwd?: string; signal?: AbortSignal }): Promise<FsTarget> {
    if (path.trim().length === 0) throw new FsError('file path must not be empty', 'FS_NOT_FOUND')
    try {
      const canonical = await this.ctx.cloudExecution.call<string>({ kind: 'fs.resolve', path, cwd: opts?.cwd ?? this.workspaceRoot }, opts?.signal)
      return { targetKey: FsTargetKey(canonical), displayPath: posix.resolve(opts?.cwd ?? this.workspaceRoot, path) }
    } catch (error: unknown) { throw mapError(error, 'resolve', path) }
  }

  override processPath(target: FsTarget): string { return String(target.targetKey) }
  override fileUrl(target: FsTarget): string { return `file://${this.processPath(target).split('/').map(encodeURIComponent).join('/')}` }
  override contains(parent: FsTarget, child: FsTarget): boolean {
    const path = posix.relative(this.processPath(parent), this.processPath(child))
    return path === '' || (path !== '..' && !path.startsWith('../') && !posix.isAbsolute(path))
  }

  override async stat(target: FsTarget, signal?: AbortSignal): Promise<FsInfo | undefined> {
    try {
      const info = await this.ctx.cloudExecution.call<RemoteFileInfo | undefined>({ kind: 'fs.stat', path: this.processPath(target), follow: true }, signal)
      if (info === undefined) return undefined
      return { version: version(info), type: info.kind === 'file' ? 'file' : info.kind === 'directory' ? 'directory' : 'other', ...(info.kind === 'file' ? { size: info.size } : {}) }
    } catch (error: unknown) { throw mapError(error, 'stat', target.displayPath) }
  }

  override async lstat(path: string, opts?: { cwd?: string }, signal?: AbortSignal): Promise<FsPathInfo | undefined> {
    const target = posix.resolve(opts?.cwd ?? this.workspaceRoot, path)
    try {
      const info = await this.ctx.cloudExecution.call<RemoteFileInfo | undefined>({ kind: 'fs.stat', path: target, follow: false }, signal)
      if (info === undefined) return undefined
      return { version: version(info), type: info.kind, ...(info.kind === 'file' ? { size: info.size } : {}) }
    } catch (error: unknown) { throw mapError(error, 'lstat', target) }
  }

  override async readText(target: FsTarget, signal?: AbortSignal): Promise<string> {
    return decodeText(await this.readBytes(target, signal, MAX_TEXT_BYTES), target.displayPath)
  }

  override async streamText(target: FsTarget, signal?: AbortSignal): Promise<AsyncIterable<string>> {
    const text = await this.readText(target, signal)
    return (async function* (): AsyncIterable<string> { for (let offset = 0; offset < text.length; offset += 64 * 1024) yield text.slice(offset, offset + 64 * 1024) })()
  }

  override async readBytes(target: FsTarget, signal: AbortSignal | undefined, maxBytes: number): Promise<Uint8Array> {
    try {
      const output = await this.ctx.cloudExecution.call<{ dataBase64: string }>({ kind: 'fs.read', path: this.processPath(target), maxBytes }, signal)
      return Buffer.from(output.dataBase64, 'base64')
    } catch (error: unknown) { throw mapError(error, 'read', target.displayPath) }
  }

  override async listDir(target: FsTarget, signal?: AbortSignal): Promise<FsDirEntry[]> {
    try {
      const entries = await this.ctx.cloudExecution.call<RemoteFileInfo[]>({ kind: 'fs.list', path: this.processPath(target) }, signal)
      return entries.map(info => ({
        name: info.name,
        type: info.kind === 'file' ? 'file' : info.kind === 'directory' ? 'directory' : 'other',
        target: { targetKey: FsTargetKey(info.path), displayPath: posix.join(target.displayPath, info.name) },
        version: version(info),
        ...(info.kind === 'file' ? { size: info.size } : {}),
      }))
    } catch (error: unknown) { throw mapError(error, 'list', target.displayPath) }
  }

  override async writeText(target: FsTarget, content: string, expected?: FsWriteIntent, signal?: AbortSignal, _policy?: SandboxExecutionPolicy): Promise<FsWriteOutcome> {
    return this.locked(this.processPath(target), () => this.writeUnlocked(target, content, expected, signal))
  }

  override async editText(target: FsTarget, edit: FsEditRequest, expected?: { version: ReturnType<typeof FsVersion> }, signal?: AbortSignal, _policy?: SandboxExecutionPolicy): Promise<FsEditOutcome> {
    return this.locked(this.processPath(target), async () => {
      const info = await this.stat(target, signal)
      if (info === undefined) throw new FsError(`cannot edit "${target.displayPath}": not found`, 'FS_NOT_FOUND')
      if (expected !== undefined && expected.version !== info.version) throw new FsError(`cannot edit "${target.displayPath}": stale version`, 'FS_STALE_VERSION')
      const before = await this.readText(target, signal)
      const after = literalEdit(before, edit, target.displayPath)
      const outcome = await this.writeUnlocked(target, after, undefined, signal)
      return { version: outcome.version, before: before.replaceAll('\r\n', '\n'), after }
    })
  }

  private async writeUnlocked(target: FsTarget, content: string, expected?: FsWriteIntent, signal?: AbortSignal): Promise<FsWriteOutcome> {
    const existing = await this.stat(target, signal)
    if (expected?.kind === 'createIfAbsent' && existing !== undefined) throw new FsError(`cannot write "${target.displayPath}": target already exists`, 'FS_NOT_OBSERVED')
    if (expected?.kind === 'replaceIfVersion' && (existing === undefined || existing.version !== expected.version)) throw new FsError(`cannot write "${target.displayPath}": stale version`, 'FS_STALE_VERSION')
    let before: string | null = null
    if (existing?.type === 'file' && (existing.size ?? 0) <= MAX_TEXT_BYTES) {
      try { before = await this.readText(target, signal) } catch { before = null }
    }
    try {
      const info = await this.ctx.cloudExecution.call<RemoteFileInfo>({ kind: 'fs.write', path: this.processPath(target), dataBase64: Buffer.from(content).toString('base64') }, signal)
      return { operation: existing === undefined ? 'create' : 'update', version: version(info), before, after: content.replaceAll('\r\n', '\n') }
    } catch (error: unknown) { throw mapError(error, 'write', target.displayPath) }
  }

  private async locked<T>(key: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.locks.get(key) ?? Promise.resolve()
    const current = previous.catch(() => undefined).then(operation)
    this.locks.set(key, current)
    try { return await current } finally { if (this.locks.get(key) === current) this.locks.delete(key) }
  }
}

export default RemoteFileSystem
