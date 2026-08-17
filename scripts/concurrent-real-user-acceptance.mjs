import { randomUUID } from 'node:crypto'
import { CloudAcceptanceClient, assistantText, percentile } from './lib/cloud-acceptance-client.mjs'

const profiles = {
  chat: token => [
    {
      prompt: `Remember this exact token for the next turn: ${token}. Reply briefly and include it exactly once.`,
      historyIncludes: [token],
    },
    {
      prompt: 'What exact token did I ask you to remember? Reply with that token.',
      historyIncludes: [token],
    },
  ],
  coding: token => [
    {
      prompt: `Create algorithms.py with a stable merge_sort implementation and deterministic tests for empty, duplicate, negative, sorted and reverse-sorted inputs. Actually run python3 algorithms.py. On success the program must print ${token}_SORT_OK.`,
      eventIncludes: [`${token}_SORT_OK`],
    },
    {
      prompt: `Keep the existing merge sort and tests. Add binary_search with tests for hit, miss, empty input and duplicate values. Run the full file again and make the final successful program output include ${token}_SEARCH_OK.`,
      eventIncludes: [`${token}_SEARCH_OK`],
      historyIncludes: [`${token}_SEARCH_OK`],
    },
  ],
}
const baseUrl = process.env.DSH_CLOUD_ACCEPTANCE_URL || 'http://127.0.0.1:18080'
const users = readInteger('DSH_CLOUD_LOAD_USERS', 6, 1, 200)
const arrivalRate = readNumber('DSH_CLOUD_LOAD_ARRIVAL_RATE', 0, 0, 1_000)
const timeoutMs = readInteger('DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS', 300_000, 10_000, 1_800_000)
const profileName = process.env.DSH_CLOUD_LOAD_PROFILE || 'chat'
const profile = profiles[profileName]
if (profile === undefined) throw new Error(`unknown DSH_CLOUD_LOAD_PROFILE: ${profileName}`)

function readInteger(name, fallback, minimum, maximum) {
  const value = Number(process.env[name] ?? fallback)
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new Error(`${name} must be an integer from ${minimum} to ${maximum}`)
  return value
}

function readNumber(name, fallback, minimum, maximum) {
  const value = Number(process.env[name] ?? fallback)
  if (!Number.isFinite(value) || value < minimum || value > maximum) throw new Error(`${name} must be a number from ${minimum} to ${maximum}`)
  return value
}

function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

async function journey(index) {
  if (arrivalRate > 0) await sleep(index * 1_000 / arrivalRate)
  const token = `DSH_EVAL_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`
  const startedAt = performance.now()
  const client = await CloudAcceptanceClient.register(baseUrl, `Load User ${index + 1}`)
  const { workspaceId, sessionId } = await client.createWorkspaceSession(`load-${index + 1}-${randomUUID().slice(0, 8)}`)
  const stream = await client.connect(sessionId)
  const turns = []
  try {
    const turnDefinitions = profile(token)
    for (const turn of turnDefinitions) {
      const result = await stream.prompt(turn.prompt, timeoutMs)
      const encoded = JSON.stringify(result.events)
      const reason = result.terminal?.data?.reason?.kind
      if (reason !== 'completed') throw new Error(`turn ended with ${JSON.stringify(result.terminal?.data?.reason)}`)
      for (const expected of turn.eventIncludes ?? []) {
        if (!encoded.includes(expected)) throw new Error(`durable browser events did not contain ${expected}`)
      }
      turns.push({
        runId: result.runId,
        durationMs: result.durationMs,
        firstAssistantMs: result.firstAssistantMs,
        events: result.events.length,
        assistantChunkFlow: result.assistantChunkFlow,
        textDeltaFlow: result.textDeltaFlow,
      })
    }
    const history = await stream.history()
    const answer = assistantText(history)
    for (const expected of turnDefinitions.at(-1)?.historyIncludes ?? []) {
      if (!answer?.includes(expected)) throw new Error(`final assistant message did not contain ${expected}`)
    }
    return { ok: true, index, sessionId, workspaceId, durationMs: Math.round(performance.now() - startedAt), turns }
  } finally {
    stream.close()
  }
}

const startedAt = performance.now()
const settled = await Promise.allSettled(Array.from({ length: users }, (_, index) => journey(index)))
const succeeded = settled.filter(result => result.status === 'fulfilled').map(result => result.value)
const failed = settled.flatMap((result, index) => result.status === 'rejected'
  ? [{ index, error: result.reason instanceof Error ? result.reason.message : String(result.reason) }]
  : [])
const turns = succeeded.flatMap(result => result.turns)
const durations = turns.map(turn => turn.durationMs)
const firstAssistant = turns.flatMap(turn => turn.firstAssistantMs === undefined ? [] : [turn.firstAssistantMs])
const report = {
  profile: profileName,
  workloadModel: arrivalRate === 0 ? 'closed-simultaneous' : 'open-arrival',
  configuredUsers: users,
  arrivalRatePerSecond: arrivalRate || undefined,
  completedUsers: succeeded.length,
  failedUsers: failed.length,
  completedTurns: turns.length,
  wallTimeMs: Math.round(performance.now() - startedAt),
  turnDurationMs: summary(durations),
  firstAssistantMs: summary(firstAssistant),
  browserVisibleEvents: turns.reduce((total, turn) => total + turn.events, 0),
  failures: failed,
  sessions: succeeded,
}
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`)
if (failed.length > 0) process.exitCode = 1

function summary(values) {
  if (values.length === 0) return undefined
  return {
    minimum: Math.min(...values),
    p50: percentile(values, 0.50),
    p95: percentile(values, 0.95),
    p99: percentile(values, 0.99),
    maximum: Math.max(...values),
    mean: Math.round(values.reduce((total, value) => total + value, 0) / values.length),
  }
}
