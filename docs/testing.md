# Testing DSH Cloud

DSH Cloud separates Agent quality from infrastructure capacity. A successful
chat response is not a storage benchmark, and a high Kafka record rate is not
evidence that an Agent can modify a repository correctly.

## Verification layers

| Layer | Purpose | Command |
| --- | --- | --- |
| Unit and protocol contracts | Event ordering, validation, replay and adapter behavior without external services | `pnpm test` |
| Service integration | PostgreSQL, Kafka and Valkey persistence, Run ownership, fencing, browser durability and recovery | `pnpm test:integration` |
| Failure semantics | Stale attempts, ambiguous dispatch, cancellation, projection rebuild and sandbox isolation | Included in `pnpm test:integration` |
| Multi-agent contracts | Root/child Session lineage, shared fence enforcement and cloud tool-policy visibility | Included in `pnpm test:integration` |
| Real-user journey | Public registration, Workspace and Session creation, authenticated WebSocket stream and a real model Turn | `pnpm acceptance:real-user` |
| Concurrent real-user journey | Multi-user chat or coding tasks with latency percentiles and objective output markers | `pnpm acceptance:concurrent` |
| Component throughput | Kafka durable producer and PostgreSQL read/write headroom on disposable targets | `pnpm benchmark:components` |

The deterministic layers run first. Real-model tests are acceptance tests, not
CI gates: they consume tokens and require a live DSH Worker and model
credential. Cube coding tests additionally require the Tool Broker and Cube
compute plane.

## Real-user acceptance

One Turn against an already running deployment:

```bash
DSH_CLOUD_ACCEPTANCE_PROMPT='Reply with the word READY.' \
  pnpm acceptance:real-user
```

The script persists its generated account, Workspace and Session in
`/tmp/dsh-cloud-real-user.json` so a second invocation exercises native
multi-Turn recovery instead of silently creating a new Session.

The refresh regression submits a follow-up Turn, immediately closes its event
connection, reconnects as the same user, and requires the completed answer to
appear in canonical Session history:

```bash
pnpm acceptance:refresh
```

## Concurrent workloads

The `chat` profile creates independent users, Workspaces and Sessions, then
runs two Turns per Session. The second Turn must recover a unique token from
the first Turn:

```bash
DSH_CLOUD_LOAD_USERS=6 \
DSH_CLOUD_LOAD_PROFILE=chat \
  pnpm acceptance:concurrent
```

The `coding` profile creates and tests merge sort, then preserves that file and
adds binary search in a second Turn. Unique success markers must occur in the
durably gated browser event stream; an assistant merely claiming success does
not pass:

```bash
DSH_CLOUD_LOAD_USERS=2 \
DSH_CLOUD_LOAD_PROFILE=coding \
DSH_CLOUD_ACCEPTANCE_TIMEOUT_MS=600000 \
  pnpm acceptance:concurrent
```

The default starts all journeys together, which is a closed simultaneous-load
test. Set an arrival rate to create an open workload that does not slow request
arrival merely because the system is responding slowly:

```bash
DSH_CLOUD_LOAD_USERS=30 \
DSH_CLOUD_LOAD_ARRIVAL_RATE=2 \
  pnpm acceptance:concurrent
```

The report distinguishes time to the first durably visible
`assistant/chunk`, terminal Turn latency, failures and total browser-visible
events. Capacity claims must state the workload profile, Worker count, Cube
compute capacity and model limits.

## Component throughput

```bash
pnpm benchmark:components
```

The benchmark creates a unique Kafka topic and PostgreSQL database, runs an
`acks=all` idempotent gzip Kafka producer and a `pgbench` read/write workload,
then deletes both targets. It never writes benchmark rows or records into
product tables or topics. Environment variables control the bounded workload:

```text
DSH_CLOUD_BENCH_KAFKA_RECORDS
DSH_CLOUD_BENCH_POSTGRES_SECONDS
DSH_CLOUD_BENCH_POSTGRES_CLIENTS
```

Run component benchmarks separately from the end-to-end workload. Their role
is to identify messaging or relational-storage headroom after the application
path has been verified, not to estimate the number of concurrent Agent Loops.

## Compaction baseline acceptance

The deterministic SessionPersistence suite is the normal regression gate: it
creates native Compaction events, verifies baseline-plus-suffix restoration,
advances the baseline twice, corrupts derived bytes and proves canonical-log
fallback. A real-model acceptance additionally verifies the summarizer and
Worker handoff, but must not require hundreds of thousands of paid tokens just
to cross a large production model's pressure threshold.

For that bounded acceptance only, lower `thresholdRatio` and `retainRatio` in
the selected preset inside the disposable Worker containers, run a multi-Turn
coding Session until native `compaction/start`, `compaction/summary` and a
successful `compaction/end` are durable, and verify the PostgreSQL runtime
baseline. Restore the packaged defaults, restart every Worker, then run another
Turn that must recall prior conversation facts and execute against the retained
Workspace. Record the temporary threshold and never publish its latency as a
default-production Compaction measurement.

## Agent-task grading principles

- Use fresh Workspaces for independent tasks.
- Pin the model and environment when comparing results.
- Grade repository state and executed tests, not the final prose response.
- Keep task inputs and expected checks deterministic.
- Report completed, failed and timed-out tasks; never discard failed samples.
- Use repeated runs before publishing a success rate or latency percentile.
- Keep model/Cube saturation separate from Control Plane, Kafka and PostgreSQL
  saturation.

This follows the same basic separation used by coding-agent evaluation
harnesses: reproducible execution environments and objective test outcomes for
task quality, plus explicit load models and thresholds for infrastructure.
