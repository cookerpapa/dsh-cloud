# Production Acceptance — 2026-08-17

## Scope

This report covers the current `main` architecture on one self-hosted Linux
machine. The deployment was recreated from empty PostgreSQL, Kafka and Valkey
volumes, then exercised through the public registration, Workspace, Session,
Run and authenticated event-stream APIs. Model responses came from DeepSeek;
all file and process Tools ran in CubeSandbox KVMs.

```text
Browser -> Gateway -> PostgreSQL Run queue -> DSH Worker -> DeepSeek
                                                |
                                                +-> Tool Broker -> Cube KVM

DSH native Session event batches
  -> Kafka (acks=all durability)
  -> Valkey (ordered live projection)
  -> Gateway (only after projected-through watermark)
  -> Browser

settled Turn -> PostgreSQL native gzip segment
successful Compaction -> PostgreSQL runtime baseline + suffix
```

The path contains no Temporal, MinIO/S3, Session Event Relay, Worker affinity,
PostgreSQL token-delta payload table or PostgreSQL event Outbox.

This is a single-host acceptance, not a host-HA, multi-region or hostile-public-
SaaS claim.

## Real user, model and Cube journey

One user registered, created a Workspace and Session, and completed the
following Turns against the same native DSH Session:

| Turn | Verification | Result |
| --- | --- | ---: |
| pure chat | exact marker, no Cube activation | completed, 12.1 s |
| coding 1 | create/test insertion sort | completed, 50.4 s |
| coding 2 | retain prior file, add/test binary search | completed, 74.0 s |
| KVM cold recovery | delete the Cube, preserve Volume, create a new Cube and rerun both tests | completed, 21.3 s |

The two coding Turns reused one Cube activation. The cold-recovery Turn ran in
a newly created KVM while retaining both Workspace files and passing both test
suites. Cube therefore supplies the isolated execution lifecycle; its durable
Volume, not VM memory or process state, supplies Workspace continuity.

## Follow-up authority and browser refresh regression

A separate Session exercised four sequential Runs while the acceptance client
closed and recreated its WebSocket between every Turn. The Runs moved across
both Workers with monotonically increasing Workspace fences `1..4`: the second
Turn recalled the first Turn's exact token, the third created and tested
insertion sort, and the fourth retained that file while adding and testing
binary search. All four terminal reasons were `completed`; no Run remained in
`queued`/`dispatched`, and no stale-authority or `UNKNOWN` result occurred.

The stricter refresh case submitted a follow-up prompt, received its queued Run
id, immediately closed that connection, then opened a new event connection.
Canonical Session history recovered the completed Turn and exact prior token
in 3.16 seconds. This verifies both rapid follow-up fencing and refresh after
admission rather than only reconnecting after a Turn has settled.

## Native Compaction baseline and cold Worker recovery

The same Session was compacted using the real DeepSeek summarizer. Because the
selected model advertises a one-million-token context window, the automatic
threshold was lowered only inside the acceptance containers so a bounded
regression transcript could trigger the production code path. The deployment
default was restored before recovery testing.

The successful Compaction emitted native `compaction/start`,
`compaction/summary` and `compaction/end` events. PostgreSQL then materialized:

| Baseline measure | Result |
| --- | ---: |
| physical prefix represented by the baseline | 2,165 native events |
| compressed baseline payload | 62,667 bytes |
| physical Session position before cold recovery | 2,537 events |
| effective history surface returned after baseline restore | 502 events |

Both DSH Workers were restarted after the baseline was committed. A subsequent
Run landed on `worker-1`, recalled that the earlier files implemented insertion
sort and binary search, mounted the persistent Workspace, reran both tests and
returned `FINAL_BASELINE_COLD_RESTORE_OK` in 24.3 seconds.

This verifies a real model-generated checkpoint and a new Worker restore from
the baseline plus active suffix. PostgreSQL retains the full native log for
history/audit, but runtime preparation no longer scans or materializes the
shadowed physical prefix.

## Foreground multi-agent workflow

A root Agent was explicitly asked to use DSH's `workflow` Tool to review the
two algorithm files. It created two native child Sessions in parallel. One
child reviewed insertion sort and the other reviewed binary search; both were
restricted to the read-only child Tool policy.

| Multi-agent measure | Result |
| --- | ---: |
| root terminal state | completed |
| child Sessions | 2 |
| child lineage | both reference the root Session |
| workflow lifecycle | 1 run start/end, 2 child start/end pairs |
| Workspace mutation by children | none |
| client-visible result | `FINAL_FOREGROUND_MULTI_AGENT_OK` |
| Run duration | 101.9 s |

Continuable/background delegation remains disabled for cloud users. Current
workflow children share the root Workspace, so parallel mutating Agents are
not enabled until each candidate has an isolated Workspace and an explicit
merge/selection boundary.

## Concurrent real-user acceptance

### Chat profile

Six users started together. Every user registered, created an independent
Workspace and Session, completed two model Turns, and recalled a unique token
from the first Turn in the second.

| Measure | Result |
| --- | ---: |
| users / completed / failed | 6 / 6 / 0 |
| completed Turns | 12 |
| wall time | 20.9 s |
| Turn duration p50 / p95 | 6.87 s / 14.48 s |
| durable-first assistant p50 / p95 | 4.78 s / 11.61 s |
| browser-visible native events | 699 |

### Coding profile

Two users started together. Each first created and tested stable merge sort;
the second Turn retained that file, added binary search tests and reran the
whole program. Unique success markers had to occur in the durably gated event
stream; final prose alone could not pass the test.

| Measure | Result |
| --- | ---: |
| users / completed / failed | 2 / 2 / 0 |
| completed Turns | 4 |
| wall time | 303.4 s |
| Turn duration p50 / p95 | 103.85 s / 199.04 s |
| durable-first assistant p50 / p95 | 4.56 s / 10.72 s |
| browser-visible native events | 8,276 |

The long terminal latency reflects real multi-step model/Tool work and Python
testing on this laptop's Cube compute plane. It must not be presented as a
storage or queue throughput result.

## Component throughput

Disposable targets were used so the benchmark did not touch product topics or
tables. The bounded final run used the same WSL host while the production stack
was online.

| Component | Workload | Result |
| --- | --- | ---: |
| Kafka | 20,000 x 1 KiB, `acks=all`, idempotent producer, gzip, 12 partitions | 14,104 records/s (13.77 MiB/s), 4 ms p50, 7 ms p95, 13 ms p99 |
| PostgreSQL | pgbench scale 10, 8 clients/threads, 5 s read/write TPC-B-like workload | 931 TPS, 8.59 ms average, 0 failures |

These numbers measure local storage/messaging headroom, not concurrent Agent
Loop capacity. Agent capacity is bounded by model latency, Worker slots and
Cube compute resources before either component reaches these raw rates.

## Automated verification

The deterministic suite covers:

- 33 default unit/protocol checks and 87 PostgreSQL/Kafka/Valkey integration
  checks on the final source;
- TypeScript builds, formatting and unused-symbol checks;
- SessionPersistence upstream contract compatibility;
- baseline/suffix reconstruction, repeated Compaction and corrupted-baseline
  fallback;
- Run claim, lease, cancellation, ambiguous dispatch and stale-fence behavior;
- Kafka group commit, Valkey projection/rebuild and browser visibility gating;
- Tool Broker authorization, cross-tenant isolation and Workspace activation;
- root/child Session lineage, forged-child rejection and read-only workflow
  policy;
- production Compose rendering, Helm rendering, shell syntax and CI checks.

Real-model and Cube tests remain explicit acceptance jobs rather than CI gates
because they consume provider credit and require a KVM execution plane.

## Accepted limits

- The Compose profile uses one PostgreSQL, Kafka and Valkey instance. The Helm
  profile expects externally operated replicated data services for HA.
- Kafka retention bounds unfinished-Turn recovery. Settled Turn and Compaction
  recovery use PostgreSQL native segments/baselines, not Valkey.
- Cube Volume persistence restores files, not process memory, sockets or PTYs.
- Arbitrary shell commands are not claimed to be exactly once. An ambiguous
  mutating Tool execution is not blindly replayed.
- The baseline contract optimizes runtime preparation; it does not delete the
  full native event log required for history, audit and future re-projection.
- Parallel mutating multi-agent work requires isolated candidate Workspaces and
  a merge policy; the current supported cloud mode is foreground, read-only
  analysis through `workflow`.
