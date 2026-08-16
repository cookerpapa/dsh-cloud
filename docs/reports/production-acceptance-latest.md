# Production Acceptance — 2026-08-16

## Scope

This acceptance exercised the current DSH Cloud source as an external user on
one self-hosted Linux machine. The fresh deployment used the released DSH Web
UI and Agent Loop, real DeepSeek model calls, two independently started DSH
Workers, PostgreSQL, Kafka, Valkey, the Tool Broker, and an existing
CubeSandbox KVM control/compute installation.

It validates the current plugin boundaries, browser durability gate, native
Session recovery and remote Tool path. It does not claim host-level high
availability, hostile-public-SaaS isolation, or multi-region storage
durability.

```text
Browser -> Gateway -> PostgreSQL Run queue -> DSH Worker -> DeepSeek
                                                |
                                                +-> Tool Broker -> Cube KVM

DSH native Session events
    -> SessionLiveLog Provider (Kafka, acks=all)
    -> SessionLiveProjection Provider (Valkey, ordered/rebuildable)
    -> PostgreSQL projected-through watermark
    -> browser-visible frame

settled Turn -> one PostgreSQL native gzip segment
```

There is no Temporal, MinIO, S3, Session Event Relay, PostgreSQL delta-payload
table or PostgreSQL event Outbox in this path.

## Automated verification

- all TypeScript project references build with unused local/parameter checks;
- 52 PostgreSQL/service integration cases pass against real PostgreSQL, Kafka
  and Valkey services;
- the tests cover native Session reconstruction, fixed-window group commit,
  segment digest verification, stale fences, ambiguous prompt dispatch,
  cross-tenant access, Run claims, cancellation, concurrent activation,
  retryable Workspace deletion, Tool Broker authorization, Valkey loss and
  rebuild, WebSocket text framing and failure injection;
- a fresh two-Worker boot validates concurrent schema initialization; the
  migration path serializes initialization with a PostgreSQL advisory lock;
- the current control-schema tests verify that users and Sessions contain no
  Worker-placement column and consecutive Runs can be claimed by different
  Workers;
- first-Run admission waits for DSH-native Session materialization, and the
  Cloud profile disposes settled ordinary Agent handles before cold recovery;
- Helm lint/render, production Compose rendering, shell syntax and whitespace
  checks pass.

## Real-user model and Cube test

The final source was rebuilt into a separate fresh production deployment with
new PostgreSQL, Kafka and Valkey volumes. Both Workers became healthy before
the user request was submitted.

The real user prompt asked the Agent to create `smoke.py`, implement a function
that returns `42`, add an assertion, and actually run `python3 smoke.py`.

| Measure | Result |
| --- | --- |
| terminal state | completed |
| real model | DeepSeek |
| Tool runtime | CubeSandbox KVM |
| client duration | 84.7 s |
| browser-observed native events | 254 |
| final Tool result | `smoke.py OK: 42` |

The file was written inside Cube, Python executed successfully, and the final
assistant response reported the tested result rather than merely generating
source text.

An additional two-Turn coding acceptance during the same migration exercised
warm Cube reuse. The first Turn created and tested insertion sort; the second
Turn read the retained file, added a deterministic 100-element test and ran it
again. Both completed against the real model and the same Workspace activation.

The no-affinity build was then exercised as one three-Turn coding Session while
alternately removing the prior Worker from service. Turn 1 ran on Worker 2 and
created/tested insertion sort; Turn 2 ran on Worker 1, recovered the same
Session and Workspace, retained that file and added/tested binary search; Turn
3 returned to Worker 2, recovered both earlier files, added a README and reran
all tests. Every Turn completed through the real DeepSeek and Cube KVM path.

| Cross-Worker Session measure | Result |
| --- | --- |
| Worker sequence | Worker 2 -> Worker 1 -> Worker 2 |
| writer fences | 1 -> 2 -> 3 |
| terminal states | 3 completed, 0 failed |
| client durations | 67.9 s / 73.6 s / 82.9 s |
| final native Session position | revision 51, next seq 4,222 |
| retained Workspace result | insertion sort, binary search and README; both test suites passed |

The control schema contained zero `preferred_worker_id` columns. This is a
forced cross-process recovery result, not load balancing that happened to pick
different Workers.

A second handoff pair kept both Worker processes alive. Capacity was used only
to force deterministic placement: Worker 1 created `handoff.txt` with a unique
marker and completed Fence 4; Worker 2 then claimed Fence 5, directly read that
file without recreating it, listed all prior Workspace files, and accurately
explained from native Session history why the previous Turn had created the
marker. The pair completed in 51.3 s and 50.7 s. The Session finished at
revision 79 / next seq 5,097. This specifically verifies that a settled live
Agent was disposed and later cold-resumed on another still-running Worker,
rather than relying on process restart to clear stale memory.

The final source was then rebuilt into a second isolated deployment and tested
as six new users starting at the same time. Every user registered through the
public API, created a new Workspace and Session, opened the authenticated event
stream, and consumed a real DeepSeek response.

| Concurrent Agent measure | Result |
| --- | ---: |
| new users / Workspaces / Sessions | 6 / 6 / 6 |
| terminal results | 6 completed, 0 failed |
| Worker placement | 3 on Worker 1, 3 on Worker 2 |
| client duration | 14.2-17.8 s |
| mean client duration | 15.5 s |
| fine native events observed by clients | 2,728 |
| durable Kafka records | 44 |
| fine-event to durable-record ratio | 62:1 |

A subsequent Turn reused one of those Sessions and correctly referred to the
previous answer. A third Turn in that same Session reused its Workspace and
Cube, created `algorithms.py`, implemented stable merge sort and binary search,
and actually ran `python3 algorithms.py`; all 20 assertions passed. The final
coding Turn completed in 101.8 seconds after 4,642 observable native events.

The initial six-way *coding* load also exposed the boundary of this particular
single-node Cube installation: four tasks completed, while two model-driven
Tool sequences repeatedly encountered Cube HTTP 502 and were cancelled after
the acceptance timeout. Inspection showed one physical Cube shim despite
multiple logical activation rows. This is not reported as successful six-way
Tool concurrency. It establishes that Worker/model concurrency is healthy,
while this laptop's current Cube compute plane must be expanded or repaired
before advertising concurrent KVM coding capacity.

## Tiered Session persistence

The final one-Turn acceptance produced 254 browser-visible native events. The
40 ms upstream grouping window persisted them as 18 Kafka records and 18
Valkey stream records. At the settled boundary PostgreSQL contained:

| Measure | Result |
| --- | ---: |
| Session schema version | 5 |
| immutable native Turn segments | 1 |
| active live-log location rows | 0 |
| semantic marker rows | 3 |
| compressed segment bytes | 23,571 |
| Kafka grouped records | 18 |
| Valkey grouped records | 18 |
| legacy delta/outbox tables | 0 |

PostgreSQL therefore did not retain 254 fine-grained payload rows. While a Turn
is active it stores only opaque Provider locations, sequence ranges and
digests. At settlement it stores one compressed native DSH segment and removes
the active locations. The active-location table has no payload, event or
records column.

Gateway forwards a Worker event only after Kafka has durably acknowledged its
grouped native envelope, Valkey has accepted the sequence-checked projection,
and PostgreSQL's `projected_through` watermark covers the event. The live-log
and projection are separate Cordis services, so Kafka and Valkey are Provider
plugins rather than dependencies embedded in the DSH SessionPersistence
implementation.

Valkey is rebuildable: if its projection is lost during an unfinished Turn, a
Worker reads the opaque locations recorded in PostgreSQL, loads the exact
envelopes from the configured SessionLiveLog Provider and reconstructs the
projection. At a completed Turn boundary, model-context recovery reads the
compressed PostgreSQL native segment; it does not reconstruct a synthetic
`messages[]` array.

## Throughput measurements

The concurrent six-user run exercised the application path, including real
model streams and the browser durability gate. The 40 ms group-commit window
reduced 2,728 client-visible native events to 44 `acks=all` Kafka records. All
six streams remained incremental and completed; grouping did not turn them
into end-of-Turn responses.

Raw component headroom was measured separately so model and Cube latency did
not distort the storage result. The benchmark used explicit temporary targets,
which were deleted after the run and never shared tables or topics with product
data.

| Component benchmark | Result |
| --- | --- |
| Kafka | 100,000 x 1 KiB records; `acks=all`, idempotent producer, gzip, 12 partitions |
| Kafka throughput | 30,543.7 records/s; 29.83 MiB/s |
| Kafka producer latency | 6.76 ms average; 11 ms p95; 13 ms p99; 0 errors |
| PostgreSQL | pgbench scale 10; 16 clients; 8 threads; 30 s read/write TPC-B-like workload |
| PostgreSQL throughput | 1,163.6 transactions/s; 34,849 transactions; 0 failures |
| PostgreSQL latency | 13.74 ms average |

These numbers measure one local Kafka broker and one local PostgreSQL instance,
not end-to-end Agent capacity. End-to-end coding concurrency is currently
bounded first by the single-node Cube compute plane and then by model latency;
the messaging and relational stores had substantial headroom in this test.

## Accepted limits

- The one-host Compose profile has one Kafka broker and is not highly
  available. The Helm profile assumes an externally operated replicated Kafka
  cluster and defaults new topics to replication factor three.
- Kafka retention defines the maximum recoverable age of an unfinished Turn;
  the default is 30 days. Completed Turn recovery no longer depends on Kafka
  or Valkey.
- Cube control/compute capacity, PostgreSQL availability and persistent Volume
  durability remain deployment responsibilities.
- The measured local Cube installation did not sustain six concurrent coding
  sandboxes. The report therefore makes no multi-KVM capacity claim even though
  six concurrent non-Tool Agent Runs completed successfully.
- Arbitrary shell execution is not advertised as exactly once. Once prompt
  dispatch may have started, a Run is not blindly replayed after an ambiguous
  transport failure.
- Valkey is currently a rebuildable projection and visibility gate. Browser
  frames travel through Gateway's shared fleet outlet over private Worker
  downlinks; Valkey is not yet the browser replay transport.
