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
- 49 PostgreSQL/service integration cases pass against real PostgreSQL, Kafka
  and Valkey services;
- the tests cover native Session reconstruction, fixed-window group commit,
  segment digest verification, stale fences, ambiguous prompt dispatch,
  cross-tenant access, Run claims, cancellation, concurrent activation,
  retryable Workspace deletion, Tool Broker authorization, Valkey loss and
  rebuild, WebSocket text framing and failure injection;
- a fresh two-Worker boot validates concurrent schema initialization; the
  migration path serializes initialization with a PostgreSQL advisory lock;
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

## Accepted limits

- The one-host Compose profile has one Kafka broker and is not highly
  available. The Helm profile assumes an externally operated replicated Kafka
  cluster and defaults new topics to replication factor three.
- Kafka retention defines the maximum recoverable age of an unfinished Turn;
  the default is 30 days. Completed Turn recovery no longer depends on Kafka
  or Valkey.
- Cube control/compute capacity, PostgreSQL availability and persistent Volume
  durability remain deployment responsibilities.
- Arbitrary shell execution is not advertised as exactly once. Once prompt
  dispatch may have started, a Run is not blindly replayed after an ambiguous
  transport failure.
- Valkey is currently a rebuildable projection and visibility gate. Browser
  frames still travel over the existing authenticated Worker Control Channel;
  Valkey is not yet the browser replay transport.
