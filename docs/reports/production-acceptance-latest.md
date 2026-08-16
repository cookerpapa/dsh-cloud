# Production Acceptance — 2026-08-16

## Scope

This acceptance exercised DSH Cloud as an external user on one self-hosted
Linux machine. It used public registration/login, the released DSH Web UI and
Agent Loop, real DeepSeek model calls, PostgreSQL-native Session persistence,
Kafka, Valkey, two independently registered Worker processes, Sandbox Manager,
and an existing CubeSandbox KVM control/compute installation.

It validates the current service boundaries, browser durability and
cross-Worker recovery. It does not claim host-level high availability,
hostile-public-SaaS isolation, or multi-region storage durability.

```text
Browser -> Gateway -> PostgreSQL Run queue -> DSH Worker -> DeepSeek
                 |                             |
                 |                             +-> Sandbox Manager -> Cube KVM
                 |
                 +-> projection watermark

DSH Session append -> PostgreSQL hot tail + Outbox
                                  -> Kafka acks=all
                                  -> Valkey ordered projection
                                  -> browser-visible frame

settled Turn -> one immutable PostgreSQL native gzip segment
```

There is no Temporal, MinIO, S3, or secondary SessionStorage authority in this
path.

## Automated verification

- all TypeScript project references build with unused local/parameter checks;
- all unit and PostgreSQL/service integration tests pass, including a real
  Kafka/Valkey publication test;
- the tests cover native Session reconstruction, group commit, segment digest
  verification, Outbox replay, stale fences, ambiguous prompt dispatch,
  cross-tenant access, Run claims, cancellation, concurrent activation,
  retryable Workspace deletion, WebSocket text framing and failure injection;
- Helm lint/render, production Compose rendering, shell syntax and whitespace
  checks pass.

## Real-user model and Cube test

The retained coding Session is
`c223ce82-43bf-4dc1-b21c-f5ba1da0b404`. Nine accepted Runs completed across
two Worker processes with monotonically increasing Workspace fences and no
failed Run.

The user-visible coding sequence was:

| User action | Worker | Result | Client time |
| --- | --- | --- | ---: |
| implement and test insertion sort | worker-2 | file and assertions completed in Cube | 9.9 s |
| read prior work, add binary search, run both suites | worker-2 | prior file present; both suites passed | 11.2 s |
| stop worker-2, read both files, add stable merge sort, run all suites | worker-1 | Session recovered; three suites passed | 12.7 s |
| exact short response after Gateway replacement | worker-1 | 18 durable events delivered; exact response | 1.4 s |
| final response after rebuilding every production image | worker-1 | current Gateway/Relay/two-Worker deployment passed | 1.7 s |

The cross-Worker coding Turn delivered 1,194 ordered Session events through the
browser path. Worker-1 restored the official DSH Session from PostgreSQL,
rebound the existing warm Cube activation, read files created by Worker-2,
wrote `merge_sort.py`, and ran all three Python programs successfully. The
Workspace retained one Cube activation throughout that handoff.

A second independently registered user ran a no-Tool conversation and received
the exact requested response in 2.3 seconds. Its native log contains no Tool
event. DSH nevertheless activated the Workspace Cube while constructing
`request/context`, because project-context discovery uses the filesystem seam.
This is an explicit DSH runtime behavior, not evidence of a model Tool call;
the current implementation does not claim sandbox-free pure chat.

## Streaming persistence

The retained nine-Turn coding Session contains 3,349 logical native DSH
events. At the final boundary:

| Measure | Result |
| --- | ---: |
| immutable PostgreSQL Turn segments | 9 |
| remaining fine-grained hot rows | 0 |
| pending PostgreSQL Outbox rows | 0 |
| compressed segment bytes | 74,007 |
| Valkey live batch records | 392 |
| Session / Gateway projection watermark | 3,348 / 3,348 |

Upstream fixed-window group commit therefore carried the 3,349 logical events
in 392 live batches, while settled relational storage contains one compressed
row per Turn rather than one permanent row per token chunk. Recovery expands
the native DSH StorageRecords and validates each segment's range and SHA-256
digest.

Gateway forwards a Session event only after the matching PostgreSQL append has
committed, Kafka has acknowledged the envelope, Valkey has accepted its
sequence-checked projection, and PostgreSQL `projected_through` covers the
event. During acceptance a protocol defect was found and fixed: forwarding an
upstream text `Buffer` without an explicit WebSocket text flag made the browser
see a binary frame. Gateway now preserves text framing and opens the private
Worker stream before completing the browser upgrade, eliminating the initial
subscription race.

Kafka and Valkey have a bounded live-delivery horizon. Long-term model-context
recovery uses PostgreSQL's compressed native Turn segments, not Kafka, Valkey,
S3, or a reconstructed `messages[]` array.

## Accepted limits

- The one-host Compose profile has one Kafka broker and is not highly
  available. The Helm profile assumes an externally operated replicated Kafka
  cluster and defaults new topics to replication factor three.
- Cube control/compute capacity, PostgreSQL availability and the persistent
  Volume driver remain deployment responsibilities.
- Arbitrary shell execution is not advertised as exactly once. Once prompt
  dispatch may have started, the Run is not blindly replayed after an ambiguous
  transport failure.
- Valkey is a rebuildable live projection; PostgreSQL and Kafka establish the
  durable boundaries.
- KEDA changes Worker replica count from PostgreSQL backlog only; it is not a
  scheduler and is not required for queued-Run correctness.
