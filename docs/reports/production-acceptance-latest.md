# Production Acceptance — 2026-08-16

## Scope

This acceptance exercised DSH Cloud as an external user on one self-hosted
Linux machine. It used the public registration/login page, the released DSH Web
UI and Agent Loop, real DeepSeek model calls, PostgreSQL native Session
persistence, two independently registered Worker processes, Sandbox Manager,
and an existing CubeSandbox KVM control/compute installation.

It validates the current service boundaries and cross-Worker recovery. It does
not claim host-level high availability, hostile-public-SaaS isolation, or
multi-region storage durability.

```text
Browser -> Gateway -> PostgreSQL Run queue -> DSH Worker -> DeepSeek
                                      |             |
                                      |             +-> Sandbox Manager -> Cube KVM
                                      +-> native DSH Session event log
```

There is no Temporal service or secondary Run dispatcher in this path.

## Automated verification

- all TypeScript project references build with unused local/parameter checks;
- all 43 unit and PostgreSQL/service integration tests pass;
- the tests cover Session persistence, cross-tenant access, Run claims,
  Workspace-global fencing, stale terminal commits, expired post-prompt
  recovery, concurrent activation, retryable Workspace deletion, browser
  durability, protocol validation, cancellation and failure injection;
- dependency audit reports no known production vulnerability;
- Helm lint/render, production Compose rendering, shell syntax and whitespace
  checks pass.

## Real-user model and Cube test

The retained acceptance Session is
`b646df56-aafc-4284-9f36-0a929ba3ae44` in the local production deployment.
The user-visible path produced these results:

| User action | Worker | Fence | Result | Run time |
| --- | --- | ---: | --- | ---: |
| simple conversation | worker-1 | 1 | completed without creating Cube | 1.397 s |
| insertion-sort implementation and tests | worker-1 | 3 | file written in Cube; five assertions passed | 6.240 s |
| read prior work, add binary search, run both suites | worker-2 | 4 | prior file recovered; five plus four assertions passed | 12.264 s |
| final post-upgrade conversation | worker-2 | 5 | exact requested reply; no Tool call | 2.759 s |
| final rebuilt-image conversation | worker-2 | 6 | exact requested reply; no Tool call | 1.496 s |

Before the successful coding rerun, the first production Tool attempt exposed
two integration defects rather than being discarded as a test artifact:

1. Gateway's default-deny proxy also blocked a tenant's own
   `session.history` operation.
2. The shared Cube installation's hardened Volume authorizer and DSH's Volume
   identity disagreed, and an absent Volume deletion was not idempotent.

The proxy now forwards every known native Session operation only after tenant
ownership checks. Cube control requests use its documented Bearer credential,
the DSH Volume driver owns deterministic identities, and deletion probes before
issuing a physical delete. The same user task was then repeated through the UI
and passed.

Worker-1 was drained and stopped before the final prompt. Worker-2 loaded the
official DSH Session from PostgreSQL, claimed a new Workspace fence, rebound
the same warm Cube activation, read `insertion_sort.py`, created
`binary_search.py`, and executed both test programs successfully. The activation
and persistent Volume identities remained unchanged while the fence advanced
from 3 to 4.

This demonstrates that correctness does not depend on one permanent DSH
process. It does not claim that process memory survives Cube destruction: a
warm Cube retains processes, while only Workspace files survive a replacement
KVM through the persistent Volume.

The final request ran after rebuilding and replacing both Worker containers
against their existing persistent DSH homes. This also verified that the
launcher repairs platform-owned plugin links whose image-local target changed
across an upgrade, without replacing unrelated user files.

## Streaming persistence

The retained six-Turn Session contains 3,274 logical native DSH events in 599
PostgreSQL rows. Lossless adjacent-delta packing therefore reduced physical rows
by 81.7 percent while reads still expand the exact ordered event sequence.
Gateway exposes a Session event only after the PostgreSQL durability watermark
covers it.

This design deliberately does not add Kafka/Valkey to DSH Cloud. The native DSH
event log is also the Harness recovery and model-context authority, so keeping
one lossless ordered store avoids a dual-log reconciliation problem.

## Tenant and lifecycle checks

- a separately registered tenant received `session-not-found` when requesting
  the acceptance Session history;
- an empty Workspace was created and deleted through the public cloud API;
- Workspace lifecycle requests remain available even when every Agent Worker
  is draining;
- persistent deletion is retryable: an ambiguous provider failure leaves the
  Workspace hidden in `deleting` instead of reactivating a possibly erased
  directory.

## Accepted limits

- Cube control/compute capacity, PostgreSQL availability and the persistent
  Volume driver remain deployment responsibilities.
- Arbitrary shell execution is not advertised as exactly once. Once the native
  prompt is durable, the Run is not blindly replayed after an ambiguous Tool
  outcome.
- KEDA changes Worker replica count from PostgreSQL backlog only; it is not a
  scheduler and is not required for queued-Run correctness.
- The local acceptance reused AgentDock's installed Cube Volume plugin solely
  to exercise the KVM path. A standalone DSH Cloud deployment uses the included
  `dsh-cloud-posix` driver and its own authorization policy.
