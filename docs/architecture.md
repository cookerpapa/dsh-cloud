# Architecture

## Why a separate repository

Pi and DSH expose different runtime and persistence models. AgentDock restores Pi-native sessions around short-lived Pi workers; DSH is already an event-sourced, Cordis-composed harness with replaceable Session, filesystem, and subprocess services. Sharing product concepts is useful, but sharing one runtime adapter would erase important semantics and increase upgrade risk.

DSH Cloud therefore depends on released DSH packages and adds a Cloud profile. Upstream Web UI and Agent Loop updates remain ordinary dependency upgrades.

## State ownership

| State | Authority | Cache allowed |
| --- | --- | --- |
| DSH Session events and metadata | PostgreSQL SessionPersistence | yes |
| Run/Attempt scheduling | PostgreSQL transactional Run queue | no |
| Workspace files | persistent Cube Volume | attached to one active Cube |
| Live processes | one Cube activation; disposable | no durable claim |
| Model credentials | trusted Worker credential provider | no sandbox access |
| UI projections | derived from durable Session events | yes |

DSH Session events, not browser deltas or a reconstructed `messages[]`, are the conversation authority. A Worker may resume a Session on another machine by loading those events through the official persistence seam.

## Run scheduling

PostgreSQL is the only Run authority. Admission inserts an idempotent queued
Run. Worker replicas claim rows with `FOR UPDATE SKIP LOCKED`, enforce one
active writer per Workspace, and heartbeat a `RunAttempt` with a monotonically
increasing fence. `LISTEN/NOTIFY` wakes idle Workers but is never required for
correctness; bounded polling covers lost notifications and reconnects.

The same `DSH_CLOUD_RUN_LEASE_SECONDS` value controls stale-Attempt
reconciliation in Workers and Tool admission in Sandbox Manager. A deployment
must not let those values diverge: an execution request is valid only while
the Run, Attempt, fence and heartbeat are all current in PostgreSQL.

There is deliberately no Temporal layer. DSH already owns the Agent Loop and
PostgreSQL owns product state, so adding another durable workflow history would
duplicate retry and ownership semantics without removing the need for fenced
Session and Workspace commits.

The released DSH Web event multiplexer is process-local. While a Worker is
healthy, Gateway therefore keeps a user's browser channel and that Session's
Runs on the same Worker. This is transport affinity, not a state authority:
Session events and Run state remain in PostgreSQL, and a dead or draining
Worker is replaced before the next request. A future shared event gateway can
remove this routing constraint without changing SessionPersistence or the Run
queue.

DSH Host remains bound to the Worker's loopback interface. DSH deliberately
rejects a public bind because the Host API controls Agent execution. A
separate Worker Control Relay exposes the byte stream on the trusted service
network, and NetworkPolicy admits that port only from Gateway Pods.

## Writer fencing

Every cloud RunAttempt receives a monotonically increasing fence. The
PostgreSQL allocates the fence from the Workspace, not from an individual
Session. This matters because different Sessions can modify the same Workspace:
a Session-local sequence could otherwise issue a fence lower than the one
already installed in a warm Cube. In the same append transaction the Session
backend verifies that the matching Run, Attempt, Workspace fence and heartbeat
are all current. An append from an older, expired, or already terminal Worker
is rejected before rows are published. This protects durable Session state if
an old Worker resumes after a network partition or long pause.

Fencing does not make arbitrary shell execution exactly once. A lost Tool
result can still be `UNKNOWN`; DSH Cloud preserves that uncertainty instead of
blindly replaying a side-effecting command.

## Execution-world boundary

DSH already defines `ctx.fs` and `ctx.subprocess` as provider seams. Its E2B proof of concept demonstrates the intended composition: the Harness, model authentication, and Session stay trusted while file and process operations run remotely. DSH Cloud implements the same boundary against a self-hosted CubeSandbox control plane:

```text
DSH tool consumer
  -> ctx.fs / ctx.subprocess
  -> authenticated execution-world client
  -> Sandbox Manager
  -> CubeSandbox KVM
```

The model cannot select a sandbox id, namespace, runtime class, mount, or network policy. Those values are derived from trusted Run authority.

The execution agent listens only behind Cube private ingress. Its initial bind
is authorized by Cube's traffic token, which only Sandbox Manager holds. The
manager then installs a random per-activation secret and monotonically
increasing writer fence. Later calls require all three identities. The agent
contains no model key, database URL, object-store credential, Cube API key, or
Sandbox Manager token.

The remote filesystem and subprocess providers share the same activation and
`/workspace`; a file written through `ctx.fs` is immediately visible to a
command started through `ctx.subprocess`. Paths are canonicalized beneath the
workspace root, process groups are terminated as a unit, output is bounded,
and credential-shaped environment variables are removed at the untrusted
boundary.

The Worker image also contains an empty `/workspace` directory because the
upstream Host validates a Session cwd during creation. It is only a metadata
stub: no tool provider reads or writes it, and no tenant Workspace bytes are
mounted into a Worker. The same logical path is resolved inside the current
Run's Cube Volume by the remote providers.

Each tenant Workspace maps to one deterministic Cube Volume. A warm activation
can preserve processes across Runs for the configured idle TTL; after KVM loss
or reaping, a new activation mounts the same Volume and recovers files while
process and memory state are explicitly treated as disposable.

## Browser durability

The official DSH Worker emits fine-grained Session events. Before Gateway
forwards a `session/event` frame, it verifies that PostgreSQL's native Session
log has advanced through that event sequence. The upstream persistence
coordinator coalesces adjacent events into one transaction. The PostgreSQL
backend additionally uses DSH's lossless chunk codec to store consecutive text
deltas as one ranged row (`seq..seq_end`) and expands them exactly when the
official persistence contract reads the log. The browser and Harness therefore
keep the original logical sequence while PostgreSQL performs substantially
fewer row inserts. This barrier adds a small group-commit delay without turning
each token into a separate write. Reconnection reads canonical history from
PostgreSQL.

## Implemented milestones

1. PostgreSQL-native DSH Session persistence and Cloud Web profile.
2. Remote Cube filesystem/subprocess providers; production profile has no local execution fallback.
3. Multi-tenant authentication, PostgreSQL durable Run queue, and horizontally scalable DSH Worker pool.
4. Persistent Workspace recovery, resumable event delivery, cancellation, and failure injection.
5. Kubernetes deployment, autoscaling, observability, and reproducible production acceptance.

The implementation deliberately ends with PostgreSQL scheduling rather than a
Temporal milestone. PostgreSQL already owns admission, leases, fencing and
terminal commits; KEDA only changes Worker replica count from the queued-Run
backlog. See the [latest acceptance report](reports/production-acceptance-latest.md)
for the measured boundary.
