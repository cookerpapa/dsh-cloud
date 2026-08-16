# Architecture

## Why a separate repository

Pi and DSH expose different runtime and persistence models. Pi Cloud restores Pi-native sessions around short-lived Pi workers; DSH is already an event-sourced, Cordis-composed harness with replaceable Session, filesystem, and subprocess services. Sharing product concepts is useful, but sharing one runtime adapter would erase important semantics and increase upgrade risk.

DSH Cloud therefore depends on released DSH packages and adds a Cloud profile. Upstream Web UI and Agent Loop updates remain ordinary dependency upgrades.

## State ownership

| State | Authority | Cache allowed |
| --- | --- | --- |
| DSH Session metadata and settled Turn segments | PostgreSQL through the tiered SessionPersistence plugin | yes |
| Exact unfinished native Session suffix | SessionLiveLog provider (Kafka by default) | no |
| Run/Attempt scheduling | PostgreSQL transactional Run queue | no |
| Workspace files | persistent Cube Volume | attached to one active Cube |
| Live processes | one Cube activation; disposable | no durable claim |
| Model credentials | trusted Worker credential provider | no sandbox access |
| UI live projection | SessionLiveProjection provider (Valkey by default), rebuildable from Kafka locators | yes |

DSH Session events, not browser deltas or a reconstructed `messages[]`, are the conversation authority. A Worker may resume a Session on another machine by loading those events through the official persistence seam.

This document, the root README and `docs/pi-cloud-alignment.md` describe the
current topology. `docs/README.md` defines the documentation hierarchy.
Historical reports record what a particular build measured; they do not
reintroduce components or routing rules removed from mainline.

## Run scheduling

PostgreSQL is the only Run authority. Admission inserts an idempotent queued
Run. Worker replicas claim rows with `FOR UPDATE SKIP LOCKED`, enforce one
active writer per Workspace, and heartbeat a `RunAttempt` with a monotonically
increasing fence. `LISTEN/NOTIFY` wakes idle Workers but is never required for
correctness; bounded polling covers lost notifications and reconnects.

The same `DSH_CLOUD_RUN_LEASE_SECONDS` value controls stale-Attempt
reconciliation in Workers and Tool admission in Tool Broker. A deployment
must not let those values diverge: an execution request is valid only while
the Run, Attempt, fence and heartbeat are all current in PostgreSQL.

There is deliberately no Temporal layer. DSH already owns the Agent Loop and
PostgreSQL owns product state, so adding another durable workflow history would
duplicate retry and ownership semantics without removing the need for fenced
Session and Workspace commits.

There is no durable user, Session, or Workspace placement on a Worker. Every
healthy Worker competes for every eligible queued Run; tenant fairness,
same-Session ordering and the single Workspace writer rule are enforced by the
shared PostgreSQL claim transaction. `runs.worker_id` and
`run_attempts.worker_id` identify only the current Attempt owner and are
cleared or superseded when ownership ends.

DSH's upstream API resolver normally reuses a live process-local Agent, which
is useful for a single-host Web UI but cannot be the continuity mechanism of an
arbitrary-Worker pool. The Cloud profile therefore captures DSH's public
`AgentHandle`. After an ordinary user Agent reaches idle, its Session is
explicitly flushed and the handle is disposed; a short grace period protects
session creation and immediate same-process wakeups. Subagent handles remain
under DSH's own continuation manager. The next user Run may land on any Worker
and resumes from the shared native Session log. This bounds Worker memory and
prevents an old Worker from later reusing a stale in-memory context.

## Per-Session Harness profiles

DSH already models a specialized Harness as an Agent preset: one trusted
Cordis composition supplies the prompt, tools, compaction and delegation
surface for every Session that joins it. DSH Cloud keeps `standard` as the
genesis/default profile and exposes only deployment-approved presets when a
new Session is blank. The selected preset is recorded in the native Session
header/event log, so an arbitrary Worker cold-resumes the same composition
without durable Worker affinity.

Cloud profile selection is deliberately not arbitrary plugin upload. A preset
executes inside the trusted Worker; the upstream `minimal` preset installs a
host-local filesystem, while `cordis` can edit the live Harness. Exposing
either would bypass the Cube boundary. Worker-local authoring would also make
the same Session resolve differently across replicas. Gateway therefore
exposes neither preset authoring nor default mutation, defaults its selection
allowlist to `standard,code`, filters out user-trust rows, and requires
specialized profiles to be reviewed and shipped identically in the immutable
Worker image. A future self-evolution
workflow should produce a candidate composition, validate it in an isolated
build, and promote a signed image/profile version; it must not load
model-written code directly into a multi-tenant trusted Worker.

A newly created control-plane Session is not claimable until its DSH-native
Session row has materialized. This closes the race in which `session.create`
returns before the asynchronous persistence batch becomes visible to a
different Worker.

## Multi-agent boundary

DSH natively supplies in-process spawn/fork providers, durable continuable
children and a worker-thread Workflow orchestrator. DSH Cloud does not treat
all three execution modes as equivalent. The production profile currently
admits only a foreground Workflow whose one-shot children finish before the
root Run commits:

```text
root RunAttempt / Lease / Workspace fence
  -> foreground Workflow
       -> child Session A (read-only Workspace view)
       -> child Session B (read-only Workspace view)
  -> parent receives results
  -> root Run settles
```

Every child keeps its own native DSH Session log. SessionPersistence accepts a
child write only when its immutable `parentSession` chain reaches the active
root Session, and records the same root Attempt/fence on the child row. This
allows arbitrary-Worker recovery and audit of delegated work without allowing
an authority for one user Session to write an unrelated Session.

Parallel children share the root Cube Volume today, so the Cloud policy makes
them read-only (`read`/search/web remain available; `bash`/write/edit and
recursive delegation are hidden). DSH's continuable `subagent`,
`subagent_fork`, `send_message` and `list_agents` tools are also hidden from
root Agents. A child that survives the parent Turn cannot safely inherit the
parent's expiring Lease: correct background continuation requires a distinct
queued child Run, Attempt, heartbeat and fence. Parallel coding will likewise
require one candidate Workspace/Volume per child followed by explicit diff
review and merge. Neither boundary is emulated with process-local handles or a
weakened Tool Broker check.

The released DSH Web event multiplexer is process-local, so each Gateway keeps
one private upstream downlink to every healthy Worker that currently has a
browser subscriber. Gateway merges those downlinks into one tenant-filtered
browser outlet. A Worker disconnect removes only that source; it does not close
the browser socket, and a later Run may execute on any other Worker. Session
events are deduplicated by their native sequence and still wait for the shared
Kafka/Valkey/PostgreSQL durability watermark before browser visibility.
Process-local `host/session-added` and `host/session-removed` frames are not
forwarded: Agent residency changes are not cloud conversation creation or
deletion. PostgreSQL ownership and explicit product APIs own that lifecycle.

An operation that must affect an already-running in-memory Agent, such as a
steer/update-queue request, is routed to the current RunAttempt owner. This is a
lookup of live ownership, not a remembered route for the next Run.

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
  -> Tool Broker
  -> CubeSandbox KVM
```

DSH Cloud owns a separate CubeAPI callback policy and API key. The callback
admits the Broker's bounded sandbox inventory/lifecycle operations and
`dsh-<48 hex>` Workspace Volumes, while rejecting Pi Cloud's `adw-*` Volume
namespace. A dedicated Cube control/compute installation is the recommended
security boundary; trusted infrastructure may share lower-level capacity only
through a distinct DSH CubeAPI frontend. DSH Cloud never imports or extends Pi
Cloud's authorizer.

The model cannot select a sandbox id, namespace, runtime class, mount, or network policy. Those values are derived from trusted Run authority.

The execution agent listens only behind Cube private ingress. Its initial bind
is authorized by Cube's traffic token, which only Tool Broker holds. The
broker then installs a random per-activation secret and monotonically
increasing writer fence. Later calls require all three identities. The agent
contains no model key, database URL, object-store credential, Cube API key, or
Tool Broker token.

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

Workspace deletion is a retryable lifecycle operation. Gateway first changes
an empty Workspace from `active` to `deleting`, making it unavailable to new
Sessions. Tool Broker then retires any activation, deletes the persistent
Volume, and finally removes the PostgreSQL Workspace row. An ambiguous remote
failure leaves the row in `deleting` for retry; it is never reactivated after
the platform may already have deleted its bytes.

## Browser durability

The official DSH Workers emit fine-grained Session events into Gateway's shared
fleet outlet. Before Gateway forwards a `session/event` frame, it verifies that the durable live projection
has advanced through that event sequence. The upstream persistence coordinator
first coalesces adjacent events into a bounded batch. The production
`SessionPersistence` plugin sends the exact native batch to the injected
`SessionLiveLog` Provider, waits for its durable acknowledgement, and applies
the same envelope to the injected `SessionLiveProjection` Provider. The
defaults are Kafka `acks=all` and a sequence-checked Valkey Stream.

Only the batch sequence range, opaque durable Provider location and canonical SHA-256
digest enter PostgreSQL; fine token payloads do not. The same PostgreSQL
transaction records small semantic markers and advances `projected_through`.
Gateway waits on that watermark before forwarding the Worker frame, so browser
visibility never gets ahead of Kafka durability and the rebuildable projection.

When `turn/end` is appended, the same Session transaction reads the indexed
Kafka suffix, compresses all events since the previous terminal boundary into
one immutable PostgreSQL segment, and deletes the corresponding location rows.
The segment uses DSH's native
StorageRecord codec plus gzip and expands to the exact logical event sequence
on recovery. PostgreSQL therefore keeps roughly one large row per Turn rather
than one permanent row per token chunk. An interrupted Turn is recovered from
the sealed PostgreSQL prefix plus exact live-log locations; Valkey can be rebuilt.
There is no S3/MinIO dependency and no separately reconstructed `messages[]`.

After a successful native DSH Compaction, the transaction also replaces one
verified runtime baseline through the Compaction boundary. The baseline is
created by DSH's Session domain rather than by cloud storage: it contains the
exact current model surface and retained plugin/runtime state, but omits raw
assistant chunks and surface payloads shadowed by Compaction. A cold Worker
loads this compact baseline plus only the later physical suffix; omitted
canonical positions become tiny, ignorable Worker-local gaps so absolute event
sequence numbers and future append cursors remain unchanged.

Canonical PostgreSQL segments remain retained for human history, export, audit
and fail-soft recovery. Gap records never enter canonical storage. If baseline
integrity or Session-contract validation fails, the Worker reloads the full
canonical log. DSH Cloud currently carries this generic upstream Session and
PersistenceCoordinator contract as reviewed pnpm patches over `0.1.0-rc.6`
until it is available in a published DSH release.

The live log and projection are Cordis Service Definitions. The tiered
SessionPersistence imports neither Kafka nor Valkey clients; deployments can
replace either Provider while retaining the official DSH storage contract.
PostgreSQL is replaceable at the wider, upstream-defined boundary by installing
a different `SessionPersistence` plugin in the Cordis profile.
See [the persistence decision](decisions/tiered-session-persistence.md).

## Implemented milestones

1. PostgreSQL-native DSH Session persistence and Cloud Web profile.
2. Remote Cube filesystem/subprocess providers; production profile has no local execution fallback.
3. Multi-tenant authentication, PostgreSQL durable Run queue, and horizontally scalable DSH Worker pool.
4. Persistent Workspace recovery, durable-before-visible live delivery,
   canonical Session reload after browser reconnect, cancellation, and failure
   injection.
5. Kubernetes deployment, autoscaling, observability, and reproducible production acceptance.

The implementation deliberately ends with PostgreSQL scheduling rather than a
Temporal milestone. PostgreSQL already owns admission, leases, fencing and
terminal commits; KEDA only changes Worker replica count from the queued-Run
backlog. See the [latest acceptance report](reports/production-acceptance-latest.md)
for the measured boundary and the [Pi Cloud alignment review](pi-cloud-alignment.md)
for the deliberate runtime-specific differences.
