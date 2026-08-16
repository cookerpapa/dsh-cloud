# Pi Cloud alignment review

This document records the architectural review against the current Pi Cloud
mainline. Alignment means preserving the same cloud invariants where DSH has
an equivalent seam; it does not mean copying components whose purpose is
already fulfilled by the upstream DSH runtime.

## Shared invariants

| Concern | Pi Cloud | DSH Cloud |
| --- | --- | --- |
| Product and Run authority | PostgreSQL | PostgreSQL |
| Workflow scheduler | none; transactional claims | none; transactional claims |
| Agent Loop | trusted Pi Worker | trusted official DSH Worker |
| Untrusted effects | fenced Tool RPC into Cube KVM | fenced native fs/subprocess providers into Cube KVM |
| Workspace bytes | persistent Cube Volume | persistent Cube Volume |
| Active model context | Pi SessionStorage | native DSH SessionPersistence |
| Live publication | Kafka, then Valkey/SSE projection | pluggable Kafka live log, then Valkey/WebSocket projection |
| Stale Writer defense | Workspace fence checked at commits and Tools | Workspace fence checked at Session appends, terminal commits and Tools |
| Ambiguous shell outcome | never blindly replayed | the Run queue never replays a prompt after its dispatch boundary |

Both projects use one product scheduler, keep credentials outside Cube,
serialize writers per Workspace, treat process memory as disposable, and let a
replacement Worker recover durable conversation and files without owning a
permanent per-Session process.

DSH Cloud enforces that last property through DSH's public lifecycle seam:
ordinary user Agents are flushed and their `AgentHandle` is disposed after
settlement. Pi and DSH keep different native storage formats, but neither
depends on retaining one process or routing the next Run back to it.

## Deliberate DSH-specific choices

### One native event vocabulary, two retention shapes

Pi has distinct canonical Session entries and high-frequency UI events. Its raw
token deltas can therefore age out of Kafka/Valkey after canonical messages are
committed to PostgreSQL.

DSH's native Session events are simultaneously its Harness recovery log and its
browser protocol. DSH Cloud consequently preserves the exact native sequence,
but changes its physical medium at a settled Turn boundary:

```text
unfinished Turn: exact live-log suffix + PostgreSQL opaque location/digest metadata
settled Turn:    one immutable native PostgreSQL gzip segment
live delivery:   Kafka durable ACK + Valkey ordered projection
```

The official `SessionPersistence` contract remains the logical authority: its
sealed PostgreSQL prefix and indexed live-log suffix form one contiguous native
log. Valkey is rebuildable. Both live media are Cordis Provider seams, so this
policy does not embed Kafka or Valkey clients in DSH's Session backend.

### Shared event outlet over process-local DSH multiplexers

Both systems avoid durable Worker placement. DSH still exposes a process-local
Host event multiplexer, so each Gateway aggregates a private downlink from
every healthy Worker and performs tenant filtering, native-sequence
deduplication and the durability barrier centrally. The browser subscribes to
the fleet outlet rather than one Worker. A command aimed at an active Agent may
use the current RunAttempt owner, but that ephemeral owner never influences
where the next Run is claimed.

### Workspace context can activate Cube before an explicit Tool

DSH builds `request/context` through its filesystem seam so project
instructions can affect the first model request. In the current remote provider
that read activates the Workspace Cube even when the eventual answer contains
no Tool call. Pi Cloud can keep a pure chat Turn sandbox-free because its
context path has a different shape. DSH Cloud records this as an upstream
runtime difference rather than adding an intent classifier that could omit
project instructions.

### Upstream interruption repair

Pi Cloud adds Pi-specific interruption and world-state facts around Pi's
Session model. DSH already persists Turn, Step, Tool and interrupted-session
events and repairs unclosed native turns when the official Harness resumes.
DSH Cloud therefore protects those appends with cloud authority rather than
injecting a second Pi-shaped recovery vocabulary.

## Review changes made from this comparison

- kept PostgreSQL as the only Run scheduler and removed Temporal from the
  target architecture;
- stored exact DSH Session events through the official persistence seam instead
  of inventing a parallel `messages[]` snapshot;
- added the same Kafka-durable, Valkey-projected visibility rule used by Pi
  Cloud without duplicating the canonical Session authority;
- made Workspace deletion a two-stage, retryable lifecycle operation that
  destroys Cube state and its persistent Volume before removing product state;
- prevented stale Attempts from committing terminal Runs and settled dirty
  Workspace revisions during post-prompt lease recovery;
- made concurrent first Tool calls share one activation;
- changed the public Host proxy to a default-deny allowlist plus tenant-checked
  native Session operations;
- kept Workspace lifecycle APIs available when no Agent Worker is schedulable.
- removed user/Session Worker preference columns and made the Gateway event
  channel fleet-wide, allowing consecutive Runs to execute on arbitrary
  healthy Workers.
- bounded ordinary Agent residency with the public DSH `AgentHandle`, and held
  first-Run claims until the native Session log is materialized.

The remaining differences follow from the two Harness contracts rather than
unfinished migration work.
