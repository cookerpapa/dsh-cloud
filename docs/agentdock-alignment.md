# AgentDock alignment review

This document records the architectural review against AgentDock `main` at
`2e6a778`. Alignment means preserving the same cloud invariants where DSH has
an equivalent seam; it does not mean copying components whose purpose is
already fulfilled by the upstream DSH runtime.

## Shared invariants

| Concern | AgentDock | DSH Cloud |
| --- | --- | --- |
| Product and Run authority | PostgreSQL | PostgreSQL |
| Workflow scheduler | none; transactional claims | none; transactional claims |
| Agent Loop | trusted Pi Worker | trusted official DSH Worker |
| Untrusted effects | fenced Tool RPC into Cube KVM | fenced native fs/subprocess providers into Cube KVM |
| Workspace bytes | persistent Cube Volume | persistent Cube Volume |
| Active model context | Pi SessionStorage | native DSH SessionPersistence |
| Stale Writer defense | Workspace fence checked at commits and Tools | Workspace fence checked at Session appends, terminal commits and Tools |
| Ambiguous shell outcome | never blindly replayed | native DSH recovery retains the incomplete Tool outcome; the Run queue does not replay a persisted prompt |
| Pure conversation | no Sandbox activation | no Sandbox activation |

Both projects now use one product scheduler, keep credentials outside Cube,
serialize writers per Workspace, treat process memory as disposable, and let a
replacement Worker recover durable conversation and files without owning a
permanent per-Session process.

## Deliberate DSH-specific choices

### Native Session log instead of Kafka/Valkey

AgentDock separates its high-frequency Worker event log from canonical Pi
messages, so Kafka retention and a Valkey SSE projection have distinct jobs.
DSH already defines a lossless, ordered Session event log that is both the
official Harness recovery source and the browser protocol. DSH Cloud stores
that log in PostgreSQL, uses upstream group commit, and packs adjacent token
deltas into ranged rows.

Adding Kafka and Valkey here would split one upstream authority into two logs
and require a new reconciliation protocol before it produced a measured need.
The current design keeps one ordered source and still reduces physical rows by
more than eighty percent in the real-user acceptance run.

### Worker affinity as a transport constraint

AgentDock's shared Event Gateway needs no Worker affinity. The released DSH
Host event multiplexer is process-local, so Gateway prefers one healthy Worker
for a user's WebSocket and active Session. PostgreSQL remains authoritative;
the preference is ignored when that Worker is draining or unhealthy, and the
next Run can recover on another Worker. It is not a correctness shard or an
in-memory Session store.

### Upstream interruption repair

AgentDock adds Pi-specific interruption and world-state facts around Pi's
Session model. DSH already persists Turn, Step, Tool and interrupted-session
events and repairs unclosed native turns when the official Harness resumes.
DSH Cloud therefore protects those appends with cloud authority rather than
injecting a second Pi-shaped recovery vocabulary.

## Review changes made from this comparison

- made Workspace deletion a two-stage, retryable lifecycle operation that
  destroys Cube state and its persistent Volume before removing product state;
- prevented stale Attempts from committing terminal Runs and settled dirty
  Workspace revisions during post-prompt lease recovery;
- made concurrent first Tool calls share one activation instead of returning a
  transient activation error;
- moved Sandbox Manager tables into an explicit versioned schema;
- changed the public Host proxy from a mutation blacklist to a default-deny
  allowlist plus tenant-checked native Session operations;
- validated every execution operation and authority field at the trusted/KVM
  protocol boundary;
- removed unused caches, exports and root dependencies, and enabled unused-code
  checks in TypeScript;
- kept Workspace lifecycle APIs available when no Agent Worker is schedulable;
- made KEDA polling and cooldown settings effective for every autoscaling
  profile.

The remaining differences are intentional runtime properties, not unfinished
AgentDock migrations.
