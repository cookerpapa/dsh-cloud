# Production Acceptance — 2026-08-16

## Scope

This report validates the current DSH Cloud architecture on one self-hosted
Linux machine. It exercises real DeepSeek model calls, the released DeepSeek
Harness Web/Agent Loop, PostgreSQL state, independently replicated Gateway and
Worker processes, and CubeSandbox KVM execution. The Kubernetes run uses two
K3d nodes on one physical host, so it validates scheduling and process-level
recovery; it does not claim host-level, control-plane, or storage high
availability.

The accepted scheduling path contains no Temporal service:

```text
Browser -> Gateway -> PostgreSQL Run queue -> DSH Worker
                                      |          |
                                      |          +-> DeepSeek model
                                      |          +-> Sandbox Manager -> Cube KVM
                                      +-> native DSH Session event log
```

PostgreSQL is the sole authority for admission, Run/Attempt ownership, leases,
Workspace fences and terminal state. KEDA observes only the queued-Run count to
scale Worker replicas; it does not claim or execute work.

## Automated verification

- TypeScript project references build cleanly.
- The default unit gate passes without requiring external infrastructure.
- All 32 PostgreSQL and service integration tests pass, including Session
  persistence, Run claims, Workspace-global fences, stale-Worker rejection,
  browser durability, Cube Volume authentication, cancellation and failure
  injection.
- Helm lint, production Compose rendering and deployment-script syntax checks
  pass in the same form used by CI.

## Kubernetes deployment

The acceptance release ran a K3d server node and agent node on the same physical
host and reached Ready state with:

- 2 Gateway replicas;
- 2 DSH Worker replicas;
- 3 Sandbox Manager replicas;
- KEDA ScaledObject ready with a 2–100 Worker range;
- PodDisruptionBudgets, non-root/read-only container policies and default-deny
  NetworkPolicies rendered by the chart.

## Real model and Cube KVM path

One authenticated user submitted three consecutive coding prompts against one
Workspace. The Harness created and tested an insertion-sort implementation,
read the existing result in the next turn, and produced a final resume probe
after the original Worker Pod was deleted.

Observed results:

- all three Runs reached `completed` through real DeepSeek calls;
- 14 Tool calls produced 14 durable Tool results;
- the first two Runs executed on one Worker and the third on a replacement
  Worker;
- native DSH Session context was recovered from PostgreSQL on the replacement;
- one Cube activation and one persistent Cube Volume were retained across the
  handoff, so the replacement Worker saw the prior Workspace files;
- Workspace writer fences advanced monotonically across Runs and Sessions.

This proves cross-Worker Session and Workspace recovery. It does not claim that
process memory survives KVM destruction: only a warm activation preserves live
processes, while the Cube Volume is the durable file boundary.

## Streaming durability and write amplification

The three real coding Runs produced 5,562 logical DSH Session events. Lossless
chunk packing stored them in 963 PostgreSQL rows, an 82.7% row reduction. Reads
expanded the ranged rows back into the original ordered DSH event stream. The
Gateway forwarded an event only after the persistence watermark covered its
logical sequence, preserving the visible-implies-durable contract.

## Cancellation and recovery

In Kubernetes, a real foreground Python command sleeping for 60 seconds was
cancelled after its Tool call started. The Run reached `cancelled` in about
614 ms and the remote process was terminated through the fenced Tool path.

The separate one-host Docker acceptance also exercised cold Cube replacement:
Workspace files remained in the persistent Cube Volume after the source KVM was
destroyed, and a replacement activation recovered them. Foreground cancellation
completed in about 575 ms in that profile.

## Accepted limits

- This is a self-hosted enterprise baseline, not evidence of hostile public
  SaaS isolation or multi-region disaster recovery.
- Cube control/compute capacity and PostgreSQL availability remain external
  deployment responsibilities.
- Arbitrary shell effects are not advertised as exactly-once. If execution
  occurred but its result cannot be confirmed, the outcome remains unknown
  rather than being replayed blindly.
- KEDA availability affects scaling responsiveness only; queued Runs remain
  durable and claimable through PostgreSQL.
