# Architecture

## Why a separate repository

Pi and DSH expose different runtime and persistence models. AgentDock restores Pi-native sessions around short-lived Pi workers; DSH is already an event-sourced, Cordis-composed harness with replaceable Session, filesystem, and subprocess services. Sharing product concepts is useful, but sharing one runtime adapter would erase important semantics and increase upgrade risk.

DSH Cloud therefore depends on released DSH packages and adds a Cloud profile. Upstream Web UI and Agent Loop updates remain ordinary dependency upgrades.

## State ownership

| State | Authority | Cache allowed |
| --- | --- | --- |
| DSH Session events and metadata | PostgreSQL SessionPersistence | yes |
| Run/Attempt scheduling | durable orchestrator (next milestone) | no |
| Workspace files | Cube-backed persistent volume/checkpoint (next milestone) | yes |
| Live processes | one Cube activation; disposable | no durable claim |
| Model credentials | trusted Worker credential provider | no sandbox access |
| UI projections | derived from durable Session events | yes |

DSH Session events, not browser deltas or a reconstructed `messages[]`, are the conversation authority. A Worker may resume a Session on another machine by loading those events through the official persistence seam.

## Writer fencing

Every cloud RunAttempt receives a monotonically increasing fence. The PostgreSQL backend stores the highest accepted fence with each Session. An append from an older fence is rejected before rows are published. This protects durable Session state if an old Worker resumes after a network partition or long pause.

Fencing does not make arbitrary shell execution exactly once. A lost tool result can still be `UNKNOWN`; the later execution milestone will preserve DSH's checkpoint-policy semantics instead of blindly replaying a side-effecting command.

## Execution-world boundary

DSH already defines `ctx.fs` and `ctx.subprocess` as provider seams. Its E2B proof of concept demonstrates the intended composition: the Harness, model authentication, and Session stay trusted while file and process operations run remotely. DSH Cloud will implement the same boundary against a self-hosted CubeSandbox gateway:

```text
DSH tool consumer
  -> ctx.fs / ctx.subprocess
  -> authenticated execution-world client
  -> Sandbox Manager
  -> CubeSandbox KVM
```

The model cannot select a sandbox id, namespace, runtime class, mount, or network policy. Those values are derived from trusted Run authority.

## Milestones

1. PostgreSQL-native DSH Session persistence and Cloud Web profile.
2. Remote Cube filesystem/subprocess providers; production profile has no local execution fallback.
3. Multi-tenant authentication, durable Run orchestration, and horizontally scalable DSH Worker pool.
4. Workspace checkpoint/recovery, resumable event delivery, cancellation, and failure injection.
5. Kubernetes deployment, autoscaling, observability, and reproducible production acceptance.

