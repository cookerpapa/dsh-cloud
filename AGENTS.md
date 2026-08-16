# DSH Cloud development rules

DSH Cloud is a cloud distribution around the public DeepSeek Harness packages. It is not a Pi Cloud adapter and it does not copy or fork DSH internals without evidence that a public seam is insufficient.

## Architecture rules

- Prefer an upstream DSH service definition, plugin seam, profile, or bundle before introducing a parallel abstraction.
- Keep the DSH Agent Loop, model credentials, session state, and orchestration in the trusted plane. User-generated filesystem and subprocess effects belong in an untrusted execution world.
- The official `SessionPersistence` seam is the logical Session authority. Its production plugin is tiered: PostgreSQL owns metadata, semantic markers and immutable settled-Turn segments; Kafka owns the exact unfinished native suffix; Valkey is only a rebuildable projection. The sealed PostgreSQL prefix plus indexed Kafka suffix must reconstruct the log exactly.
- Browser-visible Session events must cross the live-log durable ACK, ordered projection, and the PostgreSQL projection watermark. PostgreSQL may store opaque Provider locations and digests for an unfinished Turn, but never fine token payload rows. Seal each completed Turn into one native compressed segment.
- Keep live-log and projection media behind Cordis service definitions. A SessionPersistence implementation must not import a concrete Kafka, Valkey, Pulsar, or Redis client.
- Every mutating cloud operation is tenant/workspace scoped and must reject stale writer authority. Do not claim exactly-once execution for arbitrary shell commands.
- Model-visible state must come from the DSH Session event log. Transport-only diagnostics do not silently enter model context.
- The DSH Web UI remains an upstream dependency. Product changes should be implemented as client plugins or profile layers before editing upstream UI code.
- Use mature open-source infrastructure through a narrow adapter when it fits the contract; do not rebuild queues, schedulers, databases, or sandbox runtimes without a demonstrated gap.
- This repository is pre-production. Remove obsolete paths instead of preserving compatibility with unreleased local data.
- Follow `docs/README.md` when loading repository context. Treat `README.md`, `docs/architecture.md`, and `docs/pi-cloud-alignment.md` as the current topology. Acceptance reports are measured evidence, not an architectural override; superseded proposals belong only in Git history.
- Do not add durable user, Session, or Workspace placement on a DSH Worker. Any healthy Worker may claim the next Run; a current `RunAttempt.worker_id` is ephemeral ownership only.

## Change rules

- Keep packages small and role-based. A public abstraction needs a current consumer and a contract test.
- Add an integration test for every persistence, fencing, or execution-boundary change.
- Do not log credentials, model payload secrets, database URLs, or raw tenant artifacts.
- Document the reason for the next milestone, not only its implementation steps.
