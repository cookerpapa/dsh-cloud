# Documentation map

This directory describes the architecture that exists on `main`. Read it in
this order:

1. [Architecture](architecture.md) — current components, state ownership and
   failure boundaries.
2. [Pi Cloud alignment](pi-cloud-alignment.md) — invariants shared with the
   reference project and deliberate DSH-specific differences.
3. [Configuration](configuration.md) — operator-facing settings and lifecycle
   contracts.
4. [Testing and acceptance](testing.md) — correctness, real-user and capacity
   verification without conflating Agent quality with infrastructure throughput.
5. [Tiered Session persistence decision](decisions/tiered-session-persistence.md)
   — why native unfinished events and settled Turns use different physical
   media.
6. [Production acceptance](reports/production-acceptance-latest.md) — evidence
   measured from one specific build and environment.

Only the root `README.md`, `AGENTS.md`, this map, `architecture.md`,
`configuration.md`, `testing.md`, `pi-cloud-alignment.md`, and the active
decision describe current intent.
Reports are evidence, not normative architecture. Superseded design proposals
belong in Git history rather than alongside current documentation, so an AI
agent or a new maintainer does not have to reconcile mutually incompatible
topologies.
