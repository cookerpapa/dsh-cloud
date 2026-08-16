# Tiered native Session persistence

## Decision

DSH Cloud keeps the official DSH `SessionPersistence` service as the single
logical Session-storage contract. The production profile installs a
PostgreSQL-backed tiered implementation and injects two narrower Cordis
capabilities into it:

```text
Tiered SessionPersistence plugin (replaceable as a whole)
  ├─ PostgreSQL: Session metadata, semantic markers, settled Turn segments
  ├─ PostgreSQL: latest verified native restore checkpoint after Compaction
  ├─ SessionLiveLog provider: exact unfinished native event suffix
  └─ SessionLiveProjection provider: rebuildable low-latency projection
```

The official `SessionPersistence` seam is what makes PostgreSQL replaceable: a
deployment can install a different complete Session backend in the Cordis
profile without changing DSH's Agent Loop or Web UI. Inside the tiered plugin,
Kafka and Valkey are independently replaceable subproviders. Kafka can be
replaced by another ordered durable log and Valkey by another projection
provider without changing the tiered persistence implementation.

## Write path

The upstream DSH persistence coordinator coalesces adjacent events for at most
the configured batching window. The tiered provider then:

1. serializes the exact native `StorageRecord` batch;
2. waits for the live-log Provider's durable acknowledgement;
3. applies the same sequence-checked envelope to the projection Provider;
4. stores only the durable location, sequence range and digest in PostgreSQL;
5. advances PostgreSQL's visibility watermark and commits.

Gateway forwards an already-produced Worker frame only after that watermark
reaches the event sequence. Fine `assistant/chunk` payloads therefore never
become PostgreSQL hot rows, but the user still cannot observe a chunk that has
not crossed the durable log.

At `turn/end`, the provider reads the indexed live suffix through opaque
Provider locations, validates its range
and digest, writes one gzip-compressed immutable native Turn segment, and
deletes the temporary location rows. Long-term recovery reads PostgreSQL
segments; an interrupted Turn reads the sealed PostgreSQL prefix plus exact
Kafka offsets.

## Compaction-aware cold restore

DSH Compaction replaces nodes on the model-visible surface but deliberately
does not truncate the append-only Session Event Log. The released DSH Session
runtime also requires a complete, contiguous log starting at sequence zero:
Inbox, Plan, Permission, Goal and other plugins may fold native events that are
not part of the model-visible surface. DSH Cloud therefore does not synthesize
a smaller `messages[]` log or renumber events after Compaction.

Instead, a successful `compaction/end` transaction materializes one
SHA-256-verified, gzip-compressed checkpoint of the exact native log through
that sequence. A cold Worker restores:

```text
latest native restore checkpoint [0..N]
  + SessionPersistence.readFrom(N + 1) physical suffix
  = exact original DSH Session Event Log
```

The checkpoint and its Session revision advance in the same PostgreSQL
transaction. Repeated Compactions replace the one derived checkpoint only when
the new watermark is higher. Checkpoint construction runs behind a SQL
savepoint, so failure drops only this derived cache update and never rolls back
the valid native Compaction. Canonical Turn segments and Kafka locations are not
deleted: they remain the history/audit authority and let a corrupt checkpoint
fail soft to a full canonical read. This bounds the number of old segments
touched during ordinary cross-Worker restore and allows suffix seeks, but
intentionally does not claim that the logical DSH history has been truncated. A
truly context-sized Agent restore requires an upstream Session baseline
contract for every plugin projection, which `0.1.0-rc.6` does not provide.

## Failure semantics

- Kafka succeeds and PostgreSQL rolls back: the record is an unreachable orphan;
  retry publishes a new indexed record and background Kafka retention removes
  the orphan.
- Valkey is lost: it is reset and rebuilt from PostgreSQL location metadata plus
  live-log bytes before the next append is admitted.
- PostgreSQL commits: Kafka and Valkey have already acknowledged, so Gateway may
  release the corresponding sequence.
- Kafka retention expires before an interrupted Turn is sealed: recovery fails
  closed. Production retention therefore defines the maximum interrupted-Turn
  recovery horizon and defaults to 30 days.
- Restore checkpoint validation fails: the provider logs a warning and rebuilds
  from canonical Turn segments plus the indexed live suffix.

This design intentionally avoids a PostgreSQL payload Outbox and a separate
Relay service. The SessionPersistence plugin itself is the ordered producer,
which keeps DSH's append acknowledgement equal to cloud durability.
