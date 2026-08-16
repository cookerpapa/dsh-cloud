# Tiered native Session persistence

## Decision

DSH Cloud keeps the official DSH `SessionPersistence` service as the single
logical Session-storage contract. The production profile installs a
PostgreSQL-backed tiered implementation and injects two narrower Cordis
capabilities into it:

```text
Tiered SessionPersistence plugin (replaceable as a whole)
  ├─ PostgreSQL: Session metadata, semantic markers, settled Turn segments
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

This design intentionally avoids a PostgreSQL payload Outbox and a separate
Relay service. The SessionPersistence plugin itself is the ordered producer,
which keeps DSH's append acknowledgement equal to cloud durability.
