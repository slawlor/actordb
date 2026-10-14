# Tombstones & deletes

`delete` never physically removes a key. It writes a **tombstone** — a
`VersionedValue` with `is_tombstone: true` — stamped with the HLC tick at
delete time, applied at the serving owner and enqueued for replication exactly
like a put.
Tombstones exist so that a delete wins over concurrent or lagging puts on
other replicas: without them, a replica that missed the delete would keep
serving the old value forever.

## Lifecycle

1. **Write**: `node.delete(key)` → `VersionedValue::tombstone(now, node_id)`,
   merged at the serving owner, enqueued in its persistence buffer, and
   enqueued for asynchronous replication.
2. **Reads**: `get` filters tombstones — a tombstoned key reads as `Ok(None)`,
   indistinguishable from "never existed".
3. **Replication**: tombstones ride in `ReplicateBatch` ops, `SyncEntry`s, and
   `KeySummary`s with their `is_tombstone` flag. `get_all` (used for sync and
   reconcile summaries) explicitly *includes* tombstones — "sync must ship
   tombstones so deletes propagate."
4. **TTL expiry**: an expired live value becomes an expiry tombstone with the
   same `(timestamp_ms, node_id)` and a winning `is_tombstone` tie-break bit.
   It beats the TTL value and every older version without deleting a genuinely
   later put. The marker retains `expires_at_ms`. Reads synthesize this marker
   immediately; the cleaner persists it, and boot rewrites expired durable
   values before serving.
5. **Retention**: tombstones are retained indefinitely. The
   `tombstone_gc_age` compatibility field must be `None`; `Node::start`
   rejects every `Some(age)` setting.

## Conflict rules

`VersionedValue::wins_over` compares `(timestamp_ms, node_id, is_tombstone)`:

- Higher timestamp wins (last-writer-wins).
- On an exact `(timestamp, node_id)` tie, **the tombstone wins** — deletes are
  explicit.
- A later put with a higher timestamp resurrects the key normally.

## Why time-based garbage collection is rejected

Elapsed time is not evidence that every replica has durably observed a
delete. A tombstone can still be buffered or in flight after an arbitrary
delay, replication progress is not persisted across restart, and the default
`MemoryApplied` acknowledgement can precede durable storage on the receiver.
Collecting the source tombstone in any of those states can allow an older
value to reappear.

For that reason `Node::start` rejects every
`tombstone_gc_age: Some(..)` configuration before binding its listener or
spawning actors. Safe reclamation needs durable per-replica convergence
evidence plus deletion support in the storage layer; neither exists yet.
The field remains temporarily for source compatibility and must be `None`.
The public `FragmentState::gc_tombstones` method likewise remains as a
source-compatible no-op that retains every tombstone and returns zero.

## On-disk encoding

Tombstones persist with the `is_tombstone` byte set in the 21-byte value
header (see [Persistence](persistence.md)). Expiry tombstones additionally
retain the original TTL deadline. They are durable across restarts — which is
why delete state survives a node restart.
