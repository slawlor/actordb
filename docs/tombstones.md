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
3. **Replication**: tombstones ride in `ReplicateBatch` ops, reconcile
   `SyncEntry` data pages, and `KeySummary`s with their `is_tombstone` flag.
   Ordered summary pages, `GetVersioned`, and bounded `GetVersionedPage`
   explicitly include tombstones —
   anti-entropy must ship tombstones so deletes propagate.
4. **TTL expiry**: an expired live value becomes an expiry tombstone with the
   same `(timestamp_ms, logical_counter, node_id)` and a winning `is_tombstone` tie-break bit.
   It beats the TTL value and every older version without deleting a genuinely
   later put. The marker retains `expires_at_ms`. Reads synthesize this marker
   immediately; the cleaner persists it, and boot rewrites expired durable
   values before serving.
5. **Retention**: tombstones are retained indefinitely. Obsolete time-based
   collection options have been removed; no age can certify convergence.

## Conflict rules

`VersionedValue::wins_over` compares `(timestamp_ms, logical_counter, node_id, is_tombstone)`:

- Higher physical timestamp wins; its logical counter orders events in the same millisecond.
- On an exact `(timestamp, logical_counter, node_id)` tie, **the tombstone wins** — deletes are
  explicit.
- A later put with a higher timestamp resurrects the key normally.

## Why time-based garbage collection is rejected

Elapsed time is not evidence that every replica has durably observed a
delete. A tombstone can still be buffered or in flight after an arbitrary
delay, replication progress is not persisted across restart, and the default
`MemoryApplied` acknowledgement can precede durable storage on the receiver.
Collecting the source tombstone in any of those states can allow an older
value to reappear.

For that reason the obsolete `tombstone_gc_age` and `reset_senders` settings
have been removed. Safe reclamation needs durable per-replica convergence
evidence scoped to each key; those persisted watermarks do not exist yet.
Dynamic placement's whole-fragment prefix retirement is different: it has a
closed replica set, exact full-state handoff, and quiesced namespace, so it
does not authorize arbitrary tombstone collection.
There is no time-based `FragmentState::gc_tombstones` API. Periodic TTL
maintenance creates expiry tombstones and retains all existing deletion floors.

## On-disk encoding

Tombstones persist with the `is_tombstone` byte set in the 29-byte value
header (see [Persistence](persistence.md)). Expiry tombstones additionally
retain the original TTL deadline. They are durable across restarts — which is
why delete state survives a node restart.
