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
   later put. The marker retains `expires_at_ms` as its optional GC age anchor.
   Reads synthesize this marker immediately; the cleaner persists it, and boot
   rewrites expired durable values before serving.
5. **Garbage collection**: `tombstone_gc_age` defaults to `None`, retaining
   tombstones indefinitely. `Some(age)` explicitly enables unsafe,
   time-based removal from the **in-memory cache** only.

## Conflict rules

`VersionedValue::wins_over` compares `(timestamp_ms, node_id, is_tombstone)`:

- Higher timestamp wins (last-writer-wins).
- On an exact `(timestamp, node_id)` tie, **the tombstone wins** — deletes are
  explicit.
- A later put with a higher timestamp resurrects the key normally.

## Garbage collection — and its limits

`gc_tombstones(now_ms, gc_age_ms)` retains tombstones until their configured
age elapses and then drops them from the in-memory `HashMap`. Ordinary delete
tombstones age from `timestamp_ms`; expiry tombstones age from their retained
`expires_at_ms`. The cleaner calls this only when `NodeConfig` contains
`tombstone_gc_age: Some(..)`. Two important caveats:

1. **GC is memory-only.** There is no code path that deletes tombstones from
   RocksDB — the store layer only ever issues `put`s. After memory GC, the old
   tombstone bytes remain on disk and are **re-loaded into memory on the next
   boot**. Durable tombstone GC does not exist in the current implementation.
2. **GC requires convergence.** The cleaner blocks GC while any replication
   sender for the fragment is in `BufferReset` — the tombstone may not have
   reached the peer, and GC'ing it would let reconciliation resurrect the
   deleted key. That guard cannot prove convergence across an arbitrary
   partition. If time-based GC is enabled, its age **must exceed every possible
   partition**: a node rejoining after the marker was GC'd can resurrect an
   older value because it beats "nothing" in the LWW comparison. For this
   reason GC is disabled by default.

## On-disk encoding

Tombstones persist with the `is_tombstone` byte set in the 21-byte value
header (see [Persistence](persistence.md)). Expiry tombstones additionally
retain the original TTL deadline. They are durable across restarts — which is
exactly why the memory-only GC caveat above matters.
