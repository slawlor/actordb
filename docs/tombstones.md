# Tombstones & deletes

`delete` never physically removes a key. It writes a **tombstone** — a
`VersionedValue` with `is_tombstone: true` — stamped with the HLC tick at
delete time, applied locally and enqueued for replication exactly like a put.
Tombstones exist so that a delete wins over concurrent or lagging puts on
other replicas: without them, a replica that missed the delete would keep
serving the old value forever.

## Lifecycle

1. **Write**: `node.delete(key)` → `VersionedValue::tombstone(now, node_id)`,
   applied to the cache, flushed to the store, replicated.
2. **Reads**: `get` filters tombstones — a tombstoned key reads as `Ok(None)`,
   indistinguishable from "never existed".
3. **Replication**: tombstones ride in `ReplicateBatch` ops, `SyncEntry`s, and
   `KeySummary`s with their `is_tombstone` flag. `get_all` (used for sync and
   reconcile summaries) explicitly *includes* tombstones — "sync must ship
   tombstones so deletes propagate."
4. **TTL interplay**: tombstones never expire by TTL. TTL expiry (`expires_at`)
   and tombstone aging are separate mechanisms.
5. **Garbage collection**: after `tombstone_gc_age` (default 1 hour), the
   cleaner's `gc_tombstones` reaps tombstones from the **in-memory cache**.

## Conflict rules

`VersionedValue::wins_over` compares `(timestamp_ms, node_id, is_tombstone)`:

- Higher timestamp wins (last-writer-wins).
- On an exact `(timestamp, node_id)` tie, **the tombstone wins** — deletes are
  explicit.
- A later put with a higher timestamp resurrects the key normally.

## Garbage collection — and its limits

`gc_tombstones(now_ms, gc_age_ms)` retains tombstones while
`now_ms - timestamp_ms < gc_age_ms` and drops the rest from the in-memory
`HashMap`. Two honest caveats:

1. **GC is memory-only.** There is no code path that deletes tombstones from
   RocksDB — the store layer only ever issues `put`s. After memory GC, the old
   tombstone bytes remain on disk and are **re-loaded into memory on the next
   boot**. Durable tombstone GC does not exist in the current implementation.
2. **GC requires convergence.** The cleaner blocks GC while any replication
   sender for the fragment is in `BufferReset` — the tombstone may not have
   reached the peer, and GC'ing it would let reconciliation resurrect the
   deleted key. More generally, `tombstone_gc_age` **must exceed the longest
   expected partition**: a node rejoining after its tombstones were GC'd can
   resurrect deleted keys, because the rejoining node's pre-delete live value
   will beat "nothing" in the LWW comparison. This is a documented, accepted
   limitation of time-based tombstone GC.

## On-disk encoding

Tombstones persist with the `is_tombstone` byte set in the 21-byte value
header (see [Persistence](persistence.md)). They are durable across restarts —
which is exactly why the memory-only GC caveat above matters.
