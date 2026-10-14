# Consistency model

actordb provides **last-writer-wins (LWW) eventual consistency** with a
read-your-write guarantee scoped to a single priority. There is no consensus
protocol, no quorum, and no linearizability.

## Conflict resolution: LWW by HLC

Every write — put, TTL put, delete (tombstone) — is stamped by the node's
**hybrid logical clock** (`HlcClock`, one per node):

- `tick()`: `max(wall_clock_ms, last + 1)` — monotonic, never moves backward.
- `observe(remote_ms)`: `max(wall, last + 1, remote + 1)` — called on every
  inbound path (replicated batches, sync entries, forwarded puts, batch puts),
  so the clock always moves past timestamps it has seen. (One deliberate edge:
  `ReconcileData` entries are merged inside the sender task, which holds no HLC
  handle, so reconcile-fetched timestamps are applied verbatim.)

Conflicts resolve by `VersionedValue::wins_over` on
`(timestamp_ms, node_id, is_tombstone)`: the higher timestamp wins; exact
(timestamp, node_id) ties go to the tombstone. The comparison is total and
commutative, so replicas converge without coordination. `seq`/`boot_id` are
transport dedup metadata and play no role in conflict resolution.

Under clock skew the HLC keeps ordering sane: the `hlc_orders_writes_under_clock_skew`
integration test simulates skewed clocks and asserts the later logical write
still wins everywhere.

## Read-your-write — within a priority

After applying a write, `Node` issues a **barrier read**: a `Get` sent to the
same worker the write went to, at the **write's priority**. Ractor drains each
worker's mailbox FIFO, so the barrier returns only after the write applied —
the `put` future resolves with the write visible to subsequent reads *at that
priority*.

The scoping matters:

- ✅ Same-priority write then read: ordered. This is the guarantee `put`/`get`
  give you.
- ❌ **Cross-priority pairs are not ordered.** The three pools are independent
  actors with no cross-pool ordering promise. A background write followed by a
  default-priority read may not see the write. This is documented on
  `put_with_priority`/`get_with_priority` and is by design: ordering across
  isolated pools would defeat the isolation.

If the barrier read itself is shed by backpressure, the write returns
`Err(NodeError::Backpressure("put barrier shed: ..."))` — the write may or may
not have applied; the caller should retry.

## Replication lag and partitions

Writes return after the local apply; reaching replicas is asynchronous
(batched every 100 ms by default, repaired by 5-minute periodic
reconciliation). A read on another replica may return a stale value until the
batch arrives — this is ordinary eventual consistency.

On partition:

- Each side keeps accepting writes (availability over consistency).
- Concurrent writes to the same key on both sides resolve by HLC timestamp
  when the partition heals — the later timestamp wins, the earlier is
  silently discarded. There is no conflict callback.
- Deletes are tombstones, so a delete on one side beats a pre-partition put on
  the other once they meet — unless tombstone GC already reaped the tombstone
  (see [Tombstones](tombstones.md): `tombstone_gc_age` must exceed the longest
  expected partition, or deleted keys can resurrect).
- A node that restarts with the same `node_id` gets a new session id and
  reconciles; batches from its old session are discarded by receivers.

## What is *not* guaranteed

- No linearizability, no read-after-write across nodes.
- No atomicity across keys: `put_multi` is fan-out, not a transaction
  (see [Multi-key batching](batching.md)).
- No durability for the unflushed write tail on `kill -9` (see
  [Persistence](persistence.md)) — recovered via peer sync.
- Expired keys read as absent, but expiry is evaluated at read time against
  each node's clock; a heavily skewed clock can serve an expired key slightly
  longer or hide a live one slightly early.
