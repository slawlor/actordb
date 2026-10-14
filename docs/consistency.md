# Consistency model

actordb provides **last-writer-wins (LWW) eventual consistency** with a
read-your-write guarantee scoped to a single priority. There is no consensus
protocol, no quorum, and no linearizability.

## Conflict resolution: LWW by HLC

Every write — put, TTL put, delete (tombstone) — is stamped by the node's
**hybrid logical clock** (`HlcClock`, one per node):

- `tick()`: `max(wall_clock_ms, last + 1)` — monotonic, never moves backward.
- `observe(remote_ms)`: first rejects values beyond `max_clock_skew`, then
  advances to `max(wall, last + 1, remote + 1)`. Validation happens before
  replicated, forwarded, synchronized, reconciled, or read-response versions
  are accepted; a violating peer connection is closed.
- `seed_persisted(timestamp_ms)`: runs while each durable fragment starts, so
  the first post-restart write advances beyond every loaded version. A durable
  timestamp beyond `max_clock_skew` fails startup instead of freezing keys.

Conflicts resolve by `VersionedValue::wins_over` on
`(timestamp_ms, node_id, is_tombstone)`: the higher timestamp wins; exact
(timestamp, node_id) ties go to the tombstone. The comparison is total and
commutative, so replicas converge without coordination. `seq`/`boot_id` are
origin metadata; the current receiver does not deduplicate on them, and they
play no role in conflict resolution.

Under bounded clock skew the HLC keeps ordering sane: the
`hlc_orders_writes_under_clock_skew` integration test simulates skewed clocks
and asserts the later logical write still wins everywhere. The current clock
is still a scalar millisecond value: sustained rates above one event per
millisecond can move it ahead of wall time. A future on-disk/wire format change
is required for a true physical/logical tuple.

## Local apply outcomes and read-your-write

For a locally owned fragment, `Node` sends `ApplyOp` as an RPC and waits for the
worker's exact result. A remote owner returns the same result in a correlated
`MutationResponse`:

- `Applied`: this version won and changed the cache.
- `AlreadyPresent`: the identical version had already applied; replay is an
  idempotent success.
- `Superseded { .. }`: another version won; `Node` returns
  `NodeError::Superseded` and never reports this write successful.
- `Expired`: the TTL elapsed before application; its deterministic expiry
  tombstone was installed and `Node` returns `NodeError::Expired`.

Admission and storage failures are also returned directly as
`NodeError::Backpressure` and `NodeError::Storage`. There is no read-back
barrier and no inference that “a newer value exists” proves this operation
applied.

At the exact apply point, the serving owner's shared fragment cache contains
that version or an identical replay; by return time it may contain a newer LWW
winner. Subsequent reads at that owner observe the applied version or a newer
winner regardless of which priority pool serves them. A remote call has a
configured deadline (5 seconds by default) covering queueing and response; a
missing or malformed reply is never converted to success. Priority still
controls admission and scheduling.

## Replication lag and partitions

Writes return after the serving owner's apply; reaching its other replicas is asynchronous
(batched every 100 ms by default, repaired by 5-minute periodic
reconciliation). A read on another replica may return a stale value until the
batch arrives — this is ordinary eventual consistency.

Replication ACKs are separate from client mutation responses. A receiver
awaits every batch apply, attempts the fence requested on the wire, and reports
the level it actually achieved:
`MemoryApplied` requires cache application only, while `StoreCommitted`
requires the configured `KvStore` to accept a flush. `StoreCommitted` is not
an fsync guarantee unless that store provides one. The sender retires the batch
only if the reported level is sufficient. Apply failure, or a configured-store
flush failure, sends a NACK and the exact stamped batch is retried.

On partition:

- Each side keeps accepting writes (availability over consistency).
- Concurrent writes to the same key on both sides resolve by HLC timestamp
  when the partition heals — the later timestamp wins, the earlier is
  silently discarded. There is no conflict callback.
- Deletes and TTL expiry leave versioned tombstones, so they beat older values
  when replicas meet. Tombstones are retained; time-based GC is rejected
  because it would reintroduce resurrection risk (see
  [Tombstones](tombstones.md)).
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
  longer or hide a live one slightly early. The cleaner eventually persists a
  deterministic expiry tombstone using the TTL write's original LWW version.
