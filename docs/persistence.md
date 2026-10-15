# Persistence

Persistence is **opt-in**. `NodeConfig.store` defaults to `None`, which means
a purely in-memory node: the fragment cache holds everything and nothing
touches disk. Set a store to get durability.

## The `KvStore` trait

```rust
pub trait KvStore: Send + Sync + 'static {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    fn load_prefix(&self, prefix: &[u8])
        -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError>;
}
```

The contract is deliberately narrow:

- `load` is called **once at actor boot** and returns the full dataset.
  Corrupt entries fail boot loudly — they are not silently skipped.
- `load_prefix` has a source-compatible default that filters `load`. Ordered
  stores should override it; `RocksDbStore` seeks to the fragment prefix and
  stops when the prefix changes, avoiding one full-database scan per fragment.
- `write_batch` persists a batch of **winning writes** (post-LWW-merge).
  The actor never calls `get`/`put`/`delete` on the store; it works from the
  in-memory `FragmentState` cache and pushes batches down.
- Writes are serialized per fragment and each batch contains at most one
  version per key. A failed or cancelled attempt can be retried, so custom
  stores must apply identical key/version writes idempotently.
- Keys are raw bytes; the actor converts `String` keys at the boundary, so
  non-UTF-8 keys fail boot loudly rather than corrupting the store.
- Loaded records are preflighted against every required single-record wire
  envelope, including the duplicated summary continuation key. RocksDB performs
  this check from raw lengths before decoding/retaining another value copy. A
  legacy key/value that exceeds the 16 MiB hard frame limit, or whose provenance
  node id is empty or exceeds 255 UTF-8 bytes, fails startup rather than entering
  the cache in a permanently unreplicable state. Migrate or remove that record
  offline with a compatible older binary before upgrade; actordb never truncates
  or repairs it automatically.

## Implementations

| Store | Role |
|---|---|
| `MemoryStore` | Test double. `load` returns empty, `write_batch` is a no-op. Never for production. |
| `RocksDbStore` | Production backend, behind the `rocksdb` cargo feature. |
| `PrefixedStore` | Namespace wrapper: prepends a 4-byte big-endian fragment id to every key so N fragment actors can share one store without collisions. |

`RocksDbStore::open(path)` creates the directory and opens with
`DB::open_default` — a **single default column family**, no custom CFs.
`write_batch` builds one `rocksdb::WriteBatch` and commits it with
`db.write(batch)`, so a flush is a single atomic RocksDB write.

With the `rocksdb` feature enabled, `NodeConfig::with_rocksdb(path)` builds a
config on top of the defaults with a `RocksDbStore` wired in.

## On-disk format

Keys on disk are the raw key bytes, optionally prefixed by `PrefixedStore`'s
4-byte big-endian fragment id. Values are encoded as:

```
[timestamp_ms: u64 BE][expires_at_ms: u64 BE, 0 = none][is_tombstone: u8]
[node_id_len: u32 BE][node_id: UTF-8][value bytes...]
```

- The fixed header is 21 bytes (8 + 8 + 1 + 4).
- TTLs are stored exactly as `expires_at_ms`; `0` means "no expiry".
- Tombstones are stored as tombstone-flagged entries — deletes and TTL expiry
  markers are durable, not physical removals. An expiry marker keeps the TTL
  write's timestamp, node id, and expiry deadline while clearing its value.

## Flush batching and "snapshots"

`FragmentState::apply` performs the LWW cache merge and persistence enqueue as
one ordered operation. This prevents workers in different priority pools from
persisting versions in a different order from the in-memory winner. Reads use
the cache and do not wait for an ordinary buffered write to reach disk.

Only one store write may be in progress per fragment. A batch remains marked
in flight until the store write succeeds; failures and cancelled flush callers
therefore leave it available for an idempotent retry. Duplicate updates to one
key are coalesced to their LWW winner before calling the store.

The buffer is pushed to the store when:

- it reaches `flush_batch_size` (default 1000 ops; ignored when `store` is `None`), or
- a write arrives after `flush_interval` has elapsed (default 100 ms), or
- the periodic **Snapshotter** ticks (every `snapshot_interval`, default 60 s), or
- a tracked replication batch or reconciliation barrier requests a
  `StoreCommitted` ACK.

`flush_interval` is write-triggered, not an independent timer. For an idle
fragment, `snapshot_interval` is the upper bound before a periodic flush.
The cleaner enqueues TTL-expiry tombstones through the same ordered buffer.
Expired values found during startup are rewritten as tombstones before that
fragment starts serving, so a crash/restart cannot erase the version floor.

An inbound tracked replication batch adds an explicit flush fence after all of
its apply replies only when its sender requests `StoreCommitted`; the default
`MemoryApplied` requirement leaves the normal batch/interval flush policy in
effect. Untracked reconciliation repairs are memory-applied; their final
barrier forces a flush only when it requests `StoreCommitted`. With a configured
store, `BatchAck::STORE_COMMITTED` means
`KvStore::write_batch` returned successfully for every update ordered before
the fence. It does **not** mean fsync: `RocksDbStore` currently uses RocksDB's
default WAL-backed, non-sync write options. Deployments that require
power-loss durability need the later configurable-sync storage work; the wire
ACK deliberately reports only the guarantee implemented here.

Despite the name, the `Snapshotter` is **not** a RocksDB checkpoint — it is a
periodic durable-flush actor (`SnapshotterMessage::Snap` → `shared.flush()`).
There are no checkpoints or point-in-time snapshots in the current design.

Crash semantics: explicit `shutdown().await` closes public-operation admission,
waits for admitted calls, drains fragment mailboxes, and returns only after each
final local flush succeeds and the RocksDB lock is released. A flush error is
returned while the quiesced fragment and its retryable batch remain owned;
retry shutdown after storage recovers. Dropping `Node` requests a best-effort
stop but cannot await or report that flush or lock release; `kill -9` can lose
the unflushed write-buffer tail. Recovery for that tail comes from correlated
peer reconciliation after reconnect — which is why production deployments
want at least two replicas per fragment.

Sender-ingress overflow recovery reads this same authoritative cache and
revision; it never clears the persistence buffer. Because overflow commands
represent writes that have already merged locally, a correlated reset can
reconstruct their current LWW winners and tombstones. That mechanism bounds
asynchronous fan-out memory but does not strengthen origin crash durability:
a storeless or unflushed origin can still fail before any peer receives the
reconciled value.

## Clock recovery and timestamp admission

Each fragment load reports the highest persisted `timestamp_ms` to the
node-wide `HlcClock` before its dispatcher starts. The first post-restart write
therefore advances beyond durable LWW state instead of returning success while
being rejected by an older, future-dated value.

Persisted and peer-supplied timestamps are checked against
`NodeConfig.max_clock_skew`. Values farther ahead than the configured bound are
not clamped and stored: peer input closes that connection, while poisoned
durable state fails startup with an error. Operators must correct the clock or
explicitly widen the bound after investigating the data.

## What persistence does not do

- **No tombstone reclamation.** Tombstones are retained in memory and on disk.
  `Node::start` rejects every `tombstone_gc_age: Some(..)` setting because a
  time bound cannot prove durable replica convergence. Safe reclamation needs
  such evidence plus store deletion support (see
  [Tombstones & deletes](tombstones.md)).
- **No per-fragment drop/migrate at the store layer yet.** Prefix-bounded loads
  are implemented, but deletion/export lifecycle operations are not.
