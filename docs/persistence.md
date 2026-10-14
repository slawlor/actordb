# Persistence

Persistence is **opt-in**. `NodeConfig.store` defaults to `None`, which means
a purely in-memory node: the fragment cache holds everything and nothing
touches disk. Set a store to get durability.

## The `KvStore` trait

```rust
pub trait KvStore: Send + Sync + 'static {
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError>;
}
```

The contract is deliberately narrow:

- `load` is called **once at actor boot** and returns the full dataset.
  Corrupt entries fail boot loudly — they are not silently skipped.
- `write_batch` persists a batch of **winning writes** (post-LWW-merge).
  The actor never calls `get`/`put`/`delete` on the store; it works from the
  in-memory `FragmentState` cache and pushes batches down.
- Keys are raw bytes; the actor converts `String` keys at the boundary, so
  non-UTF-8 keys fail boot loudly rather than corrupting the store.

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
- Tombstones are stored as tombstone-flagged entries — deletes are durable,
  not physical removals.

## Flush batching and "snapshots"

Writes take a **memory-first** path: `FragmentState::apply` updates the cache
(read-your-write never waits for disk), and the op is appended to a
`FlushBuffer`. The buffer is pushed to the store when:

- it reaches `flush_batch_size` (default 1000 ops; ignored when `store` is `None`), or
- `flush_interval` elapses (default 100 ms), or
- the periodic **Snapshotter** ticks (every `snapshot_interval`, default 60 s).

Despite the name, the `Snapshotter` is **not** a RocksDB checkpoint — it is a
periodic durable-flush actor (`SnapshotterMessage::Snap` → `shared.flush()`).
There are no checkpoints or point-in-time snapshots in the current design.

Crash semantics: a clean `shutdown()` flushes (and releases the RocksDB lock),
but `kill -9` can lose the unflushed write-buffer tail. Recovery for that tail
comes from peer sync on reconnect — which is why production deployments want
at least two replicas per fragment.

## What persistence does not do

- **No durable tombstone GC.** `gc_tombstones` removes tombstones from the
  in-memory cache only; there is no code path that issues deletes to RocksDB.
  After memory GC, the old tombstone bytes stay on disk and are re-loaded into
  memory on the next boot (see [Tombstones & deletes](tombstones.md)).
- **No per-fragment drop/migrate at the store layer yet.** `PrefixedStore`'s
  design notes that prefix scans would enable per-fragment drop/migrate, but it
  is not implemented.
