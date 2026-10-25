# Persistence

Persistence is **opt-in**. `NodeConfig.store` defaults to `None`, which means
a purely in-memory node: the fragment cache holds everything and nothing
touches disk. This default needs no storage-backend feature; it does not use
`MemoryStore`. Set a durable store to get durability.

The crate's default feature set is empty. Enable the independent `rocksdb`
feature to use the offered durable backend:

```toml
[dependencies]
actordb = { version = "0.9.0", features = ["rocksdb"] }
```

The independent `memory` feature exposes only the no-op `MemoryStore` test
double. It does not retain writes or recover data on restart, and enabling it
does not enable RocksDB. Neither feature is required to implement a custom
`KvStore` or use the always-available `PrefixedStore` wrapper. Enabling a
backend only makes its API available; a Node still needs a configured store.

## The `KvStore` trait

```rust
pub trait KvStore: Send + Sync + 'static {
    fn bind(&self, identity: &StoreIdentity) -> Result<StoreBinding, StoreError>;
    fn load(&self) -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    fn load_prefix(&self, prefix: &[u8])
        -> Result<Vec<(Vec<u8>, VersionedValue)>, StoreError>;
    fn commit_durability(&self) -> StoreCommitDurability;
    fn stable_fence(&self) -> Result<StoreCommitDurability, StoreError>;
    fn write_batch(&self, writes: &[(Vec<u8>, VersionedValue)]) -> Result<(), StoreError>;
    fn prefix_delete_support(&self) -> PrefixDeleteSupport;
    fn delete_prefix(&self, prefix: &[u8])
        -> Result<PrefixDeleteOutcome, StoreError>;
}
```

The contract is deliberately narrow:

- `bind` runs once per Node startup after pure configuration/placement
  validation and before network bind or actor startup. Its default returns an
  unmanaged binding; custom stores remain source compatible. RocksDB validates
  its persistent identity and holds an exclusive live attachment until clean
  Node shutdown.
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
- `commit_durability` describes an ordinary successful batch as `Buffered` or
  `Stable`. `stable_fence` runs after actor buffers flush and may strengthen
  buffered writes; custom stores conservatively default to `Buffered`.
- `prefix_delete_support` is a non-mutating lifecycle capability. Custom stores
  default to `Retained`, so upgrading actordb never introduces an unexpected
  destructive call. `Supported` opts into the deletion contract below.
- `delete_prefix` is an idempotent, quiesced-writer namespace operation. The
  source-compatible custom-store default reports `Retained`; RocksDB commits a
  prefix range tombstone synchronously and reports `Deleted`.
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

| Store | Implementation module | Cargo feature | Role |
|---|---|---|---|
| `MemoryStore` | `actordb::store::memory` | `memory` (opt-in) | Test double. `load` returns empty, `write_batch` is a no-op. Never for production. |
| `RocksDbStore` | `actordb::store::rocksdb` | `rocksdb` (opt-in) | Offered durable backend; not a declaration of production readiness. |
| `PrefixedStore` | `actordb::store::prefixed` | None | Namespace wrapper: prepends a 4-byte big-endian fragment id to every key so N fragment actors can share one store without collisions. |
| `TableStore` | `actordb::store::prefixed` | None | Table-local durable view: adds a disjoint table prefix over a fragment store so each table has an independent cache/flush/iteration partition while retaining the shared backend. |

The `store` parent module owns the shared `KvStore` contract, errors, identity
and capability types, and backend options. Backend code and its unit tests live
in dedicated child modules. Existing imports such as
`actordb::store::{KvStore, RocksDbStore, RocksDbOptions, PrefixedStore}` remain
available through parent reexports when their features are enabled. Shared
types and `PrefixedStore` are unconditional; `MemoryStore` and its child module
require `memory`, while RocksDB types and its child module require `rocksdb`.
Both backend features are disabled by default and can be enabled independently.
Add future supported backends in their own opt-in `store` child modules and
reexport their public entry points from the parent.

`RocksDbStore::open(path)` creates the directory and opens the default CF for
fragment-prefixed data. Existing initialized stores also open their
`actordb-metadata` CF; a new or legacy store creates that CF only after the
first successful identity/data validation in `bind`. `write_batch` builds one
`rocksdb::WriteBatch`, so a flush is one atomic RocksDB write.

RocksDB data operations require the live `StoreBinding` created by
`Node::start` (or an explicit `KvStore::bind` call). Direct base-store keys use
the same physical format as Node: a 4-byte fragment prefix followed by the
UTF-8 logical key. Each read and write validates that prefix, the bound fragment
count, deterministic key mapping, provenance, and wire-size budget. This is a
pre-release behavioral break for direct `RocksDbStore` users. A custom wrapper
around a bind-requiring backend must delegate `bind` and retain the returned
binding; the built-in `PrefixedStore` is deliberately only a fragment view and
is rejected as a Node root.

With the `rocksdb` feature enabled, `NodeConfig::with_rocksdb(path)` builds a
config on top of the defaults with a `RocksDbStore` wired in. It now defaults
to `RocksDbDurability::SyncWal`: successful store batches force the WAL through
the operating-system buffer cache. To choose the faster, weaker policy:

```rust
use actordb::node::NodeConfig;
use actordb::store::{RocksDbDurability, RocksDbOptions};

let options = RocksDbOptions::builder()
    .durability(RocksDbDurability::AsyncWal)
    .build();
let config = NodeConfig::with_rocksdb_options("./data/node-a", options)?;
```

`AsyncWal` keeps the WAL enabled but does not synchronously flush ordinary
batches to stable media. There is deliberately no WAL-disabled mode advertised
as committed. `stable_fence` calls `flush_wal(true)`, so placement handoff can
establish stable storage even when ordinary writes use `AsyncWal`. Manifest
writes and prefix deletion are always synchronous regardless of this setting.

### RocksDB manifest and upgrades

The first successful storage bind during `Node::start` synchronously stores a
manifest containing the node id, fragment count, data-format version, and
partitioner/prefix-layout version. The manifest remains committed if a later
network or actor startup phase fails. Later starts must use the same node id and
fragment count; a mismatch,
unknown version, corrupt manifest, unknown column family, or unknown metadata
record fails closed before a listener binds or a fragment actor loads. Identity
binding errors surface as `NodeError::Storage`; errors detected while the path
is opened surface from `RocksDbStore::open` or `NodeConfig::with_rocksdb*` as
`StoreError`. Placement, membership, replication tuning, and WAL policy are
intentionally not pinned. A cloned/restored database retains the original
identity.

Two live Nodes cannot attach to the same `Arc<RocksDbStore>`. Clean shutdown
releases that attachment only after all fragment actors stop. A fully joined
startup rollback also releases it. Cancelling startup, or merely dropping a
Node, keeps the same live store object fail-closed because actor exit was not
observed.

An empty pre-manifest database is initialized automatically. A non-empty,
default-CF-only database from an older actordb release has no trustworthy
original node id or fragment count and is rejected during strict open *before*
the metadata column family is created, preserving rollback to the old binary.
Likewise, a non-empty directory with a missing or non-regular RocksDB `CURRENT`
file is rejected before RocksDB can initialize or mutate it.
After verifying the path, fragment layout, and a backup, perform the one-time
explicit adoption:

```rust
let options = RocksDbOptions::builder()
    .legacy_manifest_adoption(true)
    .build();
let config = NodeConfig::with_rocksdb_options("./data/legacy-node-a", options)?;
// The first successful storage bind during Node::start writes the manifest.
```

Adoption validates every legacy key's UTF-8 suffix, stored fragment, hash
mapping under the claimed fragment count, value encoding, provenance, and wire
budget before writing the manifest. This detects inconsistent claims but cannot
recover the original identity from legacy data; the operator remains
responsible for supplying it. Adoption never overwrites an existing corrupt,
future, or mismatched manifest. Subsequent opens should remove the flag.

## On-disk format

Keys in the offered RocksDB data CF are always a 4-byte big-endian fragment id
followed by a table-store partition prefix and logical table key. Every
0.8.0 record therefore belongs to an explicit named table; pre-0.8 unnamed
records require an application-managed migration before opening this format.
Values are encoded as:

```
[timestamp_ms: u64 BE][expires_at_ms: u64 BE, 0 = none][is_tombstone: u8]
[node_id_len: u32 BE][node_id: UTF-8][value bytes...]
```

- The fixed header is 21 bytes (8 + 8 + 1 + 4).
- TTLs are stored exactly as `expires_at_ms`; `0` means "no expiry".
- `is_tombstone` must be exactly `0` or `1`; other bytes are corruption and
  fail loading/adoption.
- Tombstones are stored as tombstone-flagged entries — deletes and TTL expiry
  markers are durable, not physical removals. An expiry marker keeps the TTL
  write's timestamp, node id, and expiry deadline while clearing its value.
- The metadata CF's `manifest` record begins with `ACTORDB\0`, independent
  manifest/data/partitioner versions, the fixed fragment count, and node id.
  Metadata never shares the user-key namespace.
- Prefix deletion writes a durable RocksDB range tombstone. Successful reads
  and reopens no longer expose the covered keys, but physical bytes are reclaimed
  later by compaction; this is not secure erasure.

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
  `StoreCommitted` or `StableCommitted` ACK.

`flush_interval` is write-triggered, not an independent timer. For an idle
fragment, `snapshot_interval` schedules a periodic flush opportunity; a stalled
actor or store can delay completion, so it is not a hard durability deadline.
The cleaner enqueues TTL-expiry tombstones through the same ordered buffer.
Expired values found during startup are rewritten as tombstones before that
fragment starts serving, so a crash/restart cannot erase the version floor.

An inbound tracked replication batch adds an explicit flush fence after all of
its apply replies when its sender requests `StoreCommitted` or stronger; the
default `MemoryApplied` requirement leaves the normal batch/interval policy in
effect. Untracked reconciliation repairs are memory-applied and their final
barrier supplies the requested fence. `STORE_COMMITTED` means
`KvStore::write_batch` returned for every ordered update under the backend's
ordinary policy. `STABLE_COMMITTED` additionally means `stable_fence` reported
`Stable`; RocksDB forces its WAL synchronously at that boundary even under
`AsyncWal`.

Despite the name, the `Snapshotter` is **not** a RocksDB checkpoint — it is a
periodic durable-flush actor (`SnapshotterMessage::Snap` → `shared.flush()`).
There are no checkpoints or point-in-time snapshots in the current design.

## Bounded automatic maintenance

Cleaner and Snapshotter each admit at most one automatic job, queued or active.
Ticks while that job is outstanding are coalesced, not accumulated in the
mailbox. A cleaner job retains its admission lease across immediate page
continuations; snapshot flushes retain theirs until completion. Rejection,
cancellation, actor teardown, and completed jobs release the lease. Explicit
low-level `CleanerMessage::Sweep` and `SnapshotterMessage::Snap` remain manual
full operations; callers can still queue those themselves, outside this bound.

Each automatic cleaner page visits at most **256 current ordered keys**, even
when no key has a TTL or the cache HashMap has large unused capacity. Its
**1 MiB soft work target** charges inspected metadata, cursor/pass key clones,
expiry-marker and persistence clones, and the capacity of removed expired
payload buffers. Processing stops after the first record that reaches the
target: a prefix can exceed it by at most that one terminal record's charge.
The first visited record's charge also includes the pass high-water key clone,
even when that metadata describes a different key.
An oversized first record is processed alone; an oversized later record may
terminate a smaller prefix. This is not a hard memory or latency bound:
arbitrary individual buffer capacity, allocation/deallocation costs, ordered
index traversal, lock waits, store calls, flush-batch copies and the existing
dirty/in-flight write buffer are not bounded by the page's soft target.

A pass pins its expiry time; its first page captures an ordered high-water key
and an initial-cardinality visit quota under the cache write lock. Subsequent
pages use an exclusive cursor (`None` is distinct from the empty-string key)
and consume quota only for actually visited records. Concurrent inserts can
consume visits, and inserts/replacements behind the cursor or after the
high-water key may wait for a later pass. This is weakly consistent
materialization, not an atomic initial snapshot. Each visit checks the current
winning value under the write lock. Reads and reconciliation continue to hide
or synthesize expired values immediately, independently of maintenance.

Every page publishes its tombstone replacements, ordered persistence enqueue,
and mutation revision together under the normal flush-buffer → cache lock
order. Flush errors/cancellation retain pending or in-flight data for retry;
a restarted pass may safely rescan. Draining stops automatic continuations.
Between nonterminal pages the cleaner yields with no cache/flush lock held,
retaining its admission lease. This offers other tasks a scheduling opportunity,
not a hard foreground-latency or priority guarantee.
The public `FragmentState::expire_values(now)` full sweep and the authoritative
post-quiescence shutdown sweep/flush are unchanged and catch the remaining
expiry version floors before successful shutdown.

Crash semantics: explicit `shutdown().await` closes public-operation admission,
waits for admitted calls, drains fragment mailboxes, and returns only after each
final local flush succeeds and the RocksDB lock is released. A flush error is
returned while the quiesced fragment and its retryable batch remain owned;
retry shutdown after storage recovers. Dropping `Node` requests a best-effort
stop but cannot await or report that flush or lock/attachment release; `kill -9` can lose
the unflushed write-buffer tail. Recovery for that tail comes from correlated
peer reconciliation after reconnect — which is why production deployments
want at least two replicas per fragment.

For a planned stop, `shutdown_with_replication_drain` can establish a bounded
peer durability fence before that local flush. Each reported target completed a
stable full reconciliation after the local mutation-admission cut and
acknowledged `SenderConfig::required_ack_durability`. `MemoryApplied` still
loses data if that receiver crashes. `StoreCommitted` means its `KvStore`
accepted the flush; the offered RocksDB backend defaults to a synchronously
flushed WAL, while `AsyncWal` is explicitly weaker. `StableCommitted`
additionally invokes the backend's stable fence; RocksDB forces its WAL at this
boundary under either ordinary-write policy. A timeout leaves the
origin's transport, senders, cache, and retryable persistence state alive;
retry after peer/store recovery or explicitly fall back to local-only
`shutdown`. If the peer fence succeeds but the origin's final store flush
fails, the returned error retains the valid peer report and a retry performs
only the local phase.

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
  Obsolete time-based collection settings have been removed because a time
  bound cannot prove durable replica convergence. Safe reclamation needs
  such evidence plus store deletion support (see
  [Tombstones & deletes](tombstones.md)).
- **No orphan-prefix scan after a crash.** Dynamic placement deletes an opted-in
  prefix only after its live transition obtains exact stable handoff evidence.
  A crash before that delete safely leaves the namespace behind, but startup
  does not persist or reconstruct the lost certificate and therefore does not
  scan/delete unassigned prefixes. Re-adding the fragment loads the retained
  data and reconciles it normally.
