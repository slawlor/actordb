# Consistency model

actordb provides **last-writer-wins (LWW) eventual consistency** with a
read-your-write guarantee scoped to a single priority. There is no consensus
protocol or linearizability. Per-write concern can require processing
acknowledgements, including majority; see [Write concern](write-concern.md).

## Conflict resolution: LWW by HLC

Every write — put, TTL put, delete (tombstone) — is stamped by the node's
**hybrid logical clock** (`HlcClock`, one per node). Its physical millisecond
timestamp and logical counter form `RecordVersion`:

- `tick()` advances physical time to wall time when it can; events
  within the same millisecond advance the logical counter. More than 1,000
  events per second do not create physical skew.
- `observe_version(remote)` first rejects physical timestamps beyond
  `max_clock_skew`, then advances beyond the local and remote versions.
  Validation precedes acceptance of peer versions.
- `seed_version(version)` retains the highest durable version during
  fragment startup. The first new write advances beyond every loaded version;
  excessive durable physical skew fails startup.
- Complete counter/time exhaustion returns an explicit clock error instead of
  reusing a version.

`VersionedValue::wins_over` orders
`(timestamp_ms, logical_counter, node_id, is_tombstone)`: higher physical/logical
versions win, writer identity breaks ties, and a tombstone wins an otherwise
exact tie. `seq` and `boot_id` are origin metadata and play no role in LWW.

For each key, one full ordering tuple **must identify exactly one payload and
expiry deadline**. Node-generated versions preserve this invariant when writer
identities are unique and durable clock floors are retained across restarts.
Custom low-level callers, offline writers, and restore tools must preserve it:
do not run independent writers with the same node identity, do not reset a
writer's durable high-water version, and give edited restore data fresh versions.
A correct identical replay remains idempotent.

Fragment apply returns `ApplyError::VersionConflict` for an equal full tuple
with different retained contents, before changing cache, revision, or persistence.
Anti-entropy carries a SHA-256 digest of payload and TTL semantics as well as
ordering metadata. Digests are computed once when records enter the cache,
including their deterministic expiry representation; bounded summary pages copy
cached hashes. Equal ordering metadata with unequal digests fails the round and
emits a version-conflict diagnostic. ActorDB does not choose an arbitrary content
winner or certify convergence for this invalid input. Repair requires an
operator to restore the intended contents under a fresh version. The digest
uses cryptographic collision resistance; it is not an independent proof of
historical data integrity.

Deletes and expiry remain valid ordered transformations: setting the tombstone
bit makes a distinct winning tuple. Expiry strips the payload, retains its
original deadline, and never resurrects the older live write. An expired
snapshot's synthetic tombstone has the same digest as the installed marker.
Digests are derived again from durable winners on startup; the persisted format
needs no digest field. Store loads must return one winner per key: duplicate
persisted keys fail startup, and an optional expiry deadline of zero is rejected
because the peer and durable codecs reserve zero for no expiry.

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
an fsync guarantee independent of backend policy: offered RocksDB defaults to
`SyncWal`, while explicit `AsyncWal` acknowledges after a non-synchronous WAL
append. `StableCommitted` adds the backend's explicit stable fence; RocksDB
forces its WAL synchronously at that boundary. The sender retires each batch
only if its own reported requirement is satisfied.
Apply failure, or a configured-store flush failure, sends a NACK and the exact
stamped batch is retried.

The Node's asynchronous sender path is count-bounded. If its mutation ingress
or owned deque fills after a local apply, it records a non-droppable overflow
generation and reconciles from the authoritative fragment snapshot. This keeps
the client path nonblocking without turning an already-applied write into a
silent replication loss. It does not make that write synchronously replicated:
the same eventual-consistency and origin crash window still apply.

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
  [Persistence](persistence.md)) — recovered via peer reconciliation.
- Expired keys read as absent, but expiry is evaluated at read time against
  each node's clock; a heavily skewed clock can serve an expired key slightly
  longer or hide a live one slightly early. The cleaner eventually persists a
  deterministic expiry tombstone using the TTL write's original LWW version.
