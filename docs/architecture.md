# Architecture

actordb is a **sharded, actor-model key–value store** written in Rust on top of
[ractor](https://github.com/slawlor/ractor) (a Rust actor framework). It is a
working prototype: a real, networked, multi-node KV store with replication,
persistence, traffic isolation, and an optional fresh challenge-bound
pre-shared-secret handshake, but without built-in TLS, individual peer
credentials, multi-datacenter support, or transactional semantics.

## What it is

- **Distributed KV store**: keys are strings, values are byte vectors, each
  value carries a hybrid-logical-clock (HLC) timestamp, a writer node id, an
  optional TTL, and a tombstone marker.
- **Static sharding**: the keyspace is split into `fragment_count` fragments by
  `seahash(key) % fragment_count` (see [Routing & Placement](routing-placement.md)).
  Each fragment is served by one or more replica nodes listed in a placement map.
- **Actor-per-fragment**: every fragment on a node runs its own supervision
  subtree — a dispatcher actor plus three worker pools — so fragments are
  isolated from each other's failures.
- **Async replication**: each fragment keeps one sender per replica peer;
  batches remain in flight until an apply/flush-aware ACK, retry on NACK or
  disconnect, and are repaired by periodic delta reconciliation
  (see [Replication](replication.md)).
- **Opt-in persistence**: RocksDB behind the `rocksdb` cargo feature. Threshold,
  write-age, periodic, and requested `StoreCommitted` ACK or reconciliation
  barrier flushes ordered batches; `StableCommitted` additionally runs an
  explicit backend fence. The offered backend has a fail-closed node/layout
  manifest, uses synchronous WAL commits by default, and retires revoked
  fragment prefixes only after exact stable handoff (see
  [Persistence](persistence.md)).

## Crate layout

The workspace root holds the library crate `actordb`, the `docker-tests`
integration crate, and an `xtask` for coverage.

| Module | Role |
|---|---|
| `actordb::node` | Public API: `Node`, `NodeConfig`, `NodeError`, peer mesh, routing, forwarding, replication wiring, placement driver |
| `actordb::actor` | Per-fragment `KvActor` dispatcher, internal managed worker pools, indexed `FragmentState` cache, bounded summary/single-value primitives, `VersionedValue`, `wins_over`, `HlcClock`, priority enum |
| `actordb::replication` | Phase 5 async sender/receiver: `ReplicationSender` state machine, sessions, acks, protocol-v3 paged `ReconcileRequest/Response/Fetch/Data` anti-entropy |
| `actordb::supervision` | `FragmentSupervisor` + children: dispatcher, `Cleaner` (TTL-expiry markers), `Snapshotter` (periodic flush) |
| `actordb::placement` | Phase 8 dynamic placement: `PlacementController` trait, `StaticPlacement`, `ManualPlacement`, migration lifecycle states |
| `actordb::store` | `KvStore` identity/batch/prefix contract, `MemoryStore`, fragment-view `PrefixedStore`, fail-closed `RocksDbStore` (feature-gated) |
| `actordb::shard` | `FragmentId`, `hash_key` (seahash), `fragment_of` |
| `actordb::network` | Transport abstraction: `Network`, `TcpNetwork`, `PeerStream`; 4-byte length-prefixed `Envelope` framing |
| `actordb::proto` | prost-generated `meta` protocol-buffer messages (ids 1–20), priorities, mutation outcomes, challenge handshake, replication and reconcile messages |

## The hierarchy

```
Node
├── fragment_map / PlacementController   (routing & placement)
├── peer mesh (TCP, redial supervisor)
└── per fragment (served locally):
    └── FragmentSupervisor (ractor supervisor)
        ├── KvActor dispatcher            (routes by priority, sticky-routes by key)
        │   ├── high_pool      (N workers)
        │   ├── default_pool   (N workers)
        │   └── background_pool(N workers)
        ├── Cleaner                        (TTL markers, every clean_interval)
        └── Snapshotter                    (periodic durable flush, every snapshot_interval)
    └── ReplicationSender × replica peers  (one sender task per fragment×target)
```

Key design points:

- **Shared state, isolated execution.** All workers of a fragment share one
  `Arc<FragmentState>` (the in-memory cache), but operations execute in three
  independent worker pools keyed by `OpPriority`. Workers and admitted
  backlogs are isolated; the dispatcher mailbox, fragment cache locks, and
  peer connection reader remain shared. Accepted reads are handed directly to
  their sticky worker, so one blocked background worker does not hold the
  dispatcher behind a nested RPC.
- **Sticky key routing.** Inside one priority pool, a key's operations land on
  the same worker (`seahash(key) % workers`). Different priorities use
  different workers; `FragmentState` therefore serializes each LWW merge with
  its persistence enqueue so disk ordering cannot diverge from the cache.
  Local and forwarded client mutations await the serving worker's exact apply
  outcome rather than inferring it from a subsequent read (see
  [Consistency](consistency.md)).
- **Supervision is local and shallow, but not yet lossless.** A dead worker is
  replaced with the same `Arc<FragmentState>` and pool slot, but accepted jobs
  in its mailbox are not replayed. Their reply ports close, and exact
  per-worker generation accounting restores the replacement's capacity.
  Cleaner and snapshotter restart in place. A Node-owned monitor republishes
  a replacement dispatcher through a generation-checked `FragmentRouter`
  entry; a stale monitor from a removed placement generation cannot overwrite
  a re-added subtree. The planned ractor `Factory` migration addresses
  discard/requeue policy and lossless restart without coupling failures across
  fragments.
- **Shutdown preserves the retry boundary.** A one-way Node admission gate
  rejects new public data calls, while operations admitted before the fence
  finish or receive their transport failure. Each fragment closes and joins
  periodic producers, drains the dispatcher and every worker mailbox, then
  performs an explicit supervisor-owned final flush. Supervisors remain alive
  until every fragment succeeds, so a store error or cancelled waiter can
  retry the retained batch without reopening service. Replication-sender
  convergence remains a separate transport-lifecycle step.
- **Placement is authoritative input.** In static mode the node routes by
  `fragment_map`. With a `PlacementController` set, a driver polls
  `desired_placement()` every `placement_poll_interval` and spawns/stops
  fragment subtrees as the map changes (see [Routing & Placement](routing-placement.md)).
- **Replication registries are snapshot-only on data paths.** Mutation fan-out,
  ACK/NACK delivery, peer notifications, and report inspection clone cheap
  exact-task handles under the node-global registries, then perform payload
  cloning, channel admission, or report waits after releasing them. Durable
  handoff uses a per-fragment finalization seal, so stopping its sender tasks
  and querying its actor cannot stall unrelated fragments or peer lifecycle.

## What actordb is not

- No transactions, no multi-key atomicity (see [Multi-key batching](batching.md)
  for exactly what `put_multi` guarantees — it's fan-out, not a transaction).
- Plain TCP by default and no built-in TLS. Protocol v3 uses two fresh
  challenges when proving possession of the optional cluster-wide secret, but
  that shared key does not distinguish members and does not protect subsequent
  frames. Confidential, individually authenticated transports remain an
  extension point via the `Network` trait.
- No consensus: conflict resolution is last-writer-wins by HLC timestamp, not
  Raft/Paxos (see [Consistency](consistency.md)).
- Persistence is opt-in (`store: None` by default = pure in-memory) and crash
  recovery for the unflushed tail relies on peer reconciliation (see
  [Persistence](persistence.md)).
