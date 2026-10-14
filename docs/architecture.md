# Architecture

actordb is a **sharded, actor-model key–value store** written in Rust on top of
[ractor](https://github.com/slawlor/ractor) (a Rust actor framework). It is a
working prototype: a real, networked, multi-node KV store with replication,
persistence, and traffic isolation, but without TLS/auth, multi-datacenter
support, or transactional semantics.

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
- **Async replication**: each fragment keeps one replication sender per peer;
  sends are batched, acked, and repaired by periodic delta reconciliation
  (see [Replication](replication.md)).
- **Opt-in persistence**: RocksDB behind the `rocksdb` cargo feature, flushed in
  batches by a periodic snapshotter (see [Persistence](persistence.md)).

## Crate layout

The workspace root holds the library crate `actordb`, the `docker-tests`
integration crate, and an `xtask` for coverage.

| Module | Role |
|---|---|
| `actordb::node` | Public API: `Node`, `NodeConfig`, `NodeError`, peer mesh, routing, forwarding, replication wiring, placement driver |
| `actordb::actor` | Per-fragment `KvActor` dispatcher, `WorkerPool`, `FragmentState` (the in-memory cache), `VersionedValue`, `wins_over`, `HlcClock`, priority enum |
| `actordb::replication` | Phase 5 async sender/receiver: `ReplicationSender` state machine, sessions, acks, `ReconcileRequest/Response/Fetch/Data` anti-entropy |
| `actordb::supervision` | `FragmentSupervisor` + children: dispatcher, `Cleaner` (TTL/tombstone GC), `Snapshotter` (periodic flush) |
| `actordb::placement` | Phase 8 dynamic placement: `PlacementController` trait, `StaticPlacement`, `ManualPlacement`, migration lifecycle states |
| `actordb::store` | `KvStore` trait, `MemoryStore`, `PrefixedStore`, `RocksDbStore` (feature-gated) |
| `actordb::shard` | `FragmentId`, `hash_key` (seahash), `fragment_of` |
| `actordb::network` | Transport abstraction: `Network`, `TcpNetwork`, `PeerStream`; 4-byte length-prefixed `Envelope` framing |
| `actordb::proto` | prost-generated `meta` protocol-buffer messages (ids 1–17), priorities, replication and reconcile messages |

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
        ├── Cleaner                        (TTL expiry + tombstone GC, every clean_interval)
        └── Snapshotter                    (periodic durable flush, every snapshot_interval)
    └── ReplicationSender × peers          (one sender task per fragment×peer)
```

Key design points:

- **Shared state, isolated execution.** All workers of a fragment share one
  `Arc<FragmentState>` (the in-memory cache), but operations execute in three
  independent worker pools keyed by `OpPriority`. A background bulk scan can
  saturate its pool without touching the p99 of high-priority traffic — this is
  covered by the `background_saturation_leaves_high_priority_p99_unaffected`
  integration test.
- **Sticky key routing.** Inside one priority pool, a key's operations land on
  the same worker (`seahash(key) % workers`). Different priorities use
  different workers; `FragmentState` therefore serializes each LWW merge with
  its persistence enqueue so disk ordering cannot diverge from the cache.
  Local mutation calls await the selected worker's exact apply outcome rather
  than inferring it from a subsequent read (see [Consistency](consistency.md)).
- **Supervision is local and shallow.** A dead worker is replaced in place with
  the same `Arc<FragmentState>` and the same pool slot; a dead child of a
  fragment supervisor (dispatcher, cleaner, snapshotter) is respawned in place.
  There is no escalation across fragments — one fragment's failure never touches
  a sibling.
- **Placement is authoritative input.** In static mode the node routes by
  `fragment_map`. With a `PlacementController` set, a driver polls
  `desired_placement()` every `placement_poll_interval` and spawns/stops
  fragment subtrees as the map changes (see [Routing & Placement](routing-placement.md)).

## What actordb is not

- No transactions, no multi-key atomicity (see [Multi-key batching](batching.md)
  for exactly what `put_multi` guarantees — it's fan-out, not a transaction).
- No security: plain TCP, no TLS, no authentication. TLS is a DIY extension
  point via the `Network` trait.
- No consensus: conflict resolution is last-writer-wins by HLC timestamp, not
  Raft/Paxos (see [Consistency](consistency.md)).
- Persistence is opt-in (`store: None` by default = pure in-memory) and crash
  recovery for the unflushed tail relies on peer sync (see [Persistence](persistence.md)).
