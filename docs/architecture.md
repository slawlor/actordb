# Architecture

actordb is a **sharded, actor-model key–value store** written in Rust on top of
[ractor](https://github.com/slawlor/ractor) (a Rust actor framework). It is a
working prototype: a real, networked, multi-node KV store with replication,
persistence, traffic isolation, and optional pre-shared-secret peer
authentication, but without built-in TLS, multi-datacenter support, or
transactional semantics.

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
  write-age, periodic, requested `StoreCommitted` ACK, and reconciliation
  fences flush ordered batches (see [Persistence](persistence.md)).

## Crate layout

The workspace root holds the library crate `actordb`, the `docker-tests`
integration crate, and an `xtask` for coverage.

| Module | Role |
|---|---|
| `actordb::node` | Public API: `Node`, `NodeConfig`, `NodeError`, peer mesh, routing, forwarding, replication wiring, placement driver |
| `actordb::actor` | Per-fragment `KvActor` dispatcher, `WorkerPool`, `FragmentState` (the in-memory cache), `VersionedValue`, `wins_over`, `HlcClock`, priority enum |
| `actordb::replication` | Phase 5 async sender/receiver: `ReplicationSender` state machine, sessions, acks, `ReconcileRequest/Response/Fetch/Data` anti-entropy |
| `actordb::supervision` | `FragmentSupervisor` + children: dispatcher, `Cleaner` (TTL-expiry markers), `Snapshotter` (periodic flush) |
| `actordb::placement` | Phase 8 dynamic placement: `PlacementController` trait, `StaticPlacement`, `ManualPlacement`, migration lifecycle states |
| `actordb::store` | `KvStore` trait, `MemoryStore`, `PrefixedStore`, `RocksDbStore` (feature-gated) |
| `actordb::shard` | `FragmentId`, `hash_key` (seahash), `fragment_of` |
| `actordb::network` | Transport abstraction: `Network`, `TcpNetwork`, `PeerStream`; 4-byte length-prefixed `Envelope` framing |
| `actordb::proto` | prost-generated `meta` protocol-buffer messages (ids 1–19), priorities, mutation outcomes, replication and reconcile messages |

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
  independent worker pools keyed by `OpPriority`. A background bulk scan can
  saturate its pool without touching the p99 of high-priority traffic — this is
  covered by the `background_saturation_leaves_high_priority_p99_unaffected`
  integration test.
- **Sticky key routing.** Inside one priority pool, a key's operations land on
  the same worker (`seahash(key) % workers`). Different priorities use
  different workers; `FragmentState` therefore serializes each LWW merge with
  its persistence enqueue so disk ordering cannot diverge from the cache.
  Local and forwarded client mutations await the serving worker's exact apply
  outcome rather than inferring it from a subsequent read (see
  [Consistency](consistency.md)).
- **Supervision is local and shallow, but not yet lossless.** A dead worker is
  replaced with the same `Arc<FragmentState>` and pool slot, but accepted jobs
  in its mailbox are not replayed and pending admission accounting can leak.
  Cleaner and snapshotter restart in place. A dispatcher restart is not yet
  republished to `FragmentRouter`, so callers can retain the dead reference.
  The planned ractor `Factory` migration addresses worker admission/drain and
  a stable routed facade without coupling failures across fragments.
- **Placement is authoritative input.** In static mode the node routes by
  `fragment_map`. With a `PlacementController` set, a driver polls
  `desired_placement()` every `placement_poll_interval` and spawns/stops
  fragment subtrees as the map changes (see [Routing & Placement](routing-placement.md)).

## What actordb is not

- No transactions, no multi-key atomicity (see [Multi-key batching](batching.md)
  for exactly what `put_multi` guarantees — it's fan-out, not a transaction).
- Plain TCP by default and no built-in TLS. Optional pre-shared-secret Hello
  authentication is available; encrypted transports remain an extension point
  via the `Network` trait.
- No consensus: conflict resolution is last-writer-wins by HLC timestamp, not
  Raft/Paxos (see [Consistency](consistency.md)).
- Persistence is opt-in (`store: None` by default = pure in-memory) and crash
  recovery for the unflushed tail relies on peer sync (see [Persistence](persistence.md)).
