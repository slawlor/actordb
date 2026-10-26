# Architecture

actordb is a **sharded, actor-model key–value store** written in Rust on top of
[ractor](https://github.com/slawlor/ractor) (a Rust actor framework). It is a
working prototype: a real, networked, multi-node KV store with replication,
persistence, traffic isolation, and an optional fresh challenge-bound
pre-shared-secret handshake, but without built-in TLS, individual peer
credentials, multi-datacenter support, or transactional semantics.

The default crate has no bundled storage backend. Its actors still keep their
working data in the fragment cache: a storeless node needs neither `memory`
nor `rocksdb`. The independent `memory` feature exposes a no-op test double;
`rocksdb` exposes the offered durable backend. Custom `KvStore` implementations
and the `PrefixedStore` wrapper are available without either feature.

## What it is

- **Distributed KV store**: keys are strings, values are byte vectors, each
  value carries a hybrid-logical-clock (HLC) timestamp, a writer node id, an
  optional TTL, and a tombstone marker.
- **Static sharding**: the keyspace is split into `fragment_count` fragments by
  `seahash(key) % fragment_count` (see [Routing & Placement](routing-placement.md)).
  Each fragment is served by one or more replica nodes listed in a placement map.
- **Actor-per-fragment**: every fragment on a node runs its own supervision
  subtree — a shared control-plane dispatcher plus independently scheduled
  CRUD pools for each named table — so fragments and tables are isolated from
  each other's query/write load.
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

The workspace contains `actordb`, its lightweight `actordb-protocol`,
`actordb-query`, and `actordb-client` companions, the `docker-tests` integration
crate, the SDK-only `docker-client-tests` scenario executable, and an `xtask`
for coverage. The client application listener wraps a
Node without creating peer membership or replication senders.

| Module | Role |
|---|---|
| `actordb::node` | Public API: `Node`, `NodeConfig`, `NodeError`, peer mesh, routing, forwarding, replication wiring, placement driver |
| `actordb::client_server` | Separate bounded application TCP listener for SDK connections |
| `actordb::query` | SQL execution policy, bounded background pages, and revision-checked fragment coordination |
| `actordb::actor` | All ractor actors: dispatcher/workers, `FragmentSupervisor`, `Cleaner`, `Snapshotter`; shared cache, clock, and operation/value contracts |
| `actordb::replication` | Phase 5 async sender/receiver: `ReplicationSender` state machine, sessions, acks, protocol-v3 paged `ReconcileRequest/Response/Fetch/Data` anti-entropy |
| `actordb::placement` | Phase 8 dynamic placement: `PlacementController` trait, `StaticPlacement`, `ManualPlacement`, migration lifecycle states |
| `actordb::store` | Shared `KvStore` identity/batch/prefix contract, errors, capabilities, backend options, and compatible implementation reexports |
| `actordb::store::prefixed` | Always-available fragment-view `PrefixedStore` wrapper |
| `actordb::store::memory` | No-op `MemoryStore` test double; opt-in `memory` feature |
| `actordb::store::rocksdb` | Fail-closed `RocksDbStore` durable backend; opt-in `rocksdb` feature |
| `actordb::shard` | `FragmentId`, `hash_key` (seahash), `fragment_of` |
| `actordb::network` | Transport abstraction: `Network`, `TcpNetwork`, `PeerStream`; 4-byte length-prefixed `Envelope` framing |
| `actordb_protocol::meta` | Shared generated peer messages, priorities, mutation outcomes, handshake, replication, reconciliation, scans, and query pages |

### Actor implementation files

Each actor's implementation, arguments, and runtime state live together in a
dedicated file under `actor/`, including the supervisor and maintenance actors.
`actor.rs` is the canonical public facade. Imports such as
`actordb::actor::{KvActor, KvArgs, Cleaner, CleanerArgs}` include their
generated Bon builder types and inferred typestate setter chains. Bon's
anonymous state modules remain private. Internal module boundaries are not
new public APIs.

- `actor/kv_actor.rs`: fragment dispatcher and its linked worker lifecycle.
- `actor/kv_worker.rs`: the public direct-operation worker.
- `actor/pool_worker.rs`: the private admission-accounted pool worker.
- `actor/pool.rs`: priority-pool configuration, routing, and reservations.
- `actor/fragment_state.rs`, `actor/protocol.rs`, and `actor/clock.rs`: shared
  cache/persistence, operation/value contracts, and HLC primitives.
- `actor/fragment.rs`: per-fragment lifecycle, child supervision, health, restart, and quiesce.
- `actor/cleaner.rs` and `actor/snapshotter.rs`: the individual
  maintenance actors; `actor/maintenance.rs` holds their shared ticker
  and coalesced trigger support.

Unit tests and small shared test fixtures use inline `#[cfg(test)]` modules in
their owning source files. Tests retain access to private invariants without
exposing runtime internals or creating a separate file for each test group.
Downstream API and multi-node integration tests remain under `actordb/tests`.
This is
an organizational refactor: it does not change scheduling, locking, admission,
durability, or replace the worker pools with ractor's `Factory`. The
`ReplicationSender` is a Tokio task/state machine, not a ractor actor; the node
transport and replication modules are separate future decomposition work.

## The hierarchy

```
Node
├── fragment_map / PlacementController   (routing & placement)
├── peer mesh (TCP, redial supervisor)
└── per fragment (served locally):
    └── FragmentSupervisor (ractor supervisor)
        ├── KvActor dispatcher            (shared ingress/supervision)
        │   ├── base pools                 (internal namespace-free messages)
        │   └── table "USERS" CRUD pools   (high/default/background workers)
        │       └── table "ORDERS" CRUD pools (created on first use)
        ├── Cleaner                        (TTL markers, every clean_interval)
        └── Snapshotter                    (periodic durable flush, every snapshot_interval)
    └── ReplicationSender × replica peers  (one sender task per fragment×target)
```

Key design points:

- **Shared control plane, isolated table execution.** Each named table owns an
  `Arc<FragmentState>` (cache, ordered index, flush buffer, and durable store
  namespace) and three independent worker pools keyed by `OpPriority`. Workers,
  admission budgets, and shed counters are therefore table-local: adding a
  busy table does not divide another table's CRUD throughput. The fragment
  dispatcher, replication, placement, and maintenance remain shared. Accepted
  reads are handed directly to their sticky table worker, so one blocked
  background worker does not hold the dispatcher behind a nested RPC.
- **Owned forwarded completion.** A separate private node-owned service admits
  serving forwarded single puts and whole batches with aggregate count/payload
  limits and eight workers per priority. It retains operation/placement leases
  through exact apply outcomes and sender handoff despite reader cancellation;
  explicit shutdown drains it before stopping actors/senders. Readers still
  await it, preserving same-connection mutation and replication frame order.
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
