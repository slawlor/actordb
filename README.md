# actordb

A rust actor-based distributed database based on *RocksDb*

* [<img alt="github" src="https://img.shields.io/badge/github-slawlor/actordb-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/slawlor/actordb)
* [<img alt="crates.io" src="https://img.shields.io/crates/v/actordb.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/actordb)
* [<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-actordb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/actordb)
* [![CI/main](https://github.com/slawlor/actordb/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/slawlor/actordb/actions/workflows/ci.yaml)
* [![codecov](https://codecov.io/gh/slawlor/actordb/branch/main/graph/badge.svg)](https://codecov.io/gh/slawlor/actordb)

* `actordb`: ![actordb Downloads](https://img.shields.io/crates/d/actordb.svg)

## Working prototype

A working, tested prototype of the actor-first sharded database vision:

* **`shard`**: deterministic key → fragment mapping (`fragment_of`
  via seahash). The keyspace is partitioned into `fragment_count`
  fragments; every node agrees on the mapping.
* **`actor`**: per fragment, a `KvActor` dispatcher (built on
  [ractor](https://github.com/slawlor/ractor)) routes ops to a
  `WorkerPool` by `hash(key) % worker_count`. Sticky dispatch preserves
  per-key ordering within a priority pool while different keys execute
  concurrently. Workers share the fragment's `FragmentState`: one in-memory
  cache, one ordered persistence buffer, one `KvStore`; cross-pool LWW merges
  are serialized there.
* **`supervision`**: every fragment runs under a `FragmentSupervisor`
  owning the dispatcher pool plus background actors — a `Cleaner`
  (durable TTL-expiry markers, optional tombstone aging) and a `Snapshotter`
  (periodic durable flush). Dead workers restart in place; dead children restart without
  touching sibling fragments. `Node::health()` reports per-fragment
  health.
* **`node`**: a `Node` pairs the per-fragment supervisors with a Tokio
  TCP mesh. Static placement (`fragment_map`) with caller-side one-hop
  routing: a node either applies a write locally and enqueues it for
  async replication, or forwards it to a fragment owner. Inbound ops
  merge with **last-writer-wins** on hybrid-logical-clock timestamps
  (tie-broken by `node_id`), which is idempotent and commutative, so
  replicas converge without coordination. Deletes are tombstones that
  propagate like writes. A joining node pulls state via handshake sync
  (anti-entropy).
* **`store`**: pluggable durable storage. In-memory by default; enable
  the `rocksdb` feature for RocksDB (per-fragment key namespacing via
  `PrefixedStore`).
* **`proto`**: the wire protocol — prost-encoded `Envelope` frames,
  length-delimited over TCP.

Run the tests (more than 40 cluster integration tests over real localhost
TCP, plus more than 20 unit tests):

```bash
cargo test
```

Run the three-node demo (writes from each node, prints every node's
converged local copy):

```bash
cargo run --example three_node_demo
```

## Documentation

Comprehensive guides live in [`docs/`](docs/):

- [Architecture](docs/architecture.md) — what actordb is, crate layout, the
  fragment/supervisor/worker-pool hierarchy
- [Public API guide](docs/api.md) — `Node`, `NodeConfig`, puts/gets/deletes,
  `put_multi`/`get_multi`, TTLs, priorities, health APIs, with examples
- [Persistence](docs/persistence.md) — RocksDB backend, on-disk format,
  `PrefixedStore`, flush batching, crash semantics
- [Routing & placement](docs/routing-placement.md) — key→fragment→owner,
  `PlacementController`, migration protocol, `placement_report()`
- [Replication](docs/replication.md) — sender/receiver state machine, sessions,
  acks, periodic reconciliation, the Hello/full-state handshake
- [Tombstones & deletes](docs/tombstones.md) — delete lifecycle, conflict
  rules, GC limits (memory-only GC is documented honestly)
- [Consistency model](docs/consistency.md) — LWW via HLC, exact local apply
  outcomes, read-your-write, partitions
- [Multi-key batching](docs/batching.md) — fan-out, failover, partial failures
- [Traffic classes & backpressure](docs/backpressure.md) — `OpPriority`,
  pool config, shed behavior and observability
- [Configuration reference](docs/configuration.md) — every `NodeConfig`
  field with defaults and guidance
- [Operations](docs/operations.md) — crash/restart, rolling restart,
  partitions, overload, what to monitor
- [Test strategy](docs/testing.md) — unit vs integration vs Docker scenarios

## Docker integration tests

Modeled on
[ractor's cluster integration tests](https://github.com/slawlor/ractor/tree/main/ractor_cluster_integration_tests):
three nodes on a private compose network (`test-net`), each container
running one scripted role from the `actordb-docker-test` binary
(`docker-tests/` crate). Unlike the in-process tests, these exercise
real multi-host networking — DNS peer addresses, container death, and
true `SIGKILL` crashes.

Scenarios live in `docker/envs/`:

* **replication** — node-a writes one key; node-b and node-c must
  converge on it.
* **restart** — `SIGKILL` node-a mid-cluster (a real kill -9, delivered
  by the driver script), restart it, and require all three nodes to
  reconverge. Exercises the redial supervisor, fresh replication sessions,
  and handshake sync across process death.

```bash
docker compose build
bash docker/run-scenario.sh docker/envs/replication.env
bash docker/restart-test.sh
```

CI runs both scenarios on every push/PR touching the relevant paths
(`.github/workflows/docker-integration.yaml`).

## Public API

### `actordb::node` — the main entry point

```rust
use actordb::node::{Node, NodeConfig};
use std::time::Duration;

let node = Node::start(
    "node-a".to_string(),
    "0.0.0.0:7447".parse().unwrap(),
    NodeConfig::default(),
)
.await?;
// port 0 picks an ephemeral port; the bound address is available via `node.addr()`

node.connect("127.0.0.1:7447".parse().unwrap()).await?;
// handshake runs a full-state sync, so a joining node converges immediately.

node.put("key".to_string(), b"value".to_vec()).await?;
node.put_with_ttl("temp".to_string(), b"value".to_vec(), Some(Duration::from_secs(60))).await?;
node.delete("key".to_string()).await?;

let value = node.get("key".to_string()).await?;  // routed read, Option<VersionedValue>
let all = node.get_all().await?;                  // full local copy (live keys)
let health = node.health().await?;                // per-fragment supervision health

// Bulk APIs: keys hash to fragments, one batch per remote fragment/owner
// attempt. Per-key Results remain in input order.
let puts = node.put_multi(vec![
    ("k1".to_string(), b"v1".to_vec()),
    ("k2".to_string(), b"v2".to_vec()),
]).await;
let gets = node.get_multi(vec!["k1".to_string(), "k2".to_string()]).await;
let peers = node.peer_count().await;

node.shutdown().await;
```

Multi-key semantics:

- `put_multi` / `get_multi` partition keys by fragment and send one
  `BatchPut` / `BatchGet` per remote fragment and owner attempt, rather than
  one message per key.
- Local and remote puts await the serving fragment's exact per-key apply
  outcome. Remote batches preserve request order/cardinality and use a
  5-second deadline that includes outbound queueing and the response.
- Batch APIs fail over across replica holders in placement order on transport,
  timeout, or malformed-response failures. Mutation attempts have a 5-second
  send-and-response deadline; read attempts time out 5 seconds after
  enqueueing. A typed all-`Unavailable` mutation response tries the next
  holder; other valid per-key outcomes are returned directly.
- Errors are per-key and classified: `NodeError::Actor` for
  misconfiguration (no replica listed for a fragment),
  `NodeError::Network` for connectivity failures, plus exact
  `Backpressure`, `Storage`, `Superseded`, and `Expired` mutation outcomes.
- `put_multi_with_priority` / `get_multi_with_priority` set the
  operation priority explicitly (default: `Background`); the priority
  is carried on the wire and honored by the remote owner.

Key `NodeConfig` knobs:

| Field | Default | Meaning |
|---|---|---|
| `fragment_count` | 1 | Fragments in the keyspace (must agree cluster-wide) |
| `fragment_map` | {} | Static placement: fragment → replica node ids (empty = serve all) |
| `members` | {} | Static membership: node id → address |
| `default_pool` / `high_pool` / `background_pool` | see below | Per-priority `PoolConfig { workers, queue_cap, rate_limit_per_sec }` |
| `high_pool` defaults | 2 workers / cap 5_000 / no limit | Latency-sensitive traffic |
| `background_pool` defaults | 2 workers / cap 5_000 / 1000 ops/s | Bulk/scan traffic (rate-limited) |
| `flush_batch_size` / `flush_interval` | 1000 / 100ms | Persistence batching |
| `clean_interval` | 30s | TTL-expiry materialization / optional tombstone-GC sweep period |
| `snapshot_interval` | 60s | Periodic durable flush |
| `tombstone_gc_age` | None | Tombstone GC disabled; `Some(age)` is an unsafe, memory-only opt-in |
| `replication` | defaults | `SenderConfig`: batching, in-flight/buffer bounds, reconcile cadence, required ACK durability |
| `placement_controller` | None | Phase 8: `Some(Arc<dyn PlacementController>)` opts into dynamic placement |
| `placement_poll_interval` | 1s | Phase 8: how often the driver re-evaluates desired placement |
| `store` | None | `Some(Arc<dyn KvStore>)`; `NodeConfig::with_rocksdb(path)` with the `rocksdb` feature |

- Local and remote writes await the serving worker's exact apply outcome;
  after `Ok`, that owner reads the version or a newer winner. Replication to
  the owner's other holders remains asynchronous.
- Conflicts resolve with last-writer-wins on HLC timestamps,
  tie-broken by `node_id`; tombstones win exact ties.
- Failures surface as typed `NodeError` variants, including `Backpressure`,
  `Storage`, `Superseded`, and `Expired` rather than false success.

### `actordb::actor` — the storage core

- `KvActor` — per-fragment dispatcher; `WorkerPool` — sticky-hash
  worker pool; `FragmentState` — the shared cache/buffer/store.
- `VersionedValue { value, timestamp_ms, node_id, expires_at_ms, is_tombstone }`
  — a value plus its LWW version; `wins_over` implements the conflict
  rule, `is_expired` the TTL check.
- `HlcClock` — hybrid logical clock behind all timestamps.
- `ApplyOutcome::{Applied, AlreadyPresent, Superseded, Expired}` — the exact
  result used instead of inferring write success from a later read.
- `KvMessage::{ApplyOp, Flush, Get, GetAll, GetShedCounts}` — for embedders
  driving the dispatcher directly instead of through `Node`.

#### Traffic isolation + backpressure (Phase 7)

Each fragment runs **three isolated worker pools** instead of one:

- `OpPriority::{High, Default, Background}` selects the pool.
  `put_with_priority` / `get_with_priority` (and the
  `put_multi_with_priority` / `get_multi_with_priority` variants) set
  it; plain `put` / `get` / `put_multi` / `get_multi` use `Default`
  (single-key) or `Background` (bulk).
- Each `PoolConfig` sets `workers`, `queue_cap`, and an optional
  token-bucket `rate_limit_per_sec`. When a pool is over its queue cap
  or rate limit, the op is **shed explicitly**: counted in the pool's
  shed counter, and the caller gets `NodeError::Backpressure` — a shed
  `Get` never returns `Ok(None)`, so backpressure is never confused
  with "not found".
- Priority is preserved end-to-end: forwarded single-key ops and batch
  messages carry the originator's priority on the wire, and the owner
  applies/serves them in the matching pool.
- Shed counts are surfaced per pool `(high, default, background)` via
  `node.health()` → `FragmentHealth.shed_counts`.

Ordering honesty: local and forwarded puts wait for the serving worker's exact
apply outcome. At that point its shared cache contains the applied version or
an identical replay; by return time a newer concurrent LWW winner may exist.
A 5-second remote deadline covers both bounded outbound-queue admission and
the reply; cancellation and late replies remove or ignore pending state.

### `actordb::supervision` — per-fragment supervision

- `FragmentSupervisor` — owns a fragment's dispatcher, `Cleaner`, and
  `Snapshotter`; restarts dead children in place.
- `FragmentHealth` — per-fragment health snapshot (worker count/names,
  restart counts, per-pool `shed_counts`).

### `actordb::replication` — async sender/receiver

- Per fragment, per replica peer, a sender task batches mutations (writes +
  tombstones) off the write path: ships on a timer, bounded in-flight,
  and retains each batch until a typed ACK reaches the configured durability
  fence. Receiver apply/flush failures NACK and requeue the exact batch.
- Session ids per pairing; stale batches discarded on reconnect.
- Buffer overflow → `buffer_reset`: shed, block tombstone GC, and
  reconcile via timestamp/tombstone comparison when the peer returns.
- Periodic anti-entropy repairs residual divergence, including in the
  connected state (timestamp/tombstone compare, fetch in 500-key
  chunks, ship winners back). Reconcile rounds are counted per sender
  (`SenderStateReport::reconcile_rounds`).

### `actordb::placement` — dynamic placement controller (Phase 8)

Opt-in via `NodeConfig::placement_controller`. With no controller the
node keeps static placement and behaves exactly as before.

```rust
use actordb::placement::ManualPlacement;
use std::sync::Arc;

let controller = Arc::new(ManualPlacement::new(initial_map));
let config = NodeConfig {
    placement_controller: Some(controller.clone()),
    placement_poll_interval: Duration::from_millis(500),
    ..NodeConfig::default()
};
// ... later, migrate fragment 0 from "a" to "b":
let mut overlap = HashMap::new();
overlap.insert(FragmentId(0), vec!["a".to_string(), "b".to_string()]);
overlap.insert(FragmentId(1), vec!["a".to_string()]);
controller.set_placement(overlap);
// wait until b reports fragment 0 as Serving...
let mut final_map = HashMap::new();
final_map.insert(FragmentId(0), vec!["b".to_string()]);
final_map.insert(FragmentId(1), vec!["a".to_string()]);
controller.set_placement(final_map);
```

- `PlacementController` trait: `desired_placement()`,
  `on_membership_change()`, `on_fragment_ready()`. Implement it for an
  external cluster manager; `ManualPlacement` (interior-mutable map)
  covers tests and operator-driven moves.
- The driver polls the controller, publishes its map as the routing
  table, and converges: gained fragments get a supervised subtree plus
  a **chunked bootstrap** from a replica holder (on-demand reconcile,
  500-key fetch chunks, 30s timeout); revoked fragments drain their
  replication senders, then stop.
- **Migration protocol** (controller-sequenced, with overlap): add the
  new owner → wait for it to report `Serving` (bootstrap done) →
  revoke the old owner. At least one node always serves the fragment;
  LWW keeps the overlap consistent. A bootstrapping node forwards
  reads/writes to the previous owner until it flips to serving.
- A node that receives a routed op for a fragment it no longer serves
  re-forwards it to the current owner (hop-limited, never bounced back
  to the sender) instead of dropping it — this covers the window where
  peers' placement maps disagree.
- `node.placement_report()` — per-fragment lifecycle state
  (`Bootstrapping` / `Serving` / `Draining` / `Stopped`) plus the
  authoritative placement map.
- In dynamic mode the handshake **skips the full-state sync**; the
  Phase 5 reconciler owns anti-entropy (delta-only). A restarted node
  therefore catches up via reconciliation, not a full copy
  (`node.sync_entries_applied()` stays 0).
- Assumes the peer mesh is established (`members` or manual
  `connect()`); the driver does not dial.

### `actordb::proto` — wire protocol

Prost-generated protobuf types (`Envelope`, `Hello`, `ReplicateOp`,
`ForwardedPut`, `ForwardedGet`, `GetResponse`, `SyncRequest`,
`SyncResponse`, `SyncEntry`, `BatchPut`, `BatchGet`,
`BatchGetResponse`, `MutationResponse`, `BatchAck`, `BatchNack`), sent as 4-byte length-prefixed frames
over TCP. Mostly of interest if you're implementing a compatible peer.

### Current limitations

This is a working prototype, not production-ready. Known gaps, tracked
as issues in the [issue tracker](https://github.com/slawlor/actordb/issues):

- **Persistence is opt-in** — pass a `store` in `NodeConfig` for
  durability; without it the dataset lives in memory and a restart
  wipes it (recoverable only via peer sync).
- **No TLS** — plaintext TCP. Handshake authentication via a
  pre-shared cluster secret (`NodeConfig::cluster_secret`): when set,
  every `Hello` carries an HMAC-SHA256(secret, node_id) tag, mismatches
  drop the connection, and non-`Hello` messages arriving before a
  completed handshake are ignored. Without a secret the mesh is
  unauthenticated (a warning is logged at startup).
- **Full-state handshake anti-entropy** — in static mode every
  handshake ships the whole dataset (tombstones included);
  steady-state replication is incremental (batches + reconciliation).
  With dynamic placement enabled the handshake skips the full-state
  sync and the reconciler owns anti-entropy (delta-only).
- **Replication ingress/timeout hardening remains** — apply/flush-aware ACKs,
  NACK retry, and disconnect replay prevent false delivery, but receiver
  mailboxes are not yet byte-bounded and an open connection that never replies
  can fill `max_in_flight` because there is no ACK timer. A sender that enters
  `BufferReset` while that connection stays open also waits for a session
  transition before reconciling.
- **Wire upgrades must be coordinated** — `Hello` does not yet negotiate
  protocol capabilities. The mutation-response and durability-ACK fields fail
  closed across old/new peers rather than silently claiming success.
- **Tombstone GC is disabled by default** — the optional time-based mode is
  memory-only. A node partitioned longer than the configured age can resurrect
  deleted keys on rejoin, and GC'd tombstones are re-loaded from RocksDB on the
  next boot (no durable delete path exists).

## Contributors

To learn more about contributing to `actordb` please see [CONTRIBUTING.md](https://github.com/slawlor/actordb/blob/main/CONTRIBUTING.md).

## License

This project is licensed under [MIT](https://github.com/slawlor/actordb/blob/main/LICENSE).
