# actordb

<p align="center">
  <img src="https://raw.githubusercontent.com/slawlor/actordb/main/docs/assets/actordb-logo-draft.svg" alt="ActorDB distributed Ferris graph" width="420">
</p>

A Rust actor-based distributed database with pluggable, opt-in storage

* [<img alt="github" src="https://img.shields.io/badge/github-slawlor/actordb-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/slawlor/actordb)
* [<img alt="crates.io" src="https://img.shields.io/crates/v/actordb.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/actordb)
* [<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-actordb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/actordb)
* [![CI/main](https://github.com/slawlor/actordb/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/slawlor/actordb/actions/workflows/ci.yaml)
* [![codecov](https://codecov.io/gh/slawlor/actordb/branch/main/graph/badge.svg)](https://codecov.io/gh/slawlor/actordb)

* `actordb`: ![actordb Downloads](https://img.shields.io/crates/d/actordb.svg)

## Crate installation

The `0.8` release series requires Rust 1.88+:

```toml
[dependencies]
actordb = "0.8.1"
```

[Write concern](docs/write-concern.md) adds per-write acknowledgement counts
and majority waits while preserving local acknowledgement as the default.
[Region requirements](docs/write-concern-regions.md) can additionally require
a processed acknowledgement outside the receiving node's configured region.

The default crate includes no concrete storage backend. Its actor cache and
`KvStore` contracts work without a database; `NodeConfig.store` remains `None`
unless you supply a custom store. RocksDB is an independent opt-in:

```toml
actordb = { version = "0.8.1", features = ["rocksdb"] }
```

The separate `memory` feature exposes the no-op `MemoryStore` test double; it
is not persistent storage. Neither backend feature enables the other. Protocol
messages ship as generated Rust in `actordb-protocol`; ordinary builds do not
compile protobuf tooling. RocksDB requires its native library and libclang
bindings. `default-features = false` is not
necessary to avoid RocksDB: the default feature set is empty.

This remains pre-1.0 software. The `0.2`, `0.3`, and `0.4` releases include API,
wire-protocol, and legacy-store compatibility changes. Read the [release notes](CHANGELOG.md)
and [release procedure](docs/releases.md) before upgrading or publishing.

## Working prototype

A working, tested prototype of the actor-first sharded database vision:

The workspace requires Rust 1.88 or newer; CI pins 1.88 as the minimum
supported toolchain.

This library workspace does not commit `Cargo.lock`; fresh checkouts resolve
compatible dependency versions, preferring the declared Rust minimum. Cargo
can generate an ignored local lockfile. Run `cargo update` to refresh it and
rerun tests; dependency resolution is not frozen across fresh builds.

* **`shard`**: deterministic key → fragment mapping (`fragment_of`
  via seahash). The keyspace is partitioned into `fragment_count`
  fragments; every node agrees on the mapping.
* **`actor`**: per fragment, a `KvActor` dispatcher (built on
  [ractor](https://github.com/slawlor/ractor)) routes ops to a
  private managed worker pool by `hash(key) % worker_count`. Sticky dispatch
  preserves per-key ordering within a priority pool while different keys
  execute concurrently. Workers share the fragment's `FragmentState`: one
  in-memory cache with a lock-coherent ordered key index, one ordered
  persistence buffer, and one `KvStore`; cross-pool LWW merges are serialized
  there.
* **Supervisor actors (in `actor`)**: every fragment runs under a `FragmentSupervisor`
  owning the dispatcher pool plus background actors — a `Cleaner`
  (durable TTL-expiry markers) and a `Snapshotter`
  (periodic durable flush). Dead workers restart in place; dead children restart without
  touching sibling fragments. `Node::health()` reports per-fragment
  health. The `supervision` module preserves older imports as compatibility
  reexports; implementations live alongside the other actors.
* **`node`**: a `Node` pairs the per-fragment supervisors with a Tokio
  TCP mesh. Static placement (`fragment_map`) with caller-side one-hop
  routing: a node either applies a write locally and enqueues it for
  async replication, or forwards it to a fragment owner. Inbound ops
  merge with **last-writer-wins** on hybrid-logical-clock timestamps
  (tie-broken by `node_id`), which is idempotent and commutative, so
  replicas converge without coordination. Deletes are tombstones that
  propagate like writes. Joining and restarted nodes converge through
  correlated, byte-bounded reconciliation after the peer handshake.
* **`store`**: pluggable durable storage. In-memory by default; enable
  the `rocksdb` feature for RocksDB (per-fragment key namespacing via
  `PrefixedStore`, a fail-closed identity/layout manifest, exclusive live
  attachment, validated physical routing, and synchronous WAL writes by
  default). Shared contracts and options stay in `store`; implementations
  live in its `memory`, `prefixed`, and feature-gated `rocksdb` child modules,
  with `store::*` imports available through reexports when the corresponding
  backend feature is enabled. No bundled backend is enabled by default.
* **`proto`**: the wire protocol — prost-encoded `Envelope` frames,
  length-delimited over TCP.

Run the unit and real-localhost-TCP integration tests:

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
  acks, byte-bounded reconciliation, and the protocol-v3 handshake
- [Tombstones & deletes](docs/tombstones.md) — delete lifecycle, conflict
  rules, retention, and requirements for safe future reclamation
- [Consistency model](docs/consistency.md) — LWW via HLC, exact local apply
  outcomes, read-your-write, partitions
- [Multi-key batching](docs/batching.md) — fan-out, failover, partial failures
- [Scanning](docs/scanning.md) — single-shard filter + transform scans,
  spec semantics, scheduling, and cost model
- [SQL queries](docs/query.md) — fragment filters, global pagination,
  byte projections, and exact aggregates over the virtual `kv` table
- [Rust client](actordb-client/README.md) — remote application connections,
  operation coverage, credentials, and mutation retry semantics
- [Custom transports and TLS](docs/transports.md) — caller-owned stream transports
  for the peer mesh, application listener, and SDK, with a runnable mTLS example
- [TTL expiry callbacks](docs/ttl-expiry-callbacks.md) — optional bounded local
  callbacks, original versions, deduplication, lifecycle, and delivery limitations
- [Traffic classes & backpressure](docs/backpressure.md) — `OpPriority`,
  pool config, shed behavior and observability
- [Configuration reference](docs/configuration.md) — every `NodeConfig`
  field with defaults and guidance
- [Operations](docs/operations.md) — crash/restart, rolling restart,
  partitions, overload, what to monitor
- [Request draining](docs/draining.md), [typed status](docs/status.md), and
  [metrics](docs/metrics.md) — bounded lifecycle controls and backend-neutral observations
- [Lifecycle migration](docs/support-api-migration.md) and
  [replacement safety](docs/replacement-safety.md) — 0.8.1 compatibility and membership boundaries
- [Test strategy](docs/testing.md) — unit vs integration vs Docker scenarios
- [Releases](docs/releases.md) — package checks, version tags, and crates.io publication

## Docker integration tests

Modeled on
[ractor's cluster integration tests](https://github.com/slawlor/ractor/tree/main/ractor_cluster_integration_tests):
three nodes on a private compose network (`test-net`), each container
running one scripted role from the `actordb-docker-test` binary
(`docker-tests/` crate). These exercise process boundaries, container-network
DNS peer addresses, container death, and
true `SIGKILL` crashes.

Scenarios live in `docker/envs/`:

* **replication** — node-a writes one key; node-b and node-c must
  converge on it.
* **restart** — `SIGKILL` node-a mid-cluster (a real kill -9, delivered
  by the driver script), restart it, and require all three nodes to
  reconverge. Exercises the redial supervisor, fresh replication sessions,
  and immediate correlated reconciliation across process death.
* **external client** — a separate SDK-only fourth container talks to three
  sharded nodes through the application protocol. It checks remote routing,
  SQL/transforms, client reconnection after an ingress SIGKILL, and recovery.

```bash
docker compose build
bash docker/run-scenario.sh docker/envs/replication.env
bash docker/restart-test.sh
docker compose -f docker-compose.client.yml build
bash docker/client-test.sh
```

CI runs all scenarios on every push/PR touching the relevant paths
(`.github/workflows/docker-integration.yaml`).

## Public API

### `actordb::node` — the main entry point

```rust
use actordb::node::{Node, NodeConfig, ReplicationDrainOptions};
use std::time::Duration;

let node = Node::start(
    "node-a".to_string(),
    "0.0.0.0:7447".parse().unwrap(),
    NodeConfig::builder().build(),
)
.await?;
// port 0 picks an ephemeral port; the bound address is available via `node.addr()`

node.connect("127.0.0.1:7447".parse().unwrap()).await?;
// connect() returns after the transport dial is owned by the redial supervisor;
// authentication and immediate reconciliation continue asynchronously. The
// first authenticated peer id at this address is pinned for later reconnects.

node.put("key".to_string(), b"value".to_vec()).await?;
node.put_with_ttl("temp".to_string(), b"value".to_vec(), Some(Duration::from_secs(60))).await?;
node.delete("key".to_string()).await?;

let value = node.get("key".to_string()).await?;  // routed read, Option<VersionedValue>
let all = node.get_all().await?;                  // full local copy (live keys)
let health = node.health().await?;                // per-fragment supervision health

// Bulk APIs: keys hash to fragments, then exact-byte/count-bounded chunks per
// remote fragment/owner attempt. Per-key Results remain in input order.
let puts = node.put_multi(vec![
    ("k1".to_string(), b"v1".to_vec()),
    ("k2".to_string(), b"v2".to_vec()),
]).await;
let gets = node.get_multi(vec!["k1".to_string(), "k2".to_string()]).await;
let peers = node.peer_count().await;

// Local-only: prompt even when a replica is unavailable.
node.shutdown().await?;

// Alternatively, choose the bounded peer fence before local shutdown:
// let report = node.shutdown_with_replication_drain(
//     ReplicationDrainOptions::builder().timeout(Duration::from_secs(30)).build(),
// ).await?;
```

`Node::start` uses the default transport and replication liveness policy. Use
`Node::start_with_options` when those runtime-only limits need tuning:

```rust
use actordb::node::NodeRuntimeOptions;
use std::time::Duration;

let runtime = NodeRuntimeOptions::builder()
    .peer_queue_frame_capacity(512)
    .peer_queue_byte_capacity(64 * 1024 * 1024)
    .replication_control_message_capacity(128)
    .inbound_connection_capacity(512)
    .receive_byte_capacity(128 * 1024 * 1024)
    .queue_admission_timeout(Duration::from_secs(2))
    .reconcile_data_admission_timeout(Duration::from_secs(20))
    .forwarded_attempt_timeout(Duration::from_secs(8))
    .connect_timeout(Duration::from_secs(3))
    .handshake_timeout(Duration::from_secs(3))
    .socket_read_timeout(Duration::from_secs(20))
    .socket_write_timeout(Duration::from_secs(20))
    .replication_ack_timeout(Duration::from_secs(20))
    .build(); // Omitted relay, read, mutation, and health settings retain defaults.

let node = Node::start_with_options(
    "node-a".to_string(),
    "0.0.0.0:7447".parse().unwrap(),
    NodeConfig::builder().build(),
    runtime,
)
.await?;
```

All configuration and actor argument types provide Bon builders with defaults
for optional tuning and compile-time checks for required dependencies. Existing
struct literals, `Default`, `with_*` helpers, and getters remain available.
`build()` constructs settings; startup/spawn still validates their values.
See the [builder reference](docs/configuration.md#configuration-builders) for
nested pool defaults, optional setters, and the 13 supported input types.

The defaults are a 256-frame queue per peer, 64 protocol-control messages per
fragment/peer sender, at most 1024 concurrent inbound handshakes or
connections, and a node-wide 64 MiB raw receive-body budget. Accepted and
outbound-dialed connections reserve body bytes before allocation, including
handshake frames; saturation closes only the contributing connection. Permits
remain attached through decoding and the handler turn. This counts encoded
bodies, not protobuf/actor/cache copies or total process memory. Queue
admission, a complete forwarded owner attempt, one dial,
and the absolute two-flight handshake each have 5-second deadlines. Reading a
frame body after its length prefix and writing a complete frame each have a
30-second deadline; an idle established connection has no read deadline until
it sends a prefix. Streamed reconciliation-data pages have a separate
30-second queue-admission deadline so a legal near-limit value can wait behind
another slow near-limit frame; an acceptable ordinary replication ACK is due
within 30 seconds. Locally applied mutations have a separate per-sender ingress lane
sized by `SenderConfig::buffer_capacity`; saturation schedules lossless reset
reconciliation without blocking the write path. Reconciliation barriers use
`SenderConfig::reconcile_timeout` instead of the ordinary ACK timeout.
Anti-entropy and ordinary replication frames are built against their exact
encoded size, targeting 1 MiB pages and never exceeding the 16 MiB hard frame
limit. The peer writer also enforces a 32 MiB default encoded-byte budget in
addition to its frame-count limit.

Multi-key semantics:

- `put_multi` / `get_multi` partition keys by fragment and send one
  or more exact-byte-bounded `BatchPut` / `BatchGet` requests per remote
  fragment and owner attempt, rather than one message per key. Large get
  responses advance as connection-correlated ordered prefixes; omitted suffixes are
  requested under fresh ids without losing duplicate positions.
- Local fan-out is capped at 1024 concurrent calls before dispatcher
  admission. A remote `BatchGet` uses one aggregate actor continuation but
  admits and sticky-routes only its next ordered key, stopping by exact encoded
  result bytes before an omitted suffix is reserved or cloned.
- Local and remote puts await the serving fragment's exact per-key apply
  outcome. Remote batches preserve request order/cardinality and use the
  configured per-attempt deadline (5 seconds by default), including outbound
  queueing and the response.
- Remotely routed single-key and batch operations fail over across replica
  holders in placement order on transport, timeout, or malformed-response
  failures. Every owner attempt has one configured budget (5 seconds by
  default) covering queue admission and the response. Typed `Unavailable`
  positions try the next holder with their original stamped mutations, even
  after other positions in the batch finish. Other valid per-key outcomes are
  returned directly and are excluded from subsequent retries.
- Errors are per-key and classified: `NodeError::Actor` for
  misconfiguration (no replica listed for a fragment) or local actor-framework
  failure,
  `NodeError::Network` for connectivity failures, plus exact
  `Backpressure`, `Storage`, `Superseded`, and `Expired` mutation outcomes.
- `put_multi_with_priority` / `get_multi_with_priority` set the
  operation priority explicitly (default: `Background`); the priority
  is carried on the wire and honored by the remote owner.

Key `NodeConfig` knobs:

| Field | Default | Meaning |
|---|---|---|
| `fragment_count` | 1 | Fragments in the keyspace, at most `u32::MAX` (must agree cluster-wide and with an existing RocksDB manifest) |
| `fragment_map` | {} | Static placement: fragment → replica node ids (empty = serve all); non-empty maps must cover every in-range fragment with a non-empty, duplicate-free replica list |
| `members` | {} | Static membership: node id → unique address; each configured id is enforced from the first authentication attempt |
| `default_pool` / `high_pool` / `background_pool` | see below | Per-priority `PoolConfig { workers, queue_cap, rate_limit_per_sec }` |
| `high_pool` defaults | 2 workers / cap 5_000 / no limit | Latency-sensitive traffic |
| `background_pool` defaults | 2 workers / cap 5_000 / 1000 ops/s | Bulk/scan traffic (rate-limited) |
| `flush_batch_size` / `flush_interval` | 1000 / 100ms | Persistence batching |
| `clean_interval` | 30s | TTL-expiry tombstone materialization cadence |
| `snapshot_interval` | 60s | Periodic durable flush |
| `replication` | defaults | `SenderConfig`: batching, in-flight/buffer bounds, reconcile cadence/timeout, required ACK durability |
| `placement_controller` | None | Phase 8: `Some(Arc<dyn PlacementController>)` opts into dynamic placement |
| `placement_poll_interval` | 1s | Phase 8: how often the driver re-evaluates desired placement |
| `placement_drain_warn_interval` | 10s | Warning cadence for a safe, non-destructive stalled drain |
| `store` | None | `Some(Arc<dyn KvStore>)`; `NodeConfig::with_rocksdb(path)` uses `SyncWal`, while `with_rocksdb_options` selects explicit RocksDB policy |

The local node id, every `members` key, and every placement replica id must
contain 1 to 255 UTF-8 bytes. Placement replica lists must be duplicate-free.
RocksDB permanently binds its first Node id and fragment count in a separate
metadata column family and rejects mismatches before network bind. Non-empty
pre-manifest databases are rejected without mutating their column-family layout
and require explicit one-time legacy adoption; see
[Persistence](docs/persistence.md).

- Local and remote writes await the serving worker's exact apply outcome;
  after `Ok`, that owner reads the version or a newer winner. Replication to
  the owner's other holders remains asynchronous.
- Conflicts resolve with last-writer-wins on HLC timestamps,
  tie-broken by `node_id`; tombstones win exact ties.
- Failures surface as typed `NodeError` variants, including `Backpressure`,
  `Storage`, `Superseded`, and `Expired` rather than false success.

### `actordb::actor` — actors and the storage core

- `KvActor` — public per-fragment dispatcher; its managed sticky-hash worker
  pools are internal; `FragmentState` — the shared cache/buffer/store. Direct
  embedders send `KvMessage` to `KvActor` rather than splitting admission from
  a raw worker route.
- `VersionedValue { value, timestamp_ms, node_id, expires_at_ms, is_tombstone }`
  — a value plus its LWW version; `wins_over` implements the conflict
  rule, `is_expired` the TTL check.
- `HlcClock` — hybrid logical clock behind all timestamps.
- `ApplyOutcome::{Applied, AlreadyPresent, Superseded, Expired}` — the exact
  result used instead of inferring write success from a later read.
- `ApplyError::{Backpressure, InvalidInput, Storage}` distinguishes a nonfatal
  unencodable direct actor request from persistence failure; invalid input does
  not restart the fragment worker.
- `BatchReadEntry`, `BatchReadPage`, `SnapshotSummary`, `SummaryPage`, and
  `VersionedPage` support priority-aware ordered batch reads plus pinned-time,
  revision-aware anti-entropy reads without cloning whole fragments or
  arbitrary fetch chunks; existing `GetAll` and `GetSnapshot` remain
  available for compatibility and debugging.
- `KvMessage::{ApplyOp, Flush, FlushStable, Get, GetBatch, GetAll, GetSnapshot,
  GetSummaryPage, GetVersioned, GetVersionedPage, GetRevision, GetShedCounts}`
  — for embedders driving the dispatcher directly instead of through `Node`.
  `KvMessage` is non-exhaustive; downstream matches must retain a wildcard arm
  as internal routing/query variants evolve.
  `FlushStable` requests the backend's explicit stable fence and reports
  `ApplyDurability::StableCommitted` only when that fence succeeds.

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
- `queue_cap` covers work accepted into that priority's workers (active plus
  queued), not the shared dispatcher mailbox or replicated ingress. Accepted
  reads are handed to their worker without making the dispatcher await them,
  so a blocked background worker does not block high-priority admission,
  health, or supervision.
- Priority is preserved end-to-end: forwarded single-key operations and batch
  messages carry the originator's priority on the wire, and the owner
  applies/serves them in the matching pool. Priority selects that remote actor
  pool; it does not prioritize the per-peer socket's ordinary data lane.
- Shed counts are surfaced per pool `(high, default, background)` via
  `node.health()` → `FragmentHealth.shed_counts`.

Ordering honesty: local and forwarded puts wait for the serving worker's exact
apply outcome. At that point its shared cache contains the applied version or
an identical replay; by return time a newer concurrent LWW winner may exist.
The configured remote deadline (5 seconds by default) covers both bounded
outbound-queue admission and the reply; cancellation and late replies remove
or ignore pending state.

### Supervisor and maintenance actors

These types live under `actordb::actor`. `actordb::supervision` reexports the
same types for compatibility, including their messages, state, and Bon builders.

- `FragmentSupervisor` — owns a fragment's dispatcher, `Cleaner`, and
  `Snapshotter`; restarts dead children in place.
- `FragmentHealth` — per-fragment health snapshot (worker count/names,
  restart counts, per-pool `shed_counts`).
- `Cleaner` — materializes expired values as durable TTL markers.
- `Snapshotter` — periodically flushes the fragment's retained write buffer.

### `actordb::replication` — async sender/receiver

- Per fragment, per replica peer, a sender task batches mutations (writes +
  tombstones) off the write path: ships on a timer, bounded in-flight,
  and retains each batch until a typed ACK reaches the configured durability
  fence. Receiver apply/flush failures NACK and requeue the exact batch.
- An ordinary ACK has a 30-second default deadline. Expiry rotates the sender
  session and replays all unacknowledged, already-stamped operations in their
  original order; LWW makes duplicates idempotent, and ACKs from the old
  session are ignored. An ACK below the requested durability neither retires
  the batch nor extends its deadline.
- Session ids per pairing; stale batches discarded on reconnect.
- Buffer overflow → `buffer_reset`: shed, rotate the session, and reconcile
  via timestamp/tombstone comparison immediately or when the peer returns.
- Periodic anti-entropy repairs residual divergence in both static and dynamic
  placement. Both sides stream ordered, byte-bounded summary pages at a pinned
  TTL time; revision drift or any repair forces a fresh stable verification
  pass. Fetch requests contain up to 256 exact-byte-bounded keys; the responder
  reads values incrementally and streams byte-bounded data pages through its
  bounded writer. Repair and ordinary replication batches use the same exact
  protobuf sizing: 1 MiB soft pages and 16 MiB hard frames. Reconcile rounds are counted per sender
  (`SenderStateReport::reconcile_rounds`).

### `actordb::placement` — dynamic placement controller (Phase 8)

Opt-in via `NodeConfig::placement_controller`. With no controller the
node keeps static placement and behaves exactly as before.

```rust
use actordb::placement::ManualPlacement;
use std::sync::Arc;

let controller = Arc::new(ManualPlacement::new(initial_map));
let config = NodeConfig::builder()
    .placement_controller(controller.clone())
    .placement_poll_interval(Duration::from_millis(500))
    .build();
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
  a **streamed bootstrap** from every connected source candidate (on-demand
  reconcile with bounded fetch chunks and value pages, 30s timeout); only an
  already-serving holder may answer, and timeout leaves the new holder
  non-authoritative. Revoked fragments drain their replication senders, then
  stop.
- Dynamic startup synchronously loads assigned durable subtrees for validation
  but leaves them `Bootstrapping` until an already-serving replica completes a
  verified reconcile round. For a brand-new cluster, seed each fragment on one
  node first, wait for `Serving`, and only then publish overlap replicas;
  simultaneously starting an all-new replica set remains non-authoritative by
  design because no node can prove it has the latest state.
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
- Static and dynamic placement use the same correlated reconciliation engine.
  `SyncRequest`/`SyncResponse` are retired in protocol v3, and
  `node.sync_entries_applied()` is a compatibility counter that remains zero.
- Assumes the peer mesh is established (`members` or manual
  `connect()`); the driver does not dial.

### `actordb::proto` — hidden wire compatibility module

Prost-generated protobuf types (`Envelope`, `Hello`, `HelloAuth`, `ReplicateOp`,
`ForwardedPut`, `ForwardedGet`, `GetResponse`, `ReconcileRequest`,
`ReconcileResponse`, `ReconcileFetch`, `ReconcileData`, `SyncEntry`, `BatchPut`,
`BatchGet`, `BatchGetResponse`, `MutationResponse`, `BatchAck`, `BatchNack`),
sent as 4-byte length-prefixed frames over TCP. `SyncRequest` and `SyncResponse`
remain generated for source compatibility but are retired; a negotiated v3
peer that sends either is rejected. These bindings are hidden from generated
`actordb` documentation and retained only for protocol and integration-test
compatibility. They are not a supported application API. For
language-independent wire bindings, depend directly on the
[`actordb-protocol`](actordb-protocol/README.md) crate; ordinary applications
should use the high-level node and client APIs.

### Current limitations

This is a working prototype, not production-ready. Known gaps, tracked
as issues in the [issue tracker](https://github.com/slawlor/actordb/issues):

- **Persistence is opt-in** — pass a `store` in `NodeConfig` for
  durability; without it the dataset lives in memory and a restart
  wipes it (recoverable only via peer reconciliation).
- **No built-in TLS or individual peer identity** — plaintext TCP. Protocol v3
  gives each physical connection two fresh 256-bit challenges. With a
  `NodeConfig::cluster_secret` containing at least 32 independently generated,
  high-entropy random bytes, both peers prove fresh possession by
  HMAC-SHA256 over the ordered dialer/listener transcript, roles, ids, version
  ranges, capabilities, and challenges. This prevents replay of a captured v3
  handshake, but a shared secret proves cluster membership, not which member is
  speaking: any holder can claim any node id. A guessable password can also be
  tested offline against a captured transcript. The proof provides no
  confidentiality or integrity for application frames after the handshake. Use
  a protected, mutually authenticated custom `Network` when those properties
  matter.
- **Anti-entropy is correlated reconciliation** — static placement, dynamic
  placement, late joins, reconnect recovery, buffer resets, and periodic repair
  all use the same session/round-correlated reconciliation engine. In legacy
  map-empty mode, per-peer senders are reaped on disconnect instead of retaining
  an unbounded set of discovered identities; mandatory immediate reconciliation
  after reconnect recovers downtime writes and tombstones. Senders named by an
  explicit placement map remain allocated while disconnected and retain their
  bounded replay/reset state.
- **Transport queues have count and byte bounds** — the shared per-peer
  writer pipeline defaults to 256 frames and 32 MiB of admitted encoded data;
  a single accepted frame can approach 16 MiB. A producer first reserves the
  data-lane ingress and its exact encoded-byte permits, then allocates the
  frame; the owned permit travels with the frame through the socket write and
  releases on every drop path. Queue admission and socket writes have bounded
  waits; expiry cancels the connection so the dialer can reconnect. The frame
  budget is split between data and a writer-priority response lane, and a full
  16 MiB maximum response is reserved from ordinary producers. Responses produced
  by the sole connection reader use nonblocking admission and cancel on
  saturation, so the reader never waits behind its own writer. After at most
  16 consecutive responses, a ready data frame is written to prevent starvation.
  Non-serving read/mutation relays separately share node-wide limits of 1024
  live attempts/timers and 64 MiB of retained request-key storage (String/Vec
  storage and buffer capacities, not mutation values or total heap). Admission
  never waits; rejected mutations return `Unavailable`, and legacy reads stay
  unavailable without false misses. Exact physical origin/owner identities
  prevent stale disconnect cleanup from removing replacement work.
  Each Node sender
  has a bounded mutation lane and a separate bounded protocol-control lane;
  mutation overflow advances a persistent reset generation, while control
  overflow closes the contributing connection for replay. Sender actor lanes
  remain operation/message-count bounds. Protocol builders separately enforce
  exact encoded sizes: anti-entropy, ordinary replication, and aggregate
  batches target 1 MiB pages and no frame may exceed 16 MiB. Mutation batches
  also reserve their worst-case complete replies; get responses use ordered
  prefix pulls. An irreducible oversized record or a record with empty or
  over-limit provenance is rejected before local apply; a legacy persisted
  record violating either invariant fails node startup instead of becoming
  permanently unreplicable. Before Prost decoding,
  an allocation-free scan caps anti-entropy pages at 256 repeated items and
  general batch fields at 4096, preventing empty-message decode amplification.
  One ticketed FIFO credit schedules anti-entropy rounds fairly across every
  fragment on a physical connection. Receiver actor mailbox byte accounting remains follow-up
  work. Inbound handlers are capped at 1024
  by default, the two-flight handshake has one absolute 5-second deadline, and
  completed Node-owned tasks are reaped while the node runs.
- **Fan-out is sequential** — remote fragment groups and their replica-holder
  attempts are currently awaited one at a time. Operation priority chooses the
  destination actor pool but does not schedule the ordinary socket data lane.
- **Wire upgrades are negotiated but coordinated** — protocol-v3 `Hello`
  advertises an inclusive version range, required capability bits, and a fresh
  challenge; `HelloAuth` completes the transcript proof. Missing or
  incompatible fields fail closed before peer installation. The current binary
  supports only wire v3 and requires the paged-anti-entropy,
  byte-bounded-batches, and stable-durability-ACK capabilities. Byte-bounded
  batches change batch-get responses to ordered-prefix semantics, while stable
  durability ACKs are required before RocksDB may retire a handed-off prefix;
  binaries missing either contract are rejected. It is
  incompatible with v2, so deploy compatible binaries together rather than
  performing a mixed v2/v3 rolling upgrade. This transition is also a Rust
  source break for callers of the hidden generated `actordb::proto` compatibility API
  and the public `replication::SenderMsg`: reconciliation messages gained
  cursor, revision, page, and completion fields, so struct literals and
  exhaustive assumptions must be updated.
- **Shutdown is explicit and fallible** — `Node::shutdown().await` atomically
  closes public-operation admission (`NodeError::ShuttingDown` for later
  calls), joins listener/connection/dial work, waits for already-admitted API
  work, drains the bounded serving forwarded-mutation completion owner without
  aborting accepted apply/outcome/fanout, stops replication senders, drains every fragment mailbox, and reports
  the final local-store flush. A flush error retains the quiesced fragment and
  its batch; retry `shutdown` until it succeeds. The node does not resume after
  an error. This local fence does not wait for replication ACKs.
  `shutdown_with_replication_drain` is the opt-in bounded alternative: it
  freezes placement, waits for admitted mutations, and requires a stable full
  reconciliation plus the configured ACK durability from every frozen target
  before entering the same local shutdown. Timeout/cancellation leaves the
  quiesced node and transports alive for retry; ordinary `shutdown` explicitly
  abandons the peer guarantee. `RetryableSender` requests a fresh exact sender
  fence, while `Unavailable` means local-only shutdown has already made the
  guarantee impossible. Its report proves a per-target fenced LWW version
  floor, not exact/global cache equality, quorum, or a backend-independent
  fsync guarantee. Offered RocksDB defaults to `SyncWal`; explicit `AsyncWal`
  is weaker for `StoreCommitted`, while `StableCommitted` forces the explicit
  WAL fence. Legacy empty-map
  mode covers only senders known at the fence. Dropping `Node` only requests
  best-effort cancellation and stop; it cannot report a flush failure, await
  a join, or safely release the same live RocksDB attachment. This pre-release change is a Rust source break:
  callers must handle the new shutdown `Result`, and exhaustive matches over
  `NodeError`, `ReplicationAckDurability`, `ApplyDurability`, `KvMessage`, the
  generated `ReplicationAckLevel`, or the advanced `SupervisorMessage` API
  must include their new variants (`StableCommitted` and `FlushStable` where
  applicable).
- **Tombstones are retained** — obsolete time-based collection settings have
  been removed. Safe reclamation needs durable
  per-replica convergence evidence; elapsed wall-clock time cannot prove that
  condition. Whole retired fragment namespaces are a separate lifecycle case.
- **Revoked RocksDB prefixes retire after a stable revision-pinned handoff** — placement
  seals every replacement sender, requires every current replica to complete a
  fresh full reconciliation and `StableCommitted` barrier at one unchanged
  source revision, then quiesces and flushes the source before RocksDB commits
  a synchronous idempotent range tombstone. A weak ACK, replica-set/generation
  change, fetched winner, deletion error, or re-add fails closed in `Draining`
  or retains the namespace. Custom stores retain prefixes unless they
  explicitly opt into lifecycle deletion. RocksDB compaction owns later
  physical space reclamation. The certificate covers LWW winners and their TTL
  metadata; independent expiry cleanup may make later byte snapshots differ.
- **Custom worker-pool supervision is not lossless** — a worker crash drops
  accepted jobs in that worker's mailbox and closes their reply ports; those
  jobs are not replayed. Exact per-worker reservations reclaim their capacity,
  and orderly shutdown drains accepted work. A restarted dispatcher is
  republished through a generation-checked fragment route, but migration to
  ractor's `Factory` lifecycle remains tracked for discard policy and lossless
  restart. The formerly public split `WorkerPool` admission/routing surface was
  removed in this pre-release API: direct embedders should use `KvActor`.

## Contributors

To learn more about contributing to `actordb` please see [CONTRIBUTING.md](https://github.com/slawlor/actordb/blob/main/CONTRIBUTING.md).

## License

This project is licensed under [MIT](https://github.com/slawlor/actordb/blob/main/LICENSE).
