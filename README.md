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
  cache with a lock-coherent ordered key index, one ordered persistence buffer,
  and one `KvStore`; cross-pool LWW merges are serialized there.
* **`supervision`**: every fragment runs under a `FragmentSupervisor`
  owning the dispatcher pool plus background actors — a `Cleaner`
  (durable TTL-expiry markers) and a `Snapshotter`
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
  propagate like writes. Joining and restarted nodes converge through
  correlated, byte-bounded reconciliation after the peer handshake.
* **`store`**: pluggable durable storage. In-memory by default; enable
  the `rocksdb` feature for RocksDB (per-fragment key namespacing via
  `PrefixedStore`).
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
  and immediate correlated reconciliation across process death.

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

// Stop admitting application work and await it before this call. shutdown()
// then joins Node-owned transport work and flushes/stops fragment actors.
node.shutdown().await;
```

`Node::start` uses the default transport and replication liveness policy. Use
`Node::start_with_options` when those runtime-only limits need tuning:

```rust
use actordb::node::NodeRuntimeOptions;
use std::time::Duration;

let runtime = NodeRuntimeOptions::default()
    .with_peer_queue_frame_capacity(512)
    .with_peer_queue_byte_capacity(64 * 1024 * 1024)
    .with_replication_control_message_capacity(128)
    .with_inbound_connection_capacity(512)
    .with_queue_admission_timeout(Duration::from_secs(2))
    .with_reconcile_data_admission_timeout(Duration::from_secs(20))
    .with_forwarded_attempt_timeout(Duration::from_secs(8))
    .with_connect_timeout(Duration::from_secs(3))
    .with_handshake_timeout(Duration::from_secs(3))
    .with_socket_read_timeout(Duration::from_secs(20))
    .with_socket_write_timeout(Duration::from_secs(20))
    .with_replication_ack_timeout(Duration::from_secs(20));

let node = Node::start_with_options(
    "node-a".to_string(),
    "0.0.0.0:7447".parse().unwrap(),
    NodeConfig::default(),
    runtime,
)
.await?;
```

The defaults are a 256-frame queue per peer, 64 protocol-control messages per
fragment/peer sender, and at most 1024 concurrent inbound handshakes or
connections. Queue admission, a complete forwarded owner attempt, one dial,
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
- Local and remote puts await the serving fragment's exact per-key apply
  outcome. Remote batches preserve request order/cardinality and use the
  configured per-attempt deadline (5 seconds by default), including outbound
  queueing and the response.
- Batch APIs fail over across replica holders in placement order on transport,
  timeout, or malformed-response failures. Every owner attempt has one
  configured budget (5 seconds by default) covering queue admission and the
  response. A typed
  all-`Unavailable` mutation response tries the next
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
| `fragment_map` | {} | Static placement: fragment → replica node ids (empty = serve all); non-empty maps must cover every in-range fragment with a non-empty, duplicate-free replica list |
| `members` | {} | Static membership: node id → unique address; each configured id is enforced from the first authentication attempt |
| `default_pool` / `high_pool` / `background_pool` | see below | Per-priority `PoolConfig { workers, queue_cap, rate_limit_per_sec }` |
| `high_pool` defaults | 2 workers / cap 5_000 / no limit | Latency-sensitive traffic |
| `background_pool` defaults | 2 workers / cap 5_000 / 1000 ops/s | Bulk/scan traffic (rate-limited) |
| `flush_batch_size` / `flush_interval` | 1000 / 100ms | Persistence batching |
| `clean_interval` | 30s | TTL-expiry tombstone materialization cadence |
| `snapshot_interval` | 60s | Periodic durable flush |
| `tombstone_gc_age` | None | Compatibility field; every `Some(age)` is rejected because safe distributed GC is not implemented |
| `replication` | defaults | `SenderConfig`: batching, in-flight/buffer bounds, reconcile cadence/timeout, required ACK durability |
| `placement_controller` | None | Phase 8: `Some(Arc<dyn PlacementController>)` opts into dynamic placement |
| `placement_poll_interval` | 1s | Phase 8: how often the driver re-evaluates desired placement |
| `placement_drain_warn_interval` | 10s | Warning cadence for a safe, non-destructive stalled drain |
| `store` | None | `Some(Arc<dyn KvStore>)`; `NodeConfig::with_rocksdb(path)` with the `rocksdb` feature |

The local node id, every `members` key, and every placement replica id must
contain 1 to 255 UTF-8 bytes. Placement replica lists must be duplicate-free.

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
- `ApplyError::{Backpressure, InvalidInput, Storage}` distinguishes a nonfatal
  unencodable direct actor request from persistence failure; invalid input does
  not restart the fragment worker.
- `SnapshotSummary`, `SummaryPage`, `VersionedPage`, and
  `KvMessage::{GetSummaryPage, GetVersioned, GetVersionedPage}` provide
  pinned-time, revision-aware anti-entropy reads without cloning whole
  fragments or arbitrary fetch chunks; existing `GetAll` and `GetSnapshot`
  remain available for compatibility and debugging.
- `KvMessage::{ApplyOp, Flush, Get, GetAll, GetSnapshot, GetSummaryPage,
  GetVersioned, GetVersionedPage, GetShedCounts}` — for embedders driving the
  dispatcher directly instead of through `Node`.

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

### `actordb::proto` — wire protocol

Prost-generated protobuf types (`Envelope`, `Hello`, `HelloAuth`, `ReplicateOp`,
`ForwardedPut`, `ForwardedGet`, `GetResponse`, `ReconcileRequest`,
`ReconcileResponse`, `ReconcileFetch`, `ReconcileData`, `SyncEntry`, `BatchPut`,
`BatchGet`, `BatchGetResponse`, `MutationResponse`, `BatchAck`, `BatchNack`),
sent as 4-byte length-prefixed frames over TCP. `SyncRequest` and `SyncResponse`
remain generated for source compatibility but are retired; a negotiated v3
peer that sends either is rejected. These types are mostly of interest when
implementing a compatible peer.

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
  supports only wire v3 and requires the paged-anti-entropy and
  byte-bounded-batches capabilities. The latter changes batch-get responses to
  ordered-prefix semantics, so binaries without it are rejected. It is
  incompatible with v2, so deploy compatible binaries together rather than
  performing a mixed v2/v3 rolling upgrade. This transition is also a Rust
  source break for advanced users of the public generated `actordb::proto` API
  and the public `replication::SenderMsg`: reconciliation messages gained
  cursor, revision, page, and completion fields, so struct literals and
  exhaustive assumptions must be updated.
- **Shutdown is explicit** — `Node::shutdown().await` closes Node-owned task
  admission, joins listener/connection/dial work, stops replication senders,
  and waits for fragment supervisors to flush and exit. Dropping `Node` only
  requests best-effort cancellation and stop; it cannot await a flush or join.
  The Node does not fence arbitrary calls already executing in other tasks, so
  callers must stop admitting operations and await them before shutdown.
- **Tombstones are retained** — `Node::start` rejects every
  `tombstone_gc_age: Some(..)` setting. Safe reclamation needs durable
  per-replica convergence evidence plus a durable store delete path; elapsed
  wall-clock time cannot prove either condition.
- **Custom worker-pool supervision is incomplete** — a worker crash can drop
  accepted mailbox work and leak admission accounting, and a restarted
  dispatcher is not yet republished through the fragment router. Migration to
  ractor's `Factory` lifecycle is tracked before production use.

## Contributors

To learn more about contributing to `actordb` please see [CONTRIBUTING.md](https://github.com/slawlor/actordb/blob/main/CONTRIBUTING.md).

## License

This project is licensed under [MIT](https://github.com/slawlor/actordb/blob/main/LICENSE).
