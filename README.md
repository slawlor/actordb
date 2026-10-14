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
  per-key ordering while different keys execute concurrently. Workers
  share the fragment's `FragmentState`: one in-memory cache, one
  persistence buffer, one `KvStore`.
* **`supervision`**: every fragment runs under a `FragmentSupervisor`
  owning the dispatcher pool plus background actors — a `Cleaner`
  (TTL expiry, tombstone aging) and a `Snapshotter` (periodic durable
  flush). Dead workers restart in place; dead children restart without
  touching sibling fragments. `Node::health()` reports per-fragment
  health.
* **`node`**: a `Node` pairs the per-fragment supervisors with a Tokio
  TCP mesh. Static placement (`fragment_map`) with caller-side one-hop
  routing: a node either applies a write locally and fans out to
  replicas, or forwards it to a fragment owner. Inbound ops merge with
  **last-writer-wins** on hybrid-logical-clock timestamps
  (tie-broken by `node_id`), which is idempotent and commutative, so
  replicas converge without coordination. Deletes are tombstones that
  propagate like writes. A joining node pulls state via handshake sync
  (anti-entropy).
* **`store`**: pluggable durable storage. In-memory by default; enable
  the `rocksdb` feature for RocksDB (per-fragment key namespacing via
  `PrefixedStore`).
* **`proto`**: the wire protocol — prost-encoded `Envelope` frames,
  length-delimited over TCP.

Run the tests (17 cluster integration tests over real localhost TCP,
plus 9 unit tests):

```bash
cargo test
```

Run the three-node demo (writes from each node, prints every node's
converged local copy):

```bash
cargo run --example three_node_demo
```

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
  reconverge. Exercises the redial supervisor, handshake sync, and
  `boot_id` dedup across process death.

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

let value = node.get("key".to_string()).await?;  // local-only read, Option<VersionedValue>
let all = node.get_all().await?;                  // full local copy (live keys)
let health = node.health().await?;                // per-fragment supervision health
let peers = node.peer_count().await;

node.shutdown().await;
```

Key `NodeConfig` knobs:

| Field | Default | Meaning |
|---|---|---|
| `fragment_count` | 1 | Fragments in the keyspace (must agree cluster-wide) |
| `fragment_map` | {} | Static placement: fragment → replica node ids (empty = serve all) |
| `members` | {} | Static membership: node id → address |
| `worker_count` | 4 | Worker actors per fragment |
| `flush_batch_size` / `flush_interval` | 1000 / 100ms | Persistence batching |
| `clean_interval` | 30s | TTL/tombstone sweep period |
| `snapshot_interval` | 60s | Periodic durable flush |
| `tombstone_gc_age` | 1h | Tombstone retention (must exceed longest expected partition) |
| `store` | None | `Some(Arc<dyn KvStore>)`; `NodeConfig::with_rocksdb(path)` with the `rocksdb` feature |

- Local writes are read-your-write; remote-fragment writes forward in
  one hop to a replica holder.
- Conflicts resolve with last-writer-wins on HLC timestamps,
  tie-broken by `node_id`; tombstones win exact ties.
- Failures surface as `NodeError::{Io, Codec, Actor}`.

### `actordb::actor` — the storage core

- `KvActor` — per-fragment dispatcher; `WorkerPool` — sticky-hash
  worker pool; `FragmentState` — the shared cache/buffer/store.
- `VersionedValue { value, timestamp_ms, node_id, expires_at_ms, is_tombstone }`
  — a value plus its LWW version; `wins_over` implements the conflict
  rule, `is_expired` the TTL check.
- `HlcClock` — hybrid logical clock behind all timestamps.
- `KvMessage::{ApplyOp, Get, GetAll}` — for embedders driving the
  dispatcher directly instead of through `Node`.

### `actordb::supervision` — per-fragment supervision

- `FragmentSupervisor` — owns a fragment's dispatcher, `Cleaner`, and
  `Snapshotter`; restarts dead children in place.
- `FragmentHealth` — per-fragment health snapshot (worker count/names,
  restart counts).

### `actordb::proto` — wire protocol

Prost-generated protobuf types (`Envelope`, `Hello`, `ReplicateOp`,
`ForwardedPut`, `ForwardedGet`, `GetResponse`, `SyncRequest`,
`SyncResponse`, `SyncEntry`), sent as 4-byte length-prefixed frames
over TCP. Mostly of interest if you're implementing a compatible peer.

### Current limitations

This is a working prototype, not production-ready. Known gaps, tracked
as issues in the [issue tracker](https://github.com/slawlor/actordb/issues):

- **Persistence is opt-in** — pass a `store` in `NodeConfig` for
  durability; without it the dataset lives in memory and a restart
  wipes it (recoverable only via peer sync).
- **No TLS/authentication** — plaintext, unauthenticated TCP.
- **Synchronous replication** — writes fan out on the write path;
  async sender/receiver with batching and acks is Phase 5 (#32).
- **Full-state anti-entropy** — every handshake ships the whole
  dataset (tombstones included).
- **Tombstone GC is time-based** — a node partitioned longer than
  `tombstone_gc_age` can resurrect deleted keys on rejoin.

## Contributors

To learn more about contributing to `actordb` please see [CONTRIBUTING.md](https://github.com/slawlor/actordb/blob/main/CONTRIBUTING.md).

## License

This project is licensed under [MIT](https://github.com/slawlor/actordb/blob/main/LICENSE).
