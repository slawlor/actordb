# actordb

A rust actor-based distributed database based on *RocksDb*

* [<img alt="github" src="https://img.shields.io/badge/github-slawlor/actordb-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/slawlor/actordb)
* [<img alt="crates.io" src="https://img.shields.io/crates/v/actordb.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/actordb)
* [<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-actordb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/actordb)
* [![CI/main](https://github.com/slawlor/actordb/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/slawlor/actordb/actions/workflows/ci.yaml)
* [![codecov](https://codecov.io/gh/slawlor/actordb/branch/main/graph/badge.svg)](https://codecov.io/gh/slawlor/actordb)

* `actordb`: ![actordb Downloads](https://img.shields.io/crates/d/actordb.svg)

## Working prototype

The crate now contains a working, tested prototype of the actor-first
distributed database vision:

* **`actor`**: a `KvActor` (built on [ractor](https://github.com/slawlor/ractor))
  owns each node's *local copy* of the keyspace. The actor's message
  handler is the only code that mutates that copy — no locks, no shared
  mutable state.
* **`node`**: a `Node` pairs the actor with a Tokio TCP mesh. Local writes
  are applied to the actor and gossiped to peers with **epidemic
  broadcast** (each node forwards each op once, tracked by
  `(origin node_id, seq)`); inbound writes merge with
  **last-writer-wins** on `(timestamp_ms, node_id)`, which is idempotent
  and commutative, so replicas converge without coordination. A joining
  node pulls full state via a handshake sync (anti-entropy).
* **`proto`**: the wire protocol — prost-encoded `Envelope` frames
  (`Hello`, `ReplicateOp`, `SyncRequest`, `SyncResponse`),
  length-delimited over TCP.

Run the tests (7 cluster integration tests over real localhost TCP, plus
a unit test for the LWW ordering):

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

Three modules:

### `actordb::node` — the main entry point

`Node` pairs a `KvActor` with the Tokio TCP replication mesh. This is
what most users interact with:

```rust
use actordb::node::{Node, NodeConfig};

let node = Node::start(
    "node-a".to_string(),
    "0.0.0.0:7447".parse().unwrap(),
    NodeConfig::default(),
)
.await?;
// port 0 picks an ephemeral port; the bound address is available via `node.addr()`
// NodeConfig { db_path: Some(path) } enables RocksDB write-through persistence

node.connect("127.0.0.1:7447".parse().unwrap()).await?;
// handshake runs a full-state sync, so a joining node converges immediately.
// The dialer side owns the connection and redials with backoff if it drops.

node.put("key".to_string(), b"value".to_vec()).await?;
let value = node.get("key".to_string()).await?;  // local-only read, Option<VersionedValue>
let all = node.get_all().await?;                  // full local copy
let peers = node.peer_count().await;

node.shutdown().await;
```

- Writes are applied to the local actor and gossiped to peers with
  epidemic broadcast; reads never touch the network.
- Conflicts resolve with last-writer-wins on `(timestamp_ms, node_id)`.
- Failures surface as `NodeError::{Io, Codec, Actor}`.

### `actordb::actor` — the storage core

- `KvActor` — the [ractor](https://github.com/slawlor/ractor) actor
  owning one node's local copy. Its message handler is the only code
  that mutates state: no locks, no shared mutability.
- `VersionedValue { value, timestamp_ms, node_id }` — a value plus its
  LWW version; `wins_over` implements the conflict rule.
- `KvMessage::{ApplyOp, Get, GetAll}` — for embedders driving the actor
  directly instead of through `Node`.

### `actordb::proto` — wire protocol

Prost-generated protobuf types (`Envelope`, `Hello`, `ReplicateOp`,
`SyncRequest`, `SyncResponse`), sent as 4-byte length-prefixed frames
over TCP. Mostly of interest if you're implementing a compatible peer.

### Current limitations

This is a working prototype, not production-ready. Known gaps, tracked
as issues in the [issue tracker](https://github.com/slawlor/actordb/issues):

- **Persistence is opt-in** — pass `db_path` in `NodeConfig` for
  RocksDB write-through persistence; without it the dataset lives in
  memory and a restart wipes it (recoverable only via peer sync).
- **No TLS/authentication** — plaintext, unauthenticated TCP.
- **No deletes** — keys live forever.
- **Wall-clock LWW** — no hybrid logical clocks yet; skewed clocks can
  lose writes.
- **Silent drops under extreme load** — the per-peer queue is bounded
  and overflow is currently silent.
- **Full-state anti-entropy** — every handshake ships the whole dataset.

## TODO List

1. Make generic input(recv)/output(send) service definitions.
2. Leave network-level protocols up to implementer (gRPC, raw TCP, Thrift etc doesn't matter)
3. Test lib can implement a basic TCP version without auth/TLS etc on a docker cluster for automated tests
4. Decide consensus models we want to implement and then do we (a) use an existing lib or (b) build it/them ourself?

## Contributors

To learn more about contributing to `actordb` please see [CONTRIBUTING.md](https://github.com/slawlor/actordb/blob/main/CONTRIBUTING.md).

## License

This project is licensed under [MIT](https://github.com/slawlor/actordb/blob/main/LICENSE).
