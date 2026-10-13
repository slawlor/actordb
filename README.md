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

Run the tests (5 cluster integration tests over real localhost TCP, plus
a unit test for the LWW ordering):

```bash
cargo test
```

Run the three-node demo (writes from each node, prints every node's
converged local copy):

```bash
cargo run --example three_node_demo
```

## TODO List

1. Make generic input(recv)/output(send) service definitions.
2. Leave network-level protocols up to implementer (gRPC, raw TCP, Thrift etc doesn't matter)
3. Test lib can implement a basic TCP version without auth/TLS etc on a docker cluster for automated tests
4. Decide consensus models we want to implement and then do we (a) use an existing lib or (b) build it/them ourself?

## Contributors

To learn more about contributing to `actordb` please see [CONTRIBUTING.md](https://github.com/slawlor/actordb/blob/main/CONTRIBUTING.md).

## License

This project is licensed under [MIT](https://github.com/slawlor/actordb/blob/main/LICENSE).
