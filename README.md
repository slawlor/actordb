# actordb

<p align="center">
  <img src="https://raw.githubusercontent.com/slawlor/actordb/main/docs/assets/actordb-logo-draft.svg" alt="ActorDB distributed Ferris graph" width="420">
</p>

An actor-based distributed key/value database for embedded Rust applications,
with logical tables, a standalone Rust SDK and opt-in persistence.

[CI](https://github.com/slawlor/actordb/actions/workflows/ci.yaml)
· [crates.io](https://crates.io/crates/actordb)
· [docs.rs](https://docs.rs/actordb)
· [coverage](https://codecov.io/gh/slawlor/actordb)

## Current release and deployment

ActorDB `0.9.1` requires Rust **1.88+** and a **fresh deployment**. Deploy
matching database, protocol, query and SDK crates together. Peers use exact
protocol **6**, application connections exact protocol **4**, and RocksDB
record/manifest format **3**. There is no rolling wire upgrade, old-store
adoption, compatibility API alias or automatic data conversion. Use new data
directories and handle any source-data export/re-ingestion separately.

```toml
[dependencies]
actordb = "0.9.1"
```

The default feature set is empty. The Node uses a volatile actor cache and
provides the `KvStore` interface without enabling a bundled store. RocksDB is
an independent opt-in:

```toml
actordb = { version = "0.9.1", features = ["rocksdb"] }
```

The `memory` feature exposes a no-op test store, not durable storage.
The `tls` feature provides a rustls transport configured with caller-provided
certificates and trust policy. Custom transports are supported independently.
Protocol bindings are committed Rust; normal builds require no protobuf
compiler. RocksDB builds additionally need its native dependencies and libclang.

This is pre-1.0 software. See [fresh deployment](docs/support-api-migration.md),
[release notes](CHANGELOG.md) and [publication procedure](docs/releases.md).

## Embedded usage

```rust
use actordb::node::{DrainOptions, Node, NodeConfig};

let node = Node::start(
    "node-a".to_string(),
    "127.0.0.1:0".parse()?,
    NodeConfig::builder().fragment_count(1).build(),
).await?;

let users = node.table("users")?;
users.put("alice", b"hello".to_vec()).await?;
let value = users.get("alice").await?;
assert_eq!(value.unwrap().value, b"hello");

// Admission closes synchronously; accepted work and responses remain owned.
node.begin_drain();
node.drain_requests(DrainOptions::default()).await?;
node.finish_shutdown(DrainOptions::default()).await?;
```

The default table is `KV`; named table handles share the same Node, listener and
peer connections while isolating keys, queries, caches and durable namespaces.
Equal user keys hash to the same fragment across tables.

Point operations support puts, reads, deletes, TTL and priority. Batch APIs
preserve input positions and per-key outcomes; they are not transactions.
SQL names its relation explicitly and supports bounded filters, projections,
pagination and exact aggregates over logical tables.

## What the database provides

- **Actor execution:** each fragment has a supervised dispatcher, table-local
  High/Default/Background worker pools, a TTL cleaner and a periodic snapshotter.
  Sticky worker routing preserves per-key ordering within a priority pool;
  shared-state LWW merging orders competing work across pools.
- **Placement and replication:** static replica maps and caller-controlled
  dynamic placement, bounded forwarding, tracked batch ACKs, and streamed
  session/round-correlated anti-entropy. Late joiners and restarted nodes recover
  through reconciliation.
- **Versioned records:** ordering by
  `(timestamp_ms, logical_counter, node_id, is_tombstone)`. Deletes and expiry
  markers are retained tombstones; reads hide them, while replication and
  checkpoints preserve them.
- **Durable boundaries:** configured or explicit stable local fences,
  cancellation-safe application drain, receive/apply shutdown and separate
  per-target replication-drain evidence.
- **Write concerns:** local/count/majority and region requirements, strict cap
  enforcement, actual memory/store/stable evidence, frozen replica identities
  and operation/version-floor receipts.
- **Observations:** owned startup, typed lifecycle/readiness/blockers, actual
  replication progress and bounded backend-neutral node/table/fragment/peer/
  pool/worker metrics with optional timing and explicit freshness/omissions.
- **Recovery and membership:** stable bounded checkpoints, authenticated chunks,
  resumable atomic import cursors, stable target-floor certificates and guarded
  joint/final placement workflows.

## Transport and identity

Plain TCP is useful for local testing. Real deployments should supply mutually
authenticated TLS through `Network` or the optional `TlsNetwork`. ActorDB does
not provision certificates or choose the host's trust policy.

`NodeConfig::cluster_secret` optionally supplies at least 32 high-entropy random
bytes. Both peers prove fresh possession through a challenge-bound HMAC.
There is no capability or version-range negotiation: a non-current peer is
rejected. The shared secret proves possession, not which member is speaking,
and does not encrypt or authenticate subsequent frame contents. TLS supplies
those properties.

Receiver generation allowlists and hot fences close old sessions and retain
already-accepted actor, response and store work until completion. They are
operational controls, not proof of credential revocation. A process retaining
valid TLS/shared-secret credentials can claim an allowed generation; excluding
it requires external credential revocation and application writer authority.
There are no Ed25519 process keys or signed-frame headers.

## Rust application SDK

The peer listener is not an application endpoint. Bind a
`client_server::ClientServer` around an `Arc<Node>` and connect using
`actordb-client`. Application connections never become replication members.

```toml
actordb-client = "0.9.1"
```

The SDK supports point and batch operations, TTL, priorities, write concerns,
logical table views, bounded scans/SQL and read-only local inspection. Clones
share one serialized connection. Count/byte admission and total request
deadlines are bounded.

Application credentials are independent of the peer cluster secret.
`Client::from_stream`, `ClientServer::bind_with_network` and `from_acceptor`
accept caller-authenticated TLS transports. Closing the SDK does not shut down
the database. Mutations are not automatically replayed after uncertain delivery.

## Persistence and checkpoints

Without a store, a Node restart loses its local cache. Use
`NodeConfig::with_rocksdb` or `with_rocksdb_options` for offered persistence,
or provide an explicit custom `KvStore`. RocksDB defaults to synchronous WAL
writes, validates its current identity/layout, and rejects non-current stores
without adoption or repair.

Request drain defaults to the configured local persistence contract.
`DrainDurability::Applied` waits for execution/response ownership only;
`StableCommitted` requests an explicit stable backend fence. Timeout or
cancellation leaves work owned and admission closed for retry.

Completed shutdown releases the Node's store attachment. Keep a caller-owned
backend `Arc` when exporting a checkpoint after shutdown; an expired weak
handle returns `BackendReleased`. Export uses a stable bounded snapshot.
Import verifies chunks before applying them and commits records/cursor
atomically. An incomplete import blocks ordinary access until exact retry
completes.

Checkpoint format is `ACTCP003`; staged membership journals use `ACTMEM04`.
Native certificates check actual receiver fence/drain reports and retained
stable target floors. Neither these certificates nor a Serving snapshot
establishes application writer authority.

## Public crate boundaries

- `actordb::node`: Node/table operations, runtime configuration, drain, status
  and metrics.
- `actordb::actor`: dispatcher/supervisor/maintenance APIs and shared fragment
  state. Managed worker pools remain internal.
- `actordb::replication`: bounded standalone sender construction and replication
  reports; no unbounded compatibility entry point.
- `actordb::store`, `checkpoint`, `membership`, `placement`, `network`,
  `query`, `write_concern` and `ttl_expiry`: their respective support contracts.
- `actordb-protocol`: public `actordb_protocol::meta` and application wire
  schemas, current protocol constants and portable write-evidence types.
- `actordb-query`: portable SQL/query planning without database/native
  dependencies.
- `actordb-client`: standalone application SDK.

There is no public `actordb::proto` or `actordb::supervision` compatibility
module. `NodeStatus.lifecycle` is the typed lifecycle, `drain` is the separate
ownership snapshot and `shutdown_stage` is always present. Unimplemented
percentage/lag fields and no-op tombstone collection are absent.

## Documentation

- [Architecture](docs/architecture.md) and [API](docs/api.md)
- [Configuration](docs/configuration.md), [routing/placement](docs/routing-placement.md)
  and [operations](docs/operations.md)
- [Replication](docs/replication.md), [consistency](docs/consistency.md)
  and [tombstones](docs/tombstones.md)
- [Persistence](docs/persistence.md), [checkpoints](docs/stable-checkpoints.md)
  and [staged membership](docs/staged-membership.md)
- [Draining](docs/draining.md), [status](docs/status.md), [metrics](docs/metrics.md)
  and [detailed observations](docs/observability.md)
- [Generation authority](docs/peer-authority.md) and
  [replacement safety](docs/replacement-safety.md)
- [Write concern](docs/write-concern.md), [regions](docs/write-concern-regions.md),
  [overrides](docs/write-concern-overrides.md) and [durability evidence](docs/write-concerns.md)
- [Batching](docs/batching.md), [scanning](docs/scanning.md) and [SQL](docs/query.md)
- [Rust SDK](actordb-client/README.md), [client options](docs/client-options.md)
  and [TLS/custom transports](docs/transports.md)
- [TTL callbacks](docs/ttl-expiry-callbacks.md), [testing](docs/testing.md)
  and [releases](docs/releases.md)

## Validation and examples

```bash
cargo test --workspace --exclude docker-tests
cargo test --workspace --exclude docker-tests --features actordb/memory
cargo test --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D clippy::all -D warnings
```

Examples include `three_node_demo`, `lifecycle_observations`, `peer_fence`,
`write_concern_evidence`, `checkpoint_restore` and `staged_membership`.
Native checkpoint/membership examples require `--features rocksdb` and fresh
directories. The SDK's `tls_demo` runs a caller-configured mTLS mesh and
application listener.

Docker scenarios exercise three-node replication, SIGKILL/restart
reconvergence and a separate SDK-only client across sharded nodes and ingress
restart. See [testing](docs/testing.md) for commands and CI coverage.

## Limits

ActorDB is not consensus or a transaction system. It does not elect writers,
allocate hosts, prove host death, or infer global equality from a queue/ACK.
Worker crashes can abandon accepted mailbox jobs; orderly shutdown drains them.
Backpressure is explicit, but actor queues and wire budgets are not a
process-wide heap bound. Unknown outcomes remain unknown.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MIT](LICENSE).
