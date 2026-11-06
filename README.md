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

## Why choose ActorDB?

**Keep state close to your Rust service, place replicas where you need them,
and choose the completion boundary for each write.** ActorDB brings local KV
access, supervised actor execution, convergent replication, optional RocksDB
persistence and operational controls into one Rust integration. Your application
can use the same Node through embedded calls or the standalone SDK.

The strongest fit is a bounded dataset that fits in RAM and represents
**latest-known state**: service/session snapshots, device state, restartable
caches and derived projections. Hybrid logical clocks and last-writer-wins merging
give replicas a deterministic way to converge. Applications that need source-event
ordering must preserve that ordering before submitting updates.

### Strongest use cases and industry alternatives

| Use case | Why ActorDB is compelling | Comparable options | When to choose ActorDB; when another option fits better |
|---|---|---|---|
| **State inside a Rust service or stateful worker** | Locally served keys use in-process calls. Fragment supervision, table execution pools and shutdown ownership are part of the application's lifecycle. | Plain RocksDB, sled, SQLite; an external Redis/Valkey service | Choose ActorDB when you want local access plus peer replication and lifecycle APIs in the same integration. A local engine is simpler for one-process storage; SQLite is stronger for embedded relational transactions. |
| **Restartable caches and materialized projections** | Fast resident reads, named tables, TTL/tombstones and peer reconciliation suit data derived from an authoritative upstream source. RocksDB preserves committed local state across restart. | Redis/Valkey; PostgreSQL projection tables; application-built KV caches | Choose ActorDB when Rust integration, replica placement and recovery control justify owning the state layer. Redis/Valkey offer a broader command/data-structure ecosystem and established managed services; PostgreSQL is stronger for indexed relational projections. |
| **Latest-known entity, session or device state** | Serving replicas can accept updates and converge through LWW. Per-key ordering within a priority pool and explicit apply outcomes make execution observable. | Redis/Valkey; Cassandra/ScyllaDB; DynamoDB | Choose ActorDB for last-value KV semantics and application-controlled execution. Cassandra/ScyllaDB and DynamoDB offer mature large-scale database deployments; transactional stores are preferable when conflicting updates must be reconciled without losing either update. |
| **Region-local application state with remote copies** | Reads prefer the serving local Node, then same-region replicas. The planner targets fragment coverage in preferred regions; separate WAN batch/round limits pace outgoing replication. Writes can additionally require an outside-region ACK. | Cassandra/ScyllaDB datacenter replication; DynamoDB Global Tables; regional Redis deployments | Choose ActorDB when you want these policies as library APIs under your controller. The alternatives also support regional architectures and provide established operational ecosystems. ActorDB's regional coverage goal is soft, and reads can be stale at any replica. |
| **Foreground requests alongside bulk work** | High/Default/Background pools and table-local admission keep scheduling decisions explicit. Bounded batches preserve per-key outcomes; scans and SQL use bounded pages. | Redis/Valkey; general database servers; custom actor/cache layers | Choose ActorDB when traffic priorities and table execution belong in your service design. Pools isolate scheduling/admission; cache locks, storage and hardware remain shared. Existing database servers are preferable when their built-in workload controls meet your needs. |
| **A custom placement and replacement controller** | Portable planning, observed-load overrides, bounded replica changes, readiness-gated overlap, checkpoints and guarded membership workflows are reusable building blocks. | Plain RocksDB plus custom distribution code; FoundationDB; CockroachDB | Choose ActorDB when you intend to own placement, host allocation and writer authority. FoundationDB/CockroachDB are stronger when distributed transactions and database-managed coordination are requirements. |
| **Operational inspection of KV state** | Key/range/prefix filtering, projections, pagination and exact aggregates can inspect named tables across fragments without a separate query service. | Plain KV scans; SQLite/PostgreSQL; DuckDB | Choose ActorDB for bounded operational queries over the state you already serve. PostgreSQL/SQLite offer richer relational SQL and indexes; DuckDB is stronger for analytical workloads. ActorDB SQL currently excludes joins, grouping and secondary value indexes. |

**The reason to choose ActorDB is the combination of locality and control.**
An embedded engine supplies local storage; a database server supplies a broader
managed data service. ActorDB is especially useful to teams building the service
that owns the state, replica topology and maintenance workflow. Its advantage is
that these boundaries are programmable together. It is pre-1.0 software, so an
established database remains the stronger choice when operational maturity and
managed support outweigh that integration value.

### What persistent RocksDB adds

| Capability | What you get with RocksDB | Completion boundary |
|---|---|---|
| **Committed state survives a Node restart** | Retained winners, TTL deadlines and tombstones reload from local storage; durable clock floors prevent restarted writers from reusing older versions. | Only persisted state is recoverable from that disk. Ordinary cached puts can return while persistence is still buffered. |
| **Synchronous WAL commits by default** | Flushed batches use `SyncWal`; `AsyncWal` is an explicit alternative for workloads accepting a weaker ordinary backend boundary. | `StoreCommitted` waits for actor buffers to flush and the backend to accept the batch. `StableCommitted` additionally waits for the backend's explicit stable fence. |
| **Durable replica acknowledgements** | Write concerns can require actual store/stable evidence from eligible replicas, including a separately requested outside-region ACK. | Numeric/majority ACKs and storage durability are independent policies. Neither establishes consensus, linearizability or a distributed transaction. |
| **Stable, resumable checkpoints** | Native immutable snapshots back bounded export; integrity-checked chunks and atomic record/import-cursor batches support retryable import while preserving versions and namespaces. | Incomplete import blocks ordinary access until the exact retry completes. Checkpoints need a trusted source; integrity digests are not signatures or writer authority. |
| **Safer replacement and retention workflows** | Store identity/layout validation, stable target floors and guarded handoff help a controller sequence replacement and retire durable prefixes. | Controllers own live readiness evidence, overlap capacity and application writer authority. Disk alone does not certify that every replica has the newest value. |

RocksDB **adds durable backing to the resident cache**. ActorDB currently keeps
the live dataset in RAM even with RocksDB enabled; storage capacity is not a
substitute for memory sizing. For authoritative mutations, request the required
durability/write concern explicitly. For money movement, locks, conditional
inventory updates or multi-key invariants, use a store with the required
transaction or consensus semantics. See [persistence](docs/persistence.md),
[write evidence](docs/write-concerns.md) and [checkpoints](docs/stable-checkpoints.md).

### Tooling that makes the design practical

| Tool or feature | Why you would use it |
|---|---|
| **`actordb-plan` and portable placement library** | Turn expected records, key/value sizes, regions, QPS and query work into a proposed fragmap and per-host resource/traffic estimates. Evaluate an existing map, expose coverage gaps and load observed fragment costs. Export JSON reports or compatible TOML. |
| **Sizing and calibration probes** | Measure record/index memory, fragment/table overhead, embedded/RPC CPU, WAL fences, visibility lag and restart recovery. Load hardware-specific templates with `--profile`; generic defaults and laptop measurements are estimates, not universal host limits. |
| **Placement and membership APIs** | Publish static maps or integrate your controller, add overlap before removing replicas, and use verified readiness and stable handoff evidence for planned changes. |
| **Drain and shutdown APIs** | Close admission and retain accepted application, response and storage work through timeout/cancellation. Choose a local stable boundary and, separately, per-target replication drain evidence. |
| **Typed status and bounded metrics** | Inspect lifecycle, readiness/blockers, node/table/fragment/peer/pool/worker activity and actual WAN permit usage. Freshness and omitted observations remain explicit. |
| **Standalone SDK and replaceable RPC/peer transports** | Use reusable gRPC channels or implement `RpcClient`/`RpcService` for a framework such as Thrift. Supply TLS, credentials and ACLs through the selected adapters; the SDK does not join peer membership. |
| **Docker and release verification** | Exercise partitions, SIGKILL/restart, cluster growth/shrink and SDK scenarios. CI checks the feature matrix, Rust minimum version and dependency-ordered package publication. |

For performance context, the [M4 laptop checks](docs/benchmarks/laptop-sizing.md)
observed approximately **1.15M embedded reads/s**, **585k in-memory writes/s**,
**55k loopback gRPC requests/s** and **25k local stable writes/s** in separate
30-second phases. Those tests used 32 fragments, four runtime threads, 16 callers,
20,000 hot keys and 1 KiB values with no replica fanout. They demonstrate the
cost of the different access/durability paths; they are not a matched benchmark
against the alternatives above or a production latency guarantee.

## Current release and deployment

ActorDB `0.11.2` requires Rust **1.88+**. Deploy matching database, protocol,
query and SDK crates together. Peers retain exact protocol **6** and RocksDB
record/manifest format **3**. Application APIs migrate together to typed unary
`actordb.rpc.v1` RPC: there is no old application listener, handshake or fallback.
Current-format storage does not need conversion for this RPC change;
non-current formats still require a fresh deployment. The SDK defaults to
64 MiB per response and 256 MiB aggregate materialization reservations;
configure both allowances explicitly for larger trusted workloads.

```toml
[dependencies]
actordb = "0.11.2"
```

The default feature set enables `grpc`, but includes no storage backend. The Node uses a volatile actor cache and
provides the `KvStore` interface without enabling a bundled store. RocksDB is
an independent opt-in:

```toml
actordb = { version = "0.11.2", features = ["rocksdb"] }
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

The peer listener is not an application endpoint. The default gRPC adapter
wraps an `Arc<Node>` through `grpc::GrpcServer`; SDK calls use unary
`actordb.rpc.v1.ActorDb/Call` and never join peer membership.

```toml
actordb-client = "0.11.2"
```

The SDK supports point/batch operations, TTL, priorities, write concerns,
table views, scans/SQL and local inspection. Calls run concurrently within
request and aggregate response-memory budgets; there is no custom client
handshake, connection identity or request-ID state.

gRPC is default-enabled in both crates. Vendors disable default features and
implement `RpcClient`/`RpcService` using another framework, such as Thrift.
`NodeRpcService` and the generic SDK do not authenticate clients. TLS,
credentials and ACLs belong to the chosen RPC adapter. The gRPC implementation
accepts provisioned tonic TLS configuration and framework-native authorization
metadata/hooks. HTTP/2 channels are reusable; RPC does not mean dialing per call.

Closing the SDK never shuts down the database. Accepted mutations remain
owned through apply/fan-out; an uncertain reply is never transparently replayed.
See [RPC/transports](docs/transports.md) and [client options](docs/client-options.md).

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
- [Regional placement and workload sizing](docs/placement-planning.md),
  [portable planner/CLI](actordb-placement/README.md)
- [Replication](docs/replication.md), [consistency](docs/consistency.md)
  and [tombstones](docs/tombstones.md)
- [Regional replication concurrency](docs/regional-replication.md)
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
