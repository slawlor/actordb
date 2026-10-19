# Release notes

## 0.6.0

- Upgrade the optional protobuf generator dependency to `protobuf-src` 2.1.
  Code generation requires CMake and a C/C++ toolchain.
- Add caller-supplied application transports with `Client::from_stream`,
  `ClientServer::bind_with_network`, and `ClientServer::from_acceptor`, preserving
  plain TCP defaults and the existing wire protocols. Flush complete peer and
  application frames for buffered streams; include a caller-configured mTLS
  example and transport lifecycle/conformance tests.
- Move all four public crates and internal dependency minimums to compatible
  `0.6.0` ranges. See `docs/transports.md` for the new application extension points.

## 0.5.0

- Add per-write `Local`, `Count(n)`, and `Majority` acknowledgement policies
  to Node and Rust client single-key put/delete APIs, including TTL/priority.
- Return explicit acknowledgement receipts and partial progress on timeout;
  updates continue through the normal asynchronous replication path.
- Count only validated, distinct processing ACKs for the stamped update and
  captured replica set. Keep local acknowledgement as the ordinary default.
- Isolate acknowledgement waits from forwarded apply workers and keep peer
  readers available for ACKs during forwarded writes.
- Add per-write requirements for a processed acknowledgement from another
  configured region, with validated region maps and frozen replica eligibility.
  Preserve the region requirement when the numeric threshold is one and report
  region progress in success and timeout receipts. Reject incomplete region
  configuration before apply and missing or malformed required region progress
  in routed and SDK receipts.
- Add explicit per-node acknowledgement caps with validated startup
  configuration and runtime set/clear controls. Receipts report requested and
  effective thresholds; existing plans retain their captured thresholds.
  Validate requested counts before overrides and preserve region requirements
  independently of numeric caps.
- Reject contradictory operation/receipt replies and preserve unknown outcomes
  after lost forwarded responses, including subsequent pre-apply rejection.
- Extend generated public wire structs with optional concern fields, require
  peer capability support, and move all four public crate versions and inter-crate
  minimums to compatible 0.5.0 ranges.

Write concern policies, region requirements, and operational caps are included
in this single release. See `docs/write-concern.md`,
`docs/write-concern-regions.md`, and `docs/write-concern-overrides.md` for support,
semantics, and migration.

## 0.4.0

- Add a Rust companion SDK and a separate application listener, with point and
  batch operations, TTL, priorities, scans, queries, and read-only inspection.
- Add bounded SQL queries over the virtual `kv` table: key and fragment filters,
  global row pagination, byte projections, and exact integer aggregates.
  Joins, subqueries, multiple statements, and grouping remain unsupported.
- Share generated protocol bindings through `actordb-protocol`; ordinary library
  builds no longer compile protobuf tooling. Negotiate peer scan and query
  capabilities explicitly.
- Keep mutation retries explicit: a lost application response can have an
  unknown outcome; the SDK does not replay a mutation with a fresh version.

## 0.3.0

- Stabilize tests around asynchronous worker health, relay/ACK deadlines, and TTL expiry;
  expiry during admission remains an explicit supported outcome, not a failure
  of scan visibility.
- Fence supervised restarts by actor identity, bound the complete integration
  polling operation, and replace a shared-runner latency threshold with a
  correctness check that completes both bulk-write and high-priority workloads.
- Remove duplicate native RocksDB package builds and tests from publishing.
  Normal CI retains backend-free, memory-only, and all-feature validation; release
  validation retains backend-free and memory-only package checks and tests.
- Update installation examples and release instructions for the 0.3 series.

- Add `Node::scan` for distributed key filtering and value projections, with
  background-pool admission, per-fragment bounds, replica failover, and explicit
  failure rather than silent partial results. Scans use per-fragment views,
  not a globally isolated snapshot; see the [scan guide](docs/scanning.md).
- Add scan request/response messages to the peer protocol and deterministic
  local scan ordering. Upgrade peers together when adopting the scan API.
- Use the protobuf-src 1.1 release series for protobuf generation.

## 0.2.0

The next pre-1.0 release replaces the original prototype with the current
actor-first, sharded database implementation. This is a breaking upgrade from
`0.1.0`, not a declaration of production readiness.

- Per-fragment actor supervision, local and forwarded mutation outcomes,
  batched routing, and bounded request, response, relay, and replication work.
- Protocol-v3 peer compatibility checks, authenticated identities, paginated
  reconciliation, placement epochs, bootstrap-before-serving, and explicit
  replication/durability acknowledgements. Do not mix incompatible protocol
  versions in a live mesh.
- Versioned TTL deletion and retained tombstones, checked wall-clock deadlines,
  restart-safe logical clocks, serialized persistence, and cancellation-aware
  drain and shutdown. `Node::shutdown` now returns a result; callers must
  handle persistence failures instead of treating shutdown as infallible.
- The officially supported RocksDB backend has isolated `store::rocksdb`
  implementation modules, configurable WAL durability, prefix isolation, and
  fail-closed identity/layout manifests. Legacy stores require deliberate
  adoption; review the persistence guide and back up data before upgrading.
- Bounded truthful health probes, coalesced automatic maintenance, and Bon
  builders for public configuration and actor arguments. Required identities
  and shared dependencies remain required; runtime validation remains at
  startup.
- Backend-free default crate with independent opt-in `rocksdb` and `memory`
  features. `MemoryStore` is a no-op test double, not a database; custom
  `KvStore` implementations and storeless nodes need neither feature.
- Rust 1.88 minimum support, backend-free/memory/full-feature CI, package
  verification, and version/tag-checked crates.io release automation.
- Actor-per-file runtime organization under `actor`, with `supervision`
  compatibility reexports preserving public imports and Bon
  builder paths; shared cache, clock, protocol, pool, and maintenance helpers
  have separate modules. Unit tests remain inline in the owning source files;
  real-node integration tests stay separate. Redundant generated-builder and
  alias/default smoke tests are omitted; regressions focus on runtime behavior
  and fail-closed validation rather than coverage percentages.
- Consolidated `getrandom` 0.4, `hmac` 0.13, `sha2` 0.11, and `tracing-glog` 0.4
  upgrades, current compatible dependency patches, and grouped future Dependabot
  updates. OS challenge generation and constant-time handshake verification
  retain the existing protocol, pinned by independent crypto test vectors.
  The internal xtask uses Clap 4 without the obsolete `atty` dependency, while
  preserving its existing commands and coverage flags.
- No committed dependency lockfile or frozen-resolution CI/release commands.
  Fresh resolution prefers dependencies compatible with each crate's declared
  Rust minimum; Cargo-generated local and packaged lockfiles remain normal.

See the [API guide](docs/api.md), [persistence guide](docs/persistence.md),
[replication guide](docs/replication.md), and
[consistency model](docs/consistency.md) for the actual guarantees and migration
requirements. The [hardening backlog](https://github.com/slawlor/actordb/issues/49)
still includes factory routing, cache architecture benchmarks, ordered runtime
execution, and node/replication module cleanup. Transport-specific TLS remains
application-owned.
