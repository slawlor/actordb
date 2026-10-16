# Release notes

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

See the [API guide](docs/api.md), [persistence guide](docs/persistence.md),
[replication guide](docs/replication.md), and
[consistency model](docs/consistency.md) for the actual guarantees and migration
requirements. The [hardening backlog](https://github.com/slawlor/actordb/issues/49)
still includes factory routing, cache architecture benchmarks, ordered runtime
execution, and module cleanup. Transport-specific TLS remains application-owned.
