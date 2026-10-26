# Fresh-deployment support validation

ActorDB `0.9.0` has one current Rust/wire/storage contract. Deploy fresh stores
and matching participants; no compatibility aliases, old-format import/adoption
or version/capability negotiation is supported.

## Required acceptance coverage

- Real persistence blocking/failure, cancellation-safe fences, exact retry,
  admission races, queued/cancelled point/batch/SQL/scan work and response
  ownership, repeated shutdown and backend release.
- Typed node/table/fragment/peer/pool/worker observations, optional timing,
  bounded callbacks, explicit freshness, reset metadata and actual omissions.
- Owned startup, established Serving during background gain, recovery blockers
  and atomically published shutdown progress.
- Exact current protocol checks, optional shared-secret challenge proof,
  operational generation fences, decoded old work, reconnect and transport TLS.
  Credential revocation remains external; no independent process keys exist.
- Fresh RocksDB format 3, checkpoint ACTCP003 and membership ACTMEM04 only;
  non-current data rejected without mutation; bounded stable snapshot/import,
  atomic cursors, partial transfer/restart and native replacement/shrink.
- Actual memory/store/stable evidence, frozen replica identity, submitted LWW
  floor, strict caps, failed fences, uncertain outcomes and retained execution.

## Delivery gates

- Backend-free, memory-only and all-feature workspace suites.
- Rust 1.88/current Clippy, protocol regeneration agreement and runnable examples.
- A documentation-wide current-version/API audit and release guard tests.
- Verified package artifacts and all ten repository CI check families,
  including Docker, coverage, docs and benchmarks.
- PRs merged only after their exact head passes checks.

These APIs do not provide consensus, a hosting allocator or writer leases.
External authority and permanent source-loss decisions remain explicit
application decisions, distinct from actual SDK work and durable floor evidence.
