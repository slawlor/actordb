# Support completion gates

The full six-contract scope follows the initial PRs #137 and #138. Release
target **0.9.0** accounts for public protocol/actor changes and the configured
local persistence drain default. Three isolated implementation streams supplied
peer security, checkpoint/membership and write certificates; integration also
completed lifecycle, scoped observations and durable draining.

## Implemented acceptance coverage

- [x] Real persistence blocking and failure, cancellation-safe fence tickets,
  exact retry, SDK admission races, queued/cancelled reads/writes, forwarded and
  client response ownership, concurrent/repeated shutdown and store binding.
- [x] Typed node/table/fragment/peer observations, bounded detailed pool/worker
  metrics, actual storage/concern/request timing and errors, explicit resets,
  freshness, omissions and callback safety.
- [x] Owned startup, initial readiness publication, healthy Serving during
  background gain, missing-peer/recovery blockers and live shutdown stages.
- [x] Ed25519 process identity plus per-frame session/direction/sequence integrity,
  never-reused historical keys, decoded old work, reconnect/tamper/replay and
  historical provenance.
- [x] Immutable stable checkpoint paging, authenticated bounded chunks and durable
  cursor, torn append recovery, partial transfer/import restart, ordinary-store
  exclusion and guarded native replacement/shrink.
- [x] Actual memory/store/stable evidence, frozen replica identity, certified
  operation floor, cap rejection, buffering/fence failures, uncertain outcomes,
  supersession and retained execution after caller deadlines.

## Required delivery gates

- Backend-free, memory-only and all-feature workspace suites.
- Rust 1.88/current Clippy, protocol regeneration agreement and examples.
- Verified package artifacts and all repository CI/Docker checks.
- Complete PRs merged only after all ten check families succeed.
- Component issues/tracking updated and the workspace synchronized to main.

Core APIs remain independent of hosting/discovery/monitoring backends. No
consensus, membership allocator or writer lease is claimed. External authority
and permanently unavailable source decisions remain explicit application
decisions, distinct from native SDK evidence.
