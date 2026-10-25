# Full support API completion

The first stage merged in PRs #137 and #138. The requested scope is all six
support feature contracts; those initial merges were not full completion.
Release target: **0.9.0**, due to public protocol/actor schema additions and
configured persistence becoming the default request-drain boundary.

## Active work and integration ownership

- Root branch `feat/complete-support-090`: #130 durable draining/acceptance
  coverage, #131 complete metrics, #132 lifecycle/status, release integration.
- Peer agent: #133, `/tmp/actordb-peer-fencing.j3YzgV`, branch
  `feat/peer-epoch-fencing-090`. Ed25519 process signing credentials and
  public verifier registry; hot change waits retained old frame/storage leases.
- Checkpoint agent: #134, `/tmp/actordb-checkpoints.oQk2dY`, branch
  `feat/stable-checkpoints-090`. Frozen bounded store snapshot and durable
  resumable import metadata; exact source/target/authority transition receipts.
- Evidence agent: #135, `/tmp/actordb-durability-evidence.JTLKuN`, branch
  `feat/durability-evidence-090`. Actual local and remote durability evidence,
  frozen replica sets, certified mutation floor and timeout/failure receipts.

Agents commit isolated changes; root integrates and resolves shared Node,
protocol, replication and storage changes before publishing PRs. All published
manifests and internal minimums remain synchronized at 0.9.0.

## Remaining acceptance gates

- [ ] Local drain waits real persistence, retained cancellation/retry tickets,
  queued/read/forwarded/table/client coverage and concurrent/repeated shutdown.
- [ ] Bounded table/fragment/peer instrumentation, actual request/storage/wait
  timing, errors, concern outcomes and outstanding-work consistency.
- [ ] Starting/recovering/serving/draining/stopping/stopped/blocked/failed view,
  component placement progress, last successful ACK/reconcile and freshness.
- [ ] Old authenticated process cannot mutate clocks/data after a completed
  hot fence; queued apply, reconnect and historical provenance regressions.
- [ ] Stable checkpoint bounded export/import, hash/cursor validation, restart
  and interruption ownership; staged shrink/replacement and qualified sources.
- [ ] Actual durability plus replica-set and operation/version-floor evidence,
  buffered/stable failures, timeout, supersession and captured membership.
- [ ] Full feature matrix, Rust 1.88/current lint, generated protocol agreement,
  package validation, examples and Docker scenarios.
- [ ] PRs merge after all ten repository check families succeed. Repository
  auto-merge is disabled; existing branch policy requires admin squash only
  after all checks pass. User has explicitly authorized these merges.
- [ ] Close each issue only when its requested contract and acceptance are met;
  update tracking #136 and sync main. No further partial-wave stopping point.

Core remains independent of hosting/discovery/metrics backends. These APIs do
not introduce consensus, writer leases, host-death proofs or membership policy.
Unknown progress/evidence remains unknown; it is never silently zero-filled.
