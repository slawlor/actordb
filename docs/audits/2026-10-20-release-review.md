# Current deployment review: ActorDB 0.9.0

This document tracks the current fresh-deployment safety boundary. Historical
probe patches and earlier review results are available in Git history; they
are not instructions for deploying the current code.

## Current contract

- Exact peer protocol 6 and application protocol 4; no capability/range
  negotiation, retired full-sync message or public wire compatibility alias.
- RocksDB record/manifest format 3, checkpoint ACTCP003 and membership ACTMEM04.
  Non-current data is rejected without adoption or automatic conversion.
- Canonical actor APIs, bounded standalone replication ingress and opaque
  connection IDs. No dummy identity channel, split worker admission API,
  no-op tombstone collector or permanent-zero health counter.
- TLS trust is externally provisioned; optional shared-secret proof authenticates
  possession. Generation barriers are operational controls, not proof of
  credential revocation or host death. No independent process signing layer.

## Execution and cancellation

Accepted work retains request, actor, response and persistence ownership through
actual completion after waiter cancellation. A completed application drain is
distinct from joined receive/apply shutdown and remote replication evidence.
Store failures preserve retry state; shutdown releases the Node's backend only
after the relevant work joins.

Regression coverage includes blocked/failing persistence, queued/cancelled
point/batch/SQL/scan operations, forwarded and application response delivery,
decoded replication/reconciliation, stale physical connection cleanup and
repeated shutdown. Credential Debug output is redacted.

## Data and recovery

Full LWW versions include physical/logical time, writer identity and the
tombstone tie-breaker. Equal version tuples cannot name conflicting content.
Tombstones and expiry are preserved through replication/checkpoints; elapsed
time cannot authorize garbage collection.

Checkpoint transfer uses stable immutable bounded pages, authenticated chunks
and atomic durable import cursors. Interrupted imports block ordinary use until
exact retry completes. Native membership workflows verify actual receiver
policy/drain reports and retained target floors, not external writer authority.
Rejection tests verify non-current source files remain unchanged.

## Resource and supervision limits

- Ingress, ordinary buffering, protocol control, wire bytes and local query
  results have their own count/byte budgets. These are not one total heap limit.
- Bounded sender saturation can request visible snapshot recovery for an
  already-applied delta; an `Ok` send is not remote delivery.
- Worker crashes can abandon accepted mailbox work. Exact ownership reclamation
  and orderly drain do not imply lossless replay across arbitrary actor failure.
- The actor cache and explicit local debug dumps are not dataset-size-bounded.
  Run workload-specific memory/latency benchmarks; do not infer production
  percentiles from finite test samples.
- Report only observed queue age, timing and progress. No unimplemented remote
  lag or dataset-percentage field is exposed.

## Delivery validation

Require backend-free, memory-only and all-feature tests, Rust 1.88 and current
Clippy, formatting, schema regeneration, release guards, package artifacts,
examples and all repository Docker/coverage/docs/benchmark checks before merge.

See [deployment](../support-api-migration.md), [replication](../replication.md),
[persistence](../persistence.md), [testing](../testing.md) and
[operations](../operations.md).
