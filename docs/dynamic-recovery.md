# Full-outage recovery with dynamic placement

Normal dynamic startup requires a surviving **serving** replica. After every
replica of a fragment stops, restored copies stay non-authoritative. They do
not automatically elect a seed or certify each other's empty state.

Stable checkpoint export/import and validated staged identity replacement or
shrink are supported separately; see [stable-checkpoints.md](stable-checkpoints.md)
and [staged-membership.md](staged-membership.md). Those APIs preserve physical
table namespaces and historical record origins while requiring source and target
evidence before retirement. Permanent source loss requires a distinct explicit
application decision and authenticated process fences; qualifying a smaller
source set alone does not authorize removal or writer election.

`NodeConfig::recovery` provides explicit operator authority to recover that
complete replica set. It requires dynamic placement, a configured store, and
successful stable-storage fences. It adds no consensus or durable callback
delivery guarantee.

## Procedure

1. Stop ordinary application writes and verify every candidate's identity,
   database path, backup and fragment layout. Include every surviving copy
   whose version floor may matter. Restore missing copies before proceeding.
2. Keep the complete placement fixed. Choose a fresh, nonzero 32-byte recovery
   epoch for this attempt and configure it identically on recovering nodes.
   The epoch is coordination metadata, not a transport credential: keep normal
   cluster authentication and protected transport configured.
3. Start each node with its durable store, controller, and recovery options.
   Establish direct authenticated peer connections between every configured
   replica, normally through `NodeConfig::members`.
4. Wait for **every** required replica to report `Serving` through
   `placement_report` or the controller's readiness callback. A missing member,
   wrong epoch, changed replica set, conflicting record, or failed stable fence
   keeps the affected recovery non-authoritative. Repair the cause; do not
   remove a candidate merely to make a timeout disappear.
5. Resume application writes and ordinary placement changes only after all
   candidates are serving. Remove recovery options during a subsequent rolling
   restart, preserving a surviving serving replica. Do not reuse the epoch for
   another outage.

```rust
use actordb::node::{NodeConfig, RecoveryOptions};

// Supply a fresh operator-managed value and the store/controller separately.
let authority = RecoveryOptions::new([42; 32])?;
let config = NodeConfig::builder().recovery(authority).build();
# Ok::<(), Box<dyn std::error::Error>>(())
```

## What completion proves

Non-serving candidates expose bounded summary/value pages only to an
authenticated member of the captured replica set presenting the matching epoch
and replica-set digest. Existing session, round, cursor, key, actor-generation
and mutation fences still apply. Repairs preserve last-writer-wins versions and
tombstones.

A recovering node waits for a new stable reconciliation round with **all**
candidates. If incoming repairs change its revision, it repeats verification.
It then closes mutation admission, verifies that the exact actor and sender set
still certify one unchanged local revision, and commits its local storage fence.
The mutation fence stays held through publication of `Serving`.

This recovers the union of the durable candidate version floors observed by
those rounds. It cannot recover writes lost from every candidate, establish a
distributed snapshot transaction, resolve conflicting payloads with identical
versions, or prove quorum commitment for expiry callbacks. A backend's stable
fence remains its explicit durability contract. Safe authority after permanent
replica loss needs a separate recovery decision backed by external evidence.
