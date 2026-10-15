# Routing & placement

## Key → fragment → owner

Routing is three deterministic steps, computed locally on every node:

1. **Key → fragment**: `fragment_of(key, fragment_count)` =
   `seahash(key) % fragment_count`. The hash is **seahash** — chosen because it
   is deterministic across processes, machines, and restarts (unlike `std`'s
   per-process-seeded hasher). `fragment_count` **must be identical on every
   node**, or keys become unreachable. It panics on `fragment_count == 0`.
2. **Fragment → replicas**: looked up in the placement map —
   `HashMap<FragmentId, Vec<String>>` mapping each fragment to its replica node
   ids. In static mode this is `NodeConfig.fragment_map` (empty = this node
   serves all fragments, i.e. single-node / legacy full replication).
3. **Replica preference**: `replica_order(frag)` — this node first *if it
   currently serves the fragment*, then the other replicas in map order.

There is **no consistent hashing** anywhere in the codebase — no hash ring, no
rendezvous hashing, no virtual nodes. The keyspace split is plain
`hash % N`, and fragment→node assignment is an explicit map.

Within a priority pool, a key's operations sticky-route to one worker
(`seahash(key) % workers`). A local mutation returns only after that worker
reports the exact LWW apply outcome (see [Consistency](consistency.md)).

## Static placement (default)

With no `placement_controller` configured, each node routes by its
`fragment_map`. The map must be **identical on every node**. Static mode keeps
the legacy connection handshake: on connect, each side sends a full-state
`SyncRequest`/`SyncResponse` so a late joiner or restarted node catches up
(see [Replication](replication.md)).

## The `PlacementController` trait (Phase 8)

Dynamic placement is opt-in: set `NodeConfig.placement_controller` to an
`Arc<dyn PlacementController>`. The trait is intentionally small:

```rust
pub trait PlacementController: Debug + Send + Sync {
    /// The source of truth, polled by the node's placement driver.
    fn desired_placement(&self) -> HashMap<FragmentId, Vec<String>>;
    /// Called when the node's observed peer set changes (default: no-op).
    fn on_membership_change(&self, _members: &[String]) {}
    /// Called when this node finishes bootstrapping a newly assigned
    /// fragment and starts serving it (default: no-op).
    fn on_fragment_ready(&self, _fragment: FragmentId) {}
}
```

A placement driver polls `desired_placement()` every `placement_poll_interval`
(default 1 s; ignored in static mode). Each poll runs `validate_placement`, and
on invalid output the driver **keeps the current placement** (fail-safe).
The map must cover every fragment `0..fragment_count` with a non-empty replica
list.

Two implementations ship with the crate:

- **`StaticPlacement`** — wraps a fixed map; exists so the dynamic code path
  shares validation logic. Nodes without an explicit controller keep the legacy
  static startup path untouched.
- **`ManualPlacement`** — an `RwLock`-guarded map with `set_placement(map)` /
  `placement()`. Publishing a new map takes effect on the driver's next poll.
  There is no `move_fragment` helper: moves are expressed as full-map
  transitions, e.g. `{F: [A]} → {F: [A, B]} → {F: [B]}`.

## Migration protocol

The documented rule: **move through an overlap state.** Never publish a
placement where a fragment has no replicas, and never remove the old owner
before the new one is serving — the driver treats the controller's map as
authoritative and tears down local state for revoked fragments.

```
1. Controller adds the new owner:  {F: [A]} -> {F: [A, B]}.
   B's driver spawns the fragment subtree and bootstraps state
   from A via the Phase 5 reconciler (chunked, bounded).
2. Controller waits until B reports F as Serving, then removes
   the old owner: {F: [A, B]} -> {F: [B]}.
   A's driver drains its replication senders, then stops the
   subtree.
```

**Gaining a fragment** (`gain_fragment`):

1. Spawn the fragment supervision subtree; mark the fragment
   `Bootstrapping` synchronously so the next tick can't double-spawn (a spawn
   failure clears the mark so the next tick retries).
2. Make the dispatcher routable immediately so the reconciler can merge into
   it — but the fragment is **not serving** yet, so local ops forward to the
   previous owner instead of being applied to possibly-stale state.
3. Ensure replication senders to each replica peer.
4. `bootstrap_from`: wait up to `BOOTSTRAP_TIMEOUT` (30 s) for a source peer,
   fire one on-demand reconcile round (`ReconcileNow`), and wait for the
   sender's completed-round counter to advance. Bootstrap transfers a
   **chunked delta reconcile** (500 keys per `ReconcileFetch` chunk), not a
   monolithic snapshot. If it fetches remote winners, their barrier is followed
   by a fresh correlated no-fetch verification round before the counter can
   advance. On timeout the fragment goes Serving anyway —
   **availability over consistency**; LWW plus periodic reconcile converge it
   later.
5. Mark `Serving`, add to the serving set, call
   `controller.on_fragment_ready(fragment)` so an in-process controller can
   sequence the next step (revoking the old owner).

**Losing a fragment** (`lose_fragment`):

1. Take the fragment's exclusive mutation-admission lease, wait for mutations
   that already chose this owner to finish apply plus replication fan-out, then
   remove the fragment from the serving set. New mutations acquire their lease
   afterward, observe revocation, and forward to the new owner. State becomes
   `Draining`.
2. Ensure senders to the current replicas so queued ops drain out.
3. Wait until every bounded sender ingress lane is empty, every issued overflow
   generation has completed correlated reset reconciliation, and every sender
   report shows `buffered == 0 && in_flight == 0` outside `BufferReset`. There
   is no destructive timeout: warnings repeat at
   `placement_drain_warn_interval`, and a placement reversal restores
   `Serving` instead of allowing stale teardown.
4. Under the placement commit lock, revalidate the authoritative map and the
   exact sender/report set, take the exclusive mutation lease (including
   inbound replication), synchronously request sender stop, and detach the
   router before any teardown wait. Then await sender exit and stop the
   supervisor subtree (the dispatcher flushes). A re-add before this commit
   restores the existing subtree; one after it waits for `Stopped` and performs
   a clean bootstrap. State becomes `Stopped`.

**Overlap-window safety.** While peers' maps transiently disagree, a node that
receives an op for a fragment it no longer serves **re-forwards** it
hop-limited (`MAX_FORWARD_HOPS = 4`, never bounced back to the sender, never
to self) instead of dropping it. Mutation relays mint a collision-free local
request id, validate the downstream peer, restore the upstream id, and return
`Unavailable` on no route, hop exhaustion, send failure, or the 5-second relay
deadline. In dynamic mode the full-state handshake sync is skipped — the Phase
5 reconciler owns anti-entropy, so the rolling-restart test asserts
`sync_entries_applied() == 0`.

## `placement_report()`

```rust
pub async fn placement_report(&self) -> PlacementReport;
// PlacementReport {
//   placement: HashMap<FragmentId, Vec<String>>, // this node's authoritative map
//   states: HashMap<FragmentId, FragmentPlacementState>,
//   dynamic: bool,                               // a controller is configured
// }
```

`FragmentPlacementState` is `Bootstrapping` (reconciler-owned, not
authoritative; local ops forwarded to the previous owner), `Serving`,
`Draining` (placement revoked; new ops relay to the current owner while existing
senders drain, then the subtree stops), or `Stopped`.

`Node::health()` complements it with per-fragment worker health (see
[API guide](api.md)); on peer join/disconnect the driver notifies the
controller via `on_membership_change`.

## Worked example

```rust
use actordb::placement::ManualPlacement;
use std::collections::HashMap;
use std::sync::Arc;

// Both fragments start on "a".
let controller = Arc::new(ManualPlacement::new(HashMap::from([
    (0.into(), vec!["a".into()]),
    (1.into(), vec!["a".into()]),
])));
// ... start nodes a and b with placement_controller: Some(controller.clone()) ...

// 1. Overlap: add "b" as a second owner of fragment 0.
controller.set_placement(HashMap::from([
    (0.into(), vec!["a".into(), "b".into()]),
    (1.into(), vec!["a".into()]),
]));
// 2. Wait until b.placement_report().await.states[&frag0] == Serving.
// 3. Revoke: remove "a".
controller.set_placement(HashMap::from([
    (0.into(), vec!["b".into()]),
    (1.into(), vec!["a".into()]),
]));
// "a" stops serving new fragment-0 writes, relays them to "b", drains its
// existing senders, then stops the subtree.
```

This exact sequence is covered by the `placement_migration_no_lost_writes`
integration test (single writer running throughout the migration asserts zero
lost writes), and the rolling-restart variant by
`placement_rolling_restart_delta_reconcile_only`.
