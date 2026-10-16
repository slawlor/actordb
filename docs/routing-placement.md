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
`fragment_map`. The map must be **identical on every node**. After the
protocol-v3 handshake, static mode uses the same correlated, byte-bounded
reconciliation engine as dynamic placement so a late joiner or restarted node
catches up (see [Replication](replication.md)).

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
The map must cover exactly the in-range fragments `0..fragment_count`, with a
non-empty, duplicate-free replica list for each. Every replica node id must
contain 1 to 255 UTF-8 bytes, matching the handshake limit; the local node id
and configured member ids have the same byte-length constraint.

Two implementations ship with the crate:

- **`StaticPlacement`** — wraps a fixed map; exists so the dynamic code path
  shares validation logic. Nodes without an explicit controller keep static
  routing/lifecycle while sharing the protocol-v3 reconciliation path.
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

1. Mark the fragment `Bootstrapping` synchronously so the next tick cannot
   double-spawn. A runtime gain spawns its supervision subtree; dynamic startup
   instead adopts the subtree that `Node::start` synchronously preloaded for
   durable-record and HLC validation. Startup never publishes that preload as
   authoritative before reconciliation (a spawn failure clears the mark so the
   next tick retries).
2. Make the dispatcher routable immediately so the reconciler can merge into
   it — but the fragment is **not serving** yet, so local ops forward to the
   previous owner instead of being applied to possibly-stale state. If the
   previous owner observed the overlap first, this node retains its in-range
   replication session before the dispatcher exists; early tracked batches
   NACK and replay instead of losing the one-time `SessionStart`.
3. Ensure replication senders to each replica peer.
4. `bootstrap_from`: for up to `BOOTSTRAP_TIMEOUT` (30 s), trigger on-demand
   reconciliation (`ReconcileNow`) against every connected source candidate
   and wait for any completed-round counter to advance. A bootstrapping or
   draining candidate refuses to answer authoritative summary/fetch requests,
   preventing simultaneously added replicas from certifying each other's empty
   state. Bootstrap transfers a **streamed delta reconcile** (one
   requester-driven key fetch at a time), not a monolithic snapshot. Any repair
   or revision drift is followed by a fresh mutation-free verification pass
   before the counter can advance. On timeout the fragment remains
   non-authoritative and is retried; it never goes `Serving` with unverified
   state. Before publication, the driver verifies that both the placement
   generation and complete replica list still match the snapshot used for
   sender alignment and bootstrap. If placement changed — including a
   `P -> Q -> P` change back to the same list — the fragment remains
   non-authoritative and repeats these steps; if this node was removed, it
   transitions to draining.
5. Mark `Serving`, add to the serving set, call
   `controller.on_fragment_ready(fragment)` so an in-process controller can
   sequence the next step (revoking the old owner).

Cold-cluster genesis must seed one owner per fragment and wait for `Serving`
before adding overlap replicas. Simultaneously starting every member of a new
multi-replica placement leaves them `Bootstrapping` by design: no
already-serving source exists, and bootstrapping nodes cannot safely certify
one another's empty state.

**Losing a fragment** (`lose_fragment`):

1. Take the fragment's exclusive mutation-admission lease, wait for mutations
   that already chose this owner to finish apply plus replication fan-out, then
   remove the fragment from the serving set. New mutations acquire their lease
   afterward, observe revocation, and forward to the new owner. State becomes
   `Draining`.
2. A store which leaves `prefix_delete_support` at `Retained` follows the
   compatibility path: ensure current senders, wait for bounded ingress,
   overflow recovery, buffers, and in-flight batches to drain, then retain the
   namespace. This preserves existing custom-store behavior.
3. A store which opts into `Supported` deletion takes the stronger path. The
   driver snapshots the exact replacement replica vector and placement
   generation, seals every corresponding sender, and requires a fresh stable
   full reconciliation plus `StableCommitted` barrier from every target. All
   certificates must cover one unchanged source revision; a fetched winner or
   sender replacement restarts the complete set. Queue emptiness alone never
   authorizes deletion.
4. Under the placement commit lock and exclusive mutation lease, revalidate the
   exact vector, generation, sender tasks, and registered supervisor. Install a
   per-fragment finalization seal and atomically detach the exact sender/report
   set, then release the node-global placement and sender registries before
   joining those tasks or re-reading the actor revision. Reacquire the commit
   lock and revalidate the placement generation plus the still-empty sealed set
   before detaching the router. An ABA/target change or revision timeout
   retries; a pre-detach re-add drops the seal, installs fresh senders,
   reconciles, and restores `Serving`.
5. After detach, quiesce periodic actors, drain dispatcher/worker mailboxes,
   and require the final local flush before touching storage. Revalidate the
   certified placement, then invoke idempotent fragment-prefix deletion while
   placement publication is excluded. An error remains `Draining` and retries;
   `Deleted` or the backend's terminal `Retained` outcome permits supervisor
   stop and `Stopped`. A post-detach re-add waits for that outcome and starts a
   fresh subtree. RocksDB uses a synchronous range tombstone; compaction later
   reclaims physical space.

The controller sequencing rule is therefore mandatory for data transfer:
add the destination during an overlap, wait for its `on_fragment_ready`, and
only then revoke the source. Directly replacing the sole source with an absent
or unbootstrapped owner is not a migration certificate. A deletion-capable
source remains `Draining` until every published replacement proves the stable
handoff.

**Overlap-window safety.** While peers' maps transiently disagree, a node that
receives an op for a fragment it no longer serves **re-forwards** it
hop-limited (`MAX_FORWARD_HOPS = 4`, never bounced back to the sender, never
to self) instead of dropping it. Mutation relays mint a collision-free local
request id, validate the downstream peer, restore the upstream id, and return
`Unavailable` on no route, hop exhaustion, send failure, or the 5-second relay
deadline. The Phase 5 reconciler owns anti-entropy in both dynamic and static
mode. The retired `SyncResponse` compatibility counter,
`sync_entries_applied()`, remains zero.

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
[API guide](api.md)). Peer joins and disconnects wake the driver through a
coalescing signal. The next wake calls `on_membership_change` with the current
peer-id set, so a burst may produce one callback rather than one callback per
edge; controllers must treat it as a current-state notification, not an ordered
membership event log. Periodic placement polling still continues independently.

## Placement boundary during node drain

`shutdown_with_replication_drain` first stops and joins the placement driver
and any owned gain/loss transition before it snapshots peer targets. This
freezes the router/sender topology while transport and reconnect supervisors
remain alive. For a non-empty placement map, every configured remote replica
of each fragment still present in the local router becomes a required target;
the coordinator creates or replaces a missing/stopped sender from the
authoritative cache before issuing its private drain generation. A sender task
replacement cannot satisfy an older generation.

Legacy empty-map mode has no configured closed-world membership. Its report
therefore covers only fragment/peer senders already installed when the fence is
taken, which may be an empty set. Deployments that need a complete target
certificate should use explicit placement. The placement freeze is one-way:
after timeout or cancellation the node rejects new operations and retains the
frozen state for retry; ordinary `shutdown` abandons the remote requirement and
finishes local-only.

This is a local lifecycle freeze; it does not write a new desired map to an
external `PlacementController`. For a rolling restart, drain one replica at a
time and wait for it to restart and return to `Serving` before advancing to the
next replica. For permanent decommissioning, first move its fragments through
the placement controller and wait for each migration/revoke to finish; the
subsequent node shutdown may correctly report no remote targets because the
placement handoff already removed the local actors. The drain API does not
coordinate simultaneous maintenance or prove that a quorum remains online.

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
