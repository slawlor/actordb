# Operations

## Failure scenarios

### Node crash (`kill -9`) and restart

1. The unflushed write-buffer tail is lost on the crashed node. Batch size and
   active-write latency are controlled by `flush_batch_size` / `flush_interval`;
   an idle fragment is flushed on `snapshot_interval`.
2. On restart with the same `node_id`, the node re-establishes sessions (new
   `session_id`; receivers discard the old session's batches) and the Phase 5
   reconciler repairs the divergence — in static mode the full-state
   `SyncRequest` handshake also runs; with the placement driver active only
   delta reconciliation runs (`sync_entries_applied` stays 0).
3. The Docker `SIGKILL + restart reconvergence` scenario covers exactly this:
   a node is SIGKILLed mid-write, restarted, and must reconverge with its
   peers. A redial supervisor in the node layer re-establishes dropped
   connections with exponential backoff from 100 ms.

Memory-only caveat: tombstones GC'd from memory before the crash are
re-loaded from RocksDB on boot (GC is memory-only — see
[Tombstones](tombstones.md)).

Startup also seeds the node HLC from the highest durable version. A persisted
timestamp farther in the future than `max_clock_skew` fails startup rather
than allowing that version to freeze its key; investigate clock rollback or
poisoned data before widening the bound.

### Rolling restart with placement

With a `PlacementController` configured, a rolling restart is the migration
gain path: the restarted node (same `node_id`) re-polls the controller, spawns
its fragment subtrees, and bootstraps each fragment via an on-demand
reconcile round from a surviving replica — chunked, bounded by
`BOOTSTRAP_TIMEOUT` (30 s), availability-over-consistency on timeout. Surviving
replicas keep serving throughout. Covered by
`placement_rolling_restart_delta_reconcile_only`, which asserts the restarted
node converges to the full dataset with zero full-state sync entries.

### Network partition

- Both sides keep accepting writes; `BufferReset` senders shed and reconcile
  on heal.
- Concurrent writes to the same key resolve by HLC timestamp on heal — later
  wins, silently.
- **Tombstone resurrection risk**: if a partition outlasts `tombstone_gc_age`,
  the GC'd tombstone is gone and the other side's pre-delete value wins on
  heal. Size `tombstone_gc_age` above your longest expected partition.
- While partitioned, `replica_order` failover only helps if a replica is
  reachable; otherwise reads fail with `NodeError::Network`.

### Overloaded node (backpressure)

Shed operations are counted per pool in `FragmentHealth.shed_counts`
(high, default, background). Sustained shedding on `High` means the pool is
undersized — add workers or raise the cap. Sustained shedding on `Background`
under bulk load is the system working as designed; callers get
`NodeError::Backpressure` and should retry with backoff. Never treat a shed
read as "not found" — the API makes that impossible by construction.

### Fragment migration gone wrong

The driver's fail-safes, in order: `validate_placement` rejects maps that drop
a fragment's replicas or leave a fragment uncovered (keeps the current
placement); a failed subtree spawn clears the `Bootstrapping` mark so the next
tick retries; bootstrap timeout goes Serving anyway; drain timeout stops the
subtree anyway. During map disagreement, ops re-forward hop-limited
(`MAX_FORWARD_HOPS = 4`) instead of being dropped. If a controller publishes a
bad map, `placement_report()` shows the authoritative map and per-fragment
states (`Bootstrapping`/`Serving`/`Draining`/`Stopped`) for diagnosis.

## What to monitor

| Signal | Source | What it tells you |
|---|---|---|
| Per-fragment health | `Node::health()` | Worker counts/names, per-child restart counts, `last_reaped`, `shed_counts` |
| Shed counters | `health()[frag].shed_counts` | `(high, default, background)` — backpressure pressure per traffic class |
| Restarts | `health()[frag].restarts` | Repeated worker/dispatcher restarts = crash loop; investigate before it cascades |
| Placement | `Node::placement_report()` | Authoritative map, lifecycle states, `dynamic` flag — is migration stuck in `Bootstrapping`/`Draining`? |
| Sync entries | `Node::sync_entries_applied()` | Full-state sync volume since start; nonzero in dynamic mode = reconciler isn't owning anti-entropy |
| Replication lag | `batch_interval` + peer RTT | Steady-state lag bound; divergence beyond that shows up via reconcile |

Operational rules of thumb:

- `fragment_count` and `fragment_map` must be identical on every node in
  static mode — a mismatch silently makes keys unreachable.
- Keep at least two replicas per fragment if you care about the unflushed
  write tail surviving `kill -9`.
- `tombstone_gc_age` > longest expected partition, always.
- The background pool's default 1000 ops/s rate limit is a feature, not a bug
  — raise it deliberately if your bulk path needs more.
