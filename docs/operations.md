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

With a durable store, persisted tombstones are retained across restarts.
Time-based collection is rejected at startup because elapsed time alone cannot
prove that every replica has durably observed a delete (see
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
- TTL expiry creates a tombstone with the TTL write's version, so a
  pre-expiry value on the other side cannot reappear when the partition heals.
- Tombstones are retained indefinitely. `Node::start` rejects every
  `tombstone_gc_age: Some(..)` setting: buffered delivery, `MemoryApplied`
  receiver crashes, and restart can all erase convergence evidence that a
  time bound cannot replace.
- While partitioned, batch reads and writes fail over across `replica_order`
  while single-key forwarded puts/gets currently select one owner. If no
  selected/retried replica is reachable, the operation returns
  `NodeError::Network`.

### Slow or non-reading peer

- Each connection has one shared outbound queue, 256 frames by default. Queue
  admission is capped at 5 seconds and each complete socket frame write at 30
  seconds. Either expiry cancels that exact connection; its senders retain or
  requeue outstanding work, and the configured dialer reconnects with backoff.
- Inbound connection-task admission is not yet bounded, completed task handles
  are retained until shutdown, and a socket that never sends Hello has no
  handshake deadline. Put an external connection/TLS proxy in front of an
  untrusted network until the transport-lifecycle hardening lands.
- Each fragment/peer sender also has a bounded local-mutation ingress lane
  sized by `SenderConfig::buffer_capacity` and a separate 64-message protocol
  control lane by default. Mutation-lane saturation coalesces a persistent
  reset generation and returns without blocking an already-applied client
  write. Control saturation closes the exact connection so ACK/session replay
  repairs it. Peer connect/disconnect and `ReconcileNow` are persistent
  side-band signals and cannot be hidden by either full lane.
- A forwarded owner attempt has one 5-second default budget for both enqueue
  and response. Expiry removes the pending request; batch APIs then try the next
  holder. Remote fragments and holder attempts are sequential, so a slow
  attempt delays those that follow.
- An ordinary replication batch must receive an acceptable ACK within 30
  seconds by default. Expiry rotates the sender session and replays every
  unacknowledged exact stamped operation in original order. Duplicate applies
  are safe under LWW, and stale ACKs from the old session are rejected. A weak
  ACK does not extend the deadline.
- Reconciliation summary/data/barrier waits use
  `SenderConfig::reconcile_timeout` (also 30 seconds by default), not the
  ordinary ACK deadline.

The peer queue, control-lane capacity, and timeout values are configurable
through `NodeRuntimeOptions`; mutation ingress follows
`SenderConfig::buffer_capacity`. Use
`Node::start_with_options`. Queue capacity counts frames, not bytes: frames can
approach the 16 MiB protocol limit, so 256 slots are not a fixed memory budget.
Operation priority does not reorder this shared socket queue.

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
tick retries; bootstrap timeout goes Serving anyway; an incomplete revoke
remains `Draining` and warns every `placement_drain_warn_interval` rather than
discarding recovery state. Re-adding the node cancels that stale teardown.
During map disagreement, ops re-forward hop-limited
(`MAX_FORWARD_HOPS = 4`) instead of being dropped. If a controller publishes a
bad map, `placement_report()` shows the authoritative map and per-fragment
states (`Bootstrapping`/`Serving`/`Draining`/`Stopped`) for diagnosis.

## What to monitor

| Signal | Source | What it tells you |
|---|---|---|
| Per-fragment health | `Node::health()` | Worker counts/names, per-child restart counts, `shed_counts` |
| Shed counters | `health()[frag].shed_counts` | `(high, default, background)` — backpressure pressure per traffic class |
| Restarts | `health()[frag].restarts` | Repeated worker/dispatcher restarts = crash loop; investigate before it cascades |
| Placement | `Node::placement_report()` | Authoritative map, lifecycle states, `dynamic` flag — is migration stuck in `Bootstrapping`/`Draining`? |
| Sync entries | `Node::sync_entries_applied()` | Full-state sync volume since start; nonzero in dynamic mode = reconciler isn't owning anti-entropy |
| Replication lag | `batch_interval` + receiver apply/flush + peer RTT | ACKs follow the receiver fence; an unanswered ordinary batch rotates and replays at `replication_ack_timeout`, while barriers use `reconcile_timeout` |
| Peer churn | `Node::peer_count()` and timeout/session-rotation warning logs | Repeated queue/write cancellation or ACK expiry usually means a non-reading, overloaded, or unhealthy peer |

Operational rules of thumb:

- `fragment_count` and `fragment_map` must be identical on every node in
  static mode — a mismatch silently makes keys unreachable.
- Keep at least two replicas per fragment if you care about the unflushed
  write tail surviving `kill -9`.
- `tombstone_gc_age` must be `None`; every `Some(..)` setting fails startup.
  Size durable storage for retained tombstones until the system has a
  durable, replica-aware reclamation protocol.
- The background pool's default 1000 ops/s rate limit is a feature, not a bug
  — raise it deliberately if your bulk path needs more.
- Size the peer frame queue with encoded message sizes in mind; it is not a
  byte-bounded memory budget. Reconciliation fetches are chunked at 500 keys,
  but the fragment summary and encoded bytes are not paginated yet.
