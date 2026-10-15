# Operations

## Failure scenarios

### Node crash (`kill -9`) and restart

1. The unflushed write-buffer tail is lost on the crashed node. Batch size and
   active-write latency are controlled by `flush_batch_size` / `flush_interval`;
   an idle fragment is flushed on `snapshot_interval`.
2. On restart with the same `node_id`, the node re-establishes sessions (new
   `session_id`; receivers discard the old session's batches) and the Phase 5
   reconciler repairs the divergence. Static and dynamic placement use the same
   correlated, byte-bounded reconciliation path; `SyncRequest`/`SyncResponse`
   are retired and `sync_entries_applied()` remains zero.
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

Startup also preflights every durable record against the protocol's required
single-record envelopes. A legacy key/value that cannot fit the 16 MiB hard
frame limit, or whose provenance node id is empty or exceeds 255 UTF-8 bytes,
fails startup instead of loading permanently unreplicable data. Repair or
remove the record offline before restarting; actordb does not rewrite it.

### Rolling restart with placement

With a `PlacementController` configured, a rolling restart is the migration
gain path: the restarted node (same `node_id`) synchronously reloads its
durable fragment subtrees without serving them, re-polls the controller, and
bootstraps each fragment via an on-demand
reconcile round from a surviving serving replica. The exchange is paged,
bounded by exact encoded bytes and `BOOTSTRAP_TIMEOUT` (30 s), and fails closed:
a timeout leaves the fragment non-authoritative for a later retry. Surviving
replicas keep serving throughout. Covered by
`placement_rolling_restart_delta_reconcile_only`, which asserts the restarted
node converges to the full dataset while the retired sync counter remains zero.

For cold cluster genesis, publish one owner per fragment first and wait for it
to report `Serving` before adding overlap replicas. If every member of a
multi-replica placement starts non-authoritative at once, all remain
`Bootstrapping`: actordb deliberately refuses to let equally unverified empty
nodes certify one another.

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

- Each connection has one shared outbound pipeline, bounded by 256 frames and
  32 MiB of admitted encoded data by default. Half the frame slots and 1 MiB
  of bytes are reserved from ordinary producers for writer-priority protocol
  responses. Queue admission is capped at 5 seconds and each complete socket
  frame write at 30 seconds. Either expiry cancels that exact connection; its
  senders retain or requeue outstanding work, and the configured dialer
  reconnects with backoff.
- At most 1024 accepted sockets are concurrently handshaking or serving as
  inbound peers by default; excess accepted sockets are closed. The complete
  `Hello`/`HelloAuth` exchange has one absolute 5-second deadline and pre-auth
  frames are limited to 4 KiB. A frame body that follows an established
  connection's prefix has a 30-second deadline. Completed Node-owned tasks are
  reaped while the node runs rather than retained until shutdown.
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
- Summary, fetch, data, repair, and ordinary replication frames are split by
  exact encoded protobuf size. They target 1 MiB pages and never exceed the
  16 MiB hard frame limit. A single record that cannot fit is rejected before
  local apply, so reconnect cannot loop forever on an unsendable mutation.

The peer queue, control-lane capacity, and timeout values are configurable
through `NodeRuntimeOptions`; mutation ingress follows
`SenderConfig::buffer_capacity`. Use
`Node::start_with_options`. The shared writer pipeline defaults to both 256
frames and 32 MiB of admitted encoded data; one ingress reservation bounds
producers waiting to acquire byte permits before any encoded body is allocated.
The owned permit then travels with the frame and releases on every write/drop path.
Reader-generated responses have a separate priority lane and a full 16 MiB
byte reserve, so one legal response remains admissible under producer
saturation; operation priority does not reorder the ordinary producer lane.
Ordinary queue admission defaults to 5 seconds; streamed reconciliation-data
pages use the separate 30-second `reconcile_data_admission_timeout` so large
legal frames do not churn an otherwise healthy slow connection. Configured
frame capacity must be at least two and byte capacity at least 32 MiB.

### Peer startup, identity, and duplicate connections

- Every configured `members` entry is a persistent dial intent. It starts even
  when the peer is initially unavailable and retries transport, authentication,
  and protocol failures with jittered exponential backoff (100 ms up to 30 s).
  The configured member id is known before the first dial and is the expected
  Hello id from the first authentication onward; a different claimant is
  rejected before it enters the peer map.
- `Node::connect(address)` tries its first dial once. It returns after that
  successful transport is handed to the Node-owned redial supervisor, before
  authentication and immediate reconciliation complete. Concurrent calls for
  the same address wait for the pending initial dial instead of returning early.
  The supervisor pins the first successfully authenticated node id and rejects a
  different id on later reconnects. Later failures retry in the background.
- Simultaneous crossed dials are expected. Both endpoints rank the same
  challenge-bound transcript and direction, keep the same winner, cancel the
  loser, and park its dial supervisor while the winner remains healthy. This
  avoids continuously rotating replication sessions.
- With `cluster_secret`, protocol v3 proves fresh possession of the shared key
  using both peers' random challenges. It does not distinguish holders of that
  shared key, encrypt traffic, or integrity-protect post-handshake frames. Use a
  mutually authenticated protected `Network` on an untrusted network.

Use at least 32 independently generated, high-entropy random bytes for
`cluster_secret`. Challenges make a captured proof stale on a new connection,
but a low-entropy password can still be guessed offline by testing candidate
keys against the captured transcript and HMAC.

### Graceful shutdown

Stop admitting application operations and await their futures before calling
`node.shutdown().await`. Shutdown closes internal task admission, aborts and
joins listener, connection, writer, dial, relay-timer, and placement work,
releases pending network waiters, stops replication senders, and finally stops
and joins fragment supervisors so their pools drain and stores flush. It is
idempotent, and a cancelled shutdown waiter may call it again.

The lifecycle fence covers Node-owned background tasks; it does not
automatically wait for arbitrary `put`, `get`, batch, health, or other Node
futures running in caller-owned tasks. Dropping `Node` requests best-effort
cancellation and actor stop only. Because `Drop` cannot await, it is not a
substitute for shutdown when durable flush, task join, or prompt RocksDB lock
release matters.

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
tick retries; bootstrap timeout leaves the fragment non-authoritative for a
later retry; an incomplete revoke remains `Draining` and warns every
`placement_drain_warn_interval` rather than discarding recovery state.
Re-adding the node cancels that stale teardown.
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
| Legacy sync entries | `Node::sync_entries_applied()` | Compatibility counter for retired `SyncResponse`; always zero in protocol v3 |
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
- Tune the peer frame and byte capacities together. Protocol messages are
  individually exact-byte bounded (1 MiB soft target, 16 MiB hard frame), and
  the writer defaults to a 32 MiB encoded-data budget regardless of its 256
  frame slots.
