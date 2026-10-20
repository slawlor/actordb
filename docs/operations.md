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

The offered RocksDB backend also validates its persistent manifest before
network bind or actor startup. A restart must use the same `node_id` and
`fragment_count`; cloned/restored database directories retain that identity.
Unknown/corrupt versions and mismatches return `NodeError::Storage` without
rewriting the manifest. Unknown column families and metadata records are also
rejected. A strict open of a non-empty default-CF-only database from a
pre-manifest release fails before adding the metadata CF, so an attempted
upgrade does not prevent rollback. Adoption must be explicitly enabled once,
after the operator verifies the path, fragment layout, and backup. It validates
that every key hashes to its stored fragment under the claimed count. Two live
Nodes cannot share one `Arc<RocksDbStore>`; cancelled startup and best-effort
Node drop leave that same object fail-closed.

Startup also seeds the node HLC from the highest durable version. A persisted
timestamp farther in the future than `max_clock_skew` fails startup rather
than allowing that version to freeze its key; investigate clock rollback or
poisoned data before widening the bound.

Fragment startup preflights every durable record in the locally assigned
prefixes against the protocol's required single-record envelopes. Explicit
legacy adoption separately scans every record before writing the manifest. A
key/value that cannot fit the 16 MiB hard frame limit, has malformed encoding,
or whose provenance node id is empty or exceeds 255 UTF-8 bytes fails startup
instead of loading permanently unreplicable data. Repair or remove the record
offline before restarting; actordb does not rewrite it.

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
- While partitioned, remotely routed single-key and batch reads and writes fail
  over across `replica_order`. Single writes retry the same stamped LWW
  version, and single reads source-pin each response and retry when an
  unavailable holder remains silent, so actor/routing failure cannot be
  confused with a valid not-found result. If no replica answers, the operation
  returns `NodeError::Network`.

### Slow or non-reading peer

- Each connection has one shared outbound pipeline, bounded by 256 frames and
  32 MiB of admitted encoded data by default. Half the frame slots and 16 MiB
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
  and response. Expiry removes the pending request; single-key and batch APIs
  then try the next holder. Remote batch fragment groups are sequential, and
  every operation's holder attempts are sequential, so a slow attempt delays
  those that follow.
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
Incoming raw bodies share a separate node-wide 64 MiB receive budget, including
handshake and outbound-dialed connections. Admission reserves the announced
body before allocation; saturation closes only the contributing connection.
Tune `with_receive_byte_capacity` for concurrent large frames (minimum 16 MiB).
Reservations release after the handler turn or on error/cancellation. This
counts encoded bodies only; decoded values, actor/relay queues, caches, kernel
buffers, and caller results remain separate memory consumers.

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

Call `node.shutdown().await` and handle its `Result`. The first call atomically
closes public-operation admission; later data calls receive
`NodeError::ShuttingDown`. Shutdown aborts and joins listener, connection,
writer, dial, relay-timer, and placement work. It closes and non-abortively
drains the separate serving forwarded single/batch mutation completion owner
while actors and replication senders remain live, then releases pending network
waiters, waits for calls admitted before the fence, and stops replication
senders. It then closes periodic fragment producers, drains dispatchers and
worker mailboxes, materializes TTL expirations, and flushes every local store
before stopping any fragment supervisor.

A store failure returns `NodeError::Storage` without destroying the quiesced
fragment or its retryable batch. The node remains one-way quiescing and admits
no new work; call `shutdown` again after storage recovers. A cancelled waiter
may also retry, and a successful/repeated call is idempotent. This is a local
durability fence, not a replication-convergence fence: current shutdown stops
senders without waiting for peer ACKs. Transport cancellation can also drop an
inbound mutation response after that mutation applied locally, so peers and
clients must retry stamped/idempotent requests after disconnect. Dropping
the response waiter does not cancel accepted forwarded apply, exact outcome
handling, or sender handoff. The completion owner retains its operation fence,
placement authority and request/RPC payload permits through that work; a
cancelled normal-shutdown caller can resume the same drain. It adds no apply
deadline that could orphan an already-queued actor mutation. Merely dropping
`Node` remains best effort and cannot await or report flush, join, or RocksDB
lock-release outcomes. It therefore leaves the same live RocksDB object
fail-closed to reattachment; call and await `shutdown` for reusable ownership.

When stopping a healthy replica intentionally, use
`shutdown_with_replication_drain(ReplicationDrainOptions)` to request a bounded
peer fence first. One absolute timeout (30 seconds by default) covers placement
freeze, previously admitted mutations, target preparation, reconnect, stable
full reconciliation, and the configured receiver durability barrier. On
success it immediately continues through the local flush sequence above.

On timeout, inspect the error's completed and pending fragment/peer targets.
The node remains one-way quiesced, but its listener, dialers, connections,
senders, and fragment state stay alive; restore connectivity or receiver-store
health and retry. Caller cancellation retains any already-issued drain
generation too. A `RetryableSender` error asks the caller to retry with a fresh
exact sender fence; `Unavailable` instead means local shutdown already crossed
the replication boundary and the peer guarantee can no longer be established.
If availability is more important than the remote fence, call ordinary
`shutdown()` afterward to abandon it and finish local-only. A later local flush
error carries the already-valid replication report and does not repeat peer
work on retry.

This is not a cluster convergence or response-delivery certificate. For every
reported target it proves the receiver acknowledged the LWW state at the
mutation-admission fence or a newer winner, at `MemoryApplied` or
`StoreCommitted`/`StableCommitted` as configured. `StoreCommitted` is ordinary
store acceptance; `StableCommitted` invokes the backend's explicit stable
fence. Offered RocksDB can force that fence even when ordinary writes use
`AsyncWal`. Explicit placement supplies the closed target set; legacy empty-map mode
can cover only authenticated peers whose senders existed at the fence. Clients
must still retry a stamped mutation if its network response was lost.

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
For a deletion-capable store, a revoked fragment also remains `Draining` until
every exact replacement replica completes a stable full reconcile at one
unchanged source revision. Prefix-delete errors retry idempotently; target-set
changes retain the namespace. Re-adding the node before detach replaces sealed
senders and cancels that stale teardown, while a re-add after detach waits for
`Stopped` and boots a fresh subtree.
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

Health uses a single configurable local snapshot deadline (five seconds by
default), including registry lookup and all fragment probes. Unavailable
dispatcher evidence returns an error, never fabricated zero counters. Worker
counts/names describe exact linked actors observed `Running`; a blocked worker
can still have that status. This non-atomic view is not cluster readiness,
replication progress, or a durability fence. Compare it with placement lifecycle
and application-level probes appropriate to your deployment.

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

## Aggregate peer writer bodies

Each Node limits encoded peer-envelope bodies across all physical outbound and
inbound connections with `NodeRuntimeOptions::node_peer_queue_byte_capacity`
(default 256 MiB, minimum 32 MiB). Configure it through the Bon builder or
`with_node_peer_queue_byte_capacity`. Per-connection frame and byte limits still
apply; their configured maxima no longer multiply into an unconstrained total
of encoded writer bodies.

Ordinary RPC and replication producers reserve both their connection allowance
and the shared Node allowance before allocating an encoded body. The Node-wide
producer share excludes 16 MiB so one maximum-size reader-generated priority
response can be admitted when ordinary producers saturate. The reserve is shared
across the Node, not repeated for every peer. Priority responses use nonwaiting
admission; exhaustion closes the contributing physical connection. Ordinary
producers retain existing queue/socket deadlines and fail through the visible
connection reset/reconciliation path when their bounded wait expires.

Owned permits remain attached to each body through queueing and an active socket
write. Dropping a body after completion, failure, cancellation, or writer abort
releases its shared allowance. Closing one connection closes only its own gates;
other connections retain access to the Node budget. A peer close also wakes its
pending aggregate admission when that writer queue closes.

`Node::peer_writer_diagnostics()` reports the aggregate capacity, reserved or
admitted body bytes, ordinary producer bytes, and saturation events. Saturation
counts failed immediate shared-byte acquisitions, including attempts that later
wait successfully. Counters saturate without wrapping and contain no payloads.
These snapshots are approximate during concurrent admission/release.

This budget charges encoded protobuf body length. It excludes the four-byte
length prefix, decoded protobuf/request objects, queue/frame/task/allocator
headers, cache/persistence/replication payloads, query results, and kernel socket
buffers. Connection/frame counts and those independent admission budgets still
need separate deployment allowances; this is not a total process-heap ceiling.

## Persistence outage admission

`NodeConfig::persistence` sets authoritative pending/active persistence limits:
10,000 records and 64 MiB per fragment, and 256 MiB across a node by default.
These are conservative charged bytes, including the backend batch copy, rather
than process RSS. Pending writes retain only the latest winner for each key;
failed attempts restore their winners into that map. An active batch remains
immutable until its store call ends. Same-size pending overwrites are admitted
at capacity. Growth or a new key that exceeds a count/byte allowance returns
`ApplyError::Backpressure` before cache mutation. Already-applied writes whose
flush fails still have an unknown durable outcome and remain retryable.

`FragmentState::persistence_backlog()` reports charged records/bytes. Admission
rejection and failed flushes emit structured backlog diagnostics. Ordinary
storage errors return to callers while workers remain alive, preserving queued
requests and pacing retries instead of creating worker restart churn. TTL cleanup
reserves its durable marker before replacing the live record; a full final
sweep reports saturation so shutdown cannot certify an undurable transition.
The cache and immutable key/version floors are independent of these budgets.

## Replication retained-memory admission

`SenderConfig::retained_byte_capacity` defaults to 64 MiB per sender.
`NodeRuntimeOptions::replication_retained_byte_capacity` defaults to 256 MiB
across a node. Owning byte permits follow each operation from ingress through
the deque and in-flight ACK batch; retransmission keeps the same reservation.
Charges conservatively include operation/protobuf/encoded representations.
Node fanout acquires both allowances before cloning a value for a replica.
Payload-bearing reconciliation control messages also reserve the allowance
until local processing completes; fixed-size ACK/lifecycle controls keep their
independent count-bounded lane so cleanup can progress at byte saturation.

Saturation schedules visible `buffer_reset` recovery from the authoritative
fragment cache. It cannot retroactively reject an applied local mutation.
ACK retirement, reset, sender cancellation and shutdown release owning permits.
`SenderStateReport::{retained_bytes,aggregate_retained_bytes}` and
`Node::replication_retained_bytes()` expose charges; structured saturation
diagnostics include both allowances. These charges exclude the separately
bounded peer frame pipeline and the cache.

Standalone callers should use `replication::spawn_bounded_sender`. Legacy
`spawn_sender`/`spawn_sender_with_options` still return unbounded channels for
compatibility: their consumed payloads are byte-bounded, but callers must
externally bound the preceding legacy channel.

## Storage execution and local deadlines

Synchronous `KvStore` bind, load, write, stable-fence and lifecycle prefix-delete
calls run on Tokio blocking threads behind the node's shared storage executor.
`PersistenceConfig::storage_concurrency` defaults to four submitted jobs.
`PersistenceConfig::validate()` rejects zero or unsupported concurrency before
Node/supervisor startup; low-level state configuration returns a validation
error before any async storage work can wait on an unusable executor.
Queued flushes retain their authoritative backlog charge while waiting for
an execution slot. `Node::active_storage_jobs()` reports submitted work;
`Node::persistence_retained_bytes()` reports the node-wide persistence charge.

`NodeRuntimeOptions::local_work_timeout` and
`SenderRuntimeOptions::local_work_timeout` default to 30 seconds. Local point
read waiters and each local reconciliation phase use explicit deadlines,
including summary/value reads, replicated applies, revisions and flushes.
A timed-out reconciliation waiter retries its idempotent round; an accepted
apply or flush may still finish in its actor or storage owner.

A storage deadline bounds the waiter, not the backend operation: synchronous
backend calls cannot be safely interrupted. Submitted flushes retain their
immutable batch and fragment ordering guard until the actual call and stable
fence finish and the authoritative buffer is committed or restored. Backend
panics are caught while retaining that fence; executor join/cancellation errors
reacquire ordering before retry-state recovery. Caller cancellation does not release those guards or discard
the authoritative retry batch. Explicit shutdown joins fragment work and drains
submitted storage jobs before releasing the exclusive store binding. The
executor retains and reaps both backend-completion and flush-owner task handles;
canceling a drain leaves them registered for a later shutdown attempt. A backend
that never returns can therefore prevent safe final teardown, while unrelated
async tasks continue running. Cancellation/best-effort Drop retains the existing
fail-closed store lease policy. Configure backend I/O limits for a finite storage
completion bound; lower waiter limits do not create one.

### Reconciliation result ownership

`NodeRuntimeOptions::reconciliation_retained_byte_capacity` provides a separate
aggregate 64 MiB allowance for local reconciliation summary/value pages.
Standalone `SenderRuntimeOptions::with_reconciliation_retained_byte_capacity`
configures the same policy. The minimum 64 MiB covers one maximum-size summary
cursor and one value/protobuf page together. This conservative reservation
charges a possible maximum-size reply before the actor clones it.

Opaque actor request leases keep result admission owned through dispatcher or
worker completion after cancellation of the waiter. Summary leases remain with
the local cursor; remote summary leases remain with their derived fetch-key
chunks through the round. Repair-key chunks stop at their byte target as well
as their count target, allowing one bounded wide key. Repairs ship each actor value page before fetching
the next, so two wide result pages do not overlap. Reset, timeout and shutdown
release credits when their owning work actually ends.

`Node::reconciliation_retained_bytes()` reports this aggregate result allowance.
Add this allowance to the independently configured replication backlog,
persistence, receive, query and peer-frame allowances when planning deployment
memory. All are charged admissions, not RSS; immutable cached key/version floors
and the loaded dataset remain independent. Result admission failure retries the
round with existing pacing and local deadlines.

## Query and compatibility scan execution limits

`NodeRuntimeOptions::with_query_runtime_byte_capacity` sets one node-wide
execution allowance (default **256 MiB**) shared by SQL coordinators, embedded
scans, incoming query pages, and compatibility peer scans. Each coordinator
reserves its configured result plus merge limits before accepting work. Incoming
pages reserve a conservative 32 times their encoded page allowance plus fixed
metadata. Embedded scans also reserve for the rows accumulated from completed
fragments. Admission rejects immediately with backpressure when the allowance
is exhausted; there is no retained semaphore-waiter queue.

Reservations follow accepted actor work through caller cancellation and follow
peer replies through writer admission. `Node::query_diagnostics()` reports the
allowance, current reserved bytes, and admission rejections. These reservations
bound active execution capacity, not RSS: returned results owned by embedded
callers, allocator overhead, caches, persistence, raw receive frames, and encoded
writer queues have separate ownership. The default 16 MiB result plus 32 MiB
merge limits therefore allow at most five such SQL coordinators concurrently
under the default allowance. Smaller per-query budgets admit more work.

Exact keys, conjunctive ranges, and byte prefixes seek the ordered fragment
index. Hash partitioning still requires range/prefix queries to contact every
selected fragment; additional indexes and explicit key-set routing are tracked
in [SQL scalability #114](https://github.com/slawlor/actordb/issues/114).
Initial head-page fetches remain sequential to preserve exact aggregate work
and memory admission. A heap merges fragment heads in global key order.

Compatibility scans preserve per-fragment offsets/limits and fragment-order
merging, with a 16 MiB aggregate result allowance and one-million-record work
limit. Oversize work, unavailable fragments, and revision changes fail the whole
operation. Low-level `FragmentState::scan` now returns `Result` for the same
explicit failures. TTL maintenance visits only indexed live deadlines due at a
pinned time, in bounded pages; tombstone keys/version floors remain resident.
