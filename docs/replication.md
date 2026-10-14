# Replication (Phase 5)

Each fragment runs **one `ReplicationSender` task per replica target** — replication is
asynchronous, batched, and session-based. There is no synchronous quorum on the
write path: a local `put` returns after the worker reports its exact apply
outcome (see [Consistency](consistency.md)); reaching replicas happens in the
background.

## Sender state machine

```
                 PeerConnected
   Disconnected ──────────────► Connected { session_id, next_batch_seq, in_flight }
        ▲                              │  buffer full on Enqueue
        │                              ▼
        │                        BufferReset ──correlated reconcile + ACK fence──► Connected
        └──────────────── PeerDisconnected (BufferReset survives disconnects)
```

- **Connected**: locally applied mutations first enter a bounded Node data lane
  and then a bounded sender-owned buffer (each uses `buffer_capacity`, default
  10 000). Batches ship on a timer
  (`batch_interval` 100 ms, `batch_size` 100), with at most `max_in_flight`
  (4) batches awaiting ack. Protocol responses use a separate bounded lane
  (64 messages by default); peer lifecycle, overflow generations, and
  on-demand reconciliation are persistent/coalesced signals, so mutation
  saturation cannot hide control progress.
- **Disconnected**: peer down; buffered and in-flight operations are retained
  for replay in the next session.
- **BufferReset**: entered when either the mutation-ingress lane or owned
  buffer is full. Shed commands name mutations already applied to authoritative
  fragment state; overflow atomically advances a persistent generation before
  returning to the caller. A live sender rotates its session and starts
  reconciliation immediately; reconnect is not required.
  Mutations arriving during reset are retained as post-snapshot deltas up to
  the same bound. Another overflow advances the reset epoch, so an older round
  cannot complete the newer reset state. `BufferReset` also survives
  disconnects.

Reset completion is deliberately strict. The sender accepts only messages for
the current nonzero `(session_id, round_id, chunk_seq)`, validates fetched data
against the exact requested key set and fragment, awaits every local apply,
fences outbound repairs with a tracked empty batch, and verifies that neither
the reset epoch nor the fragment-wide mutation revision changed since the
snapshot. Otherwise it starts another round. This revision fence includes
inbound replication, reconciliation, expiry conversion, and tombstone
reclamation—not only writes enqueued by this sender.

Tuning lives in `SenderConfig` (see [Configuration](configuration.md)):
`batch_size: 100`, `batch_interval: 100 ms`, `max_in_flight: 4`,
`buffer_capacity: 10 000`, `consistency_interval: 300 s`,
`reconcile_timeout: 30 s`, and
`required_ack_durability: MemoryApplied`.

Runtime liveness policy is separate for source compatibility. A node maps
`NodeRuntimeOptions::replication_ack_timeout` (30 seconds by default) and
`queue_admission_timeout` (5 seconds) into each sender; the Node integration
also bounds the separate protocol-control lane to 64 messages by default via
`replication_control_message_capacity`. Direct embedders get the
same defaults from `spawn_sender`; use `spawn_sender_with_options` plus
`SenderRuntimeOptions::with_ack_timeout` and
`with_queue_admission_timeout` to override them. Reconciliation barriers always
use `SenderConfig::reconcile_timeout`, not the ordinary ACK timeout.

## Sessions and acks

On a new peer channel the sender mints a fresh `session_id` (a non-cryptographic
hash of time + thread + an atomic counter, never 0) and sends
`SessionStart { session_id, fragment }`. The receiver stores it per
(peer, fragment) and **discards batches from older sessions** — receiver
admission is session-id-only; there is no per-batch sequence seen-set. Redelivered
operations are safe anyway because application is an idempotent LWW merge.
Repeated notifications for the same live channel are idempotent and do not
rotate the session. Connect/disconnect notifications carry the exact peer
queue identity, so an older connection cannot replace or clear a newer one.
Replaying `SessionStart` with the same id also does not clear a failed-repair
latch; only a genuinely new session does.

The receiver accepts `SessionStart` only for a fragment it currently hosts,
which bounds the per-connection session table by configured fragments. Normal
replication batches are processed sequentially per connection and await every
actor apply. Legacy full-state `SyncResponse` entries now do the same and are
counted only after successful application; a shed or failed apply closes the
connection so handshake sync can retry instead of silently dropping a suffix.

A tracked batch carries the sender's `required_ack_level`. The receiver first
verifies that every key belongs to the declared fragment and awaits every exact
LWW apply outcome. It then attempts the requested fence and sends the level it
actually achieved in
`BatchAck { session_id, batch_seq, fragment, level }`, where `level` is:

- `APPLIED`: every operation is visible in memory; a configured store may not
  have flushed it yet;
- `STORE_COMMITTED`: the configured `KvStore` accepted the flush. This is not
  an fsync guarantee unless the store itself provides one.

The default `MemoryApplied` requirement therefore does not force one backend
write per network batch. A `StoreCommitted` requirement forces a fragment
flush; a storeless receiver can report only `APPLIED`, which is visible as a
durability mismatch and cannot retire that batch.

Reconciliation repairs use untracked batches followed by one tracked empty
barrier. If any untracked apply fails, the receiver poisons that
`(peer, fragment, session)` and NACKs the barrier instead of falsely certifying
the successfully applied prefix. The sender then rotates the session and
retries. The barrier requests `StoreCommitted` only when configured; a
`MemoryApplied` reconciliation does not force a store flush.

The sender removes the matching `InflightBatch` only when the reported level meets
`required_ack_durability`. Unknown/omitted levels and stale sessions are
ignored, so an older fieldless ACK cannot be mistaken for success. An apply or
flush failure returns `BatchNack`; the sender requeues the exact stamped batch
at the front for idempotent retry after a one-second floor, preventing a
permanently failing store from becoming a tight write/log loop.
`SenderStateReport::ack_durability_mismatch` exposes a valid but insufficient
ACK; the batch remains in flight. A weak ACK does not extend that batch's
original deadline. ACK observations reset on every session.

`Hello` advertises an inclusive protocol-version range and capability bits for
exact mutation results, durability ACKs, and correlated reconciliation. The
connection is rejected before peer installation unless the range includes the
local version and every required bit is present. With `cluster_secret`
configured, identity, range, and capabilities are all bound into the HMAC, so
an on-path peer cannot strip a feature bit without failing tag verification.
This static tag is not peer authentication: it has no fresh challenge, so a
captured Hello can be replayed on the plaintext transport. Fieldless legacy
Hellos fail closed. The current release supports one wire version, so upgrades
still require mutually compatible binaries.

Every ordinary streaming batch receives an ACK deadline after its frame enters
the peer queue (30 seconds by default). If any ordinary batch expires, the
sender restores **all** in-flight batches to the front of its buffer in original
batch and operation order, waits at least one second before streaming again,
mints a new session, and sends `SessionStart`. It reuses the live transport when
possible. Exact timestamps and origin ids are replayed rather than restamped,
so duplicate delivery is an idempotent LWW merge. A delayed ACK or NACK names
the expired session and is ignored.

Reconciliation summary, data, and tracked-barrier phases instead use
`reconcile_timeout`; an expired phase also rotates the session and retries the
round after the one-second floor. Neither kind of session rotation necessarily
drops a healthy socket. Queue admission has its own bounded wait (5 seconds by
default). In the `Node` integration, admission expiry marks the exact peer
connection failed and cancels it; the connection is removed, senders retain or
requeue work, and the configured dialer can redial. A per-frame socket write
that exceeds its 30-second default has the same connection-cancellation and
redial path. A forwarded request's separately configured budget (5 seconds by
default) covers its entire enqueue-plus-response attempt.

## Periodic reconciliation (anti-entropy)

Every `consistency_interval` (default 300 s) each connected sender sends
`ReconcileRequest { fragment, session_id, round_id }`. The receiver replies
with the same correlation fields and one `KeySummary`
per local key — `{ key, timestamp_ms, node_id, is_tombstone }`, built from a
`GetAll` that **includes tombstones** (sync must ship deletes). The requester
then, for each key, compares with `wins_over`:

- peer wins → key goes to `fetch_keys`;
- local wins → key is shipped to the peer in a `ReplicateBatch` with
  `batch_seq: u64::MAX`; the following tracked barrier accounts for all
  repairs and the requested durability fence;
- keys only one side has are fetched or shipped accordingly.

Fetches go out as `ReconcileFetch` (chunked at 500 keys per message;
`RECONCILE_FETCH_CHUNK = 500`) and values return in a matching
`ReconcileData`. Missing, duplicate, unrequested, or wrong-fragment entries
reject the whole chunk before any apply. Valid entries are merged locally at
`OpPriority::Default`. Stale responses are ignored, and LWW makes a retried
exchange idempotent.

When a requester with a configured store fetches winners under a
`StoreCommitted` policy, it also flushes those local applies before sending the
barrier; a failed flush obligation survives into a no-fetch retry. A storeless
requester remains memory-only—the policy still describes the durability a
remote batch receiver must report.

Rounds run periodically only while streaming. `ReconcileNow` (used by dynamic
placement bootstrap) is retained across a connection race and local failures
retry on their own one-second timer rather than waiting for `batch_interval` or
the next consistency tick. Reset rounds restart on reconnect before streaming
resumes. Local, transport, NACK, and phase-timeout failures all fail closed:
they never increment the completed-round counter or clear the reset state.
Placement drain likewise treats `BufferReset` as active work even when its
ordinary buffer and in-flight counts are zero.

Tombstones are retained: `Node::start` rejects time-based GC because neither a
quiet sender nor an elapsed age proves that every current and future replica
has durably observed the deletion. Safe reclamation requires durable
convergence watermarks and durable store deletion, which are not implemented.

Reconcile fetches are chunked by key count, but the summary response still
describes the whole fragment in one frame and neither path is currently
budgeted by encoded bytes. A large fragment or value can exceed the 16 MiB
frame limit; byte-bounded reconciliation pagination remains follow-up work.

## The Hello / full-state handshake

The connection handshake is **not** replaced by the reconciler in static mode.
The dialer sends `Hello { node_id, protocol range, capabilities, auth_tag }`
immediately; the listener validates and replies; then,
when dynamic placement is *off*, each side sends a full-state `SyncRequest`
for its fragments and the peer answers with `SyncResponse` entries. With the
Phase 8 placement driver active, the handshake skips `SyncRequest` entirely —
the reconciler owns anti-entropy (the rolling-restart test asserts
`sync_entries_applied() == 0` in that mode).

## Wire framing and transport notes

- Messages are 4-byte length-prefixed protobuf `Envelope`s over TCP;
  `MAX_FRAME_BYTES` is 16 MiB.
- Reconnect uses exponential backoff from `RECONNECT_BASE` (100 ms, doubling),
  managed by the redial supervisor in the node layer.
- The per-peer outbound queue defaults to 256 **frames** through
  `NodeRuntimeOptions`; it is not byte-bounded. All protocol traffic shares the
  same queue and socket writer. `OpPriority` selects the receiver's actor pool
  but does not give its frame queue precedence.
- Each fragment/peer Node sender separately retains at most
  `buffer_capacity` queued mutation commands, `buffer_capacity` owned deltas,
  `max_in_flight * batch_size` ordinary in-flight operations, and 64 queued
  control messages by default. These are count bounds; values and control
  payloads are not byte-budgeted. Standalone `spawn_sender*` callers retain the
  source-compatible unbounded command channel.
- Remote fragment fan-out and per-fragment replica attempts are sequential;
  priority does not make those network attempts concurrent.
- Every inbound version path, including `ReconcileData`, validates and observes
  the remote timestamp before it can affect stored LWW state.

## Tombstones over replication

Tombstones are first-class replicated entries: they carry HLC timestamps, ride
in `ReplicateBatch` ops and `SyncEntry`/`KeySummary` messages with their
`is_tombstone` flag, and are compared by `wins_over` like any other write.
See [Tombstones & deletes](tombstones.md).
