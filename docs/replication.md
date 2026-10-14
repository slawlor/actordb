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

- **Connected**: local mutations accumulate in a bounded buffer
  (`buffer_capacity`, default 10 000). Batches ship on a timer
  (`batch_interval` 100 ms, `batch_size` 100), with at most `max_in_flight`
  (4) batches awaiting ack.
- **Disconnected**: peer down; buffered and in-flight operations are retained
  for replay in the next session.
- **BufferReset**: entered when an `Enqueue` arrives with the buffer already
  full. The buffer is shed and the op itself is still applied locally. A live
  sender rotates its session and starts reconciliation immediately; reconnect
  is not required.
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
ACK; the batch remains in flight. ACK observations reset on every session.

This protocol change is deliberately fail-closed but has no negotiated version
bit yet. Older receivers do not send mutation responses and emit fieldless ACKs
that a new sender rejects; older senders omit the required ACK level and ignore
the new receiver's NACK. Upgrade all nodes together until explicit Hello
capability negotiation lands.

**Ordinary streaming batches still have no ACK timeout or retransmission
timer.** `max_in_flight` only
gates *sending* new batches; an unacked batch sits in memory until a NACK or
session transition. Detected disconnects, reconnect races, and failed sends all
requeue every unacknowledged operation at the front before discarding the old
session. Reconciliation phases do have `reconcile_timeout`; a silent summary,
data, or barrier phase rotates the session and retries after the one-second
retry floor. Streaming ACK timeout/retransmission and bounded receiver ingress
remain separate hardening work; a connection that stays open but never ACKs
ordinary batches can still fill `max_in_flight` and stall that sender.
The phase deadline starts after the outbound frame enters the bounded peer
queue; queue admission itself still awaits without a timeout, so a blocked
socket writer can delay both reconciliation timers and sender mailbox work.

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
frame limit; byte-bounded pagination remains required.

## The Hello / full-state handshake

The connection handshake is **not** replaced by the reconciler in static mode.
The dialer sends `Hello { node_id }` immediately; the listener replies; then,
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
- `PEER_CHANNEL_DEPTH` is 256.
- Every inbound version path, including `ReconcileData`, validates and observes
  the remote timestamp before it can affect stored LWW state.

## Tombstones over replication

Tombstones are first-class replicated entries: they carry HLC timestamps, ride
in `ReplicateBatch` ops and `SyncEntry`/`KeySummary` messages with their
`is_tombstone` flag, and are compared by `wins_over` like any other write.
See [Tombstones & deletes](tombstones.md).
