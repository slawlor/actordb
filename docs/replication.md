# Replication (Phase 5)

Each fragment runs **one `ReplicationSender` task per peer** — replication is
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
        │                        BufferReset ──PeerConnected──► reconcile ──► Connected
        └──────────────── PeerDisconnected (BufferReset survives disconnects)
```

- **Connected**: local mutations accumulate in a bounded buffer
  (`buffer_capacity`, default 10 000). Batches ship on a timer
  (`batch_interval` 100 ms, `batch_size` 100), with at most `max_in_flight`
  (4) batches awaiting ack.
- **Disconnected**: peer down; the buffer is retained for resume. Note the
  subtlety below about in-flight batches.
- **BufferReset**: entered when an `Enqueue` arrives with the buffer already
  full. The buffer is shed, the op itself is still applied locally, and the
  `(fragment, peer)` pair is recorded in `reset_senders`. On reconnect the
  sender reconciles instead of replaying. `BufferReset` survives disconnects —
  a response received while `Disconnected` belongs to a dead session and is
  ignored.

Tuning lives in `SenderConfig` (see [Configuration](configuration.md)):
`batch_size: 100`, `batch_interval: 100 ms`, `max_in_flight: 4`,
`buffer_capacity: 10 000`, `consistency_interval: 300 s`.

## Sessions and acks

On `PeerConnected` the sender mints a fresh `session_id` (a non-cryptographic
hash of time + thread + an atomic counter, never 0) and sends
`SessionStart { session_id, fragment }`. The receiver stores it per
(peer, fragment) and **discards batches from older sessions** — receiver
dedup is session-id-only; there is no per-batch sequence seen-set. Redelivered
operations are safe anyway because application is an idempotent LWW merge.

Acks (`BatchAck { session_id, batch_seq, fragment }`) simply remove the
matching `InflightBatch` from the sender's `in_flight` deque; stale-session
acks are ignored.

**There is no ack timeout and no retransmission timer.** `max_in_flight` only
gates *sending* new batches; an unacked batch sits in memory until the session
ends. Redelivery happens in exactly one situation: when a *new* session starts
while the sender still believes it is Connected (reconnect raced ahead of the
disconnect notice), unacked batches are requeued at the **front** of the
buffer, in order.

**In-flight loss on detected disconnect.** If `PeerDisconnected` is processed
first, the state moves `Connected → Disconnected` and the `in_flight` deque —
a field of the `Connected` variant — is dropped with it. Those unacked ops are
**not replayed** on reconnect; the divergence is repaired later by periodic
anti-entropy (below). So: delivery after a *detected* disconnect relies on
reconciliation, not replay; delivery after an *undetected* disconnect (reconnect
wins the race) is preserved by requeue. A failed batch send also requeues to
the front and transitions to `Disconnected`, losing nothing on that path.

## Periodic reconciliation (anti-entropy)

Every `consistency_interval` (default 300 s) each connected sender sends
`ReconcileRequest { fragment }`. The receiver replies with one `KeySummary`
per local key — `{ key, timestamp_ms, node_id, is_tombstone }`, built from a
`GetAll` that **includes tombstones** (sync must ship deletes). The requester
then, for each key, compares with `wins_over`:

- peer wins → key goes to `fetch_keys`;
- local wins → key is shipped to the peer in a `ReplicateBatch` with
  `batch_seq: u64::MAX` — reconciliation batches are **not ack-tracked**
  ("the receiver applies them via LWW under the current session");
- keys only one side has are fetched or shipped accordingly.

Fetches go out as `ReconcileFetch` (chunked at 500 keys per message;
`RECONCILE_FETCH_CHUNK = 500`), values come back as `ReconcileData` and are
merged locally at `OpPriority::Default`. A newer round supersedes an older
still-fetching round — LWW makes the exchange idempotent. An on-demand
`ReconcileNow` (used by the Phase 8 placement driver to bootstrap a newly
assigned fragment without waiting for the timer) is a no-op unless Connected.

Per-state behavior: rounds run periodically only in `Connected`; on reconnect
from `BufferReset` the sender reconciles *before* going Connected ("don't go
Connected yet: reconcile first"); in `Disconnected` reconciliation is deferred
to the next `PeerConnected`. `finish_reconcile` clears the pair from
`reset_senders` and moves to `Connected` (fresh session) if the peer channel
is present. Failure behavior is fail-open: if `GetAll` fails or the peer
channel vanishes mid-fetch, the round is treated as complete rather than
stalled (bootstrap is additionally bounded by `BOOTSTRAP_TIMEOUT` = 30 s —
availability over consistency).

The cleaner's tombstone GC is **blocked while any sender for the fragment is
in `BufferReset`**: the tombstone may not have reached the peer, and GC'ing it
would let reconciliation resurrect the deleted key.

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
- Every inbound path observes the remote timestamp into the node's HLC —
  with one deliberate edge: `ReconcileData` entries are merged inside the
  sender task, which has no HLC handle, so reconcile-fetched timestamps are
  applied verbatim without an `hlc.observe`.

## Tombstones over replication

Tombstones are first-class replicated entries: they carry HLC timestamps, ride
in `ReplicateBatch` ops and `SyncEntry`/`KeySummary` messages with their
`is_tombstone` flag, and are compared by `wins_over` like any other write.
See [Tombstones & deletes](tombstones.md).
