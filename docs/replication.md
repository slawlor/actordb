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
- **Disconnected**: with an explicit placement map, the identity-keyed sender
  remains allocated and retains buffered/in-flight operations for replay in the
  next session. Legacy map-empty mode instead discovers peer identities at
  runtime and reaps their senders on disconnect so sequential claimants cannot
  grow the sender maps without bound. Writes made during that legacy downtime
  are recovered by the mandatory static-mode full-state exchange on reconnect,
  including tombstones; that exchange is therefore part of the legacy mode's
  safety contract, not an optional optimization.
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

The receiver accepts `SessionStart` for any in-range configured fragment, even
if its placement actor is still being installed. Placement controllers are
observed independently, so the source can see an overlap just before the
destination does; retaining the session prevents that race from silently
stalling replication until the ordinary ACK timeout. A tracked batch that
arrives before actor installation receives a NACK and is replayed under the
same session. Out-of-range fragments are rejected, which bounds the
per-connection session table by `fragment_count`. Normal replication batches
are processed sequentially per connection and await every actor apply. Legacy
full-state `SyncResponse` entries now do the same and are counted only after
successful application; a shed or failed apply closes the connection so
handshake sync can retry instead of silently dropping a suffix.

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

Protocol-v2 `Hello` advertises an inclusive protocol-version range, capability
bits for exact mutation results, durability ACKs, correlated reconciliation,
and challenge authentication, plus a fresh 256-bit challenge. Both endpoints
then send `HelloAuth`. With `cluster_secret` configured, its HMAC-SHA256 binds
the ordered dialer/listener roles, both complete Hello offers, the selected
version/capabilities, and both challenges. A captured v2 exchange therefore
cannot be replayed on a fresh connection, and an on-path peer without the key
cannot rewrite an id or feature bit.

The proof establishes fresh possession of a cluster-wide shared key, not
individual node identity: any key holder can claim any node id. Plain TCP also
provides no confidentiality or post-handshake frame integrity. Without a
secret, `HelloAuth` is empty and provides sequencing/freshness for connection
election, not authentication. Missing challenge fields, the retired v1 Hello
tag, and incompatible ranges/capabilities fail closed. The current binary
supports only wire v2, which is wire-incompatible with v1, so upgrades require
mutually compatible binaries rather than a rolling v1/v2 mix. Moving from v1
to v2 is also a Rust source break for advanced users of the public generated
`actordb::proto` types: `Hello` gained a
`challenge` field and `EnvelopeMsg` gained a `HelloAuth` variant, so struct
literals and exhaustive matches must change along with the coordinated wire
deployment.

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
default). A reconcile reply racing placement retirement is discarded when its
per-fragment sender has already stopped; it does not tear down the shared,
multiplexed peer connection. In the `Node` integration, admission expiry marks
the exact peer connection failed and cancels it; the connection is removed,
senders retain or requeue work, and the configured dialer can redial. A
per-frame socket write that exceeds its 30-second default has the same
connection-cancellation and redial path. A forwarded request's separately
configured budget (5 seconds by default) covers its entire
enqueue-plus-response attempt.

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
Both endpoints send `Hello { node_id, protocol range, capabilities, challenge }`
and then mutually prove the ordered transcript with `HelloAuth`. One absolute
handshake deadline covers both flights. A configured member dialer also checks
the claimed id against the id assigned to that address. Only after mutual proof
does either side publish the connection. If crossed or duplicate dials race,
both endpoints use the same transcript-derived rank to retain the same winner;
the losing dialer waits while that winner remains live. Then,
when dynamic placement is *off*, each side sends a full-state `SyncRequest`
for its fragments and the peer answers with `SyncResponse` entries. With the
Phase 8 placement driver active, the handshake skips `SyncRequest` entirely —
the reconciler owns anti-entropy (the rolling-restart test asserts
`sync_entries_applied() == 0` in that mode).

This distinction is especially important when the static `fragment_map` is
empty (legacy full replication). Its connection-discovered senders are removed
on disconnect rather than retaining downtime writes. The mandatory full-state
request/response restores the current LWW winners after reconnect. With an
explicit placement map, senders are bounded by configured replica identities
and remain present while disconnected, preserving their bounded buffers and
reset/reconciliation state in addition to the static full-state exchange.

## Wire framing and transport notes

- Messages are 4-byte length-prefixed protobuf `Envelope`s over TCP;
  `MAX_FRAME_BYTES` is 16 MiB.
- Reconnect uses exponential backoff from `RECONNECT_BASE` (100 ms, doubling),
  capped at 30 seconds with jitter and managed by the redial supervisor in the
  node layer. Configured members retry from their first failed dial; manual
  `Node::connect` returns its first failure but owns retries after first success.
- Inbound handlers are capped at 1024 concurrent sockets by default. The
  two-flight handshake has one absolute 5-second deadline, and a pre-auth frame
  is capped at 4 KiB. Completed listener, handler, writer, dial, timer, and
  placement tasks are continuously reaped and are abort-and-joined at explicit
  Node shutdown.
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
