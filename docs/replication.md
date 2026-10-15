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
  are recovered by mandatory immediate correlated reconciliation on reconnect,
  including tombstones; that round is therefore part of the legacy mode's
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
materialization—not only writes enqueued by this sender.

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
are processed sequentially per connection and await every actor apply.
`SyncRequest` and `SyncResponse` are retired protocol-v3 frames; receiving
either closes the connection rather than activating a second, unbounded sync
engine.

A tracked batch carries the sender's `required_ack_level`. The receiver first
verifies that every key belongs to the declared fragment and awaits every exact
LWW apply outcome. It then attempts the requested fence and sends the level it
actually achieved in
`BatchAck { session_id, batch_seq, fragment, level }`, where `level` is:

- `APPLIED`: every operation is visible in memory; a configured store may not
  have flushed it yet;
- `STORE_COMMITTED`: the configured `KvStore` accepted the flush. This is not
  an fsync guarantee independent of the store policy. Offered RocksDB defaults
  to a synchronous WAL; explicit `AsyncWal` is weaker;
- `STABLE_COMMITTED`: after flushing actor buffers, the receiver's store
  completed its explicit stable-storage fence. RocksDB implements this with
  `flush_wal(true)`, including when ordinary writes use `AsyncWal`.

The default `MemoryApplied` requirement therefore does not force one backend
write per network batch. `StoreCommitted` forces a fragment flush, while
`StableCommitted` additionally forces the backend fence. A storeless receiver
can report only `APPLIED`; a custom store without a stable fence can report at
most `STORE_COMMITTED`. Either weak result is visible as a durability mismatch
and cannot retire a stronger batch.

Reconciliation repairs use untracked batches followed by one tracked empty
barrier. If any untracked apply fails, the receiver poisons that
`(peer, fragment, session)` and NACKs the barrier instead of falsely certifying
the successfully applied prefix. The sender then rotates the session and
retries. The barrier requests the configured level; a `MemoryApplied`
reconciliation does not force a store flush, and placement retirement can
privately strengthen its barrier to `StableCommitted` without changing
ordinary batch policy.

The sender removes the matching `InflightBatch` only when the reported level meets
`required_ack_durability`. Unknown/omitted levels and stale sessions are
ignored, so an older fieldless ACK cannot be mistaken for success. An apply or
flush failure returns `BatchNack`; the sender requeues the exact stamped batch
at the front for idempotent retry after a one-second floor, preventing a
permanently failing store from becoming a tight write/log loop.
`SenderStateReport::ack_durability_mismatch` exposes a valid but insufficient
ACK; the batch remains in flight. A weak ACK does not extend that batch's
original deadline. ACK observations reset on every session.

Protocol-v3 `Hello` advertises an inclusive protocol-version range, capability
bits for exact mutation results, durability ACKs, correlated reconciliation,
challenge authentication, paged anti-entropy, and byte-bounded batches, plus a fresh 256-bit
challenge. Both endpoints then send `HelloAuth`. With `cluster_secret`
configured, its HMAC-SHA256 binds the ordered dialer/listener roles, both
complete Hello offers, the selected version/capabilities, and both challenges.
A captured v3 exchange therefore cannot be replayed on a fresh connection, and
an on-path peer without the key cannot rewrite an id or feature bit.

The proof establishes fresh possession of a cluster-wide shared key, not
individual node identity: any key holder can claim any node id. Plain TCP also
provides no confidentiality or post-handshake frame integrity. Without a
secret, `HelloAuth` is empty and provides sequencing/freshness for connection
election, not authentication. Missing challenge fields, the retired v1 Hello
tag, and incompatible ranges/capabilities fail closed. The current binary
supports only wire v3 and requires paged-anti-entropy, byte-bounded-batches,
and stable-durability-ACK capabilities. Byte-bounded batches make
`BatchGetResponse` an expected-peer-correlated ordered prefix with explicit
suffix pulls. Stable durability ACKs let placement prove the backend fence
before destructive RocksDB prefix retirement. Peers missing either contract
are rejected. V3 is
wire-incompatible with v2, so upgrades require mutually compatible binaries
rather than a mixed v2/v3 rolling deployment. It is also a Rust source break
for advanced users of the public generated `actordb::proto` types:
`ReconcileRequest`, `ReconcileResponse`, and `ReconcileData` gained pagination
and snapshot-correlation fields, and `ReplicationAckLevel` gained
`StableCommitted`. Struct literals, exhaustive matches, and protocol
assumptions must change with the coordinated deployment.

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

Every `consistency_interval` (default 300 s) each connected sender starts a
correlated round. This is also the immediate catch-up path after connection in
both static and dynamic placement. `ReconcileRequest` carries
`(session_id, round_id, page_seq)` plus an exclusive key cursor, an observed
revision, and a pinned TTL time. The responder walks its cache's ordered key
index and returns byte- and count-bounded
`KeySummary { key, timestamp_ms, node_id, is_tombstone }` pages. Expired live
values are represented by their deterministic tombstone at the pinned time.

Neither side holds a cache lock or clones a fragment-wide snapshot across
network turns. A pass tolerates revision drift while walking monotonically by
cursor and records whether either side changed or any repair was transferred.
After its durability barrier, every changed or revision-drifted pass is
followed immediately by a fresh pass from the beginning. A round is counted
complete only after a mutation-free, revision-stable verification pass whose
local revision still matches at barrier completion. This avoids both unbounded
snapshots and the page-by-page restart storm that applying repairs would
otherwise cause. The requester merge-compares the two ordered streams:

- peer wins → key goes to `fetch_keys`;
- local wins → key is shipped to the peer in a `ReplicateBatch` with
  `batch_seq: u64::MAX`; the following tracked barrier accounts for all
  repairs and the requested durability fence;
- keys only one side has are fetched or shipped accordingly.

Fetch keys are grouped into exact-byte-bounded `ReconcileFetch` chunks of at
most 256 keys. The responder reads memory-bounded `GetVersionedPage` batches
under one cache lock per value page, moves them into exact-byte-bounded
`ReconcileData` pages, and streams correlated pages through the byte-budgeted
writer. This avoids both one actor round trip per key and one network round
trip per key without cloning the fragment or accumulating an unbounded value
burst.
Every requested key must appear exactly once before the terminal page;
missing, duplicate, unrequested, or wrong-fragment data rejects the exchange
before the barrier can certify it.
Valid entries are merged locally at `OpPriority::Default`. Stale responses are
ignored, and LWW makes a retried exchange idempotent.

Summary, fetch, data, repair, and ordinary replication builders measure the
exact encoded protobuf envelope. They target 1 MiB pages; a single valid record
may exceed that soft target, but no frame may exceed the 16 MiB hard limit.
One credit shared by all fragment senders on a physical connection admits one
complete reconciliation round at a time. Credit release wakes queued retries;
this bounds responder work without a fragment-count-dependent queue cliff.
Multi-page fetch responses run in a connection-owned worker so both peers'
socket readers remain live during symmetric large-value repair.
An irreducible oversized mutation, or one with empty or over-limit provenance,
is rejected before local apply. Persisted records created by an older binary
are checked during cache load and fail startup if they cannot traverse every
required single-record envelope or their provenance node id is empty or exceeds
255 UTF-8 bytes. Repair or remove such records offline before restarting.

When a requester with a configured store fetches winners under a
`StoreCommitted` or stronger policy, it also flushes those local applies before
sending the barrier; a failed flush obligation survives into a no-fetch retry.
A storeless requester remains memory-only—the policy still describes the
durability a remote batch receiver must report.

Rounds run periodically only while streaming. `ReconcileNow` (used by dynamic
placement bootstrap) is retained across a connection race and local failures
retry on their own one-second timer rather than waiting for `batch_interval` or
the next consistency tick. Reset rounds restart on reconnect before streaming
resumes. Local, transport, NACK, and phase-timeout failures all fail closed:
they never increment the completed-round counter or clear the reset state.
An authenticated reconcile request or fetch that races a fragment's
bootstrapping/non-serving state is ignored until the sender's bounded phase
timeout; it does not tear down the multiplexed connection needed by the reverse
bootstrap or unrelated fragments.
Placement drain likewise treats `BufferReset` as active work even when its
ordinary buffer and in-flight counts are zero.

Dynamic placement uses the same drain machinery for destructive namespace
retirement, but with stricter commit rules. A store must explicitly advertise
prefix-delete support. The old owner seals a sender to every exact replacement
replica and privately requires `StableCommitted` from each. All drains must
cover one unchanged local revision; a fetched winner restarts the complete set.
The placement replica vector and generation are revalidated before router
detach and again before deletion. Re-addition replaces the sealed senders and
bootstraps normally. After detach, the source quiesces every actor, completes
its final flush, and retries idempotent prefix deletion while remaining
`Draining`; only `Deleted` or `Retained` permits `Stopped`. This certifies the
source's LWW winners and TTL metadata at the revision pin, not byte-for-byte
cache equality after independent expiry cleanup.

## Replication-drained node shutdown

`Node::shutdown_with_replication_drain` reuses the correlated anti-entropy
engine for a stronger, opt-in shutdown boundary. After the node closes local
and inbound forwarded-mutation admission, joins placement work, and waits for
admitted apply/fan-out work, each exact target sender closes its data ingress.
It consumes every pre-fence data-lane item, subsumes any persistent overflow
generation, discards superseded volatile streaming state, rotates the session,
and runs a fresh full reconciliation. Completion is published only for that
sender's private drain generation after a mutation-free, revision-stable pass
and its tracked barrier ACK at `required_ack_durability`.

This snapshot-based recovery is why queue emptiness is neither necessary nor
sufficient: an overflowed or replaced volatile queue may be empty while the
authoritative fragment cache still contains unreplicated winners. Conversely,
old buffered and in-flight deltas can be retired after the fresh stable round
has covered their LWW winners. Stale sessions, rounds, ACKs, replacement sender
tasks, and weak durability levels cannot satisfy the retained generation.
Disconnect, NACK, phase timeout, and reconnect retry that same generation.

The result is a per-fragment/per-peer LWW version floor at the admission cut,
not exact cache equality or a simultaneous global snapshot. Concurrent inbound
replication can introduce newer winners, which are safe under LWW but do not
turn the report into a quorum certificate. Timeout or caller cancellation
leaves transport and any issued exact generations alive for retry. If an exact
sender task itself stops, `RetryableSender` reports that a retry must replace it
and take a fresh fence from the authoritative cache. Ordinary `Node::shutdown`
is the explicit local-only escape. Socket response delivery is outside the
barrier, so a client may need to retry an operation whose response was lost
after its mutation became durable.

Per-key tombstones are retained: `Node::start` rejects time-based GC because
neither a quiet sender nor elapsed age proves that every current and future
replica has durably observed the deletion. Whole-fragment prefix retirement has
an exact closed replica set; it does not make arbitrary tombstone GC safe.

## The Hello handshake and unified catch-up

Both endpoints send `Hello { node_id, protocol range, capabilities, challenge }`
and then mutually prove the ordered transcript with `HelloAuth`. One absolute
handshake deadline covers both flights. A configured member dialer also checks
the claimed id against the id assigned to that address. Only after mutual proof
does either side publish the connection. If crossed or duplicate dials race,
both endpoints use the same transcript-derived rank to retain the same winner;
the losing dialer waits while that winner remains live. After authentication,
the sender state machines run immediate correlated reconciliation for every
placement mode. `SyncRequest`/`SyncResponse` are retained only as generated
compatibility types and are rejected on the v3 wire.

When the static `fragment_map` is empty (legacy full replication),
connection-discovered senders are removed on disconnect rather than retaining
downtime writes or an unbounded set of discovered identities. Mandatory
reconciliation after reconnect restores current LWW winners and tombstones.
With an explicit placement map, senders are bounded by configured replica
identities and remain present while disconnected, preserving their bounded
buffers and reset state as well. `sync_entries_applied()` is a compatibility
counter for the retired protocol and remains zero in every mode.

## Wire framing and transport notes

- Messages are 4-byte length-prefixed protobuf `Envelope`s over TCP;
  anti-entropy and ordinary replication target 1 MiB encoded bodies, and
  `MAX_FRAME_BYTES` is the absolute 16 MiB body limit.
- Before Prost allocates, an envelope preflight rejects retired/ambiguous
  messages and caps repeated anti-entropy fields at 256 items and general
  batch fields at 4096. Producers additionally byte-bound batch puts and their
  worst-case replies, and cap batch-get pulls at 256 keys. Reader-generated responses use immediate bounded
  admission; saturation closes the correlated connection for replay instead
  of parking the reader behind its own writer.
- Reconnect uses exponential backoff from `RECONNECT_BASE` (100 ms, doubling),
  capped at 30 seconds with jitter and managed by the redial supervisor in the
  node layer. Configured members retry from their first failed dial; manual
  `Node::connect` returns its first failure but owns retries after first success.
- Inbound handlers are capped at 1024 concurrent sockets by default. The
  two-flight handshake has one absolute 5-second deadline, and a pre-auth frame
  is capped at 4 KiB. Completed listener, handler, writer, dial, timer, and
  placement tasks are continuously reaped and are abort-and-joined at explicit
  Node shutdown.
- The per-peer outbound writer pipeline defaults to 256 frames and 32 MiB of
  admitted encoded data through `NodeRuntimeOptions`. Producers reserve the
  data-lane ingress and exact byte permits before encoding; the permit remains
  attached to the queued/current frame and is released by RAII on writes,
  cancellation, or task failure. Reader-generated responses use a separately
  bounded, writer-priority lane and retain a 1 MiB share of the total byte
  budget. The writer forces one ready data frame after at most 16 responses.
  Both lanes share the socket writer. `OpPriority` selects the
  receiver's actor pool but does not reorder the ordinary data lane. Ordinary
  frames use the 5-second queue admission default; streamed
  reconciliation-data pages have an explicit 30-second default so a near-limit
  value can wait behind another legal frame.
- A ticketed FIFO gate admits one reconciliation round per physical connection
  while guaranteeing that a fragment starting a follow-up round rejoins behind
  fragments already waiting for bootstrap or repair.
- Each fragment/peer Node sender separately retains at most
  `buffer_capacity` queued mutation commands, `buffer_capacity` owned deltas,
  `max_in_flight * batch_size` ordinary in-flight operations, and 64 queued
  control messages by default. These sender lanes remain count bounds even
  though the downstream writer and each encoded frame are byte-capped.
  Standalone `spawn_sender*` callers retain the source-compatible unbounded
  command channel.
- Remote fragment fan-out and per-fragment replica attempts are sequential;
  priority does not make those network attempts concurrent.
- Every inbound version path, including `ReconcileData`, validates and observes
  the remote timestamp before it can affect stored LWW state.

## Tombstones over replication

Tombstones are first-class replicated entries: they carry HLC timestamps, ride
in `ReplicateBatch` ops and `ReconcileData`/`KeySummary` messages with their
`is_tombstone` flag, and are compared by `wins_over` like any other write.
See [Tombstones & deletes](tombstones.md).
