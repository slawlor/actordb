# Replication

Each fragment has one asynchronous sender per replica target. Ordinary writes
return after their exact local apply outcome; optional write concerns add the
requested acknowledgement/durability boundary. Background replication is
bounded, batched and session-correlated.

## Current peer contract

ActorDB `0.9.0` uses exact peer protocol **6**. A Hello identifies the node,
current version, fresh challenge and optional managed generation.
`HelloAuth` completes the challenge-bound shared-secret proof. There are no
version ranges, capability bits, independent process signing keys, signed-frame
headers or alternative full-sync messages.

TLS trust/certificates belong to the configured `Network`. Shared-key proof
establishes fresh possession, not individual holder identity or subsequent
frame integrity. With no secret, the transport must supply authentication.
Generation allowlists can reject claims and fence existing sessions, but
external credential revocation is required to exclude a still-authorized holder.

Both endpoints publish a peer only after the current handshake completes.
Configured dialers check the claimed identity against the destination's member
mapping. Crossed connections use a common transcript-derived rank, and cleanup
always refers to the exact physical connection. One absolute handshake deadline
covers both flights.

## Sender state and admission

A sender is disconnected, connected, or rebuilding after buffer reset.

- With explicit placement, disconnected senders remain bounded by the configured
  replica identities and retain replay/reset state.
- In map-empty full-replication mode, connection-discovered senders are removed
  on disconnect. Immediate reconciliation after reconnect restores current
  winners and tombstones without retaining an unbounded identity map.
- A mutation enters a bounded ingress lane and then the bounded sender buffer.
  Protocol controls have a separate bounded lane; peer changes, reset requests
  and reconciliation triggers are persistent/coalesced signals.
- Saturation concerns an already-applied operation. It schedules reset recovery,
  not false application failure. `SenderIngressDiagnostics` exposes admitted
  pending data/control, retained charges, shed/reset requests and unsettled reset
  state. Ordinary buffered replay/ACK processing precedes a fresh reconcile pause.
- Reset generations survive disconnects. A later overflow cannot be settled by
  completion of an earlier reset round.

Standalone users construct `spawn_bounded_sender` with explicit dependencies,
`SenderConfig` and `SenderRuntimeOptions`. There is no unbounded constructor or
compatibility inbox. Node runtime options select acknowledgement/admission
deadlines and protocol-control capacity.

Typical sender defaults are batch size 100, batch interval 100 ms, four
in-flight batches, 10,000 buffered operations, 300-second consistency interval,
30-second reconcile timeout and `MemoryApplied` ACK durability. Reconciliation
uses its own timeout rather than ordinary ACK timing.

## Sessions and acknowledgements

A new channel starts a fresh nonzero session and sends
`SessionStart { session_id, fragment }`. The receiver tracks sessions by peer
and fragment and rejects batches from retired sessions. Repeated notifications
for the same live channel are idempotent; stale disconnects cannot clear its
replacement.

Receivers validate key/fragment routing, observe complete HLC versions and await
actual apply. A tracked batch receives an ACK describing the boundary achieved:

| Level | Actual boundary |
| --- | --- |
| `APPLIED` | Applied in memory |
| `STORE_COMMITTED` | Accepted by the configured ordinary store contract |
| `STABLE_COMMITTED` | Actor buffers flushed and the backend's explicit stable fence completed |

Storeless receivers cannot claim store/stable completion. A weak, unknown or
omitted level cannot satisfy a stronger batch. Apply/flush errors NACK the
stamped batch; retries preserve its original versions and use a backoff floor.
A weak ACK neither retires a batch nor extends its deadline.

If an ordinary ACK deadline expires, every in-flight batch returns to the front
in original order, the session rotates, and replay uses the same stamps.
Delayed ACK/NACK messages from the retired session are ignored. The sender can
reuse a healthy transport.

## Streamed anti-entropy

The sole reconciliation engine uses current
`ReconcileRequest/Response/Fetch/Data` messages. Summaries carry complete LWW
versions and content digests. Requests bind nonzero session/round/chunk
identities, key cursors, observed revisions and a pinned TTL time.

Neither side clones a whole fragment across network turns. It walks ordered,
count/byte-bounded summaries and compares the two streams:

- A peer winner is fetched.
- A local winner is sent as a repair.
- Keys present on only one side are transferred as appropriate.

The ordered metadata index publishes cached digests and mutation revisions
before cache writes become visible, without copying payloads per page. See
[reconciliation snapshots](reconciliation-snapshots.md) for pinned TTL behavior,
local revision guarantees, resident index overhead and full diagnostic snapshots.

Fetches are bounded by exact encoded bytes and at most 256 keys. Responders read
incremental versioned pages and emit `ReconcileEntry` payloads, preserving writer
provenance, logical counters, tombstones and expiry. No record exceeds the
16 MiB hard envelope limit.

Summary and fetch responses run in one connection-owned responder, outside the
sole socket reader. Each has its own bounded queue slot, and ready fetches take
priority: busy summaries cannot consume capacity reserved for already-advertised
keys. Summary queue admission and actor reads share `local_work_timeout`; busy
or timed-out readonly requests leave the socket available for ACKs and unrelated
fragment RPCs while the sender retries its round. Accepted actor messages and
queued response frames retain their exact peer authority until completion even
after cancellation. Session, round, subtree generation and serving/recovery
authority are rechecked before asynchronous results authorize or emit data.

Repairs use untracked batches followed by a tracked durability barrier. A
failed untracked apply poisons its session's barrier; it cannot certify the
successful prefix. A changed cache revision, transferred repair or reset epoch
requires a fresh verification pass. Completion means a revision-stable pass and
its actual requested fences, not global byte equality or a measured percentage.

Round failure rotates the session and retries with bounded backoff. Reconnect,
periodic anti-entropy, reset recovery, bootstrap and placement handoff use the
same engine.

## Placement and explicit retirement

A new assigned fragment remains non-serving until an already-serving source
completes its verified handoff. An unavailable source does not authorize empty
authority. Controllers own overlap sequencing and application writer authority.

A retirement fence freezes the relevant replica set and source revision,
settles bounded ingress, requires full stable reconciliation and the requested
ACK floor from every target, then quiesces the source. A generation/target
change, fetched winner, weak ACK or failed backend keeps retirement incomplete.

`shutdown_with_replication_drain` establishes its per-target contract before
local shutdown. Cancellation/timeout retains issued work for exact retry;
ordinary `shutdown` is the explicit local-only boundary. Reports are LWW version
floors, not consensus/quorum election, host-death proof or writer leases.

## Frame, socket and ownership limits

- Frames are four-byte big-endian lengths plus protobuf envelopes, at most
  **16 MiB**. Builders target **1 MiB** ordinary/anti-entropy pages.
- Preflight bounds repeated fields and rejects ambiguous or unsupported
  messages before protobuf allocation.
- Writer data and response lanes share count/byte budgets. A maximum response
  is reserved from ordinary producers; immediate reader response admission
  never waits behind the reader's own writer.
- Exact frame permits survive queueing, socket writes and terminal drop.
  After at most 16 consecutive responses, ready data gets a writer turn.
- Raw receive bytes are reserved before allocation and stay owned through
  decoding and handler work. This is not a total process/cache/actor heap bound.
- One FIFO connection credit fairly schedules reconciliation rounds.
- Forwarded operations/relays have separate bounded ownership. Accepted actor
  and queued response work remains owned after caller cancellation; old physical
  cleanup cannot consume replacement attempts.
- Socket/admission deadlines close the contributing connection and let the owned
  dialer reconnect. Idle connections have no body-read deadline until a prefix
  arrives.

Full shutdown joins accepted request/response/receive/apply/storage ownership
before releasing the Node's store. Local drain is distinct from remote evidence.

See [configuration](configuration.md), [backpressure](backpressure.md),
[draining](draining.md), [write evidence](write-concerns.md) and
[replacement safety](replacement-safety.md).
