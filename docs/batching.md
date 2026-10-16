# Multi-key batching

`put_multi` / `get_multi` (and the `_with_priority` variants) are
**fan-out conveniences**, not transactions. They reduce round trips by
grouping keys by destination; each key still succeeds or fails independently,
and results come back in input order.

## How fan-out works

1. Each key is hashed to its fragment (`seahash(key) % fragment_count`).
2. Local keys (this node serves the fragment) run through the normal single-key
   path — including the exact per-op apply outcome — in input-indexed windows
   of at most 1024 concurrent calls, then are stamped back into their input
   positions. The window prevents an unbounded caller vector from filling the
   shared dispatcher mailbox before worker-pool admission can shed it.
3. Remote puts are split when either the exact request or a conservative
   worst-case response would cross the 1 MiB soft target, or at 4096 items.
   One individually valid item may exceed the soft target but never the
   16 MiB hard frame limit.
4. Remote get requests use the same exact 1 MiB request target and at most 256
   keys. The 4096-item wire ceiling remains a defensive pre-decode limit, not
   the producer chunk size.
5. Put responses must match the request's cardinality, key order, and explicit
   request indices before results are mapped into the input-aligned `Vec`:
   `Vec<Result<(), NodeError>>` for puts,
   `Vec<Result<Option<VersionedValue>, NodeError>>` for gets.

## Priority and traffic isolation

- Plain `put_multi`/`get_multi` run at **`OpPriority::Background`** — they are
  bulk APIs by default.
- The `_with_priority` variants take an explicit priority, and the priority
  is carried on the wire (`BatchPut.priority` applies to every put in the
  batch; `BatchGet.priority` likewise). The remote owner executes at the
  caller's priority — "fan-out preserves traffic isolation end-to-end."
- Priority chooses the destination actor pool. It does **not** prioritize the
  per-peer outbound data lane; replication, high-priority, and background
  producer frames contend there. Reader-generated protocol responses use a
  separate writer-priority lane, independent of `OpPriority`.

Remote fan-out is currently sequential: the caller awaits one fragment group
and byte/count chunk at a time, and each chunk's replica holders are attempted
one at a time in placement order. A slow attempt therefore delays later
holders, chunks, and remote fragments.

Serving single/batch reads use bounded Node-wide priority-separated job queues,
not inline socket-reader actor waits. Concurrent requests may complete out of
order across request ids; each batch prefix still preserves its own input order.
There are eight fixed serving workers and 64 queue slots per priority by default,
plus a separate 64 MiB retained decoded-key budget per priority. Cancellation
keeps admission charged until actors actually release the keys. See
[backpressure](backpressure.md#node-owned-serving-reads) for the exact bounds
and the mutation/reconciliation handlers that remain inline.

## Remote puts: exact outcomes and bounded failover

For each remote fragment, the node iterates `replica_order(frag)` — itself
first if it serves the fragment — and sends the `BatchPut` to the first
holder with a live peer channel. The origin registers the expected peer, sends
one request id, and waits for one ordered `MutationResult` per put. `Ok(())`
means the serving worker returned `Applied` or the idempotent
`AlreadyPresent`; backpressure, storage failure, expiry, and supersession are
returned per key.

The producer incrementally measures both the exact `BatchPut` request and a
worst-case `MutationResponse`. Peer diagnostics are capped at 1024 UTF-8 bytes,
and record provenance ids use the cluster's 1-to-255-byte identity bound, so
that response estimate is authoritative. A peer preflights the complete batch
before observing its timestamp, relaying it, or applying any item.

Each attempt has one **configured total deadline (5 seconds by default)
covering both outbound queue admission and the reply**. A closed connection,
timeout, dropped response, or invalid cardinality/order retries the same
stamped versions at the next holder.
LWW application makes that retry idempotent. Cancellation removes the pending
request, and late or wrong-peer responses are ignored.

The peer queue separately defaults to a 5-second admission timeout. If queue
admission itself expires, the node cancels that connection so its dialer can
reconnect; if the total attempt expires while waiting for a response, the
pending request is removed and failover continues without necessarily dropping
the connection.

Transport, timeout, and malformed responses retry the current unresolved
request at the next holder. After a fully validated response, only positions
with typed `Unavailable` retry; all other per-key outcomes are retained and
excluded from later attempts. This also handles a successful prefix followed
by actor loss. Filtering preserves duplicate positions and input order, keeps
every mutation's original stamp, and restarts response indexes at zero for
each reduced request. If holders are exhausted, only unresolved positions get
`NodeError::Network(..)`. A fragment with no usable replica is a configuration
`NodeError::Actor(..)`.

Single-key `put` uses the same per-owner deadline and exact mutation result. It
retries transport, timeout, malformed-response, and typed `Unavailable`
failures across `replica_order`, carrying the original timestamp, origin, boot
id, and sequence on every attempt so a lost response remains an idempotent
retry. Any other valid typed result is terminal.

## Remote gets: timeout + failover

Remote gets carry a `req_id` and are awaited with the same **configured total
per-owner-attempt timeout for enqueue plus every prefix response** (5 seconds
by default). A peer cannot extend that deadline by returning one result at a
time. On timeout or a dropped reply channel, the pending request is removed and
the node fails over to the next replica holder for only the uncompleted suffix.
A key fails only when no replica holder answers:
`NodeError::Network("no replica answered batch get for fragment {frag} (tried ...)")`.
"A dead owner fails only its keys, and only when no replica holder answers."

A non-empty `BatchGet` receives either an empty *unavailable* response or a
non-empty exact ordered prefix of its requested keys. Empty never means
not-found. The caller validates the expected connection-bound peer and every
prefix position—including duplicate keys—before observing timestamps or completing a
result, then requests the omitted suffix under a fresh id. Missing values are
represented explicitly by a prefix entry with `found = false`.

Remote single-key reads retain `ForwardedGet` so a non-serving holder can
re-forward across temporarily disagreeing placement maps. Direct and relayed
responses are pinned to the authenticated expected peer and validated before
they affect the clock or caller. Because `GetResponse` cannot encode
unavailable, actor or routing failure produces no false `found = false`
response; the bounded attempt expires and the origin tries its next holder.
Relayed reads and mutations additionally pin the exact expected-owner and
origin physical channels. Their shared node-wide count and request-key storage
budgets reject immediately, and disconnect/replacement reclaims only those
exact channels' work. Batch mutation key accounting includes Vec/String storage
and buffer capacities, not mutation values; the outbound writer has its own
encoded-byte budget. Relay timers retain admission until they exit. A rejected
mutation relay returns explicit `Unavailable` for every original position,
preserving key order, duplicates, and operation stamps.

The responder targets 1 MiB. One aggregate `KvMessage::GetBatch` continuation
lazily admits and sticky-routes exactly one ordered key at a time. Its worker
releases that exact pool/worker-generation reservation before returning the
continuation to the dispatcher, so queue-cap and rate-limit accounting remain
per key and a completed key cannot make the next one shed spuriously. Exact
protobuf response sizing—including the request id, nested repeated fields, and
outer envelope—stops the actor page immediately after the one result that
crosses the soft target; an omitted suffix is neither cloned nor admitted. The
network layer repeats the authoritative whole-envelope size check before send.

If the crossing result cannot join the current frame without exceeding the
16 MiB hard limit, the existing prefix is returned and that item is pulled
again alone. That hard-boundary retry can consume admission again and can
therefore return explicit backpressure rather than a false miss. A configured
actor-attempt deadline turns a stopped worker into an empty unavailable reply
instead of blocking the multiplexed peer reader indefinitely. Orderly
dispatcher drain returns the already admitted prefix; the caller can retry the
untouched suffix. Dropping the reply stops further admission after the current
worker completes. One response
frame is emitted per request, so the connection reader never waits on a
multi-frame response stream.

Per-key result mapping in a `BatchGetResponse`:

- entry marked `shed` → `Err(NodeError::Backpressure(..))`;
- `found` with `is_tombstone` → `Ok(None)` (tombstones read as absent);
- explicit `found = false` → `Ok(None)`;
- otherwise → `Ok(Some(value))`.

## Error semantics

`NodeError::Actor` = misconfiguration (no usable replica listed) or a local
actor-framework failure.
`NodeError::Network` = connectivity (no replica reachable).
`NodeError::Backpressure` = shed locally or remotely — **a shed read never
returns `Ok(None)`**, so callers can't mistake backpressure for "not found".
`NodeError::Storage`, `Superseded`, and `Expired` preserve the serving
fragment's exact mutation outcome.

The `multi_key_partial_failure` and
`batch_operations_fail_over_after_a_stale_owner_returns_unavailable`
integration tests cover disconnected and live-but-stale preferred owners for
both single-key and batch operations.
