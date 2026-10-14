# Multi-key batching

`put_multi` / `get_multi` (and the `_with_priority` variants) are
**fan-out conveniences**, not transactions. They reduce round trips by
grouping keys by destination; each key still succeeds or fails independently,
and results come back in input order.

## How fan-out works

1. Each key is hashed to its fragment (`seahash(key) % fragment_count`).
2. Local keys (this node serves the fragment) run through the normal single-key
   path — including the exact per-op apply outcome — concurrently via
   `join_all`, then are stamped back into their input positions.
3. Remote keys are grouped by `FragmentId` and each group goes out as **one**
   `BatchPut` / `BatchGet` message per owner attempt — one message for all of
   that fragment's keys, not one message per key.
4. Put responses must match the request's cardinality, key order, and explicit
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
  shared per-peer outbound frame queue; control, replication, high-priority,
  and background frames contend for the same queue and socket writer.

Remote fan-out is currently sequential: the caller awaits one fragment group
at a time, and each group's replica holders are attempted one at a time in
placement order. A slow attempt therefore delays later holders and later
remote fragments.

## Remote puts: exact outcomes and bounded failover

For each remote fragment, the node iterates `replica_order(frag)` — itself
first if it serves the fragment — and sends the `BatchPut` to the first
holder with a live peer channel. The origin registers the expected peer, sends
one request id, and waits for one ordered `MutationResult` per put. `Ok(())`
means the serving worker returned `Applied` or the idempotent
`AlreadyPresent`; backpressure, storage failure, expiry, and supersession are
returned per key.

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

Transport, timeout, malformed-response, and typed all-`Unavailable` failures
try the next holder; if none succeeds, the key gets `NodeError::Network(..)`.
Other valid typed responses end failover and return their exact per-key
outcomes. A fragment with no usable replica is a configuration
`NodeError::Actor(..)`.

Single-key `put` also waits for the remote exact outcome, with the same
configured send-and-response deadline (5 seconds by default), but currently
selects only the first owner rather than failing over.

## Remote gets: timeout + failover

Remote gets carry a `req_id` and are awaited with the same **configured total
per-attempt timeout for enqueue plus response** (5 seconds by default). On
timeout or a dropped reply channel, the pending request is removed and the node
fails over to the next replica holder. A key fails only when no replica holder answers:
`NodeError::Network("no replica answered batch get for fragment {frag} (tried ...)")`.
"A dead owner fails only its keys, and only when no replica holder answers."

Per-key result mapping in a `BatchGetResponse`:

- entry marked `shed` → `Err(NodeError::Backpressure(..))`;
- `found` with `is_tombstone` → `Ok(None)` (tombstones read as absent);
- otherwise → `Ok(Some(value))`.

## Error semantics

`NodeError::Actor` = misconfiguration (no usable replica listed).
`NodeError::Network` = connectivity (no replica reachable).
`NodeError::Backpressure` = shed locally or remotely — **a shed read never
returns `Ok(None)`**, so callers can't mistake backpressure for "not found".
`NodeError::Storage`, `Superseded`, and `Expired` preserve the serving
fragment's exact mutation outcome.

The `multi_key_partial_failure` integration test covers the headline case: a
disconnected owner fails only its keys while the rest succeed.
