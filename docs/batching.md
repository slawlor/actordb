# Multi-key batching

`put_multi` / `get_multi` (and the `_with_priority` variants) are
**fan-out conveniences**, not transactions. They reduce round trips by
grouping keys by destination; each key still succeeds or fails independently,
and results come back in input order.

## How fan-out works

1. Each key is hashed to its fragment (`seahash(key) % fragment_count`).
2. Local keys (this node serves the fragment) run through the normal single-key
   path — including the per-op read-your-write barrier — concurrently via
   `join_all`, then are stamped back into their input positions.
3. Remote keys are grouped by `FragmentId` and each group goes out as **one**
   `BatchPut` / `BatchGet` message to the fragment's owner — one message per
   fragment, not per key.
4. Results are mapped back by key into a `Vec` aligned with the input order:
   `Vec<Result<(), NodeError>>` for puts,
   `Vec<Result<Option<VersionedValue>, NodeError>>` for gets.

## Priority and traffic isolation

- Plain `put_multi`/`get_multi` run at **`OpPriority::Background`** — they are
  bulk APIs by default.
- The `_with_priority` variants take an explicit priority, and the priority
  is carried on the wire (`BatchPut.priority` applies to every put in the
  batch; `BatchGet.priority` likewise). The remote owner executes at the
  caller's priority — "fan-out preserves traffic isolation end-to-end."

## Remote puts: failover, no end-to-end ack

For each remote fragment, the node iterates `replica_order(frag)` — itself
first if it serves the fragment — and sends the `BatchPut` to the first
holder with a live peer channel, **failing over across replica holders until
one accepts the batch**. This is fire-and-forget: once a holder accepts the
batch, the key's result is `Ok(())` with **no end-to-end acknowledgement**
that the write applied. At-least-once delivery to the owner, nothing more.

A remote key fails only when:

- no replica holder is connected → `NodeError::Network(..)`, or
- the fragment has no usable replica (misconfiguration) → `NodeError::Actor(..)`.

Note the asymmetry with single-key `put`: the single-key path forwards once to
`pick_replica` (the first in `replica_order`) and does *not* fail over — it
returns `Ok` after the send, or `NodeError::Network` if the send fails.

## Remote gets: timeout + failover

Remote gets carry a `req_id` and are awaited with a **5-second per-attempt
timeout**. On timeout or a dropped reply channel, the pending request is
removed and the node fails over to the next replica holder. A key fails only
when no replica holder answers:
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

The `multi_key_partial_failure` integration test covers the headline case: a
disconnected owner fails only its keys while the rest succeed.
