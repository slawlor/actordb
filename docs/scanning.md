# Scanning

A **scan** applies a filter + transform spec to every record in one
fragment's local cache and returns the transformed matches. It is the local
execution primitive for distributed map-reduce style queries: a future
network layer fans a spec out across fragments and merges the results, but
each shard's work is exactly what is described here.

## The spec is data, not code

```rust
use actordb::actor::{KeyMatcher, ScanSpec, ValueTransform};

let spec = ScanSpec {
    matcher: KeyMatcher::Prefix(b"user:".to_vec()),
    transform: ValueTransform::KeyOnly,
    limit: 1000,
    offset: 0,
};
```

* [`KeyMatcher`] selects keys: `All`, or `Prefix(bytes)` for key-prefix
  scans.
* [`ValueTransform`] shapes the returned values: `Identity` (full value
  bytes), `KeyOnly` (keys only — values are empty), or `PrefixBytes(n)`
  (first `n` bytes of each value).
* `limit` caps the matches returned; `0` means "no explicit limit" and
  falls back to a server-side default (`DEFAULT_SCAN_LIMIT` = 10,000).
  The bound is load-bearing: an unbounded full-table scan is an OOM
  vector on the reply path.
* `offset` is a skip-first-N pagination cursor over the deterministic
  result order. Page `k` of size `n` uses `offset = k * n`, `limit = n`.

Rust closures cannot cross a process boundary, so the spec is deliberately
a small serializable enum set rather than shipped functions. This covers
the common shapes — full scans, prefix scans, key listings, projections —
which is most of what map-reduce over a KV store actually does.

## Semantics

* **Visibility matches point reads.** Tombstones and TTL-expired values
  are hidden, exactly like `get()`. A deleted key is not a match.
* **Deterministic order.** Matches come back in lexicographic key order
  (the fragment's ordered key index), so `offset` pagination is stable as
  long as the key set does not change between pages.
* **Single pass, fused.** Filter and transform happen inline under one
  read lock; only matches are materialized. The scan never clones the
  whole cache to filter it afterwards.
* **No snapshot isolation.** The scan holds the fragment's read lock for
  its pass, so concurrent *writes* to that fragment pause for the scan's
  duration (concurrent reads are unaffected). Each fragment is read at its
  own instant. This matches the system's AP/eventual-consistency
  contract — callers merging multi-fragment results must not assume a
  global linearizable snapshot.

## Scheduling and backpressure

Scans are dispatched as `KvMessage::Scan` to the fragment's **background
pool** (`OpPriority::Background`, documented as "bulk/scan traffic"), so a
scan can never starve latency-sensitive reads and writes. Routing is
non-sticky round-robin — a scan has no key, so concurrent scans spread
across the background workers.

Admission control is the same as every other op: when the background pool
is at its queue cap or rate limit, the scan is **shed explicitly** with an
error, never silently dropped and never confused with "no matches".

## Cost model

A scan is a heavyweight op by design: it walks the fragment's key index
and holds the read lock while doing so. Keep `limit` tight, prefer
`Prefix` matchers over `All` when you can, and page large extracts with
`offset` rather than raising the limit. If write stalls during scans ever
become a problem, the follow-up is a chunked scan that drops and
re-acquires the read lock between batches (which trades snapshot
stability across chunks — a v2 design decision, not v1).

## What's next

The v1 primitive is map-only: filter + transform + collect per shard.
Client-side reduction over the returned matches covers count/sum/top-K.
A distributed fan-out (one owner per fragment, result merging) and a
named-function registry for richer matchers/transformers are the natural
next PRs; both build directly on this primitive.

[`KeyMatcher`]: https://docs.rs/actordb/latest/actordb/actor/enum.KeyMatcher.html
[`ValueTransform`]: https://docs.rs/actordb/latest/actordb/actor/enum.ValueTransform.html
