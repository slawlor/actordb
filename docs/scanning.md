# Scanning

For global ordering/pagination and complete aggregates, use the separate
[SQL query API](query.md). Legacy `Node::scan` retains the per-fragment
limit/offset and collection semantics described here. The application listener
bounds scan collection and reports oversized results explicitly.

A **scan** applies a filter + transform spec to every record in a
fragment's local cache and returns the transformed matches. It is the
execution primitive for distributed map-reduce style queries:
[`Node::scan`](#nodescan-the-distributed-coordinator-api) fans a spec out
across fragments — in-process for local fragments, over the wire for
remote holders — and merges the results.

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
[`Node::scan`] merges across fragments with coordinator failover; a
named-function registry for richer matchers/transformers is the natural
next PR and builds directly on this primitive.

## `Node::scan`: the distributed coordinator API

```rust
use actordb::actor::{KeyMatcher, ScanSpec, ValueTransform};
use actordb::node::{Node, NodeConfig};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let node = Node::start(
    "n1".to_string(),
    "127.0.0.1:0".parse()?,
    NodeConfig::default(),
).await?;
node.put("user:1".to_string(), b"alice".to_vec()).await?;

let matches = node.scan(&ScanSpec {
    matcher: KeyMatcher::Prefix(b"user:".to_vec()),
    transform: ValueTransform::Identity,
    limit: 1000,
    offset: 0,
}).await?;
# Ok(())
# }
```

`Node::scan` is a **server-side coordinator**, exactly like `Node::get`:
the client talks to any one node, and that node coordinates the whole
scan. For each fragment the coordinator scans in-process when it holds
the fragment locally (one `KvMessage::Scan` per fragment, background
pool, fanned out concurrently); otherwise it forwards a `ScanRequest`
to one replica holder from `replica_order` — the same pattern as
`ForwardedGet` — retrying across holders on failure. Exactly one holder
is contacted per fragment, so replicas (which converge via LWW) never
double-count.

Results merge in fragment order (each fragment's matches in
lexicographic key order), truncated at `MAX_SCAN_TOTAL_MATCHES`
(100,000) as a coordinator-side safety net; the per-fragment
`ScanSpec::limit` applies first.

### Full replication: scans are local

actordb's default is **full replication**: when no `fragment_map` is
configured, every node serves every fragment. A scan is therefore local
to the requesting node in the default configuration — no network round
trips, no cross-node coordination, no distributed failure modes.

### Failure policy: fail, don't go partial

If no holder is reachable for a fragment — not served locally and no
replica holder answered — the whole scan fails with
[`ScanError::FragmentsUnavailable`] naming the fragments. **v1 never
returns silent partial results.** A holder that refuses a scan (not
serving, bad spec, shed under backpressure) triggers failover to the
next holder, exactly like `ForwardedGet`.

### Wire size

The scan response is a single envelope per fragment, so the
per-fragment `limit` is also the wire-size safety: `limit` × average
value size must stay comfortably under the 16 MiB frame cap. Keep limits
tight and prefer `KeyOnly`/`PrefixBytes` transforms for wide scans.

## Future work

If a single host holds many fragments, should it serve the entire scan
itself, or should the coordinator fan out scan work across the fleet to
distribute fragment load? Currently the coordinator scans all local
fragments itself and only forwards fragments it doesn't hold. A future
iteration could distribute per-fragment scan work across replica holders
for load balancing.

[`KeyMatcher`]: https://docs.rs/actordb/latest/actordb/actor/enum.KeyMatcher.html
[`ValueTransform`]: https://docs.rs/actordb/latest/actordb/actor/enum.ValueTransform.html
[`Node::scan`]: https://docs.rs/actordb/latest/actordb/node/struct.Node.html#method.scan
[`ScanError::FragmentsUnavailable`]: https://docs.rs/actordb/latest/actordb/node/scan/enum.ScanError.html
