# Traffic classes & backpressure (Phase 7)

Every fragment runs **three isolated worker pools** instead of one shared pool.
Operations are classified by `OpPriority` and each pool has its own workers,
queue, rate limiter, and shed counter — so bulk traffic can't starve
latency-sensitive traffic.

## Priorities

```rust
pub enum OpPriority {
    High,       // Latency-sensitive reads/writes.
    #[default]
    Default,    // Normal operations (default).
    Background, // Bulk/scan traffic.
}
```

`put`/`get`/`delete`/`put_with_ttl` use `Default`. `put_multi`/`get_multi`
use `Background`. The `_with_priority` variants let callers choose. On the
wire, priorities travel as a protobuf enum; unknown or absent values degrade
to `Default` — "never misroute to a more aggressive pool" — and forwarding
preserves the originator's priority end to end (see
[Routing & placement](routing-placement.md)).

## Pool configuration

```rust
pub struct PoolConfig {
    pub workers: usize,                    // worker actors in this pool
    pub queue_cap: usize,                  // max admitted-but-unprocessed ops
    pub rate_limit_per_sec: Option<u32>,   // token-bucket rate limit; None = unlimited
}
```

Defaults per `NodeConfig`:

| Pool | workers | queue_cap | rate_limit_per_sec |
|---|---|---|---|
| `default_pool` | 4 | 10 000 | None |
| `high_pool` | 2 | 5 000 | None |
| `background_pool` | 2 | 5 000 | Some(1000) |

The background pool is rate-limited by default: bulk APIs (`put_multi`,
`get_multi`, sync-merge applies) can't exceed 1000 ops/s even when idle
capacity exists elsewhere.

## Admission and shedding

`WorkerPool::try_admit` checks the token bucket first, then the queue cap.
The queue depth is a pool-level `Arc<AtomicU64>` — incremented on admit,
decremented by the worker after each op. When either check fails, the op is
**shed**: dropped, **counted** in the pool's shed counter, never silent.

Shed behavior by operation:

- **Shed read** → the caller gets `Err(NodeError::Backpressure(..))`. A shed
  `Get` never returns `Ok(None)` — "not found" and "shed" are
  indistinguishable nowhere.
- **Shed local write** → the apply reply is
  `Err(NodeError::Backpressure(..))`; the operation was not dispatched to a
  worker and did not apply.
- **Replicated/reconciled write** → currently bypasses admission control. This
  avoids shedding an operation that the receiver must account for before ACK,
  but means the actor mailbox itself is not yet a node-wide bounded
  replication queue. Each connection awaits replication and legacy sync
  applies sequentially, and each outbound Node sender now has bounded mutation
  and control lanes; those count bounds do not constitute a receiver-side byte
  budget across connections.
- **Forwarded client write** → the remote owner returns an ordered typed
  mutation result. A shed single put or batch entry reaches the origin as
  `NodeError::Backpressure`; it is never reported as success.

The current custom pool does **not** provide lossless worker restart. Accepted
jobs already queued in a dead worker's actor mailbox are dropped, and their
shared pending count is never decremented; the leak can consume pool capacity,
not merely one active slot. Shutdown also stops workers rather than proving
that every accepted mailbox job completed. Replacing this lifecycle with
ractor `Factory` admission, discard replies, worker replacement, and explicit
drain is required before treating the queue bound as a production guarantee.

## Observability

`FragmentHealth.shed_counts` is `(u64, u64, u64)` ordered
**(high, default, background)** — explicit, counted, never silent. Watch the
background counter under bulk loads and the default/high counters for
capacity planning; any sustained shedding on `High` means the pool is
undersized.

## The isolation guarantee

The property the design is built around: **background saturation must not move
high-priority p99.** The `background_saturation_leaves_high_priority_p99_unaffected`
integration test asserts exactly this, and
`queue_cap_shed_is_explicit_and_counted` asserts that hitting the cap is
explicit and counted. Worker execution remains isolated by priority, while a
completed local mutation is visible through the shared fragment cache to all
three pools.
