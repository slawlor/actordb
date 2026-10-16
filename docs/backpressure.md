# Traffic classes & backpressure (Phase 7)

Every fragment runs **three isolated worker pools** instead of one shared pool.
Operations are classified by `OpPriority` and each pool has its own workers,
worker backlog, rate limiter, and shed counter. Bulk work cannot consume the
workers or queue capacity reserved for latency-sensitive traffic. The
dispatcher mailbox and each peer connection reader remain shared serialization
points, so this is worker isolation rather than a strict end-to-end priority
scheduler.

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
`get_multi`) can't exceed 1000 ops/s even when idle
capacity exists elsewhere.

## Admission and shedding

The dispatcher's internal pool reservation checks the token bucket first, then
the queue cap. `queue_cap` counts jobs handed from the dispatcher to one
priority pool, including active and worker-mailbox jobs; it does not bound the
shared dispatcher mailbox. Admission increments a pool total and the exact
sticky worker's generation counter. Completion or immediate send failure
releases both, while supervision reclaims the exact terminated generation's
remaining reservations before publishing its replacement. When rate or
capacity admission fails, the op is **shed**: dropped, **counted** in the
pool's shed counter, never silent. Worker unavailability instead drops the
reply port for reply-bearing calls and is surfaced as an actor/network
failure, not backpressure. Fire-and-forget actor messages have no result
channel to report that failure.

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
  replication queue. Each connection awaits replication and reconciliation
  applies sequentially, and each outbound Node sender has bounded mutation and
  control lanes; those count bounds do not constitute a receiver-side byte
  budget across connections.
- **Forwarded client write** → the remote owner returns an ordered typed
  mutation result. A shed single put or batch entry reaches the origin as
  `NodeError::Backpressure`; it is never reported as success.

Wire-size admission is a separate safety boundary from queue depth. Before a
mutation is applied locally, actordb verifies that the record can fit every
required single-record protocol envelope. Summary, fetch, data, repair, and
ordinary replication builders then measure exact encoded size, target 1 MiB
pages, and enforce a 16 MiB hard frame limit. This prevents an irreducible
record from wedging replication. The 256-frame peer queue is complemented by
an independent 32 MiB default encoded-byte budget (configurable through
`NodeRuntimeOptions`). Its frame slots are split evenly between ordinary data
and a writer-priority response lane. Ordinary producers also leave one full
16 MiB maximum frame of the shared byte budget reserved for responses.
Connection-reader responses never await admission: a full response lane closes
the connection for correlated replay, as does shared-byte exhaustion caused by
other queued responses. The producer-only gate cannot consume the reserved
maximum frame. Response priority
is capped at 16 consecutive frames whenever data is ready.
Anti-entropy additionally admits one peer-wide round at a time through a
ticketed FIFO gate, and an
allocation-free protobuf preflight caps repeated decode cardinality before
allocating. Receiver actor mailboxes still count messages rather than payload
bytes.

The current custom pool does **not** provide lossless worker restart. Accepted
jobs active or queued in a dead worker are dropped, their reply ports close,
and they are not replayed; supervision does reclaim their reservations, so a
replacement starts with its full configured capacity. Orderly shutdown is
different: it closes every worker mailbox, drains the work accepted before
that fence, and joins every worker before the final fragment flush. Replacing
the remaining custom lifecycle with ractor `Factory` admission and an explicit
discard/requeue policy is still required for lossless restart.

## Observability

`FragmentHealth.shed_counts` is `(u64, u64, u64)` ordered
**(high, default, background)** — explicit, counted, never silent. Watch the
background counter under bulk loads and the default/high counters for
capacity planning; any sustained shedding on `High` means the pool is
undersized.

## Isolation boundary

Worker execution and admitted backlogs are isolated by priority. A worker-bound
`Get` carries the caller's reply port directly, so waiting behind background
work no longer blocks the dispatcher from admitting a high-priority request or
serving health/supervision messages. The shared dispatcher ingress, cache
locks, snapshot/flush handlers, and peer connection reader are not isolated.
`background_saturation_leaves_high_priority_p99_unaffected` covers ordinary
load, `blocked_background_get_does_not_stall_high_priority_or_health` covers
the dispatcher handoff, and `queue_cap_shed_is_explicit_and_counted` covers
explicit capacity shedding.
