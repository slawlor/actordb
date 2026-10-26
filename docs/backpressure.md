# Traffic classes & backpressure (Phase 7)

Every named table on a fragment runs **three isolated worker pools** instead of
sharing one CRUD pool with its neighbors. Operations are classified by
`OpPriority` and each table-local pool has its own workers, worker backlog, rate
limiter, and shed counter. Bulk work cannot consume the workers or queue
capacity reserved for latency-sensitive traffic, and a busy table cannot
fractionally consume another table's CRUD throughput. The dispatcher mailbox,
replication, placement, and peer framing/writer lanes remain shared, so this is
worker isolation rather than a strict end-to-end priority scheduler.

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

To override a pool, use a nested Bon builder:

```rust
use actordb::actor::PoolConfig;
use actordb::node::NodeConfig;

let config = NodeConfig::builder()
    .background_pool(PoolConfig::builder()
        .workers(2)
        .queue_cap(8_000)
        .rate_limit_per_sec(1000)
        .build())
    .build();
```

The nested builder replaces the whole profile. Its standalone defaults are
four workers, cap 10,000, and no limit, so preserve the Background rate
explicitly when replacing that pool. `.maybe_rate_limit_per_sec(None)` is an
intentional unlimited setting, not an instruction to inherit the Node profile.
Omitting `background_pool` keeps the original profile unchanged. See
[Configuration builders](configuration.md#configuration-builders).

## Admission and shedding

The dispatcher's internal table-pool reservation checks the token bucket first,
then the queue cap. `queue_cap` counts jobs handed from the dispatcher to one
priority pool for one table, including active and worker-mailbox jobs; it does
not bound the shared dispatcher mailbox. Admission increments a pool total and
the exact sticky worker's generation counter. Completion or immediate send
failure releases both, while supervision reclaims the exact terminated
generation's remaining reservations before publishing its replacement. When rate or
capacity admission fails, the op is **shed**: dropped, **counted** in the
pool's shed counter, never silent. Worker unavailability instead drops the
reply port for reply-bearing calls and is surfaced as an actor/network
failure, not backpressure. Fire-and-forget actor messages have no result
channel to report that failure.

Local `put_multi` fan-out is issued in windows of at most the smaller of 1024
and `embedded_operation_capacity`; `get_multi` uses windows of at most 1024
calls. This is a pre-admission bound: it prevents an arbitrary caller vector
from queueing without limit in the shared dispatcher mailbox, while every item
still consumes its normal pool token and exact worker-generation reservation.
An already-admitted local write batch waits fairly for its point-completion
slots, bounded by `local_work_timeout` and shutdown. It does not reserve a set
of slots permanently or overload itself when a previous reply wakes before
its completion owner releases the slot. External point and whole-batch
count/byte admission remain nonwaiting. A point which times out before slot
admission reports backpressure and does not apply; an already-submitted
mutation retains its exact completion ownership after its user waiter ends.
Remote `BatchGet` uses one aggregate continuation but admits only its next
ordered key. A worker releases the completed reservation before the
continuation can reserve another, and exact encoded-byte sizing stops before
any omitted suffix is admitted. Orderly dispatcher drain returns the completed
admitted prefix; caller cancellation retains its current reservation until
worker completion and prevents further suffix admission.

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

Serving forwarded single puts and whole batches additionally use a node-owned
completion queue: 128 aggregate jobs and 64 MiB retained request/RPC payload by
default, configurable through `NodeRuntimeOptions`. Admission never waits and
returns ordered `Unavailable` before any apply when either budget is full.
Eight workers per priority reserve execution capacity, but count/byte admission
is shared: Background overload can reject High, so this is not strict
end-to-end priority isolation. Payload accounting includes request headers,
allocated String/Vec backing storage, and the largest single actor RPC clone
plus its correlation key; it excludes response metadata, caches, sender copies,
raw frames and allocator overhead. Accepted work retains its operation and
placement leases through exact outcome handling and fanout even after reader
cancellation. The reader still awaits completion before reading the next frame;
replicated/reconciled applies remain inline and are not covered by this owner.

Wire-size admission is a separate safety boundary from queue depth. Before a
mutation is applied locally, actordb verifies that the record can fit every
required single-record protocol envelope. Summary, fetch, data, repair, and
ordinary replication builders maintain incremental protobuf size accounting
from exact repeated-field deltas instead of rescanning each growing page.
Fetch chunks conservatively reserve the largest sequence id, and data pages
reserve the terminal `done` field before admitting entries. Every completed
envelope is still preflighted with Prost before admission and encoding; pages
target 1 MiB and the 16 MiB hard frame limit remains authoritative. This
prevents an irreducible record from wedging replication. The 256-frame peer queue is complemented by
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

Raw inbound bodies additionally share a node-wide 64 MiB receive budget across
accepted and outbound-dialed connections, including the handshake. The reader
checks the 4 KiB handshake or 16 MiB established-frame limit, then reserves all
announced bytes before allocating the body. A full budget rejects that
connection immediately, without waiting for capacity on the sole socket
reader. The reservation remains attached through decoding and the inline
handler turn, and every completion/error/timeout/cancellation/drop releases it.
This prevents incomplete bodies from multiplying the socket-count limit into
gigabytes of receive allocations. It does not bound protobuf object overhead,
decoded/actor/cache/relay/caller copies, kernel socket buffers, or total heap
use. `with_receive_byte_capacity` configures this encoded-body budget and must
fit at least one maximum frame. Request execution remains inline.

Nodes coordinate scans with bounded `QueryPageRequest`/`QueryPageResponse`
pages; they never originate peer `ScanRequest`. Consequently every incoming
peer `ScanResponse` is unsolicited. After checking the complete outer envelope
identity, length, and oneof structure, an established connection discards that
borrowed response body **before Prost decoding**: no match Vec, row headers,
keys, values, or error string are materialized. This also avoids tiny-row
decoded-object amplification without adding an unused pending registry or
runtime allowance. Before handshake establishment, application responses still
fail closed; malformed/duplicate/unsupported outer envelopes also fail closed.
Raw receive-body allocation remains covered by the independent budget above.
Inbound `ScanRequest` serving and its bounded outgoing `ScanResponse` remain
supported, and discarding an unsolicited response does not close a healthy
session or interrupt subsequent requests.

Non-serving nodes also have aggregate, node-wide relay admission: 1024 live
read/mutation relays and 64 MiB of retained request-key storage by default,
configured with `with_relay_capacity` and `with_relay_key_byte_capacity`.
Key charges include String headers/buffer capacities and the batch Vec header
and allocated String slots, including unused capacity. Mutation values, decoded
frames, writer buffers, caches, and allocator overhead are outside this key
budget; fixed relay/timer overhead is separately count-bounded. Admission never
waits. Saturation or a lost owner yields explicit mutation `Unavailable`; current
single reads stay unavailable and expire at the origin instead of falsely
reporting a miss. Registration/enqueue cancellation rolls back the exact relay
generation, and a timer retains admission until its future exits. Disconnect
and replacement cleanup is pinned to exact physical origin/owner channels, so
an old socket cannot remove replacement work under the same peer id. Serving
mutation apply and post-apply replication ownership are unchanged.

The current custom pool does **not** provide lossless worker restart. Accepted
jobs active or queued in a dead worker are dropped, their reply ports close,
and they are not replayed; supervision does reclaim their reservations, so a
replacement starts with its full configured capacity. Orderly shutdown is
different: it closes every worker mailbox, drains the work accepted before
that fence, and joins every worker before the final fragment flush. Replacing
the remaining custom lifecycle with ractor `Factory` admission and an explicit
discard/requeue policy is still required for lossless restart.

A handled storage error is a request outcome and does not trigger worker
restart. The worker returns that error, retains the authoritative persistence
state, and continues its admitted mailbox. This includes backend panics caught
by the storage executor; actual actor failures retain the restart behavior above.

## Node-owned serving reads

Serving `ForwardedGet` and `BatchGet` jobs leave the socket reader through
three independent Node-wide priority queues. Each class defaults to 64 queued
jobs, eight fixed workers, and a separate 64 MiB decoded-key-byte budget.
Admission is nonwaiting: capacity exhaustion returns explicit shed (one ordered
position for a batch); unavailable actors/routes return an empty batch prefix
or silence for the single-read protocol. The configured forwarded
attempt deadline includes queue wait and actor wait, without resetting on
dequeue. Responses use the request's exact physical channel, and stale route
incarnations cannot publish authoritative reads.

Count admission includes queued, executing, and actor-retained requests. An
opaque lease follows keys into the actor continuation, so timeout or socket
cancellation cannot admit new keys while the old actor still owns them. Raw
receive bodies and their permits drop with the nonwaiting reader turn, rather
than following the decoded job; the separate key-storage lease follows it. Raw
frame bodies, returned values, cache storage, and encoded writer queues are
separate allocations, not covered by this decoded-key budget. The storage charge
includes String buffer capacities, Vec capacity times String header size, and
the Vec header, including spare capacity; fixed job metadata and allocator
overhead are excluded and count-limited. At most 24 read
workers build responses concurrently; existing per-connection frame/byte
queues bound encoded output separately. This is not a Node-wide total response
or heap guarantee. Service admission sheds are not fragment-pool shed counts.

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
serving health/supervision messages. An actor-routed batch read follows the
same rule: only the current key occupies a worker, and the dispatcher never
awaits it. The shared dispatcher ingress, cache locks, snapshot/flush handlers,
and peer framing/writer lanes are not isolated. Serving single/batch read
execution is isolated by the Node-wide service above, keeping ACK and response
traffic moving while those actors are slow. Mutation, replication, and
reconciliation handlers still await work inline and can stall their connection
reader; this slice does not make every protocol handler independently scheduled.
Replication sender lookup also uses a node-global registry, but its critical
section is limited to map lookup/filtering and cloning cheap exact-task
handles. Potentially large mutation payload clones, bounded sender-mailbox
admission, per-sender report waits, and placement-handoff joins run after
releasing that registry; stale snapshots are identity-checked before they may
close a multiplexed peer connection or certify teardown. Lookup still scans
the global map; per-fragment indexing remains a separate scaling follow-up.
`background_saturation_leaves_high_priority_p99_unaffected` covers ordinary
load, `blocked_background_get_does_not_stall_high_priority_or_health` and
`blocked_background_batch_read_does_not_stall_high_priority_or_health` cover
the dispatcher handoff, and `queue_cap_shed_is_explicit_and_counted` covers
explicit capacity shedding.
