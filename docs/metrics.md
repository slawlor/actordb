# Metrics

See [complete observations](observability.md) for the 0.9.0 detailed actor
collector, scoped pool/worker counters, source lifetimes and sampling bounds.
`enable_metrics()` records future request/storage/pool/sender timing;
`enable_table_metrics(&TableId)` deliberately adds a bounded table scope.
Timing disabled or unobserved before enabling remains unavailable.

`Node::metrics_snapshot()` returns owned observations without choosing a metrics
backend. `Node::collect_metrics(&sink)` collects the complete bounded observation
first, releases all node/report locks, and then calls `MetricsSink::record` for
each point. A closure implements the sink trait; an adapter can translate points
to a application's recording backend. See
[`metrics_snapshot.rs`](../actordb/examples/metrics_snapshot.rs).

Each point has a stable name, description, unit, kind, value, and scope:

| Kind | Recording semantics | Examples |
| --- | --- | --- |
| Counter | Cumulative for the descriptor's source lifetime; never reset by collection | Admission/drain/concern outcomes, actual storage completions/errors, peer frame rejections, TTL events and detailed pool work |
| Gauge | Current approximate value; it can decrease | Queued/active/awaiting-response ownership, retained bytes, storage jobs, serving capacity and replication activity |
| Distribution | Cumulative sample count/sum/max; no quantiles or raw samples | Enabled request/storage/concern/pool/worker durations in nanoseconds; TTL durations in milliseconds |

Replace cumulative backend observations or derive deltas. Adding an observed
cumulative counter to a backend counter on every collection double counts it.
Use `snapshot.status.boot_id`, descriptor lifetime and `source_incarnation` to
detect node/shared-storage/sender/pool/worker reset identity. Sender
reconcile round totals are **gauges**: replacing a sender resets its total. The
session ID stays status metadata and is never a metric label. Both cumulative
amounts and distribution summaries can saturate rather than overflow.

`actordb.replication.connected` is a Boolean fragment/peer gauge: one for an
observed ready sender session, zero for an observed disconnected sender. Unknown,
reset-only, or contended reports omit the point rather than assert disconnection.
Its sender-incarnation metadata changes when the sender is replaced; connection
does not certify replica equality or durability.

Admission counters and `actordb.node.active_requests` count request/actor gate
ownership slots. A batch can own several slots. Completed slots do not mean
successful user writes, and rejected slots count the lifecycle admission fence,
not every validation failure, queue saturation, or application error. Receive
apply activity counts decoded replication frames, not unique mutations. Bounded
local drain/shutdown calls expose started, completed, timed out, failed, and
cancelled outcomes, plus cumulative elapsed milliseconds. These call outcomes
do not assert peer durability.

Scopes include node, deliberately registered table, fragment, fragment/peer,
and bounded detailed pool/worker records. Keys, values, request IDs, error
strings, and session IDs are excluded. Cardinality is bounded by configured
fragments and live replication targets, and collection independently caps the
records copied. Dynamic peer identities can still grow backend series over
time; exporters should apply their retention/cardinality policy.

Default limits copy up to 256 fragments, 256 senders, and 64 replica IDs per
fragment. `metrics_snapshot_with_limits(SnapshotLimits::new(...))` permits up to
4,096 fragments and senders, or smaller/zero limits. The snapshot contains at
most fixed per-scope point families within the selected record caps. Detailed
calls separately cap global pool/worker records and one absolute deadline.
Work and memory do not scale with stored records.
Reading configured replica assignment may scan the corresponding configured
replica list; snapshot limits bound copied output, not configuration validation.

Fast report/placement acquisitions use `try_lock`/`try_read`; fast collection does not
wait for shutdown, actor execution, storage, or replication. Check
`snapshot.status.observation`: missing or truncated sections are explicit and
their points are omitted. Unknown readiness omits `actordb.node.ready`; it does
not emit zero. A disabled TTL callback system omits TTL metrics. Individual
gauges and atomic cumulative summaries are independent approximate reads, not
a transaction across subsystems. A distribution's count/sum/max can change
between its component reads while callbacks run.

The complete API reports node admission/response/queue pressure, actual storage
queue/execution timing and errors, write-concern outcomes/waits, peer authority
frames and completion, and detailed pool/worker work. Synchronous collection
reuses owned state; detailed calls add bounded local RPCs and invoke collectors
after every registry/actor request is released. No record scan or hot-path user
callback is installed. Remote durable lag, global catch-up percentages and
allocator/cache resident bytes remain unmeasured. Retained-byte charges describe their documented
budgets, not total process memory. A successful TTL callback means its handler
returned success, not that an external recipient acknowledged a notification.

Sinks execute synchronously in the collector's caller and may read node status
or other diagnostics. Slow or panicking sinks affect that caller; ActorDB starts
no background exporter. Snapshot first if exporting through an application-owned
bounded queue. Status and lifecycle evidence are described in [status.md](status.md).
