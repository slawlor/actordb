# Metrics

`Node::metrics_snapshot()` returns owned observations without choosing a metrics
backend. `Node::collect_metrics(&sink)` collects the complete bounded observation
first, releases all node/report locks, and then calls `MetricsSink::record` for
each point. A closure implements the sink trait; an adapter can translate points
to a application's recording backend. See
[`metrics_snapshot.rs`](../actordb/examples/metrics_snapshot.rs).

Each point has a stable name, description, unit, kind, value, and scope:

| Kind | Recording semantics | Examples |
| --- | --- | --- |
| Counter | Cumulative amount for one node incarnation; collection does not reset it | Accepted/completed/rejected admission slots, peer writer saturation, drain outcomes/duration, TTL callback events |
| Gauge | Current approximate value; it can decrease | Active ownership slots, retained bytes, storage jobs, per-fragment serving, per-sender queues |
| Distribution | Cumulative sample count, sum, and maximum; no quantiles or raw samples | TTL expiry-to-dispatch and monotonic queue-to-dispatch latency in milliseconds |

Replace cumulative backend observations or derive deltas. Adding an observed
cumulative counter to a backend counter on every collection double counts it.
Use `snapshot.status.boot_id` to detect a node incarnation change. Sender
reconcile round totals are **gauges**: replacing a sender resets its total. The
session ID stays status metadata and is never a metric label. Both cumulative
amounts and distribution summaries can saturate rather than overflow.

Admission counters and `actordb.node.active_requests` count request/actor gate
ownership slots. A batch can own several slots. Completed slots do not mean
successful user writes, and rejected slots count the lifecycle admission fence,
not every validation failure, queue saturation, or application error. Receive
apply activity counts decoded replication frames, not unique mutations. Bounded
local drain/shutdown calls expose started, completed, timed out, failed, and
cancelled outcomes, plus cumulative elapsed milliseconds. These call outcomes
do not assert peer durability.

Scopes are node, fragment, and fragment/peer. Keys, values, request IDs, error
strings, and session IDs are excluded. Cardinality is bounded by configured
fragments and live replication targets, and collection independently caps the
records copied. Dynamic peer identities can still grow backend series over
time; exporters should apply their retention/cardinality policy.

Default limits copy up to 256 fragments, 256 senders, and 64 replica IDs per
fragment. `metrics_snapshot_with_limits(SnapshotLimits::new(...))` permits up to
4,096 fragments and senders, or smaller/zero limits. The snapshot contains at
most a fixed set of node points, two points per observed fragment, and five
points per observed sender. Work and memory do not scale with stored records.
Reading configured replica assignment may scan the corresponding configured
replica list; snapshot limits bound copied output, not configuration validation.

All report/placement acquisitions use `try_lock`/`try_read`; collection does not
wait for shutdown, actor execution, storage, or replication. Check
`snapshot.status.observation`: missing or truncated sections are explicit and
their points are omitted. Unknown readiness omits `actordb.node.ready`; it does
not emit zero. A disabled TTL callback system omits TTL metrics. Individual
gauges and atomic cumulative summaries are independent approximate reads, not
a transaction across subsystems. A distribution's count/sum/max can change
between its component reads while callbacks run.

This initial API reuses existing diagnostics and cheap atomic admission/drain
counters. Collection adds no request-path callback or record scan. It does not
claim per-operation latency, all queue lengths/timeouts, store latency, durable
lag, catch-up percentages, or allocator/cache resident bytes; those require
additional measurements. Retained-byte charges describe their documented
budgets, not total process memory. A successful TTL callback means its handler
returned success, not that an external recipient acknowledged a notification.

Sinks execute synchronously in the collector's caller and may read node status
or other diagnostics. Slow or panicking sinks affect that caller; ActorDB starts
no background exporter. Snapshot first if exporting through an application-owned
bounded queue. Status and lifecycle evidence are described in [status.md](status.md).
