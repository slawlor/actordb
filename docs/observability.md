# Lifecycle, status and metrics

`Node::status_snapshot()` is a bounded synchronous observation. It never waits
for an actor, storage job, shutdown coordinator or asynchronous registry lock.
Contended, truncated and stale sections are explicit in `SnapshotObservation`.
Owned snapshots have `age()`, `is_stale(max_age)` and `is_complete()` helpers;
these observations are not mutation, durability or global ordering barriers.

## Observe startup before construction

`Node::begin_start` and `begin_start_with_options` return an owned `NodeStartup`
handle immediately. `snapshot()` describes validation, storage/transport
binding, fragment initialization and runtime startup. Initialized and required
fragment counts describe construction, not catch-up percentage or authority.
`changed(revision)` waits for a new publication. `wait()` transfers the Node
only in its final synchronous poll and can be cancelled and resumed.

Dropping or cancelling the handle requests cleanup without aborting an in-flight
initializer. The manager retains the Node and retries failed cleanup. A cancelled
startup never advertises Serving, and cleanup completion is reported only after
the owned boundary completes. Failure snapshots retain the reason; cancellation
after handoff is inert. Use the returned Node to observe later lifecycle changes.

| Node lifecycle | Meaning |
| --- | --- |
| Starting / Initializing | Configuration is being validated or initial construction/authority is incomplete. |
| Recovering | Explicit durable recovery or established-node authority restoration is underway. |
| Serving | The initial readiness boundary was established and an authoritative route remains available (or the node is a routing-only role). |
| Draining | Request admission is closed while owned requests complete. |
| Stopping | Full shutdown is progressing through its published coordinator stage. |
| Blocked / Failed | An observed prerequisite or terminal startup/runtime failure prevents progress; read the reason and fragment blockers. |
| Stopped | Joined shutdown completed. |
| Unknown | Available observations cannot establish a current phase. |

The initial readiness boundary is captured by initialization and placement
publication events. Collection never creates it. A fresh learner stays offline
until its declared initial assignments gain authority. An established healthy
node can remain Serving while gaining a new fragment: `capacity_partial`,
`desired_local_fragments`, `serving_fragments` and `available_fragments` expose
that partial capacity. Published serving authority and running dispatcher
capacity are distinct. Limited observations omit an unknowable exact count.

Shutdown stages are published atomically before work begins, so they remain
visible while a call owns the coordinator lock and after its waiter is cancelled.
Fragment activity distinguishes acquiring, serving, transferring, retiring and
stalled work. Missing peer IDs describe connectivity prerequisites, not an
inferred remote serving state. Catch-up percentage stays `None`: the protocol
does not measure a dataset denominator or continuing global equality.

## Replication observations

Sender reports include their local incarnation, publication time, last
successful ACK/reconciliation times, the actual successful ACK durability,
active versus steady or waiting-for-peer activity, and current fetch-chunk
counts. A successful ACK timestamp advances only when that ACK meets the exact
batch fence. Reconciliation success advances only after its verification.
Steady means no observed outstanding work; it does not prove cache equality.

`SnapshotLimits::with_stale_after` controls sender-report freshness. A stale or
never-published report is explicit. Measured retained-operation age is enabled
for new Node submissions by `enable_metrics()`. Untimed history, unobserved
ingress and reset-generated data have unknown ages. Ages describe local queue
ownership; remote LWW lag remains `None` and is never inferred from timestamps,
queue length or completed rounds.

## Bounded detailed actor and pool data

`status_snapshot_detailed(DetailedSnapshotOptions)` additionally performs private
local introspection RPCs after releasing every Node/report registry lock. It
returns real pool and worker ownership, queued/active work, queue/rate shedding,
completion/abandonment and optional timing distributions. Default scope is the
built-in KV table plus deliberately registered metric table names. Explicit
`tables` select other validated names without discovering all lazy table pools.

The options bound fragment/sender/table records, global pool records, global
worker records and one absolute deadline. Limits are at most 4,096 pool records,
4,096 worker records and 256 table names per call. At most 16 detailed calls can
own introspection requests; their leases survive cancelled RPC waiters until
the actor drops the request. Busy admission, contended registries, unavailable
actors, timeouts, absent lazy pools and truncation remain explicit. Unknown
queues are not exported as zero.

Cheap ownership and shedding counters are always available. `enable_metrics()`
adds timing for future work, including future table pools and sender instances.
Queue timing keeps at most 256 queued timestamps per pool. An older untimed job,
sampling-capacity exhaustion or a contended timestamp registry makes oldest
queue age unknown. Distribution summaries provide count/sum/max, not samples,
quantiles or invented historical observations. Worker completion is recorded
after the actual worker future ends and includes terminal errors; actor loss
and failed handoffs are separate abandonment counts.

## Export without choosing a backend

`metrics_snapshot()` exposes the fast subset. `metrics_snapshot_detailed()` adds
the bounded actor data. `collect_metrics` and `collect_metrics_detailed` invoke
a `MetricsSink` only after collection has released all locks and completed its
RPCs, so a sink may safely re-enter diagnostics.

Descriptors define counter/gauge/distribution kinds, units and source lifetimes.
Node, deliberate table, fragment, fragment-peer, pool and worker scopes carry no
key, value, request, error or session labels. `boot_id`, persistence scope IDs
and `MetricPoint::source_incarnation` are reset metadata, not labels. Pool and
worker counters can reset independently while a Node stays alive; replace
cumulative values or compute deltas using the correct source incarnation.

See `actordb/examples/lifecycle_observations.rs` for a backend-neutral collector.
