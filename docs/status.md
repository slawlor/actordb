# Node status

See [complete lifecycle and observations](observability.md) for 0.9.0 startup,
blockers, capacity, sender freshness, and bounded actor/pool/worker detail.

`Node::status_snapshot()` returns typed lifecycle, placement, and replication
observations. It runs synchronously with nonwaiting lock acquisition and no
actor RPC, storage call, or record scan. The owned result can be retained or
serialized by an application's preferred adapter. Use
`status_snapshot_with_limits(SnapshotLimits::new(fragment_limit, sender_limit))`
to change the default limits of 256 fragment and 256 sender records; each is
clamped to 4,096. Each fragment copies at most 64 replica IDs.

The snapshot records a wall-clock observation timestamp, monotonic collection
duration, node incarnation (`boot_id`), authoritative placement generation, and
explicit collection omissions/change flags. Placement, lifecycle state, serving
membership, and dispatcher routes are read under the placement transition lock
order. Sender reports are copied after those guards are released, and replacement
is checked before publishing each report. The generation is shared with the
placement driver and increments only when its published map changes.

`readiness` has three values:

| Value | Meaning |
| --- | --- |
| `Ready` | The declared initial readiness boundary was established, admission is open, and serving capacity remains (or the node has a routing-only role) |
| `NotReady` | Initial authority is incomplete, admission is closed, or no established serving capacity remains |
| `Unknown` | Contention, truncation, or a detected placement/lifecycle change prevented establishing readiness |

A routing-only node can be ready with no locally assigned fragments. In legacy
single-node mode an empty placement map means all fragments are assigned locally.
Readiness is about local serving authority and admission, not whether every
remote replica is connected or a particular write concern can currently succeed.
The last ACK's receiver durability is historical evidence only. An established
node can stay available during background gains; desired/serving/available
capacity and `capacity_partial` expose unfinished assigned fragments. A fresh
learner cannot acquire Serving merely because a collector samples it.

Fragment records preserve observed `Bootstrapping`, `Serving`, `Draining`, or
`Stopped` placement state, replica order, local assignment, actual serving
membership, and running route availability. A lifecycle state can lag route
availability during dispatcher restart; readiness checks the route as well.
Replica list truncation does not change local-assignment calculation.

Replication records distinguish initial fragment catch-up, background
replication for a serving fragment, and placement drain. Connection state is
typed from the existing sender report. Buffered operation count, retained-byte
charge, in-flight batches, completed reconciliation rounds, last ACK durability,
and an ACK mismatch preserve the existing reports' meanings. Round counts are
sender-instance totals, not progress percentages. Catch-up fraction and lag are
currently `None`: ActorDB has no trustworthy denominator or measured lag
estimate. Local retained-operation queue ages, last actual successful ACK and
reconciliation times, their durability and measured fetch progress are exposed
separately. Treat unobserved ages and unknown remote lag as unknown.

`lifecycle` comes from `Node::drain_status()`: `Running`, `Draining`, `Drained`,
or `Stopped`, with active ownership counts and receive/apply completion.
`node_lifecycle` adds Starting/Initializing/Recovering/Serving/Blocked/Failed
and Stopping views with typed reasons and fragment activity. `Drained` proves
completion of the selected admitted local execution/persistence boundary;
incoming replication and maintenance can still run. Only full shutdown proves
receive/apply ownership ended. See [draining.md](draining.md).

`shutdown_stage` observes an atomically published full shutdown stage, including
while a call owns the coordinator or its waiter is cancelled. It identifies replication
fencing/draining, transport shutdown, fragment quiescence/stopping, or completed
shutdown after a cancelled/timed-out call. A retained persistence error remains
a blocking reason; a deadline alone is not completion evidence.

`completed_replication_drain` preserves the exact successful
`shutdown_with_replication_drain` report, including its targets and durability.
It is absent before that fence succeeds. If its target list exceeds the sender
limit, the field is omitted and `drain_report_truncated` is set; this avoids an
unbounded copy and does not represent a partial list as complete fence evidence.

Check `observation.is_complete()` for full requested coverage and detected
placement/lifecycle stability. More specific flags distinguish contended locks,
truncated output, unavailable/replaced sender reports, and generation/phase
changes. These are observations across multiple subsystems; even a complete
snapshot is not a global transaction, replication checkpoint, write fence, or
guarantee that readiness remains true after it returns. For migrations, retain
the controller's overlap protocol and recheck the relevant placement generation.
Source topology changes can happen immediately after any snapshot.

Metrics collection builds from this same status observation and adds existing
resource diagnostics; see [metrics.md](metrics.md).
