# Node status

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
| `Ready` | Public admission is open and every locally assigned fragment has serving membership and a running published dispatcher |
| `NotReady` | Public admission is closed or an observed assigned fragment cannot serve |
| `Unknown` | Contention, truncation, or a detected placement/lifecycle change prevented establishing readiness |

A routing-only node can be ready with no locally assigned fragments. In legacy
single-node mode an empty placement map means all fragments are assigned locally.
Readiness is about local serving authority and admission, not whether every
remote replica is connected or a particular write concern can currently succeed.
The last ACK's receiver durability is historical evidence only.

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
estimate. Treat unknown values as unknown.

`lifecycle` comes from `Node::drain_status()`: `Running`, `Draining`, `Drained`,
or `Stopped`, with active ownership counts and receive/apply completion.
`Drained` proves completion of admitted local application request ownership;
incoming replication and maintenance can still run. Only full shutdown proves
receive/apply ownership ended. See [draining.md](draining.md).

`shutdown_stage` observes the full shutdown coordinator when its lock is free;
it is `None` while a call owns that coordinator. It can identify replication
fencing/draining, transport shutdown, fragment quiescence/stopping, or completed
shutdown after a cancelled/timed-out call. It does not invent a failure reason
when the coordinator cannot be read.

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
