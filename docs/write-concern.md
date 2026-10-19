# Write concern

Write concern controls the processing acknowledgements required for a write to
return success. It is a per-write option on the existing Node and Rust client
APIs. Ordinary `put` and `delete` retain local acknowledgement as their default.

## Levels

| Policy | Required acknowledgements |
| --- | --- |
| `Local` (default) | The receiving node applies the update; replication continues asynchronously. |
| `Count(n)` | `n` distinct responsible replicas, including the receiving node. |
| `Majority` | More than half of the configured responsible replicas, including the receiving node. |

For five replicas, majority requires three acknowledgements: the receiving
node and two peers. Counts must be positive and no greater than majority.
There is no separate all-replicas policy. With one or two replicas, majority
naturally includes every replica.

Nonlocal concerns require an explicit `NodeConfig.fragment_map`. The receiving
node captures that fragment's replica set when it accepts the write; changing
placement does not change an existing waiter's threshold or eligible peers.
Majority is calculated from configured replicas, including unavailable ones,
rather than from the currently connected peer count. Local and `Count(1)`
also work in the legacy empty-map configuration.

## APIs

```rust,no_run
use std::time::Duration;
use actordb::node::Node;
use actordb::write_concern::{WriteConcern, WriteConcernOptions};

async fn update(node: &Node) -> Result<(), Box<dyn std::error::Error>> {
    let options = WriteConcernOptions::builder()
        .concern(WriteConcern::Count(2))
        .timeout(Duration::from_millis(500))
        .build();
    let receipt = node
        .put_with_write_concern("record".into(), b"value".to_vec(), options)
        .await?;
    println!("{} acknowledgements", receipt.acknowledged_nodes().len());
    Ok(())
}
```

- `Node::put_with_write_concern` and `delete_with_write_concern` return a
  `WriteConcernReceipt`.
- `Node::put_with_write_concern_options` supports TTL and priority;
  `delete_with_write_concern_options` supports priority.
- `Client` provides the same concern methods; its put-options method takes
  the existing `PutOptions` for TTL/priority.
- Policies and receipts are shared by `actordb-protocol` and reexported by
  the library and client.

The base APIs cover single-key updates. Multi-key operations retain their
existing fan-out behavior; acknowledgement policies inside batch requests are
not supported. A policy is validated before its update is applied.

## Receipts and failures

A receipt reports the receiving node, configured replica count when known,
requested and effective acknowledgement thresholds, and the distinct nodes
whose acknowledgements satisfied the policy. For routed writes, the node that
actually serves the fragment counts as one; an ingress node outside the
replica set does not count.

`WriteConcernError::TimedOut` includes partial acknowledgement progress.
The default acknowledgement timeout is five seconds. It starts after local
application and replication fan-out; transport and actor admission have their
own deadlines. Policy encoding has millisecond precision and requires at least
one millisecond. SDK and forwarded-request deadlines include the concern wait
in addition to their transport allowance.

A timeout does not roll back the update. It may already exist on multiple
replicas and continues through normal replication and repair. A missing or
malformed reply has an unknown outcome; the SDK does not transparently retry
the mutation with a fresh version. Invalid counts and waiter admission failures
are reported before that update is applied.

## What acknowledgements establish

The receiving node first obtains the exact local apply outcome, enqueues the
stamped update through normal replication senders, then waits for the required
processing acknowledgements. The receiver's acknowledgement counts as one.
Remaining replicas can catch up afterward.

Peer acknowledgements are counted only after their sender session, batch
identity, and configured receiver fence have been validated. Duplicate ACKs,
old sessions, other updates, and nodes outside the captured replica set cannot
satisfy the concern. Waiters are removed after completion or cancellation.
Processing uses the existing LWW rules, so a peer can acknowledge an update
whose version has been superseded or expired. A successful concern does not
certify that every acknowledged replica still retains the original value.

Write concern does not establish a global order for concurrent updates or
linearizable reads. It does not force a local flush or independently guarantee
disk durability. The existing `SenderConfig.required_ack_durability` controls
which peer ACK fences are accepted; local acknowledgement remains local apply.
Acknowledgement counts must not be used as permission to reclaim tombstones.

Normal replication overflow and repair preserve eventual convergence. When
an original update lacks an operation-specific tracked ACK, its concern can
time out even if a later repaired state has reached peers. Reading a newer
winner or receiving an unrelated repair barrier is not substituted for that
update's acknowledgement.

## Scheduling and bounds

Concern waits run outside forwarded apply workers and release placement
mutation leases after application/fan-out. The peer reader remains available
for replication ACKs while a forwarded write waits, including ACKs returning
on the same connection. Accepted completion work remains owned during normal
shutdown.

Waiter count is bounded by the configured forwarded mutation capacity. Explicit
concerns support at most 4,096 eligible replicas, and policy/result decoding is
bounded before allocating repeated acknowledgement lists. No concern threshold
is automatically lowered because peers are disconnected.

## Additional controls

| Extension | Scope |
| --- | --- |
| Region requirement | Require at least one acknowledgement outside the receiving node's region, together with the numeric threshold. |
| Operational cap | Explicitly lower the numeric threshold for new writes and report requested/effective counts in the receipt. |

The region requirement and operational cap apply independently. A numeric cap
never removes a region requirement. Decoders reject unknown required policy
fields, so an older implementation cannot silently ignore a new constraint.

## Release and compatibility

This feature uses **0.5.0** because generated public message structs gain
optional `write_concern` fields. Downstream struct literals must initialize
those fields or use `Default`. All four public crates and inter-crate minimum
requirements move to compatible `0.5.0` ranges.

Peers require the new write-concern capability before connecting. Upgrade the
peer cluster together. The application protocol negotiates concern support;
older SDK operations without a concern retain their local-acknowledged wire
behavior. The LWW record/store format and ordinary Node mutation signatures
remain the same.

## Operational overrides

A receiving node can explicitly cap acknowledgement counts for new writes.
The default is no cap. A cap never raises the requested count or removes an
additional region requirement. Receipts retain both the requested and
effective thresholds. See [operational write concern overrides](write-concern-overrides.md)
for configuration, runtime controls, and their effect on existing waiters.
