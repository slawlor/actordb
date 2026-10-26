# Write concerns and certified evidence

A write concern waits for processing acknowledgements for one stamped mutation.
It does not establish a globally ordered log, an immutable payload, or a
distributed atomic snapshot. A receiver may acknowledge the submitted LWW
version while already retaining a newer winner, including a tombstone.

`Local` and `Count(1)` need one acknowledgement. `Count(n)` and `Majority` use the
receiving node's frozen replica placement; explicit counts must be positive and
no greater than the majority. `require_other_region(true)` independently needs
an eligible acknowledgement outside the receiver's frozen region label.

An operational acknowledgement cap can reduce the requested count. Every
receipt reports the requested and effective counts. Set
`strict_acknowledgements(true)` to reject a lowering cap before apply. Strict
counts and storage durability are independent policies.

## Request an actual storage fence

`minimum_durability` applies to the local acknowledgement and every remote
acknowledgement counted toward success:

| Level | Certified minimum |
| --- | --- |
| `MemoryApplied` (default) | The operation was processed in memory. Local persistence may only be queued. |
| `StoreCommitted` | The actor buffer was flushed and the configured backend accepted the write batch. A buffered commit can be lost after a crash. |
| `StableCommitted` | The actor buffer was flushed and `KvStore::stable_fence` actually returned `StoreCommitDurability::Stable`. |

The receiver invokes the requested local fence. Replication batches request the
stronger of the sender configuration and the outstanding write concern. Only a
correlated ACK that actually meets that fence advances remote progress; the
configured desired level is never substituted for an ACK. A buffered-only or
volatile backend cannot report stable success. Custom backend wrappers must
delegate their stable-fence contract correctly.

```rust,no_run
use actordb::node::Node;
use actordb::write_concern::{WriteConcern, WriteConcernOptions, WriteDurability};
use std::time::Duration;

# async fn example(node: &Node) -> Result<(), actordb::write_concern::WriteConcernError> {
let policy = WriteConcernOptions::builder()
    .concern(WriteConcern::Majority)
    .strict_acknowledgements(true)
    .minimum_durability(WriteDurability::StableCommitted)
    .require_other_region(true)
    .timeout(Duration::from_secs(10))
    .build();
let receipt = node.put_with_write_concern("invoice:17".into(), vec![7], policy).await?;
let evidence = receipt.evidence().expect("a durable policy requires evidence");
assert_eq!(receipt.requested_durability(), WriteDurability::StableCommitted);
assert!(evidence.acknowledgements().iter()
    .all(|ack| ack.durability() >= WriteDurability::StableCommitted));
# Ok(())
# }
```

The same options and evidence types are re-exported by `actordb-client`.
`Client::put_with_write_concern`, TTL writes, and deletes carry the complete
policy through the application listener and every forwarding hop. The SDK
correlates the returned operation with the selected table, user key, payload,
and tombstone flag. Forwarding origins also compare the exact operation
certificate with the stamped mutation they dispatched.

## Interpret receipts

New receivers attach `WriteConcernEvidence` to success, timeout and known
post-apply fence failures. It contains:

- The submitted fragment, physical/logical version, originating node, tombstone
  bit, absolute expiry, key/value digests, and deterministic operation digest.
- The exact sorted eligible replica identities frozen before apply, plus a
  deterministic replica-set digest.
- The actual minimum fence observed for every acknowledged identity.

The operation digest deliberately excludes sender sequence and boot counters:
retry and reconciliation can certify the same LWW floor without reproducing a
specific transport log entry. ACKs of newer winners satisfy the submitted floor
without replacing the submitted certificate. The replica-set digest identifies
the exact set; it is not a placement epoch, lease, or continuing proof that a
replica still hosts the data. Region classification and count overrides also
remain frozen for the ticket. A configuration change cannot add a new eligible
acknowledger to an outstanding write.

These certificates are validated statements from the receiving node and its
correlated peers. Digests detect changed identities and operation metadata; they
are not signatures or an independent proof of a backend's physical media.
Use the cluster's peer authentication and trusted backend contracts.

Legacy receipts can omit evidence for default memory policies. Set
`require_evidence(true)` when a certified memory floor is required. Store and
stable policies require evidence automatically. New required policy fields are
rejected by older receivers before apply; they cannot silently ignore the new
promise. Decoders reject missing evidence, changed requested durability, reduced
strict counts, malformed certificates, duplicate or ineligible identities, and
successful progress below the requested fence.

## Failure, deadlines, and ownership

`TimedOut(receipt)` reports actual local progress and any eligible ACKs already
observed. A slow local fence can finish after the policy deadline; the call still
times out and the receipt describes the evidence reached when completion was
observed. A timeout does not roll back the mutation or stop replication.

`DurabilityFailed { receipt, error }` means ordinary local apply completed but
the explicit local fence failed or returned a weaker level. Its boxed receipt
preserves the submitted operation, requested durability and known reached floor.
An error during the apply RPC itself can follow a cache merge or partial backend
commit; that returns `UnknownOutcome` instead of fabricating an applied receipt.
A superseded operation retains its original operation error and never claims a
successful local acknowledgement.

Caller or transport deadlines can lose the reply while apply or fencing remains
owned. Embedded completion owners and forwarded mutation owners retain actual
execution independently of the caller. Actor fence requests retain admission
ownership even if their RPC waiter expires. Request draining therefore waits
for that work; it cannot release a store while an accepted fence is still live.
The policy wait starts after ordinary apply/fanout admission. Transport and local
work deadlines are separate and can yield an unknown outcome first.

Sender ingress overflow or reset can discard transport history. Outstanding
tickets retain a charged copy of the submitted operation and retry its exact
floor under the requested fence once streaming resumes. Retries respect sender
count/byte limits. An empty reconciliation-barrier ACK is never invented as
per-key evidence. Timed-out tickets release their waiter capacity; ordinary
replication and snapshot repair remain independent of the released waiter.

Batch write-concern APIs remain unsupported: the supported contract is one
submitted operation per receipt. Replication batches can contain several
operations and carry the strongest required ACK fence for their live tickets.

In map-empty local mode, `replica_count()` remains unknown and eligible evidence
names only the receiver; no cluster placement is inferred from connected peers.

`Node::write_concern_diagnostics()` exposes monotonic planning/prepared-ticket outcomes,
active ownership, and cumulative/max elapsed wait nanoseconds. Cancellation
counts unfinished ticket destruction, not a caller disappearing while its
completion owner later succeeds. Key and backend-error strings are not metric
labels.
