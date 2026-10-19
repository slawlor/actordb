# Conditional mutations and stable quorum storage

Enable the additive `quorum` feature to use `actordb::quorum::QuorumNode`:

```toml
[dependencies]
actordb = { version = "0.4.1", features = ["quorum"] }
```

This API has its own listener, protocol version, credentials, and storage.
The existing `Node`, application listener, SDK, and LWW records retain their
existing semantics. Quorum records cannot be modified through those APIs.
The `memory` and `rocksdb` features are independent of `quorum`.

## Configuration and API

Every node agrees on a cluster ID, initial fragment map, and state/retention
limits. The map covers consecutive fragments starting at zero. Each fragment
has a nonempty, unique list of voters. Configure three voters to tolerate
one unavailable voter; one voter provides stable local commits only.

Configure `members` with the addresses of all initial and prospective voters.
Each voter runs `QuorumNode::start(id, address, config)`. Use a separate
`FileQuorumStore` directory per node. `QuorumConfig` follows the existing Bon
builder pattern; `store`, `cluster_id`, `fragment_map`, and `members` are required.

```rust,no_run
use actordb::quorum::{Expectation, Mutation, QuorumNode};

async fn replace(node: &QuorumNode, revision: u64) -> Result<(), Box<dyn std::error::Error>> {
    let request = Mutation::builder()
        .request_id("globally-unique-request-id")
        .valid_until_ms(actordb::actor::now_ms() + 30_000)
        .key("document")
        .expectation(Expectation::Revision(revision))
        .value(b"replacement".to_vec())
        .build();
    let result = node.mutate(request).await?;
    if result.applied {
        println!("committed revision {}", result.revision);
    }
    Ok(())
}
```

`Expectation::Any` implements unconditional put/delete; `Absent` implements
insert-if-absent; `Revision` compares the exact record incarnation. A mutation
with `value: None` deletes. `Some(Vec::new())` writes an empty value. Expiry is
an optional absolute Unix millisecond deadline and is only valid on writes.
A write whose expiry has already elapsed is rejected.

`MutationResult` reports whether the condition matched, the resulting/current
record, the original mutation revision, and a `CommitReceipt`. A mismatch is a
committed result with `applied=false`. Deleted/expired records are absent;
recreating the same key assigns a fresh revision, preventing stale CAS tokens
from matching a new incarnation. Revisions are logical slot numbers, independent
of wall-clock write frequency.

## Commit, ownership, and recovery

Operations route to a preferred voter. If it cannot be reached, another voter
can coordinate the operation. Per-fragment Paxos promises fence old ballots.
Promises and accepted successor snapshots reach stable storage before their
acknowledgements. There are no clock-based ownership leases.

The coordinator prepares a majority, adopts the highest accepted predecessor,
and obtains stable acceptance of the chosen successor from a majority. It then
installs/disseminates the chosen snapshot. A successful result includes the
term, revision, distinct accepting voters, and voting configuration. A ballot
does not grant an isolated writer permission to acknowledge new operations.

If a coordinator dies after acceptance but before replying or disseminating
the decision, the next coordinator must recover that accepted state before
executing a new command. Reads also run through this protocol and commit a
barrier; they never serve a possibly stale local cache. Predicates, expiry, and
mutations are evaluated against the recovered state in one ordered transition.

There is no success when the required majority is unavailable. Deadlines,
disconnects, and storage failures can leave an unknown mutation outcome,
including a chosen operation whose response was lost. Preserve the complete
request and retry it; an error does not establish that a mutation was absent.
`shutdown` closes admission, drains owned work, and releases storage. A cancelled
shutdown waiter can retry the retained join; dropping all handles also closes
the listener and drains its tasks.

The protocol assumes crash/recovery faults and trusted peers following the
protocol. It is not Byzantine consensus. Configure `cluster_secret` with at
least 32 random bytes for independent mutual challenge authentication. Its
proof binds identities, challenges, protocol version, and policy. Plain TCP
does not protect post-handshake frames; the existing `Network` abstraction
allows a protected transport. Authentication is optional, as with `Node`.

## Retry identities and bounds

Request IDs are scoped to a fragment. Reuse an ID only for an identical request
until its `valid_until_ms`. The persisted receipt includes a fingerprint and
the original result. Replaying after subsequent mutations returns that result
without reapplying the change; changing any request field returns
`RequestConflict`.

The configured maximum request lifetime bounds retention. Expired requests
return `RequestExpired`, even after their receipts are removed. Snapshot time
never moves backward, so reclaimed identities cannot become valid again after
a coordinator's clock moves backward. Nodes need reasonably synchronized
wall clocks for useful expiry/deadline behavior; a fast clock can expire
records or requests early. Ownership and revision correctness do not depend
on clock synchronization.

Live records, unexpired receipts, retained changes, encoded protocol state,
and admission each have explicit bounds. New mutations fail with
`ResourceLimit` rather than discarding live state or valid retry receipts.
Admission reserves capacity for protocol traffic separately from command work.

## Membership transitions

Call `reconfigure(fragment, new_voters)` after starting the prospective voters
with the same cluster/genesis configuration and address book:

1. Commit the joint old/new configuration using the old majority.
2. Transfer its certified state to new voters.
3. Commit the new configuration using a majority of **both** old and new sets.

Before acknowledging a transition, persist its decision certificate on a
majority of the resulting configuration. This lets the new voters recover
after the old voters are decommissioned.

All operations during the joint configuration require both majorities. A
timeout may leave the joint configuration active; retry the same target to
complete it. A competing target is rejected while a transition is active.
Remove old nodes only after the final step succeeds. Disjoint voter sets are
supported. A prospective node may vote only after adopting the certified base
snapshot; a returning old voter cannot revive its previous configuration.

The immutable initial fragment map remains the genesis map on every restart.
Effective membership lives in certified snapshots, not in an edited startup
map. Addresses may change on restart. Preconfigure prospective voters in every
node's address book; updating that book currently requires a restart. This API
does not use the LWW `PlacementController`.

## Changes and automatic expiry

`changes(fragment, after, limit)` returns ordered durable `Put`, `Delete`, and
`Expire` events. Change IDs and the retention floor persist in snapshots.
Delete/expiry events include the removed record; puts include the new record.
IDs remain monotonic through ownership changes and recovery. Replaying a
mutation produces no second logical event.

`subscribe(fragment, after).next()` waits for pages. Save the page cursor after
processing; recreating a subscription from the saved cursor supports replay
after a consumer crash. Consumers must make external effects idempotent using
the fragment/change ID. There is no atomic transaction with external callbacks.

Automatic maintenance commits expiry and receipt/event reclamation even without
client requests. Expiry decisions occur within the same ordered state machine
as mutations. Delivery can be delayed by scheduling, recovery, or loss of a
quorum; the maintenance interval is not a hard deadline.

History is bounded by `max_changes`, `change_retention`, and snapshot byte
capacity. Byte pressure shortens event history while preserving valid retry
receipts and live records. A cursor older than
the retained floor returns `CursorExpired`; it never silently skips changes.
The application can resynchronize its view when that happens. Events whose
history has been reclaimed are not guaranteed to be replayable.

`snapshot(fragment)` returns a consistent full record view and its matching
`last_change_id`. After installing that view, resume changes after that ID to
avoid a gap between resynchronization and subscription.

## Safe reclamation and storage

Each certified snapshot replaces the complete preceding fragment state.
Deleted and expired records are physically removed from the snapshot rather
than retained as per-key tombstones. The durable slot floor rejects older
snapshots and operations. A stale or restored replica must recover current
certified state before serving or voting on a successor. This mechanism is
independent of the existing LWW tombstone collector, which remains a no-op.

`QuorumStore` stores complete protocol state, including ownership promises,
accepted-but-not-yet-learned proposals, committed membership, retry receipts,
and changes. Every successful save must be an atomic stable replacement.
An ordinary buffered `KvStore` does not satisfy this contract.

`FileQuorumStore` implements stable replacement using checksummed files,
file fsync, atomic rename, and directory fsync on a local filesystem providing
those guarantees. It holds an exclusive OS lock and permits one live node
attachment. Its versioned manifest pins cluster/node identity, genesis map,
and state policy. Initialization writes every genesis fragment before committing
the manifest. Missing/corrupt state fails startup; it is never recreated as an
empty voting replica. Snapshots carry cluster and fragment identity, so a
valid file copied into the wrong namespace is also rejected. Ambiguous write
failures fail the node closed until
reopened. Temporary files and the current file bound replacement overhead.

## Practical limits

This implementation transfers and synchronously persists complete bounded
fragment snapshots for each operation, including reads. Work scales with
fragment state size. It prioritizes explicit, testable semantics; it is not
an optimized replicated log or a throughput claim. Tune fragment sizing and
retention to fit the configured byte bounds. There are no transactions across
fragments or automatic provisioning of new voters.

All four public crates remain on compatible `0.4` APIs. Version `0.4.1` adds
the quorum feature and independent wire namespace without changing existing
public struct fields, enums, peer negotiation, or record formats.
