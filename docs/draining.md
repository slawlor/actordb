# Request draining and shutdown

`Node::begin_drain()` synchronously closes the shared admission gate used by
embedded operations, named tables, SQL, scans and forwarded requests. The
transition is idempotent and one way. There is no resume API. Placement freeze
is requested at the same time and joined by the async drain.

```rust
use actordb::node::DrainOptions;
use std::time::Duration;

# async fn stop(node: &actordb::node::Node) -> Result<(), actordb::node::DrainError> {
node.begin_drain();
let report = node.drain_requests(
    DrainOptions::default().with_timeout(Duration::from_secs(30)),
).await?;
assert!(!report.receive_apply_complete());
node.finish_shutdown(DrainOptions::default()).await?;
# Ok(())
# }
```

`drain_requests` (also available as `drain`) waits for admitted request work,
including actor requests and mutation completion owned after a caller timeout
or cancellation. It leaves fragment actors and peer transport alive. Incoming
replication, expiry and other maintenance can still change records. A successful
request drain establishes neither a local persistence fence nor peer ACKs.

`finish_shutdown` bounds the existing local shutdown with one absolute timeout.
Success closes incoming replication admission, joins receive/apply ownership,
quiesces every fragment through its configured persistence fence, stops actors,
and releases local store ownership. `receive_apply_complete` is true only after
this full shutdown boundary. A local persistence fence has the durability
provided by the configured store; it does not automatically imply stable disk
storage. `shutdown_with_replication_drain` separately establishes the configured
ACK durability for its exact frozen fragment/peer targets and finishes shutdown.

Both bounded calls reject zero or unrepresentable timeouts before changing
admission. Timeout or cancellation leaves admission closed and preserves
completion ownership and shutdown progress. Retry with a fresh timeout. A store
failure returns `DrainError::Local`; fragment state remains available for the
existing shutdown retry. Dropping the node remains best effort.

## Receive and apply boundary

Decoded `ReplicateBatch` and `ReconcileData` frames acquire a separate receive
lease. A reconciliation page transfers that lease into the sender queue and
then each actor apply, so validation (`connection_valid`) or stream closure
cannot be mistaken for completed application. An issued actor message retains
the lease after its RPC waiter times out or is cancelled. Full shutdown waits
for these leases before stopping senders or releasing fragment/store ownership.

Transport shutdown can reject a batch's unissued suffix or a stale queued page;
that is a terminal rejection, not successful application. No successful ACK is
sent for an incomplete batch. Peers retain their normal idempotent retry/repair
obligation. Already issued applies reach their actor terminal outcome before
the full local shutdown barrier completes. This barrier is local and does not
prove cluster convergence.

`drain_status()` is available throughout shutdown without actor RPCs or the
shutdown coordinator lock. Counts are approximate ownership slots rather than
unique user requests: a batch can own several request slots, and replication
counts describe retained frames. SQL page admission stays in the request count
while actor work owns its resources, including after coordinator cancellation.
The drain additionally checks actor-owned storage. `Drained` means the request boundary, and `Stopped` means
the full local shutdown boundary.

An application `ClientServer` calls the same Node/Table APIs, so new requests
receive the existing unavailable/shutting-down response during node draining.
An admitted application session retains its request slot through response send
or socket failure. A blocked response can therefore exhaust a drain's timeout;
retry after delivery completes or the client disconnects.
The server owns its listener independently; call `ClientServer::shutdown` to
release it. Response bytes already queued to a socket may be lost, including
after a mutation completed; clients must retry stamped writes idempotently.

All public API additions are additive. The receive ownership wait strengthens
the existing shutdown/persistence contract without changing wire negotiation.

`drain_diagnostics()` reports cumulative accepted/completed/rejected admission
slots and started/completed/timed-out/failed/cancelled bounded drain calls.
Completion counts describe terminal ownership release, including errors, and
do not count successful writes. Total drain duration includes failed and
cancelled attempts. The node metrics snapshot exports these observations.
