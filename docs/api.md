# Public API guide

For remote application connections, use the companion
[`actordb-client`](../actordb-client/README.md) and the separate
`actordb::grpc::GrpcServer` adapter or a custom `RpcService` framework. For SQL filtering, fragment
scopes, projections, and exact aggregates, see the [query guide](query.md).
For caller-supplied streams, application acceptors, and TLS configuration,
see the [transport guide](transports.md).
For optional local TTL cleanup notifications and their delivery contract,
see [TTL expiry callbacks](ttl-expiry-callbacks.md).

The main application surface lives in `actordb::node`. Start from
`NodeConfig::builder()` for database behavior and `NodeRuntimeOptions::builder()`
for transport/replication liveness policy, overriding only the settings you need
before calling `build()` — see
[Configuration](configuration.md) for every knob.
Existing literals, `Default`, `with_*` helpers, and getters remain supported.
Builders require actual dependencies for low-level actor arguments; they do not
invent node identities, shared state, HLCs, or routers. See the
[13-type builder reference](configuration.md#configuration-builders).

## API stability boundary

The supported `actordb` API is the documented node, client-server, query,
storage, replication, configuration, and write-concern surface described in
this guide. The crate follows pre-1.0 semver policy: breaking Rust API or wire
protocol changes use a new minor version, while patch releases are reserved
for compatible fixes and documentation.

Consumers that need language-independent wire bindings depend directly on
`actordb-protocol` and import `actordb_protocol::meta` or
`actordb_protocol::client`; ordinary applications use the high-level APIs.
The node crate's internal protocol shorthand is private.

## Starting a node

```rust
use actordb::node::{Node, NodeConfig};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = NodeConfig::builder().build();
    let node = Node::start("node-a".to_string(), "127.0.0.1:7447".parse::<SocketAddr>()?, config).await?;
    println!("listening on {}", node.addr());

    // Form the peer mesh; Ok means a Node-owned redial supervisor now owns
    // the successful initial transport (authentication remains asynchronous).
    node.connect("127.0.0.1:7448".parse::<SocketAddr>()?).await?;
    node.shutdown().await?;
    Ok(())
}
```

- `Node::start(node_id, listen_addr, config)` binds `listen_addr` (port `0`
  picks an ephemeral port; the bound address is returned by `node.addr()`) and
  uses `NodeRuntimeOptions::default()`.
- `Node::start_with_options(node_id, listen_addr, config, runtime_options)`
  exposes the same startup path with explicit runtime liveness limits. For
  example:

  ```rust
  use actordb::node::NodeRuntimeOptions;
  use std::time::Duration;

  let runtime = NodeRuntimeOptions::builder()
      .peer_queue_frame_capacity(512)
      .peer_queue_byte_capacity(64 * 1024 * 1024)
      .replication_control_message_capacity(128)
      .inbound_connection_capacity(512)
      .receive_byte_capacity(128 * 1024 * 1024)
      .queue_admission_timeout(Duration::from_secs(2))
      .reconcile_data_admission_timeout(Duration::from_secs(20))
      .forwarded_attempt_timeout(Duration::from_secs(8))
      .connect_timeout(Duration::from_secs(3))
      .handshake_timeout(Duration::from_secs(3))
      .socket_read_timeout(Duration::from_secs(20))
      .socket_write_timeout(Duration::from_secs(20))
      .replication_ack_timeout(Duration::from_secs(20))
      .build(); // Unspecified settings keep their defaults.
  let node = Node::start_with_options(
      "node-a".to_string(),
      "127.0.0.1:7447".parse()?,
      NodeConfig::builder().build(),
      runtime,
  )
  .await?;
  ```

  Defaults are 256 queued frames and 32 MiB of admitted encoded bytes per peer,
  64 protocol-control messages per fragment/peer sender, and 1024 concurrent
  inbound connections. A separate node-wide 64 MiB receive budget covers raw
  encoded bodies on accepted and outbound-dialed connections, including
  handshakes. It reserves bytes before allocation and retains them through
  decoding and the handler turn; saturation closes only that connection.
  Protobuf decoding and actor/cache/relay/caller copies allocate separately,
  so this is not a total-memory limit.
  Serving single/batch reads have three Node-wide queues
  (64 jobs and eight fixed workers per priority) and a separate 64 MiB key-storage
  budget per priority, held through actor ownership after waiter cancellation.
  String buffer capacities and Vec/String headers are charged; fixed job and
  allocator overhead, values, raw receive bodies, and encoded writer queues are
  excluded. Its configured minimum is 16 MiB. Queue admission, a forwarded
  owner attempt, a dial, and the complete two-flight
  handshake each default to 5 seconds. Frame-body reads after a prefix, complete
  frame writes, and acceptable ordinary replication ACKs each default to 30
  seconds. Idle established connections have no read deadline until a prefix
  arrives. Locally applied mutations have a separate lane sized by
  `SenderConfig::buffer_capacity`. The peer frame budget must be at least two
  and is split between data and priority responses; the byte budget must be at
  least 32 MiB so ordinary producers leave a full 16 MiB response reserve
  beside one maximum-size frame. The receive budget must fit at least one
  16 MiB maximum body. Other capacities/timeouts must be nonzero. Actor
  lanes remain item-count bounded. Every encoded frame is separately limited
  to 16 MiB, with 1 MiB soft pages for anti-entropy and ordinary replication.
- `node.connect(peer_addr)` tries the first transport dial once and returns an
  error if it cannot connect within the dial deadline. `Ok` means the successful
  stream has been handed to a Node-owned redial supervisor, not that
  authentication or immediate reconciliation have completed. Concurrent calls
  for the same address wait for that pending initial dial and do not return
  early merely because a dial is in progress. On success they share the active
  supervisor; after an initial failure, a waiter may reserve and make the next
  attempt. After the first successful authentication, the supervisor pins that
  node id, rejects any different claimant at the address on later reconnects,
  and retries transport or protocol failures with bounded exponential backoff.
- Entries in `NodeConfig::members` are persistent dial intents from startup,
  including when the first attempt fails. Their configured node ids are known
  before the first connection attempt and checked against the first and every
  later Hello; a mismatched claimant is never installed. If both
  nodes dial at once, a transcript-derived election makes both endpoints keep
  the same physical connection and parks the duplicate dialer while it is live.
- The local node id, every `members` key, and every placement replica id must
  contain 1 to 255 UTF-8 bytes. Every dynamic map and every non-empty static map
  must contain exactly the fragments in `0..fragment_count` and use non-empty,
  duplicate-free replica lists. An empty static map is the current
  full-replication mode.
- `node.id()`, `node.addr()`, `node.peer_count()` are the basic inspectors.
- `node.shutdown().await` is a fallible, one-way lifecycle fence. It rejects
  later public data operations with `NodeError::ShuttingDown`, aborts and joins
  listener, connection, writer, dial, timer, and placement tasks, wakes
  admitted remote requests, non-abortively drains accepted serving forwarded
  single/batch mutation completion while actors/senders remain live, waits for
  admitted API work, then stops replication
  senders. Every fragment closes cleaner/snapshotter admission, drains its
  dispatcher and worker mailboxes, materializes expired values, and reports its
  final local-store flush before any supervisor state is destroyed.
- If a final flush fails, `shutdown` returns `NodeError::Storage`, retains the
  quiesced state and persistence batch, and admits no new work. Call
  `shutdown` again to retry. Successful and repeated calls are idempotent; a
  cancelled waiter can also call it again.

This pre-release lifecycle hardening is source-breaking: `shutdown` now returns
`Result<(), NodeError>`, `NodeError` includes `ShuttingDown`, and the advanced
public `SupervisorMessage` enum includes `Quiesce`. Update ignored shutdown
calls and exhaustive matches when upgrading.

`shutdown` fences public Node data calls, but does not wait for peer replication
ACKs. Use the separate bounded API when the node must establish a peer fence
before stopping:

```rust
use actordb::node::{ReplicationDrainOptions, ReplicationDrainShutdownError};
use std::time::Duration;

let report = node
    .shutdown_with_replication_drain(
        ReplicationDrainOptions::builder().timeout(Duration::from_secs(30)).build(),
    )
    .await?;
println!("fenced {} fragment/peer senders", report.targets().len());
```

`ReplicationDrainOptions` defaults to one 30-second absolute budget; a zero or
unrepresentable timeout is rejected. The call first closes public and inbound
forwarded-mutation admission, stops placement work, and waits for admitted
local applies plus sender fan-out. It then retains one generation on each exact
target sender and runs a fresh, stable full reconciliation through
`SenderConfig::required_ack_durability`. Only after all targets acknowledge
does it run the same local flush/stop path as `shutdown`.

The report is deliberately narrow: each listed peer has the fenced LWW winner
or a newer winner for that fragment. It does not prove byte-for-byte cache
equality, a quorum, cluster-wide convergence, response delivery, or fsync.
`MemoryApplied` is volatile; `StoreCommitted` means the receiver's configured
store accepted its flush under that store's contract. Offered RocksDB defaults
to `SyncWal`; explicitly selected `AsyncWal` does not force stable media for
ordinary commits. `StableCommitted` additionally invokes the store's explicit
stable fence; RocksDB synchronously flushes its WAL for that fence under either
ordinary-write policy. Dynamic prefix retirement privately requires this
strongest level from every replacement replica.
Explicit placement
targets every configured remote replica for each locally active fragment after
placement freezes. Map-empty mode can only target senders already known
at the fence; an empty target report is therefore possible.

On `ReplicationDrainShutdownError::Timeout`, the structured `completed` and
`pending` targets describe the last observation. Transport, senders, and
fragment state stay alive while new data calls remain rejected. Retry the
method to resume retained generations, or call ordinary `shutdown` to abandon
the remote guarantee and finish local-only. Cancellation has the same retained
state. `RetryableSender` means an exact sender task stopped or could not be
prepared; retry installs a replacement and takes a fresh fence from the
authoritative cache. `Unavailable` is terminal for this guarantee because a
local-only shutdown has already passed the replication boundary. If peer work
succeeds but the later local flush fails, `Local` carries the valid
`ReplicationDrainReport`; retry skips the remote phase. A mutation response
queued just before disconnect can still be lost, so clients retry stamped
operations idempotently.

Merely dropping a `Node` requests best-effort transport cancellation and actor
stop, but cannot await or report a peer fence, durable flush, task join, or
RocksDB lock/attachment release. Reusing the same live `RocksDbStore` object
therefore requires explicit successful shutdown.

## Single-key operations

```rust
// Applies at the serving owner: locally when hosted here, otherwise
// forwarded and awaited.
node.put("user:42".to_string(), b"{\"name\":\"Ada\"}".to_vec()).await?;

// Write with TTL — the key reads as absent after `ttl` elapses.
node.put_with_ttl("session:9".to_string(), b"tok".to_vec(), Some(std::time::Duration::from_secs(300))).await?;

// Delete. This writes a *tombstone* that replicates like a put
// (see Tombstones & deletes); the key is not physically removed.
node.delete("user:42".to_string()).await?;

// Read. Returns Ok(None) for missing keys, expired keys, and tombstones.
let v: Option<actordb::actor::VersionedValue> = node.get("user:42".to_string()).await?;
```

`VersionedValue` carries `value: Vec<u8>`, `timestamp_ms: u64` (HLC),
`node_id: String`, `expires_at_ms: Option<u64>`, and `is_tombstone: bool`.

TTL expiry is anchored to the originating node's wall clock before waiting
for sequence/HLC state or routing the write. Forwarding preserves that exact
deadline; an HLC timestamp ahead of wall time does not extend it. Durations
are truncated to whole milliseconds: `None` means no expiry, while zero or
a positive sub-millisecond TTL sets the deadline to the captured origin wall
time. The serving node returns an expired apply outcome if its own wall clock
has reached that deadline; a slower clock can delay expiry there.
An unrepresentable duration or absolute deadline returns
`NodeError::Codec` before consuming a sequence/HLC stamp or applying a write.
Expiry must fit a nonzero `u64` because zero encodes absent expiry. TTL relies
on nodes' wall clocks, not a lease or a guarantee against clock adjustments.

Every mutation is preflighted against all required single-record wire
envelopes before local apply. A key/value that cannot fit the 16 MiB hard frame
limit returns `NodeError::Codec` rather than creating unreplicable local state.
The embedded `VersionedValue.node_id` must contain 1 to 255 UTF-8 bytes as
well; this applies to direct actor calls and records loaded from custom stores,
not only to `Node::start` identities.

### Priorities

```rust
use actordb::actor::OpPriority;

// Latency-sensitive write, normal read, bulk backfill.
node.put_with_priority("hot:key".into(), data, OpPriority::High).await?;
node.get_with_priority("hot:key".into(), OpPriority::High).await?;
node.put_multi_with_priority(batch, OpPriority::Background).await;
```

`OpPriority` is `High` / `Default` / `Background` (`Default` is the
`#[default]`). Each named table has three isolated worker pools per fragment;
a shed operation surfaces as `NodeError::Backpressure` — never as a silent
success or `Ok(None)`. Local and forwarded puts wait for the serving worker's
exact apply outcome; once it returns `Ok`, reads at that owner observe that
version or a newer winner. The priority travels in the request and selects the
remote table's actor pool, but it does not schedule the per-peer socket's
ordinary data lane. Pool capacity counts active-plus-queued worker jobs for
that table. It does not bound the shared dispatcher mailbox or replicated
ingress. The dispatcher hands admitted reads directly to their sticky table
worker rather than awaiting a nested worker RPC; worker execution and
backpressure are table-isolated, while dispatcher ingress, placement,
replication, and maintenance remain shared.
See
[Consistency](consistency.md) and [Traffic classes & backpressure](backpressure.md).

Remote single-key operations try configured replica holders in placement
order. Writes retain one exact stamped version across retries and terminate on
the first valid non-`Unavailable` outcome. Reads use source-pinned
`ForwardedGet` attempts, preserving hop-bounded re-forwarding while placement
maps converge. A timeout, dropped connection, or malformed response tries the
next holder, while an explicit `found = false` remains a successful miss.
Exhausting the holders returns `NodeError::Network`.

## Multi-key batching

```rust
// One Vec in, one Vec<Result> out — results stay in input order.
let results: Vec<Result<(), _>> =
    node.put_multi(vec![("a".into(), b"1".to_vec()), ("b".into(), b"2".to_vec())]).await;
let reads: Vec<Result<Option<actordb::actor::VersionedValue>, _>> =
    node.get_multi(vec!["a".into(), "b".into()]).await;
```

Semantics (full detail in [Multi-key batching](batching.md)):

- Plain `put_multi`/`get_multi` run at **`OpPriority::Background`**; the
  `_with_priority` variants take an explicit priority.
- Keys are grouped by destination fragment and split by exact encoded bytes.
  Puts also reserve their worst-case complete response and retain the 4096-item
  defensive ceiling. Get requests contain at most 256 keys; a response is a
  connection-correlated ordered prefix and the caller pulls the omitted suffix under
  a fresh request id. Duplicate positions and explicit not-found entries are
  preserved.
- Remote puts and gets have one **configured per-attempt deadline** (5 seconds
  by default) covering outbound queue admission and the response. Both fail over on a closed
  connection or missing reply; put replies additionally
  require one ordered exact outcome per key. Every `Unavailable` position
  tries the next holder with its original stamped mutation; terminal positions
  are excluded from the retry and retain their exact outcome. Duplicate keys
  retain separate input positions and order throughout filtered retries.
- Remote fragment groups are awaited sequentially, as are holder attempts
  within a group; these APIs do not issue all fan-out attempts concurrently.
- Per-key errors use `NodeError`: `Actor` = misconfiguration (no usable
  replica) or local actor-framework failure, `Network` = no remote replica
  answered, `Backpressure` = shed.

## Advanced replication embedding

`actordb::replication::spawn_bounded_sender(fragment, peer_id, config, deps,
options)` starts one per-fragment/per-peer sender and returns a `BoundedSender`.
It returns `SenderOptionsError` for zero or unrepresentable sender limits and
timeouts before spawning work. Use `SenderRuntimeOptions::default()` for the
default liveness policy. Its public options are:

- `with_ack_timeout` (default 30 seconds): deadline for an acceptable ACK to an
  ordinary streaming batch after queue admission;
- `with_queue_admission_timeout` (default 5 seconds): maximum wait to reserve a
  frame slot in the peer queue.

The Bon builder uses the field names without `with_`:

```rust
use actordb::replication::{SenderConfig, SenderDeps, SenderRuntimeOptions};
use std::time::Duration;

let config = SenderConfig::builder().batch_size(256).build();
let options = SenderRuntimeOptions::builder()
    .ack_timeout(Duration::from_secs(20))
    .queue_admission_timeout(Duration::from_secs(2))
    .build();
// Supply the real router, shared clock, and report used by this embedding.
let deps = SenderDeps::builder()
    .router(router)
    .hlc(hlc)
    .state_report(state_report)
    .build();
```

`BoundedSender::send` admits data and protocol controls through independent
bounded lanes. `Enqueue` must name an already-applied mutation: saturation
sheds its delta into visible snapshot-reset recovery rather than rejecting
the completed local write. An `Ok` submission is not remote delivery or a
durability receipt. `ingress_diagnostics()` reports admitted pending operations,
queued controls, payload charges, cumulative shed/reset requests, and an
unsettled recovery flag. Protocol `Full` errors require retry or connection
closure; raw peer frame channels are caller-owned and count-bounded, and callers
must enforce their own transport byte budget. `stop()` cancels locally and
releases ingress ownership; it does not complete pending remote recovery.

`build()` checks that dependencies were supplied, not that runtime limits are
valid. The fallible `spawn_bounded_sender` validates limits. Private Node control
capacity and queue-failure hooks are not exposed as builder setters.

An ordinary ACK timeout rotates the sender session, restores all unacknowledged
exact stamped operations to the front of the buffer in original order, and
replays them. LWW application makes duplicates idempotent; delayed ACKs from
the old session are rejected. A well-formed ACK below
`required_ack_durability` does not retire the batch or extend its original
deadline. Reconciliation barriers use `SenderConfig::reconcile_timeout`, not
`SenderRuntimeOptions::ack_timeout`.

Peer protocol 6 uses the same correlated reconciliation engine for static and
dynamic placement. Summary, fetch, data, repair, and ordinary replication
frames are split using their exact protobuf-encoded size, with a 1 MiB soft
page target and 16 MiB hard limit. The public actor surface includes
`SnapshotSummary`, `SummaryPage`, `VersionedPage`, `BatchReadEntry`,
`BatchReadPage`, `KvMessage::{GetBatch, GetSummaryPage, GetVersioned,
GetVersionedPage, GetRevision}`, and `KvMessage::FlushStable` for embedders that
need priority-aware ordered batch reads, revision-guarded pinned-TTL reads,
memory-bounded exact-value primitives, or an explicit backend fence.
`GetBatch` returns one encoded-byte-bounded ordered prefix while retaining
per-key pool admission and sticky routing. An orderly dispatcher drain returns
the prefix after its admitted key completes; callers resume the untouched
suffix explicitly. `FlushStable` returns
`ApplyDurability::StableCommitted` only after that fence succeeds. `GetAll` and
`GetSnapshot` are explicit local debug operations; the network responder uses
bounded pages instead of cloning an entire fragment.

`KvMessage` is `#[non_exhaustive]`: downstream matches include a wildcard arm.
The opaque `ContinueBatchRead` variant is dispatcher/
worker plumbing and cannot be constructed directly. Direct `ApplyOp` calls
return `ApplyError::InvalidInput` for a record that cannot traverse the wire;
this is nonfatal and does not restart a worker.
The managed `WorkerPool` is now private: its former public split
`try_admit`/`route` API could not bind one reservation atomically to one sticky
worker or roll back failed delivery. Direct actor embedders should send the
same public `KvMessage` variants to `KvActor`, which owns admission, routing,
supervision, and exact per-worker accounting.

Protocol implementations import `actordb_protocol::meta` and use exact peer
peer protocol 6 with unary `actordb.rpc.v1` application RPC. Reconciliation uses cursor, revision, page-sequence,
completion, and restart fields; acknowledgement levels explicitly distinguish
applied, store-committed, and stable-committed evidence. A non-empty
`BatchGetResponse` may now be an ordered request prefix; an empty response is
unavailable, never not-found. There are no generated whole-state sync messages
or unbounded sender constructors. `SenderMsg::{ReconcileResponse, ReconcileData}`
require exact session/round/page correlation. Direct actor integrations handle
all `ApplyDurability` and `ReplicationAckDurability` levels explicitly.

## Health and observability

```rust
// Local snapshots from this node's owned fragment supervisors.
let health = node.health().await?;
for (frag, h) in &health {
    println!("{frag}: workers={} restarts={:?} shed(high,default,bg)={:?}",
        h.worker_count, h.restarts, h.shed_counts);
}

// Placement view (Phase 8): authoritative map + per-fragment lifecycle state.
let report = node.placement_report().await;
println!("dynamic={} map={:?} states={:?}", report.dynamic, report.placement, report.states);
```

`FragmentHealth` fields: `fragment`, `worker_count`, `worker_names`,
`restarts` (per-child restart counts), and `shed_counts: (u64, u64, u64)` — operations
shed by backpressure per pool, ordered **(high, default, background)**, always
counted, never silent.

`health()` has one aggregate deadline (default five seconds, configurable with
`NodeRuntimeOptions::with_health_timeout`) covering supervisor-registry lookup
and every fragment RPC. A missing serving supervisor, stopped/nonresponding
dispatcher, or deadline expiry returns `NodeError::Actor`; unavailable counters
are never substituted with zeros. No partial successful map is returned.
Worker counts/names include only exact dispatcher-linked actors observed
`Running`, not configured slots or arbitrary actors with matching registry
names. The result may include owned bootstrapping/draining fragments and is a
local, non-atomic observation: it does not prove worker responsiveness, serving
readiness, replication convergence, or durability. Use `placement_report()` for
the separate placement lifecycle view.

The unchanged low-level `SupervisorMessage::GetHealth` also bounds its nested
dispatcher RPC using the supplied reply-port timeout, or five seconds when no
timeout is supplied. Already-closed queued reply ports are skipped. A probe
already executing may finish its bounded inner wait after its caller cancels;
this never extends the aggregate `Node::health()` deadline.

`sender_report(fragment, peer)` is a `#[doc(hidden)]` test hook. Production
monitoring uses `status_snapshot`, `metrics_snapshot` or their bounded detailed
variants; `health()` and `placement_report()` expose their separate snapshots.

## Low-level maintenance messages

`CleanerMessage::Sweep` and `SnapshotterMessage::Snap` still construct the same
manual full operations, and all maintenance/supervisor argument structs keep
their existing fields. `FragmentState::expire_values(now)` still performs a
full sweep and returns the total converted count. Automatic periodic work now
uses doc-hidden variants carrying opaque, non-forgeable admission leases.
Both public maintenance message enums are `#[non_exhaustive]`: downstream
matches include a wildcard arm. See
[maintenance bounds and consistency](persistence.md#bounded-automatic-maintenance).

## Errors

```rust
pub enum NodeError {
    Io(std::io::Error),        // transport / bind failures
    Codec(String),             // protobuf/encoded-size failures
    Actor(String),             // misconfiguration, timeouts talking to actors
    Network(String),           // no peer / no replica reachable
    Backpressure(String),      // shed by queue cap or rate limit — retry with backoff
    Storage(String),           // persistence failed during apply or shutdown flush
    Superseded { .. },         // another LWW version won; this write was not applied
    Expired,                   // TTL elapsed; its expiry tombstone was installed
    ShuttingDown,              // this Node no longer admits database operations
}
```

The distinction that matters in practice:

- A shed **read** returns `Err(NodeError::Backpressure(..))` — it never returns
  `Ok(None)`, so "not found" and "shed" can't be confused.
- A shed local or forwarded write returns `Err(NodeError::Backpressure(..))`
  directly from the serving fragment; it is never inferred from a read-back
  barrier.
- `Superseded` is explicit: the API never reports a write successful merely
  because a different, newer value was already present.
- `Network` vs `Actor`: connectivity problems vs. configuration problems (e.g.
  a fragment with no usable replica listed).

## Debug helpers

Public Node point/batch keys are literal KV keys, including strings that look
like internal namespaces. Table handles select their namespace explicitly;
see [literal keys and table execution](key-namespaces.md).

`node.get_all().await` returns every live key/value this node holds (hides
tombstones) — intended for debugging and tests, not for production scans.
Ambiguous mixed display keys (decoded KV versus other physical namespaces)
return an explicit error; per-table queries avoid that ambiguity.
