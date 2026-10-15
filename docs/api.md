# Public API guide

The main application surface lives in `actordb::node`. Start from
`NodeConfig::default()` for database behavior and `NodeRuntimeOptions::default()`
for transport/replication liveness policy — see
[Configuration](configuration.md) for every knob.

## Starting a node

```rust
use actordb::node::{Node, NodeConfig};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), actordb::node::NodeError> {
    let config = NodeConfig::default();
    let node = Node::start("node-a".to_string(), "127.0.0.1:7447".parse::<SocketAddr>()?, config).await?;
    println!("listening on {}", node.addr());

    // Form the peer mesh; Ok means a Node-owned redial supervisor now owns
    // the successful initial transport (authentication remains asynchronous).
    node.connect("127.0.0.1:7448".parse::<SocketAddr>()?).await?;
    // First stop admitting work and await application operation tasks.
    node.shutdown().await;
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

  let runtime = NodeRuntimeOptions::default()
      .with_peer_queue_frame_capacity(512)
      .with_replication_control_message_capacity(128)
      .with_inbound_connection_capacity(512)
      .with_queue_admission_timeout(Duration::from_secs(2))
      .with_forwarded_attempt_timeout(Duration::from_secs(8))
      .with_connect_timeout(Duration::from_secs(3))
      .with_handshake_timeout(Duration::from_secs(3))
      .with_socket_read_timeout(Duration::from_secs(20))
      .with_socket_write_timeout(Duration::from_secs(20))
      .with_replication_ack_timeout(Duration::from_secs(20));
  let node = Node::start_with_options(
      "node-a".to_string(),
      "127.0.0.1:7447".parse()?,
      NodeConfig::default(),
      runtime,
  )
  .await?;
  ```

  Defaults are 256 queued frames per peer, 64 protocol-control messages per
  fragment/peer sender, and 1024 concurrent inbound connections. Queue
  admission, a forwarded owner attempt, a dial, and the complete two-flight
  handshake each default to 5 seconds. Frame-body reads after a prefix, complete
  frame writes, and acceptable ordinary replication ACKs each default to 30
  seconds. Idle established connections have no read deadline until a prefix
  arrives. Locally applied mutations have a separate lane sized by
  `SenderConfig::buffer_capacity`. All capacities/timeouts must be nonzero;
  capacities are item counts, not byte bounds.
- `node.connect(peer_addr)` tries the first transport dial once and returns an
  error if it cannot connect within the dial deadline. `Ok` means the successful
  stream has been handed to a Node-owned redial supervisor, not that
  authentication or static full-state sync has completed. Concurrent calls for
  the same address wait for that pending initial dial and do not return early
  merely because a dial is in progress. On success they share the active
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
  duplicate-free replica lists. An empty static map is the legacy
  full-replication mode.
- `node.id()`, `node.addr()`, `node.peer_count()` are the basic inspectors.
- `node.shutdown().await` closes Node-owned task admission, aborts and joins the
  listener, connection, writer, dial, timer, and placement tasks, stops
  replication senders, then stops and joins each fragment supervisor. Each
  dispatcher drains its pools and flushes the store, releasing the RocksDB lock.
  The method is idempotent and a cancelled waiter can call it again.

`shutdown` owns Node background work, but it is not a fence around arbitrary
public API futures already running in application tasks. Stop admitting calls
and await those tasks before shutdown. Merely dropping a `Node` requests
best-effort transport cancellation and actor stop, but cannot await a durable
flush, task join, or RocksDB lock release.

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

### Priorities

```rust
use actordb::actor::OpPriority;

// Latency-sensitive write, normal read, bulk backfill.
node.put_with_priority("hot:key".into(), data, OpPriority::High).await?;
node.get_with_priority("hot:key".into(), OpPriority::High).await?;
node.put_multi_with_priority(batch, OpPriority::Background).await;
```

`OpPriority` is `High` / `Default` / `Background` (`Default` is the
`#[default]`). Priorities map to three isolated worker pools per fragment; a
shed operation surfaces as `NodeError::Backpressure` — never as a silent
success or `Ok(None)`. Local and forwarded puts wait for the serving worker's
exact apply outcome; once it returns `Ok`, reads at that owner observe that
version or a newer winner. The priority travels in the request and selects the
remote actor pool, but it does not schedule the shared per-peer socket queue.
See
[Consistency](consistency.md) and [Traffic classes & backpressure](backpressure.md).

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
- Keys are grouped by destination fragment; remote groups go out as **one
  `BatchPut` / `BatchGet` message per fragment and owner attempt** (not per
  key).
- Remote puts and gets have one **configured per-attempt deadline** (5 seconds
  by default) covering outbound queue admission and the response. Both fail over on a closed
  connection or missing reply; put replies additionally
  require one ordered exact outcome per key, and an all-`Unavailable` put
  response tries the next holder.
- Remote fragment groups are awaited sequentially, as are holder attempts
  within a group; these APIs do not issue all fan-out attempts concurrently.
- Per-key errors use `NodeError`: `Actor` = misconfiguration (no usable
  replica), `Network` = no replica reachable, `Backpressure` = shed.

## Advanced replication embedding

`actordb::replication::spawn_sender` starts one per-fragment/per-peer sender
with `SenderRuntimeOptions::default()`. Embedders that construct senders
directly can call `spawn_sender_with_options(fragment, peer_id, config, deps,
options)` instead; it returns `SenderOptionsError` for zero or unrepresentable
sender limits and timeouts. Its public options are:

- `with_ack_timeout` (default 30 seconds): deadline for an acceptable ACK to an
  ordinary streaming batch after queue admission;
- `with_queue_admission_timeout` (default 5 seconds): maximum wait to reserve a
  frame slot in the peer queue.

An ordinary ACK timeout rotates the sender session, restores all unacknowledged
exact stamped operations to the front of the buffer in original order, and
replays them. LWW application makes duplicates idempotent; delayed ACKs from
the old session are rejected. A well-formed ACK below
`required_ack_durability` does not retire the batch or extend its original
deadline. Reconciliation barriers use `SenderConfig::reconcile_timeout`, not
`SenderRuntimeOptions::ack_timeout`.

## Health and observability

```rust
// One snapshot per fragment this node serves.
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
`restarts` (per-child restart counts), `last_reaped` (a legacy compatibility
field that is always zero), and `shed_counts: (u64, u64, u64)` — operations
shed by backpressure per pool, ordered **(high, default, background)**, always
counted, never silent.

`sender_report(fragment, peer)` is a `#[doc(hidden)]` test hook; production
monitoring should use `health()` and `placement_report()`. There is also
`sync_entries_applied()` — the count of full-state sync entries applied since
start (stays 0 when dynamic placement owns anti-entropy).

## Errors

```rust
pub enum NodeError {
    Io(std::io::Error),        // transport / bind failures
    Codec(String),             // protobuf decode failures
    Actor(String),             // misconfiguration, timeouts talking to actors
    Network(String),           // no peer / no replica reachable
    Backpressure(String),      // shed by queue cap or rate limit — retry with backoff
    Storage(String),           // persistence failed during apply
    Superseded { .. },         // another LWW version won; this write was not applied
    Expired,                   // TTL elapsed; its expiry tombstone was installed
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

`node.get_all().await` returns every live key/value this node holds (hides
tombstones) — intended for debugging and tests, not for production scans.
