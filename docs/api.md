# Public API guide

The entire public surface lives in `actordb::node` (re-exported at the crate
root's `node` module). Start from `NodeConfig::default()` and set the fields
you need — see [Configuration](configuration.md) for every knob.

## Starting a node

```rust
use actordb::node::{Node, NodeConfig};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), actordb::node::NodeError> {
    let config = NodeConfig::default();
    let node = Node::start("node-a".to_string(), "127.0.0.1:7447".parse::<SocketAddr>()?, config).await?;
    println!("listening on {}", node.addr());

    // Form the peer mesh (best-effort; a redial supervisor keeps it alive).
    node.connect("127.0.0.1:7448".parse::<SocketAddr>()?).await?;
    Ok(())
}
```

- `Node::start(node_id, listen_addr, config)` binds `listen_addr` (port `0`
  picks an ephemeral port; the bound address is returned by `node.addr()`).
- `node.connect(peer_addr)` dials one peer. Dialing is best-effort and
  retried by the redial supervisor; dropping a connection triggers redial.
- `node.id()`, `node.addr()`, `node.peer_count()` are the basic inspectors.
- `node.shutdown().await` aborts connection tasks and stops each fragment
  supervisor (each dispatcher drains its pools and flushes the store, releasing
  the RocksDB lock).

## Single-key operations

```rust
// Write (default priority). Applies locally, then forwards to the owner
// if this node doesn't serve the key's fragment.
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
`Ok(None)`. Important: **read-your-write holds within a priority, not across
priorities** — a default-priority read is not ordered behind a background write
to the same key. See [Consistency](consistency.md) and
[Traffic classes & backpressure](backpressure.md).

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
  `BatchPut` / `BatchGet` message per fragment** (not per key).
- Remote puts fail over across replica holders until one accepts the batch,
  but are **fire-and-forget** — there is no end-to-end ack.
- Remote gets have a **5-second per-attempt timeout** and fail over to the next
  replica holder on timeout.
- Per-key errors use `NodeError`: `Actor` = misconfiguration (no usable
  replica), `Network` = no replica reachable, `Backpressure` = shed.

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
`restarts` (per-child restart counts), `last_reaped` (keys the cleaner reaped
on its last sweep), and `shed_counts: (u64, u64, u64)` — operations shed by
backpressure per pool, ordered **(high, default, background)**, always counted,
never silent.

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
}
```

The distinction that matters in practice:

- A shed **read** returns `Err(NodeError::Backpressure(..))` — it never returns
  `Ok(None)`, so "not found" and "shed" can't be confused.
- A shed **write barrier** returns
  `Err(NodeError::Backpressure("put barrier shed: ..."))`.
- `Network` vs `Actor`: connectivity problems vs. configuration problems (e.g.
  a fragment with no usable replica listed).

## Debug helpers

`node.get_all().await` returns every live key/value this node holds (hides
tombstones) — intended for debugging and tests, not for production scans.
