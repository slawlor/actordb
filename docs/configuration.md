# Configuration reference

Start with `NodeConfig::default()` and set the fields you need. With the
`rocksdb` feature, `NodeConfig::with_rocksdb(path)` layers persistence on top
of the defaults.

## `NodeConfig`

| Field | Type | Default | Guidance |
|---|---|---|---|
| `store` | `Option<Arc<dyn KvStore>>` | `None` | `None` = purely in-memory node. Set to a `RocksDbStore` (or `PrefixedStore`-wrapped) for durability. |
| `network` | `Arc<dyn Network>` | `TcpNetwork` | Plain TCP. Implement `Network` for TLS or custom transports. |
| `flush_batch_size` | `usize` | `1000` | Ops buffered before a store flush. Ignored when `store` is `None`. Larger = fewer, bigger RocksDB write batches. |
| `flush_interval` | `Duration` | `100 ms` | Max age of the unflushed write tail. `kill -9` can lose the tail; it is recovered via peer sync. |
| `fragment_count` | `usize` | `1` | Number of keyspace fragments. **Must be identical on every node** or keys become unreachable. |
| `fragment_map` | `HashMap<FragmentId, Vec<String>>` | empty | Fragment → replica node ids. Empty = this node serves all fragments. **Must be identical on every node** (static mode). |
| `members` | `HashMap<String, SocketAddr>` | empty | Static membership; `Node::start` best-effort dials all members. |
| `default_pool` | `PoolConfig` | `4` workers, cap `10_000`, no rate limit | Normal traffic. |
| `high_pool` | `PoolConfig` | `2` workers, cap `5_000`, no rate limit | Latency-sensitive traffic. |
| `background_pool` | `PoolConfig` | `2` workers, cap `5_000`, `1000`/s | Bulk traffic; rate-limited by default. |
| `clean_interval` | `Duration` | `30 s` | How often each fragment's cleaner sweeps TTL-expired keys and GC-eligible tombstones. |
| `snapshot_interval` | `Duration` | `60 s` | Snapshotter flush cadence (periodic durable flush, not a checkpoint). |
| `tombstone_gc_age` | `Duration` | `3600 s` (1 h) | Tombstone age before memory GC. **Must exceed the longest expected partition** — a node rejoining after its tombstones were GC'd can resurrect deleted keys. |
| `replication` | `SenderConfig` | see below | Per-fragment×peer sender tuning. |
| `placement_controller` | `Option<Arc<dyn PlacementController>>` | `None` | `None` = static placement. Set to `ManualPlacement` (or your own controller) for dynamic migration. |
| `placement_poll_interval` | `Duration` | `1 s` | Driver re-evaluation cadence. Ignored in static mode. |
| `max_clock_skew` | `Duration` | `300 s` (5 min) | Remote timestamps more than this far ahead of the local wall clock are clamped in `HlcClock::observe`. Bounds the damage from far-future timestamps (malicious or skewed peers). |
| `cluster_secret` | `Option<Vec<u8>>` | `None` | Pre-shared secret for handshake authentication. When set, `Hello` carries HMAC-SHA256(secret, node_id); mismatches drop the connection and pre-handshake messages are ignored. `None` = unauthenticated mesh (warning logged). Must be identical on every node. |

`clock_skew_ms` exists but is `#[doc(hidden)]` — it is a test-only HLC skew
simulator, not a tuning knob.

## `SenderConfig` (replication)

| Field | Default | Guidance |
|---|---|---|
| `batch_size` | `100` | Ops per replication batch. |
| `batch_interval` | `100 ms` | Max batching delay — bounds steady-state replication lag. |
| `max_in_flight` | `4` | Batches awaiting ack before sending pauses. There is **no ack timeout**; an unacked batch waits for session end. |
| `buffer_capacity` | `10_000` | Bounded per-sender buffer. On overflow the buffer is shed and the sender enters `BufferReset` (reconcile-on-reconnect). |
| `consistency_interval` | `300 s` | Periodic anti-entropy reconcile cadence per sender. |

## `PoolConfig`

| Field | Meaning |
|---|---|
| `workers` | Worker actors in the pool. Keys sticky-route (`seahash(key) % workers`), so more workers = more parallelism, not more ordering. |
| `queue_cap` | Max admitted-but-unprocessed ops. On overflow the op is shed and counted. |
| `rate_limit_per_sec` | Token-bucket rate limit (`None` = unlimited). Checked before the queue cap. |

## Hardcoded internals (not configurable)

These are `const`s, documented here so they don't surprise you:

| Constant | Value | Meaning |
|---|---|---|
| `MAX_FORWARD_HOPS` | `4` | Bounds re-forward chains during placement disagreement. |
| Remote-get timeout | `5 s` | Per-attempt timeout in `get_with_priority` / `get_multi_with_priority`, with failover to the next replica. |
| `BOOTSTRAP_TIMEOUT` | `30 s` | Max wait bootstrapping a gained fragment before going Serving anyway. |
| `DRAIN_TIMEOUT` | `10 s` | Max wait draining senders when losing a fragment. |
| `RECONCILE_FETCH_CHUNK` | `500` | Keys per `ReconcileFetch`/`ReconcileData` message. |
| `MAX_FRAME_BYTES` | `16 MiB` | Max protobuf envelope frame. |
| `PEER_CHANNEL_DEPTH` | `256` | Per-peer channel depth. |
| `RECONNECT_BASE` | `100 ms` | Reconnect backoff base, doubling. |
