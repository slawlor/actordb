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
| `flush_interval` | `Duration` | `100 ms` | A new write triggers a flush when the previous attempt is this old; idle fragments are bounded by `snapshot_interval`. |
| `fragment_count` | `usize` | `1` | Number of keyspace fragments. **Must be identical on every node** or keys become unreachable. |
| `fragment_map` | `HashMap<FragmentId, Vec<String>>` | empty | Fragment → replica node ids. Empty = this node serves all fragments. **Must be identical on every node** (static mode). |
| `members` | `HashMap<String, SocketAddr>` | empty | Static membership; `Node::start` best-effort dials all members. |
| `default_pool` | `PoolConfig` | `4` workers, cap `10_000`, no rate limit | Normal traffic. |
| `high_pool` | `PoolConfig` | `2` workers, cap `5_000`, no rate limit | Latency-sensitive traffic. |
| `background_pool` | `PoolConfig` | `2` workers, cap `5_000`, `1000`/s | Bulk traffic; rate-limited by default. |
| `clean_interval` | `Duration` | `30 s` | How often each fragment's cleaner materializes TTL-expiry tombstones and, when enabled, sweeps GC-eligible tombstones. |
| `snapshot_interval` | `Duration` | `60 s` | Snapshotter flush cadence (periodic durable flush, not a checkpoint). |
| `tombstone_gc_age` | `Option<Duration>` | `None` | `None` retains tombstones indefinitely. `Some(age)` enables explicitly unsafe, memory-only GC. No age makes it safe for replication: sender buffers, `MemoryApplied` crashes, and restart lose the progress needed to prove deletion convergence. |
| `replication` | `SenderConfig` | see below | Per-fragment×peer sender tuning. |
| `placement_controller` | `Option<Arc<dyn PlacementController>>` | `None` | `None` = static placement. Set to `ManualPlacement` (or your own controller) for dynamic migration. |
| `placement_poll_interval` | `Duration` | `1 s` | Driver re-evaluation cadence. Ignored in static mode. |
| `placement_drain_warn_interval` | `Duration` | `10 s` | Warning cadence while a revoked fragment cannot safely drain. This never forces destructive teardown; the node remains `Draining` until recovery completes. |
| `max_clock_skew` | `Duration` | `300 s` (5 min) | Peer and persisted timestamps farther ahead than this are rejected before storage. A violating peer connection is closed; poisoned durable state fails startup. |
| `cluster_secret` | `Option<Vec<u8>>` | `None` | Pre-shared secret for handshake authentication. When set, `Hello` carries HMAC-SHA256(secret, node_id); mismatches drop the connection and pre-handshake messages are ignored. `None` = unauthenticated mesh (warning logged). Must be identical on every node. |

`clock_skew_ms` exists but is `#[doc(hidden)]` — it is a test-only HLC skew
simulator, not a tuning knob.

`fragment_count`, `replication.batch_size`, and
`replication.max_in_flight` must be nonzero. Replication and dynamic-placement
intervals/timeouts must also be nonzero; `Node::start` rejects invalid values
instead of allowing zero-sized batches or timer panics.

## `SenderConfig` (replication)

| Field | Default | Guidance |
|---|---|---|
| `batch_size` | `100` | Ops per replication batch. |
| `batch_interval` | `100 ms` | Max batching delay — bounds steady-state replication lag. |
| `max_in_flight` | `4` | Batches awaiting ACK before sending pauses. There is **no ACK timeout**; a NACK or session change retries, but a silent open peer can stall at this bound. |
| `buffer_capacity` | `10_000` | Bounded per-sender buffer. Overflow enters `BufferReset`, rotates the live session, and immediately starts correlated reconciliation; reconnect is not required. |
| `consistency_interval` | `300 s` | Periodic anti-entropy reconcile cadence per sender. |
| `reconcile_timeout` | `30 s` | Max wait after enqueueing each reconcile summary, data, or barrier phase. Timeout rotates the session and retries. Outbound peer-queue admission is not yet separately timed. |
| `required_ack_durability` | `MemoryApplied` | Minimum receiver fence allowed to retire a batch. Set `StoreCommitted` only when every target has a configured store. Store acceptance does not imply fsync unless the backend guarantees it. |

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
| Remote-mutation deadline | `5 s` | Covers queueing plus response. Batch puts fail over; single puts currently select one owner. |
| Remote-read response timeout | `5 s` | Starts after queueing. Batch gets fail over; single gets currently select one owner. |
| `NACK_RETRY_DELAY` | `1 s` | Minimum delay before retrying a receiver-rejected replication batch. |
| `BOOTSTRAP_TIMEOUT` | `30 s` | Max wait bootstrapping a gained fragment before going Serving anyway. |
| `RECONCILE_FETCH_CHUNK` | `500` | Keys per `ReconcileFetch`/`ReconcileData` message. |
| `MAX_FRAME_BYTES` | `16 MiB` | Max protobuf envelope frame. |
| `PEER_CHANNEL_DEPTH` | `256` | Per-peer channel depth. |
| `RECONNECT_BASE` | `100 ms` | Reconnect backoff base, doubling. |
