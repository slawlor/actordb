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
| `clean_interval` | `Duration` | `30 s` | How often each fragment's cleaner materializes TTL-expiry tombstones. |
| `snapshot_interval` | `Duration` | `60 s` | Snapshotter flush cadence (periodic durable flush, not a checkpoint). |
| `tombstone_gc_age` | `Option<Duration>` | `None` | Compatibility field for the removed collector. Must remain `None`; `Node::start` rejects every `Some(age)` because elapsed time cannot prove replica convergence. |
| `replication` | `SenderConfig` | see below | Per-fragment×peer sender tuning. |
| `placement_controller` | `Option<Arc<dyn PlacementController>>` | `None` | `None` = static placement. Set to `ManualPlacement` (or your own controller) for dynamic migration. |
| `placement_poll_interval` | `Duration` | `1 s` | Driver re-evaluation cadence. Ignored in static mode. |
| `placement_drain_warn_interval` | `Duration` | `10 s` | Warning cadence while a revoked fragment cannot safely drain. This never forces destructive teardown; the node remains `Draining` until recovery completes. |
| `max_clock_skew` | `Duration` | `300 s` (5 min) | Peer and persisted timestamps farther ahead than this are rejected before storage. A violating peer connection is closed; poisoned durable state fails startup. |
| `cluster_secret` | `Option<Vec<u8>>` | `None` | Non-empty pre-shared secret for a static Hello HMAC over identity, protocol range, and capabilities. Mismatches drop the connection and pre-Hello messages are ignored, but the tag has no fresh challenge and a captured Hello is replayable. Use a protected custom `Network` transport for peer identity. Must be identical on every node. |

`clock_skew_ms` exists but is `#[doc(hidden)]` — it is a test-only HLC skew
simulator, not a tuning knob.

`fragment_count`, `replication.batch_size`, `replication.max_in_flight`, and
`replication.buffer_capacity` must be nonzero. Replication and
dynamic-placement intervals/timeouts must also be nonzero; replication timer
values must fit Tokio's clock, and bounded-channel capacities may not exceed
Tokio's semaphore maximum. `Node::start` rejects invalid values instead of
allowing channel or timer panics. An explicitly configured `cluster_secret`
must not be empty.

## `NodeRuntimeOptions`

Transport queue and liveness policy is kept outside `NodeConfig` so new policy
does not break exhaustive `NodeConfig` literals. `Node::start` uses
`NodeRuntimeOptions::default()`; pass a builder-configured value to
`Node::start_with_options` to override it.

| Builder / getter | Default | Guidance |
|---|---|---|
| `with_peer_queue_frame_capacity` / `peer_queue_frame_capacity` | `256` frames | Capacity of the single outbound queue per peer connection. This is a frame-count bound, not a byte bound, and all traffic classes share it. |
| `with_replication_control_message_capacity` / `replication_control_message_capacity` | `64` messages | Capacity of the separate ACK/NACK/reconciliation-response lane per fragment/peer sender. Full or closed admission fails the contributing connection so session replay/reconciliation recovers it. Peer lifecycle and on-demand reconcile signals are coalesced out of band. |
| `with_queue_admission_timeout` / `queue_admission_timeout` | `5 s` | Maximum wait to reserve one peer-queue slot. Expiry cancels that connection; an outbound peer is then redialed. |
| `with_forwarded_attempt_timeout` / `forwarded_attempt_timeout` | `5 s` | Total budget for one forwarded owner attempt, including queue admission and response. Batch holder attempts and remote fragment groups remain sequential. |
| `with_socket_write_timeout` / `socket_write_timeout` | `30 s` | Maximum time to write one complete length-prefixed frame. Expiry cancels the connection and allows redial. |
| `with_replication_ack_timeout` / `replication_ack_timeout` | `30 s` | Maximum wait after queue admission for an acceptable ordinary streaming ACK. Expiry rotates the sender session and replays the exact unacknowledged LWW operations. Reconciliation barriers use `SenderConfig::reconcile_timeout` instead. |

Capacity and every runtime timeout must be nonzero; startup rejects invalid
values. Operation priority selects the local or remote actor pool, but it does
not schedule the shared peer queue or socket writer.

## `SenderConfig` (replication)

| Field | Default | Guidance |
|---|---|---|
| `batch_size` | `100` | Ops per replication batch. |
| `batch_interval` | `100 ms` | Max batching delay — bounds steady-state replication lag. |
| `max_in_flight` | `4` | Ordinary batches awaiting ACK before sending pauses. Each gets the `NodeRuntimeOptions::replication_ack_timeout` deadline after queue admission. |
| `buffer_capacity` | `10_000` | Bound on both the Node sender's locally-applied-mutation ingress lane and its sender-owned deque. Either overflow advances a persistent `BufferReset` generation; a live sender rotates the session and starts correlated reconciliation without blocking the client write. Total retained operations also include bounded in-flight batches. This is an operation-count bound, not a byte bound. |
| `consistency_interval` | `300 s` | Periodic anti-entropy reconcile cadence per sender. |
| `reconcile_timeout` | `30 s` | Max wait after enqueueing each reconcile summary, data, or barrier phase. Timeout rotates the session and retries. This also governs reconciliation barrier ACKs; ordinary streaming ACKs use the runtime ACK timeout. |
| `required_ack_durability` | `MemoryApplied` | Minimum receiver fence allowed to retire a batch. Set `StoreCommitted` only when every target has a configured store. Store acceptance does not imply fsync unless the backend guarantees it. |

The batch size, in-flight limit, buffer capacity, intervals, and timeout must
all be nonzero. Timer values must also be representable by Tokio's clock.
`Node::start*` rejects an invalid replication configuration before binding;
standalone callers get `SenderOptionsError` from
`spawn_sender_with_options` (the legacy `spawn_sender` entry point panics on
an invalid configuration).

An ACK below `required_ack_durability` is observable but does not retire its
batch or extend the batch's original ordinary ACK deadline. At expiry the
sender rotates its session and restores every in-flight batch to the front in
original operation order. Replays retain the original LWW versions, so a
duplicate apply is idempotent; delayed ACKs from the previous session are
ignored.

## `SenderRuntimeOptions` (standalone senders)

Direct users of `actordb::replication::spawn_sender` get a 30-second ordinary
ACK timeout and 5-second peer-queue admission timeout. Use
`spawn_sender_with_options` with a `SenderRuntimeOptions::default()` value
modified by `with_ack_timeout` and `with_queue_admission_timeout` to override
them; the function returns `SenderOptionsError` for invalid sender limits or
timeouts, including values that Tokio's clock cannot represent.
`SenderConfig::reconcile_timeout` still controls summary, data, and barrier
phases. Node users normally configure the corresponding values through
`NodeRuntimeOptions` instead. For source compatibility these standalone
constructors still return `mpsc::UnboundedSender<SenderMsg>`; the bounded
mutation/control split is the `Node` integration's lifecycle-aware path.

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
| `NACK_RETRY_DELAY` | `1 s` | Minimum delay before retrying a receiver-rejected replication batch. |
| `BOOTSTRAP_TIMEOUT` | `30 s` | Max wait bootstrapping a gained fragment before going Serving anyway. |
| `RECONCILE_FETCH_CHUNK` | `500` | Keys per `ReconcileFetch`/`ReconcileData` message. |
| `MAX_FRAME_BYTES` | `16 MiB` | Max protobuf envelope frame. |
| `RECONNECT_BASE` | `100 ms` | Reconnect backoff base, doubling. |
