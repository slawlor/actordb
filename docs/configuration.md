# Configuration reference

Start with `NodeConfig::default()` and set the fields you need. With the
`rocksdb` feature, `NodeConfig::with_rocksdb(path)` layers persistence on top
of the defaults using synchronous WAL commits. Use
`NodeConfig::with_rocksdb_options(path, RocksDbOptions)` for an explicit
durability or one-time legacy-manifest adoption policy.

## `NodeConfig`

| Field | Type | Default | Guidance |
|---|---|---|---|
| `store` | `Option<Arc<dyn KvStore>>` | `None` | `None` = purely in-memory node. Set to a base `RocksDbStore` or custom store; Node applies the per-fragment `PrefixedStore` views itself. |
| `network` | `Arc<dyn Network>` | `TcpNetwork` | Plain TCP. Implement `Network` for TLS or custom transports. |
| `flush_batch_size` | `usize` | `1000` | Ops buffered before a store flush. Ignored when `store` is `None`. Larger = fewer, bigger RocksDB write batches. |
| `flush_interval` | `Duration` | `100 ms` | A new write triggers a flush when the previous attempt is this old; idle fragments are bounded by `snapshot_interval`. |
| `fragment_count` | `usize` | `1` | Number of keyspace fragments, from 1 through `u32::MAX`. **Must be identical on every node** or keys become unreachable. RocksDB pins it in the database manifest. |
| `fragment_map` | `HashMap<FragmentId, Vec<String>>` | empty | Fragment → replica node ids. Empty = this node serves all fragments. A non-empty map must contain exactly `0..fragment_count`, with a non-empty, duplicate-free replica list per fragment. **Must be identical on every node** (static mode). |
| `members` | `HashMap<String, SocketAddr>` | empty | Static membership. Each non-local `(node_id, address)` becomes a persistent dial intent at startup, retries from the first failure, and knows the expected Hello id before its first authentication attempt. Member ids and addresses must map one-to-one. |
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
| `cluster_secret` | `Option<Vec<u8>>` | `None` | At least 32 independently generated, high-entropy random bytes for protocol-v3 mutual HMAC proofs. Each physical connection uses two fresh challenges, and the proof binds the ordered roles, ids, version ranges, capabilities, and challenges, so a captured v3 handshake cannot be replayed. The shared key proves fresh cluster-key possession, not individual member identity, and does not encrypt or integrity-protect later frames. Use a protected, mutually authenticated custom `Network` when those properties matter. Must be identical on every node. |

`clock_skew_ms` exists but is `#[doc(hidden)]` — it is a test-only HLC skew
simulator, not a tuning knob.

`fragment_count`, `replication.batch_size`, `replication.max_in_flight`, and
`replication.buffer_capacity` must be nonzero. Replication and
dynamic-placement intervals/timeouts must also be nonzero. `clean_interval`
and `snapshot_interval` must be nonzero even without a store, because both
maintenance actors run on in-memory nodes too. Maintenance, replication, and
enabled dynamic-placement timer values must fit the runtime clock;
placement intervals remain ignored in static mode. Bounded-channel capacities may not exceed
Tokio's semaphore maximum. `Node::start` rejects invalid values instead of
allowing channel or timer panics. The local node id, every `members` key, and
every placement replica id must contain 1 to 255 UTF-8 bytes. Placement replica
lists must be duplicate-free, and placement fragment ids must be in range. An
explicitly configured `cluster_secret` must contain at least 32 bytes; generate
it with a cryptographically secure random source. Fresh challenges prevent
replay, but because a captured transcript includes a verifiable HMAC over known
fields, a low-entropy password remains vulnerable to offline guessing.

## `RocksDbOptions`

`RocksDbOptions::default()` uses `RocksDbDurability::SyncWal`. Every successful
ordinary store batch keeps the WAL enabled and synchronously flushes it through
the operating-system buffer cache. Select `AsyncWal` with `with_durability` to
keep WAL recovery while allowing acknowledged recent writes to be lost on an
OS crash or power loss. Manifest creation and prefix deletion remain
synchronous in either mode, and a requested `StableCommitted` fence calls
`flush_wal(true)` even under `AsyncWal`; there is no WAL-disabled committed
mode.

RocksDB binds its first Node's id, `fragment_count`, data-format version, and
partitioner version in a metadata column family. A mismatch or corrupt/future
manifest returns `NodeError::Storage` before network bind. Unknown column
families and metadata records are also rejected. A non-empty database from a
pre-manifest release is rejected without adding a metadata column family unless
its one-time open explicitly uses `with_legacy_manifest_adoption(true)`; verify
the claimed fragment layout and back it up first, then remove the flag after a
successful start. Open-time rejection is a `StoreError` from the constructor.
Placement, membership, and durability policy may change without changing the
identity manifest.

One `Arc<RocksDbStore>` supports only one attached live Node. Explicit clean
shutdown and fully joined startup rollback release the attachment. Cancelled
startup and best-effort Node drop deliberately leave that same object
fail-closed because actor completion was not observed.

## `NodeRuntimeOptions`

Transport queue and liveness policy is kept outside `NodeConfig` so new policy
does not break exhaustive `NodeConfig` literals. `Node::start` uses
`NodeRuntimeOptions::default()`; pass a builder-configured value to
`Node::start_with_options` to override it.

| Builder / getter | Default | Guidance |
|---|---|---|
| `with_peer_queue_frame_capacity` / `peer_queue_frame_capacity` | `256` frames | Total frame-count capacity behind the peer writer's byte gate, split evenly between ordinary producer traffic and a writer-priority reader-response lane. Must be at least `2`. |
| `with_peer_queue_byte_capacity` / `peer_queue_byte_capacity` | `32 MiB` | Encoded bytes admitted to one peer writer pipeline, including the frame currently blocked on the socket. Must be at least `32 MiB`: one maximum 16 MiB producer frame plus a full 16 MiB response reserve. Ordinary producers cannot consume that reserve. Permits remain attached through write/drop. |
| `with_replication_control_message_capacity` / `replication_control_message_capacity` | `64` messages | Capacity of the separate ACK/NACK/reconciliation-response lane per fragment/peer sender. Full admission fails the contributing connection so session replay/reconciliation recovers it. A reconcile reply targeting a sender already retired by placement is ignored without dropping the shared peer connection. Peer lifecycle and on-demand reconcile signals are coalesced out of band. |
| `with_inbound_connection_capacity` / `inbound_connection_capacity` | `1024` connections | Maximum accepted sockets concurrently handshaking or serving as inbound peers. Excess accepted sockets are closed immediately. |
| `with_receive_byte_capacity` / `receive_byte_capacity` | `64 MiB` | Node-wide raw encoded receive bodies across accepted and outbound-dialed connections, including handshakes. Must fit at least one `16 MiB` maximum frame. Bytes are reserved before body allocation and retained through decoding and the inline handler turn. Saturation rejects only the contributing connection without waiting for capacity. |
| `with_relay_capacity` / `relay_capacity` | `1024` relays | Node-wide aggregate admission for relayed single reads and single/batched mutations. Includes pending timer registration/enqueue and live timers until they exit. Admission never waits; saturation leaves legacy reads unavailable and returns explicit mutation `Unavailable`. |
| `with_relay_key_byte_capacity` / `relay_key_byte_capacity` | `64 MiB` | Node-wide retained relay request-key storage: String headers and buffer capacities, plus batch Vec header and allocated String slots, including unused capacity. Not mutation payloads, writer buffers, decoded frames, caches, allocator overhead, or total heap. Fixed relay/timer overhead is separately count-bounded. No minimum other than nonzero; smaller budgets may reject otherwise legal key sets. |
| `with_serving_read_queue_capacity` / `serving_read_queue_capacity` | `64` jobs/class | Node-wide serving single/batch-read queue per priority, backed by eight fixed workers/class. Count admission also includes active and actor-retained jobs (queue capacity + eight). Nonzero. |
| `with_serving_read_key_byte_capacity` / `serving_read_key_byte_capacity` | `64 MiB`/class | Node-wide retained decoded read-key storage per priority, charging String buffer capacities and Vec/String headers through actor ownership after waiter cancellation. Separate from raw receive bodies, values, cache, and writer queues; at least 16 MiB. |
| `with_queue_admission_timeout` / `queue_admission_timeout` | `5 s` | One absolute maximum wait to reserve a peer-queue frame slot and its exact encoded-byte permits. Expiry cancels that connection; an outbound peer is then redialed. |
| `with_reconcile_data_admission_timeout` / `reconcile_data_admission_timeout` | `30 s` | Admission wait for each streamed reconciliation-data page. This longer, explicit budget lets a legal near-16 MiB value wait behind another frame while the socket writer is still healthy. Tune with `SenderConfig::reconcile_timeout` and `socket_write_timeout`. |
| `with_forwarded_attempt_timeout` / `forwarded_attempt_timeout` | `5 s` | Total budget for one forwarded owner attempt, including queue admission and response. Serving read jobs use a separate absolute deadline from receipt through service queue/actor wait. Batch holder attempts and remote fragment groups remain sequential. |
| `with_connect_timeout` / `connect_timeout` | `5 s` | Maximum duration of one outbound transport dial. Configured member dialers retry failures with exponential backoff; manual `connect` returns the initial failure. |
| `with_handshake_timeout` / `handshake_timeout` | `5 s` | One absolute deadline for both `Hello` and `HelloAuth` flights. A peer cannot extend it by sending partial handshake data. |
| `with_socket_read_timeout` / `socket_read_timeout` | `30 s` | Maximum time to receive a frame body after its length prefix. An idle established connection has no read deadline until a prefix arrives. |
| `with_socket_write_timeout` / `socket_write_timeout` | `30 s` | Maximum time to write one complete length-prefixed frame. Expiry cancels the connection and allows redial. |
| `with_replication_ack_timeout` / `replication_ack_timeout` | `30 s` | Maximum wait after queue admission for an acceptable ordinary streaming ACK. Expiry rotates the sender session and replays the exact unacknowledged LWW operations. Reconciliation barriers use `SenderConfig::reconcile_timeout` instead. |
| `with_health_timeout` / `health_timeout` | `5 s` | One aggregate local-health deadline, including registry acquisition and all fragment probes. Failed evidence/deadline expiry returns an error instead of healthy zeros; not a readiness or worker-responsiveness guarantee. Must be nonzero and representable. |

The outbound frame capacity must be at least two, its byte capacity at least
32 MiB, receive and read-key capacities at least 16 MiB, and every other capacity and
runtime timeout nonzero; startup rejects invalid or unrepresentable values.
The receive budget counts encoded bodies, excluding four-byte length prefixes,
socket/kernel buffers, protobuf object overhead, and decoded or actor/cache/
relay/caller copies. There is no wire compression. It is not a total heap bound.
Error, EOF, body timeout, connection cancellation, and task abort release raw
receive reservations through RAII. Operation priority selects the local or remote actor pool, but it does
not schedule the ordinary peer data lane or socket writer. The peer writer
applies both the configured frame-count and encoded-byte bounds; sender actor
lanes are still bounded by operation/message count.

Relay capacity is reserved without waiting before registration and retained
through the downstream attempt and its timer's exit. Registration/enqueue
failure or cancellation rolls back synchronously. Each relay is pinned to its
exact origin and expected-owner physical channels; disconnect or replacement
reclaims only work belonging to those channels, not new work with the same
authenticated peer id. Relay timeout uses `forwarded_attempt_timeout` and does
not invent a legacy single-read miss or change mutation stamps/durability.

## `SenderConfig` (replication)

| Field | Default | Guidance |
|---|---|---|
| `batch_size` | `100` | Maximum ops per replication batch. Exact encoded-size bounds may split a batch sooner. |
| `batch_interval` | `100 ms` | Max batching delay — bounds steady-state replication lag. |
| `max_in_flight` | `4` | Ordinary batches awaiting ACK before sending pauses. Each gets the `NodeRuntimeOptions::replication_ack_timeout` deadline after queue admission. |
| `buffer_capacity` | `10_000` | Bound on both the Node sender's locally-applied-mutation ingress lane and its sender-owned deque. Either overflow advances a persistent `BufferReset` generation; a live sender rotates the session and starts correlated reconciliation without blocking the client write. Total retained operations also include bounded in-flight batches. This is an operation-count bound, not a byte bound. |
| `consistency_interval` | `300 s` | Periodic anti-entropy reconcile cadence per sender. |
| `reconcile_timeout` | `30 s` | Max wait after enqueueing each reconcile summary, data, or barrier phase. Timeout rotates the session and retries. This also governs reconciliation barrier ACKs; ordinary streaming ACKs use the runtime ACK timeout. |
| `required_ack_durability` | `MemoryApplied` | Minimum receiver fence allowed to retire an ordinary batch. `StoreCommitted` requires a configured store and means acceptance under its ordinary policy. `StableCommitted` additionally requires the backend's explicit stable fence; RocksDB synchronously flushes its WAL even when ordinary writes use `AsyncWal`. Dynamic prefix retirement privately requires `StableCommitted` regardless of this setting. |

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

## `ReplicationDrainOptions` (call-specific shutdown policy)

`ReplicationDrainOptions::default()` gives
`Node::shutdown_with_replication_drain` one 30-second absolute budget for its
remote phase. `with_timeout(Duration)` changes that budget and `timeout()`
reads it; zero or a duration that cannot be represented by Tokio's clock is
rejected by the call. This setting is intentionally not part of `NodeConfig`:
ordinary `Node::shutdown()` remains a prompt local-only lifecycle operation,
and each planned stop chooses its own tolerance for unavailable peers.

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
| `queue_cap` | Max active-plus-queued jobs accepted from the dispatcher into this priority's workers. It does not bound the shared dispatcher mailbox or replicated ingress. On overflow a client op is shed and counted. |
| `rate_limit_per_sec` | Token-bucket rate limit (`None` = unlimited). Checked before the queue cap. |

## Hardcoded internals (not configurable)

These are `const`s, documented here so they don't surprise you:

| Constant | Value | Meaning |
|---|---|---|
| `WIRE_PROTOCOL_VERSION` | `3` | The current binary accepts only wire v3. V3 requires paged-anti-entropy, byte-bounded-batches, and stable-durability-ACK capabilities. Byte-bounded batches change `BatchGetResponse` to ordered-prefix semantics; stable durability ACKs gate destructive RocksDB prefix retirement. Binaries missing either contract are rejected, so upgrades must be coordinated. |
| `MAX_FORWARD_HOPS` | `4` | Bounds re-forward chains during placement disagreement. |
| `NACK_RETRY_DELAY` | `1 s` | Minimum delay before retrying a receiver-rejected replication batch. |
| `BOOTSTRAP_TIMEOUT` | `30 s` | Max wait for one of the connected, already-serving source candidates to complete bootstrap. Timeout leaves the gained fragment non-authoritative and eligible for retry. |
| `MAX_RECONCILE_FETCH_KEYS` | `256` | Maximum keys per exact-byte-bounded fetch chunk. Values are read incrementally and returned in byte-bounded pages through the writer byte budget. |
| `RECONCILE_SUMMARY_PAGE_ITEMS` / `RECONCILE_LOCAL_READ_CHUNK` | `256` | Maximum summaries read per remote/local cache page; exact encoded-size bounds may split a wire page sooner. |
| `MAX_WIRE_BATCH_ITEMS` | `4096` | Defensive repeated-field ceiling checked before protobuf decode for replication and forwarded request/response batches. This is separate from exact encoded-byte limits. |
| `MAX_BATCH_GET_REQUEST_KEYS` | `256` | Producer-side key ceiling for one pull request. Responses advance by an expected-peer-correlated ordered prefix and resume the suffix under a fresh request id. |
| `ANTI_ENTROPY_PAGE_BYTES` | `1 MiB` | Soft encoded-body target for summaries, fetches, data, repairs, and ordinary replication. One individually valid record may exceed the soft target. |
| `MAX_FRAME_BYTES` | `16 MiB` | Hard maximum encoded protobuf envelope body. Irreducible oversized mutations are rejected before local apply. |
| `RECONNECT_BASE` | `100 ms` | Reconnect backoff base, doubling. |
| `RECONNECT_MAX` | `30 s` | Reconnect backoff cap; attempts add up to 50% jitter. |

There is no setting that permits records above `MAX_FRAME_BYTES`. On startup,
durable records written by an older binary are preflighted against every
required single-record envelope; an oversized legacy record, or one whose
provenance node id is empty or exceeds 255 UTF-8 bytes, fails startup rather
than loading data that can never replicate or be returned remotely. Repair or
remove that record offline before restarting.
