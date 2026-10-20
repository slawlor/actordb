# TTL expiry callbacks

ActorDB can notify an application when local cleanup converts its current expired
live record into the existing deterministic expiry tombstone. Registration is
optional; it is disabled by default. No callback payload is copied or retained
when disabled. Enabled callbacks move the old `VersionedValue` into an owned event,
including its opaque `Vec<u8>` payload; no serializer or application format is required.

## Registration

```rust
use actordb::node::NodeConfig;
use actordb::ttl_expiry::{expiry_handler_fn, ExpiryBatch, ExpiryCallbackError};
use std::sync::Arc;

let handler = expiry_handler_fn(|batch: ExpiryBatch| async move {
    for event in batch.events {
        let original_version = event.original_version();
        let stable_id = event.id;
        // Apply application-specific handling to owned event.previous_value.
        let _ = (original_version, stable_id);
    }
    Ok::<(), ExpiryCallbackError>(())
});
let config = NodeConfig::builder().ttl_expiry_handler(Arc::new(handler)).build();
```

`TtlExpiryHandler` is object-safe, `Send + Sync`, and its `on_expired` method returns
`BoxFuture<'_, Result<(), ExpiryCallbackError>>`. Implementors can borrow their own
state for the call. The closure adapter accepts the familiar `|batch| async move`
pattern; clone captured application handles before the async block.

Each `ExpiryEvent` owns its fragment, key and previous `VersionedValue`, scheduled
`expires_at_ms`, observed materialization `observed_at_ms`, and stable identity.
`original_version()` returns the existing `(timestamp_ms, node_id)` LWW shape.
`ExpiryBatch` includes the cleanup Node's `local_node_id`, observed dispatch wall
time, and events. Node identity can differ from the original writer.

## Which transitions emit an event?

| Case | Notification behavior |
| --- | --- |
| Current expired live record converted by periodic, manual, or final shutdown cleanup | Emits an event, subject to bounded best-effort admission. |
| Concurrent replacement or TTL extension wins before cleanup acquires mutation synchronization | The replaced version emits nothing. A future expiry can report the new version. |
| Cleanup wins first, then a new value is written | The old event can arrive after the new value; consumers compare versions. |
| Reads, `get_all`, summaries, and snapshots synthesize expiry tombstones | No callback; these do not materialize the cache transition. |
| Explicit delete, overwrite, tombstone garbage collection, losing replicated update | No TTL cleanup event. |
| An unexpired replicated live version is installed, then later cleaned locally | Eligible for a local event if it is still the current version. |
| Already-expired write reaches apply | Apply installs its existing expiry tombstone directly. There is no cleaner transition and no callback, even though the incoming operation temporarily contains payload bytes. |
| An expiry tombstone synthesized by another replica is received | No local live-to-expired transition and no previous payload to report. |

Expiry checking and conversion occur under the same cache/persistence lock order
as ordinary winning mutations. The original value is moved intact during replacement,
and nonwaiting publication preserves this local fragment order. User code runs
on a separate dispatcher task. Existing tombstone versions, retained TTL floors,
replication, snapshot behavior and persistence buffering are preserved.

## Startup

An expired live durable record still contains its old value. Startup stages a
bounded event while performing the existing normalization, then publishes only
after records validate, expiry tombstones are written, and the HLC accepts the
persisted version floor. These events precede later cleaner events for the fragment.
They can execute before `Node::start` returns, including when another part of
startup subsequently fails; they prove a local transition, not successful Node startup.

Failed validation or normalization does not publish its staged events. Staged
events abandoned by failed startup release their budgets and are counted.
Persisted tombstones cannot reconstruct the previous payload and emit nothing
on reload. This is not replay: a crash may lose a notification, or a surviving
live durable version may be converted again and produce a duplicate identity.
A live durable record that is not expired at load can emit when cleaned later.

## Best-effort local delivery

The callback reports a local cleanup transition. It does **not** prove quorum
commitment, a stable-storage fence, recipient acknowledgement, or durable callback
delivery. Notifications can be dropped. No exactly-once delivery, replay after
restart, or cluster-wide ordering is promised. Cleanup and callback dispatch may
both occur after nominal expiry. A callback can succeed or partly act even when
the associated persistence flush later fails.

Multiple replicas can report the same version. `ExpiryEventId` is SHA-256 over
the domain `actordb-ttl-expiry-v1\0`, big-endian fragment id, length-prefixed UTF-8
key, original timestamp, and length-prefixed writer id. Lengths and timestamp are
big-endian u64; the fragment id is big-endian u32. Timing, payload encoding and
local Node identity are excluded. Applications deduplicate within their own
database namespace and **atomically compare versions before clearing application
state**, since the expired version may already be superseded.

Recipient selection, ownership filtering, client fanout, networking, decoding and
business meaning belong to applications. Stronger delivery would require a
separate durable outbox with an explicit atomicity and replay design.

## Bounds, batching and lifecycle

Configure `NodeConfig.ttl_expiry_options` with `TtlExpiryOptions::builder()`:

| Option | Default | Meaning |
| --- | --- | --- |
| `queue_event_capacity` | 1024 | Total retained queued, collecting, in-flight and staged startup events. |
| `queue_byte_capacity` | 8 MiB | Conservative retained data/metadata charge, including owned allocation capacities and startup indexing. |
| `batch_size` | 64 | Maximum events per call; must be positive and at most event capacity. |
| `max_batch_delay` | 20 ms | Maximum collection wait from first admission; backlog or an active handler can delay actual dispatch further. |
| `handler_timeout` | 5 s | Cooperative deadline for one handler call. |
| `shutdown_drain_timeout` | 5 s | One overall callback drain after final fragment cleanup and persistence preparation. |

Options are validated before startup side effects when a handler is enabled.
There is one serial dispatcher per Node: handler calls do not overlap, and FIFO
admission under mutation synchronization preserves order within each fragment.
Batches may contain several fragments. There are no automatic retries. Queue
saturation drops notifications without waiting or stopping cleanup, with counters
and sampled diagnostic messages. Handler errors and unwind panics are isolated.
Timeout drops the handler future, freeing its event budget; later events continue.
Errors, panics and cancellation do not exclude partial application side effects.

Normal shutdown finishes authoritative fragment cleanup, closes notification
admission, and drains with one manager-owned deadline. Partial batches flush
immediately. The deadline includes an active call; at expiry, its future and
remaining queued events are canceled and counted as abandoned. Canceling the
shutdown *waiter* does not detach work or reset the deadline: a later shutdown
resumes waiting for the same manager. Node Drop requests immediate cancellation;
its owned task is aborted and retained events are accounted on release.

Handlers must cooperate with async cancellation and avoid blocking runtime
threads or detaching work. Synchronous blocking code and abort-mode panics cannot
be preempted safely by Rust futures. The callback deadline does not bound the
database's existing storage/actor shutdown. Application-retained or cloned data
is outside the dispatcher budget, and byte charges are not an exact total heap bound.

## Diagnostics and example

`Node::ttl_expiry_diagnostics()` returns `None` when disabled, or an approximate
snapshot of queue/in-flight depth and bytes, capacity/closed drops, successful
handler events, failures, timeouts, abandoned events, and expiry-to-dispatch and
monotonic admission-to-dispatch latency totals/maxima. Successful delivery counts
mean the handler returned `Ok`, not that a remote recipient acknowledged. Counters
remain readable after shutdown. Wall-clock latency saturates at zero after clock
reversal. ActorDB logs categories and counters, never payloads, keys, or caller
error contents; application code and panic hooks control their own output.

Run the [bounded application-worker example](../actordb/examples/ttl_expiry_callbacks.rs):

```bash
cargo run -p actordb --example ttl_expiry_callbacks
```

It registers an async closure that forwards batches through a bounded application
channel. The worker conditionally clears its own versioned index under its own
lock, making duplicate and delayed events harmless to newer state.

The pending `0.7.0` release includes this API. Full `NodeConfig` struct literals
need the new fields; prefer its builder or `..Default::default()`. Low-level
`SupervisorArgs` literals add `expiry_publisher: None`; the builder omits this
opaque Node-owned ingress by default.
