# Lifecycle support in 0.8.1

Upgrade the four public crate requirements to compatible `0.8.1` ranges.
The new drain, status and metrics interfaces are additive. Peer protocol 5,
application protocol 3, and RocksDB format 2 remain unchanged. Existing
default write-concern policies keep their encoding and override behavior.

## Shutdown and ownership

`begin_drain()` closes application admission synchronously. Requests admitted
before the cutoff keep completion ownership through actor execution even if
their response waiter disconnects or times out. `drain_requests(DrainOptions)`
provides a bounded application-work boundary and leaves the node alive with
peer replication and maintenance running. A timeout leaves admission closed;
retry with a new budget. Draining is one-way and has no resume operation.

Use `finish_shutdown(DrainOptions)` or existing `shutdown()` for the completed
receive/apply and local store shutdown barrier. It now also joins decoded
replication/reconciliation work that previously could outlive its stream.
This strengthens existing completion and storage ownership promises. Handling
a shutdown error or timeout still requires retry; dropping the node cannot
report completion. A blocked backend can therefore keep shutdown pending.

Request drain is not a stable checkpoint. Local shutdown is not remote
replication evidence. Existing `shutdown_with_replication_drain` establishes
its separate per-target configured durability contract before local stop.
See [draining](draining.md) and [replacement safety](replacement-safety.md).

## Observation

`status_snapshot()` and `metrics_snapshot()` are bounded pull observations.
Contended locks or truncated sections remain identifiable. Missing lag,
catch-up fractions, latency families or durability evidence are unavailable;
do not substitute zero. Check freshness/readiness together with application
writer authority before reporting serving capacity.

Collectors run after node/report guards are released. Repeated collection
does not reset cumulative metrics; adapters must use cumulative observations
or compute deltas rather than repeatedly adding totals. The node incarnation
identifies resets across restarts. See [status](status.md) and
[metrics](metrics.md).

## Strict concern

Opt into `WriteConcernOptions::strict_acknowledgements(true)` through the
builder, or use `with_strict_acknowledgements(true)` on an existing policy.
Lowering operational caps then fail before apply. A required strict constraint
sent to an older node is rejected as unsupported. Mixed versions can still
exchange existing default policies; use upgraded receivers for strict writes.

Strict count preserves the acknowledgement threshold and does not change
storage durability. Frozen eligibility, LWW supersession and timeout/unknown
outcome semantics still apply. See [write concern overrides](write-concern-overrides.md).
