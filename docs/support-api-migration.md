# Lifecycle support in 0.9.0

Upgrade all four public crate requirements to compatible `0.9.0` ranges.
This minor release includes the unpublished 0.8.1 implementation stages and
completes the six support feature contracts. Public `Hello` and advanced actor
messages gained fields/variants, sender reports gained progress, and
`DrainReport` now owns per-fragment evidence instead of implementing `Copy`.
Use builders/defaults for wire literals and update exhaustive actor matches.

Peer protocol 5, application protocol 3, and RocksDB record format 2 remain.
Generation-zero legacy framing/authentication preserves its wire meaning.
Configured process authority negotiates signed framing and cannot connect to
peers lacking that capability. Each signed physical body adds 72 bytes while
preserving the 16 MiB envelope/record limit. Peer byte minima/reserves include
the header; update explicitly selected limits using the runtime validation
error. Existing stored values are not rewritten or truncated for framing.

## Shutdown and ownership

`begin_drain()` closes application admission synchronously. Requests admitted
before the cutoff keep completion ownership through actor execution even if
their response waiter disconnects or times out. `drain_requests(DrainOptions)`
defaults to a configured local persistence boundary and leaves the node alive
with peer replication and maintenance running. Choose `DrainDurability::Applied`
for execution only or `StableCommitted` for an explicit stable backend fence.
The report distinguishes actual memory/store/stable completion per fragment.
Timeout/cancellation retains the same fence tickets and store ownership. Retry
with a new budget and the same selected durability. Draining stays one-way.

Use `finish_shutdown(DrainOptions)` or existing `shutdown()` for the completed
receive/apply and local store shutdown barrier. It now also joins decoded
replication/reconciliation work that previously could outlive its stream.
Accepted forwarded replies also retain admission through queued socket writes.
This strengthens existing completion and storage ownership promises. Handling
a shutdown error or timeout still requires retry; dropping the node cannot
report completion. A blocked backend can therefore keep shutdown pending.

An authenticated hot peer transition can remain live: configure independent
process signing seeds and public identities, then use `begin_peer_fence` /
`advance_peer_authority`. Both handshake and every subsequent frame bind the
fresh session, process identity, direction and sequence. Historical record
origins remain independent. Retired keys cannot be reintroduced within the
receiver's tracked history; retain never-reuse provisioning history externally
across restarts. See [peer authority](peer-authority.md).

Request drain is not a recovery checkpoint. Local shutdown is not remote
replication evidence. Existing `shutdown_with_replication_drain` establishes
its separate per-target configured durability contract before local stop.
See [draining](draining.md) and [replacement safety](replacement-safety.md).

## Observation

`status_snapshot()` and `metrics_snapshot()` are bounded pull observations.
Contended locks, stale reports and truncated sections remain identifiable.
`Node::begin_start` exposes construction before handoff. Established Serving
can coexist with background gain; capacity and component activity remain
explicit. `status_snapshot_detailed` / `metrics_snapshot_detailed` add bounded
actual actor/pool/worker data under one deadline. Enable timing for future
work with `enable_metrics`; register table names deliberately through
`enable_table_metrics`. Queue age is local ownership age, not remote LWW lag.
Unknown dataset percentages or unobserved history remain unknown.

Collectors run after node/report guards are released. Repeated collection
does not reset cumulative metrics; adapters must use cumulative observations
or compute deltas rather than repeatedly adding totals. The node incarnation
identifies node resets; descriptor lifetimes and `source_incarnation` identify
shared storage or replaced sender/pool/worker sources. See
[complete observations](observability.md), [status](status.md), and [metrics](metrics.md).

## Strict concern

Opt into `WriteConcernOptions::strict_acknowledgements(true)` through the
builder, or use `with_strict_acknowledgements(true)` on an existing policy.
Lowering operational caps then fail before apply. A required strict constraint
sent to an older node is rejected as unsupported. Mixed versions can still
exchange existing default policies; use upgraded receivers for strict writes.

Strict count preserves the acknowledgement threshold. Additionally choose
`WriteConcernOptions::minimum_durability` and `require_evidence` when the
receipt must certify actual local/remote fences, frozen logical replica-set
identity and the submitted operation/version floor. Store/stable requirements
imply evidence and are required wire constraints rejected by older peers.
`DurabilityFailed` and `UnknownOutcome` preserve partial/uncertain progress;
neither permits treating an applied operation as unexecuted. A receipt may
certify an equal-or-newer LWW winner rather than exact payload retention.
See [durability evidence](write-concerns.md) and
[write concern overrides](write-concern-overrides.md).

## Checkpoints and membership

Completed shutdown can export a stable bounded snapshot using
`Node::export_checkpoint`. The optional backend capability is implemented by
RocksDB; unsupported custom stores fail explicitly rather than loading an
unbounded dataset. Manifest-committed chunk hashes authenticate each chunk
before apply. Import progress and records commit atomically, and incomplete
imports block normal store access/startup until exact retry completes.

Checkpoint metadata preserves table namespaces, full LWW origins/versions,
tombstones and expiry. Replica certificates use actual retained stable target
imports; earlier replication-drain reports are separate historical evidence.
Staged membership requires qualified source floors, every final target's stable
imports and live readiness, and separate exact joint/final authenticated
authority. Native certificates verify real receiver fence/shutdown reports;
external decisions remain application assertions. No allocator, consensus or
writer lease is introduced. See [stable checkpoints](stable-checkpoints.md)
and [staged membership](staged-membership.md).

Back up stores before adopting checkpoint/import metadata. Incomplete import
markers are intentionally fail-closed; do not remove them to bypass validation.
The staged journal writes ACTMEM03 native boot/revision evidence and accepts
existing ACTMEM02 external journals; older binaries cannot interpret new
checkpoint/native journal metadata safely.
