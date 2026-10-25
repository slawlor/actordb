# Embedded support API delivery

The six support contracts are implemented for **0.9.0**, following the initial
PRs #137 and #138. Final delivery includes integration/package/CI validation
and merge of the complete change set.

| Request | Priority | Issue | Public guide |
| --- | --- | --- | --- |
| Admission, local persistence and receive/apply draining | P0 | [#130](https://github.com/slawlor/actordb/issues/130) | [Draining](draining.md) |
| Backend-neutral bounded metrics and scoped actor detail | P1 | [#131](https://github.com/slawlor/actordb/issues/131) | [Observations](observability.md) |
| Typed startup, lifecycle, placement, progress and readiness | P1 | [#132](https://github.com/slawlor/actordb/issues/132) | [Status](status.md) |
| Authenticated process/session fencing and packet integrity | P0 | [#133](https://github.com/slawlor/actordb/issues/133) | [Peer authority](peer-authority.md) |
| Stable resumable checkpoints and staged membership | P1 | [#134](https://github.com/slawlor/actordb/issues/134) | [Checkpoints](stable-checkpoints.md), [membership](staged-membership.md) |
| Strict concern, actual durability and version-floor evidence | P1 | [#135](https://github.com/slawlor/actordb/issues/135) | [Write evidence](write-concerns.md) |

P0 identifies replacement correctness prerequisites. P1 supplies observable,
usable recovery and durability controls. Implementation order reflects their
dependencies: old apply ownership must be sound before a hot fence can open a
new authority, and stable target/source evidence must precede retirement.

The new minor version is required by public protocol/actor extensions and
configured persistence becoming the request-drain default. All four published
crates and internal minimums are synchronized. Existing generation-zero framing
keeps its meaning; configured process sessions sign every frame, and required
write constraints fail closed on older implementations. Existing record values
and the RocksDB record layout remain unchanged.

The acceptance suite covers persistence blocking/failure/retry, real SDK
admission races, queued/cancelled reads and writes, forwarded/client responses,
decoded replication/reconciliation, signed tamper/replay/reconnect, bounded
concurrent collectors and lifecycle readiness, partial/restarted checkpoint
transfer/import, staged replacement/shrink and actual durability certificates.
See [completion gates](support-completion-plan.md) and [migration](support-api-migration.md).

These are library support contracts. They do not allocate hosts, elect writers,
prove host death or implement consensus. Qualified sources and a live Serving
snapshot are independent of application writer authority. Unmeasured progress
and unavailable evidence stay unknown rather than being reported as zero.
