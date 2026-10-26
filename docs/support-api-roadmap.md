# Embedded support API delivery

ActorDB `0.9.0` provides six embedded support contracts on a fresh-deployment
Rust/wire/storage boundary.

| Contract | Priority | Tracking issue | Guide |
| --- | --- | --- | --- |
| Admission, local persistence and receive/apply draining | P0 | [#130](https://github.com/slawlor/actordb/issues/130) | [Draining](draining.md) |
| Bounded backend-neutral metrics and scoped actor detail | P1 | [#131](https://github.com/slawlor/actordb/issues/131) | [Observations](observability.md) |
| Startup, lifecycle, readiness, placement and progress | P1 | [#132](https://github.com/slawlor/actordb/issues/132) | [Status](status.md) |
| Transport authentication and operational generation barriers | P0 | [#133](https://github.com/slawlor/actordb/issues/133) | [Peer authority](peer-authority.md) |
| Stable resumable checkpoints and staged membership | P1 | [#134](https://github.com/slawlor/actordb/issues/134) | [Checkpoints](stable-checkpoints.md), [membership](staged-membership.md) |
| Strict concern, actual durability and version-floor evidence | P1 | [#135](https://github.com/slawlor/actordb/issues/135) | [Write evidence](write-concerns.md) |

P0 contracts are local correctness prerequisites for retirement/replacement.
P1 provides observable recovery and durability controls. Accepted apply/store
ownership must drain before a policy transition; stable target/source evidence
must precede retirement.

All four public crates use `0.9.0`. Peers require exact protocol 6, applications
protocol 4, RocksDB record/manifest format 3, checkpoint ACTCP003 and membership
ACTMEM04. There is no compatibility wire mode, adoption/conversion path, public
wire/supervision alias or unbounded sender entry point.

TLS and optional shared-secret authentication belong to the deployment's trust
model. Receiver generation checks and fences prove actual admitted work drained,
not that an old process lost its credentials. External revocation is required
to exclude a credential holder. No process signatures, allocator, consensus,
host-death proof or writer lease are supplied.

See [validation gates](support-completion-plan.md) and
[fresh deployment](support-api-migration.md). Unavailable observations remain
explicit; unimplemented percentage/lag fields are not exposed.
