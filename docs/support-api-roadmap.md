# Embedded lifecycle support roadmap

This backlog prioritizes safe retirement and replacement of embedded nodes.
Public APIs remain independent of hosting, discovery and monitoring systems.

| Request | Priority | Issue | Delivery order |
| --- | --- | --- | --- |
| Application admission/drain and receive/apply shutdown barrier | P0 | [#130](https://github.com/slawlor/actordb/issues/130) | First implementation wave |
| Backend-neutral metrics snapshots/collectors | P1 | [#131](https://github.com/slawlor/actordb/issues/131) | First wave, uses drain accounting |
| Typed lifecycle, placement and replication status | P1 | [#132](https://github.com/slawlor/actordb/issues/132) | First wave, shares snapshots |
| Authenticated peer/process and epoch fencing | P0 | [#133](https://github.com/slawlor/actordb/issues/133) | After receive/apply barrier |
| Stable checkpoints and staged membership reconfiguration | P1 | [#134](https://github.com/slawlor/actordb/issues/134) | After retirement and source fencing |
| Strict write concern and durability evidence | P1 | [#135](https://github.com/slawlor/actordb/issues/135) | Strict cap policy first; receipt extensions follow |

P0 identifies correctness requirements for a replacement controller. P1
identifies observability and durability controls that make those requirements
usable. Delivery order also reflects dependencies: hot epoch changes cannot
promise a barrier until ownership of already dispatched apply work is sound.

Each issue retains its full acceptance criteria. The first implementation
wave does not imply completed epoch fencing, checkpoint support, consensus,
writer leases, host-death proof or automatic membership allocation. Remaining
coverage and staged receipt/metrics/status extensions stay in their issues.

The release line is **0.8.1**. New APIs are additive, the generated
write-concern policy type was already non-exhaustive, and strict policy is
opt-in. Existing default wire encodings and peer/application protocol
versions remain compatible. Shutdown fixes preserve its existing admission
and local persistence promises. All four published crates and internal
minimums advance together. See the release notes and individual API guides
for migration.

See [replacement safety](replacement-safety.md) for the current supported
operational boundaries, and [full-outage recovery](dynamic-recovery.md) for
existing complete-source recovery qualification.
