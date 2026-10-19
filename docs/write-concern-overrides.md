# Operational write concern overrides

A receiving node can explicitly cap the acknowledgement count for new writes.
This is an operational control for accepting weaker replication assurance
while some replicas are unavailable. The node never lowers thresholds
based on observed connectivity.

By default there is no cap. A configured cap must be positive. After the
requested write concern is validated against the captured replica set,
the effective threshold is the smaller of the requested count and the cap.
For example, a request for three acknowledgements with a cap of two can
succeed after the receiving node and one eligible replica acknowledge it.
A request for one acknowledgement remains one even if the cap is two.

Configure the initial cap with `NodeRuntimeOptions`:

```rust
use actordb::node::NodeRuntimeOptions;

let options = NodeRuntimeOptions::builder()
    .write_concern_acknowledgement_cap(2)
    .build();
```

A running node exposes `set_write_concern_acknowledgement_cap`,
`clear_write_concern_acknowledgement_cap`, and
`write_concern_acknowledgement_cap`. Setting zero fails and preserves the
current cap. Clearing the cap restores the requested acknowledgement
threshold for subsequent writes.

## Scope and reporting

- The cap belongs to the receiving node that applies the write. A forwarding
  caller's configuration does not override the receiving node's policy.
- Each write captures its cap when its acknowledgement plan is prepared.
  Changing or clearing the cap does not change an existing waiter's plan.
- A successful receipt reports the requested threshold, effective threshold,
  and whether the threshold was lowered. A configured cap that leaves the
  requested threshold unchanged is not reported as an override.
- Additional write requirements, including another-region acknowledgement
  when that feature is configured, remain independent of the numeric cap.
  Lowering the count does not remove them.

The cap changes when success can be returned. It does not change replication
fanout, conflict reconciliation, or the durability of acknowledgement
processing. A timeout does not undo an applied write; replication can continue
after the caller receives the timeout. The captured replica set and requested
threshold remain authoritative inputs to each plan even when an override is
active.

## Version requirements

This additive API is available in `0.5.0`. All four public crates use compatible
`0.5.0` dependency minimums so an upgrade resolves the matching database,
protocol, query, and SDK revisions.
