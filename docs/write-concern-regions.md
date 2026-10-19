# Region acknowledgement requirements

Write concern can additionally require an acknowledgement from a replica in a
different configured region from the node processing the mutation. Region labels
are supplied by the operator; ActorDB does not measure physical location or
change replica placement based on these labels.

Configure the node-to-region map on each receiving node:

```rust
use std::collections::HashMap;
use actordb::node::NodeRuntimeOptions;

let runtime = NodeRuntimeOptions::builder()
    .regions(HashMap::from([
        ("node-a".to_string(), "west".to_string()),
        ("node-b".to_string(), "west".to_string()),
        ("node-c".to_string(), "east".to_string()),
    ]))
    .build();
```

`NodeRuntimeOptions::with_regions` provides the same configuration on an existing
options value, and `regions()` returns the configured map. Node IDs and region
labels must contain 1 to 255 UTF-8 bytes. Labels compare exactly, including case.
Invalid entries fail startup before binding storage or transport. Empty and
partial maps are accepted for writes without a region requirement.
Region-constrained writes require explicit fragment replica placement. Labels
for nodes outside the responsible replica set cannot satisfy the condition;
the legacy configuration without a replica map does not establish eligible
outside-region peers.

Request the additional condition on a write's policy:

```rust
use actordb::write_concern::{WriteConcern, WriteConcernOptions};

let policy = WriteConcernOptions::builder()
    .concern(WriteConcern::Majority)
    .require_other_region(true)
    .build();
```

The SDK exports the same `WriteConcernOptions` type. Existing writes keep their
local acknowledgement default and do not require region configuration.

## Completion semantics

A region requirement is additional to the numeric acknowledgement threshold.
Even a local or count-one policy must wait for a processed acknowledgement from
at least one other eligible replica in a different region. Only distinct nodes
in the receiving node's resolved replica set count; duplicate acknowledgements
and stale replication acknowledgements do not satisfy the requirement.
Receipts report `receiving_region()` and
`outside_region_acknowledged_nodes()`. `other_region_satisfied()` indicates that
the receiving node observed the additional condition. Timeout receipts retain
the region metadata and any progress already made.

Before applying a region-constrained mutation, the receiving node requires
labels for itself and every eligible replica and checks that the replica set
includes another region. Missing labels and impossible region requirements
return an error before apply. Routing uses the serving node's region, which can
differ from the node that originally received the caller's request. Maintain
consistent maps on all nodes that can process mutations, including during
placement changes.

The timeout covers the additional region requirement. A timeout does not roll
back the mutation: local application and background replication can already have
occurred. Success reports processing acknowledgements, without establishing a
global order for concurrent updates or a stable-storage guarantee. Existing
last-write-wins reconciliation and eventual replication continue to apply.

## Compatibility

Region requirements use an additional policy field. Older implementations
reject unknown policy fields before application; they cannot silently ignore a
required region condition. Upgrade nodes that process region-constrained writes
before enabling the option. New callers reject missing or malformed required
region progress in a success receipt as an unknown outcome. Region metadata is
omitted when the caller has not requested the condition.
