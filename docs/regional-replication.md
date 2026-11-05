# Regional replication concurrency

Enable `NodeRuntimeOptions::regional_replication` to use distinct sender profiles
and shared outgoing WAN limits. It uses the same node-to-region map as reads and
regional write concerns. Matching explicit labels select the in-region profile;
different or missing labels select the cross-region profile. Missing the local
label classifies every peer as cross-region.

## Defaults and configuration

| Setting | In-region | Cross-region |
|---|---:|---:|
| Operations per batch | 100 | 256 |
| Batch interval | 10 ms | 100 ms |
| Outstanding batches per fragment/peer | 16 | 2 |

Each Node also shares **16 ordinary batches** and **2 active reconciliation
rounds** across all outgoing WAN fragments and peers. Separate round capacity
keeps bootstrap, reset recovery, anti-entropy and retirement from consuming
ordinary batch capacity. ACKs and session/control messages retain their existing
transport/control lanes.

```rust
use actordb::node::NodeRuntimeOptions;
use actordb::replication::{RegionalReplicationOptions, SenderConfig};
use std::collections::HashMap;
use std::time::Duration;

let regional = RegionalReplicationOptions::builder()
    .in_region(SenderConfig::builder()
        .batch_interval(Duration::from_millis(10)).max_in_flight(16).build())
    .cross_region(SenderConfig::builder()
        .batch_size(256).max_in_flight(2)
        .batch_interval(Duration::from_millis(100))
        .reconcile_timeout(Duration::from_secs(60)).build())
    .cross_region_max_in_flight(8)
    .cross_region_max_reconcile_rounds(2)
    .build();
let runtime = NodeRuntimeOptions::builder()
    .regions(HashMap::from([
        ("west-a".into(), "west".into()), ("west-b".into(), "west".into()),
        ("east-a".into(), "east".into()), ("east-b".into(), "east".into()),
    ]))
    .regional_replication(regional).build();
// Pass runtime to Node::start_with_options with your placement and members.
assert_eq!(runtime.regional_replication().unwrap().cross_region_max_in_flight, 8);
```

`with_regional_replication` sets the same option on an existing runtime-options
value. Profiles replace ordinary `NodeConfig.replication` tuning when enabled;
the Node's configured ACK durability remains a minimum. Both profiles and nonzero
shared limits are validated before storage attachment or transport startup.

Labels and profiles are fixed for the running Node. Static startup,
connection-discovered full replication, dynamic gains, reconnection, sender
replacement and replication-drain preparation share the same selection path.
Each Node constructs its own budgets even when startup options are cloned.

## Ownership and fairness

Eligible senders take FIFO tickets and try acquisition without blocking their
ACK/control state machine. An ordinary permit remains with its in-flight batch.
Weak, unknown, duplicate or stale ACKs cannot retire a stronger batch. Valid ACKs,
NACK/requeue, timeout, session replacement, failed send and sender shutdown
release the applicable credit. Retained operations preserve their replay/reset
ownership under the existing replication contract.

Round permits cover summaries, fetches, repairs and tracked durability barriers.
The existing per-connection round gate is acquired before the shared WAN slot;
its pending permit is retained while waiting, preventing repeated acquire/drop
notifications and busy loops. Disconnect/replacement drops queued/pending
admission with the old session. Failed/completed rounds release their permits.

Limits count current sender-side ACK/round ownership. Timeout or rotation does
not cancel already-accepted remote actor/storage work. Throughput depends on
RTT, batching, payloads and receiver capacity; these are concurrency controls,
not a bytes-per-second rate. Caps are per outgoing Node rather than global
cluster/destination-region limits. Incoming peer requests retain existing
responder/transport bounds; every assigned replica still receives writes.

## Inspect actual usage

`Node::regional_replication_usage()` returns `None` when disabled. When enabled:

- `in_flight_batches`, `max_in_flight_batches`, `waiting_batch_senders`.
- `active_reconcile_rounds`, `max_reconcile_rounds`, `waiting_reconcile_senders`.

Batch/round observations are independently synchronized and reflect actual
permits and queued tickets. A low cap can increase backlog and write concern
latency; size it using workload estimates and observed RTT/ACK behavior.

Tests stall receiver stable fences across four fragments and two WAN peers,
verify shared batch/round caps of one, and show in-region reconciliation and
data replication continuing. Unit tests cover FIFO cancellation, conservative
classification, ACK floors, weak ACK ownership, retry release, separate round
capacity and failed-send cleanup.
