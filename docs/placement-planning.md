# Regional routing and placement planning

## Static fragmap format

`NodeConfig.fragment_map` is a Rust `HashMap<FragmentId, Vec<String>>`. Each
fragment lists the node IDs that hold a full copy of that fragment. The optional
`actordb/toml` adapter accepts the same assignments as a TOML table:

```toml
fragment_count = 2

[fragment_map]
0 = ["east-a", "east-b", "west-a", "west-b"]
1 = ["east-b", "east-c", "west-b", "west-c"]
```

Keys must be canonical decimal u32 IDs. Nonempty placement covers every fragment
exactly; lists are nonempty and duplicate-free. Static maps and fragment counts
must agree on every node. Add member addresses separately; generated planner
host IDs are placeholders until you provide actual inventory.

## Read preference

Configure `NodeRuntimeOptions::regions` with a consistent node-to-region map,
including ingress nodes. Point and batch reads, scans and SQL choose:

1. This node when it currently serves the fragment.
2. Other replicas in this node's configured region, in map order.
3. Remaining replicas, including unlabeled replicas, in map order.

Missing the current node's region retains ordinary placement ordering. Failure
of a preferred holder allows remote fallback. Valid not-found replies remain
terminal point-read results. SQL/scan fragment pages remain pinned to their
selected holder; losing it mid-query fails that query rather than mixing views.
Reads keep existing eventual-consistency semantics. Write routing and replication
configuration retain their existing behavior.

```rust,no_run
use actordb::configuration::{ConfigurationProvider, TomlConfigProvider};
use actordb::node::{Node, NodeRuntimeOptions};
use std::collections::HashMap;

# async fn start() -> Result<(), Box<dyn std::error::Error>> {
let config = TomlConfigProvider::from_path("placement.toml").resolve().await?;
let runtime = NodeRuntimeOptions::default().with_regions(HashMap::from([
    ("east-a".into(), "east".into()),
    ("east-b".into(), "east".into()),
    ("west-a".into(), "west".into()),
    ("west-b".into(), "west".into()),
]));
let node = Node::start_with_options(
    "east-a".into(), "0.0.0.0:7447".parse()?, config, runtime,
).await?;
# node.shutdown().await?;
# Ok(())
# }
```

## Workload sizing and evaluation

The new `actordb-placement` library is also available through
`actordb::placement::planner`. The CLI is enabled with the standalone crate's
`cli` feature; it reads files and never connects to a cluster.

An initial workload can be as small as:

```json
{
  "workload": {
    "record_count": 1000000,
    "average_key_bytes": 32,
    "average_value_bytes": 1024,
    "point_read_qps": 10000,
    "write_qps": 1000,
    "queries": [
      {
        "kind": "aggregate",
        "qps": 2,
        "records_examined": 1000000,
        "rows_returned": 1,
        "result_row_bytes": 32
      }
    ]
  },
  "policy": {
    "regions": ["east", "west"],
    "replicas_per_region": 2
  }
}
```

Use `actordb-plan example` for all defaults, or supply actual hosts:

```json
{
  "id": "east-a",
  "region": "east",
  "zone": "east-zone-1",
  "available": true
}
```

Each host can override `profile`; otherwise it inherits `host_profile`. Add
these objects to `hosts` in the workload file. `regional_traffic_weights` assigns
the fraction of application traffic from each region. Query estimates distinguish
examined records from returned rows. A full scan that returns ten rows can still
do substantial work; use actual examined-record estimates when available.
Set `durable_storage = true` to account for disk when callers wait only for
memory application. Store/stable write durability also enables disk accounting.

The report explains fragments, copies, host count, each host's fragment ceiling,
actors/senders, memory, CPU, disk and network use, regional coverage and reliability
gaps. JSON retains the input profile/policy/workload for review. TOML output contains
only `fragment_count` and `fragment_map`; diagnostics go to stderr.

```sh
cargo run -p actordb-placement --features cli --bin actordb-plan -- recommend workload.json
cargo run -p actordb-placement --features cli --bin actordb-plan -- recommend workload.json --json > plan.json
cargo run -p actordb-placement --features cli --bin actordb-plan -- recommend workload.json --toml > placement.toml
```

`evaluate workload.json plan.json` also accepts a plain JSON fragmap. Evaluation
requires inventory; when initial sizing generated hosts, copy the report's `hosts`
into your workload input first. The JSON report is not a Node configuration file.

## Fragments per host and calibration

With current Node defaults, one fragment starts 16 worker actors (base and KV
pool sets, each with 2 high + 4 normal + 2 background workers) and four actors for
supervision, dispatch and maintenance. Each additional resident named table adds
eight workers. Each other replica adds a sender task. All these share the runtime;
they are not dedicated OS threads. Pools and table residency are configurable.

There is no established universal safe fragment maximum. The default 128-per-host
planner ceiling is an editable assumption. Use measured profiles and headroom.
More fragments improve assignment granularity, while increasing maintenance,
sender and distributed-query coordination costs. Increasing fragment_count on a
live database changes hashing and needs separate repartitioning work.

Run the process-isolated calibration example on representative hardware:

```sh
cargo run --release -p actordb --example fragment_capacity -- \
  --fragments 1,8,32,128 --records 10000 --value-bytes 1024 \
  --replicas 2 --samples 100 > calibration.json
```

Each fragment-count case runs in a fresh process with real Node defaults and TCP
replication on loopback. The report measures baseline/idle/loaded RSS, idle CPU,
point-operation CPU, p50/p99 latency, aggregate CPU/latency, initial replica catchup,
and concurrent read/write/aggregate latency with query failures counted. Writes
in the measured phase await every replica's processing ACK. RSS covers all
in-process replicas and is divided for partial profile calibration.

The generated `host_profile` supplies conservative upper envelopes for measured
CPU and overhead fields. CPU cores, RAM, disk and bandwidth still need the actual
host's resource budgets; storage/stable writes remain assumptions because the
example is storeless. Low sample counts are smoke measurements, not deployment
ceilings. Validate larger datasets, target sustained/burst QPS, regional latency,
failure recovery and your chosen persistence/pool configuration on real hosts.

The initial [local smoke measurements](benchmarks/fragment-capacity-smoke.md)
compare 1/8/32/128 fragments with the raw report available alongside the notes.

Copy the report's `host_profile` into the planner input. Use observed per-fragment
load overrides for live controllers. The planner reserves headroom and a recovery
bandwidth allowance; it does not simulate every failure or adjust WAN concurrency.

## Controller integration

```rust,no_run
use actordb::placement::{from_planner_map, ManualPlacement, planner};
use std::collections::{BTreeMap, BTreeSet};

# fn example(mut input: planner::PlannerInput) -> Result<(), planner::PlannerError> {
let current = input.existing_placement.clone().expect("live placement supplied");
let recommendation = planner::recommend(&input)?;
let mut transition = planner::PlacementTransition::new(
    current, recommendation.fragment_map, 4,
)?;
let controller = ManualPlacement::new(from_planner_map(transition.placement()));

// After publishing overlap and collecting fresh Serving reports from every
// affected target node, advance and publish the next map. Persist this state
// in the external controller; one local callback is insufficient evidence.
let serving: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
controller.set_placement(from_planner_map(transition.advance(&serving)));
# Ok(())
# }
```

Controllers own readiness evidence, consistent publication and application writer
authority. Temporary overlap copies need spare capacity. Existing Node migration
and replication retirement machinery performs the actual handoff.
