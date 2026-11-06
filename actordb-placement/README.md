# actordb-placement

Portable workload sizing, regional placement recommendations and fragmap
evaluation for ActorDB. The library has no database, Tokio, network or storage
dependency. External controllers can call it with expected workload or observed
per-fragment load. The optional CLI reads JSON and prints a report, JSON, or a
static NodeConfig TOML fragment map.

```sh
cargo run -p actordb-placement --features cli --bin actordb-plan -- example > workload.json
cargo run -p actordb-placement --features cli --bin actordb-plan -- recommend workload.json
cargo run -p actordb-placement --features cli --bin actordb-plan -- recommend workload.json --json > plan.json
cargo run -p actordb-placement --features cli --bin actordb-plan -- recommend workload.json --toml > placement.toml
cargo run -p actordb-placement --features cli --bin actordb-plan -- evaluate workload.json plan.json
```

Evaluation requires the host inventory in the workload input; recommendations
without inventory size synthetic hosts. Copy `hosts` from a sizing report into
the input to evaluate or replace them with real host IDs, regions, zones and
resource profiles before deploying.

```rust
use actordb_placement::{recommend, PlannerInput};

let mut input = PlannerInput::default();
input.workload.record_count = 1_000_000;
input.workload.average_value_bytes = 1024;
input.workload.point_read_qps = 10_000.0;
input.policy.regions = vec!["east".into(), "west".into()];
let plan = recommend(&input)?;
assert_eq!(plan.replica_count, 4); // two distinct hosts per region
# Ok::<(), actordb_placement::PlannerError>(())
```

## Model

- The standard preset requests two replicas per preferred region. Set
  `replicas_per_region = 1` for the economical preset, or use `replica_count` to
  cap total copies. Regional coverage is soft; gaps are explicit in the report.
- Placement favors regional coverage, different known zones within a region,
  and lower projected dominant-resource load across RAM, CPU, disk capacity,
  network and disk-write throughput. Regional traffic weights charge reads and
  queries to the holders that serve that region. Host IDs break ties consistently.
- Growth defaults to 1.5, unknown data/traffic skew to 1.2 and resource headroom
  to 30%. RAM includes all resident data even with durable storage, metadata,
  indexes, retained tombstones, worker/runtime overhead and replication backlog.
- Write CPU and optional disk writes apply at every replica. Read and query
  work is distributed among regional holders under the stated ingress assumption.
  Set `durable_storage = true` for a durable backend even if callers wait only
  for memory application; StoreCommitted/StableCommitted also account for disk.
- SQL and scan CPU depends on examined records, with touched-fragment
  coordination costs. Aggregate network estimates include partial results from
  fragments while application responses contain only the final rows. Supply
  `records_examined`, `rows_returned`, `result_row_bytes` and `fragment_fraction`
  for selective queries. Exact-key SQL can be modeled as a point read.
- Traffic separates application, query/coordinator, direct replication fanout,
  cross-region transfers and recovery bandwidth. The recovery allowance reserves
  symmetric full-copy bandwidth over `recovery_window_seconds`. It is a planning
  allowance, not a simulated failure or a replication-concurrency setting.
- Fragments per host are limited by profile policy and the smallest resource
  ceiling at the configured headroom. The report uses average assigned-fragment
  costs; hot fragments and keys require additional attention.
- With no inventory, the planner adds synthetic hosts equally across preferred
  regions until modeled resources fit, within `max_hosts`. Real inventory may
  overcommit; that returns explicit diagnostics rather than omitting fragments.

The default host profile is **uncalibrated**: 8 CPU cores, 16 GiB RAM, 100 GiB
disk, 125 MB/s network, 100 MB/s disk writes, 20 microseconds point-read CPU,
50 microseconds write CPU, 2 microseconds per examined record, 100 microseconds
fragment-query overhead, and a 128-fragment policy ceiling. These are editable
planning assumptions, not measured ActorDB throughput or supported maximums.
The report retains the complete profile and workload used for its estimates.

Average key/value geometry that cannot fit the 16 MiB replication envelope is
rejected before sizing. Smaller averages still require per-record wire checks;
large keys/table names and size-distribution tails consume additional overhead.

## Live controllers

Set `fragment_count` or provide `existing_placement` to keep the deployed hash
partitioning fixed. `fragment_loads` can override per-fragment data, read QPS,
write QPS and examined records/second. Keep failed hosts in inventory with
`available = false`; their copies do not count toward reliability. Unknown
replica IDs are errors.

Existing assignments are preserved unless coverage/failure or copy-count goals
need repair, or modeled utilization improves. Overloaded hosts and host fragment
limits can be repaired with a smaller improvement; normal balancing requires
`rebalance_improvement` (10% by default). At most `max_moved_fragments` (32 by
default) replica sets change per recommendation. Remaining excess copies are
reported during bounded changes. If diagnostics remain, reevaluate after the
current moves finish. Unavailable hosts contribute no active resource usage.

A target recommendation must be sequenced through `PlacementTransition`:

1. Publish its initial `placement()`, which adds an overlap without removing old
   replicas and bounds concurrent fragment transitions.
2. Collect fresh Serving reports from **every target node** for affected fragments.
3. Call `advance(&serving)` and publish the returned map. Missing readiness retains
   old replicas; completed transitions free slots for the next overlaps.

Persist controller state and serialize placement publishers. Local readiness
callbacks alone do not establish cluster-wide readiness. Writer authority and
the safety of control-plane observations remain the controller's responsibility.
Temporary overlap copies require additional capacity. Provisioned copies do not
establish acknowledged-write durability, version equality or consensus.

See the repository's [placement planning guide](../docs/placement-planning.md)
for Node integration and host calibration.
