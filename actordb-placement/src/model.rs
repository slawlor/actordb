use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Portable fragment IDs are the same u32 IDs used by ActorDB.
pub type FragmentMap = BTreeMap<u32, Vec<String>>;

/// Both initial sizing and live rebalancing use this input. Omitted fields
/// have documented defaults; unknown JSON fields are rejected.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlannerInput {
    pub workload: Workload,
    pub policy: PlacementPolicy,
    /// Empty inventory requests synthetic host sizing in the preferred regions.
    pub hosts: Vec<Host>,
    /// Profile for synthetic hosts and inventory hosts without an override.
    pub host_profile: HostProfile,
    /// Set this for an existing deployment; it must match existing_placement.
    pub fragment_count: Option<u32>,
    pub existing_placement: Option<FragmentMap>,
    /// Optional actual per-fragment measurements replace uniform estimates.
    pub fragment_loads: BTreeMap<u32, FragmentLoad>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Workload {
    /// Live records across all logical tables, not per table or per replica.
    pub record_count: u64,
    pub average_key_bytes: u64,
    pub average_value_bytes: u64,
    pub growth_factor: f64,
    /// Additional retained deletion/expiry markers relative to live records.
    pub tombstone_ratio: f64,
    /// Resident table worker sets per fragment, including the default KV table.
    pub resident_tables: u32,
    pub point_read_qps: f64,
    /// Includes puts, deletes and expiry mutations; applies at every replica.
    pub write_qps: f64,
    pub queries: Vec<QueryWorkload>,
    /// Fractions need not sum to one; weights are normalized. Empty = equal
    /// traffic from each preferred region.
    pub regional_traffic_weights: BTreeMap<String, f64>,
    /// Conservative multiplier for unknown skew; use fragment_loads when known.
    pub skew_factor: f64,
    pub write_durability: WriteDurability,
    /// Account for disk storage even when callers only wait for MemoryApplied.
    pub durable_storage: bool,
    /// Observed mean operations per replicated durability batch. Default one
    /// conservatively treats each peer update as its own fence; configured
    /// maximum batch sizes do not imply that actual batches are full.
    pub replication_batch_operations: f64,
}

impl Default for Workload {
    fn default() -> Self {
        Self {
            record_count: 0,
            average_key_bytes: 32,
            average_value_bytes: 1024,
            growth_factor: 1.5,
            tombstone_ratio: 0.1,
            resident_tables: 1,
            point_read_qps: 0.0,
            write_qps: 0.0,
            queries: Vec::new(),
            regional_traffic_weights: BTreeMap::new(),
            skew_factor: 1.2,
            write_durability: WriteDurability::MemoryApplied,
            durable_storage: false,
            replication_batch_operations: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteDurability {
    #[default]
    MemoryApplied,
    StoreCommitted,
    StableCommitted,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryKind {
    Scan,
    #[default]
    Sql,
    /// Full aggregate work with a compact final response.
    Aggregate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryWorkload {
    pub kind: QueryKind,
    pub qps: f64,
    /// Expected examined records per request across the selected fragments.
    /// None conservatively assumes the entire grown live dataset is examined.
    pub records_examined: Option<u64>,
    /// Expected rows in the application response; None means all examined
    /// records for Scan/Sql and one row for Aggregate.
    pub rows_returned: Option<u64>,
    /// Projected bytes per row, excluding protocol framing.
    pub result_row_bytes: Option<u64>,
    /// Fraction of fragments touched. Key/prefix ranges generally still touch
    /// all hash fragments; exact-key SQL is better modeled as a point read.
    pub fragment_fraction: f64,
    /// Optional measured CPU cost for this workload on the selected hardware.
    /// Useful for count-only aggregates, which are cheaper than general scans.
    /// None uses the host profile's conservative common record cost.
    pub examined_record_cpu_us: Option<f64>,
}

impl Default for QueryWorkload {
    fn default() -> Self {
        Self {
            kind: QueryKind::Sql,
            qps: 0.0,
            records_examined: None,
            rows_returned: None,
            result_row_bytes: None,
            fragment_fraction: 1.0,
            examined_record_cpu_us: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlacementPolicy {
    pub regions: Vec<String>,
    /// Standard reliability preset: two distinct hosts per preferred region.
    /// One is the economical preset. Coverage remains a soft preference.
    pub replicas_per_region: usize,
    /// Optional total copy limit; defaults to regions * replicas_per_region.
    pub replica_count: Option<usize>,
    /// Leave this fraction of each resource unused for growth and recovery.
    pub spare_capacity_fraction: f64,
    /// Grown resident data target per fragment, before fixed overhead.
    pub target_fragment_bytes: u64,
    pub min_fragments_per_host: u32,
    pub max_fragment_count: u32,
    pub max_hosts: usize,
    /// Rebalancing changes at most this many fragment replica sets per call.
    pub max_moved_fragments: usize,
    /// Preserve an existing placement unless its peak utilization exceeds the
    /// headroom target or improvement exceeds this fraction.
    pub rebalance_improvement: f64,
    /// Recovery transfer window used to reserve modeled network bandwidth.
    pub recovery_window_seconds: f64,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            regions: vec!["local".into()],
            replicas_per_region: 2,
            replica_count: None,
            spare_capacity_fraction: 0.3,
            target_fragment_bytes: 64 * 1024 * 1024,
            min_fragments_per_host: 4,
            max_fragment_count: 4096,
            max_hosts: 256,
            max_moved_fragments: 32,
            rebalance_improvement: 0.1,
            recovery_window_seconds: 3600.0,
        }
    }
}

/// Resource/service-time profile. Defaults are explicitly uncalibrated.
/// Memory includes the resident cache even when durable storage is attached.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HostProfile {
    /// Include hardware, runtime/pool config and benchmark date for calibration.
    pub calibration: String,
    pub cpu_cores: f64,
    pub ram_bytes: u64,
    pub disk_bytes: u64,
    pub network_bytes_per_second: f64,
    pub disk_write_bytes_per_second: f64,
    /// Optional measured local committed-write throughput; excludes MemoryApplied.
    pub store_write_ops_per_second: Option<f64>,
    /// Optional measured stable-fence throughput. Stable writes also obey any
    /// store-completion limit. None leaves this additional constraint unmeasured.
    pub stable_write_ops_per_second: Option<f64>,
    pub point_read_cpu_us: f64,
    pub write_cpu_us: f64,
    pub examined_record_cpu_us: f64,
    pub query_fragment_cpu_us: f64,
    pub stable_write_cpu_us: f64,
    pub record_metadata_bytes: u64,
    /// Resident encoded-key/index copies relative to logical user-key bytes.
    pub key_memory_multiplier: f64,
    /// Physical storage key encoding, separate from resident index copies.
    pub storage_key_multiplier: f64,
    /// None retains the historical metadata assumption for storage sizing.
    pub storage_record_metadata_bytes: Option<u64>,
    pub resident_data_multiplier: f64,
    pub disk_data_multiplier: f64,
    pub wire_overhead_bytes: u64,
    /// Fixed runtime cost, before data and traffic-retained bytes.
    pub node_overhead_bytes: u64,
    /// Fixed durable-backend/WAL disk allowance, charged once per available Node.
    pub disk_overhead_bytes: u64,
    pub fragment_overhead_bytes: u64,
    pub table_pool_overhead_bytes: u64,
    pub sender_overhead_bytes: u64,
    pub fragment_idle_cpu_cores: f64,
    /// Retain enough room for this many seconds of replication backlog.
    pub replication_backlog_seconds: f64,
    pub max_fragments_per_host: u32,
    /// Actual Node defaults sum to 2 high + 4 normal + 2 background workers.
    pub workers_per_pool_set: u32,
}

impl Default for HostProfile {
    fn default() -> Self {
        Self {
            calibration: "uncalibrated planning assumptions".into(),
            cpu_cores: 8.0,
            ram_bytes: 16 * 1024 * 1024 * 1024,
            disk_bytes: 100 * 1024 * 1024 * 1024,
            network_bytes_per_second: 125_000_000.0,
            disk_write_bytes_per_second: 100_000_000.0,
            store_write_ops_per_second: None,
            stable_write_ops_per_second: None,
            point_read_cpu_us: 20.0,
            write_cpu_us: 50.0,
            examined_record_cpu_us: 2.0,
            query_fragment_cpu_us: 100.0,
            stable_write_cpu_us: 100.0,
            record_metadata_bytes: 384,
            key_memory_multiplier: 1.0,
            storage_key_multiplier: 1.0,
            storage_record_metadata_bytes: None,
            resident_data_multiplier: 1.3,
            disk_data_multiplier: 2.0,
            wire_overhead_bytes: 128,
            node_overhead_bytes: 64 * 1024 * 1024,
            disk_overhead_bytes: 0,
            fragment_overhead_bytes: 128 * 1024,
            table_pool_overhead_bytes: 64 * 1024,
            sender_overhead_bytes: 32 * 1024,
            fragment_idle_cpu_cores: 0.001,
            replication_backlog_seconds: 5.0,
            max_fragments_per_host: 128,
            workers_per_pool_set: 8,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Host {
    pub id: String,
    pub region: String,
    #[serde(default)]
    pub zone: Option<String>,
    #[serde(default)]
    pub profile: Option<HostProfile>,
    #[serde(default = "available")]
    pub available: bool,
}
fn available() -> bool {
    true
}

/// Measured load replaces the corresponding estimate. Include normal traffic,
/// not desired spare capacity; the policy adds headroom separately.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FragmentLoad {
    pub resident_data_bytes: Option<u64>,
    pub point_read_qps: Option<f64>,
    pub write_qps: Option<f64>,
    pub records_examined_per_second: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrafficEstimate {
    pub application_bytes_per_second: f64,
    pub replication_bytes_per_second: f64,
    pub recovery_bytes_per_second: f64,
    /// Database-internal query response/coordinator traffic; application
    /// responses are reported separately, including same-host coordinators.
    pub query_bytes_per_second: f64,
    pub cross_region_application_bytes_per_second: f64,
    pub cross_region_replication_bytes_per_second: f64,
    pub cross_region_query_bytes_per_second: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostEstimate {
    pub id: String,
    pub region: String,
    pub fragments: u32,
    pub actors: u64,
    pub sender_tasks: u64,
    pub resident_bytes: f64,
    pub disk_bytes: f64,
    pub cpu_cores: f64,
    pub disk_write_bytes_per_second: f64,
    /// Equivalent immediate fence demand after local/replicated batch shares.
    #[serde(default)]
    pub committed_write_ops_per_second: f64,
    #[serde(default)]
    pub committed_write_utilization: f64,
    /// Ingress + egress, conservatively compared to one interface budget.
    pub network_bytes_per_second: f64,
    pub recommended_fragment_limit: u32,
    /// Resource ratios before headroom; 0.7 is the default operating target.
    pub memory_utilization: f64,
    pub cpu_utilization: f64,
    pub disk_utilization: f64,
    pub network_utilization: f64,
    pub disk_write_utilization: f64,
    pub traffic: TrafficEstimate,
}

impl HostEstimate {
    pub fn peak_utilization(&self) -> f64 {
        self.memory_utilization
            .max(self.cpu_utilization)
            .max(self.disk_utilization)
            .max(self.network_utilization)
            .max(self.disk_write_utilization)
            .max(self.committed_write_utilization)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoverageGap {
    pub fragment: u32,
    pub region: String,
    pub desired_replicas: usize,
    pub available_replicas: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReliabilityReport {
    /// Provisioned live copies only; does not assert version/durability equality.
    pub survives_one_host_loss: bool,
    pub survives_one_region_loss: bool,
    pub regional_coverage_survives_one_host_loss: bool,
    pub zone_diverse_fragments: u32,
    pub unknown_zone_fragments: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// Exact workload, policy and default profile used for this report.
    pub workload: Workload,
    pub policy: PlacementPolicy,
    pub host_profile: HostProfile,
    pub fragment_count: u32,
    pub replica_count: usize,
    pub fragment_map: FragmentMap,
    /// Includes generated host identities when inventory was not supplied.
    pub hosts: Vec<Host>,
    pub host_estimates: Vec<HostEstimate>,
    pub hosts_per_region: BTreeMap<String, usize>,
    pub coverage_gaps: Vec<CoverageGap>,
    pub reliability: ReliabilityReport,
    pub traffic: TrafficEstimate,
    pub moved_fragments: usize,
    pub assumptions: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerError(pub String);
impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for PlannerError {}
