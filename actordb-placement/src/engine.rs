use crate::*;
use std::collections::{BTreeMap, BTreeSet};

fn error(message: impl Into<String>) -> PlannerError {
    PlannerError(message.into())
}
fn positive(name: &str, value: f64) -> Result<(), PlannerError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(error(format!("{name} must be finite and positive")));
    }
    Ok(())
}
fn nonnegative(name: &str, value: f64) -> Result<(), PlannerError> {
    if !value.is_finite() || value < 0.0 {
        return Err(error(format!("{name} must be finite and nonnegative")));
    }
    Ok(())
}
fn identity(name: &str, value: &str) -> Result<(), PlannerError> {
    if value.is_empty() || value.len() > 255 {
        return Err(error(format!("{name} must contain 1 to 255 UTF-8 bytes")));
    }
    Ok(())
}

fn validate_profile(p: &HostProfile) -> Result<(), PlannerError> {
    for (name, value) in [
        ("cpu_cores", p.cpu_cores),
        ("network_bytes_per_second", p.network_bytes_per_second),
        ("disk_write_bytes_per_second", p.disk_write_bytes_per_second),
        ("point_read_cpu_us", p.point_read_cpu_us),
        ("write_cpu_us", p.write_cpu_us),
        ("examined_record_cpu_us", p.examined_record_cpu_us),
        ("query_fragment_cpu_us", p.query_fragment_cpu_us),
        ("resident_data_multiplier", p.resident_data_multiplier),
        ("disk_data_multiplier", p.disk_data_multiplier),
    ] {
        positive(name, value)?;
    }
    nonnegative("stable_write_cpu_us", p.stable_write_cpu_us)?;
    nonnegative("fragment_idle_cpu_cores", p.fragment_idle_cpu_cores)?;
    nonnegative("replication_backlog_seconds", p.replication_backlog_seconds)?;
    if p.ram_bytes == 0
        || p.disk_bytes == 0
        || p.max_fragments_per_host == 0
        || p.workers_per_pool_set == 0
    {
        return Err(error(
            "host RAM, disk, fragment limit and worker count must be nonzero",
        ));
    }
    Ok(())
}

fn validate_input(input: &PlannerInput) -> Result<(), PlannerError> {
    let w = &input.workload;
    let p = &input.policy;
    validate_profile(&input.host_profile)?;
    if p.regions.is_empty()
        || p.replicas_per_region == 0
        || p.replicas_per_region > p.max_hosts
        || p.regions.len() > p.max_hosts
        || p.replica_count == Some(0)
        || p.max_hosts == 0
        || p.max_hosts > 10_000
        || p.max_moved_fragments == 0
        || p.target_fragment_bytes == 0
        || p.min_fragments_per_host == 0
        || p.max_fragment_count == 0
        || p.max_fragment_count > 65_536
    {
        return Err(error(
            "invalid placement limits (maximum 10,000 hosts and 65,536 fragments)",
        ));
    }
    if p.regions.len().checked_mul(p.replicas_per_region).is_none() || copies(input) > p.max_hosts {
        return Err(error("replica count exceeds the host planning limit"));
    }
    let mut regions = BTreeSet::new();
    for region in &p.regions {
        identity("region", region)?;
        if !regions.insert(region) {
            return Err(error("duplicate preferred region"));
        }
    }
    for (name, value) in [
        ("growth_factor", w.growth_factor),
        ("skew_factor", w.skew_factor),
    ] {
        positive(name, value)?;
        if value < 1.0 {
            return Err(error(format!("{name} must be at least one")));
        }
    }
    if w.resident_tables == 0 || w.resident_tables > 4096 {
        return Err(error("resident_tables must be between 1 and 4096"));
    }
    for (name, value) in [
        ("point_read_qps", w.point_read_qps),
        ("write_qps", w.write_qps),
        ("tombstone_ratio", w.tombstone_ratio),
        ("rebalance_improvement", p.rebalance_improvement),
    ] {
        nonnegative(name, value)?;
    }
    if !p.spare_capacity_fraction.is_finite()
        || !(0.0..1.0).contains(&p.spare_capacity_fraction)
        || p.rebalance_improvement >= 1.0
    {
        return Err(error("headroom and rebalance fractions must be in [0, 1)"));
    }
    positive("recovery_window_seconds", p.recovery_window_seconds)?;
    for q in &w.queries {
        nonnegative("query qps", q.qps)?;
        if !q.fragment_fraction.is_finite()
            || !(0.0..=1.0).contains(&q.fragment_fraction)
            || q.fragment_fraction == 0.0
        {
            return Err(error("query fragment_fraction must be in (0, 1]"));
        }
        if q.records_examined
            .zip(q.rows_returned)
            .is_some_and(|(examined, rows)| {
                !matches!(q.kind, QueryKind::Aggregate) && rows > examined
            })
        {
            return Err(error("query cannot return more rows than it examines"));
        }
    }
    for (region, weight) in &w.regional_traffic_weights {
        identity("traffic region", region)?;
        nonnegative("regional traffic weight", *weight)?;
    }
    if !w.regional_traffic_weights.is_empty() {
        positive(
            "sum of regional traffic weights",
            w.regional_traffic_weights.values().sum(),
        )?;
    }
    if input.hosts.len() > p.max_hosts {
        return Err(error("inventory exceeds max_hosts"));
    }
    let mut ids = BTreeSet::new();
    for host in &input.hosts {
        identity("node id", &host.id)?;
        identity("host region", &host.region)?;
        if let Some(zone) = &host.zone {
            identity("zone", zone)?;
        }
        if !ids.insert(&host.id) {
            return Err(error("duplicate host id"));
        }
        validate_profile(profile(input, host))?;
    }
    for load in input.fragment_loads.values() {
        for value in [
            load.point_read_qps,
            load.write_qps,
            load.records_examined_per_second,
        ]
        .into_iter()
        .flatten()
        {
            nonnegative("measured fragment load", value)?;
        }
    }
    Ok(())
}

fn copies(input: &PlannerInput) -> usize {
    input.policy.replica_count.unwrap_or_else(|| {
        input
            .policy
            .regions
            .len()
            .saturating_mul(input.policy.replicas_per_region)
    })
}
fn profile<'a>(input: &'a PlannerInput, host: &'a Host) -> &'a HostProfile {
    host.profile.as_ref().unwrap_or(&input.host_profile)
}

pub(crate) fn validate_map(map: &FragmentMap, count: u32) -> Result<(), PlannerError> {
    if count == 0 || map.len() != count as usize {
        return Err(error("placement must cover exactly 0..fragment_count"));
    }
    for fragment in 0..count {
        let replicas = map
            .get(&fragment)
            .ok_or_else(|| error("placement is missing a fragment"))?;
        if replicas.is_empty() {
            return Err(error("fragment has no replicas"));
        }
        let mut seen = BTreeSet::new();
        for id in replicas {
            identity("replica id", id)?;
            if !seen.insert(id) {
                return Err(error("duplicate fragment replica"));
            }
        }
    }
    Ok(())
}

fn fixed_count(input: &PlannerInput) -> Result<Option<u32>, PlannerError> {
    let inferred = input
        .existing_placement
        .as_ref()
        .map(|map| map.len() as u32);
    if input
        .fragment_count
        .zip(inferred)
        .is_some_and(|(a, b)| a != b)
    {
        return Err(error(
            "fragment_count must match the existing placement; repartitioning is not supported",
        ));
    }
    let count = input.fragment_count.or(inferred);
    if count.is_some_and(|n| n == 0 || n > input.policy.max_fragment_count) {
        return Err(error(
            "fragment_count exceeds the configured planning limits",
        ));
    }
    if let Some(map) = &input.existing_placement {
        validate_map(map, count.unwrap())?;
    }
    if count.is_none() && !input.fragment_loads.is_empty() {
        return Err(error(
            "measured fragment loads require a fixed fragment_count",
        ));
    }
    if input
        .fragment_loads
        .keys()
        .any(|id| count.is_none_or(|n| *id >= n))
    {
        return Err(error("measured fragment load is out of range"));
    }
    Ok(count)
}

fn resident_dataset(input: &PlannerInput, p: &HostProfile) -> f64 {
    let w = &input.workload;
    let records = w.record_count as f64 * w.growth_factor;
    let live =
        w.average_key_bytes as f64 + w.average_value_bytes as f64 + p.record_metadata_bytes as f64;
    let tombstone = w.average_key_bytes as f64 + p.record_metadata_bytes as f64;
    records * (live + w.tombstone_ratio * tombstone) * p.resident_data_multiplier
}

fn choose_count(input: &PlannerInput, hosts: usize) -> u32 {
    let bytes = resident_dataset(input, &input.host_profile);
    let by_data = (bytes / input.policy.target_fragment_bytes as f64)
        .ceil()
        .max(1.0) as u32;
    let by_balance = ((hosts as f64 / copies(input) as f64)
        * input.policy.min_fragments_per_host as f64)
        .ceil()
        .max(1.0) as u32;
    by_data.max(by_balance).min(input.policy.max_fragment_count)
}

#[derive(Clone, Copy, Default)]
struct Unit {
    data: f64,
    reads: f64,
    writes: f64,
    examined: f64,
    query_requests: f64,
    query_result: f64,
    query_internal_result: f64,
}

fn unit(input: &PlannerInput, fragment: u32, count: u32, p: &HostProfile) -> Unit {
    let w = &input.workload;
    let skew = w.skew_factor;
    let divisor = count as f64;
    let mut u = Unit {
        data: resident_dataset(input, p) / divisor * skew,
        reads: w.point_read_qps / divisor * skew,
        writes: w.write_qps / divisor * skew,
        ..Unit::default()
    };
    for q in &w.queries {
        let examined = q
            .records_examined
            .map_or(w.record_count as f64 * w.growth_factor, |n| n as f64);
        let rows = q.rows_returned.map_or_else(
            || {
                if matches!(q.kind, QueryKind::Aggregate) {
                    1.0
                } else {
                    examined
                }
            },
            |n| n as f64,
        );
        let row_bytes = q.result_row_bytes.map_or_else(
            || {
                if matches!(q.kind, QueryKind::Aggregate) {
                    32.0
                } else {
                    w.average_key_bytes as f64 + w.average_value_bytes as f64
                }
            },
            |n| n as f64,
        );
        u.examined += q.qps * examined / divisor * skew;
        // Average touched-fragment count includes at least one per request.
        let touched = (divisor * q.fragment_fraction).max(1.0);
        u.query_requests += q.qps * touched / divisor * skew;
        u.query_result +=
            q.qps * rows * (row_bytes + p.wire_overhead_bytes as f64) / divisor * skew;
        u.query_internal_result += if matches!(q.kind, QueryKind::Aggregate) {
            q.qps * touched * (row_bytes + p.wire_overhead_bytes as f64) / divisor * skew
        } else {
            q.qps * rows * (row_bytes + p.wire_overhead_bytes as f64) / divisor * skew
        };
    }
    if let Some(load) = input.fragment_loads.get(&fragment) {
        if let Some(n) = load.resident_data_bytes {
            u.data = n as f64;
        }
        if let Some(n) = load.point_read_qps {
            u.reads = n;
        }
        if let Some(n) = load.write_qps {
            u.writes = n;
        }
        if let Some(n) = load.records_examined_per_second {
            u.examined = n;
        }
    }
    u
}

fn write_cpu(input: &PlannerInput, p: &HostProfile) -> f64 {
    p.write_cpu_us
        + if matches!(
            input.workload.write_durability,
            WriteDurability::StableCommitted
        ) {
            p.stable_write_cpu_us
        } else {
            0.0
        }
}
fn durable(input: &PlannerInput) -> bool {
    input.workload.durable_storage
        || !matches!(
            input.workload.write_durability,
            WriteDurability::MemoryApplied
        )
}
fn write_wire(input: &PlannerInput, p: &HostProfile) -> f64 {
    input.workload.average_key_bytes as f64
        + input.workload.average_value_bytes as f64
        + p.wire_overhead_bytes as f64
}
fn fixed_bytes(input: &PlannerInput, p: &HostProfile, replicas: usize) -> f64 {
    p.fragment_overhead_bytes as f64
        + (input.workload.resident_tables as f64 + 1.0) * p.table_pool_overhead_bytes as f64
        + replicas.saturating_sub(1) as f64 * p.sender_overhead_bytes as f64
}

fn weights(input: &PlannerInput) -> BTreeMap<String, f64> {
    let mut weights = input.workload.regional_traffic_weights.clone();
    if weights.is_empty() {
        for region in &input.policy.regions {
            weights.insert(region.clone(), 1.0);
        }
    }
    let total: f64 = weights.values().sum();
    weights.values_mut().for_each(|w| *w /= total);
    weights
}

/// Recommend a map using regional coverage, zone diversity and estimated
/// dominant-resource load. Stable host IDs break ties deterministically.
/// Overcommit is returned with diagnostics; it never silently drops fragments.
pub fn recommend(input: &PlannerInput) -> Result<Plan, PlannerError> {
    validate_input(input)?;
    let fixed = fixed_count(input)?;
    if input.hosts.is_empty() && input.existing_placement.is_some() {
        return Err(error(
            "existing placements require inventory, including unavailable hosts",
        ));
    }
    let mut hosts = input.hosts.clone();
    if hosts.is_empty() {
        let mut per_region = input
            .policy
            .replicas_per_region
            .max(copies(input).div_ceil(input.policy.regions.len()));
        loop {
            if per_region * input.policy.regions.len() > input.policy.max_hosts {
                return Err(error(
                    "workload exceeds max_hosts; supply larger profiles or raise the limit",
                ));
            }
            hosts.clear();
            for (r, region) in input.policy.regions.iter().enumerate() {
                for h in 0..per_region {
                    hosts.push(Host {
                        id: format!("planned-r{r}-h{h}"),
                        region: region.clone(),
                        zone: None,
                        profile: None,
                        available: true,
                    });
                }
            }
            let count = fixed.unwrap_or_else(|| choose_count(input, hosts.len()));
            let map = build_map(input, &hosts, count);
            let plan = assess(input, &hosts, count, map)?;
            let target = 1.0 - input.policy.spare_capacity_fraction;
            if plan.host_estimates.iter().all(|e| {
                e.peak_utilization() <= target && e.fragments <= e.recommended_fragment_limit
            }) {
                return Ok(plan);
            }
            // A single unsplittable fragment cannot be fixed by adding hosts.
            if per_region >= input.policy.max_hosts / input.policy.regions.len() {
                return Err(error(
                    "no feasible plan within max_hosts and fragment limits",
                ));
            }
            per_region += 1;
        }
    }
    if !hosts.iter().any(|h| h.available) {
        return Err(error("no available hosts"));
    }
    hosts.sort_by(|a, b| a.id.cmp(&b.id));
    let count = fixed.unwrap_or_else(|| choose_count(input, hosts.len()));
    let proposed = build_map(input, &hosts, count);
    let mut plan = assess(input, &hosts, count, proposed)?;
    if let Some(current) = &input.existing_placement {
        let previous = assess(input, &hosts, count, current.clone())?;
        let previous_peak = peak(&previous);
        let proposed_peak = peak(&plan);
        let repairs = coverage_score(&plan) < coverage_score(&previous)
            || unavailable_replicas(current, &hosts)
                > unavailable_replicas(&plan.fragment_map, &hosts);
        let improvement = if previous_peak > 0.0 {
            (previous_peak - proposed_peak) / previous_peak
        } else {
            0.0
        };
        if !repairs
            && (improvement <= input.policy.rebalance_improvement || proposed_peak >= previous_peak)
        {
            return Ok(previous);
        }
        let mut bounded = current.clone();
        let mut changed = 0;
        // Repair unavailable replicas first, then improve coverage/load.
        let mut fragments: Vec<u32> = (0..count).collect();
        fragments.sort_by_key(|id| {
            (
                !current[id]
                    .iter()
                    .any(|peer| hosts.iter().any(|h| &h.id == peer && !h.available)),
                *id,
            )
        });
        for id in fragments {
            if changed == input.policy.max_moved_fragments {
                break;
            }
            if same_replicas(&current[&id], &plan.fragment_map[&id]) {
                continue;
            }
            bounded.insert(id, plan.fragment_map[&id].clone());
            changed += 1;
        }
        plan = assess(input, &hosts, count, bounded)?;
        plan.moved_fragments = changed;
        if changed > 0 {
            plan.warnings.push("Target requires overlap/readiness sequencing before removing existing replicas; temporary copies need additional capacity.".into());
        }
    }
    Ok(plan)
}

fn same_replicas(a: &[String], b: &[String]) -> bool {
    a.iter().collect::<BTreeSet<_>>() == b.iter().collect::<BTreeSet<_>>()
}
fn unavailable_replicas(map: &FragmentMap, hosts: &[Host]) -> usize {
    map.values()
        .flatten()
        .filter(|id| hosts.iter().any(|h| &h.id == *id && !h.available))
        .count()
}
fn peak(plan: &Plan) -> f64 {
    plan.host_estimates
        .iter()
        .map(HostEstimate::peak_utilization)
        .fold(0.0, f64::max)
}

fn coverage_score(plan: &Plan) -> (usize, usize) {
    (
        plan.coverage_gaps
            .iter()
            .filter(|gap| gap.available_replicas == 0)
            .count(),
        plan.coverage_gaps
            .iter()
            .map(|gap| gap.desired_replicas - gap.available_replicas)
            .sum(),
    )
}

fn build_map(input: &PlannerInput, hosts: &[Host], count: u32) -> FragmentMap {
    let live: Vec<usize> = hosts
        .iter()
        .enumerate()
        .filter(|(_, h)| h.available)
        .map(|(i, _)| i)
        .collect();
    let count_copies = copies(input).min(live.len());
    let mut assigned = vec![0u32; hosts.len()];
    let mut scores = vec![0.0; hosts.len()];
    let mut map = FragmentMap::new();
    let mut fragments: Vec<u32> = (0..count).collect();
    // Place measured large/hot fragments first, retaining stable ID tie breaks.
    fragments.sort_by(|a, b| {
        let pa = unit(input, *a, count, &input.host_profile);
        let pb = unit(input, *b, count, &input.host_profile);
        let cost = |u: Unit| {
            u.data / input.host_profile.ram_bytes as f64
                + (u.reads + u.writes + u.examined) / 1_000_000.0
        };
        cost(pb).total_cmp(&cost(pa)).then(a.cmp(b))
    });
    for fragment in fragments {
        let mut chosen: Vec<usize> = Vec::new();
        let mut desired_regions = Vec::new();
        for _ in 0..input.policy.replicas_per_region {
            for offset in 0..input.policy.regions.len() {
                desired_regions.push(
                    &input.policy.regions
                        [(fragment as usize + offset) % input.policy.regions.len()],
                );
            }
        }
        for slot in 0..count_copies {
            let desired = desired_regions.get(slot).copied();
            let cost = |i: usize| {
                let p = profile(input, &hosts[i]);
                let u = unit(input, fragment, count, p);
                let memory = (u.data
                    + fixed_bytes(input, p, count_copies)
                    + u.writes
                        * write_wire(input, p)
                        * p.replication_backlog_seconds
                        * count_copies.saturating_sub(1) as f64)
                    / p.ram_bytes as f64;
                let cpu = (u.writes * write_cpu(input, p)
                    + u.reads * p.point_read_cpu_us / count_copies as f64
                    + u.examined * p.examined_record_cpu_us / count_copies as f64
                    + u.query_requests * p.query_fragment_cpu_us / count_copies as f64)
                    / 1_000_000.0
                    + p.fragment_idle_cpu_cores;
                let network =
                    u.writes * write_wire(input, p) * count_copies.saturating_sub(1) as f64
                        + u.data * 2.0 / input.policy.recovery_window_seconds;
                memory
                    .max(cpu / p.cpu_cores)
                    .max(network / p.network_bytes_per_second)
            };
            let target = 1.0 - input.policy.spare_capacity_fraction;
            let best = live
                .iter()
                .copied()
                .filter(|i| !chosen.contains(i))
                .min_by(|a, b| {
                    let rank = |i: usize| {
                        let h = &hosts[i];
                        let p = profile(input, h);
                        let projected = scores[i] + cost(i);
                        let exceeds = projected > target || assigned[i] >= p.max_fragments_per_host;
                        let wrong_region = desired.is_some_and(|r| &h.region != r);
                        let repeated_zone = h.zone.as_ref().is_none_or(|z| {
                            chosen.iter().any(|j| {
                                hosts[*j].region == h.region && hosts[*j].zone.as_ref() == Some(z)
                            })
                        });
                        (exceeds, wrong_region, repeated_zone, projected, &h.id)
                    };
                    let ra = rank(*a);
                    let rb = rank(*b);
                    ra.0.cmp(&rb.0)
                        .then(ra.1.cmp(&rb.1))
                        .then(ra.2.cmp(&rb.2))
                        .then(ra.3.total_cmp(&rb.3))
                        .then(ra.4.cmp(rb.4))
                })
                .expect("available unique replicas");
            scores[best] += cost(best);
            assigned[best] += 1;
            chosen.push(best);
        }
        map.insert(
            fragment,
            chosen.into_iter().map(|i| hosts[i].id.clone()).collect(),
        );
    }
    map
}

/// Evaluate an existing explicit map without changing any assignments.
/// Unknown replica IDs are errors; retain failed members in inventory with
/// available=false so coverage and failure reports exclude their copies.
pub fn evaluate(input: &PlannerInput, map: &FragmentMap) -> Result<Plan, PlannerError> {
    validate_input(input)?;
    let fixed = fixed_count(input)?;
    let count = fixed.unwrap_or(map.len() as u32);
    if count > input.policy.max_fragment_count {
        return Err(error("map exceeds max_fragment_count"));
    }
    if input.hosts.is_empty() {
        return Err(error("evaluation requires host inventory"));
    }
    if input.fragment_loads.keys().any(|id| *id >= count) {
        return Err(error("measured load out of range"));
    }
    assess(input, &input.hosts, count, map.clone())
}

fn assess(
    input: &PlannerInput,
    hosts: &[Host],
    count: u32,
    map: FragmentMap,
) -> Result<Plan, PlannerError> {
    validate_map(&map, count)?;
    let indices: BTreeMap<&str, usize> = hosts
        .iter()
        .enumerate()
        .map(|(i, h)| (h.id.as_str(), i))
        .collect();
    for id in map.values().flatten() {
        if !indices.contains_key(id.as_str()) {
            return Err(error(format!("replica {id:?} is missing from inventory")));
        }
    }
    let mut estimates: Vec<HostEstimate> = hosts
        .iter()
        .map(|h| HostEstimate {
            id: h.id.clone(),
            region: h.region.clone(),
            resident_bytes: profile(input, h).node_overhead_bytes as f64,
            ..HostEstimate::default()
        })
        .collect();
    let mut gaps = Vec::new();
    let mut reliability = ReliabilityReport {
        survives_one_host_loss: true,
        survives_one_region_loss: true,
        regional_coverage_survives_one_host_loss: true,
        ..ReliabilityReport::default()
    };
    let traffic_weights = weights(input);
    let mut traffic = TrafficEstimate::default();
    for (fragment, replicas) in &map {
        let live: Vec<usize> = replicas
            .iter()
            .map(|id| indices[id.as_str()])
            .filter(|i| hosts[*i].available)
            .collect();
        let regions: BTreeSet<_> = live.iter().map(|i| &hosts[*i].region).collect();
        reliability.survives_one_host_loss &= live.len() >= 2;
        reliability.survives_one_region_loss &= regions.len() >= 2;
        for region in &input.policy.regions {
            let n = live.iter().filter(|i| &hosts[**i].region == region).count();
            reliability.regional_coverage_survives_one_host_loss &= n >= 2;
            if n < input.policy.replicas_per_region {
                gaps.push(CoverageGap {
                    fragment: *fragment,
                    region: region.clone(),
                    desired_replicas: input.policy.replicas_per_region,
                    available_replicas: n,
                });
            }
        }
        let known_zones: BTreeSet<_> = live
            .iter()
            .filter_map(|i| hosts[*i].zone.as_ref().map(|z| (&hosts[*i].region, z)))
            .collect();
        if live.iter().any(|i| hosts[*i].zone.is_none()) {
            reliability.unknown_zone_fragments += 1;
        }
        if known_zones.len() >= 2 {
            reliability.zone_diverse_fragments += 1;
        }
        for i in &live {
            let p = profile(input, &hosts[*i]);
            let u = unit(input, *fragment, count, p);
            let e = &mut estimates[*i];
            e.fragments += 1;
            e.actors +=
                4 + (input.workload.resident_tables as u64 + 1) * p.workers_per_pool_set as u64;
            e.sender_tasks += replicas.len().saturating_sub(1) as u64;
            e.resident_bytes += u.data
                + fixed_bytes(input, p, replicas.len())
                + u.writes
                    * write_wire(input, p)
                    * p.replication_backlog_seconds
                    * replicas.len().saturating_sub(1) as f64;
            e.cpu_cores += u.writes * write_cpu(input, p) / 1_000_000.0 + p.fragment_idle_cpu_cores;
            if durable(input) {
                e.disk_bytes += u.data / p.resident_data_multiplier * p.disk_data_multiplier;
                e.disk_write_bytes_per_second +=
                    u.writes * write_wire(input, p) * p.disk_data_multiplier;
            }
            let recovery = u.data / input.policy.recovery_window_seconds;
            e.traffic.recovery_bytes_per_second += recovery;
            // Symmetric budget accounts for serving and receiving repair data.
            e.network_bytes_per_second += recovery * 2.0;
            traffic.recovery_bytes_per_second += recovery;
        }
        if live.is_empty() {
            continue;
        }
        for (origin, weight) in &traffic_weights {
            let local: Vec<usize> = live
                .iter()
                .copied()
                .filter(|i| &hosts[*i].region == origin)
                .collect();
            // Assume caller ingress is balanced across available regional replicas.
            // With no regional holder, current map order chooses the remote peer.
            let readers = if local.is_empty() {
                vec![live[0]]
            } else {
                local
            };
            let share = weight / readers.len() as f64;
            for i in readers {
                let p = profile(input, &hosts[i]);
                let u = unit(input, *fragment, count, p);
                let app = share
                    * (u.reads * write_wire(input, p)
                        + u.writes * write_wire(input, p)
                        + u.query_result);
                let query = share
                    * (u.query_internal_result + u.query_requests * p.wire_overhead_bytes as f64);
                let cross = &hosts[i].region != origin;
                let e = &mut estimates[i];
                e.cpu_cores += share
                    * (u.reads * p.point_read_cpu_us
                        + u.examined * p.examined_record_cpu_us
                        + u.query_requests * p.query_fragment_cpu_us)
                    / 1_000_000.0;
                e.network_bytes_per_second += app + query * 2.0;
                e.traffic.application_bytes_per_second += app;
                e.traffic.query_bytes_per_second += query;
                traffic.application_bytes_per_second += app;
                traffic.query_bytes_per_second += query;
                if cross {
                    e.traffic.cross_region_application_bytes_per_second += app;
                    e.traffic.cross_region_query_bytes_per_second += query;
                    traffic.cross_region_application_bytes_per_second += app;
                    traffic.cross_region_query_bytes_per_second += query;
                }
                // Each accepted write fans out directly to every other target.
                let replica_bytes = share * u.writes * write_wire(input, p);
                for target in &live {
                    if *target == i {
                        continue;
                    }
                    estimates[i].network_bytes_per_second += replica_bytes;
                    estimates[*target].network_bytes_per_second += replica_bytes;
                    estimates[i].traffic.replication_bytes_per_second += replica_bytes;
                    traffic.replication_bytes_per_second += replica_bytes;
                    if hosts[i].region != hosts[*target].region {
                        estimates[i]
                            .traffic
                            .cross_region_replication_bytes_per_second += replica_bytes;
                        traffic.cross_region_replication_bytes_per_second += replica_bytes;
                    }
                }
            }
        }
    }
    let mut warnings = Vec::new();
    let mut hosts_per_region = BTreeMap::new();
    let target = 1.0 - input.policy.spare_capacity_fraction;
    for (h, e) in hosts.iter().zip(&mut estimates) {
        let p = profile(input, h);
        if h.available && e.fragments > 0 {
            *hosts_per_region.entry(h.region.clone()).or_default() += 1;
        }
        e.memory_utilization = e.resident_bytes / p.ram_bytes as f64;
        e.cpu_utilization = e.cpu_cores / p.cpu_cores;
        e.disk_utilization = e.disk_bytes / p.disk_bytes as f64;
        e.network_utilization = e.network_bytes_per_second / p.network_bytes_per_second;
        e.disk_write_utilization = e.disk_write_bytes_per_second / p.disk_write_bytes_per_second;
        // Average assigned-fragment cost gives a useful host-specific ceiling.
        // Hot-fragment imbalance can still make the average ceiling optimistic.
        let mut limit = p.max_fragments_per_host;
        if e.fragments > 0 {
            for (used, capacity, fixed) in [
                (
                    e.resident_bytes,
                    p.ram_bytes as f64,
                    p.node_overhead_bytes as f64,
                ),
                (e.cpu_cores, p.cpu_cores, 0.0),
                (e.disk_bytes, p.disk_bytes as f64, 0.0),
                (e.network_bytes_per_second, p.network_bytes_per_second, 0.0),
                (
                    e.disk_write_bytes_per_second,
                    p.disk_write_bytes_per_second,
                    0.0,
                ),
            ] {
                let per_fragment = (used - fixed) / e.fragments as f64;
                if per_fragment > 0.0 {
                    limit = limit
                        .min(((capacity * target - fixed).max(0.0) / per_fragment).floor() as u32);
                }
            }
        }
        e.recommended_fragment_limit = limit;
        if e.peak_utilization() > target || e.fragments > limit {
            warnings.push(format!(
                "Host {} exceeds the {:.0}% operating target or its fragment limit ({}/{}).",
                h.id,
                target * 100.0,
                e.fragments,
                limit
            ));
        }
        for value in [
            e.resident_bytes,
            e.disk_bytes,
            e.cpu_cores,
            e.network_bytes_per_second,
            e.disk_write_bytes_per_second,
            e.memory_utilization,
            e.cpu_utilization,
            e.disk_utilization,
            e.network_utilization,
            e.disk_write_utilization,
        ] {
            if !value.is_finite() {
                return Err(error("workload produces non-finite resource estimates"));
            }
        }
    }
    if !gaps.is_empty() {
        warnings.push(format!(
            "{} fragment/region coverage goals are unmet.",
            gaps.len()
        ));
    }
    if map.values().any(|replicas| replicas.len() < copies(input)) {
        warnings.push("Insufficient distinct hosts for the requested total replica count.".into());
    }
    if unavailable_replicas(&map, hosts) > 0 {
        warnings.push("Placement contains unavailable replicas; their copies do not count toward reliability.".into());
    }
    if reliability.unknown_zone_fragments > 0 {
        warnings.push("Zone locations are unknown for some replicas; zone-failure resilience cannot be established.".into());
    }
    if hosts
        .iter()
        .any(|h| profile(input, h).calibration.contains("uncalibrated"))
    {
        warnings.push("Host profile includes uncalibrated assumptions; benchmark the deployment configuration before relying on throughput or fragment ceilings.".into());
    }
    if count == input.policy.max_fragment_count {
        warnings.push(
            "Fragment count reached the planning ceiling; check balancing and fragment sizes."
                .into(),
        );
    }
    let assumptions = vec![
        format!("Growth factor {}, unknown-skew factor {}, resource headroom {:.0}%.",input.workload.growth_factor,input.workload.skew_factor,input.policy.spare_capacity_fraction*100.0),
        "All records remain resident in the cache, including with durable storage; keys, indexes, metadata, tombstones and modeled replication backlog are included.".into(),
        "Caller ingress is balanced across regional replica holders. Without a regional holder, requests use the first available replica in map order. Actual ingress placement may change these estimates.".into(),
        "SQL/scan CPU uses examined records; aggregates return compact final results but partials from touched fragments. Query traffic conservatively assumes remote fragment/coordinator exchanges.".into(),
        "Traffic includes estimated framing; replication is direct sender-to-replica fanout. Recovery reserves symmetric full-copy bandwidth over the configured window; retries/anti-entropy scans and failure-load redistribution require measured headroom.".into(),
        "Reliability describes available provisioned copies, not acknowledged-write survival, version equality, consensus or writer authority. Configure replication ACK/write concern and backend durability separately.".into(),
        "The fragment limit is based on average assigned load and the host profile; a single hot key or fragment can exceed latency targets before the host average fills.".into(),
    ];
    Ok(Plan {
        workload: input.workload.clone(),
        policy: input.policy.clone(),
        host_profile: input.host_profile.clone(),
        fragment_count: count,
        replica_count: copies(input),
        fragment_map: map,
        hosts: hosts.to_vec(),
        host_estimates: estimates,
        hosts_per_region,
        coverage_gaps: gaps,
        reliability,
        traffic,
        moved_fragments: 0,
        assumptions,
        warnings,
    })
}
