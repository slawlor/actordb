use actordb_placement::*;
use std::collections::{BTreeMap, BTreeSet};

fn input() -> PlannerInput {
    let mut input = PlannerInput::default();
    input.workload.record_count = 10_000;
    input.workload.growth_factor = 1.0;
    input.workload.skew_factor = 1.0;
    input.policy.regions = vec!["east".into(), "west".into()];
    input.fragment_count = Some(8);
    input.hosts = ["east", "west"]
        .into_iter()
        .flat_map(|region| {
            (0..2).map(move |i| Host {
                id: format!("{region}-{i}"),
                region: region.into(),
                zone: Some(format!("zone-{i}")),
                profile: None,
                available: true,
            })
        })
        .collect();
    input
}

#[test]
fn standard_coverage_is_deterministic_zone_diverse_and_balanced() {
    let i = input();
    let plan = recommend(&i).unwrap();
    assert!(plan.coverage_gaps.is_empty());
    assert!(plan.reliability.survives_one_region_loss);
    assert!(plan.reliability.regional_coverage_survives_one_host_loss);
    assert_eq!(plan.reliability.zone_diverse_fragments, 8);
    assert!(plan
        .host_estimates
        .iter()
        .all(|h| h.fragments == 8 && h.actors == 160 && h.sender_tasks == 24));
    let mut reversed = i;
    reversed.hosts.reverse();
    assert_eq!(
        plan.fragment_map,
        recommend(&reversed).unwrap().fragment_map
    );
}

#[test]
fn limited_copies_and_missing_regions_are_soft_goals() {
    let mut i = input();
    i.policy.replica_count = Some(1);
    let plan = recommend(&i).unwrap();
    assert_eq!(plan.fragment_map.len(), 8);
    assert!(plan.fragment_map.values().all(|r| r.len() == 1));
    assert!(!plan.coverage_gaps.is_empty());
    assert!(!plan.reliability.survives_one_host_loss);
    i.hosts.retain(|h| h.region == "east");
    assert!(recommend(&i)
        .unwrap()
        .coverage_gaps
        .iter()
        .any(|g| g.region == "west"));
    i.hosts.clear();
    assert!(
        recommend(&i).is_ok(),
        "synthetic sizing must also allow soft coverage"
    );
}

#[test]
fn query_cost_depends_on_examined_records_and_aggregate_partials() {
    let mut i = input();
    i.workload.queries = vec![QueryWorkload {
        kind: QueryKind::Aggregate,
        examined_record_cpu_us: None,
        qps: 10.0,
        records_examined: Some(10),
        rows_returned: Some(1),
        result_row_bytes: Some(8),
        fragment_fraction: 1.0,
    }];
    let small = recommend(&i).unwrap();
    i.workload.queries[0].records_examined = Some(10_000);
    let large = recommend(&i).unwrap();
    assert!(
        large
            .host_estimates
            .iter()
            .map(|e| e.cpu_cores)
            .sum::<f64>()
            > small
                .host_estimates
                .iter()
                .map(|e| e.cpu_cores)
                .sum::<f64>()
    );
    assert_eq!(
        small.traffic.application_bytes_per_second,
        large.traffic.application_bytes_per_second
    );
    assert!(large.traffic.query_bytes_per_second > large.traffic.application_bytes_per_second);
}

#[test]
fn replication_is_one_transfer_per_other_copy_and_cross_region_edges_count() {
    let mut i = input();
    i.workload.write_qps = 100.0;
    let plan = recommend(&i).unwrap();
    let wire = (i.workload.average_key_bytes
        + i.workload.average_value_bytes
        + i.host_profile.wire_overhead_bytes) as f64;
    assert_eq!(
        plan.traffic.replication_bytes_per_second,
        100.0 * wire * 3.0
    );
    assert_eq!(
        plan.traffic.cross_region_replication_bytes_per_second,
        100.0 * wire * 2.0
    );
    assert_eq!(plan.traffic.cross_region_application_bytes_per_second, 0.0);
}

#[test]
fn measured_fragment_load_can_change_balancing_without_repartitioning() {
    let mut i = input();
    i.policy.replica_count = Some(2);
    let mut extra = i.hosts.clone();
    for h in &mut extra {
        h.id.push_str("-extra");
    }
    i.hosts.extend(extra);
    i.fragment_loads.insert(
        0,
        FragmentLoad {
            resident_data_bytes: Some(1024 * 1024 * 1024),
            point_read_qps: Some(100_000.0),
            ..Default::default()
        },
    );
    let p = recommend(&i).unwrap();
    assert_eq!(p.fragment_count, 8);
    assert_eq!(p.fragment_map[&0].len(), 2);
    let loads: Vec<_> = p.host_estimates.iter().map(|h| h.resident_bytes).collect();
    assert!(loads.iter().copied().fold(0.0, f64::max) > 1024.0 * 1024.0 * 1024.0);
}

#[test]
fn unavailable_hosts_do_not_count_and_moves_are_bounded() {
    let mut i = input();
    i.policy.replica_count = Some(2);
    i.policy.replicas_per_region = 1;
    let current = recommend(&i).unwrap().fragment_map;
    i.existing_placement = Some(current.clone());
    let failed = current[&0][0].clone();
    i.hosts
        .iter_mut()
        .find(|h| h.id == failed)
        .unwrap()
        .available = false;
    let evaluated = evaluate(&i, &current).unwrap();
    assert!(!evaluated.reliability.survives_one_host_loss);
    i.policy.max_moved_fragments = 1;
    let p = recommend(&i).unwrap();
    assert_eq!(p.moved_fragments, 1);
    assert_eq!(
        p.fragment_map
            .iter()
            .filter(|(id, r)| current[id] != **r)
            .count(),
        1
    );
}

#[test]
fn unchanged_load_preserves_existing_assignments() {
    let mut i = input();
    let p = recommend(&i).unwrap();
    i.existing_placement = Some(p.fragment_map.clone());
    assert_eq!(recommend(&i).unwrap().fragment_map, p.fragment_map);
    i.fragment_count = Some(9);
    assert!(recommend(&i).is_err());
}

#[test]
fn covering_a_missing_region_is_preferred_with_a_limited_replica_budget() {
    let mut i = input();
    i.policy.replica_count = Some(2);
    i.existing_placement = Some(
        (0..8)
            .map(|id| (id, vec!["east-0".into(), "east-1".into()]))
            .collect(),
    );
    let plan = recommend(&i).unwrap();
    assert!(plan.reliability.survives_one_region_loss);
    assert!(plan
        .coverage_gaps
        .iter()
        .all(|gap| gap.available_replicas > 0));
}

#[test]
fn validation_rejects_impossible_and_nonfinite_inputs() {
    let mut i = input();
    i.workload.write_qps = f64::NAN;
    assert!(recommend(&i).is_err());
    i.workload.write_qps = 0.0;
    i.hosts[1].id = i.hosts[0].id.clone();
    assert!(recommend(&i).is_err());
    let i = input();
    assert!(evaluate(&i, &BTreeMap::from([(0, vec!["unknown".into()])])).is_err());
    assert!(PlacementTransition::new(BTreeMap::new(), BTreeMap::new(), 1).is_err());
}

#[test]
fn host_fragment_ceiling_tracks_real_resources_and_durable_writes() {
    let mut i = input();
    let light = recommend(&i).unwrap();
    i.workload.write_qps = 10_000.0;
    i.workload.write_durability = WriteDurability::StableCommitted;
    let heavy = recommend(&i).unwrap();
    assert!(heavy.host_estimates[0].disk_write_bytes_per_second > 0.0);
    assert!(
        heavy.host_estimates[0].recommended_fragment_limit
            < light.host_estimates[0].recommended_fragment_limit
    );
    i.host_profile.ram_bytes = 100;
    assert!(
        !recommend(&i).unwrap().warnings.is_empty(),
        "inventory overcommit must be explained"
    );
}

#[test]
fn synthetic_sizing_adds_hosts_and_keeps_replica_factor() {
    let mut i = input();
    i.hosts.clear();
    i.fragment_count = None;
    i.host_profile.ram_bytes = 512 * 1024 * 1024;
    i.workload.record_count = 1_000_000;
    let plan = recommend(&i).unwrap();
    assert!(plan.hosts.len() > 4);
    assert!(plan
        .host_estimates
        .iter()
        .all(|h| h.memory_utilization <= 0.7));
    assert!(plan.fragment_map.values().all(|r| r.len() == 4));
}

#[test]
fn migration_keeps_old_owner_until_all_target_replicas_are_ready() {
    let initial = BTreeMap::from([(0, vec!["a".into()]), (1, vec!["a".into()])]);
    let target = BTreeMap::from([(0, vec!["b".into(), "c".into()]), (1, vec!["b".into()])]);
    let mut transition = PlacementTransition::new(initial, target, 1).unwrap();
    assert_eq!(transition.placement()[&0], vec!["a", "b", "c"]);
    assert_eq!(transition.placement()[&1], vec!["a"]);
    let mut ready = BTreeMap::from([(0, BTreeSet::from(["b".into()]))]);
    transition.advance(&ready);
    assert!(transition.placement()[&0].contains(&"a".into()));
    ready.get_mut(&0).unwrap().insert("c".into());
    transition.advance(&ready);
    assert_eq!(transition.placement()[&0], vec!["b", "c"]);
    assert_eq!(transition.placement()[&1], vec!["a", "b"]);
    ready.insert(1, BTreeSet::from(["b".into()]));
    transition.advance(&ready);
    assert!(transition.is_complete());
}
