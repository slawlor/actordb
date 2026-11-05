use actordb_placement::*;
use std::collections::BTreeMap;

fn host(id: &str, region: &str, profile: HostProfile) -> Host {
    Host {
        id: id.into(),
        region: region.into(),
        zone: None,
        profile: Some(profile),
        available: true,
    }
}

fn input() -> PlannerInput {
    let mut input = PlannerInput {
        fragment_count: Some(8),
        ..Default::default()
    };
    input.workload.growth_factor = 1.0;
    input.workload.skew_factor = 1.0;
    input.policy.replicas_per_region = 1;
    input.hosts = vec![
        host("a", "local", HostProfile::default()),
        host("b", "local", HostProfile::default()),
    ];
    input
}

#[test]
fn placement_respects_disk_capacity_when_a_smaller_memory_host_has_more_disk() {
    let mut i = input();
    i.workload.record_count = 1_000_000;
    i.workload.average_value_bytes = 1024;
    i.workload.durable_storage = true;
    i.hosts[0].profile.as_mut().unwrap().ram_bytes = 64 * 1024 * 1024 * 1024;
    i.hosts[0].profile.as_mut().unwrap().disk_bytes = 512 * 1024 * 1024;
    let plan = recommend(&i).unwrap();
    assert!(
        plan.host_estimates
            .iter()
            .all(|e| e.disk_utilization <= 0.7),
        "{:?}",
        plan.host_estimates
    );
    assert!(
        plan.host_estimates
            .iter()
            .find(|e| e.id == "b")
            .unwrap()
            .fragments
            >= 7
    );
}

#[test]
fn placement_respects_disk_write_throughput() {
    let mut i = input();
    i.workload.record_count = 1000;
    i.workload.average_value_bytes = 128;
    i.workload.write_qps = 40_000.0;
    i.workload.durable_storage = true;
    i.hosts[0]
        .profile
        .as_mut()
        .unwrap()
        .disk_write_bytes_per_second = 1_000_000.0;
    let plan = recommend(&i).unwrap();
    assert!(
        plan.host_estimates
            .iter()
            .all(|e| e.disk_write_utilization <= 0.7),
        "{:?}",
        plan.host_estimates
    );
    assert_eq!(
        plan.host_estimates
            .iter()
            .find(|e| e.id == "a")
            .unwrap()
            .fragments,
        0
    );
}

#[test]
fn regional_read_load_is_charged_to_the_region_serving_it() {
    let mut i = input();
    i.fragment_count = Some(2);
    i.policy.regions = vec!["west".into(), "east".into()];
    i.workload.average_value_bytes = 8;
    i.workload.point_read_qps = 80_000.0;
    i.workload.regional_traffic_weights = BTreeMap::from([("west".into(), 1.0)]);
    i.fragment_loads = (0..2)
        .map(|id| {
            (
                id,
                FragmentLoad {
                    resident_data_bytes: Some(45 * 1024 * 1024),
                    ..Default::default()
                },
            )
        })
        .collect();
    let slow = HostProfile {
        cpu_cores: 1.0,
        node_overhead_bytes: 0,
        ..Default::default()
    };
    let fast = HostProfile {
        ram_bytes: 100 * 1024 * 1024,
        node_overhead_bytes: 0,
        ..Default::default()
    };
    i.hosts = vec![
        host("a-west", "west", slow.clone()),
        host("b-west", "west", fast.clone()),
        host("c-west", "west", fast),
        host("d-east", "east", slow.clone()),
        host("e-east", "east", slow),
    ];
    let plan = recommend(&i).unwrap();
    assert!(
        plan.host_estimates.iter().all(|e| e.cpu_utilization <= 0.7),
        "{:?}",
        plan.host_estimates
    );
    assert!(plan
        .fragment_map
        .values()
        .all(|r| !r.contains(&"a-west".to_string())));
}

#[test]
fn reducing_the_copy_budget_is_applied_even_if_peak_load_barely_changes() {
    let mut i = input();
    i.fragment_count = Some(4);
    i.policy.max_moved_fragments = 1;
    i.existing_placement = Some(
        (0..4)
            .map(|id| (id, vec!["a".into(), "b".into()]))
            .collect(),
    );
    let plan = recommend(&i).unwrap();
    assert_eq!(plan.moved_fragments, 1);
    assert_eq!(
        plan.fragment_map.values().filter(|r| r.len() == 1).count(),
        1
    );
    assert!(plan
        .warnings
        .iter()
        .any(|warning| warning.contains("copy limit")));
}

#[test]
fn an_existing_host_fragment_limit_is_repaired_despite_hysteresis() {
    let mut i = input();
    i.fragment_count = Some(4);
    for host in &mut i.hosts {
        host.profile.as_mut().unwrap().max_fragments_per_host = 2;
    }
    i.existing_placement = Some((0..4).map(|id| (id, vec!["a".into()])).collect());
    let plan = recommend(&i).unwrap();
    assert!(
        plan.host_estimates.iter().all(|host| host.fragments <= 2),
        "{:?}",
        plan.host_estimates
    );
    assert!(plan.moved_fragments > 0);
}

#[test]
fn overloaded_hosts_can_improve_by_less_than_the_normal_rebalance_threshold() {
    let mut i = input();
    i.fragment_count = Some(4);
    for host in &mut i.hosts {
        host.profile.as_mut().unwrap().ram_bytes = 96 * 1024 * 1024;
    }
    i.fragment_loads = (0..4)
        .map(|id| {
            (
                id,
                FragmentLoad {
                    resident_data_bytes: Some(4 * 1024 * 1024),
                    ..Default::default()
                },
            )
        })
        .collect();
    i.existing_placement = Some(BTreeMap::from([
        (0, vec!["a".into()]),
        (1, vec!["a".into()]),
        (2, vec!["a".into()]),
        (3, vec!["b".into()]),
    ]));
    let previous = evaluate(&i, i.existing_placement.as_ref().unwrap()).unwrap();
    let plan = recommend(&i).unwrap();
    let peak = |p: &Plan| {
        p.host_estimates
            .iter()
            .map(HostEstimate::peak_utilization)
            .fold(0.0, f64::max)
    };
    assert!(peak(&plan) < peak(&previous));
    assert!((peak(&previous) - peak(&plan)) / peak(&previous) < i.policy.rebalance_improvement);
    assert!(plan.moved_fragments > 0);
}

#[test]
fn unavailable_host_overhead_cannot_prevent_rebalancing_available_hosts() {
    let mut i = input();
    i.fragment_count = Some(2);
    for host in &mut i.hosts {
        host.profile.as_mut().unwrap().ram_bytes = 400 * 1024 * 1024;
    }
    i.fragment_loads = (0..2)
        .map(|id| {
            (
                id,
                FragmentLoad {
                    resident_data_bytes: Some(128 * 1024 * 1024),
                    ..Default::default()
                },
            )
        })
        .collect();
    i.existing_placement = Some((0..2).map(|id| (id, vec!["a".into()])).collect());
    let mut unavailable = host(
        "z",
        "local",
        HostProfile {
            ram_bytes: 1,
            ..Default::default()
        },
    );
    unavailable.available = false;
    i.hosts.push(unavailable);
    let plan = recommend(&i).unwrap();
    assert!(plan.moved_fragments > 0);
    assert!(plan
        .host_estimates
        .iter()
        .all(|e| e.memory_utilization <= 0.7));
}

#[test]
fn impossible_average_record_geometry_is_rejected_before_sizing() {
    let mut i = input();
    i.workload.record_count = 1;
    i.workload.average_value_bytes = 16 * 1024 * 1024;
    assert!(recommend(&i)
        .unwrap_err()
        .0
        .contains("replication envelope"));
    i.workload.average_key_bytes = u64::MAX;
    assert!(
        recommend(&i).is_err(),
        "overflow must not hide an impossible size"
    );
}
