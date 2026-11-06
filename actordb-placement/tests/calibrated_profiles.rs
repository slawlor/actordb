use actordb_placement::*;

fn input() -> PlannerInput {
    let mut i = PlannerInput::default();
    i.workload.record_count = 1000;
    i.workload.growth_factor = 1.0;
    i.workload.skew_factor = 1.0;
    i.fragment_count = Some(4);
    i.policy.regions = vec!["west".into()];
    i.policy.replicas_per_region = 1;
    i.hosts = vec![Host {
        id: "a".into(),
        region: "west".into(),
        zone: None,
        profile: None,
        available: true,
    }];
    i
}

#[test]
fn resident_indexes_and_storage_encoding_have_separate_costs() {
    let mut i = input();
    i.workload.durable_storage = true;
    i.host_profile.storage_key_multiplier = 2.0;
    i.host_profile.storage_record_metadata_bytes = Some(128);
    let plain = recommend(&i).unwrap();
    i.host_profile.key_memory_multiplier = 8.0;
    let indexed = recommend(&i).unwrap();
    assert!(indexed.host_estimates[0].resident_bytes > plain.host_estimates[0].resident_bytes);
    assert!(
        (indexed.host_estimates[0].disk_bytes - plain.host_estimates[0].disk_bytes).abs() < 1e-6
    );
}

#[test]
fn committed_io_rate_can_limit_capacity_when_cpu_and_bandwidth_fit() {
    let mut i = input();
    i.workload.write_qps = 5000.0;
    i.workload.write_durability = WriteDurability::StableCommitted;
    i.host_profile.store_write_ops_per_second = Some(50_000.0);
    i.host_profile.stable_write_ops_per_second = Some(1000.0);
    let p = recommend(&i).unwrap();
    let h = &p.host_estimates[0];
    assert_eq!(h.committed_write_utilization, 5.0);
    assert!(h.cpu_utilization < 0.7 && h.disk_write_utilization < 0.7);
    assert_eq!(h.peak_utilization(), 5.0);
    assert!(!p.warnings.is_empty());
    i.workload.write_durability = WriteDurability::MemoryApplied;
    let p = recommend(&i).unwrap();
    assert_eq!(p.host_estimates[0].committed_write_utilization, 0.0);
}

#[test]
fn replication_batches_reduce_peer_fence_demand_but_not_local_fences() {
    let mut i = input();
    i.policy.replicas_per_region = 2;
    i.hosts.push(Host {
        id: "b".into(),
        region: "west".into(),
        zone: None,
        profile: None,
        available: true,
    });
    i.workload.write_qps = 1000.0;
    i.workload.write_durability = WriteDurability::StoreCommitted;
    i.host_profile.store_write_ops_per_second = Some(2000.0);
    let single = recommend(&i).unwrap();
    assert_eq!(
        single.host_estimates[0].committed_write_ops_per_second,
        1000.0
    );
    i.workload.replication_batch_operations = 10.0;
    let batched = recommend(&i).unwrap();
    assert_eq!(
        batched.host_estimates[0].committed_write_ops_per_second,
        550.0
    );
    assert_eq!(
        batched.traffic.replication_bytes_per_second,
        single.traffic.replication_bytes_per_second
    );
}

#[test]
fn backend_disk_reserve_is_charged_once_and_only_for_available_durable_nodes() {
    let mut i = input();
    i.workload.durable_storage = true;
    i.host_profile.disk_overhead_bytes = 1024 * 1024 * 1024;
    let many = recommend(&i).unwrap();
    i.fragment_count = Some(1);
    let one = recommend(&i).unwrap();
    assert!((many.host_estimates[0].disk_bytes - one.host_estimates[0].disk_bytes).abs() < 1e-6);
    i.workload.durable_storage = false;
    assert_eq!(recommend(&i).unwrap().host_estimates[0].disk_bytes, 0.0);
}

#[test]
fn invalid_calibration_rates_and_batch_sizes_are_rejected() {
    let mut i = input();
    i.host_profile.stable_write_ops_per_second = Some(0.0);
    assert!(recommend(&i).is_err());
    i.host_profile.stable_write_ops_per_second = Some(f64::NAN);
    assert!(recommend(&i).is_err());
    i.host_profile.stable_write_ops_per_second = None;
    i.workload.replication_batch_operations = 0.5;
    assert!(recommend(&i).is_err());
}

#[test]
fn measured_query_cost_changes_cpu_without_changing_network_estimates() {
    let mut i = input();
    i.workload.queries = vec![QueryWorkload {
        kind: QueryKind::Aggregate,
        qps: 10.0,
        records_examined: Some(100000),
        ..Default::default()
    }];
    let generic = recommend(&i).unwrap();
    i.workload.queries[0].examined_record_cpu_us = Some(0.3);
    let measured = recommend(&i).unwrap();
    assert!(measured.host_estimates[0].cpu_cores < generic.host_estimates[0].cpu_cores);
    assert_eq!(
        measured.traffic.application_bytes_per_second,
        generic.traffic.application_bytes_per_second
    );
    i.workload.queries[0].examined_record_cpu_us = Some(0.0);
    assert!(recommend(&i).is_err());
}
