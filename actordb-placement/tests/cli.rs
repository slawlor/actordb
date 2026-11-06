#![cfg(feature = "cli")]

use actordb_placement::{Plan, PlannerInput};
use std::process::Command;

#[test]
fn cli_recommend_evaluate_and_toml_exports_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("workload.json");
    let plan_path = directory.path().join("plan.json");
    let mut input = PlannerInput::default();
    input.workload.record_count = 1000;
    std::fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();
    let run = |command: &str, path: Option<&std::path::Path>, format: &str| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_actordb-plan"));
        process.arg(command).arg(&input_path);
        if let Some(path) = path {
            process.arg(path);
        }
        process.arg(format).output().unwrap()
    };
    let json = run("recommend", None, "--json");
    assert!(json.status.success());
    let plan: Plan = serde_json::from_slice(&json.stdout).unwrap();
    std::fs::write(&plan_path, &json.stdout).unwrap();
    input.hosts = plan.hosts.clone();
    std::fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();
    let evaluated = run("evaluate", Some(&plan_path), "--json");
    assert!(
        evaluated.status.success(),
        "{}",
        String::from_utf8_lossy(&evaluated.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Plan>(&evaluated.stdout)
            .unwrap()
            .fragment_map,
        plan.fragment_map
    );
    let toml = run("evaluate", Some(&plan_path), "--toml");
    assert!(toml.status.success());
    let document = toml::from_str::<toml::Value>(&String::from_utf8(toml.stdout).unwrap()).unwrap();
    assert_eq!(
        document["fragment_count"].as_integer(),
        Some(plan.fragment_count as i64)
    );
    let map = document["fragment_map"].as_table().unwrap();
    assert_eq!(map.len(), plan.fragment_count as usize);
    assert!(map.contains_key("0"));
    std::fs::write(&input_path, b"{\"worklod\": {}}").unwrap();
    assert!(
        !run("recommend", None, "--json").status.success(),
        "typos must not silently become default workload"
    );
}

#[test]
fn cli_can_load_a_calibrated_profile_without_rewriting_the_workload() {
    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("input.json");
    let profile = directory.path().join("profile.json");
    std::fs::write(&input, b"{\"workload\":{\"record_count\":1000}}").unwrap();
    std::fs::write(
        &profile,
        b"{\"calibration\":\"test-profile\",\"key_memory_multiplier\":8.0}",
    )
    .unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_actordb-plan"))
        .arg("recommend")
        .arg(&input)
        .arg("--profile")
        .arg(&profile)
        .arg("--json")
        .output()
        .unwrap();
    assert!(result.status.success());
    let plan: Plan = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(plan.host_profile.key_memory_multiplier, 8.0);
    assert_eq!(plan.host_profile.calibration, "test-profile");
    let missing = Command::new(env!("CARGO_BIN_EXE_actordb-plan"))
        .arg("recommend")
        .arg(&input)
        .arg("--profile")
        .output()
        .unwrap();
    assert!(!missing.status.success());
}
