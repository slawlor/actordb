use actordb_placement::{evaluate, recommend, FragmentMap, Plan, PlannerInput};
use serde::Serialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::Read;

const USAGE: &str = "Usage:
  actordb-plan example
  actordb-plan recommend INPUT.json [--profile PROFILE.json] [--json | --toml]
  actordb-plan evaluate INPUT.json MAP_OR_PLAN.json [--profile PROFILE.json] [--json | --toml]

Without a format flag, prints a readable capacity and placement report.
--toml emits NodeConfig fragment_count and [fragment_map] only.
--json emits the complete report, including generated hosts and assumptions.
--profile replaces the input's default host profile; per-host overrides remain.
The tool recommends offline; it never connects to or changes a database.";

fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, Box<dyn Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(16 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err("input exceeds 16 MiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args == ["--help"] || args == ["-h"] {
        println!("{USAGE}");
        return Ok(());
    }
    if args == ["example"] {
        let mut input = PlannerInput::default();
        input.workload.record_count = 1_000_000;
        input.workload.point_read_qps = 10_000.0;
        input.workload.write_qps = 1_000.0;
        input.policy.regions = vec!["east".into(), "west".into()];
        println!("{}", serde_json::to_string_pretty(&input)?);
        return Ok(());
    }
    let positional = match args[0].as_str() {
        "recommend" => 2,
        "evaluate" => 3,
        _ => return Err(USAGE.into()),
    };
    if args.len() < positional {
        return Err(USAGE.into());
    }
    let mut format = "text";
    let mut profile = None;
    let mut flags = args[positional..].iter();
    while let Some(flag) = flags.next() {
        match flag.as_str() {
            "--json" | "--toml" if format == "text" => format = flag.as_str(),
            "--profile" if profile.is_none() => profile = Some(flags.next().ok_or(USAGE)?),
            _ => return Err(USAGE.into()),
        }
    }
    let mut input: PlannerInput = read_json(&args[1])?;
    if let Some(profile) = profile {
        input.host_profile = read_json(profile)?;
    }
    let plan = if args[0] == "recommend" {
        recommend(&input)?
    } else {
        // Inspect the JSON object explicitly: serde's untagged intermediate
        // representation loses JSON's numeric-key parsing for u32 fragmaps.
        let document: serde_json::Value = read_json(&args[2])?;
        let map: FragmentMap = if document.get("fragment_map").is_some() {
            serde_json::from_value::<Plan>(document)?.fragment_map
        } else {
            serde_json::from_value(document)?
        };
        evaluate(&input, &map)?
    };
    match format {
        "--json" => println!("{}", serde_json::to_string_pretty(&plan)?),
        "--toml" => {
            #[derive(Serialize)]
            struct NodeExport {
                fragment_count: u32,
                fragment_map: BTreeMap<String, Vec<String>>,
            }
            let export = NodeExport {
                fragment_count: plan.fragment_count,
                fragment_map: plan
                    .fragment_map
                    .iter()
                    .map(|(id, replicas)| (id.to_string(), replicas.clone()))
                    .collect(),
            };
            print!("{}", toml::to_string(&export)?);
            // Diagnostics remain visible when stdout is redirected to config.
            for warning in &plan.warnings {
                eprintln!("Warning: {warning}");
            }
        }
        _ => report(&plan),
    }
    Ok(())
}

fn report(plan: &Plan) {
    println!(
        "{} fragments; {} requested replicas per fragment; {} hosts used; {} changed fragments",
        plan.fragment_count,
        plan.replica_count,
        plan.hosts_per_region.values().sum::<usize>(),
        plan.moved_fragments
    );
    println!("Hosts per region: {:?}", plan.hosts_per_region);
    println!("\nHOST                         FRAGS/LIMIT  ACTORS SENDERS  RAM GiB CPU CORES NET MiB/s PEAK");
    for e in &plan.host_estimates {
        println!(
            "{:<28} {:>5}/{:<5} {:>6} {:>7} {:>8.2} {:>9.2} {:>9.2} {:>5.0}%",
            e.id,
            e.fragments,
            e.recommended_fragment_limit,
            e.actors,
            e.sender_tasks,
            e.resident_bytes / (1024.0 * 1024.0 * 1024.0),
            e.cpu_cores,
            e.network_bytes_per_second / (1024.0 * 1024.0),
            e.peak_utilization() * 100.0
        );
    }
    println!("\nTraffic MiB/s: application {:.2}; query/coordinator {:.2}; replication {:.2}; recovery allowance {:.2}",
        plan.traffic.application_bytes_per_second/1_048_576.0,
        plan.traffic.query_bytes_per_second/1_048_576.0,
        plan.traffic.replication_bytes_per_second/1_048_576.0,
        plan.traffic.recovery_bytes_per_second/1_048_576.0);
    println!(
        "Cross-region MiB/s: application {:.2}; query {:.2}; replication {:.2}",
        plan.traffic.cross_region_application_bytes_per_second / 1_048_576.0,
        plan.traffic.cross_region_query_bytes_per_second / 1_048_576.0,
        plan.traffic.cross_region_replication_bytes_per_second / 1_048_576.0
    );
    println!("Provisioned copies survive one host loss: {}; one region loss: {}; regional coverage after host loss: {}",
        plan.reliability.survives_one_host_loss,plan.reliability.survives_one_region_loss,
        plan.reliability.regional_coverage_survives_one_host_loss);
    println!("\nFragment map:");
    for (id, replicas) in &plan.fragment_map {
        println!("  {id}: {}", replicas.join(", "));
    }
    if !plan.coverage_gaps.is_empty() {
        println!("\nCoverage gaps:");
        for gap in &plan.coverage_gaps {
            println!(
                "  fragment {} / {}: {} of {} requested copies",
                gap.fragment, gap.region, gap.available_replicas, gap.desired_replicas
            );
        }
    }
    println!("\nAssumptions:");
    for assumption in &plan.assumptions {
        println!("  {assumption}");
    }
    for warning in &plan.warnings {
        println!("Warning: {warning}");
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}
