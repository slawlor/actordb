#!/usr/bin/env python3
"""Fit laptop-specific sizing profiles; keep portable defaults unchanged."""
import argparse
import copy
import json
import math
from pathlib import Path
import statistics
import subprocess

MIB = 1024 * 1024


def least_squares(rows, values):
    width = len(rows[0])
    matrix = [[sum(row[i] * row[j] for row in rows) for j in range(width)]
              + [sum(row[i] * value for row, value in zip(rows, values))]
              for i in range(width)]
    for i in range(width):
        pivot = max(range(i, width), key=lambda j: abs(matrix[j][i]))
        matrix[i], matrix[pivot] = matrix[pivot], matrix[i]
        scale = matrix[i][i]
        if abs(scale) < 1e-12:
            raise ValueError("insufficient independent measurements")
        matrix[i] = [value / scale for value in matrix[i]]
        for j in range(width):
            if j != i:
                scale = matrix[j][i]
                matrix[j] = [a - scale * b for a, b in zip(matrix[j], matrix[i])]
    return [row[-1] for row in matrix]


def round_up(value, quantum):
    return math.ceil(value / quantum) * quantum


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--extra", type=Path, required=True)
    parser.add_argument("--rpc", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = json.loads(args.measurements.read_text())
    extra = json.loads(args.extra.read_text())
    rpc = json.loads(args.rpc.read_text())
    # Absence reads did not instantiate resident table worker pools. Retain
    # these raw samples for provenance but exclude them from the fit.
    discarded = [m for m in raw["measurements"] if m["mode"] == "memory" and m["resident_tables"] > 1
                 and not m.get("seeded_table_records", 0)]
    measurements = [m for m in raw["measurements"] if m not in discarded] + extra
    empty = [m for m in measurements if m["mode"] == "memory" and m["backend"] == "none" and m["records"] == 0]
    rows = [[1, m["fragments"], m["fragments"] * (m["resident_tables"] + 1)] for m in empty]
    node, fragment, table = least_squares(rows, [m["idle_rss_bytes"] / MIB for m in empty])
    data = [m for m in measurements if m["mode"] == "memory" and m["backend"] == "none" and m["records"] > 0]
    rows = [[1, m["records"] / 1e5 * m["value_bytes"] / 1024,
             m["records"] / 1e5 * m["key_bytes"] / 32, m["records"] / 1e5] for m in data]
    activation, value, key, metadata = least_squares(rows, [(m["loaded_rss_bytes"] - m["idle_rss_bytes"] - m["payload_bank_bytes"]) / MIB for m in data])
    value *= MIB / (1e5 * 1024)
    key *= MIB / (1e5 * 32)
    metadata *= MIB / 1e5
    template = json.loads(subprocess.check_output(["target/debug/actordb-plan", "example"], text=True))["host_profile"]
    embedded = copy.deepcopy(template)
    embedded.update(
        calibration="M4 Max local: 4 runtime threads, short KV/table names, 32-byte keys; 64 B–16 KiB values; CPU engine-only; see laptop-sizing report",
        cpu_cores=12.0, ram_bytes=32 * 1024**3, disk_bytes=100 * 1024**3,
        node_overhead_bytes=int(round_up(max(16, node + max(0, activation) + 4) * MIB, MIB)),
        fragment_overhead_bytes=int(round_up(max(32 * 1024, fragment * MIB * 1.1), 16 * 1024)),
        table_pool_overhead_bytes=int(round_up(max(32 * 1024, table * MIB * 1.1), 16 * 1024)),
        resident_data_multiplier=round_up(max(1.0, value) * 1.05, 0.05),
        key_memory_multiplier=round_up(max(1.0, key / max(value, 1e-9)), 0.5),
        record_metadata_bytes=int(round_up(max(128, metadata / max(value, 1e-9)), 64)),
        storage_key_multiplier=2.0, storage_record_metadata_bytes=128,
        disk_overhead_bytes=0, store_write_ops_per_second=None, stable_write_ops_per_second=None,
    )
    phases = [(m, p) for m in measurements if m["mode"] == "ops" and m["backend"] == "none"
              for p in m["phases"] if not p.get("rejected", 0)]
    for kind, name in [("read", "point_read_cpu_us"), ("write", "write_cpu_us")]:
        embedded[name] = round_up(max(p["cpu_us_per_operation"] for _, p in phases if p["kind"] == kind) * 1.25, 5)
    # Empty-page cost is independent of examined-row CPU. Use the maximum
    # observed marginal cost across count, range and scan shapes.
    query_empty = [m for m in extra if m["mode"] == "query"]
    slopes = []
    empty_costs = {}
    for kind in ["count", "range", "scan"]:
        fixed, slope = least_squares([[1, m["fragments"]] for m in query_empty],
                                    [next(p for p in m["phases"] if p["kind"] == kind)["cpu_us_per_query"] for m in query_empty])
        slopes.append(slope)
        empty_costs[kind] = (fixed, slope)
    record_costs = {}
    for kind in ["count", "range", "scan"]:
        samples = []
        for m in measurements:
            if m["mode"] != "query" or m["records"] == 0:
                continue
            phase = next(p for p in m["phases"] if p["kind"] == kind)
            fixed, slope = empty_costs[kind]
            samples.append(max(0, phase["cpu_us_per_query"] - fixed - slope * m["fragments"]) / phase["records_examined"])
        record_costs[kind] = max(samples)
    embedded["query_fragment_cpu_us"] = round_up(max(slopes) * 1.25, 5)
    embedded["examined_record_cpu_us"] = round_up(max(record_costs.values()) * 1.25, 0.5)
    profiles = {"m4-max-embedded": embedded}
    frontend = copy.deepcopy(embedded)
    frontend["calibration"] = "M4 Max plain loopback gRPC CPU upper bound: client+server combined, 4 threads, 32-byte keys, 1–16 KiB values; NIC/WAN unmeasured"
    for kind, name in [("read", "point_read_cpu_us"), ("write", "write_cpu_us")]:
        frontend[name] = round_up(max(p["combined_cpu_us_per_operation"] for m in rpc for p in m["phases"] if p["kind"] == kind) * 1.10, 5)
    profiles["m4-max-grpc"] = frontend
    storage_rates = {}
    for backend in ["sync", "async"]:
        for size in [1024, 16384]:
            candidates = [m for m in measurements if m["mode"] == "ops" and m["backend"] == backend and m["value_bytes"] == size]
            profile = copy.deepcopy(embedded)
            profile["calibration"] = f"M4 Max RocksDB 11.8.1 {backend} WAL; {size}-byte values; 4 storage jobs/threads, 16 disjoint callers; local SSD API fences, not power-loss validation"
            rates = {}
            for kind, name in [("store_write", "store_write_ops_per_second"), ("stable_write", "stable_write_ops_per_second")]:
                usable = [p for m in candidates for p in m["phases"] if p["kind"] == kind and not p.get("rejected", 0)]
                profile[name] = math.floor(min(p["operations_per_second"] for p in usable) / 1000) * 1000
                rates[kind] = [p["operations_per_second"] for p in usable]
            profile["node_overhead_bytes"] += 320 * MIB  # 2 CF × (2 × 64 MiB memtables + 32 MiB cache)
            profile["disk_overhead_bytes"] = 1024**3  # default WAL-pressure threshold, not a hard maximum
            immediate_cpu = max(p["cpu_us_per_operation"] for m in candidates for p in m["phases"] if p["kind"] in ["store_write", "stable_write"] and not p.get("rejected", 0))
            profile["stable_write_cpu_us"] = round_up(max(0, immediate_cpu - profile["write_cpu_us"]) * 1.15, 5)
            bulk = [m for m in measurements if m["mode"] == "memory" and m["backend"] == "sync" and m["records"] > 0]
            # Effective model bandwidth from unique-record durable ingestion;
            # this is not advertised SSD bandwidth or measured device writes.
            profile["disk_write_bytes_per_second"] = min(m["records"] * (m["key_bytes"] + m["value_bytes"]) / m["load_seconds"] for m in bulk) * profile["disk_data_multiplier"]
            profiles[f"m4-max-rocksdb-{backend}-{size}"] = profile
            combined = copy.deepcopy(profile)
            combined["point_read_cpu_us"] = frontend["point_read_cpu_us"]
            combined["write_cpu_us"] = frontend["write_cpu_us"]
            combined["calibration"] += "; gRPC CPU upper bound composed from separate loopback probe, not a combined durable-RPC run"
            profiles[f"m4-max-grpc-rocksdb-{backend}-{size}"] = combined
            storage_rates[f"{backend}-{size}"] = rates
    args.output.mkdir(parents=True, exist_ok=True)
    for name, profile in profiles.items():
        (args.output / (name + ".json")).write_text(json.dumps(profile, indent=2) + "\n")
    summary = {"raw_fit": {"node_mib": node, "fragment_mib": fragment, "table_pool_mib": table,
                           "data_activation_mib": activation, "value_multiplier": value, "key_multiplier": key,
                           "metadata_bytes_per_record": metadata},
               "query_record_cpu_us": record_costs, "storage_completion_rates": storage_rates,
               "discarded_table_samples": len(discarded), "profiles": profiles}
    (args.output / "fit-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
