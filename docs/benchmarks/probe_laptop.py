#!/usr/bin/env python3
"""Run isolated sizing cases sequentially; never overlap measured processes."""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/examples/sizing_probe"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.repeats <= 5:
        parser.error("repeats must be 1..5")
    cases = []
    for fragments in [1, 8, 32, 128, 256]:
        cases.append(dict(mode="memory", fragments=fragments, records=0))
    for tables in [4, 8]:
        cases.append(dict(mode="memory", fragments=32, records=0, tables=tables))
    for key_bytes, value_bytes, counts in [
        (32, 64, [10000, 100000]), (32, 1024, [10000, 100000]),
        (32, 16384, [4096, 16384]), (128, 1024, [10000, 100000]),
    ]:
        for count in counts:
            cases.append(dict(mode="memory", fragments=32, records=count, key_bytes=key_bytes, value_bytes=value_bytes))
    for concurrency in [1, 16]:
        cases.append(dict(mode="ops", fragments=32, records=20000, concurrency=concurrency))
    for fragments in [1, 32, 128]:
        cases.append(dict(mode="query", fragments=fragments, records=100000))
    for count in [20000, 100000]:
        cases.append(dict(mode="memory", backend="sync", fragments=32, records=count))
    for backend in ["sync", "async"]:
        cases.append(dict(mode="ops", backend=backend, fragments=32, records=20000, concurrency=16))
    for backend in ["none", "sync", "async"]:
        cases.append(dict(mode="ops", backend=backend, fragments=32, records=8192 if backend=="none" else 1000, value_bytes=16384, concurrency=16))
    if args.quick:
        cases = cases[:5] + cases[8:10] + cases[-2:]
    report = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "platform": platform.platform(), "architecture": platform.machine(),
        "logical_cpus": os.cpu_count(), "load_average_start": os.getloadavg(),
        "repeats": args.repeats, "measurements": [],
    }
    if args.resume and args.output.exists():
        report = json.loads(args.output.read_text())
    def signature(value):
        ops = value.get("phases", [])
        concurrency = value.get("concurrency", ops[0].get("concurrency", 8) if ops else 8) if value["mode"] == "ops" else 0
        return (value["mode"], value.get("backend", "none"), value["fragments"], value["records"],
                value.get("key_bytes", 32), value.get("value_bytes", 1024), value.get("resident_tables", value.get("tables", 1)), concurrency)
    completed = {(value["repeat"], signature(value)) for value in report["measurements"]}
    for repeat in range(args.repeats):
        for index, case in enumerate(cases):
            if (repeat, signature(case)) in completed:
                continue
            command = [str(args.binary), "--idle-ms", "500", "--duration-ms", "2000"]
            for name, value in case.items():
                command.extend(["--" + name.replace("_", "-"), str(value)])
            print(f"Repeat {repeat+1}, case {index+1}/{len(cases)}: {case}", flush=True)
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            if result.returncode:
                raise RuntimeError(result.stderr)
            measurement = json.loads(result.stdout)
            measurement["repeat"] = repeat
            report["measurements"].append(measurement)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    report["completed_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    report["load_average_end"] = os.getloadavg()
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
