#!/usr/bin/env python3
"""Collect corrected table/empty-query, RPC, or independent holdout cases."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=["extra", "rpc", "holdout"])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 5:
        parser.error("repeats must be 1..5")
    if args.kind == "extra":
        binary = "target/release/examples/sizing_probe"
        cases = [dict(mode="memory", fragments=32, tables=t, records=0) for t in [4, 8]]
        cases += [dict(mode="query", fragments=f, records=0, query_samples=50) for f in [1, 32, 128]]
    elif args.kind == "rpc":
        binary = "target/release/examples/rpc_sizing_probe"
        cases = [dict(value_bytes=size, concurrency=c) for size in [1024, 16384] for c in [1, 16]]
    else:
        binary = "target/release/examples/sizing_probe"
        cases = [dict(mode="memory", fragments=32, records=200000, key_bytes=k, value_bytes=1024) for k in [32, 128]]
    values = []
    for repeat in range(args.repeats):
        for case in cases:
            print(f"{args.kind}: repeat {repeat+1}: {case}", flush=True)
            command = [binary]
            for name, value in case.items():
                command.extend(["--" + name.replace("_", "-"), str(value)])
            if args.kind != "rpc":
                command.extend(["--idle-ms", "500"])
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            if result.returncode:
                raise RuntimeError(result.stderr)
            value = json.loads(result.stdout)
            value["repeat"] = repeat
            values.append(value)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(values, indent=2) + "\n")


if __name__ == "__main__":
    main()
