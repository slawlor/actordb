#!/usr/bin/env python3
"""Wait until a direct workspace dependency's exact version reaches crates.io."""

import argparse
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

MAX_INDEX_BYTES = 4 * 1024 * 1024
MAX_REQUEST_SECONDS = 30


class WaitError(Exception):
    """An actionable failure, without credentials or response bodies."""


def crate_index_url(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise WaitError("invalid crate name")
    name = name.lower()
    if len(name) <= 2:
        path = f"{len(name)}/{name}"
    elif len(name) == 3:
        path = f"3/{name[0]}/{name}"
    else:
        path = f"{name[:2]}/{name[2:4]}/{name}"
    return f"https://index.crates.io/{path}"


def dependency_version(metadata, dependent, target):
    if not isinstance(metadata, dict):
        raise WaitError("Cargo metadata must describe a workspace")
    packages, members = metadata.get("packages"), metadata.get("workspace_members")
    if not isinstance(packages, list) or not isinstance(members, list):
        raise WaitError("Cargo metadata has no workspace package list")

    def package(name):
        matches = [p for p in packages if isinstance(p, dict) and p.get("name") == name and p.get("id") in members]
        if len(matches) != 1:
            raise WaitError(f"expected one workspace package named {name}")
        return matches[0]

    source, destination = package(dependent), package(target)
    manifest = destination.get("manifest_path")
    version = destination.get("version")
    if not isinstance(manifest, str) or not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.+-]+)?", version):
        raise WaitError("target package has invalid manifest/version metadata")
    dependencies = source.get("dependencies")
    if not isinstance(dependencies, list) or not all(isinstance(d, dict) for d in dependencies):
        raise WaitError("dependent package has invalid dependency metadata")
    target_path = Path(manifest).resolve().parent
    if not any(isinstance(d, dict) and d.get("name") == target and d.get("kind") in (None, "dev", "build")
               and d.get("source") is None and isinstance(d.get("path"), str)
               and Path(d["path"]).resolve() == target_path for d in dependencies):
        raise WaitError(f"{target} is not a direct local dependency of {dependent}")
    return version  # The manifest's exact version; never its compatible req range.


def load_metadata():
    try:
        result = subprocess.run(["cargo", "metadata", "--format-version", "1", "--no-deps"], capture_output=True, text=True, timeout=30, check=False)
        if result.returncode:
            raise WaitError("Cargo metadata failed")
        return json.loads(result.stdout)
    except (OSError, UnicodeError, ValueError, subprocess.TimeoutExpired) as error:
        raise WaitError("could not read Cargo workspace metadata") from error


def fetch_index(url, timeout):
    deadline, body = time.monotonic() + timeout, bytearray()
    with urlopen(url, timeout=timeout) as response:
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError("index request deadline elapsed")
            # read1 performs at most one underlying read; timely slow-drip
            # traffic cannot indefinitely extend the checked request budget.
            chunk = response.read1(min(65536, MAX_INDEX_BYTES + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
            if len(body) > MAX_INDEX_BYTES:
                raise ValueError("index response exceeds byte limit")
    return bytes(body)


def index_ready(body, name, version):
    if not isinstance(body, bytes) or len(body) > MAX_INDEX_BYTES:
        raise ValueError("invalid index response size")
    entries = [json.loads(line) for line in body.decode("utf-8").splitlines() if line.strip()]
    if not all(isinstance(entry, dict) for entry in entries):
        raise ValueError("malformed index entries")
    return any(entry.get("name") == name and entry.get("vers") == version and entry.get("yanked") is False for entry in entries)


def wait_until_ready(name, version, timeout=900, interval=5, *, fetch=fetch_index,
                     clock=time.monotonic, sleep=time.sleep, report=print):
    if not all(math.isfinite(v) and v > 0 for v in (timeout, interval)):
        raise WaitError("timeout and interval must be positive finite seconds")
    url = crate_index_url(name)
    deadline = clock() + timeout
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise WaitError(f"timed out waiting for {name} {version} in the crates.io index")
        ready = False
        try:
            ready = index_ready(fetch(url, min(MAX_REQUEST_SECONDS, remaining)), name, version)
        except HTTPError as error:
            if error.code not in (404, 429) and not 500 <= error.code <= 599:
                raise WaitError(f"crates.io index request failed with HTTP {error.code}") from error
        except (URLError, OSError, ValueError, UnicodeError, RecursionError):
            pass  # Publication propagation and temporary index/transport failures retry.
        if clock() >= deadline:
            raise WaitError(f"timed out waiting for {name} {version} in the crates.io index")
        if ready:
            report(f"Ready: {name} {version} is indexed and not yanked")
            return
        report(f"Waiting for {name} {version}; retrying index lookup")
        remaining = deadline - clock()
        if remaining <= 0:
            raise WaitError(f"timed out waiting for {name} {version} in the crates.io index")
        sleep(min(interval, remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dependent")
    parser.add_argument("target")
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args(argv)
    try:
        version = dependency_version(load_metadata(), args.dependent, args.target)
        wait_until_ready(args.target, version, args.timeout, args.interval)
    except WaitError as error:
        print(f"Dependency readiness failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
