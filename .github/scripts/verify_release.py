#!/usr/bin/env python3
"""Verify backend opt-ins, resolve the crate version, and validate release tags."""

import argparse
import json
import os
import re
import subprocess
import sys


VERSION_PATTERN = re.compile(
    r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)

# The workflow publishes these libraries together, in dependency order. Internal
# workspace utilities must explicitly opt out of registry publication.
PUBLISHED_PACKAGES = ("actordb-protocol", "actordb-query", "actordb", "actordb-client")


class ReleaseError(Exception):
    """A verification failure safe to print without command output or secrets."""


def run_command(command, cwd=None):
    try:
        return subprocess.run(
            command, cwd=cwd, capture_output=True, text=True, check=False
        )
    except (OSError, UnicodeError) as error:
        raise ReleaseError("could not run the required verification command") from error


def verify_backends(package):
    features = package.get("features")
    if not isinstance(features, dict) or not all(
        isinstance(entries, list) and all(isinstance(entry, str) for entry in entries)
        for entries in features.values()
    ):
        raise ReleaseError("actordb had an invalid feature list")
    if features.get("default") != ["grpc"] or "grpc" not in features:
        raise ReleaseError("actordb default features must only enable grpc")
    pending, seen = ["grpc"], set()
    while pending:
        feature = pending.pop()
        if feature in seen:
            continue
        seen.add(feature)
        if feature in {"memory", "rocksdb", "dep:rocksdb"}:
            raise ReleaseError("default RPC features must not enable storage backends")
        pending.extend(entry for entry in features.get(feature, []) if entry in features or entry == "dep:rocksdb")
    if features.get("memory") != []:
        raise ReleaseError("actordb must provide a dependency-free memory opt-in")
    if features.get("rocksdb") != ["dep:rocksdb"]:
        raise ReleaseError("actordb rocksdb opt-in must enable only dep:rocksdb")

    dependencies = package.get("dependencies")
    if not isinstance(dependencies, list) or not all(
        isinstance(dependency, dict) for dependency in dependencies
    ):
        raise ReleaseError("actordb had an invalid dependency list")
    rocksdb = [
        dependency
        for dependency in dependencies
        if dependency.get("name") == "rocksdb" and dependency.get("kind") is None
    ]
    if len(rocksdb) != 1:
        raise ReleaseError("actordb must have one optional RocksDB runtime dependency")
    dependency = rocksdb[0]
    source = dependency.get("source")
    if dependency.get("optional") is not True or not (
        isinstance(source, str) and source.startswith("registry+")
    ):
        raise ReleaseError(
            "actordb RocksDB runtime dependency must be optional and registry-based"
        )


def verify_publication_packages(packages, members, version):
    for name in PUBLISHED_PACKAGES:
        matches = [package for package in packages if package.get("name") == name]
        if len(matches) != 1 or matches[0].get("id") not in members:
            raise ReleaseError("expected exactly one of each published library in the workspace")
        package = matches[0]
        if package.get("version") != version:
            raise ReleaseError("published libraries must share the actordb version")
        publish = package.get("publish")
        if publish is not None and not (
            isinstance(publish, list)
            and all(isinstance(registry, str) for registry in publish)
            and "crates-io" in publish
        ):
            raise ReleaseError("published libraries must permit the crates-io registry")

    for member in members:
        matches = [package for package in packages if package.get("id") == member]
        if len(matches) != 1:
            raise ReleaseError("workspace member metadata must describe one package")
        package = matches[0]
        if package.get("name") not in PUBLISHED_PACKAGES and package.get("publish") != []:
            raise ReleaseError("nonlibrary workspace packages must explicitly disable publication")


def resolve_version(cwd=None):
    result = run_command(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd
    )
    if result.returncode != 0:
        raise ReleaseError("Cargo metadata failed")
    try:
        metadata = json.loads(result.stdout)
    except (ValueError, TypeError) as error:
        raise ReleaseError("Cargo metadata was not valid JSON") from error
    if not isinstance(metadata, dict):
        raise ReleaseError("Cargo metadata did not describe a workspace")
    packages = metadata.get("packages")
    members = metadata.get("workspace_members")
    if (
        not isinstance(packages, list)
        or not all(isinstance(package, dict) for package in packages)
        or not isinstance(members, list)
        or not all(isinstance(member, str) for member in members)
    ):
        raise ReleaseError("Cargo metadata had an invalid package list")
    matches = [package for package in packages if package.get("name") == "actordb"]
    if len(matches) != 1 or matches[0].get("id") not in members:
        raise ReleaseError("expected exactly one actordb workspace package")
    version = matches[0].get("version")
    if not isinstance(version, str) or VERSION_PATTERN.fullmatch(version) is None:
        raise ReleaseError("actordb had an invalid package version")
    # Cargo's SemVer parser rejects leading zeros in numeric prerelease ids.
    prerelease = version.split("+", 1)[0].partition("-")[2]
    if any(
        part.isdigit() and len(part) > 1 and part.startswith("0")
        for part in prerelease.split(".")
    ):
        raise ReleaseError("actordb had an invalid package version")
    verify_backends(matches[0])
    verify_publication_packages(packages, members, version)
    return version


def verify_tag(tag, version, cwd=None):
    if tag != f"v{version}":
        raise ReleaseError("release tag must exactly match v plus the actordb version")
    if re.fullmatch(
        r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version
    ) is None:
        raise ReleaseError("release version must be stable numeric x.y.z")

    def verified_commit(reference):
        result = run_command(["git", "rev-parse", "--verify", reference], cwd)
        commit = result.stdout.strip()
        if result.returncode != 0 or re.fullmatch(
            r"[0-9a-f]{40}|[0-9a-f]{64}", commit
        ) is None:
            raise ReleaseError("could not verify release HEAD and tag commits")
        return commit

    head = verified_commit("HEAD")
    tagged = verified_commit(f"refs/tags/{tag}^{{commit}}")
    if head != tagged:
        raise ReleaseError("release tag must point to the checked-out HEAD commit")
    result = run_command(
        ["git", "merge-base", "--is-ancestor", head, "refs/remotes/origin/main"], cwd
    )
    if result.returncode != 0:
        raise ReleaseError("release HEAD must be reachable from the fetched origin/main")


def emit_version(version):
    line = f"version={version}\n"
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path is not None:
        try:
            with open(output_path, "a", encoding="utf-8") as output:
                output.write(line)
        except (OSError, UnicodeError, ValueError) as error:
            raise ReleaseError("could not write the GitHub version output") from error
    sys.stdout.write(line)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag", help="release tag; also requires HEAD to be on origin/main"
    )
    args = parser.parse_args(argv)
    try:
        version = resolve_version()
        if args.tag is not None:
            verify_tag(args.tag, version)
        emit_version(version)
    except ReleaseError as error:
        print(f"release verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
