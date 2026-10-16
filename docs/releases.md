# Releasing actordb

The `Publish` workflow validates packages on pull requests and `main` pushes.
It uploads to crates.io only when a **stable, nondraft GitHub release is
published**. Merging a version PR, pushing a tag, or saving a draft release
does not upload anything. GitHub prereleases are intentionally excluded.

## Prepare a version PR

1. Bump `actordb/Cargo.toml` and regenerate the matching `actordb` entry in
   `Cargo.lock`, without unrelated dependency updates. The internal `xtask`
   and `docker-tests` crates are not published and need not share this version.
2. Update `CHANGELOG.md`, installation examples, and any migration/API docs.
   Pre-1.0 breaking changes use a new minor version; do not promise production
   readiness or compatibility that the implementation does not enforce.
3. Commit the changes, then run the release checks from the repository root:

   ```bash
   python3 -m unittest discover -s .github/scripts -p 'test_verify_release.py' -v
   python3 .github/scripts/verify_release.py
   cargo publish --dry-run --locked --package actordb --registry crates-io
   cargo publish --dry-run --locked --package actordb --registry crates-io --features memory
   cargo publish --dry-run --locked --package actordb --registry crates-io --features rocksdb
   cargo package --locked --package actordb
   ```

   These commands do not publish and need no registry token. Cargo verifies the
   unpacked crate, including the bundled protobuf schema/build script, library
   modules, README, and license. Native protobuf compilation needs a C/C++
   toolchain; opt-in RocksDB support additionally needs working libclang.
   Inspect `cargo package --list --locked --package actordb` when changing
   source layout. Keep the bundled `actordb/LICENSE` in sync with root `LICENSE`.
4. Wait for all PR checks and reviews, including backend-free, memory-only,
   and full-feature tests, Rust 1.88, Docker, and the `Verify release package`
   check. Merge into `main`.

The package workflow uploads an `actordb-VERSION` build artifact containing the
verified `.crate` for inspection. An explicit, verified `cargo package` produces
the artifact in Cargo's normal package directory: recent Cargo versions stage
publish dry-runs in a temporary registry instead. No internal staging path is
assumed. The artifact is not a crates.io release.

Package verification uses a separate `stable-release-package` dependency cache:
an unpacked crate does not unify the workspace's dev/test features, so its
native dependency artifacts can differ from workspace CI. The first compatible
package cache is cold; later reuse depends on toolchain, dependency, and cache
availability. All three package configurations still run; no fixed build speed
is promised. The normal artifact and actual publish use the empty default
feature set. The archived source contains optional backends, but downstream
default builds do not enable or link them.

## Credentials

Configure the repository Actions secret `CARGO_REGISTRY_TOKEN` with a valid
crates.io API token authorized to publish `actordb`. This repository already
has a secret by that name; its existence does not prove that an old token is
still valid. Prefer a narrowly scoped, expiring token and rotate it as needed.
The workflow does not use the unrelated GitHub PAT or the old
`crates_io_token` name. No `cargo login` is performed, and the token is passed
only to the actual publication step, never PR checks, package dry-runs, tests,
or artifact uploads. Do not print or commit token values.

## Publish the release

For this version, after the version PR is merged and `main` checks are green:

1. Fetch `main`, select the tested release commit, and create the tag `v0.2.0`
   at that commit. Push that tag, or select that exact commit when creating it
   in GitHub's release UI. Do not tag an unmerged PR branch.
2. Create a GitHub release for `v0.2.0` using `CHANGELOG.md` as the notes. Keep
   it a draft until ready; leave **Set as a pre-release** unchecked.
3. Publish the GitHub release. The workflow checks out the event's exact commit
   with full history. Before building or uploading, the release guard requires
   the tag to be exactly `v` plus the manifest version, a stable `MAJOR.MINOR.PATCH`
   version, the peeled tag commit to equal checkout `HEAD`, and that commit to
   be contained in `origin/main`. An older tested ancestor is allowed; an
   unmerged branch, missing/moved tag, or version mismatch fails closed.
4. Backend-free, memory-only, and RocksDB-only package dry-runs and crate tests
   must pass before the workflow publishes **only `actordb`** to the explicit `crates-io`
   registry with locked dependencies. Uploads are serialized and an in-flight
   publication is never cancelled by a newer validation run.
5. Check the Actions result and verify `0.2.0` on
   [crates.io](https://crates.io/crates/actordb) and its build on
   [docs.rs](https://docs.rs/actordb). GitHub release publication precedes the
   registry upload, so a failed workflow can leave a GitHub release without a
   corresponding crates.io version.

There is deliberately no manual-dispatch upload path. Fix a failed check or
credential and rerun the release job only after confirming the version was
not already uploaded. Crates.io versions are immutable: an already-published
version must not be overwritten, and a duplicate publish fails rather than
being treated as success. If upload succeeded but a later status check failed,
verify the registry before retrying; use a new version for content changes.
