# Releasing actordb

The `Publish` workflow validates packages on pull requests and `main` pushes.
It uploads to crates.io only when a **stable, nondraft GitHub release is
published**. Merging a version PR, pushing a tag, or saving a draft release
does not upload anything. GitHub prereleases are intentionally excluded.

## Prepare a version PR

1. Bump all four library manifests (`actordb`, `actordb-protocol`,
   `actordb-query`, and `actordb-client`) together and update their version
   requirements. The current workflow publishes all four selected packages;
   every selected version must be new because crates.io versions are immutable.
   Dependency requirements remain compatible semver
   ranges; this library workspace does not commit `Cargo.lock`. The internal `xtask`
   and `docker-tests` crates are not published and need not share this version.
   Feature PRs included in the same unpublished release share that release
   version. Keep their release notes under one changelog entry; separate PRs
   do not require separate version bumps. Bump the version again after publishing
   the preceding release.
2. Update `CHANGELOG.md`, installation examples, and any migration/API docs.
   Pre-1.0 breaking changes use a new minor version; do not promise production
   readiness or compatibility that the implementation does not enforce.
3. Commit the changes, then run the release checks from the repository root:

   ```bash
   python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v
   python3 .github/scripts/verify_release.py
   cargo publish --dry-run --package actordb-protocol --package actordb-query --package actordb --package actordb-client --registry crates-io
   cargo publish --dry-run --package actordb-protocol --package actordb-query --package actordb --package actordb-client --registry crates-io --features actordb/memory
   cargo package --package actordb-protocol --package actordb-query --package actordb --package actordb-client
   ```

   These commands do not publish and need no registry token. Cargo verifies the
   unpacked libraries, including generated protocol bindings, query modules,
   SDK, README files, and licenses. Ordinary builds use checked-in protocol
   bindings. Regeneration with the protocol codegen feature needs a C/C++
   toolchain. RocksDB builds and tests remain in normal CI, not the publish
   workflow; that CI additionally needs working libclang.
   Inspect `cargo package --list --package actordb` when changing
   source layout. Keep the bundled `actordb/LICENSE` in sync with root `LICENSE`.
4. Wait for all PR checks and reviews, including backend-free, memory-only,
   and full-feature tests, Rust 1.88, Docker, and the `Verify release package`
   check. Merge into `main`.

The package workflow uploads an `actordb-workspace-VERSION` artifact containing
the verified library `.crate` files for inspection. An explicit, verified `cargo package` produces
the artifact in Cargo's normal package directory: recent Cargo versions stage
publish dry-runs in a temporary registry instead. No internal staging path is
assumed. The artifact is not a crates.io release.

Fresh checkouts resolve current compatible dependencies instead of a committed
lockfile. The workspace resolver prefers versions compatible with the declared
Rust minimum; undeclared or inaccurate dependency MSRVs can still break a build.
Cargo may create a local, ignored `Cargo.lock` and include a generated lockfile
in the `.crate` archive; neither is a repository dependency pin. Repeated
commands can reuse that local resolution. Run `cargo update` to refresh it,
and rerun validation before releasing. CI and release commands do not require
a preexisting lockfile or enforce a frozen dependency graph.

The workspace resolver configuration is not shipped as a downstream Cargo
policy. Consumers choose their own resolver and lockfile behavior; the crate's
declared Rust minimum and semver dependency requirements remain its contract.

Package verification uses a separate `stable-release-package` dependency cache:
an unpacked crate does not unify the workspace's dev/test features, so its
native dependency artifacts can differ from workspace CI. The first compatible
package cache is cold; later reuse depends on toolchain, dependency, and cache
availability. Backend-free and memory-only package configurations run; native
RocksDB validation is handled by normal CI. No fixed build speed is promised.
The normal artifact and actual publish use the empty default
feature set. The archived source contains optional backends, but downstream
default builds do not enable or link them.

## Credentials

Configure the repository Actions secret `CARGO_REGISTRY_TOKEN` with a valid
crates.io API token authorized to publish the ActorDB library packages. This repository already
has a secret by that name; its existence does not prove that an old token is
still valid. Prefer a narrowly scoped, expiring token and rotate it as needed.
The workflow does not use the unrelated GitHub PAT or the old
`crates_io_token` name. No `cargo login` is performed, and the token is passed
only to the actual publication step, never PR checks, package dry-runs, tests,
or artifact uploads. Do not print or commit token values.

## Publish the release

For this version, after the version PR is merged and `main` checks are green:

1. Fetch `main`, select the tested release commit, and create the tag `v0.5.0`
   at that commit. Push that tag, or select that exact commit when creating it
   in GitHub's release UI. Do not tag an unmerged PR branch.
2. Create a GitHub release for `v0.5.0` using `CHANGELOG.md` as the notes. Keep
   it a draft until ready; leave **Set as a pre-release** unchecked.
3. Publish the GitHub release. The workflow checks out the event's exact commit
   with full history. Before building or uploading, the release guard requires
   the tag to be exactly `v` plus the manifest version, a stable `MAJOR.MINOR.PATCH`
   version, the peeled tag commit to equal checkout `HEAD`, and that commit to
   be contained in `origin/main`. An older tested ancestor is allowed; an
   unmerged branch, missing/moved tag, or version mismatch fails closed.
4. Backend-free and memory-only package dry-runs and crate tests
   must pass before the workflow publishes the selected libraries in dependency order to the explicit
   `crates-io` registry using compatible dependency resolution. Cargo stages
   workspace dependencies together for validation. Actual uploads publish
   protocol/query first, wait for their exact non-yanked versions in the Cargo
   sparse index, publish the database, then wait for every client dependency
   before publishing the SDK. Uploads are serialized and an in-flight
   publication is never cancelled by a newer validation run.
   RocksDB builds and tests must pass in normal CI before merging; the publish
   workflow deliberately does not repeat them.
5. Check the Actions result and verify `0.5.0` on
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


## Dependency readiness

Release uploads use explicit index-readiness barriers, following ractor's
publish/wait/dependent-publish sequence:

1. Publish `actordb-protocol` and `actordb-query`.
2. Wait until both exact release versions appear in the public Cargo index.
3. Publish `actordb`.
4. Verify protocol, query, and database versions are indexed, then publish
   `actordb-client`.

`.github/scripts/wait_for_crate_dependency.py DEPENDENT TARGET` reads Cargo
metadata and checks the target's full workspace version, such as `0.4.0`.
It does not mistake a compatible dependency requirement such as `0.4` for
an exact published version. Only a matching, non-yanked index entry counts.
The helper checks immediately, then retries every five seconds for up to
15 minutes per dependency; the release job has a 60-minute overall deadline.
Transient registry errors are retried; an unavailable version fails the job.

These barriers and uploads run only on a published release. PR/main jobs
perform package dry-runs and exercise the guards with simulated registry
responses. No credential is supplied to index polling. The release guard
requires all four published libraries to share the release version and keeps
internal workspace tools nonpublishable.
