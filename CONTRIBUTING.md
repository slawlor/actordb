# Contributing to this library

We want to make contributing to this project as easy and transparent as
possible.

## Be respectful

Duh.

## Pull Requests

We actively welcome your pull requests!

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.

Keep unit tests in an inline `#[cfg(test)] mod tests` in the owning source file.
Use separate files under `tests/` for integration tests exercising the crate's
public API; do not expose production internals just to test them.

Release maintainers should follow the [release procedure](docs/releases.md).
The library workspace does not track `Cargo.lock`. Keep dependency requirements
as compatible semver ranges; generated local lockfiles remain ignored. Run
`cargo update` when refreshing local dependencies and rerun the feature matrix.
PRs validate the crates.io package without credentials or uploads; merging a
version bump does not itself publish a release.

## Issues

We use GitHub issues to track issues.

For reported bugs, please ensure your description is clear and has sufficient instructions to be able to reproduce the issue.

For feature requests, please try to be detailed in what you'd like to see so we can address it properly!

## License

By contributing to `actordb`, you agree that your contributions will be
licensed under the LICENSE file in the root directory of this source tree.
