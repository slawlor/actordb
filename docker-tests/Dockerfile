# Build from the root of the repository with
#
#   docker build --target runtime -f docker-tests/Dockerfile . -t actordb-docker-test:latest
#   docker run actordb-docker-test:latest --help
#
# Each container runs one scenario role, e.g.
#   docker run actordb-docker-test:latest replicate-writer --node-id node-a ...
#
# Layer-caching strategy (previously every build recompiled the world):
#   1. `chef`    - cargo-chef installed once; cached until rust:latest moves.
#   2. `planner` - computes recipe.json from the Cargo manifests and source
#                  layout. There is no committed dependency lockfile.
#   3. `builder` - system deps (clang for librocksdb-sys bindgen) in their own
#                  early layer, then `cargo chef cook` compiles ALL third-party
#                  dependencies into a cached layer. Compatible cached layers
#                  can be reused while the recipe and build environment match;
#                  later cold builds can resolve different dependency versions.
#   4. final build copies the real source and compiles just our crates.
#   5. `runtime` - slim image containing only the test binary: no toolchain,
#                  no source tree, no cargo registry (the old image shipped
#                  all three).

FROM rust:latest AS chef
WORKDIR /usr/src/app
RUN cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# clang: librocksdb-sys runs bindgen at build time, which needs libclang
# with working system-header search paths. Own layer so it never rebuilds.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake build-essential clang \
    && rm -rf /var/lib/apt/lists/*
COPY --from=planner /usr/src/app/recipe.json recipe.json
# Native RocksDB compilation follows Cargo's parallel job count. Bound it on
# memory-limited Docker Desktop VMs; callers can override this build argument.
ARG CARGO_BUILD_JOBS=4
# Compile third-party dependencies only. A compatible cache can retain an older
# resolution; rebuilding without that cache resolves the manifest ranges anew.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# Now compile just our workspace crates on top of the cached dependencies.
RUN cargo build --release --package docker-tests --package docker-client-tests \
    && cp target/release/actordb-docker-test /usr/local/bin/actordb-docker-test \
    && cp target/release/actordb-docker-client-test /usr/local/bin/actordb-docker-client-test \
    && cp target/release/actordb-docker-cluster-client /usr/local/bin/actordb-docker-cluster-client

FROM debian:trixie-slim AS runtime
# NOTE: this must match the Debian release underlying rust:latest
# (trixie as of Oct 2026). The builder links against its glibc/libstdc++,
# so a runtime older than the builder fails with
# "GLIBC_X.XX not found" / "CXXABI_X.X.XX not found". If rust:latest moves
# to a newer Debian, bump this tag to match.
# librocksdb-sys links rocksdb statically, but the binary still needs the
# C++ runtime. Keep the image minimal: no toolchain, no source, no registry.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/actordb-docker-test /usr/local/bin/actordb-docker-test
WORKDIR /usr/local/bin

ENTRYPOINT ["actordb-docker-test"]

# The external client image contains only the SDK scenario executable. It has
# no Node executable, database volume, peer listener, or C++ storage dependency.
FROM debian:trixie-slim AS client-runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/actordb-docker-client-test /usr/local/bin/actordb-docker-client-test
COPY --from=builder /usr/local/bin/actordb-docker-cluster-client /usr/local/bin/actordb-docker-cluster-client
WORKDIR /usr/local/bin
ENTRYPOINT ["actordb-docker-client-test"]

# Preserve the historical server image when no build target is specified.
FROM runtime AS default-runtime
