# Build from the root of the repository with
#
#   docker build -f docker-tests/Dockerfile . -t actordb-docker-test:latest
#   docker run actordb-docker-test:latest --help
#
# Each container runs one scenario role, e.g.
#   docker run actordb-docker-test:latest replicate-writer --node-id node-a ...
#
# Layer-caching strategy (previously every build recompiled the world):
#   1. `chef`    - cargo-chef installed once; cached until rust:latest moves.
#   2. `planner` - computes recipe.json from the Cargo manifests only.
#                  Invalidated only when a Cargo.toml / Cargo.lock changes.
#   3. `builder` - system deps (clang for librocksdb-sys bindgen) in their own
#                  early layer, then `cargo chef cook` compiles ALL third-party
#                  dependencies into a cached layer. Invalidated only when
#                  recipe.json changes, i.e. almost never on a code-only push.
#   4. final build copies the real source and compiles just our crates.
#   5. `runtime` - slim image containing only the test binary: no toolchain,
#                  no source tree, no cargo registry (the old image shipped
#                  all three).

FROM rust:latest AS chef
WORKDIR /usr/src/app
RUN cargo install cargo-chef --locked

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
# Compile third-party dependencies only. This layer is cached until the
# dependency recipe changes.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# Now compile just our workspace crates on top of the cached dependencies.
RUN cargo build --release --bin actordb-docker-test \
    && cp target/release/actordb-docker-test /usr/local/bin/actordb-docker-test

FROM debian:bookworm-slim AS runtime
# librocksdb-sys links rocksdb statically, but the binary still needs the
# C++ runtime. Keep the image minimal: no toolchain, no source, no registry.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/actordb-docker-test /usr/local/bin/actordb-docker-test
WORKDIR /usr/local/bin

ENTRYPOINT ["actordb-docker-test"]
