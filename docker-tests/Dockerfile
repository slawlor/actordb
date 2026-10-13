# Build from the root of the repository with
#
#   docker build -f docker-tests/Dockerfile . -t actordb-docker-test:latest
#   docker run actordb-docker-test:latest --help
#
# Each container runs one scenario role, e.g.
#   docker run actordb-docker-test:latest replicate-writer --node-id node-a ...

FROM rust:latest
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y cmake build-essential
COPY . .
RUN cargo install --path docker-tests
RUN mv /usr/local/cargo/bin/actordb-docker-test /usr/local/bin/actordb-docker-test
WORKDIR /usr/local/bin

ENTRYPOINT ["actordb-docker-test"]
