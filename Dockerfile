FROM rust:slim-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY deployments/hypercore-indexer-dev/init.sql deployments/hypercore-indexer-dev/init.sql

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/hypercore-indexer /app/hypercore-indexer
ENTRYPOINT ["/app/hypercore-indexer"]
