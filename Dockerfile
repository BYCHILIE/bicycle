# syntax=docker/dockerfile:1

FROM rust:1.78 as builder
WORKDIR /app

# Cache deps first
COPY Cargo.toml rust-toolchain.toml ./
COPY crates ./crates
COPY bin ./bin

RUN cargo build -p mini-runner --release \
 && cargo build -p jobmanager --release \
 && cargo build -p worker --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/target/release/mini-runner /usr/local/bin/mini-runner
COPY --from=builder /app/target/release/jobmanager /usr/local/bin/jobmanager
COPY --from=builder /app/target/release/worker /usr/local/bin/worker

ENTRYPOINT ["/usr/local/bin/mini-runner"]
