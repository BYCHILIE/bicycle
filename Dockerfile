# =============================================================================
# Bicycle - Distributed Streaming Engine
# Multi-stage Docker build
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM rust:1.75-bookworm AS builder

# Install build dependencies for RocksDB and protobuf-src
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    clang \
    libclang-dev \
    llvm-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    zlib1g-dev \
    autoconf \
    automake \
    libtool \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for RocksDB build
ENV LIBCLANG_PATH=/usr/lib/llvm-14/lib
ENV ROCKSDB_LIB_DIR=/usr/lib

WORKDIR /app

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/core/Cargo.toml crates/core/
COPY crates/runtime/Cargo.toml crates/runtime/
COPY crates/operators/Cargo.toml crates/operators/
COPY crates/state/Cargo.toml crates/state/
COPY crates/checkpoint/Cargo.toml crates/checkpoint/
COPY crates/network/Cargo.toml crates/network/
COPY crates/protocol/Cargo.toml crates/protocol/
COPY crates/connectors/Cargo.toml crates/connectors/
COPY bin/mini-runner/Cargo.toml bin/mini-runner/
COPY bin/jobmanager/Cargo.toml bin/jobmanager/
COPY bin/worker/Cargo.toml bin/worker/
COPY bin/webui/Cargo.toml bin/webui/
COPY bin/demo-client/Cargo.toml bin/demo-client/

# Copy proto files (needed by protocol crate build.rs)
COPY proto proto/

# Copy protocol crate build.rs (needed early for dependency resolution)
COPY crates/protocol/build.rs crates/protocol/

# Create dummy source files to build dependencies
RUN mkdir -p crates/core/src crates/runtime/src crates/operators/src \
    crates/state/src crates/checkpoint/src crates/network/src \
    crates/protocol/src crates/connectors/src \
    bin/mini-runner/src bin/jobmanager/src bin/worker/src bin/webui/src bin/demo-client/src \
    && echo "fn main() {}" > bin/mini-runner/src/main.rs \
    && echo "fn main() {}" > bin/jobmanager/src/main.rs \
    && echo "fn main() {}" > bin/worker/src/main.rs \
    && echo "fn main() {}" > bin/webui/src/main.rs \
    && echo "fn main() {}" > bin/demo-client/src/main.rs \
    && touch crates/core/src/lib.rs \
    && touch crates/runtime/src/lib.rs \
    && touch crates/operators/src/lib.rs \
    && touch crates/state/src/lib.rs \
    && touch crates/checkpoint/src/lib.rs \
    && touch crates/network/src/lib.rs \
    && touch crates/protocol/src/lib.rs \
    && touch crates/connectors/src/lib.rs

# Build dependencies (this layer is cached)
RUN cargo build --release 2>/dev/null || true

# Copy actual source code
COPY crates crates/
COPY bin bin/

# Touch files to invalidate cache and rebuild
RUN touch crates/*/src/*.rs bin/*/src/*.rs

# Build release binaries
RUN cargo build --release

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 bicycle

# Create directories
RUN mkdir -p /opt/bicycle/bin /opt/bicycle/config /var/bicycle/state /var/bicycle/checkpoints \
    && chown -R bicycle:bicycle /opt/bicycle /var/bicycle

WORKDIR /opt/bicycle

# Copy binaries from builder
COPY --from=builder /app/target/release/mini-runner bin/
COPY --from=builder /app/target/release/jobmanager bin/
COPY --from=builder /app/target/release/worker bin/
COPY --from=builder /app/target/release/webui bin/
COPY --from=builder /app/target/release/demo-client bin/

# Copy configuration
COPY config/ config/

# Set permissions
RUN chmod +x bin/*

USER bicycle

# Environment variables
ENV RUST_LOG=info
ENV BICYCLE_STATE_DIR=/var/bicycle/state
ENV BICYCLE_CHECKPOINT_DIR=/var/bicycle/checkpoints

# Default command
CMD ["bin/mini-runner"]

# -----------------------------------------------------------------------------
# Stage 3: JobManager
# -----------------------------------------------------------------------------
FROM runtime AS jobmanager

EXPOSE 9000
CMD ["bin/jobmanager", "--bind", "0.0.0.0:9000"]

# -----------------------------------------------------------------------------
# Stage 4: Worker
# -----------------------------------------------------------------------------
FROM runtime AS worker

EXPOSE 9001
CMD ["bin/worker", "--bind", "0.0.0.0:9001"]

# -----------------------------------------------------------------------------
# Stage 5: Web UI
# -----------------------------------------------------------------------------
FROM runtime AS webui

EXPOSE 8081
CMD ["bin/webui", "--bind", "0.0.0.0:8081"]

# -----------------------------------------------------------------------------
# Stage 6: Demo Client
# -----------------------------------------------------------------------------
FROM runtime AS demo

CMD ["bin/demo-client", "--jobmanager", "jobmanager:9000", "demo", "--duration", "60"]
