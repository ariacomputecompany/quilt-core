# Quilt Production Dockerfile
# Multi-stage build for optimized image size

# =============================================================================
# Stage 1: Build Environment
# =============================================================================
FROM rust:1.83-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    libsqlite3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto/ proto/

# Create dummy source to build dependencies only
RUN mkdir -p src/minit src/qcli src/qgui && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > src/minit/main.rs && \
    echo "fn main() {}" > src/qcli/main.rs && \
    echo "fn main() {}" > src/qgui/main.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src && \
    rm -f target/release/quilt target/release/minit target/release/qcli target/release/qgui \
      target/release/deps/quilt* target/release/deps/minit* target/release/deps/qcli* target/release/deps/qgui*

# Copy actual source code
COPY src/ src/

# Build release binaries (force full rebuild of main crates)
RUN touch src/main.rs src/minit/main.rs src/qcli/main.rs src/qgui/main.rs && \
    cargo build --release --bin quilt && \
    cargo build --release --bin qcli && \
    cargo build --release --bin minit

# =============================================================================
# Stage 2: Runtime Environment
# =============================================================================
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libsqlite3-0 \
    iproute2 \
    iptables \
    util-linux \
    procps \
    curl \
    dnsutils \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p \
    /var/lib/quilt/data \
    /var/lib/quilt/volumes \
    /var/lib/quilt/containers \
    /var/lib/quilt/rootfs \
    /run/quilt \
    /etc/quilt

# Copy binaries from builder
COPY --from=builder /app/target/release/quilt /usr/local/bin/quilt
COPY --from=builder /app/target/release/qcli /usr/local/bin/qcli
COPY --from=builder /app/target/release/minit /usr/local/bin/minit

# Make binaries executable
RUN chmod +x /usr/local/bin/quilt /usr/local/bin/minit /usr/local/bin/qcli

# Create default config
RUN echo '# Quilt Configuration' > /etc/quilt/config.toml

# Set environment variables
ENV RUST_LOG=info,quilt=info
ENV QUILT_DATA_DIR=/var/lib/quilt/data
ENV HTTP_ADDR=0.0.0.0:8080
ENV GRPC_ADDR=0.0.0.0:50051

# Expose ports
# 8080 - HTTP API
# 50051 - gRPC API
EXPOSE 8080 50051

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Volume for persistent data
VOLUME ["/var/lib/quilt"]

# Run as root (required for container operations)
USER root

# Default command - start daemon
CMD ["/usr/local/bin/quilt", "daemon"]
