# syntax=docker/dockerfile:1

# ============================================================================
# Stage 1 — Builder: compile a static-ish release binary against glibc.
# ----------------------------------------------------------------------------
# `tonic-build` shells out to `protoc`, so the protobuf toolchain is required.
# Dependency compilation is cached in its own layer: as long as Cargo.toml /
# Cargo.lock are unchanged, `cargo build` reuses the vendored + compiled deps
# and only recompiles the project sources — making rebuilds fast.
# ============================================================================
FROM rust:1.85-slim-bookworm AS builder

# protoc + headers for tonic-build; pkg-config kept for common transitive needs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Dependency cache layer -------------------------------------------------
# Copy only the manifests + build script + proto first, then compile a throwaway
# lib/bin so the dependency graph is baked into a cached layer independent of the
# actual source. This layer is only invalidated when the manifests change.
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
RUN mkdir -p src/bin \
    && echo 'fn main() {}' > src/bin/takyonic-server.rs \
    && echo '' > src/lib.rs \
    && cargo build --release --bin takyonic-server --features s3 \
    && rm -rf src

# --- Real build layer -------------------------------------------------------
# Now copy the true sources. Touch is needed so Cargo notices the changed mtime
# on the previously-stubbed crate root and recompiles the first-party code.
COPY src ./src
RUN touch src/lib.rs src/bin/takyonic-server.rs \
    && cargo build --release --bin takyonic-server --features s3 \
    && strip target/release/takyonic-server

# Pre-create the data directory owned by the distroless `nonroot` uid (65532) so
# a first-time named volume mount inherits writable ownership (distroless has no
# shell to chown at runtime).
RUN mkdir -p /takyonic-data

# ============================================================================
# Stage 2 — Runtime: distroless (glibc + libgcc + libstdc++, no shell).
# ----------------------------------------------------------------------------
# `cc-debian12` supplies the C runtime that Rust binaries link against while
# keeping the image tiny and attack-surface minimal (no shell, no package
# manager). Runs as the built-in non-root `nonroot` user.
# ============================================================================
FROM gcr.io/distroless/cc-debian12 AS runtime

LABEL org.opencontainers.image.title="takyonic-server" \
      org.opencontainers.image.description="Distributed MVCC NewSQL database with custom Raft and PostgreSQL wire compatibility" \
      org.opencontainers.image.source="https://github.com/hocestnonsatis/takyonic" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=builder /app/target/release/takyonic-server /takyonic-server

# Writable data dir owned by the non-root runtime user (uid 65532 = nonroot).
COPY --from=builder --chown=65532:65532 /takyonic-data /data

# 5001 = Raft / gRPC (peer replication + smart-client), 5433 = PostgreSQL wire.
EXPOSE 5001 5433

# Persist WAL + SSTs here; mount a volume for durability across restarts.
VOLUME ["/data"]
ENV TAKYONIC_DATA=/data

USER nonroot

ENTRYPOINT ["/takyonic-server"]
