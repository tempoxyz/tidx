# PostgreSQL with pg_duckdb extension and tidx_abi for Ethereum ABI decoding
# This enables DuckDB's columnar analytical engine with native log decoding

ARG PGDUCKDB_VERSION=16-v0.3.1

# =============================================================================
# Stage 1: Build tidx_abi DuckDB extension
# =============================================================================
FROM rust:1.87 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy extension source
WORKDIR /build
COPY tidx-abi/src ./src
COPY tidx-abi/Cargo.toml tidx-abi/Cargo.lock* ./

# Build the extension
RUN cargo build --release

# =============================================================================
# Stage 2: Final image with pg_duckdb + tidx_abi
# =============================================================================
FROM pgduckdb/pgduckdb:${PGDUCKDB_VERSION}

# Create extensions directory
RUN mkdir -p /usr/share/duckdb/extensions

# Copy the built extension from builder stage (Linux .so)
COPY --from=builder /build/target/release/libtidx_abi.so /usr/share/duckdb/extensions/tidx_abi.duckdb_extension

# Create init script to load extension
COPY init-pg-duckdb.sql /docker-entrypoint-initdb.d/
