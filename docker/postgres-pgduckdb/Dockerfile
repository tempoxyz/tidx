# PostgreSQL with pg_duckdb extension and tidx_abi for Ethereum ABI decoding
# This enables DuckDB's columnar analytical engine with native log decoding

ARG PGDUCKDB_VERSION=16-v0.3.1
ARG DUCKDB_VERSION=v1.2.0

# =============================================================================
# Stage 1: Build tidx_abi DuckDB extension
# =============================================================================
FROM rust:1.87 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    python3 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy extension source
WORKDIR /build
COPY tidx-abi/src ./src
COPY tidx-abi/Cargo.toml tidx-abi/Cargo.lock* ./

# Build the extension
RUN cargo build --release

# Clone extension-ci-tools for metadata script
ARG DUCKDB_VERSION
RUN git clone --depth 1 --branch ${DUCKDB_VERSION} https://github.com/duckdb/extension-ci-tools.git /extension-ci-tools

# Append DuckDB extension metadata (534-byte footer required for loading)
RUN python3 /extension-ci-tools/scripts/append_extension_metadata.py \
    --library-file target/release/libtidx_abi.so \
    --extension-name tidx_abi \
    --duckdb-platform linux_amd64_gcc4 \
    --extension-version "0.1.0" \
    --duckdb-version "${DUCKDB_VERSION}"

# =============================================================================
# Stage 2: Final image with pg_duckdb + tidx_abi
# =============================================================================
FROM pgduckdb/pgduckdb:${PGDUCKDB_VERSION}

# Create extensions directory
RUN mkdir -p /usr/share/duckdb/extensions

# Copy the built extension from builder stage (with metadata appended)
COPY --from=builder /build/tidx_abi.duckdb_extension /usr/share/duckdb/extensions/

# Create init script to load extension
COPY init-pg-duckdb.sql /docker-entrypoint-initdb.d/
