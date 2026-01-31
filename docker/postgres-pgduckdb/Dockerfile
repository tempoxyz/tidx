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
    git \
    ninja-build \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Copy extension source
WORKDIR /build
COPY tidx-abi/src ./src
COPY tidx-abi/Cargo.toml tidx-abi/Makefile ./

# Initialize git for extension-ci-tools (requires git repo)
RUN git config --global user.email "build@tidx" && \
    git config --global user.name "build" && \
    git init && git add -A && git commit -m "init"

# Clone extension-ci-tools submodule
RUN git clone --depth 1 https://github.com/duckdb/extension-ci-tools.git

# Build the extension
RUN make configure && make release

# =============================================================================
# Stage 2: Final image with pg_duckdb + tidx_abi
# =============================================================================
FROM pgduckdb/pgduckdb:${PGDUCKDB_VERSION}

# Create extensions directory
RUN mkdir -p /usr/share/duckdb/extensions

# Copy the built extension from builder stage
COPY --from=builder /build/build/release/tidx_abi.duckdb_extension /usr/share/duckdb/extensions/

# Create init script to load extension
COPY init-pg-duckdb.sql /docker-entrypoint-initdb.d/
