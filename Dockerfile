FROM rustlang/rust:nightly-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    g++ \
    clang \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY db ./db
COPY benches ./benches

RUN cargo build --release

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Pre-install DuckDB CLI to download postgres extension for offline use
# This allows the postgres extension to work without internet access at runtime
RUN curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.2.2/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip \
    && unzip /tmp/duckdb.zip -d /tmp \
    && /tmp/duckdb -c "INSTALL postgres; LOAD postgres;" \
    && rm -rf /tmp/duckdb.zip /tmp/duckdb \
    && mkdir -p /root/.duckdb

COPY --from=builder /app/target/release/tidx /usr/local/bin/
COPY --from=builder /app/db /db

ENTRYPOINT ["tidx"]
