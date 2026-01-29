# PostgreSQL with pg_parquet extension for server-side Parquet generation
# This enables COPY TO STDOUT (FORMAT 'parquet') for fast DuckDB replication

ARG PG_MAJOR=16
FROM postgres:${PG_MAJOR}

ARG PG_MAJOR=16

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libclang-dev \
    pkg-config \
    postgresql-server-dev-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable

# Install cargo-pgrx (must match pg_parquet's required version)
ARG PGRX_VERSION=0.16.1
RUN cargo install --force --locked cargo-pgrx@${PGRX_VERSION}

# Initialize pgrx for this PostgreSQL version
RUN cargo pgrx init --pg${PG_MAJOR} /usr/bin/pg_config

# Clone and build pg_parquet
WORKDIR /tmp/pg_parquet
RUN git clone --depth 1 https://github.com/CrunchyData/pg_parquet.git .
RUN cargo pgrx install --release --pg-config /usr/bin/pg_config

# Cleanup to reduce image size
RUN rm -rf /tmp/pg_parquet \
    && rm -rf /usr/local/cargo/registry \
    && rm -rf /usr/local/rustup \
    && apt-get purge -y build-essential git curl pkg-config libclang-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Add pg_parquet to shared_preload_libraries
RUN echo "shared_preload_libraries = 'pg_parquet'" >> /usr/share/postgresql/postgresql.conf.sample
