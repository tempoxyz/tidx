# PostgreSQL with pg_duckdb extension for analytical queries
# This enables DuckDB's columnar analytical engine directly within PostgreSQL
# No data replication needed - queries run directly on PostgreSQL tables

ARG PG_MAJOR=16
FROM postgres:${PG_MAJOR}

ARG PG_MAJOR=16

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    git \
    libclang-dev \
    ninja-build \
    pkg-config \
    postgresql-server-dev-${PG_MAJOR} \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Clone and build pg_duckdb
WORKDIR /tmp/pg_duckdb
RUN git clone --depth 1 https://github.com/duckdb/pg_duckdb.git . \
    && make -j$(nproc) \
    && make install

# Cleanup to reduce image size
RUN rm -rf /tmp/pg_duckdb \
    && apt-get purge -y build-essential git curl cmake ninja-build pkg-config libclang-dev python3 \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Add pg_duckdb to shared_preload_libraries
RUN echo "shared_preload_libraries = 'pg_duckdb'" >> /usr/share/postgresql/postgresql.conf.sample

# Create extension on database initialization
COPY init-pg-duckdb.sql /docker-entrypoint-initdb.d/
