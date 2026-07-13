# syntax=docker/dockerfile:1

ARG CNPG_BASE=ghcr.io/cloudnative-pg/postgresql:18.3-minimal-trixie
ARG PG_MAJOR=18

FROM ${CNPG_BASE} AS build

ARG PG_MAJOR

USER root
WORKDIR /work

COPY src/ .

RUN apt-get update && apt-get install -y --no-install-recommends \
        g++ \
        libcurl4-openssl-dev \
        libicu-dev \
        liblz4-dev \
        libre2-dev \
        libssl-dev \
        libzstd-dev \
        make \
        pgxnclient \
        postgresql-server-dev-${PG_MAJOR} \
        unzip \
        uuid-dev \
    && rm -rf /var/lib/apt/lists/*

RUN make -j"$(nproc)" && make install DESTDIR=/dest

# pg_clickhouse's re2 PostgreSQL dependency is not packaged by PGDG.
RUN cd /tmp \
    && pgxn download re2 \
    && unzip re2-*.zip \
    && rm re2-*.zip \
    && cd re2-* \
    && make -j"$(nproc)" \
    && make install DESTDIR=/dest

FROM ${CNPG_BASE}

ARG PG_MAJOR

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libcurl4t64 \
        liblz4-1 \
        libre2-11 \
        libzstd1 \
        uuid \
    && apt-get clean \
    && rm -rf /var/cache/apt/* /var/lib/apt/lists/*

COPY --chmod=644 --from=build \
    /dest/usr/share/postgresql/${PG_MAJOR}/extension/*.* \
    /usr/share/postgresql/${PG_MAJOR}/extension/

COPY --chmod=755 --from=build \
    /dest/usr/lib/postgresql/${PG_MAJOR}/lib/*.so* \
    /usr/lib/postgresql/${PG_MAJOR}/lib/

COPY --chmod=644 --from=build \
    /dest/usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/*.bc \
    /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/
COPY --chmod=644 --from=build \
    /dest/usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/pg_clickhouse/src/*.bc \
    /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/pg_clickhouse/src/

RUN test -f /usr/lib/postgresql/${PG_MAJOR}/lib/pg_clickhouse.so \
    && test -f /usr/lib/postgresql/${PG_MAJOR}/lib/re2.so \
    && test -f /usr/share/postgresql/${PG_MAJOR}/extension/pg_clickhouse.control \
    && test -f /usr/share/postgresql/${PG_MAJOR}/extension/re2.control \
    && ldd /usr/lib/postgresql/${PG_MAJOR}/lib/pg_clickhouse.so > /tmp/pg_clickhouse.ldd \
    && ldd /usr/lib/postgresql/${PG_MAJOR}/lib/re2.so > /tmp/re2.ldd \
    && ! grep -q 'not found' /tmp/pg_clickhouse.ldd /tmp/re2.ldd \
    && rm /tmp/pg_clickhouse.ldd /tmp/re2.ldd

# CloudNativePG operand images run PostgreSQL as uid 26.
USER 26
