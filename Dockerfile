# syntax=docker/dockerfile:1
FROM rustlang/rust:nightly-slim AS builder

WORKDIR /app

RUN apt-get update  --yes \
    && apt-get install --yes --no-install-recommends \
    pkg-config \
    libssl-dev \
    g++ \
    clang \
    cmake \
    && apt-get clean --yes \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY db ./db
COPY benches ./benches

ARG GIT_REV=dev
ENV GIT_REV=${GIT_REV}
RUN cargo build --release

FROM ubuntu:24.04

ARG TARGETARCH
ARG PGROLL_VERSION=v0.16.1

RUN apt-get update --yes \
    && apt-get install --yes --no-install-recommends \
    ca-certificates \
    curl \
    libssl3 \
    libstdc++6 \
    && case "${TARGETARCH}" in \
    amd64|arm64) pgroll_arch="${TARGETARCH}" ;; \
    *) echo "unsupported pgroll architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --show-error \
    --output /usr/local/bin/pgroll \
    "https://github.com/xataio/pgroll/releases/download/${PGROLL_VERSION}/pgroll.linux.${pgroll_arch}" \
    && chmod +x /usr/local/bin/pgroll \
    && apt-get purge --yes --auto-remove curl \
    && apt-get clean --yes \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tidx /usr/local/bin/
COPY --from=builder /app/db /db

ENTRYPOINT ["tidx"]
