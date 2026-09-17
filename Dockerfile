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

RUN apt-get update --yes \
    && apt-get install --yes --no-install-recommends \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    && apt-get clean --yes \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tidx /usr/local/bin/
COPY --from=builder /app/db /db

ENTRYPOINT ["tidx"]
