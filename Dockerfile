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

ARG GIT_REV=dev
ENV GIT_REV=${GIT_REV}
RUN cargo build --release

FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tidx /usr/local/bin/
COPY --from=builder /app/db /db

ENTRYPOINT ["tidx"]
