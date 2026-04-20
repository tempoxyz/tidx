FROM rustlang/rust:nightly-slim@sha256:d2d59b52ef6ebccdb34cab143acf653bbb15482c95fe3069531632e1e96ba3d6 AS builder

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

FROM ubuntu:24.04@sha256:84e77dee7d1bc93fb029a45e3c6cb9d8aa4831ccfcc7103d36e876938d28895b

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/tidx /usr/local/bin/
COPY --from=builder /app/db /db

ENTRYPOINT ["tidx"]
