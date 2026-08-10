FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p app

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/app /usr/local/bin/poly-app
RUN mkdir -p /app/shadow-data

ENV BIND_ADDR=0.0.0.0:8080
EXPOSE 8080

CMD ["poly-app", "--mode", "shadow"]
