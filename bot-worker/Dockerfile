FROM rust:latest AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
# Cache Rust
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true
COPY src/ src/
RUN cargo build --release

FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/bot-worker /usr/local/bin/bot-worker
ENTRYPOINT ["/usr/local/bin/bot-worker"]
