FROM rust:latest AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
# Cache Rust
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/contestant-sample /usr/local/bin/contestant-sample
EXPOSE 9090 8080
ENTRYPOINT ["/usr/local/bin/contestant-sample"]
