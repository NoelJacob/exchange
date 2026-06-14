#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"

echo "=== Building platform-api ==="
cd platform-api && cargo build --release && cd "$ROOT"

echo "=== Building telemetry-ingester ==="
cd telemetry-ingester && cargo build --release && cd "$ROOT"

echo "=== Building bot-worker ==="
cd bot-worker && cargo build --release && cd "$ROOT"

echo "=== Building contestant-sample (correct) ==="
cd contestant-sample && cargo build --release && cd "$ROOT"
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-correct

echo "=== Building contestant-sample (prefilled/wrong) ==="
cd contestant-sample && cargo build --release --features prefilled && cd "$ROOT"
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-wrong

echo "=== Building contestant-sample (panic_10s) ==="
cd contestant-sample && cargo build --release --features panic_10s && cd "$ROOT"
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-panic

echo "=== Building contestant-sample (slow_submit) ==="
cd contestant-sample && cargo build --release --features slow_submit && cd "$ROOT"
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-slow

echo "=== Building contestant-sample (randomize_price) ==="
cd contestant-sample && cargo build --release --features randomize_price && cd "$ROOT"
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-random

echo "=== Restoring correct binary to target/release ==="
mv -f /tmp/contestant-sample-correct contestant-sample/target/release/contestant-sample
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-correct

echo "=== build-local complete ==="
