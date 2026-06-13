#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Building platform-api, telemetry-ingester, bot-worker ==="
cargo build --release -p platform-api -p telemetry-ingester -p bot-worker

echo "=== Building contestant-sample (correct) ==="
cargo build --release -p contestant-sample
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-correct

echo "=== Building contestant-sample (prefilled/wrong) ==="
cargo build --release -p contestant-sample --features prefilled
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-wrong

echo "=== Restoring correct binary to target/release ==="
mv -f /tmp/contestant-sample-correct contestant-sample/target/release/contestant-sample

echo "=== build-local complete ==="
