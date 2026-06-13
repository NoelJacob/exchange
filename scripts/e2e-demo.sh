#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
API="http://localhost:8080"
PASS="admin123"

# Check dependencies
if ! command -v jq &>/dev/null; then
    echo "jq is required. Install: brew install jq / apt install jq"
    exit 1
fi

echo "=== 1. Build all binaries ==="
cd "$ROOT_DIR"
cargo build --release -p contestant-sample
cargo build --release -p telemetry-ingester
cargo build --release -p bot-worker

echo "=== 2. Start Docker infrastructure ==="
cd "$ROOT_DIR/infra"
docker compose up -d questdb valkey redpanda minio
echo "Waiting for services to be ready..."
sleep 15

echo "=== 3. Build runner image ==="
docker compose build runner

echo "=== 4. Build and start platform-api + telemetry-ingester ==="
docker compose build platform-api telemetry-ingester
docker compose up -d platform-api telemetry-ingester
sleep 10

# Verify health
echo "=== Health check ==="
curl -sf "$API/health" | jq .
echo ""

echo "=== 5. Admin creates 2 contestants ==="
CORRECT=$(curl -sf -X POST "$API/api/admin/register" \
    -H "X-Admin-Password: $PASS" \
    -d '{"name":"Correct Bot"}')
echo "Correct: $CORRECT" | head -c 200
CORRECT_JWT=$(echo "$CORRECT" | jq -r .jwt)

WRONG=$(curl -sf -X POST "$API/api/admin/register" \
    -H "X-Admin-Password: $PASS" \
    -d '{"name":"Wrong Bot"}')
echo "Wrong: $WRONG" | head -c 200
WRONG_JWT=$(echo "$WRONG" | jq -r .jwt)

echo ""
echo "=== 6. Contestant 1 submits correct binary ==="
CORRECT_RUN=$(curl -sf -X POST "$API/api/contestant/submit" \
	-H "Authorization: Bearer $CORRECT_JWT" \
	-F "binary=@$ROOT_DIR/contestant-sample/target/release/contestant-sample" | jq -r .run_id)
echo "Correct run_id=$CORRECT_RUN"

WRONG_RUN=$(curl -sf -X POST "$API/api/contestant/submit" \
	-H "Authorization: Bearer $WRONG_JWT" \
	-F "binary=@$ROOT_DIR/contestant-sample/target/release/contestant-sample" | jq -r .run_id)
echo "Wrong run_id=$WRONG_RUN"

echo ""
echo "=== 8. Poll deployment progress ==="
for i in $(seq 1 20); do
    echo "--- Status poll $i ---"
    echo "Correct:"
    curl -s "$API/api/contestant/status" -H "Authorization: Bearer $CORRECT_JWT" | \
        jq -r '  status=\(.status)  run_id=\(.run_id)'
    echo "Wrong:"
    curl -s "$API/api/contestant/status" -H "Authorization: Bearer $WRONG_JWT" | \
        jq -r '  status=\(.status)  run_id=\(.run_id)'
    echo ""
    sleep 3
done

echo "=== 9. Final leaderboard ==="
curl -s "$API/api/leaderboard" | jq '
  .leaderboard[] | "  \(.name): status=\(.status)  correctness=\(.correctness_pct // "?")%  orders=\(.orders_sent // "?")  penalty=\(.total_penalty // "?")"
'
echo ""

echo "=== 10. Individual contestant status ==="
echo "Correct contestant summary:"
curl -s "$API/api/contestant/status" -H "Authorization: Bearer $CORRECT_JWT" | jq '.metrics'
echo "Wrong contestant summary:"
curl -s "$API/api/contestant/status" -H "Authorization: Bearer $WRONG_JWT" | jq '.metrics'
echo ""

echo "=== 11. Verify container cleanup ==="
echo "Running contestant/bot containers (should be empty):"
docker ps --filter name=contestant- --filter name=bot- 2>/dev/null || echo "(none)"
echo ""

echo "=== DONE ==="
echo "Correct run: $CORRECT_RUN"
echo "Wrong run:   $WRONG_RUN"
