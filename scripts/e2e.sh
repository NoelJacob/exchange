#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PASS="${PASS:-admin123}"
API="${API:-http://localhost:8080}"

echo "=== Starting e2e ==="

echo "--- Starting infrastructure ---"
make infra
sleep 15

echo "--- Building runner Docker image ---"
make runner

echo "--- Building and starting platform + ingester ---"
make start

echo "--- Waiting for services to be ready ---"
sleep 10

echo "--- Submitting Correct contestant ---"
make submit NAME="Correct" FILE="contestant-sample/target/release/contestant-sample"

echo "--- Submitting Wrong contestant ---"
make submit NAME="Wrong" FILE="/tmp/contestant-sample-wrong"

echo "=== Waiting for tests to complete (90s) ==="
sleep 90

echo "--- Current leaderboard ---"
make leaderboard

echo ""
echo "=== Correctness Gap Check ==="
C=$(curl -sf "$API/api/contestant/status" \
  -H "Authorization: Bearer $(jq -r .jwt /tmp/jwt-Correct.json 2>/dev/null)" \
  2>/dev/null | jq '.metrics.correctness_pct // 0' 2>/dev/null || echo 0)
W=$(curl -sf "$API/api/contestant/status" \
  -H "Authorization: Bearer $(jq -r .jwt /tmp/jwt-Wrong.json 2>/dev/null)" \
  2>/dev/null | jq '.metrics.correctness_pct // 0' 2>/dev/null || echo 0)

BETTER=$(echo "if ($C > $W) 1 else 0" | bc 2>/dev/null || python3 -c "print(1 if $C > $W else 0)" 2>/dev/null || echo 0)
if [ "$BETTER" = "1" ]; then
    echo "PASS: Correct contestant has higher correctness_pct (${C}% > ${W}%)"
else
    echo "FAIL: Expected Correct > Wrong, got ${C}% vs ${W}%"
    exit 1
fi

echo ""
echo "=== Composite Score Check ==="
LB=$(curl -sf "$API/api/leaderboard" | jq -c '.leaderboard // []' 2>/dev/null || echo '[]')
ALICE=$(echo "$LB" | jq ".[] | select(.name==\"Correct\")" 2>/dev/null || echo "")
if [ -n "$ALICE" ]; then
    A_COMPOSITE=$(echo "$ALICE" | jq '.composite // -1' 2>/dev/null || echo -1)
    A_TPS=$(echo "$ALICE" | jq '.current_tps // 0' 2>/dev/null || echo 0)
    echo "Correct: composite=${A_COMPOSITE} tps=${A_TPS}"

    COMPOSITE_OK=$(echo "$A_COMPOSITE > 0" | bc 2>/dev/null || python3 -c "print(1 if $A_COMPOSITE > 0 else 0)" 2>/dev/null || echo 0)
    TPS_OK=$(echo "$A_TPS > 0" | bc 2>/dev/null || python3 -c "print(1 if $A_TPS > 0 else 0)" 2>/dev/null || echo 0)

    if [ "$COMPOSITE_OK" = "1" ]; then
        echo "PASS: composite > 0"
    else
        echo "FAIL: composite <= 0"
        exit 1
    fi
    if [ "$TPS_OK" = "1" ]; then
        echo "PASS: TPS > 0"
    else
        echo "FAIL: TPS <= 0"
        exit 1
    fi
else
    echo "FAIL: Correct contestant not in leaderboard"
    exit 1
fi

echo ""
echo "=== e2e PASSED ==="
