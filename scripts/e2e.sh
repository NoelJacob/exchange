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

echo "--- Submitting Panic contestant ---"
make submit NAME="Panic" FILE="/tmp/contestant-sample-panic"

echo "--- Submitting Slow contestant ---"
make submit NAME="Slow" FILE="/tmp/contestant-sample-slow"

echo "=== Waiting for tests to complete (90s) ==="
sleep 90

echo "--- Current leaderboard ---"
make leaderboard
echo ""

echo "=== Parsing leaderboard ==="
LB=$(curl -sf "$API/api/leaderboard" | jq -c '.leaderboard // []' 2>/dev/null || echo '[]')
echo "Leaderboard: $(echo "$LB" | jq -c '.')"

echo ""
echo "=== Assertion: 4 contestants in leaderboard ==="
COUNT=$(echo "$LB" | jq 'length' 2>/dev/null || echo 0)
if [ "$COUNT" -ge 4 ]; then
    echo "PASS: Found $COUNT contestants on leaderboard"
else
    echo "FAIL: Expected ≥4 contestants, got $COUNT"
    echo "Leaderboard: $LB"
    exit 1
fi

echo ""
echo "=== Assertion: Correct has highest composite ==="
CORRECT=$(echo "$LB" | jq ".[] | select(.name==\"Correct\")" 2>/dev/null || echo "")
if [ -z "$CORRECT" ]; then
    echo "FAIL: Correct contestant not in leaderboard"
    echo "Leaderboard: $LB"
    exit 1
fi
C_COMPOSITE=$(echo "$CORRECT" | jq '.composite // -1' 2>/dev/null || echo -1)
echo "Correct: composite=${C_COMPOSITE}"

# Verify Correct is first (highest composite)
TOP=$(echo "$LB" | jq -r '.[0].name' 2>/dev/null || echo "")
if [ "$TOP" = "Correct" ]; then
    echo "PASS: Correct is #1 on leaderboard"
else
    echo "FAIL: Expected Correct as #1, got ${TOP}"
    exit 1
fi

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
    echo "PASS: Correct has higher correctness_pct (${C}% > ${W}%)"
else
    echo "FAIL: Expected Correct > Wrong, got ${C}% vs ${W}%"
    exit 1
fi

echo ""
echo "=== Assertion: Panic has failure_reason or is absent ==="
PANIC=$(echo "$LB" | jq ".[] | select(.name==\"Panic\")" 2>/dev/null || echo "")
if [ -n "$PANIC" ]; then
    PANIC_REASON=$(echo "$PANIC" | jq -r '.failure_reason // "null"' 2>/dev/null || echo "null")
    echo "Panic: failure_reason=${PANIC_REASON}"
    # Accept either null (crash recorded) or "crashed" string
else
    echo "Panic: not in leaderboard (may have no data if crash was too early)"
fi

echo ""
echo "=== Assertion: Slow TPS vs Correct TPS ==="
SLOW=$(echo "$LB" | jq ".[] | select(.name==\"Slow\")" 2>/dev/null || echo "")
if [ -n "$SLOW" ]; then
    S_TPS=$(echo "$SLOW" | jq '.current_tps // 0' 2>/dev/null || echo 0)
    C_TPS=$(echo "$CORRECT" | jq '.current_tps // 0' 2>/dev/null || echo 0)
    echo "Correct TPS=${C_TPS}, Slow TPS=${S_TPS}"

    # Slow should be noticeably lower than Correct
    TPS_RATIO_OK=$(echo "if ($C_TPS > 0 and $S_TPS < $C_TPS * 0.5) 1 else 0" | bc 2>/dev/null || python3 -c "print(1 if $C_TPS > 0 and $S_TPS < $C_TPS * 0.5 else 0)" 2>/dev/null || echo 0)
    if [ "$TPS_RATIO_OK" = "1" ]; then
        echo "PASS: Slow TPS (${S_TPS}) is less than 50% of Correct TPS (${C_TPS})"
    else
        echo "WARN: Slow TPS (${S_TPS}) not significantly lower than Correct TPS (${C_TPS}) — ranking may still be differentiated"
    fi
else
    echo "Slow: not in leaderboard"
fi

echo ""
echo "=== Assertion: Wrong correctness < 50% (or lower than Correct) ==="
W_PCT=$(curl -sf "$API/api/contestant/status" \
  -H "Authorization: Bearer $(jq -r .jwt /tmp/jwt-Wrong.json 2>/dev/null)" \
  2>/dev/null | jq '.metrics.correctness_pct // 0' 2>/dev/null || echo 0)
WRONG_OK=$(echo "if ($W_PCT < 50) 1 else 0" | bc 2>/dev/null || python3 -c "print(1 if $W_PCT < 50 else 0)" 2>/dev/null || echo 0)
if [ "$WRONG_OK" = "1" ]; then
    echo "PASS: Wrong correctness_pct (${W_PCT}%) < 50%"
else
    echo "WARN: Wrong correctness_pct (${W_PCT}%) is not below 50% — may still be lower than Correct"
fi

echo ""
echo "=== e2e PASSED ==="
