#!/usr/bin/env bash
# Load test: bot-worker against exchange at specified RPS.
# No Docker/infra dependencies — exchange + bot only.
# Usage: ./tests/load_test.sh <RPS>
# Requires: cargo, ~30s runtime.

set -u -o pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <RPS>"
  exit 1
fi
TARGET_RPS="$1"

BOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EXCHANGE_DIR="$(cd "$(dirname "$0")/../../contestant-sample" && pwd)"

BOT_LOG="/tmp/bot_loadtest.log"
EXCHANGE_LOG="/tmp/exchange_loadtest.log"
CONTESTANT_ID="loadtest-$(date +%s)"
SEED=42

DURATION=10
RAMPUP=2
REPORT_INTERVAL=5

echo "=== Load test: ${TARGET_RPS} RPS ==="
echo "  Contestant: $CONTESTANT_ID"
echo "  Duration: ${DURATION}s, Ramp: ${RAMPUP}s"
echo ""

# Step 1: Kill stale
echo "[1/5] Cleaning stale processes..."
kill -9 "$(cat /tmp/exchange_loadtest.pid 2>/dev/null)" 2>/dev/null || true
pkill -9 -f contestant-sample 2>/dev/null || true
fuser -k 9090/tcp 2>/dev/null || true
fuser -k 8080/tcp 2>/dev/null || true
sleep 2
echo "  Done"

# Step 2: Find or build exchange binary
echo "[2/5] Finding exchange binary..."
BIN_CANDIDATES=(
  "$EXCHANGE_DIR/target/release/contestant-sample"
  "$EXCHANGE_DIR/target/debug/contestant-sample"
)
BIN=""
for c in "${BIN_CANDIDATES[@]}"; do
  if [ -x "$c" ]; then
    BIN="$c"
    break
  fi
done

if [ -z "$BIN" ]; then
  echo "  Building exchange release binary..."
  cd "$EXCHANGE_DIR"
  if ! cargo build --release 2>&1; then
    echo "FAIL: exchange build failed"
    exit 1
  fi
  BIN="$EXCHANGE_DIR/target/release/contestant-sample"
fi
echo "  Using: $BIN"

# Step 3: Start exchange
echo "[3/5] Starting exchange..."
"$BIN" &>"$EXCHANGE_LOG" &
EXCHANGE_PID=$!
echo "$EXCHANGE_PID" >/tmp/exchange_loadtest.pid

# Poll for port 9090 (up to 10s)
STARTED=false
for i in $(seq 1 40); do
  if ! kill -0 "$EXCHANGE_PID" 2>/dev/null; then
    echo "FAIL: exchange exited prematurely"
    cat "$EXCHANGE_LOG"
    exit 1
  fi
  if timeout 0.3 bash -c "echo >/dev/tcp/127.0.0.1/9090" 2>/dev/null; then
    STARTED=true
    break
  fi
  sleep 0.25
done

if [ "$STARTED" != "true" ]; then
  echo "FAIL: exchange did not start listening on 9090 within 10s"
  tail -20 "$EXCHANGE_LOG"
  kill "$EXCHANGE_PID" 2>/dev/null || true
  exit 1
fi
echo "  Exchange ready (PID $EXCHANGE_PID)"

# Step 4: Run bot-worker
echo "[4/5] Running bot-worker at ${TARGET_RPS} RPS..."
cd "$BOT_DIR"
RESULT=$(timeout 120 cargo run --release -- \
  --rps "$TARGET_RPS" --duration-secs "$DURATION" \
  --fix-connections 4 --ws-connections 4 --ramp-up-secs "$RAMPUP" \
  --report-interval-secs "$REPORT_INTERVAL" --seed "$SEED" \
  --contestant-id "$CONTESTANT_ID" \
  2>"$BOT_LOG")
BOT_EXIT=$?

echo "  Bot exit code: $BOT_EXIT"
echo ""

# Step 5: Parse result JSON and assert
echo "[5/5] Checking assertions..."

# Extract JSON from last line of stdout (bot prints result as last line)
RESULT_JSON=$(echo "$RESULT" | tail -1)
ORDERS_SENT=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('orders_sent',0))" 2>/dev/null || echo 0)
FILLS=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('fills',0))" 2>/dev/null || echo 0)
PARTIALS=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('partials',0))" 2>/dev/null || echo 0)
REJECTS=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('rejects',0))" 2>/dev/null || echo 0)
P50=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('p50_latency_us',0))" 2>/dev/null || echo 0)
AVG_LAT=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('avg_latency_us',0))" 2>/dev/null || echo 0)
ERRORS=$(echo "$RESULT_JSON" | python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('errors',[]))" 2>/dev/null || echo "[]")
ERROR_COUNT=$(echo "$ERRORS" | python3 -c "import sys,json; print(len(json.loads(sys.stdin.read())))" 2>/dev/null || echo 0)

echo "  Result: ${ORDERS_SENT} orders, ${FILLS} fills, ${PARTIALS} partials, ${REJECTS} rejects, ${ERROR_COUNT} errors"
echo "  Latency: p50=${P50}us, avg=${AVG_LAT}us"

FAILURES=0

# Assertion: orders_sent >= target_RPS * 9 (allow 1s of ramp loss)
MIN_ORDERS=$(( TARGET_RPS * 9 ))
if [ "$ORDERS_SENT" -lt "$MIN_ORDERS" ]; then
  echo "  FAIL: orders_sent=${ORDERS_SENT} < ${MIN_ORDERS} (target ${TARGET_RPS}RPS × 9s)"
  FAILURES=$((FAILURES + 1))
else
  echo "  PASS: orders_sent=${ORDERS_SENT} >= ${MIN_ORDERS}"
fi

# Assertion: some fills/partials/rejects
TOTAL_RESPONSES=$((FILLS + PARTIALS + REJECTS))
if [ "$TOTAL_RESPONSES" -le 0 ]; then
  echo "  FAIL: no responses (fills=${FILLS} partials=${PARTIALS} rejects=${REJECTS})"
  FAILURES=$((FAILURES + 1))
else
  echo "  PASS: ${TOTAL_RESPONSES} total responses"
fi

# Assertion: zero errors
if [ "$ERROR_COUNT" -gt 0 ]; then
  echo "  FAIL: ${ERROR_COUNT} errors"
  echo "$ERRORS" | python3 -m json.tool 2>/dev/null || echo "$ERRORS"
  FAILURES=$((FAILURES + 1))
else
  echo "  PASS: 0 errors"
fi

# Check exchange log for DISPATCH-LOST
DISPATCH_LOST=$(grep -c 'DISPATCH-LOST' "$EXCHANGE_LOG" 2>/dev/null || echo 0)
DISPATCH_ERR=$(grep -c 'DISPATCH-ERR' "$EXCHANGE_LOG" 2>/dev/null || echo 0)
if [ "$DISPATCH_LOST" -gt 0 ] || [ "$DISPATCH_ERR" -gt 0 ]; then
  echo "  FAIL: ${DISPATCH_LOST} DISPATCH-LOST + ${DISPATCH_ERR} DISPATCH-ERR in exchange log"
  FAILURES=$((FAILURES + 1))
else
  echo "  PASS: 0 dispatch drops"
fi

# Cleanup
echo ""
echo "=== Summary ==="
echo "  Target: ${TARGET_RPS} RPS"
echo "  Orders sent: $ORDERS_SENT"
echo "  Responses: $TOTAL_RESPONSES"
echo "  Errors: $ERROR_COUNT"
echo "  DISPATCH-LOST: $DISPATCH_LOST"
echo "  Failures: $FAILURES"

kill "$EXCHANGE_PID" 2>/dev/null || true
rm -f /tmp/exchange_loadtest.pid

if [ "$FAILURES" -eq 0 ]; then
  echo ""
  echo "=== LOAD TEST PASSED ==="
  exit 0
else
  echo ""
  echo "=== LOAD TEST FAILED ($FAILURES failures) ==="
  echo "Bot log: $BOT_LOG"
  echo "Exchange log: $EXCHANGE_LOG"
  exit 1
fi
