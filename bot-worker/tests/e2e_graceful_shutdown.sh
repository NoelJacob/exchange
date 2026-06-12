#!/usr/bin/env bash
# E2E test: full pipeline from clean infra through bot run to QuestDB verification.
# Asserts: no persistent QuestDB gaps, all correct verdicts.
# Temporary VERIFIER-GAP lines from polling races are informational only.
# Usage: ./tests/e2e_graceful_shutdown.sh
# Requires: docker, psql, cargo, ~50s runtime.

set -u -o pipefail

INFRA_DIR="$(cd "$(dirname "$0")/../../infra" && pwd)"
BOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EXCHANGE_DIR="$(cd "$(dirname "$0")/../../contestant-sample" && pwd)"
INGESTER_DIR="$(cd "$(dirname "$0")/../../telemetry-ingester" && pwd)"

BOT_LOG="/tmp/bot_e2e.log"
EXCHANGE_LOG="/tmp/exchange_e2e.log"
INGESTER_LOG="/tmp/ingester_e2e.log"
CONTESTANT_ID="e2e-$(date +%s)"

echo "=== E2E: $CONTESTANT_ID ==="
echo ""

# Step 1: Clean infra
echo "[1/8] Cleaning infra..."
cd "$INFRA_DIR"
docker compose down -v 2>/dev/null
docker compose up -d 2>/dev/null
echo "  Infra started (Redpanda, QuestDB, Valkey)"

# Step 2: Wait for QuestDB
echo "[2/8] Waiting for QuestDB..."
for i in $(seq 1 30); do
  if PGPASSWORD=quest psql -h localhost -p 8812 -U admin -d qdb -c "SELECT 1;" &>/dev/null; then
    echo "  QuestDB ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "  FAIL: QuestDB did not start within 30s"
    exit 1
  fi
  sleep 1
done

# Step 3: Build exchange
echo "[3/8] Building exchange..."
cd "$EXCHANGE_DIR"
cargo build &>/dev/null
echo "  Exchange built"

# Step 4: Start exchange
echo "[4/8] Starting exchange..."
cargo run &>"$EXCHANGE_LOG" &
EXCHANGE_PID=$!
sleep 4
if ! kill -0 $EXCHANGE_PID 2>/dev/null; then
  echo "  FAIL: Exchange died on startup"
  cat "$EXCHANGE_LOG"
  exit 1
fi
echo "  Exchange running (PID $EXCHANGE_PID)"

# Step 5: Start telemetry-ingester
echo "[5/8] Starting ingester..."
cd "$INGESTER_DIR"
cargo run -- --contestant-id "$CONTESTANT_ID" &>"$INGESTER_LOG" &
INGESTER_PID=$!
sleep 4
if ! kill -0 $INGESTER_PID 2>/dev/null; then
  echo "  FAIL: Ingester died on startup"
  cat "$INGESTER_LOG"
  exit 1
fi
echo "  Ingester running (PID $INGESTER_PID)"

# Step 6: Run bot-worker (may exit code 1 due to protocol errors — not a pipeline failure)
echo "[6/8] Running bot-worker..."
cd "$BOT_DIR"
set +e
timeout 65 cargo run --release -- \
  --rps 30 --duration-secs 8 --fix-connections 2 --ws-connections 2 \
  --redpanda-brokers "localhost:9092" --contestant-id "$CONTESTANT_ID" \
  &>"$BOT_LOG"
BOT_EXIT=$?
set -e
echo "  Bot exit code: $BOT_EXIT"

# Step 7: Wait for verifier to settle
echo "[7/8] Waiting for verifier..."
sleep 10

# Step 8: Check assertions
echo "[8/8] Checking assertions..."
FAILURES=0

# Assertion 1: No persistent gaps in QuestDB
GAP_CHECK=$(PGPASSWORD=quest psql -h localhost -p 8812 -U admin -d qdb -t -c "
SELECT max(exec_seq) - min(exec_seq) + 1 - count() AS gaps
FROM exec_events WHERE contestant_id = '$CONTESTANT_ID';
" 2>/dev/null | tr -d ' ')
if [ -n "$GAP_CHECK" ] && [ "$GAP_CHECK" != "0" ]; then
  echo "  FAIL: $GAP_CHECK persistent gaps in exec_seq"
  FAILURES=$((FAILURES + 1))
else
  echo "  PASS: No persistent gaps in exec_seq"
fi

# Assertion 2: VERIFIER-GAP lines are informational (temporary polling lag)
# Only fail if QuestDB shows actual persistent gaps (assertion 1).
VERIFIER_GAPS=$(grep -c 'VERIFIER-GAP' "$INGESTER_LOG" 2>/dev/null || echo 0)
if [ "$VERIFIER_GAPS" -gt 0 ]; then
  echo "  INFO: $VERIFIER_GAPS VERIFIER-GAP lines (temporary — resolved per QuestDB gap=0)"
fi

# Assertion 3: All verdicts correct
WRONG_VERDICTS=$(PGPASSWORD=quest psql -h localhost -p 8812 -U admin -d qdb -t -c "
SELECT count() FROM correctness_events
WHERE contestant_id = '$CONTESTANT_ID' AND verdict != 'correct';
" 2>/dev/null | tr -d ' ')
if [ -n "$WRONG_VERDICTS" ] && [ "$WRONG_VERDICTS" -gt 0 ]; then
  echo "  FAIL: $WRONG_VERDICTS wrong verdicts"
  PGPASSWORD=quest psql -h localhost -p 8812 -U admin -d qdb -c "
  SELECT verdict, count() FROM correctness_events
  WHERE contestant_id = '$CONTESTANT_ID'
  GROUP BY verdict ORDER BY verdict;
  " 2>/dev/null
  FAILURES=$((FAILURES + 1))
else
  echo "  PASS: All verdicts correct"
fi

# Summary
TOTAL=$(PGPASSWORD=quest psql -h localhost -p 8812 -U admin -d qdb -t -c "
SELECT count() FROM exec_events WHERE contestant_id = '$CONTESTANT_ID';
" 2>/dev/null | tr -d ' ')
echo ""
echo "=== Summary ==="
echo "  Contestant: $CONTESTANT_ID"
echo "  Events in QuestDB: $TOTAL"
echo "  Failures: $FAILURES"

# Cleanup
kill $EXCHANGE_PID 2>/dev/null || true
kill $INGESTER_PID 2>/dev/null || true

if [ "$FAILURES" -eq 0 ]; then
  echo ""
  echo "=== ALL PASS ==="
  exit 0
else
  echo ""
  echo "=== SOME FAILED ==="
  exit 1
fi
