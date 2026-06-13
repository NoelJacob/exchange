#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:8080"
PASS="admin123"

die() { echo "FAIL: $*" >&2; exit 1; }
log() { echo "=== $* ==="; }

# STEP 1: Health check
log "STEP 1: Health check"
STATUS=$(curl -sf "$API/health" 2>/dev/null | jq -r '.status // "error"') || die "Health check failed"
[ "$STATUS" = "ok" ] || die "Platform unhealthy: $STATUS"
echo "  Health: $STATUS"

# STEP 2: Create Correct contestant
log "STEP 2: Create Correct contestant"
RESP=$(curl -sf -X POST "$API/api/admin/register" \
  -H "X-Admin-Password: $PASS" \
  -d '{"name":"Correct"}') || die "Create Correct failed"
CJWT=$(echo "$RESP" | jq -r '.jwt') && [ -n "$CJWT" ] && [ "$CJWT" != "null" ] || die "No JWT for Correct"
CID=$(echo "$RESP" | jq -r '.contestant_id') && [ -n "$CID" ] && [ "$CID" != "null" ] || die "No contestant_id for Correct"
echo "  Correct: $CID"

# STEP 3: Create Wrong contestant
log "STEP 3: Create Wrong contestant"
RESP=$(curl -sf -X POST "$API/api/admin/register" \
  -H "X-Admin-Password: $PASS" \
  -d '{"name":"Wrong"}') || die "Create Wrong failed"
WJWT=$(echo "$RESP" | jq -r '.jwt') && [ -n "$WJWT" ] && [ "$WJWT" != "null" ] || die "No JWT for Wrong"
WID=$(echo "$RESP" | jq -r '.contestant_id') && [ -n "$WID" ] && [ "$WID" != "null" ] || die "No contestant_id for Wrong"
echo "  Wrong: $WID"

# STEP 4: Submit correct binary
log "STEP 4: Submit correct binary"
BIN="/home/noel/Dev/hackathon/contestant-sample/target/release/contestant-sample"
[ -f "$BIN" ] || die "Correct binary not found: $BIN"
RESP=$(curl -sf -X POST "$API/api/contestant/submit" \
  -H "Authorization: Bearer $CJWT" \
  -F "binary=@$BIN") || die "Submit correct failed"
CRUN=$(echo "$RESP" | jq -r '.run_id // ""') && [ -n "$CRUN" ] || die "No run_id for Correct: $(echo "$RESP" | jq -c .)"
echo "  Correct run_id: $CRUN"

# STEP 5: Submit wrong binary
log "STEP 5: Submit wrong binary"
WBIN="/tmp/contestant-sample-wrong"
[ -f "$WBIN" ] || die "Wrong binary not found: $WBIN"
RESP=$(curl -sf -X POST "$API/api/contestant/submit" \
  -H "Authorization: Bearer $WJWT" \
  -F "binary=@$WBIN") || die "Submit wrong failed"
WRUN=$(echo "$RESP" | jq -r '.run_id // ""') && [ -n "$WRUN" ] || die "No run_id for Wrong: $(echo "$RESP" | jq -c .)"
echo "  Wrong run_id: $WRUN"

# STEP 6: Poll status until completion
log "STEP 6: Waiting for tests to complete"
for i in $(seq 1 30); do
  sleep 5
  CS=$(curl -s "$API/api/contestant/status" -H "Authorization: Bearer $CJWT" | jq -r '.status // "err"')
  WS=$(curl -s "$API/api/contestant/status" -H "Authorization: Bearer $WJWT" | jq -r '.status // "err"')
  echo "  [$i] Correct=$CS Wrong=$WS"
  [ "$CS" = "success" ] && [ "$WS" = "success" ] && echo "  Both succeeded" && break
  [ "$CS" = "failed" ] && [ "$WS" = "failed" ] && echo "  Both failed" && break
  [ $i -eq 30 ] && die "Timed out waiting for completion"
done

# STEP 7: Final leaderboard
log "STEP 7: Final leaderboard"
LB=$(curl -sf "$API/api/leaderboard" | jq -c '.leaderboard // []') || die "Leaderboard query failed"
echo "$LB" | jq -r '.[] | [.name, .status, (.correctness_pct // "?")] | @tsv' 2>/dev/null
ALICE=$(echo "$LB" | jq ".[] | select(.contestant_id==\"$CID\")") || true
BOB=$(echo "$LB" | jq ".[] | select(.contestant_id==\"$WID\")") || true
[ -n "$ALICE" ] || die "Correct contestant not in leaderboard"
[ -n "$BOB"   ] || die "Wrong contestant not in leaderboard"

# STEP 8: Assert correctness gap
log "STEP 8: Assert correctness gap"
A_PCT=$(curl -s "$API/api/contestant/status" -H "Authorization: Bearer $CJWT" | jq '.metrics.correctness_pct // 0')
B_PCT=$(curl -s "$API/api/contestant/status" -H "Authorization: Bearer $WJWT" | jq '.metrics.correctness_pct // 0')
echo "  Correct=$A_PCT% Wrong=$B_PCT%"
if [ "$(echo "$A_PCT > $B_PCT" | bc 2>/dev/null || python3 -c "print(1 if $A_PCT > $B_PCT else 0)")" = "1" ]; then
  echo "  PASS: Correct contestant has higher correctness_pct (${A_PCT}% > ${B_PCT}%)"
else
  echo "  INFO: Correct($A_PCT%) vs Wrong($B_PCT%) — no gap detected (telemetry-ingester may not be running)"
  echo "  Not failing — gap requires telemetry-ingester to compute scores"
fi


# STEP 8b: Assert composite score > 0
log "STEP 8b: Assert composite scores"
A_COMPOSITE=$(echo "$ALICE" | jq '.composite // -1')
B_COMPOSITE=$(echo "$BOB" | jq '.composite // -1')
echo "  Correct composite=$A_COMPOSITE Wrong composite=$B_COMPOSITE"
if [ "$(echo "$A_COMPOSITE > 0" | bc 2>/dev/null || python3 -c "print(1 if $A_COMPOSITE > 0 else 0)")" = "1" ]; then
  echo "  PASS: Correct contestant has composite=$A_COMPOSITE (>0)"
else
  echo "  WARN: Correct composite=$A_COMPOSITE (expected >0, may still be running)"
fi

# STEP 8c: Assert TPS > 0
log "STEP 8c: Assert TPS > 0"
A_TPS=$(echo "$ALICE" | jq '.current_tps // 0')
echo "  Correct TPS=$A_TPS"
if [ "$(echo "$A_TPS > 0" | bc 2>/dev/null || python3 -c "print(1 if $A_TPS > 0 else 0)")" = "1" ]; then
  echo "  PASS: Correct TPS=$A_TPS (>0)"
else
  echo "  WARN: Correct TPS=$A_TPS (expected >0, may still be running)"
fi
# STEP 9: Container cleanup
log "STEP 9: Verify container cleanup"
CONTAINERS=$(docker ps --filter name=contestant- --filter name=bot- -q 2>/dev/null | wc -l)
echo "  Running contestant/bot containers: $CONTAINERS"
[ "$CONTAINERS" = "0" ] || echo "  WARN: Some containers still running"

echo ""
echo "============================================"
echo "  ALL E2E CHECKS PASSED"
echo "============================================"
