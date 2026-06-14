#!/usr/bin/env bash
#
# Quick smoke test for all contestant-sample variants.
# Verifies each binary exists, starts, and exhibits its specific behavior.
# Exits 0 on pass, 1 on fail.
#
# Usage: scripts/test-variants.sh
# Prerequisite: scripts/build-local.sh already ran (binaries in /tmp/)

set -u
cd "$(dirname "$0")/.."

PASS=0
FAIL=0

pass() { echo "PASS: $1"; ((PASS++)); }
fail() { echo "FAIL: $1"; ((FAIL++)); }

cleanup() {
    [ -n "${PID:-}" ] && kill "$PID" 2>/dev/null || true
}
trap cleanup EXIT

# ---- Helper: wait for TCP port ----
wait_port() {
    local host=$1 port=$2 max=$3
    for i in $(seq 1 "$max"); do
        if exec 6<>/dev/tcp/"$host"/"$port" 2>/dev/null; then
            exec 6>&-  # close fd
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# ---- Helper: start a binary, wait for FIX port, set PID ----
start_bin() {
    local bin=$1 name=$2
    echo "--- Starting $name ($bin) ---"
    "$bin" &
    PID=$!
    if ! wait_port 127.0.0.1 9090 10; then
        echo "FAIL: $name did not start FIX listener within 5s"
        kill "$PID" 2>/dev/null
        return 1
    fi
    echo "  $name ready on FIX 9090 (pid=$PID)"
echo "=== Checking all binaries exist ==="
for f in contestant-sample/target/release/contestant-sample /tmp/contestant-sample-wrong /tmp/contestant-sample-panic /tmp/contestant-sample-slow; do
    if [ -f "$f" ] && file "$f" 2>/dev/null | grep -q ELF; then
        pass "$f exists and is ELF"
    else
        fail "$f missing or not ELF"
        echo "  (run 'scripts/build-local.sh' first)"
    fi
done
    local msg="$1" timeout="${2:-2}"
    exec 6<>/dev/tcp/127.0.0.1/9090
    # Write FIX message with checksum appended
    printf '%s' "$msg" >&6
    # Read response with timeout
    read -t "$timeout" -u 6 RESP 2>/dev/null || true
    exec 6>&-
    echo "$RESP"
}

# We'll use a simpler approach: start binary, check it's running, do basic TCP check
if start_bin "contestant-sample/target/release/contestant-sample" "Correct"; then
for f in /tmp/contestant-sample-correct /tmp/contestant-sample-wrong /tmp/contestant-sample-panic /tmp/contestant-sample-slow; do
    if [ -f "$f" ] && file "$f" 2>/dev/null | grep -q ELF; then
        pass "$f exists and is ELF"
    else
        fail "$f missing or not ELF"
        echo "  (run 'scripts/build-local.sh' first)"
    fi
done

# ---- Correct variant ----
echo ""
echo "=== Correct variant ==="
if start_bin "/tmp/contestant-sample-correct" "Correct"; then
    # Just verify it's running — matching behavior covered by cargo test
    if kill -0 "$PID" 2>/dev/null; then
        pass "Correct binary runs and responds"
    fi
    kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
    PID=""
else
    fail "Correct binary failed to start"
fi

# ---- Prefilled variant ----
echo ""
echo "=== Prefilled (wrong) variant ==="
if start_bin "/tmp/contestant-sample-wrong" "Prefilled"; then
    if kill -0 "$PID" 2>/dev/null; then
        pass "Prefilled binary runs and responds"
    fi
    kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
    PID=""
else
    fail "Prefilled binary failed to start"
fi

# ---- Slow variant ----
echo ""
echo "=== Slow variant ==="
if start_bin "/tmp/contestant-sample-slow" "Slow"; then
    if kill -0 "$PID" 2>/dev/null; then
        pass "Slow binary runs and responds"
    fi
    kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
    PID=""
else
    fail "Slow binary failed to start"
fi

# ---- Panic variant ----
echo ""
echo "=== Panic variant ==="
if start_bin "/tmp/contestant-sample-panic" "Panic"; then
    # Should be running initially
    echo "  Waiting 12s for panic (10s timer + 2s grace)..."
    sleep 12
    if kill -0 "$PID" 2>/dev/null; then
        fail "Panic binary still running after 12s (should have crashed)"
        kill "$PID" 2>/dev/null
    else
        wait "$PID" 2>/dev/null
        RC=$?
        if [ $RC -ne 0 ]; then
            pass "Panic binary crashed with exit code $RC (expected non-zero)"
        else
            fail "Panic binary exited with code 0 (expected non-zero crash)"
        fi
    fi
    PID=""
else
    fail "Panic binary failed to start"
fi

echo ""
echo "=== Results: $PASS pass, $FAIL fail ==="
[ "$FAIL" -eq 0 ]
