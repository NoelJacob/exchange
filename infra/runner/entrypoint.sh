#!/bin/bash
set -euo pipefail
echo "[RUNNER] Starting run $RUN_ID"
BINARY_PATH="/tmp/contestant.bin"
MC_FLAGS="--config-dir /tmp/.mc"

echo "[RUNNER] Downloading binary from MinIO: $MINIO_URL/contestant-binaries/$RUN_ID/contestant.bin"
mc $MC_FLAGS alias set myminio "$MINIO_URL" "$MINIO_USER" "$MINIO_PASSWORD" 2>&1 | sed 's/^/[RUNNER] mc: /'
if ! mc $MC_FLAGS cp "myminio/contestant-binaries/$RUN_ID/contestant.bin" "$BINARY_PATH" 2>&1; then
    echo "[RUNNER] FATAL: Failed to download binary from MinIO"
    curl -sf -X POST "$PLATFORM_API_URL/api/internal/runner-failed" \
        -H "Content-Type: application/json" \
        -H "X-Internal-Token: $INTERNAL_TOKEN" \
        -d "{\"run_id\":\"$RUN_ID\",\"error\":\"startup_failed\"}" || true
    exit 1
fi

chmod +x "$BINARY_PATH"
echo "[RUNNER] Binary downloaded and ready. Starting..."
"$BINARY_PATH" > /tmp/binary_stdout.log 2>/tmp/binary_stderr.log &
BINARY_PID=$!
echo "[RUNNER] Started binary PID=$BINARY_PID"
sleep 2
if ! kill -0 $BINARY_PID 2>/dev/null; then
    echo "[RUNNER] Binary PID=$BINARY_PID exited immediately!"
    echo "[RUNNER] stdout:"
    cat /tmp/binary_stdout.log 2>/dev/null || echo "(empty)"
    echo "[RUNNER] stderr:"
    cat /tmp/binary_stderr.log 2>/dev/null || echo "(empty)"
    wait $BINARY_PID 2>&1 || true
    exit 1
fi
echo "[RUNNER] Binary PID=$BINARY_PID still running, waiting for FIX port 9090..."
echo "[RUNNER] Waiting for FIX port 9090 (timeout: 60s)..."
for i in $(seq 1 60); do
    if nc -z 127.0.0.1 9090 2>/dev/null; then
        echo "[RUNNER] FIX port 9090 ready after ${i}s"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "[RUNNER] TIMEOUT: FIX port never opened. Killing binary."
        kill $BINARY_PID 2>/dev/null
        curl -sf -X POST "$PLATFORM_API_URL/api/internal/runner-failed" \
            -H "Content-Type: application/json" \
            -H "X-Internal-Token: $INTERNAL_TOKEN" \
            -d "{\"run_id\":\"$RUN_ID\",\"error\":\"startup_failed\"}" || true
        exit 1
    fi
    sleep 1
done

echo "[RUNNER] Signalling ready for run $RUN_ID"
curl -sf -X POST "$PLATFORM_API_URL/api/internal/runner-ready" \
    -H "Content-Type: application/json" \
    -H "X-Internal-Token: $INTERNAL_TOKEN" \
    -d "{\"run_id\":\"$RUN_ID\"}" \
    && echo "[RUNNER] Ready signal sent" \
    || echo "[RUNNER] Warning: ready signal failed (race with timeout?)"

wait $BINARY_PID
EXIT_CODE=$?
echo "[RUNNER] Binary exited with code $EXIT_CODE"

curl -sf -X POST "$PLATFORM_API_URL/api/internal/runner-exited" \
    -H "Content-Type: application/json" \
    -H "X-Internal-Token: $INTERNAL_TOKEN" \
    -d "{\"run_id\":\"$RUN_ID\",\"exit_code\":$EXIT_CODE}" || true
exit $EXIT_CODE