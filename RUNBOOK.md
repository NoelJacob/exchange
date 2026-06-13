# Production Runbook — Hackathon Platform

## Architecture Overview

```
         ┌─────────────────────────────────────────────────────────────┐
         │  Internet                                                   │
         │  Port 8080                                                  │
         │                                                             │
         │  ┌─────────────────────────────────────────────────────────┐│
         │  │  platform-api (axum)                                    ││
         │  │  • /api/admin/register — contestants                   ││
         │  │  • /api/admin/config — test parameters                 ││
         │  │  • /api/contestant/submit — upload binary              ││
         │  │  • /api/contestant/status — run status                 ││
         │  │  • /api/leaderboard — scored results                   ││
         │  │  • /api/events — SSE live updates                      ││
         │  │  • /api/internal/* — runner/bot webhooks               ││
         │  └─────────────────────────────────────────────────────────┘│
         └──────────────────────┬──────────────────────────────────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          │                     │                      │
          ▼                     ▼                      ▼
   ┌────────────┐    ┌─────────────────┐    ┌──────────────────┐
   │  Valkey     │    │  QuestDB         │    │  MinIO            │
   │  (Redis)   │    │  (Timeseries)    │    │  (Binary storage) │
   │  • config  │    │  • order_events  │    │  contestant-      │
   │  • tokens  │    │  • exec_events   │    │  binaries/        │
   │  • weights │    │  • metric_events │    └──────────────────┘
   │  • pub/sub │    │  • contest_summary│
   └────────────┘    │  • correctness    │
                     └──────────────────┘
                              ▲
                              │
   ┌──────────────────────────┴──────────────────────────┐
   │  Redpanda (Kafka-compatible event bus)              │
   │  Topics: orders, executions, metrics               │
   │                                                     │
   │  ┌─────────────────┐    ┌─────────────────────┐    │
   │  │  bot-worker      │    │  telemetry-ingester  │    │
   │  │  (sends orders,  │    │  (consumes execs,   │    │
   │  │   receives exec) │    │   verifies fills,   │    │
   │  │   → Redpanda     │    │   scores composites)│    │
   │  └─────────────────┘    └─────────────────────┘    │
   └────────────────────────────────────────────────────┘
```

## Prerequisites

### System Requirements
- **OS**: Linux (tested on Arch/CachyOS, should work on Ubuntu 24.04+)
- **Docker**: 24+ with Compose v2 plugin
- **Rust**: nightly (for `fixer-fix` / `orderbook-rs` dependencies)
- **Make**: 4.x
- **Memory**: 8GB+ RAM (QuestDB + Redpanda are memory-heavy)
- **Disk**: 10GB free for Docker images + build artifacts

### Hosts / DNS
Containers communicate by hostname on the `infra_default` bridge network:
- `target-host` (the FIX server being tested) resolves via Docker DNS
- `redpanda:9092` — Redpanda bootstrap
- `valkey:6379` — Valkey
- `questdb:8812` — QuestDB PostgreSQL wire protocol
- `minio:9000` — MinIO S3 API
- `127.0.0.11` — Docker embedded DNS (required for container name resolution)

## Configuration

All services are configured via environment variables (defaults shown):

### platform-api
| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `QUESTDB_URL` | `127.0.0.1:8812` | QuestDB Postgres endpoint |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Valkey/Redis URL |
| `JWT_SECRET` | `dev-secret` | HS256 JWT signing key |
| `DOCKER_HOST` | `unix:///var/run/docker.sock` | Docker daemon socket |
| `MINIO_URL` | `http://minio:9000` | MinIO S3 endpoint |
| `MINIO_BUCKET` | `contestant-binaries` | S3 bucket for contestant binaries |
| `INTERNAL_TOKEN` | `shared-secret-token` | Shared secret for runner webhooks |
| `RUNNER_IMAGE` | `infra-runner:latest` | Docker image for test runners |

### telemetry-ingester
| Variable | Default | Description |
|---|---|---|
| `QUESTDB_URL` | `127.0.0.1:8812` | QuestDB endpoint |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Valkey URL |
| `REDPANDA_BROKERS` | `127.0.0.1:9092` | Redpanda bootstrap |
| `POLL_INTERVAL_SECS` | `2` | Verifier poll interval |
| `GAP_TIMEOUT_SECS` | `30` | Max wait for out-of-order exec_events |

### bot-worker
| Argument | Default | Description |
|---|---|---|
| `--target-host` | **required** | FIX server hostname |
| `--fix-port` | `9090` | FIX TCP port |
| `--ws-port` | `8080` | WebSocket port |
| `--rps` | `30` | Target orders per second |
| `--duration-secs` | `20` | Test duration |
| `--fix-connections` | `4` | FIX session count |
| `--ws-connections` | `4` | WebSocket connection count |
| `--redpanda-brokers` | `127.0.0.1:9092` | Redpanda endpoints |
| `--contestant-id` | **required** | Assigned contestant ID |

### Admin Configuration (API)
Weights stored in Valkey `config:weights` hash, defaults:

| Key | Default | Purpose |
|---|---|---|
| `correctness_weight` | `0.40` | Correctness score weight |
| `tps_weight` | `0.35` | Throughput score weight |
| `p99_weight` | `0.25` | Latency score weight |

## Building

### 1. Build all binaries (release)
```bash
make build
```
This runs `cargo build --release` in:
- `contestant-sample/` — the sample exchange FIX/WS server
- `bot-worker/` — the load-test bot that generates orders
- `telemetry-ingester/` — consumes events, verifies correctness, scores
- `platform-api/` — HTTP API server

### 2. Build Docker images
```bash
make build-docker
make runner
```
- `platform-api` → `infra-platform-api:latest`
- `telemetry-ingester` → `infra-telemetry-ingester:latest`
- `runner` → `infra-runner:latest`
- `bot-worker` → `infra-bot-worker:latest`

### 3. (Optional) Create wrong binary for correctness gap testing
```bash
# Build a version that will produce incorrect results
# (e.g., swap SIDE on orders)
# Copy to expected path for e2e script
cp contestant-sample/target/release/contestant-sample /tmp/contestant-sample-wrong
# Manually modify the wrong binary's behavior (the e2e script expects a wrong binary at /tmp/contestant-sample-wrong)
```

## Running

### Quick start (uses Docker multi-stage build)

```bash
# Build images + start everything
make start
```

This builds all Rust services inside Docker (`platform-api`, `telemetry-ingester`, `bot-worker`, `runner`) and starts: QuestDB, Valkey, Redpanda, MinIO, platform-api (port 8080), telemetry-ingester.
The `runner` and `bot-worker` images are pre-built but NOT started as compose services — they spawn on-demand per test run.

Wait ~15 seconds for QuestDB + Redpanda to become healthy, then verify:
```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

### Local binary start (faster iteration)

```bash
# Compile locally, then start with local binaries injected into containers
make start-local
```

This compiles all Rust binaries on your host with `cargo build --release`, then builds lightweight Docker images that COPY from the local `target/release/` directory instead of re-compiling inside Docker. Much faster when iterating on code — only `target/` contents that changed are re-linked.

Infrastructure images (QuestDB, Valkey, Redpanda, MinIO, runner) are built normally — only the Rust services use the local compilation path.

### Starting infrastructure only (for development / test workflows)

```bash
make infra
```
Starts only QuestDB, Valkey, Redpanda, MinIO — useful when running platform-api from source with `cargo run`.

### Per-service restarts (debugging)

```bash
docker compose -f infra/docker-compose.yml logs -f platform-api
docker compose -f infra/docker-compose.yml logs -f telemetry-ingester
```

### Setting admin config (before first contestant — optional)

Configure test parameters common to all contestants via the admin API. Config is locked after the first contestant is registered.

```bash
# Set RPS, duration, and score weights (all optional — see defaults below)
curl -X PUT http://localhost:8080/api/admin/config \
  -H "X-Admin-Password: admin123" \
  -H "Content-Type: application/json" \
  -d '{
    "rps": 30,
    "duration_secs": 25,
    "correctness_weight": 0.40,
    "tps_weight": 0.35,
    "p99_weight": 0.25
  }' | jq .

# Read current config
curl http://localhost:8080/api/admin/config | jq .
```

### Defaults (when config is not set)

If the admin does NOT configure parameters before registering contestants, these built-in defaults apply:

| Parameter | Default |
|---|---|
| **rps** | 30 orders/second |
| **duration_secs** | 15 seconds per test run |
| **correctness_weight** | 0.40 |
| **tps_weight** | 0.35 |
| **p99_weight** | 0.25 |

Config can be set at any time before the first `admin_register` call, after which it is locked.
Uses `make admin-config` to see current values, or the curl command above.

## Submitting Contestants

### Via Admin API (end-to-end flow)

#### a) Set admin config (optional — defaults apply)

See "Setting admin config" above. If skipped, built-in defaults (30rps/15s) are used.

#### b) Register contestants

```bash
# Register "Correct" contestant
curl -X POST http://localhost:8080/api/admin/register \
  -H "X-Admin-Password: admin123" \
  -H "Content-Type: application/json" \
  -d '{"name":"Correct"}' | jq .

# Register "Wrong" contestant
curl -X POST http://localhost:8080/api/admin/register \
  -H "X-Admin-Password: admin123" \
  -H "Content-Type: application/json" \
  -d '{"name":"Wrong"}' | jq .
```

Save the `jwt` and `contestant_id` from each response.

#### c) Submit binaries (binary only — rps/duration are set by admin)

```bash
# Submit correct binary
curl -X POST http://localhost:8080/api/contestant/submit \
  -H "Authorization: Bearer $CJWT" \
  -F "binary=@contestant-sample/target/release/contestant-sample" | jq .

# Submit wrong binary
curl -X POST http://localhost:8080/api/contestant/submit \
  -H "Authorization: Bearer $WJWT" \
  -F "binary=@/tmp/contestant-sample-wrong" | jq .
```

Save the `run_id` from each response.

#### c) Monitor status
```bash
curl -s http://localhost:8080/api/contestant/status \
  -H "Authorization: Bearer $CJWT" | jq .
# Shows: status, metrics.correctness_pct, orders_sent, fills
```

#### d) Poll until completion
```bash
for i in $(seq 1 30); do
  STATUS=$(curl -s http://localhost:8080/api/contestant/status \
    -H "Authorization: Bearer $CJWT" | jq -r '.status // "running"')
  echo "  Attempt $i: status=$STATUS"
  [ "$STATUS" = "success" ] || [ "$STATUS" = "failed" ] && break
  sleep 5
done
```

#### e) Check leaderboard
```bash
curl -s http://localhost:8080/api/leaderboard | jq .
# Fields: contestant_id, name, correctness_pct, composite,
#         current_tps, failure_reason, orders_sent, total_fills
```

#### f) Live SSE stream
```bash
curl -N http://localhost:8080/api/events
# Streams: event: keepalive\ndata: ping
#          event: leaderboard\ndata: {...}
```

## Scoring

Composite score computed by the verifier (in telemetry-ingester):

```
correctness_score = correct_fills / total_fills          (0.0–1.0)
normalized_tps    = min(current_tps / 500.0, 1.0)       (cap at max expected TPS)
normalized_p99    = 1.0 - min(p99_us / 100_000.0, 1.0)  (invert, cap at 100ms)

composite = 0.40 × correctness_score
          + 0.35 × normalized_tps
          + 0.25 × normalized_p99
```

Weights are configurable via admin API before first contestant registration.

**Hard error**: if no metric events received and total fills > 10, `composite = -1.0`.

### Failure reasons
| Reason | Trigger |
|---|---|
| `stall` | `total_fills == 0 && orders_sent > 10` |
| `correctness_fail` | `< 50%` fills correct |
| `crashed` | Bot container exits with non-zero status |
| `startup_failed` | Runner container exits before sending ready signal |
| `deploy_connect_failed` | Runner cannot connect to MinIO |
| `deploy_startup_failed` | Contestant binary fails to start |

## Makefile Targets
| Command | Description |
|---|---|
| `make build` | Build all Rust crates (release) |
| `make build-local` | Compile all Rust binaries locally (release mode) |
| `make build-wrong` | Build contestant-sample and copy to /tmp/contestant-sample-wrong |
| `make infra` | `docker compose up -d` infrastructure services |
| `make runner` | Build runner Docker image |
| `make start` | Build all Docker images + start everything (Docker multi-stage build) |
| `make start-local` | Build locally + inject into Docker via `Dockerfile.local` (faster iteration) |
| `make e2e` | Full end-to-end: infra → runner → start → submit → verify with scoring assertions |
| `make e2e-demo` | Demo script: simpler flow, single binary to both contestants |
| `make admin-create` | Create a contestant via admin API with interactive prompt |
| `make admin-config` | Show current admin config (rps, duration, weights) |
| `make submit` | Submit binary (`NAME=`, `FILE=`) |
| `make status` | Show contestant status from JWT at `/tmp/jwt-*.json` |
| `make leaderboard` | Show current leaderboard |
| `make events` | Watch SSE event stream |
| `make logs` | Tail all service logs |
| `make clean` | Remove all containers + networks + volumes |
| `make stop` | `docker compose down` |
## E2E Demo Script

```bash
bash scripts/e2e-demo.sh
```
Or via Make:
```bash
make e2e-demo
```

The demo script:
1. Builds all release binaries + Docker images
2. Starts infra (questdb, valkey, redpanda, minio)
3. Starts platform-api and telemetry-ingester
4. Creates 2 contestants via admin API
5. Submits the same binary to both
6. Polls status 20×3s
7. Shows leaderboard and contestant metrics
8. Verifies container cleanup

The full E2E (`make e2e`) additionally expects a *wrong* binary at `/tmp/contestant-sample-wrong` and asserts the correctness gap + composite > 0 + TPS > 0.

## Portal (SvelteKit)

A frontend portal lives at `portal/` in the monorepo. It connects to the platform-api on port 8080 with CORS origin `http://localhost:5173`. Start it independently:
```bash
cd portal
npm install
npm run dev
```

## Security & Sandboxing

Contestant binaries run with:
- **seccomp** — system call filtering via container runtime
- **Capability dropping** — `CAP_DROP = ALL`
- **Read-only rootfs** — contestant containers have read-only filesystems
- **cpuset-cpus** — pinned CPU set per container
- **Network isolation** — contestant can only listen on FIX/WS ports, no egress to internet

The admin password (`ADMIN_PASSWORD=admin123`, change in production) protects registration and configuration. Internal runner webhooks are authenticated via a shared `INTERNAL_TOKEN`.
### Controlling specific services
```bash
# Start individual services
docker compose -f infra/docker-compose.yml up -d questdb
docker compose -f infra/docker-compose.yml up -d valkey
docker compose -f infra/docker-compose.yml up -d redpanda
docker compose -f infra/docker-compose.yml up -d minio

# View logs
docker compose -f infra/docker-compose.yml logs -f platform-api
docker compose -f infra/docker-compose.yml logs -f telemetry-ingester
docker compose -f infra/docker-compose.yml logs -f runner
docker compose -f infra/docker-compose.yml logs -f redpanda
```

## Testing

### Unit tests
```bash
# All crates (sequential recommended for database tests)
cargo test -- --test-threads=1 -p platform-api
cargo test -- --test-threads=1 -p telemetry-ingester
cargo test -- --test-threads=1 -p bot-worker
cargo test -- --test-threads=1 -p contestant-sample
```

### E2E test
```bash
# Full pipeline: builds everything, spins up infra, submits both
# correct and wrong binaries, asserts correctness gap + composite > 0
make e2e

# Or the minimal script (requires infra+builds already done):
bash scripts/e2e-minimal.sh
```

### Integration test (telemetry-ingester)
```bash
cd telemetry-ingester
cargo test test_full_telemetry_pipeline -- --test-threads=1 --nocapture
```
⚠️ Requires Docker services running and builds all release binaries from scratch.

## Debugging

### Common issues

#### 1. `ERR Can't execute 'get': only SUBSCRIBE commands allowed`
The `leaderboard_relay` background task uses a **dedicated** Redis connection for Pub/Sub, but only if the app starts with the `redis_url` parameter properly configured. Verify:
- `platform-api` was rebuilt after the relay fix
- `cfg.redis_url` is set in env or config

#### 2. Out-of-order exec_events
The verifier polls `exec_events` by `exec_seq`. If events arrive out of order, it waits up to `GAP_TIMEOUT_SECS` (default 30s). If still missing, the gap is skipped and logged as `[VERIFIER-GAP]`.

Check:
```bash
docker compose -f infra/docker-compose.yml logs telemetry-ingester | grep VERIFIER-GAP
```

#### 3. Runner cannot connect to MinIO
The runner container needs DNS resolution for `minio:9000`. Ensure:
- Container has `--dns 127.0.0.11`
- Network is `infra_default`
- MinIO bucket `contestant-binaries` exists with public read policy

#### 4. Bot container exits immediately
Check:
```bash
docker logs $(docker ps -a --filter name=bot- --format '{{.ID}}' | head -1)
```
Common causes: cannot reach `target-host:9090`, GLIBC mismatch (fixed by Ubuntu 24.04 base image).

#### 5. Composite score is -1.0
No metric events received by telemetry-ingester after 10+ fills. Verify:
- Redpanda `metrics` topic exists and has data
- Telemetry-ingester subscribes to `metrics` topic (check logs for `[INGEST]`)
- Bot-worker configured with `--redpanda-brokers redpanda:9092`

### Useful queries

```sql
-- QuestDB: check event counts
SELECT count(*) FROM order_events;
SELECT count(*) FROM exec_events;
SELECT count(*) FROM metric_events;

-- Latest contest summary
SELECT * FROM contest_summary ORDER BY ts DESC LIMIT 10;

-- Check for exec_seq gaps
SELECT exec_seq, exec_type, cl_ord_id FROM exec_events
ORDER BY exec_seq LIMIT 100;
```

```bash
# Check Redpanda topics
docker exec infra-redpanda-1 rpk topic list
docker exec infra-redpanda-1 rpk topic consume orders --num 5
docker exec infra-redpanda-1 rpk topic consume metrics --num 5

# Check Valkey config
docker exec infra-valkey-1 redis-cli HGETALL config:weights

# Check MinIO
docker exec infra-minio-1 mc ls local/contestant-binaries/
```

## Docker Compose Services

All services defined in `infra/docker-compose.yml`:

| Service | Port(s) | Image | Purpose |
|---|---|---|---|
| `questdb` | `8812:8812` | `questdb/questdb:latest` | Timeseries database |
| `valkey` | `6379:6379` | `valkey/valkey:latest` | Redis-compatible cache/pubsub |
| `redpanda` | `9092:9092`, `9644:9644` | `docker.redpanda.com/redpandadata/redpanda:latest` | Kafka-compatible event bus |
| `minio` | `9000:9000`, `9001:9001` | `minio/minio:latest` | S3-compatible binary storage |
| `runner` | — | `infra-runner:latest` | Test runner (per-submit) |
| `platform-api` | `8080:8080` | `infra-platform-api:latest` | HTTP API |
| `telemetry-ingester` | — | `infra-telemetry-ingester:latest` | Event consumer + verifier |
| `bot-worker` | — | `infra-bot-worker:latest` | Load-test bot (per-submit) |

## State Management

**Docker stateless**: `docker compose down` or `make clean` wipes all state.

Data volumes:
- `infra_minio-data` — binary uploads (persists across `down`/`up` unless removed with `-v`)

Database tables (QuestDB, auto-created on telemetry-ingester startup):
- `order_events` — raw order submissions
- `exec_events` — execution reports (with `exec_seq` for ordering)
- `metric_events` — per-interval metrics snapshots (p50/p90/p99 + cumulative counts)
- `correctness_events` — per-fill verdicts (correct/wrong/ghost)
- `contest_summary` — aggregated per-contestant results (with composite score)

Valkey keys:
- `config:locked` — set when first contestant registered
- `config:weights` — hash: `correctness_weight`, `tps_weight`, `p99_weight`
- `cfg:default:rps` — default requests-per-second
- `cfg:default:duration_secs` — default test duration
- `token:<uuid>` — upload tokens (expiring)
- `leaderboard:updates` — PubSub channel for real-time leaderboard updates

## Stopping

```bash
make stop
```
Or equivalently:
```bash
docker compose -f infra/docker-compose.yml down
```
This stops all containers and wipes all state (databases, caches, topics). Volumes can be removed with `docker compose down -v`.

## Performance Notes

- **Telemetry-ingester** start takes ~30s as it replays unprocessed events and catches up
- **Runner** containers are per-submit and auto-cleaned after exit
- **Bot-worker** containers are per-submit and killed after completion
- QuestDB ingestion can handle 10k+ events/sec in a single container
- Redpanda default config uses in-memory storage; logs accumulate at ~200MB/hour under load
