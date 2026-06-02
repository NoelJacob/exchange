Plan approved. You MUST execute it now.

Finalized plan artifact: `local://hackathon-optimized-plan-v7.md`  
Execution may be in fresh context. Treat the finalized plan as the source of truth.

Plan
----

IICPC Summer Hackathon 2026 — Distributed Benchmarking Platform
===============================================================

Hackathon-Optimized Plan
------------------------

**Goal**: Win the hackathon. Working demo > production perfection.  
**Constraint**: No HTTPS, internal-only deployment.  
**Principle**: Cut infra complexity to the bone. Keep what demoes well (Rust perf, live charts, multi-worker load).

* * *

0\. What We Cut vs Original Plan (& Why)
----------------------------------------

Original Component

Decision

Rationale

gVisor RuntimeClass

→ **Docker resource constraints + seccomp**

CPU pinning (`--cpuset-cpus`), memory hard limit (`--memory`), read-only rootfs, `--cap-drop=ALL --security-opt=no-new-privileges`, custom seccomp profile. Not full gVisor isolation but satisfies the "prevent malicious code, fair resource allocation" requirement without a custom RuntimeClass

MinIO

→ **MinIO (lightweight Docker service)**

S3-compatible, binary stored in MinIO's Docker volume (never touches host fs). Builder downloads from MinIO during sandbox image build. Simpler than piping, safer than host volume. Single `image: minio/minio` line in compose

Redpanda (Kafka-compat)

→ **Redis Streams**

Redis already in stack for state/cache; Streams support consumer groups + ordering; one less infra service

Prometheus + Grafana

→ **Built-in Svelte leaderboard**

Custom live charts (Chart.js + WebSocket) demo _much_ better than Grafana iframes; no separate metrics pipeline

TimescaleDB

→ **PostgreSQL**

Regular PG with indexes is fine at hackathon scale (hundreds of rows, not billions); TimescaleDB adds complexity

Fleet Coordinator (separate TS service)

→ **Merged into Submission API**

Test lifecycle is ~200 lines of logic; not worth a separate deployable

Portal (SvelteKit SSR)

→ **Plain Svelte SPA**

No SSR needed; simpler build; served by a static file server or nginx

Terraform

→ **Docker Swarm stack**

Compose file IS a Swarm stack file; `docker stack deploy -c docker-compose.yml platform` deploys to multi-node Swarm; Docker Compose for local single-node dev, Swarm for cloud multi-node

Zot registry + Kaniko

→ **Direct docker build**

No in-cluster building needed outside k8s

**Remaining footprint**: 4 custom services + 3 infra (MinIO, Redis, PostgreSQL) = **7 Docker services**.

* * *

1\. Simplified Tech Stack
-------------------------

Layer

Decision

Why It Wins

Orchestration

**Docker Compose**

IaC requirement met; `docker compose up -d` deploys everything

Backend API

**TypeScript + Hono**

Lean HTTP framework; quick to build upload + admin endpoints

Load Generator

**Rust** (bot-worker)

FIX 4.2 + WebSocket client; µs-precision timing; HDR histograms

Correctness Engine

**Rust** (telemetry-ingester)

Reference order book with price-time priority validation

Frontend

**Svelte + Chart.js**

Live leaderboard with WebSocket; tiny bundle; fast iteration

Message Queue

**Redis Streams**

Replaces Redpanda; consumer groups, per-contestant ordering, no JVM

Database

**PostgreSQL**

Leaderboard persistence; simple indexed table

Cache / State

**Redis** (same instance)

Submission tokens, test status, bot config, rate limits

Binary Storage

**MinIO** (Docker volume)

S3-compatible; binary stored in MinIO's Docker volume, never touches host fs; builder pulls via `mc cp` during sandbox image build; SHA-256 in Redis for provenance

IaC

**docker-compose.yml + Makefile**

Compose for local dev; `make swarm-deploy` for multi-node Docker Swarm (cloud); meets "Docker Swarm configurations" IaC requirement verbatim

### 1.1 Rust crate changes (vs original)

    # Removed: rdkafka (replaced by redis), prometheus-client (no /metrics needed)
    # Added: fred (Redis client, pure Rust, async)
    tokio             = { version = "1", features = ["full"] }
    tokio-tungstenite = "0.24"
    fred              = "0.13"          # Pure-Rust Redis client (async, streams support)
    hdrhistogram      = "7"
    sqlx              = { version = "0.8", features = ["postgres", "runtime-tokio", "chrono"] }
    serde             = { version = "1", features = ["derive"] }
    serde_json        = "1"
    ordered-float     = "4"
    uuid              = { version = "1", features = ["v4"] }
    tracing           = "0.1"
    tracing-subscriber = "0.3"
    chrono            = { version = "0.4", features = ["serde"] }

### 1.2 TypeScript package changes (vs original)

    {
      "hono": "^4",
      "ioredis": "^5",
      "minio": "^8",
      "zod": "^3"
      // Removed: kafkajs, @kubernetes/client-node, jose
    }

* * *

2\. System Architecture
-----------------------

    ┌──────────────────────────────────────────────────────────────────┐
    │  Contestants' Browsers (any number, join at any time)             │
    │  Alice:  GET /submit/<token>  →  upload binary                    │
    │  Bob:    GET /submit/<token>  →  upload binary                    │
    │  Carol:  GET /leaderboard     →  live WebSocket stream            │
    └──────────┬────────────────────────────┬──────────────────────────┘
               │ HTTP                       │ WS (live leaderboard)
               ▼                            ▼
      ┌────────────────────┐    ┌──────────────────────────┐
      │  Submission API     │    │  Portal (Svelte SPA)     │
      │  (Hono / TS) :3000  │    │  :8080                   │
      │                     │    │                          │
      │  Handles N concurrent│   │  • Live leaderboard table│
      │  uploads + test      │   │    (all contestants,     │
      │  lifecycles via      │   │     ranked by composite) │
      │  per-contestant      │   │  • Status badges:        │
      │  async tasks         │   │    running / failed /    │
      └────┬──────┬─────────┘   │    maxed (passed)         │
           │      │             │  • Per-contestant charts  │
           │      │  Redis      │  • Join at any time: new  │
           │      │  Streams    │    rows appear live       │
           │      │  (orders,   └──────────┬────────────────┘
           │      │   metrics,             │
           │      │   executions)          │ WS (telemetry-ingester)
           │      ▼                        │ broadcasts snapshot
           │  ┌────────────────────────────────────┐     every 5s
           │  │  Redis (shared across contestants)  │
           │  │  • Streams partitioned by           │
           │  │    contestantId (orders, executions,│
           │  │    metrics, bot.events)             │
           │  │  • Per-contestant keys:             │
           │  │    token:{uuid}, test:{id}:status,  │
           │  │    bot:{id}:target_rps              │
           │  └────────────────────────────────────┘
           │
           │  ┌──────────────────────────────────────────────┐
           │  │  PostgreSQL                                   │
           │  │  • leaderboard_metrics — upsert per contestant│
           │  │  • test_runs — one row per test run           │
           │  │  • All rows visible regardless of status      │
           │  └──────────────────────────────────────────────┘
           │
           ▼
      ┌──────────────────────────────────────────────────────────────┐
      │  Per-Contestant Sandbox + Bot Fleet (independent lifecycle)   │
      │                                                                │
      │  ┌──── Contestant Alice ────┐  ┌──── Contestant Bob ──────┐  │
      │  │  Sandbox (cpu-pinned,    │  │  Sandbox (cpu-pinned,    │  │
      │  │  memory-limited)         │  │  memory-limited)         │  │
      │  │  PORT_FIX=9090 :8080     │  │  PORT_FIX=9190 :8180    │  │
      │  │                          │  │                          │  │
      │  │  bot-A1  bot-A2  bot-A3  │  │  bot-B1  bot-B2         │  │
      │  │  (FIX+WS) (FIX+WS) (rest)│  │  (FIX+WS) (rest)        │  │
      │  └──────────────────────────┘  └──────────────────────────┘  │
      │  Status: running               Status: failed (p99 timeout)  │
      └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
      ┌──────────────────────────────────────────────────────────────┐
      │  Telemetry Ingester (Rust)                                   │
      │  • Per-contestant consumer groups on Redis Streams           │
      │  • N concurrent reference OrderBooks (one per contestant)    │
      │  • N concurrent HDRHistograms                                │
      │  • Every 5s: flush all contestants → PostgreSQL              │
      │  • WebSocket broadcast: full leaderboard snapshot to Portal  │
      └──────────────────────────────────────────────────────────────┘

### Data Flow Summary

    Contestant uploads binary → API stores in MinIO, spawns sandbox
      → bot fleet controller starts per-contestant async task
      → bots ramp RPS independently of other contestants
      → ingester processes all streams concurrently (one OrderBook per contestantId)
      → every 5s: aggregate metrics → PostgreSQL upsert
      → ingester broadcasts full leaderboard (all contestants, any status) via WebSocket
      → Portal renders ranked table + live charts
    
    New contestants join at any time: new row appears in leaderboard,
    new sandbox + bot fleet start independently.

* * *

3\. Service Specifications
--------------------------

### 3.1 Submission API (`services/submission-api/`)

**TypeScript + Hono. Port 3000.** Handles N concurrent uploads — each spawns an independent async task for sandbox lifecycle + bot fleet control.

Route

Method

Purpose

`/submit/:token`

GET

Upload form (serves static HTML)

`/submit/:token`

POST

Binary upload (multipart)

`/api/admin/contestant`

POST

Create contestant → returns UUID token

`/api/admin/test/start`

POST

Manually trigger test for a contestant

`/api/admin/test/stop`

POST

Stop running test

`/api/admin/config`

GET/PUT

Read/write bot config (max\_bots, ramp\_step, weights)

`/api/status/:contestantId`

GET

Current test status (from Redis)

`/api/health`

GET

Health check

**Submission logic**:

    POST /submit/:token:
      1. Validate token in Redis → resolve contestantId
      2. Compute SHA-256 of uploaded binary (streamed, in-memory)
      3. Store binary in MinIO: bucket `submissions`, key `{contestantId}/{sha256}.bin`
      4. Redis SET submission:{contestantId} '{sha256, ts}'
      5. Redis SET test:{contestantId}:status=uploaded
      6. Build sandbox image: Dockerfile uses `mc cp` to pull binary from MinIO into image
         (Binary never touches host filesystem — stays in MinIO's Docker volume)
      7. docker run --rm sandbox-{contestantId} [isolation flags...]
      8. Wait for TCP :9090 + :8080 ready (poll every 500ms, timeout 30s)
      9. Redis SET test:{contestantId}:status=running
      10. XADD submissions * contestantId sha256
          → triggers adaptive scaling loop (Section 3.6)
      Binary persists in MinIO for replay; sandbox image is ephemeral.

**Sandbox builder**: Uses a multi-stage Dockerfile where the first stage downloads the binary from MinIO via `mc` (MinIO client), then copies it into the runner image. The MinIO credentials are build args (injected at build time, not embedded).  
**Files**:

    src/
      index.ts          — Hono app; env validation; route registration
      routes/
        submit.ts       — GET/POST /submit/:token (stores in MinIO, triggers build)
        admin.ts        — POST /api/admin/contestant, /test/start, /test/stop, /config
        status.ts       — GET /api/status/:contestantId
      lib/
        minio.ts        — MinIO client; upload binary, presigned URL for builder
        redis.ts        — ioredis client; tokens, status, config
        builder.ts      — Docker SDK: build sandbox image (pulls binary from MinIO)
        sandbox.ts      — Docker SDK: run sandbox container with isolation flags
        streams.ts      — Redis Stream producer (submissions stream)
        auth.ts         — Simple password check (env var ADMIN_PASSWORD, no JWT)

### 3.2 Bot Worker (`services/bot-worker/`)

**Rust. Env-configurable.**

Env Var

Default

Purpose

`BOT_ID`

`bot-1`

Unique bot identifier

`CONTESTANT_HOST`

`sandbox`

Target hostname

`PORT_FIX`

`9090`

FIX TCP port

`PORT_WS`

`8080`

WebSocket port

`TARGET_RPS`

`100`

Orders per second

`RANDOM_SEED`

`42`

Deterministic order sequence

`REDIS_URL`

`redis://redis:6379`

Redis for stream producer

**Protocol**: Each bot runs FIX 4.2 + WebSocket sessions in parallel tokio tasks.

**Order generation**: Same deterministic generator as original plan (SmallRng, overlapping price band \[99.50, 100.50\]).

**Measurement**: `Instant::now()` before send, after receive; latency in µs.

**Produced to Redis Streams**:

*   \`orders\` — \`{ contestant\_id, order\_id, side, price, qty, ts\_sent\_us, bot\_id }\`
*   \`executions\` — \`{ contestant\_id, order\_id, fill\_price, fill\_qty, exec\_type, ts\_recv\_us, bot\_id }\`
*   \`metrics\` — \`{ contestant\_id, latency\_us, ts, bot\_id }\`

    src/
      main.rs        — tokio main; parse env; spawn FIX + WS tasks
      fix.rs         — FIX 4.2 session (Logon, NewOrderSingle, Cancel, Heartbeat, Logout)
      ws.rs          — tokio-tungstenite client; JSON framing
      ordergen.rs    — Deterministic order generator (SmallRng)
      metrics.rs     — HDRHistogram + Redis Stream producer
      scaling.rs     — Adjustable rate via Redis config key

**FIX session**: Custom implementation, ~250 LOC, **zero FIX dependencies**. No fefix, no quickfix, no FIX library. Bare TCP + tag=value SOH-delimited encoding. 6 message types (Logon, Heartbeat, NewOrderSingle, OrderCancelRequest, ExecutionReport, Logout). Checksum verification. The entire FIX layer is `fix.rs` — one file, readable in 5 minutes.

**Why not a FIX library**: FIX 4.2 is tag=value fields delimited by 0x01 (SOH). The "protocol" is just string formatting with a checksum. Libraries like `fefix` pull in 50+ transitive deps and abstract session state that we don't need (we're always the initiator, always Logon→trade→Logout). Owning the 250 LOC means zero supply-chain risk and full control over timing instrumentation.

### 3.3 Telemetry Ingester (`services/telemetry-ingester/`)

**Rust. Core correctness + aggregation engine.**

Env Var

Default

Purpose

`REDIS_URL`

`redis://redis:6379`

Stream consumer + config

`DATABASE_URL`

`postgres://...`

PostgreSQL connection

`WS_PORT`

`3001`

WebSocket broadcast for live leaderboard

`FLUSH_INTERVAL_S`

`5`

Aggregation flush period

**Three parallel tokio tasks**:

1.  \*\*Orders consumer\*\* — reads \`orders\` stream → feeds reference OrderBook
2.  \*\*Executions consumer\*\* — reads \`executions\` stream → validates fills against expected
3.  \*\*Metrics consumer + aggregator\*\* — reads \`metrics\` stream → HDRHistogram + TPS counter

**Flush task (every 5s)**:

    p50 = hdr.value_at_quantile(0.50)
    p90 = hdr.value_at_quantile(0.90)
    p99 = hdr.value_at_quantile(0.99)
    tps = counter / 5.0
    correctness = correct_fills / total_fills
    composite = 0.40 * correctness + 0.35 * tps_score + 0.25 * latency_score
    
    INSERT INTO leaderboard_metrics (...) VALUES (...)
    ON CONFLICT (contestant_id) DO UPDATE
    
    # Broadcast via WebSocket to all connected Portal clients

**Reference OrderBook**: Same BTreeMap-based implementation as original plan (~300 LOC). Price-time priority matching. Validates fill price, quantity, and ordering.

**Files**:

    src/
      main.rs        — tokio main; spawn consumer tasks + flush task + WS server
      orderbook.rs   — Reference order book (bids/asks BTreeMap, orders HashMap)
      validator.rs   — Expected fill computation + correctness scoring
      aggregator.rs  — HDRHistogram + rolling TPS counter per contestant
      writer.rs      — sqlx bulk INSERT to PostgreSQL every 5s
      scoring.rs     — Composite score formula
      ws.rs          — tokio-tungstenite WebSocket server for live broadcast

### 3.4 Portal (`services/portal/`)

**Svelte 5 SPA + Chart.js. Port 8080.**

Nginx or simple static file server. Vite build output.

**Pages**:  
|Route|Purpose|  
|`/`|Public leaderboard: ranked table + live charts — shows ALL contestants with status badges (running / failed / maxed)|  
|`/submit/:token`|Upload form for contestants|  
|`/admin`|Password-protected admin panel (env var `ADMIN_PASSWORD`)|

**Leaderboard features**:

*   WebSocket connection to telemetry-ingester (\`ws://host:3001\`)
*   Receives full leaderboard snapshot every 5s (all contestants, any status)
*   Table columns: rank, contestant name, status badge, composite score, TPS, p50/p90/p99, correctness %
*   Status badges: 🟢 running / 🔴 failed / 🟢 maxed (passed — all bots at peak RPS)
*   New contestants appear as new table rows in real-time (join at any time)
*   Chart.js real-time line chart: p50/p90/p99 vs time (per selected contestant)
*   Bar chart: correctness % per contestant
*   Gauge: aggregate TPS across all contestants \*\*No Grafana dependency. No iframes. All native.\*\*

### 3.5 Sandbox Resource Isolation

Every contestant sandbox container is started with these Docker flags to prevent malicious code and ensure fair resource allocation:

Concern

Enforcement

Docker flag

CPU starvation

CPU pinning to dedicated cores

`--cpuset-cpus=2,3`

Memory exhaustion

Hard memory limit (OOM kill)

`--memory=2g --memory-reservation=1g`

Disk write flood

Read-only rootfs

`--read-only`

Kernel exploit

Drop all capabilities

`--cap-drop=ALL`

Privilege escalation

Block setuid binaries

`--security-opt=no-new-privileges`

Syscall attack surface

Seccomp default (or custom profile)

`--security-opt=seccomp=./seccomp-sandbox.json`

Network abuse

Isolated network, no egress

Custom Docker network, no `--network host`

Side-channel via /proc,/sys

Mask sensitive mounts

`--security-opt mask=/proc/acpi:/proc/kcore:/proc/keys`

**Custom seccomp profile** (`infra/seccomp-sandbox.json`): Starts from Docker's default profile and removes `clone`, `ptrace`, `perf_event_open`, `bpf`, `swapon`, `swapoff`, `mount`, `umount`, `reboot`. ~50 lines JSON.

**Sandbox Dockerfile** (`infra/sandbox.Dockerfile`):

    FROM debian:bookworm-slim AS runner
    RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 libgcc-s1 ca-certificates && rm -rf /var/lib/apt/lists/*
    RUN mkdir -p /tmp /binary && chmod 777 /tmp /binary
    USER 1000:1000
    COPY contestant /binary/contestant
    ENTRYPOINT ["/binary/contestant"]

Submission API builds this image per contestant using the uploaded binary, then runs it with the isolation flags above.

**Resource fairness guarantee**: Each sandbox gets dedicated CPU cores and a hard memory ceiling. The platform host has enough cores to run multiple sandboxes concurrently (each pinned to disjoint core sets). If a sandbox exceeds memory, Docker's OOM killer terminates it and the test is marked `failed`.

### 3.6 Bot Fleet Controller (RPS Ramp per Bot, Per-Contestant)

Runs as an independent async task per contestant in the Submission API. Each contestant gets its own bot fleet that ramps independently — Alice's bots don't wait for Bob's, and vice versa. Contestants can join at any time: a new upload spawns a fresh fleet controller task without affecting running tests.

Each bot ramps its send rate (RPS) upward until it hits a per-bot ceiling, then the next bot spawns. This measures how much **concurrent load** the contestant's binary can sustain before failing. Total concurrent load at peak = `max_rps_per_bot × max_bots`.

**Config** (Redis hash `test:config`, fixed per test run):

Key

Default

Purpose

`max_bots`

`5`

Hard ceiling on total bot count

`initial_rps`

`500`

Starting RPS for each bot

`max_rps_per_bot`

`10_000`

Per-bot RPS ceiling (bot signals completion when reached)

`ramp_step`

`500`

RPS increment per ramp tick

`ramp_interval_s`

`3`

Seconds between ramp ticks

`p99_hard_limit_ms`

`5000`

Above this → contestant failed

`request_mix_ws_ratio`

`0.50`

50% WS, 50% FIX (deterministic via RNG)

**Peak concurrent load**: `max_bots × max_rps_per_bot = 5 × 10,000 = 50,000 RPS`. A contestant that survives all bots at peak RPS without triggering any failure criterion passes with `outcome=success`.

**Algorithm** (runs in Submission API per contestant):

    1. Spawn bot-1 with INITIAL_RPS=500, MAX_RPS=10000
    2. If bot-1 cannot establish connection within 5s
       → record deployment_failure (platform issue)
    3. Each bot runs an independent ramp loop:
       a. Opens FIX session + WebSocket to contestant
       b. Sends orders at current target_rps using tokio::time::interval
       c. Alternates WS/FIX per request_mix_ws_ratio
       d. Every ramp_interval_s (3s):
          - Increase target_rps by ramp_step (500)
          - Update Redis key bot:{id}:target_rps for the interval adjustment
          - If new target_rps >= max_rps_per_bot:
            → Signal "bot at capacity" via Redis stream `bot.events`
            → Keep sending at max_rps_per_bot (don't stop)
       e. Measures latency per order (Instant::now())
       f. Publishes to Redis Streams: orders, executions, metrics
    4. Fleet controller (listens to `bot.events` stream):
       When "bot at capacity" received:
         if bots_spawned < max_bots:
           spawn bot-N+1
         else:
           record_success(contestant)   # all bots at full RPS, contestant survived
    5. At any point if failure criteria hit (Section 3.7):
       kill all running bots, record_failure(contestant)

**Why RPS ramp (not fixed total requests)**:

*   Judges care about \*\*concurrent throughput\*\* — how many orders per second the binary can process correctly
*   Fixed total requests rewards survivability, not throughput; RPS ramp measures the breaking point
*   Each increase in RPS stresses the contestant's order book, matching engine, and connection handling
*   The leaderboard shows the peak sustained RPS before failure as a key differentiator

**Bot worker protocol mix**: Each bot's RNG determines whether each order goes via FIX or WebSocket. The sequence is deterministic per `RANDOM_SEED` (Section 3.8), so every contestant sees the same WS/FIX interleaving.

### 3.7 Test Failure Criteria

A test keeps running until the contestant binary "fails" or all `max_bots` bots reach `max_rps_per_bot` and sustain it. Failure is defined as **any** of:

#

Criterion

Detection

Consequence

1

**p99 latency > 5000ms**

Rolling p99 from ingester exceeds `p99_hard_limit_ms`

Immediate stop, outcome=`p99_timeout`

2

**Sandbox OOM / crash**

Docker container exits with non-zero code or OOM

Ingester detects missing heartbeat → outcome=`crashed`

3

**No response for 15s**

Bot workers report connection refused/timeout for 15 consecutive seconds

outcome=`unreachable`

4

**Startup timeout**

Sandbox doesn't open TCP ports within 30s

outcome=`startup_failed`

5

**Correctness drop < 50%**

Rolling correctness ratio falls below 0.50 over 30s window

outcome=`correctness_fail`

6

**All bots at max RPS**

All `max_bots` reached `max_rps_per_bot` and contestant still responsive

outcome=`success` (peak throughput = max\_bots × max\_rps\_per\_bot)

**Outcome recording**:

    INSERT INTO test_runs (contestant_id, binary_sha256, bot_config, started_at, ended_at, outcome)
    VALUES ($1, $2, $3, $4, NOW(), $5);

The leaderboard only shows contestants with `outcome='success'`. Failed contestants get a failure reason in the admin panel.

### 3.8 Deterministic Fairness — Same Requests for Every Binary

Every contestant's binary receives **the exact same sequence of orders** (order for order, at the same pace), up to its failure point. This is the only way to make the leaderboard a true comparison of exchange engine performance, not luck of the draw.

**How it works**:

    1. Global seed = hash("IICPC-HACKATHON-2026-FIXED")   # fixed once, known to all
    2. Per-contestant seed = SHA-256(global_seed || contestant_id || submission_sequence)
    3. Per-bot seed = SHA-256(per-contestant_seed || bot_index)
    4. Bot worker initializes SmallRng from per-bot seed
    5. Order sequence is fully determined by the RNG

**Result**: Contestant A and Contestant B receive identical order streams:

*   Same per-bot RPS ramp: both start at \`initial\_rps=500\` and ramp at \`ramp\_step=500\` every \`ramp\_interval\_s=3\`
*   Same order types (45% limit buy, 45% limit sell, 5% market, 5% cancel)
*   Same protocol mix: 50% FIX, 50% WebSocket (deterministic interleaving per-bot)
*   Same prices, same quantities
*   Same per-bot RPS ceiling: \`max\_rps\_per\_bot = 10,000\`
*   The only differences: latency (how fast), correctness (how accurate), and at what RPS they fail

**This is auditable**: The platform exposes `RANDOM_SEED` per test in the admin panel. Anyone can replay the exact same order sequence against a local mock.

4\. Redis Data Model
--------------------

Key Pattern

Type

Purpose

`token:{uuid}`

String

`contestantId` (TTL 72h)

`test:{contestantId}:status`

String

`uploaded |starting |running |failed |maxed`

`test:{contestantId}:config`

Hash

`max_bots, rps_per_bot, ramp_step, p99_limit, weights`

`submission:{contestantId}`

String

`{ sha256, binary_path, ts }`

`stream:orders`

Stream

Order events (partitioned by contestantId via field)

`stream:executions`

Stream

Fill events

`stream:metrics`

Stream

Latency metrics (per contestantId)

`stream:submissions`

Stream

Upload completion triggers

`stream:bot.events`

Stream

Bot at-capacity signals (fleet controller consumer)

**Consumer groups**: `ingester-{contestantId}` for each stream, so multiple ingester replicas don't double-process.

**Why Redis Streams work**:

*   \`XREADGROUP\` with \`>\` reads only new messages per consumer group
*   Each message has a contestantId field for routing
*   Auto-claim for crash recovery (not critical at hackathon scale)
*   No disk persistence needed for hackathon (AOF appendonly=yes for safety)

* * *

5\. PostgreSQL Schema
---------------------

    CREATE TABLE leaderboard_metrics (
      time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      contestant_id TEXT NOT NULL,
      p50_us        BIGINT,
      p90_us        BIGINT,
      p99_us        BIGINT,
      tps           DOUBLE PRECISION,
      correctness   DOUBLE PRECISION,
      total_orders  BIGINT DEFAULT 0,
      correct_fills BIGINT DEFAULT 0,
      composite     DOUBLE PRECISION,
      bot_count     INT,
      PRIMARY KEY (contestant_id)  -- upsert: latest snapshot
    );
    
    -- Index for time-range queries
    CREATE INDEX idx_leaderboard_time ON leaderboard_metrics (time DESC);
    
    -- Test provenance
    CREATE TABLE test_runs (
      run_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      contestant_id TEXT NOT NULL,
      binary_sha256 TEXT NOT NULL,
      bot_config    JSONB NOT NULL,
      started_at    TIMESTAMPTZ NOT NULL,
      ended_at      TIMESTAMPTZ,
      outcome       TEXT
    );
    CREATE INDEX idx_test_runs_contestant ON test_runs (contestant_id);

6\. Scoring Formula & Correctness Validation
--------------------------------------------

### 6.1 Composite Score

    composite = 0.40 × correctness_pct + 0.35 × throughput_score + 0.25 × latency_score
    
    correctness_pct   = correct_fills / total_fills × 100
    throughput_score  = LEAST(100, (contestant_tps / global_max_tps) × 100)
    latency_score     = LEAST(100, (global_best_p99 / contestant_p99) × 100)

Admin-adjustable weights via Redis hash `test:config:weights`.

### 6.2 Correctness: How Order Matching Is Scored

The correctness score measures whether the contestant's matching engine respects **price-time priority** — the fundamental invariant of any fair exchange.

**Reference OrderBook** (in telemetry-ingester):

    Bids (sorted highest price first, then earliest timestamp):
      $100.50 @ 9:00:00.001  ← best bid, earliest
      $100.50 @ 9:00:00.005  ← same price, later = worse time priority
      $100.00 @ 9:00:00.002  ← lower price
    
    Asks (sorted lowest price first, then earliest timestamp):
      $100.75 @ 9:00:00.001  ← best ask
      $101.00 @ 9:00:00.003

**Correctness check flow**:

    1. Bot sends a NewOrder to the contestant's exchange
    2. Bot also publishes OrderEvent to Redis stream `orders`
    3. Telemetry ingester receives OrderEvent,
       runs expected_fills(&order) against reference book BEFORE adding the order
       → This computes what fills the contestant SHOULD report
       → e.g., Incoming LIMIT BUY 100 @ $100.75 matches top-of-book ASK $100.75
         Expected fill: qty=100, price=$100.75, against order X
    4. Reference book then add_order(order) — same book state as contestant
    5. Contestant processes the order and sends back ExecutionReport(s)
    6. Bot receives execution → publishes ExecutionEvent to stream `executions`
    7. Telemetry ingester compares actual ExecutionEvent against expected fills
    
    Scoring per fill attempt:
      PASS if: fill_price == expected_price AND fill_qty == expected_qty
             AND fills arrive in the same order as expected (price-time priority)
      FAIL if: wrong price, wrong qty, wrong order, extra fill, missing fill
    
    correctness = PASS / (PASS + FAIL)

**What correctness checks catch**:

Violation

Example

Detection

Price-time priority violation

Matching a later order at same price before an earlier one

Expected fill order vs actual fill order mismatch

Price improvement theft

Filling at $100.50 when top-of-book was $100.75

Fill price != expected fill price

Partial fill miscalculation

Filling 50 when should have filled 100

Fill qty != expected fill qty

Ghost fills

Reporting a fill for an order that shouldn't match

No expected fill exists for that order\_id

Lost fills

Not reporting a fill that should have happened

Expected fill exists but never appears in executions stream

Order book corruption

Bid/ask crossing, wrong side

Multiple invariants checked (bid <= ask, etc.)

**Why 40% weight**: Correctness is the hardest thing to build in a matching engine and the most important. A fast but wrong exchange is useless. The 40% weight reflects this.

* * *

7\. Infrastructure as Code
--------------------------

### 7.1 Docker Compose (Local Dev)

Single-node development. One command brings up the full stack:

    docker compose up -d

Individual service scaling:

    docker compose up -d --scale bot-worker=10 bot-worker

### 7.2 Docker Swarm (Multi-Node Cloud)

Same `docker-compose.yml` deploys as a Docker Swarm stack with zero changes:

    # On Swarm manager node:
    docker stack deploy -c docker-compose.yml platform
    
    # Scale bot workers horizontally across Swarm nodes:
    docker service scale platform_bot-worker=20
    
    # Verify:
    docker service ls
    docker stack ps platform

Swarm mode proves **horizontal scalability**: bot-worker services distribute across Swarm worker nodes; PostgreSQL and Redis use Swarm-allocated DNS for service discovery. The Compose file IS the IaC declaration — identical in content to Docker Compose, deployed differently.

### 7.3 Makefile (Orchestration)

    # Makefile — Unified automation for local + cloud
    
    .PHONY: up down build swarm-deploy swarm-ps logs clean
    
    # ── Local (Docker Compose) ──────────────────────────────
    up:
    	docker compose up -d
    
    down:
    	docker compose down
    
    build:
    	docker compose build bot-worker telemetry-ingester submission-api portal
    
    logs:
    	docker compose logs -f
    
    # ── Cloud (Docker Swarm) ────────────────────────────────
    swarm-deploy:
    	docker stack deploy -c docker-compose.yml platform
    
    swarm-rm:
    	docker stack rm platform
    
    swarm-ps:
    	docker stack ps platform
    
    swarm-logs:
    	docker service logs --follow platform_bot-worker
    
    # ── Operations ──────────────────────────────────────────
    contestant:
    	curl -s -X POST http://localhost:3000/api/admin/contestant \
    		-H "Content-Type: application/json" \
    		-d '{"name":"$(NAME)"}' | jq .
    
    bot-spawn:
    	docker compose run -d --rm bot-worker \
    		-e BOT_ID=$(ID) \
    		-e CONTESTANT_HOST=$(TARGET) \
    		-e TARGET_RPS=$(RPS)
    
    clean:
    	docker compose down -v
    	docker system prune -f

### 7.4 Swarm-Ready Compose File

    # docker-compose.yml — Works with both `docker compose` and `docker stack deploy`
    version: "3.8"
    
    services:
      postgres:
        image: postgres:16-alpine
        volumes:
          - pgdata:/var/lib/postgresql/data
          - ./infra/init.sql:/docker-entrypoint-initdb.d/init.sql
        environment:
          POSTGRES_DB: hackathon
          POSTGRES_PASSWORD: hackathon
        ports: ["5432:5432"]
        healthcheck: { test: ["CMD-SHELL", "pg_isready"], interval: 5s }
        deploy:
          resources:
            limits: { memory: "1g" }
            reservations: { memory: "512m" }
    
      redis:
        image: redis:7-alpine
        ports: ["6379:6379"]
        volumes: [redis-data:/data]
        sysctls: [net.core.somaxconn=1024]
        deploy:
          resources:
            limits: { memory: "1g" }
    
      minio:
        image: minio/minio:latest
        command: server /data --console-address ":9001"
        ports: ["9000:9000", "9001:9001"]
        volumes: [minio-data:/data]
        environment:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        healthcheck:
          test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
          interval: 5s
        deploy:
          resources:
            limits: { memory: "512m" }
    
      submission-api:
        build: services/submission-api
        ports: ["3000:3000"]
        volumes:
          - /var/run/docker.sock:/var/run/docker.sock  # sandbox lifecycle (build containers)
        depends_on: [redis, postgres, minio]
        environment:
          REDIS_URL: redis://redis:6379
          MINIO_ENDPOINT: minio:9000
          MINIO_ACCESS_KEY: minioadmin
          MINIO_SECRET_KEY: minioadmin
          DATABASE_URL: postgres://hackathon:hackathon@postgres/hackathon
          ADMIN_PASSWORD: ${ADMIN_PASSWORD:-admin}
        deploy:
          replicas: 1
          resources:
            limits: { memory: "512m" }
    
      telemetry-ingester:
        build: services/telemetry-ingester
        ports: ["3001:3001"]   # WebSocket for live leaderboard
        depends_on: [redis, postgres]
        environment:
          REDIS_URL: redis://redis:6379
          DATABASE_URL: postgres://hackathon:hackathon@postgres/hackathon
          WS_PORT: "3001"
        deploy:
          replicas: 1
          resources:
            limits: { memory: "512m" }
    
      portal:
        build: services/portal
        ports: ["8080:80"]
        depends_on: [submission-api, telemetry-ingester]
        deploy:
          replicas: 2  # can scale for more concurrent viewers
          resources:
            limits: { memory: "256m" }
    
      # Bot workers spawned on-demand by submission-api
      bot-worker:
        build: services/bot-worker
        profiles: [manual]
        depends_on: [redis]
        deploy:
          replicas: 0  # scaled up by operator or admin API
          resources:
            limits: { cpus: "1", memory: "256m" }
    
    volumes:
      pgdata:
      redis-data:
      minio-data:

8\. Repository Structure
------------------------

    .
    ├── PLAN.md
    ├── docker-compose.yml
    ├── Makefile                         # Unified automation (local compose + cloud swarm)
    ├── services/
    │   ├── bot-worker/
    │   │   ├── src/
    │   │   │   ├── main.rs
    │   │   │   ├── fix.rs
    │   │   │   ├── ws.rs
    │   │   │   ├── ordergen.rs
    │   │   │   ├── metrics.rs
    │   │   │   └── scaling.rs
    │   │   ├── Cargo.toml
    │   │   └── Dockerfile
    │   ├── telemetry-ingester/
    │   │   ├── src/
    │   │   │   ├── main.rs
    │   │   │   ├── orderbook.rs
    │   │   │   ├── validator.rs
    │   │   │   ├── aggregator.rs
    │   │   │   ├── writer.rs
    │   │   │   ├── scoring.rs
    │   │   │   └── ws.rs
    │   │   ├── Cargo.toml
    │   │   └── Dockerfile
    │   ├── submission-api/
    │   │   ├── src/
    │   │   │   ├── index.ts
    │   │   │   ├── routes/
    │   │   │   │   ├── submit.ts
    │   │   │   │   ├── admin.ts
    │   │   │   │   └── status.ts
    │   │   │   └── lib/
    │   │   │       ├── minio.ts
    │   │   │       ├── redis.ts
    │   │   │       ├── builder.ts
    │   │   │       ├── sandbox.ts
    │   │   │       ├── streams.ts
    │   │   │       └── auth.ts
    │   │   ├── package.json
    │   │   └── Dockerfile
    │   └── portal/
    │       ├── src/
    │       │   ├── App.svelte
    │       │   ├── main.js
    │       │   ├── lib/
    │       │   │   ├── Leaderboard.svelte
    │       │   │   ├── UploadForm.svelte
    │       │   │   ├── AdminPanel.svelte
    │       │   │   ├── LiveChart.svelte
    │       │   │   └── ws.js
    │       │   └── pages/
    │       │       ├── Home.svelte
    │       │       ├── Submit.svelte
    │       │       └── Admin.svelte
    │       ├── package.json
    │       └── nginx.conf
    ├── infra/
    │   ├── init.sql                       # PostgreSQL schema
    │   ├── sandbox.Dockerfile             # Contestant runner image
    │   ├── seccomp-sandbox.json           # Custom seccomp profile
    │   └── nginx.conf                     # Portal reverse proxy
    └── (binaries stored in MinIO Docker volume — never on host fs)

* * *

9\. Build Order
---------------

Step

What

Why This Order

1

`docker compose up postgres redis`

Stateful services needed first

2

`psql -f init.sql`

Create tables (auto-runs from init script)

3

`docker compose build telemetry-ingester`

Most critical code; reference OrderBook + validator

4

`docker compose up telemetry-ingester`

Start stream consumers

5

`docker compose build bot-worker`

FIX + WS client; test against a mock echo server

6

`docker compose build submission-api`

Upload endpoints + sandbox lifecycle

7

`docker compose up submission-api`

Now the platform accepts uploads

8

`docker compose build portal`

Frontend with Chart.js leaderboard

9

`docker compose up portal`

Full stack operational

10

Build sample contestant binary (echo server)

End-to-end smoke test

11

`make swarm-deploy`

Cloud multi-node deployment via Docker Swarm

12

End-to-end demo

Upload → deploy → spawn bots → live leaderboard

* * *

10\. Verification / Demo Script (Multi-Contestant Demo)
-------------------------------------------------------

1.  \*\*Create contestant Alice\*\*: \`curl -X POST .../api/admin/contestant -d '{"name":"Alice"}'\` → returns upload URL
2.  \*\*Alice uploads binary\*\*: Open URL in browser, upload \`contestant-a.bin\`
3.  \*\*System auto-builds sandbox, spawns bot fleet, leaderboard shows Alice as "running"\*\*
4.  \*\*Wait 30s\*\*: Watch Alice's p50/p90/p99 charts populate, TPS ramp up on leaderboard
5.  \*\*Create contestant Bob\*\* (while Alice is still running): \`curl -X POST .../api/admin/contestant -d '{"name":"Bob"}'\`
6.  \*\*Bob uploads binary\*\*: Upload \`contestant-b.bin\` — leaderboard now shows both Alice and Bob
7.  \*\*Alice's test finishes\*\* (either fails or maxes out) → status changes to "failed" or "maxed"
8.  \*\*Bob's test continues\*\* independently — unaffected by Alice's completion
9.  \*\*Create contestant Carol\*\* after Alice finishes: upload, watch her appear on the leaderboard
10.  \*\*Admin panel\*\*: Adjust weights, view per-contestant stats, see status history

**Key demo moments for judges**:

*   \*\*Concurrent runners\*\*: Multiple contestants running simultaneously with independent bot fleets
*   \*\*Join at any time\*\*: New contestant appears as a new row mid-demo
*   \*\*Status badges\*\*: Running → failed/maxed transitions visible live
*   \*\*Leaderboard ranking\*\*: Composite score updates every 5s across all contestants
*   \*\*Deterministic fairness\*\*: Both binaries get the exact same order sequence — pure A/B comparison

* * *

11\. What NOT to Build
----------------------

Aspect

Choice

Auth

Single `ADMIN_PASSWORD` env var (set at build time). No JWT, no OAuth, no multi-tenant

TLS

None. HTTP only (internal-only per requirement)

Persistent Redis

AOF appendonly=yes but no cluster, no sentinel

PG replication

Single instance, no replica

Load balancing

Docker Compose DNS round-robin for bot → sandbox

FIX session recovery

Quick reconnect on disconnect, no persistent session state

Unit tests

Integration/smoke test only. Wait until after hackathon for the full suite

CI/CD

None. Manual `docker compose up`

* * *

12\. Scoring Against Deliverables
---------------------------------

Deliverable

How We Meet It

Working Prototype

Full pipeline: upload → MinIO → deploy → load test → results. One command to start. Malicious-code prevention via `--cap-drop=ALL`, `--read-only`, seccomp, CPU pinning, memory limits. Binaries stored in MinIO Docker volume (not host fs)

Architecture Blueprint

This PLAN.md + inline code docs

IaC

`docker-compose.yml` + `Makefile` — single-node via `docker compose`, multi-node via `docker stack deploy -c docker-compose.yml platform`. Meets "Docker Swarm configurations" spec. Horizontal scaling: `docker service scale platform_bot-worker=20`

You MUST execute this plan step by step from `local://hackathon-optimized-plan-v7.md`. You have full tool access.

You MUST verify each step before proceeding to the next.

You MUST keep going until complete. This matters.