## Architecture
> This project is only partially working due to time constaints.

```
Admin
  │
  ├── curl http://localhost:8080/...           (direct)
  └── browser http://localhost:5173            (SvelteKit UI)

                    ┌───────────────────────────────────────────────────────┐
                    │  platform-api  (Rust / axum / :8080)                  │
                    │                                                       │
                    │  JSON endpoints                                       │
                    │  Auth: JWT (jsonwebtoken)                             │
                    │                                                       │
                    │  Background tasks (tokio::spawn):                     │
                    │    leaderboard_relay() — Redis SUBSCRIBE → SSE fan-out│
                    │    docker_watcher()  — sandbox crash detection        │
                    │                                                       │
                    │  Clients: bollard (docker), s3 (minio),               |
                    |  sqlx (questdb), rskafka (redpanda),                  |
                    |  fred (redis), jsonwebtoken (auth)                    │
                    └──────┬────────────────────┬───────────────────────────┘
                           │ bollard            │ fred (SET bot:*:rps,
                           │ docker.sock        │       SUBSCRIBE leaderboard:updates)
         ┌─────────────────┼───────────────────────────────┐          │
         ▼                 ▼                               ▼          │
  sandbox-alice     sandbox-bob                    bot-alice-1  bot-alice-2
  (runner image)    (runner image)                 bot-bob-1
  FIX :9090         FIX :9090     (same internal    (rskafka produce)
  WS  :8080         WS  :8080      port, docker net)      │
         │                │                               │
         └───────────────►│◄──────────────────────────────┘
                          │ FIX + WebSocket orders
                          │
                    ┌─────▼────────────────────────────────────────────────┐
                    │  Redpanda  (Kafka-compatible, :9092)                 │
                    │                                                      │
                    └──────┬───────────────────────────────────────────────┘
                           │ rskafka consume
                    ┌──────▼──────────────────────────────────────────────────────┐
                    │  telemetry-ingester  (Rust)                                 │
                    │                                                             │
                    │  Per-contestant Verifier tasks (tokio::spawn):              │
                    │    One task per contestant, each with own:                  │
                    │      reference OrderBook — price-time priority mirror       │
                    │      HDR Histogram        — real-time p99 for scaler        │
                    │      correctness counters — correct/total fills             │
                    │      exec_seq gap detection — per-contestant sequence       │
                    │                                                             │
                    │  At configurable interval (default 2s):                     │
                    │    INSERT raw latency_events batch → QuestDB                │
                    │    INSERT correctness_events batch → QuestDB                │
                    │    UPSERT contest_summary (composite) → QuestDB             │
                    │    PUBLISH leaderboard:updates {snapshot} → Redis           │
                    └──────┬──────────────────────┬───────────────────────────────┘
                           │ sqlx                 │ fred PUBLISH
                    ┌──────▼───────────────┐  ┌───▼──────────────────────┐
                    │  QuestDB (:5432)     │  │  Valkey (:6379)          │
                    │                      │  │                          │
                    │  contestants         │  │  token:{uuid} → id       │
                    │  submission_tokens   │  │  test:{id}:status        │
                    │  test_runs           │  │  bot:{id}:{n}:rps        │
                    │  latency_events      │  │  config:weights          │
                    │  correctness_events  │  │  cpu:pool (Set)          │
                    │  contest_summary     │  │  leaderboard:updates     │
                    └──────────────────────┘  │  (pub/sub channel)       │
                           │                  └──────────────────────────┘
                    Grafana reads
                    percentile_disc() SQL
                    on raw hypertables → charts

  ┌─────────────────────────────────────────────────────────────────────┐
  │  portal  (SvelteKit / :5173)                                        │
  │  Server-side: cookie auth only (hooks.server.ts + login route)      │
  │  Client-side: fetch() to platform-api:8080 for all data + mutations │
  │  EventSource to platform-api:8080/api/leaderboard/stream (SSE)      │
  │  Grafana iframe for charts (:3000)                                  │
  └─────────────────────────────────────────────────────────────────────┘
```

# Roles
Admin - Sets Test parameters and creates contestants. Default password admin123.
Contestant - Receives JWT token from admin and uses it upload binary. They can also see specific metrics on their specific page
Public - They can see the leaderboard

# What is done
This project is not fully complete but architecture and is in place and only debugging and rewriting current code is required to make it work.

## Sample contestant binary in ./contestant-sample
This is a FIX 4.2 and Websocket exchange binary. It can handle limit, market orders with GTC time set permanently and Self-trade prevention set permanently. Cancel orders are not implemented. These configs were out of scope for now. All these can be implemented trivially with minimal changes.

It uses correct FIX 4.2 protocol and a WS format inspired by FIX protocol. The FIX 4.2 and WS schema -ish files are in constant-sample folder. Once started it receives FIX at 9090 port and WS at 8080 port.

### Rust features
Enable the following features to emulate a wrong binary:
- prefilled: has prefilled orderbook with few limit orders already in
- panic_10s: panics after 10 seconds and binary crashes
- slow_submit: has a 100ms gap between the submit function taking inputs
- randomize_price: increases the price by 10% up or down randomly

## Bot worker
It sends deterministic but high RPS requests to sample. It takes in the following params and defaults are set if not provided in bot-worker/src/config.rs. Config options:
- Exchange target host
- Exchange FIX port
- Exchange WS port
- Total RPS across all connections in this process
- Starting RPS per process (ramps up to target)
- Seconds to ramp from min_rps to rps
- Redpanda brokers (empty = stdout only)
- Contestant ID for multi-contestant routing
- Test duration in seconds
- RNG seed for deterministic order sequence
- Seconds between metrics snapshot emissions
- Number of parallel FIX sessions to open
- Number of parallel WS connections to open
- Prefix for SenderCompID — each session appends index ("BOT00", "BOT01", …)
- Exchange TargetCompID for FIX

It starts at min_rps and ramps to max_rps evenly through the time given. Multiple bots are spawned when one bot notifies it has reached max capacity over Redis.

**It is feature complete and thoroughly tested**

## Platform worker
It coordinates the entire lifecycle of contestant, bot and verifier/telemetry containers and the entire thing. It has endpoints for for running and controlling with Axum. It uses Bollard API to build containers programmatically.

The platform can be interacted through website and through curl directly at the endpoint. The curl direct endpoint exists to make Test Driven Development easier and test as we go along.
After it boots up, admin configs settings via /config endpoint and then creates contestants at /register endpoint. It returns JWT token which is passed to contestant who use the JWT and POSTs binary at the /submit endpoint. They can also use the JWT to see /status endpoint which will give metrics. There is a /leaderboard public endpoint which sends current leaderboard snapshot as json. There is /sse endpoint which gives leaderboard stream.

When binaries are uploaded to /submit endpoint, they are directly uploaded to MinIO which is open source S3 compatible storage. Then instantly images with runner script from infra/runner (runner images which are prebuilt during docker startup stage) are spawned. These use the MC binary, which is the MinIO client application to download from MinIO storage and start connecting to Redpanda and run it. Redpanda forwards info to ./telemetry-ingester, where correctness of orders are tested and raw data is writtern to QuestDB.

The platform also creates bot-worker, it will spawn a nest bot when the current bot is at its full RPS for a given amount of seconds. And keep doing until everything fails or a platform limit is reached. If reached, it succeeds. The QuestDB data is used to create the leaderboard.

**Currently can only run 1 binary. Not fully tested has spightetti code**

## Verifier Ingestor
This gets data from the bot and has a list of orders received and filled. The bot only issues order from a deterministic seed. The respose which goes to bot, goes to Redpanda, which goes to QuestDB. This QuestDB is polled by ingester and spawns a verifier for each binary. The verifier is a correct exchange made from the extensive tested code of contestant-binary. This sorts transaction for each binary by SEQ number and replays them and sees if the filling by binary is correct as per verifier. Weights are assigned to RPS, correctness, etc and this finally creates a composite score.

The binary fills taker order immediately and gives notification but only gets notification for maker order filling later than when the order ws issued. Here, each order has a SEQ number and the verifier waits until the next seq arrives as order fill arrive not in order. When the next seq number arrives, the verifier continue. SEQ is a native tag in FIX and also added to custom WS specification.

**Fully tested to work with 1 binary but not multiple and code line by line audit not done**

## Redis, Redpanda, QuestDB and Minio
It is pre-made docker image used to run these services, the images are started before everything and then bot worker is made but not started and platform is made and started.
Redis is the shared config or status store, that provides configs to binaries when set by admin through curl or webpage.

Redpanda is used to buffer and support scale, currently only 1 partition is used. For single partition, it is faster than Kafka although for multi partitions, currently Kafka 4+ (without zookeeper) is better.
QuestDB was chosen because of its high throughout put but **bad choice** as it does not support all PGSQSL operations and a PGSQL client was used in Rust. It also shows poor concurrency, might be because WAL or Write Ahead Logging or Async writes was not used. TimescaleDB was perfect.
MinIO is the object storage used to store uploaded binaries and download them after runner images are built. Isolates binaries rather than storing them on any other sensitive image or on admin device.

### Security
The runner docker image is hardened with strict cpu pinning, memory limit (256mb) and other things including an sccomp profile. Gvisor or firecracker VM was considered but docker provides enough isolation using strict settings. Read only runtime was considered but the mc binary downloads the contestant binary after the runner docker boots up and building docker with contestant binary inbuilt would increase time by too much.

**Are premade images and provision and run correctly**

## Infra and Scripts
They have the docker-compose and docker-compose.local for local build of Rust component. It is used as docker compose override file. See Makefile to see how they are called. It also has a runner script which is given into each docker runner image and is used to set up binaries by downloading them from MinIo.

**Makefiles will need changing**

## Portal
Web interface to the endpoints exposed by platform-api. Can be used instead of curl ing endpoint directly with JWT. Uses Svelte for simplicity but with full features of a React app.

**Not started. Only scaffolding**

# Development Style
- The development was foundational AI heavy with DeepSeek v4 flash at xhigh using oh-my-pi harness. All working parts are not AI slop, AI was built to create a draft and let me incrementally build and customize that generated code to make it perfect.
- AI was used to generate aggressive tests to test all cases including edg
- Test driven development was used with integration test and E2E tests.
- The Rust compiler also should provide correctness guarantees for AI rather than using loosely typed language.
- The team was only me and intentionally set that way to see if it is doable by 1 person + AI. But **bad idea** manually coding, with isolated AI agent help, was faster and easier to debug.
- 1 week can make this fully production ready by just sweeping code and fixing minor bugs and adding tests

# Actually run it
The following commands are actually simple curl to endpoints formatted by the jq tool. See Makefile for more details:
The workflow to run is:
1) make start (builds binaries in docker builder in Rust and runs in docker runner)
-- OR --
1) make start-local (builds binaries in Rust locally and copied release binaries to docker)

2) Create a contestant using admin:
make admin-create NAME="Alice"

3) Use JWT received to upload binary by contestant:
make submit NAME="Alice" FILE="/path/to/binary"
-- OR USE SAMPLE --
make submit NAME="Alice" FILE="./contestant-sample/target/release/contestant-sample"

4) Get current binary status
make status NAME="Alice"

5) See leaderboard snapshot at current time:
make leaderboard

6) Stream SSE events (Use CTRL + C to STOP):
make events

7) Stop everything and clean state:
make clean

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