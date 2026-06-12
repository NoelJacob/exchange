# IICPC Summer Hackathon 2026 — Distributed Benchmarking Platform
## PLAN.md — Final (v5) — Comprehensive

---

## 0. Architecture Principle

**One backend (Rust / axum).** Platform-api is the API — curl uses it directly,
SvelteKit uses it. If SvelteKit crashes: tests keep running, bots keep firing,
metrics keep flowing. Restart SvelteKit and regain full control.

**Three data stores, clear separation of concerns.**
Redis (Valkey) — sub-millisecond hot state: tokens, test status, bot RPS hot-reload,
leaderboard pub/sub. Redpanda — durable event streams: orders, executions, metrics,
audit. TimescaleDB — persistent time-series: raw latency rows, correctness events,
scoring history, provenance.

**Grafana computes p50/p90/p99** from raw latency rows stored in TimescaleDB.
The ingester writes raw events; percentiles are SQL queries, not pre-aggregated.

---

## 1. Services

| Service | Lang | Port | Role |
|---|---|---|---|
| `platform-api` | Rust / axum | **:8080** | Single backend. All logic. Curl-able. |
| `telemetry-ingester` | Rust | internal | Redpanda consumer → correctness + raw events → TimescaleDB |
| `bot-worker` | Rust | — | Spawned per bot by platform-api via bollard |
| `portal` | SvelteKit / adapter-node | **:5173** | UI only. Calls platform-api. Cookie auth. |
| `timescaledb` | — | :5432 | Persistent time-series: raw latency, correctness, scoring history |
| `redis` | Valkey | :6379 | Hot state: tokens, test status, bot RPS, leaderboard pub/sub |
| `redpanda` | — | :9092 | Event streams: orders, executions, metrics, test.events |
| `minio` | — | :9000 | Binary storage |
| `grafana` | — | :3000 | Dashboards (percentiles computed live from raw data) |

**9 services. 3 custom code.**

---

## 2. System Architecture

```
Admin / Judge
  │
  ├── curl http://localhost:8080/...           (direct, always works)
  └── browser http://localhost:5173            (SvelteKit UI, optional)

                    ┌──────────────────────────────────────────────────────┐
                    │  platform-api  (Rust / axum / :8080)                 │
                    │                                                       │
                    │  All curl-able endpoints (§17)                        │
                    │  Auth: JWT (jsonwebtoken)                             │
                    │                                                       │
                    │  Background tasks (tokio::spawn at startup):          │
                    │    scaler_daemon()   — per-contestant FSM tick/1s     │
                    │    leaderboard_relay() — Redis SUBSCRIBE → SSE fan-out│
                    │    docker_watcher()  — sandbox crash detection        │
                    │                                                       │
                    │  Clients: bollard, s3, sqlx, rskafka, fred,          │
                    │           jsonwebtoken                                │
                    └──────┬───────────────────┬──────────────────────────┘
                           │ bollard            │ fred (SET bot:*:rps,
                           │ docker.sock        │       SUBSCRIBE leaderboard:updates)
         ┌─────────────────┼──────────────────────────────┐          │
         ▼                 ▼                               ▼          │
  sandbox-alice     sandbox-bob                    bot-alice-1  bot-alice-2
  (runner image)    (runner image)                 bot-bob-1
  FIX :9090         FIX :9090     (same internal    (rskafka produce)
  WS  :8080         WS  :8080      port, diff name)       │
         │                │                               │
         └───────────────►│◄──────────────────────────────┘
                          │ FIX + WebSocket orders
                          │
                    ┌─────▼────────────────────────────────────────────────┐
                    │  Redpanda  (Kafka-compatible, :9092)                  │
                    │                                                       │
                    │  orders          (8 partitions, key=contestantId)     │
                    │  executions      (8 partitions, key=contestantId)     │
                    │  metrics         (8 partitions, key=contestantId)     │
                    │  test.events     (1 partition, audit log)             │
                    └──────┬───────────────────────────────────────────────┘
                           │ rskafka consume
                    ┌──────▼───────────────────────────────────────────────┐
                    │  telemetry-ingester  (Rust)                           │
                    │                                                       │
                    │  Per-contestant state (HashMap, keyed by id):         │
                    │    reference OrderBook  — price-time priority mirror  │
                    │    HDR Histogram        — real-time p99 for scaler    │
                    │    correctness counters — correct/total fills         │
                    │                                                       │
                    │  Every 5s:                                            │
                    │    INSERT raw latency_events batch → TimescaleDB      │
                    │    INSERT correctness_events batch → TimescaleDB      │
                    │    UPSERT contest_summary (composite) → TimescaleDB   │
                    │    PUBLISH leaderboard:updates {snapshot} → Redis     │
                    └──────┬──────────────────────┬───────────────────────┘
                           │ sqlx                  │ fred PUBLISH
                    ┌──────▼───────────────┐  ┌───▼──────────────────────┐
                    │  TimescaleDB (:5432) │  │  Redis / Valkey (:6379)  │
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

---

## 3. Contestant Binary Contract

| Requirement | Value |
|---|---|
| Architecture | Linux AMD64 ELF (statically linked preferred; glibc dynamic OK) |
| FIX listener | TCP on `$PORT_FIX` (default 9090), FIX 4.2 |
| WS listener | TCP on `$PORT_WS` (default 8080), path `/ws` |
| Startup SLA | Both ports open within 30 s of process start |
| Egress | Blocked (no outbound network from sandbox container) |
| Filesystem | Read-only rootfs; `/tmp` writable |
| Symbol | `BENCH` (single instrument, fixed) |

### FIX messages received (bot → contestant)
| Tag 35 | Name | Key fields |
|---|---|---|
| `A` | Logon | 98=0, 108=30 |
| `D` | NewOrderSingle | 11=ClOrdID, 55=BENCH, 54=Side, 40=OrdType, 38=Qty, 44=Price |
| `F` | OrderCancelRequest | 11=ClOrdID, 41=OrigClOrdID, 55=BENCH |
| `0` | Heartbeat | 112=TestReqID (if responding to 35=1) |
| `5` | Logout | |

### FIX messages sent (contestant → bot)
| Tag 35 | Name | Key fields |
|---|---|---|
| `8` | ExecutionReport | 11=ClOrdID, 37=OrderID, 39=OrdStatus, 150=ExecType, 32=LastQty, 31=LastPx, 151=LeavesQty, 14=CumQty |
| `0` | Heartbeat | |

### WebSocket message format
```json
// Order (platform → contestant)
{ "type": "NewOrder",  "order_id": "uuid", "side": "B"|"S",
  "order_type": "LIMIT"|"MARKET", "price": 100.50, "qty": 42 }
{ "type": "Cancel", "order_id": "uuid", "orig_order_id": "uuid" }

// Execution report (contestant → platform)
{ "type": "ExecutionReport", "order_id": "uuid",
  "exec_type": "NEW"|"TRADE"|"CANCEL"|"REJECTED",
  "fill_price": 100.50, "fill_qty": 42, "leaves_qty": 0 }
```

---

## 4. Redpanda Topics

| Topic | Producer | Consumer | Partitions | Key | Retention |
|---|---|---|---|---|---|
| `orders` | bot-worker | telemetry-ingester | 8 | contestantId | 24h |
| `executions` | bot-worker | telemetry-ingester | 8 | contestantId | 24h |
| `metrics` | bot-worker | telemetry-ingester | 8 | contestantId | 24h |
| `test.events` | platform-api | (audit log) | 1 | contestantId | 7d |

Partitioned by `contestantId` on orders/executions/metrics so the ingester sees
each contestant's messages in-order — required for reference book consistency.

**Bot config hot-reload** uses Redis (`SET bot:{contestantId}:{botId}:rps {N}`),
not Redpanda. Redis SET + poll every 500ms is simpler and sub-millisecond vs
Kafka consumer overhead. Scaler writes, bot-worker reads.

**Leaderboard live updates** use Redis Pub/Sub (`PUBLISH leaderboard:updates {json}`),
not Redpanda. Ingester publishes after each 5s flush. platform-api subscribes and
fans out to all connected SSE clients.

---

## 4.5 Redis Key Schema

| Key | Type | TTL | Set by | Read by | Purpose |
|---|---|---|---|---|---|
| `token:{uuid}` | String | 72h | platform-api (on contestant create) | platform-api (on /submit) | One-shot upload token → contestantId |
| `test:{id}:status` | String | — | platform-api | scaler, health endpoint | Hot-read status without DB query |
| `bot:{contestantId}:{botIdx}:rps` | String | — | scaler daemon | bot-worker (poll 500ms) | RPS hot-reload |
| `config:weights` | Hash | — | admin PUT /api/config | ingester (scoring), scaler | Adjustable score weights |
| `cpu:pool` | Set | — | platform-api startup | scaler (SPOP/SADD) | Available CPU cores (distributed state, survives restart) |
| `leaderboard:latest` | String | — | telemetry-ingester | platform-api GET /api/leaderboard | Latest full leaderboard JSON snapshot |
| `leaderboard:updates` | Pub/Sub | — | telemetry-ingester | platform-api SSE relay | Push to SSE clients on each 5s flush |

**Why Redis for tokens (also in TimescaleDB)?** `submission_tokens` table is the
audit record. Redis is the fast lookup path — O(1) GET vs a DB query on every
upload request. Written to both on contestant creation; Redis has 72h TTL matching
token expiry.

---

## 5. TimescaleDB Schema

```sql
-- infra/init.sql

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── Auth / Submission ────────────────────────────────────────────────────────

CREATE TABLE contestants (
  contestant_id  TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE submission_tokens (
  token          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contestant_id  TEXT NOT NULL REFERENCES contestants(contestant_id),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at     TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '72 hours',
  used           BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX ON submission_tokens (contestant_id);

-- ── Test Lifecycle ───────────────────────────────────────────────────────────

CREATE TABLE test_runs (
  run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contestant_id   TEXT NOT NULL REFERENCES contestants(contestant_id),
  binary_sha256   TEXT NOT NULL,
  rng_seed        BIGINT NOT NULL,
  bot_config      JSONB NOT NULL,        -- snapshot of config at test start
  platform_ver    TEXT NOT NULL DEFAULT 'v5',
  status          TEXT NOT NULL DEFAULT 'starting',  -- starting|running|failed|success
  failure_reason  TEXT,                  -- null if success
  peak_bots       INT,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ended_at        TIMESTAMPTZ
);
CREATE INDEX ON test_runs (contestant_id, started_at DESC);

-- ── Raw Metrics (hypertables — Grafana queries these for charts) ─────────────

-- One row per order round-trip (bot send → execution report received)
CREATE TABLE latency_events (
  time           TIMESTAMPTZ NOT NULL,
  contestant_id  TEXT NOT NULL,
  latency_us     BIGINT NOT NULL,        -- microseconds
  bot_id         TEXT NOT NULL,
  run_id         UUID NOT NULL
);
SELECT create_hypertable('latency_events', 'time');
CREATE INDEX ON latency_events (contestant_id, time DESC);
SELECT add_compression_policy('latency_events', INTERVAL '1 hour');

-- One row per fill attempt (correct or incorrect)
CREATE TABLE correctness_events (
  time           TIMESTAMPTZ NOT NULL,
  contestant_id  TEXT NOT NULL,
  is_correct     BOOLEAN NOT NULL,
  violation_type TEXT,                   -- null if correct; 'price'|'qty'|'priority'|'ghost'|'lost'
  run_id         UUID NOT NULL
);
SELECT create_hypertable('correctness_events', 'time');
CREATE INDEX ON correctness_events (contestant_id, time DESC);

-- ── Summary (upserted every 5s by ingester — used for leaderboard ranking) ──

CREATE TABLE contest_summary (
  contestant_id   TEXT PRIMARY KEY,
  run_id          UUID,
  status          TEXT NOT NULL DEFAULT 'running',
  total_orders    BIGINT NOT NULL DEFAULT 0,
  correct_fills   BIGINT NOT NULL DEFAULT 0,
  total_fills     BIGINT NOT NULL DEFAULT 0,
  peak_tps        DOUBLE PRECISION NOT NULL DEFAULT 0,
  current_tps     DOUBLE PRECISION NOT NULL DEFAULT 0,
  bot_count       INT NOT NULL DEFAULT 0,
  composite       DOUBLE PRECISION,      -- recomputed by Grafana query; stored for API
  last_updated    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Grafana Queries (reference) ──────────────────────────────────────────────
-- p99 latency per contestant per 5s bucket:
--   SELECT time_bucket('5s', time) AS t, contestant_id,
--          percentile_disc(0.99) WITHIN GROUP (ORDER BY latency_us) / 1000.0 AS p99_ms
--   FROM latency_events
--   WHERE $__timeFilter(time) AND contestant_id = '$contestant'
--   GROUP BY 1, 2 ORDER BY 1
--
-- Leaderboard ranked table:
--   SELECT c.contestant_id, c.status,
--          percentile_disc(0.99) WITHIN GROUP (ORDER BY l.latency_us) / 1000.0 AS p99_ms,
--          c.current_tps AS tps,
--          ROUND((c.correct_fills::float / NULLIF(c.total_fills,0)) * 100, 1) AS correctness_pct,
--          c.composite AS score
--   FROM contest_summary c
--   LEFT JOIN latency_events l ON l.contestant_id = c.contestant_id
--     AND l.time > NOW() - INTERVAL '30 seconds'
--   GROUP BY c.contestant_id, c.status, c.current_tps, c.correct_fills,
--            c.total_fills, c.composite
--   ORDER BY c.composite DESC NULLS LAST
```

---

## 6. Grafana Setup

**Why Grafana computes percentiles**: Raw `latency_events` rows are inserted by the
ingester. Grafana queries them with `percentile_disc()` SQL (standard PostgreSQL
window function, no TimescaleDB toolkit required). This means Grafana panels can
show any percentile at any time granularity without schema changes.

**grafana.ini** (mounted at `/etc/grafana/grafana.ini`):
```ini
[security]
allow_embedding = true
[auth.anonymous]
enabled  = true
org_role = Viewer
```

**Datasource** (`infra/grafana/provisioning/datasources/timescale.yaml`):
```yaml
apiVersion: 1
datasources:
  - name: TimescaleDB
    type: postgres
    url: timescaledb:5432
    database: hackathon
    user: hackathon
    secureJsonData: { password: hackathon }
    jsonData: { sslmode: disable, timescaledb: true }
```

**Dashboard 1 — Live Leaderboard** (5s auto-refresh, table panel):
```sql
SELECT
  ROW_NUMBER() OVER (ORDER BY cs.composite DESC NULLS LAST) AS rank,
  cs.contestant_id,
  cs.status,
  cs.bot_count,
  ROUND(cs.current_tps::numeric, 0)  AS tps,
  ROUND(p.p50 / 1000.0, 1)           AS p50_ms,
  ROUND(p.p90 / 1000.0, 1)           AS p90_ms,
  ROUND(p.p99 / 1000.0, 1)           AS p99_ms,
  ROUND((cs.correct_fills::float / NULLIF(cs.total_fills,0)) * 100, 1) AS correctness_pct,
  ROUND(cs.composite::numeric, 2)     AS score
FROM contest_summary cs
LEFT JOIN LATERAL (
  SELECT
    percentile_disc(0.50) WITHIN GROUP (ORDER BY latency_us) AS p50,
    percentile_disc(0.90) WITHIN GROUP (ORDER BY latency_us) AS p90,
    percentile_disc(0.99) WITHIN GROUP (ORDER BY latency_us) AS p99
  FROM latency_events
  WHERE contestant_id = cs.contestant_id
    AND time > NOW() - INTERVAL '30 seconds'
) p ON TRUE
ORDER BY cs.composite DESC NULLS LAST
```

**Dashboard 2 — Per-Contestant Drill-Down** (variable `$contestant`):
```sql
-- p50/p90/p99 over time (time series panel):
SELECT
  time_bucket('5 seconds', time) AS time,
  percentile_disc(0.50) WITHIN GROUP (ORDER BY latency_us) / 1000.0 AS "p50 ms",
  percentile_disc(0.90) WITHIN GROUP (ORDER BY latency_us) / 1000.0 AS "p90 ms",
  percentile_disc(0.99) WITHIN GROUP (ORDER BY latency_us) / 1000.0 AS "p99 ms"
FROM latency_events
WHERE $__timeFilter(time) AND contestant_id = '$contestant'
GROUP BY 1 ORDER BY 1

-- TPS over time:
SELECT
  time_bucket('5 seconds', time) AS time,
  COUNT(*) / 5.0 AS tps
FROM latency_events
WHERE $__timeFilter(time) AND contestant_id = '$contestant'
GROUP BY 1 ORDER BY 1

-- Correctness over time:
SELECT
  time_bucket('5 seconds', time) AS time,
  ROUND(SUM(is_correct::int)::float / COUNT(*) * 100, 1) AS correctness_pct
FROM correctness_events
WHERE $__timeFilter(time) AND contestant_id = '$contestant'
GROUP BY 1 ORDER BY 1
```

---

## 7. FIX 4.2 Session — Custom Implementation (~250 LOC)

No FIX library. FIX 4.2 is `tag=value\x01` with a checksum. Six message types,
one file (`fix.rs`), zero extra dependencies.

```rust
// services/bot-worker/src/fix.rs
const SOH: u8 = 0x01;

pub fn encode(msg_type: &str, seq: u64, sender: &str, target: &str,
              fields: &[(u32, String)]) -> Vec<u8> {
    let mut body = format!("35={SOH_}49={sender}{SOH_}56={target}{SOH_}34={seq}{SOH_}",
                           SOH_ = "\x01");
    body += &format!("52={}\x01", utc_now());          // SendingTime
    for (tag, val) in fields { body += &format!("{}={}\x01", tag, val); }
    let body = format!("35={}\x01{}", msg_type, body); // prepend MsgType
    let prefix = format!("8=FIX.4.2\x019={}\x01", body.len());
    let full   = format!("{}{}", prefix, body);
    let cksum  = full.bytes().map(u32::from).sum::<u32>() % 256;
    format!("{}10={:03}\x01", full, cksum).into_bytes()
}

pub fn decode(buf: &[u8]) -> HashMap<u32, String> {
    buf.split(|&b| b == SOH)
       .filter_map(|f| {
           let s = std::str::from_utf8(f).ok()?;
           let (t, v) = s.split_once('=')?;
           Some((t.parse::<u32>().ok()?, v.to_owned()))
       })
       .collect()
}

// FIX tags used
// 8  BeginString      9  BodyLength       10 CheckSum
// 34 MsgSeqNum        35 MsgType          49 SenderCompID
// 52 SendingTime      56 TargetCompID     11 ClOrdID
// 37 OrderID          38 OrderQty         39 OrdStatus
// 40 OrdType (1=Mkt, 2=Lmt)              41 OrigClOrdID
// 44 Price            54 Side (1=Buy, 2=Sell)
// 55 Symbol           98 EncryptMethod    108 HeartBtInt
// 112 TestReqID       131 QuoteReqID      150 ExecType
// 31 LastPx           32 LastQty          14 CumQty     151 LeavesQty
```

**Session lifecycle per bot per contestant:**
```
connect TCP :9090
send 35=A Logon (98=0, 108=30)
recv 35=A Logon
loop:
  send 35=D NewOrderSingle  OR  35=F Cancel
  recv 35=8 ExecutionReport
  every 30s: send 35=0 Heartbeat (if no messages sent)
  on recv 35=1 TestRequest: send 35=0 Heartbeat with 112=TestReqID
send 35=5 Logout
```

---

## 8. Order Generation Strategy

Deterministic. Every contestant receives the **same order sequence** (§13).

```rust
// services/bot-worker/src/ordergen.rs
use rand::{SeedableRng, Rng};
use rand::rngs::SmallRng;

const SYMBOL: &str = "BENCH";

pub fn next_order(rng: &mut SmallRng, open_orders: &[String]) -> OrderRequest {
    let roll: f64 = rng.gen();
    match roll {
        r if r < 0.45 => OrderRequest::LimitBuy {
            price: 97.00 + rng.gen::<f64>() * 3.50,    // [97.00, 100.50]
            qty:   rng.gen_range(1u64..=100),
        },
        r if r < 0.90 => OrderRequest::LimitSell {
            price: 99.50 + rng.gen::<f64>() * 3.50,    // [99.50, 103.00]
            qty:   rng.gen_range(1u64..=100),
        },
        r if r < 0.95 => if rng.gen() {
            OrderRequest::MarketBuy  { qty: rng.gen_range(1u64..=50) }
        } else {
            OrderRequest::MarketSell { qty: rng.gen_range(1u64..=50) }
        },
        _ => {
            if open_orders.is_empty() { return next_order(rng, open_orders); }
            let idx = rng.gen_range(0..open_orders.len());
            OrderRequest::Cancel { orig_order_id: open_orders[idx].clone() }
        }
    }
}
```

**Price band overlap [99.50, 100.50]** ensures ~45% of limit orders are immediately
matchable → real fills → real correctness signal. Market orders always match.
5% cancel rate generates open-order management pressure.

**Protocol mix** (deterministic per-bot RNG):
- 50% of orders sent via FIX 4.2
- 50% via WebSocket JSON
- Which protocol: `if rng.gen::<bool>() { fix } else { ws }`

---

## 9. Reference Order Book & Correctness Validation

### 9.1 Reference OrderBook (~250 LOC, `orderbook.rs`)

```rust
use std::collections::{BTreeMap, VecDeque, HashMap};
use ordered_float::OrderedFloat;

#[derive(Clone, Debug)]
pub struct Order {
    pub order_id:   String,
    pub side:       Side,
    pub order_type: OrdType,
    pub price:      f64,
    pub qty:        u64,
    pub leaves_qty: u64,
    pub ts_us:      u64,    // insertion timestamp — time priority key
}

pub struct OrderBook {
    // Bids: highest price first; within price, earliest first
    bids: BTreeMap<std::cmp::Reverse<OrderedFloat<f64>>, VecDeque<Order>>,
    // Asks: lowest price first; within price, earliest first
    asks: BTreeMap<OrderedFloat<f64>, VecDeque<Order>>,
    // O(1) lookup by order_id for cancels
    index: HashMap<String, (Side, f64)>,
}

impl OrderBook {
    // Called BEFORE add_order — produces expected fills for incoming order
    pub fn expected_fills(&self, incoming: &Order) -> Vec<ExpectedFill> {
        let mut remaining = incoming.leaves_qty;
        let mut fills = vec![];
        match incoming.side {
            Side::Buy => {
                // Match against asks, lowest price first
                for (ask_price, queue) in &self.asks {
                    if incoming.order_type == OrdType::Limit
                       && ask_price.0 > OrderedFloat(incoming.price) { break; }
                    for passive in queue {
                        if remaining == 0 { break; }
                        let fill_qty = remaining.min(passive.leaves_qty);
                        fills.push(ExpectedFill {
                            passive_order_id: passive.order_id.clone(),
                            fill_price:       ask_price.0.into_inner(),
                            fill_qty,
                        });
                        remaining -= fill_qty;
                    }
                    if remaining == 0 { break; }
                }
            }
            Side::Sell => { /* mirror for asks matching against bids */ }
        }
        fills
    }

    pub fn add_order(&mut self, order: Order) { /* insert into correct side */ }
    pub fn cancel_order(&mut self, order_id: &str) { /* remove from book + index */ }
    pub fn apply_fill(&mut self, order_id: &str, fill_qty: u64) { /* reduce leaves_qty, evict if 0 */ }
}
```

### 9.2 Correctness validation flow

```
telemetry-ingester (per contestant):

Task A — consume `orders` topic:
  for each OrderEvent { order_id, side, price, qty, ts_us }:
    expected = book.expected_fills(&order)          // compute BEFORE inserting
    pending.insert(order_id, expected)              // store expected fills queue
    book.add_order(order)                           // mirror contestant's book

Task B — consume `executions` topic:
  for each ExecutionEvent { order_id, exec_type, fill_price, fill_qty }:
    if exec_type == TRADE:
      if let Some(expected_queue) = pending.get_mut(&order_id):
        if let Some(exp) = expected_queue.pop_front():
          let correct = fill_price == exp.fill_price && fill_qty == exp.fill_qty
          INSERT correctness_events { time: now(), contestant_id, is_correct: correct,
                 violation_type: if !correct { classify_violation(...) } else { None } }
          if correct { book.apply_fill(&order_id, fill_qty) }
        else:
          INSERT correctness_events { is_correct: false, violation_type: "ghost" }
    if exec_type == CANCEL:
      book.cancel_order(&order_id)
      pending.remove(&order_id)
```

**Violation types:**
| Type | Meaning |
|---|---|
| `price` | Filled at wrong price |
| `qty` | Wrong fill quantity |
| `priority` | Correct price but wrong time priority (earlier order skipped) |
| `ghost` | Fill reported for non-matching order |
| `lost` | Expected fill never arrived (detected at end of test) |

---

## 10. Adaptive Bot Scaling — Multi-Contestant

Each contestant gets a **fully independent** bot fleet and scaler state.
Multiple contestants run concurrently with no coordination between their fleets.

### Config (stored in TimescaleDB `test_runs.bot_config` JSONB at test start)

| Key | Default | Meaning |
|---|---|---|
| `initial_rps` | 500 | Starting RPS for first bot |
| `max_rps_per_bot` | 10_000 | Per-bot RPS ceiling before spawning next |
| `ramp_step` | 500 | RPS increment per tick |
| `ramp_interval_s` | 3 | Seconds between ramp ticks |
| `max_bots` | 5 | Hard ceiling on bot count |
| `p99_hard_limit_ms` | 5_000 | Failure trigger |
| `warmup_s` | 10 | Discard initial seconds |
| `saturation_window` | 10 | Ticks of <5% TPS growth = saturated |

**Peak concurrent load**: 5 bots × 10,000 RPS = **50,000 RPS per contestant**.

### Scaling FSM (runs in scaler_daemon, once per second, per active test)

```
state per contestant:
  bots:          u32          = 1
  rps_per_bot:   u32          = initial_rps
  tps_window:    VecDeque<f64> = [] (last 10 readings)
  phase:         Enum         = Warmup | Ramping | Saturated | Failed | Success

tick(contestant_id, state, metrics):
  if phase == Warmup && elapsed > warmup_s:
    phase = Ramping

  if metrics.p99_ms > p99_hard_limit:
    → fail(contestant, "p99_timeout")
    return

  if sandbox_exited(contestant_id):
    → fail(contestant, "crashed")
    return

  if phase == Ramping:
    tps_window.push_back(metrics.tps)
    if tps_window.len() > saturation_window: tps_window.pop_front()
    if tps_window.len() == saturation_window:
      growth = (tps_window.back - tps_window.front) / tps_window.front
      if growth < 0.05:
        → succeed(contestant, peak_tps=max(tps_window))
        return

    if metrics.tps >= (bots * rps_per_bot) as f64 * 0.95:
      // current capacity is saturating
      new_rps = rps_per_bot + ramp_step
      if new_rps > max_rps_per_bot:
        if bots < max_bots:
          spawn_bot(contestant_id, bots + 1, initial_rps)
          bots += 1
          rps_per_bot = initial_rps          // reset per-bot target
        else:
          → succeed(contestant, "all bots at max")
      else:
        rps_per_bot = new_rps
        redis.set("bot:{contestantId}:{botIdx}:rps", rps_per_bot)  // hot-reload via Redis
```

### Multi-contestant concurrency

```rust
// platform-api/src/scaler.rs
pub struct ScalerState {
    tests: Arc<RwLock<HashMap<String, TestState>>>,  // keyed by contestant_id
    cpu_pool: Arc<Mutex<CpuPool>>,
}

pub async fn daemon(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let ids: Vec<String> = state.scaler.tests.read().await.keys().cloned().collect();
        // All contestants ticked concurrently each second
        let tasks: Vec<_> = ids.into_iter()
            .map(|id| tokio::spawn(tick_contestant(state.clone(), id)))
            .collect();
        for t in tasks { t.await.ok(); }
    }
}
```

**CPU allocation**: `CpuPool` maintains a set of available logical cores.
Each sandbox gets `--cpuset-cpus` = 2 cores (disjoint across all running sandboxes).

```rust
pub struct CpuPool {
    available: BTreeSet<u32>,    // all logical cores minus OS/platform cores
}
impl CpuPool {
    fn allocate(&mut self, n: usize) -> Option<String> {
        if self.available.len() < n { return None; }
        let cores: Vec<u32> = self.available.iter().take(n).copied().collect();
        cores.iter().for_each(|c| { self.available.remove(c); });
        Some(cores.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(","))
    }
    fn release(&mut self, cpuset: &str) {
        cpuset.split(',').filter_map(|s| s.trim().parse().ok())
              .for_each(|c| { self.available.insert(c); });
    }
}
```

**Sandbox container naming** (Docker DNS-resolvable by bots on same network):
```
sandbox-{contestant_id}     → FIX :9090, WS :8080 (internal ports, no host binding)
bot-{contestant_id}-{idx}   → connects to sandbox-{contestant_id}:9090
```

No external port mapping needed. Docker bridge DNS resolves container names.

---

## 11. Test Failure Criteria

A test terminates when **any** criterion is met:

| # | Criterion | Detection | Outcome |
|---|---|---|---|
| 1 | p99 latency > `p99_hard_limit_ms` | Rolling p99 from HDR histogram | `p99_timeout` |
| 2 | Sandbox OOM / crash | bollard: container state != running | `crashed` |
| 3 | No response for 15s | Bot reports ConnectionRefused/Timeout × 15s | `unreachable` |
| 4 | Startup timeout | TCP ports not open after 30s | `startup_failed` |
| 5 | Correctness drop < 50% | Rolling correctness rate < 0.50 over 30s | `correctness_fail` |
| 6 | All bots at max RPS, still responsive | All `max_bots` reached `max_rps_per_bot` | **`success`** |

On termination:
```rust
UPDATE test_runs SET status=$1, failure_reason=$2, ended_at=NOW(), peak_bots=$3
WHERE run_id=$4
```
All bot containers killed. Sandbox container killed. CPU cores returned to pool.

---

## 12. Scoring Formula

```
composite = 0.40 × correctness_pct
          + 0.35 × LEAST(100, contestant_tps    / global_max_tps   × 100)
          + 0.25 × LEAST(100, global_best_p99_us / contestant_p99_us × 100)

correctness_pct = correct_fills / total_fills × 100

-- In Grafana leaderboard SQL:
WITH stats AS (
  SELECT contestant_id,
         current_tps,
         correct_fills::float / NULLIF(total_fills,0) * 100 AS correctness_pct,
         (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY latency_us)
          FROM latency_events
          WHERE contestant_id = cs.contestant_id
            AND time > NOW() - INTERVAL '30s') AS p99_us
  FROM contest_summary cs WHERE status IN ('running','success')
),
globals AS (
  SELECT MAX(current_tps) AS max_tps, MIN(p99_us) AS min_p99
  FROM stats
)
SELECT s.contestant_id,
  ROUND((0.40 * COALESCE(s.correctness_pct,0)
       + 0.35 * LEAST(100, s.current_tps / NULLIF(g.max_tps,0) * 100)
       + 0.25 * LEAST(100, g.min_p99::float / NULLIF(s.p99_us,0) * 100))::numeric, 2)
  AS composite
FROM stats s, globals g
ORDER BY composite DESC
```

Admin-adjustable weights: `PUT /api/config { "score_weights": { "correctness": 0.40, ... } }`.
Weights stored in `test_runs.bot_config` JSONB per run for reproducibility.

---

## 13. Deterministic Fairness — Same Orders for Every Contestant

Every contestant's binary receives the **identical order sequence** up to its failure
point. Pure A/B comparison of matching engine performance, not luck.

```
GLOBAL_SEED = SHA-256("IICPC-HACKATHON-2026-FIXED")   ← fixed, published to contestants

per-contestant-seed = SHA-256(GLOBAL_SEED || contestant_id || submission_sequence_number)
per-bot-seed        = SHA-256(per-contestant-seed || bot_index_as_u8)

Bot initialises SmallRng::seed_from_u64(per_bot_seed_lower_64_bits)
```

**All contestants receive:**
- Same order type distribution (45/45/5/5%)
- Same price band ([97.00, 100.50] buys, [99.50, 103.00] sells)
- Same protocol mix (50% FIX / 50% WS)
- Same RPS ramp schedule (initial_rps → max_rps_per_bot in ramp_step increments)
- Differences: only latency (how fast they respond) and correctness (whether they match correctly)

`rng_seed` stored in `test_runs` table. Any test can be replayed by re-uploading the
binary and specifying the same seed.

---

## 14. Reproducibility Guarantees

| Guarantee | Mechanism |
|---|---|
| Binary integrity | SHA-256 at upload; stored in `test_runs.binary_sha256`; verified in entrypoint.sh before exec |
| Deterministic orders | `rng_seed` in `test_runs`; SmallRng seeded from it |
| Monotonic timing | `Instant::now()` (never SystemTime); RTT measured on same Docker bridge |
| CPU isolation | `--cpuset-cpus` (disjoint cores); `resources.limits` = Guaranteed |
| Warmup discard | First `warmup_s` seconds not counted in HDR histogram or correctness |
| Config snapshot | Full bot_config JSONB in `test_runs` at START of every test |
| Replay | `run_id` → read `binary_sha256` + `rng_seed` + `bot_config` → re-upload binary → re-run |
| Audit trail | Every order, execution, latency, correctness event in TimescaleDB with `run_id` |

---

## 15. Sandbox Isolation

```rust
// platform-api/src/sandbox.rs
pub fn make_host_config(cpuset: &str, seccomp_path: &str) -> HostConfig {
    HostConfig {
        network_mode:       Some("platform_platform-net".into()),
        cpuset_cpus:        Some(cpuset.into()),
        memory:             Some(2 * 1_073_741_824),   // 2 GiB hard limit → OOM kill
        memory_reservation: Some(1 * 1_073_741_824),   // 1 GiB soft reservation
        readonly_rootfs:    Some(true),
        cap_drop:           Some(vec!["ALL".into()]),
        security_opt: Some(vec![
            "no-new-privileges".into(),
            format!("seccomp={}", seccomp_path),
        ]),
        tmpfs: Some(HashMap::from([
            ("/tmp".into(), "rw,noexec,nosuid,size=128m".into()),
        ])),
        ..Default::default()
    }
}
```

`infra/seccomp-sandbox.json`: Docker default profile minus `clone`, `ptrace`,
`perf_event_open`, `bpf`, `mount`, `umount2`, `reboot`, `swapon`, `swapoff`,
`process_vm_readv`, `process_vm_writev`. ~70 lines JSON.

**NetworkPolicy equivalent (Docker)**: Sandbox containers are on `platform-net`
but have no `--publish` flags. They are only reachable by other containers on
the same network (bot workers). They cannot reach the internet or other sandboxes'
ports because Docker bridge networking requires explicit connection initiation.

---

## 16. Binary Lifecycle (MinIO → Runner)

```
POST /submit/:token  (multipart binary):
  1. SHA-256(stream) — computed while streaming, no full buffering
  2. minio.put_object("submissions", "{contestantId}/{sha256}.bin", stream)
  3. INSERT submission_tokens SET used=TRUE WHERE token=$1
  4. INSERT test_runs { contestant_id, binary_sha256, rng_seed, bot_config, status='starting' }
  5. cpuset = cpu_pool.allocate(2)?  else { error: no cores available }
  6. docker.create_container("sandbox-{id}", runner:latest, isolation_config, env=[
       CONTESTANT_ID={id}, BINARY_SHA256={sha256},
       MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
       PORT_FIX=9090, PORT_WS=8080
     ])
  7. docker.start_container("sandbox-{id}")
  8. Poll TCP :9090 + :8080 every 500ms (timeout 30s)
  9. UPDATE test_runs SET status='running'
  10. scaler.register(contestant_id, test_state)
  11. spawn_bot(contestant_id, bot_idx=1, rps=initial_rps)
```

**Runner entrypoint**:
```bash
#!/bin/sh
# infra/runner/entrypoint.sh
set -e
mc alias set store "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --quiet
mc cp "store/submissions/${CONTESTANT_ID}/${BINARY_SHA256}.bin" /tmp/contestant
EXPECTED="$BINARY_SHA256  /tmp/contestant"
echo "$EXPECTED" | sha256sum -c - || { echo "SHA-256 mismatch"; exit 1; }
chmod +x /tmp/contestant
exec /tmp/contestant
```

**Runner Dockerfile** (`infra/runner/Dockerfile`):
```dockerfile
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 libgcc-s1 ca-certificates curl && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc \
    -o /usr/local/bin/mc && chmod +x /usr/local/bin/mc
RUN useradd -u 1000 -m runner
COPY infra/runner/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER 1000
ENTRYPOINT ["/entrypoint.sh"]
```

---

## 17. platform-api — Full Specification

### Crates

```toml
[dependencies]
tokio              = { version = "1", features = ["full"] }
axum               = { version = "0.7", features = ["multipart"] }
tower-http         = { version = "0.5", features = ["cors", "trace"] }
bollard            = "0.17"
s3                 = "0.35"           # rust-s3: MinIO/S3 (no AWS SDK bloat)
sqlx               = { version = "0.8", features = ["postgres", "runtime-tokio", "uuid", "chrono"] }
rskafka            = "0.5"
fred               = "9"              # pure-Rust async Redis (tokens, status, pub/sub, hot-reload)
jsonwebtoken       = "9"
sha2               = "0.10"
serde              = { version = "1", features = ["derive"] }
serde_json         = "1"
uuid               = { version = "1", features = ["v4"] }
anyhow             = "1"
tokio-stream       = "0.1"
tower              = "0.4"
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
chrono             = { version = "0.4", features = ["serde"] }
```

### Source layout

```
src/
  main.rs            axum Router + startup init + tokio::spawn background tasks
  state.rs           AppState: db pool, docker, minio, kafka producer, redis client, scaler state
  auth.rs            JWT issue (login) + FromRequestParts extractor (middleware)
  docker.rs          bollard: create/start/kill/rm/inspect containers; CpuPool
  minio.rs           s3: put_object, verify sha256
  kafka.rs           rskafka: produce to test.events topic
  redis.rs           fred: token cache, test status, bot RPS SET, SSE pub/sub relay
  scaler.rs          ScalerState + daemon() + per-contestant FSM
  sandbox.rs         IsolationConfig builder, port poll, cleanup
  routes/
    auth.rs          POST /api/auth/login
    contestants.rs   POST/GET /api/contestants, /:id/start, /:id/stop, /:id/status
    submit.rs        POST /submit/:token  (multipart stream → MinIO → startTest)
    leaderboard.rs   GET /api/leaderboard  +  GET /api/leaderboard/stream (SSE)
    config.rs        GET/PUT /api/config
    health.rs        GET /health
```

### All endpoints

```
GET  /health
POST /api/auth/login           { password } → { token }

POST /api/contestants          { name } → { contestant_id, submit_token, submit_url }
GET  /api/contestants          → [{ contestant_id, name, status, composite, ... }]
POST /api/contestants/:id/start
POST /api/contestants/:id/stop
GET  /api/contestants/:id/status

GET  /api/leaderboard          → [{ contestant_id, status, tps, composite, ... }] (current snapshot)
GET  /api/leaderboard/stream   → text/event-stream  (SSE, one event per Redpanda snapshot)

GET  /api/config               → bot config JSON
PUT  /api/config               { max_bots, initial_rps, max_rps_per_bot, ramp_step,
                                  p99_hard_limit_ms, score_weights }

POST /submit/:token            multipart binary → starts test pipeline
```

Auth: `Authorization: Bearer <jwt>` required on all `/api/*` except `/api/leaderboard`
and `/api/leaderboard/stream` (public read). CORS: `tower-http` CorsLayer allowing
`http://localhost:5173`.

---

## 18. telemetry-ingester — Specification

### Crates

```toml
[dependencies]
tokio              = { version = "1", features = ["full"] }
rskafka            = "0.5"
sqlx               = { version = "0.8", features = ["postgres", "runtime-tokio"] }
fred               = "9"              # Redis PUBLISH leaderboard:updates + SET leaderboard:latest
hdrhistogram       = "7"
serde              = { version = "1", features = ["derive"] }
serde_json         = "1"
ordered-float      = "4"
anyhow             = "1"
tracing            = "0.1"
tracing-subscriber = "0.3"
```

### Source layout

```
src/
  main.rs          tokio main; spawn 4 tasks
  orderbook.rs     BTreeMap reference book per contestant (price-time priority)
  validator.rs     expected_fills() computation + violation classification
  aggregator.rs    HDRHistogram per contestant (for scaler failure detection)
  writer.rs        sqlx batch INSERT → latency_events, correctness_events, contest_summary UPSERT
  scoring.rs       composite score + global normalization (reads contest_summary for max/min)
  kafka.rs         rskafka consumer (orders/executions/metrics)
  redis.rs         fred: PUBLISH leaderboard:updates + SET leaderboard:latest after each flush
```

### Four tokio tasks

```
Task 1 — orders consumer (consumer group "ingester-orders"):
  HashMap<contestantId, OrderBook>
  on OrderEvent: book.expected_fills() → pending; book.add_order()

Task 2 — executions consumer (consumer group "ingester-executions"):
  on ExecutionEvent TRADE: validate against pending → batch correctness_events
  on ExecutionEvent CANCEL: book.cancel_order(); pending.remove()

Task 3 — metrics consumer (consumer group "ingester-metrics"):
  HashMap<contestantId, HdrHistogram>
  on MetricEvent: hdr.record(latency_us); tps_counter += 1

Task 4 — flush (every 5s):
  for each contestant_id:
    batch INSERT latency_events (collected from metrics consumer)
    batch INSERT correctness_events (collected from executions consumer)
    UPSERT contest_summary {
      current_tps    = tps_counter / 5.0,
      correct_fills  += Δ,
      total_fills    += Δ,
      composite      = compute_composite(correctness, tps, p99_from_hdr)
    }
    redis.SET("leaderboard:latest", all_summaries_json)
    redis.PUBLISH("leaderboard:updates", all_summaries_json)  // platform-api SSE relay picks this up
    hdr.reset(); tps_counter = 0

  // HDR histogram also polled by platform-api scaler via shared Arc<RwLock<...>>
  // for real-time p99 failure detection (no DB query needed)
```

> **Why HDR in ingester AND raw rows in TimescaleDB?**
> HDR histogram → scaler reads p99 in real-time without a DB query (failure detection).
> Raw rows → Grafana queries any percentile at any time granularity (display).

---

## 19. bot-worker — Specification

### Crates

```toml
[dependencies]
tokio              = { version = "1", features = ["full"] }
tokio-tungstenite  = "0.24"
rskafka            = "0.5"
fred               = "9"              # Redis GET bot:*:rps every 500ms for RPS hot-reload
serde              = { version = "1", features = ["derive"] }
serde_json         = "1"
uuid               = { version = "1", features = ["v4"] }
rand               = "0.8"
anyhow             = "1"
tracing            = "0.1"
```

### Source layout + env vars

```
src/
  main.rs       parse env; tokio::join!(fix_task, ws_task, config_poll_task)
  fix.rs        FIX 4.2 session (Logon/Heartbeat/NOS/Cancel/Logout, ~250 LOC)
  ws.rs         tokio-tungstenite client; JSON NewOrder/Cancel/ExecutionReport
  ordergen.rs   SmallRng seeded; next_order()
  publish.rs    rskafka: produce orders/executions/metrics
  config.rs     fred: poll Redis GET bot:{id}:{idx}:rps every 500ms; adjust rate

ENV:
  BOT_ID, BOT_INDEX, CONTESTANT_ID
  CONTESTANT_HOST          (e.g. "sandbox-alice")
  PORT_FIX=9090, PORT_WS=8080
  RNG_SEED                 (u64, deterministic)
  REDIS_URL                (e.g. redis://redis:6379)
  REDPANDA_BROKERS
```

**Config hot-reload via Redis** (replaces Redpanda compacted topic):
```rust
// src/config.rs
pub async fn poll_rps(redis: fred::clients::RedisClient,
                      key: String,
                      rate_tx: tokio::sync::watch::Sender<u64>) {
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    loop {
        interval.tick().await;
        if let Ok(rps) = redis.get::<u64, _>(&key).await {
            rate_tx.send(rps).ok();
        }
    }
}
// key = format!("bot:{}:{}:rps", contestant_id, bot_index)
// scaler writes this; bot reads it every 500ms and adjusts tokio::time::interval period
```

---

## 20. portal (SvelteKit) — Minimal Server-Side

Server-side exists for exactly three website mechanics. No business logic.

| File | Purpose |
|---|---|
| `hooks.server.ts` | Parse JWT from cookie → `event.locals.jwt` |
| `routes/admin/+layout.server.ts` | No valid jwt → redirect /admin/login |
| `routes/admin/+page.server.ts` | SSR load: GET platform-api /api/contestants |
| `routes/submit/[token]/+page.server.ts` | Verify token is valid (HEAD /submit/:token) |
| `routes/login/+server.ts` | POST: call platform-api auth → set httpOnly cookie |
| `routes/logout/+server.ts` | POST: clear cookie |

### Packages

```json
{
  "dependencies": {
    "@sveltejs/kit": "^2", "@sveltejs/adapter-node": "^5", "svelte": "^5",
    "jose": "^5", "chart.js": "^4", "zod": "^3"
  }
}
```

`jose` — cookie JWT verification only. Nothing else.
No dockerode, ioredis, minio, kafkajs, or any infra client in portal.

### Client-side API helper (`src/lib/api.ts`)

```typescript
// All API calls go to platform-api. Token held in memory after login.
const BASE = import.meta.env.PUBLIC_PLATFORM_URL;   // "http://localhost:8080"
let _token: string | null = null;

export const api = {
  setToken(t: string) { _token = t; },
  async get(path: string) {
    return fetch(`${BASE}${path}`, {
      headers: _token ? { Authorization: `Bearer ${_token}` } : {}
    }).then(r => r.json());
  },
  async post(path: string, body?: unknown) {
    return fetch(`${BASE}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json',
                 ...(_token ? { Authorization: `Bearer ${_token}` } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    }).then(r => r.json());
  },
  leaderboardStream() {
    return new EventSource(`${BASE}/api/leaderboard/stream`);
  }
};
```

---

## 21. Docker Compose (also valid `docker stack deploy` file)

```yaml
version: "3.8"

networks:
  platform-net:
    driver: bridge

volumes:
  pgdata: {}
  minio-data: {}
  redpanda-data: {}
  grafana-data: {}
  redis-data: {}

services:

  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_DB:       hackathon
      POSTGRES_USER:     hackathon
      POSTGRES_PASSWORD: hackathon
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./infra/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks: [platform-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U hackathon"]
      interval: 5s
    deploy:
      resources:
        limits: { memory: "2g" }

  redis:
    image: valkey/valkey:7-alpine
    command: valkey-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru
    volumes: [redis-data:/data]
    networks: [platform-net]
    sysctls: [net.core.somaxconn=1024]
    healthcheck:
      test: ["CMD", "valkey-cli", "ping"]
      interval: 5s
    deploy:
      resources:
        limits: { memory: "512m" }

  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 2
      - --memory 1G
      - --overprovisioned
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://redpanda:9092
    volumes: [redpanda-data:/var/lib/redpanda/data]
    networks: [platform-net]
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:..*true'"]
      interval: 10s
    deploy:
      resources:
        limits: { memory: "1.5g" }

  redpanda-init:
    image: redpandadata/redpanda:latest
    depends_on: [redpanda]
    networks: [platform-net]
    restart: "no"
    entrypoint: ["/bin/bash", "-c"]
    command: >
      "sleep 8
       rpk --brokers redpanda:9092 topic create orders      --partitions 8 &&
       rpk --brokers redpanda:9092 topic create executions  --partitions 8 &&
       rpk --brokers redpanda:9092 topic create metrics     --partitions 8 &&
       rpk --brokers redpanda:9092 topic create test.events --partitions 1"

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER:     minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes: [minio-data:/data]
    networks: [platform-net]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s

  grafana:
    image: grafana/grafana:latest
    environment:
      GF_SECURITY_ALLOW_EMBEDDING: "true"
      GF_AUTH_ANONYMOUS_ENABLED:   "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE:  Viewer
    volumes:
      - grafana-data:/var/lib/grafana
      - ./infra/grafana/provisioning:/etc/grafana/provisioning
      - ./infra/grafana/dashboards:/var/lib/grafana/dashboards
    ports: ["3000:3000"]
    networks: [platform-net]

  platform-api:
    build: services/platform-api
    ports: ["8080:8080"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./infra/seccomp-sandbox.json:/app/infra/seccomp-sandbox.json:ro
    environment:
      DATABASE_URL:       postgres://hackathon:hackathon@timescaledb/hackathon
      REDPANDA_BROKERS:   redpanda:9092
      REDIS_URL:          redis://redis:6379
      MINIO_ENDPOINT:     http://minio:9000
      MINIO_ACCESS_KEY:   minioadmin
      MINIO_SECRET_KEY:   minioadmin
      ADMIN_PASSWORD:     ${ADMIN_PASSWORD:-admin}
      JWT_SECRET:         ${JWT_SECRET:-change-me-32-chars-minimum}
      DOCKER_NETWORK:     platform_platform-net
      CPU_POOL:           "4,5,6,7,8,9,10,11"   # cores reserved for sandboxes
      RUNNER_IMAGE:       platform/runner:latest
      BOT_IMAGE:          platform/bot-worker:latest
      CORS_ORIGIN:        http://localhost:5173
    networks: [platform-net]
    depends_on:
      timescaledb: { condition: service_healthy }
      redpanda:    { condition: service_healthy }
      redis:       { condition: service_healthy }
      minio:       { condition: service_healthy }
    deploy:
      resources:
        limits: { memory: "512m" }

  telemetry-ingester:
    build: services/telemetry-ingester
    environment:
      DATABASE_URL:      postgres://hackathon:hackathon@timescaledb/hackathon
      REDPANDA_BROKERS:  redpanda:9092
      REDIS_URL:         redis://redis:6379
      FLUSH_INTERVAL_S:  "5"
    networks: [platform-net]
    depends_on:
      timescaledb: { condition: service_healthy }
      redpanda:    { condition: service_healthy }
      redis:       { condition: service_healthy }
    deploy:
      resources:
        limits: { memory: "512m" }

  portal:
    build: services/portal
    ports: ["5173:5173"]
    environment:
      PLATFORM_API:        http://platform-api:8080
      PUBLIC_PLATFORM_URL: http://localhost:8080
      PUBLIC_GRAFANA_URL:  http://localhost:3000
      JWT_SECRET:          ${JWT_SECRET:-change-me-32-chars-minimum}
      ORIGIN:              http://localhost:5173
    networks: [platform-net]
    depends_on: [platform-api]

  # Not started automatically — spawned by platform-api via bollard
  bot-worker:
    build: services/bot-worker
    profiles: [manual]
    networks: [platform-net]
    environment:
      REDPANDA_BROKERS: redpanda:9092
      REDIS_URL:        redis://redis:6379
    deploy:
      replicas: 0
      resources:
        limits: { cpus: "1", memory: "256m" }
```

---

## 22. Makefile + curl reference

```makefile
.PHONY: up down build clean logs swarm-deploy swarm-scale token contestant start stop status leaderboard e2e

# ── Stack ───────────────────────────────────────────────────────────────────
up:
	docker compose up -d

build:
	docker compose build platform-api telemetry-ingester portal
	docker build -t platform/runner:latest infra/runner/
	docker build -t platform/bot-worker:latest services/bot-worker/

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f

# ── Cloud (Swarm) ───────────────────────────────────────────────────────────
swarm-deploy:
	docker stack deploy -c docker-compose.yml platform

swarm-rm:
	docker stack rm platform

swarm-scale:   ## make swarm-scale N=20
	docker service scale platform_bot-worker=$(N)

# ── Curl shortcuts (all call platform-api directly) ─────────────────────────
API ?= http://localhost:8080

token:         ## make token PASS=admin
	@curl -sf -X POST $(API)/api/auth/login \
	  -H "Content-Type: application/json" \
	  -d '{"password":"$(PASS)"}' | jq -r .token | tee .token

contestant:    ## make contestant NAME=Alice
	@curl -sf -X POST $(API)/api/contestants \
	  -H "Authorization: Bearer $$(cat .token)" \
	  -H "Content-Type: application/json" \
	  -d '{"name":"$(NAME)"}' | jq .

upload:        ## make upload BIN=./contestant.bin TOKEN=<submit_token>
	@curl -sf -X POST "$(API)/submit/$(TOKEN)" \
	  -F "binary=@$(BIN)" | jq .

start:         ## make start ID=<contestant_id>
	@curl -sf -X POST $(API)/api/contestants/$(ID)/start \
	  -H "Authorization: Bearer $$(cat .token)" | jq .

stop:          ## make stop ID=<contestant_id>
	@curl -sf -X POST $(API)/api/contestants/$(ID)/stop \
	  -H "Authorization: Bearer $$(cat .token)" | jq .

status:        ## make status ID=<contestant_id>
	@curl -sf $(API)/api/contestants/$(ID)/status \
	  -H "Authorization: Bearer $$(cat .token)" | jq .

leaderboard:   ## make leaderboard
	@curl -sf $(API)/api/leaderboard | jq .

stream:        ## live SSE stream
	@curl -N $(API)/api/leaderboard/stream

config:        ## make config KEY=max_bots VAL=10
	@curl -sf -X PUT $(API)/api/config \
	  -H "Authorization: Bearer $$(cat .token)" \
	  -H "Content-Type: application/json" \
	  -d '{"$(KEY)": $(VAL)}' | jq .

e2e:           ## full end-to-end test
	@bash scripts/e2e.sh

open:
	open http://localhost:5173
grafana:
	open http://localhost:3000
```

---

## 23. E2E Test Script (`scripts/e2e.sh`)

Used for feature testing, lifecycle verification, and CI smoke testing.
Requires: `curl`, `jq`, `bc`. Requires a mock contestant binary at `tests/fixtures/echo_exchange`.

```bash
#!/usr/bin/env bash
# scripts/e2e.sh — Full lifecycle E2E test via curl
set -euo pipefail

API="${API:-http://localhost:8080}"
PASS="${ADMIN_PASSWORD:-admin}"
BIN="${TEST_BINARY:-./tests/fixtures/echo_exchange}"
TIMEOUT="${TEST_TIMEOUT:-120}"

log() { echo "[e2e] $*"; }
fail() { echo "[e2e] FAIL: $*" >&2; exit 1; }

# ── 1. Health check ─────────────────────────────────────────────────────────
log "1. Health check"
STATUS=$(curl -sf "$API/health" | jq -r .status)
[ "$STATUS" = "ok" ] || fail "Platform unhealthy: $STATUS"

# ── 2. Auth ─────────────────────────────────────────────────────────────────
log "2. Authenticating"
TOKEN=$(curl -sf -X POST "$API/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"password\":\"$PASS\"}" | jq -r .token)
[ -n "$TOKEN" ] || fail "No token returned"
AUTH="Authorization: Bearer $TOKEN"

# ── 3. Create two contestants ────────────────────────────────────────────────
log "3. Creating contestants Alice and Bob"
ALICE=$(curl -sf -X POST "$API/api/contestants" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"name":"alice-e2e"}')
ALICE_ID=$(echo "$ALICE" | jq -r .contestant_id)
ALICE_TOKEN=$(echo "$ALICE" | jq -r .submit_token)
log "   Alice id=$ALICE_ID"

BOB=$(curl -sf -X POST "$API/api/contestants" \
  -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"name":"bob-e2e"}')
BOB_ID=$(echo "$BOB" | jq -r .contestant_id)
BOB_TOKEN=$(echo "$BOB" | jq -r .submit_token)
log "   Bob   id=$BOB_ID"

# ── 4. Upload binaries ───────────────────────────────────────────────────────
log "4. Uploading binaries"
curl -sf -X POST "$API/submit/$ALICE_TOKEN" -F "binary=@$BIN" > /dev/null
curl -sf -X POST "$API/submit/$BOB_TOKEN"  -F "binary=@$BIN" > /dev/null

# ── 5. Both tests start automatically; verify running ───────────────────────
log "5. Waiting for both tests to start (max 35s)"
for i in $(seq 1 35); do
  sleep 1
  A_STATUS=$(curl -sf "$API/api/contestants/$ALICE_ID/status" -H "$AUTH" | jq -r .status)
  B_STATUS=$(curl -sf "$API/api/contestants/$BOB_ID/status"  -H "$AUTH" | jq -r .status)
  [ "$A_STATUS" = "running" ] && [ "$B_STATUS" = "running" ] && break
  [ $i -eq 35 ] && fail "Timed out waiting for running. Alice=$A_STATUS Bob=$B_STATUS"
done
log "   Both running"

# ── 6. Verify leaderboard shows both ────────────────────────────────────────
log "6. Checking leaderboard"
LB=$(curl -sf "$API/api/leaderboard")
echo "$LB" | jq -e ".[] | select(.contestant_id == \"$ALICE_ID\")" > /dev/null \
  || fail "Alice not in leaderboard"
echo "$LB" | jq -e ".[] | select(.contestant_id == \"$BOB_ID\")" > /dev/null \
  || fail "Bob not in leaderboard"

# ── 7. Wait for completion ───────────────────────────────────────────────────
log "7. Waiting for tests to complete (max ${TIMEOUT}s)"
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  sleep 5; ELAPSED=$((ELAPSED + 5))
  A_STATUS=$(curl -sf "$API/api/contestants/$ALICE_ID/status" -H "$AUTH" | jq -r .status)
  B_STATUS=$(curl -sf "$API/api/contestants/$BOB_ID/status"  -H "$AUTH" | jq -r .status)
  log "   t=${ELAPSED}s Alice=$A_STATUS Bob=$B_STATUS"
  [ "$A_STATUS" != "running" ] && [ "$B_STATUS" != "running" ] && break
done

# ── 8. Assert scores ─────────────────────────────────────────────────────────
log "8. Checking final scores"
LB=$(curl -sf "$API/api/leaderboard")
A_SCORE=$(echo "$LB" | jq ".[] | select(.contestant_id==\"$ALICE_ID\") | .composite // 0")
B_SCORE=$(echo "$LB" | jq ".[] | select(.contestant_id==\"$BOB_ID\")  | .composite // 0")
log "   Alice composite=$A_SCORE  Bob composite=$B_SCORE"
[ "$(echo "$A_SCORE > 0" | bc -l)" = "1" ] || fail "Alice score is 0"
[ "$(echo "$B_SCORE > 0" | bc -l)" = "1" ] || fail "Bob score is 0"

# ── 9. Config update ─────────────────────────────────────────────────────────
log "9. Updating config"
curl -sf -X PUT "$API/api/config" -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"max_bots": 3}' | jq -e '.max_bots == 3' > /dev/null || fail "Config update failed"

log "ALL TESTS PASSED"
```

---

## 24. Repository Structure

```
.
├── PLAN.md
├── docker-compose.yml
├── Makefile
├── .env.example
├── scripts/
│   └── e2e.sh
├── tests/
│   └── fixtures/
│       └── echo_exchange          (mock contestant binary for e2e)
├── services/
│   ├── platform-api/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │       ├── main.rs
│   │       ├── state.rs
│   │       ├── auth.rs
│   │       ├── docker.rs          (bollard + CpuPool)
│   │       ├── minio.rs
│   │       ├── kafka.rs
│   │       ├── redis.rs           (fred: tokens, status, bot RPS, pub/sub relay)
│   │       ├── scaler.rs          (daemon + per-contestant FSM)
│   │       ├── sandbox.rs
│   │       └── routes/
│   │           ├── auth.rs
│   │           ├── contestants.rs
│   │           ├── submit.rs
│   │           ├── leaderboard.rs (snapshot + SSE stream)
│   │           ├── config.rs
│   │           └── health.rs
│   ├── telemetry-ingester/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │       ├── main.rs
│   │       ├── orderbook.rs
│   │       ├── validator.rs
│   │       ├── aggregator.rs      (HDRHistogram per contestant)
│   │       ├── writer.rs          (batch INSERT latency_events + correctness_events)
│   │       ├── scoring.rs
│   │       ├── kafka.rs
│   │       └── redis.rs           (fred: PUBLISH leaderboard:updates + SET leaderboard:latest)
│   ├── bot-worker/
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── src/
│   │       ├── main.rs
│   │       ├── fix.rs             (~250 LOC, zero FIX deps)
│   │       ├── ws.rs
│   │       ├── ordergen.rs
│   │       ├── publish.rs
│   │       └── config.rs          (fred: GET bot:*:rps from Redis every 500ms)
│   └── portal/
│       ├── package.json
│       ├── svelte.config.js       (adapter-node)
│       ├── vite.config.ts
│       ├── Dockerfile
│       └── src/
│           ├── hooks.server.ts    (JWT cookie → locals.jwt)
│           ├── app.html
│           ├── lib/
│           │   ├── api.ts         (client-side platform-api wrapper)
│           │   ├── Leaderboard.svelte
│           │   ├── UploadForm.svelte
│           │   └── AdminPanel.svelte
│           └── routes/
│               ├── +layout.svelte
│               ├── +layout.server.ts
│               ├── +page.svelte           (leaderboard: Grafana iframe)
│               ├── submit/[token]/
│               │   ├── +page.svelte       (upload form)
│               │   └── +page.server.ts    (verify token)
│               ├── admin/
│               │   ├── +layout.server.ts  (JWT guard)
│               │   ├── +page.svelte       (admin panel)
│               │   └── +page.server.ts    (load: GET /api/contestants)
│               └── login/
│                   ├── +page.svelte
│                   └── +server.ts         (POST → platform-api auth → set cookie)
├── infra/
│   ├── init.sql                   (full TimescaleDB schema)
│   ├── seccomp-sandbox.json
│   ├── runner/
│   │   ├── Dockerfile
│   │   └── entrypoint.sh
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/timescale.yaml
│       │   └── dashboards/dashboards.yaml
│       └── dashboards/
│           ├── leaderboard.json
│           └── contestant.json
```

---

## 25. Build Order

```
Step  Command                                               Verify
─────────────────────────────────────────────────────────────────────────────
1     docker compose up timescaledb redis redpanda minio   healthchecks green
2     docker compose up redpanda-init                      logs: "topic created" ×4
3     (init.sql auto-runs from initdb.d)                    psql: \dt → 6 tables
4     cargo build -p telemetry-ingester                    no errors
5     cargo build -p platform-api                          no errors
6     cargo build -p bot-worker                            no errors
7     docker build -t platform/runner:latest infra/runner/
8     docker compose build --no-cache                      all images built
9     docker compose up -d                                 all 9 services running
10    make token PASS=admin                                JWT in .token
11    make contestant NAME=smoke-test                      submit_url printed
12    make upload BIN=tests/fixtures/echo_exchange TOKEN=<from above>
13    make status ID=<id>                                  status: running
14    make stream  (ctrl-c after seeing events)            SSE events arriving
15    make e2e                                             ALL TESTS PASSED
16    open http://localhost:5173                           Portal loads
17    open http://localhost:3000                           Grafana leaderboard live
18    make swarm-deploy  (cloud)                           docker stack ps: Running
```

---

## 26. Local ↔ Cloud Parity

The `docker-compose.yml` is the IaC declaration for both environments.
`docker compose up` for local single-node; `docker stack deploy` for multi-node Swarm.

| Aspect | Local (single node) | Cloud (Docker Swarm) |
|---|---|---|
| Deploy command | `docker compose up -d` | `docker stack deploy -c docker-compose.yml platform` |
| Compose file | unchanged | unchanged (Swarm reads same file) |
| Bot scaling | spawned by platform-api via bollard | `docker service scale platform_bot-worker=N` |
| Storage | Named volumes on host | Named volumes on Swarm manager node |
| Networking | Bridge `platform-net` | Overlay `platform-net` (Swarm auto-converts bridge → overlay) |
| Registry | `docker build` in-place | Push to registry; Swarm nodes pull on deploy |
| Sandbox CPU pinning | `--cpuset-cpus` on same host | `--cpuset-cpus` on assigned Swarm node |
| Redis | Single Valkey instance | Single Valkey instance |
| Redpanda | Single broker | Single broker |
| TimescaleDB | Single instance | Single instance |
| platform-api port | `localhost:8080` | node-internal `:8080` (no internet exposure needed) |
| Portal port | `localhost:5173` | node-internal `:5173` |

Only `deploy.replicas` and `deploy.placement` differ. All env vars, image names,
volumes, and service configs are identical between local and cloud.

---

## 27. Multi-Contestant Demo / Verification Script

Run during the hackathon demo to show the full platform live.

```bash
#!/usr/bin/env bash
# scripts/demo.sh — Live multi-contestant demo
# Usage: bash scripts/demo.sh [binary_path]
set -euo pipefail

API="http://localhost:8080"
BIN="${1:-./tests/fixtures/echo_exchange}"

log()  { echo -e "\033[1;36m[demo]\033[0m $*"; }
ok()   { echo -e "\033[1;32m  ✓\033[0m $*"; }

TOKEN=$(curl -sf -X POST "$API/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"password":"admin"}' | jq -r .token)
AUTH="Authorization: Bearer $TOKEN"

log "=== IICPC Hackathon 2026 — Live Demo ==="

# ── Create Alice ──────────────────────────────────────────────────────────────
log "1. Creating contestant Alice"
ALICE=$(curl -sf -X POST "$API/api/contestants" \
  -H "$AUTH" -H "Content-Type: application/json" -d '{"name":"Alice"}')
ALICE_ID=$(echo "$ALICE" | jq -r .contestant_id)
ok "Alice id=$ALICE_ID  url=$(echo "$ALICE" | jq -r .submit_url)"

# ── Alice uploads ─────────────────────────────────────────────────────────────
log "2. Alice uploads binary"
curl -sf -X POST "$API/submit/$(echo "$ALICE" | jq -r .submit_token)" \
  -F "binary=@$BIN" > /dev/null
ok "Uploaded. Sandbox starting..."

# ── Create Bob concurrently ───────────────────────────────────────────────────
sleep 5
log "3. Creating Bob while Alice is already running (concurrent test demo)"
BOB=$(curl -sf -X POST "$API/api/contestants" \
  -H "$AUTH" -H "Content-Type: application/json" -d '{"name":"Bob"}')
BOB_ID=$(echo "$BOB" | jq -r .contestant_id)
curl -sf -X POST "$API/submit/$(echo "$BOB" | jq -r .submit_token)" \
  -F "binary=@$BIN" > /dev/null
ok "Bob id=$BOB_ID  both running concurrently with independent bot fleets"

# ── Live leaderboard poll ─────────────────────────────────────────────────────
log "4. Polling leaderboard every 5s (ctrl-c to exit)"
log "   Portal: http://localhost:5173"
log "   Grafana: http://localhost:3000"
echo ""
while true; do
  echo "--- $(date '+%H:%M:%S') ---"
  curl -sf "$API/api/leaderboard" | jq -r \
    '.[] | "  \(.contestant_id | .[0:8])  status=\(.status // "?")  bots=\(.bot_count // 0)  tps=\(.current_tps // 0 | floor)  composite=\(.composite // "—")"'
  sleep 5
done
```

**What judges see:**
- Alice appears on leaderboard immediately (`starting` → `running`)
- Bob appears ~30s later, **both running concurrently** with independent bot fleets
- TPS ramps independently per contestant: Alice's bots don't wait for Bob's
- Status badge transitions: `running` → `success` (all bots maxed) or `failed`
- Grafana iframe shows live p50/p90/p99 time-series, updating every 5s
- Composite score updates live; rank can change as tests progress
- Carol can join at any time without affecting Alice or Bob: `make contestant NAME=Carol`

---

## 28. What NOT to Build

| Concern | Decision |
|---|---|
| HTTPS / TLS | None — HTTP only, internal deployment |
| Multi-node Redis | Single Valkey; no Sentinel needed at hackathon scale |
| PostgreSQL replication | Single TimescaleDB instance |
| JWT refresh tokens | Single long-lived token per login (24h expiry) |
| Rate limiting on API | Not needed for internal judging platform |
| gVisor / Firecracker | Docker flags + seccomp + cap-drop satisfies isolation requirement |
| FIX session persistence | Bots reconnect on disconnect; no persisted sequence numbers |
| Unit test suite | E2E via `make e2e` + `bash scripts/demo.sh` only. Unit tests post-hackathon. |
| CI/CD pipeline | Manual `docker compose up` during hackathon |
| Contestant accounts / OAuth | Token IS the identity. No signup, no password reset |
| Admin multi-user | Single `ADMIN_PASSWORD`. One admin at a time |
| Metrics archival / rotation | Docker volumes persist for hackathon duration; no cleanup needed |
