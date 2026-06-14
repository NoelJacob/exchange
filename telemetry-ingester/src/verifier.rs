use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use orderbook_rs::orderbook::book::OrderBook;
use pricelevel::{Side, Hash32, Id, MatchResult};
use sha2::{Digest, Sha256};
use sqlx::Row;
use crate::config::Config;
use crate::models::ExecutionEvent;
use crate::storage::{CorrectnessRow, Storage};

fn hash_user_id(user_id: &str) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(user_id.as_bytes());
    let result = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&result[..32]);
    Hash32::new(bytes)
}

#[derive(Debug, Clone)]
struct ExpectedFill { price_f64: f64, qty: u64 }

struct ContestantState {
    book: OrderBook<()>,
    pending_fills: HashMap<String, VecDeque<ExpectedFill>>,
    orders_sent: u64, execs_received: u64, correct_fills: u64,
    total_fills: u64, total_penalty: f64,
    target_id: String,
}

impl ContestantState {
    fn new(target_id: &str) -> Self {
        Self {
            book: OrderBook::new("AAPL"),
            pending_fills: HashMap::new(),
            orders_sent: 0, execs_received: 0, correct_fills: 0,
            total_fills: 0, total_penalty: 0.0,
            target_id: target_id.to_string(),
        }
    }

    fn submit_order(&mut self, side: Side, qty: u64, price: f64, is_market: bool,
                    cl_ord_id: &str, exchange_leaves_qty: Option<u64>) {
        let user_hash = hash_user_id(&self.target_id);
        let limit_price_cents = (price * 100.0).round() as u128;
        let taker_id = Id::new();
        let result: MatchResult = if is_market {
            match self.book.submit_market_order_with_user(taker_id, qty, side, user_hash) {
                Ok(r) => r,
                Err(e) => { eprintln!("[VERIFY-ERR] {}: market submit FAILED: {:?}", cl_ord_id, e); return; }
            }
        } else {
            match self.book.match_limit_order_with_user(taker_id, qty, side, limit_price_cents, user_hash) {
                Ok(r) => r,
                Err(e) => { eprintln!("[VERIFY-ERR] {}: limit submit FAILED: {:?}", cl_ord_id, e); return; }
            }
        };

        let filled_qty = result.executed_quantity().map_err(|e| eprintln!("[VERIFY-ERR] executed_qty: {e}")).unwrap_or(0);
        let resting_qty = if is_market { 0 } else { result.remaining_quantity() };
        let all_trades = result.trades().as_vec();

        self.orders_sent += 1;
        self.execs_received += if filled_qty > 0 { 1 } else { 0 };

        // If order filled immediately, remember expected fills.
        let expected_quantity = if is_market { qty } else { filled_qty };
        if expected_quantity > 0 {
            let mut expected_fills = VecDeque::new();
            for t in all_trades {
                let q = t.quantity().as_u64();
                let px = t.price().as_u128() as f64 / 100.0;
                expected_fills.push_back(ExpectedFill { price_f64: px, qty: q });
            }
            self.pending_fills.insert(cl_ord_id.to_string(), expected_fills);
        }

        if resting_qty > 0 {
            let leaves_key = format!("leaves_{}", cl_ord_id);
            self.pending_fills.insert(leaves_key, VecDeque::new());
        }

        if let Some(exq) = exchange_leaves_qty {
            if exq == 0 && resting_qty > 0 {
                eprintln!("[VERIFY-INCONSISTENT] {}: exchange says resting=0 but our book says resting={}", cl_ord_id, resting_qty);
            }
        }
    }

    fn compare_fill(&mut self, exec: &ExecutionEvent) -> (String, f64, Option<CorrectnessRow>) {
        let expected = self.pending_fills.get_mut(&exec.cl_ord_id);
        match expected {
            None => {
                eprintln!("[VERIFY-GHOST] {} seq={}: fill without expected order", exec.cl_ord_id, exec.exec_seq);
                return ("ghost".to_string(), 0.0, None);
            }
            Some(deque) => {
                if deque.is_empty() {
                    eprintln!("[VERIFY-GHOST] {} seq={}: more fills than expected (deque empty)", exec.cl_ord_id, exec.exec_seq);
                    return ("ghost".to_string(), 0.0, None);
                }
                let exp = deque.pop_front().unwrap();
                let actual_px = exec.last_px.unwrap_or(0.0);
                let actual_qty = exec.last_shares.unwrap_or(0);
                let price_ok = (actual_px - exp.price_f64).abs() < 0.001;
                let qty_ok = actual_qty == exp.qty;
                let correct = price_ok && qty_ok;
                let penalty = if correct { 0.0 } else {
                    (actual_px - exp.price_f64).abs() * actual_qty as f64 / 100.0
                };
                self.total_fills += 1;
                if correct {
                    self.correct_fills += 1;
                } else {
                    self.total_penalty += penalty;
                }
                let verdict = if correct { "correct" } else { "incorrect" };
                eprintln!("[VERIFY-FILL] {}: expected ({:.2}@{:.2}) actual ({}@{:.2}) => {} penalty={:.2}",
                    exec.cl_ord_id, exp.qty, exp.price_f64, actual_qty, actual_px, verdict, penalty);
                let cr = CorrectnessRow {
                    contestant_id: exec.contestant_id.clone(),
                    exec_id: exec.exec_id.clone(),
                    cl_ord_id: exec.cl_ord_id.clone(),
                    verdict: verdict.to_string(),
                    penalty,
                    expected_px: exp.price_f64,
                    actual_px,
                    expected_qty: exp.qty,
                    actual_qty,
                };
                (verdict.to_string(), penalty, Some(cr))
            }
        }
    }
}

/// Verifier for ONE contestant. Each contestant gets its own Verifier task
/// to maintain strict exec_seq ordering per binary. Queries only its own
/// exec_events, processes in exec_seq order with gap detection.
pub struct Verifier {
    storage: Arc<Storage>,
    config: Config,
    contestant_id: String,
    expected_seq: i64,
    state: ContestantState,
    // TPS/composite tracking
    last_poll_time: Option<std::time::Instant>,
    last_total_fills: u64,
    peak_tps: f64,
    /// Set to true when gap timeout expires — contestant permanently stalled.
    stall: bool,
}

impl Verifier {
    pub fn new(storage: Arc<Storage>, config: Config, contestant_id: &str) -> Self {
        Self {
            storage, config,
            contestant_id: contestant_id.to_string(),
            expected_seq: 0,
            state: ContestantState::new(contestant_id),
            last_poll_time: None,
            last_total_fills: 0,
            peak_tps: 0.0,
            stall: false,
        }
    }
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Initialize expected_seq from the minimum exec_seq in the database.
        // This avoids the first-event offset problem: if events arrive out of
        // insert order, we start at the lowest known seq rather than jumping
        // ahead to the first seq we happen to poll.
        let first_seq: Option<i64> = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MIN(exec_seq) FROM exec_events WHERE contestant_id = $1"
        )
        .bind(&self.contestant_id)
        .fetch_one(&self.storage.pg)
        .await?;
        self.expected_seq = first_seq.unwrap_or(1);
        if let Some(min_seq) = first_seq {
            eprintln!("[VERIFIER-INIT] contestant={}: init expected_seq to {} (min seq in DB)", self.contestant_id, min_seq);
        } else {
            eprintln!("[VERIFIER-INIT] contestant={}: no events yet, expected_seq=1", self.contestant_id);
        }

        let mut poll_no = 0u64;
        loop {
            poll_no += 1;
            let (active, gap_filled) = self.poll(poll_no).await?;
            eprintln!("[VERIFIER-ITER] contestant={}: poll={} active={} gap_filled={} stall={}",
                self.contestant_id, poll_no, active, gap_filled, self.stall);
            if self.stall {
                eprintln!("[VERIFIER-STALL] contestant={}: gap timed out → failed", self.contestant_id);
                self.publish_summary().await?;
                return Ok(());
            }
            if !active && !gap_filled {
                eprintln!("[VERIFIER-DONE] contestant={}: no more events", self.contestant_id);
                return Ok(());
            }
            if !gap_filled {
                self.publish_summary().await?;
            }
            eprintln!("[VERIFIER-ITER] contestant={}: end of poll {} — sleeping {}s", self.contestant_id, poll_no, self.config.poll_interval_secs);

            tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs)).await;
        }
    }

    /// Poll exec_events for this contestant, process in exec_seq order with gap detection.
    /// Returns (had_events, gap_was_filled).
    async fn poll(&mut self, poll_no: u64) -> Result<(bool, bool), Box<dyn std::error::Error + Send + Sync>> {
        let query_start = std::time::Instant::now();
        let rows = sqlx::query(
            "SELECT exec_seq, cl_ord_id, exec_id, exec_type, side, qty, price, is_market, last_shares, last_px, leaves_qty, cum_qty FROM exec_events WHERE contestant_id = $1 AND exec_seq >= $2 ORDER BY exec_seq LIMIT 500"
        )
        .bind(&self.contestant_id)
        .bind(self.expected_seq)
        .fetch_all(&self.storage.pg).await?;
        let query_elapsed = query_start.elapsed();
        if rows.is_empty() {
            eprintln!("[VERIFIER-POLL] contestant={}: poll {} QUERY: {} rows in {:?}", self.contestant_id, poll_no, rows.len(), query_elapsed);
            return Ok((false, false));
        }

        let first_seq = rows.first().map(|r| r.try_get::<i64,_>("exec_seq").unwrap_or(0)).unwrap_or(0);
        let last_seq = rows.last().map(|r| r.try_get::<i64,_>("exec_seq").unwrap_or(0)).unwrap_or(0);
        eprintln!("[VERIFIER-POLL] contestant={}: poll {} QUERY: {} rows (seq {}..{}) in {:?}",
            self.contestant_id, poll_no, rows.len(), first_seq, last_seq, query_elapsed);

        let mut gap_filled = false;

        // Process rows in exec_seq order with strict gap detection.
        for row in &rows {
            let es: i64 = match row.try_get("exec_seq") {
                Ok(s) => s,
                Err(_) => continue,
            };

            if es < self.expected_seq { continue; }

            if es > self.expected_seq {
                // Gap — wait for missing seq
                let missing = self.expected_seq;
                eprintln!("[VERIFIER-GAP] contestant={} expected seq={} but found seq={}", self.contestant_id, missing, es);
                let max_iters = self.config.gap_timeout_secs * 10;
                for wait_iter in 1..=max_iters {
                    let check = sqlx::query(
                        "SELECT 1 FROM exec_events WHERE exec_seq = $1 AND contestant_id = $2 LIMIT 1"
                    )
                    .bind(missing)
                    .bind(&self.contestant_id)
                    .fetch_optional(&self.storage.pg).await;
                    match check {
                        Ok(Some(_)) => {
                            eprintln!("[VERIFIER-GAP] contestant={}: seq={} arrived after {} waits", self.contestant_id, missing, wait_iter);
                            gap_filled = true;
                            break;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            eprintln!("[VERIFIER-ERR] contestant={}: check seq={} failed: {e:?}", self.contestant_id, missing);
                            break;
                        }
                    }
                    if wait_iter % 10 == 0 {
                        eprintln!("[VERIFIER-GAP-WAIT] contestant={}: waiting for seq={} — iter={}/{} ({:.1}s)",
                            self.contestant_id, missing, wait_iter, max_iters, wait_iter as f64 * 0.1);
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                if gap_filled {
                    // Missing seq now exists — return so run() re-polls immediately
                    return Ok((true, true));
                }
                // Gap timed out — contestant permanently stalled.
                self.stall = true;
                eprintln!("[VERIFIER-ERR] contestant={}: seq={} timed out after {}s — marked stalled",
                    self.contestant_id, missing, self.config.gap_timeout_secs);
                break;
            }

            // es == expected_seq — process this event
            let side: String = match row.try_get::<String, _>("side") {
                Ok(s) if !s.is_empty() => s,
                _ => { eprintln!("[VERIFY-ERR] contestant={}: missing side at seq={}", self.contestant_id, es); self.expected_seq = es + 1; continue; }
            };

            let exec = ExecutionEvent {
                contestant_id: self.contestant_id.clone(),
                bot_id: String::new(), cl_ord_id: row.try_get("cl_ord_id")?,
                exec_id: row.try_get("exec_id")?,
                exec_seq: es as u64, exec_type: row.try_get("exec_type")?,
                side,
                qty: row.try_get::<i64,_>("qty").unwrap_or(0) as u64,
                price: row.try_get::<f64,_>("price").unwrap_or(0.0),
                is_market: row.try_get::<bool,_>("is_market").unwrap_or(false),
                latency_us: 0,
                last_shares: row.try_get::<i64,_>("last_shares").ok().map(|x| x as u64),
                last_px: row.try_get::<f64,_>("last_px").ok(),
                leaves_qty: row.try_get::<i64,_>("leaves_qty").ok().map(|x| x as u64),
                cum_qty: row.try_get::<i64,_>("cum_qty").ok().map(|x| x as u64),
                ts_us: 0,
            };
            eprintln!("[VERIFY-ROW] {}: seq={} type={} side={} qty={} price={:.4} market={}",
                exec.cl_ord_id, exec.exec_seq, exec.exec_type, exec.side, exec.qty, exec.price, exec.is_market);

            match exec.exec_type.as_str() {
                "0" => {
                    let ob_side = if exec.side == "buy" { Side::Buy } else { Side::Sell };
                    self.state.submit_order(ob_side, exec.qty, exec.price,
                        exec.is_market, &exec.cl_ord_id, exec.leaves_qty);
                }
                "1" | "2" => {
                    let ob_side = if exec.side == "buy" { Side::Buy } else { Side::Sell };
                    if !self.state.pending_fills.contains_key(&exec.cl_ord_id) {
                        eprintln!("[VERIFY-EVENT] immediate-fill {} seq={} submitting to ref book", exec.cl_ord_id, exec.exec_seq);
                        self.state.submit_order(ob_side, exec.qty, exec.price,
                            exec.is_market, &exec.cl_ord_id, exec.leaves_qty);
                    }

                    let (verdict, _penalty, row_cr) = self.state.compare_fill(&exec);
                    eprintln!("[VERIFY-RESULT] {}: {}", exec.cl_ord_id, &verdict);
                    if let Some(cr) = row_cr {
                        if let Err(e) = self.storage.insert_correctness(&cr).await {
                            eprintln!("[VERIFY-ERR] insert_correctness: {e}");
                        }
                    } else if verdict == "ghost" {
                        if let Err(e) = self.storage.insert_ghost(&exec).await {
                            eprintln!("[VERIFY-ERR] insert_ghost: {e}");
                        }
                    }
                }
                "8" => eprintln!("[VERIFY-EVENT] REJECT seq={}", exec.exec_seq),
                _ => eprintln!("[VERIFY-EVENT] UNKNOWN seq={} type={}", exec.exec_seq, exec.exec_type),
            }

            self.expected_seq = es + 1;
        }

        eprintln!("[VERIFIER-CKPT] contestant={}: next_seq={}", self.contestant_id, self.expected_seq);
        eprintln!("[VERIFIER-BATCH] contestant={}: processed {} rows in poll {} — expected_seq now {}, orders_sent={}, fills={}, correct={}",
            self.contestant_id, rows.len(), poll_no, self.expected_seq,
            self.state.orders_sent, self.state.total_fills, self.state.correct_fills);
        Ok((true, false))
    }
    /// Compute composite score and upsert contest_summary + publish leaderboard snapshot.
    async fn publish_summary(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let st = &self.state;
        let cid = &self.contestant_id;

        let failure_reason = if self.stall {
            Some("stall")
        } else if st.total_fills == 0 && st.orders_sent > 10 {
            Some("stall")
        } else if st.total_fills > 0 {
            let pct = st.correct_fills as f64 / st.total_fills as f64;
            if pct < 0.50 { Some("correctness_fail") } else { None }
        } else {
            None
        };
        let pct = if st.total_fills > 0 { st.correct_fills as f64 / st.total_fills as f64 * 100.0 } else { 0.0 };

        let p99_latency: f64 = match sqlx::query_scalar::<_, f64>(
            "SELECT p99 FROM metric_events WHERE contestant_id = $1 ORDER BY ts DESC LIMIT 1"
        )
        .bind(cid)
        .fetch_optional(&self.storage.pg)
        .await
        {
            Ok(Some(p)) => { eprintln!("[VERIFY-P99] {}: p99={:.0}us", cid, p); p }
            Ok(None) => { eprintln!("[VERIFY-P99] {}: no metric events", cid); 0.0 }
            Err(e) => { eprintln!("[VERIFY-P99] {}: query error: {e}", cid); 0.0 }
        };

        let now = std::time::Instant::now();
        let current_tps = if st.total_fills > self.last_total_fills {
            if let Some(last_t) = self.last_poll_time {
                let elapsed = now.duration_since(last_t).as_secs_f64();
                if elapsed > 0.0 {
                    (st.total_fills - self.last_total_fills) as f64 / elapsed
                } else { 0.0 }
            } else { 0.0 }
        } else { 0.0 };
        let peak = if current_tps > self.peak_tps { current_tps } else { self.peak_tps };

        // Update TPS tracking fields for next computation
        self.last_poll_time = Some(now);
        self.last_total_fills = st.total_fills;

        let (cw, tw, pw) = self.storage.read_weights().await
            .unwrap_or((0.40, 0.35, 0.25));

        let correctness_score = if st.total_fills > 0 {
            st.correct_fills as f64 / st.total_fills as f64
        } else { 0.0 };
        let max_tps = 500.0;
        let normalized_tps = (current_tps / max_tps).min(1.0);
        let max_p99_us = 100_000.0;
        let normalized_p99 = if p99_latency > 0.0 {
            1.0 - (p99_latency / max_p99_us).min(1.0)
        } else { 0.0 };

        let composite = if p99_latency == 0.0 && st.total_fills > 10 {
            eprintln!("[VERIFY-COMPOSITE] {}: HARD ERROR — metrics missing", cid);
            -1.0
        } else if st.total_fills == 0 {
            0.0
        } else {
            cw * correctness_score + tw * normalized_tps + pw * normalized_p99
        };

        eprintln!("[VERIFY-COMPOSITE] {}: correctness={:.4} tps={:.1}/{} p99={:.0}us composite={:.4}",
            cid, correctness_score, current_tps, peak, p99_latency, composite);
        eprintln!("[VERIFIER-COMPOSITE-BREAKDOWN] {}: cw={:.2}*corr={:.4} + tw={:.2}*tps={:.4} + pw={:.2}*p99={:.4} = {:.4}",
            cid, cw, correctness_score, tw, normalized_tps, pw, normalized_p99, composite);

        let status = if failure_reason.is_some() { "failed" } else { "running" };
        if let Err(e) = self.storage.upsert_summary(cid, status,
            st.orders_sent, st.execs_received, st.correct_fills, st.total_fills, st.total_penalty,
            composite, current_tps, peak, p99_latency, failure_reason).await {
            eprintln!("[VERIFY-ERR] upsert_summary: {e}");
        }

        let snap = serde_json::json!({
            "contestant_id": cid, "status": status,
            "orders_sent": st.orders_sent, "fills": st.execs_received,
            "correctness_pct": pct, "total_penalty": st.total_penalty,
            "composite": composite, "current_tps": current_tps,
            "failure_reason": failure_reason,
        });
        if let Err(e) = self.storage.publish_leaderboard(&snap.to_string()).await {
            eprintln!("[VERIFY-ERR] publish_leaderboard: {e}");
        }

        Ok(())
    }
}

/// Discover contestants from exec_events and test_runs, spawn one Verifier per contestant.
pub async fn run_verifiers(storage: Arc<Storage>, config: Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut active: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    loop {
        let mut found: Vec<String> = Vec::new();

        // Reap completed verifier handles so contestants get re-verified
        // when new data arrives after their first pass.
        active.retain(|cid, h| {
            let alive = !h.is_finished();
            if !alive {
                eprintln!("[VERIFIER-REAP] contestant={} handle finished — reaping for re-discovery", cid);
            }
            alive
        });

        // Discover from exec_events
        if let Ok(rows) = sqlx::query("SELECT DISTINCT contestant_id FROM exec_events")
            .fetch_all(&storage.pg).await
        {
            for r in rows {
                if let Ok(cid) = r.try_get::<String, _>("contestant_id") {
                    if !found.contains(&cid) {
                        found.push(cid);
                    }
                }
            }
        }

        // Discover from test_runs
        match sqlx::query("SELECT DISTINCT contestant_id FROM test_runs WHERE status IN ('running', 'success', 'failed')")
            .fetch_all(&storage.pg).await
        {
            Ok(rows) => {
                for r in rows {
                    if let Ok(cid) = r.try_get::<String, _>("contestant_id") {
                        if !found.contains(&cid) {
                            found.push(cid);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[VERIFIER-WARN] test_runs query failed (table may not exist yet): {e}");
            }
        }

        for cid in &found {
            if active.contains_key(cid) {
                continue;
            }
            eprintln!("[VERIFIER-SPAWN] Spawning verifier for contestant={}", cid);
            let s = Arc::clone(&storage);
            let c = config.clone();
            let cid2 = cid.clone();
            let handle = tokio::spawn(async move {
                let mut ver = Verifier::new(s, c, &cid2);
                if let Err(e) = ver.run().await {
                    eprintln!("[VERIFIER-ERR] contestant={} verifier failed: {e}", cid2);
                } else {
                    eprintln!("[VERIFIER-DONE] contestant={} verification complete", cid2);
                }
            });
            active.insert(cid.clone(), handle);
        }
        eprintln!("[VERIFIER-DISCOVER] found={} active={} — next discovery in {}s",
            found.len(), active.len(), config.poll_interval_secs);

        tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::sync::Arc;
    #[tokio::test]
    async fn tps_fields_updated_after_publish() {
        let cfg = Config {
            questdb_pgwire: "127.0.0.1:8812".into(),
            valkey_addr: "redis://127.0.0.1:6379".into(),
            redpanda_brokers: "".into(), contestant_id: "test".into(),
            drain_timeout_secs: 10, poll_interval_secs: 1, gap_timeout_secs: 10,
        };
        let storage = Arc::new(crate::storage::Storage::connect(&cfg).await.unwrap());
        let mut ver = Verifier::new(Arc::clone(&storage), cfg, "tps-test");
        assert!(ver.last_poll_time.is_none());
        assert_eq!(ver.last_total_fills, 0);
        // publish_summary with empty data — should update fields even with no events
        ver.publish_summary().await.unwrap();
        assert!(ver.last_poll_time.is_some(), "last_poll_time should be updated after publish_summary");
        assert_eq!(ver.last_total_fills, 0, "last_total_fills should match st.total_fills (0)");
    }

    #[tokio::test]
    /// MIN(exec_seq) query returns correct value in isolation.
    /// Requires QuestDB at 127.0.0.1:8812.
    async fn verifier_min_init_returns_correct_min() {
        let pg = crate::storage::Storage::connect(&Config {
            questdb_pgwire: "127.0.0.1:8812".into(),
            valkey_addr: "redis://127.0.0.1:6379".into(),
            redpanda_brokers: "".into(), contestant_id: "min-init-test".into(),
            drain_timeout_secs: 10, poll_interval_secs: 1, gap_timeout_secs: 10,
        }).await.unwrap();
        pg.ensure_schema().await.unwrap();
        // Clean any previous test rows
        if let Err(e) = sqlx::query("DELETE FROM exec_events WHERE contestant_id = 'min-init-test'")
            .execute(&pg.pg).await {
            eprintln!("[TEST] cleanup delete failed (ok if table new): {e}");
        }
        // Insert a single event with seq=5 to simulate out-of-order insert
        let ts = chrono::Utc::now().naive_utc();
        sqlx::query("INSERT INTO exec_events (ts,contestant_id,cl_ord_id,exec_id,exec_seq,exec_type,side,qty,price,is_market) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(ts).bind("min-init-test").bind("o1").bind("e1").bind(5i64).bind("0").bind("buy").bind(100i64).bind(100.0).bind(false)
            .execute(&pg.pg).await.unwrap();
        // QuestDB PG wire needs brief WAL flush delay
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // Query the min seq directly
        let min_seq: Option<i64> = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MIN(exec_seq) FROM exec_events WHERE contestant_id = 'min-init-test'"
        ).fetch_one(&pg.pg).await.unwrap();
        assert_eq!(min_seq, Some(5), "MIN(exec_seq) should be 5");
        // Cleanup
        if let Err(e) = sqlx::query("DELETE FROM exec_events WHERE contestant_id = 'min-init-test'")
            .execute(&pg.pg).await {
            eprintln!("[TEST] cleanup delete failed: {e}");
        }
    }
}
