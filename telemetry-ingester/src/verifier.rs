use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use orderbook_rs::orderbook::book::OrderBook;
use pricelevel::{Side, TimeInForce, Hash32, Id, MatchResult};
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
        let last_shares = all_trades.last().map(|t| t.quantity().as_u64()).unwrap_or(0);
        let last_px_cents = all_trades.last().map(|t| t.price().as_u128()).unwrap_or(0);

        self.orders_sent += 1;
        eprintln!("[VERIFY-SUBMIT] {}: trades={} last=({}@{}) filled={} resting={} leaves_qty={:?}",
            cl_ord_id, all_trades.len(), last_shares, last_px_cents, filled_qty, resting_qty, exchange_leaves_qty);
        for (i, t) in all_trades.iter().enumerate() {
            eprintln!("[VERIFY-TRADES] {}:   [{}] maker={:?} qty={} price_cents={}",
                cl_ord_id, i, t.maker_order_id(), t.quantity().as_u64(), t.price().as_u128());
        }

        if last_shares > 0 {
            let exp_px = last_px_cents as f64 / 100.0;
            eprintln!("[VERIFY-EXPECT] {}: last_trade({:.4}, {})", cl_ord_id, exp_px, last_shares);
            self.pending_fills.insert(cl_ord_id.to_string(), VecDeque::from([
                ExpectedFill { price_f64: exp_px, qty: last_shares }
            ]));
        } else {
            self.pending_fills.insert(cl_ord_id.to_string(), VecDeque::new());
        }

        let book_resting = exchange_leaves_qty.unwrap_or(resting_qty);
        if !is_market && book_resting > 0 {
            let rest_id = Id::new();
            if let Err(e) = self.book.add_limit_order_with_user(
                rest_id, limit_price_cents, book_resting, side,
                TimeInForce::Gtc, user_hash, None::<()>,
            ) {
                eprintln!("[VERIFY-ERR] {}: resting add FAILED: {:?}", cl_ord_id, e);
            } else {
                eprintln!("[VERIFY-RESTING] {}: {} @ {} cents", cl_ord_id, book_resting, limit_price_cents);
            }
        }
    }

    fn compare_fill(&mut self, exec: &ExecutionEvent) -> (String, f64, Option<CorrectnessRow>) {
        self.execs_received += 1;
        let reported_shares = exec.last_shares.unwrap_or(0);
        let reported_px = exec.last_px.unwrap_or(0.0);
        eprintln!("[VERIFY-CMP] {}: exchange reports last_shares={} last_px={:.4}",
            exec.cl_ord_id, reported_shares, reported_px);

        if let Some(q) = self.pending_fills.get_mut(&exec.cl_ord_id) {
            if let Some(exp) = q.pop_front() {
                self.total_fills += 1;
                let diff = (reported_px - exp.price_f64).abs();
                let p_ok = diff < 0.001;
                let q_ok = reported_shares == exp.qty;
                let (v, p) = match (p_ok, q_ok) {
                    (true, true)  => { self.correct_fills += 1; ("correct", 1.0) }
                    (false, true) => ("wrong_price", -2.0),
                    (true, false) => ("wrong_qty", -1.0),
                    _             => { eprintln!("[VERIFY-ERR] {}: exp({:.4},{}) actual({:.4},{}) diff={:.6}",
                        exec.cl_ord_id, exp.price_f64, exp.qty, reported_px, reported_shares, diff); ("wrong_price+qty", -3.0) }
                };
                self.total_penalty += p;
                eprintln!("[VERIFY-CMP] {}: expected({:.4},{}) actual({:.4},{}) -> {}",
                    exec.cl_ord_id, exp.price_f64, exp.qty, reported_px, reported_shares, v);
                return (v.to_string(), p, Some(CorrectnessRow {
                    contestant_id: exec.contestant_id.clone(), cl_ord_id: exec.cl_ord_id.clone(),
                    exec_id: exec.exec_id.clone(), verdict: v.to_string(), penalty: p,
                    expected_px: exp.price_f64, actual_px: reported_px,
                    expected_qty: exp.qty, actual_qty: reported_shares,
                }));
            }
            // Maker fill (resting order matched by taker)
            self.total_fills += 1;
            self.correct_fills += 1;
            self.total_penalty += 1.0;
            eprintln!("[VERIFY-MAKER] {}: accepted (resting fill @ {:.4}x{})", exec.cl_ord_id, reported_px, reported_shares);
            return ("correct".to_string(), 1.0, Some(CorrectnessRow {
                contestant_id: exec.contestant_id.clone(), cl_ord_id: exec.cl_ord_id.clone(),
                exec_id: exec.exec_id.clone(), verdict: "correct".into(), penalty: 1.0,
                expected_px: reported_px, actual_px: reported_px,
                expected_qty: reported_shares, actual_qty: reported_shares,
            }));
        }
        // No pending_fills → ghost (market order late-submitted at compare_fill time)
        self.total_penalty += -0.5;
        eprintln!("[VERIFY-GHOST] {}: no pending_fills", exec.cl_ord_id);
        ("ghost".to_string(), -0.5, None)
    }
}

pub struct Verifier {
    storage: Arc<Storage>, config: Config, states: HashMap<String, ContestantState>, next_seq: i64,
}

impl Verifier {
    pub fn new(storage: Arc<Storage>, config: Config) -> Self {
        Self { storage, config, states: HashMap::new(), next_seq: 0 }
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut poll_no = 0u64;
        loop {
            poll_no += 1;
            self.poll(poll_no).await?;
            tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs)).await;
        }
    }

    async fn poll(&mut self, poll_no: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Outer loop: re-fetch from gap position after waiting for missing events.
        'outer: loop {
            let rows = sqlx::query(
                "SELECT exec_seq, contestant_id, cl_ord_id, exec_id, exec_type, side, qty, price, is_market, last_shares, last_px, leaves_qty, cum_qty FROM exec_events WHERE exec_seq >= $1 ORDER BY exec_seq LIMIT 500"
            )
            .bind(self.next_seq as i64)
            .fetch_all(&self.storage.pg).await?;
            if rows.is_empty() {
                eprintln!("[VERIFIER] poll {}: no rows >= seq {} (idle)", poll_no, self.next_seq);
                return Ok(());
            }

            let mut expected = self.next_seq;
            let mut processed = false;
            let mut gap_seqs: Vec<i64> = Vec::new();
            let mut idx = 0usize;
            eprintln!("[VERIFIER] poll {}: {} rows >= seq {}, expected seq={}",
                poll_no, rows.len(), self.next_seq, expected);

            while idx < rows.len() {
                let row = &rows[idx];
                let es: i64 = row.try_get("exec_seq").unwrap_or(-1);
                if es < 0 { idx += 1; continue; }

                if es > expected {
                    if processed {
                        // Gap — wait for the missing event in a tight loop, then re-fetch.
                        gap_seqs.push(expected);
                        eprintln!("[VERIFIER-GAP] expected seq={} but found seq={} — waiting for events to arrive", expected, es);

                        // Tight wait: query for the specific expected seq every 100ms.
                        // Timeout after config.gap_timeout_secs.
                        let max_iters = self.config.gap_timeout_secs * 10; // 10 polls per sec
                        let mut wait_iters = 0u64;
                        'wait: loop {
                            wait_iters += 1;
                            if wait_iters > max_iters {
                                eprintln!("[VERIFIER-ERR] seq={} still missing after {}s — gap never cleared, exiting poll", expected, self.config.gap_timeout_secs);
                                break 'outer Ok(());
                            }
                            let check = sqlx::query(
                                "SELECT 1 FROM exec_events WHERE exec_seq = $1 LIMIT 1"
                            )
                            .bind(expected)
                            .fetch_optional(&self.storage.pg).await;
                            match check {
                                Ok(Some(_)) => {
                                    eprintln!("[VERIFIER-GAP] seq={} arrived after {} waits, re-fetching from idx={} es={}", expected, wait_iters, idx, es);
                                    break 'wait;
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    eprintln!("[VERIFIER-ERR] check for seq={} failed: {e:?}", expected);
                                    break 'wait;
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }

                        // Re-fetch from the gap position — events before expected were
                        // already processed; events >= expected are re-queried.
                        self.next_seq = expected;
                        continue 'outer;
                    }
                    // Very first event starts above expected — plausible offset.
                    eprintln!("[VERIFIER-START] first event seq={} > expected={}, adjusting", es, expected);
                    expected = es;
                }
                if es < expected { idx += 1; continue; }

                processed = true;

                // — process this event (es == expected) —
                let side: String = match row.try_get::<String, _>("side") {
                    Ok(s) if !s.is_empty() => s,
                    _ => { eprintln!("[VERIFY-ERR] missing side at seq={}", es); expected += 1; idx += 1; continue; }
                };

                let exec = ExecutionEvent {
                    contestant_id: row.try_get("contestant_id")?,
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
                eprintln!("[VERIFY-ROW] {}: seq={} type={} side={} qty={} price={:.4} market={} lst_shares={:?} lst_px={:?} lv_qty={:?} cum_qty={:?}",
                    exec.cl_ord_id, exec.exec_seq, exec.exec_type, exec.side, exec.qty,
                    exec.price, exec.is_market, exec.last_shares, exec.last_px,
                    exec.leaves_qty, exec.cum_qty);

                let target_id = exec.contestant_id.clone();
                let st = self.states.entry(target_id.clone())
                    .or_insert_with(|| ContestantState::new(&target_id));

                match exec.exec_type.as_str() {
                    "0" => {
                        eprintln!("[VERIFY-EVENT] NEW seq={} {} {} {}@{:.4} is_market={}",
                            exec.exec_seq, exec.cl_ord_id, exec.side, exec.qty, exec.price, exec.is_market);
                        let ob_side = if exec.side == "buy" { Side::Buy } else { Side::Sell };
                        st.submit_order(ob_side, exec.qty, exec.price,
                            exec.is_market, &exec.cl_ord_id, exec.leaves_qty);
                    }
                    "1" | "2" => {
                        eprintln!("[VERIFY-EVENT] FILL seq={} {} {} last=({}@{:.4}) is_market={}",
                            exec.exec_seq, exec.cl_ord_id, exec.side,
                            exec.last_shares.unwrap_or(0), exec.last_px.unwrap_or(0.0),
                            exec.is_market);

                        let ob_side = if exec.side == "buy" { Side::Buy } else { Side::Sell };
                        // Orders that fill immediately (both market and aggressive limit)
                        // never emit ExecType=0 — submit now so the ref book matches.
                        if !st.pending_fills.contains_key(&exec.cl_ord_id) {
                            eprintln!("[VERIFY-EVENT] immediate-fill order {} submitting to ref book (is_market={})", exec.cl_ord_id, exec.is_market);
                            st.submit_order(ob_side, exec.qty, exec.price,
                                exec.is_market, &exec.cl_ord_id, exec.leaves_qty);
                        }

                        let (verdict, penalty, row_cr) = st.compare_fill(&exec);
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
                    "8" => eprintln!("[VERIFY-EVENT] REJECT seq={} {}", exec.exec_seq, exec.cl_ord_id),
                    _ => eprintln!("[VERIFY-EVENT] UNKNOWN seq={} type={}", exec.exec_seq, exec.exec_type),
                }

                expected += 1;
                idx += 1;
            }

            // Advance checkpoint
            let prev = self.next_seq;
            self.next_seq = expected;
            let processed_count = expected - prev - gap_seqs.len() as i64;
            if processed_count > 0 || !gap_seqs.is_empty() {
                eprintln!("[VERIFIER-CKPT] processed {} events, {} gaps, next_seq={}",
                    processed_count, gap_seqs.len(), expected);
            }

            // Report lost fills and update leaderboard
            for (cid, st) in &self.states {
                let lost_count: usize = st.pending_fills.values().map(|q| q.len()).sum();
                if lost_count > 0 {
                    eprintln!("[VERIFY-LOST] {}: {} pending expected fills lost", cid, lost_count);
                    for (cl, q) in &st.pending_fills {
                        for f in q {
                            eprintln!("[VERIFY-LOST] {}: ({:.4}, {})", cl, f.price_f64, f.qty);
                            if let Err(e) = self.storage.insert_lost(cid, cl, f.price_f64, f.qty).await {
                                eprintln!("[VERIFY-ERR] insert_lost: {e}");
                            }
                        }
                    }
                }
                let pct = if st.total_fills > 0 { st.correct_fills as f64 / st.total_fills as f64 * 100.0 } else { 0.0 };
                if let Err(e) = self.storage.upsert_summary(cid, "running",
                    st.orders_sent, st.execs_received, st.correct_fills, st.total_fills, st.total_penalty).await {
                    eprintln!("[VERIFY-ERR] upsert_summary: {e}");
                }
                let snap = serde_json::json!({
                    "contestant_id": cid, "status": "running",
                    "orders_sent": st.orders_sent, "fills": st.execs_received,
                    "correctness_pct": pct, "total_penalty": st.total_penalty,
                });
                if let Err(e) = self.storage.publish_leaderboard(&snap.to_string()).await {
                    eprintln!("[VERIFY-ERR] publish_leaderboard: {e}");
                }
            }
            return Ok(());
        }
    }

}
