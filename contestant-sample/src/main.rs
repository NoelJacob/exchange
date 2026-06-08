mod fix_server;
mod ws_server;
use orderbook_rs::{OrderBook, TimeInForce};
use sha2::{Digest, Sha256};

use orderbook_rs::Side;
use parking_lot::Mutex;
use pricelevel::{Hash32, Id, MatchResult};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::mpsc;

// ── types used by fix_server and ws_server ───────────────────────────

/// Info tracked for a resting order so the dispatch task can route async
/// fill notifications back to the correct connection.
#[derive(Clone)]
pub struct OrderInfo {
    pub user_id: String,
    /// Target (recipient) identifier for async notifications — the original
    /// `sender_id` from the request (FIX `TargetCompID` / WS `target_id`).
    /// For omnibus, this is the user the response should route back to.
    pub target_id: String,
    pub cl_ord_id: String,
    pub order_qty: u64,
    pub cum_value_cents: u128,
    pub ex_ord_id: String,
    pub connection_kind: ConnectionKind,
}

#[derive(Clone)]
pub enum ConnectionKind {
    Fix {
        session_id: Arc<fixer::session::session_id::SessionID>,
        reply_tx: mpsc::UnboundedSender<fix_server::PendingReply>,
    },
    Ws {
        sender: mpsc::UnboundedSender<String>,
    },
}

/// Shared mutable state for the entire application.
pub struct AppState {
    pub books: Mutex<HashMap<String, Arc<OrderBook<()>>>>,
    pub pending: Mutex<HashMap<Id, OrderInfo>>,
    pub trade_tx: mpsc::UnboundedSender<orderbook_rs::orderbook::trade::TradeResult>,
    pub order_id_seq: AtomicU64,
    pub exec_id_seq: AtomicU64,
}

// ── helpers ──────────────────────────────────────────────────────────

/// SHA-256 hash of a user_id string, used as STP identifier.
pub fn hash_user_id(user_id: &str) -> Hash32 {
    let mut hasher = Sha256::new();
    hasher.update(user_id.as_bytes());
    let result = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&result[..32]);
    Hash32::new(bytes)
}
/// Break a UNIX timestamp (duration since epoch) into calendar components.
fn unix_ms_to_parts(now: &std::time::Duration) -> (i64, usize, i64, u64, u64, u64, u64) {
    let s = now.as_secs();
    let ms = now.subsec_millis();
    let days = s / 86400;
    let time_secs = s % 86400;
    let h = time_secs / 3600;
    let m = (time_secs % 3600) / 60;
    let sec = time_secs % 60;
    let mut y = 1970i64;
    let mut d = days as i64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if d < days_in_year {
            break;
        }
        d -= days_in_year;
        y += 1;
    }
    let month_days = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut mo = 1usize;
    for &md in &month_days {
        if d < md {
            break;
        }
        d -= md;
        mo += 1;
    }
    (y, mo, d + 1, h, m, sec, ms as u64)
}

/// Current time formatted as `YYYYMMDD-HH:MM:SS` for FIX Tag 60.
pub fn tag60_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let (y, mo, d, h, m, s, _ms) = unix_ms_to_parts(&now);
    format!("{:04}{:02}{:02}-{:02}:{:02}:{:02}", y, mo, d, h, m, s)
}

/// Current time formatted as ISO 8601 with millisecond precision
/// (`YYYY-MM-DDTHH:MM:SS.sssZ`) for the WS `sending_time` field.
pub fn tag60_iso8601_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let (y, mo, d, h, m, s, ms) = unix_ms_to_parts(&now);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y, mo, d, h, m, s, ms
    )
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Look up the orderbook for `symbol`, lazily creating one with a
pub fn get_or_create_book(
    books: &mut HashMap<String, Arc<OrderBook<()>>>,
    symbol: &str,
    trade_tx: &mpsc::UnboundedSender<orderbook_rs::orderbook::trade::TradeResult>,
) -> Option<Arc<OrderBook<()>>> {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return None;
    }
    Some(Arc::clone(
        books
            .entry(symbol.to_string())
            .or_insert_with(|| {
                let tx = trade_tx.clone();
                let listener: orderbook_rs::orderbook::trade::TradeListener = Arc::new(
                    move |result: &orderbook_rs::orderbook::trade::TradeResult| {
                        let _ = tx.send(result.clone());
                    },
                );
                Arc::new(OrderBook::with_trade_listener(symbol, listener))
            }),
    ))
}

// ── existing submit infrastructure ───────────────────────────────────

/// Result of a successful submit.
#[derive(Debug, Clone)]
pub struct SubmitOutcome {
    /// Total quantity filled (Σ trade quantities).
    pub filled_qty: u64,
    /// Volume-weighted average fill price in integer cents.
    /// `0` if `filled_qty == 0`.
    pub avg_price_cents: u128,
    /// For limit orders, quantity left to rest on the book (0 if fully
    /// filled). Always 0 for market orders.
    pub resting_qty: u64,
    /// Echo of the limit price in cents, for the resting order. 0 for
    /// market orders.
    pub limit_price_cents: u128,
    /// Exchange-assigned order ID, stable across all reports for this order.
    pub ex_ord_id: String,
    /// Execution report ID for this specific execution (unique per report).
    pub exec_id: String,
    /// Quantity filled in the last (or only) match.
    pub last_shares: u64,
    /// Fill price of the last (or only) match in integer cents.
    pub last_px_cents: u128,
    /// Cumulative filled quantity for this order.
    pub cum_qty: u64,
    /// Remaining quantity after this execution (0 for fully-filled/market).
    pub leaves_qty: u64,
}

/// Error returned by [`submit`].
///
/// These are recoverable business errors, not library corruption:
/// callers should surface a Reject to the client.
#[derive(Debug)]
pub enum SubmitError {
    /// Empty or partial opposite side. Market-order-only path can hit
    /// this; limit orders still rest at the limit price.
    InsufficientLiquidity {
        side: Side,
        requested: u64,
        available: u64,
    },
    /// Self-Trade Prevention cancelled the taker before any fills.
    SelfTradePrevented,
    /// Operator-engaged kill switch is active.
    KillSwitchActive,
    /// Pre-trade risk check failed (open-order count, notional, price band).
    RiskRejected { reason: String },
    /// Underlying pricelevel error (e.g. checked arithmetic overflow,
    /// invalid input, internal state issue).
    PriceLevel(String),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientLiquidity {
                side,
                requested,
                available,
            } => write!(
                f,
                "insufficient liquidity on {:?} side: requested {} available {}",
                side, requested, available
            ),
            Self::SelfTradePrevented => f.write_str("self-trade prevented"),
            Self::KillSwitchActive => f.write_str("kill switch active"),
            Self::RiskRejected { reason } => write!(f, "risk gate: {reason}"),
            Self::PriceLevel(msg) => write!(f, "price level error: {msg}"),
        }
    }
}

impl std::error::Error for SubmitError {}

/// Map an `OrderBookError` from the matching calls into our `SubmitError`.
fn map_book_err(e: orderbook_rs::OrderBookError) -> SubmitError {
    use orderbook_rs::OrderBookError;
    match e {
        OrderBookError::InsufficientLiquidity {
            side,
            requested,
            available,
        } => SubmitError::InsufficientLiquidity {
            side,
            requested,
            available,
        },
        OrderBookError::SelfTradePrevented { .. } => SubmitError::SelfTradePrevented,
        OrderBookError::KillSwitchActive => SubmitError::KillSwitchActive,
        OrderBookError::RiskMaxOpenOrders { .. }
        | OrderBookError::RiskMaxNotional { .. }
        | OrderBookError::RiskPriceBand { .. } => SubmitError::RiskRejected {
            reason: e.to_string(),
        },
        other => SubmitError::PriceLevel(other.to_string()),
    }
}

/// Submit a market or limit order to a book, returning the outcome or a
/// typed error.
///
/// For limit orders with unfilled remainder, the helper automatically
/// re-adds the order to the book as a resting GTC order at the limit
/// price, and registers it in `pending` for async fill notifications.
///
/// **TradeListener is assumed to be already set on `book`** — the
/// `match_limit_order_with_user` / `submit_market_order_with_user` calls
/// fire the listener for every match, notifying the *existing* resting
/// order's owner. The taker (this call) gets a sync response via the
/// returned `SubmitOutcome`.
#[allow(clippy::too_many_arguments)]
pub fn submit(
    book: &OrderBook<()>,
    is_market: bool,
    side: Side,
    qty: u64,
    limit_price_cents: u128,
    user_hash: Hash32,
    pending: &mut HashMap<Id, OrderInfo>,
    mut info: OrderInfo,
    order_id_seq: &AtomicU64,
    exec_id_seq: &AtomicU64,
) -> Result<SubmitOutcome, SubmitError> {
    let taker_id = Id::new();
    let result: MatchResult = if is_market {
        book.submit_market_order_with_user(taker_id, qty, side, user_hash)
            .map_err(map_book_err)?
    } else {
        book.match_limit_order_with_user(taker_id, qty, side, limit_price_cents, user_hash)
            .map_err(map_book_err)?
    };
    let filled_qty = result
        .executed_quantity()
        .map_err(|e| SubmitError::PriceLevel(e.to_string()))?;
    let total_value = result
        .executed_value()
        .map_err(|e| SubmitError::PriceLevel(e.to_string()))?;
    let avg_price_cents = if filled_qty > 0 {
        total_value / filled_qty as u128
    } else {
        0
    };
    let resting_qty = if is_market {
        0
    } else {
        result.remaining_quantity()
    };
    let leaves_qty = if is_market {
        0
    } else {
        qty.saturating_sub(filled_qty)
    };
    let cum_qty = filled_qty;
    let last_shares = filled_qty;
    let last_px_cents = avg_price_cents;
    let ex_ord_id = order_id_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let exec_id = exec_id_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string();

    if !is_market && resting_qty > 0 {
        let rest_id = Id::new();
        info.ex_ord_id = ex_ord_id.clone();
        book.add_limit_order_with_user(
            rest_id,
            limit_price_cents,
            resting_qty,
            side,
            TimeInForce::Gtc,
            user_hash,
            None::<()>,
        )
        .map_err(|e| SubmitError::PriceLevel(format!("resting add failed: {e}")))?;
        pending.insert(rest_id, info);
    }
    // Taker's info is intentionally NOT inserted into `pending` — the
    // WS handler emits the taker's async fill notification synchronously
    // after `submit` returns (see `ws_server.rs`), avoiding the race
    // between the dispatch task and the WS handler writing to `ws_tx`.
    Ok(SubmitOutcome {
        filled_qty,
        avg_price_cents,
        resting_qty,
        limit_price_cents,
        ex_ord_id,
        exec_id,
        last_shares,
        last_px_cents,
        cum_qty,
        leaves_qty,
    })
}
// ── ExecutionReport: shared structured type for WS + FIX responses ────

/// Structured execution report shared by the WS and FIX response builders.
/// Created by [`build_execution_report`] (success) or manually for
/// fill notifications / rejects.
pub struct ExecutionReport {
    /// "added" | "fill" | "partial" | "rejected"
    pub method: &'static str,
    pub ex_ord_id: String,
    pub exec_id: String,
    pub symbol: String,
    /// "buy" or "sell"
    pub side: String,
    pub qty: u64,
    pub leaves_qty: u64,
    pub cum_qty: u64,
    pub last_shares: u64,
    pub last_px: f64,
    pub avg_px: f64,
    pub cl_ord_id: String,
    /// Original client that placed the order (for routing the response back).
    pub target_id: String,
    pub reject_reason: Option<String>,
    pub sending_time: String,
}
pub fn build_execution_report(
    outcome: &SubmitOutcome,
    info: &OrderInfo,
    symbol: &str,
    side: Side,
    order_qty: u64,
) -> ExecutionReport {
    let filled = outcome.filled_qty > 0;
    let fully_filled = outcome.leaves_qty == 0 && filled;
    let method = if !filled {
        "added"
    } else if fully_filled {
        "fill"
    } else {
        "partial"
    };
    let avg_px = if outcome.filled_qty > 0 {
        outcome.avg_price_cents as f64 / 100.0
    } else {
        0.0
    };
    ExecutionReport {
        method,
        ex_ord_id: outcome.ex_ord_id.clone(),
        exec_id: outcome.exec_id.clone(),
        symbol: symbol.to_string(),
        side: match side {
            Side::Buy => "buy".to_string(),
            Side::Sell => "sell".to_string(),
        },
        qty: order_qty,
        leaves_qty: outcome.leaves_qty,
        cum_qty: outcome.cum_qty,
        last_shares: outcome.last_shares,
        last_px: outcome.last_px_cents as f64 / 100.0,
        avg_px,
        cl_ord_id: info.cl_ord_id.clone(),
        target_id: info.target_id.clone(),
        reject_reason: None,
        sending_time: tag60_iso8601_now(),
    }
}
/// Build an [`ExecutionReport`] for a business-logic rejection.
pub fn build_reject_report(
    reason: &str,
    info: &OrderInfo,
    symbol: &str,
    side: Side,
    order_qty: u64,
    exec_id: &str,
) -> ExecutionReport {
    ExecutionReport {
        method: "rejected",
        ex_ord_id: "NONE".to_string(),
        exec_id: exec_id.to_string(),
        symbol: symbol.to_string(),
        side: match side {
            Side::Buy => "buy".to_string(),
            Side::Sell => "sell".to_string(),
        },
        qty: order_qty,
        leaves_qty: 0,
        cum_qty: 0,
        last_shares: 0,
        last_px: 0.0,
        avg_px: 0.0,
        cl_ord_id: info.cl_ord_id.clone(),
        target_id: info.target_id.clone(),
        reject_reason: Some(reason.to_string()),
        sending_time: tag60_iso8601_now(),
    }
}
/// Build and dispatch a fill notification to the order's connection.
/// Used by the dispatch task for both maker and taker notifications.
#[allow(clippy::too_many_arguments)]
fn send_fill_notification(
    info: &OrderInfo,
    symbol: &str,
    side: Side,
    order_qty: u64,
    fill_qty: u64,
    fill_price_cents: u128,
    leaves: u64,
    cum: u64,
    cum_value_cents: u128,
    state: &AppState,
) {
    let exec_id = state
        .exec_id_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let method = if leaves == 0 { "fill" } else { "partial" };
    let avg_px = if cum > 0 {
        cum_value_cents as f64 / cum as f64 / 100.0
    } else {
        0.0
    };

    let report = ExecutionReport {
        method,
        ex_ord_id: info.ex_ord_id.clone(),
        exec_id,
        symbol: symbol.to_string(),
        side: match side {
            Side::Buy => "buy".to_string(),
            Side::Sell => "sell".to_string(),
        },
        qty: order_qty,
        leaves_qty: leaves,
        cum_qty: cum,
        last_shares: fill_qty,
        last_px: fill_price_cents as f64 / 100.0,
        avg_px,
        cl_ord_id: info.cl_ord_id.clone(),
        target_id: info.target_id.clone(),
        reject_reason: None,
        sending_time: tag60_iso8601_now(),
    };

    match &info.connection_kind {
        ConnectionKind::Fix {
            session_id,
            reply_tx,
        } => {
            let mut reply = fixer::message::Message::new();
            reply
                .header
                .set_string(fixer::tag::TAG_SENDER_COMP_ID, "XCANG3");
            reply
                .header
                .set_string(fixer::tag::TAG_TARGET_COMP_ID, &session_id.target_comp_id);
            fix_server::exec_report_to_fix_body(&report, &mut reply);
            if reply_tx.send(fix_server::PendingReply {
                msg: reply,
                session_id: Arc::clone(session_id),
            }).is_err() {
                eprintln!("[FIX] Failed to send async fill notification");
            }
        }
        ConnectionKind::Ws { sender } => {
            ws_server::send_notification(sender, &report);
        }
    }
}

// ── dispatch task & main ────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Contestant sample matching engine starting...");

    let (trade_tx, mut trade_rx) =
        mpsc::unbounded_channel::<orderbook_rs::orderbook::trade::TradeResult>();
    let state = Arc::new(AppState {
        books: Mutex::new(HashMap::new()),
        pending: Mutex::new(HashMap::new()),
        trade_tx,
        order_id_seq: AtomicU64::new(1),
        exec_id_seq: AtomicU64::new(1),
    });
    // Dispatch task: receives TradeResults from all books and routes async
    // fill notifications to the *resting* order's original connection.
    // The taker's fill details are returned synchronously by `submit`
    // (FIX: ExecReport via from_app; WS: included in the sync response).
    let dispatch_state = Arc::clone(&state);
    tokio::spawn(async move {
        while let Some(result) = trade_rx.recv().await {
            for trade in result.match_result.trades().as_vec() {
                let maker_id = trade.maker_order_id();
                // Peek at pending info without removing — we may need it
                // again for a subsequent partial fill.
                let maker_info = {
                    let pending = dispatch_state.pending.lock();
                    pending.get(&maker_id).cloned()
                };
                if let Some(mut info) = maker_info {
                    let fill_qty = trade.quantity().as_u64();
                    let fill_price_cents = trade.price().as_u128();
                    let side = trade.maker_side();
                    info.cum_value_cents += fill_price_cents * fill_qty as u128;
                    // Get leaves/cum from book if order still there.
                    let (leaves, cum) = dispatch_state
                        .books
                        .lock()
                        .get(&result.symbol)
                        .and_then(|b| b.get_order(maker_id))
                        .map(|o| (o.visible_quantity(), info.order_qty.saturating_sub(o.visible_quantity())))
                        .unwrap_or((0, info.order_qty));
                    // Write back cum_value_cents to pending (remove on full fill).
                    {
                        let mut pending = dispatch_state.pending.lock();
                        if leaves == 0 {
                            pending.remove(&maker_id);
                        } else if let Some(entry) = pending.get_mut(&maker_id) {
                            entry.cum_value_cents = info.cum_value_cents;
                        }
                    }
                    send_fill_notification(
                        &info,
                        &result.symbol,
                        side,
                        info.order_qty,
                        fill_qty,
                        fill_price_cents,
                        leaves,
                        cum,
                        info.cum_value_cents,
                        &dispatch_state,
                    );
                }
            }
        }
    });

    let fix_state = Arc::clone(&state);
    let fix_handle = tokio::spawn(async move {
        if let Err(e) = fix_server::run(fix_state).await {
            eprintln!("FIX server error: {}", e);
        }
    });

    let ws_state = Arc::clone(&state);
    let ws_handle = tokio::spawn(async move {
        if let Err(e) = ws_server::run(ws_state).await {
            eprintln!("WS server error: {}", e);
        }
    });

    eprintln!("FIX server listening on 0.0.0.0:9090");
    eprintln!("WebSocket server listening on 0.0.0.0:8080");

    tokio::select! {
        _ = ws_handle => eprintln!("WS server exited"),
        _ = fix_handle => eprintln!("FIX server exited"),
    }

    Ok(())
}
