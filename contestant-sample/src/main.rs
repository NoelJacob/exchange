mod fix_server;
mod ws_server;
use chrono::prelude::*;
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
use chrono::Utc;

// ── types used by fix_server and ws_server ───────────────────────────

/// Info tracked for a resting order so the dispatch task can route async
/// fill notifications back to the correct connection.
#[derive(Clone)]
pub struct OrderInfo {
    /// Target (recipient) identifier for async notifications — the original
    /// `sender_id` from the request (FIX `TargetCompID` / WS `target_id`).
    /// For omnibus, this is the user the response should route back to.
    pub target_id: String,
    pub cl_ord_id: String,
    pub order_qty: u64,
    pub cum_value_cents: u128,
    pub cum_qty: u64,
    pub ex_ord_id: String,
    pub connection_kind: ConnectionKind,
    pub price: f64
}

#[derive(Clone, Debug)]
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

/// Look up the orderbook for `symbol`, lazily creating one with a
pub fn get_or_create_book(
    books: &mut HashMap<String, Arc<OrderBook<()>>>,
    symbol: &str,
    trade_tx: &mpsc::UnboundedSender<orderbook_rs::orderbook::trade::TradeResult>,
) -> Arc<OrderBook<()>> {
    Arc::clone(
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
    )
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
    pending: &mut HashMap<Id, OrderInfo>,
    info: &OrderInfo,
) -> Result<SubmitOutcome, SubmitError> {
    let user_hash = crate::hash_user_id(&info.target_id);
    let limit_price_cents = (info.price * 100.0).round() as u128;
    let qty = info.order_qty;
    let taker_id = Id::new();
    let result: MatchResult = if is_market {
        book.submit_market_order_with_user(taker_id, qty, side, user_hash)
            .map_err(map_book_err)?
    } else {
        book.match_limit_order_with_user(taker_id, qty, side, limit_price_cents, user_hash)
            .map_err(map_book_err)?
    };
    eprintln!("[EXCHANGE-TRADE] {}: submit {} {} @ price_cents={} is_market={}",
        info.cl_ord_id, if side == Side::Buy { "BUY" } else { "SELL" }, qty, limit_price_cents, is_market);
    for (i, t) in result.trades().as_vec().iter().enumerate() {
        eprintln!("[EXCHANGE-TRADE] {}:   [{}] maker={:?} qty={} price_cents={}",
            info.cl_ord_id, i, t.maker_order_id(), t.quantity().as_u64(), t.price().as_u128());
    }
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
    let last_shares = result
        .trades()
        .as_vec()
        .last()
        .map(|t| t.quantity().as_u64())
        .unwrap_or(0);
    let last_px_cents = result
       .trades()
       .as_vec()
       .last()
       .map(|t| t.price().as_u128())
       .unwrap_or(0);

    if !is_market && resting_qty > 0 {
        let rest_id = Id::new();
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
        pending.insert(rest_id, info.clone());
    }
    // Taker's info is intentionally NOT inserted into `pending` — the
    // WS handler emits the taker's async fill notification synchronously
    // after `submit` returns (see `ws_server.rs`), avoiding the race
    // between the dispatch task and the WS handler writing to `ws_tx`.
    Ok(SubmitOutcome {
        filled_qty,
        avg_price_cents,
        resting_qty,
        last_shares,
        last_px_cents,
        cum_qty,
        leaves_qty,
    })
}
// ── ExecutionReport: shared structured type for WS + FIX responses ────
pub enum ExecutionReportMethod { New, Fill, Partial, Rejected }

/// Structured execution report shared by the WS and FIX response builders.
/// Created by [`build_execution_report`] (success) or manually for
/// fill notifications / rejects.
pub struct ExecutionReport {
    /// "New" | "Fill" | "Partial" | "Rejected"
    pub method: ExecutionReportMethod,
    pub target_id: String,
    pub transact_time: DateTime<Utc>,
    // "NONE" for rejected
    pub ex_ord_id: Option<String>,
    pub cl_ord_id: String,
    pub exec_id: String,
    pub symbol: String,
    pub side: Side,
    pub qty: u64,
    pub leaves_qty: u64,
    pub cum_qty: u64,
    // None for rejected
    pub last_shares: Option<u64>,
    // None for rejected
    pub last_px: Option<f64>,
    pub avg_px: f64,
    // Some for rejected
    pub reject_reason: Option<String>,
    pub is_market: bool,
    pub price: f64
}

pub fn build_execution_report(
    outcome: SubmitOutcome,
    info: &OrderInfo,
    symbol: &str,
    side: Side,
    exec_id: &str,
    is_market: bool,
) -> ExecutionReport {
    let filled = outcome.filled_qty > 0;
    let fully_filled = outcome.leaves_qty == 0 && filled;
    let method = if !filled {
        ExecutionReportMethod::New
    } else if fully_filled {
        ExecutionReportMethod::Fill
    } else {
        ExecutionReportMethod::Partial
    };
    let avg_px = if outcome.filled_qty > 0 {
        outcome.avg_price_cents as f64 / 100.0
    } else {
        0.0
    };

    ExecutionReport {
        method,
        ex_ord_id: Some(info.ex_ord_id.clone()),
        exec_id: exec_id.to_string(),
        symbol: symbol.to_string(),
        side,
        qty: info.order_qty,
        leaves_qty: outcome.leaves_qty,
        cum_qty: outcome.cum_qty,
        last_shares: Some(outcome.last_shares),
        last_px: Some(outcome.last_px_cents as f64 / 100.0),
        avg_px,
        cl_ord_id: info.cl_ord_id.clone(),
        target_id: info.target_id.clone(),
        reject_reason: None,
        transact_time: Utc::now(),
        is_market,
        price: info.price
    }
}
/// Build an [`ExecutionReport`] for a business-logic rejection.
pub fn build_reject_report(
    reason: String,
    info: &OrderInfo,
    symbol: &str,
    side: Side,
    exec_id: &str,
    is_market: bool,
) -> ExecutionReport {
    ExecutionReport {
        method: ExecutionReportMethod::Rejected,
        ex_ord_id: Some(info.ex_ord_id.clone()),
        exec_id: exec_id.to_string(),
        symbol: symbol.to_string(),
        side,
        qty: info.order_qty,
        leaves_qty: 0,
        cum_qty: 0,
        last_shares: None,
        last_px: None,
        avg_px: 0.0,
        cl_ord_id: info.cl_ord_id.clone(),
        target_id: info.target_id.clone(),
        reject_reason: Some(reason),
        transact_time: Utc::now(),
        is_market,
        price: info.price
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
                let fill_qty = trade.quantity().as_u64();
                let fill_price_cents = trade.price().as_u128();
                let side = trade.maker_side();
                let (report, info, exec_id) = {
                let mut pending = dispatch_state.pending.lock();
                let info = pending.get_mut(&maker_id).expect("[FIX] Missing OrderInfo");
                info.cum_value_cents += fill_price_cents * fill_qty as u128;
                info.cum_qty += fill_qty;
                // Get leaves from book if order still there.
                let leaves = dispatch_state
                    .books
                    .lock()
                    .get(&result.symbol)
                    .and_then(|b| b.get_order(maker_id))
                    .map(|o| o.visible_quantity())
                    .unwrap_or(0);

                let info = info.clone();
                    if leaves == 0 {
                        pending.remove(&maker_id);
                }

                let outcome = SubmitOutcome {
                    filled_qty: fill_qty,
                    avg_price_cents: if info.cum_qty > 0 { info.cum_value_cents / info.cum_qty as u128 } else { 0 },
                    resting_qty: leaves,
                    last_shares: fill_qty,
                    last_px_cents: fill_price_cents,
                    cum_qty: info.cum_qty,
                    leaves_qty: leaves,
                };

                // Reserve seq BEFORE building report (exec_id is part of the FIX/WS payload)
                let exec_id = dispatch_state.exec_id_seq
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    .to_string();
                eprintln!("[EXEC-ID-GEN] Dispatch: maker_id={:?} exec_id={} (before report)", maker_id, exec_id);

                let report = crate::build_execution_report(
                    outcome,
                    &info,
                    &result.symbol,
                    side,
                    &exec_id,
                    false,
                );
                eprintln!("[EXCHANGE-DISPATCH] {}: sending maker fill via {:?} last_shares={} last_px_cents={} exec_id={}",
                    info.cl_ord_id, info.connection_kind, fill_qty, fill_price_cents, exec_id);

                (report, info, exec_id)
            };

                let send_ok = match info.connection_kind {
                    ConnectionKind::Fix { session_id, reply_tx } => {
                        let resp = fix_server::report_to_fix(&report);
                        reply_tx.send(fix_server::PendingReply {
                            msg: resp,
                            session_id,
                        }).is_ok()
                    },
                    ConnectionKind::Ws { sender } => {
                        let val = ws_server::report_to_json(&report);
                        let notif = serde_json::json!({
                            "jsonrpc": "2.0",
                            "method": val.get("method"),
                            "params": val.get("params")
                        });
                        sender.send(notif.to_string()).is_ok()
                    },
                };

                if !send_ok {
                    eprintln!("[DISPATCH] Dropped maker fill for {} — connection closed (seq={} lost)", info.cl_ord_id, exec_id);
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
