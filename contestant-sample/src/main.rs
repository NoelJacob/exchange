mod fix_server;
mod ws_server;

use orderbook_rs::{OrderBook, Side};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Multi-symbol book registry.
///
/// We use a manual `RwLock<HashMap<...>>` rather than `orderbook_rs::BookManager`
/// because `BookManager::get_book` returns `&OrderBook` (borrowed from the
/// manager) and `add_book` requires `&mut self`. That shape forces a single
/// global mutex held for the entire order processing pipeline, which serializes
/// all matching across all symbols and defeats the lock-free `OrderBook`
/// internals. Our shape — a tiny map guard released before any book call —
/// preserves per-symbol lock-free matching.
pub type Books = Arc<RwLock<HashMap<String, Arc<orderbook_rs::OrderBook<()>>>>>;

/// Look up the orderbook for `symbol`, lazily creating one on first use.
///
/// Returns `None` if `symbol` is empty or whitespace-only so callers can
/// reject the order.
pub fn get_or_create_book(books: &Books, symbol: &str) -> Option<Arc<orderbook_rs::OrderBook<()>>> {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return None;
    }
    if let Some(book) = books.read().get(symbol) {
        return Some(Arc::clone(book));
    }
    let mut w = books.write();
    Some(
        w.entry(symbol.to_string())
            .or_insert_with(|| Arc::new(orderbook_rs::OrderBook::<()>::new(symbol)))
            .clone(),
    )
}
/// Result of a successful submit.
#[derive(Debug, Clone, Copy)]
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
            Self::InsufficientLiquidity { side, requested, available } => write!(
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
        OrderBookError::InsufficientLiquidity { side, requested, available } => {
            SubmitError::InsufficientLiquidity { side, requested, available }
        }
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
/// price.
///
/// Helper-owned I/O: none. The caller decides what to do with the
/// outcome (build an ExecutionReport, log) or the error (send a Reject,
/// log).
pub fn submit(
    book: &OrderBook<()>,
    is_market: bool,
    side: Side,
    qty: u64,
    limit_price_cents: u128,
) -> Result<SubmitOutcome, SubmitError> {
    let id = orderbook_rs::Id::new();

    let result = if is_market {
        book.submit_market_order(id, qty, side)
            .map_err(map_book_err)?
    } else {
        book.match_limit_order(id, qty, side, limit_price_cents)
            .map_err(map_book_err)?
    };

    // `executed_quantity` and `executed_value` use checked arithmetic;
    // a `PriceLevelError` here means overflow in the Σ pass. Surface it.
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

    if !is_market && resting_qty > 0 {
        book.add_limit_order(
            orderbook_rs::Id::new(),
            limit_price_cents,
            resting_qty,
            side,
            orderbook_rs::TimeInForce::Gtc,
            None::<()>,
        )
        .map_err(|e| SubmitError::PriceLevel(format!("resting add failed: {e}")))?;
    }

    Ok(SubmitOutcome {
        filled_qty,
        avg_price_cents,
        resting_qty,
        limit_price_cents,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Contestant sample matching engine starting...");

    let books: Books = Arc::new(RwLock::new(HashMap::new()));

    let fix_books = Arc::clone(&books);
    let ws_books = Arc::clone(&books);

    // FIX server runs its own tokio runtime in a std thread (fixer types are !Send)
    let fix_handle = std::thread::spawn(move || {
        if let Err(e) = fix_server::run(fix_books) {
            eprintln!("FIX server error: {}", e);
        }
    });

    let ws_handle = tokio::spawn(async move {
        if let Err(e) = ws_server::run(ws_books).await {
            eprintln!("WS server error: {}", e);
        }
    });

    eprintln!("FIX server listening on 0.0.0.0:9090");
    eprintln!("WebSocket server listening on 0.0.0.0:8080");

    tokio::select! {
        _ = ws_handle => eprintln!("WS server exited"),
    }

    fix_handle.join().ok();
    Ok(())
}
