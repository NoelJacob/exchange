use crate::aggregator::ContestantAggregator;
use crate::orderbook::OrderBook;
use crate::validator::FillValidator;

/// Per-contestant state: reference order book, fill validator, and latency aggregator.
#[derive(Debug, Clone)]
pub struct ContestantState {
    pub orderbook: OrderBook,
    pub validator: FillValidator,
    pub aggregator: ContestantAggregator,
}

impl ContestantState {
    pub fn new() -> Self {
        Self {
            orderbook: OrderBook::new(),
            validator: FillValidator::new(),
            aggregator: ContestantAggregator::new(),
        }
    }
}
