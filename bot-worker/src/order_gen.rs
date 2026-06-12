use rand::Rng;
use rand::rngs::SmallRng;

/// An order to send to the exchange.
#[derive(Debug, Clone)]
pub enum OrderRequest {
    LimitBuy { price: f64, qty: u64 },
    LimitSell { price: f64, qty: u64 },
    MarketBuy { qty: u64 },
    MarketSell { qty: u64 },
}

impl OrderRequest {
    pub fn side_str(&self) -> &'static str {
        match self {
            OrderRequest::LimitBuy { .. } | OrderRequest::MarketBuy { .. } => "1",
            OrderRequest::LimitSell { .. } | OrderRequest::MarketSell { .. } => "2",
        }
    }

    pub fn side_ws(&self) -> &'static str {
        match self {
            OrderRequest::LimitBuy { .. } | OrderRequest::MarketBuy { .. } => "buy",
            OrderRequest::LimitSell { .. } | OrderRequest::MarketSell { .. } => "sell",
        }
    }

    pub fn is_market(&self) -> bool {
        matches!(self, OrderRequest::MarketBuy { .. } | OrderRequest::MarketSell { .. })
    }

    pub fn price(&self) -> f64 {
        match self {
            OrderRequest::LimitBuy { price, .. } | OrderRequest::LimitSell { price, .. } => *price,
            OrderRequest::MarketBuy { .. } | OrderRequest::MarketSell { .. } => 0.0,
        }
    }

    pub fn qty(&self) -> u64 {
        match self {
            OrderRequest::LimitBuy { qty, .. }
            | OrderRequest::LimitSell { qty, .. }
            | OrderRequest::MarketBuy { qty, .. }
            | OrderRequest::MarketSell { qty, .. } => *qty,
        }
    }
}

/// Generate the next deterministic order from a seeded RNG.
///
/// Distribution (no cancels):
/// - 47.5% LimitBuy  price in [97.00, 100.50], qty in [1, 100]
/// - 47.5% LimitSell price in [99.50, 103.00], qty in [1, 100]
/// -   5% Market     even buy/sell,           qty in [1, 50]
pub fn next_order(rng: &mut SmallRng) -> OrderRequest {
    let roll: f64 = rng.gen(); // [0, 1)

    if roll < 0.475 {
        // LimitBuy
        let price = rng.gen_range(97.00..=100.50);
        let qty = rng.gen_range(1..=100);
        OrderRequest::LimitBuy { price, qty }
    } else if roll < 0.95 {
        // LimitSell
        let price = rng.gen_range(99.50..=103.00);
        let qty = rng.gen_range(1..=100);
        OrderRequest::LimitSell { price, qty }
    } else {
        // Market
        let qty = rng.gen_range(1..=50);
        if rng.gen_bool(0.5) {
            OrderRequest::MarketBuy { qty }
        } else {
            OrderRequest::MarketSell { qty }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn deterministic_same_seed() {
        let mut a = SmallRng::seed_from_u64(42);
        let mut b = SmallRng::seed_from_u64(42);
        let seq_a: Vec<_> = (0..100).map(|_| next_order(&mut a)).collect();
        let seq_b: Vec<_> = (0..100).map(|_| next_order(&mut b)).collect();
        for (i, (oa, ob)) in seq_a.iter().zip(&seq_b).enumerate() {
            assert_eq!(format!("{:?}", oa), format!("{:?}", ob), "mismatch at {i}");
        }
    }

    #[test]
    fn different_seed_different() {
        let mut a = SmallRng::seed_from_u64(42);
        let mut b = SmallRng::seed_from_u64(99);
        let oa = next_order(&mut a);
        let ob = next_order(&mut b);
        // Almost certainly different
        assert_ne!(format!("{:?}", oa), format!("{:?}", ob));
    }

    #[test]
    fn distribution_ratio() {
        let mut rng = SmallRng::seed_from_u64(42);
        let n = 10000;
        let mut limit_buy = 0u64;
        let mut limit_sell = 0u64;
        let mut market = 0u64;

        for _ in 0..n {
            match next_order(&mut rng) {
                OrderRequest::LimitBuy { .. } => limit_buy += 1,
                OrderRequest::LimitSell { .. } => limit_sell += 1,
                OrderRequest::MarketBuy { .. } | OrderRequest::MarketSell { .. } => market += 1,
            }
        }

        let lb_pct = limit_buy as f64 / n as f64;
        let ls_pct = limit_sell as f64 / n as f64;
        let m_pct = market as f64 / n as f64;

        assert!((lb_pct - 0.475).abs() < 0.03, "LimitBuy {lb_pct}");
        assert!((ls_pct - 0.475).abs() < 0.03, "LimitSell {ls_pct}");
        assert!((m_pct - 0.05).abs() < 0.03, "Market {m_pct}");
    }

    #[test]
    fn prices_in_range() {
        let mut rng = SmallRng::seed_from_u64(42);
        for _ in 0..1000 {
            match next_order(&mut rng) {
                OrderRequest::LimitBuy { price, .. } => {
                    assert!((97.00..=100.50).contains(&price), "buy price {price}");
                }
                OrderRequest::LimitSell { price, .. } => {
                    assert!((99.50..=103.00).contains(&price), "sell price {price}");
                }
                _ => {}
            }
        }
    }

    #[test]
    fn qtys_in_range() {
        let mut rng = SmallRng::seed_from_u64(42);
        for _ in 0..1000 {
            match next_order(&mut rng) {
                OrderRequest::LimitBuy { qty, .. } | OrderRequest::LimitSell { qty, .. } => {
                    assert!((1..=100).contains(&qty), "limit qty {qty}");
                }
                OrderRequest::MarketBuy { qty, .. } | OrderRequest::MarketSell { qty, .. } => {
                    assert!((1..=50).contains(&qty), "market qty {qty}");
                }
            }
        }
    }
}
