use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Shared message types
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OrderMessage {
    pub order_id: String,
    pub contestant_id: String,
    pub side: u8,       // 1=buy, 2=sell
    pub price: f64,
    pub qty: u32,
    pub ord_type: u8,   // 1=market, 2=limit, 3=cancel (local convention)
    pub symbol: String,
    pub ts_sent_us: i64,
    pub bot_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExecutionMessage {
    pub order_id: String,
    pub contestant_id: String,
    pub fill_price: f64,
    pub fill_qty: u32,
    pub exec_type: String, // "fill", "partial_fill", "cancelled"
    pub ts_recv_us: i64,
    pub bot_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetricMessage {
    pub contestant_id: String,
    pub latency_us: u64,
    pub ts: i64,
    pub bot_id: String,
}

// ---------------------------------------------------------------------------
// Deterministic order generator
// ---------------------------------------------------------------------------

pub struct OrderGenerator {
    rng: SmallRng,
    _next_id: u64,
    contestant_id: String,
    bot_id: String,
    last_order_id: Option<String>,
}

impl OrderGenerator {
    pub fn new(seed: u64, contestant_id: &str, bot_id: &str) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            _next_id: 0,
            contestant_id: contestant_id.to_string(),
            bot_id: bot_id.to_string(),
            last_order_id: None,
        }
    }

    pub fn next_order(&mut self) -> OrderMessage {
        let order_id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let roll: f64 = self.rng.gen();

        let (side, price, qty, ord_type, is_cancel) = if roll < 0.45 {
            // 45% limit buy
            let p: f64 = (self.rng.gen_range(99.50_f64..=100.50) * 100.0).round() / 100.0;
            (1, p, self.rng.gen_range(1u32..=100), 2, false)
        } else if roll < 0.90 {
            // 45% limit sell
            let p: f64 = (self.rng.gen_range(99.50_f64..=100.50) * 100.0).round() / 100.0;
            (2, p, self.rng.gen_range(1u32..=100), 2, false)
        } else if roll < 0.95 {
            // 5% market buy
            (1, 0.0, self.rng.gen_range(1u32..=100), 1, false)
        } else {
            // 5% cancel — only if we have a previous order to cancel
            if self.last_order_id.is_some() {
                (1, 0.0, 0, 3, true)
            } else {
                // fallback to limit buy
                let p: f64 = (self.rng.gen_range(99.50_f64..=100.50) * 100.0).round() / 100.0;
                (1, p, self.rng.gen_range(1u32..=100), 2, false)
            }
        };

        if !is_cancel {
            self.last_order_id = Some(order_id.clone());
        }

        OrderMessage {
            order_id,
            contestant_id: self.contestant_id.clone(),
            side,
            price,
            qty,
            ord_type,
            symbol: "AAPL".to_string(),
            ts_sent_us: now,
            bot_id: self.bot_id.clone(),
        }
    }

    /// Return the last non-cancel order id, if any.
    pub fn last_order_id(&self) -> Option<&str> {
        self.last_order_id.as_deref()
    }
}
