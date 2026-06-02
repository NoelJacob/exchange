use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub contestant_id: String,
    pub side: u8,
    pub price: f64,
    pub qty: u32,
    pub ord_type: u8,
    pub symbol: String,
    pub ts_sent_us: i64,
    pub bot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedFill {
    pub resting_order_id: String,
    pub fill_price: f64,
    pub fill_qty: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMessage {
    pub order_id: String,
    pub contestant_id: String,
    pub fill_price: f64,
    pub fill_qty: u32,
    pub exec_type: String,
    pub ts_recv_us: i64,
    pub bot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricMessage {
    pub contestant_id: String,
    pub latency_us: u64,
    pub bot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedSnapshot {
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub tps: f64,
    pub total_orders: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardRow {
    pub contestant_id: String,
    pub composite_score: f64,
    pub correctness: f64,
    pub tps: f64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub total_orders: u64,
}
