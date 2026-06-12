use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub contestant_id: String,
    #[serde(default)]
    pub bot_id: String,
    pub cl_ord_id: String,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub qty: u64,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub is_market: bool,
    #[serde(default)]
    pub protocol: String,
    #[serde(default)]
    pub ts_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub contestant_id: String,
    #[serde(default)]
    pub bot_id: String,
    pub cl_ord_id: String,
    #[serde(default)]
    pub exec_id: String,
    #[serde(default)]
    pub exec_seq: u64,
    #[serde(default)]
    pub exec_type: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub qty: u64,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub is_market: bool,
    #[serde(default)]
    pub latency_us: u64,
    #[serde(default)]
    pub last_shares: Option<u64>,
    #[serde(default)]
    pub last_px: Option<f64>,
    #[serde(default)]
    pub leaves_qty: Option<u64>,
    #[serde(default)]
    pub cum_qty: Option<u64>,
    #[serde(default)]
    pub ts_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricEvent {
    #[serde(default)]
    pub contestant_id: String,
    #[serde(default)]
    pub bot_id: String,
    #[serde(default)]
    pub ts_us: u64,
    #[serde(default)]
    pub orders_sent: u64,
    #[serde(default)]
    pub fills: u64,
    #[serde(default)]
    pub partials: u64,
    #[serde(default)]
    pub rejects: u64,
    #[serde(default)]
    pub errors: u64,
    #[serde(default)]
    pub p50: u64,
    #[serde(default)]
    pub p90: u64,
    #[serde(default)]
    pub p99: u64,
    #[serde(default)]
    pub avg_latency_us: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_order_event_from_bot_output() {
        let json = r#"{"stream":"order","ts":1749697545123456,"contestant_id":"manual","bot_id":"MAN","cl_ord_id":"MAN-1","side":"buy","symbol":"AAPL","qty":30,"price":98.06,"is_market":false,"protocol":"fix"}"#;
        let e: OrderEvent = serde_json::from_str(json).expect("parse order");
        assert_eq!(e.contestant_id, "manual");
        assert_eq!(e.qty, 30);
    }

    #[test]
    fn parse_execution_event_from_bot_output() {
        let json = r#"{"stream":"execution","ts":1749697545123456,"contestant_id":"manual","bot_id":"MAN","cl_ord_id":"MAN-1","exec_type":"2","latency_us":500,"last_shares":50,"last_px":100.5}"#;
        let e: ExecutionEvent = serde_json::from_str(json).expect("parse exec");
        assert_eq!(e.contestant_id, "manual");
        assert_eq!(e.exec_type, "2");
        assert_eq!(e.last_shares, Some(50));
    }

    #[test]
    fn parse_execution_with_exec_seq() {
        let json = r#"{"stream":"execution","ts":100,"contestant_id":"t","bot_id":"t","cl_ord_id":"t1","exec_seq":42,"exec_type":"2","side":"buy","qty":100,"price":100.5,"is_market":false,"latency_us":500,"last_shares":50,"last_px":100.5}"#;
        let e: ExecutionEvent = serde_json::from_str(json).expect("parse exec with seq");
        assert_eq!(e.exec_seq, 42);
        assert_eq!(e.side, "buy");
        assert_eq!(e.qty, 100);
        assert!(!e.is_market);
    }
}
