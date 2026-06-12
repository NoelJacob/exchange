use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;

/// Sink for one event stream. Stdout for Part 1, swappable to Redis/Redpanda later.
pub trait MetricSink: Send + Sync {
    fn write(&self, line: &str);
    fn flush(&self);
}

/// Write JSON lines to stdout.
pub struct StdoutSink;

impl MetricSink for StdoutSink {
    fn write(&self, line: &str) {
        println!("{}", line);
    }
    fn flush(&self) {
        use std::io::Write;
        std::io::stdout().flush().ok();
    }
}

/// A sink that broadcasts to multiple inner sinks.
pub struct MultiSink(pub Vec<Arc<dyn MetricSink>>);

impl MetricSink for MultiSink {
    fn write(&self, line: &str) {
        for s in &self.0 {
            s.write(line);
        }
    }
    fn flush(&self) {
        for s in &self.0 {
            s.flush();
        }
    }
}

/// Timestamp in microseconds since epoch.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

pub struct OrderRecord {
    pub contestant_id: String,
    pub bot_id: String,
    pub cl_ord_id: String,
    pub side: String,
    pub symbol: String,
    pub qty: u64,
    pub price: f64,
    pub is_market: bool,
    pub via_fix: bool,
}

pub struct ExecRecord {
    pub contestant_id: String,
    pub bot_id: String,
    pub cl_ord_id: String,
    pub exec_seq: u64,
    pub exec_type: String,
    pub side: String,
    pub qty: u64,
    pub price: f64,
    pub is_market: bool,
    pub latency_us: u64,
    pub last_shares: Option<u64>,
    pub last_px: Option<f64>,
    pub leaves_qty: Option<u64>,   // NEW: exchange-assigned remaining qty (151)
    pub cum_qty: Option<u64>,      // NEW: exchange-assigned cumulative fill qty (14)
}

/// Final aggregated result (printed as last stdout line).
#[derive(Debug, serde::Serialize)]
pub struct BotResult {
    pub orders_sent: u64,
    pub fills: u64,
    pub partials: u64,
    pub rejects: u64,
    pub errors: Vec<String>,
    pub orders_fix: u64,
    pub orders_ws: u64,
    pub avg_latency_us: f64,
    pub p50_latency_us: f64,
    pub p90_latency_us: f64,
    pub p99_latency_us: f64,
}

/// Metrics collector with three concurrent event streams.
pub struct MetricsCollector {
    /// Latency histogram
    histogram: std::sync::Mutex<Histogram<u64>>,
    /// Counters
    orders_sent: AtomicU64,
    fills: AtomicU64,
    partials: AtomicU64,
    rejects: AtomicU64,
    errors: std::sync::Mutex<Vec<String>>,
    orders_fix: AtomicU64,
    orders_ws: AtomicU64,
    /// Event sinks
    order_sink: Arc<dyn MetricSink>,
    exec_sink: Arc<dyn MetricSink>,
    metrics_sink: Arc<dyn MetricSink>,
}

impl MetricsCollector {
    /// Create a new collector with stdout sinks.
    pub fn new_stdout() -> Self {
        Self::new(
            Arc::new(StdoutSink),
            Arc::new(StdoutSink),
            Arc::new(StdoutSink),
        )
    }

    /// Create collector with custom sinks (e.g., stdout + Redpanda).
    pub fn new_with_sinks(sinks: Vec<Arc<dyn MetricSink>>) -> Self {
        Self::new(
            sinks[0].clone(),
            sinks.get(1).cloned().unwrap_or_else(|| sinks[0].clone()),
            sinks.get(2).cloned().unwrap_or_else(|| sinks[0].clone()),
        )
    }

    pub fn new(
        order_sink: Arc<dyn MetricSink>,
        exec_sink: Arc<dyn MetricSink>,
        metrics_sink: Arc<dyn MetricSink>,
    ) -> Self {
        Self {
            histogram: std::sync::Mutex::new(Histogram::<u64>::new(3).expect("HdrHistogram init")),
            orders_sent: AtomicU64::new(0),
            fills: AtomicU64::new(0),
            partials: AtomicU64::new(0),
            rejects: AtomicU64::new(0),
            errors: std::sync::Mutex::new(Vec::new()),
            orders_fix: AtomicU64::new(0),
            orders_ws: AtomicU64::new(0),
            order_sink,
            exec_sink,
            metrics_sink,
        }
    }

    /// Record an order sent to the order stream.
    pub fn record_sent(&self, rec: OrderRecord) {
        self.orders_sent.fetch_add(1, Ordering::Relaxed);
        if rec.via_fix {
            self.orders_fix.fetch_add(1, Ordering::Relaxed);
        } else {
            self.orders_ws.fetch_add(1, Ordering::Relaxed);
        }
        let line = serde_json::json!({
            "stream": "order",
            "ts": now_us(),
            "contestant_id": rec.contestant_id,
            "bot_id": rec.bot_id,
            "cl_ord_id": rec.cl_ord_id,
            "side": rec.side,
            "symbol": rec.symbol,
            "qty": rec.qty,
            "price": rec.price,
            "is_market": rec.is_market,
            "protocol": if rec.via_fix { "fix" } else { "ws" },
        });
        let json_str = line.to_string();
        eprintln!("[BOT-SINK-ORDER] {}", &json_str);
        self.order_sink.write(&json_str);
    }

    /// Record an execution received.
    pub fn record_exec(&self, rec: ExecRecord) {
        // Record latency
        if rec.latency_us > 0 {
            if let Ok(mut h) = self.histogram.lock() {
                if let Err(e) = h.record(rec.latency_us) {
                    eprintln!("[METRIC-ERR] histogram record failed: {e}");
                }
            }
        }

        // Classify exec type
        match rec.exec_type.as_str() {
            "2" => { self.fills.fetch_add(1, Ordering::Relaxed); }
            "1" => { self.partials.fetch_add(1, Ordering::Relaxed); }
            "8" => { self.rejects.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }

        let line = serde_json::json!({
            "stream": "execution",
            "ts": now_us(),
            "contestant_id": rec.contestant_id,
            "bot_id": rec.bot_id,
            "cl_ord_id": rec.cl_ord_id,
            "exec_seq": rec.exec_seq,
            "exec_type": rec.exec_type,
            "side": rec.side,
            "qty": rec.qty,
            "price": rec.price,
            "is_market": rec.is_market,
            "latency_us": rec.latency_us,
            "last_shares": rec.last_shares,
            "last_px": rec.last_px,
            "leaves_qty": rec.leaves_qty,
            "cum_qty": rec.cum_qty,
        });
        let json_str = line.to_string();
        eprintln!("[BOT-SINK-EXEC] {}", &json_str);
        self.exec_sink.write(&json_str);
    }

    /// Record a protocol error.
    pub fn record_error(&self, msg: String) {
        if let Ok(mut errs) = self.errors.lock() {
            errs.push(msg);
        }
    }

    /// Emit a periodic metrics snapshot.
    pub fn snapshot(&self) {
        let (p50, p90, p99, avg) = if let Ok(h) = self.histogram.lock() {
            (
                h.value_at_percentile(50.0),
                h.value_at_percentile(90.0),
                h.value_at_percentile(99.0),
                h.mean(),
            )
        } else {
            (0, 0, 0, 0.0)
        };

        let line = serde_json::json!({
            "stream": "metrics",
            "ts": now_us(),
            "orders_sent": self.orders_sent.load(Ordering::Relaxed),
            "fills": self.fills.load(Ordering::Relaxed),
            "partials": self.partials.load(Ordering::Relaxed),
            "rejects": self.rejects.load(Ordering::Relaxed),
            "errors": {
                "count": self.errors.lock().map(|e| e.len()).unwrap_or(0),
            },
            "orders_fix": self.orders_fix.load(Ordering::Relaxed),
            "orders_ws": self.orders_ws.load(Ordering::Relaxed),
            "p50": p50,
            "p90": p90,
            "p99": p99,
            "avg_latency_us": avg,
        });
        self.metrics_sink.write(&line.to_string());
    }

    /// Flush all sinks.
    pub fn flush_sinks(&self) {
        self.order_sink.flush();
        self.exec_sink.flush();
        self.metrics_sink.flush();
    }

    /// Generate final aggregated result.
    pub fn report(&self) -> BotResult {
        let (p50, p90, p99, avg) = if let Ok(h) = self.histogram.lock() {
            (
                h.value_at_percentile(50.0),
                h.value_at_percentile(90.0),
                h.value_at_percentile(99.0),
                h.mean(),
            )
        } else {
            (0, 0, 0, 0.0)
        };

        let errors = self.errors.lock().map(|e| e.clone()).unwrap_or_default();

        BotResult {
            orders_sent: self.orders_sent.load(Ordering::Relaxed),
            fills: self.fills.load(Ordering::Relaxed),
            partials: self.partials.load(Ordering::Relaxed),
            rejects: self.rejects.load(Ordering::Relaxed),
            errors,
            orders_fix: self.orders_fix.load(Ordering::Relaxed),
            orders_ws: self.orders_ws.load(Ordering::Relaxed),
            avg_latency_us: avg,
            p50_latency_us: p50 as f64,
            p90_latency_us: p90 as f64,
            p99_latency_us: p99 as f64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct VecSink {
        lines: std::sync::Mutex<Vec<String>>,
    }
    impl MetricSink for VecSink {
        fn write(&self, line: &str) {
            self.lines.lock().unwrap().push(line.to_string());
        }
        fn flush(&self) {}
    }

    #[test]
    fn record_order_increments_counters() {
        let sink = Arc::new(VecSink { lines: std::sync::Mutex::new(vec![]) });
        let m = MetricsCollector::new(
            Arc::clone(&sink) as Arc<dyn MetricSink>,
            Arc::clone(&sink) as Arc<dyn MetricSink>,
            sink as Arc<dyn MetricSink>,
        );

        m.record_sent(OrderRecord {
            contestant_id: "test".into(),
            bot_id: "TEST".into(),
            cl_ord_id: "t1".into(),
            side: "1".into(),
            symbol: "AAPL".into(),
            qty: 100,
            price: 100.50,
            is_market: false,
            via_fix: true,
        });

        assert_eq!(m.orders_sent.load(Ordering::Relaxed), 1);
        assert_eq!(m.orders_fix.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn record_exec_tracks_latency() {
        let sink = Arc::new(VecSink { lines: std::sync::Mutex::new(vec![]) });
        let m = MetricsCollector::new(
            Arc::clone(&sink) as Arc<dyn MetricSink>,
            Arc::clone(&sink) as Arc<dyn MetricSink>,
            sink as Arc<dyn MetricSink>,
        );

        m.record_exec(ExecRecord {
            contestant_id: "test".into(),
            bot_id: "TEST".into(),
            cl_ord_id: "t1".into(),
            exec_seq: 1,
            exec_type: "2".into(),
            side: "buy".into(),
            qty: 50,
            price: 100.0,
            is_market: false,
            latency_us: 500,
            last_shares: Some(50),
            last_px: Some(100.0),
            leaves_qty: None,
            cum_qty: None,
        });

        assert_eq!(m.fills.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn report_aggregates() {
        let sink = Arc::new(VecSink { lines: std::sync::Mutex::new(vec![]) });
        let m = MetricsCollector::new(
            Arc::clone(&sink) as Arc<dyn MetricSink>,
            Arc::clone(&sink) as Arc<dyn MetricSink>,
            sink as Arc<dyn MetricSink>,
        );

        m.record_sent(OrderRecord {
            contestant_id: "test".into(), bot_id: "TEST".into(),
            cl_ord_id: "t1".into(), side: "1".into(), symbol: "AAPL".into(),
            qty: 100, price: 100.0, is_market: false, via_fix: true,
        });
        m.record_exec(ExecRecord {
            contestant_id: "test".into(), bot_id: "TEST".into(),
            cl_ord_id: "t1".into(), exec_seq: 1, exec_type: "2".into(),
            side: "buy".into(), qty: 50, price: 100.0, is_market: false,
            latency_us: 500, last_shares: Some(50), last_px: Some(100.0),
            leaves_qty: None, cum_qty: None,
        });
        let r = m.report();
        assert_eq!(r.orders_sent, 1);
        assert_eq!(r.fills, 1);
        assert!(r.avg_latency_us > 0.0);
    }
}
