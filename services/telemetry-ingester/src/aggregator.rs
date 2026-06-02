use std::time::Instant;

use hdrhistogram::Histogram;

use crate::types::AggregatedSnapshot;

#[derive(Debug, Clone)]
pub struct ContestantAggregator {
    pub histogram: Histogram<u64>,
    pub order_count: u64,
    pub window_start: Instant,
}

impl ContestantAggregator {
    pub fn new() -> Self {
        Self {
            histogram: Histogram::new(3).expect("valid histogram config"),
            order_count: 0,
            window_start: Instant::now(),
        }
    }

    /// Records a latency measurement into the HDR histogram.
    pub fn record_latency(&mut self, latency_us: u64) {
        self.histogram.record(latency_us).ok();
    }

    /// Increments the order counter for TPS calculation.
    pub fn record_order(&mut self) {
        self.order_count += 1;
    }

    /// Produces a snapshot of current statistics.
    pub fn snapshot(&self) -> AggregatedSnapshot {
        let elapsed = self.window_start.elapsed().as_secs_f64();
        let tps = if elapsed > 0.0 {
            self.order_count as f64 / elapsed
        } else {
            0.0
        };

        AggregatedSnapshot {
            p50_us: self.histogram.value_at_percentile(50.0),
            p90_us: self.histogram.value_at_percentile(90.0),
            p99_us: self.histogram.value_at_percentile(99.0),
            tps,
            total_orders: self.order_count,
        }
    }
}
