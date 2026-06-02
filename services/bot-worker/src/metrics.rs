use std::time::{SystemTime, UNIX_EPOCH};
use hdrhistogram::Histogram;
use redis::{cmd, AsyncCommands};

use crate::ordergen::{ExecutionMessage, MetricMessage, OrderMessage};

/// Latency percentiles captured from the HDR histogram.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub count: u64,
}

/// Collects per-session latency histograms and publishes order/execution/
/// metric events to Redis Streams.
pub struct MetricsCollector {
    histogram: Histogram<u64>,
    contestant_id: String,
    bot_id: String,
}

impl MetricsCollector {
    pub fn new(contestant_id: &str, bot_id: &str) -> Self {
        Self {
            histogram: Histogram::new(3).expect("HDR histogram creation"),
            contestant_id: contestant_id.to_string(),
            bot_id: bot_id.to_string(),
        }
    }

    pub fn record_latency(&mut self, latency_us: u64) {
        self.histogram.record(latency_us).ok();
    }

    pub fn snapshot(&mut self) -> MetricsSnapshot {
        let snapshot = MetricsSnapshot {
            p50: self.histogram.value_at_quantile(0.50),
            p90: self.histogram.value_at_quantile(0.90),
            p99: self.histogram.value_at_quantile(0.99),
            count: self.histogram.len(),
        };
        self.histogram.reset();
        snapshot
    }

    pub async fn publish_order(
        &self,
        con: &mut redis::aio::MultiplexedConnection,
        order: &OrderMessage,
    ) -> Result<(), anyhow::Error> {
        let json = serde_json::to_string(order)?;
        let _: String = cmd("XADD")
            .arg("stream:orders").arg("*").arg("data").arg(&json)
            .query_async(con).await?;
        Ok(())
    }

    pub async fn publish_execution(
        &self,
        con: &mut redis::aio::MultiplexedConnection,
        exec: &ExecutionMessage,
    ) -> Result<(), anyhow::Error> {
        let json = serde_json::to_string(exec)?;
        let _: String = cmd("XADD")
            .arg("stream:executions").arg("*").arg("data").arg(&json)
            .query_async(con).await?;
        Ok(())
    }

    pub async fn publish_metrics(
        &self,
        con: &mut redis::aio::MultiplexedConnection,
        latency_us: u64,
    ) -> Result<(), anyhow::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let msg = MetricMessage {
            contestant_id: self.contestant_id.clone(),
            latency_us,
            ts: now,
            bot_id: self.bot_id.clone(),
        };
        let json = serde_json::to_string(&msg)?;
        let _: String = cmd("XADD")
            .arg("stream:metrics").arg("*").arg("data").arg(&json)
            .query_async(con).await?;
        Ok(())
    }
}
