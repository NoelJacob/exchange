use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rskafka::client::{
    ClientBuilder,
    partition::UnknownTopicHandling,
    producer::{BatchProducerBuilder, aggregator::RecordAggregator},
};
use tokio::sync::{mpsc, Notify};

use crate::metrics::MetricSink;

pub struct RedpandaSink {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    sent_count: Arc<AtomicU64>,
    acked_count: Arc<AtomicU64>,
    drain_notify: Arc<Notify>,
}

impl RedpandaSink {
    pub async fn connect(brokers: &str, topic: &str) -> Result<Self, Box<dyn std::error::Error>> {
        use std::time::Duration;
        let client = tokio::time::timeout(
            Duration::from_secs(3),
            ClientBuilder::new(vec![brokers.to_owned()]).build(),
        ).await.map_err(|_| "Redpanda connect timeout (3s)")??;
        let partition_client = Arc::new(
            client.partition_client(topic, 0, UnknownTopicHandling::Retry).await?,
        );
        let producer = Arc::new(
            BatchProducerBuilder::new(partition_client)
                .build(RecordAggregator::new(1024 * 1024)),
        );
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let topic_owned = topic.to_string();
        let sent_count = Arc::new(AtomicU64::new(0));
        let acked_count = Arc::new(AtomicU64::new(0));
        let drain_notify = Arc::new(Notify::new());
        let notify_handle = Arc::clone(&drain_notify);

        // Background producer task — processes records sequentially,
        // signaling ack for each.
        let acked = Arc::clone(&acked_count);
        tokio::spawn(async move {
            while let Some(value) = rx.recv().await {
                let raw = String::from_utf8_lossy(&value);
                eprintln!("[REDPANDA-PUB] topic={topic_owned} len={} data={raw}", value.len());
                let record = rskafka::record::Record {
                    key: None,
                    value: Some(value),
                    headers: Default::default(),
                    timestamp: chrono::Utc::now(),
                };
                if let Err(e) = producer.produce(record).await {
                    eprintln!("[REDPANDA-ERR] topic={topic_owned} produce failed: {e}");
                }
                acked.fetch_add(1, Ordering::Release);
                notify_handle.notify_one();
            }
        });

        Ok(Self { tx, sent_count, acked_count, drain_notify })
    }
}

impl MetricSink for RedpandaSink {
    /// Non-blocking send to the background producer task.
    fn write(&self, line: &str) {
        let value = line.as_bytes().to_vec();
        if let Err(e) = self.tx.send(value) {
            eprintln!("[SINK-ERR] RedpandaSink: channel send failed: {e}");
            return;
        }
        self.sent_count.fetch_add(1, Ordering::Release);
    }

    /// Block until all sent records have been produced to Redpanda.
    fn flush(&self) {
        let sent = self.sent_count.load(Ordering::Acquire);
        let start = std::time::Instant::now();
        loop {
            let acked = self.acked_count.load(Ordering::Acquire);
            if acked >= sent {
                break;
            }
            if start.elapsed().as_millis() > 10000 {
                eprintln!("[SINK-ERR] RedpandaSink: flush timeout (sent={sent} acked={acked})");
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
