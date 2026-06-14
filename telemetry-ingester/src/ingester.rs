use std::sync::Arc;
use std::time::Duration;
use futures::StreamExt;
use rskafka::client::{
    ClientBuilder, consumer::{StartOffset, StreamConsumerBuilder}, partition::UnknownTopicHandling,
};
use crate::config::Config;
use crate::models::{ExecutionEvent, MetricEvent, OrderEvent};
use crate::storage::Storage;

pub struct Ingester {
    storage: Arc<Storage>,
    config: Config,
}

impl Ingester {
    pub fn new(storage: Arc<Storage>, config: Config) -> Self {
        Self { storage, config }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = Arc::new(
            tokio::time::timeout(
                Duration::from_secs(5),
                ClientBuilder::new(vec![self.config.redpanda_brokers.clone()]).build(),
            ).await.map_err(|_| "Ingester Redpanda connect timeout (5s)")??,
        );
        let mut handles = Vec::new();
        for &topic in &["orders", "executions", "metrics"] {
            let client = Arc::clone(&client);
            let storage = Arc::clone(&self.storage);
            let ts = topic.to_string();
            handles.push(tokio::spawn(async move {
                eprintln!("[INGEST-CONSUMER] Starting for topic={ts}");
                let mut batch_count: u64 = 0;
                loop {
                    // Create a fresh PartitionClient each retry so StartOffset::Latest
                    // always starts from the current end — never re-consumes old offsets.
                    let pc = match client.partition_client(
                        ts.clone(), 0, UnknownTopicHandling::Retry,
                    ).await {
                        Ok(pc) => Arc::new(pc),
                        Err(e) => {
                            eprintln!("[INGEST-ERR] {ts}: partition_client failed: {e:?}, retrying in 1s");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };
                    let mut stream = StreamConsumerBuilder::new(pc, StartOffset::Latest)
                        .with_max_wait_ms(1000).build();
                    eprintln!("[INGEST-CONSUMER] {ts}: entering poll loop");
                    loop {
                        eprintln!("[INGEST-STREAM] {ts}: calling stream.next()... (batch_count={batch_count})");
                        let result = stream.next().await;
                        match result {
                            Some(Ok((ro, _highwater))) => {
                                batch_count += 1;
                                let offset = ro.offset;
                                eprintln!("[INGEST-STREAM] {ts}: got record offset={offset} len={} batch={batch_count}",
                                    ro.record.value.as_ref().map(|v| v.len()).unwrap_or(0));
                                if let Some(value) = &ro.record.value {
                                    if let Ok(text) = std::str::from_utf8(value) {
                                        eprintln!("[INGEST-CONSUMER] {ts} offset={offset} len={} batch={batch_count}",
                                            value.len());
                                        if let Err(e) = store_event(&storage, &ts, text).await {
                                            eprintln!("[INGEST-ERR] store_event failed: {e}");
                                        }
                                    } else {
                                        eprintln!("[INGEST-CONSUMER] {ts} offset={offset}: invalid UTF-8");
                                    }
                                } else {
                                    eprintln!("[INGEST-CONSUMER] {ts} offset={offset}: null record value");
                                }
                            }
                            Some(Err(e)) => {
                                batch_count += 1;
                                eprintln!("[INGEST-STREAM] {ts}: stream error: {e:?}");
                                eprintln!("[INGEST-CONSUMER] {ts} stream error: {e:?}");
                            }
                            None => {
                                eprintln!("[INGEST-STREAM] {ts}: stream.next() returned None — stream ended after {batch_count} batches");
                                eprintln!("[INGEST-CONSUMER] {ts} stream ENDED after {batch_count} batches — rebuilding consumer");
                                break;
                            }
                        }
                        if batch_count > 0 && batch_count % 100 == 0 {
                            eprintln!("[INGEST-HEARTBEAT] {ts}: consumed {batch_count} batches, still running");
                        }
                    }
                    eprintln!("[INGEST-STREAM] {ts}: exited inner poll loop, sleeping 500ms before rebuild");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    eprintln!("[INGEST-STREAM] {ts}: wake from rebuild sleep, creating new stream");
                }
            }));
        }
        for h in handles {
            match h.await {
                Ok(()) => {
                    eprintln!("[INGEST-ERR] consumer task completed OK (should not happen in loop)");
                }
                Err(e) => {
                    eprintln!("[INGEST-ERR] consumer task panicked: {e:?}");
                    if let Ok(panic) = e.try_into_panic() {
                        let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = panic.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic payload".to_string()
                        };
                        eprintln!("[INGEST-ERR] panic message: {msg}");
                    }
                }
            }
        }
        eprintln!("[INGEST] Both consumer tasks exited — run() returning");
        Ok(())
    }
}

async fn store_event(storage: &Storage, topic: &str, text: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    eprintln!("[INGEST] {} raw: {}", topic, text.chars().take(150).collect::<String>());
    match topic {
        "executions" => {
            match serde_json::from_str::<ExecutionEvent>(text) {
                Ok(e) => {
                    eprintln!("[INGEST] EXEC deser OK: {} seq={} type={} side={}", e.cl_ord_id, e.exec_seq, e.exec_type, e.side);
                    if let Err(err) = insert_exec(storage, &e).await {
                        eprintln!("[INGEST] EXEC INSERT ERROR: {} for {}", err, e.cl_ord_id);
                    }
                }
                Err(err) => { eprintln!("[INGEST] EXEC PARSE ERROR: {}", err); }
            }
        }
        "orders" => {
            if let Ok(e) = serde_json::from_str::<OrderEvent>(text) {
                let ts = chrono::DateTime::from_timestamp(
                    (e.ts_us / 1_000_000) as i64,
                    ((e.ts_us % 1_000_000) as u32) * 1_000,
                ).map(|dt| dt.naive_utc())
                .unwrap_or_else(|| chrono::Utc::now().naive_utc());
                if let Err(err) = sqlx::query("INSERT INTO order_events (ts,contestant_id,cl_ord_id,side,qty,price,is_market,protocol) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
                    .bind(ts).bind(&e.contestant_id).bind(&e.cl_ord_id)
                    .bind(&e.side).bind(e.qty as i64).bind(e.price).bind(e.is_market)
                    .bind(&e.protocol)
                    .execute(&storage.pg).await
                {
                    eprintln!("[INGEST-ERR] order insert failed: {err}");
                }
            }
        }
        "metrics" => {
            match serde_json::from_str::<MetricEvent>(text) {
                Ok(ev) => {
                    storage.insert_metric(&ev).await?;
                }
                Err(e) => {
                    eprintln!("[INGEST-ERR] metrics parse failed: {e} — text={}", &text[..text.len().min(200)]);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

async fn insert_exec(storage: &Storage, e: &ExecutionEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ts = chrono::DateTime::from_timestamp(
        (e.ts_us / 1_000_000) as i64,
        ((e.ts_us % 1_000_000) as u32) * 1_000, // nanoseconds
    ).map(|dt| dt.naive_utc())
    .unwrap_or_else(|| chrono::Utc::now().naive_utc());
    eprintln!("[INGEST-INSERT] EXEC {}: seq={} type={} side={} qty={} price={} market={} last_shares={:?} last_px={:?} leaves={:?} cum={:?}",
        e.cl_ord_id, e.exec_seq, e.exec_type, e.side, e.qty, e.price, e.is_market,
        e.last_shares, e.last_px, e.leaves_qty, e.cum_qty);
    sqlx::query("INSERT INTO exec_events (ts,contestant_id,cl_ord_id,exec_id,exec_seq,exec_type,side,qty,price,is_market,last_shares,last_px,leaves_qty,cum_qty,latency_us) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)")
        .bind(ts).bind(&e.contestant_id).bind(&e.cl_ord_id)
        .bind(&e.exec_id).bind(e.exec_seq as i64).bind(&e.exec_type)
        .bind(&e.side).bind(e.qty as i64).bind(e.price).bind(e.is_market)
        .bind(e.last_shares.map(|x| x as i64))
        .bind(e.last_px).bind(e.leaves_qty.map(|x| x as i64))
        .bind(e.cum_qty.map(|x| x as i64))
        .bind(e.latency_us as i64)
        .execute(&storage.pg).await?;
    eprintln!("[INGEST-INSERT] EXEC {}: SUCCESS", e.cl_ord_id);
    Ok(())
}
