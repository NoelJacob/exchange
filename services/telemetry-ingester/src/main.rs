use std::collections::HashMap;
use std::sync::Arc;

use redis::{cmd, AsyncCommands};
use sqlx::PgPool;
use tokio::sync::{broadcast, Mutex};
use tracing::info;

mod aggregator;
mod orderbook;
mod scoring;
mod state;
mod types;
mod validator;
mod writer;
mod ws;

use state::ContestantState;
use types::{ExecutionMessage, MetricMessage, Order};

// ---------------------------------------------------------------------------
// Helper: extract a String from a redis::Value
// ---------------------------------------------------------------------------

fn redis_value_to_string(val: &redis::Value) -> Option<String> {
    match val {
        redis::Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        redis::Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// XREADGROUP response parser
// ---------------------------------------------------------------------------

type StreamEntry = (String, HashMap<String, String>);

fn parse_xread_response(val: redis::Value, expected_stream: &str) -> Vec<StreamEntry> {
    let mut entries = Vec::new();

    // XREADGROUP response structure (RESP2 via redis crate):
    // Array of stream results:
    //   [0] => Bulk([ stream_name, messages ])
    // where messages => Bulk([ msg_id, Bulk([f1, v1, f2, v2, ...]) ])
    //
    match val {
        redis::Value::Nil => return entries,
        redis::Value::Array(streams) => {
            for stream_val in streams {
                let mut parts = match stream_val {
                    redis::Value::Array(p) => p,
                    _ => continue,
                };
                if parts.len() < 2 {
                    continue;
                }

                let stream_name = match redis_value_to_string(&parts[0]) {
                    Some(n) => n,
                    None => continue,
                };
                if stream_name != expected_stream {
                    continue;
                }

                let messages = match &parts[1] {
                    redis::Value::Array(m) => m.clone(),
                    _ => continue,
                };

                for msg_val in messages {
                    let mut msg_parts = match msg_val {
                        redis::Value::Array(p) => p,
                        _ => continue,
                    };
                    if msg_parts.len() < 2 {
                        continue;
                    }

                    let msg_id = match redis_value_to_string(&msg_parts[0]) {
                        Some(id) => id,
                        None => continue,
                    };

                    let mut fields = HashMap::new();
                    if let redis::Value::Array(field_pairs) = &msg_parts[1] {
                        for chunk in field_pairs.chunks(2) {
                            if chunk.len() == 2 {
                                if let (Some(k), Some(v)) = (
                                    redis_value_to_string(&chunk[0]),
                                    redis_value_to_string(&chunk[1]),
                                ) {
                                    fields.insert(k, v);
                                }
                            }
                        }
                    }

                    entries.push((msg_id, fields));
                }
            }
        }
        _ => {}
    }

    entries
}

// ---------------------------------------------------------------------------
// XREADGROUP helper
// ---------------------------------------------------------------------------

async fn xread_stream(
    con: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    consumer: &str,
) -> anyhow::Result<Vec<StreamEntry>> {
    let result: redis::Value = cmd("XREADGROUP")
        .arg("GROUP").arg("ingester").arg(consumer)
        .arg("COUNT").arg(10u64)
        .arg("BLOCK").arg(5000i64)
        .arg("STREAMS").arg(stream).arg(">")
        .query_async(con).await?;

    Ok(parse_xread_response(result, stream))
}

// ---------------------------------------------------------------------------
// Consumer tasks
// ---------------------------------------------------------------------------

async fn orders_consumer(
    mut redis: redis::aio::MultiplexedConnection,
    state: Arc<Mutex<HashMap<String, ContestantState>>>,
) {
    loop {
        let entries = match xread_stream(&mut redis, "stream:orders", "orders_consumer").await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Orders stream read error: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        for (_msg_id, fields) in entries {
            let data = match fields.get("data") {
                Some(d) => d,
                None => {
                    tracing::warn!("Skipping order entry: missing 'data' field");
                    continue;
                }
            };

            let order: Order = match serde_json::from_str(data) {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!("Failed to deserialize Order: {}", e);
                    continue;
                }
            };

            let mut guard = state.lock().await;
            let contestant = guard
                .entry(order.contestant_id.clone())
                .or_insert_with(ContestantState::new);

            let fills = contestant.orderbook.expected_fills(&order);
            contestant.validator.register_expected(fills);
            contestant.orderbook.update_from_order(&order);
            contestant.aggregator.record_order();
        }
    }
}

async fn executions_consumer(
    mut redis: redis::aio::MultiplexedConnection,
    state: Arc<Mutex<HashMap<String, ContestantState>>>,
) {
    loop {
        let entries = match xread_stream(&mut redis, "stream:executions", "executions_consumer").await
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Executions stream read error: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        for (_msg_id, fields) in entries {
            let data = match fields.get("data") {
                Some(d) => d,
                None => {
                    tracing::warn!("Skipping execution entry: missing 'data' field");
                    continue;
                }
            };

            let exec: ExecutionMessage = match serde_json::from_str(data) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("Failed to deserialize ExecutionMessage: {}", e);
                    continue;
                }
            };

            let mut guard = state.lock().await;
            if let Some(contestant) = guard.get_mut(&exec.contestant_id) {
                let pass = contestant.validator.validate_execution(&exec);
                if !pass {
                    tracing::warn!(
                        contestant_id = %exec.contestant_id,
                        order_id = %exec.order_id,
                        "Execution validation failed"
                    );
                }
            } else {
                tracing::warn!(
                    contestant_id = %exec.contestant_id,
                    "Received execution for unknown contestant"
                );
            }
        }
    }
}

async fn metrics_consumer(
    mut redis: redis::aio::MultiplexedConnection,
    state: Arc<Mutex<HashMap<String, ContestantState>>>,
) {
    loop {
        let entries = match xread_stream(&mut redis, "stream:metrics", "metrics_consumer").await {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Metrics stream read error: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        for (_msg_id, fields) in entries {
            let data = match fields.get("data") {
                Some(d) => d,
                None => {
                    tracing::warn!("Skipping metric entry: missing 'data' field");
                    continue;
                }
            };

            let metric: MetricMessage = match serde_json::from_str(data) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("Failed to deserialize MetricMessage: {}", e);
                    continue;
                }
            };

            let mut guard = state.lock().await;
            if let Some(contestant) = guard.get_mut(&metric.contestant_id) {
                contestant.aggregator.record_latency(metric.latency_us);
            } else {
                tracing::warn!(
                    contestant_id = %metric.contestant_id,
                    "Received metric for unknown contestant"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Flush task
// ---------------------------------------------------------------------------

async fn flush_task(
    pool: PgPool,
    state: Arc<Mutex<HashMap<String, ContestantState>>>,
    tx: broadcast::Sender<String>,
    interval_s: u64,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval_s)).await;

        let contestants = {
            let guard = state.lock().await;
            guard.clone()
        };

        match writer::flush_contestants(&pool, &contestants).await {
            Ok(rows) => {
                if !rows.is_empty() {
                    match serde_json::to_string(&rows) {
                        Ok(json) => {
                            let _ = tx.send(json);
                        }
                        Err(e) => {
                            tracing::error!("Failed to serialize leaderboard: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to flush contestants: {}", e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost:5432/telemetry".to_string());
    let ws_port: u16 = std::env::var("WS_PORT")
        .unwrap_or_else(|_| "3001".to_string())
        .parse()?;
    let flush_interval_s: u64 = std::env::var("FLUSH_INTERVAL_S")
        .unwrap_or_else(|_| "5".to_string())
        .parse()?;

    // ---- Redis connection ----
    let redis_client = redis::Client::open(redis_url.as_str())?;
    let redis = redis_client.get_multiplexed_async_connection().await?;
    info!("Connected to Redis at {}", redis_url);

    // ---- PostgreSQL connection ----
    let pool = PgPool::connect(&database_url).await?;
    writer::ensure_schema(&pool).await?;
    info!("Connected to PostgreSQL");

    // Create consumer groups (ignore errors – group may already exist)
    for stream in &["stream:orders", "stream:executions", "stream:metrics"] {
        let mut con = redis.clone();
        let result: Result<redis::Value, _> = cmd("XGROUP")
            .arg("CREATE").arg(stream).arg("ingester").arg("$").arg("MKSTREAM")
            .query_async(&mut con)
            .await;
        if let Err(e) = result {
            tracing::warn!("XGROUP CREATE {} (likely exists): {}", stream, e);
        }
    }
    info!("Consumer groups ready");

    // ---- Shared state ----
    let state: Arc<Mutex<HashMap<String, ContestantState>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // ---- Broadcast channel for WebSocket ----
    let (tx, _) = broadcast::channel::<String>(256);

    // ---- Spawn tasks ----
    let ws_handle = {
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = ws::ws_server(ws_port, tx).await {
                tracing::error!("WS server exited with error: {}", e);
            }
        })
    };

    let orders_handle = {
        let state = state.clone();
        let redis = redis.clone();
        tokio::spawn(async move {
            orders_consumer(redis, state).await;
        })
    };

    let executions_handle = {
        let state = state.clone();
        let redis = redis.clone();
        tokio::spawn(async move {
            executions_consumer(redis, state).await;
        })
    };

    let metrics_handle = {
        let state = state.clone();
        let redis = redis.clone();
        tokio::spawn(async move {
            metrics_consumer(redis, state).await;
        })
    };

    let flush_handle = {
        let state = state.clone();
        let pool = pool.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            flush_task(pool, state, tx, flush_interval_s).await;
        })
    };

    info!("All tasks spawned, entering main loop");

    tokio::select! {
        _ = ws_handle => tracing::warn!("WS server task exited"),
        _ = orders_handle => tracing::warn!("Orders consumer task exited"),
        _ = executions_handle => tracing::warn!("Executions consumer task exited"),
        _ = metrics_handle => tracing::warn!("Metrics consumer task exited"),
        _ = flush_handle => tracing::warn!("Flush task exited"),
    }

    Ok(())
}
