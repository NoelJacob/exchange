mod fix;
mod metrics;
mod ordergen;
mod scaling;
mod ws;

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use redis::{cmd, AsyncCommands};
use tracing::{error, info};
use ordergen::{ExecutionMessage, OrderGenerator};
use metrics::MetricsCollector;

// ---------------------------------------------------------------------------
//  Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let bot_id = std::env::var("BOT_ID").unwrap_or_else(|_| "bot-1".to_string());
    let contestant_host = std::env::var("CONTESTANT_HOST").unwrap_or_else(|_| "sandbox".to_string());
    let port_fix = std::env::var("PORT_FIX").unwrap_or_else(|_| "9090".to_string());
    let port_ws = std::env::var("PORT_WS").unwrap_or_else(|_| "8080".to_string());
    let target_rps: u64 = std::env::var("TARGET_RPS")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .unwrap_or(100);
    let seed: u64 = std::env::var("RANDOM_SEED")
        .unwrap_or_else(|_| "42".to_string())
        .parse()
        .unwrap_or(42);
    let contestant_id =
        std::env::var("CONTESTANT_ID").unwrap_or_else(|_| "contestant-unknown".to_string());
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_string());

    let fix_addr = format!("{}:{}", contestant_host, port_fix);
    let ws_addr = format!("ws://{}:{}/ws", contestant_host, port_ws);

    let redis = match connect_redis(&redis_url).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to connect to Redis: {e}");
            return;
        }
    };

    info!("Bot {bot_id} connecting to FIX@{fix_addr}, WS@{ws_addr}");
    let redis_fix = redis.clone();
    let redis_ws = redis.clone();
    let fix_addr_owned = fix_addr.clone();
    let ws_addr_owned = ws_addr.clone();
    let bid = bot_id.clone();
    let cid = contestant_id.clone();

    let fix_handle = tokio::spawn(async move {
        if let Err(e) = fix_task(redis_fix, &fix_addr_owned, &bid, &cid, target_rps, seed).await {
            error!("FIX task failed: {e}");
        }
    });

    let ws_handle = tokio::spawn(async move {
        if let Err(e) = ws_task(redis_ws, &ws_addr_owned, &bot_id, &contestant_id, target_rps, seed).await {
            error!("WS task failed: {e}");
        }
    });

    tokio::select! {
        _ = fix_handle => info!("FIX task exited"),
        _ = ws_handle => info!("WS task exited"),
    }
}

// ---------------------------------------------------------------------------
//  Redis helper
// ---------------------------------------------------------------------------

async fn connect_redis(redis_url: &str) -> Result<redis::aio::MultiplexedConnection, anyhow::Error> {
    let client = redis::Client::open(redis_url)?;
    let con = client.get_multiplexed_async_connection().await?;
    info!("Connected to Redis");
    Ok(con)
}

// ---------------------------------------------------------------------------
//  FIX protocol task
// ---------------------------------------------------------------------------

async fn fix_task(
    mut redis: redis::aio::MultiplexedConnection,
    fix_addr: &str,
    bot_id: &str,
    contestant_id: &str,
    target_rps: u64,
    seed: u64,
) -> anyhow::Result<()> {
    let mut generator = OrderGenerator::new(seed, contestant_id, bot_id);
    let mut metrics = MetricsCollector::new(contestant_id, bot_id);
    let interval = Duration::from_secs_f64(1.0 / target_rps as f64);

    loop {
        let mut session = match fix::FixSession::connect(fix_addr, bot_id, contestant_id).await {
            Ok(s) => s,
            Err(e) => {
                error!("FIX connect: {e} — retry in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if let Err(e) = session.logon().await {
            error!("FIX logon: {e} — retry in 5s");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        info!("FIX session logged on");

        let mut last_heartbeat = Instant::now();
        let mut last_order_time = Instant::now();
        let mut last_order_id: Option<String> = None;

        tokio::time::sleep(Duration::from_millis(50)).await;

        loop {
            while let Some(msg) = session.try_read_message() {
                if let Err(e) = handle_fix_inbound(&msg, &mut redis, &metrics, bot_id, contestant_id).await {
                    error!("FIX inbound handler: {e}");
                }
                if msg.get("35") == Some(&"5".to_string()) {
                    info!("FIX peer sent Logout — closing session");
                    let _ = session.logout().await;
                    break;
                }
            }

            if last_order_time.elapsed() >= interval {
                let mut order = generator.next_order();
                let now_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64;
                order.ts_sent_us = now_us;

                let send_start = Instant::now();

                if order.ord_type == 3 {
                    if let Some(orig) = &last_order_id {
                        session.cancel_request(&order.order_id, orig).await?;
                    }
                } else {
                    let side = format!("{}", order.side);
                    let qty = format!("{}", order.qty);
                    let price = format!("{:.2}", order.price);
                    let ot = format!("{}", order.ord_type);
                    session
                        .new_order_single(&order.order_id, &side, &qty, &price, &ot)
                        .await?;
                    last_order_id = Some(order.order_id.clone());
                }

                let latency_us = send_start.elapsed().as_micros() as u64;
                metrics.record_latency(latency_us);
                metrics.publish_order(&mut redis, &order).await?;

                last_order_time = Instant::now();
            }

            if last_heartbeat.elapsed() >= Duration::from_secs(30) {
                session.heartbeat().await?;
                last_heartbeat = Instant::now();
            }

            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }
}

async fn handle_fix_inbound(
    msg: &HashMap<String, String>,
    redis: &mut redis::aio::MultiplexedConnection,
    metrics: &MetricsCollector,
    bot_id: &str,
    contestant_id: &str,
) -> anyhow::Result<()> {
    match msg.get("35").map(|s| s.as_str()) {
        Some("8") => {
            if let Some(exec) = parse_execution_report(msg, bot_id, contestant_id) {
                metrics.publish_execution(redis, &exec).await?;
            }
        }
        Some("0") | Some("A") => {}
        _ => {}
    }
    Ok(())
}

fn parse_execution_report(
    msg: &HashMap<String, String>,
    bot_id: &str,
    contestant_id: &str,
) -> Option<ExecutionMessage> {
    let order_id = msg.get("11")?;
    let fill_price: f64 = msg.get("31")?.parse().ok()?;
    let fill_qty: u32 = msg.get("32")?.parse().ok()?;

    let exec_type = match msg.get("150").map(|s| s.as_str()) {
        Some("F") | Some("2") => "fill",
        Some("1") | Some("D") => "partial_fill",
        Some("4") | Some("C") => "cancelled",
        _ => "unknown",
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    Some(ExecutionMessage {
        order_id: order_id.clone(),
        contestant_id: contestant_id.to_string(),
        fill_price,
        fill_qty,
        exec_type: exec_type.to_string(),
        ts_recv_us: now,
        bot_id: bot_id.to_string(),
    })
}

// ---------------------------------------------------------------------------
//  WebSocket protocol task
// ---------------------------------------------------------------------------

async fn ws_task(
    mut redis: redis::aio::MultiplexedConnection,
    ws_addr: &str,
    bot_id: &str,
    contestant_id: &str,
    target_rps: u64,
    seed: u64,
) -> anyhow::Result<()> {
    let mut generator = OrderGenerator::new(seed, contestant_id, bot_id);
    let metrics = MetricsCollector::new(contestant_id, bot_id);
    let interval = Duration::from_secs_f64(1.0 / target_rps as f64);

    loop {
        let mut ws = match ws::WsSession::connect(ws_addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("WS connect: {e} — retry in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        info!("WS session connected");

        let mut last_order_time = Instant::now();
        tokio::time::sleep(Duration::from_millis(50)).await;

        loop {
            let exec = tokio::time::timeout(Duration::from_millis(1), ws.read_execution()).await;

            match exec {
                Ok(Some(exec)) => {
                    if let Err(e) = metrics.publish_execution(&mut redis, &exec).await {
                        error!("WS publish execution: {e}");
                    }
                }
                Ok(None) => {
                    info!("WS peer closed connection");
                    break;
                }
                Err(_) => {}
            }

            if last_order_time.elapsed() >= interval {
                let mut order = generator.next_order();
                let now_us = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64;
                order.ts_sent_us = now_us;

                if let Err(e) = ws.send_order(&order).await {
                    error!("WS send order: {e}");
                    break;
                }
                metrics.publish_order(&mut redis, &order).await?;

                last_order_time = Instant::now();
            }

            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }
}
