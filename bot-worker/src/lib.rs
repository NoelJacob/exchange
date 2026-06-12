pub mod config;
pub mod fix_session;
pub mod metrics;
pub mod order_gen;
pub mod redpanda_sink;
pub mod session_pool;
pub mod ws_client;

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use config::Config;
use metrics::{ExecRecord, MetricsCollector, OrderRecord};
use order_gen::next_order;
use session_pool::SessionPool;
use std::sync::atomic::{AtomicU64, Ordering};
use fixer_fix::tag;

pub async fn run(config: Config) -> metrics::BotResult {
    // Connect
    eprintln!(
        "[Bot] Connecting pool ({} FIX, {} WS)...",
        config.fix_connections, config.ws_connections
    );
    let mut pool = match SessionPool::connect(&config).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[Bot] Fatal: connection failed: {e}");
            return metrics::BotResult {
                orders_sent: 0,
                fills: 0,
                partials: 0,
                rejects: 0,
                errors: vec![format!("connect failed: {e}")],
                orders_fix: 0,
                orders_ws: 0,
                avg_latency_us: 0.0,
                p50_latency_us: 0.0,
                p90_latency_us: 0.0,
                p99_latency_us: 0.0,
            };
        }
    };
    // Create metrics sinks: each stream writes to stdout + Redpanda
    let stdout = Arc::new(metrics::StdoutSink) as Arc<dyn metrics::MetricSink>;
    let mut order_sinks: Vec<Arc<dyn metrics::MetricSink>> = vec![stdout.clone()];
    let mut exec_sinks: Vec<Arc<dyn metrics::MetricSink>> = vec![stdout.clone()];
    let mut metric_sinks: Vec<Arc<dyn metrics::MetricSink>> = vec![stdout];

    if !config.redpanda_brokers.is_empty() {
        let topics = ["orders", "executions", "metrics"];
        for topic in &topics {
            match redpanda_sink::RedpandaSink::connect(&config.redpanda_brokers, topic).await {
                Ok(s) => {
                    let s = Arc::new(s) as Arc<dyn metrics::MetricSink>;
                    match *topic {
                        "orders" => order_sinks.push(s),
                        "executions" => exec_sinks.push(s),
                        "metrics" => metric_sinks.push(s),
                        _ => {}
                    }
                }
                Err(e) => eprintln!("[Bot] Redpanda {topic} sink failed: {e}"),
            }
        }
    }
    let metrics = Arc::new(metrics::MetricsCollector::new(
        Arc::new(metrics::MultiSink(order_sinks)),
        Arc::new(metrics::MultiSink(exec_sinks)),
        Arc::new(metrics::MultiSink(metric_sinks)),
    ));
    let contestant_id = config.contestant_id.clone();
    let bot_id = config.sender_comp_id_prefix.clone();
    let symbol = "AAPL";

    let start = Instant::now();
    let mut last_snapshot_elapsed = Duration::ZERO;
    let mut orders_committed: u64 = 0;
    let mut rps_cycle: u64 = 0;
    let mut rng = SmallRng::seed_from_u64(config.seed);
    loop {
        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        if elapsed_secs >= config.duration_secs as f64 {
            break;
        }

        let ramp_t = (elapsed_secs / config.ramp_up_secs as f64).min(1.0);
        let current_rps =
            config.min_rps as f64 + (config.rps - config.min_rps) as f64 * ramp_t;

        // Pace
        let target_orders = (elapsed_secs * current_rps).ceil() as u64;
        if orders_committed >= target_orders {
            let ahead = orders_committed - target_orders;
            let sleep_ms = 1.max((ahead as f64 / current_rps * 1000.0 * 0.9) as u64).min(50);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            continue;
        }

        // Generate order
        let order = next_order(&mut rng);
        rps_cycle += 1;
        let cl_ord_id = format!("{}-{}", config.sender_comp_id_prefix, rps_cycle);

        // Choose protocol
        let via_fix = rng.gen_bool(0.5);
        eprintln!("[BOT-GEN] cl_ord_id={} side={} qty={} price={} is_market={} via_fix={}",
            cl_ord_id, order.side_str(), order.qty(), order.price(), order.is_market(),
            if via_fix { "fix" } else { "ws" });

        // Send via chosen protocol
        if via_fix {
            match pool
                .send_fix(
                    &cl_ord_id,
                    order.side_str(),
                    symbol,
                    order.qty(),
                    order.price(),
                    order.is_market(),
                )
                .await
            {
                Ok((msg, latency_us)) => {
                    validate_fix_report(
                        &msg, &cl_ord_id, &metrics, latency_us,
                        &contestant_id, &bot_id,
                    );
                    // Drain stray FIX notifications (maker fills) that arrived
                    // out of band while we were waiting for the sync response.
                    let strays = pool.drain_fix_notifications();
                    if !strays.is_empty() {
                        eprintln!("[BOT-FIX-STRAYS] {} stray notifications", strays.len());
                        for stray in &strays {
                            let nid = stray.body.get_string(tag::CL_ORD_ID).unwrap_or_default();
                            validate_fix_report(stray, &nid, &metrics, 0, &contestant_id, &bot_id);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[Bot] FIX error: {e}");
                    metrics.record_error(format!("FIX: {e}"));
                }
            }
        } else {
            match pool
                .send_ws(
                    &pool.sender_id(),
                    &cl_ord_id,
                    order.side_ws(),
                    symbol,
                    order.qty(),
                    if order.is_market() { None } else { Some(order.price()) },
                )
                .await
            {
                Ok((resp, latency_us)) => {
                    validate_ws_response(
                        &resp, &cl_ord_id, &metrics, latency_us,
                        &contestant_id, &bot_id,
                    );
                    let notifs = pool.poll_notifications().await;
                    eprintln!("[BOT-WS-NOTIF] {} notifications after {}",
                        notifs.len(), cl_ord_id);
                    for notif in &notifs {
                        let nid = notif.params.as_ref()
                            .and_then(|p| p.get("cl_ord_id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("<unknown>");
                        if let Some(params) = &notif.params {
                            let seq = params.get("exec_id").and_then(|v| v.as_str()).unwrap_or("?");
                            let et = notif.method.as_deref().unwrap_or("?");
                            eprintln!("[BOT-WS-NOTIF-DETAIL] cl_ord_id={nid} method={et} exec_seq={seq}");
                        }
                        validate_ws_response(notif, nid, &metrics, 0, &contestant_id, &bot_id);
                    }
                }
                Err(e) => {
                    eprintln!("[Bot] WS error: {e}");
                    metrics.record_error(format!("WS: {e}"));
                }
            }
        };

        // Record order sent
        metrics.record_sent(OrderRecord {
            contestant_id: contestant_id.clone(),
            bot_id: bot_id.clone(),
            cl_ord_id,
            side: if via_fix { order.side_str().to_string() } else { order.side_ws().to_string() },
            symbol: symbol.to_string(),
            qty: order.qty(),
            price: order.price(),
            is_market: order.is_market(),
            via_fix,
        });

        orders_committed += 1;

        // Periodic snapshot
        if elapsed.saturating_sub(last_snapshot_elapsed) >= Duration::from_secs(config.report_interval_secs) {
            metrics.snapshot();
            last_snapshot_elapsed = elapsed;
        }
    }

    // Shutdown
    // Flush sinks (ensure all records are produced to Redpanda before shutdown)
    // Flush sinks — ensures all records produced before drain are committed
    eprintln!("[Bot] Flushing sinks...");
    metrics.flush_sinks();

    // Drain any remaining FIX notifications (maker fills on other sessions)
    let remaining_fix = pool.drain_all_fix().await;
    if !remaining_fix.is_empty() {
        eprintln!("[Bot] {} unread FIX notifications at shutdown", remaining_fix.len());
        for msg in &remaining_fix {
            let nid = msg.body.get_string(fixer_fix::tag::CL_ORD_ID).unwrap_or_default();
            validate_fix_report(msg, &nid, &metrics, 0, &contestant_id, &bot_id);
        }
    }

    // Flush again — the FIX drain may have generated new exec events that need to
    // complete their Redpanda produce before shutdown kills the background task.
    eprintln!("[Bot] Flushing sinks after drain...");
    metrics.flush_sinks();

    eprintln!("[Bot] Shutting down...");
    pool.shutdown().await;

    // Report
    let result = metrics.report();
    let result_json = serde_json::to_string(&result).unwrap_or_default();
    println!("{}", result_json);
    result
}

fn validate_fix_report(
    msg: &fixer::message::Message,
    cl_ord_id: &str,
    metrics: &MetricsCollector,
    latency_us: u64,
    contestant_id: &str,
    bot_id: &str,
) {
    if !msg.is_msg_type_of(fixer_fix::enums::msg_type::EXECUTION_REPORT) {
        metrics.record_error(format!("[FIX] {cl_ord_id}: not 35=8"));
        return;
    }

    let order_id = match msg.body.get_string(tag::ORDER_ID) {
        Ok(id) => id,
        Err(_) => { metrics.record_error(format!("[FIX] {cl_ord_id}: missing OrderID")); return; }
    };
    if order_id.trim().is_empty() {
        metrics.record_error(format!("[FIX] {cl_ord_id}: empty OrderID")); return;
    }

    let exec_type = match msg.body.get_string(tag::EXEC_TYPE) {
        Ok(et) => et,
        Err(_) => { metrics.record_error(format!("[FIX] {cl_ord_id}: missing ExecType")); return; }
    };

    let (last_shares, last_px): (Option<isize>, Option<f64>) = match exec_type.as_str() {
        "0" => (None, None),
        "1" | "2" => {
            let ls = msg.body.get_int(tag::LAST_SHARES).ok();
            let lp = msg.body.get_string(tag::LAST_PX).ok();
            if ls.is_none() || ls.map(|x| x <= 0).unwrap_or(true) {
                metrics.record_error(format!("[FIX] {cl_ord_id}: missing/bad LastShares"));
            }
            let last_shares = ls;
            let last_px = lp.and_then(|s| s.parse::<f64>().ok());
            (last_shares, last_px)
        }
        "8" => (None, None),
        other => { metrics.record_error(format!("[FIX] {cl_ord_id}: unknown ExecType {other:?}")); return; }
    };

    // DEBUG: print raw exec values
    if let Ok(ev) = msg.body.get_string(tag::EXEC_ID) {
        eprintln!("[FIX] {cl_ord_id}: ExecID(body)={ev:?}");
    } else if let Ok(ev) = msg.header.get_string(tag::EXEC_ID) {
        eprintln!("[FIX] {cl_ord_id}: ExecID(header)={ev:?}");
    } else {
        eprintln!("[FIX] {cl_ord_id}: ExecID NOT FOUND");
    }
    let exec_seq = msg.body.get_string(tag::EXEC_ID)
        .ok()
        .or_else(|| msg.header.get_string(tag::EXEC_ID).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            static FALLBACK_EXEC_SEQ: AtomicU64 = AtomicU64::new(1);
            let fallback = FALLBACK_EXEC_SEQ.fetch_add(1, Ordering::Relaxed);
            let hr = msg.header.get_int(tag::EXEC_ID).ok().map(|x| x as u64);
            let br = msg.body.get_int(tag::EXEC_ID).ok().map(|x| x as u64);
            eprintln!("[FIX] {cl_ord_id}: ExecID not found as string, assigning fallback_seq={fallback} (header_int={hr:?} body_int={br:?})");
            fallback
        });
    let side_val = match msg.body.get_string(tag::SIDE) {
        Ok(s) => s,
        Err(_) => { metrics.record_error(format!("[FIX] {cl_ord_id}: missing Side(54)")); String::new() }
    };
    let qty_val = match msg.body.get_int(tag::ORDER_QTY) {
        Ok(q) => q as u64,
        Err(_) => { metrics.record_error(format!("[FIX] {cl_ord_id}: missing OrderQty(38)")); 0 }
    };
    let price_val = match msg.body.get_string(tag::PRICE) {
        Ok(p) => p.parse::<f64>().unwrap_or_else(|_| { metrics.record_error(format!("[FIX] {cl_ord_id}: bad Price(44): {p}")); 0.0 }),
        Err(_) => { metrics.record_error(format!("[FIX] {cl_ord_id}: missing Price(44)")); 0.0 }
    };
    let is_market = msg.body.get_string(tag::ORD_TYPE).map(|t| t == "1").unwrap_or(false);
    let leaves_qty = msg.body.get_int(tag::LEAVES_QTY).ok().filter(|&x| x >= 0).map(|x| x as u64);
    let cum_qty = msg.body.get_int(tag::CUM_QTY).ok().filter(|&x| x >= 0).map(|x| x as u64);

    eprintln!("[BOT-FIX] ExecRecord {}: seq={} type={} side={} qty={} price={} market={} last_shares={:?} last_px={:?} leaves={:?} cum={:?}",
        cl_ord_id, exec_seq, &exec_type, &side_val, qty_val, price_val, is_market, last_shares, last_px, leaves_qty, cum_qty);
    metrics.record_exec(ExecRecord {
        contestant_id: contestant_id.to_string(),
        bot_id: bot_id.to_string(),
        cl_ord_id: cl_ord_id.to_string(),
        exec_seq,
        exec_type,
        side: if side_val == "1" { "buy".into() } else if side_val == "2" { "sell".into() } else { side_val.clone() },
        qty: qty_val,
        price: price_val,
        is_market,
        latency_us,
        last_shares: last_shares.map(|x| x as u64),
        last_px,
        leaves_qty,
        cum_qty,
    });
}

fn validate_ws_response(
    resp: &ws_client::WsResponse,
    cl_ord_id: &str,
    metrics: &MetricsCollector,
    latency_us: u64,
    contestant_id: &str,
    bot_id: &str,
) {
    if resp.is_error {
        let msg = resp.error_message.as_deref().unwrap_or("unknown");
        metrics.record_error(format!("[WS] {cl_ord_id}: error {}: {}", resp.error_code.unwrap_or(0), msg));
        metrics.record_exec(ExecRecord {
            contestant_id: contestant_id.to_string(),
            bot_id: bot_id.to_string(),
            cl_ord_id: cl_ord_id.to_string(),
            exec_seq: 0,
            exec_type: "8".to_string(),
            side: String::new(),
            qty: 0,
            price: 0.0,
            is_market: false,
            latency_us,
            last_shares: None,
            last_px: None,
            leaves_qty: None,
            cum_qty: None,
        });
    }

    let method = match &resp.method {
        Some(m) => m.as_str(),
        None => { metrics.record_error(format!("[WS] {cl_ord_id}: no method")); return; }
    };

    let params = match &resp.params {
        Some(p) => p,
        None => { metrics.record_error(format!("[WS] {cl_ord_id}: no params")); return; }
    };

    if params.get("cl_ord_id").and_then(|v| v.as_str()).is_none() {
        metrics.record_error(format!("[WS] {cl_ord_id}: missing cl_ord_id in params")); return;
    }

    let (exec_type, last_shares, last_px) = match method {
        "order.report.new" => ("0", None, None),
        "order.report.partial" => {
            ("1", params.get("last_shares").and_then(|v| v.as_i64()),
             params.get("last_px").and_then(|v| v.as_f64()))
        }
        "order.report.fill" => {
            ("2", params.get("last_shares").and_then(|v| v.as_i64()),
             params.get("last_px").and_then(|v| v.as_f64()))
        }
        "order.report.rejected" => ("8", None, None),
        other => { metrics.record_error(format!("[WS] {cl_ord_id}: unknown method {other}")); return; }
    };

    let exec_seq = params.get("exec_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let ws_side = params.get("side").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let ws_qty = params.get("qty").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
    let ws_price = params.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let ws_market = ws_price == 0.0;
    let ws_leaves = params.get("leaves_qty").and_then(|v| v.as_i64()).map(|x| x as u64);
    let ws_cum = params.get("cum_qty").and_then(|v| v.as_i64()).map(|x| x as u64);

    eprintln!("[BOT-WS] ExecRecord {}: seq={} type={} side={} qty={} price={} market={} last_shares={:?} last_px={:?} leaves={:?} cum={:?}",
        cl_ord_id, exec_seq, exec_type, &ws_side, ws_qty, ws_price, ws_market, last_shares, last_px, ws_leaves, ws_cum);
    metrics.record_exec(ExecRecord {
        contestant_id: contestant_id.to_string(),
        bot_id: bot_id.to_string(),
        cl_ord_id: cl_ord_id.to_string(),
        exec_seq,
        exec_type: exec_type.to_string(),
        side: ws_side,
        qty: ws_qty,
        price: ws_price,
        is_market: ws_market,
        latency_us,
        last_shares: last_shares.map(|x| x as u64),
        last_px,
        leaves_qty: ws_leaves,
        cum_qty: ws_cum,
    });
}
