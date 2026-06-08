use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// ── JSON-RPC 2.0 request envelope (per WS.md §1.1) ───────────────────

/// Top-level WS request envelope. We use `serde_json::Value` for `params`
/// and dispatch on `method` to validate the inner shape.
#[derive(Deserialize, Debug)]
struct WsRequest {
    jsonrpc: String,
    method: String,
    params: serde_json::Value,
    id: serde_json::Value,
}

/// Limit-order params (per WS.md §2.1).
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct CreateLimitParams {
    sender_id: String,
    #[allow(dead_code)] // validated via WS.md schema; routing uses sender_id
    target_id: String,
    #[allow(dead_code)] // echoed for client correlation, not used in matching
    sending_time: String,
    cl_ord_id: String,
    symbol: String,
    side: String,
    qty: u64,
    price: f64,
    #[allow(dead_code)] // client-side seq number; not used in matching
    seq: u64,
}

/// Market-order params (per WS.md §2.2).
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct CreateMarketParams {
    sender_id: String,
    #[allow(dead_code)]
    target_id: String,
    #[allow(dead_code)]
    sending_time: String,
    cl_ord_id: String,
    symbol: String,
    side: String,
    qty: u64,
    #[allow(dead_code)]
    seq: u64,
}

#[derive(Debug)]
enum ParsedParams {
    Limit(CreateLimitParams),
    Market(CreateMarketParams),
}

impl ParsedParams {
    fn sender_id(&self) -> &str {
        match self {
            ParsedParams::Limit(p) => &p.sender_id,
            ParsedParams::Market(p) => &p.sender_id,
        }
    }
    fn cl_ord_id(&self) -> &str {
        match self {
            ParsedParams::Limit(p) => &p.cl_ord_id,
            ParsedParams::Market(p) => &p.cl_ord_id,
        }
    }
    fn symbol(&self) -> &str {
        match self {
            ParsedParams::Limit(p) => &p.symbol,
            ParsedParams::Market(p) => &p.symbol,
        }
    }
    fn qty(&self) -> u64 {
        match self {
            ParsedParams::Limit(p) => p.qty,
            ParsedParams::Market(p) => p.qty,
        }
    }
    fn is_market(&self) -> bool {
        matches!(self, ParsedParams::Market(_))
    }
    fn price_cents(&self) -> f64 {
        match self {
            ParsedParams::Limit(p) => p.price,
            ParsedParams::Market(_) => 0.0,
        }
    }
    fn side_str(&self) -> &str {
        match self {
            ParsedParams::Limit(p) => &p.side,
            ParsedParams::Market(p) => &p.side,
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────

/// Send a sync `error` envelope (per WS.md §1.3).
fn send_error(
    ws_tx: &mpsc::UnboundedSender<String>,
    id: &serde_json::Value,
    code: i32,
    message: &str,
    details: &str,
) {
    let resp = serde_json::json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message,
            "data": { "details": details },
        },
        "id": id,
    });
    let _ = ws_tx.send(resp.to_string());
}

/// Send a sync `result` envelope with an `ExecutionReport` as params.
pub fn send_result(
    ws_tx: &mpsc::UnboundedSender<String>,
    id: &serde_json::Value,
    report: &crate::ExecutionReport,
) {
    let resp = serde_json::json!({
        "jsonrpc": "2.0",
        "result": {
            "method": format!("order.report.{}", report.method),
            "params": {
                "sender_id": "XCANG3",
                "target_id": report.target_id,
                "sending_time": report.sending_time,
                "ex_ord_id": report.ex_ord_id,
                "cl_ord_id": report.cl_ord_id,
                "exec_id": report.exec_id,
                "symbol": report.symbol,
                "side": report.side,
                "qty": report.qty,
                "leaves_qty": report.leaves_qty,
                "cum_qty": report.cum_qty,
                "last_shares": report.last_shares,
                "last_px": report.last_px,
                "avg_px": report.avg_px,
                "reject_reason": report.reject_reason,
            },
        },
        "id": id,
    });
    let _ = ws_tx.send(resp.to_string());
}

/// Send a server-initiated notification with an `ExecutionReport` as params.
/// No `id` field — per WS.md §3.
pub fn send_notification(ws_tx: &mpsc::UnboundedSender<String>, report: &crate::ExecutionReport) {
    let resp = serde_json::json!({
        "jsonrpc": "2.0",
        "method": format!("order.report.{}", report.method),
        "params": {
                "sender_id": "XCANG3",
                "target_id": report.target_id,
            "sending_time": report.sending_time,
            "ex_ord_id": report.ex_ord_id,
            "cl_ord_id": report.cl_ord_id,
            "exec_id": report.exec_id,
            "symbol": report.symbol,
            "side": report.side,
            "qty": report.qty,
            "leaves_qty": report.leaves_qty,
            "cum_qty": report.cum_qty,
            "last_shares": report.last_shares,
            "last_px": report.last_px,
            "avg_px": report.avg_px,
            "reject_reason": report.reject_reason,
        },
    });
    let _ = ws_tx.send(resp.to_string());
}

fn parse_params(method: &str, params: &serde_json::Value) -> Result<ParsedParams, String> {
    match method {
        "order.create.limit" => serde_json::from_value::<CreateLimitParams>(params.clone())
            .map(ParsedParams::Limit)
            .map_err(|e| format!("invalid limit params: {e}")),
        "order.create.market" => serde_json::from_value::<CreateMarketParams>(params.clone())
            .map(ParsedParams::Market)
            .map_err(|e| format!("invalid market params: {e}")),
        other => Err(format!("unknown method: {other}")),
    }
}

fn side_to_book(s: &str) -> Result<orderbook_rs::Side, String> {
    match s {
        "buy" => Ok(orderbook_rs::Side::Buy),
        "sell" => Ok(orderbook_rs::Side::Sell),
        other => Err(format!(
            "invalid side {other:?} (expected \"buy\" or \"sell\")"
        )),
    }
}

// ── per-connection handler ───────────────────────────────────────────

async fn handle_ws_client(stream: tokio::net::TcpStream, state: Arc<crate::AppState>) {
    let addr = stream.peer_addr().ok();
    eprintln!("[WS] New connection from {:?}", addr);

    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("[WS] Handshake failed: {}", e);
            return;
        }
    };

    let (mut writer, mut reader) = ws_stream.split();

    // Per-connection mpsc channel for async fill notifications.
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<String>();

    // Writer task: drain ws_rx and send to WebSocket.
    let write_handle = tokio::spawn(async move {
        while let Some(text) = ws_rx.recv().await {
            if writer.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg) = reader.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[WS] Read error: {}", e);
                break;
            }
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => {
                eprintln!("[WS] Client closed connection");
                break;
            }
            _ => continue,
        };

        // 1. Parse the outer envelope.
        let req: WsRequest = match serde_json::from_str(&text) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[WS] Malformed request: {e}");
                // Cannot echo id; send a null-id error.
                send_error(
                    &ws_tx,
                    &serde_json::Value::Null,
                    -32600,
                    "Invalid Request",
                    &format!("malformed JSON-RPC envelope: {e}"),
                );
                continue;
            }
        };

        if req.jsonrpc != "2.0" {
            send_error(
                &ws_tx,
                &req.id,
                -32600,
                "Invalid Request",
                "jsonrpc must be \"2.0\"",
            );
            continue;
        }

        // 2. Parse params per method.
        let parsed = match parse_params(&req.method, &req.params) {
            Ok(p) => p,
            Err(e) => {
                send_error(&ws_tx, &req.id, -32602, "Invalid params", &e);
                continue;
            }
        };

        // 3. Side validation.
        let ob_side = match side_to_book(parsed.side_str()) {
            Ok(s) => s,
            Err(e) => {
                send_error(&ws_tx, &req.id, -32602, "Invalid params", &e);
                continue;
            }
        };

        let is_market = parsed.is_market();
        let price_f64 = parsed.price_cents();
        let qty = parsed.qty();
        if qty == 0 {
            send_error(&ws_tx, &req.id, -32602, "Invalid params", "non-positive quantity");
            continue;
        }

        // 4. Price validation (limit only).
        if !is_market && (!price_f64.is_finite() || price_f64 <= 0.0) {
            send_error(
                &ws_tx,
                &req.id,
                -32602,
                "Invalid params",
                &format!("non-positive/non-finite price {price_f64} for limit order"),
            );
            continue;
        }

        let price_cents: u128 = if is_market {
            0
        } else {
            (price_f64 * 100.0).round() as u128
        };

        let user_hash = crate::hash_user_id(parsed.sender_id());

        eprintln!(
            "[WS] Order cl_ord_id={} sender={} symbol={} side={} price={} qty={} method={}",
            parsed.cl_ord_id(),
            parsed.sender_id(),
            parsed.symbol(),
            if ob_side == orderbook_rs::Side::Buy {
                "Buy"
            } else {
                "Sell"
            },
            price_f64,
            parsed.qty(),
            req.method,
        );
        // 5. Get or create the book.
        let book = {
            let mut books = state.books.lock();
            crate::get_or_create_book(&mut books, parsed.symbol(), &state.trade_tx)
                .map(|b| Arc::clone(&b))
        };
        let book = match book {
            Some(b) => b,
            None => {
                send_error(
                    &ws_tx,
                    &req.id,
                    -32602,
                    "Invalid params",
                    "missing/empty symbol",
                );
                continue;
            }
        };
        // 5. Build OrderInfo with target_id for omnibus routing.
        let info = crate::OrderInfo {
            user_id: parsed.sender_id().to_string(),
            target_id: parsed.sender_id().to_string(),
            cl_ord_id: parsed.cl_ord_id().to_string(),
            order_qty: parsed.qty(),
            cum_value_cents: 0,
            ex_ord_id: String::new(),
            connection_kind: crate::ConnectionKind::Ws {
                sender: ws_tx.clone(),
            },
        };
        // 6. Submit the order. Clone info first since submit() moves it.
        let info_for_report = crate::OrderInfo {
            ex_ord_id: String::new(),
            ..info.clone()
        };
        let outcome = {
            let mut pending = state.pending.lock();
            crate::submit(
                &book,
                is_market,
                ob_side,
                parsed.qty(),
                price_cents,
                user_hash,
                &mut pending,
                info,
                &state.order_id_seq,
                &state.exec_id_seq,
            )
        };
        match outcome {
            Ok(o) => {
                let report = crate::build_execution_report(
                    &o,
                    &info_for_report,
                    parsed.symbol(),
                    ob_side,
                    parsed.qty(),
                );
                send_result(&ws_tx, &req.id, &report);
            }
            Err(e) => {
                eprintln!("[WS] Order {} rejected: {e}", parsed.cl_ord_id());
                let exec_id = state
                    .exec_id_seq
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    .to_string();
                let report = crate::build_reject_report(
                    &e.to_string(),
                    &info_for_report,
                    parsed.symbol(),
                    ob_side,
                    parsed.qty(),
                    &exec_id,
                );
                send_result(&ws_tx, &req.id, &report);
            }
        }
    }

    eprintln!("[WS] Client disconnected");
    // Dropping ws_tx will cause the writer task to exit.
    write_handle.await.ok();
}

pub async fn run(state: Arc<crate::AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    eprintln!("[WS] Server listening on 0.0.0.0:8080");

    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("[WS] Connection from {}", addr);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            handle_ws_client(stream, state).await;
        });
    }
}
