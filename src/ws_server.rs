use std::sync::Arc;

use pricelevel::prelude::*;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use chrono::prelude::*;

use crate::ExecutionReportMethod;

#[derive(Deserialize, Debug)]
struct WsRequest {
    jsonrpc: String,
    method: String,
    params: serde_json::Value,
    id: serde_json::Value,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct CreateLimitParams {
    sender_id: String,
    #[allow(dead_code)]
    target_id: String,
    #[allow(dead_code)]
    sending_time: String,
    cl_ord_id: String,
    symbol: String,
    side: String,
    qty: u64,
    price: f64,
}

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
    fn price(&self) -> f64 {
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

pub fn report_to_json(report: &crate::ExecutionReport) -> serde_json::Value {
    let side = match report.side {
        Side::Buy => "buy",
        Side::Sell => "sell"
    };
    let transact_time = report.transact_time.to_rfc3339_opts(SecondsFormat::Micros, true);

    match report.method {
        ExecutionReportMethod::Fill | ExecutionReportMethod::Partial => {
            let method = match report.method {
                ExecutionReportMethod::Fill => "fill",
                ExecutionReportMethod::Partial => "partial",
                _ => unreachable!()
            };
            let ex_ord_id = report.ex_ord_id.clone().expect("[WS] Missing ex_order_id");
            let last_shares = report.last_shares.expect("[WS] Missing last_shares");
            let last_px = report.last_px.expect("[WS] Missing last_px");

            let mut params = serde_json::json!({
                    "sender_id": "XCANG3",
                    "target_id": report.target_id,
                    "transact_time": transact_time,
                    "ex_ord_id": ex_ord_id,
                    "cl_ord_id": report.cl_ord_id,
                    "exec_id": report.exec_id,
                    "symbol": report.symbol,
                    "side": side,
                    "qty": report.qty,
                    "leaves_qty": report.leaves_qty,
                    "cum_qty": report.cum_qty,
                    "last_shares": last_shares,
                    "last_px": last_px,
                    "avg_px": report.avg_px
            });
            if !report.is_market {
                params.as_object_mut().expect("[WS] Params missing").insert("price".to_string(), serde_json::json!(report.price));
            }
            serde_json::json!({
                "method": format!("order.report.{}", method),
                "params": params
            })
        }

        ExecutionReportMethod::New => {
            let ex_ord_id = report.ex_ord_id.clone().expect("[WS] Missing ex_order_id");

            let mut params = serde_json::json!({
                    "sender_id": "XCANG3",
                    "target_id": report.target_id,
                    "transact_time": transact_time,
                    "ex_ord_id": ex_ord_id,
                    "cl_ord_id": report.cl_ord_id,
                    "exec_id": report.exec_id,
                    "symbol": report.symbol,
                    "side": side,
                    "qty": report.qty
            });
            if !report.is_market {
                params.as_object_mut().expect("[WS] Params missing").insert("price".to_string(), serde_json::json!(report.price));
            }
            serde_json::json!({
                "method": format!("order.report.new"),
                "params": params
            })
        }

        ExecutionReportMethod::Rejected => {
            let ex_ord_id = report.ex_ord_id.clone().unwrap_or("NONE".to_string());
            let reject_reason = report.reject_reason.clone().expect("[WS] Missing reject_reason");

            let mut params = serde_json::json!({
                    "sender_id": "XCANG3",
                    "target_id": report.target_id,
                    "transact_time": transact_time,
                    "ex_ord_id": ex_ord_id,
                    "cl_ord_id": report.cl_ord_id,
                    "exec_id": report.exec_id,
                    "symbol": report.symbol,
                    "side": side,
                    "qty": report.qty,
                    "reject_reason": reject_reason
            });
            if !report.is_market {
                params.as_object_mut().expect("[WS] Params missing").insert("price".to_string(), serde_json::json!(report.price));
            }
            serde_json::json!({
                "method": format!("order.report.{}", "rejected"),
                "params": params
            })
        }
    }
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

    // Writer: consume ws_rx and send to WebSocket.
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

        let parsed = match parse_params(&req.method, &req.params) {
            Ok(p) => p,
            Err(e) => {
                send_error(
                    &ws_tx,
                    &req.id,
                    -32602,
                    "Invalid params",
                    &e
                );
                continue;
            }
        };

        let ob_side = match side_to_book(parsed.side_str()) {
            Ok(s) => s,
            Err(e) => {
                send_error(
                    &ws_tx,
                    &req.id,
                    -32602,
                    "Invalid params",
                    &e
                );
                continue;
            }
        };

        let is_market = parsed.is_market();
        let price_f64 = parsed.price();
        let qty = parsed.qty();
        if qty == 0 {
            send_error(
                &ws_tx,
                &req.id,
                -32602,
                "Invalid params",
                "non-positive quantity"
            );
            continue;
        }

        if !is_market && (!price_f64.is_finite() || price_f64 <= 0.0) {
            send_error(
                &ws_tx,
                &req.id,
                -32602,
                "Invalid params",
                &format!("invalid price {price_f64} for limit order"),
            );
            continue;
        }

        let symbol = parsed.symbol().trim();
        if symbol.is_empty() {
                send_error(
                    &ws_tx,
                    &req.id,
                    -32602,
                    "Invalid params",
                    "missing/empty symbol",
                );
                continue;
        }

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
        };

        let ex_ord_id = state.order_id_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string();
        // 5. Build OrderInfo with target_id for omnibus routing.
        let info = crate::OrderInfo {
            target_id: parsed.sender_id().to_string(),
            cl_ord_id: parsed.cl_ord_id().to_string(),
            order_qty: parsed.qty(),
            cum_value_cents: 0,
            cum_qty: 0,
            ex_ord_id,
            connection_kind: crate::ConnectionKind::Ws {
                sender: ws_tx.clone(),
            },
            price: price_f64
        };

        let (outcome, exec_id) = {
            let mut pending = state.pending.lock();
            let exec_id = state.exec_id_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                .to_string();
            eprintln!("[EXEC-ID-GEN] WS sync: cl_ord_id={} exec_id={} (before submit)", parsed.cl_ord_id(), exec_id);
            let outcome = crate::submit(
                &book,
                is_market,
                ob_side,
                &mut pending,
                &info
            );

            (outcome, exec_id)
        };

        let report = match outcome {
            Ok(o) => {
                crate::build_execution_report(
                    o,
                    &info,
                    parsed.symbol(),
                    ob_side,
                    &exec_id,
                    is_market
                )
            }
            Err(e) => {
                eprintln!("[WS] Order {} rejected: {e}", parsed.cl_ord_id());
                crate::build_reject_report(
                    e.to_string(),
                    &info,
                    parsed.symbol(),
                    ob_side,
                    &exec_id,
                    is_market
                )
            }
        };
        let val = report_to_json(&report);

        let resp = serde_json::json!({
            "jsonrpc": "2.0",
            "result" : val,
            "id": req.id
        });

        if ws_tx.send(resp.to_string()).is_err() {
            eprintln!("[WS] Dropped response for {} client disconnected (seq={} lost)", parsed.cl_ord_id(), exec_id);
            break;  // Client is gone, stop processing
        }
    }

    eprintln!("[WS] Client disconnected");
    // Dropping ws_tx will stop writer
    let _ = write_handle.await;
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
