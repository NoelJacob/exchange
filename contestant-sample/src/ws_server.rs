use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize, Debug)]
struct OrderMessage {
    order_id: String,
    contestant_id: String,
    side: u8,
    price: f64,
    qty: u32,
    ord_type: u8,
    symbol: String,
    #[allow(dead_code)] // wire protocol latency field, not used in matching
    ts_sent_us: i64,
    bot_id: String,
}

#[derive(Serialize, Debug)]
struct ExecutionMessage {
    order_id: String,
    contestant_id: String,
    fill_price: f64,
    fill_qty: u32,
    exec_type: String,
    ts_recv_us: i64,
    bot_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

fn timestamp_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

async fn handle_ws_client(stream: tokio::net::TcpStream, books: crate::Books) {
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

    async fn send_reject(
        writer: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
            tokio_tungstenite::tungstenite::Message,
        >,
        order: &OrderMessage,
        reason: &str,
    ) {
        let response = ExecutionMessage {
            order_id: order.order_id.clone(),
            contestant_id: order.contestant_id.clone(),
            fill_price: 0.0,
            fill_qty: 0,
            exec_type: "rejected".to_string(),
            ts_recv_us: timestamp_us(),
            bot_id: order.bot_id.clone(),
            reason: Some(reason.to_string()),
        };
        let json = serde_json::to_string(&response).unwrap();
        if writer.send(Message::Text(json.into())).await.is_err() {
            eprintln!("[WS] Failed to send reject");
        }
    }

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

        let order: OrderMessage = match serde_json::from_str(&text) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("[WS] Failed to parse order: {}", e);
                continue;
            }
        };

        let ob_side = match order.side {
            1 => orderbook_rs::Side::Buy,
            2 => orderbook_rs::Side::Sell,
            other => {
                eprintln!(
                    "[WS] Invalid side {} in order {}",
                    other, order.order_id
                );
                send_reject(
                    &mut writer,
                    &order,
                    &format!("invalid side {other} (expected 1=Buy or 2=Sell)"),
                )
                .await;
                continue;
            }
        };

        let is_market = match order.ord_type {
            1 => true,
            2 => false,
            other => {
                eprintln!(
                    "[WS] Invalid ord_type {} in order {}",
                    other, order.order_id
                );
                send_reject(
                    &mut writer,
                    &order,
                    &format!("invalid ord_type {other} (expected 1=market or 2=limit)"),
                )
                .await;
                continue;
            }
        };

        let book = match crate::get_or_create_book(&books, &order.symbol) {
            Some(b) => b,
            None => {
                eprintln!("[WS] Missing/empty symbol in order {}", order.order_id);
                send_reject(
                    &mut writer,
                    &order,
                    "missing/empty symbol",
                )
                .await;
                continue;
            }
        };

        let price_cents: u128 = if is_market {
            0
        } else {
            if !order.price.is_finite() || order.price <= 0.0 {
                eprintln!(
                    "[WS] Non-positive/non-finite price {} in order {}",
                    order.price, order.order_id
                );
                send_reject(
                    &mut writer,
                    &order,
                    &format!(
                        "non-positive/non-finite price {} for limit order",
                        order.price
                    ),
                )
                .await;
                continue;
            }
            (order.price * 100.0).round() as u128
        };

        eprintln!(
            "[WS] Order id={} symbol={} side={} price={} qty={}",
            order.order_id,
            order.symbol,
            if ob_side == orderbook_rs::Side::Buy { "Buy" } else { "Sell" },
            order.price,
            order.qty,
        );

        let outcome = match crate::submit(&book, is_market, ob_side, order.qty as u64, price_cents) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("[WS] Order {} rejected: {e}", order.order_id);
                send_reject(&mut writer, &order, &e.to_string()).await;
                continue;
            }
        };

        let fill_price = if outcome.filled_qty > 0 {
            // Round to 2 decimal places so wire format matches FIX's {:.2} string.
            let f = outcome.avg_price_cents as f64 / 100.0;
            (f * 100.0).round() / 100.0
        } else {
            0.0
        };
        let exec_type = if outcome.filled_qty > 0 {
            if outcome.filled_qty == order.qty as u64 { "fill" } else { "partial_fill" }
        } else {
            "new"
        }
        .to_string();

        let response = ExecutionMessage {
            order_id: order.order_id,
            contestant_id: order.contestant_id,
            fill_price,
            fill_qty: outcome.filled_qty as u32,
            exec_type,
            ts_recv_us: timestamp_us(),
            bot_id: order.bot_id,
            reason: None,
        };
        let json = serde_json::to_string(&response).unwrap();
        if writer.send(Message::Text(json.into())).await.is_err() {
            eprintln!("[WS] Failed to send response");
            break;
        }
    }

    eprintln!("[WS] Client disconnected");
}

pub async fn run(books: crate::Books) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    eprintln!("[WS] Server listening on 0.0.0.0:8080");

    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("[WS] Connection from {}", addr);
        let books = Arc::clone(&books);
        tokio::spawn(async move {
            handle_ws_client(stream, books).await;
        });
    }
}
