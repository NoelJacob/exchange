use std::collections::VecDeque;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;

/// Parsed WS response from the exchange.
#[derive(Debug)]
pub struct WsResponse {
    pub is_error: bool,
    pub error_code: Option<i32>,
    pub error_message: Option<String>,
    pub method: Option<String>,
    pub params: Option<Value>,
    pub is_notification: bool,
}

/// One WebSocket client connection to the exchange.
pub struct WsClient {
    write: futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    seq: u64,
    /// Queue of notifications that arrived out of order (before the matching sync response).
    pending_notifications: VecDeque<WsResponse>,
}

impl WsClient {
    /// Connect to ws://host:port/ws
    pub async fn connect(host: &str, port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let url = format!("ws://{host}:{port}/ws");
        let (ws_stream, _) = connect_async(&url).await?;
        let (write, read) = ws_stream.split();
        Ok(Self { write, read, seq: 0, pending_notifications: VecDeque::new() })
    }

    fn next_id(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Send a limit order and wait for the matching sync response by JSON-RPC id.
    pub async fn send_limit_order(
        &mut self,
        sender_id: &str,
        cl_ord_id: &str,
        side: &str,
        symbol: &str,
        qty: u64,
        price: f64,
    ) -> Result<WsResponse, Box<dyn std::error::Error>> {
        let id = self.next_id();
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "order.create.limit",
            "params": {
                "sender_id": sender_id,
                "target_id": "XCANG3",
                "sending_time": timestamp_rfc3339(),
                "cl_ord_id": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": price,
            },
            "id": id,
        });
        self.write.send(Message::Text(req.to_string())).await?;
        self.recv_matching(id).await
    }

    /// Send a market order and wait for the matching sync response by JSON-RPC id.
    pub async fn send_market_order(
        &mut self,
        sender_id: &str,
        cl_ord_id: &str,
        side: &str,
        symbol: &str,
        qty: u64,
    ) -> Result<WsResponse, Box<dyn std::error::Error>> {
        let id = self.next_id();
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "order.create.market",
            "params": {
                "sender_id": sender_id,
                "target_id": "XCANG3",
                "sending_time": timestamp_rfc3339(),
                "cl_ord_id": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
            },
            "id": id,
        });
        self.write.send(Message::Text(req.to_string())).await?;
        self.recv_matching(id).await
    }

    /// Read from WebSocket until we find the response matching `expected_id`.
    /// Notifications (messages without id or with is_notification=true) are queued.
    async fn recv_matching(&mut self, expected_id: u64) -> Result<WsResponse, Box<dyn std::error::Error>> {
        // First drain any already-queued notifications
        if let Some(notif) = self.pending_notifications.pop_front() {
            // Check if the queued notification is actually the sync response we want
            let resp_id = extract_id_from_ws(&notif);
            if resp_id == Some(expected_id) {
                return Ok(notif);
            }
            // Re-check: if we had queued a sync response by mistake, handle it
        }

        loop {
            match self.read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let raw = &text[..text.len().min(500)];
                    eprintln!("[WS-RECV] raw={raw}");
                    let parsed = parse_ws_message(&text);

                    // Extract the JSON-RPC id from the raw response
                    let msg_id = extract_id_from_raw(&text);

                    if parsed.is_notification || msg_id.is_none() {
                        // Notification — queue for later polling
                        eprintln!("[WS-QUEUE] notification method={:?}", parsed.method);
                        self.pending_notifications.push_back(parsed);
                        continue;
                    }

                    if msg_id == Some(expected_id) {
                        return Ok(parsed);
                    }

                    // Response for a different id — queue as pending (shouldn't happen normally)
                    eprintln!("[WS-QUEUE] unexpected id {:?} (expected {})", msg_id, expected_id);
                    self.pending_notifications.push_back(parsed);
                }
                Some(Ok(Message::Ping(data))) => {
                    self.write.send(Message::Pong(data)).await?;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => return Err("WebSocket stream closed".into()),
            }
        }
    }

    /// Read the next available message without filtering (for tests).
    pub async fn recv(&mut self) -> Result<WsResponse, Box<dyn std::error::Error>> {
        if let Some(notif) = self.pending_notifications.pop_front() {
            return Ok(notif);
        }
        loop {
            match self.read.next().await {
                Some(Ok(Message::Text(text))) => {
                    eprintln!("[WS-RECV] raw={}", &text[..text.len().min(500)]);
                    let parsed = parse_ws_message(&text);
                    return Ok(parsed);
                }
                Some(Ok(Message::Ping(data))) => {
                    self.write.send(Message::Pong(data)).await?;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => return Err("WebSocket stream closed".into()),
            }
        }
    }

    /// Non-blocking poll for a single notification from the queue or wire.
    pub async fn try_recv_notification(&mut self) -> Option<WsResponse> {
        // First drain queued notifications
        if let Some(notif) = self.pending_notifications.pop_front() {
            return Some(notif);
        }
        // Then try wire with short timeout
        match tokio::time::timeout(
            std::time::Duration::from_millis(5),
            self.recv(),
        )
        .await
        {
            Ok(Ok(r)) if r.is_notification => Some(r),
            // If we got a sync response via recv, queue it and return None
            Ok(Ok(r)) => {
                self.pending_notifications.push_back(r);
                None
            }
            _ => None,
        }
    }

    /// Close gracefully.
    pub async fn close(self) {
        // Drop sends close frame automatically.
    }
}

fn timestamp_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Extract JSON-RPC id from a raw JSON text string.
fn extract_id_from_raw(text: &str) -> Option<u64> {
    let v: Value = serde_json::from_str(text).ok()?;
    v.get("id")?.as_u64()
}

/// Extract JSON-RPC id from a parsed WsResponse (reconstruct from the data).
fn extract_id_from_ws(resp: &WsResponse) -> Option<u64> {
    // WsResponse doesn't store the id; this is only used for queued responses.
    // For notifications this returns None.
    if resp.is_notification {
        return None;
    }
    // We can't extract id from the parsed response — only used internally.
    None
}

/// Parse a JSON-RPC 2.0 text frame into a WsResponse.
fn parse_ws_message(text: &str) -> WsResponse {
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return WsResponse {
                is_error: true,
                error_code: Some(-32700),
                error_message: Some(format!("parse error: {e}")),
                method: None,
                params: None,
                is_notification: false,
            };
        }
    };

    // Check for JSON-RPC error envelope
    if let Some(err) = v.get("error") {
        return WsResponse {
            is_error: true,
            error_code: err.get("code").and_then(|c| c.as_i64()).map(|c| c as i32),
            error_message: err.get("message").and_then(|m| m.as_str()).map(|s| s.to_string()),
            method: None,
            params: None,
            is_notification: false,
        };
    }

    // Method from outer (notification) or nested result.method (sync response)
    let method = v
        .get("method")
        .or_else(|| v.get("result")?.get("method"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());

    // Params from outer (notification) or nested result.params (sync response)
    let params = v
        .get("params")
        .or_else(|| v.get("result")?.get("params"))
        .cloned();

    // Notification if no id field
    let is_notification = v.get("id").is_none() || v.get("id") == Some(&Value::Null);

    WsResponse {
        is_error: false,
        error_code: None,
        error_message: None,
        method,
        params,
        is_notification,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sync_limit_new() {
        let text = r#"{"jsonrpc":"2.0","result":{"method":"order.report.new","params":{"sender_id":"XCANG3","target_id":"CLIENT01","transact_time":"2026-06-04T14:35:05.000Z","cl_ord_id":"CL_ORD_03","ex_ord_id":"EX_ORD_99","exec_id":"FILL_883","symbol":"AAPL","side":"buy","qty":100,"price":150.25}},"id":10003}"#;
        let parsed = parse_ws_message(text);
        assert!(!parsed.is_error);
        assert!(!parsed.is_notification);
        assert_eq!(parsed.method.as_deref(), Some("order.report.new"));
        assert!(parsed.params.is_some());
    }

    #[test]
    fn parse_notification() {
        let text = r#"{"jsonrpc":"2.0","method":"order.report.fill","params":{"sender_id":"XCANG3","target_id":"CLIENT01","transact_time":"2026-06-04T14:35:05.000Z","cl_ord_id":"CL_ORD_03","ex_ord_id":"EX_ORD_99","exec_id":"FILL_883","symbol":"AAPL","side":"buy","qty":100,"leaves_qty":0,"cum_qty":100,"last_shares":60,"last_px":150.25,"avg_px":150.25,"price":150.25}}"#;
        let parsed = parse_ws_message(text);
        assert!(!parsed.is_error);
        assert!(parsed.is_notification);
        assert_eq!(parsed.method.as_deref(), Some("order.report.fill"));
    }
}
