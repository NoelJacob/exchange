// E2E tests for contestant-sample matching engine.
// Run: cargo test --test e2e -- --test-threads=1

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::StreamExt;

const FIX_PORT: u16 = 9090;
const WS_PORT: u16 = 8080;
const BIN: &str = env!("CARGO_BIN_EXE_contestant-sample");

fn utc_now() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    let s = d.as_secs();
    format!(
        "20260601-{:02}:{:02}:{:02}.{:03}",
        (s / 3600) % 24,
        (s / 60) % 60,
        s % 60,
        d.subsec_millis(),
    )
}

fn get(map: &HashMap<String, String>, tag: &str) -> String {
    map.get(tag).cloned().unwrap_or_default()
}

// ── FIX client ───────────────────────────────────────────────────────

struct FixCli {
    stream: tokio::net::TcpStream,
    seq: u32,
    sender_comp_id: String,
}

impl FixCli {
    async fn connect() -> Self {
        Self::connect_with_id("CLIENT").await
    }

    async fn connect_with_id(sender_comp_id: &str) -> Self {
        let addr = format!("127.0.0.1:{FIX_PORT}");
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::net::TcpStream::connect(&addr),
        )
        .await
        .expect("FIX connect timeout")
        .expect("FIX connect failed");
        Self {
            stream,
            seq: 1,
            sender_comp_id: sender_comp_id.to_string(),
        }
    }

    fn msg(&mut self, msg_type: &str, tags: &[(&str, &str)]) -> Vec<u8> {
        let ts = utc_now();
        let seq = self.seq;
        self.seq += 1;
        let mut body =
            format!("35={msg_type}\x0134={seq}\x0149={}\x0156=XCANG3\x0152={ts}\x01", self.sender_comp_id);
        for (k, v) in tags {
            body += &format!("{k}={v}\x01");
        }
        let hdr = format!("8=FIX.4.2\x019={}\x01", body.len());
        let all = format!("{hdr}{body}");
        let cs: u8 = all.bytes().fold(0u8, |a, b| a.wrapping_add(b));
        format!("{all}10={cs:03}\x01").into_bytes()
    }

    async fn send(&mut self, msg_type: &str, tags: &[(&str, &str)]) {
        use tokio::io::AsyncWriteExt;
        let data = self.msg(msg_type, tags);
        self.stream.write_all(&data).await.unwrap();
    }

    async fn recv(&mut self) -> HashMap<String, String> {
        use tokio::io::AsyncReadExt;
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut buf = Vec::with_capacity(1024);
            let mut tmp = [0u8; 1];
            loop {
                let n = self.stream.read(&mut tmp).await.unwrap();
                assert!(n > 0, "connection closed");
                buf.push(tmp[0]);
                let len = buf.len();
                if len >= 6 && buf[len - 3] == b'=' && &buf[len - 5..len - 3] == b"10" {
                    // Strip trailing SOH if present, then parse
                    let end = if buf[len - 1] == b'\x01' {
                        len - 1
                    } else {
                        len
                    };
                    let raw = String::from_utf8_lossy(&buf[..end]);
                    return raw
                        .split('\x01')
                        .filter_map(|f| f.find('=').map(|i| (f[..i].into(), f[i + 1..].into())))
                        .collect();
                }
            }
        })
        .await
        .expect("FIX recv timeout (5s)")
    }
}

// ── WS client ────────────────────────────────────────────────────────
type WsStr =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
struct WsCli {
    tx: futures_util::stream::SplitSink<WsStr, tokio_tungstenite::tungstenite::Message>,
    rx: futures_util::stream::SplitStream<WsStr>,
    next_id: i64,
}
impl WsCli {
    async fn connect() -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{WS_PORT}"))
            .await
            .expect("WS connect");
        let (tx, rx) = ws.split();
        Self {
            tx,
            rx,
            next_id: 10000,
        }
    }
    fn next_req_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
    /// Send a `order.create.limit` request envelope (per WS.md §2.1).
    async fn order_limit(
        &mut self,
        symbol: &str,
        side: &str,
        price: f64,
        qty: u64,
        cl_ord_id: &str,
        sender_id: &str,
    ) -> i64 {
        use futures_util::SinkExt;
        let id = self.next_req_id();
        // utc_now() yields "YYYYMMDD-HH:MM:SS.mmm" — rewrite to ISO 8601
        // "YYYY-MM-DDTHH:MM:SS.mmmZ" for the WS spec.
        let raw = utc_now();
        let sending_time = format!(
            "{}-{}-{}T{}Z",
            &raw[0..4],
            &raw[4..6],
            &raw[6..8],
            &raw[9..]
        );
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "order.create.limit",
            "params": {
                "sender_id": sender_id,
                "target_id": "XCANG3",
                "sending_time": sending_time,
                "cl_ord_id": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": price,
                "seq": 1
            },
            "id": id
        });
        self.tx
            .send(tokio_tungstenite::tungstenite::Message::Text(
                payload.to_string(),
            ))
            .await
            .unwrap();
        id
    }
    /// Send a `order.create.market` request envelope (per WS.md §2.2).
    async fn order_market(
        &mut self,
        symbol: &str,
        side: &str,
        qty: u64,
        cl_ord_id: &str,
        sender_id: &str,
    ) -> i64 {
        use futures_util::SinkExt;
        let id = self.next_req_id();
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "order.create.market",
            "params": {
                "sender_id": sender_id,
                "target_id": "XCANG3",
                "sending_time": "2026-06-04T14:33:05.000Z",
                "cl_ord_id": cl_ord_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "seq": 1
            },
            "id": id
        });
        self.tx
            .send(tokio_tungstenite::tungstenite::Message::Text(
                payload.to_string(),
            ))
            .await
            .unwrap();
        id
    }
    /// Receive the next JSON message and return it as a `serde_json::Value`.
    async fn recv(&mut self) -> serde_json::Value {
        let msg = self.rx.next().await.unwrap().unwrap();
        match msg {
            tokio_tungstenite::tungstenite::Message::Text(t) => serde_json::from_str(&t).unwrap(),
            other => panic!("expected text, got {other:?}"),
        }
    }
    /// Receive and assert a sync `result` envelope (per WS.md §1.2).
    /// Returns the inner `method` string (e.g. "order.report.filled").
    /// The `params` object is returned for further assertions.
    async fn recv_result(&mut self) -> (String, serde_json::Value) {
        let v = self.recv().await;
        assert_eq!(v["jsonrpc"], "2.0", "must be JSON-RPC 2.0");
        assert!(
            v.get("result").is_some(),
            "expected sync result envelope, got: {v}"
        );
        let method = v["result"]["method"]
            .as_str()
            .expect("method string")
            .to_string();
        let params = v["result"]["params"].clone();
        let _ = v["id"].as_i64().expect("id integer");
        (method, params)
    }
    /// Receive and assert a sync `error` envelope. Returns `(code, details)`.
    async fn recv_error(&mut self) -> (i64, String) {
        let v = self.recv().await;
        assert_eq!(v["jsonrpc"], "2.0");
        assert!(
            v.get("error").is_some(),
            "expected error envelope, got: {v}"
        );
        let code = v["error"]["code"].as_i64().expect("code integer");
        let details = v["error"]["data"]["details"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let _ = v["id"].as_i64().expect("id integer");
        (code, details)
    }
    /// Receive and assert a server-initiated notification with the given method.
    /// Returns the `params` object.
    async fn recv_report(&mut self, method: &str) -> serde_json::Value {
        let v = self.recv().await;
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(
            v["method"], method,
            "expected method {method}, got envelope: {v}"
        );
        assert!(v.get("id").is_none(), "notification must not have id");
        assert!(v.get("params").is_some(), "notification must have params");
        v["params"].clone()
    }
}
// ── server lifecycle ─────────────────────────────────────────────────

struct Server(Child);

impl Server {
    fn start() -> Self {
        // Drop from prior test kills process. If orphan persists,
        // tests fail with "address in use" — kill manually then rerun.
        let child = Command::new(BIN)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn contestant-sample");
        std::thread::sleep(Duration::from_secs(3));
        Server(child)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ── tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn fix_logon() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let r = c.recv().await;

    assert_eq!(get(&r, "35"), "A", "expected Logon reply");
    assert_eq!(get(&r, "49"), "XCANG3", "SenderCompID");
    assert_eq!(get(&r, "98"), "0", "EncryptMethod");
    assert_eq!(get(&r, "108"), "30", "HeartBtInt");
}

#[tokio::test]
async fn fix_heartbeat() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    c.send("1", &[("112", "test1")]).await;
    let r = c.recv().await;
    assert_eq!(get(&r, "35"), "0", "expected Heartbeat");
}

#[tokio::test]
async fn fix_limit_rests() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    c.send(
        "D",
        &[
            ("11", "o1"),
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "100"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = c.recv().await;

    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(get(&r, "150"), "0", "ExecType=New");
    assert_eq!(get(&r, "39"), "0", "OrdStatus=New");
    assert_eq!(get(&r, "151"), "100", "LeavesQty=100");
    assert!(!get(&r, "37").is_empty(), "Tag 37 OrderID must be present");
}

#[tokio::test]
async fn fix_limit_fill() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    c.send(
        "D",
        &[
            ("11", "o1"),
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "100"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let _ = c.recv().await;

    c.send(
        "D",
        &[
            ("11", "o2"),
            ("54", "2"),
            ("55", "AAPL"),
            ("38", "60"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = c.recv().await;

    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(get(&r, "150"), "2", "ExecType=Fill");
    assert_eq!(get(&r, "32"), "60", "LastShares=60");

    // Async fill notification for the resting Buy (maker)
    let r2 = c.recv().await;
    assert_eq!(get(&r2, "35"), "8", "async ExecReport");
    assert_eq!(get(&r2, "11"), "o1", "ClOrdID of resting buy");
    assert_eq!(get(&r2, "150"), "1", "ExecType=PartialFill (60/100 filled)");
    assert_eq!(get(&r2, "32"), "60", "LastShares=60");
    assert_eq!(get(&r2, "151"), "40", "LeavesQty=40");
}

#[tokio::test]
async fn fix_market_buy() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    c.send(
        "D",
        &[
            ("11", "o1"),
            ("54", "2"),
            ("55", "AAPL"),
            ("38", "100"),
            ("40", "2"),
            ("44", "100.00"),
        ],
    )
    .await;
    let _ = c.recv().await;

    c.send(
        "D",
        &[
            ("11", "o2"),
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "30"),
            ("40", "1"),
        ],
    )
    .await;
    let r = c.recv().await;

    assert_eq!(get(&r, "35"), "8", "ExecReport");
    let et = get(&r, "150");
    assert!(et == "2" || et == "1", "ExecType fill/partial, got {et}");
    assert_eq!(get(&r, "32"), "30", "LastShares=30");
}
#[tokio::test]
async fn ws_rest_and_fill() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    // Resting Buy 50 — sync "added" result.
    w.order_limit("AAPL", "buy", 100.50, 50, "ws-rb", "CLIENT01")
        .await;
    let (method, p) = w.recv_result().await;
    assert_eq!(method, "order.report.added");
    assert_eq!(p["cl_ord_id"], "ws-rb", "echoes cl_ord_id");
    assert_eq!(p["qty"], 50);
    assert_eq!(p["leaves_qty"], 50, "resting leaves");
    assert!(
        !p["ex_ord_id"].as_str().unwrap_or("").is_empty(),
        "ex_ord_id must be present in sync response",
    );
    // Sell 20 matches the resting buy — taker gets sync fill.
    w.order_limit("AAPL", "sell", 100.50, 20, "ws-s", "CLIENT01")
        .await;
    let (method, p) = w.recv_result().await;
    assert_eq!(method, "order.report.fill");
    assert_eq!(p["cl_ord_id"], "ws-s", "taker cl_ord_id");
    assert_eq!(p["qty"], 20);
    assert_eq!(p["leaves_qty"], 0, "fully filled");
    assert_eq!(p["cum_qty"], 20);
    assert_eq!(p["last_shares"], 20);
    // Async partial fill for the resting buy.
    let p = w.recv_report("order.report.partial").await;
    assert_eq!(p["cl_ord_id"], "ws-rb", "maker async cl_ord_id");
    assert_eq!(p["side"], "buy");
    assert_eq!(p["qty"], 50);
    assert_eq!(p["last_shares"], 20);
    assert_eq!(p["cum_qty"], 20);
    assert_eq!(p["leaves_qty"], 30);
}

#[tokio::test]
async fn cross_protocol() {
    let _srv = Server::start();
    let mut fix = FixCli::connect().await;
    let mut ws = WsCli::connect().await;
    fix.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = fix.recv().await;
    fix.send(
        "D",
        &[
            ("11", "fb"),
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "100"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let _ = fix.recv().await;
    // WS Sells 40 against the FIX rest. Taker (WS) gets sync fill.
    ws.order_limit("AAPL", "sell", 100.50, 40, "ws-cp-s", "CLIENT01")
        .await;
    let (method, p) = ws.recv_result().await;
    assert_eq!(method, "order.report.fill");
    assert_eq!(p["cl_ord_id"], "ws-cp-s");
    assert_eq!(p["avg_px"].as_f64(), Some(100.5));
    let a = fix.recv().await;
    assert_eq!(get(&a, "35"), "8", "async ExecReport");
    assert_eq!(get(&a, "11"), "fb", "ClOrdID of resting buy");
    assert_eq!(get(&a, "150"), "1", "ExecType=PartialFill (40/100)");
    assert_eq!(get(&a, "32"), "40", "LastShares=40");
    assert_eq!(get(&a, "151"), "60", "LeavesQty=60");
    assert_eq!(get(&a, "14"), "40", "CumQty=40");
    assert!(
        !get(&a, "60").is_empty(),
        "Tag 60 TransactTime must be present"
    );
    assert_eq!(get(&a, "6"), "100.50", "AvgPx=100.50");
    // FIX Sells remaining 60 to fully close the resting buy.
    fix.send(
        "D",
        &[
            ("11", "fs"),
            ("54", "2"),
            ("55", "AAPL"),
            ("38", "60"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = fix.recv().await;
    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(get(&r, "150"), "2", "ExecType=Fill for remaining 60");
    assert_eq!(get(&r, "32"), "60", "LastShares=60");
    assert_eq!(get(&r, "6"), "100.50", "AvgPx=100.50");
    // Async fill for resting FIX Buy after remaining 60 filled.
    let a2 = fix.recv().await;
    assert_eq!(get(&a2, "35"), "8", "async ExecReport for remaining fill");
    assert_eq!(get(&a2, "11"), "fb", "ClOrdID of resting buy");
    assert_eq!(get(&a2, "150"), "2", "ExecType=Fill (fully filled)");
    assert_eq!(get(&a2, "32"), "60", "LastShares=60");
    assert_eq!(get(&a2, "151"), "0", "LeavesQty=0");
    assert_eq!(get(&a2, "6"), "100.50", "AvgPx=100.50");
}

#[tokio::test]
async fn fix_multi_symbol_independence() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    // Buy AAPL 100 @ 100.50 — rests in AAPL book
    c.send(
        "D",
        &[
            ("11", "o1"),
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "100"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = c.recv().await;
    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(get(&r, "150"), "0", "ExecType=New");
    assert_eq!(get(&r, "55"), "AAPL", "Symbol=AAPL");
    assert_eq!(get(&r, "151"), "100", "LeavesQty=100");

    // Sell MSFT 60 @ 100.50 — should ALSO rest (different symbol, NOT match AAPL buy)
    c.send(
        "D",
        &[
            ("11", "o2"),
            ("54", "2"),
            ("55", "MSFT"),
            ("38", "60"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = c.recv().await;
    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(
        get(&r, "150"),
        "0",
        "ExecType=New — MSFT should NOT match AAPL"
    );
    assert_eq!(get(&r, "55"), "MSFT", "Symbol=MSFT");
    assert_eq!(get(&r, "151"), "60", "LeavesQty=60");

    // Sell AAPL 60 @ 100.50 — fills against resting AAPL buy (same symbol)
    c.send(
        "D",
        &[
            ("11", "o3"),
            ("54", "2"),
            ("55", "AAPL"),
            ("38", "60"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = c.recv().await;
    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(get(&r, "150"), "2", "ExecType=Fill");
    assert_eq!(get(&r, "55"), "AAPL", "Symbol=AAPL");
    assert_eq!(get(&r, "32"), "60", "LastShares=60");
    assert_eq!(get(&r, "151"), "0", "LeavesQty=0");

    // Async fill for resting AAPL Buy after matched by AAPL Sell
    let a = c.recv().await;
    assert_eq!(get(&a, "35"), "8", "async ExecReport");
    assert_eq!(get(&a, "11"), "o1", "ClOrdID of resting AAPL buy");
    assert_eq!(get(&a, "150"), "1", "ExecType=PartialFill (60/100)");
}

#[tokio::test]
async fn ws_multi_symbol() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    w.order_limit("AAPL", "buy", 100.50, 50, "aapl-b", "CLIENT01")
        .await;
    let (method, p) = w.recv_result().await;
    assert_eq!(method, "order.report.added");
    assert_eq!(p["cl_ord_id"], "aapl-b");
    // Buy MSFT at a different price — proves it creates a separate book.
    w.order_limit("MSFT", "buy", 99.50, 30, "msft-b", "CLIENT01")
        .await;
    let (method, p) = w.recv_result().await;
    assert_eq!(method, "order.report.added");
    assert_eq!(p["cl_ord_id"], "msft-b");
    // Sell MSFT at 99.50 — matches MSFT buy. Taker gets sync fill.
    w.order_limit("MSFT", "sell", 99.50, 30, "msft-s", "CLIENT01")
        .await;
    let (method, p) = w.recv_result().await;
    assert_eq!(method, "order.report.fill");
    assert_eq!(p["cl_ord_id"], "msft-s");
    // Async fill for resting MSFT Buy (fully filled).
    let p = w.recv_report("order.report.fill").await;
    assert_eq!(p["cl_ord_id"], "msft-b");
    assert_eq!(p["qty"], 30);
    assert_eq!(p["leaves_qty"], 0);
}
#[tokio::test]
async fn fix_invalid_order_rejected() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    // Send NewOrderSingle without ClOrdID (tag 11) — required field.
    // We accept either a session-layer Reject (35=3) from the FIX engine
    // or an ExecutionReport with ExecType=8 from our application. Both
    // prove the order was not silently dropped.
    c.send(
        "D",
        &[
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "100"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = c.recv().await;

    let msg_type = get(&r, "35");
    assert!(
        msg_type == "3" || msg_type == "8",
        "expected session Reject (35=3) or ExecReport reject (35=8), got 35={msg_type}"
    );
    if msg_type == "8" {
        assert_eq!(get(&r, "150"), "8", "ExecType=Rejected");
        assert_eq!(get(&r, "39"), "8", "OrdStatus=Rejected");
    }
}
#[tokio::test]
async fn fix_submit_error_rejected() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    // Market buy on an empty book: the matching engine returns
    // InsufficientLiquidity, the helper converts it to SubmitError,
    // and process_new_order turns it into a Reject ExecutionReport.
    c.send(
        "D",
        &[
            ("11", "mbe"),
            ("54", "1"),
            ("55", "EMPTY"),
            ("38", "50"),
            ("40", "1"),
        ],
    )
    .await;
    let r = c.recv().await;

    let msg_type = get(&r, "35");
    assert!(
        msg_type == "3" || msg_type == "8",
        "expected session Reject (35=3) or ExecReport reject (35=8), got 35={msg_type}"
    );
    if msg_type == "8" {
        assert_eq!(get(&r, "150"), "8", "ExecType=Rejected");
    }
}

#[tokio::test]
async fn ws_submit_error_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    // Market buy on an empty book: the engine processes the request and
    // returns a sync `result` with method "rejected".
    w.order_market("WS_EMPTY", "buy", 50, "ws-mbe", "CLIENT01")
        .await;
    let (method, p) = w.recv_result().await;
    assert_eq!(method, "order.report.rejected");
    let reason = p["reject_reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("insufficient liquidity"),
        "reject_reason should mention insufficient liquidity, got: {reason}"
    );
}

#[tokio::test]
async fn ws_invalid_side_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    // side="bogus" is invalid; the engine must send a sync `error` envelope
    // with code -32602 and a non-empty `data.details` mentioning the cause.
    w.order_limit("AAPL", "bogus", 100.50, 50, "ws-bad-side", "CLIENT01")
        .await;
    let (code, details) = w.recv_error().await;
    assert_eq!(code, -32602, "code = Invalid params");
    assert!(
        details.contains("invalid side"),
        "details should mention invalid side, got: {details}"
    );
}


#[tokio::test]
async fn parity_ws_rest_fill_fix() {
    let _srv = Server::start();
    let mut fix = FixCli::connect().await;
    let mut ws = WsCli::connect().await;
    ws.order_limit("MSFT", "buy", 100.50, 100, "ws-msft-b", "CLIENT01")
        .await;
    let (method, p) = ws.recv_result().await;
    assert_eq!(method, "order.report.added");
    assert_eq!(p["cl_ord_id"], "ws-msft-b");
    fix.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = fix.recv().await;
    // FIX Sells 30 MSFT @ 100.50 against the WS-resting buy.
    fix.send(
        "D",
        &[
            ("11", "fs"),
            ("54", "2"),
            ("55", "MSFT"),
            ("38", "30"),
            ("40", "2"),
            ("44", "100.50"),
        ],
    )
    .await;
    let r = fix.recv().await;
    assert_eq!(get(&r, "35"), "8", "ExecReport");
    assert_eq!(get(&r, "150"), "2", "ExecType=Fill for 30");
    assert_eq!(get(&r, "32"), "30", "LastShares=30");
    // Async partial fill for resting WS Buy.
    let p = ws.recv_report("order.report.partial").await;
    assert_eq!(p["cl_ord_id"], "ws-msft-b");
    assert_eq!(p["side"], "buy");
    assert_eq!(p["qty"], 100);
    assert_eq!(p["last_shares"], 30);
    assert_eq!(p["cum_qty"], 30);
    assert_eq!(p["leaves_qty"], 70);
}

#[tokio::test]
async fn parity_ws_zero_price_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    // limit order with price 0.0 is non-positive and must be rejected.
    w.order_limit("AAPL", "buy", 0.0, 50, "ws-zp", "CLIENT01")
        .await;
    let (code, details) = w.recv_error().await;
    assert_eq!(code, -32602);
    assert!(
        details.contains("non-positive"),
        "details should mention non-positive, got: {details}"
    );
}
#[tokio::test]
async fn parity_ws_negative_price_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    w.order_limit("AAPL", "buy", -1.0, 50, "ws-np", "CLIENT01")
        .await;
    let (code, details) = w.recv_error().await;
    assert_eq!(code, -32602);
    assert!(
        details.contains("non-positive"),
        "details should mention non-positive, got: {details}"
    );
}
#[tokio::test]
async fn parity_ws_invalid_ord_type_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    // Sending a method that is not a recognised order method triggers the
    // `unknown method` error in the WS server's parse_params helper.
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "order.create.bogus",
        "params": {
            "sender_id": "CLIENT01",
            "target_id": "XCANG3",
            "sending_time": "2026-06-04T14:34:00.000Z",
            "cl_ord_id": "ws-bad-type",
            "symbol": "AAPL",
            "side": "buy",
            "qty": 50,
            "price": 100.50,
            "seq": 1
        },
        "id": 99999
    });
    use futures_util::SinkExt;
    w.tx.send(tokio_tungstenite::tungstenite::Message::Text(
        payload.to_string(),
    ))
    .await
    .unwrap();
    let (code, details) = w.recv_error().await;
    assert_eq!(code, -32602);
    assert!(
        details.contains("unknown method"),
        "details should mention unknown method, got: {details}"
    );
}

#[tokio::test]
async fn parity_fix_zero_price_rejected() {
    let _srv = Server::start();
    let mut c = FixCli::connect().await;
    c.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = c.recv().await;

    c.send(
        "D",
        &[
            ("11", "zp"),
            ("54", "1"),
            ("55", "AAPL"),
            ("38", "50"),
            ("40", "2"),
            ("44", "0.00"),
        ],
    )
    .await;
    let r = c.recv().await;

    let msg_type = get(&r, "35");
    assert!(
        msg_type == "3" || msg_type == "8",
        "expected session Reject (35=3) or ExecReport reject (35=8), got 35={msg_type}"
    );
    if msg_type == "8" {
        assert_eq!(get(&r, "150"), "8", "ExecType=Rejected");
        assert_eq!(get(&r, "39"), "8", "OrdStatus=Rejected");
    }
}



#[tokio::test]
async fn ws_empty_symbol_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;
    // Whitespace-only symbol triggers the get_or_create_book empty-symbol guard.
    w.order_limit("  ", "buy", 100.50, 10, "ws-empty", "CLIENT01")
        .await;
    let (code, details) = w.recv_error().await;
    assert_eq!(code, -32602);
    assert!(
        details.contains("empty"),
        "details should mention empty/missing symbol, got: {details:?}"
    );
}

#[tokio::test]
async fn fix_dynamic_session_multiplex() {
    let _srv = Server::start();

    // Spawn client 1
    let mut c1 = FixCli::connect_with_id("CLIENT01").await;
    c1.send("A", &[("98", "0"), ("108", "30")]).await;
    let r1_logon = c1.recv().await;
    assert_eq!(get(&r1_logon, "35"), "A");
    assert_eq!(get(&r1_logon, "49"), "XCANG3");
    assert_eq!(get(&r1_logon, "56"), "CLIENT01");

    // Spawn client 2
    let mut c2 = FixCli::connect_with_id("CLIENT02").await;
    c2.send("A", &[("98", "0"), ("108", "30")]).await;
    let r2_logon = c2.recv().await;
    assert_eq!(get(&r2_logon, "35"), "A");
    assert_eq!(get(&r2_logon, "49"), "XCANG3");
    assert_eq!(get(&r2_logon, "56"), "CLIENT02");

    // Client 1 places limit Buy
    c1.send(
        "D",
        &[
            ("11", "c1-order"),
            ("54", "1"), // Buy
            ("55", "AAPL"),
            ("38", "10"),
            ("40", "2"), // Limit
            ("44", "100.00"),
        ],
    )
    .await;

    let r1_added = c1.recv().await;
    assert_eq!(get(&r1_added, "35"), "8"); // ExecReport
    assert_eq!(get(&r1_added, "49"), "XCANG3");
    assert_eq!(get(&r1_added, "56"), "CLIENT01");
    assert_eq!(get(&r1_added, "150"), "0"); // ExecType=New
    assert_eq!(get(&r1_added, "39"), "0"); // OrdStatus=New

    // Client 2 places matching Limit Sell
    c2.send(
        "D",
        &[
            ("11", "c2-order"),
            ("54", "2"), // Sell
            ("55", "AAPL"),
            ("38", "10"),
            ("40", "2"), // Limit
            ("44", "100.00"),
        ],
    )
    .await;

    // Client 2 gets sync fill (taker)
    let r2_fill = c2.recv().await;
    assert_eq!(get(&r2_fill, "35"), "8");
    assert_eq!(get(&r2_fill, "49"), "XCANG3");
    assert_eq!(get(&r2_fill, "56"), "CLIENT02");
    assert_eq!(get(&r2_fill, "150"), "2"); // ExecType=Fill
    assert_eq!(get(&r2_fill, "39"), "2"); // OrdStatus=Fill
    assert_eq!(get(&r2_fill, "38"), "10");
    assert_eq!(get(&r2_fill, "14"), "10"); // CumQty

    // Client 1 gets async fill (maker)
    let r1_fill = c1.recv().await;
    assert_eq!(get(&r1_fill, "35"), "8");
    assert_eq!(get(&r1_fill, "49"), "XCANG3");
    assert_eq!(get(&r1_fill, "56"), "CLIENT01");
    assert_eq!(get(&r1_fill, "150"), "2"); // ExecType=Fill
    assert_eq!(get(&r1_fill, "39"), "2"); // OrdStatus=Fill
    assert_eq!(get(&r1_fill, "38"), "10");
    assert_eq!(get(&r1_fill, "14"), "10"); // CumQty
}

