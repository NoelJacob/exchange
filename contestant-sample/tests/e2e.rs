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
}

impl FixCli {
    async fn connect() -> Self {
        let addr = format!("127.0.0.1:{FIX_PORT}");
        let stream =
            tokio::time::timeout(Duration::from_secs(10), tokio::net::TcpStream::connect(&addr))
                .await
                .expect("FIX connect timeout")
                .expect("FIX connect failed");
        Self { stream, seq: 1 }
    }

    fn msg(&mut self, msg_type: &str, tags: &[(&str, &str)]) -> Vec<u8> {
        let ts = utc_now();
        let seq = self.seq;
        self.seq += 1;
        let mut body = format!(
            "35={msg_type}\x0134={seq}\x0149=CLIENT\x0156=SERVER\x0152={ts}\x01"
        );
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
                // Debug: print what we have
                if buf.len() <= 100 || buf.len() % 20 == 0 {
                    let clean = String::from_utf8_lossy(&buf).replace('\x01', "|");
                    eprintln!("[FIX TEST] buf({}) = {clean}", buf.len());
                }
                let len = buf.len();
                if len >= 6
                    && buf[len - 3] == b'='
                    && &buf[len - 5..len - 3] == b"10"
                {
                    // Strip trailing SOH if present, then parse
                    let end = if buf[len - 1] == b'\x01' { len - 1 } else { len };
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

type WsStr = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

struct WsCli {
    tx: futures_util::stream::SplitSink<WsStr, tokio_tungstenite::tungstenite::Message>,
    rx: futures_util::stream::SplitStream<WsStr>,
}

impl WsCli {
    async fn connect() -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{WS_PORT}"))
            .await
            .expect("WS connect");
        let (tx, rx) = ws.split();
        Self { tx, rx }
    }

    async fn order(&mut self, symbol: &str, side: u8, price: f64, qty: u32, ord_type: u8) {
        use futures_util::SinkExt;
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let payload = serde_json::json!({
            "order_id": "ws-ord",
            "contestant_id": "test",
            "side": side,
            "price": price,
            "qty": qty,
            "ord_type": ord_type,
            "symbol": symbol,
            "ts_sent_us": now_us,
            "bot_id": "bot",
        });
        self.tx
            .send(tokio_tungstenite::tungstenite::Message::Text(
                payload.to_string().into(),
            ))
            .await
            .unwrap();
    }

    async fn exec(&mut self) -> serde_json::Value {
        let msg = self.rx.next().await.unwrap().unwrap();
        match msg {
            tokio_tungstenite::tungstenite::Message::Text(t) => {
                serde_json::from_str(&t).unwrap()
            }
            other => panic!("expected text, got {other:?}"),
        }
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
    assert_eq!(get(&r, "49"), "SERVER", "SenderCompID");
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
        &[("11", "o2"), ("54", "1"), ("55", "AAPL"), ("38", "30"), ("40", "1")],
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

    w.order("AAPL", 1, 100.50, 50, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "new", "should rest");

    w.order("AAPL", 2, 100.50, 20, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "fill", "should fill");
    assert_eq!(r["fill_qty"], 20, "fill_qty=20");
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

    ws.order("AAPL", 2, 100.50, 40, 2).await;
    let r = ws.exec().await;
    assert_eq!(r["exec_type"], "fill", "WS fills FIX rest");
    assert_eq!(r["fill_qty"], 40, "fill_qty=40");

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
    assert_eq!(get(&r, "150"), "0", "ExecType=New — MSFT should NOT match AAPL");
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
}

#[tokio::test]
async fn ws_multi_symbol() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;

    w.order("AAPL", 1, 100.50, 50, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "new", "AAPL buy rests");

    // Buy MSFT at a different price — proves it creates a separate book
    w.order("MSFT", 1, 99.50, 30, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "new", "MSFT buy rests (separate book)");

    // Sell MSFT at 99.50 — fills against MSFT buy, proves AAPL book untouched
    w.order("MSFT", 2, 99.50, 30, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "fill", "MSFT sell fills");
    assert_eq!(r["fill_qty"], 30, "fill_qty=30");
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
        &[("54", "1"), ("55", "AAPL"), ("38", "100"), ("40", "2"), ("44", "100.50")],
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
        assert_eq!(get(&r, "39"), "8", "OrdStatus=Rejected");
    }
}

#[tokio::test]
async fn ws_submit_error_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;

    // Market buy on an empty book: the helper returns SubmitError
    // InsufficientLiquidity and the engine sends a Reject ExecutionMessage
    // with the reason populated.
    w.order("WS_EMPTY", 1, 0.0, 50, 1).await;
    let r = w.exec().await;
    assert_eq!(
        r["exec_type"], "rejected",
        "market buy on empty book should be rejected"
    );
    assert_eq!(r["fill_qty"], 0, "fill_qty=0 on reject");
    assert_eq!(r["fill_price"], 0.0, "fill_price=0.0 on reject");
    let reason = r["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("insufficient liquidity") || reason.contains("non-positive"),
        "reason should mention insufficient liquidity or non-positive price, got: {reason}"
    );
}

#[tokio::test]
async fn ws_invalid_side_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;

    // side=3 is invalid; the engine must send a rejected ExecutionMessage
    // rather than silently defaulting to Buy.
    w.order("AAPL", 3, 100.50, 50, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "rejected", "side=3 should be rejected");
    assert_eq!(r["fill_qty"], 0, "fill_qty=0 on reject");
    assert_eq!(r["fill_price"], 0.0, "fill_price=0.0 on reject");
    let reason = r["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("invalid side"),
        "reason should mention invalid side, got: {reason}"
    );
}

#[tokio::test]
async fn parity_fix_rest_fill_ws() {
    let _srv = Server::start();
    let mut fix = FixCli::connect().await;
    let mut ws = WsCli::connect().await;

    fix.send("A", &[("98", "0"), ("108", "30")]).await;
    let _ = fix.recv().await;

    // FIX rests Buy 100 AAPL @ 100.50.
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

    // WS Sells 40 AAPL @ 100.50 against the FIX-resting buy.
    ws.order("AAPL", 2, 100.50, 40, 2).await;
    let r = ws.exec().await;
    assert_eq!(r["exec_type"], "fill", "WS fills FIX rest");
    assert_eq!(r["fill_qty"], 40, "fill_qty=40");
}

#[tokio::test]
async fn parity_ws_rest_fill_fix() {
    let _srv = Server::start();
    let mut fix = FixCli::connect().await;
    let mut ws = WsCli::connect().await;

    // WS rests Buy 100 MSFT @ 100.50.
    ws.order("MSFT", 1, 100.50, 100, 2).await;
    let _ = ws.exec().await;

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
}

#[tokio::test]
async fn parity_ws_zero_price_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;

    // limit order with price 0.0 is non-positive and must be rejected.
    w.order("AAPL", 1, 0.0, 50, 2).await;
    let r = w.exec().await;
    assert_eq!(r["exec_type"], "rejected", "price 0.0 should be rejected");
    let reason = r["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("non-positive"),
        "reason should mention non-positive, got: {reason}"
    );
}

#[tokio::test]
async fn parity_ws_negative_price_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;

    w.order("AAPL", 1, -1.0, 50, 2).await;
    let r = w.exec().await;
    assert_eq!(
        r["exec_type"], "rejected",
        "negative price should be rejected"
    );
    let reason = r["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("non-positive"),
        "reason should mention non-positive, got: {reason}"
    );
}

#[tokio::test]
async fn parity_ws_invalid_ord_type_rejected() {
    let _srv = Server::start();
    let mut w = WsCli::connect().await;

    w.order("AAPL", 1, 100.50, 50, 99).await;
    let r = w.exec().await;
    assert_eq!(
        r["exec_type"], "rejected",
        "ord_type=99 should be rejected"
    );
    let reason = r["reason"].as_str().unwrap_or("");
    assert!(
        reason.contains("invalid ord_type"),
        "reason should mention invalid ord_type, got: {reason}"
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