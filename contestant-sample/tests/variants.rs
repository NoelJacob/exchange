//! Integration tests for contestant-sample binary variants.
//!
//! Verifies each feature-gated variant behaves as intended:
//! - default (correct): normal fast matching
//! - prefilled: pre-inserted stale orders on BENCH
//! - panic_10s: process crashes after 10s
//! - slow_submit: each order submission takes ~100ms
//!
//! Run:
//!   cargo test --test variants -- --test-threads=1
//!
//! Prerequisite: all variants must be built first:
//!   scripts/build-local.sh   (or the manual build steps in the plan)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── Persistent FIX session ────────────────────────────────────────────

struct FixSession {
    stream: TcpStream,
    seq: u64,
}

impl FixSession {
    fn connect(host: &str, port: u16) -> Self {
        let mut stream = TcpStream::connect(format!("{host}:{port}"))
            .expect("connect to FIX port");
        stream.set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set_read_timeout");

        let logon = build_fix("35=A|34=1|49=CLIENT|52=D|56=SERVER|98=0|108=30|");
        stream.write_all(&logon).expect("write logon");

        let resp = read_all(&mut stream);
        let tags = parse_fix_tags(&resp);
        assert_eq!(tags.get("35").map(String::as_str), Some("A"),
            "Logon failed, got: {tags:?}");
        eprintln!("  [FIX] Logon accepted");
        FixSession { stream, seq: 2 }
    }

    fn send_order(&mut self, cl_ord_id: &str, symbol: &str, side: &str,
                  qty: u64, price: f64, is_market: bool) -> std::collections::HashMap<String, String> {
        let ord_type = if is_market { "1" } else { "2" };
        let seq_s = self.seq.to_string();
        self.seq += 1;
        let body = format!(
            "35=D|34={seq_s}|49=CLIENT|52=D|56=SERVER|11={cl_ord_id}|55={symbol}|54={side}|38={qty}|40={ord_type}|44={price}|"
        );
        self.stream.write_all(&build_fix(&body)).expect("write order");
        let resp = read_all(&mut self.stream);
        parse_fix_tags(&resp)
    }
}

fn build_fix(body_no_pipes: &str) -> Vec<u8> {
    let body = body_no_pipes.replace('|', "\x01");
    let len = body.len();
    let mut msg = format!("8=FIX.4.2\x019={len}\x01{body}");
    let cksum: u8 = msg.bytes().fold(0u8, |acc, b| acc.wrapping_add(b));
    msg.push_str(&format!("10={cksum:03}\x01"));
    msg.into_bytes()
}

fn read_all(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 16384];
    match stream.read(&mut buf) {
        Ok(0) => vec![],
        Ok(n) => { buf.truncate(n); buf }
        Err(e) => { eprintln!("  [FIX] Read error: {e}"); vec![] }
    }
}

fn parse_fix_tags(data: &[u8]) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for part in String::from_utf8_lossy(data).split('\x01') {
        if let Some((tag, value)) = part.split_once('=') {
            map.insert(tag.to_string(), value.to_string());
        }
    }
    map
}

fn get(map: &std::collections::HashMap<String, String>, tag: &str) -> String {
    map.get(tag).cloned().unwrap_or_default()
}

// ── Server lifecycle ──────────────────────────────────────────────────

struct Server(Child);

impl Server {
    fn spawn(bin: &str, name: &str) -> Self {
        eprintln!("[TEST] Spawning {name} ({bin})...");
        let child = Command::new(bin)
            .stdout(Stdio::null()).stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {name}: {e}"));
        eprintln!("[TEST] {name} pid={}, waiting for FIX port...", child.id());
        for i in 0..20 {
            if TcpStream::connect_timeout(
                &"127.0.0.1:9090".parse().unwrap(), Duration::from_secs(1)
            ).is_ok() {
                eprintln!("  port 9090 ready after ~{}ms", (i + 1) * 500);
                return Server(child);
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        // Port 9090 never opened — binary likely crashed because port 8080
        // (WS server) is occupied. Check and report.
        if TcpStream::connect_timeout(
            &"127.0.0.1:8080".parse().unwrap(), Duration::from_millis(100)
        ).is_ok() {
            panic!("Port 8080 is already in use — the contestant binary's WS server cannot bind. \
                    Stop the conflicting service (e.g. `docker compose -f infra/docker-compose.yml down`) \
                    and retry.");
        }
        panic!("Timed out waiting for {name} on port 9090 — binary may have crashed.");
}
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        for _ in 0..30 {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => return,
            }
        }
    }
}

// ── Binary paths ──────────────────────────────────────────────────────

const BIN_CORRECT: &str = env!("CARGO_BIN_EXE_contestant-sample");
const BIN_PREFILLED: &str = "/tmp/contestant-sample-wrong";
const BIN_PANIC: &str = "/tmp/contestant-sample-panic";
const BIN_SLOW: &str = "/tmp/contestant-sample-slow";
const BIN_RANDOM: &str = "/tmp/contestant-sample-random";

// ── Tests ─────────────────────────────────────────────────────────────

#[test]
fn variant_correct_normal_behavior() {
    let _server = Server::spawn(BIN_CORRECT, "correct");
    let mut fix = FixSession::connect("127.0.0.1", 9090);

    // Buy 100 @ 100.50 — rests (no sell side)
    let r1 = fix.send_order("CORR-1", "TEST", "1", 100, 100.50, false);
    assert_eq!(get(&r1, "150"), "0", "expected New(0)");
    eprintln!("  Buy CORR-1: New (resting)");

    // Sell 100 @ 100.50 — fills against resting buy
    let r2 = fix.send_order("CORR-2", "TEST", "2", 100, 100.50, false);
    let exec = get(&r2, "150");
    assert!(exec == "2" || exec == "1", "expected Fill(2)/Partial(1), got {exec}");
    let shares: u64 = get(&r2, "32").parse().unwrap_or(0);
    assert!(shares > 0, "expected last_shares > 0");
    eprintln!("  Sell CORR-2: filled {shares}");
    eprintln!("PASS: correct — normal matching works");
}

#[test]
fn variant_prefilled_has_stale_liquidity() {
    let _server = Server::spawn(BIN_PREFILLED, "prefilled");
    let mut fix = FixSession::connect("127.0.0.1", 9090);

    // BENCH has prefilled buys at 97.00-99.25. Market sell fills ~97.00.
    let r1 = fix.send_order("PREF-1", "BENCH", "2", 10, 0.0, true);
    let exec = get(&r1, "150");
    assert!(exec == "2" || exec == "1",
        "expected Fill/Partial, got {exec}");
    let px: f64 = get(&r1, "31").parse().unwrap_or(0.0);
    assert!(px > 90.0 && px < 105.0, "fill price {px} unexpected");
    eprintln!("  Market sell filled at {px}");
    eprintln!("PASS: prefilled — stale liquidity present");
}

#[test]
fn variant_panic_crashes_after_10_seconds() {
    let mut server = Server::spawn(BIN_PANIC, "panic_10s");
    eprintln!("  Waiting 12s for panic timer...");
    std::thread::sleep(Duration::from_secs(12));

    match server.0.try_wait() {
        Ok(Some(st)) => {
            assert!(!st.success(), "expected non-zero exit");
            eprintln!("  Exited with {st}");
        }
        Ok(None) => { server.0.kill().ok(); panic!("still running after 12s"); }
        Err(e) => panic!("check failed: {e}"),
    }
    eprintln!("PASS: panic — crashed after 10s");
}

#[test]
fn variant_slow_submit_adds_100ms_delay() {
    let _server = Server::spawn(BIN_SLOW, "slow_submit");
    let mut fix = FixSession::connect("127.0.0.1", 9090);

    let t1 = Instant::now();
    let r1 = fix.send_order("SLOW-1", "TEST", "1", 100, 100.50, false);
    let e1 = t1.elapsed();
    assert_eq!(get(&r1, "150"), "0", "expected New(0)");
    eprintln!("  Order 1 latency: {e1:?}");
    assert!(e1 >= Duration::from_millis(80), "expected ≥80ms, got {e1:?}");

    let t2 = Instant::now();
    let r2 = fix.send_order("SLOW-2", "TEST", "2", 50, 100.50, false);
    let e2 = t2.elapsed();
    let exec = get(&r2, "150");
    assert!(exec == "2" || exec == "1", "expected Fill/Partial, got {exec}");
    eprintln!("  Order 2 latency: {e2:?}");
    assert!(e2 >= Duration::from_millis(80), "expected ≥80ms, got {e2:?}");

    eprintln!("PASS: slow — both orders had ≥80ms latency");
}

#[test]
fn variant_randomize_price_modifies_price() {
    // randomize_price adjusts limit_price_cents by ±10% before submitting
    // to the orderbook. Tag 44 (Price) echoes the nominal price, but the
    // fill price (tag 6 AvgPx, tag 31 LastPx) reflects the internal
    // randomized value. A market sell against a resting limit buy fills
    // at the randomized price, NEVER at the nominal price.
    let _server = Server::spawn(BIN_RANDOM, "randomize_price");
    let mut fix = FixSession::connect("127.0.0.1", 9090);

    // Limit buy 10 @ 100 — internally price becomes 90 or 110, rests on BID
    let r1 = fix.send_order("RAND-1", "TEST", "1", 10, 100.0, false);
    assert_eq!(get(&r1, "150"), "0", "expected New(0) for buy");
    // Tag 44 echoes the nominal price, NOT the randomized one
    assert_eq!(get(&r1, "44"), "100.00", "buy tag 44 should show nominal price");
    eprintln!("  Limit buy @ 100 -> New (resting)");

    // Market sell 10 — fills against the resting limit buy at the
    // INTERNAL randomized price (90 or 110), never at 100.
    let r2 = fix.send_order("RAND-S", "TEST", "2", 10, 0.0, true);
    let exec = get(&r2, "150");
    assert!(exec == "1" || exec == "2",
        "expected Fill(1) or Partial(2), got {exec}");
    // Market orders have no tag 44 (Price is only for limit orders)
    assert_eq!(get(&r2, "44"), "", "market order should not have tag 44");

    // Tag 6 (AvgPx) = actual fill price from matching engine.
    // With randomize_price this is 90.00 or 110.00 — NEVER 100.00.
    let avg_px: f64 = get(&r2, "6").parse().expect("missing AvgPx (tag 6)");
    assert!((avg_px - 90.0).abs() < f64::EPSILON || (avg_px - 110.0).abs() < f64::EPSILON,
        "fill price {avg_px} not 90 or 110 — randomization may be broken");
    eprintln!("  Market sell -> fill at {avg_px} (randomized ±10% from 100)");

    eprintln!("PASS: randomize_price — internal price randomization verified via fill price");
}
