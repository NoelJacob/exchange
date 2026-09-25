#![deny(warnings)]

use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::env;
use tokio_tungstenite::connect_async;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;

fn parse_args() -> (u64, u64, u16, Option<u64>) {
    let args: Vec<String> = env::args().collect();
    let mut start = 1000;
    let mut step = 1000;
    let mut port = 8080;
    let mut per_conn: Option<u64> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--start" => { if i + 1 < args.len() { start = args[i + 1].parse().unwrap_or(1000); i += 2; } else { i += 1; } }
            "--step" => { if i + 1 < args.len() { step = args[i + 1].parse().unwrap_or(1000); i += 2; } else { i += 1; } }
            "--port" => { if i + 1 < args.len() { port = args[i + 1].parse().unwrap_or(8080); i += 2; } else { i += 1; } }
            "--per-conn" => { if i + 1 < args.len() { per_conn = args[i + 1].parse().ok(); i += 2; } else { i += 1; } }
            _ => { i += 1; }
        }
    }
    (start, step, port, per_conn)
}

fn build_market_order(id: u64, cl_ord_id: &str) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "method": "order.create.market",
        "params": {
            "sender_id": "CLIENT01",
            "target_id": "XCANG3",
            "sending_time": SENDING_TIME,
            "cl_ord_id": cl_ord_id,
            "symbol": "STRESS",
            "side": "buy",
            "qty": 1
        },
        "id": id
    })
}

struct Connection {
    write: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
        tokio_tungstenite::tungstenite::Message
    >,
    next_req_id: u64,
}

impl Connection {
    async fn connect(
        port: u16,
        received: Arc<AtomicU64>,
        crashed: Arc<AtomicBool>,
        cause: Arc<AtomicU8>,
        disconnect_tx: tokio::sync::watch::Sender<bool>,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("ws://127.0.0.1:{}/", port);
        let (ws_stream, _) = connect_async(&url).await?;
        let (write, mut read) = ws_stream.split();
        let handle = tokio::spawn(async move {
            use tokio_tungstenite::tungstenite::Message;
            let signal = |why: &str| {
                eprintln!("[reader] disconnect ({why})");
                crashed.store(true, Ordering::Relaxed);
                cause.store(CAUSE_DISCONNECTED, Ordering::Relaxed);
                let _ = disconnect_tx.send(true);
            };
            loop {
                match read.next().await {
                    Some(Ok(Message::Text(_))) => {
                        received.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(Ok(Message::Close(_))) => {
                        signal("server Close frame");
                        break;
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Err(e)) => {
                        signal(&format!("read error: {e}"));
                        break;
                    }
                    None => {
                        signal("stream ended");
                        break;
                    }
                }
            }
        });
        Ok((Self { write, next_req_id: 1 }, handle))
    }

    async fn send_order(&mut self, cl_ord_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let order = build_market_order(self.next_req_id, cl_ord_id);
        self.write
            .send(tokio_tungstenite::tungstenite::Message::Text(order.to_string()))
            .await?;
        self.next_req_id += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhaseType {
    Ramp,
    Sustain,
}

const SENDING_TIME: &str = "2026-06-04T14:33:05.000Z";

/// Absolute time (seconds from phase start) at which the n-th send (1-based)
/// should fire so cumulative sends follow the rate integral:
/// Sustain: S(t) = from*t → t = n/from.
/// Ramp: S(t) = from*t + a*t²/2 with a = (to-from)/1s, inverted by quadratic formula.
fn send_time_secs(phase: PhaseType, from: u64, to: u64, n: u64) -> f64 {
    match phase {
        PhaseType::Sustain => n as f64 / from as f64,
        PhaseType::Ramp => {
            if to == from {
                return n as f64 / from as f64;
            }
            let a = (to as f64 - from as f64) / 1.0;
            let from_f = from as f64;
            (-from_f + (from_f * from_f + 2.0 * a * n as f64).sqrt()) / a
        }
    }
}

/// Sustain counts only if measured rate is within 5% of target (u128: no overflow).
fn meets_sustain(avg: u64, target: u64) -> bool {
    (avg as u128) * 100 >= (target as u128) * 95
}
/// Wall-clock throughput: aggregate in-window sends divided by ACTUAL elapsed
/// seconds (START barrier → last COMPLETE), never the nominal 1s. Under
/// backpressure the window stretches and the reported rate honestly drops.
/// Used by `hold_aggregate` consumers for gating and FINAL TPS.
fn wall_clock_tps(sent: u64, wall_secs: f64) -> u64 {
    (sent as f64 / wall_secs.max(0.001)) as u64
}

/// Accepted sustained rate is the MEASURED average, never the target.
/// A 950/s measurement against a 1000/s target records 950.
fn accept_sustain(avg: u64, target: u64) -> Option<u64> {
    meets_sustain(avg, target).then_some(avg)
}

/// Running maximum over accepted phases: never decreases.
/// Accepted 2000 then 1950 keeps 2000.
fn track_highest(current: u64, measured: u64) -> u64 {
    current.max(measured)
}

/// Settle one completed hold window: wall-clock rate → gate → running max.
/// Returns (avg, new_final, passed). BOTH `run_scale` consumers call this —
/// it is the single place where a hold becomes FINAL TPS, so a regression
/// to nominal accounting here fails every consumer at once.
fn settle_hold(sent: u64, wall_secs: f64, target: u64, final_tps: u64) -> (u64, u64, bool) {
    let avg = wall_clock_tps(sent, wall_secs);
    match accept_sustain(avg, target) {
        Some(measured) => (avg, track_highest(final_tps, measured), true),
        None => (avg, final_tps, false),
    }
}

fn validate_args(start: u64, step: u64) -> Result<(), &'static str> {
    if start < 1 || step < 1 {
        return Err("--start and --step must be >= 1");
    }
    Ok(())
}

/// Scale-mode arg validation: per-conn is the ceiling, so start and step
/// must each fit inside one connection.
fn validate_scale_args(start: u64, step: u64, per_conn: u64) -> Result<(), &'static str> {
    if per_conn < 1 || start < 1 || step < 1 {
        return Err("--start, --step and --per-conn must be >= 1");
    }
    if start > per_conn || step > per_conn {
        return Err("--start and --step must each be <= --per-conn");
    }
    Ok(())
}

/// Per-worker fill decision for the next cycle step. The coordinator calls
/// this and matches the result — it is the shipped decision logic, not a
/// test-only model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FillAction {
    /// Ramp worker `idx` from→to (it has headroom); others sustain.
    Ramp { idx: usize, from: u64, to: u64 },
    /// All workers full; spawn a new one and ramp it 0→delta.
    Spawn { delta: u64 },
}

/// Decide the next cycle from per-worker frozen rates: newest worker with
/// `f + step <= per_conn` ramps; otherwise spawn. Saturating, never panics.
/// Spawn delta is always exactly `step` (validated `<= per_conn` at startup),
/// so no total arithmetic is needed here.
fn fill_action(frozen: &[u64], step: u64, per_conn: u64) -> FillAction {
    if let Some(idx) = frozen.iter().rposition(|f| f.saturating_add(step) <= per_conn) {
        FillAction::Ramp { idx, from: frozen[idx], to: frozen[idx].saturating_add(step) }
    } else {
        FillAction::Spawn { delta: step }
    }
}
/// Lazy infinite alternating phase stream for production use.
/// Index 0 is (Ramp, 0, start); odd indices are Sustain, even are Ramp.
struct PhaseIter {
    start: u64,
    step: u64,
    idx: u64,
}

fn phase_iter(start: u64, step: u64) -> PhaseIter {
    PhaseIter { start, step, idx: 0 }
}

impl Iterator for PhaseIter {
    type Item = (PhaseType, u64, u64);
    fn next(&mut self) -> Option<Self::Item> {
        use PhaseType::{Ramp, Sustain};
        let i = self.idx;
        self.idx = self.idx.saturating_add(1);
        if i == 0 {
            return Some((Ramp, 0, self.start));
        }
        let c = (i - 1) / 2;
        let sustain = self.start.saturating_add(c.saturating_mul(self.step));
        if i % 2 == 1 {
            Some((Sustain, sustain, sustain))
        } else {
            Some((Ramp, sustain, sustain.saturating_add(self.step)))
        }
    }
}

/// Summed average rate over per-connection (sent, elapsed) samples.
/// Test helper pinning the coordinator's summation semantics.
#[cfg(test)]
fn aggregate_avgs(samples: &[(u64, f64)], _secs: f64) -> u64 {
    samples.iter().map(|(n, _)| *n).sum()
}

/// Run a single phase (ramp or sustain) for exactly 1 second.
/// Returns (success, sent_this_phase, recv_this_phase, avg_rate).
async fn run_phase(
    conn: &mut Connection,
    phase_type: PhaseType,
    from_rate: u64,
    to_rate: u64,
    sent: &Arc<AtomicU64>,
    received: &Arc<AtomicU64>,
    crashed: &Arc<AtomicBool>,
    disconnect_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> (bool, u64, u64, u64, bool) {
    let phase_start = Instant::now();
    let phase_duration = Duration::from_secs(1);
    let recv_start = received.load(Ordering::Relaxed);
    let mut sends_this_phase = 0u64;

    loop {
        if crashed.load(Ordering::Relaxed) || *disconnect_rx.borrow() {
            crashed.store(true, Ordering::Relaxed);
            let duration = phase_start.elapsed().as_secs_f64().max(0.001);
            return (false, sends_this_phase, received.load(Ordering::Relaxed) - recv_start, (sends_this_phase as f64 / duration) as u64, false);
        }

        // Absolute fire time of the next send on the integral schedule.
        let t_next = send_time_secs(phase_type, from_rate, to_rate, sends_this_phase + 1);
        let fire_at = phase_start + Duration::from_secs_f64(t_next);
        if fire_at >= phase_start + phase_duration {
            break;
        }
        // Schedule sleep is itself cancellable: a disconnect during the
        // wait must not idle until the next fire time.
        let delay = fire_at.saturating_duration_since(Instant::now());
        if delay > Duration::ZERO {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = disconnect_rx.wait_for(|down| *down) => {
                    crashed.store(true, Ordering::Relaxed);
                    let duration = phase_start.elapsed().as_secs_f64().max(0.001);
                    return (false, sends_this_phase, received.load(Ordering::Relaxed) - recv_start, (sends_this_phase as f64 / duration) as u64, false);
                }
            }
        }
        if crashed.load(Ordering::Relaxed) || *disconnect_rx.borrow() {
            crashed.store(true, Ordering::Relaxed);
            let duration = phase_start.elapsed().as_secs_f64().max(0.001);
            return (false, sends_this_phase, received.load(Ordering::Relaxed) - recv_start, (sends_this_phase as f64 / duration) as u64, false);
        }

        // The send future is dropped the instant disconnect fires, so a
        // wedged writer cannot hold the phase open until the watchdog.
        let cl_ord_id = format!("ws-{:06}", sent.load(Ordering::Relaxed) + 1);
        tokio::select! {
            res = conn.send_order(&cl_ord_id) => {
                if res.is_err() {
                    crashed.store(true, Ordering::Relaxed);
                    let duration = phase_start.elapsed().as_secs_f64().max(0.001);
                    return (false, sends_this_phase, received.load(Ordering::Relaxed) - recv_start, (sends_this_phase as f64 / duration) as u64, false);
                }
            }
            _ = disconnect_rx.wait_for(|down| *down) => {
                crashed.store(true, Ordering::Relaxed);
                let duration = phase_start.elapsed().as_secs_f64().max(0.001);
                return (false, sends_this_phase, received.load(Ordering::Relaxed) - recv_start, (sends_this_phase as f64 / duration) as u64, false);
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                eprintln!("STOPPED: send blocked >5s (backpressure; cause unverified)");
                crashed.store(true, Ordering::Relaxed);
                let duration = phase_start.elapsed().as_secs_f64().max(0.001);
                return (false, sends_this_phase, received.load(Ordering::Relaxed) - recv_start, (sends_this_phase as f64 / duration) as u64, true);
            }
        }
        sent.fetch_add(1, Ordering::Relaxed);
        sends_this_phase += 1;
    }

    let actual_duration = phase_start.elapsed().as_secs_f64().max(0.001);
    let recv_this_phase = received.load(Ordering::Relaxed) - recv_start;
    let avg_rate = (sends_this_phase as f64 / actual_duration) as u64;
    (true, sends_this_phase, recv_this_phase, avg_rate, false)
}

/// Worker command: what rate profile to run until the next command.
/// `id` matches START/COMPLETE acks so the coordinator never attributes
/// a stale ack to the current window.
#[derive(Debug, Clone, Copy)]
enum Cmd {
    Ramp { id: u64, from: u64, to: u64 },
    Sustain { id: u64, rate: u64 },
    Stop,
}

/// Per-window acknowledgement payload.
#[derive(Debug, Clone, Copy)]
struct WindowAck {
    id: u64,
    started: bool,
    sent_at_edge: u64,
    recv_at_edge: u64,
}

/// Per-worker stop cause: 0 = none, 1 = disconnected (reader fired / send Err),
/// 2 = send watchdog (blocked writer, cause unverified), 3 = coordinator ack timeout.
const CAUSE_NONE: u8 = 0;
const CAUSE_DISCONNECTED: u8 = 1;
const CAUSE_WATCHDOG: u8 = 2;
const CAUSE_COORD_TIMEOUT: u8 = 3;

/// Coordinator-side handle: atomics + channels only, never the Connection.
struct WorkerHandle {
    cmd_tx: tokio::sync::mpsc::Sender<Cmd>,
    /// Worker sends WindowAck when it STARTS each window (with the send
    /// count at start) and when it COMPLETES it (with the send count at end).
    /// Coordinator derives the aggregate strictly from in-window deltas:
    /// no barrier skew, no bleed. Counters live only in the worker task.
    ack_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<WindowAck>>,
    crashed: Arc<AtomicBool>,
    cause: Arc<AtomicU8>,
    join: tokio::task::JoinHandle<()>,
    reader: tokio::task::JoinHandle<()>,
}

/// Wait for the START edge of window `id` from every worker.
/// Returns per-worker (sent, recv) snapshots at window start.
async fn await_starts(workers: &[WorkerHandle], id: u64, what: &str) -> Option<Vec<(u64, u64)>> {
    let mut out = Vec::with_capacity(workers.len());
    for w in workers {
        let mut rx = w.ack_rx.lock().await;
        let ack = tokio::select! {
            r = rx.recv() => r,
            _ = tokio::time::sleep(Duration::from_secs(5)) => None,
        };
        match ack {
            Some(a) if a.started && a.id == id => out.push((a.sent_at_edge, a.recv_at_edge)),
            _ => {
                eprintln!("ACK TIMEOUT/MISMATCH waiting for {what} (want id={id})");
                w.cause.store(CAUSE_COORD_TIMEOUT, Ordering::Relaxed);
                return None;
            }
        }
        if w.crashed.load(Ordering::Relaxed) {
            return None;
        }
    }
    Some(out)
}

/// Wait for the COMPLETE edge of window `id` from every worker.
/// Returns per-worker (sent_delta, recv_delta) strictly inside the window,
/// in worker order. Callers divide Σ(sent deltas) by the measured wall-clock
/// window via `wall_clock_tps`, never by a nominal 1s.
async fn await_completes(workers: &[WorkerHandle], id: u64, starts: &[(u64, u64)], what: &str) -> Option<Vec<(u64, u64)>> {
    let mut out = Vec::with_capacity(workers.len());
    for (w, (s0, r0)) in workers.iter().zip(starts.iter()) {
        let mut rx = w.ack_rx.lock().await;
        let ack = tokio::select! {
            r = rx.recv() => r,
            _ = tokio::time::sleep(Duration::from_secs(5)) => None,
        };
        match ack {
            Some(a) if !a.started && a.id == id => {
                out.push((a.sent_at_edge.saturating_sub(*s0), a.recv_at_edge.saturating_sub(*r0)));
            }
            _ => {
                eprintln!("ACK TIMEOUT/MISMATCH waiting for {what} (want id={id})");
                w.cause.store(CAUSE_COORD_TIMEOUT, Ordering::Relaxed);
                return None;
            }
        }
        if w.crashed.load(Ordering::Relaxed) {
            return None;
        }
    }
    Some(out)
}

/// Abort every worker and reader.
async fn stop_all(workers: &[WorkerHandle]) {
    for w in workers {
        let _ = w.cmd_tx.send(Cmd::Stop).await;
        w.join.abort();
        w.reader.abort();
    }
}

/// Spawn one connection worker. The task owns `conn` for its whole life;
/// the coordinator only sends Cmds and reads atomics.
async fn spawn_worker(
    idx: u64,
    port: u16,
) -> Result<WorkerHandle, Box<dyn std::error::Error + Send + Sync>> {
    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let crashed = Arc::new(AtomicBool::new(false));
    let cause = Arc::new(AtomicU8::new(CAUSE_NONE));
    let (disconnect_tx, disconnect_rx) = tokio::sync::watch::channel(false);
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<Cmd>(8);
    let (ack_tx, ack_rx) = tokio::sync::mpsc::channel::<WindowAck>(16);
    let (conn, reader) =
        Connection::connect(port, Arc::clone(&received), Arc::clone(&crashed), Arc::clone(&cause), disconnect_tx).await?;
    let mut cmd_rx2 = cmd_rx;
    let ack_tx2 = ack_tx.clone();
    let s2 = Arc::clone(&sent);
    let r2 = Arc::clone(&received);
    let c2 = Arc::clone(&crashed);
    let cause2 = Arc::clone(&cause);
    let mut rx2 = disconnect_rx.clone();
    let mut conn2 = conn;
    let join = tokio::spawn(async move {
        let mut local_n = 0u64;
        loop {
            let cmd = tokio::select! {
                c = cmd_rx2.recv() => c,
                _ = rx2.wait_for(|d| *d) => None,
            };
            let cmd = match cmd {
                Some(c) => c,
                None => break,
            };
            match cmd {
                Cmd::Stop => break,
                Cmd::Ramp { id, from, to } => {
                    let _ = ack_tx2.send(WindowAck { id, started: true, sent_at_edge: s2.load(Ordering::Relaxed), recv_at_edge: r2.load(Ordering::Relaxed) }).await;
                    run_worker_window(&mut conn2, PhaseType::Ramp, from, to, idx, &mut local_n, &s2, &r2, &c2, &cause2, &mut rx2).await;
                    let _ = ack_tx2.send(WindowAck { id, started: false, sent_at_edge: s2.load(Ordering::Relaxed), recv_at_edge: r2.load(Ordering::Relaxed) }).await;
                }
                Cmd::Sustain { id, rate } => {
                    let _ = ack_tx2.send(WindowAck { id, started: true, sent_at_edge: s2.load(Ordering::Relaxed), recv_at_edge: r2.load(Ordering::Relaxed) }).await;
                    run_worker_window(&mut conn2, PhaseType::Sustain, rate, rate, idx, &mut local_n, &s2, &r2, &c2, &cause2, &mut rx2).await;
                    let _ = ack_tx2.send(WindowAck { id, started: false, sent_at_edge: s2.load(Ordering::Relaxed), recv_at_edge: r2.load(Ordering::Relaxed) }).await;
                }
            }
            if c2.load(Ordering::Relaxed) {
                break;
            }
        }
    });
    Ok(WorkerHandle { cmd_tx, ack_rx: tokio::sync::Mutex::new(ack_rx), crashed, cause, join, reader })
}

/// One 1-second paced window inside a worker task. Returns false on disconnect/stall.
#[allow(clippy::too_many_arguments)]
async fn run_worker_window(
    conn: &mut Connection,
    phase: PhaseType,
    from: u64,
    to: u64,
    idx: u64,
    local_n: &mut u64,
    sent: &Arc<AtomicU64>,
    received: &Arc<AtomicU64>,
    crashed: &Arc<AtomicBool>,
    cause: &Arc<AtomicU8>,
    disconnect_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let phase_start = Instant::now();
    let phase_duration = Duration::from_secs(1);
    let mut n = 0u64;
    loop {
        if crashed.load(Ordering::Relaxed) || *disconnect_rx.borrow() {
            crashed.store(true, Ordering::Relaxed);
            cause.store(CAUSE_DISCONNECTED, Ordering::Relaxed);
            return false;
        }
        let t_next = send_time_secs(phase, from, to, n + 1);
        let fire_at = phase_start + Duration::from_secs_f64(t_next);
        if fire_at >= phase_start + phase_duration {
            return !crashed.load(Ordering::Relaxed);
        }
        let delay = fire_at.saturating_duration_since(Instant::now());
        if delay > Duration::ZERO {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = disconnect_rx.wait_for(|d| *d) => {
                    crashed.store(true, Ordering::Relaxed);
                    cause.store(CAUSE_DISCONNECTED, Ordering::Relaxed);
                    return false;
                }
            }
        }
        if crashed.load(Ordering::Relaxed) || *disconnect_rx.borrow() {
            crashed.store(true, Ordering::Relaxed);
            cause.store(CAUSE_DISCONNECTED, Ordering::Relaxed);
            return false;
        }
        // local_n increments only after a successful send: failed/stalled
        // attempts never consume client-order IDs.
        let cl_ord_id = format!("c{:02}-{:06}", idx, *local_n + 1);
        let send_ok = tokio::select! {
            res = conn.send_order(&cl_ord_id) => res.is_ok(),
            _ = disconnect_rx.wait_for(|d| *d) => false,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                eprintln!("STOPPED: conn{idx} send blocked >2s (backpressure; cause unverified)");
                cause.store(CAUSE_WATCHDOG, Ordering::Relaxed);
                false
            }
        };
        if !send_ok {
            crashed.store(true, Ordering::Relaxed);
            if cause.load(Ordering::Relaxed) == CAUSE_NONE {
                cause.store(CAUSE_DISCONNECTED, Ordering::Relaxed);
            }
            return false;
        }
        *local_n += 1;
        sent.fetch_add(1, Ordering::Relaxed);
        n += 1;
        let _ = received;
    }
}

/// Multi-connection scale mode (lazy spawn): one connection fills to
/// --per-conn before the next spawns. `frozen[i]` is worker i's sustained
/// rate; invariant: sum(frozen) == current_rate after every accepted cycle.
async fn run_scale(start: u64, step: u64, per_conn: u64, port: u16) {
    let mut workers: Vec<WorkerHandle> = Vec::new();
    let mut frozen: Vec<u64> = Vec::new();
    let mut final_tps = 0u64;
    let mut current_rate: u64;
    let mut cycle: u64;
    let mut win_id = 0u64;

    println!("Starting multi-connection scale test (lazy spawn)");
    println!("  Start: {} orders/sec, step: {}, per-conn cap: {}", start, step, per_conn);
    println!("  Port: {}", port);

    // Cycle 0: conn0 ramps 0→start, sustains start.
    match spawn_worker(0, port).await {
        Ok(w) => workers.push(w),
        Err(e) => {
            println!("Failed to connect conn0: {}", e);
            println!("FINAL TPS: 0 (measured, 0 connections)");
            return;
        }
    }
    frozen.push(start);
    win_id += 1;
    match drive_single_ramp(&workers[0], win_id, 0, start).await {
        Err(reason) => {
            report_stop(reason, "ramp", 0, 1);
            stop_all(&workers).await;
            return;
        }
        Ok(()) => {}
    }
    // frozen[0] is preset to `start`: the hold must sustain the ramped rate,
    // and is only confirmed (kept) if the gate passes below.
    let mut completed_conns = 1u64;
    win_id += 1;
    match hold_aggregate(&workers, win_id, &frozen, start).await {
        HoldOutcome::Ok { sent, wall_secs } => {
            let (avg, next_final, passed) = settle_hold(sent, wall_secs, start, final_tps);
            println!("  Completed: sent={}, wall_secs={:.3}s, avg_rate={}/s", sent, wall_secs, avg);
            if !passed {
                println!("FAILED TO SUSTAIN {}: measured {} (wall {:.3}s)", start, avg, wall_secs);
                println!("FINAL TPS: {} (measured, 1 connections)", final_tps);
                stop_all(&workers).await;
                return;
            }
            final_tps = next_final;
            current_rate = start;
            frozen[0] = start;
            cycle = 1;
            println!("CYCLE {}: 1 connection(s), target {}/s, measured {}/s (wall {:.3}s), FINAL TPS: {}", cycle, start, avg, wall_secs, final_tps);
        }
        HoldOutcome::Stopped(reason) => {
            report_stop(reason, "sustain", final_tps, 1);
            stop_all(&workers).await;
            return;
        }
    }

    // Subsequent cycles: fill the newest non-full worker; spawn only when
    // every existing worker is at the cap. `frozen[i]` is worker i's rate.
    loop {
        let next = current_rate.saturating_add(step);
        cycle += 1;
        // Single decision point: fill the newest worker with headroom,
        // else spawn. `next` is only the aggregate target for gating.
        match fill_action(&frozen, step, per_conn) {
            FillAction::Ramp { idx: last, from, to } => {
                // Room on worker `last`: ramp it by step, sustain others.
                win_id += 1;
                debug_assert_eq!(from + step, to);
                debug_assert_eq!(frozen.iter().sum::<u64>() + step, next);
                if let Err(reason) = drive_fill_ramp(&workers, &frozen, last, win_id, from, to).await {
                    report_stop(reason, "ramp", final_tps, completed_conns);
                    stop_all(&workers).await;
                    return;
                }
                frozen[last] = to;
            }
            FillAction::Spawn { delta } => {
                let idx = workers.len() as u64;
                match spawn_worker(idx, port).await {
                    Ok(w) => workers.push(w),
                    Err(e) => {
                        println!("Failed to connect conn{}: {}", idx, e);
                        println!("FINAL TPS: {} (measured, {} connections)", final_tps, completed_conns);
                        stop_all(&workers).await;
                        return;
                    }
                }
                frozen.push(0);
                let last = frozen.len() - 1;
                let (frozen_old, _) = frozen.split_at(last);
                win_id += 1;
                // Old workers sustain; new worker ramps 0→delta in the same window.
                if let Err(reason) = drive_mixed_ramp(&workers, frozen_old, win_id, delta).await {
                    report_stop(reason, "ramp", final_tps, completed_conns);
                    stop_all(&workers).await;
                    return;
                }
                frozen[last] = delta;
            }
        }
        // Aggregate sustain at `next` across all workers.
        win_id += 1;
        let k = workers.len() as u64;
        match hold_aggregate(&workers, win_id, &frozen, next).await {
            HoldOutcome::Stopped(reason) => {
                report_stop(reason, "sustain", final_tps, completed_conns);
                stop_all(&workers).await;
                return;
            }
            HoldOutcome::Ok { sent, wall_secs } => {
                let (avg, next_final, passed) = settle_hold(sent, wall_secs, next, final_tps);
                println!("  Completed: sent={}, wall_secs={:.3}s, avg_rate={}/s", sent, wall_secs, avg);
                if !passed {
                    println!("FAILED TO SUSTAIN {}: measured {} (wall {:.3}s)", next, avg, wall_secs);
                    println!("FINAL TPS: {} (measured, {} connections)", final_tps, completed_conns);
                    stop_all(&workers).await;
                    return;
                }
                final_tps = next_final;
                current_rate = next;
                completed_conns = k;
                println!("CYCLE {}: {} connection(s), target {}/s, measured {}/s (wall {:.3}s), FINAL TPS: {}", cycle, k, next, avg, wall_secs, final_tps);
            }
        }
    }
}

/// Why a driven window failed.
/// Disconnected = reader fired (Close/Err/None) or send returned Err:
/// the peer is gone. Watchdog = a worker's 2s send watchdog fired (blocked
/// writer, cause unverified). CoordTimeout = the coordinator never got an
/// id-matched ack (scheduling stall, not a send verdict).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    Disconnected,
    Watchdog,
    CoordTimeout,
}

/// Print the honest stop line for a failed window.
fn report_stop(reason: StopReason, phase: &str, final_tps: u64, completed_conns: u64) {
    match reason {
        StopReason::Disconnected => println!("CRASHED during {}", phase),
        StopReason::Watchdog => println!("STOPPED during {}: send blocked >2s (backpressure; cause unverified)", phase),
        StopReason::CoordTimeout => println!("STOPPED during {}: coordinator ack timeout (scheduling stall, not a send verdict)", phase),
    }
    println!("FINAL TPS: {} (measured, {} connections)", final_tps, completed_conns);
}

/// Send one window command to a single worker and await its id-matched edges.
async fn drive_one(w: &WorkerHandle, id: u64, cmd: Cmd) -> Result<(), StopReason> {
    if w.cmd_tx.send(cmd).await.is_err() {
        return Err(StopReason::Disconnected);
    }
    let starts = match await_starts(std::slice::from_ref(w), id, "window start").await {
        Some(s) => s,
        None => return Err(reason_of(std::slice::from_ref(w))),
    };
    match await_completes(std::slice::from_ref(w), id, &starts, "window complete").await {
        Some(_) if !w.crashed.load(Ordering::Relaxed) => Ok(()),
        _ => Err(reason_of(std::slice::from_ref(w))),
    }
}

/// Drive a mixed ramp window: old workers sustain frozen rates, new ramps 0→delta.
async fn drive_mixed_ramp(workers: &[WorkerHandle], frozen_old: &[u64], id: u64, delta: u64) -> Result<(), StopReason> {
    let k = workers.len();
    assert!(frozen_old.len() + 1 == k);
    for (i, w) in workers.iter().enumerate() {
        let cmd = if i + 1 == k {
            Cmd::Ramp { id, from: 0, to: delta }
        } else {
            Cmd::Sustain { id, rate: frozen_old[i] }
        };
        if w.cmd_tx.send(cmd).await.is_err() {
            return Err(StopReason::Disconnected);
        }
    }
    let starts = match await_starts(workers, id, "ramp start").await {
        Some(s) => s,
        None => return Err(reason_of(workers)),
    };
    match await_completes(workers, id, &starts, "ramp complete").await {
        Some(_) if !workers.iter().any(|w| w.crashed.load(Ordering::Relaxed)) => Ok(()),
        _ => Err(reason_of(workers)),
    }
}
/// Drive a fill ramp: worker `active` ramps from→to while all others sustain
/// their frozen rates in the same window.
async fn drive_fill_ramp(workers: &[WorkerHandle], frozen: &[u64], active: usize, id: u64, from: u64, to: u64) -> Result<(), StopReason> {
    assert_eq!(workers.len(), frozen.len());
    for (i, w) in workers.iter().enumerate() {
        let cmd = if i == active {
            Cmd::Ramp { id, from, to }
        } else {
            Cmd::Sustain { id, rate: frozen[i] }
        };
        if w.cmd_tx.send(cmd).await.is_err() {
            return Err(StopReason::Disconnected);
        }
    }
    let starts = match await_starts(workers, id, "ramp start").await {
        Some(s) => s,
        None => return Err(reason_of(workers)),
    };
    match await_completes(workers, id, &starts, "ramp complete").await {
        Some(_) if !workers.iter().any(|w| w.crashed.load(Ordering::Relaxed)) => Ok(()),
        _ => Err(reason_of(workers)),
    }
}


/// Drive a single-worker ramp window.
async fn drive_single_ramp(w: &WorkerHandle, id: u64, from: u64, to: u64) -> Result<(), StopReason> {
    drive_one(w, id, Cmd::Ramp { id, from, to }).await
}

/// Classify a failed window from per-worker causes only: a DISCONNECTED worker
/// means the reader fired (peer gone); a WATCHDOG worker means its send
/// blocked (cause unverified); a COORD_TIMEOUT worker means the coordinator
/// never got id-matched acks. The bare `crashed` flag is NEVER consulted:
/// it is set on every path and cannot distinguish causes.
fn reason_of(workers: &[WorkerHandle]) -> StopReason {
    let mut seen_watchdog = false;
    for w in workers {
        match w.cause.load(Ordering::Relaxed) {
            CAUSE_DISCONNECTED => return StopReason::Disconnected,
            CAUSE_WATCHDOG => seen_watchdog = true,
            _ => {}
        }
    }
    if seen_watchdog {
        StopReason::Watchdog
    } else {
        StopReason::CoordTimeout
    }
}

enum HoldOutcome {
    /// sent = in-window aggregate sends; wall_secs = START-barrier to last
    /// COMPLETE (actual elapsed, not the nominal 1s). Consumers divide to get
    /// wall-clock TPS. Under backpressure the window stretches past 1s and
    /// the rate honestly drops instead of claiming nominal throughput.
    Ok { sent: u64, wall_secs: f64 },
    Stopped(StopReason),
}

/// Aggregate sustain hold: every worker sustains its frozen rate; workers pace
/// a nominal 1s of sends each, but under backpressure the window stretches.
/// Returns `Ok { sent, wall_secs }` with actual START-barrier→last-COMPLETE
/// elapsed; consumers divide via `settle_hold` for wall-clock TPS.
async fn hold_aggregate(workers: &[WorkerHandle], id: u64, frozen: &[u64], _target: u64) -> HoldOutcome {
    let t_dispatch = Instant::now();
    assert_eq!(workers.len(), frozen.len());
    for (w, rate) in workers.iter().zip(frozen.iter()) {
        if w.cmd_tx.send(Cmd::Sustain { id, rate: *rate }).await.is_err() {
            return HoldOutcome::Stopped(StopReason::Disconnected);
        }
    }
    eprintln!("[trace] SUSTAIN_DISPATCH id={} workers={} +{:?} after entry", id, workers.len(), t_dispatch.elapsed());
    let starts = match await_starts(workers, id, "sustain start").await {
        Some(s) => s,
        None => return HoldOutcome::Stopped(reason_of(workers)),
    };
    eprintln!("[trace] SUSTAIN_START id={} workers={} +{:?} after dispatch", id, workers.len(), t_dispatch.elapsed());
    // Wall-clock barrier: START edges received = window opens for accounting.
    let t_sustain_start = Instant::now();
    let deltas = match await_completes(workers, id, &starts, "sustain complete").await {
        Some(d) => d,
        None => return HoldOutcome::Stopped(reason_of(workers)),
    };
    let wall_secs = t_sustain_start.elapsed().as_secs_f64().max(0.001);
    eprintln!("[trace] SUSTAIN_END id={} workers={} wall_secs={:.3}s", id, workers.len(), wall_secs);
    if workers.iter().any(|w| w.crashed.load(Ordering::Relaxed)) {
        return HoldOutcome::Stopped(StopReason::Disconnected);
    }
    let agg_sent: u64 = deltas.iter().map(|(s, _)| *s).sum();
    HoldOutcome::Ok { sent: agg_sent, wall_secs }
}

#[tokio::main]
async fn main() {
    let (start_rate, step, port, per_conn) = parse_args();
    if let Some(n) = per_conn {
        if let Err(e) = validate_scale_args(start_rate, step, n) {
            eprintln!("{e}");
            std::process::exit(2);
        }
        run_scale(start_rate, step, n, port).await;
        return;
    }
    if let Err(e) = validate_args(start_rate, step) {
        eprintln!("{e}");
        std::process::exit(2);
    }

    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let crashed = Arc::new(AtomicBool::new(false));
    let cause = Arc::new(AtomicU8::new(CAUSE_NONE));
    let mut highest_sustain_rate = 0u64;

    println!("Starting ramp/sustain stress test (single connection)");
    println!("  Start rate: {} orders/sec", start_rate);
    println!("  Step: +{} orders/sec per cycle", step);
    println!("  Port: {}", port);
    println!("  Pattern: Ramp 0→{}, Sustain {}, Ramp {}→{}, Sustain {}, ...",
             start_rate, start_rate, start_rate, start_rate + step, start_rate + step);

    // Connect once; background reader owns the read half and signals
    // disconnects over a watch channel so in-flight sends cancel promptly.
    let (disconnect_tx, disconnect_rx) = tokio::sync::watch::channel(false);
    let mut disconnect_rx = disconnect_rx;
    let (mut conn, reader) = match Connection::connect(port, Arc::clone(&received), Arc::clone(&crashed), Arc::clone(&cause), disconnect_tx).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
            return;
        }
    };

    println!("Connected. Starting test...\n");

    // Walk the alternating plan; Phase 0 is the initial ramp.
    for (idx, (phase, from, to)) in phase_iter(start_rate, step).enumerate() {
        match phase {
            PhaseType::Ramp => {
                println!("\nPhase {}: Ramp {} → {} orders/sec", idx, from, to);
                let (ok, sent_n, recv_n, avg, stalled) = run_phase(&mut conn, phase, from, to, &sent, &received, &crashed, &mut disconnect_rx).await;
                let backlog = sent.load(Ordering::Relaxed).saturating_sub(received.load(Ordering::Relaxed));
                println!("  Completed: sent={}, recv={}, backlog={}, avg_rate={}/sec", sent_n, recv_n, backlog, avg);
                if !ok || crashed.load(Ordering::Relaxed) {
                    if stalled {
                        println!("STOPPED during ramp: send backpressure (cause unverified)");
                    } else {
                        println!("CRASHED during ramp");
                    }
                    println!("HIGHEST: {} orders/sec sustained (measured)", highest_sustain_rate);
                    reader.abort();
                    return;
                }
            }
            PhaseType::Sustain => {
                println!("\nPhase {}: Sustain {} orders/sec", idx, from);
                let (ok, sent_n, recv_n, avg, stalled) = run_phase(&mut conn, phase, from, to, &sent, &received, &crashed, &mut disconnect_rx).await;
                let backlog = sent.load(Ordering::Relaxed).saturating_sub(received.load(Ordering::Relaxed));
                println!("  Completed: sent={}, recv={}, backlog={}, avg_rate={}/sec", sent_n, recv_n, backlog, avg);
                if !ok || crashed.load(Ordering::Relaxed) {
                    if stalled {
                        println!("STOPPED during sustain: send backpressure (cause unverified)");
                    } else {
                        println!("CRASHED during sustain");
                    }
                    println!("HIGHEST: {} orders/sec sustained (measured)", highest_sustain_rate);
                    reader.abort();
                    return;
                }
                match accept_sustain(avg, from) {
                    None => {
                        println!("FAILED TO SUSTAIN {}: measured {}", from, avg);
                        println!("HIGHEST: {} orders/sec sustained (measured)", highest_sustain_rate);
                        reader.abort();
                        return;
                    }
                    Some(measured) => {
                        println!("  Phase measured: target {}, measured {}", from, avg);
                        highest_sustain_rate = track_highest(highest_sustain_rate, measured);
                        println!("  HIGHEST: {} orders/sec sustained (measured)", highest_sustain_rate);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Production watchdog path: a server that accepts but never reads must
    /// drive `run_worker_window` into its 2s arm and mark CAUSE_WATCHDOG —
    /// never CAUSE_DISCONNECTED. This is the exact regression the cause
    /// refactor addressed (one shared `crashed` flag used to imply disconnect).
    #[tokio::test]
    async fn worker_watchdog_marks_cause() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Read exactly one message (completes the client's first send),
            // then hold the connection open without reading: the client's
            // kernel send buffer fills and the next sends wedge.
            use futures_util::StreamExt;
            let _ = ws.next().await;
            std::future::pending::<()>().await;
        });
        let sent = Arc::new(AtomicU64::new(0));
        let received = Arc::new(AtomicU64::new(0));
        let crashed = Arc::new(AtomicBool::new(false));
        let cause = Arc::new(AtomicU8::new(CAUSE_NONE));
        let (_dtx, drx) = tokio::sync::watch::channel(false);
        let mut rx = drx;
        let (mut conn, _reader) =
            Connection::connect(port, Arc::clone(&received), Arc::clone(&crashed), Arc::clone(&cause), _dtx).await.unwrap();
        // Flood at 50k/s with tiny pacing: buffer saturates, 2s arm fires.
        let t0 = Instant::now();
        let mut local_n = 0u64;
        let ok = run_worker_window(&mut conn, PhaseType::Sustain, 50_000, 50_000, 7, &mut local_n, &sent, &received, &crashed, &cause, &mut rx).await;
        let dt = t0.elapsed();
        assert!(!ok, "wedged writer must fail the window");
        assert_eq!(cause.load(Ordering::Relaxed), CAUSE_WATCHDOG, "watchdog arm must mark WATCHDOG, not disconnect");
        assert!(crashed.load(Ordering::Relaxed));
        assert!(dt >= Duration::from_secs(2), "must reach the 2s arm, took {dt:?}");
        assert!(dt < Duration::from_secs(10), "must not hang, took {dt:?}");
        server.abort();
        _reader.abort();
    }

    #[tokio::test]
    async fn disconnect_cancels_stalled_send() {
        // Bind a local TCP listener and accept one side as a fake WS server
        // that reads slowly (never responds), so the client send buffer fills
        // or at least the send future can be wedged by pausing reads.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Read one message then stall forever holding the connection open:
            // client sends will eventually backpressure.
            use futures_util::StreamExt;
            let _ = ws.next().await;
            std::future::pending::<()>().await;
        });
        let sent = Arc::new(AtomicU64::new(0));
        let received = Arc::new(AtomicU64::new(0));
        let crashed = Arc::new(AtomicBool::new(false));
        let (disconnect_tx, disconnect_rx) = tokio::sync::watch::channel(false);
        let mut rx = disconnect_rx;
        let (mut conn, _reader) =
            Connection::connect(port, Arc::clone(&received), Arc::clone(&crashed), Arc::new(AtomicU8::new(CAUSE_NONE)), disconnect_tx.clone()).await.unwrap();
        // Flood until the writer stalls, then fire disconnect mid-send.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = disconnect_tx.send(true);
        });
        let t0 = Instant::now();
        // High-rate sustain keeps sends back-to-back so the stall lands inside send_order.
        let (ok, _, _, _, _) = run_phase(&mut conn, PhaseType::Sustain, 50_000, 50_000, &sent, &received, &crashed, &mut rx).await;
        let dt = t0.elapsed();
        assert!(!ok, "phase must report failure on disconnect");
        assert!(crashed.load(Ordering::Relaxed), "crashed must be set");
        assert!(dt < Duration::from_secs(4), "cancel took {dt:?}, watchdog (5s) must not decide");
        server.abort();
        _reader.abort();
    }

    #[test]
    fn send_time_sustain() {
        assert!((send_time_secs(PhaseType::Sustain, 1000, 1000, 1000) - 1.0).abs() < 1e-9);
        assert!((send_time_secs(PhaseType::Sustain, 2000, 2000, 1000) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn send_time_ramp_from_zero() {
        let t500 = send_time_secs(PhaseType::Ramp, 0, 1000, 500);
        assert!((0.999..1.001).contains(&t500), "t500={t500}");
        let t125 = send_time_secs(PhaseType::Ramp, 0, 1000, 125);
        assert!((0.499..0.501).contains(&t125), "t125={t125}");
    }

    #[test]
    fn send_time_ramp_mid() {
        let t = send_time_secs(PhaseType::Ramp, 1000, 2000, 1500);
        assert!((0.999..1.001).contains(&t), "t={t}");
    }

    #[test]
    fn send_time_monotonic() {
        let mut prev = 0.0;
        for n in 1..=100u64 {
            let t = send_time_secs(PhaseType::Ramp, 0, 1000, n);
            assert!(t > prev, "n={n} t={t} prev={prev}");
            prev = t;
        }
    }

    /// `await_completes` sums strictly in-window deltas from the production
    /// ack path: 2 workers x 10k in-window sends each aggregate to 20000
    /// even when B's COMPLETE arrives late (barrier skew). Unlike
    /// `scale_aggregate_nominal_window` (which pins the `#[cfg(test)]`
    /// summation helper), this drives the real function with live channels.
    #[tokio::test]
    async fn await_completes_immune_to_skew() {
        fn live_handle() -> (WorkerHandle, tokio::sync::mpsc::Sender<WindowAck>) {
            let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel::<Cmd>(1);
            let (ack_tx, ack_rx) = tokio::sync::mpsc::channel::<WindowAck>(4);
            let h = WorkerHandle {
                cmd_tx,
                ack_rx: tokio::sync::Mutex::new(ack_rx),
                crashed: Arc::new(AtomicBool::new(false)),
                cause: Arc::new(AtomicU8::new(CAUSE_NONE)),
                join: tokio::spawn(async {}),
                reader: tokio::spawn(async {}),
            };
            (h, ack_tx)
        }
        let (a, atx) = live_handle();
        let (b, btx) = live_handle();
        let workers = [a, b];
        // START edges consumed separately; COMPLETE deltas carry the counts.
        let starts = vec![(0u64, 0u64), (5000u64, 5000u64)];
        atx.send(WindowAck { id: 7, started: false, sent_at_edge: 10_000, recv_at_edge: 9_900 }).await.unwrap();
        // B's ack arrives late with a different absolute base but same delta.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            btx.send(WindowAck { id: 7, started: false, sent_at_edge: 15_000, recv_at_edge: 14_900 }).await.unwrap();
        });
        let got = await_completes(&workers, 7, &starts, "skew test").await.expect("completes");
        assert_eq!(got, vec![(10_000, 9_900), (10_000, 9_900)]);
        for w in &workers {
            w.join.abort();
            w.reader.abort();
        }
    }

    /// Wall-clock regression: a stretched window must report below nominal.
    /// 20k sends over 2.5s wall = 8000 TPS, not 20000. This pins the helper
    /// the old `sent / 1.0` code would have failed.
    #[test]
    fn wall_clock_tps_stretched_window() {
        assert_eq!(wall_clock_tps(1000, 1.0), 1000);
        assert_eq!(wall_clock_tps(20_000, 2.5), 8000);
        assert!(wall_clock_tps(20_000, 2.5) < 20_000);
    }

    /// Stretched window fails the production gate: 39998 sends over 1.375s
    /// wall = 29089 TPS, which `accept_sustain` rejects against a 40000
    /// target (the live case at wall ~1.375s). The old nominal code reported
    /// 39998 and passed dishonestly.
    #[test]
    fn stretched_window_fails_gate() {
        let avg = wall_clock_tps(39_998, 1.375);
        assert_eq!(avg, 29_089);
        assert_eq!(accept_sustain(avg, 40_000), None);
        // ...while the same sends over a true 1s window would pass.
        assert_eq!(accept_sustain(wall_clock_tps(39_998, 1.0), 40_000), Some(39_998));
    }

    /// Coordinator-level regression through the SHARED `settle_hold` helper
    /// both `run_scale` arms call: a stretched hold (39998 sends, 1.375s
    /// wall) must reject with final_tps untouched; the same sends over a
    /// true 1s window must accept and advance. A nominal `/1.0` regression
    /// anywhere in the consumer chain fails this.
    #[test]
    fn stretched_hold_rejects_at_coordinator() {
        let (avg, next_final, passed) = settle_hold(39_998, 1.375, 40_000, 29_981);
        assert_eq!(avg, 29_089);
        assert!(!passed);
        assert_eq!(next_final, 29_981);
        let (avg2, next_final2, passed2) = settle_hold(39_998, 1.0, 40_000, 29_981);
        assert_eq!(avg2, 39_998);
        assert!(passed2);
        assert_eq!(next_final2, 39_998);
    }

    /// Stop-cause mapping is deterministic over the cause atomics:
    /// watchdog → STOPPED (Watchdog), disconnect → CRASHED (Disconnected),
    /// ack-timeout/none → CoordTimeout. The bare `crashed` flag alone must
    /// NEVER imply disconnect.
    fn fake_handle(cause: u8, crashed: bool) -> WorkerHandle {
        let (_cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<Cmd>(1);
        let (_ack_tx, ack_rx) = tokio::sync::mpsc::channel::<WindowAck>(1);
        let (_dtx, _drx) = tokio::sync::watch::channel(false);
        let _ = cmd_rx;
        WorkerHandle {
            cmd_tx: _cmd_tx,
            ack_rx: tokio::sync::Mutex::new(ack_rx),
            crashed: Arc::new(AtomicBool::new(crashed)),
            cause: Arc::new(AtomicU8::new(cause)),
            // Never polled; only the struct shape matters for reason_of.
            join: tokio::spawn(async {}),
            reader: tokio::spawn(async {}),
        }
    }

    /// Reader Close frame must mark CAUSE_DISCONNECTED: a server-side close
    /// is a disconnect, never a watchdog/coord verdict.
    #[tokio::test]
    async fn reader_close_marks_disconnected() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            use futures_util::{SinkExt, StreamExt};
            // Read one message, then clean-close: reader must see Close.
            let _ = ws.next().await;
            let _ = ws.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
        });
        let received = Arc::new(AtomicU64::new(0));
        let crashed = Arc::new(AtomicBool::new(false));
        let cause = Arc::new(AtomicU8::new(CAUSE_NONE));
        let (disconnect_tx, _rx) = tokio::sync::watch::channel(false);
        let (mut conn, reader) =
            Connection::connect(port, Arc::clone(&received), Arc::clone(&crashed), Arc::clone(&cause), disconnect_tx).await.unwrap();
        // One send so the server proceeds to Close.
        let _ = conn.send_order("c99-000001").await;
        // Reader task observes the Close within 5s.
        let t0 = Instant::now();
        while cause.load(Ordering::Relaxed) == CAUSE_NONE && t0.elapsed() < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(cause.load(Ordering::Relaxed), CAUSE_DISCONNECTED, "reader must mark disconnect on Close");
        assert!(crashed.load(Ordering::Relaxed));
        reader.abort();
        server.abort();
    }

    /// 3 workers sustain concurrently in one global ~1s window: dispatch all
    /// Sustain cmds, assert every COMPLETE arrives with each worker near its
    /// 1s target and total wall-clock < 2.5s (serialized 1s windows would
    /// take ~3s). Draining mock server: no watchdog, no disconnect.
    #[tokio::test]
    async fn hold_aggregate_concurrent_3_workers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            use futures_util::StreamExt;
            for _ in 0..3 {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    while ws.next().await.is_some() {}
                });
            }
            std::future::pending::<()>().await;
        });
        let mut workers = Vec::new();
        for idx in 0..3u64 {
            workers.push(spawn_worker(idx, port).await.expect("connect"));
        }
        let frozen = vec![100u64, 100, 100];
        let t0 = Instant::now();
        // Drive the ack level directly for per-worker visibility.
        for (w, rate) in workers.iter().zip(frozen.iter()) {
            w.cmd_tx.send(Cmd::Sustain { id: 42, rate: *rate }).await.unwrap();
        }
        let starts = await_starts(&workers, 42, "test sustain start").await.expect("starts");
        let deltas = await_completes(&workers, 42, &starts, "test sustain complete").await.expect("completes");
        let dt = t0.elapsed();
        // Balanced contribution: each worker sends ~100, not 0/300 splits.
        assert_eq!(deltas.len(), 3);
        for (i, (s, _)) in deltas.iter().enumerate() {
            assert!((90..=110).contains(s), "worker {i} sent={s}, want ~100");
        }
        let agg_sent: u64 = deltas.iter().map(|(s, _)| *s).sum();
        assert!((270..=330).contains(&agg_sent), "agg_sent={agg_sent}");
        assert!(dt < Duration::from_millis(2500), "hold took {dt:?}, workers ran serially");
        for w in &workers {
            w.join.abort();
            w.reader.abort();
        }
        server.abort();
    }

    #[tokio::test]
    async fn stop_reason_from_cause() {
        // Watchdog with crash flag set still reports Watchdog, not disconnect.
        let h = fake_handle(CAUSE_WATCHDOG, true);
        assert_eq!(reason_of(std::slice::from_ref(&h)), StopReason::Watchdog);
        h.join.abort();
        h.reader.abort();
        // Reader-fired disconnect reports Disconnected.
        let h = fake_handle(CAUSE_DISCONNECTED, true);
        assert_eq!(reason_of(std::slice::from_ref(&h)), StopReason::Disconnected);
        h.join.abort();
        h.reader.abort();
        // Ack timeout reports CoordTimeout.
        let h = fake_handle(CAUSE_COORD_TIMEOUT, false);
        assert_eq!(reason_of(std::slice::from_ref(&h)), StopReason::CoordTimeout);
        h.join.abort();
        h.reader.abort();
        // Bare crash flag with no cause is NOT a disconnect.
        let h = fake_handle(CAUSE_NONE, true);
        assert_eq!(reason_of(std::slice::from_ref(&h)), StopReason::CoordTimeout);
        h.join.abort();
        h.reader.abort();
        // Disconnect wins over watchdog in a mixed set.
        let a = fake_handle(CAUSE_WATCHDOG, true);
        let b = fake_handle(CAUSE_DISCONNECTED, true);
        assert_eq!(reason_of(&[a, b]), StopReason::Disconnected);
    }

    #[test]
    fn sustain_records_measured() {
        assert_eq!(accept_sustain(950, 1000), Some(950));
        assert_eq!(accept_sustain(1000, 1000), Some(1000));
        assert_eq!(accept_sustain(949, 1000), None);
    }

    #[test]
    fn highest_never_decreases() {
        assert_eq!(track_highest(2000, 1950), 2000);
        assert_eq!(track_highest(1950, 2000), 2000);
        assert_eq!(track_highest(0, 950), 950);
    }

    #[test]
    fn sustain_gate() {
        assert!(meets_sustain(950, 1000));
        assert!(meets_sustain(1000, 1000));
        assert!(!meets_sustain(949, 1000));
    }

    #[test]
    fn args_validated() {
        assert!(validate_args(0, 1000).is_err());
        assert!(validate_args(1000, 0).is_err());
        assert!(validate_args(1000, 1000).is_ok());
    }

    #[test]
    fn fill_action_ramps_with_headroom() {
        // 9000+1000 on cap 10000: ramp worker 0, no spawn. Cap equality ramps.
        assert_eq!(
            fill_action(&[9000], 1000, 10_000),
            FillAction::Ramp { idx: 0, from: 9000, to: 10_000 }
        );
        // Newest non-full worker wins: [6000, 2000] ramps index 1 to 4000.
        assert_eq!(
            fill_action(&[6000, 2000], 2000, 6000),
            FillAction::Ramp { idx: 1, from: 2000, to: 4000 }
        );
        // [6000, 4000] ramps index 1 to the cap.
        assert_eq!(
            fill_action(&[6000, 4000], 2000, 6000),
            FillAction::Ramp { idx: 1, from: 4000, to: 6000 }
        );
    }

    #[test]
    fn fill_action_spawns_when_full() {
        // 9500+1000 and 10000+1000 on cap 10000: spawn with delta == step.
        assert_eq!(fill_action(&[9500], 1000, 10_000), FillAction::Spawn { delta: 1000 });
        assert_eq!(fill_action(&[10_000], 1000, 10_000), FillAction::Spawn { delta: 1000 });
        // [6000, 6000] full: spawn, delta 2000 <= cap.
        assert_eq!(fill_action(&[6000, 6000], 2000, 6000), FillAction::Spawn { delta: 2000 });
    }

    #[test]
    fn fill_walk_per_step_states() {
        // S=4000,T=2000,N=6000 driven through the production helper:
        // states after cycle 0 plus each of 5 transitions.
        let (step, per_conn) = (2000u64, 6000u64);
        let mut frozen = vec![4000u64]; // after cycle 0
        let mut states = vec![frozen.clone()];
        let mut sums = vec![4000u64];
        for _ in 0..5 {
            match fill_action(&frozen, step, per_conn) {
                FillAction::Ramp { idx, from: _, to } => {
                    frozen[idx] = to;
                }
                FillAction::Spawn { delta } => {
                    assert!(delta > 0 && delta <= per_conn);
                    frozen.push(delta);
                }
            }
            states.push(frozen.clone());
            sums.push(frozen.iter().sum());
        }
        assert_eq!(
            states,
            vec![
                vec![4000],
                vec![6000],
                vec![6000, 2000],
                vec![6000, 4000],
                vec![6000, 6000],
                vec![6000, 6000, 2000],
            ]
        );
        assert_eq!(sums, vec![4000, 6000, 8000, 10000, 12000, 14000]);
    }

    #[test]
    fn scale_args_validated() {
        assert!(validate_scale_args(4000, 2000, 6000).is_ok());
        assert!(validate_scale_args(6000, 2000, 6000).is_ok());
        assert!(validate_scale_args(6001, 2000, 6000).is_err());
        assert!(validate_scale_args(4000, 6001, 6000).is_err());
        assert!(validate_scale_args(4000, 2000, 0).is_err());
    }
    #[test]
    fn scale_aggregate_math() {
        assert_eq!(aggregate_avgs(&[(1000, 1.0)], 1.0), 1000);
        assert_eq!(aggregate_avgs(&[(1000, 1.0), (1000, 1.0)], 1.0), 2000);
        assert_eq!(aggregate_avgs(&[(500, 1.0), (500, 1.0), (500, 1.0)], 1.0), 1500);
    }



    /// Aggregate over a nominal window is immune to barrier skew:
    /// 2 workers x 10k in-window sends each = 20000 TPS even if one
    /// worker's wall-clock span was longer.
    #[test]
    fn scale_aggregate_nominal_window() {
        let agg = aggregate_avgs(&[(10_000, 1.0), (10_000, 1.43)], 1.0);
        assert_eq!(agg, 20_000);
    }

    #[test]
    fn scale_gate_aggregate() {
        assert!(meets_sustain(19_900, 20_000));
        assert!(!meets_sustain(18_999, 20_000));
    }

    #[test]
    fn plan_sequence() {
        use PhaseType::{Ramp, Sustain};
        let first6: Vec<_> = phase_iter(1000, 1000).take(6).collect();
        assert_eq!(
            first6,
            vec![
                (Ramp, 0, 1000),
                (Sustain, 1000, 1000),
                (Ramp, 1000, 2000),
                (Sustain, 2000, 2000),
                (Ramp, 2000, 3000),
                (Sustain, 3000, 3000),
            ]
        );
    }
}