use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN_SERVER: &str = env!("CARGO_BIN_EXE_contestant-sample");
const BIN_BENCH: &str = env!("CARGO_BIN_EXE_fix_bench");

struct Server(Child);

impl Server {
    fn start() -> Self {
        let child = Command::new(BIN_SERVER)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn contestant-sample");
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if std::net::TcpStream::connect("127.0.0.1:8080").is_ok() {
                return Server(child);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("server did not open 127.0.0.1:8080 in 10s");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn ws_scale_closed_port_fails_fast() {
    let t0 = Instant::now();
    let out = Command::new(BIN_BENCH)
        .args([
            "--start", "50", "--step", "50", "--per-conn", "50", "--port", "9",
        ])
        .output()
        .expect("run fix_bench --per-conn");
    let dt = t0.elapsed();
    assert!(dt < Duration::from_secs(10), "took {dt:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Failed to connect") || stdout.contains("CRASHED") || stdout.contains("FINAL TPS: 0"),
        "expected connect failure, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("FINAL TPS: 1"),
        "must not report nonzero FINAL TPS, got:\n{stdout}"
    );
}

#[test]
fn ws_scale_shape_spawn_on_overflow() {
    let _srv = Server::start();
    // S=50,T=50,N=100: cycle 2 (50+50=100, no overflow) stays single-conn;
    // cycle 3 (100+50>100) spawns conn2.
    let out = Command::new("timeout")
        .args([
            "30",
            "stdbuf",
            "-o0",
            BIN_BENCH,
            "--start",
            "50",
            "--step",
            "50",
            "--per-conn",
            "100",
            "--port",
            "8080",
        ])
        .output()
        .expect("run fix_bench under timeout");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("CYCLE"), "no CYCLE lines, got:\n{stdout}");
    assert!(
        stdout.contains("2 connection(s)"),
        "no second connection, got:\n{stdout}"
    );
    assert!(stdout.contains("FINAL TPS:"), "no FINAL TPS, got:\n{stdout}");
    // Wall-clock accounting: every Completed line must carry wall_secs and
    // an avg_rate consistent with sent/wall_secs (not a nominal passthrough).
    // Parses `sent=<n>, ..., wall_secs=<f>s, avg_rate=<r>/s`. Since the rate
    // is truncated to an integer, the printed wall time must satisfy
    // sent/(r+1) <= wall <= sent/r (with 3-decimal display slack). At 40k/s
    // a 0.0005s rounding wobble swings recomputed rate by ~20, so an
    // absolute ±2 tolerance on the rate would flake; the interval form is
    // immune to display rounding.
    fn field_after<'a>(line: &'a str, key: &str) -> &'a str {
        let i = line.find(key).unwrap_or_else(|| panic!("no {key} in: {line}"));
        &line[i + key.len()..]
    }
    let mut completed = 0u32;
    for line in stdout.lines().filter(|l| l.contains("Completed:")) {
        completed += 1;
        let sent: f64 = field_after(line, "sent=")
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        let wall: f64 = field_after(line, "wall_secs=")
            .split('s')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let rate: f64 = field_after(line, "avg_rate=")
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert!(wall > 0.0, "nonpositive wall in: {line}");
        assert!(rate > 0.0, "nonpositive rate in: {line}");
        // Exact truncation check: printed rate must equal floor(sent/wall),
        // allowing only {:.3} display rounding (±0.001s on wall). At 50/s a
        // nominal-/1.0 regression (rate=sent, wall≈1.0) still passes this —
        // the stretched-window gate failure is pinned deterministically by
        // `stretched_window_fails_gate` in the unit suite instead.
        let lo = (sent / (wall + 0.001)).floor();
        let hi = (sent / (wall - 0.001).max(0.0005)).floor();
        assert!(
            rate >= lo && rate <= hi,
            "avg_rate={rate} not floor({sent}/{wall}) in [{lo},{hi}]: {line}"
        );
        assert!(wall > 0.5, "suspiciously short wall in: {line}");
    }
    assert!(completed >= 2, "expected >=2 Completed lines, got {completed}:\n{stdout}");
}
