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
        // Wait for the WS port to accept instead of a blind sleep.
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
fn ws_ramp_shape_small() {
    let _srv = Server::start();
    // Bound the run: bench climbs until killed; `timeout` (coreutils) caps it.
    // Exit 124 from timeout is expected and still carries partial stdout.
    // `stdbuf -o0` forces unbuffered stdout so piped output survives SIGTERM.
    let out = Command::new("timeout")
        .args([
            "25",
            "stdbuf",
            "-o0",
            BIN_BENCH,
            "--start",
            "200",
            "--step",
            "200",
            "--port",
            "8080",
        ])
        .output()
        .expect("run fix_bench under timeout");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Phase 0: Ramp 0 → 200"),
        "missing phase 0 header, got:\n{stdout}"
    );
    let phase0_sent: u64 = stdout
        .lines()
        .skip_while(|l| !l.contains("Phase 0:"))
        .skip(1)
        .find_map(|l| {
            l.split("sent=")
                .nth(1)?
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        })
        .expect("phase 0 sent count");
    assert!(
        (90..=110).contains(&phase0_sent),
        "phase 0 sent={phase0_sent}, want 90..=110"
    );
    assert!(stdout.contains("HIGHEST:"), "no HIGHEST line, got:\n{stdout}");
}
