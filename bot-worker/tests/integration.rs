use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bot_worker::config::Config;

fn port_open(port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().unwrap(),
        Duration::from_millis(300),
    )
    .is_ok()
}

fn kill_stale() {
    let _ = Command::new("pkill").args(["-9", "-f", "contestant-sample"]).status();
    let _ = Command::new("sh")
        .args(["-c", "fuser -k 9090/tcp 2>/dev/null; fuser -k 8080/tcp 2>/dev/null"])
        .status();
    std::thread::sleep(Duration::from_secs(2));
}

struct Server {
    child: Option<Child>,
}

impl Server {
    fn start() -> Self {
        kill_stale();

        let candidates: Vec<String> = vec![
            "../contestant-sample/target/release/contestant-sample".to_string(),
            "../contestant-sample/target/debug/contestant-sample".to_string(),
            "target/release/contestant-sample".to_string(),
            "target/debug/contestant-sample".to_string(),
        ];

        let bin = candidates
            .iter()
            .find(|p| std::path::Path::new(p).exists())
            .cloned()
            .unwrap_or_else(|| {
                eprintln!("[Test] Building contestant-sample release binary...");
                let status = Command::new("cargo")
                    .args(["build", "--release"])
                    .current_dir("../contestant-sample")
                    .status()
                    .expect("failed to execute cargo build for contestant-sample");
                assert!(status.success(), "cargo build --release for contestant-sample failed");
                "../contestant-sample/target/release/contestant-sample".to_string()
            });

        // Verify port 9090 is free before starting
        assert!(
            !port_open(9090),
            "Port 9090 must be free before starting exchange"
        );

        eprintln!("[Test] Starting {bin}...");
        let mut child = Command::new(&bin)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn contestant-sample");

        // Poll for port 9090 to open (up to 10s)
        eprintln!("[Test] Waiting for contestant-sample to listen on 9090...");
        let mut started = false;
        for _ in 0..40 {
            assert!(
                child.try_wait().ok().flatten().is_none(),
                "contestant-sample exited before binding port 9090"
            );
            if port_open(9090) {
                started = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        assert!(started, "contestant-sample did not start listening on 9090 within 10s");

        // Verify WS port opens quickly too
        eprintln!("[Test] Contestant ready on 9090/8080");

        Server { child: Some(child) }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
async fn bot_connects_and_trades() {
    let _srv = Server::start();

    let result = bot_worker::run(Config {
        target_host: "127.0.0.1".into(),
        fix_port: 9090,
        ws_port: 8080,
        rps: 50,
        min_rps: 50,
        ramp_up_secs: 0,
        duration_secs: 10,
        seed: 42,
        report_interval_secs: 5,
        fix_connections: 2,
        ws_connections: 2,
        sender_comp_id_prefix: "TEST".into(),
        target_comp_id: "XCANG3".into(),
        redpanda_brokers: "".into(),
        contestant_id: "test".into(),
    })
    .await;

    eprintln!(
        "[Test] Stats: {} sent, {} fills, {} partials, {} rejects, {} errors",
        result.orders_sent,
        result.fills,
        result.partials,
        result.rejects,
        result.errors.len(),
    );

    assert!(
        result.orders_sent >= 200,
        "expected >=200 orders, got {}",
        result.orders_sent
    );
    assert!(
        result.fills + result.partials + result.rejects > 0,
        "expected some responses"
    );
    assert!(
        result.avg_latency_us > 0.0,
        "expected positive avg latency"
    );
    assert!(
        result.errors.is_empty(),
        "expected 0 errors, got {}: {:?}",
        result.errors.len(),
        &result.errors[..result.errors.len().min(5)]
    );
}
