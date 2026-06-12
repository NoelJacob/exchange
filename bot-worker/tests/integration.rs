use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bot_worker::config::Config;

struct Server {
    child: Option<Child>,
}

impl Server {
    fn start() -> Self {
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
                let _status = Command::new("cargo")
                    .args(["build", "--release"])
                    .current_dir("../contestant-sample")
                    .status()
                    .expect("failed to build contestant-sample");
                "../contestant-sample/target/release/contestant-sample".to_string()
            });

        eprintln!("[Test] Starting {bin}...");
        let child = Command::new(&bin)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn contestant-sample");

        eprintln!("[Test] Waiting 4s for startup...");
        std::thread::sleep(Duration::from_secs(4));

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
