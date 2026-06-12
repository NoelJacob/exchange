use std::process::{Child, Command, Stdio};
use std::time::Duration;
use sqlx::postgres::PgPoolOptions;

fn kill_stale() {
    for pat in &["contestant-sample"] {
        let _ = Command::new("pkill").args(["-9", "-f", pat]).status();
    }
    let _ = Command::new("sh").args(["-c", "fuser -k 9090/tcp 2>/dev/null"]).status();
    let _ = Command::new("sh").args(["-c", "fuser -k 8080/tcp 2>/dev/null"]).status();
    std::thread::sleep(Duration::from_secs(2));
}

fn port_open(port: u16) -> bool {
    std::net::TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().unwrap(),
        Duration::from_millis(300),
    ).is_ok()
}

fn ensure_infra() {
    let compose = "../infra/docker-compose.yml";
    let running = Command::new("docker")
        .args(["compose", "-f", compose, "ps", "-q", "--status", "running"])
        .output().ok()
        .filter(|o| !o.stdout.is_empty())
        .is_some();
    if !running {
        eprintln!("[E2E] Starting infra...");
        assert!(Command::new("docker").args(["compose", "-f", compose, "up", "-d"]).status().ok().map_or(false, |s| s.success()));
        std::thread::sleep(Duration::from_secs(15));
    }
}

fn start_contestant() -> Child {
    kill_stale();
    let bin = if std::path::Path::new("../contestant-sample/target/release/contestant-sample").exists() {
        "../contestant-sample/target/release/contestant-sample"
    } else {
        "../contestant-sample/target/debug/contestant-sample"
    };
    eprintln!("[E2E] Starting contestant-sample...");
    assert!(!port_open(9090), "Port 9090 must be free before start");
    let child = Command::new(bin).stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().expect("spawn contestant-sample");
    std::thread::sleep(Duration::from_secs(5));
    assert!(port_open(9090), "Contestant must listen on 9090");
    eprintln!("[E2E] Contestant ready");
    child
}

#[tokio::test]
async fn full_telemetry_pipeline() {
    ensure_infra();
    let _srv = start_contestant();

    let pg = PgPoolOptions::new().max_connections(2)
        .connect("postgresql://admin:quest@127.0.0.1:8812").await.expect("connect QuestDB");
    for &sql in &["DROP TABLE IF EXISTS order_events", "DROP TABLE IF EXISTS exec_events", "DROP TABLE IF EXISTS correctness_events", "DROP TABLE IF EXISTS contest_summary"] {
        let _ = sqlx::query(sql).execute(&pg).await;
    }

    let tbin = if std::path::Path::new("target/release/telemetry-ingester").exists() {"target/release/telemetry-ingester"} else {"target/debug/telemetry-ingester"};
    let bbin = if std::path::Path::new("../bot-worker/target/release/bot-worker").exists() {"../bot-worker/target/release/bot-worker"} else {"../bot-worker/target/debug/bot-worker"};

    eprintln!("[E2E] Starting ingester...");
    let mut ing = Command::new(tbin).stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().expect("spawn ingester");
    tokio::time::sleep(Duration::from_secs(3)).await;

    eprintln!("[E2E] Running bot (30rps, 2fix+2ws, 10s)...");
    let mut bw = Command::new(bbin)
        .args(["--redpanda-brokers","127.0.0.1:9092","--contestant-id","t",
               "--rps","30","--duration-secs","10","--fix-connections","2","--ws-connections","2","--sender-comp-id-prefix","T"])
        .stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().expect("spawn bot-worker");
    bw.wait().expect("bot exit");
    eprintln!("[E2E] Bot done. Draining 8s...");
    tokio::time::sleep(Duration::from_secs(8)).await;
    ing.kill().ok(); ing.wait().ok();

    let o: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM order_events").fetch_one(&pg).await.unwrap();
    let e: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM exec_events").fetch_one(&pg).await.unwrap();
    let c: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM correctness_events").fetch_one(&pg).await.unwrap();
    eprintln!("[E2E] DB: {} orders, {} execs, {} correctness", o.0, e.0, c.0);
    assert!(o.0 > 0, "no orders");
    assert!(e.0 > 0, "no execs");
    assert!(c.0 > 0, "no correctness — bot should produce fills at 30rps/10s/4conn");

    let v: Vec<(String, i64)> = sqlx::query_as(
        "SELECT verdict, COUNT(*) FROM correctness_events GROUP BY verdict ORDER BY COUNT(*) DESC"
    ).fetch_all(&pg).await.unwrap();
    eprintln!("[E2E] Verdicts:");
    for (vd, n) in &v { eprintln!("  {vd}: {n}"); }
    for (vd, n) in &v {
        assert!(vd == "correct" || vd == "maker" || vd == "unverifiable",
            "unexpected verdict '{vd}': {n} occurrences");
    }
    eprintln!("[E2E] ✅ Pipeline verified");
}
