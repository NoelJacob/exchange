use std::process::{Child, Command, Stdio};
use std::time::Duration;
use sqlx::postgres::PgPoolOptions;

/// Kill any leftover exchange process from a previous run.
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

/// Run `cargo build --release` in a sibling project directory.
/// Prints the project name to stderr always so failures are visible.
fn build_release(project: &str) {
    let cwd = format!("../{project}");
    eprintln!("[E2E] Building {project} --release ...");
    let status = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(&cwd)
        .status()
        .expect(&format!("execute cargo build for {project}"));
    assert!(status.success(), "cargo build --release in {project} failed");
}

/// Ensure Docker Compose services (QuestDB, Redpanda, Valkey) are running.
fn ensure_infra() {
    let compose = "../infra/docker-compose.yml";
    let running = Command::new("docker")
        .args(["compose", "-f", compose, "ps", "-q", "--status", "running"])
        .output().ok()
        .filter(|o| !o.stdout.is_empty())
        .is_some();
    if !running {
        eprintln!("[E2E] Starting infra (docker compose up -d)...");
        let status = Command::new("docker")
            .args(["compose", "-f", compose, "up", "-d"])
            .status().expect("docker compose up failed");
        assert!(status.success(), "docker compose up failed");
    }
    // Wait for services
    eprintln!("[E2E] Waiting for infra services...");
    let mut redpanda_ok = false;
    let mut questdb_ok = false;
    for _ in 0..40 {
        if !redpanda_ok {
            redpanda_ok = port_open(9092);
            if redpanda_ok { eprintln!("[E2E] Redpanda ready (port 9092)"); }
        }
        if !questdb_ok {
            questdb_ok = port_open(8812);
        }
        if redpanda_ok && questdb_ok { break; }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(redpanda_ok, "Redpanda did not become healthy");
    assert!(questdb_ok, "QuestDB did not become ready (port 8812)");
    eprintln!("[E2E] All infra services ready");
}

/// Build all required binaries and clean DB state before the test.
async fn prepare() -> sqlx::Pool<sqlx::Postgres> {
    // Kill leftover processes
    kill_stale();

    // Ensure docker services are up
    ensure_infra();

    // Build every binary the pipeline needs
    build_release("contestant-sample");
    build_release("telemetry-ingester");
    build_release("bot-worker");

    // Connect to QuestDB and drop/recreate tables for a clean slate
    let pg = PgPoolOptions::new().max_connections(5)
        .connect("postgresql://admin:quest@localhost:8812")
        .await
        .expect("connect to QuestDB");
    // Drop tables for a clean slate
    let _ = sqlx::query("DROP TABLE IF EXISTS order_events").execute(&pg).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS exec_events").execute(&pg).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS correctness_events").execute(&pg).await;
    let _ = sqlx::query("DROP TABLE IF EXISTS contest_summary").execute(&pg).await;
    // Recreate tables so the ingester can insert (schema matches CREATE IF NOT EXISTS)
    sqlx::query("CREATE TABLE IF NOT EXISTS order_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, side SYMBOL, qty LONG, price DOUBLE, is_market BOOLEAN, protocol SYMBOL) TIMESTAMP(ts) PARTITION BY DAY")
        .execute(&pg).await.expect("create order_events");
    sqlx::query("CREATE TABLE IF NOT EXISTS exec_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, exec_id SYMBOL, exec_seq LONG, exec_type SYMBOL, side SYMBOL, qty LONG, price DOUBLE, is_market BOOLEAN, last_shares LONG, last_px DOUBLE, leaves_qty LONG, cum_qty LONG, latency_us LONG) TIMESTAMP(ts) PARTITION BY DAY")
        .execute(&pg).await.expect("create exec_events");
    sqlx::query("CREATE TABLE IF NOT EXISTS correctness_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, exec_id SYMBOL, verdict SYMBOL, penalty DOUBLE, expected_px DOUBLE, actual_px DOUBLE, expected_qty LONG, actual_qty LONG) TIMESTAMP(ts) PARTITION BY DAY")
        .execute(&pg).await.expect("create correctness_events");

    pg
}

/// Start the contestant-sample exchange binary.
fn start_exchange() -> Child {
    kill_stale();
    let bin = "../contestant-sample/target/release/contestant-sample";
    eprintln!("[E2E] Starting contestant-sample...");
    assert!(!port_open(9090), "Port 9090 must be free before start");
    let child = Command::new(bin).stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().expect("spawn contestant-sample");
    std::thread::sleep(Duration::from_secs(5));
    assert!(port_open(9090), "Contestant must listen on 9090");
    eprintln!("[E2E] Contestant ready on 9090/8080");
    child
}

#[tokio::test]
async fn full_telemetry_pipeline() {
    // Phase 1: prepare (builds binaries, cleans DBs, starts infra)
    let pg = prepare().await;
    let mut ex = start_exchange();

    // Phase 2: start the ingester as a subprocess
    eprintln!("[E2E] Starting telemetry-ingester...");
    let mut ing = Command::new("target/release/telemetry-ingester")
        .stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().expect("spawn ingester");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Phase 3: run the bot
    eprintln!("[E2E] Running bot (30rps, 2fix+2ws, 10s)...");
    let bot_exit = Command::new("../bot-worker/target/release/bot-worker")
        .args(["--rps", "30", "--min-rps", "10",
               "--duration-secs", "10",
               "--fix-connections", "2", "--ws-connections", "2",
               "--redpanda-brokers", "127.0.0.1:9092",
               "--contestant-id", "test",
               "--fix-port", "9090", "--ws-port", "8080"])
        .stdout(Stdio::null()).stderr(Stdio::null())
        .status().expect("bot exit");
    eprintln!("[E2E] Bot exited with status={bot_exit}");
    assert!(bot_exit.success(), "bot-worker failed: {bot_exit}");
    eprintln!("[E2E] Bot done. Draining 8s for late events...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Phase 4: query QuestDB and assert pipeline health
    let o: (i64,) = sqlx::query_as("SELECT count(*) FROM order_events")
        .fetch_one(&pg).await.unwrap_or((0,));
    let e: (i64,) = sqlx::query_as("SELECT count(*) FROM exec_events")
        .fetch_one(&pg).await.unwrap_or((0,));
    let max_seq: (i64,) = sqlx::query_as("SELECT coalesce(max(exec_seq), 0) FROM exec_events")
        .fetch_one(&pg).await.unwrap_or((0,));
    let min_seq: (i64,) = sqlx::query_as("SELECT coalesce(min(exec_seq), 0) FROM exec_events")
        .fetch_one(&pg).await.unwrap_or((0,));

    eprintln!("[E2E] DB: {} orders, {} execs", o.0, e.0);
    eprintln!("[E2E] exec_seq range: min={} max={} count={}", min_seq.0, max_seq.0, e.0);

    // Assertions: events arrived with no persistent gaps
    assert!(o.0 > 0, "must have order_events");
    assert!(e.0 > 0, "must have exec_events");
    let expected = max_seq.0 - min_seq.0 + 1;
    assert_eq!(e.0, expected,
        "exec_seq gap detected: count({}) != max-min+1({})", e.0, expected);

    // Cleanup
    drop(pg);
    let _ = ing.kill();
    let _ = ex.kill();
    eprintln!("[E2E] ALL PASS");
}
