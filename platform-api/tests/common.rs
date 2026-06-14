use std::sync::LazyLock;

use fred::prelude::*;
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

/// Initialize tracing once across all tests.
static TRACING_INIT: LazyLock<()> = LazyLock::new(|| {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
});

const QUESTDB_HOST: &str = "127.0.0.1:8812";
const REDIS_URL: &str = "redis://127.0.0.1:6379";

/// Set up DB + Redis for integration tests.
/// Drops and recreates platform-api tables. Flushes Redis.
pub async fn setup() -> (PgPool, Client) {
    LazyLock::force(&TRACING_INIT);

    // QuestDB
    let db = platform_api::db::connect(QUESTDB_HOST)
        .await
        .expect("Failed to connect to QuestDB. Is docker compose up?");
    platform_api::db::drop_tables(&db)
        .await
        .expect("Failed to drop tables");
    platform_api::db::migrate(&db)
        .await
        .expect("Failed to migrate");
    // Wait for all tables to be ready (QuestDB may still be syncing after DDL)
    for table in &["contestants", "submission_tokens", "test_runs", "contest_summary"] {
        for attempt in 0..10 {
            match sqlx::query_scalar::<_, i64>(&format!("SELECT count() FROM {table}"))
                .fetch_one(&db).await
            {
                Ok(_) => break,
                Err(e) => {
                    if e.as_database_error()
                        .map(|de| de.message())
                        .is_some_and(|m| m.contains("table busy"))
                    {
                        if attempt == 9 {
                            panic!("Table {table} still busy after 10 retries");
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    } else {
                        panic!("Error probing table {table}: {e}");
                    }
                }
            }
        }
    }

    // Redis
    let redis = platform_api::redis::connect(REDIS_URL)
        .await
        .expect("Failed to connect to Redis");
    // Flush Redis — soft error; tests can still function with stale keys
    if let Err(e) = redis.flushall::<()>(false).await {
        tracing::warn!("[TEST-SETUP] Redis flushall failed (non-fatal): {e}");
    }

    // Seed admin config defaults — soft errors, tests that need these will fail explicitly
    for &(key, val) in &[
        ("cfg:default:rps", "30"),
        ("cfg:default:duration_secs", "15"),
    ] {
        if let Err(e) = redis.set::<(), &str, &str>(key, val, None, None, false).await {
            tracing::warn!("[TEST-SETUP] Redis set {key}={val} failed (non-fatal): {e}");
        }
    }
    for &(field, val) in &[
        ("correctness_weight", "0.40"),
        ("tps_weight", "0.35"),
        ("p99_weight", "0.25"),
    ] {
        if let Err(e) = redis.hset::<(), _, _>("config:weights", (field, val)).await {
            tracing::warn!("[TEST-SETUP] Redis hset config:weights {field}={val} failed (non-fatal): {e}");
        }
    }

    (db, redis)
}

/// Create a test app.
pub async fn test_app(db: PgPool, redis: Client, jwt_secret: &str) -> axum::Router {
    let minio = platform_api::minio::MinioClient::new("", "", "", "", "").await
        .expect("[TEST] MinioClient::new(\"\", \"\") should succeed for noop mode");
    let (tx, _) = tokio::sync::broadcast::channel::<String>(100);
    platform_api::app(
        db,
        redis,
        jwt_secret.to_string(),
        "unix:///var/run/docker.sock".to_string(),
        minio,
        "test-internal-token".to_string(),
        "infra-runner:latest".to_string(),
        tx,
        "redis://127.0.0.1:6379".to_string(),
        3, // sse_keepalive_secs — fast for tests
    )
}

/// Spawn the app on a random port, return base URL + DB pool handle.
pub async fn spawn_app() -> (String, PgPool) {
    let (db, redis) = setup().await;
    let pool = db.clone();
    let app = test_app(db.clone(), redis.clone(), "test-secret").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr()
        .expect("[TEST] get listener local addr after bind(0)");
    tokio::spawn(async move {
        axum::serve(listener, app).await
            .expect("[TEST] axum::serve failed — app panicked");
    });
    (format!("http://{addr}"), pool)
}

/// Register a contestant via the public API. Returns (contestant_id, exchange_token).
pub async fn register_contestant(addr: &str, name: &str) -> (String, String) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .expect("[TEST] register contestant request failed");
    let status = resp.status();
    let body_text = resp.text().await
        .unwrap_or_else(|e| format!("<error reading body: {e}>"));
    assert_eq!(status, 201, "register {name} returned {status}: {body_text}");
    let body: serde_json::Value = serde_json::from_str(&body_text)
        .expect("[TEST] parse register response as JSON");
    let cid = body["contestant_id"].as_str()
        .unwrap_or_else(|| panic!("[TEST] register {name}: 'contestant_id' missing in {body:?}"))
        .to_string();
    let token = body["token"].as_str()
        .unwrap_or_else(|| panic!("[TEST] register {name}: 'token' missing in {body:?}"))
        .to_string();
    (cid, token)
}

pub async fn exchange_jwt(addr: &str, token: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .expect("[TEST] exchange token request failed");
    assert_eq!(resp.status(), 200,
        "[TEST] exchange token returned {}: {}", resp.status(),
        resp.text().await.unwrap_or_else(|e| format!("<error: {e}>")));
    let body: serde_json::Value = resp.json().await
        .expect("[TEST] parse exchange response as JSON");
    body["jwt"].as_str()
        .unwrap_or_else(|| panic!("[TEST] exchange: 'jwt' missing in {body:?}"))
        .to_string()
}

/// Insert a row into contest_summary for testing.
/// Columns: ts=TIMESTAMP, contestant_id=SYMBOL, status=SYMBOL, orders_sent=LONG,
/// execs_received=LONG, correct_fills=LONG, total_fills=LONG, total_penalty=DOUBLE,
/// correctness_pct=DOUBLE, composite=DOUBLE, current_tps=DOUBLE, peak_tps=DOUBLE,
/// p99_latency_us=DOUBLE, failure_reason=SYMBOL
/// Bind ordering must exactly match VALUE positions ($1-$9).
pub async fn seed_contest_summary(
    pool: &PgPool,
    contestant_id: &str,
    correctness_pct: f64,
    composite: f64,
    current_tps: f64,
    failure_reason: Option<&str>,
    orders_sent: i64,
    total_fills: i64,
) {
    let fr = failure_reason.unwrap_or("");
    let result = platform_api::db::retry_execute(|| async {
        sqlx::query(
            "INSERT INTO contest_summary \
             (ts, contestant_id, status, orders_sent, execs_received, correct_fills, \
              total_fills, total_penalty, correctness_pct, composite, current_tps, \
              peak_tps, p99_latency_us, failure_reason) \
             VALUES (now(), $1, 'success', $2, $3, $4, $5, 0.0, $6, $7, $8, 0.0, 0.0, $9)"
        )
        .bind(contestant_id)     // $1 contestant_id (SYMBOL)
        .bind(orders_sent)       // $2 orders_sent (LONG)
        .bind(total_fills)       // $3 execs_received (LONG)
        .bind(total_fills)       // $4 correct_fills (LONG)
        .bind(total_fills)       // $5 total_fills (LONG)
        .bind(correctness_pct)   // $6 correctness_pct (DOUBLE)
        .bind(composite)         // $7 composite (DOUBLE)
        .bind(current_tps)       // $8 current_tps (DOUBLE)
        .bind(fr)                // $9 failure_reason (SYMBOL)
        .execute(pool)
        .await
    })
    .await;
    if let Err(e) = result {
        tracing::error!(
            "[TEST-SEED] contest_summary insert failed for contestant={}: {:?}. \
             SQL params: cid={}, orders={}, fills={}, correctness={}, composite={}, tps={}, reason={}",
            contestant_id, e, contestant_id, orders_sent, total_fills,
            correctness_pct, composite, current_tps, fr
        );
        panic!("seed_contest_summary failed: {e}");
    }
}

/// Insert a row into test_runs for fallback-path testing.
pub async fn seed_test_run(
    pool: &PgPool,
    run_id: &uuid::Uuid,
    contestant_id: &str,
    status: &str,
    failure_reason: Option<&str>,
) {
    let fr = failure_reason.unwrap_or("");
    platform_api::db::retry_execute(|| async {
        sqlx::query(
            "INSERT INTO test_runs (run_id, contestant_id, binary_sha256, rng_seed, \
             bot_config, platform_ver, status, peak_bots, started_at, ended_at, failure_reason) \
             VALUES ($1, $2, 'abc', 42, '{}', '0.1.0', $3, 5, now(), now(), $4)"
        )
        .bind(run_id)
        .bind(contestant_id)
        .bind(status)
        .bind(fr)
        .execute(pool)
        .await
    })
    .await
    .expect("seed test_run");
}
