use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::PgQueryResult;
use std::future::Future;
use sqlx::PgPool;

/// Execute a query against QuestDB with retry on transient "table busy" errors.
///
/// QuestDB locks the table during internal sync; concurrent writes get
/// "table busy" until the lock releases. Retries 3× with exponential
/// backoff (100ms → 200ms → 400ms).
///
/// Uses `Fn() -> Fut` bound for compatibility with `tokio::spawn` and
/// other `Send`-required contexts.
///
/// # Usage
/// ```ignore
/// retry_execute(|| async {
///     sqlx::query("INSERT INTO t VALUES ($1)").bind(x).execute(&pool).await
/// }).await?;
/// ```
pub async fn retry_execute<Fut>(
    f: impl Fn() -> Fut,
) -> Result<PgQueryResult, sqlx::Error>
where
    Fut: Future<Output = Result<PgQueryResult, sqlx::Error>> + Send,
{
    let mut last_err = None;
    for attempt in 0..5 {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if attempt < 4 && is_table_busy(&e) {
                    let delay_ms = 200 << attempt; // 200, 400, 800, 1600
                    tracing::warn!("[DB] table busy, retrying (attempt {}, delay={delay_ms}ms)", attempt + 1);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    last_err = Some(e);
                } else {
                }
            }
        }
    }
    // Safety: loop returns above when attempt=2 and not table-busy,
    // or when any non-busy error occurs. This line is only reachable
    // if all 3 attempts got "table busy" — last_err is always Some.
    Err(last_err.unwrap())
}

fn is_table_busy(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .map(|de| de.message())
        .is_some_and(|m| m.contains("table busy"))
}

/// Build a connection pool to QuestDB (PostgreSQL wire protocol).
/// Retries up to 5 times with 3s delay to handle startup races.
pub async fn connect(host: &str) -> Result<PgPool, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("postgresql://admin:quest@{host}");
    let mut last_err = String::new();
    for attempt in 1..=5 {
        tracing::info!("[DB] Connecting to QuestDB at {url} (attempt {attempt}/5)");
        match PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&url)
            .await
        {
            Ok(pool) => {
                tracing::info!("[DB] Connected to QuestDB at {url}");
                return Ok(pool);
            }
            Err(e) => {
                last_err = e.to_string();
                tracing::warn!("[DB] QuestDB connection attempt {attempt} failed: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    }
    Err(format!("Failed to connect to QuestDB after 5 attempts: {last_err}").into())
}

/// Create platform-api tables in QuestDB if they do not already exist.
/// QuestDB does not support IF NOT EXISTS or PRIMARY KEY.
/// This function silently ignores "table already exists" errors.
pub async fn migrate(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Migrate: create tables if they don't exist (errors on "already exists" ignored)
    let sql = r#"
CREATE TABLE contestants (
    contestant_id TEXT,
    name TEXT,
    created_at TIMESTAMP
);
CREATE TABLE submission_tokens (
    token UUID,
    contestant_id TEXT,
    created_at TIMESTAMP,
    expires_at TIMESTAMP,
    used BOOLEAN
);
CREATE TABLE test_runs (
    run_id UUID,
    contestant_id TEXT,
    binary_sha256 TEXT,
    rng_seed BIGINT,
    bot_config TEXT,
    platform_ver TEXT,
    status TEXT,
    failure_reason TEXT,
    peak_bots INT,
    rps INT,
    duration_secs INT,
    contestant_container_id TEXT,
    bot_container_id TEXT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP
);
CREATE TABLE contest_summary (
    ts TIMESTAMP,
    contestant_id SYMBOL,
    status SYMBOL,
    orders_sent LONG,
    execs_received LONG,
    correct_fills LONG,
    total_fills LONG,
    total_penalty DOUBLE,
    correctness_pct DOUBLE,
    composite DOUBLE,
    current_tps DOUBLE,
    peak_tps DOUBLE,
    p99_latency_us DOUBLE,
    failure_reason SYMBOL
);
"#;
    for statement in sql.split(';') {
        let stmt = statement.trim();
        if stmt.is_empty() {
            continue;
        }
        match sqlx::query(stmt).execute(pool).await {
            Ok(_) => {}
            Err(e) => {
                // QuestDB returns "table already exists" for duplicate CREATE — not fatal
                let msg = e.to_string().to_lowercase();
                if msg.contains("already exists") {
                    tracing::debug!("Table already exists (ignored): {stmt}");
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(())
}

/// Drop all platform-api tables. Ignores errors (QuestDB doesn't support IF EXISTS).
pub async fn drop_tables(pool: &PgPool) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tables = ["test_runs", "submission_tokens", "contestants", "contest_summary"];
    for table in tables {
        let sql = format!("DROP TABLE {table}");
        if let Err(e) = retry_execute(|| async {
            sqlx::query(&sql).execute(pool).await
        }).await {
            tracing::warn!("[DB] DROP TABLE {table} failed (non-fatal): {e}");
        }
    }
    Ok(())
}
