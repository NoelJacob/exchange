mod common;

use platform_api::db;
use sqlx::Row;
use uuid::Uuid;

#[tokio::test]
async fn migrate_creates_tables() {
    let pool = db::connect("127.0.0.1:8812").await.unwrap();
    db::drop_tables(&pool).await.unwrap();
    db::migrate(&pool).await.unwrap();

    let rows = sqlx::query("SELECT count() AS cnt FROM contestants")
        .fetch_one(&pool)
        .await
        .unwrap();
    let cnt: i64 = rows.get("cnt");
    assert_eq!(cnt, 0);
}

#[tokio::test]
async fn contestants_roundtrip() {
    let (pool, _redis) = common::setup().await;

    let cid = "test-contestant";
    sqlx::query("INSERT INTO contestants (contestant_id, name, created_at) VALUES ($1, $2, now())")
        .bind(cid)
        .bind("Alice")
        .execute(&pool)
        .await
        .unwrap();

    let row =
        sqlx::query("SELECT contestant_id, name FROM contestants WHERE contestant_id = $1")
            .bind(cid)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.get::<String, _>("contestant_id"), cid);
    assert_eq!(row.get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn submission_tokens_roundtrip() {
    let (pool, _redis) = common::setup().await;

    let token = Uuid::new_v4();
    let cid = "test-cid";
    sqlx::query(
        "INSERT INTO submission_tokens (token, contestant_id, created_at, expires_at, used) \
         VALUES ($1, $2, now(), now() + 10000000, false)",
    )
    .bind(token)
    .bind(cid)
    .execute(&pool)
    .await
    .unwrap();

    let row =
        sqlx::query("SELECT token, contestant_id, used FROM submission_tokens WHERE token = $1")
            .bind(token)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.get::<Uuid, _>("token"), token);
    assert_eq!(row.get::<String, _>("contestant_id"), cid);
    assert!(!row.get::<bool, _>("used"));
}

#[tokio::test]
async fn test_runs_roundtrip() {
    let (pool, _redis) = common::setup().await;

    let run_id = Uuid::new_v4();
    let cid = "test-cid";
    let config = r#"{"initial_bots":1,"rps":30}"#;
    db::retry_execute(|| {
        sqlx::query(
            "INSERT INTO test_runs (run_id, contestant_id, binary_sha256, rng_seed, bot_config, \
             platform_ver, status, peak_bots, started_at) \
             VALUES ($1, $2, $3, 42, $4, '0.1.0', 'uploaded', 1, now())",
        )
        .bind(run_id)
        .bind(cid)
        .bind("abc123")
        .bind(config)
        .execute(&pool)
    })
    .await
    .expect("INSERT should succeed after retries");

    let row = sqlx::query(
        "SELECT run_id, contestant_id, status, peak_bots FROM test_runs WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<Uuid, _>("run_id"), run_id);
    assert_eq!(row.get::<String, _>("contestant_id"), cid);
    assert_eq!(row.get::<String, _>("status"), "uploaded");
    assert_eq!(row.get::<i32, _>("peak_bots"), 1);
}
