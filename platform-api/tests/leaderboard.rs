use fred::interfaces::PubsubInterface;
mod common;

use serde_json::Value;
use std::time::Duration;

// ─── Phase C: contest_summary primary path ────────────────────────────

#[tokio::test]
async fn leaderboard_contest_summary_path() {
    let (addr, pool) = common::spawn_app().await;
    let (cid, _token) = common::register_contestant(&addr, "Alice").await;

    common::seed_contest_summary(&pool, &cid, 95.0, 0.85, 120.0, None, 1000, 800).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard failed");
    assert_eq!(resp.status(), 200,
        "[TEST] expected 200, got {}", resp.status());
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be an array");
    eprintln!("[DEBUG] leaderboard response: {body:?}");
    assert_eq!(entries.len(), 1,
        "expected 1 entry from contest_summary, got {}", entries.len());
    let entry = &entries[0];
    assert_eq!(entry["name"].as_str(), Some("Alice"));
    assert_eq!(entry["contestant_id"].as_str(), Some(cid.as_str()));
    assert_eq!(entry["correctness_pct"].as_f64(), Some(95.0),
        "correctness_pct mismatch");
    assert_eq!(entry["composite"].as_f64(), Some(0.85),
        "composite mismatch");
    assert_eq!(entry["current_tps"].as_f64(), Some(120.0),
        "current_tps mismatch");
    assert!(entry["failure_reason"].is_null(),
        "failure_reason should be null");
    assert_eq!(entry["orders_sent"].as_i64(), Some(1000),
        "orders_sent mismatch");
    assert_eq!(entry["total_fills"].as_i64(), Some(800),
        "total_fills mismatch");
}

#[tokio::test]
async fn leaderboard_orders_by_composite_desc() {
    let (addr, pool) = common::spawn_app().await;
    let (cid_a, _) = common::register_contestant(&addr, "Alpha").await;
    let (cid_b, _) = common::register_contestant(&addr, "Beta").await;
    let (cid_c, _) = common::register_contestant(&addr, "Gamma").await;

    common::seed_contest_summary(&pool, &cid_a, 80.0, 0.90, 100.0, None, 500, 400).await;
    common::seed_contest_summary(&pool, &cid_b, 70.0, 0.75, 80.0, None, 400, 300).await;
    common::seed_contest_summary(&pool, &cid_c, 60.0, 0.60, 60.0, None, 300, 200).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be array");
    assert_eq!(entries.len(), 3,
        "expected 3 entries, got {}", entries.len());

    let names: Vec<&str> = entries.iter()
        .map(|e| e["name"].as_str()
            .unwrap_or_else(|| panic!("[TEST] name missing in entry: {e:?}")))
        .collect();
    assert_eq!(names, vec!["Alpha", "Beta", "Gamma"],
        "expected composite DESC: Alpha(0.90) > Beta(0.75) > Gamma(0.60)");
}

#[tokio::test]
async fn leaderboard_composite_nulls_last() {
    let (addr, pool) = common::spawn_app().await;
    let (cid_a, _) = common::register_contestant(&addr, "Alice").await;
    let (cid_b, _) = common::register_contestant(&addr, "Bob").await;
    let (cid_c, _) = common::register_contestant(&addr, "Charlie").await;

    common::seed_contest_summary(&pool, &cid_a, 90.0, 0.80, 100.0, None, 500, 400).await;
    common::seed_contest_summary(&pool, &cid_c, 60.0, 0.50, 60.0, None, 300, 200).await;

    // Bob — insert without composite (NULL)
    let result = platform_api::db::retry_execute(|| async {
        sqlx::query(
            "INSERT INTO contest_summary (ts, contestant_id, status, orders_sent, \
             execs_received, correct_fills, total_fills, total_penalty, correctness_pct) \
             VALUES (now(), $1, 'success', 400, 300, 300, 300, 0.0, 50.0)"
        )
        .bind(&cid_b)
        .execute(&pool)
        .await
    })
    .await;
    if let Err(e) = result {
        panic!("[TEST] seed Bob without composite failed: {e}");
    }

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be array");
    assert_eq!(entries.len(), 3,
        "expected 3 entries, got {}", entries.len());

    let names: Vec<&str> = entries.iter()
        .map(|e| e["name"].as_str()
            .unwrap_or_else(|| panic!("[TEST] name missing in entry: {e:?}")))
        .collect();
    assert_eq!(names, vec!["Alice", "Charlie", "Bob"],
        "expected Alice(0.80) > Charlie(0.50) > Bob(NULL last)");

    let bob = entries.iter().find(|e| e["name"].as_str() == Some("Bob"))
        .expect("[TEST] Bob should appear in leaderboard");
    assert!(bob["composite"].is_null(),
        "Bob's composite should be NULL, got {:?}", bob["composite"]);
}

#[tokio::test]
async fn leaderboard_contest_summary_max_ts() {
    let (addr, pool) = common::spawn_app().await;
    let (cid, _) = common::register_contestant(&addr, "Dave").await;

    // Old row with low composite
    let result = platform_api::db::retry_execute(|| async {
        sqlx::query(
            "INSERT INTO contest_summary (ts, contestant_id, status, orders_sent, \
             execs_received, correct_fills, total_fills, total_penalty, correctness_pct, composite) \
             VALUES (dateadd('h', -2, now()), $1, 'success', 100, 100, 50, 100, 0.0, 50.0, 0.30)"
        )
        .bind(&cid)
        .execute(&pool)
        .await
    })
    .await;
    if let Err(e) = result {
        panic!("[TEST] seed old contest_summary row failed: {e}");
    }

    // Current row with high composite
    common::seed_contest_summary(&pool, &cid, 95.0, 0.95, 120.0, None, 500, 480).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be array");
    assert_eq!(entries.len(), 1,
        "only latest ts per contestant should appear, got {} entries", entries.len());
    let composite = entries[0]["composite"].as_f64()
        .expect("[TEST] composite should be present");
    assert!((composite - 0.95).abs() < 0.01,
        "expected latest composite 0.95, got {composite}");
}

#[tokio::test]
async fn leaderboard_contest_summary_failure_reason() {
    let (addr, pool) = common::spawn_app().await;
    let (cid_eve, _) = common::register_contestant(&addr, "Eve").await;
    let (cid_frank, _) = common::register_contestant(&addr, "Frank").await;

    common::seed_contest_summary(&pool, &cid_eve, 0.0, 0.0, 0.0, Some("crashed"), 10, 0).await;
    common::seed_contest_summary(&pool, &cid_frank, 90.0, 0.80, 100.0, None, 500, 450).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be array");

    let eve = entries.iter().find(|e| e["name"].as_str() == Some("Eve"))
        .expect("[TEST] Eve should be in leaderboard");
    let eve_reason = eve["failure_reason"].as_str();
    assert_eq!(eve_reason, Some("crashed"),
        "Eve should have failure_reason='crashed', got {eve_reason:?}");

    let frank = entries.iter().find(|e| e["name"].as_str() == Some("Frank"))
        .expect("[TEST] Frank should be in leaderboard");
    let frank_reason = frank["failure_reason"].as_str();
    assert!(frank_reason.is_none() || frank_reason == Some("") || frank_reason == Some(""),
        "Frank should have no failure_reason, got {frank_reason:?}");
}

// ─── Phase D: Fallback path edge cases ────────────────────────────────


// ─── Phase E: SSE event verification ──────────────────────────────────

#[tokio::test]
async fn sse_receives_leaderboard_event() {
    let (addr, _pool) = common::spawn_app().await;

    let client = reqwest::Client::new();
    let mut resp = client
        .get(format!("{addr}/api/events"))
        .send()
        .await
        .expect("[TEST] GET /api/events");
    assert_eq!(resp.status(), 200,
        "[SSE] expected 200, got {}", resp.status());
    let ct = resp.headers().get("content-type")
        .and_then(|v| v.to_str().ok())
        .expect("[TEST] SSE response must have content-type header with valid UTF-8");
    assert!(ct.contains("text/event-stream"),
        "[SSE] expected text/event-stream, got {ct}");

    // Publish to Redis channel — relay forwards to SSE
    let (_db, redis) = common::setup().await;
    let payload = r#"{"contestant_id":"abc","composite":0.85,"name":"SSETest"}"#;
    if let Err(e) = redis.publish::<(), _, _>("leaderboard:updates", payload).await {
        tracing::warn!("[TEST-SSE] Redis publish failed (non-fatal): {e}");
    }

    // Read SSE stream looking for the event (timeout 5s)
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match resp.chunk().await {
                Ok(Some(data)) => {
                    let text = String::from_utf8_lossy(&data);
                    if text.contains("event: leaderboard") && text.contains(payload) {
                        return true;
                    }
                }
                Ok(None) => return false,
                Err(e) => {
                    tracing::warn!("[TEST-SSE] chunk read error: {e}");
                    return false;
                }
            }
        }
    }).await;

    match found {
        Ok(true) => {} // PASS
        Ok(false) => panic!("[TEST-SSE] stream ended before leaderboard event received"),
        Err(_) => panic!("[TEST-SSE] timed out waiting for leaderboard event (5s)"),
    }
}

#[tokio::test]
async fn sse_multiple_connections_receive_events() {
    let (addr, _pool) = common::spawn_app().await;

    let client = reqwest::Client::new();
    let mut resp1 = client.get(format!("{addr}/api/events")).send().await
        .expect("[TEST] SSE conn 1");
    let mut resp2 = client.get(format!("{addr}/api/events")).send().await
        .expect("[TEST] SSE conn 2");
    assert_eq!(resp1.status(), 200);
    assert_eq!(resp2.status(), 200);

    // Publish one message
    let (_db, redis) = common::setup().await;
    let payload = r#"{"contestant_id":"multi","composite":0.75}"#;
    if let Err(e) = redis.publish::<(), _, _>("leaderboard:updates", payload).await {
        tracing::warn!("[TEST-SSE] Redis publish failed (non-fatal): {e}");
    }

    let payload_bytes = payload.as_bytes();

    // Check connection 1
    let got1 = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match resp1.chunk().await {
                Ok(Some(data)) => {
                    if data.windows(payload_bytes.len()).any(|w| w == payload_bytes) {
                        return true;
                    }
                }
                Ok(None) => return false,
                Err(e) => {
                    tracing::warn!("[TEST-SSE] conn1 chunk read error: {e}");
                    return false;
                }
            }
        }
    }).await;

    // Check connection 2
    let got2 = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match resp2.chunk().await {
                Ok(Some(data)) => {
                    if data.windows(payload_bytes.len()).any(|w| w == payload_bytes) {
                        return true;
                    }
                }
                Ok(None) => return false,
                Err(e) => {
                    tracing::warn!("[TEST-SSE] conn2 chunk read error: {e}");
                    return false;
                }
            }
        }
    }).await;

    assert!(got1 == Ok(true), "[TEST-SSE] connection 1 should receive event (got {got1:?})");
    assert!(got2 == Ok(true), "[TEST-SSE] connection 2 should receive event (got {got2:?})");
}

#[tokio::test]
async fn sse_keepalive_pings() {
    let (addr, _pool) = common::spawn_app().await;
    let client = reqwest::Client::new();
    let mut resp = client.get(format!("{addr}/api/events")).send().await
        .expect("[TEST] GET /api/events");
    assert_eq!(resp.status(), 200);

    let mut ping_count = 0;
    // Keepalive interval is 3s in tests; expect ≥2 pings within 10s
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), resp.chunk()).await {
            Ok(Ok(Some(data))) => {
                if data.windows(4).any(|w| w == b"ping") {
                    ping_count += 1;
                    if ping_count >= 2 {
                        return;
                    }
                }
            }
            Ok(Ok(None)) => {
                tracing::warn!("[TEST-SSE] stream ended early");
                break;
            }
            Ok(Err(e)) => {
                tracing::warn!("[TEST-SSE] chunk read error: {e}");
                break;
            }
            Err(_) => {} // timeout — expected between pings
        }
    }
    panic!("[TEST-SSE] expected ≥2 keepalive pings in 10s, got {ping_count}");
}

// ─── Phase F: Admin config endpoint tests ─────────────────────────────

fn admin_client() -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("X-Admin-Password", "admin123".parse()
        .expect("[TEST] parse admin123 as header value"));
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .expect("[TEST] build admin client")
}

#[tokio::test]
async fn admin_config_default_weights() {
    let (addr, _pool) = common::spawn_app().await;
    let client = admin_client();
    let resp = client
        .get(format!("{addr}/api/admin/config"))
        .send()
        .await
        .expect("[TEST] GET /api/admin/config");
    let status = resp.status();
    let body_text = resp.text().await
        .unwrap_or_else(|e| format!("<error reading body: {e}>"));
    assert_eq!(status, 200,
        "[TEST] GET admin config returned {status}: {body_text}");
    let body: Value = serde_json::from_str(&body_text)
        .expect("[TEST] parse admin config JSON");
    assert_eq!(body["rps"].as_u64(), Some(30),
        "rps mismatch in {body:?}");
    assert_eq!(body["duration_secs"].as_u64(), Some(15),
        "duration_secs mismatch in {body:?}");
    let cw = body["correctness_weight"].as_f64()
        .expect("[TEST] correctness_weight must be present in admin config");
    assert!((cw - 0.40).abs() < 0.01, "expected correctness_weight≈0.40, got {cw}");
    let tw = body["tps_weight"].as_f64()
        .expect("[TEST] tps_weight must be present in admin config");
    assert!((tw - 0.35).abs() < 0.01, "expected tps_weight≈0.35, got {tw}");
    let pw = body["p99_weight"].as_f64()
        .expect("[TEST] p99_weight must be present in admin config");
    assert!((pw - 0.25).abs() < 0.01, "expected p99_weight≈0.25, got {pw}");
}

#[tokio::test]
async fn admin_config_locked_after_first_contestant() {
    let (addr, _pool) = common::spawn_app().await;
    let admin = admin_client();

    // Register first contestant → locks config
    let resp = admin
        .post(format!("{addr}/api/admin/register"))
        .json(&serde_json::json!({"name": "LockTest"}))
        .send()
        .await
        .expect("[TEST] admin register");
    assert_eq!(resp.status(), 201,
        "[TEST] admin register returned {}", resp.status());

    // PUT should fail with 403
    let resp = admin
        .put(format!("{addr}/api/admin/config"))
        .json(&serde_json::json!({
            "rps":50, "duration_secs":30,
            "correctness_weight":0.5, "tps_weight":0.3, "p99_weight":0.2
        }))
        .send()
        .await
        .expect("[TEST] PUT admin config");
    assert_eq!(resp.status(), 403,
        "[TEST] config should be locked after registration, got {}", resp.status());

    // GET still returns original values
    let resp = admin
        .get(format!("{addr}/api/admin/config"))
        .send()
        .await
        .expect("[TEST] GET admin config after lock");
    let body: Value = resp.json().await
        .expect("[TEST] parse admin config JSON");
    assert_eq!(body["rps"].as_u64(), Some(30),
        "rps should still be default after lock, got {body:?}");
}

#[tokio::test]
async fn admin_config_set_before_lock() {
    let (addr, _pool) = common::spawn_app().await;
    let admin = admin_client();

    // Set config before any registration
    let resp = admin
        .put(format!("{addr}/api/admin/config"))
        .json(&serde_json::json!({
            "rps":50, "duration_secs":30,
            "correctness_weight":0.5, "tps_weight":0.3, "p99_weight":0.2
        }))
        .send()
        .await
        .expect("[TEST] PUT admin config");
    assert_eq!(resp.status(), 200,
        "[TEST] PUT admin config should succeed before lock, got {}", resp.status());

    // GET returns new values
    let resp = admin
        .get(format!("{addr}/api/admin/config"))
        .send()
        .await
        .expect("[TEST] GET admin config");
    let body: Value = resp.json().await
        .expect("[TEST] parse admin config JSON");
    assert_eq!(body["rps"].as_u64(), Some(50),
        "rps should be updated, got {body:?}");
    let cw = body["correctness_weight"].as_f64()
        .expect("[TEST] correctness_weight must be present after PUT");
    assert!((cw - 0.5).abs() < 0.01, "expected correctness_weight≈0.5, got {cw}");
}

// ─── Phase G: Edge cases and error handling ───────────────────────────

#[tokio::test]
async fn leaderboard_returns_200_empty() {
    let (addr, _pool) = common::spawn_app().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be array");
    assert!(entries.is_empty(),
        "empty leaderboard should be [], got {entries:?}");
}

#[tokio::test]
async fn leaderboard_zero_values() {
    let (addr, pool) = common::spawn_app().await;
    let (cid, _) = common::register_contestant(&addr, "Zero").await;

    common::seed_contest_summary(&pool, &cid, 0.0, 0.0, 0.0, None, 0, 0).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entry = &body["leaderboard"][0];
    assert_eq!(entry["correctness_pct"].as_f64(), Some(0.0),
        "correctness_pct should be 0.0");
    assert_eq!(entry["composite"].as_f64(), Some(0.0),
        "composite should be 0.0");
    assert_eq!(entry["current_tps"].as_f64(), Some(0.0),
        "current_tps should be 0.0");
}

#[tokio::test]
async fn leaderboard_contestant_without_runs() {
    let (addr, _pool) = common::spawn_app().await;
    let (_cid, _) = common::register_contestant(&addr, "NoRun").await;

    // No contest_summary or test_runs inserted
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .expect("[TEST] GET /api/leaderboard");
    let body: Value = resp.json().await
        .expect("[TEST] parse leaderboard JSON");
    let entries = body["leaderboard"].as_array()
        .expect("[TEST] leaderboard should be array");
    assert!(entries.is_empty(),
        "contestant without runs should not appear, got {entries:?}");
}

#[tokio::test]
async fn leaderboard_contest_summary_table_exists_after_migration() {
    let db = platform_api::db::connect("127.0.0.1:8812")
        .await
        .expect("[TEST] connect to QuestDB");
    platform_api::db::drop_tables(&db).await
        .expect("[TEST] drop_tables");
    platform_api::db::migrate(&db).await
        .expect("[TEST] migrate");

    let count: (i64,) = sqlx::query_as("SELECT count() FROM contest_summary")
        .fetch_one(&db)
        .await
        .expect("[TEST] contest_summary should exist after migrate");
    assert_eq!(count.0, 0,
        "contest_summary should be empty after fresh migrate");
}
