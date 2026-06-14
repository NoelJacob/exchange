mod common;

use serde_json::Value;
use platform_api::db::retry_execute;
fn client() -> reqwest::Client {
    reqwest::Client::new()
}


/// Full pipeline: register → exchange → submit → status → contestant detail → leaderboard
#[tokio::test]
async fn full_lifecycle_pipeline() {
    let (addr, pool) = common::spawn_app().await;

    // 1. Register contestant (public endpoint)
    let resp = client()
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": "E2EUser"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    let contestant_id = body["contestant_id"].as_str().unwrap().to_string();
    let token = body["token"].as_str().unwrap().to_string();
    assert!(!contestant_id.is_empty());
    assert!(!token.is_empty());

    // 2. Exchange token for JWT
    let resp = client()
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let jwt = body["token"].as_str().unwrap().to_string();
    assert!(jwt.len() > 20, "JWT should be a reasonable token");

    // 3. Submit binary via multipart — triggers auto-deploy pipeline
    let form = reqwest::multipart::Form::new()
        .part("binary", reqwest::multipart::Part::bytes(b"e2e binary data"));
    let resp = client()
        .post(format!("{addr}/api/contestant/submit"))
        .bearer_auth(&jwt)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    let run_id = body["run_id"].as_str().unwrap().to_string();
    assert_eq!(body["status"].as_str().unwrap(), "uploaded");
    assert!(body["binary_sha256"].as_str().is_some());

    // 4. Get own status — latest test run details
    let resp = client()
        .get(format!("{addr}/api/contestant/status"))
        .bearer_auth(&jwt)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["run_id"].as_str().unwrap(), run_id);
    assert_eq!(body["contestant_id"].as_str().unwrap(), contestant_id);
    let status = body["status"].as_str().unwrap();
    assert!(
        ["uploaded", "building", "starting", "running"].contains(&status),
        "expected early-lifecycle status, got {status}"
    );

    // 5. Get contestant details — should include the test run
    let resp = client()
        .get(format!("{addr}/api/contestants/{contestant_id}"))
        .bearer_auth(&jwt)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["contestant_id"].as_str().unwrap(), contestant_id);
    assert_eq!(body["name"].as_str().unwrap(), "E2EUser");
    let runs = body["test_runs"].as_array().unwrap();
    assert!(
        runs.iter().any(|r| r["run_id"].as_str() == Some(&run_id)),
        "contestant details should list the test run"
    );

    // 6. Leaderboard should be empty (no completed runs yet)
    let resp = client()
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let before = body["leaderboard"].as_array().unwrap().len();

    // Insert a completed test run directly for leaderboard verification
    let completed_id = uuid::Uuid::new_v4();
    retry_execute(|| {
        sqlx::query(
            "INSERT INTO test_runs (run_id, contestant_id, binary_sha256, rng_seed, \
             bot_config, platform_ver, status, peak_bots, started_at, ended_at) \
             VALUES ($1, $2, 'xyz', 42, '{}', '0.1.0', 'success', 10, now(), now())",
        )
        .bind(completed_id)
        .bind(&contestant_id)
        .execute(&pool)
    })
    .await
    .expect("INSERT should succeed after retries");

    // 7. Leaderboard now has the new completed run
    let resp = client()
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let after = body["leaderboard"].as_array().unwrap();
    assert_eq!(after.len(), before + 1, "leaderboard should gain one entry");
    assert!(
        after.iter().any(|e| e["run_id"].as_str() == Some(&completed_id.to_string())),
        "completed run should appear in leaderboard"
    );

    // 8. Token reuse should fail (single-use upload token)
    let resp = client()
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "reused token should be rejected");
}

/// Protected endpoints reject unauthenticated requests
#[tokio::test]
async fn protected_endpoints_require_auth() {
    let (addr, _pool) = common::spawn_app().await;

    // GET /api/contestants/:id without auth
    let resp = client()
        .get(format!("{addr}/api/contestants/some-id"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // POST /api/contestant/submit without auth
    let resp = client()
        .post(format!("{addr}/api/contestant/submit"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // GET /api/contestant/status without auth
    let resp = client()
        .get(format!("{addr}/api/contestant/status"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

/// Public endpoints work without authentication
#[tokio::test]
async fn public_endpoints_work_without_auth() {
    let (addr, _pool) = common::spawn_app().await;

    // GET /health — always public
    let resp = client()
        .get(format!("{addr}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET /api/leaderboard — public
    let resp = client()
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // POST /api/auth/exchange with bad token — public endpoint, returns 401 for invalid data
    let resp = client()
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": "invalid"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "exchange with bad token returns 401");

    // POST /api/contestants — public registration
    let resp = client()
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": "PublicTest"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
}
