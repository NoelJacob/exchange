mod common;
/// Register a contestant (public endpoint) and exchange token for a JWT.
async fn register_and_auth(addr: &str, name: &str) -> (String, String) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "failed to register contestant");
    let body: serde_json::Value = resp.json().await.unwrap();
    let contestant_id = body["contestant_id"].as_str().unwrap().to_string();
    let token = body["token"].as_str().unwrap().to_string();
    let resp = client
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "failed to exchange token");
    let body: serde_json::Value = resp.json().await.unwrap();
    let jwt = body["token"].as_str().unwrap().to_string();
    (contestant_id, jwt)
}

/// Submit a binary as a contestant, return the run_id.
async fn submit_binary(addr: &str, jwt: &str) -> String {
    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new()
        .part("binary", reqwest::multipart::Part::bytes(b"test binary data".to_vec()));
    let resp = client
        .post(format!("{addr}/api/contestant/submit"))
        .bearer_auth(jwt)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "submit failed: {:?}",
        resp.json::<serde_json::Value>().await.unwrap_or_default());
    let body: serde_json::Value = resp.json().await.unwrap();
    body["run_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn leaderboard_returns_ok() {
    // Run this first — no submit needed, avoids background task interference
    let (addr, _pool) = common::spawn_app().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/leaderboard"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["leaderboard"].is_array(),
        "leaderboard response should contain an array, got: {body:?}");
}

#[tokio::test]
async fn nonexistent_test_returns_no_test_run_status() {
    let (addr, _pool) = common::spawn_app().await;
    let (_, jwt) = register_and_auth(&addr, "NoTest").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/contestant/status"))
        .bearer_auth(&jwt)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"].as_str().unwrap(), "no_test_run");
}

#[tokio::test]
async fn submit_creates_test_run() {
    let (addr, _pool) = common::spawn_app().await;
    let (_, jwt) = register_and_auth(&addr, "LifecycleTest").await;
    let run_id = submit_binary(&addr, &jwt).await;

    // Allow background deploy task to settle (fails without Docker) before querying status
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/contestant/status"))
        .bearer_auth(&jwt)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["run_id"].as_str().unwrap(), run_id);
    // Status may be "uploaded", "building", or "failed" depending on timing of the
    // background deploy task (which fails without Docker and updates to "failed")
    let status = body["status"].as_str().unwrap();
    assert!(
        status == "uploaded" || status == "building" || status == "starting" || status == "failed",
    );
}
