mod common;

/// Helper: create contestant and extract contestant_id + upload token.
async fn create_contestant(addr: &str) -> (String, String) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": "AuthTest"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let contestant_id = body["contestant_id"].as_str().unwrap().to_string();
    let token = body["token"].as_str().unwrap().to_string();
    (contestant_id, token)
}

/// Helper: exchange upload token for JWT.
async fn exchange_for_jwt(addr: &str, token: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn exchange_token_returns_jwt() {
    let (addr, _pool) = common::spawn_app().await;
    let (contestant_id, token) = create_contestant(&addr).await;

    // Exchange token for JWT
    let resp = reqwest::Client::new()
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({ "token": &token }))
        .send()
        .await
        .expect("exchange request failed");
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    eprintln!("[TEST-DEBUG] exchange status={} body={:?}", status, body);
    assert_eq!(status, 200, "expected 200, got {status}: {body:?}");
    let jwt = body["token"].as_str().expect("JWT not in response").to_string();
    assert!(jwt.len() > 20);

    // Use JWT to call protected endpoint
    let resp = reqwest::Client::new()
        .get(format!("{addr}/api/contestants/{contestant_id}"))
        .bearer_auth(&jwt)
        .send()
        .await
        .expect("protected endpoint failed");
    assert_eq!(resp.status(), 200, "Protected endpoint returned {}", resp.status());
}

#[tokio::test]
async fn reuse_token_returns_401() {
    let (addr, _pool) = common::spawn_app().await;
    let (_, token) = create_contestant(&addr).await;

    // First exchange succeeds
    let _ = exchange_for_jwt(&addr, &token).await;

    // Second exchange fails
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn protected_endpoint_requires_auth() {
    let (addr, _pool) = common::spawn_app().await;
    let (contestant_id, _) = create_contestant(&addr).await;
    let (_cid2, token2) = create_contestant(&addr).await;
    let jwt2 = exchange_for_jwt(&addr, &token2).await;

    let client = reqwest::Client::new();

    // No auth header → 401
    let resp = client
        .get(format!("{addr}/api/contestants/{contestant_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Bad JWT → 401
    let resp = client
        .get(format!("{addr}/api/contestants/{contestant_id}"))
        .bearer_auth("invalid.jwt.token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Valid JWT for different contestant — should still work (auth passes, but we check own data)
    let resp = client
        .get(format!("{addr}/api/contestants/{contestant_id}"))
        .bearer_auth(&jwt2)
        .send()
        .await
        .unwrap();
    // Valid JWT for different contestant — auth passes, returns 200 (public data)
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn invalid_exchange_token_returns_401() {
    let (addr, _pool) = common::spawn_app().await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": "not-a-real-uuid"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}
