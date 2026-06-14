mod common;

use fred::interfaces::KeysInterface;

/// Helper: create contestant and get JWT.
async fn create_and_auth(addr: &str, name: &str) -> (String, String) {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let contestant_id = body["contestant_id"].as_str().unwrap().to_string();
    let token = body["token"].as_str().unwrap().to_string();

    // Exchange for JWT
    let jwt_resp = client
        .post(format!("{addr}/api/auth/exchange"))
        .json(&serde_json::json!({"token": token}))
        .send()
        .await
        .unwrap();
    assert_eq!(jwt_resp.status(), 200);
    let jwt_body: serde_json::Value = jwt_resp.json().await.unwrap();
    let jwt = jwt_body["token"].as_str().unwrap().to_string();

    (contestant_id, jwt)
}

#[tokio::test]
async fn create_contestant_returns_201() {
    let (addr, _pool) = common::spawn_app().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": "Alice"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["contestant_id"].as_str().unwrap().len() > 0);
    assert!(body["token"].as_str().unwrap().len() > 0);
    assert_eq!(body["name"], "Alice");
    assert!(body["created_at"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn create_then_get_contestant() {
    let (addr, _pool) = common::spawn_app().await;
    let (contestant_id, jwt) = create_and_auth(&addr, "Bob").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/contestants/{contestant_id}"))
        .bearer_auth(&jwt)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["contestant_id"], contestant_id);
    assert_eq!(body["name"], "Bob");
    assert!(body["test_runs"].is_array());
}

#[tokio::test]
async fn get_nonexistent_contestant_returns_404() {
    let (addr, _pool) = common::spawn_app().await;
    let (_cid, jwt) = create_and_auth(&addr, "Dummy").await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/api/contestants/nonexistent-id"))
        .bearer_auth(&jwt)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn redis_has_token_after_creation() {
    let (db, redis) = common::setup().await;
    let app = common::test_app(db.clone(), redis.clone(), "test-secret").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let addr = format!("http://{addr}");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/contestants"))
        .json(&serde_json::json!({"name": "TokenTest"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let token = body["token"].as_str().unwrap();
    let cid = body["contestant_id"].as_str().unwrap();

    // Verify token in Redis
    let token_key = platform_api::redis::token_key(token);
    let stored: Option<String> = redis.get(&token_key).await.unwrap();
    assert_eq!(stored, Some(cid.to_string()));
}
