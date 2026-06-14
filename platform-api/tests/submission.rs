mod common;
/// Helper: register via admin to get a JWT.
async fn admin_register_jwt(addr: &str, name: &str) -> String {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/admin/register"))
        .header("X-Admin-Password", "admin123")
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "admin register failed for {name}");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["jwt"].as_str().unwrap_or_else(|| body["token"].as_str().unwrap()).to_string()
}

#[tokio::test]
async fn submit_binary_creates_test_run() {
    let (addr, _pool) = common::spawn_app().await;
    let jwt = admin_register_jwt(&addr, "SubmitTest").await;

    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new()
        .part("binary", reqwest::multipart::Part::bytes(vec![0u8; 1024]).file_name("contestant.bin"));

    let resp = client
        .post(format!("{addr}/api/contestant/submit"))
        .bearer_auth(&jwt)
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["run_id"].as_str().unwrap().len() > 0);
    assert_eq!(body["status"], "uploaded");
    assert!(body["binary_sha256"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn submit_without_auth_returns_401() {
    let (addr, _pool) = common::spawn_app().await;

    let client = reqwest::Client::new();
    let form = reqwest::multipart::Form::new()
        .part("binary", reqwest::multipart::Part::bytes(vec![0u8; 64]).file_name("contestant.bin"));

    let resp = client
        .post(format!("{addr}/api/contestant/submit"))
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn submit_without_binary_returns_400() {
    let (addr, _pool) = common::spawn_app().await;
    let jwt = admin_register_jwt(&addr, "NoBinaryTest").await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{addr}/api/contestant/submit"))
        .bearer_auth(&jwt)
        .json(&serde_json::json!({"unrelated": true}))  // no binary field
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}
