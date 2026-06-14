mod common;


#[tokio::test]
async fn health_returns_200() {
    let (db, redis) = common::setup().await;
    let app = common::test_app(db, redis, "test-secret").await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let addr = format!("http://{addr}");

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{addr}/health"))
        .send()
        .await
        .expect("Failed to GET /health");

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}
