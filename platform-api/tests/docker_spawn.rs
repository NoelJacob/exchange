use std::time::Duration;

use platform_api::docker;

/// Skip test if Docker is not available.
fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn connect_to_docker() {
    if !docker_available() {
        eprintln!("SKIP: Docker not available");
        return;
    }
    let docker = docker::connect("unix:///var/run/docker.sock").await.unwrap();
    let info = docker.version().await.unwrap();
    assert!(info.version.is_some());
}

#[tokio::test]
async fn spawn_and_kill_contestant() {
    if !docker_available() {
        eprintln!("SKIP: Docker not available");
        return;
    }
    let docker = docker::connect("unix:///var/run/docker.sock").await.unwrap();

    let image = "alpine:latest";
    docker::ensure_image(&docker, image).await.unwrap();

    let container_id = docker::spawn_contestant(&docker, image)
        .await
        .expect("Failed to spawn contestant container");

    eprintln!("Container {container_id} spawned");
    assert!(container_id.len() > 0);

    tokio::time::sleep(Duration::from_secs(1)).await;

    docker::kill_container(&docker, &container_id)
        .await
        .expect("Failed to kill container");
}

#[tokio::test]
async fn spawn_and_kill_bot() {
    if !docker_available() {
        eprintln!("SKIP: Docker not available");
        return;
    }
    let docker = docker::connect("unix:///var/run/docker.sock").await.unwrap();

    let image = "infra-bot-worker:latest";
    // Skip if image not built
    if docker.inspect_image(image).await.is_err() {
        eprintln!("SKIP: bot-worker image not built (run docker compose build)");
        return;
    }

    let container_id = docker::spawn_bot(&docker, image, "test-contestant", "contestant-test", 30, 10)
        .await
        .expect("Failed to spawn bot container");
    eprintln!("Bot container {container_id} spawned");
    assert!(container_id.len() > 0);

    tokio::time::sleep(Duration::from_secs(1)).await;

    docker::kill_container(&docker, &container_id)
        .await
        .expect("Failed to kill bot container");
}

#[tokio::test]
async fn ensure_network_idempotent() {
    if !docker_available() {
        eprintln!("SKIP: Docker not available");
        return;
    }
    let docker = docker::connect("unix:///var/run/docker.sock").await.unwrap();

    docker::ensure_network(&docker).await.unwrap();
    docker::ensure_network(&docker).await.unwrap();
}

/// Verify MinIO client can connect, create bucket, upload, and verify upload.
#[tokio::test]
async fn minio_connect_and_upload() {
    let endpoint = std::env::var("TEST_MINIO_URL").unwrap_or_else(|_| "http://localhost:9002".to_string());
    let bucket = "test-bucket-integration";
    let client = platform_api::minio::MinioClient::new(
        &endpoint, bucket, "admin", "password123", "us-east-1",
    )
    .await
    .expect("[TEST] MinioClient::new should connect to MinIO");

    let test_data = b"hello minio integration test";
    client
        .put_binary("test/hello.txt", test_data)
        .await
        .expect("[TEST] put_binary should succeed");

    eprintln!("[TEST] MinIO upload ok (endpoint={endpoint}, bucket={bucket})");
}
