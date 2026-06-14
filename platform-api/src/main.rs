use clap::Parser;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

use platform_api::config::Config;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();
    let cfg = Config::parse();
    tracing::info!("=== PLATFORM-API STARTUP ===");
    tracing::info!("Config: port={} questdb_url={} redis_url={} docker_url={} minio_url={} bucket={} internal_token={} runner_image={}",
        cfg.port, cfg.questdb_url, cfg.redis_url, cfg.docker_url, cfg.minio_url,
        cfg.minio_bucket, cfg.internal_token, cfg.runner_image);

    // Log all relevant environment variables for debugging
    for var in &["QUESTDB_URL", "REDIS_URL", "DOCKER_HOST", "MINIO_URL",
                 "MINIO_BUCKET", "ADMIN_PASSWORD", "INTERNAL_TOKEN", "RUNNER_IMAGE",
                 "JWT_SECRET", "RUST_LOG"] {
        if let Ok(val) = std::env::var(var) {
            tracing::info!("ENV {}={}", var, val);
        } else {
            tracing::info!("ENV {} not set (will use default)", var);
        }
    }

    tracing::info!("[CONNECT] Connecting to QuestDB...");
    let db = platform_api::db::connect(&cfg.questdb_url)
        .await
        .expect("Failed to connect to QuestDB");
    tracing::info!("[CONNECT] QuestDB connected, running migrations...");
    if let Err(e) = platform_api::db::migrate(&db).await {
        tracing::warn!("[CONNECT] DB migration issue (tables may already exist): {e}");
    }
    tracing::info!("[CONNECT] Redis connecting...");
    let redis = platform_api::redis::connect(&cfg.redis_url)
        .await
        .expect("Failed to connect to Redis");
    tracing::info!("[CONNECT] Redis connected.");

    tracing::info!("[CONNECT] MinIO connecting...");
    let minio = platform_api::minio::MinioClient::new(
        &cfg.minio_url, &cfg.minio_bucket,
        &cfg.minio_access_key, &cfg.minio_secret_key, &cfg.minio_region,
    )
    .await
    .expect("Failed to connect to MinIO");
    tracing::info!("[CONNECT] MinIO connected.");
    let (leaderboard_tx, _) = tokio::sync::broadcast::channel::<String>(100);

    let sse_keepalive_secs: u64 = std::env::var("SSE_KEEPALIVE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);
    tracing::info!("SSE keepalive interval: {sse_keepalive_secs}s (set SSE_KEEPALIVE_SECS to override)");

    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("Listening on {addr}");
    axum::serve(listener, platform_api::app(
        db, redis, cfg.jwt_secret, cfg.docker_url,
        minio, cfg.internal_token, cfg.runner_image,
        leaderboard_tx, cfg.redis_url, sse_keepalive_secs,
    ))
    .await
    .unwrap();
}
