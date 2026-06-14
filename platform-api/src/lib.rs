pub mod auth;
pub mod config;
pub mod db;
pub mod docker;
pub mod handlers;
pub mod minio;
pub mod redis;
pub mod leaderboard_relay;
pub mod routes;

use std::sync::Arc;

use axum::{Json, Router, extract::State, middleware};
use axum::http::StatusCode;
use axum::http::HeaderValue;
use tower_http::cors::{Any, CorsLayer};
use serde_json::{Value, json};
use uuid::Uuid;

use handlers::contestants::ErrorResponse;
use fred::prelude::*;
use fred::interfaces::KeysInterface;
use sqlx::PgPool;
use crate::db::retry_execute;

use auth::auth_middleware;
use handlers::contestants::AppState;
use handlers::leaderboard::leaderboard_routes;

/// Build the application router with shared state.
pub fn app(
    db: PgPool,
    redis: Client,
    jwt_secret: String,
    docker_url: String,
    minio: minio::MinioClient,
    internal_token: String,
    runner_image: String,
    leaderboard_tx: tokio::sync::broadcast::Sender<String>,
    redis_url: String,
    sse_keepalive_secs: u64,
) -> Router {
    // Clone before moving into state
    let relay_tx = leaderboard_tx.clone();
    let relay_url = redis_url.clone();

    let state = Arc::new(AppState {
        db,
        redis,
        jwt_secret,
        docker_url,
        minio,
        internal_token,
        runner_image,
        sse_keepalive_secs,
        leaderboard_tx,
        questdb_write_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    });

    // Spawn SSE relay (background task subscribing to Redis, uses its own connection)
    tokio::spawn(async move {
        leaderboard_relay::run(relay_tx, &relay_url).await;
    });

    // Internal runner callbacks (validated via X-Internal-Token)
    let internal = Router::<Arc<AppState>>::new()
        .route("/api/internal/runner-ready", axum::routing::post(routes::internal::inner_runner_ready))
        .route("/api/internal/runner-failed", axum::routing::post(routes::internal::inner_runner_failed))
        .route("/api/internal/runner-exited", axum::routing::post(routes::internal::inner_runner_exited));

    // Admin endpoints (validated via X-Admin-Password)
    let admin = routes::admin::admin_routes();

    // Contestant endpoints (JWT required)
    let contestant = Router::<Arc<AppState>>::new()
        .merge(routes::contestant::contestant_routes())
        .route("/api/contestants/{id}", axum::routing::get(routes::contestant::get_contestant));

    // Public endpoints (no auth)
    let public = Router::<Arc<AppState>>::new()
        .route("/health", axum::routing::get(health))
        .route("/api/contestants", axum::routing::post(public_register))
        .merge(leaderboard_routes())
        .merge(auth::auth_routes());
    let cors = CorsLayer::new()
        .allow_origin("http://localhost:5173".parse::<HeaderValue>().expect("static URL is valid"))
        .allow_methods(Any)
        .allow_headers(Any);

    Router::<Arc<AppState>>::new()
        .merge(internal.layer(middleware::from_fn_with_state(state.clone(), auth::internal_auth_middleware)))
        .merge(admin.layer(middleware::from_fn_with_state(state.clone(), auth::admin_auth_middleware)))
        .merge(contestant.layer(middleware::from_fn_with_state(state.clone(), auth_middleware)))
        .merge(public)
        .layer(cors)
        .with_state(state)
}

/// Public contestant registration (returns upload token, no JWT).
async fn public_register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<routes::admin::RegisterRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<ErrorResponse>)> {
    let cid = Uuid::new_v4().to_string();
    let token = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().naive_utc();

    retry_execute(|| async {
        sqlx::query("INSERT INTO contestants (contestant_id, name, created_at) VALUES ($1, $2, $3)")
            .bind(&cid).bind(&req.name).bind(now)
            .execute(&state.db).await
    }).await
        .map_err(|e| {
            tracing::error!("[REGISTER] failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") }))
        })?;

    use crate::redis::token_key;
    let _: () = state.redis.set(&token_key(&token), &cid, None, None, false).await
        .unwrap_or_else(|e| {
            tracing::warn!("[REGISTER] Redis token write failed (non-fatal): {e}");
            Default::default()
        });
    if let Err(e) = retry_execute(|| async {
        sqlx::query("INSERT INTO submission_tokens (token, contestant_id, used) VALUES ($1, $2, false)")
            .bind(Uuid::parse_str(&token).expect("generated UUID is always valid"))
            .bind(&cid)
            .execute(&state.db).await
    }).await {
        tracing::warn!("[REGISTER] submission_tokens insert failed (non-fatal): {e}");
    }

    tracing::info!("[REGISTER] contestant={cid} name={}", req.name);
    Ok((StatusCode::CREATED, Json(json!({
        "contestant_id": cid,
        "name": req.name,
        "token": token,
        "created_at": now.and_utc().to_rfc3339(),
    }))))
}

async fn health() -> axum::Json<serde_json::Value> {
    serde_json::json!({"status": "ok"}).into()
}
