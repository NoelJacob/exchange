use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use chrono::Utc;
use fred::interfaces::KeysInterface;
use fred::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use sqlx::Row;
use crate::db::retry_execute;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: Client,
    pub jwt_secret: String,
    pub docker_url: String,
    pub minio: crate::minio::MinioClient,
    pub internal_token: String,
    pub runner_image: String,
    pub sse_keepalive_secs: u64,
    pub leaderboard_tx: tokio::sync::broadcast::Sender<String>,
    /// Serializes QuestDB UPDATEs. QuestDB (OLAP) cannot handle concurrent
    /// writes to the same table — throws AssertionError. All UPDATEs acquire
    /// this lock before executing. Reads (SELECT) are fine without it.
    pub questdb_write_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

/// Request body for creating a contestant.
#[derive(Deserialize)]
pub struct CreateContestantRequest {
    pub name: String,
}
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// POST /api/contestants — public.
async fn create_contestant(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateContestantRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<ErrorResponse>)> {
    let contestant_id = Uuid::new_v4().to_string();
    let token = Uuid::new_v4().to_string();
    let now = Utc::now();

    let pool = state.db.clone();
    let cid = contestant_id.clone();
    let name = req.name.clone();

    retry_execute(move || {
        let cid = cid.clone();
        let name = name.clone();
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO contestants (contestant_id, name, created_at) VALUES ($1, $2, $3)",
            )
            .bind(&cid)
            .bind(&name)
            .bind(now)
            .execute(&pool)
            .await
        }
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("DB error: {e}") }),
        )
    })?;

    let token_key = crate::redis::token_key(&token);
    let _ = state
        .redis
        .set::<(), _, _>(token_key, contestant_id.as_str(), Some(fred::types::Expiration::EX(259200)), None, false)
        .await;

    let pool2 = state.db.clone();
    let cid2 = contestant_id.clone();
    let token_uuid = Uuid::parse_str(&token).unwrap();

    if let Err(e) = retry_execute(move || {
        let pool2 = pool2.clone();
        let cid2 = cid2.clone();
        async move {
            sqlx::query(
                "INSERT INTO submission_tokens (token, contestant_id, created_at, expires_at, used) \
                 VALUES ($1, $2, $3, $4, false)",
            )
            .bind(token_uuid)
            .bind(&cid2)
            .bind(now)
            .bind(now + chrono::Duration::hours(72))
            .execute(&pool2)
            .await
        }
    })
    .await
    {
        tracing::warn!("[REGISTER] submission_tokens insert failed (non-fatal): {e}");
    }

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "contestant_id": contestant_id,
            "name": req.name,
            "token": token,
            "created_at": now.to_rfc3339(),
        })),
    ))
}

/// GET /api/contestants/:id — protected.
async fn get_contestant(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let row = sqlx::query("SELECT contestant_id, name, created_at FROM contestants WHERE contestant_id = $1")
        .bind(&id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: format!("DB error: {e}") }),
            )
        })?;

    match row {
        Some(r) => {
            let cid: String = r.get("contestant_id");
            let name: String = r.get("name");
            let created_at: chrono::NaiveDateTime = r.get("created_at");
            let created_at_utc: chrono::DateTime<Utc> = chrono::DateTime::from_naive_utc_and_offset(created_at, Utc);

            let test_runs = sqlx::query(
                "SELECT run_id, status, started_at, ended_at FROM test_runs WHERE contestant_id = $1 ORDER BY started_at DESC",
            )
            .bind(&id)
            .fetch_all(&state.db)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("[CONTESTANT] Failed to fetch test_runs for contestant {id}: {e}");
                Vec::new()
            });

            let runs: Vec<Value> = test_runs
                .into_iter()
                .map(|tr| {
                    json!({
                        "run_id": tr.get::<Uuid, _>("run_id").to_string(),
                        "status": tr.get::<String, _>("status"),
                        "started_at": tr.get::<Option<chrono::NaiveDateTime>, _>("started_at").map(|t| chrono::DateTime::<Utc>::from_naive_utc_and_offset(t, Utc).to_rfc3339()),
                        "ended_at": tr.get::<Option<chrono::NaiveDateTime>, _>("ended_at").map(|t| chrono::DateTime::<Utc>::from_naive_utc_and_offset(t, Utc).to_rfc3339()),
                    })
                })
                .collect();

            Ok(Json(json!({
                "contestant_id": cid,
                "name": name,
                "created_at": created_at_utc.to_rfc3339(),
                "test_runs": runs,
            })))
        }
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: "contestant not found".into() }),
        )),
    }
}

/// Public routes (no auth required).
pub fn contestant_public_routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/contestants", post(create_contestant))
        .with_state(state)
}

/// Protected routes (auth required).
pub fn contestant_protected_routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/contestants/{id}", get(get_contestant))
        .with_state(state)
}
