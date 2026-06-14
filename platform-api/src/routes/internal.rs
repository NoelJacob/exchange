use std::sync::Arc;

use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

use crate::handlers::contestants::AppState;
use crate::db::retry_execute;

#[derive(Serialize)]
pub struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
pub struct RunnerReady {
    run_id: String,
}

#[derive(Deserialize)]
pub struct RunnerFailed {
    run_id: String,
    error: String,
}

#[derive(Deserialize)]
pub struct RunnerExited {
    run_id: String,
    exit_code: i32,
}

pub async fn inner_runner_ready(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RunnerReady>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let run_id = &body.run_id;
    tracing::info!("[RUNNER-READY] run_id={run_id} — runner signalled ready");

    tracing::info!("[RUNNER-READY-STEP1] Querying test_runs for {run_id}");
    let row = match sqlx::query("SELECT contestant_id, rps, duration_secs FROM test_runs WHERE run_id=$1")
        .bind(run_id).fetch_optional(&state.db).await
    {
        Ok(Some(r)) => (r.get::<String, _>("contestant_id"), r.get::<i32, _>("rps"), r.get::<i32, _>("duration_secs")),
        Ok(None) => return Err((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "not found".into() }))),
        Err(e) => {
            tracing::error!("[RUNNER-READY-DBERR] {run_id} DB query failed: {e}");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") })));
        }
    };
    let (contestant_id, rps_i32, duration_secs_i32) = row;
    let rps = rps_i32 as u32;
    let duration_secs = duration_secs_i32 as u32;
    tracing::info!("[RUNNER-READY-STEP1] contestant_id={contestant_id} rps={rps} dur={duration_secs}");

    tracing::info!("[RUNNER-READY-STEP2] Updating status to running");
    {
        let _guard = state.questdb_write_lock.lock().await;
        if let Err(e) = sqlx::query("UPDATE test_runs SET status='running' WHERE run_id=$1")
            .bind(run_id).execute(&state.db).await
        {
            tracing::error!("[RUNNER-READY-DBERR] status update failed: {e}");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") })));
        }
    }
    tracing::info!("[RUNNER-READY-STEP2] status=running set");

    tracing::info!("[RUNNER-READY-STEP3] Connecting to Docker...");
    let docker = match crate::docker::connect(&state.docker_url).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!("[RUNNER-READY-DOCKER] connect failed: {e}");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("conn: {e}") })));
        }
    };
    tracing::info!("[RUNNER-READY-STEP3] Docker connected");

    tracing::info!("[RUNNER-READY-STEP4] Spawning bot-worker for {contestant_id}");
    let target_host = format!("contestant-{run_id}");
    let bot_id = match crate::docker::spawn_bot(&docker, "infra-bot-worker:latest", &contestant_id, &target_host, rps, duration_secs).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("[RUNNER-READY-BOTERR] spawn_bot failed: {e}");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("bot: {e}") })));
        }
    };
    tracing::info!("[RUNNER-READY-STEP4] bot={bot_id} spawned");

    if let Err(e) = retry_execute(|| async {
        sqlx::query("UPDATE test_runs SET bot_container_id=$1 WHERE run_id=$2")
            .bind(&bot_id).bind(run_id).execute(&state.db).await
    }).await {
        tracing::error!("[RUNNER-START] UPDATE test_runs bot_container_id failed: {e}");
    }

    let db2 = state.db.clone();
    let rid2 = run_id.to_string();
    let docker2 = docker.clone();
    let cid2: Option<String> = sqlx::query_scalar("SELECT contestant_container_id FROM test_runs WHERE run_id=$1")
        .bind(run_id).fetch_optional(&state.db).await
        .unwrap_or_else(|e| {
            tracing::error!("[RUNNER-WAITER] query contestant_container_id for {run_id} failed: {e}");
            None
        }).flatten();
    let bot_id_clone = bot_id.clone();
    tokio::spawn(async move {
        use bollard::container::WaitContainerOptions;
        use futures_util::StreamExt;
        tracing::info!("[RUNNER-WAITER] {rid2} waiting for bot to exit...");
        let exit_result: Vec<_> = docker2.wait_container(bot_id_clone.as_str(), None::<WaitContainerOptions<&str>>).collect().await;
        let exit_code: i64 = exit_result.iter().find_map(|r| {
            match r {
                Ok(status) => Some(status.status_code),
                Err(e) => {
                    tracing::error!("[RUNNER-WAITER] {rid2} bot wait error: {e}");
                    None
                }
            }
        }).unwrap_or(0);
        tracing::info!("[RUNNER-WAITER] {rid2} bot exit code={exit_code}");
        let _ = crate::docker::kill_container(&docker2, &bot_id_clone).await;
        if let Some(ref cid) = cid2 {
            let _ = crate::docker::kill_container(&docker2, cid).await;
        }
        if exit_code != 0 {
            tracing::warn!("[RUNNER-WAITER] {rid2} bot crashed with exit code {exit_code}");
            if let Err(e) = retry_execute(|| async {
                sqlx::query("UPDATE test_runs SET status='failed', failure_reason='crashed', ended_at=now() WHERE run_id=$1 AND status='running'")
                    .bind(&rid2).execute(&db2).await
            }).await {
                tracing::error!("[RUNNER-WAITER] UPDATE test_runs crashed failed: {e}");
            }
        } else {
            tracing::info!("[RUNNER-WAITER] {rid2} status=success (waiter)");
            if let Err(e) = retry_execute(|| async {
                sqlx::query("UPDATE test_runs SET status='success', ended_at=now() WHERE run_id=$1 AND status='running'")
                    .bind(&rid2).execute(&db2).await
            }).await {
                tracing::error!("[RUNNER-WAITER] UPDATE test_runs success failed: {e}");
            }
        }
    });
    Ok(Json(json!({"status": "running", "bot_id": bot_id})))
}

pub async fn inner_runner_failed(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RunnerFailed>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    tracing::error!("[RUNNER-FAILED] run_id={} error={}", body.run_id, body.error);
    if let Err(e) = retry_execute(|| async {
        sqlx::query("UPDATE test_runs SET status='failed', failure_reason=$1 WHERE run_id=$2")
            .bind(&body.error).bind(&body.run_id).execute(&state.db).await
    }).await {
        tracing::error!("[RUNNER-FAILED] UPDATE test_runs failed: {e}");
    }
    Ok(Json(json!({"status": "failed"})))
}

pub async fn inner_runner_exited(
    State(_state): State<Arc<AppState>>,
    Json(body): Json<RunnerExited>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!("[RUNNER-EXITED] run_id={} exit_code={}", body.run_id, body.exit_code);
    Ok(Json(json!({"status": "acknowledged"})))
}

/// Build internal routes.
pub fn internal_routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/internal/runner-ready", post(inner_runner_ready))
        .route("/api/internal/runner-failed", post(inner_runner_failed))
        .route("/api/internal/runner-exited", post(inner_runner_exited))
        .with_state(state)
}
