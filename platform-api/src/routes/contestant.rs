use std::sync::Arc;

use axum::{Extension, Json, Router, extract::{Multipart, State}, http::StatusCode, routing::{get, post}};
use fred::interfaces::KeysInterface;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::auth::Claims;
use crate::docker::{self};
use crate::handlers::contestants::AppState;
use crate::db::retry_execute;


#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// POST /api/contestant/submit — upload binary, triggers auto-deploy.
async fn submit(
    Extension(claims): Extension<Claims>,
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<ErrorResponse>)> {
    let contestant_id = claims.sub;
    tracing::info!("[SUBMIT] starting multipart parse for contestant={contestant_id}");
    let mut binary_data: Option<Vec<u8>> = None;

    loop {
        match multipart.next_field().await {
            Ok(Some(field)) => {
                let name = field.name().unwrap_or("").to_string();
                tracing::info!("[SUBMIT] multipart field: name={name}");
                match name.as_str() {
                    "binary" => {
                        match field.bytes().await {
                            Ok(data) => {
                                tracing::info!("[SUBMIT] read binary field: {} bytes", data.len());
                                binary_data = Some(data.to_vec());
                            }
                            Err(e) => {
                                tracing::error!("[SUBMIT] binary field read error: {e}");
                                return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("binary read error: {e}") })));
                            }
                        }
                    }
                    _ => tracing::info!("[SUBMIT] unknown field: {name}"),
                }
            }
            Ok(None) => {
                tracing::info!("[SUBMIT] multipart complete");
                break;
            }
            Err(e) => {
                tracing::error!("[SUBMIT] multipart parse error: {e}");
                return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("multipart error: {e}") })));
            }
        }
    }

    let binary_data = match binary_data {
        Some(d) => d,
        None => {
            tracing::error!("[SUBMIT] missing binary field");
            return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "missing binary field".into() })));
        }
    };
    // Read rps/duration from admin config (Redis), fall back to defaults if missing
    let rps_raw: String = match state.redis.get::<String, _>("cfg:default:rps").await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("[SUBMIT] Redis get rps failed (defaulting to 30): {e}");
            "30".to_string()
        }
    };
    let rps: i32 = rps_raw.parse().unwrap_or_else(|e| {
        tracing::warn!("[SUBMIT] Invalid rps value in Redis '{rps_raw}' (defaulting to 30): {e}");
        30
    });
    let dur_raw: String = match state.redis.get::<String, _>("cfg:default:duration_secs").await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("[SUBMIT] Redis get duration failed (defaulting to 15): {e}");
            "15".to_string()
        }
    };
    let duration_secs: i32 = dur_raw.parse().unwrap_or_else(|e| {
        tracing::warn!("[SUBMIT] Invalid duration value in Redis '{dur_raw}' (defaulting to 15): {e}");
        15
    });

    tracing::info!("[SUBMIT] contestant={} using admin config rps={} duration_secs={}", contestant_id, rps, duration_secs);

    let sha256 = hex::encode(Sha256::digest(&binary_data));
    let run_id = Uuid::new_v4();
    let rng_seed = run_id.as_u128() as i64;

    // Insert test run
    retry_execute(|| async {
        sqlx::query(
            "INSERT INTO test_runs (run_id, contestant_id, binary_sha256, rng_seed, bot_config, \
             platform_ver, status, peak_bots, rps, duration_secs, started_at) \
             VALUES ($1, $2, $3, $4, '{}', '0.1.0', 'uploaded', 0, $5, $6, now())"
        )
        .bind(run_id).bind(&contestant_id).bind(&sha256).bind(rng_seed)
        .bind(rps).bind(duration_secs)
        .execute(&state.db).await
    }).await
    .map_err(|e| {
        tracing::error!("[SUBMIT] insert failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") }))
    })?;
    tracing::info!("[SUBMIT] run_id={run_id} contestant={contestant_id} sha256={}", &sha256[..12]);

    // Store locally
    let sub_dir = format!("/tmp/submissions/{run_id}");
    std::fs::create_dir_all(&sub_dir).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("fs: {e}") }))
    })?;
    std::fs::write(format!("{sub_dir}/contestant.bin"), &binary_data).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("fs: {e}") }))
    })?;

    // Store in MinIO (non-fatal — local file mount still works without it)
    match state.minio.put_binary(&format!("{run_id}/contestant.bin"), &binary_data).await {
        Ok(_) => tracing::info!("[SUBMIT] {run_id} minio=ok"),
        Err(e) => tracing::warn!("[SUBMIT] {run_id} minio upload failed (non-fatal): {e}"),
    }

    // Auto-deploy in background
    let state_clone = state.clone();
    let run_id_clone = run_id.to_string();
    tokio::spawn(async move {
        if let Err(e) = deploy_contestant(&state_clone, &run_id_clone).await {
            tracing::error!("[DEPLOY] {run_id_clone} failed: {e}");
            let msg = format!("deploy: {e}");
            if let Err(db_err) = retry_execute(|| async {
                sqlx::query("UPDATE test_runs SET status='failed', failure_reason=$1 WHERE run_id=$2")
                    .bind(&msg).bind(&run_id_clone)
                    .execute(&state_clone.db).await
            }).await {
                tracing::error!("[DEPLOY] UPDATE test_runs failed: {db_err}");
            }
        }
    });

    Ok((StatusCode::CREATED, Json(json!({
        "run_id": run_id.to_string(),
        "binary_sha256": sha256,
        "status": "uploaded"
    }))))
}

/// Deploy contestant container: spawn runner which downloads binary from MinIO via `mc`.
/// On failure, writes standardized PLAN.md failure_reason code to test_runs.
async fn deploy_contestant(state: &Arc<AppState>, run_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let fail = |code: &str| -> Box<dyn std::error::Error + Send + Sync> {
        format!("deploy_{code}").into()
    };
    {
        let _guard = state.questdb_write_lock.lock().await;
        sqlx::query("UPDATE test_runs SET status='building' WHERE run_id=$1")
            .bind(run_id).execute(&state.db).await?;
    }
    tracing::info!("[DEPLOY] {run_id} status=building");

    let docker = docker::connect(&state.docker_url).await
        .map_err(|_| fail("connect_failed"))?;
    docker::ensure_network(&docker).await
        .map_err(|_| fail("startup_failed"))?;

    let _contestant_id: String = sqlx::query_scalar("SELECT contestant_id FROM test_runs WHERE run_id=$1")
        .bind(run_id).fetch_one(&state.db).await?;

    {
        let _guard = state.questdb_write_lock.lock().await;
        sqlx::query("UPDATE test_runs SET status='starting' WHERE run_id=$1")
            .bind(run_id).execute(&state.db).await?;
    }
    // Ensure binary is in MinIO before spawning runner.
    // Retry upload in case submit handler's initial attempt failed (MinIO wasn't ready).
    let bin_path = format!("/tmp/submissions/{run_id}/contestant.bin");
    match tokio::fs::read(&bin_path).await {
        Ok(data) => {
            if let Err(e) = state.minio.put_binary(&format!("{run_id}/contestant.bin"), &data).await {
                tracing::warn!("[DEPLOY] {run_id} minio upload failed (non-fatal): {e}");
            } else {
                tracing::info!("[DEPLOY] {run_id} minio upload ok");
            }
        }
        Err(e) => {
            tracing::error!("[DEPLOY] {run_id} missing binary at {bin_path}: {e}");
            return Err(format!("deploy_missing_binary: {e}").into());
        }
    }

    tracing::info!("[DEPLOY] {run_id} creating container with env vars: RUN_ID={run_id}, PLATFORM_API_URL=http://infra-platform-api-1:8080");
    let container_name = format!("contestant-{run_id}");
    let config = bollard::container::Config {
        image: Some(state.runner_image.clone()),
        env: Some(vec![
            format!("RUN_ID={run_id}"),
            "MINIO_URL=http://minio:9000".into(),
            "MINIO_USER=admin".into(),
            "MINIO_PASSWORD=password123".into(),
            "PLATFORM_API_URL=http://infra-platform-api-1:8080".into(),
            format!("INTERNAL_TOKEN={}", state.internal_token),
        ]),
        host_config: Some(bollard::models::HostConfig {
            memory: Some(256 * 1024 * 1024),
            memory_swap: Some(256 * 1024 * 1024),
            nano_cpus: Some(1_000_000_000),
            init: Some(true),
            dns: Some(vec!["127.0.0.11".into()]),
            network_mode: Some("infra_default".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    use bollard::container::{CreateContainerOptions, StartContainerOptions};
    let id = docker.create_container(
        Some(CreateContainerOptions { name: container_name, ..Default::default() }),
        config,
    ).await.map_err(|_| fail("startup_failed"))?.id;
    docker.start_container(&id, None::<StartContainerOptions<String>>).await
        .map_err(|_| fail("startup_failed"))?;
    tracing::info!("[DEPLOY] {run_id} container_id={id} spawned");

    {
        let _guard = state.questdb_write_lock.lock().await;
        sqlx::query("UPDATE test_runs SET contestant_container_id=$1 WHERE run_id=$2")
            .bind(&id).bind(run_id).execute(&state.db).await?;
    }
    Ok(())
}

/// GET /api/contestant/status — own latest test status + metrics.
async fn contestant_status(
    Extension(claims): Extension<Claims>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let cid = claims.sub;
    tracing::info!("[STATUS] request for contestant={cid}");

    let run_row = match sqlx::query(
        "SELECT run_id, status, failure_reason, rps, \
         started_at, ended_at \
         FROM test_runs WHERE contestant_id=$1 \
         ORDER BY started_at DESC LIMIT 1"
    ).bind(&cid).fetch_optional(&state.db).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("[STATUS-ERR] DB query failed: {e}");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") })));
        }
    };
    let run = match run_row {
        Some(r) => r,
        None => return Ok(Json(json!({"contestant_id": cid, "status": "no_test_run"}))),
    };

    use sqlx::Row;
    let run_id: uuid::Uuid = run.get("run_id");
    let status: String = run.get("status");
    let rps: Option<i32> = run.get("rps");

    // Query contest_summary for metrics
    let summary = sqlx::query_as::<_, (f64, i64, i64, i64, i64)>(
        "SELECT correctness_pct, orders_sent, execs_received, correct_fills, total_fills \
         FROM contest_summary WHERE contestant_id=$1 \
         ORDER BY ts DESC LIMIT 1"
    ).bind(&cid).fetch_optional(&state.db).await;

    let metrics = match summary {
        Ok(Some((pct, sent, recv, correct, total))) => json!({
            "correctness_pct": pct,
            "orders_sent": sent,
            "execs_received": recv,
            "correct_fills": correct,
            "total_fills": total,
        }),
        _ => Value::Null,
    };


    tracing::info!("[STATUS] {cid} run_id={run_id} status={status}");
    Ok(Json(json!({
        "contestant_id": cid,
        "run_id": run_id,
        "status": status,
        "rps": rps,
        "failure_reason": run.get::<Option<String>, _>("failure_reason"),
        "started_at": run.get::<Option<chrono::NaiveDateTime>, _>("started_at")
            .map(|t| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(t, chrono::Utc).to_rfc3339()),
        "ended_at": run.get::<Option<chrono::NaiveDateTime>, _>("ended_at")
            .map(|t| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(t, chrono::Utc).to_rfc3339()),
        "metrics": metrics,
    })))
}

/// Build contestant routes.
pub fn contestant_routes() -> Router<Arc<AppState>> {
    use axum::extract::DefaultBodyLimit;
    Router::new()
        .route("/api/contestant/submit", post(submit))
        .route("/api/contestant/status", get(contestant_status))
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
}

/// GET /api/contestants/:id — get contestant details + test runs.
pub async fn get_contestant(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let row = sqlx::query("SELECT contestant_id, name, created_at FROM contestants WHERE contestant_id = $1")
        .bind(&id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| {
            tracing::error!("[GET_CONTESTANT] DB error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") }))
        })?;

    let (contestant_id, name, created_at) = match row {
        Some(r) => {
            use sqlx::Row;
            (r.get::<String, _>("contestant_id"), r.get::<String, _>("name"), r.get::<Option<chrono::NaiveDateTime>, _>("created_at"))
        }
        None => return Err((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "contestant not found".into() }))),
    };

    // Get test runs
    let runs = sqlx::query(
        "SELECT run_id, status, rps, failure_reason, started_at, ended_at \
         FROM test_runs WHERE contestant_id = $1 ORDER BY started_at DESC"
    ).bind(&contestant_id).fetch_all(&state.db).await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") }))
        })?;

    let test_runs: Vec<Value> = runs.iter().map(|r| {
        use sqlx::Row;
        json!({
            "run_id": r.get::<uuid::Uuid, _>("run_id").to_string(),
            "status": r.get::<String, _>("status"),
            "failure_reason": r.get::<Option<String>, _>("failure_reason"),
            "started_at": r.get::<Option<chrono::NaiveDateTime>, _>("started_at")
                .map(|t| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(t, chrono::Utc).to_rfc3339()),
            "ended_at": r.get::<Option<chrono::NaiveDateTime>, _>("ended_at")
                .map(|t| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(t, chrono::Utc).to_rfc3339()),
        })
    }).collect();

    Ok(Json(json!({
        "contestant_id": contestant_id,
        "name": name,
        "created_at": created_at.map(|t| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(t, chrono::Utc).to_rfc3339()),
        "test_runs": test_runs,
    })))
}
