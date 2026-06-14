use std::sync::Arc;

use axum::{Json, Router, extract::State, http::StatusCode, routing::{get, post}};
use fred::interfaces::{KeysInterface, HashesInterface};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::handlers::contestants::AppState;
use crate::db::retry_execute;

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize)]
pub struct RegisterRequest {
    pub name: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct AdminConfig {
    pub rps: Option<u32>,
    pub duration_secs: Option<u32>,
    pub max_rps: Option<u32>,
    pub p99_hard_limit_ms: Option<f64>,
    pub correctness_weight: Option<f64>,
    pub tps_weight: Option<f64>,
    pub p99_weight: Option<f64>,
}

/// POST /api/admin/register — create contestant + return JWT. Locks config after first use.
/// Contestants inherit rps/duration from the admin config in Redis.
async fn admin_register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<ErrorResponse>)> {
    // Check if config is locked (a contestant was already created)
    let locked: Option<String> = state.redis.get("config:locked").await
        .map_err(|e| {
            tracing::error!("[ADMIN] Redis check config:locked failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;

    // Read rps/duration from admin config (Redis), fall back to defaults
    let rps_raw: String = state.redis.get("cfg:default:rps").await
        .map_err(|e| {
            tracing::error!("[ADMIN] Redis get rps failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    let rps: u32 = rps_raw.parse().map_err(|e| {
        tracing::error!("[ADMIN] Invalid rps value in Redis: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid rps: {e}") }))
    })?;
    let duration_raw: String = state.redis.get("cfg:default:duration_secs").await
        .map_err(|e| {
            tracing::error!("[ADMIN] Redis get duration failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    let duration_secs: u32 = duration_raw.parse().map_err(|e| {
        tracing::error!("[ADMIN] Invalid duration value in Redis: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid duration: {e}") }))
    })?;
    let now = chrono::Utc::now().naive_utc();
    let cid = Uuid::new_v4().to_string();
    let token = Uuid::new_v4().to_string();
    let db = state.db.clone();
    let cid1 = cid.clone();
    let name = req.name.clone();
    // Insert contestant
    retry_execute(move || {
        let cid = cid1.clone();
        let name = name.clone();
        let db = db.clone();
        async move {
            sqlx::query("INSERT INTO contestants (contestant_id, name, created_at) VALUES ($1, $2, $3)")
                .bind(&cid).bind(&name).bind(now)
                .execute(&db).await
        }
    }).await
    .map_err(|e| {
        tracing::error!("[ADMIN] register failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") }))
    })?;

    // Store token in Redis + DB
    use crate::redis::token_key;
    state.redis.set::<(), _, _>(&token_key(&token), &cid, None, None, false).await
        .map_err(|e| {
            tracing::error!("[ADMIN] Redis set token failed: {e}");
            // non-fatal — DB fallback exists
            tracing::warn!("[ADMIN] Continuing despite Redis token write failure");
        }).ok();
    let db2 = state.db.clone();
    let cid2 = cid.clone();
    let token_uuid = Uuid::parse_str(&token).expect("generated UUID is valid");
    if let Err(e) = retry_execute(move || {
        let cid = cid2.clone();
        let db = db2.clone();
        async move {
            sqlx::query("INSERT INTO submission_tokens (token, contestant_id, used) VALUES ($1, $2, false)")
                .bind(token_uuid)
                .bind(&cid)
                .execute(&db).await
        }
    }).await {
        tracing::warn!("[ADMIN] submission_tokens insert failed (non-fatal): {e}");
    }

    // Sign JWT
    let claims = crate::auth::Claims {
        sub: cid.clone(),
        exp: (chrono::Utc::now().timestamp() as usize) + 3600,
        iat: chrono::Utc::now().timestamp() as usize,
    };
    let jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(state.jwt_secret.as_bytes()),
    ).map_err(|e| {
        tracing::error!("[ADMIN] jwt sign failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("jwt: {e}") }))
    })?;

    // Store config defaults
    state.redis.set::<(), _, _>("cfg:default:rps", rps.to_string(), None, None, false).await
        .map_err(|e| tracing::warn!("[ADMIN] Redis set rps failed: {e}")).ok();
    state.redis.set::<(), _, _>("cfg:default:duration_secs", duration_secs.to_string(), None, None, false).await
        .map_err(|e| tracing::warn!("[ADMIN] Redis set duration failed: {e}")).ok();

    // Store default score weights in config:weights hash
    if locked.is_none() {
        use fred::interfaces::HashesInterface;
        let _ = state.redis.hset::<(), _, _>("config:weights", ("correctness_weight", "0.40")).await
            .map_err(|e| tracing::warn!("[ADMIN] Redis set correctness_weight failed: {e}")).ok();
        let _ = state.redis.hset::<(), _, _>("config:weights", ("tps_weight", "0.35")).await
            .map_err(|e| tracing::warn!("[ADMIN] Redis set tps_weight failed: {e}")).ok();
        let _ = state.redis.hset::<(), _, _>("config:weights", ("p99_weight", "0.25")).await
            .map_err(|e| tracing::warn!("[ADMIN] Redis set p99_weight failed: {e}")).ok();
    }

    tracing::info!("[ADMIN] registered contestant={cid} name={} rps={} duration={}", req.name, rps, duration_secs);

    // Lock config on first use if not already locked
    if locked.is_none() {
        tracing::info!("[ADMIN] Locking config — first contestant created");
        state.redis.set::<(), _, _>("config:locked", "1", None, None, false).await
            .map_err(|e| {
                tracing::error!("[ADMIN] Failed to lock config: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
            })?;
    }

    Ok((StatusCode::CREATED, Json(json!({
        "contestant_id": cid,
        "name": req.name,
        "jwt": jwt,
        "rps": rps,
        "duration_secs": duration_secs,
    }))))
}

/// GET /api/admin/config — read current test parameters.
async fn admin_get_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let rps: String = state.redis.get("cfg:default:rps").await
        .map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] Redis get rps failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    let duration: String = state.redis.get("cfg:default:duration_secs").await
        .map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] Redis get duration failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;

    let correctness_weight: String = state.redis.hget("config:weights", "correctness_weight").await
        .map_err(|e| { tracing::error!("[ADMIN-CONFIG] hget correctness_weight failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
    })?;
    let tps_weight: String = state.redis.hget("config:weights", "tps_weight").await
        .map_err(|e| { tracing::error!("[ADMIN-CONFIG] hget tps_weight failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
    })?;
    let p99_weight: String = state.redis.hget("config:weights", "p99_weight").await
        .map_err(|e| { tracing::error!("[ADMIN-CONFIG] hget p99_weight failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
    })?;

    Ok(Json(json!({
        "rps": rps.parse::<u32>().map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] non-numeric rps: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid rps: {e}") }))
        })?,
        "duration_secs": duration.parse::<u32>().map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] non-numeric duration: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid duration: {e}") }))
        })?,
        "correctness_weight": correctness_weight.parse::<f64>().map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] non-numeric correctness_weight: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid correctness_weight: {e}") }))
        })?,
        "tps_weight": tps_weight.parse::<f64>().map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] non-numeric tps_weight: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid tps_weight: {e}") }))
        })?,
        "p99_weight": p99_weight.parse::<f64>().map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] non-numeric p99_weight: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Invalid p99_weight: {e}") }))
        })?,
    })))
}
/// PUT /api/admin/config — update test parameters (locked after first contestant creation).
async fn admin_set_config(
    State(state): State<Arc<AppState>>,
    Json(cfg): Json<AdminConfig>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Check lock
    let locked: Option<String> = state.redis.get("config:locked").await
        .map_err(|e| {
            tracing::error!("[ADMIN-CONFIG] Redis check lock failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    if locked.is_some() {
        tracing::warn!("[ADMIN-CONFIG] Rejected — config locked after first contestant");
        return Err((StatusCode::FORBIDDEN, Json(ErrorResponse { error: "config locked after first contestant created".into() })));
    }

    if let Some(rps) = cfg.rps {
        state.redis.set::<(), _, _>("cfg:default:rps", rps.to_string(), None, None, false).await
            .map_err(|e| {
                tracing::error!("[ADMIN-CONFIG] Redis set rps failed: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
            })?;
    }
    if let Some(dur) = cfg.duration_secs {
        state.redis.set::<(), _, _>("cfg:default:duration_secs", dur.to_string(), None, None, false).await
            .map_err(|e| {
                tracing::error!("[ADMIN-CONFIG] Redis set duration failed: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
            })?;
    }

    if let Some(w) = cfg.correctness_weight {
        let val = format!("{:.2}", w);
        state.redis.hset::<(), _, _>("config:weights", ("correctness_weight", val.as_str())).await
            .map_err(|e| { tracing::error!("[ADMIN-CONFIG] hset correctness_weight failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    }
    if let Some(w) = cfg.tps_weight {
        let val = format!("{:.2}", w);
        state.redis.hset::<(), _, _>("config:weights", ("tps_weight", val.as_str())).await
            .map_err(|e| { tracing::error!("[ADMIN-CONFIG] hset tps_weight failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    }
    if let Some(w) = cfg.p99_weight {
        let val = format!("{:.2}", w);
        state.redis.hset::<(), _, _>("config:weights", ("p99_weight", val.as_str())).await
            .map_err(|e| { tracing::error!("[ADMIN-CONFIG] hset p99_weight failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("Redis: {e}") }))
        })?;
    }

    tracing::info!("[ADMIN-CONFIG] updated");
    Ok(Json(json!({"status": "updated"})))
}

/// Build admin routes.
pub fn admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/admin/register", post(admin_register))
        .route("/api/admin/config", get(admin_get_config).put(admin_set_config))
}
