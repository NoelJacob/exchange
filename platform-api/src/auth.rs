use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
    routing::post,
};
use fred::interfaces::KeysInterface;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::handlers::contestants::AppState;

/// JWT claims.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// Subject: contestant_id
    pub sub: String,
    /// Expiry (UNIX timestamp)
    pub exp: usize,
    /// Issued at
    pub iat: usize,
}

/// Request body for token exchange.
#[derive(Deserialize)]
pub struct ExchangeRequest {
    pub token: String,
}

/// Error response.
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// POST /api/auth/exchange — exchange an upload token for a JWT.
async fn exchange_token(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExchangeRequest>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    eprintln!("[EXCHANGE-INIT] exchange_token called with token={}", &req.token[..8.min(req.token.len())]);
    let token_uuid = match uuid::Uuid::parse_str(&req.token) {
        Ok(u) => u,
        Err(_) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse { error: "token is not a valid UUID".into() }),
            ));
        }
    };
    let token_key = crate::redis::token_key(&req.token);

    // Check Redis for the token (non-fatal — DB fallback on Redis failure or miss)
    let contestant_id: Option<String> = match state.redis.get(&token_key).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("[EXCHANGE] Redis get failed (falling back to DB): {e}");
            None
        }
    };
    let contestant_id = match contestant_id {
        Some(cid) => cid,
        None => {
            // Fallback: check QuestDB (token might have expired from Redis)
            eprintln!("[EXCHANGE-DEBUG] Redis miss, checking DB...");
            let row = sqlx::query(
                "SELECT contestant_id, used FROM submission_tokens WHERE token = $1",
            )
            .bind(token_uuid)
            .fetch_optional(&state.db)
            .await
            .map_err(|e| {
                eprintln!("[EXCHANGE-DEBUG] DB error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: format!("DB error: {e}") }),
                )
            })?;
            match row {
                Some(r) => {
                    use sqlx::Row;
                    let used: bool = r.get("used");
                    if used {
                        return Err((
                            StatusCode::UNAUTHORIZED,
                            Json(ErrorResponse { error: "token already used".into() }),
                        ));
                    }
                    r.get::<String, _>("contestant_id")
                }
                None => {
                    return Err((
                        StatusCode::UNAUTHORIZED,
                        Json(ErrorResponse { error: "invalid token".into() }),
                    ));
                }
            }
        }
    };
    eprintln!("[EXCHANGE-DEBUG] contestant_id = {}", contestant_id);

    // Delete token from Redis (one-time use) — non-fatal
    let _: () = match state.redis.del::<(), _>(&token_key).await {
        Ok(_) => (),
        Err(e) => {
            eprintln!("[EXCHANGE-DEBUG] Redis del failed (non-fatal): {e}");
        }
    };

    // Mark token as used in QuestDB
    eprintln!("[EXCHANGE-DEBUG] Updating DB...");
    if let Err(e) = sqlx::query("UPDATE submission_tokens SET used = true WHERE token = $1")
        .bind(token_uuid)
        .execute(&state.db).await
    {
        eprintln!("[EXCHANGE-DEBUG] DB update failed: {e}");
    }

    // Generate JWT
    eprintln!("[EXCHANGE-DEBUG] Generating JWT...");
    let now = chrono::Utc::now();
    let claims = Claims {
        sub: contestant_id.clone(),
        iat: now.timestamp() as usize,
        exp: (now + chrono::Duration::hours(1)).timestamp() as usize,
    };

    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.jwt_secret.as_bytes()),
    )
    .map_err(|e| {
        eprintln!("[EXCHANGE-DEBUG] JWT encode error: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("JWT encoding error: {e}") }),
        )
    })?;

    eprintln!("[EXCHANGE-DEBUG] JWT generated, returning success");
    Ok(Json(json!({
        "token": token,
        "contestant_id": contestant_id,
        "expires_at": (now + chrono::Duration::hours(1)).to_rfc3339(),
    })))
}

/// Extract claims from Authorization header.
pub fn extract_claims(req: &Request, jwt_secret: &str) -> Result<Claims, StatusCode> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    Ok(token_data.claims)
}

/// Auth middleware: validates JWT and injects contestant_id into request extensions.
pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let claims = extract_claims(&req, &state.jwt_secret)?;
    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
}

/// Admin auth middleware: validates X-Admin-Password header.
pub async fn admin_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let password = req.headers()
        .get("X-Admin-Password")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if password != state.jwt_secret {
        // Check env ADMIN_PASSWORD if it differs from jwt_secret
        let admin_pw = std::env::var("ADMIN_PASSWORD").unwrap_or_else(|_| "admin123".into());
        if password != admin_pw && password != state.jwt_secret {
            tracing::warn!("[AUTH] admin auth failed");
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(req).await)
}

/// Internal auth middleware: validates X-Internal-Token header.
pub async fn internal_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = req.headers()
        .get("X-Internal-Token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if token != state.internal_token {
        tracing::warn!("[AUTH] internal auth failed (got={token})");
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(req).await)
}

/// Build auth routes.
pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/auth/exchange", post(exchange_token))
}
