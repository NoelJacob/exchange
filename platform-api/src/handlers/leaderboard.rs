use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::sse::{Event, Sse},
    routing::get,
};
use futures_util::stream::{Stream};
use serde_json::{Value, json};
use crate::handlers::contestants::AppState;

#[derive(serde::Serialize)]
struct ErrorResponse {
    error: String,
}

/// GET /api/leaderboard — return completed test runs sorted by peak_bots desc.
async fn leaderboard(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    // Primary: contest_summary with contestant name
    let rows = match sqlx::query(
        "SELECT cs.contestant_id, c.name, cs.status, cs.correctness_pct, cs.composite, \
         cs.current_tps, cs.failure_reason, cs.orders_sent, cs.total_fills \
         FROM ( \
           SELECT ts, contestant_id, status, orders_sent, execs_received, \
                  correct_fills, total_fills, total_penalty, correctness_pct, \
                  composite, current_tps, peak_tps, p99_latency_us, failure_reason, \
                  ROW_NUMBER() OVER (PARTITION BY contestant_id ORDER BY ts DESC) as rn \
           FROM contest_summary \
         ) cs \
         JOIN contestants c ON cs.contestant_id = c.contestant_id \
         WHERE cs.rn = 1 \
         ORDER BY composite IS NOT NULL DESC, composite DESC \
         LIMIT 100"
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) if !rows.is_empty() => {
            tracing::info!("[LEADERBOARD] {} entries from contest_summary (composite)", rows.len());
            rows.into_iter().map(|r| {
                use sqlx::Row;
                json!({
                    "contestant_id": r.get::<String, usize>(0),
                    "name": r.get::<String, usize>(1),
                    "status": r.get::<String, usize>(2),
                    "correctness_pct": r.get::<Option<f64>, usize>(3),
                    "composite": r.get::<Option<f64>, _>("composite"),
                    "current_tps": r.get::<Option<f64>, _>("current_tps"),
                    "failure_reason": r.get::<Option<String>, _>("failure_reason").and_then(|s| if s.is_empty() { None } else { Some(s) }),
                    "orders_sent": r.get::<Option<i64>, _>("orders_sent"),
                    "total_fills": r.get::<Option<i64>, _>("total_fills"),
                })
            }).collect::<Vec<Value>>()
        }
        Ok(_) => {
            tracing::info!("[LEADERBOARD] contest_summary empty — no scoring data yet");
            vec![]
        }
        Err(e) => {
            tracing::error!("[LEADERBOARD] query failed: {e}");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: format!("DB: {e}") })));
        }
    };
    Ok(Json(json!({"leaderboard": rows})))
}

/// GET /api/events — SSE endpoint: merges leaderboard updates with configurable keepalive ping.
async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    use futures_util::stream;
    use tokio::sync::broadcast;
    use tokio::time::{interval, Duration};

    // Create an unbounded channel to forward broadcast events into
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut broadcast_rx: broadcast::Receiver<String> = state.leaderboard_tx.subscribe();

    // Spawn a task that reads from broadcast and forwards to mpsc
    tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(data) => {
                    let _ = tx.send(data);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("[SSE] lagged by {n} messages");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Convert mpsc receiver into a stream
    let event_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
        .map(|data| Ok(Event::default().data(data).event("leaderboard")));

    let keepalive = stream::unfold(interval(Duration::from_secs(state.sse_keepalive_secs)), |mut i| async {
        i.tick().await;
        Some((Ok(Event::default().data("ping").event("keepalive")), i))
    });

    // Merge both streams
    use futures_util::stream::StreamExt;
    let merged = futures_util::stream::select(event_stream, keepalive);
    Sse::new(merged)
}

/// Build leaderboard routes.
pub fn leaderboard_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/leaderboard", get(leaderboard))
        .route("/api/events", get(events))
}
