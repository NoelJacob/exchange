use std::collections::HashMap;

use sqlx::PgPool;
use tracing::info;

use crate::scoring::compute_composite;
use crate::state::ContestantState;
use crate::types::{AggregatedSnapshot, LeaderboardRow};

/// Creates the contestant_scores table if it does not exist.
pub async fn ensure_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS contestant_scores (
            contestant_id VARCHAR(255) PRIMARY KEY,
            composite_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            correctness DOUBLE PRECISION NOT NULL DEFAULT 0,
            tps DOUBLE PRECISION NOT NULL DEFAULT 0,
            p50_us BIGINT NOT NULL DEFAULT 0,
            p90_us BIGINT NOT NULL DEFAULT 0,
            p99_us BIGINT NOT NULL DEFAULT 0,
            total_orders BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Computes leaderboard, upserts each contestant into PostgreSQL,
/// and returns the sorted leaderboard rows.
pub async fn flush_contestants(
    pool: &PgPool,
    contestants: &HashMap<String, ContestantState>,
) -> Result<Vec<LeaderboardRow>, sqlx::Error> {
    // Phase 1: collect raw snapshots and correctness scores
    let raw: Vec<(&str, AggregatedSnapshot, f64)> = contestants
        .iter()
        .map(|(id, state)| {
            let snapshot = state.aggregator.snapshot();
            let correctness = state.validator.correctness();
            (id.as_str(), snapshot, correctness)
        })
        .collect();

    // Phase 2: compute global maxima for normalization
    let global_max_tps = raw
        .iter()
        .map(|(_, s, _)| s.tps)
        .fold(0.0_f64, f64::max);

    let global_best_p99 = raw
        .iter()
        .map(|(_, s, _)| s.p99_us)
        .min()
        .unwrap_or(1);

    // Phase 3: build leaderboard rows with composite scores
    let mut rows: Vec<LeaderboardRow> = raw
        .into_iter()
        .map(|(id, snapshot, correctness)| {
            let composite = compute_composite(
                correctness,
                snapshot.tps,
                snapshot.p99_us,
                global_max_tps,
                global_best_p99,
            );
            LeaderboardRow {
                contestant_id: id.to_string(),
                composite_score: composite,
                correctness,
                tps: snapshot.tps,
                p50_us: snapshot.p50_us,
                p90_us: snapshot.p90_us,
                p99_us: snapshot.p99_us,
                total_orders: snapshot.total_orders,
            }
        })
        .collect();

    // Phase 4: sort descending by composite score
    rows.sort_by(|a, b| b.composite_score.partial_cmp(&a.composite_score).unwrap());

    // Phase 5: upsert each row into PostgreSQL
    for row in &rows {
        sqlx::query(
            r#"INSERT INTO contestant_scores
               (contestant_id, composite_score, correctness, tps, p50_us, p90_us, p99_us, total_orders, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
               ON CONFLICT (contestant_id) DO UPDATE SET
               composite_score = EXCLUDED.composite_score,
               correctness = EXCLUDED.correctness,
               tps = EXCLUDED.tps,
               p50_us = EXCLUDED.p50_us,
               p90_us = EXCLUDED.p90_us,
               p99_us = EXCLUDED.p99_us,
               total_orders = EXCLUDED.total_orders,
               updated_at = NOW()"#,
        )
        .bind(&row.contestant_id)
        .bind(row.composite_score)
        .bind(row.correctness)
        .bind(row.tps)
        .bind(row.p50_us as i64)
        .bind(row.p90_us as i64)
        .bind(row.p99_us as i64)
        .bind(row.total_orders as i64)
        .execute(pool)
        .await?;
    }

    info!(count = rows.len(), "Flushed contestant scores to PostgreSQL");
    Ok(rows)
}
