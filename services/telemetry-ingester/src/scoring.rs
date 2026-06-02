/// Computes composite score from normalized sub-scores.
///
/// Formula:
/// - correctness_pct = correctness * 100
/// - throughput_score = min(100, (tps / global_max_tps) * 100)
/// - latency_score = min(100, (global_best_p99 / p99_us) * 100)
/// - composite = 0.40 * correctness_pct + 0.35 * throughput_score + 0.25 * latency_score
pub fn compute_composite(
    correctness: f64,
    tps: f64,
    p99_us: u64,
    global_max_tps: f64,
    global_best_p99: u64,
) -> f64 {
    let correctness_pct = correctness * 100.0;

    let throughput_score = if global_max_tps > 0.0 {
        ((tps / global_max_tps) * 100.0).min(100.0)
    } else {
        0.0
    };

    let latency_score = if p99_us > 0 && global_best_p99 > 0 {
        ((global_best_p99 as f64 / p99_us as f64) * 100.0).min(100.0)
    } else {
        0.0
    };

    0.40 * correctness_pct + 0.35 * throughput_score + 0.25 * latency_score
}
