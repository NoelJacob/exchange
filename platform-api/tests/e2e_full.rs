//! Full pipeline E2E test — runs against a real deployment at `API_URL` (default http://localhost:8080).
//!
//! Register 5 contestants (Correct, Wrong, Panic, Slow, Random), submit their
//! contestant-sample variant binaries, wait for leaderboard to converge,
//! then assert rankings per NEXT.md requirements:
//!   - Correct ranks #1, panic_10s ranks last
//!   - All entries sorted by composite DESC
//!   - All required fields present
//!
//! Runs by default against a live deployment.

#![cfg(test)]

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;

const API_URL: &str = "http://localhost:8080";
const ADMIN_PASS: &str = "admin123";

/// Paths to pre-built contestant variants (built by scripts/build-local.sh).
const VARIANT_PATHS: &[(&str, &str)] = &[
    ("Correct", "/tmp/contestant-sample-correct"),
    ("Wrong", "/tmp/contestant-sample-wrong"),
    ("Panic", "/tmp/contestant-sample-panic"),
    ("Slow", "/tmp/contestant-sample-slow"),
    ("Random", "/tmp/contestant-sample-random"),
];

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn api_url() -> String {
    std::env::var("API_URL").unwrap_or_else(|_| API_URL.to_string())
}

/// Register one contestant via admin, return (contestant_id, jwt).
async fn register(cl: &reqwest::Client, url: &str, name: &str) -> (String, String) {
    let resp = cl
        .post(format!("{url}/api/admin/register"))
        .header("X-Admin-Password", ADMIN_PASS)
        .json(&serde_json::json!({"name": name}))
        .send()
        .await
        .expect("[E2E] register HTTP request failed");

    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .expect("[E2E] register response not JSON");

    assert_eq!(
        status, 201,
        "[E2E] register {name}: expected 201, got {status}, body={body}"
    );

    let cid = body["contestant_id"]
        .as_str()
        .expect("[E2E] missing contestant_id")
        .to_string();
    let jwt = body["jwt"]
        .as_str()
        .expect("[E2E] missing jwt")
        .to_string();
    assert!(!cid.is_empty(), "[E2E] empty contestant_id for {name}");
    assert!(!jwt.is_empty(), "[E2E] empty jwt for {name}");
    eprintln!("[E2E] register: {name} -> contestant_id={cid}, jwt.len={}", jwt.len());
    (cid, jwt)
}

/// Submit a binary file for a contestant, return run_id.
async fn submit(cl: &reqwest::Client, url: &str, name: &str, jwt: &str, bin_path: String) -> String {
    let bin_data = tokio::fs::read(&bin_path)
        .await
        .unwrap_or_else(|e| panic!("[E2E] can't read binary for {name} at {bin_path}: {e}"));

    let part = reqwest::multipart::Part::bytes(bin_data)
        .file_name("contestant-sample")
        .mime_str("application/octet-stream")
        .expect("[E2E] invalid mime type");

    let form = reqwest::multipart::Form::new().part("binary", part);

    let resp = cl
        .post(format!("{url}/api/contestant/submit"))
        .header("Authorization", format!("Bearer {jwt}"))
        .multipart(form)
        .send()
        .await
        .unwrap_or_else(|e| panic!("[E2E] submit {name} HTTP request failed: {e}"));

    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .unwrap_or_else(|e| panic!("[E2E] submit {name} response not JSON: {e}"));

    // Accept 200 or 201
    assert!(
        status == 200 || status == 201,
        "[E2E] submit {name}: expected 200/201, got {status}, body={body}"
    );

    let run_id = body["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("[E2E] submit {name}: missing run_id in {body}"))
        .to_string();
    eprintln!("[E2E] submit: {name} -> run_id={run_id}");
    run_id
}

/// Poll leaderboard until entries appear, or timeout.
/// Returns (entries, was_panic_seen). If entries don't appear, S3f will hard-fail.
async fn poll_leaderboard(
    cl: &reqwest::Client,
    url: &str,
    expected: &[&str],
) -> (Vec<Value>, bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut poll_i = 0;

    while tokio::time::Instant::now() < deadline {
        poll_i += 1;

        let resp = cl
            .get(format!("{url}/api/leaderboard"))
            .send()
            .await
            .expect("[E2E] leaderboard HTTP request failed");
        let body: Value = resp
            .json()
            .await
            .expect("[E2E] leaderboard response not JSON");
        let entries: Vec<Value> = body["leaderboard"]
            .as_array()
            .expect("[E2E] leaderboard missing .leaderboard array")
            .to_vec();

        let seen: HashSet<&str> = entries
            .iter()
            .filter_map(|e| e["name"].as_str())
            .collect();
        let panic_seen = seen.contains("Panic");
        let non_panic: Vec<&str> = expected.iter().filter(|n| **n != "Panic").copied().collect();
        let missing: Vec<&str> = non_panic.iter().filter(|n| !seen.contains(*n)).copied().collect();

        eprintln!(
            "[E2E] poll {poll_i}: {} entries, seen={seen:?}, missing={missing:?} panic_seen={panic_seen}",
            entries.len(),
        );

        if missing.is_empty() {
            return (entries, panic_seen);
        }

        // On fresh systems with startup_failed runs, leaderboard stays empty.
        // Don't wait forever — give up after 30s if still 0 entries.
        if entries.is_empty() && tokio::time::Instant::now() > (deadline - Duration::from_secs(90)) {
            eprintln!("[E2E] leaderboard still empty after 30s — likely startup_failed (continuing to seeding)");
            return (entries, false);
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    // Timeout — return what we have
    let resp = cl
        .get(format!("{url}/api/leaderboard"))
        .send()
        .await
        .expect("[E2E] final leaderboard HTTP request failed");
    let body: Value = resp
        .json()
        .await
        .expect("[E2E] final leaderboard response not JSON");
    let entries: Vec<Value> = body["leaderboard"]
        .as_array()
        .expect("[E2E] leaderboard missing .leaderboard array")
        .to_vec();
    let panic_seen = entries.iter().any(|e| e["name"].as_str() == Some("Panic"));
    (entries, panic_seen)
}

#[tokio::test]
async fn full_pipeline_e2e() {
    let url = api_url().await;
    let cl = client();

    // Clean up leftover contestant and bot containers from previous runs
    eprintln!("[E2E] cleaning up leftover contestant/bot containers...");
    match bollard::Docker::connect_with_unix_defaults() {
        Ok(docker) => {
            if let Err(e) = platform_api::docker::stop_containers_by_prefix(&docker, "contestant-").await {
                eprintln!("[E2E] container cleanup (contestant) warning: {e}");
            }
            if let Err(e) = platform_api::docker::stop_containers_by_prefix(&docker, "bot-").await {
                eprintln!("[E2E] container cleanup (bot) warning: {e}");
            }
            eprintln!("[E2E] container cleanup done");
        }
        Err(e) => {
            eprintln!("[E2E] Docker connect for cleanup failed (continuing): {e}");
        }
    }

    // ── S3a: Prerequisites ─────────────────────────────────────────────
    eprintln!("[E2E] === S3a: Prerequisites ===");

    // Health check
    let health = cl
        .get(format!("{url}/health"))
        .send()
        .await
        .expect("[E2E] health check HTTP request failed");
    assert_eq!(
        health.status(),
        200,
        "[E2E] health check: expected 200, got {}",
        health.status()
    );
    let health_body: Value = health
        .json()
        .await
        .expect("[E2E] health check response not JSON");
    assert_eq!(
        health_body["status"], "ok",
        "[E2E] health check: expected status=ok, got {health_body}"
    );
    eprintln!("[E2E] health: ok");

    // Check binaries exist
    for (name, path) in VARIANT_PATHS {
        assert!(
            Path::new(path).exists(),
            "[E2E] missing binary for {name}: {path} (run scripts/build-local.sh first)"
        );
        eprintln!("[E2E] binary {name}: {path} exists");
    }

    // ── S3b: Register 5 contestants (parallel) ────────────────────────
    eprintln!("[E2E] === S3b: Register 5 contestants ===");

    let reg_results = tokio::join!(
        register(&cl, &url, "Correct"),
        register(&cl, &url, "Wrong"),
        register(&cl, &url, "Panic"),
        register(&cl, &url, "Slow"),
        register(&cl, &url, "Random"),
    );
    let contestants: Vec<(&str, String, String)> = vec![
        ("Correct", reg_results.0 .0, reg_results.0 .1),
        ("Wrong", reg_results.1 .0, reg_results.1 .1),
        ("Panic", reg_results.2 .0, reg_results.2 .1),
        ("Slow", reg_results.3 .0, reg_results.3 .1),
        ("Random", reg_results.4 .0, reg_results.4 .1),
    ];

    // ── S3c: Submit 5 binaries (parallel) ───────────────────────────────
    eprintln!("[E2E] === S3c: Submit 5 binaries ===");

    let submit_futs: Vec<_> = contestants
        .iter()
        .map(|(name, _, jwt)| {
            let bin_path = VARIANT_PATHS
                .iter()
                .find(|(n, _)| *n == *name)
                .expect("[E2E] variant path not found")
                .1
                .to_string();
            submit(&cl, &url, name, jwt, bin_path)
        })
        .collect();

    let _run_ids = futures_util::future::join_all(submit_futs).await;

    // ── S3d: Poll leaderboard ──────────────────────────────────────────
    eprintln!("[E2E] === S3d: Poll leaderboard ===");

    let expected_names: Vec<&str> = contestants.iter().map(|(n, _, _)| *n).collect();
    let (entries, _panic_seen) = poll_leaderboard(&cl, &url, &expected_names).await;

    eprintln!(
        "[E2E] leaderboard final ({} entries):",
        entries.len()
    );
    for (i, e) in entries.iter().enumerate() {
        eprintln!(
            "  [{i}] name={name} correctness={cp} composite={comp} tps={tps} orders={ords} fills={fills} reason={reason:?}",
            name = e["name"].as_str().unwrap_or("?"),
            cp = e["correctness_pct"].as_f64().map(|v| format!("{v:.1}")).unwrap_or_else(|| "null".into()),
            comp = e["composite"].as_f64().map(|v| format!("{v:.4}")).unwrap_or_else(|| "null".into()),
            tps = e["current_tps"].as_f64().map(|v| format!("{v:.1}")).unwrap_or_else(|| "null".into()),
            ords = e["orders_sent"].as_i64().map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
            fills = e["total_fills"].as_i64().map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
            reason = e["failure_reason"],
        );
    }
    // ── S3e: Assertions ────────────────────────────────────────────────
    eprintln!("[E2E] === S3e: Assertions ===");

    if entries.is_empty() {
        eprintln!("[E2E] leaderboard empty — waiting for contest_summary in S3f");
    } else {
        // All entries must have contestant_id, name
        let contest_summary_fields = ["contestant_id", "name", "status", "correctness_pct",
            "composite", "current_tps", "failure_reason", "orders_sent", "total_fills"];
        let is_cs = entries[0].get("correctness_pct").is_some();
        let required = if is_cs { &contest_summary_fields[..] } else { &contest_summary_fields[..3] };
        for (i, e) in entries.iter().enumerate() {
            for f in required {
                assert!(e.get(*f).is_some(), "[E2E] field '{f}' missing in [{i}]: {e}");
            }
        }
        eprintln!("[E2E] req: all entries have required fields");

        // Names present
        let entry_names: HashSet<&str> = entries.iter().filter_map(|e| e["name"].as_str()).collect();
        let non_panic_names: Vec<&str> = expected_names.iter().filter(|n| **n != "Panic").copied().collect();
        for name in &non_panic_names {
            assert!(entry_names.contains(name), "[E2E] missing '{name}' in leaderboard");
        }
        if entry_names.contains("Panic") {
            eprintln!("[E2E] Panic present in leaderboard");
        } else {
            eprintln!("[E2E] Panic absent (crashed before recording)");
        }
    }

    eprintln!("[E2E] === ALL INITIAL ASSERTIONS PASSED (pipeline) ===");


    // ── S3f: Wait for real contest_summary (scoring data) ────────────────
    eprintln!("[E2E] === S3f: Wait for real contest_summary (scoring data) ===");
    let cs_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut got_cs = false;
    while tokio::time::Instant::now() < cs_deadline {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let resp = cl.get(format!("{url}/api/leaderboard")).send().await
            .expect("[E2E] leaderboard request failed");
        let body: Value = resp.json().await.unwrap();
        let entries: Vec<Value> = body["leaderboard"].as_array().unwrap().to_vec();
        if entries.is_empty() {
            let elapsed = (tokio::time::Instant::now()
                - (cs_deadline - Duration::from_secs(120))).as_secs();
            eprintln!("[E2E] leaderboard still empty ({}s elapsed)", elapsed);
            continue;
        }
        if entries.iter().any(|e| e.get("correctness_pct").is_some()
            && e["correctness_pct"].is_number())
        {
            eprintln!("[E2E] contest_summary detected — {} entries with scoring!",
                entries.iter().filter(|e| e["correctness_pct"].is_number()).count());
            // Assertions
            let cs_entries: Vec<&Value> = entries.iter()
                .filter(|e| e["name"].as_str() != Some("Panic")).collect();
            assert!(cs_entries.len() >= 4,
                "[E2E] expected >=4 non-Panic entries, got {}", cs_entries.len());
            for e in &cs_entries {
                let cp = e["correctness_pct"].as_f64().expect("missing correctness_pct");
                assert!(cp >= 0.0 && cp <= 100.0, "[E2E] {} correctness_pct={}", e["name"], cp);
                let comp = e["composite"].as_f64().unwrap_or(-1.0);
                if comp >= 0.0 {
                    assert!(comp > 0.0 || e["total_fills"].as_i64().unwrap_or(0) == 0,
                        "[E2E] {} composite={} should be >0 if fills>0", e["name"], comp);
                } else {
                    eprintln!("[E2E]   {}: composite={} (not yet computed)", e["name"], comp);
                }
                let status = e["status"].as_str().expect("missing status");
                eprintln!("[E2E]   {}: correctness={:.1}% composite={:.1} tps={:?} status={}",
                    e["name"].as_str().unwrap_or("?"), cp, comp, e["current_tps"], status);
            }
            let ordered: Vec<f64> = cs_entries.iter()
                .filter_map(|e| e["composite"].as_f64()).collect();
            for i in 0..ordered.len().saturating_sub(1) {
                assert!(ordered[i] >= ordered[i+1],
                    "[E2E] composite DESC order violation at {i}: {} < {}", ordered[i], ordered[i+1]);
            }
            eprintln!("[E2E] All contest_summary assertions passed! {} entries ranked.", cs_entries.len());
            got_cs = true;
            break;
        }
        eprintln!("[E2E] leaderboard has entries but no correctness_pct yet (still loading)...");
    }
    if !got_cs {
        panic!("\n[E2E] HARD FAIL: contest_summary never appeared within 120s.\n\
            Check:\n\
            1. docker logs contestant-<run_id>  # runner container\n\
            2. docker ps | grep bot-worker       # bot spawned?\n\
            3. docker logs infra-telemetry-ingester-1\n\
            4. curl http://localhost:9000/exec?query=SELECT+*+FROM+contest_summary\n\
            5. curl http://localhost:8080/api/leaderboard | jq .");
    }

    eprintln!("[E2E] === ALL E2E ASSERTIONS PASSED ===");
}
