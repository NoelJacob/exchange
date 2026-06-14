mod config;
mod ingester;
mod models;
mod storage;
mod verifier;

use std::sync::Arc;

use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    eprintln!("[TELEMETRY] main() called — init starting");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();
    eprintln!("[TELEMETRY] tracing initialized, parsing config...");

    let cfg = config::Config::parse();

    eprintln!("[Main] Telemetry ingester starting...");
    eprintln!("[Main] Redpanda: {}", cfg.redpanda_brokers);
    eprintln!("[Main] QuestDB: {}", cfg.questdb_pgwire);
    eprintln!("[Main] Valkey: {}", cfg.valkey_addr);
    eprintln!("[Main] Config: redpanda={} questdb={} valkey={}", 
        cfg.redpanda_brokers, cfg.questdb_pgwire, cfg.valkey_addr);
    eprintln!("[Main] Env REDPANDA_BROKERS={:?}", std::env::var("REDPANDA_BROKERS"));
    eprintln!("[Main] Env QUESTDB_URL={:?}", std::env::var("QUESTDB_URL"));

    let storage = Arc::new(storage::Storage::connect(&cfg).await?);
    storage.ensure_schema().await?;
    eprintln!("[Main] Schema ready");

    let ing = ingester::Ingester::new(Arc::clone(&storage), cfg.clone());

    let (ing_res, ver_res) = tokio::join!(
        tokio::spawn(async move { ing.run().await }),
        tokio::spawn(async move {
            verifier::run_verifiers(Arc::clone(&storage), cfg).await
        }),
    );
    ing_res.map_err(|e| format!("Ingester panicked: {e}"))??;
    ver_res.map_err(|e| format!("Verifier panicked: {e}"))??;

    Ok(())
}
