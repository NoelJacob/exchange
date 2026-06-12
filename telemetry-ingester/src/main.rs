mod config;
mod ingester;
mod models;
mod storage;
mod verifier;

use std::sync::Arc;

use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();

    let config = config::Config::parse();

    eprintln!("[Main] Telemetry ingester starting...");
    eprintln!("[Main] Redpanda: {}", config.redpanda_brokers);
    eprintln!("[Main] QuestDB: {}", config.questdb_pgwire);
    eprintln!("[Main] Valkey: {}", config.valkey_addr);

    let storage = Arc::new(storage::Storage::connect(&config).await?);
    storage.ensure_schema().await?;
    eprintln!("[Main] Schema ready");

    let ing = ingester::Ingester::new(Arc::clone(&storage), config.clone());
    let mut ver = verifier::Verifier::new(Arc::clone(&storage), config);

    let (ing_res, ver_res) = tokio::join!(
        tokio::spawn(async move { ing.run().await }),
        tokio::spawn(async move { ver.run().await }),
    );
    ing_res.map_err(|e| format!("Ingester panicked: {e}"))??;
    ver_res.map_err(|e| format!("Verifier panicked: {e}"))??;

    Ok(())
}
