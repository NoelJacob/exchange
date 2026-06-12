use clap::Parser;

use bot_worker::config::Config;

#[tokio::main]
async fn main() {
    let config = Config::parse();

    eprintln!(
        "[Main] Bot worker starting: {} FIX x {} WS, {} RPS target, {}s duration",
        config.fix_connections,
        config.ws_connections,
        config.rps,
        config.duration_secs,
    );

    let result = bot_worker::run(config).await;

    // The final BotResult is already printed by run() as JSON
    // Print a summary to stderr
    eprintln!(
        "[Main] Done: {} orders, {} fills, {} partials, {} rejects, {} errors, p50={}µs",
        result.orders_sent,
        result.fills,
        result.partials,
        result.rejects,
        result.errors.len(),
        result.p50_latency_us as u64,
    );

    if !result.errors.is_empty() {
        eprintln!("[Main] Errors ({})", result.errors.len());
        for (i, err) in result.errors.iter().enumerate().take(10) {
            eprintln!("  {i}: {err}");
        }
    }

    // Exit with code 1 if there were errors
    if !result.errors.is_empty() {
        std::process::exit(1);
    }
}
