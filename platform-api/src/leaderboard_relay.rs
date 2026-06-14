use fred::interfaces::{EventInterface, PubsubInterface};
use tokio::sync::broadcast;

/// Background task: subscribe to Redis leaderboard:updates and forward to broadcast channel.
/// Uses its own Redis connection to avoid interfering with the main client.
pub async fn run(leaderboard_tx: broadcast::Sender<String>, redis_url: &str) {
    tracing::info!("[SSE-RELAY] Starting — connecting dedicated subscriber to {redis_url}");

    // Create a separate Redis connection for pub/sub
    let sub_client = match crate::redis::connect(redis_url).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("[SSE-RELAY] Failed to connect: {e}");
            return;
        }
    };

    if let Err(e) = sub_client.subscribe("leaderboard:updates").await {
        tracing::error!("[SSE-RELAY] Subscribe failed: {e}");
        return;
    }
    tracing::info!("[SSE-RELAY] Subscribed to leaderboard:updates");

    let mut rx = sub_client.message_rx();
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.value.as_str() {
                    tracing::info!("[SSE-RELAY] received update, broadcasting");
                    let _ = leaderboard_tx.send(payload.to_string());
                }
            }
            Err(e) => {
                tracing::error!("[SSE-RELAY] message receive error: {e}");
            }
        }
    }
}
