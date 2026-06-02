use futures_util::StreamExt;
use futures_util::SinkExt;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::info;

/// WebSocket broadcast server.
///
/// Listens on the given port. Each new connection receives a broadcast
/// subscriber that sends every leaderboard snapshot as JSON text frames.
pub async fn ws_server(
    port: u16,
    tx: broadcast::Sender<String>,
) -> Result<(), anyhow::Error> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("WebSocket server listening on ws://{}", addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            info!("New WebSocket connection from {}", peer);
            match accept_async(stream).await {
                Ok(ws_stream) => {
                    let (mut write, _) = ws_stream.split();
                    let mut rx = tx.subscribe();
                    loop {
                        match rx.recv().await {
                            Ok(msg) => {
                                if write.send(Message::Text(msg)).await.is_err() {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(skipped = n, "WS broadcast lagged");
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("WebSocket handshake failed from {}: {}", peer, e);
                }
            }
        });
    }
}
