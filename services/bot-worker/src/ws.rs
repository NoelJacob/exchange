use crate::ordergen::{ExecutionMessage, OrderMessage};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Simple WebSocket client for the bot-worker.
/// Wraps a split tokio-tungstenite stream so send and receive can be
/// used independently without pinning gymnastics.
pub struct WsSession {
    writer: SplitSink<WsStream, Message>,
    reader: SplitStream<WsStream>,
}

impl WsSession {
    /// Connect to a WebSocket endpoint.
    pub async fn connect(url: &str) -> Result<Self, anyhow::Error> {
        let (ws_stream, _) = connect_async(url).await?;
        let (writer, reader) = ws_stream.split();
        Ok(Self { writer, reader })
    }

    /// Serialize an `OrderMessage` to JSON and send as a text frame.
    pub async fn send_order(&mut self, order: &OrderMessage) -> Result<(), anyhow::Error> {
        let json = serde_json::to_string(order)?;
        self.writer.send(Message::Text(json.into())).await?;
        Ok(())
    }

    /// Read one frame and attempt to deserialize as an `ExecutionMessage`.
    /// Returns `None` on connection close, protocol error, or non-JSON frame.
    pub async fn read_execution(&mut self) -> Option<ExecutionMessage> {
        let msg = self.reader.next().await?;
        match msg.ok()? {
            Message::Text(text) => serde_json::from_str(&text).ok(),
            Message::Binary(data) => serde_json::from_slice(&data).ok(),
            Message::Close(_) => None,
            _ => None,
        }
    }

    /// Send a raw text frame.
    pub async fn send_text(&mut self, text: &str) -> Result<(), anyhow::Error> {
        self.writer.send(Message::Text(text.into())).await?;
        Ok(())
    }
}
