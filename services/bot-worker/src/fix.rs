use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

/// Custom FIX 4.2 session over bare TCP.
/// Tag=value fields delimited by SOH (\x01). No external FIX libraries.
pub struct FixSession {
    reader: OwnedReadHalf,
    writer: BufWriter<OwnedWriteHalf>,
    seq_num: u64,
    sender_comp_id: String,
    target_comp_id: String,
    /// Accumulates partial reads across non-blocking attempts.
    read_buf: Vec<u8>,
}

impl FixSession {
    /// Connect to a FIX acceptor at `addr` (host:port).
    pub async fn connect(
        addr: &str,
        sender_comp_id: &str,
        target_comp_id: &str,
    ) -> Result<Self, anyhow::Error> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer: BufWriter::new(writer),
            seq_num: 1,
            sender_comp_id: sender_comp_id.to_string(),
            target_comp_id: target_comp_id.to_string(),
            read_buf: Vec::new(),
        })
    }

    // ------------------------------------------------------------------
    //  Outbound message construction
    // ------------------------------------------------------------------

    /// Build a complete FIX message (including header, body, checksum).
    /// Wires standard header fields: BeginString, BodyLength, MsgType,
    /// MsgSeqNum, SenderCompID, TargetCompID, SendingTime.
    fn build_message(&mut self, msg_type: &str, body_fields: &[(&str, &str)]) -> Vec<u8> {
        let sending_time = chrono::Utc::now()
            .format("%Y%m%d-%H:%M:%S%.3f")
            .to_string();

        // ---- body (everything between BodyLength and Checksum) ----
        let mut body = String::new();
        body.push_str(&format!("35={}\x01", msg_type));
        body.push_str(&format!("34={}\x01", self.seq_num));
        body.push_str(&format!("49={}\x01", self.sender_comp_id));
        body.push_str(&format!("56={}\x01", self.target_comp_id));
        body.push_str(&format!("52={}\x01", sending_time));
        for (tag, value) in body_fields {
            body.push_str(&format!("{}={}\x01", tag, value));
        }

        let body_len = body.len();
        let header = format!("8=FIX.4.2\x019={}\x01", body_len);

        let mut msg = header + &body;

        // Checksum = sum of all bytes modulo 256, formatted as 3 digits.
        let checksum: u32 = msg.bytes().map(|b| b as u32).sum::<u32>() % 256;
        msg.push_str(&format!("10={:03}\x01", checksum));

        self.seq_num += 1;
        msg.into_bytes()
    }

    /// Build and send a FIX message.
    pub async fn send_message(
        &mut self,
        msg_type: &str,
        body_fields: &[(&str, &str)],
    ) -> Result<(), anyhow::Error> {
        let msg = self.build_message(msg_type, body_fields);
        self.writer.write_all(&msg).await?;
        self.writer.flush().await?;
        Ok(())
    }

    // ------------------------------------------------------------------
    //  Inbound message parsing
    // ------------------------------------------------------------------

    /// Split raw bytes on SOH, return tag→value map.
    pub fn parse_message(data: &[u8]) -> HashMap<String, String> {
        let mut map = HashMap::new();
        for part in data.split(|&b| b == b'\x01') {
            if part.is_empty() {
                continue;
            }
            if let Ok(s) = std::str::from_utf8(part) {
                if let Some(eq) = s.find('=') {
                    map.insert(s[..eq].to_string(), s[eq + 1..].to_string());
                }
            }
        }
        map
    }

    /// Scan `read_buf` for a complete FIX message (delimited by "10=NNN\x01").
    /// Returns the parsed message and drains consumed bytes.
    fn try_extract_message(&mut self) -> Option<HashMap<String, String>> {
        let buf = &self.read_buf;
        if buf.len() < 10 {
            return None;
        }

        // In standard FIX "10=" only appears as the checksum tag and is
        // always preceded by SOH (or is at offset 0 which can't happen here
        // because the message starts with "8=FIX.4.2\x01").
        for i in 0..buf.len().saturating_sub(6) {
            if (i == 0 || buf[i - 1] == b'\x01')
                && buf[i..].starts_with(b"10=")
                && buf[i + 3].is_ascii_digit()
                && buf[i + 4].is_ascii_digit()
                && buf[i + 5].is_ascii_digit()
                && buf[i + 6] == b'\x01'
            {
                // Sanity-check the message preamble.
                if !buf.starts_with(b"8=FIX.4.2\x01") {
                    // Skip past the garbage (advance past first SOH).
                    if let Some(pos) = buf[1..].iter().position(|&b| b == b'\x01') {
                        self.read_buf.drain(..pos + 2);
                    }
                    return None;
                }
                let end = i + 7;
                let msg_data = self.read_buf.drain(..end).collect::<Vec<_>>();
                return Some(Self::parse_message(&msg_data));
            }
        }
        None
    }

    /// Non-blocking: read any available data and try to extract a message.
    /// Returns `None` if no complete message is available yet.
    pub fn try_read_message(&mut self) -> Option<HashMap<String, String>> {
        if let Some(msg) = self.try_extract_message() {
            return Some(msg);
        }

        let mut chunk = [0u8; 4096];
        let r = self.reader.try_read(&mut chunk);
        match r {
            Ok(0) => None,
            Ok(n) => {
                self.read_buf.extend_from_slice(&chunk[..n]);
                self.try_extract_message()
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    None
                } else {
                    tracing::error!("FIX read error: {}", e);
                    None
                }
            }
        }
    }

    /// Block until a complete message arrives.
    pub async fn read_message(
        &mut self,
    ) -> Result<HashMap<String, String>, anyhow::Error> {
        loop {
            if let Some(msg) = self.try_extract_message() {
                return Ok(msg);
            }
            self.reader.readable().await?;
            let mut chunk = [0u8; 4096];
            let r = self.reader.try_read(&mut chunk);
            match r {
                Ok(0) => return Err(anyhow::anyhow!("Connection closed")),
                Ok(n) => {
                    self.read_buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }
    }

    // ------------------------------------------------------------------
    //  FIX application-level helpers
    // ------------------------------------------------------------------

    /// Send Logon (35=A) and wait for a Logon response.
    pub async fn logon(&mut self) -> Result<(), anyhow::Error> {
        self.send_message("A", &[]).await?;
        let resp = self.read_message().await?;
        if resp.get("35") != Some(&"A".to_string()) {
            return Err(anyhow::anyhow!("Expected Logon (35=A) response"));
        }
        Ok(())
    }

    /// Send Heartbeat (35=0).
    pub async fn heartbeat(&mut self) -> Result<(), anyhow::Error> {
        self.send_message("0", &[]).await
    }

    /// Send NewOrderSingle (35=D).
    pub async fn new_order_single(
        &mut self,
        cl_ord_id: &str,
        side: &str,
        qty: &str,
        price: &str,
        ord_type: &str,
    ) -> Result<(), anyhow::Error> {
        self.send_message(
            "D",
            &[
                ("11", cl_ord_id), // ClOrdID
                ("55", "AAPL"),    // Symbol
                ("54", side),      // Side (1=buy, 2=sell)
                ("38", qty),       // OrderQty
                ("40", ord_type),  // OrdType (1=market, 2=limit)
                ("44", price),     // Price (0 for market orders)
            ],
        )
        .await
    }

    /// Send OrderCancelRequest (35=F).
    pub async fn cancel_request(
        &mut self,
        cl_ord_id: &str,
        orig_cl_ord_id: &str,
    ) -> Result<(), anyhow::Error> {
        self.send_message(
            "F",
            &[
                ("11", cl_ord_id),       // ClOrdID (new)
                ("41", orig_cl_ord_id),  // OrigClOrdID (order to cancel)
                ("55", "AAPL"),          // Symbol
                ("54", "1"),             // Side (default buy)
            ],
        )
        .await
    }

    /// Send Logout (35=5).
    pub async fn logout(&mut self) -> Result<(), anyhow::Error> {
        self.send_message("5", &[]).await
    }

    /// Drain incoming messages until a Logout (35=5) is received, then
    /// respond with our own Logout.
    pub async fn read_until_logout(&mut self) -> Result<(), anyhow::Error> {
        loop {
            let msg = self.read_message().await?;
            if msg.get("35") == Some(&"5".to_string()) {
                self.logout().await?;
                return Ok(());
            }
        }
    }
}
