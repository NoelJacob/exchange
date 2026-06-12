use std::sync::atomic::{AtomicUsize, Ordering};

use fixer::message::Message;
use fixer_fix::tag;

use crate::config::Config;
use crate::fix_session::FixSession;
use crate::ws_client::{WsClient, WsResponse};

/// Manages N FIX sessions + M WS connections with round-robin dispatch.
pub struct SessionPool {
    fix_sessions: Vec<FixSession>,
    ws_clients: Vec<WsClient>,
    fix_idx: AtomicUsize,
    ws_idx: AtomicUsize,
    sender_prefix: String,
    /// Stray FIX maker-fill notifications that arrived out of band.
    pub fix_notifications: Vec<Message>,
}

impl SessionPool {
    /// Create all FIX sessions and WS connections.
    pub async fn connect(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let mut fix_sessions = Vec::with_capacity(config.fix_connections as usize);
        let mut ws_clients = Vec::with_capacity(config.ws_connections as usize);

        for i in 0..config.fix_connections {
            let sender = format!("{}{:02}", config.sender_comp_id_prefix, i);
            eprintln!("[Pool] FIX {sender} connecting...");
            let session = FixSession::connect(
                &config.target_host,
                config.fix_port,
                &sender,
                &config.target_comp_id,
            )
            .await?;
            fix_sessions.push(session);
        }

        for i in 0..config.ws_connections {
            eprintln!("[Pool] WS {i} connecting...");
            let client = WsClient::connect(&config.target_host, config.ws_port).await?;
            ws_clients.push(client);
        }

        Ok(Self {
            fix_sessions,
            ws_clients,
            fix_idx: AtomicUsize::new(0),
            ws_idx: AtomicUsize::new(0),
            sender_prefix: config.sender_comp_id_prefix.clone(),
            fix_notifications: Vec::new(),
        })
    }

    /// Round-robin FIX: send, then match responses by cl_ord_id.
    /// Non-matching stray messages (maker fills) are accumulated into
    /// `fix_notifications` for later processing.
    pub async fn send_fix(
        &mut self,
        cl_ord_id: &str,
        side: &str,
        symbol: &str,
        qty: u64,
        price: f64,
        is_market: bool,
    ) -> Result<(Message, u64), Box<dyn std::error::Error>> {
        if self.fix_sessions.is_empty() {
            return Err("no FIX sessions available".into());
        }
        let idx = self.fix_idx.fetch_add(1, Ordering::Relaxed) % self.fix_sessions.len();
        let session = &mut self.fix_sessions[idx];
        let send_start = std::time::Instant::now();
        session.send_order(cl_ord_id, side, symbol, qty, price, is_market).await?;

        // Loop until we get the response matching our cl_ord_id.
        // Stray messages (maker fills for other orders) accumulate in fix_notifications.
        loop {
            let msg = session
                .recv_execution_report()
                .await
                .ok_or("FIX session closed before response")?;
            let resp_cl = msg.body.get_string(tag::CL_ORD_ID).unwrap_or_default();

            let seq = msg.body.get_string(tag::EXEC_ID).unwrap_or_default();
            let et = msg.body.get_string(tag::EXEC_TYPE).unwrap_or_default();
            eprintln!("[FIX-POOL-READ] want={cl_ord_id} got={resp_cl} exec_type={et} exec_seq={seq}");

            if resp_cl == cl_ord_id {
                let latency_us = send_start.elapsed().as_micros() as u64;
                eprintln!("[FIX-POOL-MATCH] matched cl_ord_id={cl_ord_id} in {latency_us}µs");
                return Ok((msg, latency_us));
            }

            // Stray maker fill — push to notifications
            eprintln!("[FIX-POOL-STRAY] {resp_cl} exec_type={et} exec_seq={seq} → queued");
            self.fix_notifications.push(msg);
        }
    }

    /// Drain accumulated FIX notifications (maker fills consumed as stray).
    pub fn drain_fix_notifications(&mut self) -> Vec<Message> {
        std::mem::take(&mut self.fix_notifications)
    }

    /// Round-robin WS: send, recv, return (WsResponse, latency_us).
    pub async fn send_ws(
        &mut self,
        sender_id: &str,
        cl_ord_id: &str,
        side: &str,
        symbol: &str,
        qty: u64,
        price: Option<f64>,
    ) -> Result<(WsResponse, u64), Box<dyn std::error::Error>> {
        if self.ws_clients.is_empty() {
            return Err("no WS clients available".into());
        }
        let idx = self.ws_idx.fetch_add(1, Ordering::Relaxed) % self.ws_clients.len();
        let client = &mut self.ws_clients[idx];

        let send_start = std::time::Instant::now();
        let resp = match price {
            Some(p) => client.send_limit_order(sender_id, cl_ord_id, side, symbol, qty, p).await?,
            None => client.send_market_order(sender_id, cl_ord_id, side, symbol, qty).await?,
        };
        let latency_us = send_start.elapsed().as_micros() as u64;
        Ok((resp, latency_us))
    }

    /// Poll all WS clients for pending notifications.
    pub async fn poll_notifications(&mut self) -> Vec<WsResponse> {
        let mut all = Vec::new();
        for client in &mut self.ws_clients {
            while let Some(n) = client.try_recv_notification().await {
                all.push(n);
            }
        }
        all
    }

    /// Drain ALL FIX sessions of pending execution reports (not just stray ones).
    pub async fn drain_all_fix(&mut self) -> Vec<Message> {
        let mut all = Vec::new();
        for session in &mut self.fix_sessions {
            while let Some(msg) = session.try_recv_execution_report() {
                all.push(msg);
            }
        }
        all
    }

    /// Shutdown all connections.
    pub async fn shutdown(self) {
        for (i, s) in self.fix_sessions.into_iter().enumerate() {
            eprintln!("[Pool] FIX session {i} logging out...");
            s.logout().await;
        }
        for (i, c) in self.ws_clients.into_iter().enumerate() {
            eprintln!("[Pool] WS client {i} closing...");
            c.close().await;
        }
    }

    /// Get the sender ID prefix for this pool.
    pub fn sender_id(&self) -> String {
        format!("{}00", self.sender_prefix)
    }
}
