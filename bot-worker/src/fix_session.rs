use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::BufReader;
use tokio::sync::mpsc;
use tokio::time::Duration;

use fixer::application::Application;
use fixer::initiator::Initiator;
use fixer::log::screen_log::ScreenLogFactory;
use fixer::message::Message;
use fixer::registry;
use fixer::session::session_id::SessionID;
use fixer::settings::Settings;
use fixer::store::MemoryStoreFactory;
use fixer::errors::MessageRejectErrorResult;
use fixer_fix::tag;
use fixer_fix::enums;

struct BotApp {
    response_tx: mpsc::UnboundedSender<Message>,
    connected: Arc<AtomicBool>,
}

impl Application for BotApp {
    fn on_create(&self, _session_id: &Arc<SessionID>) {}
    fn on_logon(&self, _session_id: &Arc<SessionID>) {
        self.connected.store(true, Ordering::Relaxed);
    }
    fn on_logout(&self, _session_id: &Arc<SessionID>) {
        self.connected.store(false, Ordering::Relaxed);
    }
    fn to_admin(&self, _msg: &mut Message, _session_id: &Arc<SessionID>) {}
    fn to_app(&self, _msg: &mut Message, _session_id: &Arc<SessionID>) -> simple_error::SimpleResult<()> { Ok(()) }
    fn from_admin(&self, _msg: &Message, _session_id: &Arc<SessionID>) -> MessageRejectErrorResult { Ok(()) }
    fn from_app(&self, msg: &Message, _session_id: &Arc<SessionID>) -> MessageRejectErrorResult {
        // Log ALL incoming FIX app messages
        if let Ok(cl) = msg.body.get_string(tag::CL_ORD_ID) {
            if let Ok(et) = msg.body.get_string(tag::EXEC_TYPE) {
                if let Ok(seq) = msg.body.get_string(tag::EXEC_ID) {
                    eprintln!("[FIX-FROM-APP] cl_ord_id={cl} exec_type={et} exec_seq={seq}");
                } else if let Ok(si) = msg.body.get_int(tag::EXEC_ID) {
                    eprintln!("[FIX-FROM-APP] cl_ord_id={cl} exec_type={et} exec_seq={si}");
                } else {
                    eprintln!("[FIX-FROM-APP] cl_ord_id={cl} exec_type={et} exec_seq=???");
                }
            }
        } else {
            if let Ok(et) = msg.body.get_string(tag::EXEC_TYPE) {
                eprintln!("[FIX-FROM-APP] exec_type={et} (no cl_ord_id)");
            } else {
                eprintln!("[FIX-FROM-APP] msg_type={:?}", msg.header.get_string(tag::MSG_TYPE));
            }
        }
        if self.response_tx.send(msg.clone()).is_err() {
            eprintln!("[FIX] from_app: response channel closed, dropping message");
        }
        Ok(())
    }
}

/// One FIX session via fixer::initiator::Initiator.
pub struct FixSession {
    initiator: Option<Initiator>,
    response_rx: mpsc::UnboundedReceiver<Message>,
    sender: String,
    target: String,
}

impl FixSession {
    pub async fn connect(
        host: &str,
        port: u16,
        sender: &str,
        target: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = format!(
            "\
[DEFAULT]
ConnectionType=Initiator
SocketConnectHost={host}
SocketConnectPort={port}
SenderCompID={sender}
TargetCompID={target}
HeartBtInt=30
ReconnectInterval=2
ResetOnLogon=Y
FileStorePath=/tmp/fix-sessions/{sender}

[SESSION]
BeginString=FIX.4.2
"
        );

        let settings = Settings::parse(BufReader::new(cfg.as_bytes())).await?;
        let (response_tx, response_rx) = mpsc::unbounded_channel();
        let connected = Arc::new(AtomicBool::new(false));
        let app: Arc<dyn Application> = Arc::new(BotApp {
            response_tx,
            connected: Arc::clone(&connected),
        });

        let mut initiator = Initiator::new(
            app,
            MemoryStoreFactory::new(),
            settings,
            ScreenLogFactory::new(),
        )
        .await?;

        initiator.start().await?;

        for _ in 0..100 {
            if connected.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Ok(Self {
            initiator: Some(initiator),
            response_rx,
            sender: sender.to_string(),
            target: target.to_string(),
        })
    }

    /// Send a NewOrderSingle (35=D). Sets MsgType + body fields.
    /// Also sets BeginString, SenderCompID, TargetCompID so send_owned
    /// can route the message to the correct session.
    pub async fn send_order(
        &mut self,
        cl_ord_id: &str,
        side: &str,
        symbol: &str,
        qty: u64,
        price: f64,
        is_market: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut msg = Message::new();
        msg.header.set_string(tag::MSG_TYPE, "D");
        msg.header.set_string(tag::BEGIN_STRING, "FIX.4.2");
        msg.header.set_string(tag::SENDER_COMP_ID, &self.sender);
        msg.header.set_string(tag::TARGET_COMP_ID, &self.target);
        msg.body.set_string(tag::CL_ORD_ID, cl_ord_id);
        msg.body.set_int(tag::HANDL_INST, 1);
        msg.body.set_string(tag::SYMBOL, symbol);
        msg.body.set_string(tag::SIDE, side);
        msg.body.set_int(tag::ORDER_QTY, qty as isize);
        msg.body.set_string(
            tag::TRANSACT_TIME,
            &chrono::Utc::now().format("%Y%m%d-%H:%M:%S").to_string(),
        );
        if is_market {
            msg.body.set_string(tag::ORD_TYPE, "1");
        } else {
            msg.body.set_string(tag::ORD_TYPE, "2");
            msg.body.set_string(tag::PRICE, &format!("{:.2}", price));
        }
        Ok(registry::send_owned(msg).await?)
    }

    pub async fn recv_execution_report(&mut self) -> Option<Message> {
        loop {
            let msg = self.response_rx.recv().await?;
            if msg.is_msg_type_of(enums::msg_type::EXECUTION_REPORT) {
                return Some(msg);
            }
        }
    }

    /// Try to receive any pending execution report without blocking.
    pub fn try_recv_execution_report(&mut self) -> Option<Message> {
        loop {
            match self.response_rx.try_recv() {
                Ok(msg) => {
                    if msg.is_msg_type_of(enums::msg_type::EXECUTION_REPORT) {
                        return Some(msg);
                    }
                    // Non-execution-report, skip
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return None,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return None,
            }
        }
    }

    pub async fn logout(mut self) {
        let mut msg = Message::new();
        msg.header.set_string(tag::MSG_TYPE, "5");
        msg.header.set_string(tag::BEGIN_STRING, "FIX.4.2");
        msg.header.set_string(tag::SENDER_COMP_ID, &self.sender);
        msg.header.set_string(tag::TARGET_COMP_ID, &self.target);
        if let Err(e) = registry::send_owned(msg).await {
            eprintln!("[FIX] logout send failed: {e}");
        }
        if let Some(mut init) = self.initiator.take() {
            init.stop().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_order_has_routing_headers() {
        let mut msg = Message::new();
        msg.header.set_string(tag::MSG_TYPE, "D");
        msg.header.set_string(tag::BEGIN_STRING, "FIX.4.2");
        msg.header.set_string(tag::SENDER_COMP_ID, "BOT");
        msg.header.set_string(tag::TARGET_COMP_ID, "XCANG3");
        msg.body.set_string(tag::CL_ORD_ID, "t1");
        msg.body.set_string(tag::ORD_TYPE, "2");
        msg.body.set_string(tag::PRICE, "100.50");
        assert_eq!(msg.header.get_string(tag::BEGIN_STRING).unwrap(), "FIX.4.2");
        assert_eq!(msg.header.get_string(tag::SENDER_COMP_ID).unwrap(), "BOT");
        assert_eq!(msg.header.get_string(tag::TARGET_COMP_ID).unwrap(), "XCANG3");
    }

    #[test]
    fn market_order_no_price() {
        let mut msg = Message::new();
        msg.header.set_string(tag::MSG_TYPE, "D");
        msg.header.set_string(tag::BEGIN_STRING, "FIX.4.2");
        msg.body.set_string(tag::ORD_TYPE, "1");
        assert!(msg.body.get_string(tag::PRICE).is_err());
    }
}
