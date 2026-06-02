use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use fixer::application::Application;
use fixer::errors::MessageRejectErrorResult;
use fixer::log::screen_log::ScreenLogFactory;
use fixer::message::Message;
use fixer::registry::send_to_target_owned;
use fixer::session::session_id::SessionID;
use fixer::settings::Settings;
use fixer::store::MemoryStoreFactory;
use fixer::tag::TAG_MSG_TYPE;
use simple_error::SimpleResult;
use tokio::io::BufReader;
use tokio::sync::mpsc;

// FIX 4.2 body field tags (not in fixer::tag)
const TAG_CL_ORD_ID: fixer::tag::Tag = 11;
const TAG_EXEC_ID: fixer::tag::Tag = 17;
const TAG_EXEC_TRANS_TYPE: fixer::tag::Tag = 101;
const TAG_ORDER_ID: fixer::tag::Tag = 37;
const TAG_ORDER_QTY: fixer::tag::Tag = 38;
const TAG_ORD_STATUS: fixer::tag::Tag = 39;
const TAG_ORD_TYPE: fixer::tag::Tag = 40;
const TAG_PRICE: fixer::tag::Tag = 44;
const TAG_SIDE: fixer::tag::Tag = 54;
const TAG_SYMBOL: fixer::tag::Tag = 55;
const TAG_AVG_PX: fixer::tag::Tag = 6;
const TAG_LAST_SHARES: fixer::tag::Tag = 32;
const TAG_LAST_PX: fixer::tag::Tag = 31;
const TAG_EXEC_TYPE: fixer::tag::Tag = 150;
const TAG_LEAVES_QTY: fixer::tag::Tag = 151;
const TAG_CUM_QTY: fixer::tag::Tag = 14;

/// Pending reply to send through the channel.
struct PendingReply {
    msg: Message,
    session_id: Arc<SessionID>,
}

struct FixApp {
    reply_tx: mpsc::UnboundedSender<PendingReply>,
    books: crate::Books,
    order_id: AtomicU32,
    exec_id: AtomicU32,
}

impl FixApp {
    fn gen_order_id(&self) -> u32 {
        self.order_id.fetch_add(1, Ordering::Relaxed)
    }

    fn gen_exec_id(&self) -> u32 {
        self.exec_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Build and queue a Reject ExecutionReport (35=8, 150=8, 39=8).
    fn send_reject(
        &self,
        msg: &Message,
        session_id: &Arc<SessionID>,
        cl_ord_id: Option<&str>,
        symbol: Option<&str>,
        reason: &str,
    ) {
        eprintln!("[FIX] Reject: {reason}");
        let mut reply = msg.reverse_route();
        reply.header.set_string(TAG_MSG_TYPE, "8");
        reply.body.set_int(TAG_EXEC_ID, self.gen_exec_id() as isize);
        reply.body.set_string(TAG_EXEC_TRANS_TYPE, "0");
        reply.body.set_string(TAG_EXEC_TYPE, "8");
        reply.body.set_string(TAG_ORD_STATUS, "8");
        if let Some(c) = cl_ord_id {
            reply.body.set_string(TAG_CL_ORD_ID, c);
        }
        if let Some(s) = symbol {
            reply.body.set_string(TAG_SYMBOL, s);
        }
        let _ = self.reply_tx.send(PendingReply {
            msg: reply,
            session_id: Arc::clone(session_id),
        });
    }

    /// Process a NewOrderSingle and build an ExecutionReport.
    fn process_new_order(&self, msg: &Message, session_id: &Arc<SessionID>) {
        // 1. Extract all fields as Options so partial data is preserved for the reject site.
        let cl_ord_id_opt = msg.body.get_string(TAG_CL_ORD_ID).ok();
        let symbol_opt = msg.body.get_string(TAG_SYMBOL).ok();
        let side_opt = msg.body.get_string(TAG_SIDE).ok();
        let order_qty_opt = msg.body.get_int(TAG_ORDER_QTY).ok();
        let ord_type_opt = msg.body.get_string(TAG_ORD_TYPE).ok();
        let price_raw_opt = msg.body.get_string(TAG_PRICE).ok();

        // 2. ClOrdID
        let Some(cl_ord_id) = cl_ord_id_opt else {
            self.send_reject(msg, session_id, None, None, "missing ClOrdID (tag 11)");
            return;
        };

        // 3. Symbol
        let Some(symbol) = symbol_opt else {
            self.send_reject(
                msg,
                session_id,
                Some(&cl_ord_id),
                None,
                "missing Symbol (tag 55)",
            );
            return;
        };

        // 4. Side
        let Some(side_val) = side_opt else {
            self.send_reject(
                msg,
                session_id,
                Some(&cl_ord_id),
                Some(&symbol),
                "missing Side (tag 54)",
            );
            return;
        };

        // 5. OrderQty
        let Some(order_qty_raw) = order_qty_opt else {
            self.send_reject(
                msg,
                session_id,
                Some(&cl_ord_id),
                Some(&symbol),
                "missing OrderQty (tag 38)",
            );
            return;
        };
        let order_qty = order_qty_raw as u64;

        // 6. OrdType (default to limit = "2" if missing, matches existing behaviour)
        let ord_type = ord_type_opt.unwrap_or_else(|| "2".to_string());
        let is_market = ord_type == "1";

        // 7. Side value mapping
        let ob_side = match side_val.as_str() {
            "1" => orderbook_rs::Side::Buy,
            "2" => orderbook_rs::Side::Sell,
            _ => {
                self.send_reject(
                    msg,
                    session_id,
                    Some(&cl_ord_id),
                    Some(&symbol),
                    "invalid Side value (tag 54), expected 1=Buy or 2=Sell",
                );
                return;
            }
        };

        // 8. Book lookup (also rejects empty/whitespace symbol)
        let book = match crate::get_or_create_book(&self.books, &symbol) {
            Some(b) => b,
            None => {
                self.send_reject(
                    msg,
                    session_id,
                    Some(&cl_ord_id),
                    Some(&symbol),
                    "empty/whitespace Symbol (tag 55)",
                );
                return;
            }
        };

        // 9. Price validation (E6) — market orders ignore price, limit orders require positive f64
        let price: f64 = if is_market {
            0.0
        } else {
            let Some(price_raw) = price_raw_opt else {
                self.send_reject(
                    msg,
                    session_id,
                    Some(&cl_ord_id),
                    Some(&symbol),
                    "missing Price (tag 44) for limit order",
                );
                return;
            };
            match price_raw.parse::<f64>() {
                Ok(p) if p > 0.0 => p,
                Ok(_) => {
                    self.send_reject(
                        msg,
                        session_id,
                        Some(&cl_ord_id),
                        Some(&symbol),
                        "non-positive Price (tag 44) for limit order",
                    );
                    return;
                }
                Err(_) => {
                    self.send_reject(
                        msg,
                        session_id,
                        Some(&cl_ord_id),
                        Some(&symbol),
                        "unparseable Price (tag 44) for limit order",
                    );
                    return;
                }
            }
        };

        let price_cents = (price * 100.0).round() as u128;

        eprintln!(
            "[FIX] NewOrderSingle cl_ord_id={cl_ord_id} symbol={symbol} side={} price={price_cents} qty={order_qty}",
            if ob_side == orderbook_rs::Side::Buy { "Buy" } else { "Sell" },
        );

        let outcome = match crate::submit(&book, is_market, ob_side, order_qty, price_cents) {
            Ok(o) => o,
            Err(e) => {
                self.send_reject(
                    msg,
                    session_id,
                    Some(&cl_ord_id),
                    Some(&symbol),
                    &e.to_string(),
                );
                return;
            }
        };

        let total_fill_qty = outcome.filled_qty;
        let avg_fill_price_cents = outcome.avg_price_cents;
        let leaves = order_qty.saturating_sub(total_fill_qty);
        let (exec_type, ord_status) = if leaves == 0 && total_fill_qty > 0 {
            ("2", "2") // Fill / Filled
        } else if total_fill_qty > 0 {
            ("1", "1") // PartialFill / PartiallyFilled
        } else {
            ("0", "0") // New / New
        };

        let avg_px = if total_fill_qty > 0 {
            format!("{:.2}", avg_fill_price_cents as f64 / 100.0)
        } else {
            "0".to_string()
        };

        // Build ExecutionReport using the swap-routing pattern: reverse_route()
        // creates a fresh Message with sender/target swapped and body empty.
        let mut reply = msg.reverse_route();
        reply.header.set_string(TAG_MSG_TYPE, "8");

        // Body fields
        reply.body.set_int(TAG_ORDER_ID, self.gen_order_id() as isize);
        reply.body.set_int(TAG_EXEC_ID, self.gen_exec_id() as isize);
        reply.body.set_string(TAG_EXEC_TRANS_TYPE, "0");
        reply.body.set_string(TAG_EXEC_TYPE, exec_type);
        reply.body.set_string(TAG_ORD_STATUS, ord_status);
        reply.body.set_string(TAG_SYMBOL, &symbol);
        reply.body.set_string(TAG_SIDE, &side_val);
        reply.body.set_int(TAG_LEAVES_QTY, leaves as isize);
        reply.body.set_int(TAG_CUM_QTY, total_fill_qty as isize);
        reply.body.set_string(TAG_AVG_PX, &avg_px);
        reply.body.set_string(TAG_CL_ORD_ID, &cl_ord_id);

        if total_fill_qty > 0 {
            reply.body.set_int(TAG_LAST_SHARES, total_fill_qty as isize);
            reply.body.set_string(TAG_LAST_PX, &avg_px);
        }

        // Queue reply for async send
        let _ = self.reply_tx.send(PendingReply {
            msg: reply,
            session_id: Arc::clone(session_id),
        });
    }
}

impl Application for FixApp {
    fn on_create(&self, session_id: &Arc<SessionID>) {
        eprintln!("[FIX] Session created: {session_id}");
    }

    fn on_logon(&self, session_id: &Arc<SessionID>) {
        eprintln!("[FIX] Logon: {session_id}");
    }

    fn on_logout(&self, session_id: &Arc<SessionID>) {
        eprintln!("[FIX] Logout: {session_id}");
    }

    fn to_admin(&self, _msg: &mut Message, _session_id: &Arc<SessionID>) {}

    fn to_app(&self, _msg: &mut Message, _session_id: &Arc<SessionID>) -> SimpleResult<()> {
        Ok(())
    }

    fn from_admin(&self, _msg: &Message, _session_id: &Arc<SessionID>) -> MessageRejectErrorResult {
        Ok(())
    }

    fn from_app(&self, msg: &Message, session_id: &Arc<SessionID>) -> MessageRejectErrorResult {
        if msg.is_msg_type_of("D") {
            self.process_new_order(msg, session_id);
        }
        Ok(())
    }
}

pub fn run(books: crate::Books) -> Result<(), Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let cfg = "\
[DEFAULT]
SocketAcceptPort=9090
HeartBtInt=30
CheckLatency=N

[SESSION]
BeginString=FIX.4.2
SenderCompID=SERVER
TargetCompID=CLIENT
ResetOnLogon=Y
";
        let settings = Settings::parse(BufReader::new(cfg.as_bytes())).await?;

        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<PendingReply>();

        let app: Arc<dyn Application> = Arc::new(FixApp {
            reply_tx,
            books,
            order_id: AtomicU32::new(1),
            exec_id: AtomicU32::new(1),
        });

        let store_factory = MemoryStoreFactory::new();
        let log_factory = ScreenLogFactory::new();

        let mut acceptor =
            fixer::acceptor::Acceptor::new(app, store_factory, settings, log_factory).await?;

        acceptor.start().await?;
        eprintln!("[FIX] Acceptor started on port 9090");

        // Background task: drain reply channel and send messages
        tokio::spawn(async move {
            while let Some(PendingReply { msg, session_id }) = reply_rx.recv().await {
                if let Err(e) = send_to_target_owned(msg, &session_id).await {
                    eprintln!("[FIX] Send error: {e}");
                }
            }
        });

        // Keep alive
        tokio::signal::ctrl_c().await.ok();
        acceptor.stop().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
