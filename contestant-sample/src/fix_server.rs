use std::sync::Arc;

use fixer::application::Application;
use fixer::errors::{
    incorrect_data_format_for_value, required_tag_missing, value_is_incorrect,
    MessageRejectErrorResult,
};
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
pub const TAG_CL_ORD_ID: fixer::tag::Tag = 11;
pub const TAG_EXEC_ID: fixer::tag::Tag = 17;
pub const TAG_EXEC_TRANS_TYPE: fixer::tag::Tag = 101;
pub const TAG_ORDER_ID: fixer::tag::Tag = 37;
pub const TAG_ORDER_QTY: fixer::tag::Tag = 38;
pub const TAG_ORD_STATUS: fixer::tag::Tag = 39;
pub const TAG_ORD_TYPE: fixer::tag::Tag = 40;
pub const TAG_PRICE: fixer::tag::Tag = 44;
pub const TAG_SIDE: fixer::tag::Tag = 54;
pub const TAG_SYMBOL: fixer::tag::Tag = 55;
pub const TAG_AVG_PX: fixer::tag::Tag = 6;
pub const TAG_LAST_SHARES: fixer::tag::Tag = 32;
pub const TAG_LAST_PX: fixer::tag::Tag = 31;
pub const TAG_EXEC_TYPE: fixer::tag::Tag = 150;
pub const TAG_LEAVES_QTY: fixer::tag::Tag = 151;
pub const TAG_CUM_QTY: fixer::tag::Tag = 14;
pub const TAG_TRANSACT_TIME: fixer::tag::Tag = 60;
pub const TAG_SENDER_COMP_ID: fixer::tag::Tag = 49;

/// Pending reply to send through the channel.
pub struct PendingReply {
    pub msg: Message,
    pub session_id: Arc<SessionID>,
}

struct FixApp {
    reply_tx: mpsc::UnboundedSender<PendingReply>,
    state: Arc<crate::AppState>,
}

impl FixApp {
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
        let exec_id = self
            .state
            .exec_id_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut reply = msg.reverse_route();
        reply.header.set_string(TAG_MSG_TYPE, "8");
        reply.body.set_int(TAG_EXEC_ID, exec_id as isize);
        reply.body.set_string(TAG_EXEC_TRANS_TYPE, "0");
        reply.body.set_string(TAG_EXEC_TYPE, "8");
        reply.body.set_string(TAG_ORD_STATUS, "8");
        if let Some(c) = cl_ord_id {
            reply.body.set_string(TAG_CL_ORD_ID, c);
        }
        if let Some(s) = symbol {
            reply.body.set_string(TAG_SYMBOL, s);
        }
        reply
            .body
            .set_string(TAG_TRANSACT_TIME, &crate::tag60_now());
        let _ = self.reply_tx.send(PendingReply {
            msg: reply,
            session_id: Arc::clone(session_id),
        });
    }

    /// Process a validated NewOrderSingle and send the sync ExecutionReport.
    #[allow(clippy::too_many_arguments)]
    fn process_new_order(
        &self,
        msg: &Message,
        session_id: &Arc<SessionID>,
        cl_ord_id: &str,
        symbol: &str,
        side_val: &str,
        order_qty: u64,
        ord_type: &str,
        price: f64,
    ) {
        let is_market = ord_type == "1";

        let ob_side = match side_val {
            "1" => orderbook_rs::Side::Buy,
            "2" => orderbook_rs::Side::Sell,
            _ => unreachable!(), // validated in from_app
        };

        let price_cents = if is_market {
            0
        } else {
            (price * 100.0).round() as u128
        };

        // Extract SenderCompID for STP user hash and omnibus support
        let sender_comp = msg
            .header
            .get_string(TAG_SENDER_COMP_ID)
            .unwrap_or_else(|_| "UNKNOWN".to_string());

        let user_hash = crate::hash_user_id(&sender_comp);

        eprintln!(
            "[FIX] NewOrderSingle cl_ord_id={cl_ord_id} symbol={symbol} side={side_val} price={price_cents} qty={order_qty} sender={sender_comp}",
        );

        // Lock books, get/create book with TradeListener (scoped — released before submit)
        let book = {
            let mut books = self.state.books.lock();
            match crate::get_or_create_book(&mut books, symbol, &self.state.trade_tx) {
                Some(b) => b,
                None => {
                    drop(books);
                    self.send_reject(
                        msg,
                        session_id,
                        Some(cl_ord_id),
                        Some(symbol),
                        "empty/whitespace Symbol (tag 55)",
                    );
                    return;
                }
            }
        };

        let info = crate::OrderInfo {
            user_id: sender_comp.clone(),
            target_id: sender_comp.clone(),
            cl_ord_id: cl_ord_id.to_string(),
            order_qty,
            cum_value_cents: 0,
            ex_ord_id: String::new(),
            connection_kind: crate::ConnectionKind::Fix {
                session_id: Arc::clone(session_id),
                reply_tx: self.reply_tx.clone(),
            },
        };

        let info_for_report = crate::OrderInfo {
            ex_ord_id: String::new(),
            ..info.clone()
        };

        let outcome = match crate::submit(
            &book,
            is_market,
            ob_side,
            order_qty,
            price_cents,
            user_hash,
            &mut self.state.pending.lock(),
            info,
            &self.state.order_id_seq,
            &self.state.exec_id_seq,
        ) {
            Ok(o) => o,
            Err(e) => {
                self.send_reject(
                    msg,
                    session_id,
                    Some(cl_ord_id),
                    Some(symbol),
                    &e.to_string(),
                );
                return;
            }
        };

        // Build ExecutionReport from outcome and send FIX 35=8
        let report =
            crate::build_execution_report(&outcome, &info_for_report, symbol, ob_side, order_qty);
        let mut reply = msg.reverse_route();
        exec_report_to_fix_body(&report, &mut reply);
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
        if !msg.is_msg_type_of("D") {
            return Ok(());
        }

        // --- session-level validation: return Err → library sends 35=3 ---

        let cl_ord_id = msg
            .body
            .get_string(TAG_CL_ORD_ID)
            .map_err(|_| required_tag_missing(TAG_CL_ORD_ID))?;

        let symbol = msg
            .body
            .get_string(TAG_SYMBOL)
            .map_err(|_| required_tag_missing(TAG_SYMBOL))?;

        let side_str = msg
            .body
            .get_string(TAG_SIDE)
            .map_err(|_| required_tag_missing(TAG_SIDE))?;

        let order_qty_raw = msg
            .body
            .get_int(TAG_ORDER_QTY)
            .map_err(|_| required_tag_missing(TAG_ORDER_QTY))?;
        let order_qty = order_qty_raw as u64;
        if order_qty == 0 {
            return Err(required_tag_missing(TAG_ORDER_QTY));
        }
        if order_qty_raw < 0 {
            return Err(value_is_incorrect(TAG_ORDER_QTY));
        }

        // Side value must be 1 (Buy) or 2 (Sell)
        match side_str.as_str() {
            "1" | "2" => {}
            _ => return Err(value_is_incorrect(TAG_SIDE)),
        }

        // OrdType (default limit if missing)
        let ord_type = msg
            .body
            .get_string(TAG_ORD_TYPE)
            .unwrap_or_else(|_| "2".to_string());
        // Validate OrdType — only 1 (Market) and 2 (Limit) are supported
        if ord_type != "1" && ord_type != "2" {
            return Err(incorrect_data_format_for_value(TAG_ORD_TYPE));
        }

        // For limit orders, validate Price exists and is parseable
        if ord_type == "2" {
            let price_str = msg
                .body
                .get_string(TAG_PRICE)
                .map_err(|_| required_tag_missing(TAG_PRICE))?;
            // Validate format — parse as f64, reject unparseable
            price_str
                .parse::<f64>()
                .map_err(|_| incorrect_data_format_for_value(TAG_PRICE))?;
        }

        // --- business logic (sends 35=8 ExecutionReport Reject on failure) ---
        // For non-positive price validation we still go through business reject
        // since 0.00 is valid FIX format, just not a valid trading price.
        let price: f64 = if ord_type == "1" {
            0.0
        } else {
            let price_str = msg.body.get_string(TAG_PRICE).expect("price tag must exist for limit orders");
            match price_str.parse::<f64>() {
                Ok(p) if p > 0.0 => p,
                Ok(_) => {
                    self.send_reject(
                        msg,
                        session_id,
                        Some(&cl_ord_id),
                        Some(&symbol),
                        "non-positive Price (tag 44) for limit order",
                    );
                    return Ok(());
                }
                Err(_) => unreachable!(), // caught by format validation above
            }
        };

        self.process_new_order(
            msg, session_id, &cl_ord_id, &symbol, &side_str, order_qty, &ord_type, price,
        );
        Ok(())
    }
}
/// Fill body tags of a FIX 35=8 ExecutionReport message from a structured
/// [`ExecutionReport`]. The caller is responsible for setting the message
/// type and header routing (e.g. via [`Message::reverse_route`] or manual
/// SenderCompID / TargetCompID).
pub fn exec_report_to_fix_body(report: &crate::ExecutionReport, reply: &mut Message) {
    reply.header.set_string(TAG_MSG_TYPE, "8");

    if report.ex_ord_id == "NONE" {
        reply.body.set_string(TAG_ORDER_ID, "NONE");
    } else {
        reply
            .body
            .set_int(TAG_ORDER_ID, report.ex_ord_id.parse().expect("ex_ord_id always numeric"));
    }
    reply
        .body
        .set_int(TAG_EXEC_ID, report.exec_id.parse().expect("exec_id always numeric"));
    reply.body.set_string(TAG_EXEC_TRANS_TYPE, "0");
    let (exec_type, ord_status) = match report.method {
        "fill" => ("2", "2"),
        "partial" => ("1", "1"),
        "rejected" => ("8", "8"),
        "added" => ("0", "0"),
        _ => unreachable!("unexpected report.method: {}", report.method),
    };
    reply.body.set_string(TAG_EXEC_TYPE, exec_type);
    reply.body.set_string(TAG_ORD_STATUS, ord_status);
    reply.body.set_string(TAG_SYMBOL, &report.symbol);

    let side_fix = match report.side.as_str() {
        "buy" => "1",
        "sell" => "2",
        _ => unreachable!("unexpected report.side: {}", report.side),
    };
    reply.body.set_string(TAG_SIDE, side_fix);
    reply.body.set_int(TAG_ORDER_QTY, report.qty as isize);
    reply
        .body
        .set_int(TAG_LEAVES_QTY, report.leaves_qty as isize);
    reply.body.set_int(TAG_CUM_QTY, report.cum_qty as isize);

    if report.last_shares > 0 {
        reply
            .body
            .set_int(TAG_LAST_SHARES, report.last_shares as isize);
        let last_px = format!("{:.2}", report.last_px);
        reply.body.set_string(TAG_LAST_PX, &last_px);
    }

    let avg_px = format!("{:.2}", report.avg_px);
    reply.body.set_string(TAG_AVG_PX, &avg_px);
    reply.body.set_string(TAG_CL_ORD_ID, &report.cl_ord_id);

    // FIX uses YYYYMMDD-HH:MM:SS format regardless of the building_time
    reply
        .body
        .set_string(TAG_TRANSACT_TIME, &crate::tag60_now());
}
pub async fn run(state: Arc<crate::AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = "\
[DEFAULT]
SocketAcceptPort=9090
HeartBtInt=30
CheckLatency=N
DynamicSessions=Y

[SESSION]
BeginString=FIX.4.2
SenderCompID=XCANG3
ResetOnLogon=Y
";
    let settings = Settings::parse(BufReader::new(cfg.as_bytes())).await?;

    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<PendingReply>();

    let app: Arc<dyn Application> = Arc::new(FixApp { reply_tx, state });

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
    Ok(())
}
