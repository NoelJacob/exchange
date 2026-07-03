use std::sync::Arc;

use fixer::application::Application;
use fixer::errors::{
    MessageRejectErrorResult, incorrect_data_format_for_value, required_tag_missing, unsupported_message_type, value_is_incorrect
};
use fixer::log::screen_log::ScreenLogFactory;
use fixer::message::Message;
use fixer::registry::send_to_target_owned;
use fixer::session::session_id::SessionID;
use fixer::settings::Settings;
use fixer::store::MemoryStoreFactory;
use simple_error::SimpleResult;
use tokio::io::BufReader;
use tokio::sync::mpsc;
use pricelevel::prelude::*;
use fixer_fix::tag;
use fixer_fix::enums;

use crate::ExecutionReportMethod;

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
    /// Process a validated NewOrderSingle and send the sync ExecutionReport.
    #[allow(clippy::too_many_arguments)]
    fn process_new_order(
        &self,
        session_id: &Arc<SessionID>,
        cl_ord_id: &str,
        symbol: &str,
        side_val: &str,
        order_qty: u64,
        ord_type: &str,
        price: f64,
        sender_comp: &str
    ) {
        let is_market = ord_type == "1";

        let ob_side = match side_val {
            "1" => orderbook_rs::Side::Buy,
            "2" => orderbook_rs::Side::Sell,
            _ => unreachable!(),
        };

        eprintln!(
            "[FIX] NewOrderSingle cl_ord_id={cl_ord_id} symbol={symbol} side={side_val} price={price} qty={order_qty} sender={sender_comp}",
        );

        // Lock books, get/create book with TradeListener (scoped — released before submit)
        let book = {
            let mut books = self.state.books.lock();
            crate::get_or_create_book(&mut books, symbol, &self.state.trade_tx)
        };

        let ex_ord_id = self.state.order_id_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string();
        let info = crate::OrderInfo {
            target_id: sender_comp.to_string(),
            cl_ord_id: cl_ord_id.to_string(),
            order_qty,
            cum_value_cents: 0,
            cum_qty: 0,
            ex_ord_id,
            connection_kind: crate::ConnectionKind::Fix {
                session_id: Arc::clone(session_id),
                reply_tx: self.reply_tx.clone(),
            },
            price
        };

        let (outcome, exec_id) = {
            let mut pending = self.state.pending.lock();
            // Reserve exec_seq atomically. Will be reclaimed via CAS if send fails.
            let exec_id = self.state.exec_id_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                .to_string();
            eprintln!("[EXEC-ID-GEN] FIX sync: cl_ord_id={} exec_id={} (before submit)", cl_ord_id, exec_id);
            let outcome = crate::submit(
                &book,
                is_market,
                ob_side,
                &mut pending,
                &info
            );

            (outcome, exec_id)
        };


        // Build ExecutionReport from outcome and send FIX 35=8
        let msg_body = match outcome {
            Ok(o) => {
                let report = crate::build_execution_report(
                    o,
                    &info,
                    symbol,
                    ob_side,
                    &exec_id,
                    is_market,
                );
                report_to_fix(&report)
            }
            Err(e) => {
                eprintln!("[FIX] Order {} rejected: {e}", cl_ord_id);
                let report = crate::build_reject_report(
                    e.to_string(),
                    &info,
                    symbol,
                    ob_side,
                    &exec_id,
                    is_market,
                );
                report_to_fix(&report)
            }
        };
        if self.reply_tx.send(PendingReply {
            msg: msg_body,
            session_id: Arc::clone(session_id),
        }).is_err() {
            eprintln!("[FIX] Dropped response for {} — connection closed (seq={} lost)", cl_ord_id, exec_id);
        }
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
        if !msg.is_msg_type_of(enums::msg_type::ORDER_SINGLE) {
            return Err(unsupported_message_type());
        }

        // --- session-level validation: return Err → library sends 35=3 ---
        let comp_id = msg
            .header
            .get_string(tag::SENDER_COMP_ID)
            .map_err(|_| required_tag_missing(tag::SENDER_COMP_ID))?;

        let cl_ord_id = msg
            .body
            .get_string(tag::CL_ORD_ID)
            .map_err(|_| required_tag_missing(tag::CL_ORD_ID))?;

        let symbol_raw = msg
            .body
            .get_string(tag::SYMBOL)
            .map_err(|_| required_tag_missing(tag::SYMBOL))?;

        let symbol = symbol_raw.trim();
        if symbol.is_empty() {
            return Err(value_is_incorrect(tag::SYMBOL));
        }

        let side_str = msg
            .body
            .get_string(tag::SIDE)
            .map_err(|_| required_tag_missing(tag::SIDE))?;

        // Side value must be 1 (Buy) or 2 (Sell)
        match side_str.as_str() {
            "1" | "2" => {}
            _ => return Err(value_is_incorrect(tag::SIDE)),
        }

        let order_qty_raw = msg
            .body
            .get_int(tag::ORDER_QTY)
            .map_err(|_| required_tag_missing(tag::ORDER_QTY))?;

        let order_qty = order_qty_raw as u64;
        if order_qty == 0 {
            return Err(required_tag_missing(tag::ORDER_QTY));
        }
        if order_qty_raw < 0 {
            return Err(value_is_incorrect(tag::ORDER_QTY));
        }

        let ord_type = msg
            .body
            .get_string(tag::ORD_TYPE)
            .map_err(|_| required_tag_missing(tag::ORD_TYPE))?;

        // For non-positive price validation we still go through business reject
        // since 0.00 is valid FIX format, just not a valid trading price.
        let price: f64 = if ord_type == "1" {
            0.0
        } else if ord_type == "2" {
            let price_str = msg
            .body
            .get_string(tag::PRICE)
            .map_err(|_| required_tag_missing(tag::PRICE))?;

            match price_str.parse::<f64>() {
                Ok(p) if p > 0.0 => p,
                _ => return Err(value_is_incorrect(tag::PRICE)), // caught by format validation above
            }
        } else {
            // Validate OrdType — only 1 (Market) and 2 (Limit) are supported
            return Err(incorrect_data_format_for_value(tag::ORD_TYPE));
        };

        self.process_new_order(
            session_id,
            &cl_ord_id,
            symbol,
            &side_str,
            order_qty,
            &ord_type,
            price,
            &comp_id
        );
        Ok(())
    }
}
/// Fill body tags of a FIX 35=8 ExecutionReport message from a structured
/// [`ExecutionReport`].
pub fn report_to_fix(report: &crate::ExecutionReport) -> Message {
    let mut msg = Message::new();
    msg.header.set_string(tag::MSG_TYPE, enums::msg_type::EXECUTION_REPORT);
    msg.body.set_string(tag::CL_ORD_ID, &report.cl_ord_id);
    msg.body.set_string(tag::EXEC_ID, &report.exec_id);
    msg.body.set_int(tag::EXEC_TRANS_TYPE, 0);
    msg.body.set_string(tag::SYMBOL, &report.symbol);
    msg.body.set_int(tag::ORDER_QTY, report.qty as isize);
    msg.body.set_int(tag::LEAVES_QTY, report.leaves_qty as isize);
    msg.body.set_int(tag::CUM_QTY, report.cum_qty as isize);
    msg.body.set_string(tag::AVG_PX, &format!("{:.2}", report.avg_px));
    msg.body.set_string(tag::TRANSACT_TIME, &report.transact_time.format("%Y%m%d-%H:%M:%S.%.6f").to_string());
    if report.is_market {
        msg.body.set_int(tag::ORD_TYPE, 1);
    } else {
        msg.body.set_int(tag::ORD_TYPE, 2);
        msg.body.set_string(tag::PRICE, &format!("{:.2}", report.price));
    }
    match report.side {
        Side::Buy => {
            msg.body.set_string(tag::SIDE, enums::side::BUY);
        }
        Side::Sell => {
            msg.body.set_string(tag::SIDE, enums::side::SELL);
        }
    };
    match report.method {
        ExecutionReportMethod::New => {
            msg.body.set_string(tag::EXEC_TYPE, enums::exec_type::NEW);
            msg.body.set_string(tag::ORD_STATUS, enums::ord_status::NEW);

            let ex_ord_id = report.ex_ord_id.as_ref().expect("[FIX] Missing ex_ord_id");
            msg.body.set_string(tag::ORDER_ID, ex_ord_id);
        }
        ExecutionReportMethod::Partial => {
            msg.body.set_string(tag::EXEC_TYPE, enums::exec_type::PARTIAL_FILL);
            msg.body.set_string(tag::ORD_STATUS, enums::ord_status::PARTIALLY_FILLED);
            msg.body.set_int(tag::LAST_CAPACITY, 1);
            msg.body.set_string(tag::LAST_MKT, "XCANG3");


            let ex_ord_id = report.ex_ord_id.as_ref().expect("[FIX] Missing ex_ord_id");
            let last_shares = report.last_shares.expect("[FIX] Missing last_shares");
            let last_px = report.last_px.expect("[FIX] Missing last_px");
            msg.body.set_string(tag::ORDER_ID, ex_ord_id);
            msg.body.set_int(tag::LAST_SHARES, last_shares as isize);
            msg.body.set_string(tag::LAST_PX, &format!("{:.2}", last_px));
        }
        ExecutionReportMethod::Fill => {
            msg.body.set_string(tag::EXEC_TYPE, enums::exec_type::FILL);
            msg.body.set_string(tag::ORD_STATUS, enums::ord_status::FILLED);
            msg.body.set_int(tag::LAST_CAPACITY, 1);
            msg.body.set_string(tag::LAST_MKT, "XCANG3");

            let ex_ord_id = report.ex_ord_id.as_ref().expect("[FIX] Missing ex_ord_id");
            let last_shares = report.last_shares.expect("[FIX] Missing last_shares");
            let last_px = report.last_px.expect("[FIX] Missing last_px");
            msg.body.set_string(tag::ORDER_ID, ex_ord_id);
            msg.body.set_int(tag::LAST_SHARES, last_shares as isize);
            msg.body.set_string(tag::LAST_PX, &format!("{:.2}", last_px));
        }
        ExecutionReportMethod::Rejected => {
            let reject_reason = report.reject_reason.as_ref().expect("[FIX] Missing reject_reason");
            msg.body.set_string(tag::EXEC_TYPE, enums::exec_type::REJECTED);
            msg.body.set_string(tag::ORD_STATUS, enums::ord_status::REJECTED);
            msg.body.set_string(tag::TEXT, reject_reason);

            let ex_ord_id = report.ex_ord_id.as_ref().expect("[FIX] Missing ex_ord_id");
            msg.body.set_string(tag::ORDER_ID, ex_ord_id);
        }
    };
    msg

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
            let exec_id = msg.body.get_string(tag::EXEC_ID).unwrap_or_else(|_| "?".into());
            let cl_ord_id = msg.body.get_string(tag::CL_ORD_ID).unwrap_or_else(|_| "?".into());
            match send_to_target_owned(msg, &session_id).await {
                Ok(()) => {
                    eprintln!("[FIX-DISPATCH-DELIVERED] exec_id={exec_id} cl_ord_id={cl_ord_id}");
                }
                Err(e) => {
                    eprintln!("[FIX-DISPATCH-LOST] exec_id={exec_id} cl_ord_id={cl_ord_id}: {e}");
                }
            }
        }
    });

    // Keep alive
    tokio::signal::ctrl_c().await.ok();
    acceptor.stop().await;
    Ok(())
}
