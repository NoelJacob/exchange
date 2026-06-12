use fred::prelude::*;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use chrono::Utc;

use crate::config::Config as AppConfig;
use crate::models::ExecutionEvent;

pub struct Storage {
    pub pg: PgPool,
    pub valkey: Client,
}

#[derive(Debug)]
pub struct CorrectnessRow {
    pub contestant_id: String,
    pub cl_ord_id: String,
    pub exec_id: String,
    pub verdict: String,
    pub penalty: f64,
    pub expected_px: f64,
    pub actual_px: f64,
    pub expected_qty: u64,
    pub actual_qty: u64,
}

fn fred_config(addr: &str) -> fred::types::config::Config {
    let mut cfg = fred::types::config::Config::default();
    let parts: Vec<&str> = addr.split(':').collect();
    let host = parts.first().map(|s| s.to_string()).filter(|s| !s.is_empty()).unwrap_or_else(|| {
        eprintln!("[STORAGE] Empty valkey host in addr '{addr}', defaulting to 127.0.0.1");
        "127.0.0.1".into()
    });
    let port: u16 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or_else(|| {
        eprintln!("[STORAGE] Invalid valkey port in addr '{addr}', defaulting to 6379");
        6379
    });
    cfg.server = fred::types::config::ServerConfig::Centralized {
        server: fred::types::config::Server { host: host.into(), port },
    };
    cfg
}

impl Storage {
    pub async fn connect(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let pg = PgPoolOptions::new()
            .max_connections(10)
            .connect(&format!("postgresql://admin:quest@{}", config.questdb_pgwire))
            .await?;

        let vcfg = fred_config(&config.valkey_addr);
        let valkey = Builder::default().set_config(vcfg).build()?;
        valkey.connect();
        valkey.wait_for_connect().await?;

        Ok(Self { pg, valkey })
    }

    pub async fn ensure_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sqls: [&str; 4] = [
            "CREATE TABLE IF NOT EXISTS order_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, side SYMBOL, qty LONG, price DOUBLE, is_market BOOLEAN, protocol SYMBOL) TIMESTAMP(ts) PARTITION BY DAY",
            "CREATE TABLE IF NOT EXISTS exec_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, exec_id SYMBOL, exec_seq LONG, exec_type SYMBOL, side SYMBOL, qty LONG, price DOUBLE, is_market BOOLEAN, last_shares LONG, last_px DOUBLE, leaves_qty LONG, cum_qty LONG, latency_us LONG) TIMESTAMP(ts) PARTITION BY DAY",
            "CREATE TABLE IF NOT EXISTS correctness_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, exec_id SYMBOL, verdict SYMBOL, penalty DOUBLE, expected_px DOUBLE, actual_px DOUBLE, expected_qty LONG, actual_qty LONG) TIMESTAMP(ts) PARTITION BY DAY",
            "CREATE TABLE IF NOT EXISTS contest_summary (ts TIMESTAMP, contestant_id SYMBOL, status SYMBOL, orders_sent LONG, execs_received LONG, correct_fills LONG, total_fills LONG, total_penalty DOUBLE, correctness_pct DOUBLE)",
        ];
        for sql in &sqls {
            sqlx::raw_sql(*sql).execute(&self.pg).await?;
        }
        Ok(())
    }

    pub async fn insert_correctness(&self, row: &CorrectnessRow) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = Utc::now().naive_utc();
        sqlx::query("INSERT INTO correctness_events (ts,contestant_id,cl_ord_id,exec_id,verdict,penalty,expected_px,actual_px,expected_qty,actual_qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(ts).bind(&row.contestant_id).bind(&row.cl_ord_id).bind(&row.exec_id)
            .bind(&row.verdict).bind(row.penalty)
            .bind(row.expected_px).bind(row.actual_px)
            .bind(row.expected_qty as i64).bind(row.actual_qty as i64)
            .execute(&self.pg).await?;
        Ok(())
    }

    pub async fn insert_ghost(&self, exec: &ExecutionEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = Utc::now().naive_utc();
        sqlx::query("INSERT INTO correctness_events (ts,contestant_id,cl_ord_id,exec_id,verdict,penalty,expected_px,actual_px,expected_qty,actual_qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(ts).bind(&exec.contestant_id).bind(&exec.cl_ord_id).bind(&exec.exec_id)
            .bind("ghost").bind(-0.5f64).bind(0.0f64).bind(exec.last_px.unwrap_or(0.0))
            .bind(0i64).bind(exec.last_shares.unwrap_or(0) as i64)
            .execute(&self.pg).await?;
        Ok(())
    }

    pub async fn insert_lost(&self, cid: &str, cl: &str, px: f64, qty: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = Utc::now().naive_utc();
        sqlx::query("INSERT INTO correctness_events (ts,contestant_id,cl_ord_id,exec_id,verdict,penalty,expected_px,actual_px,expected_qty,actual_qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(ts).bind(cid).bind(cl).bind("").bind("lost").bind(-0.5f64).bind(px).bind(0.0f64)
            .bind(qty as i64).bind(0i64).execute(&self.pg).await?;
        Ok(())
    }

    pub async fn upsert_summary(&self, cid: &str, status: &str, sent: u64, recv: u64, correct: u64, total: u64, penalty: f64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = Utc::now().naive_utc();
        let pct = if total > 0 { correct as f64 / total as f64 * 100.0 } else { 0.0 };
        sqlx::query("INSERT INTO contest_summary (ts,contestant_id,status,orders_sent,execs_received,correct_fills,total_fills,total_penalty,correctness_pct) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(ts).bind(cid).bind(status).bind(sent as i64).bind(recv as i64)
            .bind(correct as i64).bind(total as i64).bind(penalty).bind(pct)
            .execute(&self.pg).await?;
        Ok(())
    }

    pub async fn publish_leaderboard(&self, json: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _: () = self.valkey.set("leaderboard:latest", json, None, None, false).await?;
        let _: () = self.valkey.publish("leaderboard:updates", json).await?;
        Ok(())
    }
}


#[cfg(test)]
mod parse_tests {
    use crate::models::*;

    /// Parse actual execution JSON from bot-worker output
    #[test]
    fn parse_actual_execution_json() {
        let json = r#"{"stream":"execution","ts":1749697545123456,"contestant_id":"manual","bot_id":"MAN","cl_ord_id":"MAN-1","exec_type":"2","latency_us":500,"last_shares":50,"last_px":100.5}"#;
        let e: ExecutionEvent = serde_json::from_str(json).expect("parse");
        assert_eq!(e.contestant_id, "manual");
        assert_eq!(e.exec_type, "2");
        assert_eq!(e.last_shares, Some(50));
        eprintln!("[Test] Parsed execution: {e:?}");
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[tokio::test]
    #[ignore]
    async fn questdb_write_and_read() {
        let cfg = Config {
            questdb_pgwire: "127.0.0.1:8812".into(),
            valkey_addr: "127.0.0.1:6379".into(),
            redpanda_brokers: "".into(),
            contestant_id: "test".into(),
            drain_timeout_secs: 10,
            poll_interval_secs: 2,
        };
        let storage = Storage::connect(&cfg).await.expect("connect");
        storage.ensure_schema().await.expect("schema");

        let ts = chrono::Utc::now().naive_utc();
        let res = sqlx::query(
            "INSERT INTO order_events (ts,contestant_id,cl_ord_id,side,qty,price,is_market,protocol) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
        )
        .bind(ts).bind("test").bind("t1").bind("buy").bind(100i64).bind(100.50).bind(false).bind("fix")
        .execute(&storage.pg).await;
        eprintln!("[Test] Insert result: {:?}", res);
        assert!(res.is_ok(), "insert failed: {:?}", res.err());

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM order_events")
            .fetch_one(&storage.pg).await.expect("query");
        eprintln!("[Test] Count: {}", count.0);
        assert!(count.0 > 0, "expected > 0 after insert");
    }
}
