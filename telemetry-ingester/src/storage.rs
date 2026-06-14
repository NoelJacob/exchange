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

fn fred_config(addr: &str) -> Result<fred::types::config::Config, String> {
    let mut cfg = fred::types::config::Config::default();
    let addr = addr.strip_prefix("redis://").unwrap_or(addr);
    let parts: Vec<&str> = addr.split(':').collect();
    let host: String = parts.first()
        .ok_or_else(|| format!("Empty valkey host in addr '{addr}'"))?
        .to_string();
    if host.is_empty() {
        return Err(format!("Empty valkey host in addr '{addr}'"));
    }
    let port: u16 = parts.get(1).and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("Invalid or missing valkey port in addr '{addr}'"))?;
    tracing::info!("[STORAGE] Valkey config: host={host} port={port}");
    cfg.server = fred::types::config::ServerConfig::new_centralized(host, port);
    Ok(cfg)
}

impl Storage {
    pub async fn connect(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let pg = PgPoolOptions::new()
            .max_connections(10)
            .connect(&format!("postgresql://admin:quest@{}", config.questdb_pgwire))
            .await?;

        let vcfg = fred_config(&config.valkey_addr)?;
        let valkey = Builder::default().set_config(vcfg).build()?;
        valkey.connect();
        valkey.wait_for_connect().await?;

        Ok(Self { pg, valkey })
    }

    pub async fn ensure_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sqls: [&str; 5] = [
            "CREATE TABLE IF NOT EXISTS order_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, side SYMBOL, qty LONG, price DOUBLE, is_market BOOLEAN, protocol SYMBOL) TIMESTAMP(ts) PARTITION BY DAY",
            "CREATE TABLE IF NOT EXISTS exec_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, exec_id SYMBOL, exec_seq LONG, exec_type SYMBOL, side SYMBOL, qty LONG, price DOUBLE, is_market BOOLEAN, last_shares LONG, last_px DOUBLE, leaves_qty LONG, cum_qty LONG, latency_us LONG) TIMESTAMP(ts) PARTITION BY DAY",
            "CREATE TABLE IF NOT EXISTS correctness_events (ts TIMESTAMP, contestant_id SYMBOL, cl_ord_id SYMBOL, exec_id SYMBOL, verdict SYMBOL, penalty DOUBLE, expected_px DOUBLE, actual_px DOUBLE, expected_qty LONG, actual_qty LONG) TIMESTAMP(ts) PARTITION BY DAY",
            "CREATE TABLE IF NOT EXISTS contest_summary (ts TIMESTAMP, contestant_id SYMBOL, status SYMBOL, orders_sent LONG, execs_received LONG, correct_fills LONG, total_fills LONG, total_penalty DOUBLE, correctness_pct DOUBLE)",
            "CREATE TABLE IF NOT EXISTS metric_events (ts TIMESTAMP, contestant_id SYMBOL, bot_id SYMBOL, orders_sent LONG, fills LONG, partials LONG, rejects LONG, errors LONG, orders_fix LONG, orders_ws LONG, p50 DOUBLE, p90 DOUBLE, p99 DOUBLE, avg_latency_us DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
        ];
        for sql in &sqls {
            sqlx::raw_sql(*sql).execute(&self.pg).await?;
        }
        // Add new columns to contest_summary (if they don't exist)
        let alter_sqls = [
            "ALTER TABLE contest_summary ADD COLUMN composite DOUBLE",
            "ALTER TABLE contest_summary ADD COLUMN current_tps DOUBLE",
            "ALTER TABLE contest_summary ADD COLUMN peak_tps DOUBLE",
            "ALTER TABLE contest_summary ADD COLUMN p99_latency_us DOUBLE",
            "ALTER TABLE contest_summary ADD COLUMN failure_reason SYMBOL",
        ];
        for sql in &alter_sqls {
            if let Err(e) = sqlx::raw_sql(*sql).execute(&self.pg).await {
                let msg = e.to_string().to_lowercase();
                if msg.contains("already exist") || msg.contains("duplicate column") {
                    eprintln!("[STORAGE] Column already exists (ignored): {sql}");
                } else {
                    eprintln!("[STORAGE] Alter warning (non-fatal): {e}");
                }
            }
        }
        Ok(())
    }

    pub async fn insert_correctness(&self, row: &CorrectnessRow) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = Utc::now().naive_utc();
        eprintln!("[STORAGE-INSERT] correctness {} {} {} px_exp={:.2} px_act={:.2} qty_exp={} qty_act={}",
            row.contestant_id, row.cl_ord_id, row.verdict,
            row.expected_px, row.actual_px, row.expected_qty, row.actual_qty);
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
        eprintln!("[STORAGE-INSERT] ghost {} {} seq={} last_shares={} last_px={:.4}",
            exec.contestant_id, exec.cl_ord_id, exec.exec_seq,
            exec.last_shares.unwrap_or(0), exec.last_px.unwrap_or(0.0));
        sqlx::query("INSERT INTO correctness_events (ts,contestant_id,cl_ord_id,exec_id,verdict,penalty,expected_px,actual_px,expected_qty,actual_qty) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(ts).bind(&exec.contestant_id).bind(&exec.cl_ord_id).bind(&exec.exec_id)
            .bind("ghost").bind(-0.5f64).bind(0.0f64).bind(exec.last_px.unwrap_or(0.0))
            .bind(0i64).bind(exec.last_shares.unwrap_or(0) as i64)
            .execute(&self.pg).await?;
        Ok(())
    }

    pub async fn insert_metric(&self, ev: &crate::models::MetricEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = chrono::DateTime::from_timestamp(
            (ev.ts_us / 1_000_000) as i64,
            ((ev.ts_us % 1_000_000) as u32) * 1_000,
        ).map(|dt| dt.naive_utc())
        .unwrap_or_else(|| chrono::Utc::now().naive_utc());
        eprintln!("[STORAGE-INSERT] metric {} sent={} fills={} p99={}",
            ev.contestant_id, ev.orders_sent, ev.fills, ev.p99);
        sqlx::query("INSERT INTO metric_events (ts,contestant_id,bot_id,orders_sent,fills,partials,rejects,errors,orders_fix,orders_ws,p50,p90,p99,avg_latency_us) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(ts).bind(&ev.contestant_id).bind(&ev.bot_id)
            .bind(ev.orders_sent as i64).bind(ev.fills as i64)
            .bind(ev.partials as i64).bind(ev.rejects as i64)
            .bind(ev.errors as i64).bind(ev.orders_fix as i64)
            .bind(ev.orders_ws as i64)
            .bind(ev.p50 as f64).bind(ev.p90 as f64).bind(ev.p99 as f64)
            .bind(ev.avg_latency_us)
            .execute(&self.pg).await?;
        Ok(())
    }

    pub async fn upsert_summary(&self, cid: &str, status: &str, sent: u64, recv: u64, correct: u64, total: u64, penalty: f64, composite: f64, current_tps: f64, peak_tps: f64, p99_latency_us: f64, failure_reason: Option<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ts = Utc::now().naive_utc();
        let pct = if total > 0 { correct as f64 / total as f64 * 100.0 } else { 0.0 };
        eprintln!("[STORAGE-UPSERT] {} status={} sent={} recv={} correct={} total={} pct={:.1} composite={:.4} tps={:.1} peak_tps={:.1} p99={:.0} fail={:?}",
            cid, status, sent, recv, correct, total, pct, composite, current_tps, peak_tps, p99_latency_us, failure_reason);
        sqlx::query("INSERT INTO contest_summary (ts,contestant_id,status,orders_sent,execs_received,correct_fills,total_fills,total_penalty,correctness_pct,composite,current_tps,peak_tps,p99_latency_us,failure_reason) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(ts).bind(cid).bind(status).bind(sent as i64).bind(recv as i64)
            .bind(correct as i64).bind(total as i64).bind(penalty).bind(pct)
            .bind(composite).bind(current_tps).bind(peak_tps).bind(p99_latency_us)
            .bind(failure_reason)
            .execute(&self.pg).await?;
        Ok(())
    }

    pub async fn publish_leaderboard(&self, json: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        eprintln!("[STORAGE-LEADERBOARD] publishing: {}", json);
        let _: () = self.valkey.set("leaderboard:latest", json, None, None, false).await?;
        let _: () = self.valkey.publish("leaderboard:updates", json).await?;
        Ok(())
    }

    pub async fn read_weights(&self) -> Result<(f64, f64, f64), Box<dyn std::error::Error + Send + Sync>> {
        use fred::interfaces::HashesInterface;
        let cw: String = self.valkey.hget("config:weights", "correctness_weight").await
            .unwrap_or_else(|_| "0.40".to_string());
        let tw: String = self.valkey.hget("config:weights", "tps_weight").await
            .unwrap_or_else(|_| "0.35".to_string());
        let pw: String = self.valkey.hget("config:weights", "p99_weight").await
            .unwrap_or_else(|_| "0.25".to_string());
        let cw_f = cw.parse::<f64>().unwrap_or(0.40);
        let tw_f = tw.parse::<f64>().unwrap_or(0.35);
        let pw_f = pw.parse::<f64>().unwrap_or(0.25);
        eprintln!("[STORAGE-WEIGHTS] correctness={:.2} tps={:.2} p99={:.2}", cw_f, tw_f, pw_f);
        Ok((cw_f, tw_f, pw_f))
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
    async fn questdb_write_and_read() {
        let cfg = Config {
            questdb_pgwire: "127.0.0.1:8812".into(),
            valkey_addr: "127.0.0.1:6379".into(),
            redpanda_brokers: "".into(),
            contestant_id: "test".into(),
            drain_timeout_secs: 10,
            poll_interval_secs: 2,
            gap_timeout_secs: 10,
        };
        let storage = Storage::connect(&cfg).await.expect("connect");
        // Drop and recreate table for clean test
        sqlx::query("DROP TABLE IF EXISTS order_events")
            .execute(&storage.pg).await.expect("drop");
        storage.ensure_schema().await.expect("schema");

        let ts = chrono::Utc::now().naive_utc();
        let res = sqlx::query(
            "INSERT INTO order_events (ts,contestant_id,cl_ord_id,side,qty,price,is_market,protocol) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
        )
        .bind(ts).bind("test").bind("t1").bind("buy").bind(100i64).bind(100.50).bind(false).bind("fix")
        .execute(&storage.pg).await;
        eprintln!("[Test] Insert result: {:?}", res);
        assert!(res.is_ok(), "insert failed: {:?}", res.err());
        // QuestDB PG wire may need a brief moment to flush WAL before read
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM order_events")
            .fetch_one(&storage.pg).await.expect("query");
        eprintln!("[Test] Count: {}", count.0);
        assert!(count.0 > 0, "expected > 0 after insert");
    }
}

#[cfg(test)]
mod fred_config_tests {
    use super::fred_config;

    #[test]
    fn rejects_empty_host() {
        let result = fred_config("redis://:6379");
        assert!(result.is_err(), "empty host should produce Err");
        let msg = result.unwrap_err();
        assert!(msg.contains("Empty"), "error should mention 'Empty': {msg}");
    }

    #[test]
    fn accepts_valid_addr() {
        let result = fred_config("redis://127.0.0.1:6379");
        assert!(result.is_ok(), "valid addr should produce Ok");
    }

    #[test]
    fn rejects_bad_port() {
        let result = fred_config("redis://host:notaport");
        assert!(result.is_err(), "non-numeric port should produce Err");
    }
}
