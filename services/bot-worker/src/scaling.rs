use redis::{cmd, AsyncCommands};

/// Read the per-bot target rate from Redis.
/// Key: `bot:{bot_id}:target_rps`
pub async fn read_target_rps(
    con: &mut redis::aio::MultiplexedConnection,
    bot_id: &str,
) -> Result<u64, anyhow::Error> {
    let key = format!("bot:{}:target_rps", bot_id);
    let val: Option<String> = cmd("GET").arg(&key).query_async(con).await?;
    match val {
        Some(s) => Ok(s.parse().unwrap_or(100)),
        None => Ok(100),
    }
}
