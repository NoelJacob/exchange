use fred::prelude::*;

/// Connect to Redis.
pub async fn connect(redis_url: &str) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let cfg = if let Some(stripped) = redis_url.strip_prefix("redis://") {
        let parts: Vec<&str> = stripped.split(':').collect();
        let host = parts.first().map(|s| s.to_string()).filter(|s| !s.is_empty())
            .ok_or_else::<String, _>(|| {
                tracing::error!("[REDIS] Invalid Redis URL '{}' — no host parsed from 'redis://...'", redis_url);
                format!("Invalid Redis URL: {redis_url}")
            }).map_err(|e| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?;
        let port: u16 = parts.get(1).and_then(|s| s.parse().ok())
            .unwrap_or(6379);
        let mut c = fred::types::config::Config::default();
        c.server = fred::types::config::ServerConfig::Centralized {
            server: fred::types::config::Server { host: host.into(), port },
        };
        c
    } else {
        fred::types::config::Config::from_url(redis_url)?
    };

    let client = Builder::default().set_config(cfg).build()?;
    client.connect();
    client.wait_for_connect().await?;
    Ok(client)
}

/// Key for an upload token.
pub fn token_key(token: &str) -> String {
    format!("token:{token}")
}

/// Key for a bot's RPS config.
pub fn bot_rps_key(contestant_id: &str, bot_idx: u32) -> String {
    format!("bot:{contestant_id}:{bot_idx:02}:rps")
}
