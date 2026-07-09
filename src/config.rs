//! Runtime configuration, loaded from environment variables.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    /// Inbound proxy listen address.
    pub bind: String,
    /// Metrics/health listen address (separate port).
    pub metrics_bind: String,
    /// Timeout for the outbound Discord request (excludes ratelimit wait).
    pub outbound_timeout: Duration,
    /// Force HTTP/1.1 on the outbound client.
    pub disable_http2: bool,
    /// `tracing` env filter string.
    pub log_level: String,
    /// Default per-bot global req/s.
    pub global_ratelimit: u32,
    /// Per-bot global req/s overrides, keyed by bot id (from the token).
    pub bot_ratelimit_overrides: HashMap<String, u32>,
    /// Retire idle bucket workers after this duration.
    pub bucket_idle_ttl: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            bind: env_or("LZ_BIND", "0.0.0.0:8080"),
            metrics_bind: env_or("LZ_METRICS_BIND", "0.0.0.0:9000"),
            outbound_timeout: Duration::from_millis(env_parse("LZ_OUTBOUND_TIMEOUT_MS", 5000)?),
            disable_http2: env_bool("LZ_DISABLE_HTTP2", true)?,
            log_level: env_or("LZ_LOG_LEVEL", "info"),
            global_ratelimit: env_parse("LZ_GLOBAL_RATELIMIT", 50)?,
            bot_ratelimit_overrides: parse_overrides(&env_or("LZ_BOT_RATELIMIT_OVERRIDES", ""))?,
            bucket_idle_ttl: Duration::from_secs(env_parse("LZ_BUCKET_IDLE_TTL_S", 300)?),
        })
    }

    /// Global req/s for a given bot id, honouring overrides.
    pub fn global_limit_for(&self, bot_id: &str) -> u32 {
        self.bot_ratelimit_overrides
            .get(bot_id)
            .copied()
            .unwrap_or(self.global_ratelimit)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("invalid {key}={v}: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> Result<bool> {
    match std::env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => anyhow::bail!("invalid {key}={other}: expected a boolean"),
        },
        Err(_) => Ok(default),
    }
}

/// Parse `botid:limit,botid:limit` (no spaces). Fail fast on malformed input.
fn parse_overrides(raw: &str) -> Result<HashMap<String, u32>> {
    let mut map = HashMap::new();
    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, limit) = entry.split_once(':').with_context(|| {
            format!("malformed LZ_BOT_RATELIMIT_OVERRIDES entry {entry:?}: expected botid:limit")
        })?;
        let limit: u32 = limit
            .parse()
            .with_context(|| format!("malformed limit in override entry {entry:?}"))?;
        map.insert(id.to_string(), limit);
    }
    Ok(map)
}
