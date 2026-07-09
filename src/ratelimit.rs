//! Route normalization, token scoping, and the per-bot global limiter.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

/// Major parameters get their own bucket; every other snowflake collapses.
/// The parent segment name tells us whether the following id is major.
const MAJOR_PARENTS: [&str; 3] = ["channels", "guilds", "webhooks"];

/// Collapse a request path into a stable synthetic bucket key.
///
/// Major parameters (`channel_id`, `guild_id`, `webhook_id`/`webhook_token`)
/// are preserved; all other ids are replaced with `{id}`. The method is part
/// of the key because Discord buckets are method-specific. Query string is
/// dropped. This is only a first guess — the real `x-ratelimit-bucket` from
/// the response is what ultimately converges distinct routes.
pub fn normalize_route(method: &str, path: &str) -> String {
    let path = path.split('?').next().unwrap_or(path);

    let mut out: Vec<String> = Vec::new();
    let mut prev = "";
    let mut segments = path.split('/').filter(|s| !s.is_empty()).peekable();

    while let Some(seg) = segments.next() {
        if is_snowflake(seg) {
            if MAJOR_PARENTS.contains(&prev) {
                // Major parameter: keep the real value so it gets its own bucket.
                out.push(seg.to_string());
                // A webhook id is followed by a webhook token — also major.
                if prev == "webhooks" {
                    if let Some(tok) = segments.peek() {
                        out.push((*tok).to_string());
                        prev = segments.next().unwrap();
                        continue;
                    }
                }
            } else {
                out.push("{id}".to_string());
            }
        } else if prev == "reactions" {
            // Emoji segment — collapse so all reactions on a message share a bucket.
            out.push("{emoji}".to_string());
        } else {
            out.push(seg.to_string());
        }
        prev = seg;
    }

    format!("{} /{}", method, out.join("/"))
}

/// Whether a path segment looks like a snowflake / opaque id we should collapse.
fn is_snowflake(seg: &str) -> bool {
    !seg.is_empty() && seg.chars().all(|c| c.is_ascii_digit())
}

/// Derive the per-bot scope key from the `Authorization` header.
///
/// The token is never stored or logged; only its SHA-256 hex digest is used.
/// The `bot_id` (public, embedded in the token) is extracted for override
/// lookup and metrics — it is not secret on its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BotScope {
    /// Full hex digest of the auth header — used as the map key.
    pub hash: String,
    /// Public bot id decoded from the token, or empty if undecodable.
    pub bot_id: String,
}

impl BotScope {
    pub fn from_auth(auth: Option<&str>) -> Option<Self> {
        let auth = auth?;
        if auth.is_empty() {
            return None;
        }
        let mut hasher = Sha256::new();
        hasher.update(auth.as_bytes());
        let hash = hex(&hasher.finalize());
        Some(Self {
            bot_id: decode_bot_id(auth),
            hash,
        })
    }

    /// Short prefix safe to log.
    pub fn log_prefix(&self) -> &str {
        &self.hash[..self.hash.len().min(8)]
    }
}

/// A Discord bot token's first dot-segment is base64(bot_id). Best-effort decode.
fn decode_bot_id(auth: &str) -> String {
    let token = auth.strip_prefix("Bot ").unwrap_or(auth);
    let first = token.split('.').next().unwrap_or("");
    match base64_decode_urlsafe(first) {
        Some(bytes) => String::from_utf8(bytes).unwrap_or_default(),
        None => String::new(),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Minimal urlsafe base64 decoder (no padding), enough for the bot-id segment.
fn base64_decode_urlsafe(input: &str) -> Option<Vec<u8>> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut lut = [255u8; 256];
    for (i, &c) in TABLE.iter().enumerate() {
        lut[c as usize] = i as u8;
    }
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0u32;
    for &c in input.as_bytes() {
        if c == b'=' {
            break;
        }
        let v = lut[c as usize];
        if v == 255 {
            return None;
        }
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

/// Fixed 1-second-window token bucket, one per bot scope, plus a global-pause
/// gate for `global`-scoped 429s.
pub struct GlobalLimiter {
    limit: u32,
    inner: Mutex<GlobalInner>,
}

struct GlobalInner {
    window_start: Instant,
    used: u32,
    /// If set and in the future, all acquisitions block until this instant
    /// (set when Discord returns a `global`-scoped 429).
    pause_until: Option<Instant>,
}

impl GlobalLimiter {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            inner: Mutex::new(GlobalInner {
                window_start: Instant::now(),
                used: 0,
                pause_until: None,
            }),
        }
    }

    /// Block until a global token is available for this window.
    /// Returns `true` if it had to wait.
    pub async fn acquire(&self) -> bool {
        let mut waited = false;
        loop {
            let sleep_for = {
                let mut g = self.inner.lock().unwrap();
                let now = Instant::now();

                if let Some(until) = g.pause_until {
                    if now < until {
                        Some(until - now)
                    } else {
                        g.pause_until = None;
                        None
                    }
                } else {
                    None
                }
                .or_else(|| {
                    if now.duration_since(g.window_start) >= Duration::from_secs(1) {
                        g.window_start = now;
                        g.used = 0;
                    }
                    if g.used < self.limit {
                        g.used += 1;
                        None
                    } else {
                        Some(g.window_start + Duration::from_secs(1) - now)
                    }
                })
            };

            match sleep_for {
                Some(d) if !d.is_zero() => {
                    waited = true;
                    tokio::time::sleep(d).await;
                }
                _ => return waited,
            }
        }
    }

    /// Pause the whole scope for `retry_after` (a `global`-scoped 429).
    pub fn pause(&self, retry_after: Duration) {
        let mut g = self.inner.lock().unwrap();
        g.pause_until = Some(Instant::now() + retry_after);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_major_params_collapses_others() {
        assert_eq!(
            normalize_route("GET", "/api/v10/channels/123/messages/456"),
            "GET /api/v10/channels/123/messages/{id}"
        );
    }

    #[test]
    fn guild_id_is_major() {
        assert_eq!(
            normalize_route("GET", "/api/v10/guilds/999/members/111"),
            "GET /api/v10/guilds/999/members/{id}"
        );
    }

    #[test]
    fn webhook_id_and_token_both_major() {
        assert_eq!(
            normalize_route("POST", "/api/webhooks/555/abcdef"),
            "POST /api/webhooks/555/abcdef"
        );
    }

    #[test]
    fn query_string_dropped() {
        assert_eq!(
            normalize_route("GET", "/api/v10/guilds/1/members?limit=2"),
            "GET /api/v10/guilds/1/members"
        );
    }
}
