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
/// dropped. This is only a first guess - the real `x-ratelimit-bucket` from
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
                // A webhook id is followed by a webhook token - also major.
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
            // Emoji segment - collapse so all reactions on a message share a bucket.
            out.push("{emoji}".to_string());
        } else {
            out.push(seg.to_string());
        }
        prev = seg;
    }

    format!("{} /{}", method, out.join("/"))
}

/// Template a request path for metrics labels and log lines.
///
/// For reporting only, never for bucket keys (use `normalize_route` for
/// those). Every id becomes `{id}`, webhook and interaction tokens become
/// `{token}`, reaction emoji `{emoji}`, invite and template codes `{code}`.
/// Any other segment that looks opaque also becomes `{token}`, so no secret
/// survives and the label set stays bounded. Opaque means: contains both an
/// ASCII letter and an ASCII digit (except API version segments like `v10`
/// and the literal `oauth2`, which are static routes), is longer than 32
/// chars, or contains an uppercase letter. Query string is dropped.
pub fn metric_route(method: &str, path: &str) -> String {
    let path = path.split('?').next().unwrap_or(path);

    let mut out: Vec<&str> = Vec::new();
    let mut prev2 = "";
    let mut prev = "";

    for seg in path.split('/').filter(|s| !s.is_empty()) {
        let templated = if is_snowflake(seg) {
            "{id}"
        } else if prev == "reactions" {
            "{emoji}"
        } else if (prev2 == "webhooks" || prev2 == "interactions") && is_snowflake(prev) {
            "{token}"
        } else if prev == "invites" || prev == "templates" {
            "{code}"
        } else if (is_mixed_alnum(seg) && !is_version(seg) && seg != "oauth2")
            || seg.len() > 32
            || seg.chars().any(|c| c.is_ascii_uppercase())
        {
            "{token}"
        } else {
            seg
        };
        out.push(templated);
        prev2 = prev;
        prev = seg;
    }

    format!("{} /{}", method, out.join("/"))
}

/// Whether a segment contains both an ASCII letter and an ASCII digit.
fn is_mixed_alnum(seg: &str) -> bool {
    seg.chars().any(|c| c.is_ascii_alphabetic()) && seg.chars().any(|c| c.is_ascii_digit())
}

/// API version segment: `v` followed by only digits (`v10`, `v9`).
fn is_version(seg: &str) -> bool {
    seg.strip_prefix('v')
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()))
}

/// Whether a path segment looks like a snowflake / opaque id we should collapse.
fn is_snowflake(seg: &str) -> bool {
    !seg.is_empty() && seg.chars().all(|c| c.is_ascii_digit())
}

/// Derive the per-bot scope key from the `Authorization` header.
///
/// The token is never stored or logged; only its SHA-256 hex digest is used.
/// The `bot_id` (public, embedded in the token) is extracted for override
/// lookup and metrics - it is not secret on its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BotScope {
    /// Full hex digest of the auth header - used as the map key.
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

    #[test]
    fn metric_route_collapses_major_ids() {
        assert_eq!(
            metric_route("GET", "/api/v10/channels/123/messages/456"),
            "GET /api/v10/channels/{id}/messages/{id}"
        );
        assert_eq!(
            metric_route("GET", "/api/v10/guilds/999/members/111?limit=2"),
            "GET /api/v10/guilds/{id}/members/{id}"
        );
    }

    #[test]
    fn webhook_token_never_in_metric_route() {
        let r = metric_route("POST", "/api/webhooks/555/aBcD-ef_123/messages/@original");
        assert_eq!(r, "POST /api/webhooks/{id}/{token}/messages/@original");
    }

    #[test]
    fn interaction_token_never_in_metric_route() {
        let r = metric_route("POST", "/api/v10/interactions/777/aW50ZXJhY3Rpb24/callback");
        assert_eq!(r, "POST /api/v10/interactions/{id}/{token}/callback");
    }

    #[test]
    fn reaction_emoji_collapsed() {
        assert_eq!(
            metric_route("PUT", "/api/v10/channels/1/messages/2/reactions/%F0%9F%91%8D/@me"),
            "PUT /api/v10/channels/{id}/messages/{id}/reactions/{emoji}/@me"
        );
        assert_eq!(
            metric_route("DELETE", "/api/v10/channels/1/messages/2/reactions/name:3/4"),
            "DELETE /api/v10/channels/{id}/messages/{id}/reactions/{emoji}/{id}"
        );
    }

    #[test]
    fn short_lowercase_alnum_segment_is_token() {
        assert_eq!(
            metric_route("GET", "/api/v10/future/ab12cd"),
            "GET /api/v10/future/{token}"
        );
    }

    #[test]
    fn version_and_oauth2_segments_kept() {
        assert_eq!(
            metric_route("GET", "/api/v10/oauth2/applications/@me"),
            "GET /api/v10/oauth2/applications/@me"
        );
        assert_eq!(
            metric_route("GET", "/api/v9/gateway/bot"),
            "GET /api/v9/gateway/bot"
        );
    }

    #[test]
    fn encoded_emoji_then_user_id() {
        assert_eq!(
            metric_route("DELETE", "/api/v10/channels/1/messages/2/reactions/%F0%9F%91%8D/123"),
            "DELETE /api/v10/channels/{id}/messages/{id}/reactions/{emoji}/{id}"
        );
    }

    #[test]
    fn invite_and_template_codes_collapsed() {
        assert_eq!(
            metric_route("GET", "/api/v10/invites/discord-dev"),
            "GET /api/v10/invites/{code}"
        );
        assert_eq!(
            metric_route("GET", "/api/v10/guilds/templates/hgM48av5Q69A"),
            "GET /api/v10/guilds/templates/{code}"
        );
    }

    #[test]
    fn unknown_long_or_mixed_case_segment_is_token() {
        assert_eq!(
            metric_route("GET", "/api/v10/future/AbCdEf"),
            "GET /api/v10/future/{token}"
        );
        assert_eq!(
            metric_route("GET", "/api/v10/future/abcdefghijklmnopqrstuvwxyz0123456789"),
            "GET /api/v10/future/{token}"
        );
        assert_eq!(
            metric_route("GET", "/api/v10/guilds/1/auto-moderation/rules"),
            "GET /api/v10/guilds/{id}/auto-moderation/rules"
        );
    }
}
