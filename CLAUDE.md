# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

lz-proxy is a single-node, transparent Discord REST ratelimit proxy in Rust. It sits in front of `discord.com`, tracks per-bucket and per-bot global ratelimits, paces requests so clients can fire freely, and forwards everything else byte-for-byte. `PLAN.md` is the authoritative spec and phase-by-phase build plan - read it before making non-trivial changes.

**Hard scope boundaries (do not cross without being asked):** no clustering / multi-instance coordination, no Redis or distributed state, no request retries (return the 429 to the client), no caching or request/response rewriting, no gateway/websocket proxying (REST only).

## Commands

```sh
cargo build                 # debug build
cargo build --release       # optimized (strip + LTO + codegen-units=1)
cargo test                  # all tests
cargo test <name>           # single test, e.g. cargo test webhook_id
cargo run                   # boots proxy :8080 + metrics/health :9000

# manual smoke test
curl localhost:9000/lz/healthz          # 200
curl localhost:8080/api/v10/gateway     # proxied, unauthenticated
curl localhost:9000/metrics             # prometheus exposition

# container
docker build -t lz-proxy .              # musl static build → alpine
docker compose up -d --build            # Pattern B: proxy in Docker, bot on host
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d  # + Prometheus/Grafana
```

There is no linter config beyond stock `cargo`; run `cargo clippy` if you want lint feedback. The build must stay warning-clean.

## Architecture

Two axum servers boot from `main.rs`: the proxy (`LZ_BIND`, default `:8080`) and a separate metrics/health server (`LZ_METRICS_BIND`, default `:9000`). They are deliberately on different ports so one can be exposed without the other.

Request flow: `proxy::handle` (fallback for every path) strips hop-by-hop headers, derives the bot scope, normalizes the route, then hands the request to `BucketRegistry::dispatch`, which routes it onto a per-bucket FIFO worker that does the actual outbound `reqwest` call and returns a buffered response.

### The ratelimit model (the core, and the subtle part)

This is the reason the project exists; get it right. Three cooperating pieces in `buckets.rs` and `ratelimit.rs`:

1. **Route normalization** (`ratelimit::normalize_route`). Collapses a path into a synthetic bucket key. Only the _major parameters_ keep their real value (`channel_id`, `guild_id`, `webhook_id` + `webhook_token`); every other snowflake becomes `{id}`, reactions emoji becomes `{emoji}`. This is only a _first guess_ at the bucket.

2. **Bucket learning / remapping.** Discord returns the real bucket hash in `x-ratelimit-bucket`. After each response the worker records `route_key → real hash` in `BucketRegistry::route_to_bucket`, so future requests for that route dispatch onto the real bucket's worker and distinct routes that share a bucket converge. Until learned, a route uses a `route:...` fallback worker id. Over- or under-collapsing is _safe_ (extra serialization or extra workers), never a correctness bug - the real hash self-heals convergence.

3. **Per-bucket FIFO worker** (`buckets::bucket_worker`). Each bucket has a dedicated task fed by an mpsc channel, so ordering is preserved and requests within a bucket are strictly serialized (one in flight at a time - required, because the next request's pacing depends on this response's headers). It gates on the bucket's `remaining`/`reset`, acquires a global token (unless tokenless), fires, then parses `x-ratelimit-*` to update state. Do **not** replace this with a `Mutex` - `tokio::sync::Mutex` acquisition is not FIFO. Idle workers self-retire after `LZ_BUCKET_IDLE_TTL_S`; `dispatch` retries once if it races a retiring worker.

**Global limiter** (`ratelimit::GlobalLimiter`): one per bot scope, fixed 1-second window, default 50 req/s (`LZ_GLOBAL_RATELIMIT`, overridable per bot). Tokenless requests (e.g. webhooks with no `Authorization`) bypass it but still get per-bucket handling.

**429 handling** (`buckets::update_state`): read `retry-after` + `x-ratelimit-scope`. `global` scope pauses the whole bot's global limiter; `user`/`shared` pauses just that bucket. The proxy never retries - the 429 is returned to the client with headers intact. Scope is split out in the `lz_ratelimited_total{scope}` metric.

### Token scoping and the security invariant

The bot scope (`ratelimit::BotScope`) is a SHA-256 hash of the `Authorization` header; buckets and global limiters are keyed on it so bots never share budgets. The `bot_id` is best-effort base64-decoded from the token (public, used for override lookup and logs). **The raw token / `Authorization` header must never be logged or stored** - only `BotScope::log_prefix()` (8-char hash prefix) ever reaches logs. Preserve this whenever touching logging or error paths.

## Module map

- `main.rs` - config load, reqwest client (HTTP/1.1-only when `LZ_DISABLE_HTTP2`), boots both servers
- `config.rs` - env-var config, fail-fast parsing of `LZ_BOT_RATELIMIT_OVERRIDES`
- `proxy.rs` - inbound axum handler, header stripping, response rebuild, request logging
- `buckets.rs` - `BucketRegistry`, per-bucket workers, outbound firing, header parsing, 429 logic
- `ratelimit.rs` - route normalization, `BotScope`, `GlobalLimiter` (has the unit tests)
- `metrics.rs` - Prometheus registry + exposition
- `routes.rs` - router construction for both servers

## Deployment gotcha (matters for compose/docs changes)

Docker's iptables rules are evaluated _before_ UFW, so publishing a bare `"8080:8080"` exposes the proxy to the public internet even when UFW says deny. This proxy must never be internet-reachable. The shipped `docker-compose.yml` is Pattern B (loopback-only `127.0.0.1:8080:8080`, bot on host); Pattern A (no `ports:`, bot on the `internal` network) is the alternative for a dockerized bot. Keep any published port bound to loopback or a private interface. Full detail in `README.md`.
