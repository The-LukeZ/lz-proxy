# lz-proxy — Build Plan

A single-node, transparent Discord REST ratelimit proxy written in Rust. Conceptually a stripped-down `nirn-proxy` (no clustering, no gossip/SWIM, no consistent hashing). One process sits in front of Discord's API, tracks ratelimit buckets, paces requests, and forwards everything else untouched. Multiple client processes (e.g. bot shards) point at it instead of `discord.com`.

This document is written for Claude Code to implement. Follow the phases in order. Do **not** add clustering, Redis, or any distributed coordination — that scope is explicitly excluded.

---

## Goals & non-goals

**Goals**

- Transparent proxy: any Discord-API-compatible client can point at it with only a base-URL change. Requests and responses are passed through byte-for-byte, headers intact.
- Correct per-bucket ratelimit handling so clients can fire requests immediately and let the proxy pace them.
- Global (per-bot) ratelimit handling.
- Multi-bot support: buckets scoped per bot token so two bots never share a budget.
- Prometheus metrics + health endpoint.
- Small, statically-linked Docker image.
- Safe-by-default deployment on a VPS (not internet-exposed).

**Non-goals (do not build)**

- Clustering / multi-instance coordination.
- Request retries (return the 429 to the client and let them retry).
- Request/response modification, caching, or endpoint rewriting.
- Gateway/websocket proxying. REST only.

---

## Ratelimit mechanics to get right

This is the hard part. Discord's REST ratelimits work per _bucket_, and buckets are not the same as routes.

1. **Route → bucket, before you know the bucket.** On the first request to a route you don't yet have a `x-ratelimit-bucket` for, group by a synthetic key: `METHOD + normalized-path`, where the path template collapses IDs. Only **major parameters** get their own bucket: `channel_id`, `guild_id`, and `webhook_id`/`webhook_token`. All other snowflake IDs in the path (message IDs, user IDs, etc.) are replaced with a placeholder and share a bucket. Example: `GET /channels/{channel_id}/messages/{message_id}` — keep `channel_id`, placeholder the message ID.
2. **Learn the real bucket from responses.** Discord returns `x-ratelimit-bucket` on responses. Once seen, remap that route's synthetic key onto Discord's real bucket hash so distinct routes that share a bucket converge correctly.
3. **Per-bucket state** to track from response headers: `x-ratelimit-limit`, `x-ratelimit-remaining`, `x-ratelimit-reset` / `x-ratelimit-reset-after`, `x-ratelimit-bucket`, `x-ratelimit-scope`.
4. **Ordering & serialization.** Fire requests sequentially per bucket and preserve submission order. Use a FIFO queue per bucket (see architecture below) — do **not** rely on `tokio::sync::Mutex` acquisition order, which is not guaranteed FIFO.
5. **Global limit.** ~50 req/s per bot by default, higher for elevated concurrency. Implement a per-bot global limiter (fixed 1s window). Allow overrides via config (some bots have custom REST limits).
6. **429 handling.** Respect `retry-after`. Distinguish `x-ratelimit-scope`: `shared` 429s do not count toward Cloudflare's firewall limit and should be flagged differently in metrics; `global` 429s should pause the whole bot's global limiter; `user` 429s pause just that bucket. The proxy does not retry — it returns the 429 to the client (with the standard ratelimit headers preserved).
7. **Token scoping.** Derive the bot scope from the `Authorization` header. Buckets are keyed on `(bot-scope, bucket-hash)`. Requests with no token (e.g. webhooks) bypass the global limiter but still get per-route bucket handling.

**Security:** never log the `Authorization` header or token contents. Hash it for the scope key; log only the hash prefix if anything.

---

## Architecture

```
                 ┌────────────────────────────────────────┐
  client ───────►│  axum HTTP server (inbound, port 8080)  │
 (bot shards)    └───────────────┬────────────────────────┘
                                 │  extract token-scope + route
                                 ▼
                     ┌───────────────────────┐
                     │  BucketRegistry        │  DashMap<BucketKey, BucketHandle>
                     │  (route→bucket mapping) │
                     └───────────┬────────────┘
                                 │ send request onto bucket's FIFO channel
                                 ▼
              ┌────────────────────────────────────┐
              │  per-bucket worker task            │
              │  - waits for tokens / reset        │
              │  - checks global limiter           │
              │  - fires request via reqwest       │
              │  - parses ratelimit headers        │
              │  - returns response to caller      │
              └───────────────┬────────────────────┘
                              │
                              ▼
                        discord.com/api
```

**Per-bucket worker model.** Each bucket owns a `tokio::mpsc` channel and a dedicated worker task. Inbound requests are packaged as a job `{ request_parts, body, oneshot::Sender<Response> }` and pushed onto the channel. The worker processes jobs FIFO: it waits until the bucket has a token (or sleeps until reset), checks the global limiter, fires the request, updates bucket state from the response headers, and sends the response back through the oneshot. This gives correct ordering per bucket while allowing full parallelism _across_ buckets. Idle bucket workers may self-retire after a timeout to bound memory.

**Global limiter.** One shared per-bot-scope limiter (e.g. a token-bucket over a fixed 1s window). Bucket workers must acquire a global token before firing (except tokenless requests).

---

## Suggested crates

- `tokio` — async runtime (full features).
- `axum` — inbound HTTP server. `hyper` underneath if you need lower-level control.
- `reqwest` — outbound client. **Disable HTTP/2 by default** (config-gated); Discord + Go/Rust HTTP2 edge cases have historically caused spurious failures in nirn, so default to HTTP/1.1.
- `dashmap` — concurrent bucket registry.
- `prometheus` — metrics.
- `tracing` + `tracing-subscriber` — structured logging with env-configurable level.
- `serde` + `envy` (or manual env parsing) — config.
- `anyhow` / `thiserror` — errors.

Build target: `x86_64-unknown-linux-musl` for a static binary and a tiny final image.

---

## Configuration (env vars)

| Var                          | Default        | Purpose                                                   |
| ---------------------------- | -------------- | --------------------------------------------------------- |
| `LZ_BIND`                    | `0.0.0.0:8080` | inbound listen address inside the container               |
| `LZ_METRICS_BIND`            | `0.0.0.0:9000` | metrics/health listen address (separate port)             |
| `LZ_OUTBOUND_TIMEOUT_MS`     | `5000`         | timeout for the Discord request (excludes ratelimit wait) |
| `LZ_DISABLE_HTTP2`           | `true`         | force HTTP/1.1 on the outbound client                     |
| `LZ_LOG_LEVEL`               | `info`         | `tracing` filter                                          |
| `LZ_GLOBAL_RATELIMIT`        | `50`           | default per-bot global req/s                              |
| `LZ_BOT_RATELIMIT_OVERRIDES` | _(empty)_      | `botid:limit,botid:limit` comma list, no spaces           |
| `LZ_BUCKET_IDLE_TTL_S`       | `300`          | retire idle bucket workers after this                     |

Keep the metrics/health server on a **different port** from the proxy so you can expose one without the other.

---

## Endpoints

- `ANY /api/*` and any other path → proxied to `https://discord.com`.
- `GET /lz/healthz` → `200 OK` liveness/readiness (on the metrics port).
- `GET /metrics` → Prometheus exposition (on the metrics port).

**Metrics to export:** request count by method/route/status, 429 count split by scope (`user`/`shared`/`global`), in-flight requests, bucket count, outbound latency histogram, global-limit waits.

---

## Implementation phases

Work through these in order. After each phase, it should compile and the described behavior should be manually verifiable. Commit per phase.

### Phase 0 — Scaffold

- `cargo new lz-proxy`, set up module layout: `main.rs`, `config.rs`, `proxy.rs`, `buckets.rs`, `ratelimit.rs`, `metrics.rs`, `routes.rs`.
- Wire `tokio`, `axum`, `tracing`. Boot an axum server that returns `501 Not Implemented` for everything and serves `/lz/healthz`.

### Phase 1 — Dumb transparent passthrough

- Forward any inbound request to `https://discord.com` + same path + query, copying method, headers (minus hop-by-hop like `host`, `connection`), and body. Stream the response back unchanged (status, headers, body).
- No ratelimiting yet. Verify a real bot library can talk to Discord through it.

### Phase 2 — Route normalization & bucket keys

- Implement path templating with major-parameter awareness (`channel_id`, `guild_id`, `webhook_id`/`webhook_token` preserved; other IDs collapsed).
- Derive `bot-scope` from a hash of the `Authorization` header.
- Build `BucketKey = (bot_scope, method, normalized_route)` and a `BucketRegistry` (DashMap). No enforcement yet — just classify and expose bucket count as a metric.

### Phase 3 — Per-bucket FIFO worker + token tracking

- Spin up a per-bucket worker task with an mpsc job queue.
- Parse `x-ratelimit-*` response headers into `BucketState` (limit, remaining, reset-after, discord bucket hash).
- Worker gates on `remaining`; when exhausted, sleep until reset. Remap synthetic key → real `x-ratelimit-bucket` once learned.
- Verify ordering: rapid-fire N requests to one channel's messages endpoint and confirm they go out sequentially and don't 429.

### Phase 4 — Global limiter & 429 handling

- Add per-bot-scope global limiter (fixed 1s window, config default + overrides).
- Handle 429s: read `retry-after` + `x-ratelimit-scope`; pause the appropriate scope (global vs bucket); return the 429 to the client unmodified. Do not retry.
- Tokenless requests bypass global limiter but keep bucket handling.

### Phase 5 — Observability

- Full Prometheus metric set (above). Structured logging with client IP + method + route + status + wait time. **No tokens in logs.**
- Wire `/metrics` and `/lz/healthz` on the metrics port.

### Phase 6 — Config

- Load all env vars (table above) with defaults. Fail fast with a clear message on malformed overrides.

### Phase 7 — Dockerize

- Multi-stage build (see below). Static musl binary → minimal runtime image. `EXPOSE 8080 9000`.

### Phase 8 — Deployment assets & safe VPS wiring

- Provide `docker-compose.yml` and README deployment section using the safe binding pattern below.

---

## Dockerfile (target shape)

```dockerfile
# ---- builder ----
FROM rust:1-slim AS builder
RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && apt-get install -y musl-tools pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# cache deps
RUN mkdir src && echo 'fn main(){}' > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl || true
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

# ---- runtime ----
FROM alpine:3
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/lz-proxy /usr/local/bin/lz-proxy
EXPOSE 8080 9000
ENTRYPOINT ["/usr/local/bin/lz-proxy"]
```

(Use `scratch` instead of `alpine` if you vendor the CA certs and don't need a shell — alpine is friendlier for debugging.)

---

## Deploying safely on the VPS — read this carefully

**The Docker/UFW gotcha:** Docker writes its own iptables rules into the `DOCKER` chain, and those are evaluated **before** UFW's rules. So publishing a port with `-p 8080:8080` (or `ports: - "8080:8080"` in compose) exposes it to the **public internet** even if `ufw` says `deny`. UFW appears to be protecting you and is not. This is the single most common way a "firewalled" proxy ends up open to the world.

lz-proxy should never be reachable from the internet — only your bot processes need it. Pick one of these patterns:

**Pattern A — bot runs on the same host (simplest, recommended).**
Don't expose the port to the host at all; put the bot and proxy in the same compose network and let the bot reach `http://lz-proxy:8080`.

```yaml
services:
  lz-proxy:
    build: .
    restart: unless-stopped
    environment:
      LZ_BIND: "0.0.0.0:8080"
      LZ_METRICS_BIND: "0.0.0.0:9000"
    networks: [internal]
    # NOTE: no `ports:` block — not published to the host at all.

  yourbot:
    build: ./bot
    restart: unless-stopped
    environment:
      DISCORD_API_BASE: "http://lz-proxy:8080/api"
    networks: [internal]
    depends_on: [lz-proxy]

networks:
  internal:
    driver: bridge
```

**Pattern B — bot on the host but not in Docker.**
Publish bound to loopback only, so Docker's rule can't reach the public interface:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

Then the bot connects to `http://127.0.0.1:8080/api`. The `127.0.0.1:` prefix is what keeps it off the public internet — a bare `"8080:8080"` would not.

**Pattern C — bot on another machine.**
Do **not** publish to `0.0.0.0`. Put the traffic on a private network (WireGuard/Tailscale) and bind the published port to that interface's IP: `- "10.0.0.2:8080:8080"`. Or add an explicit `iptables` rule in the `DOCKER-USER` chain (evaluated before Docker's own rules) to restrict source IPs, or install the `ufw-docker` helper to make UFW rules actually apply to Docker-published ports.

**Metrics port:** keep `9000` internal-only too (Pattern A, or `127.0.0.1:9000:9000`). Scrape it locally or over your private network. Never publish it publicly.

Default to **Pattern A** in the README; mention B and C as alternatives.

---

## Testing / acceptance

- A real bot library, pointed at the proxy, behaves identically to talking to Discord directly (create/edit/delete messages, fetch, etc.).
- Firing a burst well over a bucket's limit produces **zero 429s** from Discord (the proxy paces them), and requests come back in order.
- A deliberately induced 429 (e.g. very low override) is returned to the client with correct headers and is **not** retried by the proxy.
- `/lz/healthz` returns 200; `/metrics` shows sane counters including a 429-by-scope split.
- No token material appears anywhere in logs.
- `docker compose up` with Pattern A works, and the port is confirmed **not** reachable from outside the host (verify with `curl` from another machine — it should fail/refuse).

**Be gentle with the real API during testing** — use a test bot, keep volumes low, and lean on the override config to trigger 429 paths rather than genuinely hammering Discord.

---

## Stretch (only after the above is solid)

- Idle-bucket worker retirement tuning.
- Optional `x-lz-*` debug headers (e.g. bucket key, wait time) behind a config flag.
- Graceful shutdown that drains in-flight buckets.
