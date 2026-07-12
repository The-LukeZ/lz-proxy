# lz-proxy

A single-node, transparent Discord REST ratelimit proxy in Rust. Sit it in front
of Discord's API; point your bot(s) at it with a base-URL change. It tracks
per-bucket and per-bot global ratelimits, paces requests so clients can fire
freely, and forwards everything else byte-for-byte.

Conceptually a stripped-down `nirn-proxy` - **no clustering, no gossip, no
consistent hashing, no Redis**. One process, in front of `discord.com`.

## What it does

- **Transparent passthrough.** Any Discord-API-compatible client works with only
  a base-URL change. Headers and bodies are forwarded unchanged.
- **Per-bucket ratelimiting.** Requests are grouped by a synthetic route key
  (major parameters `channel_id` / `guild_id` / `webhook_id` preserved, other IDs
  collapsed), then remapped onto Discord's real `x-ratelimit-bucket` once learned.
  Each bucket has a dedicated FIFO worker, so ordering is preserved per bucket and
  requests are paced to avoid 429s.
- **Global limiter.** A per-bot (per-token) fixed-1s-window limiter, default
  50 req/s, overridable per bot.
- **429 handling.** Honours `retry-after` and `x-ratelimit-scope`: `global` 429s
  pause the whole bot; `user`/`shared` 429s pause just that bucket. The proxy
  **never retries** - it returns the 429 to the client with headers intact.
- **Multi-bot.** Buckets are scoped per token, so two bots never share a budget.
- **Observability.** Prometheus metrics + health endpoint on a separate port.
- **No token leakage.** The `Authorization` header is never logged; only a short
  SHA-256 prefix is.

Non-goals: retries, caching, request/response rewriting, gateway/websocket
proxying, and any form of clustering.

## lz-proxy vs nirn-proxy

Both sit in front of `discord.com` and pace requests per-bucket so bots don't eat 429s. Differ in scope:

|                     | lz-proxy                                                      | nirn-proxy                                                  |
| ------------------- | ------------------------------------------------------------- | ----------------------------------------------------------- |
| Topology            | one proxy instance (many bot/shard processes can point at it) | multi-instance proxy, clustered (gossip/consistent hashing) |
| Shared state        | none - in-memory per process                                  | Redis-backed shared bucket state                            |
| Retries             | never - 429 returned to client                                | can retry internally                                        |
| Caching / rewriting | none - byte-for-byte passthrough                              | none (also transparent)                                     |
| Gateway/WS proxy    | no - REST only                                                | no - REST only                                              |
| Language            | Rust                                                          | Go                                                          |
| Ops complexity      | one binary, no external deps                                  | needs Redis + cluster config for multi-node                 |

Note: sharding (many bot processes, same token) is fine on lz-proxy - all
shards point at the one proxy instance, which holds the shared bucket state in
memory. The clustering nirn-proxy adds is for running _multiple proxy
instances_ (e.g. behind a load balancer for HA/scale-out), which then need
Redis to share state between proxy nodes.

**Pick nirn-proxy** if need multiple proxy instances (HA, scale-out beyond one
box) and already run Redis.

**Pick lz-proxy** if one proxy instance is enough (true even with many bot
shard processes behind it), want zero external deps, small Rust binary.
Simpler = fewer moving parts to break.

## Quick start

```sh
cargo run
# proxy on :8080, metrics/health on :9000
curl localhost:9000/lz/healthz            # 200 OK
curl localhost:8080/api/v10/gateway       # proxied to discord.com
```

Point your bot library at `http://<host>:8080/api` instead of
`https://discord.com/api`.

## Configuration (env vars)

| Var                          | Default        | Purpose                                                   |
| ---------------------------- | -------------- | --------------------------------------------------------- |
| `LZ_BIND`                    | `0.0.0.0:8080` | inbound listen address                                    |
| `LZ_METRICS_BIND`            | `0.0.0.0:9000` | metrics/health listen address (separate port)             |
| `LZ_OUTBOUND_TIMEOUT_MS`     | `5000`         | timeout for the Discord request (excludes ratelimit wait) |
| `LZ_DISABLE_HTTP2`           | `true`         | force HTTP/1.1 on the outbound client                     |
| `LZ_LOG_LEVEL`               | `info`         | `tracing` filter                                          |
| `LZ_GLOBAL_RATELIMIT`        | `50`           | default per-bot global req/s                              |
| `LZ_BOT_RATELIMIT_OVERRIDES` | _(empty)_      | `botid:limit,botid:limit` comma list, no spaces           |
| `LZ_BUCKET_IDLE_TTL_S`       | `300`          | retire idle bucket workers after this many seconds        |

## Endpoints

- `ANY /*` → proxied to `https://discord.com` (path + query verbatim).
- `GET /lz/healthz` → `200 OK` (metrics port).
- `GET /metrics` → Prometheus exposition (metrics port).

Metrics: `lz_requests_total{method,route,status}`,
`lz_ratelimited_total{scope}`, `lz_in_flight`, `lz_buckets`,
`lz_outbound_latency_seconds`, `lz_global_limit_waits_total`.

## Deployment - read this carefully

lz-proxy should **never** be reachable from the internet - only your bot
processes need it.

**The Docker/UFW gotcha:** Docker writes its own iptables rules into the `DOCKER`
chain, evaluated **before** UFW's rules. Publishing a port with `-p 8080:8080`
(or `ports: - "8080:8080"`) exposes it to the **public internet even if `ufw`
says deny**. UFW appears to protect you and does not. This is the single most
common way a "firewalled" proxy ends up open to the world.

Pick one:

### Pattern A - bot on the host, not in Docker (this repo's default)

The shipped `docker-compose.yml` publishes the proxy to **loopback only**:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

Your host bot connects to `http://127.0.0.1:8080/api`. The `127.0.0.1:` prefix is
what keeps it off the public internet - a bare `"8080:8080"` would not. Bring it
up:

```sh
docker compose up -d --build
```

### Pattern B - bot also in Docker

If the bot runs in Docker too, don't publish the port at all. Drop the `ports:`
block, put the bot on the `internal` network, and let it reach
`http://lz-proxy:8080`. Nothing is published to a host interface, so the
Docker/UFW gotcha can't bite.

### Pattern C - bot on another machine

Do **not** publish to `0.0.0.0`. Put the traffic on a private network
(WireGuard/Tailscale) and bind the published port to that interface's IP:
`- "10.0.0.2:8080:8080"`. Alternatively add an `iptables` rule in the
`DOCKER-USER` chain (evaluated before Docker's own rules) to restrict source IPs,
or install the `ufw-docker` helper so UFW rules actually apply to published ports.

**Metrics port (9000):** keep it internal-only too (Pattern B, or `127.0.0.1:9000:9000`). Never publish it publicly.

To confirm you got it right, `curl` the proxy port from another machine - it
should refuse/fail.

## Monitoring (Prometheus + Grafana)

Two ways to consume the metrics:

**Turnkey stack (recommended).** An optional compose overlay brings up Prometheus
(scraping `lz-proxy:9000`, internal-only) and Grafana with the datasource and
dashboard already provisioned:

```sh
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d
```

Grafana is published to **loopback only** (`127.0.0.1:3131`, admin/admin by
default - change it). View it locally or over an SSH tunnel; never expose it
publicly. The `lz-proxy` dashboard appears automatically.

**Bring-your-own Grafana.** If you already run Prometheus + Grafana, point your
Prometheus at `lz-proxy:9000/metrics` and import
`monitoring/grafana/dashboards/lz-proxy.json` directly (Grafana → Dashboards →
Import). This mirrors how nirn-proxy ships a lone dashboard file.

The dashboard panels: request rate by status, 429s by scope, in-flight, live
buckets, global-limiter waits, outbound latency quantiles (p50/p95/p99), and top
routes by request rate.

## Build

```sh
cargo build --release
# static musl binary + tiny image:
docker build -t lz-proxy .
```

### License

Licensed under [PolyForm Perimeter 1.0.1](LICENSE) — free to use, including commercially, but you may not offer a competing product built on it.

---

### Built with AI

Because I needed this and I'm not that good with Rust yet, I used Claude to help me get the first working version. It was a great way to get a working prototype quickly and I learned from it.
