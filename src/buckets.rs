//! Per-bucket FIFO workers, the route→bucket registry, and outbound firing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use reqwest::header::HeaderMap;
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;
use crate::metrics::Metrics;
use crate::ratelimit::{BotScope, GlobalLimiter};

/// A single proxied request handed to a bucket worker.
pub struct Job {
    pub method: reqwest::Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// The synthetic route key, so the worker can record the learned bucket hash.
    pub route_key: String,
    /// Templated route (no ids or tokens), for metric labels only.
    pub metric_route: String,
    pub respond: oneshot::Sender<Result<ProxyResponse, ProxyError>>,
}

/// A buffered upstream response, ready to be turned into an axum response.
pub struct ProxyResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Templated route (no ids or tokens), safe for metrics and logs.
    pub route: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream request failed: {0}")]
    Upstream(reqwest::Error),
    #[error("worker unavailable")]
    WorkerGone,
}

/// Registry of route→bucket mappings and live bucket workers.
pub struct BucketRegistry {
    client: reqwest::Client,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    /// route_key → learned Discord bucket hash.
    route_to_bucket: DashMap<String, String>,
    /// worker_key → job sender.
    workers: DashMap<String, mpsc::Sender<Job>>,
    /// scope hash → shared global limiter.
    globals: DashMap<String, Arc<GlobalLimiter>>,
}

impl BucketRegistry {
    pub fn new(client: reqwest::Client, config: Arc<Config>, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            client,
            config,
            metrics,
            route_to_bucket: DashMap::new(),
            workers: DashMap::new(),
            globals: DashMap::new(),
        })
    }

    fn global_for(&self, scope: &BotScope) -> Arc<GlobalLimiter> {
        if let Some(g) = self.globals.get(&scope.hash) {
            return g.clone();
        }
        let limit = self.config.global_limit_for(&scope.bot_id);
        self.globals
            .entry(scope.hash.clone())
            .or_insert_with(|| Arc::new(GlobalLimiter::new(limit)))
            .clone()
    }

    /// Route a request onto its bucket worker and await the response.
    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch(
        self: &Arc<Self>,
        scope: Option<BotScope>,
        route: String,
        metric_route: String,
        method: reqwest::Method,
        url: String,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<ProxyResponse, ProxyError> {
        let scope_key = scope.as_ref().map(|s| s.hash.as_str()).unwrap_or("-");
        let route_key = format!("{scope_key}|{route}");

        // Global limiter is shared per bot scope; tokenless requests bypass it.
        let global = scope.as_ref().map(|s| self.global_for(s));

        // Retry once: the target worker may retire between lookup and send.
        for _ in 0..2 {
            let bucket_id = self
                .route_to_bucket
                .get(&route_key)
                .map(|h| h.clone())
                .unwrap_or_else(|| format!("route:{route_key}"));
            let worker_key = format!("{scope_key}|{bucket_id}");

            let tx = self.worker(&worker_key, global.clone());

            let (respond, rx) = oneshot::channel();
            let job = Job {
                method: method.clone(),
                url: url.clone(),
                headers: headers.clone(),
                body: body.clone(),
                route_key: route_key.clone(),
                metric_route: metric_route.clone(),
                respond,
            };

            if tx.send(job).await.is_err() {
                // Worker retired; drop the stale entry and try again.
                self.workers.remove(&worker_key);
                continue;
            }
            let result = rx.await.unwrap_or(Err(ProxyError::WorkerGone));
            if let Err(e) = &result {
                self.count_failure(&method, &metric_route, e);
            }
            return result;
        }
        let err = ProxyError::WorkerGone;
        self.count_failure(&method, &metric_route, &err);
        Err(err)
    }

    /// Count a request that failed before Discord answered (502 / 503).
    fn count_failure(&self, method: &reqwest::Method, metric_route: &str, err: &ProxyError) {
        let status = match err {
            ProxyError::Upstream(_) => "502",
            ProxyError::WorkerGone => "503",
        };
        self.metrics
            .requests
            .with_label_values(&[method.as_str(), metric_route, status])
            .inc();
    }

    /// Get an existing worker's sender or spawn a fresh worker task.
    fn worker(
        self: &Arc<Self>,
        worker_key: &str,
        global: Option<Arc<GlobalLimiter>>,
    ) -> mpsc::Sender<Job> {
        if let Some(tx) = self.workers.get(worker_key) {
            return tx.clone();
        }
        let (tx, rx) = mpsc::channel::<Job>(256);
        let entry = self
            .workers
            .entry(worker_key.to_string())
            .or_insert_with(|| tx.clone());
        // If another task raced us, use theirs and drop ours.
        if entry.value().same_channel(&tx) {
            let registry = self.clone();
            let key = worker_key.to_string();
            self.metrics.buckets.inc();
            tokio::spawn(async move {
                bucket_worker(registry, key, rx, global).await;
            });
        }
        entry.value().clone()
    }
}

/// Per-bucket state learned from Discord's ratelimit headers.
#[derive(Default)]
struct BucketState {
    remaining: Option<i64>,
    reset_at: Option<Instant>,
}

/// The FIFO worker loop for a single bucket. Processes jobs strictly in order.
async fn bucket_worker(
    registry: Arc<BucketRegistry>,
    worker_key: String,
    mut rx: mpsc::Receiver<Job>,
    global: Option<Arc<GlobalLimiter>>,
) {
    let idle_ttl = registry.config.bucket_idle_ttl;
    let mut state = BucketState::default();

    loop {
        let job = match tokio::time::timeout(idle_ttl, rx.recv()).await {
            Ok(Some(job)) => job,
            // Channel closed or idle for too long - retire.
            Ok(None) | Err(_) => break,
        };

        registry.metrics.in_flight.inc();

        // Gate on this bucket's remaining tokens.
        if state.remaining == Some(0) {
            if let Some(reset) = state.reset_at {
                let now = Instant::now();
                if reset > now {
                    tokio::time::sleep(reset - now).await;
                }
            }
            // After reset we no longer know the count; allow one and re-learn.
            state.remaining = None;
        }

        // Global limiter (skipped for tokenless requests).
        if let Some(g) = &global {
            if g.acquire().await {
                registry.metrics.global_waits.inc();
            }
        }

        let result = fire(&registry, &job, &mut state).await;
        let _ = job.respond.send(result);

        registry.metrics.in_flight.dec();
    }

    registry.workers.remove(&worker_key);
    registry.metrics.buckets.dec();
}

/// Fire one request upstream, update bucket state, and buffer the response.
async fn fire(
    registry: &Arc<BucketRegistry>,
    job: &Job,
    state: &mut BucketState,
) -> Result<ProxyResponse, ProxyError> {
    let route = job.metric_route.clone();
    let method_label = job.method.as_str().to_string();
    let timer = registry
        .metrics
        .outbound_latency
        .with_label_values(&[&method_label, &route])
        .start_timer();

    let resp = registry
        .client
        .request(job.method.clone(), &job.url)
        .headers(job.headers.clone())
        .body(job.body.clone())
        .send()
        .await;
    timer.observe_duration();

    let resp = match resp {
        Ok(r) => r,
        // Strip the URL: it can carry webhook / interaction tokens.
        Err(e) => return Err(ProxyError::Upstream(e.without_url())),
    };

    let status = resp.status().as_u16();
    let headers = resp.headers().clone();

    update_state(registry, job, &headers, status, state);

    let body = resp
        .bytes()
        .await
        .map_err(|e| ProxyError::Upstream(e.without_url()))?;

    registry
        .metrics
        .requests
        .with_label_values(&[&method_label, &route, &status.to_string()])
        .inc();

    Ok(ProxyResponse {
        status,
        headers,
        body,
        route,
    })
}

/// Parse `x-ratelimit-*` headers, learn the real bucket hash, and handle 429s.
fn update_state(
    registry: &Arc<BucketRegistry>,
    job: &Job,
    headers: &HeaderMap,
    status: u16,
    state: &mut BucketState,
) {
    // Learn the real bucket hash and remap the synthetic key onto it.
    if let Some(hash) = header_str(headers, "x-ratelimit-bucket") {
        let changed = registry
            .route_to_bucket
            .get(&job.route_key)
            .map(|h| *h != hash)
            .unwrap_or(true);
        if changed {
            registry
                .route_to_bucket
                .insert(job.route_key.clone(), hash.to_string());
        }
    }

    if status == 429 {
        let scope = header_str(headers, "x-ratelimit-scope").unwrap_or("unknown");
        registry
            .metrics
            .ratelimited
            .with_label_values(&[scope])
            .inc();

        let retry_after = header_f64(headers, "retry-after")
            .or_else(|| header_f64(headers, "x-ratelimit-reset-after"))
            .unwrap_or(1.0);
        let dur = Duration::from_secs_f64(retry_after.max(0.0));

        match scope {
            // Whole bot is limited: pause the shared global limiter.
            "global" => {
                let scope_hash = job.route_key.split('|').next().unwrap_or("-");
                if let Some(g) = registry.globals.get(scope_hash) {
                    g.pause(dur);
                }
            }
            // Just this bucket: pause locally until retry-after.
            _ => {
                state.remaining = Some(0);
                state.reset_at = Some(Instant::now() + dur);
            }
        }
        return;
    }

    // Normal path: track remaining / reset for proactive pacing.
    if let Some(r) = header_i64(headers, "x-ratelimit-remaining") {
        state.remaining = Some(r);
    }
    if let Some(reset_after) = header_f64(headers, "x-ratelimit-reset-after") {
        state.reset_at = Some(Instant::now() + Duration::from_secs_f64(reset_after.max(0.0)));
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    header_str(headers, name).and_then(|v| v.parse().ok())
}

fn header_f64(headers: &HeaderMap, name: &str) -> Option<f64> {
    header_str(headers, name).and_then(|v| v.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ratelimit::{metric_route, normalize_route};
    use std::collections::HashMap;

    const SECRET: &str = "SECRETwebhookTOKEN";

    fn test_registry() -> Arc<BucketRegistry> {
        let config = Config {
            bind: "127.0.0.1:0".to_string(),
            metrics_bind: "127.0.0.1:0".to_string(),
            outbound_timeout: Duration::from_secs(2),
            disable_http2: true,
            log_level: "info".to_string(),
            global_ratelimit: 50,
            bot_ratelimit_overrides: HashMap::new(),
            bucket_idle_ttl: Duration::from_secs(60),
        };
        BucketRegistry::new(
            reqwest::Client::new(),
            Arc::new(config),
            Arc::new(Metrics::new()),
        )
    }

    async fn fire_at_closed_port(reg: &Arc<BucketRegistry>) -> Result<ProxyResponse, ProxyError> {
        let path = format!("/api/webhooks/555/{SECRET}");
        reg.dispatch(
            None,
            normalize_route("POST", &path),
            metric_route("POST", &path),
            reqwest::Method::POST,
            format!("http://127.0.0.1:1{path}"),
            HeaderMap::new(),
            Bytes::new(),
        )
        .await
    }

    #[tokio::test]
    async fn upstream_failure_counted_as_502() {
        let reg = test_registry();
        assert!(fire_at_closed_port(&reg).await.is_err());
        let n = reg
            .metrics
            .requests
            .with_label_values(&["POST", "POST /api/webhooks/{id}/{token}", "502"])
            .get();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn upstream_error_hides_url() {
        let reg = test_registry();
        let err = fire_at_closed_port(&reg).await.err().unwrap();
        assert!(!err.to_string().contains(SECRET));
    }

    #[tokio::test]
    async fn metrics_export_never_contains_token() {
        let reg = test_registry();
        let _ = fire_at_closed_port(&reg).await;
        let out = reg.metrics.export();
        assert!(!out.contains(SECRET));
        assert!(out.contains(r#"route="POST /api/webhooks/{id}/{token}""#));
    }
}
