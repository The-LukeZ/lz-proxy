//! lz-proxy — a single-node transparent Discord REST ratelimit proxy.

mod buckets;
mod config;
mod metrics;
mod proxy;
mod ratelimit;
mod routes;

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::buckets::BucketRegistry;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::proxy::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::from_env().context("loading configuration")?);

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.log_level))
        .init();

    info!(
        bind = %config.bind,
        metrics_bind = %config.metrics_bind,
        disable_http2 = config.disable_http2,
        global_ratelimit = config.global_ratelimit,
        "starting lz-proxy"
    );

    let metrics = Arc::new(Metrics::new());
    let client = build_client(&config)?;
    let registry = BucketRegistry::new(client, config.clone(), metrics.clone());

    let app_state = AppState { registry };

    let proxy = serve_proxy(&config.bind, routes::proxy_router(app_state));
    let metrics_srv = serve(
        &config.metrics_bind,
        routes::metrics_router(metrics),
        "metrics",
    );

    tokio::try_join!(proxy, metrics_srv)?;
    Ok(())
}

/// Build the outbound reqwest client, honouring the HTTP/2 and timeout config.
fn build_client(config: &Config) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(config.outbound_timeout)
        .use_rustls_tls();
    if config.disable_http2 {
        builder = builder.http1_only();
    }
    builder.build().context("building outbound HTTP client")
}

/// Bind and serve an axum router, labelling failures with the server name.
async fn serve(bind: &str, router: axum::Router, name: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding {name} server to {bind}"))?;
    info!(%bind, server = name, "listening");
    axum::serve(listener, router)
        .await
        .with_context(|| format!("{name} server error"))
}

/// Serve the proxy router with per-connection peer info (for request logging).
async fn serve_proxy(bind: &str, router: axum::Router) -> Result<()> {
    use std::net::SocketAddr;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding proxy server to {bind}"))?;
    info!(%bind, server = "proxy", "listening");
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("proxy server error")
}
