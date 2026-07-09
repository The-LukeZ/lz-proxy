//! Router construction for the two servers (proxy port + metrics port).

use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use axum::Router;

use crate::metrics::Metrics;
use crate::proxy::{self, AppState};

/// The inbound proxy router: everything falls through to Discord.
pub fn proxy_router(state: AppState) -> Router {
    Router::new().fallback(any(proxy::handle)).with_state(state)
}

/// The metrics/health router, served on a separate port.
pub fn metrics_router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/lz/healthz", get(|| async { StatusCode::OK }))
        .route("/metrics", get(metrics_handler))
        .with_state(metrics)
}

async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics.export(),
    )
}
