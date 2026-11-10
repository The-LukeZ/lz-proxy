//! The inbound axum handler: prepare the outbound request, dispatch it onto a
//! bucket worker, and translate the buffered upstream response back.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderName, Request, Response, StatusCode};
use axum::response::IntoResponse;
use tracing::{info, warn};

use crate::buckets::{BucketRegistry, ProxyError};
use crate::ratelimit::{metric_route, normalize_route, BotScope};

/// Upstream Discord API origin. Path + query are appended verbatim.
const DISCORD_BASE: &str = "https://discord.com";

/// Headers that must not be forwarded (connection-specific / hop-by-hop).
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
];

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<BucketRegistry>,
}

/// Fallback handler: proxy everything to Discord.
pub async fn handle(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    let started = Instant::now();
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();

    let path = parts.uri.path();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(path);
    let url = format!("{DISCORD_BASE}{path_and_query}");

    let route = normalize_route(method.as_str(), path);
    // Templated route for logs: never contains ids or webhook/interaction tokens.
    let metric_route = metric_route(method.as_str(), path);
    let method_log = method.as_str().to_string();

    let scope = BotScope::from_auth(
        parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    // Only a short hash prefix is ever logged - never the token itself.
    let scope_log = scope
        .as_ref()
        .map(|s| s.log_prefix().to_string())
        .unwrap_or_else(|| "-".to_string());

    let mut headers = parts.headers.clone();
    strip_hop_by_hop(&mut headers);
    // reqwest derives Host and Content-Length from the URL and body.
    headers.remove(axum::http::header::HOST);
    headers.remove(axum::http::header::CONTENT_LENGTH);

    let body = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "failed to read inbound body");
            return (StatusCode::BAD_REQUEST, "failed to read request body").into_response();
        }
    };

    let result = state
        .registry
        .dispatch(scope, route, metric_route.clone(), method, url, headers, body)
        .await;

    match result {
        Ok(resp) => {
            info!(
                client = %peer.ip(),
                scope = %scope_log,
                method = %method_log,
                route = %resp.route,
                status = resp.status,
                latency_ms = started.elapsed().as_millis() as u64,
                "proxied"
            );
            build_response(resp)
        }
        Err(ProxyError::Upstream(e)) => {
            // `e` was built with `without_url()`, so no token-bearing path is logged.
            warn!(error = %e, method = %method_log, route = %metric_route, "upstream request failed");
            (StatusCode::BAD_GATEWAY, "upstream request failed").into_response()
        }
        Err(ProxyError::WorkerGone) => {
            warn!(method = %method_log, route = %metric_route, "bucket worker unavailable");
            (StatusCode::SERVICE_UNAVAILABLE, "bucket worker unavailable").into_response()
        }
    }
}

fn build_response(resp: crate::buckets::ProxyResponse) -> Response<Body> {
    let mut builder = Response::builder().status(resp.status);
    for (name, value) in resp.headers.iter() {
        if is_hop_by_hop(name.as_str()) || name == axum::http::header::CONTENT_LENGTH {
            continue;
        }
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(resp.body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn strip_hop_by_hop(headers: &mut HeaderMap) {
    for name in HOP_BY_HOP {
        headers.remove(HeaderName::from_static(name));
    }
}

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.contains(&name.to_ascii_lowercase().as_str())
}
