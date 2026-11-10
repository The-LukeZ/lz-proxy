//! Prometheus metric definitions and the exposition helper.

use prometheus::{
    Encoder, HistogramVec, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder,
};

pub struct Metrics {
    pub registry: Registry,
    /// Requests by method, templated route, and status.
    pub requests: IntCounterVec,
    /// 429s split by `x-ratelimit-scope` (user/shared/global/unknown).
    pub ratelimited: IntCounterVec,
    /// In-flight (queued + firing) requests.
    pub in_flight: IntGauge,
    /// Live bucket workers.
    pub buckets: IntGauge,
    /// Discord response time (seconds), by method and templated route.
    pub outbound_latency: HistogramVec,
    /// Count of waits caused by the global limiter.
    pub global_waits: IntCounter,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests = IntCounterVec::new(
            prometheus::opts!(
                "lz_requests_total",
                "Proxied requests by method/route/status"
            ),
            &["method", "route", "status"],
        )
        .unwrap();
        let ratelimited = IntCounterVec::new(
            prometheus::opts!("lz_ratelimited_total", "429 responses by scope"),
            &["scope"],
        )
        .unwrap();
        let in_flight =
            IntGauge::new("lz_in_flight", "Requests currently queued or firing").unwrap();
        let buckets = IntGauge::new("lz_buckets", "Live bucket worker tasks").unwrap();
        let outbound_latency = HistogramVec::new(
            prometheus::histogram_opts!(
                "lz_outbound_latency_seconds",
                "Outbound request latency in seconds",
                vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
            ),
            &["method", "route"],
        )
        .unwrap();
        let global_waits = IntCounter::new(
            "lz_global_limit_waits_total",
            "Cumulative global-limiter waits",
        )
        .unwrap();

        registry.register(Box::new(requests.clone())).unwrap();
        registry.register(Box::new(ratelimited.clone())).unwrap();
        registry.register(Box::new(in_flight.clone())).unwrap();
        registry.register(Box::new(buckets.clone())).unwrap();
        registry
            .register(Box::new(outbound_latency.clone()))
            .unwrap();
        registry.register(Box::new(global_waits.clone())).unwrap();

        Self {
            registry,
            requests,
            ratelimited,
            in_flight,
            buckets,
            outbound_latency,
            global_waits,
        }
    }

    /// Render the registry in Prometheus text exposition format.
    pub fn export(&self) -> String {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        encoder.encode(&families, &mut buf).ok();
        String::from_utf8(buf).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
