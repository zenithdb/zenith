use std::convert::Infallible;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail};
use hyper0::header::CONTENT_TYPE;
use hyper0::{Body, Request, Response, StatusCode};
use measured::text::BufferedTextEncoder;
use measured::MetricGroup;
use metrics::NeonMetrics;
use tracing::{info, info_span};
use utils::http::endpoint::{self, request_span};
use utils::http::error::ApiError;
use utils::http::json::json_response;
use utils::http::{RouterBuilder, RouterService};

use crate::ext::{LockExt, TaskExt};
use crate::jemalloc;

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

fn make_router(metrics: AppMetrics) -> RouterBuilder<hyper0::Body, ApiError> {
    let state = Arc::new(Mutex::new(PrometheusHandler {
        encoder: BufferedTextEncoder::new(),
        metrics,
    }));

    endpoint::make_router()
        .get("/metrics", move |r| {
            let state = state.clone();
            request_span(r, move |b| prometheus_metrics_handler(b, state))
        })
        .get("/v1/status", status_handler)
}

pub async fn task_main(
    http_listener: TcpListener,
    metrics: AppMetrics,
) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router(metrics).build()?);

    hyper0::Server::from_tcp(http_listener)?
        .serve(service().map_err(|e| anyhow!(e))?)
        .await?;

    bail!("hyper server without shutdown handling cannot shutdown successfully");
}

struct PrometheusHandler {
    encoder: BufferedTextEncoder,
    metrics: AppMetrics,
}

#[derive(MetricGroup)]
pub struct AppMetrics {
    #[metric(namespace = "jemalloc")]
    pub jemalloc: Option<jemalloc::MetricRecorder>,
    #[metric(flatten)]
    pub neon_metrics: NeonMetrics,
    #[metric(flatten)]
    pub proxy: &'static crate::metrics::Metrics,
}

async fn prometheus_metrics_handler(
    _req: Request<Body>,
    state: Arc<Mutex<PrometheusHandler>>,
) -> Result<Response<Body>, ApiError> {
    let started_at = std::time::Instant::now();

    let span = info_span!("blocking");
    let body = tokio::task::spawn_blocking(move || {
        let _span = span.entered();

        let mut state = state.lock_propagate_poison();
        let PrometheusHandler { encoder, metrics } = &mut *state;

        metrics
            .collect_group_into(&mut *encoder)
            .unwrap_or_else(|infallible| match infallible {});

        let body = encoder.finish();

        tracing::info!(
            bytes = body.len(),
            elapsed_ms = started_at.elapsed().as_millis(),
            "responded /metrics"
        );

        body
    })
    .await
    .propagate_task_panic();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .expect("response headers should be valid");

    Ok(response)
}
