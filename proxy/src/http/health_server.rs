use anyhow::{anyhow, bail};
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use hyper::{header::CONTENT_TYPE, Body, Request, Response, StatusCode};
use measured::{text::BufferedTextEncoder, MetricGroup};
use metrics::NeonMetrics;
use once_cell::sync::Lazy;
use std::{
    convert::Infallible,
    ffi::CString,
    net::TcpListener,
    sync::{Arc, Mutex},
};
use tracing::{info, info_span, warn};
use utils::http::{
    endpoint::{self, request_span},
    error::ApiError,
    json::json_response,
    RouterBuilder, RouterService,
};

use crate::jemalloc;

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, "")
}

async fn prof_dump(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    static PROF_MIB: Lazy<jemalloc::dump_mib> =
        Lazy::new(|| jemalloc::dump::mib().expect("could not create prof.dump MIB"));
    static PROF_DIR: Lazy<Utf8TempDir> =
        Lazy::new(|| camino_tempfile::tempdir().expect("could not create tempdir"));
    static PROF_FILE: Lazy<Utf8PathBuf> = Lazy::new(|| PROF_DIR.path().join("prof.dump"));
    static PROF_FILE0: Lazy<CString> = Lazy::new(|| CString::new(PROF_FILE.as_str()).unwrap());
    static DUMP_LOCK: Mutex<()> = Mutex::new(());

    tokio::task::spawn_blocking(|| {
        let _guard = DUMP_LOCK.lock();
        PROF_MIB
            .write(&PROF_FILE0)
            .expect("could not trigger prof.dump");
        let prof_dump = std::fs::read_to_string(&*PROF_FILE).expect("could not open prof.dump");

        Response::new(Body::from(prof_dump))
    })
    .await
    .map_err(|e| ApiError::InternalServerError(e.into()))
}

fn make_router(metrics: AppMetrics) -> RouterBuilder<hyper::Body, ApiError> {
    let state = Arc::new(Mutex::new(PrometheusHandler {
        encoder: BufferedTextEncoder::new(),
        metrics,
    }));

    let mut router = endpoint::make_router()
        .get("/metrics", move |r| {
            let state = state.clone();
            request_span(r, move |b| prometheus_metrics_handler(b, state))
        })
        .get("/v1/status", status_handler);

    let prof_enabled = jemalloc::prof::read().unwrap_or_default();
    if prof_enabled {
        warn!("activating jemalloc profiling");
        jemalloc::active::write(true).unwrap();
        router = router.get("/v1/jemalloc/prof.dump", prof_dump);
    }

    router
}

pub async fn task_main(
    http_listener: TcpListener,
    metrics: AppMetrics,
) -> anyhow::Result<Infallible> {
    scopeguard::defer! {
        info!("http has shut down");
    }

    let service = || RouterService::new(make_router(metrics).build()?);

    hyper::Server::from_tcp(http_listener)?
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

        let mut state = state.lock().unwrap();
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
    .unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .unwrap();

    Ok(response)
}
