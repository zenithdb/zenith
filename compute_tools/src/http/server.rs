use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use http::StatusCode;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::{debug, error, info, Span};

use super::routes::{
    check_writability, configure, database_schema, dbs_and_roles, extension_server, extensions,
    grants, info as info_route, insights, installed_extensions, metrics, metrics_json, status,
    terminate,
};
use crate::compute::ComputeNode;

async fn handle_404() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Clone, Default)]
struct ComputeMakeRequestId(Arc<AtomicU64>);

impl MakeRequestId for ComputeMakeRequestId {
    fn make_request_id<B>(
        &mut self,
        _request: &http::Request<B>,
    ) -> Option<tower_http::request_id::RequestId> {
        let request_id = self
            .0
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .parse()
            .unwrap();

        Some(RequestId::new(request_id))
    }
}

/// Run the HTTP server and wait on it forever.
#[tokio::main]
async fn serve(port: u16, compute: Arc<ComputeNode>) {
    const X_REQUEST_ID: &str = "x-request-id";

    let mut app = Router::new()
        .route("/check_writability", post(check_writability::is_writable))
        .route("/configure", post(configure::configure))
        .route("/database_schema", get(database_schema::get_schema_dump))
        .route("/dbs_and_roles", get(dbs_and_roles::get_catalog_objects))
        .route(
            "/extension_server/*filename",
            post(extension_server::download_extension),
        )
        .route("/extensions", post(extensions::install_extension))
        .route("/grants", post(grants::add_grant))
        .route("/info", get(info_route::get_info))
        .route("/insights", get(insights::get_insights))
        .route(
            "/installed_extensions",
            get(installed_extensions::get_installed_extensions),
        )
        .route("/metrics", get(metrics::get_metrics))
        .route("/metrics.json", get(metrics_json::get_metrics))
        .route("/status", get(status::get_status))
        .route("/terminate", post(terminate::terminate))
        .fallback(handle_404)
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(
                    ComputeMakeRequestId::default(),
                ))
                .layer(
                    TraceLayer::new_for_http()
                        .on_request(|request: &http::Request<_>, _span: &Span| {
                            let request_id = request
                                .headers()
                                .get(X_REQUEST_ID)
                                .unwrap()
                                .to_str()
                                .unwrap();

                            match request.uri().path() {
                                "/metrics" => {
                                    debug!(%request_id, "{} {}", request.method(), request.uri())
                                }
                                _ => info!(%request_id, "{} {}", request.method(), request.uri()),
                            };
                        })
                        .on_response(
                            |response: &http::Response<_>, latency: Duration, _span: &Span| {
                                let request_id = response
                                    .headers()
                                    .get(X_REQUEST_ID)
                                    .unwrap()
                                    .to_str()
                                    .unwrap();

                                info!(
                                    %request_id,
                                    code = response.status().as_u16(),
                                    latency = latency.as_millis()
                                )
                            },
                        ),
                )
                .layer(PropagateRequestIdLayer::x_request_id()),
        )
        .with_state(compute);

    // Add in any testing support
    if cfg!(feature = "testing") {
        use super::routes::failpoints;

        app = app.route("/failpoints", post(failpoints::configure_failpoints))
    }

    // This usually binds to both IPv4 and IPv6 on Linux, see
    // https://github.com/rust-lang/rust/pull/34440 for more information
    let addr = SocketAddr::new(IpAddr::from(Ipv6Addr::UNSPECIFIED), port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(
                "failed to bind the compute_ctl HTTP server to port {}: {}",
                port, e
            );
            return;
        }
    };

    if let Ok(local_addr) = listener.local_addr() {
        info!("compute_ctl HTTP server listening on {}", local_addr);
    } else {
        info!("compute_ctl HTTP server listening on port {}", port);
    }

    if let Err(e) = axum::serve(listener, app).await {
        error!("compute_ctl HTTP server error: {}", e);
    }
}

/// Launch a separate HTTP server thread and return its `JoinHandle`.
pub fn launch_http_server(port: u16, state: &Arc<ComputeNode>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("http-server".into())
        .spawn(move || serve(port, state))?)
}
