use std::{
    fmt::Display,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::Result;
use axum::{
    extract::Request,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use http::StatusCode;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{request_id::PropagateRequestIdLayer, trace::TraceLayer};
use tracing::{debug, error, info, Span};
use uuid::Uuid;

use super::routes::{
    check_writability, configure, database_schema, dbs_and_roles, extension_server, extensions,
    grants, insights, metrics, metrics_json, status, terminate,
};
use crate::compute::ComputeNode;

const X_REQUEST_ID: &str = "x-request-id";

/// `compute_ctl` has two servers: internal and external. The internal server
/// binds to the loopback interface and handles communication from clients on
/// the compute. The external server is what receives communication from the
/// control plane, the metrics scraper, etc. We make the distinction because
/// certain routes in `compute_ctl` only need to be exposed to local processes
/// like Postgres via the neon extension and local_proxy.
#[derive(Clone, Copy, Debug)]
pub enum Server {
    Internal(u16),
    External(u16),
}

impl Display for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Server::Internal(_) => f.write_str("internal"),
            Server::External(_) => f.write_str("external"),
        }
    }
}

impl From<Server> for Router<Arc<ComputeNode>> {
    fn from(server: Server) -> Self {
        let mut router = Router::<Arc<ComputeNode>>::new();

        router = match server {
            Server::Internal(_) => {
                router = router
                    .route(
                        "/extension_server/{*filename}",
                        post(extension_server::download_extension),
                    )
                    .route("/extensions", post(extensions::install_extension))
                    .route("/grants", post(grants::add_grant));

                // Add in any testing support
                if cfg!(feature = "testing") {
                    use super::routes::failpoints;

                    router = router.route("/failpoints", post(failpoints::configure_failpoints));
                }

                router
            }
            Server::External(_) => router
                .route("/check_writability", post(check_writability::is_writable))
                .route("/configure", post(configure::configure))
                .route("/database_schema", get(database_schema::get_schema_dump))
                .route("/dbs_and_roles", get(dbs_and_roles::get_catalog_objects))
                .route("/insights", get(insights::get_insights))
                .route("/metrics", get(metrics::get_metrics))
                .route("/metrics.json", get(metrics_json::get_metrics))
                .route("/status", get(status::get_status))
                .route("/terminate", post(terminate::terminate)),
        };

        router.fallback(Server::handle_404).method_not_allowed_fallback(Server::handle_405).layer(
            ServiceBuilder::new()
                // Add this middleware since we assume the request ID exists
                .layer(middleware::from_fn(maybe_add_request_id_header))
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
    }
}

impl Server {
    async fn handle_404() -> impl IntoResponse {
        StatusCode::NOT_FOUND
    }

    async fn handle_405() -> impl IntoResponse {
        StatusCode::METHOD_NOT_ALLOWED
    }

    async fn listener(&self) -> Result<TcpListener> {
        let addr = SocketAddr::new(self.ip(), self.port());
        let listener = TcpListener::bind(&addr).await?;

        Ok(listener)
    }

    fn ip(&self) -> IpAddr {
        match self {
            Server::Internal(_) => IpAddr::from(Ipv6Addr::LOCALHOST),
            Server::External(_) => IpAddr::from(Ipv6Addr::UNSPECIFIED),
        }
    }

    fn port(self) -> u16 {
        match self {
            Server::Internal(port) => port,
            Server::External(port) => port,
        }
    }

    #[tokio::main]
    async fn serve(self, compute: Arc<ComputeNode>) {
        let listener = self.listener().await.unwrap_or_else(|e| {
            // If we can't bind, the compute cannot operate correctly
            panic!(
                "failed to bind the compute_ctl HTTP server to {}: {}",
                SocketAddr::new(self.ip(), self.port()),
                e
            );
        });

        if tracing::enabled!(tracing::Level::INFO) {
            let local_addr = match listener.local_addr() {
                Ok(local_addr) => local_addr,
                Err(_) => SocketAddr::new(self.ip(), self.port()),
            };

            info!(
                "compute_ctl {} HTTP server listening at {}",
                self, local_addr
            );
        }

        let router = Router::from(self).with_state(compute);

        if let Err(e) = axum::serve(listener, router).await {
            error!("compute_ctl {} HTTP server error: {}", self, e);
        }
    }

    pub fn launch(self, compute: &Arc<ComputeNode>) {
        let state = Arc::clone(compute);

        thread::Builder::new()
            .name(format!("http-server-{self}"))
            .spawn(move || self.serve(state))
            .unwrap_or_else(|_| panic!("Failed to start the {self} HTTP server"));
    }
}

/// This middleware function allows compute_ctl to generate its own request ID
/// if one isn't supplied. The control plane will always send one as a UUID. The
/// neon Postgres extension on the other hand does not send one.
async fn maybe_add_request_id_header(mut request: Request, next: Next) -> Response {
    let headers = request.headers_mut();

    if headers.get(X_REQUEST_ID).is_none() {
        headers.append(X_REQUEST_ID, Uuid::new_v4().to_string().parse().unwrap());
    }

    next.run(request).await
}
