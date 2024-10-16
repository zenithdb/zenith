//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod backend;
pub mod cancel_set;
mod conn_pool;
mod http_conn_pool;
mod http_util;
mod json;
mod json_raw_value;
mod local_conn_pool;
mod sql_over_http;
mod websocket;

use async_trait::async_trait;
use atomic_take::AtomicTake;
use bytes::Bytes;
pub use conn_pool::GlobalConnPoolOptions;

use anyhow::Context;
use futures::future::{select, Either};
use futures::TryFutureExt;
use http::{Method, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto::Builder;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_util::task::TaskTracker;

use crate::cancellation::CancellationHandlerMain;
use crate::config::ProxyConfig;
use crate::context::RequestMonitoring;
use crate::metrics::Metrics;
use crate::protocol2::{read_proxy_protocol, ChainRW};
use crate::proxy::run_until_cancelled;
use crate::rate_limiter::EndpointRateLimiter;
use crate::serverless::backend::PoolingBackend;
use crate::serverless::http_util::{api_error_into_response, json_response};

use std::net::{IpAddr, SocketAddr};
use std::pin::{pin, Pin};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, Instrument};
use utils::http::error::ApiError;

pub(crate) const SERVERLESS_DRIVER_SNI: &str = "api";

pub async fn task_main(
    config: &'static ProxyConfig,
    auth_backend: &'static crate::auth::Backend<'static, ()>,
    ws_listener: TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandlerMain>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("websocket server has shut down");
    }

    let local_pool = local_conn_pool::LocalConnPool::new(&config.http_config);
    let conn_pool = conn_pool::GlobalConnPool::new(&config.http_config);
    {
        let conn_pool = Arc::clone(&conn_pool);
        tokio::spawn(async move {
            conn_pool.gc_worker(StdRng::from_entropy()).await;
        });
    }

    // shutdown the connection pool
    tokio::spawn({
        let cancellation_token = cancellation_token.clone();
        let conn_pool = conn_pool.clone();
        async move {
            cancellation_token.cancelled().await;
            tokio::task::spawn_blocking(move || conn_pool.shutdown())
                .await
                .unwrap();
        }
    });

    let http_conn_pool = http_conn_pool::GlobalConnPool::new(&config.http_config);
    {
        let http_conn_pool = Arc::clone(&http_conn_pool);
        tokio::spawn(async move {
            http_conn_pool.gc_worker(StdRng::from_entropy()).await;
        });
    }

    // shutdown the connection pool
    tokio::spawn({
        let cancellation_token = cancellation_token.clone();
        let http_conn_pool = http_conn_pool.clone();
        async move {
            cancellation_token.cancelled().await;
            tokio::task::spawn_blocking(move || http_conn_pool.shutdown())
                .await
                .unwrap();
        }
    });

    let backend = Arc::new(PoolingBackend {
        http_conn_pool: Arc::clone(&http_conn_pool),
        local_pool,
        pool: Arc::clone(&conn_pool),
        config,
        auth_backend,
        endpoint_rate_limiter: Arc::clone(&endpoint_rate_limiter),
    });
    let tls_acceptor: Arc<dyn MaybeTlsAcceptor> = match config.tls_config.as_ref() {
        Some(config) => {
            let mut tls_server_config = rustls::ServerConfig::clone(&config.to_server_config());
            // prefer http2, but support http/1.1
            tls_server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            Arc::new(tls_server_config)
        }
        None => {
            warn!("TLS config is missing");
            Arc::new(NoTls)
        }
    };

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    connections.close(); // allows `connections.wait to complete`

    while let Some(res) = run_until_cancelled(ws_listener.accept(), &cancellation_token).await {
        let (conn, peer_addr) = res.context("could not accept TCP stream")?;
        if let Err(e) = conn.set_nodelay(true) {
            tracing::error!("could not set nodelay: {e}");
            continue;
        }
        let conn_id = uuid::Uuid::new_v4();
        let http_conn_span = tracing::info_span!("http_conn", ?conn_id);

        let n_connections = Metrics::get()
            .proxy
            .client_connections
            .sample(crate::metrics::Protocol::Http);
        tracing::trace!(?n_connections, threshold = ?config.http_config.client_conn_threshold, "check");
        if n_connections > config.http_config.client_conn_threshold {
            tracing::trace!("attempting to cancel a random connection");
            if let Some(token) = config.http_config.cancel_set.take() {
                tracing::debug!("cancelling a random connection");
                token.cancel();
            }
        }

        let conn_token = cancellation_token.child_token();
        let tls_acceptor = tls_acceptor.clone();
        let backend = backend.clone();
        let connections2 = connections.clone();
        let cancellation_handler = cancellation_handler.clone();
        let endpoint_rate_limiter = endpoint_rate_limiter.clone();
        connections.spawn(
            async move {
                let conn_token2 = conn_token.clone();
                let _cancel_guard = config.http_config.cancel_set.insert(conn_id, conn_token2);

                let session_id = uuid::Uuid::new_v4();

                let _gauge = Metrics::get()
                    .proxy
                    .client_connections
                    .guard(crate::metrics::Protocol::Http);

                let startup_result = Box::pin(connection_startup(
                    config,
                    tls_acceptor,
                    session_id,
                    conn,
                    peer_addr,
                ))
                .await;
                let Some((conn, peer_addr)) = startup_result else {
                    return;
                };

                Box::pin(connection_handler(
                    config,
                    backend,
                    connections2,
                    cancellation_handler,
                    endpoint_rate_limiter,
                    conn_token,
                    conn,
                    peer_addr,
                    session_id,
                ))
                .await;
            }
            .instrument(http_conn_span),
        );
    }

    connections.wait().await;

    Ok(())
}

pub(crate) trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + 'static> AsyncReadWrite for T {}
pub(crate) type AsyncRW = Pin<Box<dyn AsyncReadWrite>>;

#[async_trait]
trait MaybeTlsAcceptor: Send + Sync + 'static {
    async fn accept(self: Arc<Self>, conn: ChainRW<TcpStream>) -> std::io::Result<AsyncRW>;
}

#[async_trait]
impl MaybeTlsAcceptor for rustls::ServerConfig {
    async fn accept(self: Arc<Self>, conn: ChainRW<TcpStream>) -> std::io::Result<AsyncRW> {
        Ok(Box::pin(TlsAcceptor::from(self).accept(conn).await?))
    }
}

struct NoTls;

#[async_trait]
impl MaybeTlsAcceptor for NoTls {
    async fn accept(self: Arc<Self>, conn: ChainRW<TcpStream>) -> std::io::Result<AsyncRW> {
        Ok(Box::pin(conn))
    }
}

/// Handles the TCP startup lifecycle.
/// 1. Parses PROXY protocol V2
/// 2. Handles TLS handshake
async fn connection_startup(
    config: &ProxyConfig,
    tls_acceptor: Arc<dyn MaybeTlsAcceptor>,
    session_id: uuid::Uuid,
    conn: TcpStream,
    peer_addr: SocketAddr,
) -> Option<(AsyncRW, IpAddr)> {
    // handle PROXY protocol
    let (conn, peer) = match read_proxy_protocol(conn).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(?session_id, %peer_addr, "failed to accept TCP connection: invalid PROXY protocol V2 header: {e:#}");
            return None;
        }
    };

    let peer_addr = peer.unwrap_or(peer_addr).ip();
    let has_private_peer_addr = match peer_addr {
        IpAddr::V4(ip) => ip.is_private(),
        IpAddr::V6(_) => false,
    };
    info!(?session_id, %peer_addr, "accepted new TCP connection");

    // try upgrade to TLS, but with a timeout.
    let conn = match timeout(config.handshake_timeout, tls_acceptor.accept(conn)).await {
        Ok(Ok(conn)) => {
            info!(?session_id, %peer_addr, "accepted new TLS connection");
            conn
        }
        // The handshake failed
        Ok(Err(e)) => {
            if !has_private_peer_addr {
                Metrics::get().proxy.tls_handshake_failures.inc();
            }
            warn!(?session_id, %peer_addr, "failed to accept TLS connection: {e:?}");
            return None;
        }
        // The handshake timed out
        Err(e) => {
            if !has_private_peer_addr {
                Metrics::get().proxy.tls_handshake_failures.inc();
            }
            warn!(?session_id, %peer_addr, "failed to accept TLS connection: {e:?}");
            return None;
        }
    };

    Some((conn, peer_addr))
}

/// Handles HTTP connection
/// 1. With graceful shutdowns
/// 2. With graceful request cancellation with connection failure
/// 3. With websocket upgrade support.
#[allow(clippy::too_many_arguments)]
async fn connection_handler(
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandlerMain>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    cancellation_token: CancellationToken,
    conn: AsyncRW,
    peer_addr: IpAddr,
    session_id: uuid::Uuid,
) {
    let session_id = AtomicTake::new(session_id);

    // Cancel all current inflight HTTP requests if the HTTP connection is closed.
    let http_cancellation_token = CancellationToken::new();
    let _cancel_connection = http_cancellation_token.clone().drop_guard();

    let server = Builder::new(TokioExecutor::new());
    let conn = server.serve_connection_with_upgrades(
        hyper_util::rt::TokioIo::new(conn),
        hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
            // First HTTP request shares the same session ID
            let session_id = session_id.take().unwrap_or_else(uuid::Uuid::new_v4);

            // Cancel the current inflight HTTP request if the requets stream is closed.
            // This is slightly different to `_cancel_connection` in that
            // h2 can cancel individual requests with a `RST_STREAM`.
            let http_request_token = http_cancellation_token.child_token();
            let cancel_request = http_request_token.clone().drop_guard();

            // `request_handler` is not cancel safe. It expects to be cancelled only at specific times.
            // By spawning the future, we ensure it never gets cancelled until it decides to.
            let handler = connections.spawn(
                request_handler(
                    req,
                    config,
                    backend.clone(),
                    connections.clone(),
                    cancellation_handler.clone(),
                    session_id,
                    peer_addr,
                    http_request_token,
                    endpoint_rate_limiter.clone(),
                )
                .in_current_span()
                .map_ok_or_else(api_error_into_response, |r| r),
            );
            async move {
                let res = handler.await;
                cancel_request.disarm();
                res
            }
        }),
    );

    // On cancellation, trigger the HTTP connection handler to shut down.
    let res = match select(pin!(cancellation_token.cancelled()), pin!(conn)).await {
        Either::Left((_cancelled, mut conn)) => {
            tracing::debug!(%peer_addr, "cancelling connection");
            conn.as_mut().graceful_shutdown();
            conn.await
        }
        Either::Right((res, _)) => res,
    };

    match res {
        Ok(()) => tracing::info!(%peer_addr, "HTTP connection closed"),
        Err(e) => tracing::warn!(%peer_addr, "HTTP connection error {e}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn request_handler(
    mut request: hyper::Request<Incoming>,
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    ws_connections: TaskTracker,
    cancellation_handler: Arc<CancellationHandlerMain>,
    session_id: uuid::Uuid,
    peer_addr: IpAddr,
    // used to cancel in-flight HTTP requests. not used to cancel websockets
    http_cancellation_token: CancellationToken,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, ApiError> {
    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next())
        .map(|s| s.to_string());

    // Check if the request is a websocket upgrade request.
    if config.http_config.accept_websockets
        && framed_websockets::upgrade::is_upgrade_request(&request)
    {
        let ctx = RequestMonitoring::new(
            session_id,
            peer_addr,
            crate::metrics::Protocol::Ws,
            &config.region,
        );

        let span = ctx.span();
        info!(parent: &span, "performing websocket upgrade");

        let (response, websocket) = framed_websockets::upgrade::upgrade(&mut request)
            .map_err(|e| ApiError::BadRequest(e.into()))?;

        ws_connections.spawn(
            async move {
                if let Err(e) = websocket::serve_websocket(
                    config,
                    backend.auth_backend,
                    ctx,
                    websocket,
                    cancellation_handler,
                    endpoint_rate_limiter,
                    host,
                )
                .await
                {
                    warn!("error in websocket connection: {e:#}");
                }
            }
            .instrument(span),
        );

        // Return the response so the spawned future can continue.
        Ok(response.map(|b| b.map_err(|x| match x {}).boxed()))
    } else if request.uri().path() == "/sql" && *request.method() == Method::POST {
        let ctx = RequestMonitoring::new(
            session_id,
            peer_addr,
            crate::metrics::Protocol::Http,
            &config.region,
        );
        let span = ctx.span();

        sql_over_http::handle(config, ctx, request, backend, http_cancellation_token)
            .instrument(span)
            .await
    } else if request.uri().path() == "/sql" && *request.method() == Method::OPTIONS {
        Response::builder()
            .header("Allow", "OPTIONS, POST")
            .header("Access-Control-Allow-Origin", "*")
            .header(
                "Access-Control-Allow-Headers",
                "Authorization, Neon-Connection-String, Neon-Raw-Text-Output, Neon-Array-Mode, Neon-Pool-Opt-In, Neon-Batch-Read-Only, Neon-Batch-Isolation-Level",
            )
            .header("Access-Control-Max-Age", "86400" /* 24 hours */)
            .status(StatusCode::OK) // 204 is also valid, but see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS#status_code
            .body(Empty::new().map_err(|x| match x {}).boxed())
            .map_err(|e| ApiError::InternalServerError(e.into()))
    } else {
        json_response(StatusCode::BAD_REQUEST, "query is not supported")
    }
}
