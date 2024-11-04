//! Routers for our serverless APIs
//!
//! Handles both SQL over HTTP and SQL over Websockets.

mod backend;
pub mod cancel_set;
mod conn_pool;
mod conn_pool_lib;
mod error;
mod http_conn_pool;
mod http_util;
mod json;
mod local_conn_pool;
mod sql_over_http;
mod websocket;

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::Poll;

use anyhow::Context;
use async_trait::async_trait;
use atomic_take::AtomicTake;
use bytes::Bytes;
pub use conn_pool_lib::GlobalConnPoolOptions;
use futures::future::{select, Either};
use futures::FutureExt;
use http::{Method, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rand::rngs::StdRng;
use rand::SeedableRng;
use smallvec::SmallVec;
use sql_over_http::{uuid_to_header_value, NEON_REQUEST_ID};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tracing::{debug, info, warn, Instrument};
use utils::http::error::ApiError;

use crate::cancellation::CancellationHandlerMain;
use crate::config::ProxyConfig;
use crate::context::RequestMonitoring;
use crate::metrics::Metrics;
use crate::protocol2::{read_proxy_protocol, ChainRW};
use crate::proxy::run_until_cancelled;
use crate::rate_limiter::EndpointRateLimiter;
use crate::serverless::backend::PoolingBackend;
use crate::serverless::http_util::{api_error_into_response, json_response};

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
    let conn_pool = conn_pool_lib::GlobalConnPool::new(&config.http_config);
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

        let conn_cancellation_token = cancellation_token.child_token();
        let tls_acceptor = tls_acceptor.clone();
        let backend = backend.clone();
        let cancellation_handler = cancellation_handler.clone();
        let endpoint_rate_limiter = endpoint_rate_limiter.clone();
        let connection_token = connections.token();
        tokio::spawn(
            async move {
                let _cancel_guard = config
                    .http_config
                    .cancel_set
                    .insert(conn_id, conn_cancellation_token.clone());

                let session_id = uuid::Uuid::new_v4();

                let _gauge = Metrics::get()
                    .proxy
                    .client_connections
                    .guard(crate::metrics::Protocol::Http);

                let startup_result =
                    connection_startup(config, tls_acceptor, session_id, conn, peer_addr)
                        .boxed()
                        .await;
                let Some(conn) = startup_result else {
                    return;
                };

                let ws_upgrade = http_connection_handler(
                    config,
                    backend,
                    conn_cancellation_token,
                    connection_token.clone(),
                    conn,
                    session_id,
                )
                .boxed()
                .await;

                if let Some((ctx, host, websocket)) = ws_upgrade {
                    let ws = websocket::serve_websocket(
                        config,
                        auth_backend,
                        ctx,
                        websocket,
                        cancellation_handler,
                        endpoint_rate_limiter,
                        host,
                    )
                    .boxed();

                    if let Err(e) = ws.await {
                        warn!("error in websocket connection: {e:#}");
                    }
                }
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
    async fn accept(self: Arc<Self>, conn: ChainRW<TcpStream>) -> std::io::Result<(AsyncRW, Alpn)>;
}

#[async_trait]
impl MaybeTlsAcceptor for rustls::ServerConfig {
    async fn accept(self: Arc<Self>, conn: ChainRW<TcpStream>) -> std::io::Result<(AsyncRW, Alpn)> {
        let conn = TlsAcceptor::from(self).accept(conn).await?;
        let alpn = conn
            .get_ref()
            .1
            .alpn_protocol()
            .map_or_else(SmallVec::new, SmallVec::from_slice);
        Ok((Box::pin(conn), alpn))
    }
}

struct NoTls;

#[async_trait]
impl MaybeTlsAcceptor for NoTls {
    async fn accept(self: Arc<Self>, conn: ChainRW<TcpStream>) -> std::io::Result<(AsyncRW, Alpn)> {
        Ok((Box::pin(conn), SmallVec::new()))
    }
}

type Alpn = SmallVec<[u8; 8]>;
type ConnWithInfo = (AsyncRW, IpAddr, Alpn);

/// Handles the TCP startup lifecycle.
/// 1. Parses PROXY protocol V2
/// 2. Handles TLS handshake
async fn connection_startup(
    config: &ProxyConfig,
    tls_acceptor: Arc<dyn MaybeTlsAcceptor>,
    session_id: uuid::Uuid,
    conn: TcpStream,
    peer_addr: SocketAddr,
) -> Option<ConnWithInfo> {
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
    let (conn, alpn) = match timeout(config.handshake_timeout, tls_acceptor.accept(conn)).await {
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

    Some((conn, peer_addr, alpn))
}

trait GracefulShutdown: Future<Output = Result<(), hyper::Error>> + Send {
    fn graceful_shutdown(self: Pin<&mut Self>);
}

impl GracefulShutdown
    for hyper::server::conn::http1::UpgradeableConnection<TokioIo<AsyncRW>, ProxyService>
{
    fn graceful_shutdown(self: Pin<&mut Self>) {
        self.graceful_shutdown();
    }
}

impl GracefulShutdown for hyper::server::conn::http1::Connection<TokioIo<AsyncRW>, ProxyService> {
    fn graceful_shutdown(self: Pin<&mut Self>) {
        self.graceful_shutdown();
    }
}

impl GracefulShutdown
    for hyper::server::conn::http2::Connection<TokioIo<AsyncRW>, ProxyService, TokioExecutor>
{
    fn graceful_shutdown(self: Pin<&mut Self>) {
        self.graceful_shutdown();
    }
}

/// Handles HTTP connection
/// 1. With graceful shutdowns
/// 2. With graceful request cancellation with connection failure
/// 3. With websocket upgrade support.
async fn http_connection_handler(
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    cancellation_token: CancellationToken,
    connection_token: TaskTrackerToken,
    conn: ConnWithInfo,
    session_id: uuid::Uuid,
) -> Option<WsUpgrade> {
    let (conn, peer_addr, alpn) = conn;
    let session_id = AtomicTake::new(session_id);

    // Cancel all current inflight HTTP requests if the HTTP connection is closed.
    let http_cancellation_token = CancellationToken::new();
    let _cancel_connection = http_cancellation_token.clone().drop_guard();

    let (ws_tx, ws_rx) = oneshot::channel();
    let ws_tx = Arc::new(AtomicTake::new(ws_tx));

    let http2 = match &*alpn {
        b"h2" => true,
        b"http/1.1" => false,
        _ => {
            debug!("no alpn negotiated");
            false
        }
    };

    let service = ProxyService {
        config,
        backend,
        connection_token,

        http_cancellation_token,
        ws_tx,
        peer_addr,
        session_id,
    };

    let io = hyper_util::rt::TokioIo::new(conn);
    let conn: Pin<Box<dyn GracefulShutdown>> = if http2 {
        service.ws_tx.take();

        Box::pin(
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, service),
        )
    } else if config.http_config.accept_websockets {
        Box::pin(
            hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .with_upgrades(),
        )
    } else {
        service.ws_tx.take();

        Box::pin(hyper::server::conn::http1::Builder::new().serve_connection(io, service))
    };

    // On cancellation, trigger the HTTP connection handler to shut down.
    let res = match select(pin!(cancellation_token.cancelled()), conn).await {
        Either::Left((_cancelled, mut conn)) => {
            tracing::debug!(%peer_addr, "cancelling connection");
            conn.as_mut().graceful_shutdown();
            conn.await
        }
        Either::Right((res, _)) => res,
    };

    match res {
        Ok(()) => {
            if let Ok(ws_upgrade) = ws_rx.await {
                tracing::info!(%peer_addr, "connection upgraded to websockets");

                return Some(ws_upgrade);
            }
            tracing::info!(%peer_addr, "HTTP connection closed");
        }
        Err(e) => tracing::warn!(%peer_addr, "HTTP connection error {e}"),
    }

    None
}

struct ProxyService {
    // global state
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,

    // connection state only
    connection_token: TaskTrackerToken,
    http_cancellation_token: CancellationToken,
    ws_tx: WsSpawner,
    peer_addr: IpAddr,
    session_id: AtomicTake<uuid::Uuid>,
}

impl hyper::service::Service<hyper::Request<Incoming>> for ProxyService {
    type Response = Response<BoxBody<Bytes, hyper::Error>>;

    type Error = tokio::task::JoinError;

    type Future = ReqFut;

    fn call(&self, req: hyper::Request<Incoming>) -> Self::Future {
        // First HTTP request shares the same session ID
        let mut session_id = self.session_id.take().unwrap_or_else(uuid::Uuid::new_v4);

        if matches!(self.backend.auth_backend, crate::auth::Backend::Local(_)) {
            // take session_id from request, if given.
            if let Some(id) = req
                .headers()
                .get(&NEON_REQUEST_ID)
                .and_then(|id| uuid::Uuid::try_parse_ascii(id.as_bytes()).ok())
            {
                session_id = id;
            }
        }

        // Cancel the current inflight HTTP request if the requets stream is closed.
        // This is slightly different to `_cancel_connection` in that
        // h2 can cancel individual requests with a `RST_STREAM`.
        let http_req_cancellation_token = self.http_cancellation_token.child_token();
        let cancel_request = Some(http_req_cancellation_token.clone().drop_guard());

        let handle = request_handler(
            req,
            self.config,
            self.backend.clone(),
            self.ws_tx.clone(),
            session_id,
            self.peer_addr,
            http_req_cancellation_token,
            &self.connection_token,
        );

        ReqFut {
            session_id,
            cancel_request,
            handle,
        }
    }
}

struct ReqFut {
    session_id: uuid::Uuid,
    cancel_request: Option<tokio_util::sync::DropGuard>,
    handle: HandleOrResponse,
}

enum HandleOrResponse {
    Handle(JoinHandle<Response<BoxBody<Bytes, hyper::Error>>>),
    Response(Option<Response<BoxBody<Bytes, hyper::Error>>>),
}

impl Future for ReqFut {
    type Output = Result<Response<BoxBody<Bytes, hyper::Error>>, tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut res = match &mut self.handle {
            HandleOrResponse::Handle(join_handle) => std::task::ready!(join_handle.poll_unpin(cx)),
            HandleOrResponse::Response(response) => {
                Ok(response.take().expect("polled after completion"))
            }
        };

        self.cancel_request
            .take()
            .map(tokio_util::sync::DropGuard::disarm);

        // add the session ID to the response
        if let Ok(resp) = &mut res {
            resp.headers_mut()
                .append(&NEON_REQUEST_ID, uuid_to_header_value(self.session_id));
        }

        Poll::Ready(res)
    }
}

type WsUpgrade = (RequestMonitoring, Option<String>, OnUpgrade);
type WsSpawner = Arc<AtomicTake<oneshot::Sender<WsUpgrade>>>;

#[allow(clippy::too_many_arguments)]
fn request_handler(
    mut request: hyper::Request<Incoming>,
    config: &'static ProxyConfig,
    backend: Arc<PoolingBackend>,
    ws_spawner: WsSpawner,
    session_id: uuid::Uuid,
    peer_addr: IpAddr,
    // used to cancel in-flight HTTP requests. not used to cancel websockets
    http_cancellation_token: CancellationToken,
    connection_token: &TaskTrackerToken,
) -> HandleOrResponse {
    // Check if the request is a websocket upgrade request.
    if framed_websockets::upgrade::is_upgrade_request(&request) {
        let Some(spawner) = ws_spawner.take() else {
            return HandleOrResponse::Response(Some(
                json_response(StatusCode::BAD_REQUEST, "query is not supported")
                    .unwrap_or_else(api_error_into_response),
            ));
        };

        let (response, websocket) = match framed_websockets::upgrade::upgrade(&mut request) {
            Err(e) => {
                return HandleOrResponse::Response(Some(api_error_into_response(
                    ApiError::BadRequest(e.into()),
                )))
            }
            Ok(upgrade) => upgrade,
        };

        let host = request
            .headers()
            .get("host")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.split(':').next())
            .map(|s| s.to_string());

        let ctx = RequestMonitoring::new(
            session_id,
            peer_addr,
            crate::metrics::Protocol::Ws,
            &config.region,
        );

        if let Err(_e) = spawner.send((ctx, host, websocket)) {
            return HandleOrResponse::Response(Some(api_error_into_response(
                ApiError::InternalServerError(anyhow::anyhow!("could not upgrade WS connection")),
            )));
        }

        // Return the response so the spawned future can continue.
        HandleOrResponse::Response(Some(response.map(|b| b.map_err(|x| match x {}).boxed())))
    } else if request.uri().path() == "/sql" && *request.method() == Method::POST {
        let ctx = RequestMonitoring::new(
            session_id,
            peer_addr,
            crate::metrics::Protocol::Http,
            &config.region,
        );
        let span = ctx.span();

        let token = connection_token.clone();

        // `sql_over_http::handle` is not cancel safe. It expects to be cancelled only at specific times.
        // By spawning the future, we ensure it never gets cancelled until it decides to.
        HandleOrResponse::Handle(tokio::spawn(async move {
            let _token = token;
            sql_over_http::handle(config, ctx, request, backend, http_cancellation_token)
                .instrument(span)
                .await
                .unwrap_or_else(api_error_into_response)
        }))
    } else if request.uri().path() == "/sql" && *request.method() == Method::OPTIONS {
        HandleOrResponse::Response(Some( Response::builder()
            .header("Allow", "OPTIONS, POST")
            .header("Access-Control-Allow-Origin", "*")
            .header(
                "Access-Control-Allow-Headers",
                "Authorization, Neon-Connection-String, Neon-Raw-Text-Output, Neon-Array-Mode, Neon-Pool-Opt-In, Neon-Batch-Read-Only, Neon-Batch-Isolation-Level",
            )
            .header("Access-Control-Max-Age", "86400" /* 24 hours */)
            .status(StatusCode::OK) // 204 is also valid, but see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS#status_code
            .body(Empty::new().map_err(|x| match x {}).boxed())
            .map_err(|e| ApiError::InternalServerError(e.into())).unwrap_or_else(api_error_into_response)))
    } else {
        HandleOrResponse::Response(Some(
            json_response(StatusCode::BAD_REQUEST, "query is not supported")
                .unwrap_or_else(api_error_into_response),
        ))
    }
}
