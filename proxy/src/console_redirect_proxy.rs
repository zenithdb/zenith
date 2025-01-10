use std::sync::Arc;

use futures::{FutureExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, Instrument};

use crate::auth::backend::ConsoleRedirectBackend;
use crate::cancellation::{CancellationHandlerMain, CancellationHandlerMainInternal};
use crate::config::{ProxyConfig, ProxyProtocolV2};
use crate::context::RequestContext;
use crate::error::ReportableError;
use crate::metrics::{Metrics, NumClientConnectionsGuard};
use crate::protocol2::{read_proxy_protocol, ConnectHeader, ConnectionInfo};
use crate::proxy::connect_compute::{connect_to_compute, TcpMechanism};
use crate::proxy::handshake::{handshake, HandshakeData};
use crate::proxy::passthrough::ProxyPassthrough;
use crate::proxy::{
    prepare_client_connection, run_until_cancelled, ClientRequestError, ErrorSource,
};

pub async fn task_main(
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandlerMain>,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    let cancellations = tokio_util::task::task_tracker::TaskTracker::new();

    while let Some(accept_result) =
        run_until_cancelled(listener.accept(), &cancellation_token).await
    {
        let (socket, peer_addr) = accept_result?;

        let conn_gauge = Metrics::get()
            .proxy
            .client_connections
            .guard(crate::metrics::Protocol::Tcp);

        let session_id = uuid::Uuid::new_v4();
        let cancellation_handler = Arc::clone(&cancellation_handler);
        let cancellations = cancellations.clone();

        debug!(protocol = "tcp", %session_id, "accepted new TCP connection");

        connections.spawn(async move {
            let (socket, peer_addr) = match read_proxy_protocol(socket).await {
                Err(e) => {
                    error!("per-client task finished with an error: {e:#}");
                    return;
                }
                // our load balancers will not send any more data. let's just exit immediately
                Ok((_socket, ConnectHeader::Local)) => {
                    debug!("healthcheck received");
                    return;
                }
                Ok((_socket, ConnectHeader::Missing)) if config.proxy_protocol_v2 == ProxyProtocolV2::Required => {
                    error!("missing required proxy protocol header");
                    return;
                }
                Ok((_socket, ConnectHeader::Proxy(_))) if config.proxy_protocol_v2 == ProxyProtocolV2::Rejected => {
                    error!("proxy protocol header not supported");
                    return;
                }
                Ok((socket, ConnectHeader::Proxy(info))) => (socket, info),
                Ok((socket, ConnectHeader::Missing)) => (socket, ConnectionInfo{ addr: peer_addr, extra: None }),
            };

            match socket.inner.set_nodelay(true) {
                Ok(()) => {}
                Err(e) => {
                    error!("per-client task finished with an error: failed to set socket option: {e:#}");
                    return;
                }
            };

            let ctx = RequestContext::new(
                session_id,
                peer_addr,
                crate::metrics::Protocol::Tcp,
                &config.region,
            );

            let res = handle_client(
                config,
                backend,
                &ctx,
                cancellation_handler,
                socket,
                conn_gauge,
                cancellations,
            )
            .instrument(ctx.span())
            .boxed()
            .await;

            match res {
                Err(e) => {
                    ctx.set_error_kind(e.get_error_kind());
                    error!(parent: &ctx.span(), "per-client task finished with an error: {e:#}");
                }
                Ok(None) => {
                    ctx.set_success();
                }
                Ok(Some(p)) => {
                    ctx.set_success();
                    let _disconnect = ctx.log_connect();
                    match p.proxy_pass(&config.connect_to_compute).await {
                        Ok(()) => {}
                        Err(ErrorSource::Client(e)) => {
                            error!(?session_id, "per-client task finished with an IO error from the client: {e:#}");
                        }
                        Err(ErrorSource::Compute(e)) => {
                            error!(?session_id, "per-client task finished with an IO error from the compute: {e:#}");
                        }
                    }
                }
            }
        });
    }

    connections.close();
    cancellations.close();
    drop(listener);

    // Drain connections
    connections.wait().await;
    cancellations.wait().await;

    Ok(())
}

pub(crate) async fn handle_client<S: AsyncRead + AsyncWrite + Unpin>(
    config: &'static ProxyConfig,
    backend: &'static ConsoleRedirectBackend,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandlerMain>,
    stream: S,
    conn_gauge: NumClientConnectionsGuard<'static>,
    cancellations: tokio_util::task::task_tracker::TaskTracker,
) -> Result<Option<ProxyPassthrough<CancellationHandlerMainInternal, S>>, ClientRequestError> {
    debug!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let metrics = &Metrics::get().proxy;
    let proto = ctx.protocol();
    let request_gauge = metrics.connection_requests.guard(proto);

    let tls = config.tls_config.as_ref();

    let record_handshake_error = !ctx.has_private_peer_addr();
    let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Client);
    let do_handshake = handshake(ctx, stream, tls, record_handshake_error);

    let (mut stream, params) = match tokio::time::timeout(config.handshake_timeout, do_handshake)
        .await??
    {
        HandshakeData::Startup(stream, params) => (stream, params),
        HandshakeData::Cancel(cancel_key_data) => {
            // spawn a task to cancel the session, but don't wait for it
            cancellations.spawn({
                let cancellation_handler_clone = Arc::clone(&cancellation_handler);
                let ctx = ctx.clone();
                let cancel_span = tracing::span!(parent: None, tracing::Level::INFO, "cancel_session", session_id = ?ctx.session_id());
                cancel_span.follows_from(tracing::Span::current());
                async move {
                    cancellation_handler_clone
                        .cancel_session_auth(
                            cancel_key_data,
                            ctx,
                            config.authentication_config.ip_allowlist_check_enabled,
                            backend,
                        )
                        .await
                        .inspect_err(|e | debug!(error = ?e, "cancel_session failed")).ok();
                }.instrument(cancel_span)
            });

            return Ok(None);
        }
    };
    drop(pause);

    ctx.set_db_options(params.clone());

    let (node_info, user_info, ip_allowlist) = match backend
        .authenticate(ctx, &config.authentication_config, &mut stream)
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => {
            return stream.throw_error(e).await?;
        }
    };

    let mut node = connect_to_compute(
        ctx,
        &TcpMechanism {
            user_info,
            params_compat: true,
            params: &params,
            locks: &config.connect_compute_locks,
        },
        &node_info,
        config.wake_compute_retry_config,
        &config.connect_to_compute,
    )
    .or_else(|e| stream.throw_error(e))
    .await?;

    node.cancel_closure
        .set_ip_allowlist(ip_allowlist.unwrap_or_default());
    let session = cancellation_handler.get_session();
    prepare_client_connection(&node, &session, &mut stream).await?;

    // Before proxy passing, forward to compute whatever data is left in the
    // PqStream input buffer. Normally there is none, but our serverless npm
    // driver in pipeline mode sends startup, password and first query
    // immediately after opening the connection.
    let (stream, read_buf) = stream.into_inner();
    node.stream.write_all(&read_buf).await?;

    Ok(Some(ProxyPassthrough {
        client: stream,
        aux: node.aux.clone(),
        compute: node,
        session_id: ctx.session_id(),
        _req: request_gauge,
        _conn: conn_gauge,
        _cancel: session,
    }))
}
