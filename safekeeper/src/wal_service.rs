//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::{Context, Result};
use postgres_backend::QueryError;
use safekeeper_api::models::ConnectionId;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_io_timeout::TimeoutReader;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::{auth::Scope, measured_stream::MeasuredStream};

use crate::metrics::TrafficMetrics;
use crate::SafeKeeperConf;
use crate::{handler::SafekeeperPostgresHandler, GlobalTimelines};
use postgres_backend::{AuthType, PostgresBackend};

/// Accept incoming TCP connections and spawn them into a background thread.
///
/// allowed_auth_scope is either SafekeeperData (wide JWT tokens giving access
/// to any tenant are allowed) or Tenant (only tokens giving access to specific
/// tenant are allowed). Doesn't matter if auth is disabled in conf.
pub async fn task_main(
    conf: Arc<SafeKeeperConf>,
    pg_listener: std::net::TcpListener,
    allowed_auth_scope: Scope,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    // Tokio's from_std won't do this for us, per its comment.
    pg_listener.set_nonblocking(true)?;

    let listener = tokio::net::TcpListener::from_std(pg_listener)?;
    let mut connection_count: ConnectionCount = 0;

    loop {
        let (socket, peer_addr) = listener.accept().await.context("accept")?;
        debug!("accepted connection from {}", peer_addr);
        let conf = conf.clone();
        let conn_id = issue_connection_id(&mut connection_count);
        let global_timelines = global_timelines.clone();
        tokio::spawn(
            async move {
                if let Err(err) = handle_socket(socket, conf, conn_id, allowed_auth_scope, global_timelines).await {
                    error!("connection handler exited: {}", err);
                }
            }
            .instrument(info_span!("", cid = %conn_id, ttid = field::Empty, application_name = field::Empty, shard = field::Empty)),
        );
    }
}

/// This is run by `task_main` above, inside a background thread.
///
async fn handle_socket(
    socket: TcpStream,
    conf: Arc<SafeKeeperConf>,
    conn_id: ConnectionId,
    allowed_auth_scope: Scope,
    global_timelines: Arc<GlobalTimelines>,
) -> Result<(), QueryError> {
    socket.set_nodelay(true)?;
    let peer_addr = socket.peer_addr()?;

    // Set timeout on reading from the socket. It prevents hanged up connection
    // if client suddenly disappears. Note that TCP_KEEPALIVE is not enabled by
    // default, and tokio doesn't provide ability to set it out of the box.
    let mut socket = TimeoutReader::new(socket);
    let wal_service_timeout = Duration::from_secs(60 * 10);
    socket.set_timeout(Some(wal_service_timeout));
    // pin! is here because TimeoutReader (due to storing sleep future inside)
    // is not Unpin, and all pgbackend/framed/tokio dependencies require stream
    // to be Unpin. Which is reasonable, as indeed something like TimeoutReader
    // shouldn't be moved.
    let socket = std::pin::pin!(socket);

    let traffic_metrics = TrafficMetrics::new();
    if let Some(current_az) = conf.availability_zone.as_deref() {
        traffic_metrics.set_sk_az(current_az);
    }

    let socket = MeasuredStream::new(
        socket,
        |cnt| {
            traffic_metrics.observe_read(cnt);
        },
        |cnt| {
            traffic_metrics.observe_write(cnt);
        },
    );

    let auth_key = match allowed_auth_scope {
        Scope::Tenant => conf.pg_tenant_only_auth.clone(),
        _ => conf.pg_auth.clone(),
    };
    let auth_type = match auth_key {
        None => AuthType::Trust,
        Some(_) => AuthType::NeonJWT,
    };
    let auth_pair = auth_key.map(|key| (allowed_auth_scope, key));
    let mut conn_handler = SafekeeperPostgresHandler::new(
        conf,
        conn_id,
        Some(traffic_metrics.clone()),
        auth_pair,
        global_timelines,
    );
    let pgbackend = PostgresBackend::new_from_io(socket, peer_addr, auth_type, None)?;
    // libpq protocol between safekeeper and walproposer / pageserver
    // We don't use shutdown.
    pgbackend
        .run(&mut conn_handler, &CancellationToken::new())
        .await
}

pub type ConnectionCount = u32;

pub fn issue_connection_id(count: &mut ConnectionCount) -> ConnectionId {
    *count = count.wrapping_add(1);
    *count
}
