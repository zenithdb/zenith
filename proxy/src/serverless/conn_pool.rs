use std::fmt;
use std::pin::pin;
use std::sync::{Arc, Weak};
use std::task::{ready, Poll};

use futures::future::poll_fn;
use futures::Future;
use postgres_client::tls::NoTlsStream;
use postgres_client::AsyncMessage;
use smallvec::SmallVec;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
#[cfg(test)]
use {
    super::conn_pool_lib::GlobalConnPoolOptions,
    crate::auth::backend::ComputeUserInfo,
    std::{sync::atomic, time::Duration},
};

use super::conn_pool_lib::{
    Client, ClientDataEnum, ClientInnerCommon, ClientInnerExt, ConnInfo, EndpointConnPool,
    GlobalConnPool,
};
use crate::context::RequestContext;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::Metrics;

#[derive(Debug, Clone)]
pub(crate) struct ConnInfoWithAuth {
    pub(crate) conn_info: ConnInfo,
    pub(crate) auth: AuthData,
}

#[derive(Debug, Clone)]
pub(crate) enum AuthData {
    Password(SmallVec<[u8; 16]>),
    Jwt(String),
}

impl fmt::Display for ConnInfo {
    // use custom display to avoid logging password
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}/{}?{}",
            self.user_info.user,
            self.user_info.endpoint,
            self.dbname,
            self.user_info.options.get_cache_key("")
        )
    }
}

pub(crate) fn poll_client<C: ClientInnerExt>(
    global_pool: Arc<GlobalConnPool<C, EndpointConnPool<C>>>,
    ctx: &RequestContext,
    conn_info: ConnInfo,
    client: C,
    mut connection: postgres_client::Connection<TcpStream, NoTlsStream>,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<C> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol());
    let mut session_id = ctx.session_id();
    let (tx, mut rx) = tokio::sync::watch::channel(session_id);

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info();
    span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, %session_id, "new connection");
    });
    let pool = match conn_info.endpoint_cache_key() {
        Some(endpoint) => Arc::downgrade(&global_pool.get_or_create_endpoint_pool(&endpoint)),
        None => Weak::new(),
    };
    let pool_clone = pool.clone();

    let db_user = conn_info.db_and_user();
    let idle = global_pool.get_idle_timeout();
    let cancel = CancellationToken::new();
    let cancelled = cancel.clone().cancelled_owned();

    tokio::spawn(
    async move {
        let _conn_gauge = conn_gauge;
        let mut idle_timeout = pin!(tokio::time::sleep(idle));
        let mut cancelled = pin!(cancelled);

        poll_fn(move |cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                info!("connection dropped");
                return Poll::Ready(())
            }

            match rx.has_changed() {
                Ok(true) => {
                    session_id = *rx.borrow_and_update();
                    info!(%session_id, "changed session");
                    idle_timeout.as_mut().reset(Instant::now() + idle);
                }
                Err(_) => {
                    info!("connection dropped");
                    return Poll::Ready(())
                }
                _ => {}
            }

            // 5 minute idle connection timeout
            if idle_timeout.as_mut().poll(cx).is_ready() {
                idle_timeout.as_mut().reset(Instant::now() + idle);
                info!("connection idle");
                if let Some(pool) = pool.clone().upgrade() {
                    // remove client from pool - should close the connection if it's idle.
                    // does nothing if the client is currently checked-out and in-use
                    if pool.write().remove_client(db_user.clone(), conn_id) {
                        info!("idle connection removed");
                    }
                }
            }

            loop {
                let message = ready!(connection.poll_message(cx));

                match message {
                    Some(Ok(AsyncMessage::Notice(notice))) => {
                        info!(%session_id, "notice: {}", notice);
                    }
                    Some(Ok(AsyncMessage::Notification(notif))) => {
                        warn!(%session_id, pid = notif.process_id(), channel = notif.channel(), "notification received");
                    }
                    Some(Ok(_)) => {
                        warn!(%session_id, "unknown message");
                    }
                    Some(Err(e)) => {
                        error!(%session_id, "connection error: {}", e);
                        break
                    }
                    None => {
                        info!("connection closed");
                        break
                    }
                }
            }

            // remove from connection pool
            if let Some(pool) = pool.clone().upgrade() {
                if pool.write().remove_client(db_user.clone(), conn_id) {
                    info!("closed connection removed");
                }
            }

            Poll::Ready(())
        }).await;

    }
    .instrument(span));
    let inner = ClientInnerCommon {
        inner: client,
        aux,
        conn_id,
        data: ClientDataEnum::Remote(ClientDataRemote {
            session: tx,
            cancel,
        }),
    };

    Client::new(inner, conn_info, pool_clone)
}

#[derive(Clone)]
pub(crate) struct ClientDataRemote {
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    cancel: CancellationToken,
}

impl ClientDataRemote {
    pub fn session(&mut self) -> &mut tokio::sync::watch::Sender<uuid::Uuid> {
        &mut self.session
    }

    pub fn cancel(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::proxy::NeonOptions;
    use crate::serverless::cancel_set::CancelSet;
    use crate::types::{BranchId, EndpointId, ProjectId};

    struct MockClient(Arc<AtomicBool>);
    impl MockClient {
        fn new(is_closed: bool) -> Self {
            MockClient(Arc::new(is_closed.into()))
        }
    }
    impl ClientInnerExt for MockClient {
        fn is_closed(&self) -> bool {
            self.0.load(atomic::Ordering::Relaxed)
        }
        fn get_process_id(&self) -> i32 {
            0
        }
    }

    fn create_inner() -> ClientInnerCommon<MockClient> {
        create_inner_with(MockClient::new(false))
    }

    fn create_inner_with(client: MockClient) -> ClientInnerCommon<MockClient> {
        ClientInnerCommon {
            inner: client,
            aux: MetricsAuxInfo {
                endpoint_id: (&EndpointId::from("endpoint")).into(),
                project_id: (&ProjectId::from("project")).into(),
                branch_id: (&BranchId::from("branch")).into(),
                cold_start_info: crate::control_plane::messages::ColdStartInfo::Warm,
            },
            conn_id: uuid::Uuid::new_v4(),
            data: ClientDataEnum::Remote(ClientDataRemote {
                session: tokio::sync::watch::Sender::new(uuid::Uuid::new_v4()),
                cancel: CancellationToken::new(),
            }),
        }
    }

    #[tokio::test]
    async fn test_pool() {
        let _ = env_logger::try_init();
        let config = Box::leak(Box::new(crate::config::HttpConfig {
            accept_websockets: false,
            pool_options: GlobalConnPoolOptions {
                max_conns_per_endpoint: 2,
                gc_epoch: Duration::from_secs(1),
                pool_shards: 2,
                idle_timeout: Duration::from_secs(1),
                opt_in: false,
                max_total_conns: 3,
            },
            cancel_set: CancelSet::new(0),
            client_conn_threshold: u64::MAX,
            max_request_size_bytes: usize::MAX,
            max_response_size_bytes: usize::MAX,
        }));
        let pool = GlobalConnPool::new(config);
        let conn_info = ConnInfo {
            user_info: ComputeUserInfo {
                user: "user".into(),
                endpoint: "endpoint".into(),
                options: NeonOptions::default(),
            },
            dbname: "dbname".into(),
        };
        let ep_pool = Arc::downgrade(
            &pool.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key().unwrap()),
        );
        {
            let mut client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            assert_eq!(0, pool.get_global_connections_count());
            client.inner().1.discard();
            // Discard should not add the connection from the pool.
            assert_eq!(0, pool.get_global_connections_count());
        }
        {
            let client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            drop(client);
            assert_eq!(1, pool.get_global_connections_count());
        }
        {
            let closed_client = Client::new(
                create_inner_with(MockClient::new(true)),
                conn_info.clone(),
                ep_pool.clone(),
            );
            drop(closed_client);
            assert_eq!(1, pool.get_global_connections_count());
        }
        let is_closed: Arc<AtomicBool> = Arc::new(false.into());
        {
            let client = Client::new(
                create_inner_with(MockClient(is_closed.clone())),
                conn_info.clone(),
                ep_pool.clone(),
            );
            drop(client);
            // The client should be added to the pool.
            assert_eq!(2, pool.get_global_connections_count());
        }
        {
            let client = Client::new(create_inner(), conn_info, ep_pool);
            drop(client);

            // The client shouldn't be added to the pool. Because the ep-pool is full.
            assert_eq!(2, pool.get_global_connections_count());
        }

        let conn_info = ConnInfo {
            user_info: ComputeUserInfo {
                user: "user".into(),
                endpoint: "endpoint-2".into(),
                options: NeonOptions::default(),
            },
            dbname: "dbname".into(),
        };
        let ep_pool = Arc::downgrade(
            &pool.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key().unwrap()),
        );
        {
            let client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            drop(client);
            assert_eq!(3, pool.get_global_connections_count());
        }
        {
            let client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            drop(client);

            // The client shouldn't be added to the pool. Because the global pool is full.
            assert_eq!(3, pool.get_global_connections_count());
        }

        is_closed.store(true, atomic::Ordering::Relaxed);
        // Do gc for all shards.
        pool.gc(0);
        pool.gc(1);
        // Closed client should be removed from the pool.
        assert_eq!(2, pool.get_global_connections_count());
    }
}
