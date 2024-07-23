use dashmap::DashMap;
use futures::{future::poll_fn, Future};
use parking_lot::RwLock;
use rand::Rng;
use smallvec::SmallVec;
use std::{collections::HashMap, pin::pin, sync::Arc, sync::Weak, time::Duration};
use std::{
    fmt,
    task::{ready, Poll},
};
use std::{
    ops::Deref,
    sync::atomic::{self, AtomicUsize},
};
use tokio::time::Instant;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};
use tokio_util::sync::CancellationToken;

use crate::console::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::{HttpEndpointPoolsGuard, Metrics};
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{
    auth::backend::ComputeUserInfo, context::RequestMonitoring, DbName, EndpointCacheKey, RoleName,
};

use tracing::{debug, error, warn, Span};
use tracing::{info, info_span, Instrument};

use super::backend::HttpConnError;

#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub user_info: ComputeUserInfo,
    pub dbname: DbName,
    pub password: SmallVec<[u8; 16]>,
}

impl ConnInfo {
    // hm, change to hasher to avoid cloning?
    pub fn db_and_user(&self) -> (DbName, RoleName) {
        (self.dbname.clone(), self.user_info.user.clone())
    }

    pub fn endpoint_cache_key(&self) -> Option<EndpointCacheKey> {
        // We don't want to cache http connections for ephemeral endpoints.
        if self.user_info.options.is_ephemeral() {
            None
        } else {
            Some(self.user_info.endpoint_cache_key())
        }
    }
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

struct ConnPoolEntry<C: ClientInnerExt> {
    conn: ClientInner<C>,
    _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub struct EndpointConnPool<C: ClientInnerExt> {
    pools: HashMap<(DbName, RoleName), DbUserConnPool<C>>,
    total_conns: usize,
    max_conns: usize,
    _guard: HttpEndpointPoolsGuard<'static>,
    global_connections_count: Arc<AtomicUsize>,
    global_pool_size_max_conns: usize,
}

impl<C: ClientInnerExt> EndpointConnPool<C> {
    fn get_conn_entry(&mut self, db_user: (DbName, RoleName)) -> Option<ConnPoolEntry<C>> {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        pools.get_mut(&db_user).and_then(|pool_entries| {
            pool_entries.get_conn_entry(total_conns, global_connections_count.clone())
        })
    }

    fn remove_client(&mut self, db_user: (DbName, RoleName), conn_id: uuid::Uuid) -> bool {
        let Self {
            pools,
            total_conns,
            global_connections_count,
            ..
        } = self;
        if let Some(pool) = pools.get_mut(&db_user) {
            let old_len = pool.conns.len();
            pool.conns.retain(|conn| conn.conn.conn_id != conn_id);
            let new_len = pool.conns.len();
            let removed = old_len - new_len;
            if removed > 0 {
                global_connections_count.fetch_sub(removed, atomic::Ordering::Relaxed);
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .dec_by(removed as i64);
            }
            *total_conns -= removed;
            removed > 0
        } else {
            false
        }
    }

    fn put(pool: &RwLock<Self>, conn_info: &ConnInfo, client: ClientInner<C>) {
        let conn_id = client.conn_id;

        if client.is_closed() {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because connection is closed");
            return;
        }
        let global_max_conn = pool.read().global_pool_size_max_conns;
        if pool
            .read()
            .global_connections_count
            .load(atomic::Ordering::Relaxed)
            >= global_max_conn
        {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full");
            return;
        }

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < pool.max_conns {
                let pool_entries = pool.pools.entry(conn_info.db_and_user()).or_default();
                pool_entries.conns.push(ConnPoolEntry {
                    conn: client,
                    _last_access: std::time::Instant::now(),
                });

                returned = true;
                per_db_size = pool_entries.conns.len();

                pool.total_conns += 1;
                pool.global_connections_count
                    .fetch_add(1, atomic::Ordering::Relaxed);
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .inc();
            }

            pool.total_conns
        };

        // do logging outside of the mutex
        if returned {
            info!(%conn_id, "pool: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!(%conn_id, "pool: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }
    }
}

impl<C: ClientInnerExt> Drop for EndpointConnPool<C> {
    fn drop(&mut self) {
        if self.total_conns > 0 {
            self.global_connections_count
                .fetch_sub(self.total_conns, atomic::Ordering::Relaxed);
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(self.total_conns as i64);
        }
    }
}

pub struct DbUserConnPool<C: ClientInnerExt> {
    conns: Vec<ConnPoolEntry<C>>,
}

impl<C: ClientInnerExt> Default for DbUserConnPool<C> {
    fn default() -> Self {
        Self { conns: Vec::new() }
    }
}

impl<C: ClientInnerExt> DbUserConnPool<C> {
    fn clear_closed_clients(&mut self, conns: &mut usize) -> usize {
        let old_len = self.conns.len();

        self.conns.retain(|conn| !conn.conn.is_closed());

        let new_len = self.conns.len();
        let removed = old_len - new_len;
        *conns -= removed;
        removed
    }

    fn get_conn_entry(
        &mut self,
        conns: &mut usize,
        global_connections_count: Arc<AtomicUsize>,
    ) -> Option<ConnPoolEntry<C>> {
        let mut removed = self.clear_closed_clients(conns);
        let conn = self.conns.pop();
        if conn.is_some() {
            *conns -= 1;
            removed += 1;
        }
        global_connections_count.fetch_sub(removed, atomic::Ordering::Relaxed);
        Metrics::get()
            .proxy
            .http_pool_opened_connections
            .get_metric()
            .dec_by(removed as i64);
        conn
    }
}

pub struct GlobalConnPool<C: ClientInnerExt> {
    // endpoint -> per-endpoint connection pool
    //
    // That should be a fairly conteded map, so return reference to the per-endpoint
    // pool as early as possible and release the lock.
    global_pool: DashMap<EndpointCacheKey, Arc<RwLock<EndpointConnPool<C>>>>,

    /// Number of endpoint-connection pools
    ///
    /// [`DashMap::len`] iterates over all inner pools and acquires a read lock on each.
    /// That seems like far too much effort, so we're using a relaxed increment counter instead.
    /// It's only used for diagnostics.
    global_pool_size: AtomicUsize,

    /// Total number of connections in the pool
    global_connections_count: Arc<AtomicUsize>,

    config: &'static crate::config::HttpConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct GlobalConnPoolOptions {
    // Maximum number of connections per one endpoint.
    // Can mix different (dbname, username) connections.
    // When running out of free slots for a particular endpoint,
    // falls back to opening a new connection for each request.
    pub max_conns_per_endpoint: usize,

    pub gc_epoch: Duration,

    pub pool_shards: usize,

    pub idle_timeout: Duration,

    pub opt_in: bool,

    // Total number of connections in the pool.
    pub max_total_conns: usize,
}

impl<C: ClientInnerExt> GlobalConnPool<C> {
    pub fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        let shards = config.pool_options.pool_shards;
        Arc::new(Self {
            global_pool: DashMap::with_shard_amount(shards),
            global_pool_size: AtomicUsize::new(0),
            config,
            global_connections_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    #[cfg(test)]
    pub fn get_global_connections_count(&self) -> usize {
        self.global_connections_count
            .load(atomic::Ordering::Relaxed)
    }

    pub fn get_idle_timeout(&self) -> Duration {
        self.config.pool_options.idle_timeout
    }

    pub fn shutdown(&self) {
        // drops all strong references to endpoint-pools
        self.global_pool.clear();
    }

    pub async fn gc_worker(&self, mut rng: impl Rng) {
        let epoch = self.config.pool_options.gc_epoch;
        let mut interval = tokio::time::interval(epoch / (self.global_pool.shards().len()) as u32);
        loop {
            interval.tick().await;

            let shard = rng.gen_range(0..self.global_pool.shards().len());
            self.gc(shard);
        }
    }

    fn gc(&self, shard: usize) {
        debug!(shard, "pool: performing epoch reclamation");

        // acquire a random shard lock
        let mut shard = self.global_pool.shards()[shard].write();

        let timer = Metrics::get()
            .proxy
            .http_pool_reclaimation_lag_seconds
            .start_timer();
        let current_len = shard.len();
        let mut clients_removed = 0;

        crate::rawtable::retain(&mut *shard, |endpoint, x| {
            // if the current endpoint pool is unique (no other strong or weak references)
            // then it is currently not in use by any connections.
            if let Some(pool) = Arc::get_mut(x.get_mut()) {
                let EndpointConnPool {
                    pools, total_conns, ..
                } = pool.get_mut();

                // ensure that closed clients are removed
                pools.iter_mut().for_each(|(_, db_pool)| {
                    clients_removed += db_pool.clear_closed_clients(total_conns);
                });

                // we only remove this pool if it has no active connections
                if *total_conns == 0 {
                    info!("pool: discarding pool for endpoint {endpoint}");
                    return false;
                }
            }

            true
        });

        let new_len = shard.len();
        drop(shard);
        timer.observe();

        // Do logging outside of the lock.
        if clients_removed > 0 {
            let size = self
                .global_connections_count
                .fetch_sub(clients_removed, atomic::Ordering::Relaxed)
                - clients_removed;
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(clients_removed as i64);
            info!("pool: performed global pool gc. removed {clients_removed} clients, total number of clients in pool is {size}");
        }
        let removed = current_len - new_len;

        if removed > 0 {
            let global_pool_size = self
                .global_pool_size
                .fetch_sub(removed, atomic::Ordering::Relaxed)
                - removed;
            info!("pool: performed global pool gc. size now {global_pool_size}");
        }
    }

    pub fn get(
        self: &Arc<Self>,
        ctx: &mut RequestMonitoring,
        conn_info: &ConnInfo,
    ) -> Result<Option<Client<C>>, HttpConnError> {
        let mut client: Option<ClientInner<C>> = None;
        let Some(endpoint) = conn_info.endpoint_cache_key() else {
            return Ok(None);
        };

        let endpoint_pool = self.get_or_create_endpoint_pool(&endpoint);
        if let Some(entry) = endpoint_pool
            .write()
            .get_conn_entry(conn_info.db_and_user())
        {
            client = Some(entry.conn)
        }
        let endpoint_pool = Arc::downgrade(&endpoint_pool);

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            if client.is_closed() {
                info!("pool: cached connection '{conn_info}' is closed, opening a new one");
                return Ok(None);
            } else {
                tracing::Span::current().record("conn_id", tracing::field::display(client.conn_id));
                tracing::Span::current().record(
                    "pid",
                    tracing::field::display(client.inner.get_process_id()),
                );
                info!(
                    cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
                    "pool: reusing connection '{conn_info}'"
                );
                client.session.send(ctx.session_id)?;
                ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
                ctx.latency_timer.success();
                return Ok(Some(Client::new(client, conn_info.clone(), endpoint_pool)));
            }
        }
        Ok(None)
    }

    fn get_or_create_endpoint_pool(
        self: &Arc<Self>,
        endpoint: &EndpointCacheKey,
    ) -> Arc<RwLock<EndpointConnPool<C>>> {
        // fast path
        if let Some(pool) = self.global_pool.get(endpoint) {
            return pool.clone();
        }

        // slow path
        let new_pool = Arc::new(RwLock::new(EndpointConnPool {
            pools: HashMap::new(),
            total_conns: 0,
            max_conns: self.config.pool_options.max_conns_per_endpoint,
            _guard: Metrics::get().proxy.http_endpoint_pools.guard(),
            global_connections_count: self.global_connections_count.clone(),
            global_pool_size_max_conns: self.config.pool_options.max_total_conns,
        }));

        // find or create a pool for this endpoint
        let mut created = false;
        let pool = self
            .global_pool
            .entry(endpoint.clone())
            .or_insert_with(|| {
                created = true;
                new_pool
            })
            .clone();

        // log new global pool size
        if created {
            let global_pool_size = self
                .global_pool_size
                .fetch_add(1, atomic::Ordering::Relaxed)
                + 1;
            info!(
                "pool: created new pool for '{endpoint}', global pool size now {global_pool_size}"
            );
        }

        pool
    }
}

pub fn poll_client<C: ClientInnerExt>(
    global_pool: Arc<GlobalConnPool<C>>,
    ctx: &mut RequestMonitoring,
    conn_info: ConnInfo,
    client: C,
    mut connection: tokio_postgres::Connection<Socket, NoTlsStream>,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> Client<C> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol);
    let mut session_id = ctx.session_id;
    let (tx, mut rx) = tokio::sync::watch::channel(session_id);

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info;
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
    let inner = ClientInner {
        inner: client,
        session: tx,
        cancel,
        aux,
        conn_id,
    };
    Client::new(inner, conn_info, pool_clone)
}

struct ClientInner<C: ClientInnerExt> {
    inner: C,
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    cancel: CancellationToken,
    aux: MetricsAuxInfo,
    conn_id: uuid::Uuid,
}

impl<C: ClientInnerExt> Drop for ClientInner<C> {
    fn drop(&mut self) {
        // on client drop, tell the conn to shut down
        self.cancel.cancel();
    }
}

pub trait ClientInnerExt: Sync + Send + 'static {
    fn is_closed(&self) -> bool;
    fn get_process_id(&self) -> i32;
}

impl ClientInnerExt for tokio_postgres::Client {
    fn is_closed(&self) -> bool {
        self.is_closed()
    }
    fn get_process_id(&self) -> i32 {
        self.get_process_id()
    }
}

impl<C: ClientInnerExt> ClientInner<C> {
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl<C: ClientInnerExt> Client<C> {
    pub fn metrics(&self) -> Arc<MetricCounter> {
        let aux = &self.inner.as_ref().unwrap().aux;
        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id,
            branch_id: aux.branch_id,
        })
    }
}

pub struct Client<C: ClientInnerExt> {
    span: Span,
    inner: Option<ClientInner<C>>,
    conn_info: ConnInfo,
    pool: Weak<RwLock<EndpointConnPool<C>>>,
}

pub struct Discard<'a, C: ClientInnerExt> {
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<RwLock<EndpointConnPool<C>>>,
}

impl<C: ClientInnerExt> Client<C> {
    pub(self) fn new(
        inner: ClientInner<C>,
        conn_info: ConnInfo,
        pool: Weak<RwLock<EndpointConnPool<C>>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            span: Span::current(),
            conn_info,
            pool,
        }
    }
    pub fn inner(&mut self) -> (&mut C, Discard<'_, C>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner = inner.as_mut().expect("client inner should not be removed");
        (&mut inner.inner, Discard { pool, conn_info })
    }
}

impl<C: ClientInnerExt> Discard<'_, C> {
    pub fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_info = &self.conn_info;
        if status != ReadyForQueryStatus::Idle && std::mem::take(self.pool).strong_count() > 0 {
            info!("pool: throwing away connection '{conn_info}' because connection is not idle")
        }
    }
    pub fn discard(&mut self) {
        let conn_info = &self.conn_info;
        if std::mem::take(self.pool).strong_count() > 0 {
            info!("pool: throwing away connection '{conn_info}' because connection is potentially in a broken state")
        }
    }
}

impl<C: ClientInnerExt> Deref for Client<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self
            .inner
            .as_ref()
            .expect("client inner should not be removed")
            .inner
    }
}

impl<C: ClientInnerExt> Client<C> {
    fn do_drop(&mut self) -> Option<impl FnOnce()> {
        let conn_info = self.conn_info.clone();
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");
        if let Some(conn_pool) = std::mem::take(&mut self.pool).upgrade() {
            let current_span = self.span.clone();
            // return connection to the pool
            return Some(move || {
                let _span = current_span.enter();
                EndpointConnPool::put(&conn_pool, &conn_info, client);
            });
        }
        None
    }
}

impl<C: ClientInnerExt> Drop for Client<C> {
    fn drop(&mut self) {
        if let Some(drop) = self.do_drop() {
            tokio::task::spawn_blocking(drop);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{mem, sync::atomic::AtomicBool};

    use crate::{serverless::cancel_set::CancelSet, BranchId, EndpointId, ProjectId};

    use super::*;

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

    fn create_inner() -> ClientInner<MockClient> {
        create_inner_with(MockClient::new(false))
    }

    fn create_inner_with(client: MockClient) -> ClientInner<MockClient> {
        ClientInner {
            inner: client,
            session: tokio::sync::watch::Sender::new(uuid::Uuid::new_v4()),
            cancel: CancellationToken::new(),
            aux: MetricsAuxInfo {
                endpoint_id: (&EndpointId::from("endpoint")).into(),
                project_id: (&ProjectId::from("project")).into(),
                branch_id: (&BranchId::from("branch")).into(),
                cold_start_info: crate::console::messages::ColdStartInfo::Warm,
            },
            conn_id: uuid::Uuid::new_v4(),
        }
    }

    #[tokio::test]
    async fn test_pool() {
        let _ = env_logger::try_init();
        let config = Box::leak(Box::new(crate::config::HttpConfig {
            pool_options: GlobalConnPoolOptions {
                max_conns_per_endpoint: 2,
                gc_epoch: Duration::from_secs(1),
                pool_shards: 2,
                idle_timeout: Duration::from_secs(1),
                opt_in: false,
                max_total_conns: 3,
            },
            request_timeout: Duration::from_secs(1),
            cancel_set: CancelSet::new(0),
            client_conn_threshold: u64::MAX,
        }));
        let pool = GlobalConnPool::new(config);
        let conn_info = ConnInfo {
            user_info: ComputeUserInfo {
                user: "user".into(),
                endpoint: "endpoint".into(),
                options: Default::default(),
            },
            dbname: "dbname".into(),
            password: "password".as_bytes().into(),
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
            let mut client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            client.do_drop().unwrap()();
            mem::forget(client); // drop the client
            assert_eq!(1, pool.get_global_connections_count());
        }
        {
            let mut closed_client = Client::new(
                create_inner_with(MockClient::new(true)),
                conn_info.clone(),
                ep_pool.clone(),
            );
            closed_client.do_drop().unwrap()();
            mem::forget(closed_client); // drop the client
                                        // The closed client shouldn't be added to the pool.
            assert_eq!(1, pool.get_global_connections_count());
        }
        let is_closed: Arc<AtomicBool> = Arc::new(false.into());
        {
            let mut client = Client::new(
                create_inner_with(MockClient(is_closed.clone())),
                conn_info.clone(),
                ep_pool.clone(),
            );
            client.do_drop().unwrap()();
            mem::forget(client); // drop the client

            // The client should be added to the pool.
            assert_eq!(2, pool.get_global_connections_count());
        }
        {
            let mut client = Client::new(create_inner(), conn_info, ep_pool);
            client.do_drop().unwrap()();
            mem::forget(client); // drop the client

            // The client shouldn't be added to the pool. Because the ep-pool is full.
            assert_eq!(2, pool.get_global_connections_count());
        }

        let conn_info = ConnInfo {
            user_info: ComputeUserInfo {
                user: "user".into(),
                endpoint: "endpoint-2".into(),
                options: Default::default(),
            },
            dbname: "dbname".into(),
            password: "password".as_bytes().into(),
        };
        let ep_pool = Arc::downgrade(
            &pool.get_or_create_endpoint_pool(&conn_info.endpoint_cache_key().unwrap()),
        );
        {
            let mut client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            client.do_drop().unwrap()();
            mem::forget(client); // drop the client
            assert_eq!(3, pool.get_global_connections_count());
        }
        {
            let mut client = Client::new(create_inner(), conn_info.clone(), ep_pool.clone());
            client.do_drop().unwrap()();
            mem::forget(client); // drop the client

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
