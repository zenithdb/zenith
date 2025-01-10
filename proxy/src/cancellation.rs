use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use dashmap::DashMap;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use postgres_client::tls::MakeTlsConnect;
use postgres_client::CancelToken;
use pq_proto::CancelKeyData;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, info};
use uuid::Uuid;

use crate::auth::backend::{BackendIpAllowlist, ComputeUserInfo};
use crate::auth::{check_peer_addr_is_in_list, AuthError, IpPattern};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::error::ReportableError;
use crate::ext::LockExt;
use crate::metrics::{CancellationRequest, CancellationSource, Metrics};
use crate::rate_limiter::LeakyBucketRateLimiter;
use crate::redis::cancellation_publisher::{
    CancellationPublisher, CancellationPublisherMut, RedisPublisherClient,
};
use crate::tls::postgres_rustls::MakeRustlsConnect;

pub type CancelMap = Arc<DashMap<CancelKeyData, Option<CancelClosure>>>;
pub type CancellationHandlerMain = CancellationHandler<Option<Arc<Mutex<RedisPublisherClient>>>>;
pub(crate) type CancellationHandlerMainInternal = Option<Arc<Mutex<RedisPublisherClient>>>;

type IpSubnetKey = IpNet;

/// Enables serving `CancelRequest`s.
///
/// If `CancellationPublisher` is available, cancel request will be used to publish the cancellation key to other proxy instances.
pub struct CancellationHandler<P> {
    compute_config: &'static ComputeConfig,
    map: CancelMap,
    client: P,
    /// This field used for the monitoring purposes.
    /// Represents the source of the cancellation request.
    from: CancellationSource,
    // rate limiter of cancellation requests
    limiter: Arc<std::sync::Mutex<LeakyBucketRateLimiter<IpSubnetKey>>>,
}

#[derive(Debug, Error)]
pub(crate) enum CancelError {
    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Postgres(#[from] postgres_client::Error),

    #[error("rate limit exceeded")]
    RateLimit,

    #[error("IP is not allowed")]
    IpNotAllowed,

    #[error("Authentication backend error")]
    AuthError(#[from] AuthError),
}

impl ReportableError for CancelError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            CancelError::IO(_) => crate::error::ErrorKind::Compute,
            CancelError::Postgres(e) if e.as_db_error().is_some() => {
                crate::error::ErrorKind::Postgres
            }
            CancelError::Postgres(_) => crate::error::ErrorKind::Compute,
            CancelError::RateLimit => crate::error::ErrorKind::RateLimit,
            CancelError::IpNotAllowed => crate::error::ErrorKind::User,
            CancelError::AuthError(_) => crate::error::ErrorKind::ControlPlane,
        }
    }
}

impl<P: CancellationPublisher> CancellationHandler<P> {
    /// Run async action within an ephemeral session identified by [`CancelKeyData`].
    pub(crate) fn get_session(self: Arc<Self>) -> Session<P> {
        // we intentionally generate a random "backend pid" and "secret key" here.
        // we use the corresponding u64 as an identifier for the
        // actual endpoint+pid+secret for postgres/pgbouncer.
        //
        // if we forwarded the backend_pid from postgres to the client, there would be a lot
        // of overlap between our computes as most pids are small (~100).
        let key = loop {
            let key = rand::random();

            // Random key collisions are unlikely to happen here, but they're still possible,
            // which is why we have to take care not to rewrite an existing key.
            match self.map.entry(key) {
                dashmap::mapref::entry::Entry::Occupied(_) => continue,
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    e.insert(None);
                }
            }
            break key;
        };

        debug!("registered new query cancellation key {key}");
        Session {
            key,
            cancellation_handler: self,
        }
    }

    /// Cancelling only in notification, will be removed
    pub(crate) async fn cancel_session(
        &self,
        key: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
        check_allowed: bool,
    ) -> Result<(), CancelError> {
        // TODO: check for unspecified address is only for backward compatibility, should be removed
        if !peer_addr.is_unspecified() {
            let subnet_key = match peer_addr {
                IpAddr::V4(ip) => IpNet::V4(Ipv4Net::new_assert(ip, 24).trunc()), // use defaut mask here
                IpAddr::V6(ip) => IpNet::V6(Ipv6Net::new_assert(ip, 64).trunc()),
            };
            if !self.limiter.lock_propagate_poison().check(subnet_key, 1) {
                // log only the subnet part of the IP address to know which subnet is rate limited
                tracing::warn!("Rate limit exceeded. Skipping cancellation message, {subnet_key}");
                Metrics::get()
                    .proxy
                    .cancellation_requests_total
                    .inc(CancellationRequest {
                        source: self.from,
                        kind: crate::metrics::CancellationOutcome::RateLimitExceeded,
                    });
                return Err(CancelError::RateLimit);
            }
        }

        // NB: we should immediately release the lock after cloning the token.
        let cancel_state = self.map.get(&key).and_then(|x| x.clone());
        let Some(cancel_closure) = cancel_state else {
            tracing::warn!("query cancellation key not found: {key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    source: self.from,
                    kind: crate::metrics::CancellationOutcome::NotFound,
                });

            if session_id == Uuid::nil() {
                // was already published, do not publish it again
                return Ok(());
            }

            match self.client.try_publish(key, session_id, peer_addr).await {
                Ok(()) => {} // do nothing
                Err(e) => {
                    // log it here since cancel_session could be spawned in a task
                    tracing::error!("failed to publish cancellation key: {key}, error: {e}");
                    return Err(CancelError::IO(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )));
                }
            }
            return Ok(());
        };

        if check_allowed
            && !check_peer_addr_is_in_list(&peer_addr, cancel_closure.ip_allowlist.as_slice())
        {
            // log it here since cancel_session could be spawned in a task
            tracing::warn!("IP is not allowed to cancel the query: {key}");
            return Err(CancelError::IpNotAllowed);
        }

        Metrics::get()
            .proxy
            .cancellation_requests_total
            .inc(CancellationRequest {
                source: self.from,
                kind: crate::metrics::CancellationOutcome::Found,
            });
        info!(
            "cancelling query per user's request using key {key}, hostname {}, address: {}",
            cancel_closure.hostname, cancel_closure.socket_addr
        );
        cancel_closure.try_cancel_query(self.compute_config).await
    }

    /// Try to cancel a running query for the corresponding connection.
    /// If the cancellation key is not found, it will be published to Redis.
    /// check_allowed - if true, check if the IP is allowed to cancel the query.
    /// Will fetch IP allowlist internally.
    ///
    /// return Result primarily for tests
    pub(crate) async fn cancel_session_auth<T: BackendIpAllowlist>(
        &self,
        key: CancelKeyData,
        ctx: RequestContext,
        check_allowed: bool,
        auth_backend: &T,
    ) -> Result<(), CancelError> {
        // TODO: check for unspecified address is only for backward compatibility, should be removed
        if !ctx.peer_addr().is_unspecified() {
            let subnet_key = match ctx.peer_addr() {
                IpAddr::V4(ip) => IpNet::V4(Ipv4Net::new_assert(ip, 24).trunc()), // use defaut mask here
                IpAddr::V6(ip) => IpNet::V6(Ipv6Net::new_assert(ip, 64).trunc()),
            };
            if !self.limiter.lock_propagate_poison().check(subnet_key, 1) {
                // log only the subnet part of the IP address to know which subnet is rate limited
                tracing::warn!("Rate limit exceeded. Skipping cancellation message, {subnet_key}");
                Metrics::get()
                    .proxy
                    .cancellation_requests_total
                    .inc(CancellationRequest {
                        source: self.from,
                        kind: crate::metrics::CancellationOutcome::RateLimitExceeded,
                    });
                return Err(CancelError::RateLimit);
            }
        }

        // NB: we should immediately release the lock after cloning the token.
        let cancel_state = self.map.get(&key).and_then(|x| x.clone());
        let Some(cancel_closure) = cancel_state else {
            tracing::warn!("query cancellation key not found: {key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    source: self.from,
                    kind: crate::metrics::CancellationOutcome::NotFound,
                });

            if ctx.session_id() == Uuid::nil() {
                // was already published, do not publish it again
                return Ok(());
            }

            match self
                .client
                .try_publish(key, ctx.session_id(), ctx.peer_addr())
                .await
            {
                Ok(()) => {} // do nothing
                Err(e) => {
                    // log it here since cancel_session could be spawned in a task
                    tracing::error!("failed to publish cancellation key: {key}, error: {e}");
                    return Err(CancelError::IO(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )));
                }
            }
            return Ok(());
        };

        let ip_allowlist = auth_backend
            .get_allowed_ips(&ctx, &cancel_closure.user_info)
            .await
            .map_err(CancelError::AuthError)?;

        if check_allowed && !check_peer_addr_is_in_list(&ctx.peer_addr(), &ip_allowlist) {
            // log it here since cancel_session could be spawned in a task
            tracing::warn!("IP is not allowed to cancel the query: {key}");
            return Err(CancelError::IpNotAllowed);
        }

        Metrics::get()
            .proxy
            .cancellation_requests_total
            .inc(CancellationRequest {
                source: self.from,
                kind: crate::metrics::CancellationOutcome::Found,
            });
        info!("cancelling query per user's request using key {key}");
        cancel_closure.try_cancel_query(self.compute_config).await
    }

    #[cfg(test)]
    fn contains(&self, session: &Session<P>) -> bool {
        self.map.contains_key(&session.key)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl CancellationHandler<()> {
    pub fn new(
        compute_config: &'static ComputeConfig,
        map: CancelMap,
        from: CancellationSource,
    ) -> Self {
        Self {
            compute_config,
            map,
            client: (),
            from,
            limiter: Arc::new(std::sync::Mutex::new(
                LeakyBucketRateLimiter::<IpSubnetKey>::new_with_shards(
                    LeakyBucketRateLimiter::<IpSubnetKey>::DEFAULT,
                    64,
                ),
            )),
        }
    }
}

impl<P: CancellationPublisherMut> CancellationHandler<Option<Arc<Mutex<P>>>> {
    pub fn new(
        compute_config: &'static ComputeConfig,
        map: CancelMap,
        client: Option<Arc<Mutex<P>>>,
        from: CancellationSource,
    ) -> Self {
        Self {
            compute_config,
            map,
            client,
            from,
            limiter: Arc::new(std::sync::Mutex::new(
                LeakyBucketRateLimiter::<IpSubnetKey>::new_with_shards(
                    LeakyBucketRateLimiter::<IpSubnetKey>::DEFAULT,
                    64,
                ),
            )),
        }
    }
}

/// This should've been a [`std::future::Future`], but
/// it's impossible to name a type of an unboxed future
/// (we'd need something like `#![feature(type_alias_impl_trait)]`).
#[derive(Clone)]
pub struct CancelClosure {
    socket_addr: SocketAddr,
    cancel_token: CancelToken,
    ip_allowlist: Vec<IpPattern>,
    hostname: String, // for pg_sni router
    user_info: ComputeUserInfo,
}

impl CancelClosure {
    pub(crate) fn new(
        socket_addr: SocketAddr,
        cancel_token: CancelToken,
        ip_allowlist: Vec<IpPattern>,
        hostname: String,
        user_info: ComputeUserInfo,
    ) -> Self {
        Self {
            socket_addr,
            cancel_token,
            ip_allowlist,
            hostname,
            user_info,
        }
    }
    /// Cancels the query running on user's compute node.
    pub(crate) async fn try_cancel_query(
        self,
        compute_config: &ComputeConfig,
    ) -> Result<(), CancelError> {
        let socket = TcpStream::connect(self.socket_addr).await?;

        let mut mk_tls =
            crate::tls::postgres_rustls::MakeRustlsConnect::new(compute_config.tls.clone());
        let tls = <MakeRustlsConnect as MakeTlsConnect<tokio::net::TcpStream>>::make_tls_connect(
            &mut mk_tls,
            &self.hostname,
        )
        .map_err(|e| {
            CancelError::IO(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        self.cancel_token.cancel_query_raw(socket, tls).await?;
        debug!("query was cancelled");
        Ok(())
    }

    /// Obsolete (will be removed after moving CancelMap to Redis), only for notifications
    pub(crate) fn set_ip_allowlist(&mut self, ip_allowlist: Vec<IpPattern>) {
        self.ip_allowlist = ip_allowlist;
    }
}

/// Helper for registering query cancellation tokens.
pub(crate) struct Session<P> {
    /// The user-facing key identifying this session.
    key: CancelKeyData,
    /// The [`CancelMap`] this session belongs to.
    cancellation_handler: Arc<CancellationHandler<P>>,
}

impl<P> Session<P> {
    /// Store the cancel token for the given session.
    /// This enables query cancellation in `crate::proxy::prepare_client_connection`.
    pub(crate) fn enable_query_cancellation(&self, cancel_closure: CancelClosure) -> CancelKeyData {
        debug!("enabling query cancellation for this session");
        self.cancellation_handler
            .map
            .insert(self.key, Some(cancel_closure));

        self.key
    }
}

impl<P> Drop for Session<P> {
    fn drop(&mut self) {
        self.cancellation_handler.map.remove(&self.key);
        debug!("dropped query cancellation key {}", &self.key);
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::RetryConfig;
    use crate::tls::client_config::compute_client_config_with_certs;

    fn config() -> ComputeConfig {
        let retry = RetryConfig {
            base_delay: Duration::from_secs(1),
            max_retries: 5,
            backoff_factor: 2.0,
        };

        ComputeConfig {
            retry,
            tls: Arc::new(compute_client_config_with_certs(std::iter::empty())),
            timeout: Duration::from_secs(2),
        }
    }

    #[tokio::test]
    async fn check_session_drop() -> anyhow::Result<()> {
        let cancellation_handler = Arc::new(CancellationHandler::<()>::new(
            Box::leak(Box::new(config())),
            CancelMap::default(),
            CancellationSource::FromRedis,
        ));

        let session = cancellation_handler.clone().get_session();
        assert!(cancellation_handler.contains(&session));
        drop(session);
        // Check that the session has been dropped.
        assert!(cancellation_handler.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn cancel_session_noop_regression() {
        let handler = CancellationHandler::<()>::new(
            Box::leak(Box::new(config())),
            CancelMap::default(),
            CancellationSource::Local,
        );
        handler
            .cancel_session(
                CancelKeyData {
                    backend_pid: 0,
                    cancel_key: 0,
                },
                Uuid::new_v4(),
                "127.0.0.1".parse().unwrap(),
                true,
            )
            .await
            .unwrap();
    }
}
