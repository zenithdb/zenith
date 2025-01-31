use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use postgres_client::tls::MakeTlsConnect;
use postgres_client::CancelToken;
use pq_proto::CancelKeyData;
use redis::{pipe, FromRedisValue, Pipeline, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::auth::backend::{BackendIpAllowlist, ComputeUserInfo};
use crate::auth::{check_peer_addr_is_in_list, AuthError};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::error::ReportableError;
use crate::ext::LockExt;
use crate::metrics::{CancelChannelSizeGuard, CancellationRequest, Metrics, RedisMsgKind};
use crate::rate_limiter::LeakyBucketRateLimiter;
use crate::redis::keys::KeyPrefix;
use crate::redis::kv_ops::RedisKVClient;
use crate::tls::postgres_rustls::MakeRustlsConnect;

type IpSubnetKey = IpNet;

const CANCEL_KEY_TTL: i64 = 1_209_600; // 2 weeks cancellation key expire time
const REDIS_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(10);

// Message types for sending through mpsc channel
pub enum CancelKeyOp {
    StoreCancelKey {
        key: String,
        field: String,
        value: String,
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
        _guard: CancelChannelSizeGuard<'static>,
        expire: i64, // TTL for key
    },
    GetCancelData {
        key: String,
        resp_tx: oneshot::Sender<anyhow::Result<Vec<(String, String)>>>,
        _guard: CancelChannelSizeGuard<'static>,
    },
    RemoveCancelKey {
        key: String,
        field: String,
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
        _guard: CancelChannelSizeGuard<'static>,
    },
}

impl CancelKeyOp {
    fn register(self, pipe: &mut Pipeline) -> CancelReplyOp {
        #[allow(clippy::used_underscore_binding)]
        match self {
            CancelKeyOp::StoreCancelKey {
                key,
                field,
                value,
                resp_tx,
                _guard,
                expire: _,
            } => {
                pipe.hset(key, field, value);
                CancelReplyOp::StoreCancelKey { resp_tx, _guard }
            }
            CancelKeyOp::GetCancelData {
                key,
                resp_tx,
                _guard,
            } => {
                pipe.hgetall(key);
                CancelReplyOp::GetCancelData { resp_tx, _guard }
            }
            CancelKeyOp::RemoveCancelKey {
                key,
                field,
                resp_tx,
                _guard,
            } => {
                pipe.hdel(key, field);
                CancelReplyOp::RemoveCancelKey { resp_tx, _guard }
            }
        }
    }
}

// Message types for sending through mpsc channel
pub enum CancelReplyOp {
    StoreCancelKey {
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
        _guard: CancelChannelSizeGuard<'static>,
    },
    GetCancelData {
        resp_tx: oneshot::Sender<anyhow::Result<Vec<(String, String)>>>,
        _guard: CancelChannelSizeGuard<'static>,
    },
    RemoveCancelKey {
        resp_tx: Option<oneshot::Sender<anyhow::Result<()>>>,
        _guard: CancelChannelSizeGuard<'static>,
    },
}

impl CancelReplyOp {
    fn send_err(self, e: anyhow::Error) {
        match self {
            CancelReplyOp::StoreCancelKey { resp_tx, _guard } => {
                if let Some(resp_tx) = resp_tx {
                    resp_tx
                        .send(Err(e))
                        .inspect_err(|_| tracing::debug!("could not send reply"))
                        .ok();
                }
            }
            CancelReplyOp::GetCancelData { resp_tx, _guard } => {
                resp_tx
                    .send(Err(e))
                    .inspect_err(|_| tracing::debug!("could not send reply"))
                    .ok();
            }
            CancelReplyOp::RemoveCancelKey { resp_tx, _guard } => {
                if let Some(resp_tx) = resp_tx {
                    resp_tx
                        .send(Err(e))
                        .inspect_err(|_| tracing::debug!("could not send reply"))
                        .ok();
                }
            }
        }
    }

    fn send_value(self, v: redis::Value) {
        match self {
            CancelReplyOp::StoreCancelKey { resp_tx, _guard } => {
                if let Some(resp_tx) = resp_tx {
                    let send =
                        FromRedisValue::from_owned_redis_value(v).context("could not parse value");
                    resp_tx
                        .send(send)
                        .inspect_err(|_| tracing::debug!("could not send reply"))
                        .ok();
                }
            }
            CancelReplyOp::GetCancelData { resp_tx, _guard } => {
                let send =
                    FromRedisValue::from_owned_redis_value(v).context("could not parse value");
                resp_tx
                    .send(send)
                    .inspect_err(|_| tracing::debug!("could not send reply"))
                    .ok();
            }
            CancelReplyOp::RemoveCancelKey { resp_tx, _guard } => {
                let send =
                    FromRedisValue::from_owned_redis_value(v).context("could not parse value");
                if let Some(resp_tx) = resp_tx {
                    resp_tx
                        .send(send)
                        .inspect_err(|_| tracing::debug!("could not send reply"))
                        .ok();
                }
            }
        }
    }
}

// Running as a separate task to accept messages through the rx channel
// In case of problems with RTT: switch to recv_many() + redis pipeline
pub async fn handle_cancel_messages(
    client: &mut RedisKVClient,
    mut rx: mpsc::Receiver<CancelKeyOp>,
) -> anyhow::Result<Infallible> {
    let mut buffer = Vec::new();
    let mut replies = vec![];

    loop {
        if rx.recv_many(&mut buffer, 8).await == 0 {
            warn!("shutting down cancellation queue");
            break std::future::pending().await;
        }
        debug!(n = buffer.len(), "running cancellation jobs");

        let mut pipe = pipe();
        for msg in buffer.drain(..) {
            replies.push(msg.register(&mut pipe));
        }

        match client.query(pipe).await {
            Ok(Value::Bulk(values)) if values.len() == replies.len() => {
                debug!(n = buffer.len(), "successfully completed cancellation jobs");
                for (value, reply) in std::iter::zip(values, replies.drain(..)) {
                    reply.send_value(value);
                }
            }
            Ok(value) => {
                debug!(?value, "unexpected redis return value");

                for reply in replies.drain(..) {
                    reply.send_err(anyhow!(
                        "could not send cmd to redis: incorrect response type"
                    ));
                }
            }
            Err(err) => {
                debug!(?err, "failed to run batched query");

                for reply in replies.drain(..) {
                    reply.send_err(anyhow!("could not send cmd to redis: {err}"));
                }
            }
        };
    }
}

/// Enables serving `CancelRequest`s.
///
/// If `CancellationPublisher` is available, cancel request will be used to publish the cancellation key to other proxy instances.
pub struct CancellationHandler {
    compute_config: &'static ComputeConfig,
    // rate limiter of cancellation requests
    limiter: Arc<std::sync::Mutex<LeakyBucketRateLimiter<IpSubnetKey>>>,
    tx: Option<mpsc::Sender<CancelKeyOp>>, // send messages to the redis KV client task
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

    #[error("key not found")]
    NotFound,

    #[error("proxy service error")]
    InternalError,
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
            CancelError::NotFound => crate::error::ErrorKind::User,
            CancelError::AuthError(_) => crate::error::ErrorKind::ControlPlane,
            CancelError::InternalError => crate::error::ErrorKind::Service,
        }
    }
}

impl CancellationHandler {
    pub fn new(
        compute_config: &'static ComputeConfig,
        tx: Option<mpsc::Sender<CancelKeyOp>>,
    ) -> Self {
        Self {
            compute_config,
            tx,
            limiter: Arc::new(std::sync::Mutex::new(
                LeakyBucketRateLimiter::<IpSubnetKey>::new_with_shards(
                    LeakyBucketRateLimiter::<IpSubnetKey>::DEFAULT,
                    64,
                ),
            )),
        }
    }

    pub(crate) fn get_key(self: &Arc<Self>) -> Session {
        // we intentionally generate a random "backend pid" and "secret key" here.
        // we use the corresponding u64 as an identifier for the
        // actual endpoint+pid+secret for postgres/pgbouncer.
        //
        // if we forwarded the backend_pid from postgres to the client, there would be a lot
        // of overlap between our computes as most pids are small (~100).

        let key: CancelKeyData = rand::random();

        let prefix_key: KeyPrefix = KeyPrefix::Cancel(key);
        let redis_key = prefix_key.build_redis_key();

        debug!("registered new query cancellation key {key}");
        Session {
            key,
            redis_key,
            cancellation_handler: Arc::clone(self),
        }
    }

    async fn get_cancel_key(
        &self,
        key: CancelKeyData,
    ) -> Result<Option<CancelClosure>, CancelError> {
        let prefix_key: KeyPrefix = KeyPrefix::Cancel(key);
        let redis_key = prefix_key.build_redis_key();

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let op = CancelKeyOp::GetCancelData {
            key: redis_key,
            resp_tx,
            _guard: Metrics::get()
                .proxy
                .cancel_channel_size
                .guard(RedisMsgKind::HGetAll),
        };

        let Some(tx) = &self.tx else {
            tracing::warn!("cancellation handler is not available");
            return Err(CancelError::InternalError);
        };

        tx.send_timeout(op, REDIS_SEND_TIMEOUT)
            .await
            .map_err(|e| {
                tracing::warn!("failed to send GetCancelData for {key}: {e}");
            })
            .map_err(|()| CancelError::InternalError)?;

        let result = resp_rx.await.map_err(|e| {
            tracing::warn!("failed to receive GetCancelData response: {e}");
            CancelError::InternalError
        })?;

        let cancel_state_str: Option<String> = match result {
            Ok(mut state) => {
                if state.len() == 1 {
                    Some(state.remove(0).1)
                } else {
                    tracing::warn!("unexpected number of entries in cancel state: {state:?}");
                    return Err(CancelError::InternalError);
                }
            }
            Err(e) => {
                tracing::warn!("failed to receive cancel state from redis: {e}");
                return Err(CancelError::InternalError);
            }
        };

        let cancel_state: Option<CancelClosure> = match cancel_state_str {
            Some(state) => {
                let cancel_closure: CancelClosure = serde_json::from_str(&state).map_err(|e| {
                    tracing::warn!("failed to deserialize cancel state: {e}");
                    CancelError::InternalError
                })?;
                Some(cancel_closure)
            }
            None => None,
        };
        Ok(cancel_state)
    }
    /// Try to cancel a running query for the corresponding connection.
    /// If the cancellation key is not found, it will be published to Redis.
    /// check_allowed - if true, check if the IP is allowed to cancel the query.
    /// Will fetch IP allowlist internally.
    ///
    /// return Result primarily for tests
    pub(crate) async fn cancel_session<T: BackendIpAllowlist>(
        &self,
        key: CancelKeyData,
        ctx: RequestContext,
        check_allowed: bool,
        auth_backend: &T,
    ) -> Result<(), CancelError> {
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
                    kind: crate::metrics::CancellationOutcome::RateLimitExceeded,
                });
            return Err(CancelError::RateLimit);
        }

        let cancel_state = self.get_cancel_key(key).await.map_err(|e| {
            tracing::warn!("failed to receive RedisOp response: {e}");
            CancelError::InternalError
        })?;

        let Some(cancel_closure) = cancel_state else {
            tracing::warn!("query cancellation key not found: {key}");
            Metrics::get()
                .proxy
                .cancellation_requests_total
                .inc(CancellationRequest {
                    kind: crate::metrics::CancellationOutcome::NotFound,
                });
            return Err(CancelError::NotFound);
        };

        if check_allowed {
            let ip_allowlist = auth_backend
                .get_allowed_ips(&ctx, &cancel_closure.user_info)
                .await
                .map_err(CancelError::AuthError)?;

            if !check_peer_addr_is_in_list(&ctx.peer_addr(), &ip_allowlist) {
                // log it here since cancel_session could be spawned in a task
                tracing::warn!(
                    "IP is not allowed to cancel the query: {key}, address: {}",
                    ctx.peer_addr()
                );
                return Err(CancelError::IpNotAllowed);
            }
        }

        Metrics::get()
            .proxy
            .cancellation_requests_total
            .inc(CancellationRequest {
                kind: crate::metrics::CancellationOutcome::Found,
            });
        info!("cancelling query per user's request using key {key}");
        cancel_closure.try_cancel_query(self.compute_config).await
    }
}

/// This should've been a [`std::future::Future`], but
/// it's impossible to name a type of an unboxed future
/// (we'd need something like `#![feature(type_alias_impl_trait)]`).
#[derive(Clone, Serialize, Deserialize)]
pub struct CancelClosure {
    socket_addr: SocketAddr,
    cancel_token: CancelToken,
    hostname: String, // for pg_sni router
    user_info: ComputeUserInfo,
}

impl CancelClosure {
    pub(crate) fn new(
        socket_addr: SocketAddr,
        cancel_token: CancelToken,
        hostname: String,
        user_info: ComputeUserInfo,
    ) -> Self {
        Self {
            socket_addr,
            cancel_token,
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
}

/// Helper for registering query cancellation tokens.
pub(crate) struct Session {
    /// The user-facing key identifying this session.
    key: CancelKeyData,
    redis_key: String,
    cancellation_handler: Arc<CancellationHandler>,
}

impl Session {
    pub(crate) fn key(&self) -> &CancelKeyData {
        &self.key
    }

    // Send the store key op to the cancellation handler
    pub(crate) async fn write_cancel_key(
        &self,
        cancel_closure: CancelClosure,
    ) -> Result<(), CancelError> {
        let Some(tx) = &self.cancellation_handler.tx else {
            tracing::warn!("cancellation handler is not available");
            return Err(CancelError::InternalError);
        };

        let closure_json = serde_json::to_string(&cancel_closure).map_err(|e| {
            tracing::warn!("failed to serialize cancel closure: {e}");
            CancelError::InternalError
        })?;

        let op = CancelKeyOp::StoreCancelKey {
            key: self.redis_key.clone(),
            field: "data".to_string(),
            value: closure_json,
            resp_tx: None,
            _guard: Metrics::get()
                .proxy
                .cancel_channel_size
                .guard(RedisMsgKind::HSet),
            expire: CANCEL_KEY_TTL,
        };

        let _ = tx.send_timeout(op, REDIS_SEND_TIMEOUT).await.map_err(|e| {
            let key = self.key;
            tracing::warn!("failed to send StoreCancelKey for {key}: {e}");
        });
        Ok(())
    }

    pub(crate) async fn remove_cancel_key(&self) -> Result<(), CancelError> {
        let Some(tx) = &self.cancellation_handler.tx else {
            tracing::warn!("cancellation handler is not available");
            return Err(CancelError::InternalError);
        };

        let op = CancelKeyOp::RemoveCancelKey {
            key: self.redis_key.clone(),
            field: "data".to_string(),
            resp_tx: None,
            _guard: Metrics::get()
                .proxy
                .cancel_channel_size
                .guard(RedisMsgKind::HSet),
        };

        let _ = tx.send_timeout(op, REDIS_SEND_TIMEOUT).await.map_err(|e| {
            let key = self.key;
            tracing::warn!("failed to send RemoveCancelKey for {key}: {e}");
        });
        Ok(())
    }
}
