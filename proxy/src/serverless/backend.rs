use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tracing::{field::display, info};

use crate::{
    auth::{backend::ComputeCredentials, check_peer_addr_is_in_list, AuthError},
    cancellation::CancelClosure,
    compute::{self, ConnectionError},
    config::{AuthenticationConfig, ProxyConfig},
    console::{
        errors::{GetAuthInfoError, WakeComputeError},
        locks::ApiLocks,
        provider::ApiLockError,
        CachedNodeInfo,
    },
    context::RequestMonitoring,
    error::{ErrorKind, ReportableError, UserFacingError},
    intern::EndpointIdInt,
    proxy::{
        connect_compute::ConnectMechanism,
        retry::{CouldRetry, ShouldRetryWakeCompute},
    },
    rate_limiter::EndpointRateLimiter,
    Host,
};

use super::conn_pool::{poll_client, Client, ConnInfo, GlobalConnPool};

pub struct PoolingBackend {
    pub pool: Arc<GlobalConnPool<tokio_postgres::Client>>,
    pub config: &'static ProxyConfig,
    pub endpoint_rate_limiter: Arc<EndpointRateLimiter>,
}

impl PoolingBackend {
    pub async fn authenticate(
        &self,
        ctx: &RequestMonitoring,
        config: &AuthenticationConfig,
        conn_info: &ConnInfo,
    ) -> Result<ComputeCredentials, AuthError> {
        let user_info = conn_info.user_info.clone();
        let backend = self.config.auth_backend.as_ref().map(|_| user_info.clone());
        let (allowed_ips, maybe_secret) = backend.get_allowed_ips_and_secret(ctx).await?;
        if !check_peer_addr_is_in_list(&ctx.peer_addr(), &allowed_ips) {
            return Err(AuthError::ip_address_not_allowed(ctx.peer_addr()));
        }
        if !self
            .endpoint_rate_limiter
            .check(conn_info.user_info.endpoint.clone().into(), 1)
        {
            return Err(AuthError::too_many_connections());
        }
        let cached_secret = match maybe_secret {
            Some(secret) => secret,
            None => backend.get_role_secret(ctx).await?,
        };

        let secret = match cached_secret.value.clone() {
            Some(secret) => self.config.authentication_config.check_rate_limit(
                ctx,
                config,
                secret,
                &user_info.endpoint,
                true,
            )?,
            None => {
                // If we don't have an authentication secret, for the http flow we can just return an error.
                info!("authentication info not found");
                return Err(AuthError::auth_failed(&*user_info.user));
            }
        };
        let ep = EndpointIdInt::from(&conn_info.user_info.endpoint);
        let auth_outcome = crate::auth::validate_password_and_exchange(
            &config.thread_pool,
            ep,
            &conn_info.password,
            secret,
        )
        .await?;
        let res = match auth_outcome {
            crate::sasl::Outcome::Success(key) => {
                info!("user successfully authenticated");
                Ok(key)
            }
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                Err(AuthError::auth_failed(&*conn_info.user_info.user))
            }
        };
        res.map(|key| ComputeCredentials {
            info: user_info,
            keys: key,
        })
    }

    // Wake up the destination if needed. Code here is a bit involved because
    // we reuse the code from the usual proxy and we need to prepare few structures
    // that this code expects.
    #[tracing::instrument(fields(pid = tracing::field::Empty), skip_all)]
    pub async fn connect_to_compute(
        &self,
        ctx: &RequestMonitoring,
        conn_info: ConnInfo,
        keys: ComputeCredentials,
        force_new: bool,
    ) -> Result<Client<tokio_postgres::Client>, HttpConnError> {
        let maybe_client = if !force_new {
            info!("pool: looking for an existing connection");
            self.pool.get(ctx, &conn_info)?
        } else {
            info!("pool: pool is disabled");
            None
        };

        if let Some(client) = maybe_client {
            return Ok(client);
        }
        let conn_id = uuid::Uuid::new_v4();
        tracing::Span::current().record("conn_id", display(conn_id));
        info!(%conn_id, "pool: opening a new connection '{conn_info}'");
        let backend = self.config.auth_backend.as_ref().map(|_| keys);
        crate::proxy::connect_compute::connect_to_compute(
            ctx,
            &TokioMechanism {
                conn_id,
                conn_info,
                pool: self.pool.clone(),
                locks: &self.config.connect_compute_locks,
            },
            &backend,
            false, // do not allow self signed compute for http flow
            self.config.wake_compute_retry_config,
            self.config.connect_to_compute_retry_config,
        )
        .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpConnError {
    #[error("pooled connection closed at inconsistent state")]
    ConnectionClosedAbruptly(#[from] tokio::sync::watch::error::SendError<uuid::Uuid>),
    #[error("could not connection to compute")]
    ConnectionError(#[from] ConnectionError),

    #[error("could not get auth info")]
    GetAuthInfo(#[from] GetAuthInfoError),
    #[error("user not authenticated")]
    AuthError(#[from] AuthError),
    #[error("wake_compute returned error")]
    WakeCompute(#[from] WakeComputeError),
    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

impl ReportableError for HttpConnError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            HttpConnError::ConnectionClosedAbruptly(_) => ErrorKind::Compute,
            HttpConnError::ConnectionError(p) => p.get_error_kind(),
            HttpConnError::GetAuthInfo(a) => a.get_error_kind(),
            HttpConnError::AuthError(a) => a.get_error_kind(),
            HttpConnError::WakeCompute(w) => w.get_error_kind(),
            HttpConnError::TooManyConnectionAttempts(w) => w.get_error_kind(),
        }
    }
}

impl UserFacingError for HttpConnError {
    fn to_string_client(&self) -> String {
        match self {
            HttpConnError::ConnectionClosedAbruptly(_) => self.to_string(),
            HttpConnError::ConnectionError(p) => p.to_string(),
            HttpConnError::GetAuthInfo(c) => c.to_string_client(),
            HttpConnError::AuthError(c) => c.to_string_client(),
            HttpConnError::WakeCompute(c) => c.to_string_client(),
            HttpConnError::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
        }
    }
}

impl CouldRetry for HttpConnError {
    fn could_retry(&self) -> bool {
        match self {
            HttpConnError::ConnectionError(e) => e.could_retry(),
            HttpConnError::ConnectionClosedAbruptly(_) => false,
            HttpConnError::GetAuthInfo(_) => false,
            HttpConnError::AuthError(_) => false,
            HttpConnError::WakeCompute(_) => false,
            HttpConnError::TooManyConnectionAttempts(_) => false,
        }
    }
}
impl ShouldRetryWakeCompute for HttpConnError {
    fn should_retry_wake_compute(&self) -> bool {
        match self {
            HttpConnError::ConnectionError(e) => e.should_retry_wake_compute(),
            // we never checked cache validity
            HttpConnError::TooManyConnectionAttempts(_) => false,
            _ => true,
        }
    }
}

struct TokioMechanism {
    pool: Arc<GlobalConnPool<tokio_postgres::Client>>,
    conn_info: ConnInfo,
    conn_id: uuid::Uuid,

    /// connect_to_compute concurrency lock
    locks: &'static ApiLocks<Host>,
}

#[async_trait]
impl ConnectMechanism for TokioMechanism {
    type Connection = Client<tokio_postgres::Client>;
    type ConnectError = HttpConnError;
    type Error = HttpConnError;

    async fn connect_once(
        &self,
        ctx: &RequestMonitoring,
        node_info: &CachedNodeInfo,
        timeout: Duration,
    ) -> Result<Self::Connection, Self::ConnectError> {
        let host = node_info.config.get_host()?;
        let permit = self.locks.get_permit(&host).await?;

        let (socket_addr, client, connection) = permit.release_result(
            node_info
                .config
                .managed_connect(ctx, timeout, &mut tokio_postgres::NoTls)
                .await,
        )?;

        // NB: CancelToken is supposed to hold socket_addr, but we use connect_raw.
        // Yet another reason to rework the connection establishing code.
        let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

        tracing::Span::current().record("pid", tracing::field::display(client.get_process_id()));
        Ok(poll_client(
            self.pool.clone(),
            ctx,
            self.conn_info.clone(),
            client,
            connection,
            self.conn_id,
            node_info.aux.clone(),
            cancel_closure,
        ))
    }

    fn update_connect_config(&self, config: &mut compute::ConnCfg) {
        config
            .user(&self.conn_info.user_info.user)
            .dbname(&self.conn_info.dbname)
            .password(&self.conn_info.password);
    }
}
