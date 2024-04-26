use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio_postgres::NoTls;
use tracing::{field::display, info};

use crate::{
    auth::{backend::ComputeCredentials, check_peer_addr_is_in_list, AuthError},
    compute::{self, ConnectionError},
    config::{AuthenticationConfig, ProxyConfig},
    console::{
        errors::{GetAuthInfoError, WakeComputeError},
        CachedNodeInfo,
    },
    context::RequestMonitoring,
    dns::Dns,
    error::{ErrorKind, ReportableError, UserFacingError},
    proxy::connect_compute::ConnectMechanism,
};

use super::conn_pool::{poll_client, Client, ConnInfo, GlobalConnPool};

pub struct PoolingBackend {
    pub pool: Arc<GlobalConnPool<tokio_postgres::Client>>,
    pub config: &'static ProxyConfig,
}

impl PoolingBackend {
    pub async fn authenticate(
        &self,
        ctx: &mut RequestMonitoring,
        config: &AuthenticationConfig,
        conn_info: &ConnInfo,
    ) -> Result<ComputeCredentials, AuthError> {
        let user_info = conn_info.user_info.clone();
        let backend = self.config.auth_backend.as_ref().map(|_| user_info.clone());
        let (allowed_ips, maybe_secret) = backend.get_allowed_ips_and_secret(ctx).await?;
        if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
            return Err(AuthError::ip_address_not_allowed(ctx.peer_addr));
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
        let auth_outcome =
            crate::auth::validate_password_and_exchange(&conn_info.password, secret).await?;
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
        ctx: &mut RequestMonitoring,
        conn_info: ConnInfo,
        keys: ComputeCredentials,
        force_new: bool,
    ) -> Result<Client<tokio_postgres::Client>, HttpConnError> {
        let maybe_client = if !force_new {
            info!("pool: looking for an existing connection");
            self.pool.get(ctx, &conn_info).await?
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
            },
            &backend,
            &self.config.dns,
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
}

impl ReportableError for HttpConnError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            HttpConnError::ConnectionClosedAbruptly(_) => ErrorKind::Compute,
            HttpConnError::ConnectionError(p) => p.get_error_kind(),
            HttpConnError::GetAuthInfo(a) => a.get_error_kind(),
            HttpConnError::AuthError(a) => a.get_error_kind(),
            HttpConnError::WakeCompute(w) => w.get_error_kind(),
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
        }
    }
}

struct TokioMechanism {
    pool: Arc<GlobalConnPool<tokio_postgres::Client>>,
    conn_info: ConnInfo,
    conn_id: uuid::Uuid,
}

#[async_trait]
impl ConnectMechanism for TokioMechanism {
    type Connection = Client<tokio_postgres::Client>;
    type ConnectError = ConnectionError;
    type Error = HttpConnError;

    async fn connect_once(
        &self,
        ctx: &mut RequestMonitoring,
        dns: &Dns,
        node_info: &CachedNodeInfo,
        timeout: Duration,
    ) -> Result<Self::Connection, ConnectionError> {
        let mut config = node_info.config.clone();
        config
            .user(&self.conn_info.user_info.user)
            .password(&*self.conn_info.password)
            .dbname(&self.conn_info.dbname)
            .connect_timeout(timeout);

        let (_, client, connection) = config.connect_managed(ctx, dns, timeout, NoTls).await?;

        tracing::Span::current().record("pid", &tracing::field::display(client.get_process_id()));
        Ok(poll_client(
            self.pool.clone(),
            ctx,
            self.conn_info.clone(),
            client,
            connection,
            self.conn_id,
            node_info.aux.clone(),
        ))
    }

    fn update_connect_config(&self, _config: &mut compute::ConnCfg) {}
}
