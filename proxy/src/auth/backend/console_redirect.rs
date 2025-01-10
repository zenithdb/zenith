use std::fmt;

use async_trait::async_trait;
use postgres_client::config::SslMode;
use pq_proto::BeMessage as Be;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, info_span};

use super::{ComputeCredentialKeys, ControlPlaneApi};
use crate::auth::backend::{BackendIpAllowlist, ComputeUserInfo};
use crate::auth::IpPattern;
use crate::cache::Cached;
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::control_plane::client::cplane_proxy_v1;
use crate::control_plane::{self, CachedNodeInfo, NodeInfo};
use crate::error::{ReportableError, UserFacingError};
use crate::proxy::connect_compute::ComputeConnectBackend;
use crate::proxy::NeonOptions;
use crate::stream::PqStream;
use crate::types::RoleName;
use crate::{auth, compute, waiters};

#[derive(Debug, Error)]
pub(crate) enum ConsoleRedirectError {
    #[error(transparent)]
    WaiterRegister(#[from] waiters::RegisterError),

    #[error(transparent)]
    WaiterWait(#[from] waiters::WaitError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct ConsoleRedirectBackend {
    console_uri: reqwest::Url,
    api: cplane_proxy_v1::NeonControlPlaneClient,
}

impl fmt::Debug for cplane_proxy_v1::NeonControlPlaneClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NeonControlPlaneClient")
    }
}

impl UserFacingError for ConsoleRedirectError {
    fn to_string_client(&self) -> String {
        "Internal error".to_string()
    }
}

impl ReportableError for ConsoleRedirectError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Self::WaiterRegister(_) => crate::error::ErrorKind::Service,
            Self::WaiterWait(_) => crate::error::ErrorKind::Service,
            Self::Io(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

fn hello_message(
    redirect_uri: &reqwest::Url,
    session_id: &str,
    duration: std::time::Duration,
) -> String {
    let formatted_duration = humantime::format_duration(duration).to_string();
    format!(
        concat![
            "Welcome to Neon!\n",
            "Authenticate by visiting (will expire in {duration}):\n",
            "    {redirect_uri}{session_id}\n\n",
        ],
        duration = formatted_duration,
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
}

pub(crate) fn new_psql_session_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

#[async_trait]
impl BackendIpAllowlist for ConsoleRedirectBackend {
    async fn get_allowed_ips(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> auth::Result<Vec<auth::IpPattern>> {
        self.api
            .get_allowed_ips_and_secret(ctx, user_info)
            .await
            .map(|(ips, _)| ips.as_ref().clone())
            .map_err(|e| e.into())
    }
}

impl ConsoleRedirectBackend {
    pub fn new(console_uri: reqwest::Url, api: cplane_proxy_v1::NeonControlPlaneClient) -> Self {
        Self { console_uri, api }
    }

    pub(crate) async fn authenticate(
        &self,
        ctx: &RequestContext,
        auth_config: &'static AuthenticationConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    ) -> auth::Result<(
        ConsoleRedirectNodeInfo,
        ComputeUserInfo,
        Option<Vec<IpPattern>>,
    )> {
        authenticate(ctx, auth_config, &self.console_uri, client)
            .await
            .map(|(node_info, user_info, ip_allowlist)| {
                (ConsoleRedirectNodeInfo(node_info), user_info, ip_allowlist)
            })
    }
}

pub struct ConsoleRedirectNodeInfo(pub(super) NodeInfo);

#[async_trait]
impl ComputeConnectBackend for ConsoleRedirectNodeInfo {
    async fn wake_compute(
        &self,
        _ctx: &RequestContext,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        Ok(Cached::new_uncached(self.0.clone()))
    }

    fn get_keys(&self) -> &ComputeCredentialKeys {
        &ComputeCredentialKeys::None
    }
}

async fn authenticate(
    ctx: &RequestContext,
    auth_config: &'static AuthenticationConfig,
    link_uri: &reqwest::Url,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<(NodeInfo, ComputeUserInfo, Option<Vec<IpPattern>>)> {
    ctx.set_auth_method(crate::context::AuthMethod::ConsoleRedirect);

    // registering waiter can fail if we get unlucky with rng.
    // just try again.
    let (psql_session_id, waiter) = loop {
        let psql_session_id = new_psql_session_id();

        match control_plane::mgmt::get_waiter(&psql_session_id) {
            Ok(waiter) => break (psql_session_id, waiter),
            Err(_e) => continue,
        }
    };

    let span = info_span!("console_redirect", psql_session_id = &psql_session_id);
    let greeting = hello_message(
        link_uri,
        &psql_session_id,
        auth_config.console_redirect_confirmation_timeout,
    );

    // Give user a URL to spawn a new database.
    info!(parent: &span, "sending the auth URL to the user");
    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&Be::CLIENT_ENCODING)?
        .write_message(&Be::NoticeResponse(&greeting))
        .await?;

    // Wait for console response via control plane (see `mgmt`).
    info!(parent: &span, "waiting for console's reply...");
    let db_info = tokio::time::timeout(auth_config.console_redirect_confirmation_timeout, waiter)
        .await
        .map_err(|_elapsed| {
            auth::AuthError::confirmation_timeout(
                auth_config.console_redirect_confirmation_timeout.into(),
            )
        })?
        .map_err(ConsoleRedirectError::from)?;

    if auth_config.ip_allowlist_check_enabled {
        if let Some(allowed_ips) = &db_info.allowed_ips {
            if !auth::check_peer_addr_is_in_list(&ctx.peer_addr(), allowed_ips) {
                return Err(auth::AuthError::ip_address_not_allowed(ctx.peer_addr()));
            }
        }
    }

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database."))?;

    // This config should be self-contained, because we won't
    // take username or dbname from client's startup message.
    let mut config = compute::ConnCfg::new(db_info.host.to_string(), db_info.port);
    config.dbname(&db_info.dbname).user(&db_info.user);

    let user: RoleName = db_info.user.into();
    let user_info = ComputeUserInfo {
        endpoint: db_info.aux.endpoint_id.as_str().into(),
        user: user.clone(),
        options: NeonOptions::default(),
    };

    ctx.set_dbname(db_info.dbname.into());
    ctx.set_user(user);
    ctx.set_project(db_info.aux.clone());
    info!("woken up a compute node");

    // Backwards compatibility. pg_sni_proxy uses "--" in domain names
    // while direct connections do not. Once we migrate to pg_sni_proxy
    // everywhere, we can remove this.
    if db_info.host.contains("--") {
        // we need TLS connection with SNI info to properly route it
        config.ssl_mode(SslMode::Require);
    } else {
        config.ssl_mode(SslMode::Disable);
    }

    if let Some(password) = db_info.password {
        config.password(password.as_ref());
    }

    Ok((
        NodeInfo {
            config,
            aux: db_info.aux,
        },
        user_info,
        db_info.allowed_ips,
    ))
}
