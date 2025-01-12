//! Mock console backend which relies on a user-provided postgres instance.

use std::str::FromStr;
use std::sync::Arc;

use futures::TryFutureExt;
use thiserror::Error;
use tokio_postgres::Client;
use tracing::{error, info, info_span, warn, Instrument};

use crate::auth::backend::jwt::AuthRule;
use crate::auth::backend::ComputeUserInfo;
use crate::auth::IpPattern;
use crate::cache::Cached;
use crate::context::RequestContext;
use crate::control_plane::client::{CachedAllowedIps, CachedRoleSecret};
use crate::control_plane::errors::{
    ControlPlaneError, GetAuthInfoError, GetEndpointJwksError, WakeComputeError,
};
use crate::control_plane::messages::MetricsAuxInfo;
use crate::control_plane::{AuthInfo, AuthSecret, CachedNodeInfo, NodeInfo};
use crate::error::io_error;
use crate::intern::RoleNameInt;
use crate::types::{BranchId, EndpointId, ProjectId, RoleName};
use crate::url::ApiUrl;
use crate::{compute, scram};

#[derive(Debug, Error)]
enum MockApiError {
    #[error("Failed to read password: {0}")]
    PasswordNotSet(tokio_postgres::Error),
}

impl From<MockApiError> for ControlPlaneError {
    fn from(e: MockApiError) -> Self {
        io_error(e).into()
    }
}

impl From<tokio_postgres::Error> for ControlPlaneError {
    fn from(e: tokio_postgres::Error) -> Self {
        io_error(e).into()
    }
}

#[derive(Clone)]
pub struct MockControlPlane {
    endpoint: ApiUrl,
    ip_allowlist_check_enabled: bool,
}

impl MockControlPlane {
    pub fn new(endpoint: ApiUrl, ip_allowlist_check_enabled: bool) -> Self {
        Self {
            endpoint,
            ip_allowlist_check_enabled,
        }
    }

    pub(crate) fn url(&self) -> &str {
        self.endpoint.as_str()
    }

    async fn do_get_auth_info(
        &self,
        user_info: &ComputeUserInfo,
    ) -> Result<AuthInfo, GetAuthInfoError> {
        let (secret, allowed_ips) = async {
            // Perhaps we could persist this connection, but then we'd have to
            // write more code for reopening it if it got closed, which doesn't
            // seem worth it.
            let (client, connection) =
                tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

            tokio::spawn(connection);

            let secret = {
                let query = "select rolpassword from pg_catalog.pg_authid where rolname = $1";
                if let Some(row) = client.query_opt(query, &[&&*user_info.user]).await? {
                    let entry: String = row.get("rolpassword");

                    info!("got a secret: {entry}"); // safe since it's not a prod scenario
                    let secret = scram::ServerSecret::parse(&entry).map(AuthSecret::Scram);
                    secret.or_else(|| parse_md5(&entry).map(AuthSecret::Md5))
                } else {
                    warn!("user '{}' does not exist", user_info.user);
                    None
                }
            };

            let mut allowed_ips = vec![];
            if self.ip_allowlist_check_enabled {
                let query =
                    "select allowed_ips from neon_control_plane.endpoints where endpoint_id = $1";
                let row = client
                    .query_one(query, &[&user_info.endpoint.as_str()])
                    .await?;

                let s: Option<String> = row.get("allowed_ips");
                if let Some(s) = s {
                    info!("got allowed_ips: {s}");
                    allowed_ips = s
                        .split(',')
                        .map(|s| {
                            IpPattern::from_str(s).expect("mocked ip pattern should be correct")
                        })
                        .collect();
                }
            }

            Ok((secret, allowed_ips))
        }
        .inspect_err(|e: &GetAuthInfoError| tracing::error!("{e}"))
        .instrument(info_span!("postgres", url = self.endpoint.as_str()))
        .await?;
        Ok(AuthInfo {
            secret,
            allowed_ips,
            project_id: None,
        })
    }

    async fn do_get_endpoint_jwks(
        &self,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        let (client, connection) =
            tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;

        let connection = tokio::spawn(connection);

        let query = "select id, jwks_url, audience, role_names from neon_control_plane.endpoint_jwks where endpoint_id = $1";
        let res = client.query(query, &[&endpoint.as_str()]).await?;

        let mut rows = vec![];
        for row in res {
            rows.push(AuthRule {
                id: row.get("id"),
                jwks_url: url::Url::parse(row.get("jwks_url"))?,
                audience: row.get("audience"),
                role_names: row
                    .get::<_, Vec<String>>("role_names")
                    .into_iter()
                    .map(RoleName::from)
                    .map(|s| RoleNameInt::from(&s))
                    .collect(),
            });
        }

        drop(client);
        connection.await??;

        Ok(rows)
    }

    async fn do_wake_compute(
        &self,
        user_info: &ComputeUserInfo,
    ) -> Result<NodeInfo, WakeComputeError> {
        let (client, connection) =
            tokio_postgres::connect(self.endpoint.as_str(), tokio_postgres::NoTls).await?;
        tokio::spawn(connection);

        let query = "select host, port from neon_control_plane.endpoints where endpoint_id = $1";
        let row = client
            .query_one(query, &[&user_info.endpoint.as_str()])
            .await?;

        let host: String = row.get("host");
        let port: i32 = row.get("port");

        let mut config = compute::ConnCfg::new(host, port as u16);
        config.ssl_mode(postgres_client::config::SslMode::Disable);

        let node = NodeInfo {
            config,
            aux: MetricsAuxInfo {
                endpoint_id: (&EndpointId::from("endpoint")).into(),
                project_id: (&ProjectId::from("project")).into(),
                branch_id: (&BranchId::from("branch")).into(),
                cold_start_info: crate::control_plane::messages::ColdStartInfo::Warm,
            },
        };

        Ok(node)
    }
}

impl super::ControlPlaneApi for MockControlPlane {
    #[tracing::instrument(skip_all)]
    async fn get_role_secret(
        &self,
        _ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, GetAuthInfoError> {
        Ok(CachedRoleSecret::new_uncached(
            self.do_get_auth_info(user_info).await?.secret,
        ))
    }

    async fn get_allowed_ips_and_secret(
        &self,
        _ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError> {
        Ok((
            Cached::new_uncached(Arc::new(
                self.do_get_auth_info(user_info).await?.allowed_ips,
            )),
            None,
        ))
    }

    async fn get_endpoint_jwks(
        &self,
        _ctx: &RequestContext,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        self.do_get_endpoint_jwks(endpoint).await
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        _ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        self.do_wake_compute(user_info)
            .map_ok(Cached::new_uncached)
            .await
    }
}

fn parse_md5(input: &str) -> Option<[u8; 16]> {
    let text = input.strip_prefix("md5")?;

    let mut bytes = [0u8; 16];
    hex::decode_to_slice(text, &mut bytes).ok()?;

    Some(bytes)
}
