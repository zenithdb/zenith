//! Various stuff for dealing with the Neon Console.
//! Later we might move some API wrappers here.

/// Payloads used in the console's APIs.
pub mod messages;

/// Wrappers for console APIs and their mocks.
pub mod client;

pub(crate) mod errors;

use std::sync::Arc;

use crate::auth::backend::jwt::AuthRule;
use crate::auth::backend::{ComputeCredentialKeys, ComputeUserInfo};
use crate::auth::IpPattern;
use crate::cache::project_info::ProjectInfoCacheImpl;
use crate::cache::{Cached, TimedLru};
use crate::config::ComputeConfig;
use crate::context::RequestContext;
use crate::control_plane::messages::{ControlPlaneErrorMessage, MetricsAuxInfo};
use crate::intern::ProjectIdInt;
use crate::types::{EndpointCacheKey, EndpointId};
use crate::{compute, scram};

/// Various cache-related types.
pub mod caches {
    pub use super::client::ApiCaches;
}

/// Various cache-related types.
pub mod locks {
    pub use super::client::ApiLocks;
}

/// Console's management API.
pub mod mgmt;

/// Auth secret which is managed by the cloud.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AuthSecret {
    #[cfg(any(test, feature = "testing"))]
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

#[derive(Default)]
pub(crate) struct AuthInfo {
    pub(crate) secret: Option<AuthSecret>,
    /// List of IP addresses allowed for the autorization.
    pub(crate) allowed_ips: Vec<IpPattern>,
    /// Project ID. This is used for cache invalidation.
    pub(crate) project_id: Option<ProjectIdInt>,
}

/// Info for establishing a connection to a compute node.
/// This is what we get after auth succeeded, but not before!
#[derive(Clone)]
pub(crate) struct NodeInfo {
    /// Compute node connection params.
    /// It's sad that we have to clone this, but this will improve
    /// once we migrate to a bespoke connection logic.
    pub(crate) config: compute::ConnCfg,

    /// Labels for proxy's metrics.
    pub(crate) aux: MetricsAuxInfo,
}

impl NodeInfo {
    pub(crate) async fn connect(
        &self,
        ctx: &RequestContext,
        config: &ComputeConfig,
        user_info: ComputeUserInfo,
    ) -> Result<compute::PostgresConnection, compute::ConnectionError> {
        self.config
            .connect(ctx, self.aux.clone(), config, user_info)
            .await
    }

    pub(crate) fn reuse_settings(&mut self, other: Self) {
        self.config.reuse_password(other.config);
    }

    pub(crate) fn set_keys(&mut self, keys: &ComputeCredentialKeys) {
        match keys {
            #[cfg(any(test, feature = "testing"))]
            ComputeCredentialKeys::Password(password) => self.config.password(password),
            ComputeCredentialKeys::AuthKeys(auth_keys) => self.config.auth_keys(*auth_keys),
            ComputeCredentialKeys::JwtPayload(_) | ComputeCredentialKeys::None => &mut self.config,
        };
    }
}

pub(crate) type NodeInfoCache =
    TimedLru<EndpointCacheKey, Result<NodeInfo, Box<ControlPlaneErrorMessage>>>;
pub(crate) type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;
pub(crate) type CachedRoleSecret = Cached<&'static ProjectInfoCacheImpl, Option<AuthSecret>>;
pub(crate) type CachedAllowedIps = Cached<&'static ProjectInfoCacheImpl, Arc<Vec<IpPattern>>>;

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
pub(crate) trait ControlPlaneApi {
    /// Get the client's auth secret for authentication.
    /// Returns option because user not found situation is special.
    /// We still have to mock the scram to avoid leaking information that user doesn't exist.
    async fn get_role_secret(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, errors::GetAuthInfoError>;

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), errors::GetAuthInfoError>;

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestContext,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, errors::GetEndpointJwksError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError>;
}
