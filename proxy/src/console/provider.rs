#[cfg(any(test, feature = "testing"))]
pub mod mock;
pub mod neon;

use super::messages::{ConsoleError, MetricsAuxInfo};
use crate::{
    auth::{
        backend::{ComputeCredentialKeys, ComputeUserInfo},
        IpPattern,
    },
    cache::{endpoints::EndpointsCache, project_info::ProjectInfoCacheImpl, Cached, TimedLru},
    compute,
    config::{CacheOptions, EndpointCacheConfig, ProjectInfoCacheOptions},
    context::RequestMonitoring,
    error::ReportableError,
    intern::ProjectIdInt,
    metrics::ApiLockMetrics,
    rate_limiter::{DynamicLimiter, Outcome, RateLimiterConfig, Token},
    scram, EndpointCacheKey,
};
use dashmap::DashMap;
use std::{hash::Hash, sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::info;

pub mod errors {
    use crate::{
        console::messages::{self, ConsoleError, Reason},
        error::{io_error, ReportableError, UserFacingError},
        proxy::retry::CouldRetry,
    };
    use thiserror::Error;

    use super::ApiLockError;

    /// A go-to error message which doesn't leak any detail.
    pub const REQUEST_FAILED: &str = "Console request failed";

    /// Common console API error.
    #[derive(Debug, Error)]
    pub enum ApiError {
        /// Error returned by the console itself.
        #[error("{REQUEST_FAILED} with {0}")]
        Console(ConsoleError),

        /// Various IO errors like broken pipe or malformed payload.
        #[error("{REQUEST_FAILED}: {0}")]
        Transport(#[from] std::io::Error),
    }

    impl ApiError {
        /// Returns HTTP status code if it's the reason for failure.
        pub fn get_reason(&self) -> messages::Reason {
            use ApiError::*;
            match self {
                Console(e) => e.get_reason(),
                _ => messages::Reason::Unknown,
            }
        }
    }

    impl UserFacingError for ApiError {
        fn to_string_client(&self) -> String {
            use ApiError::*;
            match self {
                // To minimize risks, only select errors are forwarded to users.
                Console(c) => c.get_user_facing_message(),
                _ => REQUEST_FAILED.to_owned(),
            }
        }
    }

    impl ReportableError for ApiError {
        fn get_error_kind(&self) -> crate::error::ErrorKind {
            match self {
                ApiError::Console(e) => {
                    use crate::error::ErrorKind::*;
                    match e.get_reason() {
                        Reason::RoleProtected => User,
                        Reason::ResourceNotFound => User,
                        Reason::ProjectNotFound => User,
                        Reason::EndpointNotFound => User,
                        Reason::BranchNotFound => User,
                        Reason::RateLimitExceeded => ServiceRateLimit,
                        Reason::NonDefaultBranchComputeTimeExceeded => User,
                        Reason::ActiveTimeQuotaExceeded => User,
                        Reason::ComputeTimeQuotaExceeded => User,
                        Reason::WrittenDataQuotaExceeded => User,
                        Reason::DataTransferQuotaExceeded => User,
                        Reason::LogicalSizeQuotaExceeded => User,
                        Reason::ConcurrencyLimitReached => ControlPlane,
                        Reason::LockAlreadyTaken => ControlPlane,
                        Reason::RunningOperations => ControlPlane,
                        Reason::Unknown => match &e {
                            ConsoleError {
                                http_status_code:
                                    http::StatusCode::NOT_FOUND | http::StatusCode::NOT_ACCEPTABLE,
                                ..
                            } => crate::error::ErrorKind::User,
                            ConsoleError {
                                http_status_code: http::StatusCode::UNPROCESSABLE_ENTITY,
                                error,
                                ..
                            } if error.contains(
                                "compute time quota of non-primary branches is exceeded",
                            ) =>
                            {
                                crate::error::ErrorKind::User
                            }
                            ConsoleError {
                                http_status_code: http::StatusCode::LOCKED,
                                error,
                                ..
                            } if error.contains("quota exceeded")
                                || error.contains("the limit for current plan reached") =>
                            {
                                crate::error::ErrorKind::User
                            }
                            ConsoleError {
                                http_status_code: http::StatusCode::TOO_MANY_REQUESTS,
                                ..
                            } => crate::error::ErrorKind::ServiceRateLimit,
                            ConsoleError { .. } => crate::error::ErrorKind::ControlPlane,
                        },
                    }
                }
                ApiError::Transport(_) => crate::error::ErrorKind::ControlPlane,
            }
        }
    }

    impl CouldRetry for ApiError {
        fn could_retry(&self) -> bool {
            match self {
                // retry some transport errors
                Self::Transport(io) => io.could_retry(),
                Self::Console(e) => e.could_retry(),
            }
        }
    }

    impl From<reqwest::Error> for ApiError {
        fn from(e: reqwest::Error) -> Self {
            io_error(e).into()
        }
    }

    impl From<reqwest_middleware::Error> for ApiError {
        fn from(e: reqwest_middleware::Error) -> Self {
            io_error(e).into()
        }
    }

    #[derive(Debug, Error)]
    pub enum GetAuthInfoError {
        // We shouldn't include the actual secret here.
        #[error("Console responded with a malformed auth secret")]
        BadSecret,

        #[error(transparent)]
        ApiError(ApiError),
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for GetAuthInfoError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
        }
    }

    impl UserFacingError for GetAuthInfoError {
        fn to_string_client(&self) -> String {
            use GetAuthInfoError::*;
            match self {
                // We absolutely should not leak any secrets!
                BadSecret => REQUEST_FAILED.to_owned(),
                // However, API might return a meaningful error.
                ApiError(e) => e.to_string_client(),
            }
        }
    }

    impl ReportableError for GetAuthInfoError {
        fn get_error_kind(&self) -> crate::error::ErrorKind {
            match self {
                GetAuthInfoError::BadSecret => crate::error::ErrorKind::ControlPlane,
                GetAuthInfoError::ApiError(_) => crate::error::ErrorKind::ControlPlane,
            }
        }
    }

    #[derive(Debug, Error)]
    pub enum WakeComputeError {
        #[error("Console responded with a malformed compute address: {0}")]
        BadComputeAddress(Box<str>),

        #[error(transparent)]
        ApiError(ApiError),

        #[error("Too many connections attempts")]
        TooManyConnections,

        #[error("error acquiring resource permit: {0}")]
        TooManyConnectionAttempts(#[from] ApiLockError),
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for WakeComputeError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
        }
    }

    impl UserFacingError for WakeComputeError {
        fn to_string_client(&self) -> String {
            use WakeComputeError::*;
            match self {
                // We shouldn't show user the address even if it's broken.
                // Besides, user is unlikely to care about this detail.
                BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
                // However, API might return a meaningful error.
                ApiError(e) => e.to_string_client(),

                TooManyConnections => self.to_string(),

                TooManyConnectionAttempts(_) => {
                    "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
                }
            }
        }
    }

    impl ReportableError for WakeComputeError {
        fn get_error_kind(&self) -> crate::error::ErrorKind {
            match self {
                WakeComputeError::BadComputeAddress(_) => crate::error::ErrorKind::ControlPlane,
                WakeComputeError::ApiError(e) => e.get_error_kind(),
                WakeComputeError::TooManyConnections => crate::error::ErrorKind::RateLimit,
                WakeComputeError::TooManyConnectionAttempts(e) => e.get_error_kind(),
            }
        }
    }

    impl CouldRetry for WakeComputeError {
        fn could_retry(&self) -> bool {
            match self {
                WakeComputeError::BadComputeAddress(_) => false,
                WakeComputeError::ApiError(e) => e.could_retry(),
                WakeComputeError::TooManyConnections => false,
                WakeComputeError::TooManyConnectionAttempts(_) => false,
            }
        }
    }
}

/// Auth secret which is managed by the cloud.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum AuthSecret {
    #[cfg(any(test, feature = "testing"))]
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

#[derive(Default)]
pub struct AuthInfo {
    pub secret: Option<AuthSecret>,
    /// List of IP addresses allowed for the autorization.
    pub allowed_ips: Vec<IpPattern>,
    /// Project ID. This is used for cache invalidation.
    pub project_id: Option<ProjectIdInt>,
}

/// Info for establishing a connection to a compute node.
/// This is what we get after auth succeeded, but not before!
#[derive(Clone)]
pub struct NodeInfo {
    /// Compute node connection params.
    /// It's sad that we have to clone this, but this will improve
    /// once we migrate to a bespoke connection logic.
    pub config: compute::ConnCfg,

    /// Labels for proxy's metrics.
    pub aux: MetricsAuxInfo,

    /// Whether we should accept self-signed certificates (for testing)
    pub allow_self_signed_compute: bool,
}

impl NodeInfo {
    pub async fn connect(
        &self,
        ctx: &mut RequestMonitoring,
        timeout: Duration,
    ) -> Result<compute::PostgresConnection, compute::ConnectionError> {
        self.config
            .connect(
                ctx,
                self.allow_self_signed_compute,
                self.aux.clone(),
                timeout,
            )
            .await
    }
    pub fn reuse_settings(&mut self, other: Self) {
        self.allow_self_signed_compute = other.allow_self_signed_compute;
        self.config.reuse_password(other.config);
    }

    pub fn set_keys(&mut self, keys: &ComputeCredentialKeys) {
        match keys {
            ComputeCredentialKeys::Password(password) => self.config.password(password),
            ComputeCredentialKeys::AuthKeys(auth_keys) => self.config.auth_keys(*auth_keys),
        };
    }
}

pub type NodeInfoCache = TimedLru<EndpointCacheKey, Result<NodeInfo, Box<ConsoleError>>>;
pub type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;
pub type CachedRoleSecret = Cached<&'static ProjectInfoCacheImpl, Option<AuthSecret>>;
pub type CachedAllowedIps = Cached<&'static ProjectInfoCacheImpl, Arc<Vec<IpPattern>>>;

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
pub(crate) trait Api {
    /// Get the client's auth secret for authentication.
    /// Returns option because user not found situation is special.
    /// We still have to mock the scram to avoid leaking information that user doesn't exist.
    async fn get_role_secret(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, errors::GetAuthInfoError>;

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), errors::GetAuthInfoError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError>;
}

#[non_exhaustive]
pub enum ConsoleBackend {
    /// Current Cloud API (V2).
    Console(neon::Api),
    /// Local mock of Cloud API (V2).
    #[cfg(any(test, feature = "testing"))]
    Postgres(mock::Api),
    /// Internal testing
    #[cfg(test)]
    Test(Box<dyn crate::auth::backend::TestBackend>),
}

impl Api for ConsoleBackend {
    async fn get_role_secret(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, errors::GetAuthInfoError> {
        use ConsoleBackend::*;
        match self {
            Console(api) => api.get_role_secret(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Postgres(api) => api.get_role_secret(ctx, user_info).await,
            #[cfg(test)]
            Test(_) => unreachable!("this function should never be called in the test backend"),
        }
    }

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), errors::GetAuthInfoError> {
        use ConsoleBackend::*;
        match self {
            Console(api) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Postgres(api) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            #[cfg(test)]
            Test(api) => api.get_allowed_ips_and_secret(),
        }
    }

    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError> {
        use ConsoleBackend::*;

        match self {
            Console(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Postgres(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(test)]
            Test(api) => api.wake_compute(),
        }
    }
}

/// Various caches for [`console`](super).
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub node_info: NodeInfoCache,
    /// Cache which stores project_id -> endpoint_ids mapping.
    pub project_info: Arc<ProjectInfoCacheImpl>,
    /// List of all valid endpoints.
    pub endpoints_cache: Arc<EndpointsCache>,
}

impl ApiCaches {
    pub fn new(
        wake_compute_cache_config: CacheOptions,
        project_info_cache_config: ProjectInfoCacheOptions,
        endpoint_cache_config: EndpointCacheConfig,
    ) -> Self {
        Self {
            node_info: NodeInfoCache::new(
                "node_info_cache",
                wake_compute_cache_config.size,
                wake_compute_cache_config.ttl,
                true,
            ),
            project_info: Arc::new(ProjectInfoCacheImpl::new(project_info_cache_config)),
            endpoints_cache: Arc::new(EndpointsCache::new(endpoint_cache_config)),
        }
    }
}

/// Various caches for [`console`](super).
pub struct ApiLocks<K> {
    name: &'static str,
    node_locks: DashMap<K, Arc<DynamicLimiter>>,
    config: RateLimiterConfig,
    timeout: Duration,
    epoch: std::time::Duration,
    metrics: &'static ApiLockMetrics,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiLockError {
    #[error("timeout acquiring resource permit")]
    TimeoutError(#[from] tokio::time::error::Elapsed),
}

impl ReportableError for ApiLockError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ApiLockError::TimeoutError(_) => crate::error::ErrorKind::RateLimit,
        }
    }
}

impl<K: Hash + Eq + Clone> ApiLocks<K> {
    pub fn new(
        name: &'static str,
        config: RateLimiterConfig,
        shards: usize,
        timeout: Duration,
        epoch: std::time::Duration,
        metrics: &'static ApiLockMetrics,
    ) -> prometheus::Result<Self> {
        Ok(Self {
            name,
            node_locks: DashMap::with_shard_amount(shards),
            config,
            timeout,
            epoch,
            metrics,
        })
    }

    pub async fn get_permit(&self, key: &K) -> Result<WakeComputePermit, ApiLockError> {
        if self.config.initial_limit == 0 {
            return Ok(WakeComputePermit {
                permit: Token::disabled(),
            });
        }
        let now = Instant::now();
        let semaphore = {
            // get fast path
            if let Some(semaphore) = self.node_locks.get(key) {
                semaphore.clone()
            } else {
                self.node_locks
                    .entry(key.clone())
                    .or_insert_with(|| {
                        self.metrics.semaphores_registered.inc();
                        DynamicLimiter::new(self.config)
                    })
                    .clone()
            }
        };
        let permit = semaphore.acquire_timeout(self.timeout).await;

        self.metrics
            .semaphore_acquire_seconds
            .observe(now.elapsed().as_secs_f64());
        info!("acquired permit {:?}", now.elapsed().as_secs_f64());
        Ok(WakeComputePermit { permit: permit? })
    }

    pub async fn garbage_collect_worker(&self) {
        if self.config.initial_limit == 0 {
            return;
        }
        let mut interval =
            tokio::time::interval(self.epoch / (self.node_locks.shards().len()) as u32);
        loop {
            for (i, shard) in self.node_locks.shards().iter().enumerate() {
                interval.tick().await;
                // temporary lock a single shard and then clear any semaphores that aren't currently checked out
                // race conditions: if strong_count == 1, there's no way that it can increase while the shard is locked
                // therefore releasing it is safe from race conditions
                info!(
                    name = self.name,
                    shard = i,
                    "performing epoch reclamation on api lock"
                );
                let mut lock = shard.write();
                let timer = self.metrics.reclamation_lag_seconds.start_timer();

                let mut removed = 0;
                crate::rawtable::retain(&mut *lock, |_, semaphore| {
                    let remove = Arc::strong_count(semaphore.get_mut()) == 1;
                    if remove {
                        removed += 1;
                    }
                    !remove
                });

                drop(lock);
                self.metrics.semaphores_unregistered.inc_by(removed as u64);
                timer.observe();
            }
        }
    }
}

pub struct WakeComputePermit {
    permit: Token,
}

impl WakeComputePermit {
    pub fn should_check_cache(&self) -> bool {
        !self.permit.is_disabled()
    }
    pub fn release(self, outcome: Outcome) {
        self.permit.release(outcome)
    }
    pub fn release_result<T, E>(self, res: Result<T, E>) -> Result<T, E> {
        match res {
            Ok(_) => self.release(Outcome::Success),
            Err(_) => self.release(Outcome::Overload),
        }
        res
    }
}
