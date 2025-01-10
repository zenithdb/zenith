use std::fmt::{self, Display};

use measured::FixedCardinalityLabel;
use serde::{Deserialize, Serialize};

use crate::auth::IpPattern;
use crate::intern::{BranchIdInt, EndpointIdInt, ProjectIdInt, RoleNameInt};
use crate::proxy::retry::CouldRetry;

/// Generic error response with human-readable description.
/// Note that we can't always present it to user as is.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ControlPlaneErrorMessage {
    pub(crate) error: Box<str>,
    #[serde(skip)]
    pub(crate) http_status_code: http::StatusCode,
    pub(crate) status: Option<Status>,
}

impl ControlPlaneErrorMessage {
    pub(crate) fn get_reason(&self) -> Reason {
        self.status
            .as_ref()
            .and_then(|s| s.details.error_info.as_ref())
            .map_or(Reason::Unknown, |e| e.reason)
    }

    pub(crate) fn get_user_facing_message(&self) -> String {
        use super::errors::REQUEST_FAILED;
        self.status
            .as_ref()
            .and_then(|s| s.details.user_facing_message.as_ref())
            .map_or_else(|| {
                // Ask @neondatabase/control-plane for review before adding more.
                match self.http_status_code {
                    http::StatusCode::NOT_FOUND => {
                        // Status 404: failed to get a project-related resource.
                        format!("{REQUEST_FAILED}: endpoint cannot be found")
                    }
                    http::StatusCode::NOT_ACCEPTABLE => {
                        // Status 406: endpoint is disabled (we don't allow connections).
                        format!("{REQUEST_FAILED}: endpoint is disabled")
                    }
                    http::StatusCode::LOCKED | http::StatusCode::UNPROCESSABLE_ENTITY => {
                        // Status 423: project might be in maintenance mode (or bad state), or quotas exceeded.
                        format!("{REQUEST_FAILED}: endpoint is temporarily unavailable. Check your quotas and/or contact our support.")
                    }
                    _ => REQUEST_FAILED.to_owned(),
                }
            }, |m| m.message.clone().into())
    }
}

impl Display for ControlPlaneErrorMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg: &str = self
            .status
            .as_ref()
            .and_then(|s| s.details.user_facing_message.as_ref())
            .map_or_else(|| self.error.as_ref(), |m| m.message.as_ref());
        write!(f, "{msg}")
    }
}

impl CouldRetry for ControlPlaneErrorMessage {
    fn could_retry(&self) -> bool {
        // If the error message does not have a status,
        // the error is unknown and probably should not retry automatically
        let Some(status) = &self.status else {
            return false;
        };

        // retry if the retry info is set.
        if status.details.retry_info.is_some() {
            return true;
        }

        // if no retry info set, attempt to use the error code to guess the retry state.
        let reason = status
            .details
            .error_info
            .map_or(Reason::Unknown, |e| e.reason);

        reason.can_retry()
    }
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub(crate) struct Status {
    pub(crate) code: Box<str>,
    pub(crate) message: Box<str>,
    pub(crate) details: Details,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Details {
    pub(crate) error_info: Option<ErrorInfo>,
    pub(crate) retry_info: Option<RetryInfo>,
    pub(crate) user_facing_message: Option<UserFacingMessage>,
}

#[derive(Copy, Clone, Debug, Deserialize)]
pub(crate) struct ErrorInfo {
    pub(crate) reason: Reason,
    // Schema could also have `metadata` field, but it's not structured. Skip it for now.
}

#[derive(Clone, Copy, Debug, Deserialize, Default)]
pub(crate) enum Reason {
    /// RoleProtected indicates that the role is protected and the attempted operation is not permitted on protected roles.
    #[serde(rename = "ROLE_PROTECTED")]
    RoleProtected,
    /// ResourceNotFound indicates that a resource (project, endpoint, branch, etc.) wasn't found,
    /// usually due to the provided ID not being correct or because the subject doesn't have enough permissions to
    /// access the requested resource.
    /// Prefer a more specific reason if possible, e.g., ProjectNotFound, EndpointNotFound, etc.
    #[serde(rename = "RESOURCE_NOT_FOUND")]
    ResourceNotFound,
    /// ProjectNotFound indicates that the project wasn't found, usually due to the provided ID not being correct,
    /// or that the subject doesn't have enough permissions to access the requested project.
    #[serde(rename = "PROJECT_NOT_FOUND")]
    ProjectNotFound,
    /// EndpointNotFound indicates that the endpoint wasn't found, usually due to the provided ID not being correct,
    /// or that the subject doesn't have enough permissions to access the requested endpoint.
    #[serde(rename = "ENDPOINT_NOT_FOUND")]
    EndpointNotFound,
    /// BranchNotFound indicates that the branch wasn't found, usually due to the provided ID not being correct,
    /// or that the subject doesn't have enough permissions to access the requested branch.
    #[serde(rename = "BRANCH_NOT_FOUND")]
    BranchNotFound,
    /// RateLimitExceeded indicates that the rate limit for the operation has been exceeded.
    #[serde(rename = "RATE_LIMIT_EXCEEDED")]
    RateLimitExceeded,
    /// NonDefaultBranchComputeTimeExceeded indicates that the compute time quota of non-default branches has been
    /// exceeded.
    #[serde(rename = "NON_PRIMARY_BRANCH_COMPUTE_TIME_EXCEEDED")]
    NonDefaultBranchComputeTimeExceeded,
    /// ActiveTimeQuotaExceeded indicates that the active time quota was exceeded.
    #[serde(rename = "ACTIVE_TIME_QUOTA_EXCEEDED")]
    ActiveTimeQuotaExceeded,
    /// ComputeTimeQuotaExceeded indicates that the compute time quota was exceeded.
    #[serde(rename = "COMPUTE_TIME_QUOTA_EXCEEDED")]
    ComputeTimeQuotaExceeded,
    /// WrittenDataQuotaExceeded indicates that the written data quota was exceeded.
    #[serde(rename = "WRITTEN_DATA_QUOTA_EXCEEDED")]
    WrittenDataQuotaExceeded,
    /// DataTransferQuotaExceeded indicates that the data transfer quota was exceeded.
    #[serde(rename = "DATA_TRANSFER_QUOTA_EXCEEDED")]
    DataTransferQuotaExceeded,
    /// LogicalSizeQuotaExceeded indicates that the logical size quota was exceeded.
    #[serde(rename = "LOGICAL_SIZE_QUOTA_EXCEEDED")]
    LogicalSizeQuotaExceeded,
    /// RunningOperations indicates that the project already has some running operations
    /// and scheduling of new ones is prohibited.
    #[serde(rename = "RUNNING_OPERATIONS")]
    RunningOperations,
    /// ConcurrencyLimitReached indicates that the concurrency limit for an action was reached.
    #[serde(rename = "CONCURRENCY_LIMIT_REACHED")]
    ConcurrencyLimitReached,
    /// LockAlreadyTaken indicates that the we attempted to take a lock that was already taken.
    #[serde(rename = "LOCK_ALREADY_TAKEN")]
    LockAlreadyTaken,
    /// ActiveEndpointsLimitExceeded indicates that the limit of concurrently active endpoints was exceeded.
    #[serde(rename = "ACTIVE_ENDPOINTS_LIMIT_EXCEEDED")]
    ActiveEndpointsLimitExceeded,
    #[default]
    #[serde(other)]
    Unknown,
}

impl Reason {
    pub(crate) fn is_not_found(self) -> bool {
        matches!(
            self,
            Reason::ResourceNotFound
                | Reason::ProjectNotFound
                | Reason::EndpointNotFound
                | Reason::BranchNotFound
        )
    }

    pub(crate) fn can_retry(self) -> bool {
        match self {
            // do not retry role protected errors
            // not a transitive error
            Reason::RoleProtected => false,
            // on retry, it will still not be found
            Reason::ResourceNotFound
            | Reason::ProjectNotFound
            | Reason::EndpointNotFound
            | Reason::BranchNotFound => false,
            // we were asked to go away
            Reason::RateLimitExceeded
            | Reason::NonDefaultBranchComputeTimeExceeded
            | Reason::ActiveTimeQuotaExceeded
            | Reason::ComputeTimeQuotaExceeded
            | Reason::WrittenDataQuotaExceeded
            | Reason::DataTransferQuotaExceeded
            | Reason::LogicalSizeQuotaExceeded
            | Reason::ActiveEndpointsLimitExceeded => false,
            // transitive error. control plane is currently busy
            // but might be ready soon
            Reason::RunningOperations
            | Reason::ConcurrencyLimitReached
            | Reason::LockAlreadyTaken => true,
            // unknown error. better not retry it.
            Reason::Unknown => false,
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct RetryInfo {
    pub(crate) retry_delay_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct UserFacingMessage {
    pub(crate) message: Box<str>,
}

/// Response which holds client's auth secret, e.g. [`crate::scram::ServerSecret`].
/// Returned by the `/get_endpoint_access_control` API method.
#[derive(Deserialize)]
pub(crate) struct GetEndpointAccessControl {
    pub(crate) role_secret: Box<str>,
    pub(crate) allowed_ips: Option<Vec<IpPattern>>,
    pub(crate) project_id: Option<ProjectIdInt>,
    pub(crate) allowed_vpc_endpoint_ids: Option<Vec<EndpointIdInt>>,
}

/// Response which holds compute node's `host:port` pair.
/// Returned by the `/proxy_wake_compute` API method.
#[derive(Debug, Deserialize)]
pub(crate) struct WakeCompute {
    pub(crate) address: Box<str>,
    pub(crate) aux: MetricsAuxInfo,
}

/// Async response which concludes the console redirect auth flow.
/// Also known as `kickResponse` in the console.
#[derive(Debug, Deserialize)]
pub(crate) struct KickSession<'a> {
    /// Session ID is assigned by the proxy.
    pub(crate) session_id: &'a str,

    /// Compute node connection params.
    #[serde(deserialize_with = "KickSession::parse_db_info")]
    pub(crate) result: DatabaseInfo,
}

impl KickSession<'_> {
    fn parse_db_info<'de, D>(des: D) -> Result<DatabaseInfo, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum Wrapper {
            // Currently, console only reports `Success`.
            // `Failure(String)` used to be here... RIP.
            Success(DatabaseInfo),
        }

        Wrapper::deserialize(des).map(|x| match x {
            Wrapper::Success(info) => info,
        })
    }
}

/// Compute node connection params.
#[derive(Deserialize)]
pub(crate) struct DatabaseInfo {
    pub(crate) host: Box<str>,
    pub(crate) port: u16,
    pub(crate) dbname: Box<str>,
    pub(crate) user: Box<str>,
    /// Console always provides a password, but it might
    /// be inconvenient for debug with local PG instance.
    pub(crate) password: Option<Box<str>>,
    pub(crate) aux: MetricsAuxInfo,
    #[serde(default)]
    pub(crate) allowed_ips: Option<Vec<IpPattern>>,
}

// Manually implement debug to omit sensitive info.
impl fmt::Debug for DatabaseInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseInfo")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("dbname", &self.dbname)
            .field("user", &self.user)
            .field("allowed_ips", &self.allowed_ips)
            .finish_non_exhaustive()
    }
}

/// Various labels for prometheus metrics.
/// Also known as `ProxyMetricsAuxInfo` in the console.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct MetricsAuxInfo {
    pub(crate) endpoint_id: EndpointIdInt,
    pub(crate) project_id: ProjectIdInt,
    pub(crate) branch_id: BranchIdInt,
    #[serde(default)]
    pub(crate) cold_start_info: ColdStartInfo,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, FixedCardinalityLabel)]
#[serde(rename_all = "snake_case")]
pub enum ColdStartInfo {
    #[default]
    Unknown,
    /// Compute was already running
    Warm,
    #[serde(rename = "pool_hit")]
    #[label(rename = "pool_hit")]
    /// Compute was not running but there was an available VM
    VmPoolHit,
    #[serde(rename = "pool_miss")]
    #[label(rename = "pool_miss")]
    /// Compute was not running and there were no VMs available
    VmPoolMiss,

    // not provided by control plane
    /// Connection available from HTTP pool
    HttpPoolHit,
    /// Cached connection info
    WarmCached,
}

impl ColdStartInfo {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ColdStartInfo::Unknown => "unknown",
            ColdStartInfo::Warm => "warm",
            ColdStartInfo::VmPoolHit => "pool_hit",
            ColdStartInfo::VmPoolMiss => "pool_miss",
            ColdStartInfo::HttpPoolHit => "http_pool_hit",
            ColdStartInfo::WarmCached => "warm_cached",
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct EndpointJwksResponse {
    pub jwks: Vec<JwksSettings>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JwksSettings {
    pub id: String,
    pub jwks_url: url::Url,
    pub provider_name: String,
    pub jwt_audience: Option<String>,
    pub role_names: Vec<RoleNameInt>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn dummy_aux() -> serde_json::Value {
        json!({
            "endpoint_id": "endpoint",
            "project_id": "project",
            "branch_id": "branch",
            "cold_start_info": "unknown",
        })
    }

    #[test]
    fn parse_kick_session() -> anyhow::Result<()> {
        // This is what the console's kickResponse looks like.
        let json = json!({
            "session_id": "deadbeef",
            "result": {
                "Success": {
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "postgres",
                    "user": "john_doe",
                    "password": "password",
                    "aux": dummy_aux(),
                }
            }
        });
        serde_json::from_str::<KickSession<'_>>(&json.to_string())?;

        Ok(())
    }

    #[test]
    fn parse_db_info() -> anyhow::Result<()> {
        // with password
        serde_json::from_value::<DatabaseInfo>(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "password": "password",
            "aux": dummy_aux(),
        }))?;

        // without password
        serde_json::from_value::<DatabaseInfo>(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "aux": dummy_aux(),
        }))?;

        // new field (forward compatibility)
        serde_json::from_value::<DatabaseInfo>(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "project": "hello_world",
            "N.E.W": "forward compatibility check",
            "aux": dummy_aux(),
        }))?;

        // with allowed_ips
        let dbinfo = serde_json::from_value::<DatabaseInfo>(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "password": "password",
            "aux": dummy_aux(),
            "allowed_ips": ["127.0.0.1"],
        }))?;

        assert_eq!(
            dbinfo.allowed_ips,
            Some(vec![IpPattern::Single("127.0.0.1".parse()?)])
        );

        Ok(())
    }

    #[test]
    fn parse_wake_compute() -> anyhow::Result<()> {
        let json = json!({
            "address": "0.0.0.0",
            "aux": dummy_aux(),
        });
        serde_json::from_str::<WakeCompute>(&json.to_string())?;
        Ok(())
    }

    #[test]
    fn parse_get_role_secret() -> anyhow::Result<()> {
        // Empty `allowed_ips` field.
        let json = json!({
            "role_secret": "secret",
        });
        serde_json::from_str::<GetEndpointAccessControl>(&json.to_string())?;
        let json = json!({
            "role_secret": "secret",
            "allowed_ips": ["8.8.8.8"],
        });
        serde_json::from_str::<GetEndpointAccessControl>(&json.to_string())?;
        let json = json!({
            "role_secret": "secret",
            "allowed_ips": ["8.8.8.8"],
            "project_id": "project",
        });
        serde_json::from_str::<GetEndpointAccessControl>(&json.to_string())?;

        Ok(())
    }
}
