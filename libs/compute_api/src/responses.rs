//! Structs representing the JSON formats used in the compute_ctl's HTTP API.

use std::fmt::Display;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

use crate::{
    privilege::Privilege,
    spec::{ComputeSpec, Database, ExtVersion, PgIdent, Role},
};

#[derive(Serialize, Debug, Deserialize)]
pub struct GenericAPIError {
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InfoResponse {
    pub num_cpus: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExtensionInstallResponse {
    pub extension: PgIdent,
    pub version: ExtVersion,
}

/// Response of the /status API
#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeStatusResponse {
    pub start_time: DateTime<Utc>,
    pub tenant: Option<String>,
    pub timeline: Option<String>,
    pub status: ComputeStatus,
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(Serialize, Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    // Spec wasn't provided at start, waiting for it to be
    // provided by control-plane.
    Empty,
    // Compute configuration was requested.
    ConfigurationPending,
    // Compute node has spec and initial startup and
    // configuration is in progress.
    Init,
    // Compute is configured and running.
    Running,
    // New spec is being applied.
    Configuration,
    // Either startup or configuration failed,
    // compute will exit soon or is waiting for
    // control-plane to terminate it.
    Failed,
    // Termination requested
    TerminationPending,
    // Terminated Postgres
    Terminated,
}

impl Display for ComputeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComputeStatus::Empty => f.write_str("empty"),
            ComputeStatus::ConfigurationPending => f.write_str("configuration-pending"),
            ComputeStatus::Init => f.write_str("init"),
            ComputeStatus::Running => f.write_str("running"),
            ComputeStatus::Configuration => f.write_str("configuration"),
            ComputeStatus::Failed => f.write_str("failed"),
            ComputeStatus::TerminationPending => f.write_str("termination-pending"),
            ComputeStatus::Terminated => f.write_str("terminated"),
        }
    }
}

pub fn rfc3339_serialize<S>(x: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(x) = x {
        x.to_rfc3339().serialize(s)
    } else {
        s.serialize_none()
    }
}

/// Response of the /metrics.json API
#[derive(Clone, Debug, Default, Serialize)]
pub struct ComputeMetrics {
    /// Time spent waiting in pool
    pub wait_for_spec_ms: u64,

    /// Time spent checking if safekeepers are synced
    pub sync_sk_check_ms: u64,

    /// Time spent syncing safekeepers (walproposer.c).
    /// In most cases this should be zero.
    pub sync_safekeepers_ms: u64,

    /// Time it took to establish a pg connection to the pageserver.
    /// This is two roundtrips, so it's a good proxy for compute-pageserver
    /// latency. The latency is usually 0.2ms, but it's not safe to assume
    /// that.
    pub pageserver_connect_micros: u64,

    /// Time to get basebackup from pageserver and write it to disk.
    pub basebackup_ms: u64,

    /// Compressed size of basebackup received.
    pub basebackup_bytes: u64,

    /// Time spent starting potgres. This includes initialization of shared
    /// buffers, preloading extensions, and other pg operations.
    pub start_postgres_ms: u64,

    /// Time spent applying pg catalog updates that were made in the console
    /// UI. This should be 0 when startup time matters, since cplane tries
    /// to do these updates eagerly, and passes the skip_pg_catalog_updates
    /// when it's safe to skip this step.
    pub config_ms: u64,

    /// Total time, from when we receive the spec to when we're ready to take
    /// pg connections.
    pub total_startup_ms: u64,
    pub load_ext_ms: u64,
    pub num_ext_downloaded: u64,
    pub largest_ext_size: u64, // these are measured in bytes
    pub total_ext_download_size: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CatalogObjects {
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,
}

/// Response of the `/computes/{compute_id}/spec` control-plane API.
/// This is not actually a compute API response, so consider moving
/// to a different place.
#[derive(Deserialize, Debug)]
pub struct ControlPlaneSpecResponse {
    pub spec: Option<ComputeSpec>,
    pub status: ControlPlaneComputeStatus,
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneComputeStatus {
    // Compute is known to control-plane, but it's not
    // yet attached to any timeline / endpoint.
    Empty,
    // Compute is attached to some timeline / endpoint and
    // should be able to start with provided spec.
    Attached,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct InstalledExtension {
    pub extname: String,
    pub version: String,
    pub n_databases: u32, // Number of databases using this extension
    pub owned_by_superuser: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct InstalledExtensions {
    pub extensions: Vec<InstalledExtension>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ExtensionInstallResult {
    pub extension: PgIdent,
    pub version: ExtVersion,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct SetRoleGrantsResponse {
    pub database: PgIdent,
    pub schema: PgIdent,
    pub privileges: Vec<Privilege>,
    pub role: PgIdent,
}
