use metrics::core::Collector;
use metrics::proto::MetricFamily;
use metrics::{register_int_counter_vec, register_uint_gauge_vec, IntCounterVec, UIntGaugeVec};
use once_cell::sync::Lazy;

pub(crate) static INSTALLED_EXTENSIONS: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "compute_installed_extensions",
        "Number of databases where the version of extension is installed",
        &["extension_name", "version", "owned_by_superuser"]
    )
    .expect("failed to define a metric")
});

// Normally, any HTTP API request is described by METHOD (e.g. GET, POST, etc.) + PATH,
// but for all our APIs we defined a 'slug'/method/operationId in the OpenAPI spec.
// And it's fair to call it a 'RPC' (Remote Procedure Call).
pub enum CPlaneRequestRPC {
    GetSpec,
}

impl CPlaneRequestRPC {
    pub fn as_str(&self) -> &str {
        match self {
            CPlaneRequestRPC::GetSpec => "GetSpec",
        }
    }
}

pub const UNKNOWN_HTTP_STATUS: &str = "unknown";

pub(crate) static CPLANE_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "compute_ctl_cplane_requests_total",
        "Total number of control plane requests made by compute_ctl by status",
        &["rpc", "http_status"]
    )
    .expect("failed to define a metric")
});

/// Total number of failed database migrations. Per-compute, this is actually a boolean metric,
/// either empty or with a single value (1, migration_id) because we stop at the first failure.
/// Yet, the sum over the fleet will provide the total number of failures.
pub(crate) static DB_MIGRATION_FAILED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "compute_ctl_db_migration_failed_total",
        "Total number of failed database migrations",
        &["migration_id"]
    )
    .expect("failed to define a metric")
});

pub(crate) static REMOTE_EXT_REQUESTS_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "compute_ctl_remote_ext_requests_total",
        "Total number of requests made by compute_ctl to download extensions from S3 proxy by status",
        // Do not use any labels like extension name yet.
        // We can add them later if needed.
        &["http_status"]
    )
    .expect("failed to define a metric")
});

pub fn collect() -> Vec<MetricFamily> {
    let mut metrics = INSTALLED_EXTENSIONS.collect();
    metrics.extend(CPLANE_REQUESTS_TOTAL.collect());
    metrics.extend(REMOTE_EXT_REQUESTS_TOTAL.collect());
    metrics.extend(DB_MIGRATION_FAILED.collect());
    metrics
}
