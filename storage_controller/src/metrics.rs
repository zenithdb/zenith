//!
//! This module provides metric definitions for the storage controller.
//!
//! All metrics are grouped in [`StorageControllerMetricGroup`]. [`StorageControllerMetrics`] holds
//! the mentioned metrics and their encoder. It's globally available via the [`METRICS_REGISTRY`]
//! constant.
//!
//! The rest of the code defines label group types and deals with converting outer types to labels.
//!
use bytes::Bytes;
use measured::{label::LabelValue, metric::histogram, FixedCardinalityLabel, MetricGroup};
use metrics::NeonMetrics;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use strum::IntoEnumIterator;

use crate::{
    persistence::{DatabaseError, DatabaseOperation},
    service::LeadershipStatus,
};

pub(crate) static METRICS_REGISTRY: Lazy<StorageControllerMetrics> =
    Lazy::new(StorageControllerMetrics::default);

pub fn preinitialize_metrics() {
    Lazy::force(&METRICS_REGISTRY);
}

pub(crate) struct StorageControllerMetrics {
    pub(crate) metrics_group: StorageControllerMetricGroup,
    encoder: Mutex<measured::text::BufferedTextEncoder>,
}

#[derive(measured::MetricGroup)]
#[metric(new())]
pub(crate) struct StorageControllerMetricGroup {
    /// Count of how many times we spawn a reconcile task
    pub(crate) storage_controller_reconcile_spawn: measured::Counter,

    /// Size of the in-memory map of tenant shards
    pub(crate) storage_controller_tenant_shards: measured::Gauge,

    /// Size of the in-memory map of pageserver_nodes
    pub(crate) storage_controller_pageserver_nodes: measured::Gauge,

    /// Reconciler tasks completed, broken down by success/failure/cancelled
    pub(crate) storage_controller_reconcile_complete:
        measured::CounterVec<ReconcileCompleteLabelGroupSet>,

    /// Count of how many times we make an optimization change to a tenant's scheduling
    pub(crate) storage_controller_schedule_optimization: measured::Counter,

    /// How many shards are not scheduled into their preferred AZ
    pub(crate) storage_controller_schedule_az_violation: measured::Gauge,

    /// How many shard locations (secondary or attached) on each node
    pub(crate) storage_controller_node_shards: measured::GaugeVec<NodeLabelGroupSet>,

    /// How many _attached_ shard locations on each node
    pub(crate) storage_controller_node_attached_shards: measured::GaugeVec<NodeLabelGroupSet>,

    /// How many _home_ shard locations on each node (i.e. the node's AZ matches the shard's
    /// preferred AZ)
    pub(crate) storage_controller_node_home_shards: measured::GaugeVec<NodeLabelGroupSet>,

    /// How many shards would like to reconcile but were blocked by concurrency limits
    pub(crate) storage_controller_pending_reconciles: measured::Gauge,

    /// HTTP request status counters for handled requests
    pub(crate) storage_controller_http_request_status:
        measured::CounterVec<HttpRequestStatusLabelGroupSet>,

    /// HTTP request handler latency across all status codes
    #[metric(metadata = histogram::Thresholds::exponential_buckets(0.1, 2.0))]
    pub(crate) storage_controller_http_request_latency:
        measured::HistogramVec<HttpRequestLatencyLabelGroupSet, 5>,

    /// Count of HTTP requests to the pageserver that resulted in an error,
    /// broken down by the pageserver node id, request name and method
    pub(crate) storage_controller_pageserver_request_error:
        measured::CounterVec<PageserverRequestLabelGroupSet>,

    /// Count of HTTP requests to the safekeeper that resulted in an error,
    /// broken down by the safekeeper node id, request name and method
    pub(crate) storage_controller_safekeeper_request_error:
        measured::CounterVec<PageserverRequestLabelGroupSet>,

    /// Latency of HTTP requests to the pageserver, broken down by pageserver
    /// node id, request name and method. This include both successful and unsuccessful
    /// requests.
    #[metric(metadata = histogram::Thresholds::exponential_buckets(0.1, 2.0))]
    pub(crate) storage_controller_pageserver_request_latency:
        measured::HistogramVec<PageserverRequestLabelGroupSet, 5>,

    /// Latency of HTTP requests to the safekeeper, broken down by safekeeper
    /// node id, request name and method. This include both successful and unsuccessful
    /// requests.
    #[metric(metadata = histogram::Thresholds::exponential_buckets(0.1, 2.0))]
    pub(crate) storage_controller_safekeeper_request_latency:
        measured::HistogramVec<PageserverRequestLabelGroupSet, 5>,

    /// Count of pass-through HTTP requests to the pageserver that resulted in an error,
    /// broken down by the pageserver node id, request name and method
    pub(crate) storage_controller_passthrough_request_error:
        measured::CounterVec<PageserverRequestLabelGroupSet>,

    /// Latency of pass-through HTTP requests to the pageserver, broken down by pageserver
    /// node id, request name and method. This include both successful and unsuccessful
    /// requests.
    #[metric(metadata = histogram::Thresholds::exponential_buckets(0.1, 2.0))]
    pub(crate) storage_controller_passthrough_request_latency:
        measured::HistogramVec<PageserverRequestLabelGroupSet, 5>,

    /// Count of errors in database queries, broken down by error type and operation.
    pub(crate) storage_controller_database_query_error:
        measured::CounterVec<DatabaseQueryErrorLabelGroupSet>,

    /// Latency of database queries, broken down by operation.
    #[metric(metadata = histogram::Thresholds::exponential_buckets(0.1, 2.0))]
    pub(crate) storage_controller_database_query_latency:
        measured::HistogramVec<DatabaseQueryLatencyLabelGroupSet, 5>,

    pub(crate) storage_controller_leadership_status: measured::GaugeVec<LeadershipStatusGroupSet>,

    /// HTTP request status counters for handled requests
    pub(crate) storage_controller_reconcile_long_running:
        measured::CounterVec<ReconcileLongRunningLabelGroupSet>,
}

impl StorageControllerMetrics {
    pub(crate) fn encode(&self, neon_metrics: &NeonMetrics) -> Bytes {
        let mut encoder = self.encoder.lock().unwrap();
        neon_metrics
            .collect_group_into(&mut *encoder)
            .unwrap_or_else(|infallible| match infallible {});
        self.metrics_group
            .collect_group_into(&mut *encoder)
            .unwrap_or_else(|infallible| match infallible {});
        encoder.finish()
    }
}

impl Default for StorageControllerMetrics {
    fn default() -> Self {
        let mut metrics_group = StorageControllerMetricGroup::new();
        metrics_group
            .storage_controller_reconcile_complete
            .init_all_dense();

        Self {
            metrics_group,
            encoder: Mutex::new(measured::text::BufferedTextEncoder::new()),
        }
    }
}

#[derive(measured::LabelGroup, Clone)]
#[label(set = NodeLabelGroupSet)]
pub(crate) struct NodeLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) az: &'a str,
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) node_id: &'a str,
}

#[derive(measured::LabelGroup)]
#[label(set = ReconcileCompleteLabelGroupSet)]
pub(crate) struct ReconcileCompleteLabelGroup {
    pub(crate) status: ReconcileOutcome,
}

#[derive(measured::LabelGroup)]
#[label(set = HttpRequestStatusLabelGroupSet)]
pub(crate) struct HttpRequestStatusLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) path: &'a str,
    pub(crate) method: Method,
    pub(crate) status: StatusCode,
}

#[derive(measured::LabelGroup)]
#[label(set = HttpRequestLatencyLabelGroupSet)]
pub(crate) struct HttpRequestLatencyLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) path: &'a str,
    pub(crate) method: Method,
}

#[derive(measured::LabelGroup, Clone)]
#[label(set = PageserverRequestLabelGroupSet)]
pub(crate) struct PageserverRequestLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) pageserver_id: &'a str,
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) path: &'a str,
    pub(crate) method: Method,
}

#[derive(measured::LabelGroup)]
#[label(set = DatabaseQueryErrorLabelGroupSet)]
pub(crate) struct DatabaseQueryErrorLabelGroup {
    pub(crate) error_type: DatabaseErrorLabel,
    pub(crate) operation: DatabaseOperation,
}

#[derive(measured::LabelGroup)]
#[label(set = DatabaseQueryLatencyLabelGroupSet)]
pub(crate) struct DatabaseQueryLatencyLabelGroup {
    pub(crate) operation: DatabaseOperation,
}

#[derive(measured::LabelGroup)]
#[label(set = LeadershipStatusGroupSet)]
pub(crate) struct LeadershipStatusGroup {
    pub(crate) status: LeadershipStatus,
}

#[derive(measured::LabelGroup, Clone)]
#[label(set = ReconcileLongRunningLabelGroupSet)]
pub(crate) struct ReconcileLongRunningLabelGroup<'a> {
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) tenant_id: &'a str,
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) shard_number: &'a str,
    #[label(dynamic_with = lasso::ThreadedRodeo, default)]
    pub(crate) sequence: &'a str,
}

#[derive(FixedCardinalityLabel, Clone, Copy)]
pub(crate) enum ReconcileOutcome {
    #[label(rename = "ok")]
    Success,
    Error,
    Cancel,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub(crate) enum Method {
    Get,
    Put,
    Post,
    Delete,
    Other,
}

impl From<hyper::Method> for Method {
    fn from(value: hyper::Method) -> Self {
        if value == hyper::Method::GET {
            Method::Get
        } else if value == hyper::Method::PUT {
            Method::Put
        } else if value == hyper::Method::POST {
            Method::Post
        } else if value == hyper::Method::DELETE {
            Method::Delete
        } else {
            Method::Other
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct StatusCode(pub(crate) hyper::http::StatusCode);

impl LabelValue for StatusCode {
    fn visit<V: measured::label::LabelVisitor>(&self, v: V) -> V::Output {
        v.write_int(self.0.as_u16() as i64)
    }
}

impl FixedCardinalityLabel for StatusCode {
    fn cardinality() -> usize {
        (100..1000).len()
    }

    fn encode(&self) -> usize {
        self.0.as_u16() as usize
    }

    fn decode(value: usize) -> Self {
        Self(hyper::http::StatusCode::from_u16(u16::try_from(value).unwrap()).unwrap())
    }
}

#[derive(FixedCardinalityLabel, Clone, Copy)]
pub(crate) enum DatabaseErrorLabel {
    Query,
    Connection,
    ConnectionPool,
    Logical,
    Migration,
}

impl DatabaseError {
    pub(crate) fn error_label(&self) -> DatabaseErrorLabel {
        match self {
            Self::Query(_) => DatabaseErrorLabel::Query,
            Self::Connection(_) => DatabaseErrorLabel::Connection,
            Self::ConnectionPool(_) => DatabaseErrorLabel::ConnectionPool,
            Self::Logical(_) => DatabaseErrorLabel::Logical,
            Self::Migration(_) => DatabaseErrorLabel::Migration,
        }
    }
}

/// Update the leadership status metric gauges to reflect the requested status
pub(crate) fn update_leadership_status(status: LeadershipStatus) {
    let status_metric = &METRICS_REGISTRY
        .metrics_group
        .storage_controller_leadership_status;

    for s in LeadershipStatus::iter() {
        if s == status {
            status_metric.set(LeadershipStatusGroup { status: s }, 1);
        } else {
            status_metric.set(LeadershipStatusGroup { status: s }, 0);
        }
    }
}
