//! Global safekeeper mertics and per-timeline safekeeper metrics.

use std::{
    sync::{Arc, RwLock},
    time::{Instant, SystemTime},
};

use anyhow::Result;
use futures::Future;
use metrics::{
    core::{AtomicU64, Collector, Desc, GenericCounter, GenericGaugeVec, Opts},
    pow2_buckets,
    proto::MetricFamily,
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_pair,
    register_int_counter_pair_vec, register_int_counter_vec, register_int_gauge, Gauge, GaugeVec,
    Histogram, HistogramVec, IntCounter, IntCounterPair, IntCounterPairVec, IntCounterVec,
    IntGauge, IntGaugeVec, DISK_FSYNC_SECONDS_BUCKETS,
};
use once_cell::sync::Lazy;
use postgres_ffi::XLogSegNo;
use utils::{id::TenantTimelineId, lsn::Lsn, pageserver_feedback::PageserverFeedback};

use crate::{
    receive_wal::MSG_QUEUE_SIZE,
    state::{TimelineMemState, TimelinePersistentState},
    GlobalTimelines,
};

// Global metrics across all timelines.
pub static WRITE_WAL_BYTES: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "safekeeper_write_wal_bytes",
        "Bytes written to WAL in a single request",
        vec![
            1.0,
            10.0,
            100.0,
            1024.0,
            8192.0,
            128.0 * 1024.0,
            1024.0 * 1024.0,
            10.0 * 1024.0 * 1024.0
        ]
    )
    .expect("Failed to register safekeeper_write_wal_bytes histogram")
});
pub static WRITE_WAL_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "safekeeper_write_wal_seconds",
        "Seconds spent writing and syncing WAL to a disk in a single request",
        DISK_FSYNC_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_write_wal_seconds histogram")
});
pub static FLUSH_WAL_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "safekeeper_flush_wal_seconds",
        "Seconds spent syncing WAL to a disk (excluding segment initialization)",
        DISK_FSYNC_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_flush_wal_seconds histogram")
});
pub static PERSIST_CONTROL_FILE_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "safekeeper_persist_control_file_seconds",
        "Seconds to persist and sync control file",
        DISK_FSYNC_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_persist_control_file_seconds histogram vec")
});
pub static WAL_STORAGE_OPERATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "safekeeper_wal_storage_operation_seconds",
        "Seconds spent on WAL storage operations",
        &["operation"],
        DISK_FSYNC_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_wal_storage_operation_seconds histogram vec")
});
pub static MISC_OPERATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "safekeeper_misc_operation_seconds",
        "Seconds spent on miscellaneous operations",
        &["operation"],
        DISK_FSYNC_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_misc_operation_seconds histogram vec")
});
pub static PG_IO_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "safekeeper_pg_io_bytes_total",
        "Bytes read from or written to any PostgreSQL connection",
        &["client_az", "sk_az", "app_name", "dir", "same_az"]
    )
    .expect("Failed to register safekeeper_pg_io_bytes gauge")
});
pub static BROKER_PUSHED_UPDATES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_broker_pushed_updates_total",
        "Number of timeline updates pushed to the broker"
    )
    .expect("Failed to register safekeeper_broker_pushed_updates_total counter")
});
pub static BROKER_PULLED_UPDATES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "safekeeper_broker_pulled_updates_total",
        "Number of timeline updates pulled and processed from the broker",
        &["result"]
    )
    .expect("Failed to register safekeeper_broker_pulled_updates_total counter")
});
pub static PG_QUERIES_GAUGE: Lazy<IntCounterPairVec> = Lazy::new(|| {
    register_int_counter_pair_vec!(
        "safekeeper_pg_queries_received_total",
        "Number of queries received through pg protocol",
        "safekeeper_pg_queries_finished_total",
        "Number of queries finished through pg protocol",
        &["query"]
    )
    .expect("Failed to register safekeeper_pg_queries_finished_total counter")
});
pub static REMOVED_WAL_SEGMENTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_removed_wal_segments_total",
        "Number of WAL segments removed from the disk"
    )
    .expect("Failed to register safekeeper_removed_wal_segments_total counter")
});
pub static BACKED_UP_SEGMENTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_backed_up_segments_total",
        "Number of WAL segments backed up to the S3"
    )
    .expect("Failed to register safekeeper_backed_up_segments_total counter")
});
pub static BACKUP_ERRORS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_backup_errors_total",
        "Number of errors during backup"
    )
    .expect("Failed to register safekeeper_backup_errors_total counter")
});
pub static BROKER_PUSH_ALL_UPDATES_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "safekeeper_broker_push_update_seconds",
        "Seconds to push all timeline updates to the broker",
        DISK_FSYNC_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_broker_push_update_seconds histogram vec")
});
pub const TIMELINES_COUNT_BUCKETS: &[f64] = &[
    1.0, 10.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0,
];
pub static BROKER_ITERATION_TIMELINES: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "safekeeper_broker_iteration_timelines",
        "Count of timelines pushed to the broker in a single iteration",
        TIMELINES_COUNT_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_broker_iteration_timelines histogram vec")
});
pub static RECEIVED_PS_FEEDBACKS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_received_ps_feedbacks_total",
        "Number of pageserver feedbacks received"
    )
    .expect("Failed to register safekeeper_received_ps_feedbacks_total counter")
});
pub static PARTIAL_BACKUP_UPLOADS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "safekeeper_partial_backup_uploads_total",
        "Number of partial backup uploads to the S3",
        &["result"]
    )
    .expect("Failed to register safekeeper_partial_backup_uploads_total counter")
});
pub static PARTIAL_BACKUP_UPLOADED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_partial_backup_uploaded_bytes_total",
        "Number of bytes uploaded to the S3 during partial backup"
    )
    .expect("Failed to register safekeeper_partial_backup_uploaded_bytes_total counter")
});
pub static MANAGER_ITERATIONS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_manager_iterations_total",
        "Number of iterations of the timeline manager task"
    )
    .expect("Failed to register safekeeper_manager_iterations_total counter")
});
pub static MANAGER_ACTIVE_CHANGES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "safekeeper_manager_active_changes_total",
        "Number of timeline active status changes in the timeline manager task"
    )
    .expect("Failed to register safekeeper_manager_active_changes_total counter")
});
pub static WAL_BACKUP_TASKS: Lazy<IntCounterPair> = Lazy::new(|| {
    register_int_counter_pair!(
        "safekeeper_wal_backup_tasks_started_total",
        "Number of active WAL backup tasks",
        "safekeeper_wal_backup_tasks_finished_total",
        "Number of finished WAL backup tasks",
    )
    .expect("Failed to register safekeeper_wal_backup_tasks_finished_total counter")
});
pub static WAL_RECEIVERS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "safekeeper_wal_receivers",
        "Number of currently connected WAL receivers (i.e. connected computes)"
    )
    .expect("Failed to register safekeeper_wal_receivers")
});
pub static WAL_RECEIVER_QUEUE_DEPTH: Lazy<Histogram> = Lazy::new(|| {
    // Use powers of two buckets, but add a bucket at 0 and the max queue size to track empty and
    // full queues respectively.
    let mut buckets = pow2_buckets(1, MSG_QUEUE_SIZE);
    buckets.insert(0, 0.0);
    buckets.insert(buckets.len() - 1, (MSG_QUEUE_SIZE - 1) as f64);
    assert!(buckets.len() <= 12, "too many histogram buckets");

    register_histogram!(
        "safekeeper_wal_receiver_queue_depth",
        "Number of queued messages per WAL receiver (sampled every 5 seconds)",
        buckets
    )
    .expect("Failed to register safekeeper_wal_receiver_queue_depth histogram")
});
pub static WAL_RECEIVER_QUEUE_DEPTH_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "safekeeper_wal_receiver_queue_depth_total",
        "Total number of queued messages across all WAL receivers",
    )
    .expect("Failed to register safekeeper_wal_receiver_queue_depth_total gauge")
});
// TODO: consider adding a per-receiver queue_size histogram. This will require wrapping the Tokio
// MPSC channel to update counters on send, receive, and drop, while forwarding all other methods.
pub static WAL_RECEIVER_QUEUE_SIZE_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "safekeeper_wal_receiver_queue_size_total",
        "Total memory byte size of queued messages across all WAL receivers",
    )
    .expect("Failed to register safekeeper_wal_receiver_queue_size_total gauge")
});

// Metrics collected on operations on the storage repository.
#[derive(strum_macros::EnumString, strum_macros::Display, strum_macros::IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum EvictionEvent {
    Evict,
    Restore,
}

pub(crate) static EVICTION_EVENTS_STARTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "safekeeper_eviction_events_started_total",
        "Number of eviction state changes, incremented when they start",
        &["kind"]
    )
    .expect("Failed to register metric")
});

pub(crate) static EVICTION_EVENTS_COMPLETED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "safekeeper_eviction_events_completed_total",
        "Number of eviction state changes, incremented when they complete",
        &["kind"]
    )
    .expect("Failed to register metric")
});

pub static NUM_EVICTED_TIMELINES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "safekeeper_evicted_timelines",
        "Number of currently evicted timelines"
    )
    .expect("Failed to register metric")
});

pub const LABEL_UNKNOWN: &str = "unknown";

/// Labels for traffic metrics.
#[derive(Clone)]
struct ConnectionLabels {
    /// Availability zone of the connection origin.
    client_az: String,
    /// Availability zone of the current safekeeper.
    sk_az: String,
    /// Client application name.
    app_name: String,
}

impl ConnectionLabels {
    fn new() -> Self {
        Self {
            client_az: LABEL_UNKNOWN.to_string(),
            sk_az: LABEL_UNKNOWN.to_string(),
            app_name: LABEL_UNKNOWN.to_string(),
        }
    }

    fn build_metrics(
        &self,
    ) -> (
        GenericCounter<metrics::core::AtomicU64>,
        GenericCounter<metrics::core::AtomicU64>,
    ) {
        let same_az = match (self.client_az.as_str(), self.sk_az.as_str()) {
            (LABEL_UNKNOWN, _) | (_, LABEL_UNKNOWN) => LABEL_UNKNOWN,
            (client_az, sk_az) => {
                if client_az == sk_az {
                    "true"
                } else {
                    "false"
                }
            }
        };

        let read = PG_IO_BYTES.with_label_values(&[
            &self.client_az,
            &self.sk_az,
            &self.app_name,
            "read",
            same_az,
        ]);
        let write = PG_IO_BYTES.with_label_values(&[
            &self.client_az,
            &self.sk_az,
            &self.app_name,
            "write",
            same_az,
        ]);
        (read, write)
    }
}

struct TrafficMetricsState {
    /// Labels for traffic metrics.
    labels: ConnectionLabels,
    /// Total bytes read from this connection.
    read: GenericCounter<metrics::core::AtomicU64>,
    /// Total bytes written to this connection.
    write: GenericCounter<metrics::core::AtomicU64>,
}

/// Metrics for measuring traffic (r/w bytes) in a single PostgreSQL connection.
#[derive(Clone)]
pub struct TrafficMetrics {
    state: Arc<RwLock<TrafficMetricsState>>,
}

impl Default for TrafficMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TrafficMetrics {
    pub fn new() -> Self {
        let labels = ConnectionLabels::new();
        let (read, write) = labels.build_metrics();
        let state = TrafficMetricsState {
            labels,
            read,
            write,
        };
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub fn set_client_az(&self, value: &str) {
        let mut state = self.state.write().unwrap();
        state.labels.client_az = value.to_string();
        (state.read, state.write) = state.labels.build_metrics();
    }

    pub fn set_sk_az(&self, value: &str) {
        let mut state = self.state.write().unwrap();
        state.labels.sk_az = value.to_string();
        (state.read, state.write) = state.labels.build_metrics();
    }

    pub fn set_app_name(&self, value: &str) {
        let mut state = self.state.write().unwrap();
        state.labels.app_name = value.to_string();
        (state.read, state.write) = state.labels.build_metrics();
    }

    pub fn observe_read(&self, cnt: usize) {
        self.state.read().unwrap().read.inc_by(cnt as u64)
    }

    pub fn observe_write(&self, cnt: usize) {
        self.state.read().unwrap().write.inc_by(cnt as u64)
    }
}

/// Metrics for WalStorage in a single timeline.
#[derive(Clone, Default)]
pub struct WalStorageMetrics {
    /// How much bytes were written in total.
    write_wal_bytes: u64,
    /// How much time spent writing WAL to disk, waiting for write(2).
    write_wal_seconds: f64,
    /// How much time spent syncing WAL to disk, waiting for fsync(2).
    flush_wal_seconds: f64,
}

impl WalStorageMetrics {
    pub fn observe_write_bytes(&mut self, bytes: usize) {
        self.write_wal_bytes += bytes as u64;
        WRITE_WAL_BYTES.observe(bytes as f64);
    }

    pub fn observe_write_seconds(&mut self, seconds: f64) {
        self.write_wal_seconds += seconds;
        WRITE_WAL_SECONDS.observe(seconds);
    }

    pub fn observe_flush_seconds(&mut self, seconds: f64) {
        self.flush_wal_seconds += seconds;
        FLUSH_WAL_SECONDS.observe(seconds);
    }
}

/// Accepts async function that returns empty anyhow result, and returns the duration of its execution.
pub async fn time_io_closure<E: Into<anyhow::Error>>(
    closure: impl Future<Output = Result<(), E>>,
) -> Result<f64> {
    let start = std::time::Instant::now();
    closure.await.map_err(|e| e.into())?;
    Ok(start.elapsed().as_secs_f64())
}

/// Metrics for a single timeline.
#[derive(Clone)]
pub struct FullTimelineInfo {
    pub ttid: TenantTimelineId,
    pub ps_feedback_count: u64,
    pub last_ps_feedback: PageserverFeedback,
    pub wal_backup_active: bool,
    pub timeline_is_active: bool,
    pub num_computes: u32,
    pub last_removed_segno: XLogSegNo,

    pub epoch_start_lsn: Lsn,
    pub mem_state: TimelineMemState,
    pub persisted_state: TimelinePersistentState,

    pub flush_lsn: Lsn,

    pub wal_storage: WalStorageMetrics,
}

/// Collects metrics for all active timelines.
pub struct TimelineCollector {
    global_timelines: Arc<GlobalTimelines>,
    descs: Vec<Desc>,
    commit_lsn: GenericGaugeVec<AtomicU64>,
    backup_lsn: GenericGaugeVec<AtomicU64>,
    flush_lsn: GenericGaugeVec<AtomicU64>,
    epoch_start_lsn: GenericGaugeVec<AtomicU64>,
    peer_horizon_lsn: GenericGaugeVec<AtomicU64>,
    remote_consistent_lsn: GenericGaugeVec<AtomicU64>,
    ps_last_received_lsn: GenericGaugeVec<AtomicU64>,
    feedback_last_time_seconds: GenericGaugeVec<AtomicU64>,
    ps_feedback_count: GenericGaugeVec<AtomicU64>,
    timeline_active: GenericGaugeVec<AtomicU64>,
    wal_backup_active: GenericGaugeVec<AtomicU64>,
    connected_computes: IntGaugeVec,
    disk_usage: GenericGaugeVec<AtomicU64>,
    acceptor_term: GenericGaugeVec<AtomicU64>,
    written_wal_bytes: GenericGaugeVec<AtomicU64>,
    written_wal_seconds: GaugeVec,
    flushed_wal_seconds: GaugeVec,
    collect_timeline_metrics: Gauge,
    timelines_count: IntGauge,
    active_timelines_count: IntGauge,
}

impl TimelineCollector {
    pub fn new(global_timelines: Arc<GlobalTimelines>) -> TimelineCollector {
        let mut descs = Vec::new();

        let commit_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_commit_lsn",
                "Current commit_lsn (not necessarily persisted to disk), grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(commit_lsn.desc().into_iter().cloned());

        let backup_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_backup_lsn",
                "Current backup_lsn, up to which WAL is backed up, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(backup_lsn.desc().into_iter().cloned());

        let flush_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_flush_lsn",
                "Current flush_lsn, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(flush_lsn.desc().into_iter().cloned());

        let epoch_start_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_epoch_start_lsn",
                "Point since which compute generates new WAL in the current consensus term",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(epoch_start_lsn.desc().into_iter().cloned());

        let peer_horizon_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_peer_horizon_lsn",
                "LSN of the most lagging safekeeper",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(peer_horizon_lsn.desc().into_iter().cloned());

        let remote_consistent_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_remote_consistent_lsn",
                "LSN which is persisted to the remote storage in pageserver",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(remote_consistent_lsn.desc().into_iter().cloned());

        let ps_last_received_lsn = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_ps_last_received_lsn",
                "Last LSN received by the pageserver, acknowledged in the feedback",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(ps_last_received_lsn.desc().into_iter().cloned());

        let feedback_last_time_seconds = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_feedback_last_time_seconds",
                "Timestamp of the last feedback from the pageserver",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(feedback_last_time_seconds.desc().into_iter().cloned());

        let ps_feedback_count = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_ps_feedback_count_total",
                "Number of feedbacks received from the pageserver",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();

        let timeline_active = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_timeline_active",
                "Reports 1 for active timelines, 0 for inactive",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(timeline_active.desc().into_iter().cloned());

        let wal_backup_active = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_wal_backup_active",
                "Reports 1 for timelines with active WAL backup, 0 otherwise",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(wal_backup_active.desc().into_iter().cloned());

        let connected_computes = IntGaugeVec::new(
            Opts::new(
                "safekeeper_connected_computes",
                "Number of active compute connections",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(connected_computes.desc().into_iter().cloned());

        let disk_usage = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_disk_usage_bytes",
                "Estimated disk space used to store WAL segments",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(disk_usage.desc().into_iter().cloned());

        let acceptor_term = GenericGaugeVec::new(
            Opts::new("safekeeper_acceptor_term", "Current consensus term"),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(acceptor_term.desc().into_iter().cloned());

        let written_wal_bytes = GenericGaugeVec::new(
            Opts::new(
                "safekeeper_written_wal_bytes_total",
                "Number of WAL bytes written to disk, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(written_wal_bytes.desc().into_iter().cloned());

        let written_wal_seconds = GaugeVec::new(
            Opts::new(
                "safekeeper_written_wal_seconds_total",
                "Total time spent in write(2) writing WAL to disk, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(written_wal_seconds.desc().into_iter().cloned());

        let flushed_wal_seconds = GaugeVec::new(
            Opts::new(
                "safekeeper_flushed_wal_seconds_total",
                "Total time spent in fsync(2) flushing WAL to disk, grouped by timeline",
            ),
            &["tenant_id", "timeline_id"],
        )
        .unwrap();
        descs.extend(flushed_wal_seconds.desc().into_iter().cloned());

        let collect_timeline_metrics = Gauge::new(
            "safekeeper_collect_timeline_metrics_seconds",
            "Time spent collecting timeline metrics, including obtaining mutex lock for all timelines",
        )
        .unwrap();
        descs.extend(collect_timeline_metrics.desc().into_iter().cloned());

        let timelines_count = IntGauge::new(
            "safekeeper_timelines",
            "Total number of timelines loaded in-memory",
        )
        .unwrap();
        descs.extend(timelines_count.desc().into_iter().cloned());

        let active_timelines_count = IntGauge::new(
            "safekeeper_active_timelines",
            "Total number of active timelines",
        )
        .unwrap();
        descs.extend(active_timelines_count.desc().into_iter().cloned());

        TimelineCollector {
            global_timelines,
            descs,
            commit_lsn,
            backup_lsn,
            flush_lsn,
            epoch_start_lsn,
            peer_horizon_lsn,
            remote_consistent_lsn,
            ps_last_received_lsn,
            feedback_last_time_seconds,
            ps_feedback_count,
            timeline_active,
            wal_backup_active,
            connected_computes,
            disk_usage,
            acceptor_term,
            written_wal_bytes,
            written_wal_seconds,
            flushed_wal_seconds,
            collect_timeline_metrics,
            timelines_count,
            active_timelines_count,
        }
    }
}

impl Collector for TimelineCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let start_collecting = Instant::now();

        // reset all metrics to clean up inactive timelines
        self.commit_lsn.reset();
        self.backup_lsn.reset();
        self.flush_lsn.reset();
        self.epoch_start_lsn.reset();
        self.peer_horizon_lsn.reset();
        self.remote_consistent_lsn.reset();
        self.ps_last_received_lsn.reset();
        self.feedback_last_time_seconds.reset();
        self.ps_feedback_count.reset();
        self.timeline_active.reset();
        self.wal_backup_active.reset();
        self.connected_computes.reset();
        self.disk_usage.reset();
        self.acceptor_term.reset();
        self.written_wal_bytes.reset();
        self.written_wal_seconds.reset();
        self.flushed_wal_seconds.reset();

        let timelines_count = self.global_timelines.get_all().len();
        let mut active_timelines_count = 0;

        // Prometheus Collector is sync, and data is stored under async lock. To
        // bridge the gap with a crutch, collect data in spawned thread with
        // local tokio runtime.
        let global_timelines = self.global_timelines.clone();
        let infos = std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("failed to create rt");
            rt.block_on(collect_timeline_metrics(global_timelines))
        })
        .join()
        .expect("collect_timeline_metrics thread panicked");

        for tli in &infos {
            let tenant_id = tli.ttid.tenant_id.to_string();
            let timeline_id = tli.ttid.timeline_id.to_string();
            let labels = &[tenant_id.as_str(), timeline_id.as_str()];

            if tli.timeline_is_active {
                active_timelines_count += 1;
            }

            self.commit_lsn
                .with_label_values(labels)
                .set(tli.mem_state.commit_lsn.into());
            self.backup_lsn
                .with_label_values(labels)
                .set(tli.mem_state.backup_lsn.into());
            self.flush_lsn
                .with_label_values(labels)
                .set(tli.flush_lsn.into());
            self.epoch_start_lsn
                .with_label_values(labels)
                .set(tli.epoch_start_lsn.into());
            self.peer_horizon_lsn
                .with_label_values(labels)
                .set(tli.mem_state.peer_horizon_lsn.into());
            self.remote_consistent_lsn
                .with_label_values(labels)
                .set(tli.mem_state.remote_consistent_lsn.into());
            self.timeline_active
                .with_label_values(labels)
                .set(tli.timeline_is_active as u64);
            self.wal_backup_active
                .with_label_values(labels)
                .set(tli.wal_backup_active as u64);
            self.connected_computes
                .with_label_values(labels)
                .set(tli.num_computes as i64);
            self.acceptor_term
                .with_label_values(labels)
                .set(tli.persisted_state.acceptor_state.term);
            self.written_wal_bytes
                .with_label_values(labels)
                .set(tli.wal_storage.write_wal_bytes);
            self.written_wal_seconds
                .with_label_values(labels)
                .set(tli.wal_storage.write_wal_seconds);
            self.flushed_wal_seconds
                .with_label_values(labels)
                .set(tli.wal_storage.flush_wal_seconds);

            self.ps_last_received_lsn
                .with_label_values(labels)
                .set(tli.last_ps_feedback.last_received_lsn.0);
            self.ps_feedback_count
                .with_label_values(labels)
                .set(tli.ps_feedback_count);
            if let Ok(unix_time) = tli
                .last_ps_feedback
                .replytime
                .duration_since(SystemTime::UNIX_EPOCH)
            {
                self.feedback_last_time_seconds
                    .with_label_values(labels)
                    .set(unix_time.as_secs());
            }

            if tli.last_removed_segno != 0 {
                let segno_count = tli
                    .flush_lsn
                    .segment_number(tli.persisted_state.server.wal_seg_size as usize)
                    - tli.last_removed_segno;
                let disk_usage_bytes = segno_count * tli.persisted_state.server.wal_seg_size as u64;
                self.disk_usage
                    .with_label_values(labels)
                    .set(disk_usage_bytes);
            }
        }

        // collect MetricFamilys.
        let mut mfs = Vec::new();
        mfs.extend(self.commit_lsn.collect());
        mfs.extend(self.backup_lsn.collect());
        mfs.extend(self.flush_lsn.collect());
        mfs.extend(self.epoch_start_lsn.collect());
        mfs.extend(self.peer_horizon_lsn.collect());
        mfs.extend(self.remote_consistent_lsn.collect());
        mfs.extend(self.ps_last_received_lsn.collect());
        mfs.extend(self.feedback_last_time_seconds.collect());
        mfs.extend(self.ps_feedback_count.collect());
        mfs.extend(self.timeline_active.collect());
        mfs.extend(self.wal_backup_active.collect());
        mfs.extend(self.connected_computes.collect());
        mfs.extend(self.disk_usage.collect());
        mfs.extend(self.acceptor_term.collect());
        mfs.extend(self.written_wal_bytes.collect());
        mfs.extend(self.written_wal_seconds.collect());
        mfs.extend(self.flushed_wal_seconds.collect());

        // report time it took to collect all info
        let elapsed = start_collecting.elapsed().as_secs_f64();
        self.collect_timeline_metrics.set(elapsed);
        mfs.extend(self.collect_timeline_metrics.collect());

        // report total number of timelines
        self.timelines_count.set(timelines_count as i64);
        mfs.extend(self.timelines_count.collect());

        self.active_timelines_count
            .set(active_timelines_count as i64);
        mfs.extend(self.active_timelines_count.collect());

        mfs
    }
}

async fn collect_timeline_metrics(global_timelines: Arc<GlobalTimelines>) -> Vec<FullTimelineInfo> {
    let mut res = vec![];
    let active_timelines = global_timelines.get_global_broker_active_set().get_all();

    for tli in active_timelines {
        if let Some(info) = tli.info_for_metrics().await {
            res.push(info);
        }
    }
    res
}
