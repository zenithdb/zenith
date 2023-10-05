use enum_map::EnumMap;
use metrics::metric_vec_duration::DurationResultObserver;
use metrics::{
    register_counter_vec, register_gauge_vec, register_histogram, register_histogram_vec,
    register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
    register_uint_gauge, register_uint_gauge_vec, Counter, CounterVec, GaugeVec, Histogram,
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, UIntGauge, UIntGaugeVec,
};
use once_cell::sync::Lazy;
use strum::{EnumCount, IntoEnumIterator, VariantNames};
use strum_macros::{EnumVariantNames, IntoStaticStr};
use utils::id::{TenantId, TimelineId};

/// Prometheus histogram buckets (in seconds) for operations in the critical
/// path. In other words, operations that directly affect that latency of user
/// queries.
///
/// The buckets capture the majority of latencies in the microsecond and
/// millisecond range but also extend far enough up to distinguish "bad" from
/// "really bad".
const CRITICAL_OP_BUCKETS: &[f64] = &[
    0.000_001, 0.000_010, 0.000_100, // 1 us, 10 us, 100 us
    0.001_000, 0.010_000, 0.100_000, // 1 ms, 10 ms, 100 ms
    1.0, 10.0, 100.0, // 1 s, 10 s, 100 s
];

// Metrics collected on operations on the storage repository.
#[derive(Debug, EnumVariantNames, IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub enum StorageTimeOperation {
    #[strum(serialize = "layer flush")]
    LayerFlush,

    #[strum(serialize = "compact")]
    Compact,

    #[strum(serialize = "create images")]
    CreateImages,

    #[strum(serialize = "logical size")]
    LogicalSize,

    #[strum(serialize = "imitate logical size")]
    ImitateLogicalSize,

    #[strum(serialize = "load layer map")]
    LoadLayerMap,

    #[strum(serialize = "gc")]
    Gc,

    #[strum(serialize = "create tenant")]
    CreateTenant,
}

pub static STORAGE_TIME_SUM_PER_TIMELINE: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "pageserver_storage_operations_seconds_sum",
        "Total time spent on storage operations with operation, tenant and timeline dimensions",
        &["operation", "tenant_id", "timeline_id"],
    )
    .expect("failed to define a metric")
});

pub static STORAGE_TIME_COUNT_PER_TIMELINE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_storage_operations_seconds_count",
        "Count of storage operations with operation, tenant and timeline dimensions",
        &["operation", "tenant_id", "timeline_id"],
    )
    .expect("failed to define a metric")
});

// Buckets for background operations like compaction, GC, size calculation
const STORAGE_OP_BUCKETS: &[f64] = &[0.010, 0.100, 1.0, 10.0, 100.0, 1000.0];

pub(crate) static STORAGE_TIME_GLOBAL: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_storage_operations_seconds_global",
        "Time spent on storage operations",
        &["operation"],
        STORAGE_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

pub(crate) static READ_NUM_FS_LAYERS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_read_num_fs_layers",
        "Number of persistent layers accessed for processing a read request, including those in the cache",
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 10.0, 20.0, 50.0, 100.0],
    )
    .expect("failed to define a metric")
});

// Metrics collected on operations on the storage repository.

pub(crate) struct ReconstructTimeMetrics {
    ok: Histogram,
    err: Histogram,
}

pub(crate) static RECONSTRUCT_TIME: Lazy<ReconstructTimeMetrics> = Lazy::new(|| {
    let inner = register_histogram_vec!(
        "pageserver_getpage_reconstruct_seconds",
        "Time spent in reconstruct_value (reconstruct a page from deltas)",
        &["result"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric");
    ReconstructTimeMetrics {
        ok: inner.get_metric_with_label_values(&["ok"]).unwrap(),
        err: inner.get_metric_with_label_values(&["err"]).unwrap(),
    }
});

impl ReconstructTimeMetrics {
    pub(crate) fn for_result<T, E>(&self, result: &Result<T, E>) -> &Histogram {
        match result {
            Ok(_) => &self.ok,
            Err(_) => &self.err,
        }
    }
}

pub(crate) static MATERIALIZED_PAGE_CACHE_HIT_DIRECT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_materialized_cache_hits_direct_total",
        "Number of cache hits from materialized page cache without redo",
    )
    .expect("failed to define a metric")
});

pub(crate) static GET_RECONSTRUCT_DATA_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_getpage_get_reconstruct_data_seconds",
        "Time spent in get_reconstruct_value_data",
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

pub(crate) static MATERIALIZED_PAGE_CACHE_HIT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_materialized_cache_hits_total",
        "Number of cache hits from materialized page cache",
    )
    .expect("failed to define a metric")
});

pub struct PageCacheMetricsForTaskKind {
    pub read_accesses_materialized_page: IntCounter,
    pub read_accesses_immutable: IntCounter,

    pub read_hits_immutable: IntCounter,
    pub read_hits_materialized_page_exact: IntCounter,
    pub read_hits_materialized_page_older_lsn: IntCounter,
}

pub struct PageCacheMetrics {
    map: EnumMap<TaskKind, EnumMap<PageContentKind, PageCacheMetricsForTaskKind>>,
}

static PAGE_CACHE_READ_HITS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_page_cache_read_hits_total",
        "Number of read accesses to the page cache that hit",
        &["task_kind", "key_kind", "content_kind", "hit_kind"]
    )
    .expect("failed to define a metric")
});

static PAGE_CACHE_READ_ACCESSES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_page_cache_read_accesses_total",
        "Number of read accesses to the page cache",
        &["task_kind", "key_kind", "content_kind"]
    )
    .expect("failed to define a metric")
});

pub static PAGE_CACHE: Lazy<PageCacheMetrics> = Lazy::new(|| PageCacheMetrics {
    map: EnumMap::from_array(std::array::from_fn(|task_kind| {
        let task_kind = <TaskKind as enum_map::Enum>::from_usize(task_kind);
        let task_kind: &'static str = task_kind.into();
        EnumMap::from_array(std::array::from_fn(|content_kind| {
            let content_kind = <PageContentKind as enum_map::Enum>::from_usize(content_kind);
            let content_kind: &'static str = content_kind.into();
            PageCacheMetricsForTaskKind {
                read_accesses_materialized_page: {
                    PAGE_CACHE_READ_ACCESSES
                        .get_metric_with_label_values(&[
                            task_kind,
                            "materialized_page",
                            content_kind,
                        ])
                        .unwrap()
                },

                read_accesses_immutable: {
                    PAGE_CACHE_READ_ACCESSES
                        .get_metric_with_label_values(&[task_kind, "immutable", content_kind])
                        .unwrap()
                },

                read_hits_immutable: {
                    PAGE_CACHE_READ_HITS
                        .get_metric_with_label_values(&[task_kind, "immutable", content_kind, "-"])
                        .unwrap()
                },

                read_hits_materialized_page_exact: {
                    PAGE_CACHE_READ_HITS
                        .get_metric_with_label_values(&[
                            task_kind,
                            "materialized_page",
                            content_kind,
                            "exact",
                        ])
                        .unwrap()
                },

                read_hits_materialized_page_older_lsn: {
                    PAGE_CACHE_READ_HITS
                        .get_metric_with_label_values(&[
                            task_kind,
                            "materialized_page",
                            content_kind,
                            "older_lsn",
                        ])
                        .unwrap()
                },
            }
        }))
    })),
});

impl PageCacheMetrics {
    pub(crate) fn for_ctx(&self, ctx: &RequestContext) -> &PageCacheMetricsForTaskKind {
        &self.map[ctx.task_kind()][ctx.page_content_kind()]
    }
}

pub struct PageCacheSizeMetrics {
    pub max_bytes: UIntGauge,

    pub current_bytes_ephemeral: UIntGauge,
    pub current_bytes_immutable: UIntGauge,
    pub current_bytes_materialized_page: UIntGauge,
}

static PAGE_CACHE_SIZE_CURRENT_BYTES: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_page_cache_size_current_bytes",
        "Current size of the page cache in bytes, by key kind",
        &["key_kind"]
    )
    .expect("failed to define a metric")
});

pub static PAGE_CACHE_SIZE: Lazy<PageCacheSizeMetrics> = Lazy::new(|| PageCacheSizeMetrics {
    max_bytes: {
        register_uint_gauge!(
            "pageserver_page_cache_size_max_bytes",
            "Maximum size of the page cache in bytes"
        )
        .expect("failed to define a metric")
    },

    current_bytes_ephemeral: {
        PAGE_CACHE_SIZE_CURRENT_BYTES
            .get_metric_with_label_values(&["ephemeral"])
            .unwrap()
    },
    current_bytes_immutable: {
        PAGE_CACHE_SIZE_CURRENT_BYTES
            .get_metric_with_label_values(&["immutable"])
            .unwrap()
    },
    current_bytes_materialized_page: {
        PAGE_CACHE_SIZE_CURRENT_BYTES
            .get_metric_with_label_values(&["materialized_page"])
            .unwrap()
    },
});

pub(crate) static PAGE_CACHE_ACQUIRE_PINNED_SLOT_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_page_cache_acquire_pinned_slot_seconds",
        "Time spent acquiring a pinned slot in the page cache",
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

pub(crate) static PAGE_CACHE_FIND_VICTIMS_ITERS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_page_cache_find_victim_iters_total",
        "Counter for the number of iterations in the find_victim loop",
    )
    .expect("failed to define a metric")
});

static PAGE_CACHE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "page_cache_errors_total",
        "Number of timeouts while acquiring a pinned slot in the page cache",
        &["error_kind"]
    )
    .expect("failed to define a metric")
});

#[derive(IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum PageCacheErrorKind {
    AcquirePinnedSlotTimeout,
    EvictIterLimit,
}

pub(crate) fn page_cache_errors_inc(error_kind: PageCacheErrorKind) {
    PAGE_CACHE_ERRORS
        .get_metric_with_label_values(&[error_kind.into()])
        .unwrap()
        .inc();
}

pub(crate) static WAIT_LSN_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wait_lsn_seconds",
        "Time spent waiting for WAL to arrive",
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

static LAST_RECORD_LSN: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_last_record_lsn",
        "Last record LSN grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static RESIDENT_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_resident_physical_size",
        "The size of the layer files present in the pageserver's filesystem.",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

pub(crate) static RESIDENT_PHYSICAL_SIZE_GLOBAL: Lazy<UIntGauge> = Lazy::new(|| {
    register_uint_gauge!(
        "pageserver_resident_physical_size_global",
        "Like `pageserver_resident_physical_size`, but without tenant/timeline dimensions."
    )
    .expect("failed to define a metric")
});

static REMOTE_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_remote_physical_size",
        "The size of the layer files present in the remote storage that are listed in the the remote index_part.json.",
        // Corollary: If any files are missing from the index part, they won't be included here.
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static REMOTE_PHYSICAL_SIZE_GLOBAL: Lazy<UIntGauge> = Lazy::new(|| {
    register_uint_gauge!(
        "pageserver_remote_physical_size_global",
        "Like `pageserver_remote_physical_size`, but without tenant/timeline dimensions."
    )
    .expect("failed to define a metric")
});

pub(crate) static REMOTE_ONDEMAND_DOWNLOADED_LAYERS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_remote_ondemand_downloaded_layers_total",
        "Total on-demand downloaded layers"
    )
    .unwrap()
});

pub(crate) static REMOTE_ONDEMAND_DOWNLOADED_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_remote_ondemand_downloaded_bytes_total",
        "Total bytes of layers on-demand downloaded",
    )
    .unwrap()
});

static CURRENT_LOGICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_current_logical_size",
        "Current logical size grouped by timeline",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define current logical size metric")
});

pub(crate) static TENANT_STATE_METRIC: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_tenant_states_count",
        "Count of tenants per state",
        &["state"]
    )
    .expect("Failed to register pageserver_tenant_states_count metric")
});

/// A set of broken tenants.
///
/// These are expected to be so rare that a set is fine. Set as in a new timeseries per each broken
/// tenant.
pub(crate) static BROKEN_TENANTS_SET: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_broken_tenants_count",
        "Set of broken tenants",
        &["tenant_id"]
    )
    .expect("Failed to register pageserver_tenant_states_count metric")
});

pub(crate) static TENANT_SYNTHETIC_SIZE_METRIC: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_tenant_synthetic_cached_size_bytes",
        "Synthetic size of each tenant in bytes",
        &["tenant_id"]
    )
    .expect("Failed to register pageserver_tenant_synthetic_cached_size_bytes metric")
});

// Metrics for cloud upload. These metrics reflect data uploaded to cloud storage,
// or in testing they estimate how much we would upload if we did.
static NUM_PERSISTENT_FILES_CREATED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_created_persistent_files_total",
        "Number of files created that are meant to be uploaded to cloud storage",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static PERSISTENT_BYTES_WRITTEN: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_written_persistent_bytes_total",
        "Total bytes written that are meant to be uploaded to cloud storage",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

pub(crate) static EVICTION_ITERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_eviction_iteration_duration_seconds_global",
        "Time spent on a single eviction iteration",
        &["period_secs", "threshold_secs"],
        STORAGE_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

static EVICTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_evictions",
        "Number of layers evicted from the pageserver",
        &["tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static EVICTIONS_WITH_LOW_RESIDENCE_DURATION: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_evictions_with_low_residence_duration",
        "If a layer is evicted that was resident for less than `low_threshold`, it is counted to this counter. \
         Residence duration is determined using the `residence_duration_data_source`.",
        &["tenant_id", "timeline_id", "residence_duration_data_source", "low_threshold_secs"]
    )
    .expect("failed to define a metric")
});

pub(crate) static UNEXPECTED_ONDEMAND_DOWNLOADS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_unexpected_ondemand_downloads_count",
        "Number of unexpected on-demand downloads. \
         We log more context for each increment, so, forgo any labels in this metric.",
    )
    .expect("failed to define a metric")
});

/// How long did we take to start up?  Broken down by labels to describe
/// different phases of startup.
pub static STARTUP_DURATION: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "pageserver_startup_duration_seconds",
        "Time taken by phases of pageserver startup, in seconds",
        &["phase"]
    )
    .expect("Failed to register pageserver_startup_duration_seconds metric")
});

pub static STARTUP_IS_LOADING: Lazy<UIntGauge> = Lazy::new(|| {
    register_uint_gauge!(
        "pageserver_startup_is_loading",
        "1 while in initial startup load of tenants, 0 at other times"
    )
    .expect("Failed to register pageserver_startup_is_loading")
});

/// How long did tenants take to go from construction to active state?
pub(crate) static TENANT_ACTIVATION: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_tenant_activation_seconds",
        "Time taken by tenants to activate, in seconds",
        CRITICAL_OP_BUCKETS.into()
    )
    .expect("Failed to register pageserver_tenant_activation_seconds metric")
});

/// Each `Timeline`'s  [`EVICTIONS_WITH_LOW_RESIDENCE_DURATION`] metric.
#[derive(Debug)]
pub struct EvictionsWithLowResidenceDuration {
    data_source: &'static str,
    threshold: Duration,
    counter: Option<IntCounter>,
}

pub struct EvictionsWithLowResidenceDurationBuilder {
    data_source: &'static str,
    threshold: Duration,
}

impl EvictionsWithLowResidenceDurationBuilder {
    pub fn new(data_source: &'static str, threshold: Duration) -> Self {
        Self {
            data_source,
            threshold,
        }
    }

    fn build(&self, tenant_id: &str, timeline_id: &str) -> EvictionsWithLowResidenceDuration {
        let counter = EVICTIONS_WITH_LOW_RESIDENCE_DURATION
            .get_metric_with_label_values(&[
                tenant_id,
                timeline_id,
                self.data_source,
                &EvictionsWithLowResidenceDuration::threshold_label_value(self.threshold),
            ])
            .unwrap();
        EvictionsWithLowResidenceDuration {
            data_source: self.data_source,
            threshold: self.threshold,
            counter: Some(counter),
        }
    }
}

impl EvictionsWithLowResidenceDuration {
    fn threshold_label_value(threshold: Duration) -> String {
        format!("{}", threshold.as_secs())
    }

    pub fn observe(&self, observed_value: Duration) {
        if observed_value < self.threshold {
            self.counter
                .as_ref()
                .expect("nobody calls this function after `remove_from_vec`")
                .inc();
        }
    }

    pub fn change_threshold(
        &mut self,
        tenant_id: &str,
        timeline_id: &str,
        new_threshold: Duration,
    ) {
        if new_threshold == self.threshold {
            return;
        }
        let mut with_new =
            EvictionsWithLowResidenceDurationBuilder::new(self.data_source, new_threshold)
                .build(tenant_id, timeline_id);
        std::mem::swap(self, &mut with_new);
        with_new.remove(tenant_id, timeline_id);
    }

    // This could be a `Drop` impl, but, we need the `tenant_id` and `timeline_id`.
    fn remove(&mut self, tenant_id: &str, timeline_id: &str) {
        let Some(_counter) = self.counter.take() else {
            return;
        };

        let threshold = Self::threshold_label_value(self.threshold);

        let removed = EVICTIONS_WITH_LOW_RESIDENCE_DURATION.remove_label_values(&[
            tenant_id,
            timeline_id,
            self.data_source,
            &threshold,
        ]);

        match removed {
            Err(e) => {
                // this has been hit in staging as
                // <https://neondatabase.sentry.io/issues/4142396994/>, but we don't know how.
                // because we can be in the drop path already, don't risk:
                // - "double-panic => illegal instruction" or
                // - future "drop panick => abort"
                //
                // so just nag: (the error has the labels)
                tracing::warn!("failed to remove EvictionsWithLowResidenceDuration, it was already removed? {e:#?}");
            }
            Ok(()) => {
                // to help identify cases where we double-remove the same values, let's log all
                // deletions?
                tracing::info!("removed EvictionsWithLowResidenceDuration with {tenant_id}, {timeline_id}, {}, {threshold}", self.data_source);
            }
        }
    }
}

// Metrics collected on disk IO operations
//
// Roughly logarithmic scale.
const STORAGE_IO_TIME_BUCKETS: &[f64] = &[
    0.000030, // 30 usec
    0.001000, // 1000 usec
    0.030,    // 30 ms
    1.000,    // 1000 ms
    30.000,   // 30000 ms
];

/// VirtualFile fs operation variants.
///
/// Operations:
/// - open ([`std::fs::OpenOptions::open`])
/// - close (dropping [`std::fs::File`])
/// - close-by-replace (close by replacement algorithm)
/// - read (`read_at`)
/// - write (`write_at`)
/// - seek (modify internal position or file length query)
/// - fsync ([`std::fs::File::sync_all`])
/// - metadata ([`std::fs::File::metadata`])
#[derive(
    Debug, Clone, Copy, strum_macros::EnumCount, strum_macros::EnumIter, strum_macros::FromRepr,
)]
pub(crate) enum StorageIoOperation {
    Open,
    Close,
    CloseByReplace,
    Read,
    Write,
    Seek,
    Fsync,
    Metadata,
}

impl StorageIoOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageIoOperation::Open => "open",
            StorageIoOperation::Close => "close",
            StorageIoOperation::CloseByReplace => "close-by-replace",
            StorageIoOperation::Read => "read",
            StorageIoOperation::Write => "write",
            StorageIoOperation::Seek => "seek",
            StorageIoOperation::Fsync => "fsync",
            StorageIoOperation::Metadata => "metadata",
        }
    }
}

/// Tracks time taken by fs operations near VirtualFile.
#[derive(Debug)]
pub(crate) struct StorageIoTime {
    metrics: [Histogram; StorageIoOperation::COUNT],
}

impl StorageIoTime {
    fn new() -> Self {
        let storage_io_histogram_vec = register_histogram_vec!(
            "pageserver_io_operations_seconds",
            "Time spent in IO operations",
            &["operation"],
            STORAGE_IO_TIME_BUCKETS.into()
        )
        .expect("failed to define a metric");
        let metrics = std::array::from_fn(|i| {
            let op = StorageIoOperation::from_repr(i).unwrap();
            let metric = storage_io_histogram_vec
                .get_metric_with_label_values(&[op.as_str()])
                .unwrap();
            metric
        });
        Self { metrics }
    }

    pub(crate) fn get(&self, op: StorageIoOperation) -> &Histogram {
        &self.metrics[op as usize]
    }
}

pub(crate) static STORAGE_IO_TIME_METRIC: Lazy<StorageIoTime> = Lazy::new(StorageIoTime::new);

const STORAGE_IO_SIZE_OPERATIONS: &[&str] = &["read", "write"];

// Needed for the https://neonprod.grafana.net/d/5uK9tHL4k/picking-tenant-for-relocation?orgId=1
pub(crate) static STORAGE_IO_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_io_operations_bytes_total",
        "Total amount of bytes read/written in IO operations",
        &["operation", "tenant_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

#[derive(Debug)]
struct GlobalAndPerTimelineHistogram {
    global: Histogram,
    per_tenant_timeline: Histogram,
}

impl GlobalAndPerTimelineHistogram {
    fn observe(&self, value: f64) {
        self.global.observe(value);
        self.per_tenant_timeline.observe(value);
    }
}

struct GlobalAndPerTimelineHistogramTimer<'a> {
    h: &'a GlobalAndPerTimelineHistogram,
    start: std::time::Instant,
}

impl<'a> Drop for GlobalAndPerTimelineHistogramTimer<'a> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        self.h.observe(elapsed.as_secs_f64());
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    IntoStaticStr,
    strum_macros::EnumCount,
    strum_macros::EnumIter,
    strum_macros::FromRepr,
)]
#[strum(serialize_all = "snake_case")]
pub enum SmgrQueryType {
    GetRelExists,
    GetRelSize,
    GetPageAtLsn,
    GetDbSize,
}

#[derive(Debug)]
pub struct SmgrQueryTimePerTimeline {
    metrics: [GlobalAndPerTimelineHistogram; SmgrQueryType::COUNT],
}

static SMGR_QUERY_TIME_PER_TENANT_TIMELINE: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_smgr_query_seconds",
        "Time spent on smgr query handling, aggregated by query type and tenant/timeline.",
        &["smgr_query_type", "tenant_id", "timeline_id"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

static SMGR_QUERY_TIME_GLOBAL: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_smgr_query_seconds_global",
        "Time spent on smgr query handling, aggregated by query type.",
        &["smgr_query_type"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

impl SmgrQueryTimePerTimeline {
    pub(crate) fn new(tenant_id: &TenantId, timeline_id: &TimelineId) -> Self {
        let tenant_id = tenant_id.to_string();
        let timeline_id = timeline_id.to_string();
        let metrics = std::array::from_fn(|i| {
            let op = SmgrQueryType::from_repr(i).unwrap();
            let global = SMGR_QUERY_TIME_GLOBAL
                .get_metric_with_label_values(&[op.into()])
                .unwrap();
            let per_tenant_timeline = SMGR_QUERY_TIME_PER_TENANT_TIMELINE
                .get_metric_with_label_values(&[op.into(), &tenant_id, &timeline_id])
                .unwrap();
            GlobalAndPerTimelineHistogram {
                global,
                per_tenant_timeline,
            }
        });
        Self { metrics }
    }
    pub(crate) fn start_timer(&self, op: SmgrQueryType) -> impl Drop + '_ {
        let metric = &self.metrics[op as usize];
        GlobalAndPerTimelineHistogramTimer {
            h: metric,
            start: std::time::Instant::now(),
        }
    }
}

#[cfg(test)]
mod smgr_query_time_tests {
    use strum::IntoEnumIterator;
    use utils::id::{TenantId, TimelineId};

    // Regression test, we used hard-coded string constants before using an enum.
    #[test]
    fn op_label_name() {
        use super::SmgrQueryType::*;
        let expect: [(super::SmgrQueryType, &'static str); 4] = [
            (GetRelExists, "get_rel_exists"),
            (GetRelSize, "get_rel_size"),
            (GetPageAtLsn, "get_page_at_lsn"),
            (GetDbSize, "get_db_size"),
        ];
        for (op, expect) in expect {
            let actual: &'static str = op.into();
            assert_eq!(actual, expect);
        }
    }

    #[test]
    fn basic() {
        let ops: Vec<_> = super::SmgrQueryType::iter().collect();

        for op in &ops {
            let tenant_id = TenantId::generate();
            let timeline_id = TimelineId::generate();
            let metrics = super::SmgrQueryTimePerTimeline::new(&tenant_id, &timeline_id);

            let get_counts = || {
                let global: u64 = ops
                    .iter()
                    .map(|op| metrics.metrics[*op as usize].global.get_sample_count())
                    .sum();
                let per_tenant_timeline: u64 = ops
                    .iter()
                    .map(|op| {
                        metrics.metrics[*op as usize]
                            .per_tenant_timeline
                            .get_sample_count()
                    })
                    .sum();
                (global, per_tenant_timeline)
            };

            let (pre_global, pre_per_tenant_timeline) = get_counts();
            assert_eq!(pre_per_tenant_timeline, 0);

            let timer = metrics.start_timer(*op);
            drop(timer);

            let (post_global, post_per_tenant_timeline) = get_counts();
            assert_eq!(post_per_tenant_timeline, 1);
            assert!(post_global > pre_global);
        }
    }
}

// keep in sync with control plane Go code so that we can validate
// compute's basebackup_ms metric with our perspective in the context of SLI/SLO.
static COMPUTE_STARTUP_BUCKETS: Lazy<[f64; 28]> = Lazy::new(|| {
    // Go code uses milliseconds. Variable is called `computeStartupBuckets`
    [
        5, 10, 20, 30, 50, 70, 100, 120, 150, 200, 250, 300, 350, 400, 450, 500, 600, 800, 1000,
        1500, 2000, 2500, 3000, 5000, 10000, 20000, 40000, 60000,
    ]
    .map(|ms| (ms as f64) / 1000.0)
});

pub struct BasebackupQueryTime(HistogramVec);
pub static BASEBACKUP_QUERY_TIME: Lazy<BasebackupQueryTime> = Lazy::new(|| {
    BasebackupQueryTime({
        register_histogram_vec!(
            "pageserver_basebackup_query_seconds",
            "Histogram of basebackup queries durations, by result type",
            &["result"],
            COMPUTE_STARTUP_BUCKETS.to_vec(),
        )
        .expect("failed to define a metric")
    })
});

impl DurationResultObserver for BasebackupQueryTime {
    fn observe_result<T, E>(&self, res: &Result<T, E>, duration: std::time::Duration) {
        let label_value = if res.is_ok() { "ok" } else { "error" };
        let metric = self.0.get_metric_with_label_values(&[label_value]).unwrap();
        metric.observe(duration.as_secs_f64());
    }
}

pub static LIVE_CONNECTIONS_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_live_connections",
        "Number of live network connections",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric")
});

// remote storage metrics

/// NB: increment _after_ recording the current value into [`REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST`].
static REMOTE_TIMELINE_CLIENT_CALLS_UNFINISHED_GAUGE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_remote_timeline_client_calls_unfinished",
        "Number of ongoing calls to remote timeline client. \
         Used to populate pageserver_remote_timeline_client_calls_started. \
         This metric is not useful for sampling from Prometheus, but useful in tests.",
        &["tenant_id", "timeline_id", "file_kind", "op_kind"],
    )
    .expect("failed to define a metric")
});

static REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_remote_timeline_client_calls_started",
        "When calling a remote timeline client method, we record the current value \
         of the calls_unfinished gauge in this histogram. Plot the histogram \
         over time in a heatmap to visualize how many operations were ongoing \
         at a given instant. It gives you a better idea of the queue depth \
         than plotting the gauge directly, since operations may complete faster \
         than the sampling interval.",
        &["file_kind", "op_kind"],
        // The calls_unfinished gauge is an integer gauge, hence we have integer buckets.
        vec![0.0, 1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 15.0, 20.0, 40.0, 60.0, 80.0, 100.0, 500.0],
    )
    .expect("failed to define a metric")
});

static REMOTE_TIMELINE_CLIENT_BYTES_STARTED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_remote_timeline_client_bytes_started",
        "Incremented by the number of bytes associated with a remote timeline client operation. \
         The increment happens when the operation is scheduled.",
        &["tenant_id", "timeline_id", "file_kind", "op_kind"],
    )
    .expect("failed to define a metric")
});

static REMOTE_TIMELINE_CLIENT_BYTES_FINISHED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_remote_timeline_client_bytes_finished",
        "Incremented by the number of bytes associated with a remote timeline client operation. \
         The increment happens when the operation finishes (regardless of success/failure/shutdown).",
        &["tenant_id", "timeline_id", "file_kind", "op_kind"],
    )
    .expect("failed to define a metric")
});

pub(crate) struct DeletionQueueMetrics {
    pub(crate) keys_submitted: IntCounter,
    pub(crate) keys_dropped: IntCounter,
    pub(crate) keys_executed: IntCounter,
    pub(crate) keys_validated: IntCounter,
    pub(crate) dropped_lsn_updates: IntCounter,
    pub(crate) unexpected_errors: IntCounter,
    pub(crate) remote_errors: IntCounterVec,
}
pub(crate) static DELETION_QUEUE: Lazy<DeletionQueueMetrics> = Lazy::new(|| {
    DeletionQueueMetrics{

    keys_submitted: register_int_counter!(
        "pageserver_deletion_queue_submitted_total",
        "Number of objects submitted for deletion"
    )
    .expect("failed to define a metric"),

    keys_dropped: register_int_counter!(
        "pageserver_deletion_queue_dropped_total",
        "Number of object deletions dropped due to stale generation."
    )
    .expect("failed to define a metric"),

    keys_executed: register_int_counter!(
        "pageserver_deletion_queue_executed_total",
        "Number of objects deleted. Only includes objects that we actually deleted, sum with pageserver_deletion_queue_dropped_total for the total number of keys processed to completion"
    )
    .expect("failed to define a metric"),

    keys_validated: register_int_counter!(
        "pageserver_deletion_queue_validated_total",
        "Number of keys validated for deletion.  Sum with pageserver_deletion_queue_dropped_total for the total number of keys that have passed through the validation stage."
    )
    .expect("failed to define a metric"),

    dropped_lsn_updates: register_int_counter!(
        "pageserver_deletion_queue_dropped_lsn_updates_total",
        "Updates to remote_consistent_lsn dropped due to stale generation number."
    )
    .expect("failed to define a metric"),
    unexpected_errors: register_int_counter!(
        "pageserver_deletion_queue_unexpected_errors_total",
        "Number of unexpected condiions that may stall the queue: any value above zero is unexpected."
    )
    .expect("failed to define a metric"),
    remote_errors: register_int_counter_vec!(
        "pageserver_deletion_queue_remote_errors_total",
        "Retryable remote I/O errors while executing deletions, for example 503 responses to DeleteObjects",
        &["op_kind"],
    )
    .expect("failed to define a metric")
}
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteOpKind {
    Upload,
    Download,
    Delete,
}
impl RemoteOpKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum RemoteOpFileKind {
    Layer,
    Index,
}
impl RemoteOpFileKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Layer => "layer",
            Self::Index => "index",
        }
    }
}

pub(crate) static REMOTE_OPERATION_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_remote_operation_seconds",
        "Time spent on remote storage operations. \
        Grouped by tenant, timeline, operation_kind and status. \
        Does not account for time spent waiting in remote timeline client's queues.",
        &["file_kind", "op_kind", "status"]
    )
    .expect("failed to define a metric")
});

pub(crate) static TENANT_TASK_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_tenant_task_events",
        "Number of task start/stop/fail events.",
        &["event"],
    )
    .expect("Failed to register tenant_task_events metric")
});

pub(crate) static BACKGROUND_LOOP_PERIOD_OVERRUN_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_background_loop_period_overrun_count",
        "Incremented whenever warn_when_period_overrun() logs a warning.",
        &["task", "period"],
    )
    .expect("failed to define a metric")
});

// walreceiver metrics

pub(crate) static WALRECEIVER_STARTED_CONNECTIONS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_walreceiver_started_connections_total",
        "Number of started walreceiver connections"
    )
    .expect("failed to define a metric")
});

pub(crate) static WALRECEIVER_ACTIVE_MANAGERS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "pageserver_walreceiver_active_managers",
        "Number of active walreceiver managers"
    )
    .expect("failed to define a metric")
});

pub(crate) static WALRECEIVER_SWITCHES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_walreceiver_switches_total",
        "Number of walreceiver manager change_connection calls",
        &["reason"]
    )
    .expect("failed to define a metric")
});

pub(crate) static WALRECEIVER_BROKER_UPDATES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_walreceiver_broker_updates_total",
        "Number of received broker updates in walreceiver"
    )
    .expect("failed to define a metric")
});

pub(crate) static WALRECEIVER_CANDIDATES_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_walreceiver_candidates_events_total",
        "Number of walreceiver candidate events",
        &["event"]
    )
    .expect("failed to define a metric")
});

pub(crate) static WALRECEIVER_CANDIDATES_ADDED: Lazy<IntCounter> =
    Lazy::new(|| WALRECEIVER_CANDIDATES_EVENTS.with_label_values(&["add"]));

pub(crate) static WALRECEIVER_CANDIDATES_REMOVED: Lazy<IntCounter> =
    Lazy::new(|| WALRECEIVER_CANDIDATES_EVENTS.with_label_values(&["remove"]));

// Metrics collected on WAL redo operations
//
// We collect the time spent in actual WAL redo ('redo'), and time waiting
// for access to the postgres process ('wait') since there is only one for
// each tenant.

/// Time buckets are small because we want to be able to measure the
/// smallest redo processing times. These buckets allow us to measure down
/// to 5us, which equates to 200'000 pages/sec, which equates to 1.6GB/sec.
/// This is much better than the previous 5ms aka 200 pages/sec aka 1.6MB/sec.
///
/// Values up to 1s are recorded because metrics show that we have redo
/// durations and lock times larger than 0.250s.
macro_rules! redo_histogram_time_buckets {
    () => {
        vec![
            0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001_000,
            0.002_500, 0.005_000, 0.010_000, 0.025_000, 0.050_000, 0.100_000, 0.250_000, 0.500_000,
            1.000_000,
        ]
    };
}

/// While we're at it, also measure the amount of records replayed in each
/// operation. We have a global 'total replayed' counter, but that's not
/// as useful as 'what is the skew for how many records we replay in one
/// operation'.
macro_rules! redo_histogram_count_buckets {
    () => {
        vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]
    };
}

macro_rules! redo_bytes_histogram_count_buckets {
    () => {
        // powers of (2^.5), from 2^4.5 to 2^15 (22 buckets)
        // rounded up to the next multiple of 8 to capture any MAXALIGNed record of that size, too.
        vec![
            24.0, 32.0, 48.0, 64.0, 96.0, 128.0, 184.0, 256.0, 368.0, 512.0, 728.0, 1024.0, 1456.0,
            2048.0, 2904.0, 4096.0, 5800.0, 8192.0, 11592.0, 16384.0, 23176.0, 32768.0,
        ]
    };
}

pub(crate) static WAL_REDO_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_seconds",
        "Time spent on WAL redo",
        redo_histogram_time_buckets!()
    )
    .expect("failed to define a metric")
});

pub(crate) static WAL_REDO_WAIT_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_wait_seconds",
        "Time spent waiting for access to the Postgres WAL redo process",
        redo_histogram_time_buckets!(),
    )
    .expect("failed to define a metric")
});

pub(crate) static WAL_REDO_RECORDS_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_records_histogram",
        "Histogram of number of records replayed per redo in the Postgres WAL redo process",
        redo_histogram_count_buckets!(),
    )
    .expect("failed to define a metric")
});

pub(crate) static WAL_REDO_BYTES_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_bytes_histogram",
        "Histogram of number of records replayed per redo sent to Postgres",
        redo_bytes_histogram_count_buckets!(),
    )
    .expect("failed to define a metric")
});

// FIXME: isn't this already included by WAL_REDO_RECORDS_HISTOGRAM which has _count?
pub(crate) static WAL_REDO_RECORD_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_replayed_wal_records_total",
        "Number of WAL records replayed in WAL redo process"
    )
    .unwrap()
});

/// Similar to `prometheus::HistogramTimer` but does not record on drop.
pub struct StorageTimeMetricsTimer {
    metrics: StorageTimeMetrics,
    start: Instant,
}

impl StorageTimeMetricsTimer {
    fn new(metrics: StorageTimeMetrics) -> Self {
        Self {
            metrics,
            start: Instant::now(),
        }
    }

    /// Record the time from creation to now.
    pub fn stop_and_record(self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics.timeline_sum.inc_by(duration);
        self.metrics.timeline_count.inc();
        self.metrics.global_histogram.observe(duration);
    }
}

/// Timing facilities for an globally histogrammed metric, which is supported by per tenant and
/// timeline total sum and count.
#[derive(Clone, Debug)]
pub struct StorageTimeMetrics {
    /// Sum of f64 seconds, per operation, tenant_id and timeline_id
    timeline_sum: Counter,
    /// Number of operations, per operation, tenant_id and timeline_id
    timeline_count: IntCounter,
    /// Global histogram having only the "operation" label.
    global_histogram: Histogram,
}

impl StorageTimeMetrics {
    pub fn new(operation: StorageTimeOperation, tenant_id: &str, timeline_id: &str) -> Self {
        let operation: &'static str = operation.into();

        let timeline_sum = STORAGE_TIME_SUM_PER_TIMELINE
            .get_metric_with_label_values(&[operation, tenant_id, timeline_id])
            .unwrap();
        let timeline_count = STORAGE_TIME_COUNT_PER_TIMELINE
            .get_metric_with_label_values(&[operation, tenant_id, timeline_id])
            .unwrap();
        let global_histogram = STORAGE_TIME_GLOBAL
            .get_metric_with_label_values(&[operation])
            .unwrap();

        StorageTimeMetrics {
            timeline_sum,
            timeline_count,
            global_histogram,
        }
    }

    /// Starts timing a new operation.
    ///
    /// Note: unlike `prometheus::HistogramTimer` the returned timer does not record on drop.
    pub fn start_timer(&self) -> StorageTimeMetricsTimer {
        StorageTimeMetricsTimer::new(self.clone())
    }
}

#[derive(Debug)]
pub struct TimelineMetrics {
    tenant_id: String,
    timeline_id: String,
    pub flush_time_histo: StorageTimeMetrics,
    pub compact_time_histo: StorageTimeMetrics,
    pub create_images_time_histo: StorageTimeMetrics,
    pub logical_size_histo: StorageTimeMetrics,
    pub imitate_logical_size_histo: StorageTimeMetrics,
    pub load_layer_map_histo: StorageTimeMetrics,
    pub garbage_collect_histo: StorageTimeMetrics,
    pub last_record_gauge: IntGauge,
    resident_physical_size_gauge: UIntGauge,
    /// copy of LayeredTimeline.current_logical_size
    pub current_logical_size_gauge: UIntGauge,
    pub num_persistent_files_created: IntCounter,
    pub persistent_bytes_written: IntCounter,
    pub evictions: IntCounter,
    pub evictions_with_low_residence_duration: std::sync::RwLock<EvictionsWithLowResidenceDuration>,
}

impl TimelineMetrics {
    pub fn new(
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        evictions_with_low_residence_duration_builder: EvictionsWithLowResidenceDurationBuilder,
    ) -> Self {
        let tenant_id = tenant_id.to_string();
        let timeline_id = timeline_id.to_string();
        let flush_time_histo =
            StorageTimeMetrics::new(StorageTimeOperation::LayerFlush, &tenant_id, &timeline_id);
        let compact_time_histo =
            StorageTimeMetrics::new(StorageTimeOperation::Compact, &tenant_id, &timeline_id);
        let create_images_time_histo =
            StorageTimeMetrics::new(StorageTimeOperation::CreateImages, &tenant_id, &timeline_id);
        let logical_size_histo =
            StorageTimeMetrics::new(StorageTimeOperation::LogicalSize, &tenant_id, &timeline_id);
        let imitate_logical_size_histo = StorageTimeMetrics::new(
            StorageTimeOperation::ImitateLogicalSize,
            &tenant_id,
            &timeline_id,
        );
        let load_layer_map_histo =
            StorageTimeMetrics::new(StorageTimeOperation::LoadLayerMap, &tenant_id, &timeline_id);
        let garbage_collect_histo =
            StorageTimeMetrics::new(StorageTimeOperation::Gc, &tenant_id, &timeline_id);
        let last_record_gauge = LAST_RECORD_LSN
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let resident_physical_size_gauge = RESIDENT_PHYSICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let current_logical_size_gauge = CURRENT_LOGICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let num_persistent_files_created = NUM_PERSISTENT_FILES_CREATED
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let persistent_bytes_written = PERSISTENT_BYTES_WRITTEN
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let evictions = EVICTIONS
            .get_metric_with_label_values(&[&tenant_id, &timeline_id])
            .unwrap();
        let evictions_with_low_residence_duration =
            evictions_with_low_residence_duration_builder.build(&tenant_id, &timeline_id);

        TimelineMetrics {
            tenant_id,
            timeline_id,
            flush_time_histo,
            compact_time_histo,
            create_images_time_histo,
            logical_size_histo,
            imitate_logical_size_histo,
            garbage_collect_histo,
            load_layer_map_histo,
            last_record_gauge,
            resident_physical_size_gauge,
            current_logical_size_gauge,
            num_persistent_files_created,
            persistent_bytes_written,
            evictions,
            evictions_with_low_residence_duration: std::sync::RwLock::new(
                evictions_with_low_residence_duration,
            ),
        }
    }

    pub fn record_new_file_metrics(&self, sz: u64) {
        self.resident_physical_size_add(sz);
        self.num_persistent_files_created.inc_by(1);
        self.persistent_bytes_written.inc_by(sz);
    }

    pub fn resident_physical_size_sub(&self, sz: u64) {
        self.resident_physical_size_gauge.sub(sz);
        crate::metrics::RESIDENT_PHYSICAL_SIZE_GLOBAL.sub(sz);
    }

    pub fn resident_physical_size_add(&self, sz: u64) {
        self.resident_physical_size_gauge.add(sz);
        crate::metrics::RESIDENT_PHYSICAL_SIZE_GLOBAL.add(sz);
    }

    pub fn resident_physical_size_set(&self, sz: u64) {
        self.resident_physical_size_gauge.set(sz);
        crate::metrics::RESIDENT_PHYSICAL_SIZE_GLOBAL.set(sz);
    }

    pub fn resident_physical_size_get(&self) -> u64 {
        self.resident_physical_size_gauge.get()
    }
}

impl Drop for TimelineMetrics {
    fn drop(&mut self) {
        let tenant_id = &self.tenant_id;
        let timeline_id = &self.timeline_id;
        let _ = LAST_RECORD_LSN.remove_label_values(&[tenant_id, timeline_id]);
        {
            RESIDENT_PHYSICAL_SIZE_GLOBAL.sub(self.resident_physical_size_get());
            let _ = RESIDENT_PHYSICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        }
        let _ = CURRENT_LOGICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        let _ = NUM_PERSISTENT_FILES_CREATED.remove_label_values(&[tenant_id, timeline_id]);
        let _ = PERSISTENT_BYTES_WRITTEN.remove_label_values(&[tenant_id, timeline_id]);
        let _ = EVICTIONS.remove_label_values(&[tenant_id, timeline_id]);

        self.evictions_with_low_residence_duration
            .write()
            .unwrap()
            .remove(tenant_id, timeline_id);

        // The following metrics are born outside of the TimelineMetrics lifecycle but still
        // removed at the end of it. The idea is to have the metrics outlive the
        // entity during which they're observed, e.g., the smgr metrics shall
        // outlive an individual smgr connection, but not the timeline.

        for op in StorageTimeOperation::VARIANTS {
            let _ =
                STORAGE_TIME_SUM_PER_TIMELINE.remove_label_values(&[op, tenant_id, timeline_id]);
            let _ =
                STORAGE_TIME_COUNT_PER_TIMELINE.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in STORAGE_IO_SIZE_OPERATIONS {
            let _ = STORAGE_IO_SIZE.remove_label_values(&[op, tenant_id, timeline_id]);
        }

        for op in SmgrQueryType::iter() {
            let _ = SMGR_QUERY_TIME_PER_TENANT_TIMELINE.remove_label_values(&[
                op.into(),
                tenant_id,
                timeline_id,
            ]);
        }
    }
}

pub fn remove_tenant_metrics(tenant_id: &TenantId) {
    let tid = tenant_id.to_string();
    let _ = TENANT_SYNTHETIC_SIZE_METRIC.remove_label_values(&[&tid]);
    // we leave the BROKEN_TENANTS_SET entry if any
}

use futures::Future;
use pin_project_lite::pin_project;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::context::{PageContentKind, RequestContext};
use crate::task_mgr::TaskKind;

/// Maintain a per timeline gauge in addition to the global gauge.
struct PerTimelineRemotePhysicalSizeGauge {
    last_set: u64,
    gauge: UIntGauge,
}

impl PerTimelineRemotePhysicalSizeGauge {
    fn new(per_timeline_gauge: UIntGauge) -> Self {
        Self {
            last_set: per_timeline_gauge.get(),
            gauge: per_timeline_gauge,
        }
    }
    fn set(&mut self, sz: u64) {
        self.gauge.set(sz);
        if sz < self.last_set {
            REMOTE_PHYSICAL_SIZE_GLOBAL.sub(self.last_set - sz);
        } else {
            REMOTE_PHYSICAL_SIZE_GLOBAL.add(sz - self.last_set);
        };
        self.last_set = sz;
    }
    fn get(&self) -> u64 {
        self.gauge.get()
    }
}

impl Drop for PerTimelineRemotePhysicalSizeGauge {
    fn drop(&mut self) {
        REMOTE_PHYSICAL_SIZE_GLOBAL.sub(self.last_set);
    }
}

pub struct RemoteTimelineClientMetrics {
    tenant_id: String,
    timeline_id: String,
    remote_physical_size_gauge: Mutex<Option<PerTimelineRemotePhysicalSizeGauge>>,
    calls_unfinished_gauge: Mutex<HashMap<(&'static str, &'static str), IntGauge>>,
    bytes_started_counter: Mutex<HashMap<(&'static str, &'static str), IntCounter>>,
    bytes_finished_counter: Mutex<HashMap<(&'static str, &'static str), IntCounter>>,
}

impl RemoteTimelineClientMetrics {
    pub fn new(tenant_id: &TenantId, timeline_id: &TimelineId) -> Self {
        RemoteTimelineClientMetrics {
            tenant_id: tenant_id.to_string(),
            timeline_id: timeline_id.to_string(),
            calls_unfinished_gauge: Mutex::new(HashMap::default()),
            bytes_started_counter: Mutex::new(HashMap::default()),
            bytes_finished_counter: Mutex::new(HashMap::default()),
            remote_physical_size_gauge: Mutex::new(None),
        }
    }

    pub(crate) fn remote_physical_size_set(&self, sz: u64) {
        let mut guard = self.remote_physical_size_gauge.lock().unwrap();
        let gauge = guard.get_or_insert_with(|| {
            PerTimelineRemotePhysicalSizeGauge::new(
                REMOTE_PHYSICAL_SIZE
                    .get_metric_with_label_values(&[
                        &self.tenant_id.to_string(),
                        &self.timeline_id.to_string(),
                    ])
                    .unwrap(),
            )
        });
        gauge.set(sz);
    }

    pub(crate) fn remote_physical_size_get(&self) -> u64 {
        let guard = self.remote_physical_size_gauge.lock().unwrap();
        guard.as_ref().map(|gauge| gauge.get()).unwrap_or(0)
    }

    pub fn remote_operation_time(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
        status: &'static str,
    ) -> Histogram {
        let key = (file_kind.as_str(), op_kind.as_str(), status);
        REMOTE_OPERATION_TIME
            .get_metric_with_label_values(&[key.0, key.1, key.2])
            .unwrap()
    }

    fn calls_unfinished_gauge(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> IntGauge {
        let mut guard = self.calls_unfinished_gauge.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_TIMELINE_CLIENT_CALLS_UNFINISHED_GAUGE
                .get_metric_with_label_values(&[
                    &self.tenant_id.to_string(),
                    &self.timeline_id.to_string(),
                    key.0,
                    key.1,
                ])
                .unwrap()
        });
        metric.clone()
    }

    fn calls_started_hist(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> Histogram {
        let key = (file_kind.as_str(), op_kind.as_str());
        REMOTE_TIMELINE_CLIENT_CALLS_STARTED_HIST
            .get_metric_with_label_values(&[key.0, key.1])
            .unwrap()
    }

    fn bytes_started_counter(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> IntCounter {
        let mut guard = self.bytes_started_counter.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_TIMELINE_CLIENT_BYTES_STARTED_COUNTER
                .get_metric_with_label_values(&[
                    &self.tenant_id.to_string(),
                    &self.timeline_id.to_string(),
                    key.0,
                    key.1,
                ])
                .unwrap()
        });
        metric.clone()
    }

    fn bytes_finished_counter(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> IntCounter {
        let mut guard = self.bytes_finished_counter.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_TIMELINE_CLIENT_BYTES_FINISHED_COUNTER
                .get_metric_with_label_values(&[
                    &self.tenant_id.to_string(),
                    &self.timeline_id.to_string(),
                    key.0,
                    key.1,
                ])
                .unwrap()
        });
        metric.clone()
    }
}

#[cfg(test)]
impl RemoteTimelineClientMetrics {
    pub fn get_bytes_started_counter_value(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> Option<u64> {
        let guard = self.bytes_started_counter.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        guard.get(&key).map(|counter| counter.get())
    }

    pub fn get_bytes_finished_counter_value(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> Option<u64> {
        let guard = self.bytes_finished_counter.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        guard.get(&key).map(|counter| counter.get())
    }
}

/// See [`RemoteTimelineClientMetrics::call_begin`].
#[must_use]
pub(crate) struct RemoteTimelineClientCallMetricGuard {
    /// Decremented on drop.
    calls_unfinished_metric: Option<IntGauge>,
    /// If Some(), this references the bytes_finished metric, and we increment it by the given `u64` on drop.
    bytes_finished: Option<(IntCounter, u64)>,
}

impl RemoteTimelineClientCallMetricGuard {
    /// Consume this guard object without performing the metric updates it would do on `drop()`.
    /// The caller vouches to do the metric updates manually.
    pub fn will_decrement_manually(mut self) {
        let RemoteTimelineClientCallMetricGuard {
            calls_unfinished_metric,
            bytes_finished,
        } = &mut self;
        calls_unfinished_metric.take();
        bytes_finished.take();
    }
}

impl Drop for RemoteTimelineClientCallMetricGuard {
    fn drop(&mut self) {
        let RemoteTimelineClientCallMetricGuard {
            calls_unfinished_metric,
            bytes_finished,
        } = self;
        if let Some(guard) = calls_unfinished_metric.take() {
            guard.dec();
        }
        if let Some((bytes_finished_metric, value)) = bytes_finished {
            bytes_finished_metric.inc_by(*value);
        }
    }
}

/// The enum variants communicate to the [`RemoteTimelineClientMetrics`] whether to
/// track the byte size of this call in applicable metric(s).
pub(crate) enum RemoteTimelineClientMetricsCallTrackSize {
    /// Do not account for this call's byte size in any metrics.
    /// The `reason` field is there to make the call sites self-documenting
    /// about why they don't need the metric.
    DontTrackSize { reason: &'static str },
    /// Track the byte size of the call in applicable metric(s).
    Bytes(u64),
}

impl RemoteTimelineClientMetrics {
    /// Update the metrics that change when a call to the remote timeline client instance starts.
    ///
    /// Drop the returned guard object once the operation is finished to updates corresponding metrics that track completions.
    /// Or, use [`RemoteTimelineClientCallMetricGuard::will_decrement_manually`] and [`call_end`](Self::call_end) if that
    /// is more suitable.
    /// Never do both.
    pub(crate) fn call_begin(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
        size: RemoteTimelineClientMetricsCallTrackSize,
    ) -> RemoteTimelineClientCallMetricGuard {
        let calls_unfinished_metric = self.calls_unfinished_gauge(file_kind, op_kind);
        self.calls_started_hist(file_kind, op_kind)
            .observe(calls_unfinished_metric.get() as f64);
        calls_unfinished_metric.inc(); // NB: inc after the histogram, see comment on underlying metric

        let bytes_finished = match size {
            RemoteTimelineClientMetricsCallTrackSize::DontTrackSize { reason: _reason } => {
                // nothing to do
                None
            }
            RemoteTimelineClientMetricsCallTrackSize::Bytes(size) => {
                self.bytes_started_counter(file_kind, op_kind).inc_by(size);
                let finished_counter = self.bytes_finished_counter(file_kind, op_kind);
                Some((finished_counter, size))
            }
        };
        RemoteTimelineClientCallMetricGuard {
            calls_unfinished_metric: Some(calls_unfinished_metric),
            bytes_finished,
        }
    }

    /// Manually update the metrics that track completions, instead of using the guard object.
    /// Using the guard object is generally preferable.
    /// See [`call_begin`](Self::call_begin) for more context.
    pub(crate) fn call_end(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
        size: RemoteTimelineClientMetricsCallTrackSize,
    ) {
        let calls_unfinished_metric = self.calls_unfinished_gauge(file_kind, op_kind);
        debug_assert!(
            calls_unfinished_metric.get() > 0,
            "begin and end should cancel out"
        );
        calls_unfinished_metric.dec();
        match size {
            RemoteTimelineClientMetricsCallTrackSize::DontTrackSize { reason: _reason } => {}
            RemoteTimelineClientMetricsCallTrackSize::Bytes(size) => {
                self.bytes_finished_counter(file_kind, op_kind).inc_by(size);
            }
        }
    }
}

impl Drop for RemoteTimelineClientMetrics {
    fn drop(&mut self) {
        let RemoteTimelineClientMetrics {
            tenant_id,
            timeline_id,
            remote_physical_size_gauge,
            calls_unfinished_gauge,
            bytes_started_counter,
            bytes_finished_counter,
        } = self;
        for ((a, b), _) in calls_unfinished_gauge.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_CALLS_UNFINISHED_GAUGE.remove_label_values(&[
                tenant_id,
                timeline_id,
                a,
                b,
            ]);
        }
        for ((a, b), _) in bytes_started_counter.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_BYTES_STARTED_COUNTER.remove_label_values(&[
                tenant_id,
                timeline_id,
                a,
                b,
            ]);
        }
        for ((a, b), _) in bytes_finished_counter.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_BYTES_FINISHED_COUNTER.remove_label_values(&[
                tenant_id,
                timeline_id,
                a,
                b,
            ]);
        }
        {
            let _ = remote_physical_size_gauge; // use to avoid 'unused' warning in desctructuring above
            let _ = REMOTE_PHYSICAL_SIZE.remove_label_values(&[tenant_id, timeline_id]);
        }
    }
}

/// Wrapper future that measures the time spent by a remote storage operation,
/// and records the time and success/failure as a prometheus metric.
pub trait MeasureRemoteOp: Sized {
    fn measure_remote_op(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: RemoteOpFileKind,
        op: RemoteOpKind,
        metrics: Arc<RemoteTimelineClientMetrics>,
    ) -> MeasuredRemoteOp<Self> {
        let start = Instant::now();
        MeasuredRemoteOp {
            inner: self,
            tenant_id,
            timeline_id,
            file_kind,
            op,
            start,
            metrics,
        }
    }
}

impl<T: Sized> MeasureRemoteOp for T {}

pin_project! {
    pub struct MeasuredRemoteOp<F>
    {
        #[pin]
        inner: F,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: RemoteOpFileKind,
        op: RemoteOpKind,
        start: Instant,
        metrics: Arc<RemoteTimelineClientMetrics>,
    }
}

impl<F: Future<Output = Result<O, E>>, O, E> Future for MeasuredRemoteOp<F> {
    type Output = Result<O, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let poll_result = this.inner.poll(cx);
        if let Poll::Ready(ref res) = poll_result {
            let duration = this.start.elapsed();
            let status = if res.is_ok() { &"success" } else { &"failure" };
            this.metrics
                .remote_operation_time(this.file_kind, this.op, status)
                .observe(duration.as_secs_f64());
        }
        poll_result
    }
}

pub fn preinitialize_metrics() {
    // Python tests need these and on some we do alerting.
    //
    // FIXME(4813): make it so that we have no top level metrics as this fn will easily fall out of
    // order:
    // - global metrics reside in a Lazy<PageserverMetrics>
    //   - access via crate::metrics::PS_METRICS.materialized_page_cache_hit.inc()
    // - could move the statics into TimelineMetrics::new()?

    // counters
    [
        &MATERIALIZED_PAGE_CACHE_HIT,
        &MATERIALIZED_PAGE_CACHE_HIT_DIRECT,
        &UNEXPECTED_ONDEMAND_DOWNLOADS,
        &WALRECEIVER_STARTED_CONNECTIONS,
        &WALRECEIVER_BROKER_UPDATES,
        &WALRECEIVER_CANDIDATES_ADDED,
        &WALRECEIVER_CANDIDATES_REMOVED,
    ]
    .into_iter()
    .for_each(|c| {
        Lazy::force(c);
    });

    // Deletion queue stats
    Lazy::force(&DELETION_QUEUE);

    // countervecs
    [&BACKGROUND_LOOP_PERIOD_OVERRUN_COUNT]
        .into_iter()
        .for_each(|c| {
            Lazy::force(c);
        });

    // gauges
    WALRECEIVER_ACTIVE_MANAGERS.get();

    // histograms
    [
        &READ_NUM_FS_LAYERS,
        &WAIT_LSN_TIME,
        &WAL_REDO_TIME,
        &WAL_REDO_WAIT_TIME,
        &WAL_REDO_RECORDS_HISTOGRAM,
        &WAL_REDO_BYTES_HISTOGRAM,
    ]
    .into_iter()
    .for_each(|h| {
        Lazy::force(h);
    });

    // Custom
    Lazy::force(&RECONSTRUCT_TIME);
}
