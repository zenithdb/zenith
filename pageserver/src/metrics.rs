use enum_map::EnumMap;
use metrics::{
    register_counter_vec, register_gauge_vec, register_histogram, register_histogram_vec,
    register_int_counter, register_int_counter_pair_vec, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, register_uint_gauge, register_uint_gauge_vec,
    Counter, CounterVec, GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterPair,
    IntCounterPairVec, IntCounterVec, IntGauge, IntGaugeVec, UIntGauge, UIntGaugeVec,
};
use once_cell::sync::Lazy;
use pageserver_api::shard::TenantShardId;
use strum::{EnumCount, VariantNames};
use strum_macros::{IntoStaticStr, VariantNames};
use tracing::warn;
use utils::id::TimelineId;

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
#[derive(Debug, VariantNames, IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum StorageTimeOperation {
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

    #[strum(serialize = "find gc cutoffs")]
    FindGcCutoffs,
}

pub(crate) static STORAGE_TIME_SUM_PER_TIMELINE: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "pageserver_storage_operations_seconds_sum",
        "Total time spent on storage operations with operation, tenant and timeline dimensions",
        &["operation", "tenant_id", "shard_id", "timeline_id"],
    )
    .expect("failed to define a metric")
});

pub(crate) static STORAGE_TIME_COUNT_PER_TIMELINE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_storage_operations_seconds_count",
        "Count of storage operations with operation, tenant and timeline dimensions",
        &["operation", "tenant_id", "shard_id", "timeline_id"],
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

pub(crate) static READ_NUM_LAYERS_VISITED: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_layers_visited_per_read_global",
        "Number of layers visited to reconstruct one key",
        vec![1.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0],
    )
    .expect("failed to define a metric")
});

pub(crate) static VEC_READ_NUM_LAYERS_VISITED: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_layers_visited_per_vectored_read_global",
        "Average number of layers visited to reconstruct one key",
        vec![1.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0],
    )
    .expect("failed to define a metric")
});

// Metrics collected on operations on the storage repository.
#[derive(
    Clone, Copy, enum_map::Enum, strum_macros::EnumString, strum_macros::Display, IntoStaticStr,
)]
pub(crate) enum GetKind {
    Singular,
    Vectored,
}

pub(crate) struct ReconstructTimeMetrics {
    singular: Histogram,
    vectored: Histogram,
}

pub(crate) static RECONSTRUCT_TIME: Lazy<ReconstructTimeMetrics> = Lazy::new(|| {
    let inner = register_histogram_vec!(
        "pageserver_getpage_reconstruct_seconds",
        "Time spent in reconstruct_value (reconstruct a page from deltas)",
        &["get_kind"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric");

    ReconstructTimeMetrics {
        singular: inner.with_label_values(&[GetKind::Singular.into()]),
        vectored: inner.with_label_values(&[GetKind::Vectored.into()]),
    }
});

impl ReconstructTimeMetrics {
    pub(crate) fn for_get_kind(&self, get_kind: GetKind) -> &Histogram {
        match get_kind {
            GetKind::Singular => &self.singular,
            GetKind::Vectored => &self.vectored,
        }
    }
}

pub(crate) struct ReconstructDataTimeMetrics {
    singular: Histogram,
    vectored: Histogram,
}

impl ReconstructDataTimeMetrics {
    pub(crate) fn for_get_kind(&self, get_kind: GetKind) -> &Histogram {
        match get_kind {
            GetKind::Singular => &self.singular,
            GetKind::Vectored => &self.vectored,
        }
    }
}

pub(crate) static GET_RECONSTRUCT_DATA_TIME: Lazy<ReconstructDataTimeMetrics> = Lazy::new(|| {
    let inner = register_histogram_vec!(
        "pageserver_getpage_get_reconstruct_data_seconds",
        "Time spent in get_reconstruct_value_data",
        &["get_kind"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric");

    ReconstructDataTimeMetrics {
        singular: inner.with_label_values(&[GetKind::Singular.into()]),
        vectored: inner.with_label_values(&[GetKind::Vectored.into()]),
    }
});

pub(crate) struct GetVectoredLatency {
    map: EnumMap<TaskKind, Option<Histogram>>,
}

#[allow(dead_code)]
pub(crate) struct ScanLatency {
    map: EnumMap<TaskKind, Option<Histogram>>,
}

impl GetVectoredLatency {
    // Only these task types perform vectored gets. Filter all other tasks out to reduce total
    // cardinality of the metric.
    const TRACKED_TASK_KINDS: [TaskKind; 2] = [TaskKind::Compaction, TaskKind::PageRequestHandler];

    pub(crate) fn for_task_kind(&self, task_kind: TaskKind) -> Option<&Histogram> {
        self.map[task_kind].as_ref()
    }
}

impl ScanLatency {
    // Only these task types perform vectored gets. Filter all other tasks out to reduce total
    // cardinality of the metric.
    const TRACKED_TASK_KINDS: [TaskKind; 1] = [TaskKind::PageRequestHandler];

    pub(crate) fn for_task_kind(&self, task_kind: TaskKind) -> Option<&Histogram> {
        self.map[task_kind].as_ref()
    }
}

pub(crate) struct ScanLatencyOngoingRecording<'a> {
    parent: &'a Histogram,
    start: std::time::Instant,
}

impl<'a> ScanLatencyOngoingRecording<'a> {
    pub(crate) fn start_recording(parent: &'a Histogram) -> ScanLatencyOngoingRecording<'a> {
        let start = Instant::now();
        ScanLatencyOngoingRecording { parent, start }
    }

    pub(crate) fn observe(self, throttled: Option<Duration>) {
        let elapsed = self.start.elapsed();
        let ex_throttled = if let Some(throttled) = throttled {
            elapsed.checked_sub(throttled)
        } else {
            Some(elapsed)
        };
        if let Some(ex_throttled) = ex_throttled {
            self.parent.observe(ex_throttled.as_secs_f64());
        } else {
            use utils::rate_limit::RateLimit;
            static LOGGED: Lazy<Mutex<RateLimit>> =
                Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(10))));
            let mut rate_limit = LOGGED.lock().unwrap();
            rate_limit.call(|| {
                warn!("error deducting time spent throttled; this message is logged at a global rate limit");
            });
        }
    }
}

pub(crate) static GET_VECTORED_LATENCY: Lazy<GetVectoredLatency> = Lazy::new(|| {
    let inner = register_histogram_vec!(
        "pageserver_get_vectored_seconds",
        "Time spent in get_vectored, excluding time spent in timeline_get_throttle.",
        &["task_kind"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric");

    GetVectoredLatency {
        map: EnumMap::from_array(std::array::from_fn(|task_kind_idx| {
            let task_kind = <TaskKind as enum_map::Enum>::from_usize(task_kind_idx);

            if GetVectoredLatency::TRACKED_TASK_KINDS.contains(&task_kind) {
                let task_kind = task_kind.into();
                Some(inner.with_label_values(&[task_kind]))
            } else {
                None
            }
        })),
    }
});

pub(crate) static SCAN_LATENCY: Lazy<ScanLatency> = Lazy::new(|| {
    let inner = register_histogram_vec!(
        "pageserver_scan_seconds",
        "Time spent in scan, excluding time spent in timeline_get_throttle.",
        &["task_kind"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric");

    ScanLatency {
        map: EnumMap::from_array(std::array::from_fn(|task_kind_idx| {
            let task_kind = <TaskKind as enum_map::Enum>::from_usize(task_kind_idx);

            if ScanLatency::TRACKED_TASK_KINDS.contains(&task_kind) {
                let task_kind = task_kind.into();
                Some(inner.with_label_values(&[task_kind]))
            } else {
                None
            }
        })),
    }
});

pub(crate) struct PageCacheMetricsForTaskKind {
    pub read_accesses_immutable: IntCounter,
    pub read_hits_immutable: IntCounter,
}

pub(crate) struct PageCacheMetrics {
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

pub(crate) static PAGE_CACHE: Lazy<PageCacheMetrics> = Lazy::new(|| PageCacheMetrics {
    map: EnumMap::from_array(std::array::from_fn(|task_kind| {
        let task_kind = <TaskKind as enum_map::Enum>::from_usize(task_kind);
        let task_kind: &'static str = task_kind.into();
        EnumMap::from_array(std::array::from_fn(|content_kind| {
            let content_kind = <PageContentKind as enum_map::Enum>::from_usize(content_kind);
            let content_kind: &'static str = content_kind.into();
            PageCacheMetricsForTaskKind {
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
            }
        }))
    })),
});

impl PageCacheMetrics {
    pub(crate) fn for_ctx(&self, ctx: &RequestContext) -> &PageCacheMetricsForTaskKind {
        &self.map[ctx.task_kind()][ctx.page_content_kind()]
    }
}

pub(crate) struct PageCacheSizeMetrics {
    pub max_bytes: UIntGauge,

    pub current_bytes_immutable: UIntGauge,
}

static PAGE_CACHE_SIZE_CURRENT_BYTES: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_page_cache_size_current_bytes",
        "Current size of the page cache in bytes, by key kind",
        &["key_kind"]
    )
    .expect("failed to define a metric")
});

pub(crate) static PAGE_CACHE_SIZE: Lazy<PageCacheSizeMetrics> =
    Lazy::new(|| PageCacheSizeMetrics {
        max_bytes: {
            register_uint_gauge!(
                "pageserver_page_cache_size_max_bytes",
                "Maximum size of the page cache in bytes"
            )
            .expect("failed to define a metric")
        },
        current_bytes_immutable: {
            PAGE_CACHE_SIZE_CURRENT_BYTES
                .get_metric_with_label_values(&["immutable"])
                .unwrap()
        },
    });

pub(crate) mod page_cache_eviction_metrics {
    use std::num::NonZeroUsize;

    use metrics::{register_int_counter_vec, IntCounter, IntCounterVec};
    use once_cell::sync::Lazy;

    #[derive(Clone, Copy)]
    pub(crate) enum Outcome {
        FoundSlotUnused { iters: NonZeroUsize },
        FoundSlotEvicted { iters: NonZeroUsize },
        ItersExceeded { iters: NonZeroUsize },
    }

    static ITERS_TOTAL_VEC: Lazy<IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "pageserver_page_cache_find_victim_iters_total",
            "Counter for the number of iterations in the find_victim loop",
            &["outcome"],
        )
        .expect("failed to define a metric")
    });

    static CALLS_VEC: Lazy<IntCounterVec> = Lazy::new(|| {
        register_int_counter_vec!(
            "pageserver_page_cache_find_victim_calls",
            "Incremented at the end of each find_victim() call.\
             Filter by outcome to get e.g., eviction rate.",
            &["outcome"]
        )
        .unwrap()
    });

    pub(crate) fn observe(outcome: Outcome) {
        macro_rules! dry {
            ($label:literal, $iters:expr) => {{
                static LABEL: &'static str = $label;
                static ITERS_TOTAL: Lazy<IntCounter> =
                    Lazy::new(|| ITERS_TOTAL_VEC.with_label_values(&[LABEL]));
                static CALLS: Lazy<IntCounter> =
                    Lazy::new(|| CALLS_VEC.with_label_values(&[LABEL]));
                ITERS_TOTAL.inc_by(($iters.get()) as u64);
                CALLS.inc();
            }};
        }
        match outcome {
            Outcome::FoundSlotUnused { iters } => dry!("found_empty", iters),
            Outcome::FoundSlotEvicted { iters } => {
                dry!("found_evicted", iters)
            }
            Outcome::ItersExceeded { iters } => {
                dry!("err_iters_exceeded", iters);
                super::page_cache_errors_inc(super::PageCacheErrorKind::EvictIterLimit);
            }
        }
    }
}

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
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static PITR_HISTORY_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_pitr_history_size",
        "Data written since PITR cutoff on this timeline",
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

#[derive(strum_macros::EnumString, strum_macros::Display, strum_macros::IntoStaticStr)]
#[strum(serialize_all = "kebab_case")]
pub(crate) enum MetricLayerKind {
    Delta,
    Image,
}

static TIMELINE_LAYER_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_layer_bytes",
        "Sum of layer physical sizes in bytes",
        &["tenant_id", "shard_id", "timeline_id", "kind"]
    )
    .expect("failed to define a metric")
});

static TIMELINE_LAYER_COUNT: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_layer_count",
        "Number of layers that exist",
        &["tenant_id", "shard_id", "timeline_id", "kind"]
    )
    .expect("failed to define a metric")
});

static TIMELINE_ARCHIVE_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_archive_size",
        "Timeline's logical size if it is considered eligible for archival (outside PITR window), else zero",
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static STANDBY_HORIZON: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_standby_horizon",
        "Standby apply LSN for which GC is hold off, by timeline.",
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static RESIDENT_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_resident_physical_size",
        "The size of the layer files present in the pageserver's filesystem, for attached locations.",
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static VISIBLE_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_visible_physical_size",
        "The size of the layer files present in the pageserver's filesystem.",
        &["tenant_id", "shard_id", "timeline_id"]
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
        "The size of the layer files present in the remote storage that are listed in the remote index_part.json.",
        // Corollary: If any files are missing from the index part, they won't be included here.
        &["tenant_id", "shard_id", "timeline_id"]
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
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define current logical size metric")
});

static AUX_FILE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "pageserver_aux_file_estimated_size",
        "The size of all aux files for a timeline in aux file v2 store.",
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static VALID_LSN_LEASE_COUNT: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_valid_lsn_lease_count",
        "The number of valid leases after refreshing gc info.",
        &["tenant_id", "shard_id", "timeline_id"],
    )
    .expect("failed to define a metric")
});

pub(crate) static CIRCUIT_BREAKERS_BROKEN: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_circuit_breaker_broken",
        "How many times a circuit breaker has broken"
    )
    .expect("failed to define a metric")
});

pub(crate) static CIRCUIT_BREAKERS_UNBROKEN: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_circuit_breaker_unbroken",
        "How many times a circuit breaker has been un-broken (recovered)"
    )
    .expect("failed to define a metric")
});

pub(crate) static COMPRESSION_IMAGE_INPUT_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_compression_image_in_bytes_total",
        "Size of data written into image layers before compression"
    )
    .expect("failed to define a metric")
});

pub(crate) static COMPRESSION_IMAGE_INPUT_BYTES_CONSIDERED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_compression_image_in_bytes_considered",
        "Size of potentially compressible data written into image layers before compression"
    )
    .expect("failed to define a metric")
});

pub(crate) static COMPRESSION_IMAGE_INPUT_BYTES_CHOSEN: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_compression_image_in_bytes_chosen",
        "Size of data whose compressed form was written into image layers"
    )
    .expect("failed to define a metric")
});

pub(crate) static COMPRESSION_IMAGE_OUTPUT_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "pageserver_compression_image_out_bytes_total",
        "Size of compressed image layer written"
    )
    .expect("failed to define a metric")
});

pub(crate) mod initial_logical_size {
    use metrics::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec};
    use once_cell::sync::Lazy;

    pub(crate) struct StartCalculation(IntCounterVec);
    pub(crate) static START_CALCULATION: Lazy<StartCalculation> = Lazy::new(|| {
        StartCalculation(
            register_int_counter_vec!(
                "pageserver_initial_logical_size_start_calculation",
                "Incremented each time we start an initial logical size calculation attempt. \
                 The `circumstances` label provides some additional details.",
                &["attempt", "circumstances"]
            )
            .unwrap(),
        )
    });

    struct DropCalculation {
        first: IntCounter,
        retry: IntCounter,
    }

    static DROP_CALCULATION: Lazy<DropCalculation> = Lazy::new(|| {
        let vec = register_int_counter_vec!(
            "pageserver_initial_logical_size_drop_calculation",
            "Incremented each time we abort a started size calculation attmpt.",
            &["attempt"]
        )
        .unwrap();
        DropCalculation {
            first: vec.with_label_values(&["first"]),
            retry: vec.with_label_values(&["retry"]),
        }
    });

    pub(crate) struct Calculated {
        pub(crate) births: IntCounter,
        pub(crate) deaths: IntCounter,
    }

    pub(crate) static CALCULATED: Lazy<Calculated> = Lazy::new(|| Calculated {
        births: register_int_counter!(
            "pageserver_initial_logical_size_finish_calculation",
            "Incremented every time we finish calculation of initial logical size.\
             If everything is working well, this should happen at most once per Timeline object."
        )
        .unwrap(),
        deaths: register_int_counter!(
            "pageserver_initial_logical_size_drop_finished_calculation",
            "Incremented when we drop a finished initial logical size calculation result.\
             Mainly useful to turn pageserver_initial_logical_size_finish_calculation into a gauge."
        )
        .unwrap(),
    });

    pub(crate) struct OngoingCalculationGuard {
        inc_drop_calculation: Option<IntCounter>,
    }

    #[derive(strum_macros::IntoStaticStr)]
    pub(crate) enum StartCircumstances {
        EmptyInitial,
        SkippedConcurrencyLimiter,
        AfterBackgroundTasksRateLimit,
    }

    impl StartCalculation {
        pub(crate) fn first(&self, circumstances: StartCircumstances) -> OngoingCalculationGuard {
            let circumstances_label: &'static str = circumstances.into();
            self.0
                .with_label_values(&["first", circumstances_label])
                .inc();
            OngoingCalculationGuard {
                inc_drop_calculation: Some(DROP_CALCULATION.first.clone()),
            }
        }
        pub(crate) fn retry(&self, circumstances: StartCircumstances) -> OngoingCalculationGuard {
            let circumstances_label: &'static str = circumstances.into();
            self.0
                .with_label_values(&["retry", circumstances_label])
                .inc();
            OngoingCalculationGuard {
                inc_drop_calculation: Some(DROP_CALCULATION.retry.clone()),
            }
        }
    }

    impl Drop for OngoingCalculationGuard {
        fn drop(&mut self) {
            if let Some(counter) = self.inc_drop_calculation.take() {
                counter.inc();
            }
        }
    }

    impl OngoingCalculationGuard {
        pub(crate) fn calculation_result_saved(mut self) -> FinishedCalculationGuard {
            drop(self.inc_drop_calculation.take());
            CALCULATED.births.inc();
            FinishedCalculationGuard {
                inc_on_drop: CALCULATED.deaths.clone(),
            }
        }
    }

    pub(crate) struct FinishedCalculationGuard {
        inc_on_drop: IntCounter,
    }

    impl Drop for FinishedCalculationGuard {
        fn drop(&mut self) {
            self.inc_on_drop.inc();
        }
    }

    // context: https://github.com/neondatabase/neon/issues/5963
    pub(crate) static TIMELINES_WHERE_WALRECEIVER_GOT_APPROXIMATE_SIZE: Lazy<IntCounter> =
        Lazy::new(|| {
            register_int_counter!(
                "pageserver_initial_logical_size_timelines_where_walreceiver_got_approximate_size",
                "Counter for the following event: walreceiver calls\
                 Timeline::get_current_logical_size() and it returns `Approximate` for the first time."
            )
            .unwrap()
        });
}

static DIRECTORY_ENTRIES_COUNT: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_directory_entries_count",
        "Sum of the entries in pageserver-stored directory listings",
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
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
        &["tenant_id", "shard_id"]
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
        &["tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

static EVICTIONS_WITH_LOW_RESIDENCE_DURATION: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_evictions_with_low_residence_duration",
        "If a layer is evicted that was resident for less than `low_threshold`, it is counted to this counter. \
         Residence duration is determined using the `residence_duration_data_source`.",
        &["tenant_id", "shard_id", "timeline_id", "residence_duration_data_source", "low_threshold_secs"]
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

pub(crate) static TIMELINE_EPHEMERAL_BYTES: Lazy<UIntGauge> = Lazy::new(|| {
    register_uint_gauge!(
        "pageserver_timeline_ephemeral_bytes",
        "Total number of bytes in ephemeral layers, summed for all timelines.  Approximate, lazily updated."
    )
    .expect("Failed to register metric")
});

/// Metrics related to the lifecycle of a [`crate::tenant::Tenant`] object: things
/// like how long it took to load.
///
/// Note that these are process-global metrics, _not_ per-tenant metrics.  Per-tenant
/// metrics are rather expensive, and usually fine grained stuff makes more sense
/// at a timeline level than tenant level.
pub(crate) struct TenantMetrics {
    /// How long did tenants take to go from construction to active state?
    pub(crate) activation: Histogram,
    pub(crate) preload: Histogram,
    pub(crate) attach: Histogram,

    /// How many tenants are included in the initial startup of the pagesrever?
    pub(crate) startup_scheduled: IntCounter,
    pub(crate) startup_complete: IntCounter,
}

pub(crate) static TENANT: Lazy<TenantMetrics> = Lazy::new(|| {
    TenantMetrics {
    activation: register_histogram!(
        "pageserver_tenant_activation_seconds",
        "Time taken by tenants to activate, in seconds",
        CRITICAL_OP_BUCKETS.into()
    )
    .expect("Failed to register metric"),
    preload: register_histogram!(
        "pageserver_tenant_preload_seconds",
        "Time taken by tenants to load remote metadata on startup/attach, in seconds",
        CRITICAL_OP_BUCKETS.into()
    )
    .expect("Failed to register metric"),
    attach: register_histogram!(
        "pageserver_tenant_attach_seconds",
        "Time taken by tenants to intialize, after remote metadata is already loaded",
        CRITICAL_OP_BUCKETS.into()
    )
    .expect("Failed to register metric"),
    startup_scheduled: register_int_counter!(
        "pageserver_tenant_startup_scheduled",
        "Number of tenants included in pageserver startup (doesn't count tenants attached later)"
    ).expect("Failed to register metric"),
    startup_complete: register_int_counter!(
        "pageserver_tenant_startup_complete",
        "Number of tenants that have completed warm-up, or activated on-demand during initial startup: \
         should eventually reach `pageserver_tenant_startup_scheduled_total`.  Does not include broken \
         tenants: such cases will lead to this metric never reaching the scheduled count."
    ).expect("Failed to register metric"),
}
});

/// Each `Timeline`'s  [`EVICTIONS_WITH_LOW_RESIDENCE_DURATION`] metric.
#[derive(Debug)]
pub(crate) struct EvictionsWithLowResidenceDuration {
    data_source: &'static str,
    threshold: Duration,
    counter: Option<IntCounter>,
}

pub(crate) struct EvictionsWithLowResidenceDurationBuilder {
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

    fn build(
        &self,
        tenant_id: &str,
        shard_id: &str,
        timeline_id: &str,
    ) -> EvictionsWithLowResidenceDuration {
        let counter = EVICTIONS_WITH_LOW_RESIDENCE_DURATION
            .get_metric_with_label_values(&[
                tenant_id,
                shard_id,
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
        shard_id: &str,
        timeline_id: &str,
        new_threshold: Duration,
    ) {
        if new_threshold == self.threshold {
            return;
        }
        let mut with_new = EvictionsWithLowResidenceDurationBuilder::new(
            self.data_source,
            new_threshold,
        )
        .build(tenant_id, shard_id, timeline_id);
        std::mem::swap(self, &mut with_new);
        with_new.remove(tenant_id, shard_id, timeline_id);
    }

    // This could be a `Drop` impl, but, we need the `tenant_id` and `timeline_id`.
    fn remove(&mut self, tenant_id: &str, shard_id: &str, timeline_id: &str) {
        let Some(_counter) = self.counter.take() else {
            return;
        };

        let threshold = Self::threshold_label_value(self.threshold);

        let removed = EVICTIONS_WITH_LOW_RESIDENCE_DURATION.remove_label_values(&[
            tenant_id,
            shard_id,
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
/// - close (dropping [`crate::virtual_file::VirtualFile`])
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
    OpenAfterReplace,
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
            StorageIoOperation::OpenAfterReplace => "open-after-replace",
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
            storage_io_histogram_vec
                .get_metric_with_label_values(&[op.as_str()])
                .unwrap()
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
        &["operation", "tenant_id", "shard_id", "timeline_id"]
    )
    .expect("failed to define a metric")
});

#[cfg(not(test))]
pub(crate) mod virtual_file_descriptor_cache {
    use super::*;

    pub(crate) static SIZE_MAX: Lazy<UIntGauge> = Lazy::new(|| {
        register_uint_gauge!(
            "pageserver_virtual_file_descriptor_cache_size_max",
            "Maximum number of open file descriptors in the cache."
        )
        .unwrap()
    });

    // SIZE_CURRENT: derive it like so:
    // ```
    // sum (pageserver_io_operations_seconds_count{operation=~"^(open|open-after-replace)$")
    // -ignoring(operation)
    // sum(pageserver_io_operations_seconds_count{operation=~"^(close|close-by-replace)$"}
    // ```
}

#[cfg(not(test))]
pub(crate) mod virtual_file_io_engine {
    use super::*;

    pub(crate) static KIND: Lazy<UIntGaugeVec> = Lazy::new(|| {
        register_uint_gauge_vec!(
            "pageserver_virtual_file_io_engine_kind",
            "The configured io engine for VirtualFile",
            &["kind"],
        )
        .unwrap()
    });
}

struct GlobalAndPerTimelineHistogramTimer<'a, 'c> {
    global_metric: &'a Histogram,

    // Optional because not all op types are tracked per-timeline
    timeline_metric: Option<&'a Histogram>,

    ctx: &'c RequestContext,
    start: std::time::Instant,
    op: SmgrQueryType,
    count: usize,
}

impl<'a, 'c> Drop for GlobalAndPerTimelineHistogramTimer<'a, 'c> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let ex_throttled = self
            .ctx
            .micros_spent_throttled
            .close_and_checked_sub_from(elapsed);
        let ex_throttled = match ex_throttled {
            Ok(res) => res,
            Err(error) => {
                use utils::rate_limit::RateLimit;
                static LOGGED: Lazy<Mutex<enum_map::EnumMap<SmgrQueryType, RateLimit>>> =
                    Lazy::new(|| {
                        Mutex::new(enum_map::EnumMap::from_array(std::array::from_fn(|_| {
                            RateLimit::new(Duration::from_secs(10))
                        })))
                    });
                let mut guard = LOGGED.lock().unwrap();
                let rate_limit = &mut guard[self.op];
                rate_limit.call(|| {
                    warn!(op=?self.op, error, "error deducting time spent throttled; this message is logged at a global rate limit");
                });
                elapsed
            }
        };
        for _ in 0..self.count {
            self.global_metric.observe(ex_throttled.as_secs_f64());
            if let Some(timeline_metric) = self.timeline_metric {
                timeline_metric.observe(ex_throttled.as_secs_f64());
            }
        }
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
    enum_map::Enum,
)]
#[strum(serialize_all = "snake_case")]
pub enum SmgrQueryType {
    GetRelExists,
    GetRelSize,
    GetPageAtLsn,
    GetDbSize,
    GetSlruSegment,
}

#[derive(Debug)]
pub(crate) struct SmgrQueryTimePerTimeline {
    global_metrics: [Histogram; SmgrQueryType::COUNT],
    per_timeline_getpage: Histogram,
}

static SMGR_QUERY_TIME_PER_TENANT_TIMELINE: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_smgr_query_seconds",
        "Time spent on smgr query handling, aggegated by query type and tenant/timeline.",
        &["smgr_query_type", "tenant_id", "shard_id", "timeline_id"],
        CRITICAL_OP_BUCKETS.into(),
    )
    .expect("failed to define a metric")
});

static SMGR_QUERY_TIME_GLOBAL_BUCKETS: Lazy<Vec<f64>> = Lazy::new(|| {
    [
        1,
        10,
        20,
        40,
        60,
        80,
        100,
        200,
        300,
        400,
        500,
        600,
        700,
        800,
        900,
        1_000, // 1ms
        2_000,
        4_000,
        6_000,
        8_000,
        10_000, // 10ms
        20_000,
        40_000,
        60_000,
        80_000,
        100_000,
        200_000,
        400_000,
        600_000,
        800_000,
        1_000_000, // 1s
        2_000_000,
        4_000_000,
        6_000_000,
        8_000_000,
        10_000_000, // 10s
        20_000_000,
        50_000_000,
        100_000_000,
        200_000_000,
        1_000_000_000, // 1000s
    ]
    .into_iter()
    .map(Duration::from_micros)
    .map(|d| d.as_secs_f64())
    .collect()
});

static SMGR_QUERY_TIME_GLOBAL: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "pageserver_smgr_query_seconds_global",
        "Time spent on smgr query handling, aggregated by query type.",
        &["smgr_query_type"],
        SMGR_QUERY_TIME_GLOBAL_BUCKETS.clone(),
    )
    .expect("failed to define a metric")
});

impl SmgrQueryTimePerTimeline {
    pub(crate) fn new(tenant_shard_id: &TenantShardId, timeline_id: &TimelineId) -> Self {
        let tenant_id = tenant_shard_id.tenant_id.to_string();
        let shard_slug = format!("{}", tenant_shard_id.shard_slug());
        let timeline_id = timeline_id.to_string();
        let global_metrics = std::array::from_fn(|i| {
            let op = SmgrQueryType::from_repr(i).unwrap();
            SMGR_QUERY_TIME_GLOBAL
                .get_metric_with_label_values(&[op.into()])
                .unwrap()
        });

        let per_timeline_getpage = SMGR_QUERY_TIME_PER_TENANT_TIMELINE
            .get_metric_with_label_values(&[
                SmgrQueryType::GetPageAtLsn.into(),
                &tenant_id,
                &shard_slug,
                &timeline_id,
            ])
            .unwrap();
        Self {
            global_metrics,
            per_timeline_getpage,
        }
    }
    pub(crate) fn start_timer<'c: 'a, 'a>(
        &'a self,
        op: SmgrQueryType,
        ctx: &'c RequestContext,
    ) -> Option<impl Drop + '_> {
        self.start_timer_many(op, 1, ctx)
    }
    pub(crate) fn start_timer_many<'c: 'a, 'a>(
        &'a self,
        op: SmgrQueryType,
        count: usize,
        ctx: &'c RequestContext,
    ) -> Option<impl Drop + '_> {
        let global_metric = &self.global_metrics[op as usize];
        let start = Instant::now();
        match ctx.micros_spent_throttled.open() {
            Ok(()) => (),
            Err(error) => {
                use utils::rate_limit::RateLimit;
                static LOGGED: Lazy<Mutex<enum_map::EnumMap<SmgrQueryType, RateLimit>>> =
                    Lazy::new(|| {
                        Mutex::new(enum_map::EnumMap::from_array(std::array::from_fn(|_| {
                            RateLimit::new(Duration::from_secs(10))
                        })))
                    });
                let mut guard = LOGGED.lock().unwrap();
                let rate_limit = &mut guard[op];
                rate_limit.call(|| {
                    warn!(?op, error, "error opening micros_spent_throttled; this message is logged at a global rate limit");
                });
            }
        }

        let timeline_metric = if matches!(op, SmgrQueryType::GetPageAtLsn) {
            Some(&self.per_timeline_getpage)
        } else {
            None
        };

        Some(GlobalAndPerTimelineHistogramTimer {
            global_metric,
            timeline_metric,
            ctx,
            start,
            op,
            count,
        })
    }
}

#[cfg(test)]
mod smgr_query_time_tests {
    use pageserver_api::shard::TenantShardId;
    use strum::IntoEnumIterator;
    use utils::id::{TenantId, TimelineId};

    use crate::{
        context::{DownloadBehavior, RequestContext},
        task_mgr::TaskKind,
    };

    // Regression test, we used hard-coded string constants before using an enum.
    #[test]
    fn op_label_name() {
        use super::SmgrQueryType::*;
        let expect: [(super::SmgrQueryType, &'static str); 5] = [
            (GetRelExists, "get_rel_exists"),
            (GetRelSize, "get_rel_size"),
            (GetPageAtLsn, "get_page_at_lsn"),
            (GetDbSize, "get_db_size"),
            (GetSlruSegment, "get_slru_segment"),
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
            let metrics = super::SmgrQueryTimePerTimeline::new(
                &TenantShardId::unsharded(tenant_id),
                &timeline_id,
            );

            let get_counts = || {
                let global: u64 = ops
                    .iter()
                    .map(|op| metrics.global_metrics[*op as usize].get_sample_count())
                    .sum();
                (global, metrics.per_timeline_getpage.get_sample_count())
            };

            let (pre_global, pre_per_tenant_timeline) = get_counts();
            assert_eq!(pre_per_tenant_timeline, 0);

            let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Download);
            let timer = metrics.start_timer(*op, &ctx);
            drop(timer);

            let (post_global, post_per_tenant_timeline) = get_counts();
            if matches!(op, super::SmgrQueryType::GetPageAtLsn) {
                // getpage ops are tracked per-timeline, others aren't
                assert_eq!(post_per_tenant_timeline, 1);
            } else {
                assert_eq!(post_per_tenant_timeline, 0);
            }
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

pub(crate) struct BasebackupQueryTime {
    ok: Histogram,
    error: Histogram,
}

pub(crate) static BASEBACKUP_QUERY_TIME: Lazy<BasebackupQueryTime> = Lazy::new(|| {
    let vec = register_histogram_vec!(
        "pageserver_basebackup_query_seconds",
        "Histogram of basebackup queries durations, by result type",
        &["result"],
        COMPUTE_STARTUP_BUCKETS.to_vec(),
    )
    .expect("failed to define a metric");
    BasebackupQueryTime {
        ok: vec.get_metric_with_label_values(&["ok"]).unwrap(),
        error: vec.get_metric_with_label_values(&["error"]).unwrap(),
    }
});

pub(crate) struct BasebackupQueryTimeOngoingRecording<'a, 'c> {
    parent: &'a BasebackupQueryTime,
    ctx: &'c RequestContext,
    start: std::time::Instant,
}

impl BasebackupQueryTime {
    pub(crate) fn start_recording<'c: 'a, 'a>(
        &'a self,
        ctx: &'c RequestContext,
    ) -> BasebackupQueryTimeOngoingRecording<'_, '_> {
        let start = Instant::now();
        match ctx.micros_spent_throttled.open() {
            Ok(()) => (),
            Err(error) => {
                use utils::rate_limit::RateLimit;
                static LOGGED: Lazy<Mutex<RateLimit>> =
                    Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(10))));
                let mut rate_limit = LOGGED.lock().unwrap();
                rate_limit.call(|| {
                    warn!(error, "error opening micros_spent_throttled; this message is logged at a global rate limit");
                });
            }
        }
        BasebackupQueryTimeOngoingRecording {
            parent: self,
            ctx,
            start,
        }
    }
}

impl<'a, 'c> BasebackupQueryTimeOngoingRecording<'a, 'c> {
    pub(crate) fn observe<T, E>(self, res: &Result<T, E>) {
        let elapsed = self.start.elapsed();
        let ex_throttled = self
            .ctx
            .micros_spent_throttled
            .close_and_checked_sub_from(elapsed);
        let ex_throttled = match ex_throttled {
            Ok(ex_throttled) => ex_throttled,
            Err(error) => {
                use utils::rate_limit::RateLimit;
                static LOGGED: Lazy<Mutex<RateLimit>> =
                    Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(10))));
                let mut rate_limit = LOGGED.lock().unwrap();
                rate_limit.call(|| {
                    warn!(error, "error deducting time spent throttled; this message is logged at a global rate limit");
                });
                elapsed
            }
        };
        let metric = if res.is_ok() {
            &self.parent.ok
        } else {
            &self.parent.error
        };
        metric.observe(ex_throttled.as_secs_f64());
    }
}

pub(crate) static LIVE_CONNECTIONS: Lazy<IntCounterPairVec> = Lazy::new(|| {
    register_int_counter_pair_vec!(
        "pageserver_live_connections_started",
        "Number of network connections that we started handling",
        "pageserver_live_connections_finished",
        "Number of network connections that we finished handling",
        &["pageserver_connection_kind"]
    )
    .expect("failed to define a metric")
});

#[derive(Clone, Copy, enum_map::Enum, IntoStaticStr)]
pub(crate) enum ComputeCommandKind {
    PageStreamV2,
    Basebackup,
    Fullbackup,
    LeaseLsn,
}

pub(crate) struct ComputeCommandCounters {
    map: EnumMap<ComputeCommandKind, IntCounter>,
}

pub(crate) static COMPUTE_COMMANDS_COUNTERS: Lazy<ComputeCommandCounters> = Lazy::new(|| {
    let inner = register_int_counter_vec!(
        "pageserver_compute_commands",
        "Number of compute -> pageserver commands processed",
        &["command"]
    )
    .expect("failed to define a metric");

    ComputeCommandCounters {
        map: EnumMap::from_array(std::array::from_fn(|i| {
            let command = <ComputeCommandKind as enum_map::Enum>::from_usize(i);
            let command_str: &'static str = command.into();
            inner.with_label_values(&[command_str])
        })),
    }
});

impl ComputeCommandCounters {
    pub(crate) fn for_command(&self, command: ComputeCommandKind) -> &IntCounter {
        &self.map[command]
    }
}

// remote storage metrics

static REMOTE_TIMELINE_CLIENT_CALLS: Lazy<IntCounterPairVec> = Lazy::new(|| {
    register_int_counter_pair_vec!(
        "pageserver_remote_timeline_client_calls_started",
        "Number of started calls to remote timeline client.",
        "pageserver_remote_timeline_client_calls_finished",
        "Number of finshed calls to remote timeline client.",
        &[
            "tenant_id",
            "shard_id",
            "timeline_id",
            "file_kind",
            "op_kind"
        ],
    )
    .unwrap()
});

static REMOTE_TIMELINE_CLIENT_BYTES_STARTED_COUNTER: Lazy<IntCounterVec> =
    Lazy::new(|| {
        register_int_counter_vec!(
        "pageserver_remote_timeline_client_bytes_started",
        "Incremented by the number of bytes associated with a remote timeline client operation. \
         The increment happens when the operation is scheduled.",
        &["tenant_id", "shard_id", "timeline_id", "file_kind", "op_kind"],
    )
        .expect("failed to define a metric")
    });

static REMOTE_TIMELINE_CLIENT_BYTES_FINISHED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "pageserver_remote_timeline_client_bytes_finished",
        "Incremented by the number of bytes associated with a remote timeline client operation. \
         The increment happens when the operation finishes (regardless of success/failure/shutdown).",
        &["tenant_id", "shard_id", "timeline_id", "file_kind", "op_kind"],
    )
    .expect("failed to define a metric")
});

pub(crate) struct TenantManagerMetrics {
    tenant_slots_attached: UIntGauge,
    tenant_slots_secondary: UIntGauge,
    tenant_slots_inprogress: UIntGauge,
    pub(crate) tenant_slot_writes: IntCounter,
    pub(crate) unexpected_errors: IntCounter,
}

impl TenantManagerMetrics {
    /// Helpers for tracking slots.  Note that these do not track the lifetime of TenantSlot objects
    /// exactly: they track the lifetime of the slots _in the tenant map_.
    pub(crate) fn slot_inserted(&self, slot: &TenantSlot) {
        match slot {
            TenantSlot::Attached(_) => {
                self.tenant_slots_attached.inc();
            }
            TenantSlot::Secondary(_) => {
                self.tenant_slots_secondary.inc();
            }
            TenantSlot::InProgress(_) => {
                self.tenant_slots_inprogress.inc();
            }
        }
    }

    pub(crate) fn slot_removed(&self, slot: &TenantSlot) {
        match slot {
            TenantSlot::Attached(_) => {
                self.tenant_slots_attached.dec();
            }
            TenantSlot::Secondary(_) => {
                self.tenant_slots_secondary.dec();
            }
            TenantSlot::InProgress(_) => {
                self.tenant_slots_inprogress.dec();
            }
        }
    }

    #[cfg(all(debug_assertions, not(test)))]
    pub(crate) fn slots_total(&self) -> u64 {
        self.tenant_slots_attached.get()
            + self.tenant_slots_secondary.get()
            + self.tenant_slots_inprogress.get()
    }
}

pub(crate) static TENANT_MANAGER: Lazy<TenantManagerMetrics> = Lazy::new(|| {
    let tenant_slots = register_uint_gauge_vec!(
        "pageserver_tenant_manager_slots",
        "How many slots currently exist, including all attached, secondary and in-progress operations",
        &["mode"]
    )
    .expect("failed to define a metric");
    TenantManagerMetrics {
        tenant_slots_attached: tenant_slots
            .get_metric_with_label_values(&["attached"])
            .unwrap(),
        tenant_slots_secondary: tenant_slots
            .get_metric_with_label_values(&["secondary"])
            .unwrap(),
        tenant_slots_inprogress: tenant_slots
            .get_metric_with_label_values(&["inprogress"])
            .unwrap(),
        tenant_slot_writes: register_int_counter!(
            "pageserver_tenant_manager_slot_writes",
            "Writes to a tenant slot, including all of create/attach/detach/delete"
        )
        .expect("failed to define a metric"),
        unexpected_errors: register_int_counter!(
            "pageserver_tenant_manager_unexpected_errors_total",
            "Number of unexpected conditions encountered: nonzero value indicates a non-fatal bug."
        )
        .expect("failed to define a metric"),
    }
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

pub(crate) struct SecondaryModeMetrics {
    pub(crate) upload_heatmap: IntCounter,
    pub(crate) upload_heatmap_errors: IntCounter,
    pub(crate) upload_heatmap_duration: Histogram,
    pub(crate) download_heatmap: IntCounter,
    pub(crate) download_layer: IntCounter,
}
pub(crate) static SECONDARY_MODE: Lazy<SecondaryModeMetrics> = Lazy::new(|| {
    SecondaryModeMetrics {
    upload_heatmap: register_int_counter!(
        "pageserver_secondary_upload_heatmap",
        "Number of heatmaps written to remote storage by attached tenants"
    )
    .expect("failed to define a metric"),
    upload_heatmap_errors: register_int_counter!(
        "pageserver_secondary_upload_heatmap_errors",
        "Failures writing heatmap to remote storage"
    )
    .expect("failed to define a metric"),
    upload_heatmap_duration: register_histogram!(
        "pageserver_secondary_upload_heatmap_duration",
        "Time to build and upload a heatmap, including any waiting inside the remote storage client"
    )
    .expect("failed to define a metric"),
    download_heatmap: register_int_counter!(
        "pageserver_secondary_download_heatmap",
        "Number of downloads of heatmaps by secondary mode locations, including when it hasn't changed"
    )
    .expect("failed to define a metric"),
    download_layer: register_int_counter!(
        "pageserver_secondary_download_layer",
        "Number of downloads of layers by secondary mode locations"
    )
    .expect("failed to define a metric"),
}
});

pub(crate) static SECONDARY_RESIDENT_PHYSICAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_secondary_resident_physical_size",
        "The size of the layer files present in the pageserver's filesystem, for secondary locations.",
        &["tenant_id", "shard_id"]
    )
    .expect("failed to define a metric")
});

pub(crate) static NODE_UTILIZATION_SCORE: Lazy<UIntGauge> = Lazy::new(|| {
    register_uint_gauge!(
        "pageserver_utilization_score",
        "The utilization score we report to the storage controller for scheduling, where 0 is empty, 1000000 is full, and anything above is considered overloaded",
    )
    .expect("failed to define a metric")
});

pub(crate) static SECONDARY_HEATMAP_TOTAL_SIZE: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_secondary_heatmap_total_size",
        "The total size in bytes of all layers in the most recently downloaded heatmap.",
        &["tenant_id", "shard_id"]
    )
    .expect("failed to define a metric")
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

pub struct BackgroundLoopSemaphoreMetrics {
    counters: EnumMap<BackgroundLoopKind, IntCounterPair>,
    durations: EnumMap<BackgroundLoopKind, Counter>,
}

pub(crate) static BACKGROUND_LOOP_SEMAPHORE: Lazy<BackgroundLoopSemaphoreMetrics> = Lazy::new(
    || {
        let counters = register_int_counter_pair_vec!(
            "pageserver_background_loop_semaphore_wait_start_count",
            "Counter for background loop concurrency-limiting semaphore acquire calls started",
            "pageserver_background_loop_semaphore_wait_finish_count",
            "Counter for background loop concurrency-limiting semaphore acquire calls finished",
            &["task"],
        )
        .unwrap();

        let durations = register_counter_vec!(
            "pageserver_background_loop_semaphore_wait_duration_seconds",
            "Sum of wall clock time spent waiting on the background loop concurrency-limiting semaphore acquire calls",
            &["task"],
        )
        .unwrap();

        BackgroundLoopSemaphoreMetrics {
            counters: enum_map::EnumMap::from_array(std::array::from_fn(|i| {
                let kind = <BackgroundLoopKind as enum_map::Enum>::from_usize(i);
                counters.with_label_values(&[kind.into()])
            })),
            durations: enum_map::EnumMap::from_array(std::array::from_fn(|i| {
                let kind = <BackgroundLoopKind as enum_map::Enum>::from_usize(i);
                durations.with_label_values(&[kind.into()])
            })),
        }
    },
);

impl BackgroundLoopSemaphoreMetrics {
    pub(crate) fn measure_acquisition(&self, task: BackgroundLoopKind) -> impl Drop + '_ {
        struct Record<'a> {
            metrics: &'a BackgroundLoopSemaphoreMetrics,
            task: BackgroundLoopKind,
            _counter_guard: metrics::IntCounterPairGuard,
            start: Instant,
        }
        impl Drop for Record<'_> {
            fn drop(&mut self) {
                let elapsed = self.start.elapsed().as_secs_f64();
                self.metrics.durations[self.task].inc_by(elapsed);
            }
        }
        Record {
            metrics: self,
            task,
            _counter_guard: self.counters[task].guard(),
            start: Instant::now(),
        }
    }
}

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

pub(crate) struct WalIngestMetrics {
    pub(crate) bytes_received: IntCounter,
    pub(crate) records_received: IntCounter,
    pub(crate) records_committed: IntCounter,
    pub(crate) records_filtered: IntCounter,
}

pub(crate) static WAL_INGEST: Lazy<WalIngestMetrics> = Lazy::new(|| WalIngestMetrics {
    bytes_received: register_int_counter!(
        "pageserver_wal_ingest_bytes_received",
        "Bytes of WAL ingested from safekeepers",
    )
    .unwrap(),
    records_received: register_int_counter!(
        "pageserver_wal_ingest_records_received",
        "Number of WAL records received from safekeepers"
    )
    .expect("failed to define a metric"),
    records_committed: register_int_counter!(
        "pageserver_wal_ingest_records_committed",
        "Number of WAL records which resulted in writes to pageserver storage"
    )
    .expect("failed to define a metric"),
    records_filtered: register_int_counter!(
        "pageserver_wal_ingest_records_filtered",
        "Number of WAL records filtered out due to sharding"
    )
    .expect("failed to define a metric"),
});

pub(crate) static WAL_REDO_TIME: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_seconds",
        "Time spent on WAL redo",
        redo_histogram_time_buckets!()
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

#[rustfmt::skip]
pub(crate) static WAL_REDO_PROCESS_LAUNCH_DURATION_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "pageserver_wal_redo_process_launch_duration",
        "Histogram of the duration of successful WalRedoProcess::launch calls",
        vec![
            0.0002, 0.0004, 0.0006, 0.0008, 0.0010,
            0.0020, 0.0040, 0.0060, 0.0080, 0.0100,
            0.0200, 0.0400, 0.0600, 0.0800, 0.1000,
            0.2000, 0.4000, 0.6000, 0.8000, 1.0000,
            1.5000, 2.0000, 2.5000, 3.0000, 4.0000, 10.0000
        ],
    )
    .expect("failed to define a metric")
});

pub(crate) struct WalRedoProcessCounters {
    pub(crate) started: IntCounter,
    pub(crate) killed_by_cause: enum_map::EnumMap<WalRedoKillCause, IntCounter>,
    pub(crate) active_stderr_logger_tasks_started: IntCounter,
    pub(crate) active_stderr_logger_tasks_finished: IntCounter,
}

#[derive(Debug, enum_map::Enum, strum_macros::IntoStaticStr)]
pub(crate) enum WalRedoKillCause {
    WalRedoProcessDrop,
    NoLeakChildDrop,
    Startup,
}

impl Default for WalRedoProcessCounters {
    fn default() -> Self {
        let started = register_int_counter!(
            "pageserver_wal_redo_process_started_total",
            "Number of WAL redo processes started",
        )
        .unwrap();

        let killed = register_int_counter_vec!(
            "pageserver_wal_redo_process_stopped_total",
            "Number of WAL redo processes stopped",
            &["cause"],
        )
        .unwrap();

        let active_stderr_logger_tasks_started = register_int_counter!(
            "pageserver_walredo_stderr_logger_tasks_started_total",
            "Number of active walredo stderr logger tasks that have started",
        )
        .unwrap();

        let active_stderr_logger_tasks_finished = register_int_counter!(
            "pageserver_walredo_stderr_logger_tasks_finished_total",
            "Number of active walredo stderr logger tasks that have finished",
        )
        .unwrap();

        Self {
            started,
            killed_by_cause: EnumMap::from_array(std::array::from_fn(|i| {
                let cause = <WalRedoKillCause as enum_map::Enum>::from_usize(i);
                let cause_str: &'static str = cause.into();
                killed.with_label_values(&[cause_str])
            })),
            active_stderr_logger_tasks_started,
            active_stderr_logger_tasks_finished,
        }
    }
}

pub(crate) static WAL_REDO_PROCESS_COUNTERS: Lazy<WalRedoProcessCounters> =
    Lazy::new(WalRedoProcessCounters::default);

/// Similar to `prometheus::HistogramTimer` but does not record on drop.
pub(crate) struct StorageTimeMetricsTimer {
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

    /// Turns this timer into a timer, which will always record -- usually this means recording
    /// regardless an early `?` path was taken in a function.
    pub(crate) fn record_on_drop(self) -> AlwaysRecordingStorageTimeMetricsTimer {
        AlwaysRecordingStorageTimeMetricsTimer(Some(self))
    }
}

pub(crate) struct AlwaysRecordingStorageTimeMetricsTimer(Option<StorageTimeMetricsTimer>);

impl Drop for AlwaysRecordingStorageTimeMetricsTimer {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            inner.stop_and_record();
        }
    }
}

/// Timing facilities for an globally histogrammed metric, which is supported by per tenant and
/// timeline total sum and count.
#[derive(Clone, Debug)]
pub(crate) struct StorageTimeMetrics {
    /// Sum of f64 seconds, per operation, tenant_id and timeline_id
    timeline_sum: Counter,
    /// Number of oeprations, per operation, tenant_id and timeline_id
    timeline_count: IntCounter,
    /// Global histogram having only the "operation" label.
    global_histogram: Histogram,
}

impl StorageTimeMetrics {
    pub fn new(
        operation: StorageTimeOperation,
        tenant_id: &str,
        shard_id: &str,
        timeline_id: &str,
    ) -> Self {
        let operation: &'static str = operation.into();

        let timeline_sum = STORAGE_TIME_SUM_PER_TIMELINE
            .get_metric_with_label_values(&[operation, tenant_id, shard_id, timeline_id])
            .unwrap();
        let timeline_count = STORAGE_TIME_COUNT_PER_TIMELINE
            .get_metric_with_label_values(&[operation, tenant_id, shard_id, timeline_id])
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
pub(crate) struct TimelineMetrics {
    tenant_id: String,
    shard_id: String,
    timeline_id: String,
    pub flush_time_histo: StorageTimeMetrics,
    pub compact_time_histo: StorageTimeMetrics,
    pub create_images_time_histo: StorageTimeMetrics,
    pub logical_size_histo: StorageTimeMetrics,
    pub imitate_logical_size_histo: StorageTimeMetrics,
    pub load_layer_map_histo: StorageTimeMetrics,
    pub garbage_collect_histo: StorageTimeMetrics,
    pub find_gc_cutoffs_histo: StorageTimeMetrics,
    pub last_record_gauge: IntGauge,
    pub pitr_history_size: UIntGauge,
    pub archival_size: UIntGauge,
    pub(crate) layer_size_image: UIntGauge,
    pub(crate) layer_count_image: UIntGauge,
    pub(crate) layer_size_delta: UIntGauge,
    pub(crate) layer_count_delta: UIntGauge,
    pub standby_horizon_gauge: IntGauge,
    pub resident_physical_size_gauge: UIntGauge,
    pub visible_physical_size_gauge: UIntGauge,
    /// copy of LayeredTimeline.current_logical_size
    pub current_logical_size_gauge: UIntGauge,
    pub aux_file_size_gauge: IntGauge,
    pub directory_entries_count_gauge: Lazy<UIntGauge, Box<dyn Send + Fn() -> UIntGauge>>,
    pub evictions: IntCounter,
    pub evictions_with_low_residence_duration: std::sync::RwLock<EvictionsWithLowResidenceDuration>,
    /// Number of valid LSN leases.
    pub valid_lsn_lease_count_gauge: UIntGauge,
    shutdown: std::sync::atomic::AtomicBool,
}

impl TimelineMetrics {
    pub fn new(
        tenant_shard_id: &TenantShardId,
        timeline_id_raw: &TimelineId,
        evictions_with_low_residence_duration_builder: EvictionsWithLowResidenceDurationBuilder,
    ) -> Self {
        let tenant_id = tenant_shard_id.tenant_id.to_string();
        let shard_id = format!("{}", tenant_shard_id.shard_slug());
        let timeline_id = timeline_id_raw.to_string();
        let flush_time_histo = StorageTimeMetrics::new(
            StorageTimeOperation::LayerFlush,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let compact_time_histo = StorageTimeMetrics::new(
            StorageTimeOperation::Compact,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let create_images_time_histo = StorageTimeMetrics::new(
            StorageTimeOperation::CreateImages,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let logical_size_histo = StorageTimeMetrics::new(
            StorageTimeOperation::LogicalSize,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let imitate_logical_size_histo = StorageTimeMetrics::new(
            StorageTimeOperation::ImitateLogicalSize,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let load_layer_map_histo = StorageTimeMetrics::new(
            StorageTimeOperation::LoadLayerMap,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let garbage_collect_histo = StorageTimeMetrics::new(
            StorageTimeOperation::Gc,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let find_gc_cutoffs_histo = StorageTimeMetrics::new(
            StorageTimeOperation::FindGcCutoffs,
            &tenant_id,
            &shard_id,
            &timeline_id,
        );
        let last_record_gauge = LAST_RECORD_LSN
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();

        let pitr_history_size = PITR_HISTORY_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();

        let archival_size = TIMELINE_ARCHIVE_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();

        let layer_size_image = TIMELINE_LAYER_SIZE
            .get_metric_with_label_values(&[
                &tenant_id,
                &shard_id,
                &timeline_id,
                MetricLayerKind::Image.into(),
            ])
            .unwrap();

        let layer_count_image = TIMELINE_LAYER_COUNT
            .get_metric_with_label_values(&[
                &tenant_id,
                &shard_id,
                &timeline_id,
                MetricLayerKind::Image.into(),
            ])
            .unwrap();

        let layer_size_delta = TIMELINE_LAYER_SIZE
            .get_metric_with_label_values(&[
                &tenant_id,
                &shard_id,
                &timeline_id,
                MetricLayerKind::Delta.into(),
            ])
            .unwrap();

        let layer_count_delta = TIMELINE_LAYER_COUNT
            .get_metric_with_label_values(&[
                &tenant_id,
                &shard_id,
                &timeline_id,
                MetricLayerKind::Delta.into(),
            ])
            .unwrap();

        let standby_horizon_gauge = STANDBY_HORIZON
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();
        let resident_physical_size_gauge = RESIDENT_PHYSICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();
        let visible_physical_size_gauge = VISIBLE_PHYSICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();
        // TODO: we shouldn't expose this metric
        let current_logical_size_gauge = CURRENT_LOGICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();
        let aux_file_size_gauge = AUX_FILE_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();
        // TODO use impl Trait syntax here once we have ability to use it: https://github.com/rust-lang/rust/issues/63065
        let directory_entries_count_gauge_closure = {
            let tenant_shard_id = *tenant_shard_id;
            let timeline_id_raw = *timeline_id_raw;
            move || {
                let tenant_id = tenant_shard_id.tenant_id.to_string();
                let shard_id = format!("{}", tenant_shard_id.shard_slug());
                let timeline_id = timeline_id_raw.to_string();
                let gauge: UIntGauge = DIRECTORY_ENTRIES_COUNT
                    .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
                    .unwrap();
                gauge
            }
        };
        let directory_entries_count_gauge: Lazy<UIntGauge, Box<dyn Send + Fn() -> UIntGauge>> =
            Lazy::new(Box::new(directory_entries_count_gauge_closure));
        let evictions = EVICTIONS
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();
        let evictions_with_low_residence_duration = evictions_with_low_residence_duration_builder
            .build(&tenant_id, &shard_id, &timeline_id);

        let valid_lsn_lease_count_gauge = VALID_LSN_LEASE_COUNT
            .get_metric_with_label_values(&[&tenant_id, &shard_id, &timeline_id])
            .unwrap();

        TimelineMetrics {
            tenant_id,
            shard_id,
            timeline_id,
            flush_time_histo,
            compact_time_histo,
            create_images_time_histo,
            logical_size_histo,
            imitate_logical_size_histo,
            garbage_collect_histo,
            find_gc_cutoffs_histo,
            load_layer_map_histo,
            last_record_gauge,
            pitr_history_size,
            archival_size,
            layer_size_image,
            layer_count_image,
            layer_size_delta,
            layer_count_delta,
            standby_horizon_gauge,
            resident_physical_size_gauge,
            visible_physical_size_gauge,
            current_logical_size_gauge,
            aux_file_size_gauge,
            directory_entries_count_gauge,
            evictions,
            evictions_with_low_residence_duration: std::sync::RwLock::new(
                evictions_with_low_residence_duration,
            ),
            valid_lsn_lease_count_gauge,
            shutdown: std::sync::atomic::AtomicBool::default(),
        }
    }

    pub(crate) fn record_new_file_metrics(&self, sz: u64) {
        self.resident_physical_size_add(sz);
    }

    pub(crate) fn resident_physical_size_sub(&self, sz: u64) {
        self.resident_physical_size_gauge.sub(sz);
        crate::metrics::RESIDENT_PHYSICAL_SIZE_GLOBAL.sub(sz);
    }

    pub(crate) fn resident_physical_size_add(&self, sz: u64) {
        self.resident_physical_size_gauge.add(sz);
        crate::metrics::RESIDENT_PHYSICAL_SIZE_GLOBAL.add(sz);
    }

    pub(crate) fn resident_physical_size_get(&self) -> u64 {
        self.resident_physical_size_gauge.get()
    }

    pub(crate) fn shutdown(&self) {
        let was_shutdown = self
            .shutdown
            .swap(true, std::sync::atomic::Ordering::Relaxed);

        if was_shutdown {
            // this happens on tenant deletion because tenant first shuts down timelines, then
            // invokes timeline deletion which first shuts down the timeline again.
            // TODO: this can be removed once https://github.com/neondatabase/neon/issues/5080
            return;
        }

        let tenant_id = &self.tenant_id;
        let timeline_id = &self.timeline_id;
        let shard_id = &self.shard_id;
        let _ = LAST_RECORD_LSN.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        let _ = STANDBY_HORIZON.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        {
            RESIDENT_PHYSICAL_SIZE_GLOBAL.sub(self.resident_physical_size_get());
            let _ = RESIDENT_PHYSICAL_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        }
        let _ = VISIBLE_PHYSICAL_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        let _ = CURRENT_LOGICAL_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        if let Some(metric) = Lazy::get(&DIRECTORY_ENTRIES_COUNT) {
            let _ = metric.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        }

        let _ = TIMELINE_ARCHIVE_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        let _ = PITR_HISTORY_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);

        let _ = TIMELINE_LAYER_SIZE.remove_label_values(&[
            tenant_id,
            shard_id,
            timeline_id,
            MetricLayerKind::Image.into(),
        ]);
        let _ = TIMELINE_LAYER_COUNT.remove_label_values(&[
            tenant_id,
            shard_id,
            timeline_id,
            MetricLayerKind::Image.into(),
        ]);
        let _ = TIMELINE_LAYER_SIZE.remove_label_values(&[
            tenant_id,
            shard_id,
            timeline_id,
            MetricLayerKind::Delta.into(),
        ]);
        let _ = TIMELINE_LAYER_COUNT.remove_label_values(&[
            tenant_id,
            shard_id,
            timeline_id,
            MetricLayerKind::Delta.into(),
        ]);

        let _ = EVICTIONS.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        let _ = AUX_FILE_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        let _ = VALID_LSN_LEASE_COUNT.remove_label_values(&[tenant_id, shard_id, timeline_id]);

        self.evictions_with_low_residence_duration
            .write()
            .unwrap()
            .remove(tenant_id, shard_id, timeline_id);

        // The following metrics are born outside of the TimelineMetrics lifecycle but still
        // removed at the end of it. The idea is to have the metrics outlive the
        // entity during which they're observed, e.g., the smgr metrics shall
        // outlive an individual smgr connection, but not the timeline.

        for op in StorageTimeOperation::VARIANTS {
            let _ = STORAGE_TIME_SUM_PER_TIMELINE.remove_label_values(&[
                op,
                tenant_id,
                shard_id,
                timeline_id,
            ]);
            let _ = STORAGE_TIME_COUNT_PER_TIMELINE.remove_label_values(&[
                op,
                tenant_id,
                shard_id,
                timeline_id,
            ]);
        }

        for op in STORAGE_IO_SIZE_OPERATIONS {
            let _ = STORAGE_IO_SIZE.remove_label_values(&[op, tenant_id, shard_id, timeline_id]);
        }

        let _ = SMGR_QUERY_TIME_PER_TENANT_TIMELINE.remove_label_values(&[
            SmgrQueryType::GetPageAtLsn.into(),
            tenant_id,
            shard_id,
            timeline_id,
        ]);
    }
}

pub(crate) fn remove_tenant_metrics(tenant_shard_id: &TenantShardId) {
    // Only shard zero deals in synthetic sizes
    if tenant_shard_id.is_shard_zero() {
        let tid = tenant_shard_id.tenant_id.to_string();
        let _ = TENANT_SYNTHETIC_SIZE_METRIC.remove_label_values(&[&tid]);
    }

    // we leave the BROKEN_TENANTS_SET entry if any
}

use futures::Future;
use pin_project_lite::pin_project;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::context::{PageContentKind, RequestContext};
use crate::task_mgr::TaskKind;
use crate::tenant::mgr::TenantSlot;
use crate::tenant::tasks::BackgroundLoopKind;

/// Maintain a per timeline gauge in addition to the global gauge.
pub(crate) struct PerTimelineRemotePhysicalSizeGauge {
    last_set: AtomicU64,
    gauge: UIntGauge,
}

impl PerTimelineRemotePhysicalSizeGauge {
    fn new(per_timeline_gauge: UIntGauge) -> Self {
        Self {
            last_set: AtomicU64::new(0),
            gauge: per_timeline_gauge,
        }
    }
    pub(crate) fn set(&self, sz: u64) {
        self.gauge.set(sz);
        let prev = self.last_set.swap(sz, std::sync::atomic::Ordering::Relaxed);
        if sz < prev {
            REMOTE_PHYSICAL_SIZE_GLOBAL.sub(prev - sz);
        } else {
            REMOTE_PHYSICAL_SIZE_GLOBAL.add(sz - prev);
        };
    }
    pub(crate) fn get(&self) -> u64 {
        self.gauge.get()
    }
}

impl Drop for PerTimelineRemotePhysicalSizeGauge {
    fn drop(&mut self) {
        REMOTE_PHYSICAL_SIZE_GLOBAL.sub(self.last_set.load(std::sync::atomic::Ordering::Relaxed));
    }
}

pub(crate) struct RemoteTimelineClientMetrics {
    tenant_id: String,
    shard_id: String,
    timeline_id: String,
    pub(crate) remote_physical_size_gauge: PerTimelineRemotePhysicalSizeGauge,
    calls: Mutex<HashMap<(&'static str, &'static str), IntCounterPair>>,
    bytes_started_counter: Mutex<HashMap<(&'static str, &'static str), IntCounter>>,
    bytes_finished_counter: Mutex<HashMap<(&'static str, &'static str), IntCounter>>,
}

impl RemoteTimelineClientMetrics {
    pub fn new(tenant_shard_id: &TenantShardId, timeline_id: &TimelineId) -> Self {
        let tenant_id_str = tenant_shard_id.tenant_id.to_string();
        let shard_id_str = format!("{}", tenant_shard_id.shard_slug());
        let timeline_id_str = timeline_id.to_string();

        let remote_physical_size_gauge = PerTimelineRemotePhysicalSizeGauge::new(
            REMOTE_PHYSICAL_SIZE
                .get_metric_with_label_values(&[&tenant_id_str, &shard_id_str, &timeline_id_str])
                .unwrap(),
        );

        RemoteTimelineClientMetrics {
            tenant_id: tenant_id_str,
            shard_id: shard_id_str,
            timeline_id: timeline_id_str,
            calls: Mutex::new(HashMap::default()),
            bytes_started_counter: Mutex::new(HashMap::default()),
            bytes_finished_counter: Mutex::new(HashMap::default()),
            remote_physical_size_gauge,
        }
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

    fn calls_counter_pair(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
    ) -> IntCounterPair {
        let mut guard = self.calls.lock().unwrap();
        let key = (file_kind.as_str(), op_kind.as_str());
        let metric = guard.entry(key).or_insert_with(move || {
            REMOTE_TIMELINE_CLIENT_CALLS
                .get_metric_with_label_values(&[
                    &self.tenant_id,
                    &self.shard_id,
                    &self.timeline_id,
                    key.0,
                    key.1,
                ])
                .unwrap()
        });
        metric.clone()
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
                    &self.tenant_id,
                    &self.shard_id,
                    &self.timeline_id,
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
                    &self.tenant_id,
                    &self.shard_id,
                    &self.timeline_id,
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
    calls_counter_pair: Option<IntCounterPair>,
    /// If Some(), this references the bytes_finished metric, and we increment it by the given `u64` on drop.
    bytes_finished: Option<(IntCounter, u64)>,
}

impl RemoteTimelineClientCallMetricGuard {
    /// Consume this guard object without performing the metric updates it would do on `drop()`.
    /// The caller vouches to do the metric updates manually.
    pub fn will_decrement_manually(mut self) {
        let RemoteTimelineClientCallMetricGuard {
            calls_counter_pair,
            bytes_finished,
        } = &mut self;
        calls_counter_pair.take();
        bytes_finished.take();
    }
}

impl Drop for RemoteTimelineClientCallMetricGuard {
    fn drop(&mut self) {
        let RemoteTimelineClientCallMetricGuard {
            calls_counter_pair,
            bytes_finished,
        } = self;
        if let Some(guard) = calls_counter_pair.take() {
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
        let calls_counter_pair = self.calls_counter_pair(file_kind, op_kind);
        calls_counter_pair.inc();

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
            calls_counter_pair: Some(calls_counter_pair),
            bytes_finished,
        }
    }

    /// Manually udpate the metrics that track completions, instead of using the guard object.
    /// Using the guard object is generally preferable.
    /// See [`call_begin`](Self::call_begin) for more context.
    pub(crate) fn call_end(
        &self,
        file_kind: &RemoteOpFileKind,
        op_kind: &RemoteOpKind,
        size: RemoteTimelineClientMetricsCallTrackSize,
    ) {
        let calls_counter_pair = self.calls_counter_pair(file_kind, op_kind);
        calls_counter_pair.dec();
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
            shard_id,
            timeline_id,
            remote_physical_size_gauge,
            calls,
            bytes_started_counter,
            bytes_finished_counter,
        } = self;
        for ((a, b), _) in calls.get_mut().unwrap().drain() {
            let mut res = [Ok(()), Ok(())];
            REMOTE_TIMELINE_CLIENT_CALLS
                .remove_label_values(&mut res, &[tenant_id, shard_id, timeline_id, a, b]);
            // don't care about results
        }
        for ((a, b), _) in bytes_started_counter.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_BYTES_STARTED_COUNTER.remove_label_values(&[
                tenant_id,
                shard_id,
                timeline_id,
                a,
                b,
            ]);
        }
        for ((a, b), _) in bytes_finished_counter.get_mut().unwrap().drain() {
            let _ = REMOTE_TIMELINE_CLIENT_BYTES_FINISHED_COUNTER.remove_label_values(&[
                tenant_id,
                shard_id,
                timeline_id,
                a,
                b,
            ]);
        }
        {
            let _ = remote_physical_size_gauge; // use to avoid 'unused' warning in desctructuring above
            let _ = REMOTE_PHYSICAL_SIZE.remove_label_values(&[tenant_id, shard_id, timeline_id]);
        }
    }
}

/// Wrapper future that measures the time spent by a remote storage operation,
/// and records the time and success/failure as a prometheus metric.
pub(crate) trait MeasureRemoteOp: Sized {
    fn measure_remote_op(
        self,
        file_kind: RemoteOpFileKind,
        op: RemoteOpKind,
        metrics: Arc<RemoteTimelineClientMetrics>,
    ) -> MeasuredRemoteOp<Self> {
        let start = Instant::now();
        MeasuredRemoteOp {
            inner: self,
            file_kind,
            op,
            start,
            metrics,
        }
    }
}

impl<T: Sized> MeasureRemoteOp for T {}

pin_project! {
    pub(crate) struct MeasuredRemoteOp<F>
    {
        #[pin]
        inner: F,
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

pub mod tokio_epoll_uring {
    use metrics::{register_int_counter, UIntGauge};
    use once_cell::sync::Lazy;

    pub struct Collector {
        descs: Vec<metrics::core::Desc>,
        systems_created: UIntGauge,
        systems_destroyed: UIntGauge,
    }

    impl metrics::core::Collector for Collector {
        fn desc(&self) -> Vec<&metrics::core::Desc> {
            self.descs.iter().collect()
        }

        fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
            let mut mfs = Vec::with_capacity(Self::NMETRICS);
            let tokio_epoll_uring::metrics::Metrics {
                systems_created,
                systems_destroyed,
            } = tokio_epoll_uring::metrics::global();
            self.systems_created.set(systems_created);
            mfs.extend(self.systems_created.collect());
            self.systems_destroyed.set(systems_destroyed);
            mfs.extend(self.systems_destroyed.collect());
            mfs
        }
    }

    impl Collector {
        const NMETRICS: usize = 2;

        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            let mut descs = Vec::new();

            let systems_created = UIntGauge::new(
                "pageserver_tokio_epoll_uring_systems_created",
                "counter of tokio-epoll-uring systems that were created",
            )
            .unwrap();
            descs.extend(
                metrics::core::Collector::desc(&systems_created)
                    .into_iter()
                    .cloned(),
            );

            let systems_destroyed = UIntGauge::new(
                "pageserver_tokio_epoll_uring_systems_destroyed",
                "counter of tokio-epoll-uring systems that were destroyed",
            )
            .unwrap();
            descs.extend(
                metrics::core::Collector::desc(&systems_destroyed)
                    .into_iter()
                    .cloned(),
            );

            Self {
                descs,
                systems_created,
                systems_destroyed,
            }
        }
    }

    pub(crate) static THREAD_LOCAL_LAUNCH_SUCCESSES: Lazy<metrics::IntCounter> = Lazy::new(|| {
        register_int_counter!(
            "pageserver_tokio_epoll_uring_pageserver_thread_local_launch_success_count",
            "Number of times where thread_local_system creation spanned multiple executor threads",
        )
        .unwrap()
    });

    pub(crate) static THREAD_LOCAL_LAUNCH_FAILURES: Lazy<metrics::IntCounter> = Lazy::new(|| {
        register_int_counter!(
            "pageserver_tokio_epoll_uring_pageserver_thread_local_launch_failures_count",
            "Number of times thread_local_system creation failed and was retried after back-off.",
        )
        .unwrap()
    });
}

pub(crate) mod tenant_throttling {
    use metrics::{register_int_counter_vec, IntCounter};
    use once_cell::sync::Lazy;

    use crate::tenant::{self, throttle::Metric};

    pub(crate) struct TimelineGet {
        wait_time: IntCounter,
        count: IntCounter,
    }

    pub(crate) static TIMELINE_GET: Lazy<TimelineGet> = Lazy::new(|| {
        static WAIT_USECS: Lazy<metrics::IntCounterVec> = Lazy::new(|| {
            register_int_counter_vec!(
            "pageserver_tenant_throttling_wait_usecs_sum_global",
            "Sum of microseconds that tenants spent waiting for a tenant throttle of a given kind.",
            &["kind"]
        )
            .unwrap()
        });

        static WAIT_COUNT: Lazy<metrics::IntCounterVec> = Lazy::new(|| {
            register_int_counter_vec!(
                "pageserver_tenant_throttling_count_global",
                "Count of tenant throttlings, by kind of throttle.",
                &["kind"]
            )
            .unwrap()
        });

        let kind = "timeline_get";
        TimelineGet {
            wait_time: WAIT_USECS.with_label_values(&[kind]),
            count: WAIT_COUNT.with_label_values(&[kind]),
        }
    });

    impl Metric for &'static TimelineGet {
        #[inline(always)]
        fn observe_throttling(
            &self,
            tenant::throttle::Observation { wait_time }: &tenant::throttle::Observation,
        ) {
            let val = u64::try_from(wait_time.as_micros()).unwrap();
            self.wait_time.inc_by(val);
            self.count.inc();
        }
    }
}

pub(crate) mod disk_usage_based_eviction {
    use super::*;

    pub(crate) struct Metrics {
        pub(crate) tenant_collection_time: Histogram,
        pub(crate) tenant_layer_count: Histogram,
        pub(crate) layers_collected: IntCounter,
        pub(crate) layers_selected: IntCounter,
        pub(crate) layers_evicted: IntCounter,
    }

    impl Default for Metrics {
        fn default() -> Self {
            let tenant_collection_time = register_histogram!(
                "pageserver_disk_usage_based_eviction_tenant_collection_seconds",
                "Time spent collecting layers from a tenant -- not normalized by collected layer amount",
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
            )
            .unwrap();

            let tenant_layer_count = register_histogram!(
                "pageserver_disk_usage_based_eviction_tenant_collected_layers",
                "Amount of layers gathered from a tenant",
                vec![5.0, 50.0, 500.0, 5000.0, 50000.0]
            )
            .unwrap();

            let layers_collected = register_int_counter!(
                "pageserver_disk_usage_based_eviction_collected_layers_total",
                "Amount of layers collected"
            )
            .unwrap();

            let layers_selected = register_int_counter!(
                "pageserver_disk_usage_based_eviction_select_layers_total",
                "Amount of layers selected"
            )
            .unwrap();

            let layers_evicted = register_int_counter!(
                "pageserver_disk_usage_based_eviction_evicted_layers_total",
                "Amount of layers successfully evicted"
            )
            .unwrap();

            Self {
                tenant_collection_time,
                tenant_layer_count,
                layers_collected,
                layers_selected,
                layers_evicted,
            }
        }
    }

    pub(crate) static METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);
}

static TOKIO_EXECUTOR_THREAD_COUNT: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "pageserver_tokio_executor_thread_configured_count",
        "Total number of configued tokio executor threads in the process.
         The `setup` label denotes whether we're running with multiple runtimes or a single runtime.",
        &["setup"],
    )
    .unwrap()
});

pub(crate) static CONSECUTIVE_NONBLOCKING_GETPAGE_REQUESTS_HISTOGRAM: Lazy<Histogram> =
    Lazy::new(|| {
        register_histogram!(
            "pageserver_consecutive_nonblocking_getpage_requests",
            "Number of consecutive nonblocking getpage requests",
            (0..=256).map(|x| x as f64).collect::<Vec<f64>>(),
        )
        .unwrap()
    });

pub(crate) fn set_tokio_runtime_setup(setup: &str, num_threads: NonZeroUsize) {
    static SERIALIZE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = SERIALIZE.lock().unwrap();
    TOKIO_EXECUTOR_THREAD_COUNT.reset();
    TOKIO_EXECUTOR_THREAD_COUNT
        .get_metric_with_label_values(&[setup])
        .unwrap()
        .set(u64::try_from(num_threads.get()).unwrap());
}

pub fn preinitialize_metrics() {
    // Python tests need these and on some we do alerting.
    //
    // FIXME(4813): make it so that we have no top level metrics as this fn will easily fall out of
    // order:
    // - global metrics reside in a Lazy<PageserverMetrics>
    //   - access via crate::metrics::PS_METRICS.some_metric.inc()
    // - could move the statics into TimelineMetrics::new()?

    // counters
    [
        &UNEXPECTED_ONDEMAND_DOWNLOADS,
        &WALRECEIVER_STARTED_CONNECTIONS,
        &WALRECEIVER_BROKER_UPDATES,
        &WALRECEIVER_CANDIDATES_ADDED,
        &WALRECEIVER_CANDIDATES_REMOVED,
        &tokio_epoll_uring::THREAD_LOCAL_LAUNCH_FAILURES,
        &tokio_epoll_uring::THREAD_LOCAL_LAUNCH_SUCCESSES,
        &REMOTE_ONDEMAND_DOWNLOADED_LAYERS,
        &REMOTE_ONDEMAND_DOWNLOADED_BYTES,
        &CIRCUIT_BREAKERS_BROKEN,
        &CIRCUIT_BREAKERS_UNBROKEN,
    ]
    .into_iter()
    .for_each(|c| {
        Lazy::force(c);
    });

    // Deletion queue stats
    Lazy::force(&DELETION_QUEUE);

    // Tenant stats
    Lazy::force(&TENANT);

    // Tenant manager stats
    Lazy::force(&TENANT_MANAGER);

    Lazy::force(&crate::tenant::storage_layer::layer::LAYER_IMPL_METRICS);
    Lazy::force(&disk_usage_based_eviction::METRICS);

    for state_name in pageserver_api::models::TenantState::VARIANTS {
        // initialize the metric for all gauges, otherwise the time series might seemingly show
        // values from last restart.
        TENANT_STATE_METRIC.with_label_values(&[state_name]).set(0);
    }

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
        &READ_NUM_LAYERS_VISITED,
        &VEC_READ_NUM_LAYERS_VISITED,
        &WAIT_LSN_TIME,
        &WAL_REDO_TIME,
        &WAL_REDO_RECORDS_HISTOGRAM,
        &WAL_REDO_BYTES_HISTOGRAM,
        &WAL_REDO_PROCESS_LAUNCH_DURATION_HISTOGRAM,
    ]
    .into_iter()
    .for_each(|h| {
        Lazy::force(h);
    });

    // Custom
    Lazy::force(&RECONSTRUCT_TIME);
    Lazy::force(&tenant_throttling::TIMELINE_GET);
    Lazy::force(&BASEBACKUP_QUERY_TIME);
    Lazy::force(&COMPUTE_COMMANDS_COUNTERS);
}
