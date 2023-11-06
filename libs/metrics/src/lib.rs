//! We re-export those from prometheus crate to
//! make sure that we use the same dep version everywhere.
//! Otherwise, we might not see all metrics registered via
//! a default registry.
use once_cell::sync::Lazy;
use prometheus::core::{AtomicU64, Collector, GenericGauge, GenericGaugeVec};
pub use prometheus::opts;
pub use prometheus::register;
pub use prometheus::Error;
pub use prometheus::{core, default_registry, proto};
pub use prometheus::{exponential_buckets, linear_buckets};
pub use prometheus::{register_counter_vec, Counter, CounterVec};
pub use prometheus::{register_gauge, Gauge};
pub use prometheus::{register_gauge_vec, GaugeVec};
pub use prometheus::{register_histogram, Histogram};
pub use prometheus::{register_histogram_vec, HistogramVec};
pub use prometheus::{register_int_counter, IntCounter};
pub use prometheus::{register_int_counter_vec, IntCounterVec};
pub use prometheus::{register_int_gauge, IntGauge};
pub use prometheus::{register_int_gauge_vec, IntGaugeVec};
pub use prometheus::{Encoder, TextEncoder};
use prometheus::{Registry, Result};

pub mod tokio_metrics;

pub mod launch_timestamp;
mod wrappers;
pub use wrappers::{CountedReader, CountedWriter};
pub mod metric_vec_duration;

pub type UIntGauge = GenericGauge<AtomicU64>;
pub type UIntGaugeVec = GenericGaugeVec<AtomicU64>;

#[macro_export]
macro_rules! register_uint_gauge_vec {
    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr $(,)?) => {{
        let gauge_vec = UIntGaugeVec::new($crate::opts!($NAME, $HELP), $LABELS_NAMES).unwrap();
        $crate::register(Box::new(gauge_vec.clone())).map(|_| gauge_vec)
    }};
}

#[macro_export]
macro_rules! register_uint_gauge {
    ($NAME:expr, $HELP:expr $(,)?) => {{
        let gauge = $crate::UIntGauge::new($NAME, $HELP).unwrap();
        $crate::register(Box::new(gauge.clone())).map(|_| gauge)
    }};
}

/// Special internal registry, to collect metrics independently from the default registry.
/// Was introduced to fix deadlock with lazy registration of metrics in the default registry.
static INTERNAL_REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

/// Register a collector in the internal registry. MUST be called before the first call to `gather()`.
/// Otherwise, we can have a deadlock in the `gather()` call, trying to register a new collector
/// while holding the lock.
pub fn register_internal(c: Box<dyn Collector>) -> Result<()> {
    INTERNAL_REGISTRY.register(c)
}

/// Gathers all Prometheus metrics and records the I/O stats just before that.
///
/// Metrics gathering is a relatively simple and standalone operation, so
/// it might be fine to do it this way to keep things simple.
pub fn gather() -> Vec<prometheus::proto::MetricFamily> {
    update_rusage_metrics();
    let mut mfs = prometheus::gather();
    let mut internal_mfs = INTERNAL_REGISTRY.gather();
    mfs.append(&mut internal_mfs);
    mfs
}

static DISK_IO_BYTES: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "libmetrics_disk_io_bytes_total",
        "Bytes written and read from disk, grouped by the operation (read|write)",
        &["io_operation"]
    )
    .expect("Failed to register disk i/o bytes int gauge vec")
});

static MAXRSS_KB: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!(
        "libmetrics_maxrss_kb",
        "Memory usage (Maximum Resident Set Size)"
    )
    .expect("Failed to register maxrss_kb int gauge")
});

pub const DISK_WRITE_SECONDS_BUCKETS: &[f64] = &[
    0.000_050, 0.000_100, 0.000_500, 0.001, 0.003, 0.005, 0.01, 0.05, 0.1, 0.3, 0.5,
];

pub fn set_build_info_metric(revision: &str, build_tag: &str) {
    let metric = register_int_gauge_vec!(
        "libmetrics_build_info",
        "Build/version information",
        &["revision", "build_tag"]
    )
    .expect("Failed to register build info metric");
    metric.with_label_values(&[revision, build_tag]).set(1);
}

// Records I/O stats in a "cross-platform" way.
// Compiles both on macOS and Linux, but current macOS implementation always returns 0 as values for I/O stats.
// An alternative is to read procfs (`/proc/[pid]/io`) which does not work under macOS at all, hence abandoned.
//
// Uses https://www.freebsd.org/cgi/man.cgi?query=getrusage to retrieve the number of block operations
// performed by the process.
// We know the size of the block, so we can determine the I/O bytes out of it.
// The value might be not 100% exact, but should be fine for Prometheus metrics in this case.
#[allow(clippy::unnecessary_cast)]
fn update_rusage_metrics() {
    let rusage_stats = get_rusage_stats();

    const BYTES_IN_BLOCK: i64 = 512;
    DISK_IO_BYTES
        .with_label_values(&["read"])
        .set(rusage_stats.ru_inblock * BYTES_IN_BLOCK);
    DISK_IO_BYTES
        .with_label_values(&["write"])
        .set(rusage_stats.ru_oublock * BYTES_IN_BLOCK);
    MAXRSS_KB.set(rusage_stats.ru_maxrss);
}

fn get_rusage_stats() -> libc::rusage {
    let mut rusage = std::mem::MaybeUninit::uninit();

    // SAFETY: kernel will initialize the struct for us
    unsafe {
        let ret = libc::getrusage(libc::RUSAGE_SELF, rusage.as_mut_ptr());
        assert!(ret == 0, "getrusage failed: bad args");
        rusage.assume_init()
    }
}
