//! We re-export those from prometheus crate to
//! make sure that we use the same dep version everywhere.
//! Otherwise, we might not see all metrics registered via
//! a default registry.
#![deny(clippy::undocumented_unsafe_blocks)]

use measured::{
    label::{LabelGroupSet, LabelGroupVisitor, LabelName, NoLabels},
    metric::{
        counter::CounterState,
        gauge::GaugeState,
        group::{Encoding, MetricValue},
        name::{MetricName, MetricNameEncoder},
        MetricEncoding, MetricFamilyEncoding,
    },
    FixedCardinalityLabel, LabelGroup, MetricGroup,
};
use once_cell::sync::Lazy;
use prometheus::core::{
    Atomic, AtomicU64, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec,
};
pub use prometheus::opts;
pub use prometheus::register;
pub use prometheus::Error;
use prometheus::Registry;
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

pub mod launch_timestamp;
mod wrappers;
pub use wrappers::{CountedReader, CountedWriter};
mod hll;
pub use hll::{HyperLogLog, HyperLogLogState, HyperLogLogVec};
#[cfg(target_os = "linux")]
pub mod more_process_metrics;

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
pub fn register_internal(c: Box<dyn Collector>) -> prometheus::Result<()> {
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

pub struct BuildInfo {
    pub revision: &'static str,
    pub build_tag: &'static str,
}

// todo: allow label group without the set
impl LabelGroup for BuildInfo {
    fn visit_values(&self, v: &mut impl LabelGroupVisitor) {
        const REVISION: &LabelName = LabelName::from_str("revision");
        v.write_value(REVISION, &self.revision);
        const BUILD_TAG: &LabelName = LabelName::from_str("build_tag");
        v.write_value(BUILD_TAG, &self.build_tag);
    }
}

impl<T: Encoding> MetricFamilyEncoding<T> for BuildInfo
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_family_into(
        &self,
        name: impl measured::metric::name::MetricNameEncoder,
        enc: &mut T,
    ) -> Result<(), T::Err> {
        enc.write_help(&name, "Build/version information")?;
        GaugeState::write_type(&name, enc)?;
        GaugeState {
            count: std::sync::atomic::AtomicI64::new(1),
        }
        .collect_into(&(), self, name, enc)
    }
}

#[derive(MetricGroup)]
#[metric(new(build_info: BuildInfo))]
pub struct NeonMetrics {
    #[cfg(target_os = "linux")]
    #[metric(namespace = "process")]
    #[metric(init = measured_process::ProcessCollector::for_self())]
    process: measured_process::ProcessCollector,

    #[cfg(tokio_unstable)]
    #[metric(namespace = "tokio")]
    #[metric(init = measured_tokio::NamedRuntimesCollector::new())]
    pub tokio: measured_tokio::NamedRuntimesCollector,

    #[metric(namespace = "libmetrics")]
    #[metric(init = LibMetrics::new(build_info))]
    libmetrics: LibMetrics,
}

#[derive(MetricGroup)]
#[metric(new(build_info: BuildInfo))]
pub struct LibMetrics {
    #[metric(init = build_info)]
    build_info: BuildInfo,

    #[metric(flatten)]
    rusage: Rusage,

    serve_count: CollectionCounter,
}

fn write_gauge<Enc: Encoding>(
    x: i64,
    labels: impl LabelGroup,
    name: impl MetricNameEncoder,
    enc: &mut Enc,
) -> Result<(), Enc::Err> {
    enc.write_metric_value(name, labels, MetricValue::Int(x))
}

#[derive(Default)]
struct Rusage;

#[derive(FixedCardinalityLabel, Clone, Copy)]
#[label(singleton = "io_operation")]
enum IoOp {
    Read,
    Write,
}

impl<T: Encoding> MetricGroup<T> for Rusage
where
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), T::Err> {
        const DISK_IO: &MetricName = MetricName::from_str("disk_io_bytes_total");
        const MAXRSS: &MetricName = MetricName::from_str("maxrss_kb");

        let ru = get_rusage_stats();

        enc.write_help(
            DISK_IO,
            "Bytes written and read from disk, grouped by the operation (read|write)",
        )?;
        GaugeState::write_type(DISK_IO, enc)?;
        write_gauge(ru.ru_inblock * BYTES_IN_BLOCK, IoOp::Read, DISK_IO, enc)?;
        write_gauge(ru.ru_oublock * BYTES_IN_BLOCK, IoOp::Write, DISK_IO, enc)?;

        enc.write_help(MAXRSS, "Memory usage (Maximum Resident Set Size)")?;
        GaugeState::write_type(MAXRSS, enc)?;
        write_gauge(ru.ru_maxrss, IoOp::Read, MAXRSS, enc)?;

        Ok(())
    }
}

#[derive(Default)]
struct CollectionCounter(CounterState);

impl<T: Encoding> MetricFamilyEncoding<T> for CollectionCounter
where
    CounterState: MetricEncoding<T>,
{
    fn collect_family_into(
        &self,
        name: impl measured::metric::name::MetricNameEncoder,
        enc: &mut T,
    ) -> Result<(), T::Err> {
        self.0.inc();
        enc.write_help(&name, "Number of metric requests made")?;
        self.0.collect_into(&(), NoLabels, name, enc)
    }
}

pub fn set_build_info_metric(revision: &str, build_tag: &str) {
    let metric = register_int_gauge_vec!(
        "libmetrics_build_info",
        "Build/version information",
        &["revision", "build_tag"]
    )
    .expect("Failed to register build info metric");
    metric.with_label_values(&[revision, build_tag]).set(1);
}
const BYTES_IN_BLOCK: i64 = 512;

// Records I/O stats in a "cross-platform" way.
// Compiles both on macOS and Linux, but current macOS implementation always returns 0 as values for I/O stats.
// An alternative is to read procfs (`/proc/[pid]/io`) which does not work under macOS at all, hence abandoned.
//
// Uses https://www.freebsd.org/cgi/man.cgi?query=getrusage to retrieve the number of block operations
// performed by the process.
// We know the size of the block, so we can determine the I/O bytes out of it.
// The value might be not 100% exact, but should be fine for Prometheus metrics in this case.
fn update_rusage_metrics() {
    let rusage_stats = get_rusage_stats();

    DISK_IO_BYTES
        .with_label_values(&["read"])
        .set(rusage_stats.ru_inblock * BYTES_IN_BLOCK);
    DISK_IO_BYTES
        .with_label_values(&["write"])
        .set(rusage_stats.ru_oublock * BYTES_IN_BLOCK);

    // On macOS, the unit of maxrss is bytes; on Linux, it's kilobytes. https://stackoverflow.com/a/59915669
    #[cfg(target_os = "macos")]
    {
        MAXRSS_KB.set(rusage_stats.ru_maxrss / 1024);
    }
    #[cfg(not(target_os = "macos"))]
    {
        MAXRSS_KB.set(rusage_stats.ru_maxrss);
    }
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

/// Create an [`IntCounterPairVec`] and registers to default registry.
#[macro_export(local_inner_macros)]
macro_rules! register_int_counter_pair_vec {
    ($NAME1:expr, $HELP1:expr, $NAME2:expr, $HELP2:expr, $LABELS_NAMES:expr $(,)?) => {{
        match (
            $crate::register_int_counter_vec!($NAME1, $HELP1, $LABELS_NAMES),
            $crate::register_int_counter_vec!($NAME2, $HELP2, $LABELS_NAMES),
        ) {
            (Ok(inc), Ok(dec)) => Ok($crate::IntCounterPairVec::new(inc, dec)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }};
}

/// Create an [`IntCounterPair`] and registers to default registry.
#[macro_export(local_inner_macros)]
macro_rules! register_int_counter_pair {
    ($NAME1:expr, $HELP1:expr, $NAME2:expr, $HELP2:expr $(,)?) => {{
        match (
            $crate::register_int_counter!($NAME1, $HELP1),
            $crate::register_int_counter!($NAME2, $HELP2),
        ) {
            (Ok(inc), Ok(dec)) => Ok($crate::IntCounterPair::new(inc, dec)),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }};
}

/// A Pair of [`GenericCounterVec`]s. Like an [`GenericGaugeVec`] but will always observe changes
pub struct GenericCounterPairVec<P: Atomic> {
    inc: GenericCounterVec<P>,
    dec: GenericCounterVec<P>,
}

/// A Pair of [`GenericCounter`]s. Like an [`GenericGauge`] but will always observe changes
pub struct GenericCounterPair<P: Atomic> {
    inc: GenericCounter<P>,
    dec: GenericCounter<P>,
}

impl<P: Atomic> GenericCounterPairVec<P> {
    pub fn new(inc: GenericCounterVec<P>, dec: GenericCounterVec<P>) -> Self {
        Self { inc, dec }
    }

    /// `get_metric_with_label_values` returns the [`GenericCounterPair<P>`] for the given slice
    /// of label values (same order as the VariableLabels in Desc). If that combination of
    /// label values is accessed for the first time, a new [`GenericCounterPair<P>`] is created.
    ///
    /// An error is returned if the number of label values is not the same as the
    /// number of VariableLabels in Desc.
    pub fn get_metric_with_label_values(
        &self,
        vals: &[&str],
    ) -> prometheus::Result<GenericCounterPair<P>> {
        Ok(GenericCounterPair {
            inc: self.inc.get_metric_with_label_values(vals)?,
            dec: self.dec.get_metric_with_label_values(vals)?,
        })
    }

    /// `with_label_values` works as `get_metric_with_label_values`, but panics if an error
    /// occurs.
    pub fn with_label_values(&self, vals: &[&str]) -> GenericCounterPair<P> {
        self.get_metric_with_label_values(vals).unwrap()
    }

    pub fn remove_label_values(&self, res: &mut [prometheus::Result<()>; 2], vals: &[&str]) {
        res[0] = self.inc.remove_label_values(vals);
        res[1] = self.dec.remove_label_values(vals);
    }
}

impl<P: Atomic> GenericCounterPair<P> {
    pub fn new(inc: GenericCounter<P>, dec: GenericCounter<P>) -> Self {
        Self { inc, dec }
    }

    /// Increment the gauge by 1, returning a guard that decrements by 1 on drop.
    pub fn guard(&self) -> GenericCounterPairGuard<P> {
        self.inc.inc();
        GenericCounterPairGuard(self.dec.clone())
    }

    /// Increment the gauge by n, returning a guard that decrements by n on drop.
    pub fn guard_by(&self, n: P::T) -> GenericCounterPairGuardBy<P> {
        self.inc.inc_by(n);
        GenericCounterPairGuardBy(self.dec.clone(), n)
    }

    /// Increase the gauge by 1.
    #[inline]
    pub fn inc(&self) {
        self.inc.inc();
    }

    /// Decrease the gauge by 1.
    #[inline]
    pub fn dec(&self) {
        self.dec.inc();
    }

    /// Add the given value to the gauge. (The value can be
    /// negative, resulting in a decrement of the gauge.)
    #[inline]
    pub fn inc_by(&self, v: P::T) {
        self.inc.inc_by(v);
    }

    /// Subtract the given value from the gauge. (The value can be
    /// negative, resulting in an increment of the gauge.)
    #[inline]
    pub fn dec_by(&self, v: P::T) {
        self.dec.inc_by(v);
    }
}

impl<P: Atomic> Clone for GenericCounterPair<P> {
    fn clone(&self) -> Self {
        Self {
            inc: self.inc.clone(),
            dec: self.dec.clone(),
        }
    }
}

/// Guard returned by [`GenericCounterPair::guard`]
pub struct GenericCounterPairGuard<P: Atomic>(GenericCounter<P>);

impl<P: Atomic> Drop for GenericCounterPairGuard<P> {
    fn drop(&mut self) {
        self.0.inc();
    }
}
/// Guard returned by [`GenericCounterPair::guard_by`]
pub struct GenericCounterPairGuardBy<P: Atomic>(GenericCounter<P>, P::T);

impl<P: Atomic> Drop for GenericCounterPairGuardBy<P> {
    fn drop(&mut self) {
        self.0.inc_by(self.1);
    }
}

/// A Pair of [`IntCounterVec`]s. Like an [`IntGaugeVec`] but will always observe changes
pub type IntCounterPairVec = GenericCounterPairVec<AtomicU64>;

/// A Pair of [`IntCounter`]s. Like an [`IntGauge`] but will always observe changes
pub type IntCounterPair = GenericCounterPair<AtomicU64>;

/// A guard for [`IntCounterPair`] that will decrement the gauge on drop
pub type IntCounterPairGuard = GenericCounterPairGuard<AtomicU64>;

pub trait CounterPairAssoc {
    const INC_NAME: &'static MetricName;
    const DEC_NAME: &'static MetricName;

    const INC_HELP: &'static str;
    const DEC_HELP: &'static str;

    type LabelGroupSet: LabelGroupSet;
}

pub struct CounterPairVec<A: CounterPairAssoc> {
    vec: measured::metric::MetricVec<MeasuredCounterPairState, A::LabelGroupSet>,
}

impl<A: CounterPairAssoc> Default for CounterPairVec<A>
where
    A::LabelGroupSet: Default,
{
    fn default() -> Self {
        Self {
            vec: Default::default(),
        }
    }
}

impl<A: CounterPairAssoc> CounterPairVec<A> {
    pub fn guard(
        &self,
        labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>,
    ) -> MeasuredCounterPairGuard<'_, A> {
        let id = self.vec.with_labels(labels);
        self.vec.get_metric(id).inc.inc();
        MeasuredCounterPairGuard { vec: &self.vec, id }
    }
    pub fn inc(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>) {
        let id = self.vec.with_labels(labels);
        self.vec.get_metric(id).inc.inc();
    }
    pub fn dec(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>) {
        let id = self.vec.with_labels(labels);
        self.vec.get_metric(id).dec.inc();
    }
    pub fn remove_metric(
        &self,
        labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>,
    ) -> Option<MeasuredCounterPairState> {
        let id = self.vec.with_labels(labels);
        self.vec.remove_metric(id)
    }

    pub fn sample(&self, labels: <A::LabelGroupSet as LabelGroupSet>::Group<'_>) -> u64 {
        let id = self.vec.with_labels(labels);
        let metric = self.vec.get_metric(id);

        let inc = metric.inc.count.load(std::sync::atomic::Ordering::Relaxed);
        let dec = metric.dec.count.load(std::sync::atomic::Ordering::Relaxed);
        inc.saturating_sub(dec)
    }
}

impl<T, A> ::measured::metric::group::MetricGroup<T> for CounterPairVec<A>
where
    T: ::measured::metric::group::Encoding,
    A: CounterPairAssoc,
    ::measured::metric::counter::CounterState: ::measured::metric::MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), T::Err> {
        // write decrement first to avoid a race condition where inc - dec < 0
        T::write_help(enc, A::DEC_NAME, A::DEC_HELP)?;
        self.vec
            .collect_family_into(A::DEC_NAME, &mut Dec(&mut *enc))?;

        T::write_help(enc, A::INC_NAME, A::INC_HELP)?;
        self.vec
            .collect_family_into(A::INC_NAME, &mut Inc(&mut *enc))?;

        Ok(())
    }
}

#[derive(MetricGroup, Default)]
pub struct MeasuredCounterPairState {
    pub inc: CounterState,
    pub dec: CounterState,
}

impl measured::metric::MetricType for MeasuredCounterPairState {
    type Metadata = ();
}

pub struct MeasuredCounterPairGuard<'a, A: CounterPairAssoc> {
    vec: &'a measured::metric::MetricVec<MeasuredCounterPairState, A::LabelGroupSet>,
    id: measured::metric::LabelId<A::LabelGroupSet>,
}

impl<A: CounterPairAssoc> Drop for MeasuredCounterPairGuard<'_, A> {
    fn drop(&mut self) {
        self.vec.get_metric(self.id).dec.inc();
    }
}

/// [`MetricEncoding`] for [`MeasuredCounterPairState`] that only writes the inc counter to the inner encoder.
struct Inc<T>(T);
/// [`MetricEncoding`] for [`MeasuredCounterPairState`] that only writes the dec counter to the inner encoder.
struct Dec<T>(T);

impl<T: Encoding> Encoding for Inc<T> {
    type Err = T::Err;

    fn write_help(&mut self, name: impl MetricNameEncoder, help: &str) -> Result<(), Self::Err> {
        self.0.write_help(name, help)
    }

    fn write_metric_value(
        &mut self,
        name: impl MetricNameEncoder,
        labels: impl LabelGroup,
        value: MetricValue,
    ) -> Result<(), Self::Err> {
        self.0.write_metric_value(name, labels, value)
    }
}

impl<T: Encoding> MetricEncoding<Inc<T>> for MeasuredCounterPairState
where
    CounterState: MetricEncoding<T>,
{
    fn write_type(name: impl MetricNameEncoder, enc: &mut Inc<T>) -> Result<(), T::Err> {
        CounterState::write_type(name, &mut enc.0)
    }
    fn collect_into(
        &self,
        metadata: &(),
        labels: impl LabelGroup,
        name: impl MetricNameEncoder,
        enc: &mut Inc<T>,
    ) -> Result<(), T::Err> {
        self.inc.collect_into(metadata, labels, name, &mut enc.0)
    }
}

impl<T: Encoding> Encoding for Dec<T> {
    type Err = T::Err;

    fn write_help(&mut self, name: impl MetricNameEncoder, help: &str) -> Result<(), Self::Err> {
        self.0.write_help(name, help)
    }

    fn write_metric_value(
        &mut self,
        name: impl MetricNameEncoder,
        labels: impl LabelGroup,
        value: MetricValue,
    ) -> Result<(), Self::Err> {
        self.0.write_metric_value(name, labels, value)
    }
}

/// Write the dec counter to the encoder
impl<T: Encoding> MetricEncoding<Dec<T>> for MeasuredCounterPairState
where
    CounterState: MetricEncoding<T>,
{
    fn write_type(name: impl MetricNameEncoder, enc: &mut Dec<T>) -> Result<(), T::Err> {
        CounterState::write_type(name, &mut enc.0)
    }
    fn collect_into(
        &self,
        metadata: &(),
        labels: impl LabelGroup,
        name: impl MetricNameEncoder,
        enc: &mut Dec<T>,
    ) -> Result<(), T::Err> {
        self.dec.collect_into(metadata, labels, name, &mut enc.0)
    }
}
