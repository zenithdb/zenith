use std::sync::{Arc, OnceLock};

use lasso::ThreadedRodeo;
use measured::label::{
    FixedCardinalitySet, LabelGroupSet, LabelName, LabelSet, LabelValue, StaticLabelSet,
};
use measured::metric::histogram::Thresholds;
use measured::metric::name::MetricName;
use measured::{
    Counter, CounterVec, FixedCardinalityLabel, Gauge, Histogram, HistogramVec, LabelGroup,
    MetricGroup,
};
use metrics::{CounterPairAssoc, CounterPairVec, HyperLogLog, HyperLogLogVec};
use tokio::time::{self, Instant};

use crate::control_plane::messages::ColdStartInfo;
use crate::error::ErrorKind;

#[derive(MetricGroup)]
#[metric(new(thread_pool: Arc<ThreadPoolMetrics>))]
pub struct Metrics {
    #[metric(namespace = "proxy")]
    #[metric(init = ProxyMetrics::new(thread_pool))]
    pub proxy: ProxyMetrics,

    #[metric(namespace = "wake_compute_lock")]
    pub wake_compute_lock: ApiLockMetrics,
}

static SELF: OnceLock<Metrics> = OnceLock::new();
impl Metrics {
    pub fn install(thread_pool: Arc<ThreadPoolMetrics>) {
        SELF.set(Metrics::new(thread_pool))
            .ok()
            .expect("proxy metrics must not be installed more than once");
    }

    pub fn get() -> &'static Self {
        #[cfg(test)]
        return SELF.get_or_init(|| Metrics::new(Arc::new(ThreadPoolMetrics::new(0))));

        #[cfg(not(test))]
        SELF.get()
            .expect("proxy metrics must be installed by the main() function")
    }
}

#[derive(MetricGroup)]
#[metric(new(thread_pool: Arc<ThreadPoolMetrics>))]
pub struct ProxyMetrics {
    #[metric(flatten)]
    pub db_connections: CounterPairVec<NumDbConnectionsGauge>,
    #[metric(flatten)]
    pub client_connections: CounterPairVec<NumClientConnectionsGauge>,
    #[metric(flatten)]
    pub connection_requests: CounterPairVec<NumConnectionRequestsGauge>,
    #[metric(flatten)]
    pub http_endpoint_pools: HttpEndpointPools,
    #[metric(flatten)]
    pub cancel_channel_size: CounterPairVec<CancelChannelSizeGauge>,

    /// Time it took for proxy to establish a connection to the compute endpoint.
    // largest bucket = 2^16 * 0.5ms = 32s
    #[metric(metadata = Thresholds::exponential_buckets(0.0005, 2.0))]
    pub compute_connection_latency_seconds: HistogramVec<ComputeConnectionLatencySet, 16>,

    /// Time it took for proxy to receive a response from control plane.
    #[metric(
        // largest bucket = 2^16 * 0.2ms = 13s
        metadata = Thresholds::exponential_buckets(0.0002, 2.0),
    )]
    pub console_request_latency: HistogramVec<ConsoleRequestSet, 16>,

    /// Time it takes to acquire a token to call console plane.
    // largest bucket = 3^16 * 0.05ms = 2.15s
    #[metric(metadata = Thresholds::exponential_buckets(0.00005, 3.0))]
    pub control_plane_token_acquire_seconds: Histogram<16>,

    /// Size of the HTTP request body lengths.
    // smallest bucket = 16 bytes
    // largest bucket = 4^12 * 16 bytes = 256MB
    #[metric(metadata = Thresholds::exponential_buckets(16.0, 4.0))]
    pub http_conn_content_length_bytes: HistogramVec<StaticLabelSet<HttpDirection>, 12>,

    /// Time it takes to reclaim unused connection pools.
    #[metric(metadata = Thresholds::exponential_buckets(1e-6, 2.0))]
    pub http_pool_reclaimation_lag_seconds: Histogram<16>,

    /// Number of opened connections to a database.
    pub http_pool_opened_connections: Gauge,

    /// Number of cache hits/misses for allowed ips.
    pub allowed_ips_cache_misses: CounterVec<StaticLabelSet<CacheOutcome>>,

    /// Number of allowed ips
    #[metric(metadata = Thresholds::with_buckets([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 50.0, 100.0]))]
    pub allowed_ips_number: Histogram<10>,

    /// Number of connections (per sni).
    pub accepted_connections_by_sni: CounterVec<StaticLabelSet<SniKind>>,

    /// Number of connection failures (per kind).
    pub connection_failures_total: CounterVec<StaticLabelSet<ConnectionFailureKind>>,

    /// Number of wake-up failures (per kind).
    pub connection_failures_breakdown: CounterVec<ConnectionFailuresBreakdownSet>,

    /// Number of bytes sent/received between all clients and backends.
    pub io_bytes: CounterVec<StaticLabelSet<Direction>>,

    /// Number of errors by a given classification.
    pub errors_total: CounterVec<StaticLabelSet<crate::error::ErrorKind>>,

    /// Number of cancellation requests (per found/not_found).
    pub cancellation_requests_total: CounterVec<CancellationRequestSet>,

    /// Number of errors by a given classification
    pub redis_errors_total: CounterVec<RedisErrorsSet>,

    /// Number of TLS handshake failures
    pub tls_handshake_failures: Counter,

    /// Number of connection requests affected by authentication rate limits
    pub requests_auth_rate_limits_total: Counter,

    /// HLL approximate cardinality of endpoints that are connecting
    pub connecting_endpoints: HyperLogLogVec<StaticLabelSet<Protocol>, 32>,

    /// Number of endpoints affected by errors of a given classification
    pub endpoints_affected_by_errors: HyperLogLogVec<StaticLabelSet<crate::error::ErrorKind>, 32>,

    /// Number of endpoints affected by authentication rate limits
    pub endpoints_auth_rate_limits: HyperLogLog<32>,

    /// Number of invalid endpoints (per protocol, per rejected).
    pub invalid_endpoints_total: CounterVec<InvalidEndpointsSet>,

    /// Number of retries (per outcome, per retry_type).
    #[metric(metadata = Thresholds::with_buckets([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]))]
    pub retries_metric: HistogramVec<RetriesMetricSet, 9>,

    /// Number of events consumed from redis (per event type).
    pub redis_events_count: CounterVec<StaticLabelSet<RedisEventsCount>>,

    #[metric(namespace = "connect_compute_lock")]
    pub connect_compute_lock: ApiLockMetrics,

    #[metric(namespace = "scram_pool")]
    #[metric(init = thread_pool)]
    pub scram_pool: Arc<ThreadPoolMetrics>,
}

#[derive(MetricGroup)]
#[metric(new())]
pub struct ApiLockMetrics {
    /// Number of semaphores registered in this api lock
    pub semaphores_registered: Counter,
    /// Number of semaphores unregistered in this api lock
    pub semaphores_unregistered: Counter,
    /// Time it takes to reclaim unused semaphores in the api lock
    #[metric(metadata = Thresholds::exponential_buckets(1e-6, 2.0))]
    pub reclamation_lag_seconds: Histogram<16>,
    /// Time it takes to acquire a semaphore lock
    #[metric(metadata = Thresholds::exponential_buckets(1e-4, 2.0))]
    pub semaphore_acquire_seconds: Histogram<16>,
}

impl Default for ApiLockMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "direction")]
pub enum HttpDirection {
    Request,
    Response,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "direction")]
pub enum Direction {
    Tx,
    Rx,
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
#[label(singleton = "protocol")]
pub enum Protocol {
    Http,
    Ws,
    Tcp,
    SniRouter,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Http => "http",
            Protocol::Ws => "ws",
            Protocol::Tcp => "tcp",
            Protocol::SniRouter => "sni_router",
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum Bool {
    True,
    False,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "outcome")]
pub enum Outcome {
    Success,
    Failed,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "outcome")]
pub enum CacheOutcome {
    Hit,
    Miss,
}

#[derive(LabelGroup)]
#[label(set = ConsoleRequestSet)]
pub struct ConsoleRequest<'a> {
    #[label(dynamic_with = ThreadedRodeo, default)]
    pub request: &'a str,
}

#[derive(MetricGroup, Default)]
pub struct HttpEndpointPools {
    /// Number of endpoints we have registered pools for
    pub http_pool_endpoints_registered_total: Counter,
    /// Number of endpoints we have unregistered pools for
    pub http_pool_endpoints_unregistered_total: Counter,
}

pub struct HttpEndpointPoolsGuard<'a> {
    dec: &'a Counter,
}

impl Drop for HttpEndpointPoolsGuard<'_> {
    fn drop(&mut self) {
        self.dec.inc();
    }
}

impl HttpEndpointPools {
    pub fn guard(&self) -> HttpEndpointPoolsGuard<'_> {
        self.http_pool_endpoints_registered_total.inc();
        HttpEndpointPoolsGuard {
            dec: &self.http_pool_endpoints_unregistered_total,
        }
    }
}
pub struct NumDbConnectionsGauge;
impl CounterPairAssoc for NumDbConnectionsGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("opened_db_connections_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_db_connections_total");
    const INC_HELP: &'static str = "Number of opened connections to a database.";
    const DEC_HELP: &'static str = "Number of closed connections to a database.";
    type LabelGroupSet = StaticLabelSet<Protocol>;
}
pub type NumDbConnectionsGuard<'a> = metrics::MeasuredCounterPairGuard<'a, NumDbConnectionsGauge>;

pub struct NumClientConnectionsGauge;
impl CounterPairAssoc for NumClientConnectionsGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("opened_client_connections_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_client_connections_total");
    const INC_HELP: &'static str = "Number of opened connections from a client.";
    const DEC_HELP: &'static str = "Number of closed connections from a client.";
    type LabelGroupSet = StaticLabelSet<Protocol>;
}
pub type NumClientConnectionsGuard<'a> =
    metrics::MeasuredCounterPairGuard<'a, NumClientConnectionsGauge>;

pub struct NumConnectionRequestsGauge;
impl CounterPairAssoc for NumConnectionRequestsGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("accepted_connections_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_connections_total");
    const INC_HELP: &'static str = "Number of client connections accepted.";
    const DEC_HELP: &'static str = "Number of client connections closed.";
    type LabelGroupSet = StaticLabelSet<Protocol>;
}
pub type NumConnectionRequestsGuard<'a> =
    metrics::MeasuredCounterPairGuard<'a, NumConnectionRequestsGauge>;

pub struct CancelChannelSizeGauge;
impl CounterPairAssoc for CancelChannelSizeGauge {
    const INC_NAME: &'static MetricName = MetricName::from_str("opened_msgs_cancel_channel_total");
    const DEC_NAME: &'static MetricName = MetricName::from_str("closed_msgs_cancel_channel_total");
    const INC_HELP: &'static str = "Number of processing messages in the cancellation channel.";
    const DEC_HELP: &'static str = "Number of closed messages in the cancellation channel.";
    type LabelGroupSet = StaticLabelSet<RedisMsgKind>;
}
pub type CancelChannelSizeGuard<'a> = metrics::MeasuredCounterPairGuard<'a, CancelChannelSizeGauge>;

#[derive(LabelGroup)]
#[label(set = ComputeConnectionLatencySet)]
pub struct ComputeConnectionLatencyGroup {
    protocol: Protocol,
    cold_start_info: ColdStartInfo,
    outcome: ConnectOutcome,
    excluded: LatencyExclusions,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum LatencyExclusions {
    Client,
    ClientAndCplane,
    ClientCplaneCompute,
    ClientCplaneComputeRetry,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "kind")]
pub enum SniKind {
    Sni,
    NoSni,
    PasswordHack,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "kind")]
pub enum ConnectionFailureKind {
    ComputeCached,
    ComputeUncached,
}

#[derive(LabelGroup)]
#[label(set = ConnectionFailuresBreakdownSet)]
pub struct ConnectionFailuresBreakdownGroup {
    pub kind: ErrorKind,
    pub retry: Bool,
}

#[derive(LabelGroup, Copy, Clone)]
#[label(set = RedisErrorsSet)]
pub struct RedisErrors<'a> {
    #[label(dynamic_with = ThreadedRodeo, default)]
    pub channel: &'a str,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
pub enum CancellationOutcome {
    NotFound,
    Found,
    RateLimitExceeded,
}

#[derive(LabelGroup)]
#[label(set = CancellationRequestSet)]
pub struct CancellationRequest {
    pub kind: CancellationOutcome,
}

#[derive(Clone, Copy)]
pub enum Waiting {
    Cplane,
    Client,
    Compute,
    RetryTimeout,
}

#[derive(FixedCardinalityLabel, Copy, Clone)]
#[label(singleton = "kind")]
pub enum RedisMsgKind {
    HSet,
    HSetMultiple,
    HGet,
    HGetAll,
    HDel,
}

#[derive(Default)]
struct Accumulated {
    cplane: time::Duration,
    client: time::Duration,
    compute: time::Duration,
    retry: time::Duration,
}

pub struct LatencyTimer {
    // time since the stopwatch was started
    start: time::Instant,
    // time since the stopwatch was stopped
    stop: Option<time::Instant>,
    // accumulated time on the stopwatch
    accumulated: Accumulated,
    // label data
    protocol: Protocol,
    cold_start_info: ColdStartInfo,
    outcome: ConnectOutcome,

    skip_reporting: bool,
}

impl LatencyTimer {
    pub fn new(protocol: Protocol) -> Self {
        Self {
            start: time::Instant::now(),
            stop: None,
            accumulated: Accumulated::default(),
            protocol,
            cold_start_info: ColdStartInfo::Unknown,
            // assume failed unless otherwise specified
            outcome: ConnectOutcome::Failed,
            skip_reporting: false,
        }
    }

    pub(crate) fn noop(protocol: Protocol) -> Self {
        Self {
            start: time::Instant::now(),
            stop: None,
            accumulated: Accumulated::default(),
            protocol,
            cold_start_info: ColdStartInfo::Unknown,
            // assume failed unless otherwise specified
            outcome: ConnectOutcome::Failed,
            skip_reporting: true,
        }
    }

    pub fn unpause(&mut self, start: Instant, waiting_for: Waiting) {
        let dur = start.elapsed();
        match waiting_for {
            Waiting::Cplane => self.accumulated.cplane += dur,
            Waiting::Client => self.accumulated.client += dur,
            Waiting::Compute => self.accumulated.compute += dur,
            Waiting::RetryTimeout => self.accumulated.retry += dur,
        }
    }

    pub fn cold_start_info(&mut self, cold_start_info: ColdStartInfo) {
        self.cold_start_info = cold_start_info;
    }

    pub fn success(&mut self) {
        // stop the stopwatch and record the time that we have accumulated
        self.stop = Some(time::Instant::now());

        // success
        self.outcome = ConnectOutcome::Success;
    }
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
pub enum ConnectOutcome {
    Success,
    Failed,
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        if self.skip_reporting {
            return;
        }

        let duration = self
            .stop
            .unwrap_or_else(time::Instant::now)
            .duration_since(self.start);

        let metric = &Metrics::get().proxy.compute_connection_latency_seconds;

        // Excluding client communication from the accumulated time.
        metric.observe(
            ComputeConnectionLatencyGroup {
                protocol: self.protocol,
                cold_start_info: self.cold_start_info,
                outcome: self.outcome,
                excluded: LatencyExclusions::Client,
            },
            duration
                .saturating_sub(self.accumulated.client)
                .as_secs_f64(),
        );

        // Exclude client and cplane communication from the accumulated time.
        let accumulated_total = self.accumulated.client + self.accumulated.cplane;
        metric.observe(
            ComputeConnectionLatencyGroup {
                protocol: self.protocol,
                cold_start_info: self.cold_start_info,
                outcome: self.outcome,
                excluded: LatencyExclusions::ClientAndCplane,
            },
            duration.saturating_sub(accumulated_total).as_secs_f64(),
        );

        // Exclude client cplane, compue communication from the accumulated time.
        let accumulated_total =
            self.accumulated.client + self.accumulated.cplane + self.accumulated.compute;
        metric.observe(
            ComputeConnectionLatencyGroup {
                protocol: self.protocol,
                cold_start_info: self.cold_start_info,
                outcome: self.outcome,
                excluded: LatencyExclusions::ClientCplaneCompute,
            },
            duration.saturating_sub(accumulated_total).as_secs_f64(),
        );

        // Exclude client cplane, compue, retry communication from the accumulated time.
        let accumulated_total = self.accumulated.client
            + self.accumulated.cplane
            + self.accumulated.compute
            + self.accumulated.retry;
        metric.observe(
            ComputeConnectionLatencyGroup {
                protocol: self.protocol,
                cold_start_info: self.cold_start_info,
                outcome: self.outcome,
                excluded: LatencyExclusions::ClientCplaneComputeRetry,
            },
            duration.saturating_sub(accumulated_total).as_secs_f64(),
        );
    }
}

impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        if value {
            Bool::True
        } else {
            Bool::False
        }
    }
}

#[derive(LabelGroup)]
#[label(set = InvalidEndpointsSet)]
pub struct InvalidEndpointsGroup {
    pub protocol: Protocol,
    pub rejected: Bool,
    pub outcome: ConnectOutcome,
}

#[derive(LabelGroup)]
#[label(set = RetriesMetricSet)]
pub struct RetriesMetricGroup {
    pub outcome: ConnectOutcome,
    pub retry_type: RetryType,
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
pub enum RetryType {
    WakeCompute,
    ConnectToCompute,
}

#[derive(FixedCardinalityLabel, Clone, Copy, Debug)]
#[label(singleton = "event")]
pub enum RedisEventsCount {
    EndpointCreated,
    BranchCreated,
    ProjectCreated,
    CancelSession,
    PasswordUpdate,
    AllowedIpsUpdate,
}

pub struct ThreadPoolWorkers(usize);
#[derive(Copy, Clone)]
pub struct ThreadPoolWorkerId(pub usize);

impl LabelValue for ThreadPoolWorkerId {
    fn visit<V: measured::label::LabelVisitor>(&self, v: V) -> V::Output {
        v.write_int(self.0 as i64)
    }
}

impl LabelGroup for ThreadPoolWorkerId {
    fn visit_values(&self, v: &mut impl measured::label::LabelGroupVisitor) {
        v.write_value(LabelName::from_str("worker"), self);
    }
}

impl LabelGroupSet for ThreadPoolWorkers {
    type Group<'a> = ThreadPoolWorkerId;

    fn cardinality(&self) -> Option<usize> {
        Some(self.0)
    }

    fn encode_dense(&self, value: Self::Unique) -> Option<usize> {
        Some(value)
    }

    fn decode_dense(&self, value: usize) -> Self::Group<'_> {
        ThreadPoolWorkerId(value)
    }

    type Unique = usize;

    fn encode(&self, value: Self::Group<'_>) -> Option<Self::Unique> {
        Some(value.0)
    }

    fn decode(&self, value: &Self::Unique) -> Self::Group<'_> {
        ThreadPoolWorkerId(*value)
    }
}

impl LabelSet for ThreadPoolWorkers {
    type Value<'a> = ThreadPoolWorkerId;

    fn dynamic_cardinality(&self) -> Option<usize> {
        Some(self.0)
    }

    fn encode(&self, value: Self::Value<'_>) -> Option<usize> {
        (value.0 < self.0).then_some(value.0)
    }

    fn decode(&self, value: usize) -> Self::Value<'_> {
        ThreadPoolWorkerId(value)
    }
}

impl FixedCardinalitySet for ThreadPoolWorkers {
    fn cardinality(&self) -> usize {
        self.0
    }
}

#[derive(MetricGroup)]
#[metric(new(workers: usize))]
pub struct ThreadPoolMetrics {
    #[metric(init = CounterVec::with_label_set(ThreadPoolWorkers(workers)))]
    pub worker_task_turns_total: CounterVec<ThreadPoolWorkers>,
    #[metric(init = CounterVec::with_label_set(ThreadPoolWorkers(workers)))]
    pub worker_task_skips_total: CounterVec<ThreadPoolWorkers>,
}
