//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
use crate::config::PageServerConf;
use crate::consumption_metrics::metrics::MetricsKey;
use crate::consumption_metrics::upload::KeyGen as _;
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::size::CalculateSyntheticSizeError;
use crate::tenant::tasks::BackgroundLoopKind;
use crate::tenant::{mgr::TenantShardManager, LogicalSizeCalculationCause, TenantShard};
use camino::Utf8PathBuf;
use consumption_metrics::EventType;
use itertools::Itertools as _;
use pageserver_api::models::TenantState;
use remote_storage::{GenericRemoteStorage, RemoteStorageConfig};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::id::NodeId;

mod disk_cache;
mod metrics;
mod upload;

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

/// Basically a key-value pair, but usually in a Vec except for [`Cache`].
///
/// This is as opposed to `consumption_metrics::Event` which is the externally communicated form.
/// Difference is basically the missing idempotency key, which lives only for the duration of
/// upload attempts.
type RawMetric = (MetricsKey, (EventType, u64));

/// The new serializable metrics format
#[derive(Serialize, Deserialize)]
struct NewMetricsRoot {
    version: usize,
    metrics: Vec<NewRawMetric>,
}

impl NewMetricsRoot {
    pub fn is_v2_metrics(json_value: &serde_json::Value) -> bool {
        if let Some(ver) = json_value.get("version") {
            if let Some(2) = ver.as_u64() {
                return true;
            }
        }
        false
    }
}

/// The new serializable metrics format
#[derive(Serialize)]
struct NewMetricsRefRoot<'a> {
    version: usize,
    metrics: &'a [NewRawMetric],
}

impl<'a> NewMetricsRefRoot<'a> {
    fn new(metrics: &'a [NewRawMetric]) -> Self {
        Self {
            version: 2,
            metrics,
        }
    }
}

/// The new serializable metrics format
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct NewRawMetric {
    key: MetricsKey,
    kind: EventType,
    value: u64,
    // TODO: add generation field and check against generations
}

impl NewRawMetric {
    #[cfg(test)]
    fn to_kv_pair(&self) -> (MetricsKey, NewRawMetric) {
        (self.key, self.clone())
    }
}

/// Caches the [`RawMetric`]s
///
/// In practice, during startup, last sent values are stored here to be used in calculating new
/// ones. After successful uploading, the cached values are updated to cache. This used to be used
/// for deduplication, but that is no longer needed.
type Cache = HashMap<MetricsKey, NewRawMetric>;

pub async fn run(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantShardManager>,
    cancel: CancellationToken,
) {
    let Some(metric_collection_endpoint) = conf.metric_collection_endpoint.as_ref() else {
        return;
    };

    let local_disk_storage = conf.workdir.join("last_consumption_metrics.json");

    let metrics_ctx = RequestContext::todo_child(
        TaskKind::MetricsCollection,
        // This task itself shouldn't download anything.
        // The actual size calculation does need downloads, and
        // creates a child context with the right DownloadBehavior.
        DownloadBehavior::Error,
    );
    let collect_metrics = BACKGROUND_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
        "consumption metrics collection",
        collect_metrics(
            tenant_manager.clone(),
            metric_collection_endpoint,
            &conf.metric_collection_bucket,
            conf.metric_collection_interval,
            conf.id,
            local_disk_storage,
            cancel.clone(),
            metrics_ctx,
        )
        .instrument(info_span!("metrics_collection")),
    ));

    let worker_ctx =
        RequestContext::todo_child(TaskKind::CalculateSyntheticSize, DownloadBehavior::Download);
    let synthetic_size_worker = BACKGROUND_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
        "synthetic size calculation",
        calculate_synthetic_size_worker(
            tenant_manager.clone(),
            conf.synthetic_size_calculation_interval,
            cancel.clone(),
            worker_ctx,
        )
        .instrument(info_span!("synthetic_size_worker")),
    ));

    let (collect_metrics, synthetic_size_worker) =
        futures::future::join(collect_metrics, synthetic_size_worker).await;
    collect_metrics
        .expect("unreachable: exit_on_panic_or_error would catch the panic and exit the process");
    synthetic_size_worker
        .expect("unreachable: exit_on_panic_or_error would catch the panic and exit the process");
}

/// Main thread that serves metrics collection
#[allow(clippy::too_many_arguments)]
async fn collect_metrics(
    tenant_manager: Arc<TenantShardManager>,
    metric_collection_endpoint: &Url,
    metric_collection_bucket: &Option<RemoteStorageConfig>,
    metric_collection_interval: Duration,
    node_id: NodeId,
    local_disk_storage: Utf8PathBuf,
    cancel: CancellationToken,
    ctx: RequestContext,
) -> anyhow::Result<()> {
    let path: Arc<Utf8PathBuf> = Arc::new(local_disk_storage);

    let restore_and_reschedule = restore_and_reschedule(&path, metric_collection_interval);

    let mut cached_metrics = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        ret = restore_and_reschedule => ret,
    };

    // define client here to reuse it for all requests
    let client = reqwest::ClientBuilder::new()
        .timeout(DEFAULT_HTTP_REPORTING_TIMEOUT)
        .build()
        .expect("Failed to create http client with timeout");

    let bucket_client = if let Some(bucket_config) = metric_collection_bucket {
        match GenericRemoteStorage::from_config(bucket_config).await {
            Ok(client) => Some(client),
            Err(e) => {
                // Non-fatal error: if we were given an invalid config, we will proceed
                // with sending metrics over the network, but not to S3.
                tracing::warn!("Invalid configuration for metric_collection_bucket: {e}");
                None
            }
        }
    } else {
        None
    };

    let node_id = node_id.to_string();

    loop {
        let started_at = Instant::now();

        // these are point in time, with variable "now"
        let metrics = metrics::collect_all_metrics(&tenant_manager, &cached_metrics, &ctx).await;

        // Pre-generate event idempotency keys, to reuse them across the bucket
        // and HTTP sinks.
        let idempotency_keys = std::iter::repeat_with(|| node_id.as_str().generate())
            .take(metrics.len())
            .collect_vec();

        let metrics = Arc::new(metrics);

        // why not race cancellation here? because we are one of the last tasks, and if we are
        // already here, better to try to flush the new values.

        let flush = async {
            match disk_cache::flush_metrics_to_disk(&metrics, &path).await {
                Ok(()) => {
                    tracing::debug!("flushed metrics to disk");
                }
                Err(e) => {
                    // idea here is that if someone creates a directory as our path, then they
                    // might notice it from the logs before shutdown and remove it
                    tracing::error!("failed to persist metrics to {path:?}: {e:#}");
                }
            }

            if let Some(bucket_client) = &bucket_client {
                let res = upload::upload_metrics_bucket(
                    bucket_client,
                    &cancel,
                    &node_id,
                    &metrics,
                    &idempotency_keys,
                )
                .await;
                if let Err(e) = res {
                    tracing::error!("failed to upload to remote storage: {e:#}");
                }
            }
        };

        let upload = async {
            let res = upload::upload_metrics_http(
                &client,
                metric_collection_endpoint,
                &cancel,
                &metrics,
                &mut cached_metrics,
                &idempotency_keys,
            )
            .await;
            if let Err(e) = res {
                // serialization error which should never happen
                tracing::error!("failed to upload via HTTP due to {e:#}");
            }
        };

        // let these run concurrently
        let (_, _) = tokio::join!(flush, upload);

        crate::tenant::tasks::warn_when_period_overrun(
            started_at.elapsed(),
            metric_collection_interval,
            BackgroundLoopKind::ConsumptionMetricsCollectMetrics,
        );

        let res =
            tokio::time::timeout_at(started_at + metric_collection_interval, cancel.cancelled())
                .await;
        if res.is_ok() {
            return Ok(());
        }
    }
}

/// Called on the first iteration in an attempt to join the metric uploading schedule from previous
/// pageserver session. Pageserver is supposed to upload at intervals regardless of restarts.
///
/// Cancellation safe.
async fn restore_and_reschedule(
    path: &Arc<Utf8PathBuf>,
    metric_collection_interval: Duration,
) -> Cache {
    let (cached, earlier_metric_at) = match disk_cache::read_metrics_from_disk(path.clone()).await {
        Ok(found_some) => {
            // there is no min needed because we write these sequentially in
            // collect_all_metrics
            let earlier_metric_at = found_some
                .iter()
                .map(|item| item.kind.recorded_at())
                .copied()
                .next();

            let cached = found_some
                .into_iter()
                .map(|item| (item.key, item))
                .collect::<Cache>();

            (cached, earlier_metric_at)
        }
        Err(e) => {
            use std::io::{Error, ErrorKind};

            let root = e.root_cause();
            let maybe_ioerr = root.downcast_ref::<Error>();
            let is_not_found = maybe_ioerr.is_some_and(|e| e.kind() == ErrorKind::NotFound);

            if !is_not_found {
                tracing::info!("failed to read any previous metrics from {path:?}: {e:#}");
            }

            (HashMap::new(), None)
        }
    };

    if let Some(earlier_metric_at) = earlier_metric_at {
        let earlier_metric_at: SystemTime = earlier_metric_at.into();

        let error = reschedule(earlier_metric_at, metric_collection_interval).await;

        if let Some(error) = error {
            if error.as_secs() >= 60 {
                tracing::info!(
                    error_ms = error.as_millis(),
                    "startup scheduling error due to restart"
                )
            }
        }
    }

    cached
}

async fn reschedule(
    earlier_metric_at: SystemTime,
    metric_collection_interval: Duration,
) -> Option<Duration> {
    let now = SystemTime::now();
    match now.duration_since(earlier_metric_at) {
        Ok(from_last_send) if from_last_send < metric_collection_interval => {
            let sleep_for = metric_collection_interval - from_last_send;

            let deadline = std::time::Instant::now() + sleep_for;

            tokio::time::sleep_until(deadline.into()).await;

            let now = std::time::Instant::now();

            // executor threads might be busy, add extra measurements
            Some(if now < deadline {
                deadline - now
            } else {
                now - deadline
            })
        }
        Ok(from_last_send) => Some(from_last_send.saturating_sub(metric_collection_interval)),
        Err(_) => {
            tracing::warn!(
                ?now,
                ?earlier_metric_at,
                "oldest recorded metric is in future; first values will come out with inconsistent timestamps"
            );
            earlier_metric_at.duration_since(now).ok()
        }
    }
}

/// Caclculate synthetic size for each active tenant
async fn calculate_synthetic_size_worker(
    tenant_manager: Arc<TenantShardManager>,
    synthetic_size_calculation_interval: Duration,
    cancel: CancellationToken,
    ctx: RequestContext,
) -> anyhow::Result<()> {
    info!("starting calculate_synthetic_size_worker");
    scopeguard::defer! {
        info!("calculate_synthetic_size_worker stopped");
    };

    loop {
        let started_at = Instant::now();

        let tenants = match tenant_manager.list_tenants() {
            Ok(tenants) => tenants,
            Err(e) => {
                warn!("cannot get tenant list: {e:#}");
                continue;
            }
        };

        for (tenant_shard_id, tenant_state, _gen) in tenants {
            if tenant_state != TenantState::Active {
                continue;
            }

            if !tenant_shard_id.is_shard_zero() {
                // We only send consumption metrics from shard 0, so don't waste time calculating
                // synthetic size on other shards.
                continue;
            }

            let Ok(tenant) = tenant_manager.get_attached_tenant_shard(tenant_shard_id) else {
                continue;
            };

            if !tenant.is_active() {
                continue;
            }

            // there is never any reason to exit calculate_synthetic_size_worker following any
            // return value -- we don't need to care about shutdown because no tenant is found when
            // pageserver is shut down.
            calculate_and_log(&tenant, &cancel, &ctx).await;
        }

        crate::tenant::tasks::warn_when_period_overrun(
            started_at.elapsed(),
            synthetic_size_calculation_interval,
            BackgroundLoopKind::ConsumptionMetricsSyntheticSizeWorker,
        );

        let res = tokio::time::timeout_at(
            started_at + synthetic_size_calculation_interval,
            cancel.cancelled(),
        )
        .await;
        if res.is_ok() {
            return Ok(());
        }
    }
}

async fn calculate_and_log(tenant: &TenantShard, cancel: &CancellationToken, ctx: &RequestContext) {
    const CAUSE: LogicalSizeCalculationCause =
        LogicalSizeCalculationCause::ConsumptionMetricsSyntheticSize;

    // TODO should we use concurrent_background_tasks_rate_limit() here, like the other background tasks?
    // We can put in some prioritization for consumption metrics.
    // Same for the loop that fetches computed metrics.
    // By using the same limiter, we centralize metrics collection for "start" and "finished" counters,
    // which turns out is really handy to understand the system.
    match tenant.calculate_synthetic_size(CAUSE, cancel, ctx).await {
        Ok(_) => {}
        Err(CalculateSyntheticSizeError::Cancelled) => {}
        Err(e) => {
            let tenant_shard_id = tenant.tenant_shard_id();
            error!("failed to calculate synthetic size for tenant {tenant_shard_id}: {e:#}");
        }
    }
}
