//! Periodically collect consumption metrics for all active tenants
//! and push them to a HTTP endpoint.
use crate::context::{DownloadBehavior, RequestContext};
use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};
use crate::tenant::{mgr, LogicalSizeCalculationCause};
use camino::Utf8PathBuf;
use consumption_metrics::EventType;
use pageserver_api::models::TenantState;
use reqwest::Url;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::*;
use utils::id::NodeId;

mod metrics;
use metrics::MetricsKey;
mod disk_cache;
mod upload;

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

/// Basically a key-value pair, but usually in a Vec except for [`Cache`].
///
/// This is as opposed to `consumption_metrics::Event` which is the externally communicated form.
/// Difference is basically the missing idempotency key, which lives only for the duration of
/// upload attempts.
type RawMetric = (MetricsKey, (EventType, u64));

/// Caches the [`RawMetric`]s
///
/// In practice, during startup, last sent values are stored here to be used in calculating new
/// ones. After successful uploading, the cached values are updated to cache. This used to be used
/// for deduplication, but that is no longer needed.
type Cache = HashMap<MetricsKey, (EventType, u64)>;

/// Main thread that serves metrics collection
pub async fn collect_metrics(
    metric_collection_endpoint: &Url,
    metric_collection_interval: Duration,
    _cached_metric_collection_interval: Duration,
    synthetic_size_calculation_interval: Duration,
    node_id: NodeId,
    local_disk_storage: Utf8PathBuf,
    ctx: RequestContext,
) -> anyhow::Result<()> {
    if _cached_metric_collection_interval != Duration::ZERO {
        tracing::warn!(
            "cached_metric_collection_interval is no longer used, please set it to zero."
        )
    }

    // spin up background worker that calculates tenant sizes
    let worker_ctx =
        ctx.detached_child(TaskKind::CalculateSyntheticSize, DownloadBehavior::Download);
    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::CalculateSyntheticSize,
        None,
        None,
        "synthetic size calculation",
        false,
        async move {
            calculate_synthetic_size_worker(synthetic_size_calculation_interval, &worker_ctx)
                .instrument(info_span!("synthetic_size_worker"))
                .await?;
            Ok(())
        },
    );

    let path: Arc<Utf8PathBuf> = Arc::new(local_disk_storage);

    let cancel = task_mgr::shutdown_token();

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

    let node_id = node_id.to_string();

    // reminder: ticker is ready immediatedly
    let mut ticker = tokio::time::interval(metric_collection_interval);

    loop {
        let tick_at = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            tick_at = ticker.tick() => tick_at,
        };

        // these are point in time, with variable "now"
        let metrics = metrics::collect_all_metrics(&cached_metrics, &ctx).await;

        if metrics.is_empty() {
            continue;
        }

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
        };

        let upload = async {
            let res = upload::upload_metrics(
                &client,
                metric_collection_endpoint,
                &cancel,
                &node_id,
                &metrics,
                &mut cached_metrics,
            )
            .await;
            if let Err(e) = res {
                // serialization error which should never happen
                tracing::error!("failed to upload due to {e:#}");
            }
        };

        // let these run concurrently
        let (_, _) = tokio::join!(flush, upload);

        crate::tenant::tasks::warn_when_period_overrun(
            tick_at.elapsed(),
            metric_collection_interval,
            "consumption_metrics_collect_metrics",
        );
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
                .map(|(_, (et, _))| et.recorded_at())
                .copied()
                .next();

            let cached = found_some.into_iter().collect::<Cache>();

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
    synthetic_size_calculation_interval: Duration,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    info!("starting calculate_synthetic_size_worker");

    // reminder: ticker is ready immediatedly
    let mut ticker = tokio::time::interval(synthetic_size_calculation_interval);
    let cause = LogicalSizeCalculationCause::ConsumptionMetricsSyntheticSize;

    loop {
        let tick_at = tokio::select! {
            _ = task_mgr::shutdown_watcher() => return Ok(()),
            tick_at = ticker.tick() => tick_at,
        };

        let tenants = match mgr::list_tenants().await {
            Ok(tenants) => tenants,
            Err(e) => {
                warn!("cannot get tenant list: {e:#}");
                continue;
            }
        };

        for (tenant_id, tenant_state) in tenants {
            if tenant_state != TenantState::Active {
                continue;
            }

            if let Ok(tenant) = mgr::get_tenant(tenant_id, true).await {
                if let Err(e) = tenant.calculate_synthetic_size(cause, ctx).await {
                    error!("failed to calculate synthetic size for tenant {tenant_id}: {e:#}");
                }
            }
        }

        crate::tenant::tasks::warn_when_period_overrun(
            tick_at.elapsed(),
            synthetic_size_calculation_interval,
            "consumption_metrics_synthetic_size_worker",
        );
    }
}
