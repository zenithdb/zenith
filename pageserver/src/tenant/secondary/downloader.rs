use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    disk_usage_eviction_task::{
        finite_f32, DiskUsageEvictionInfo, EvictionCandidate, EvictionLayer, EvictionSecondaryLayer,
    },
    metrics::SECONDARY_MODE,
    tenant::{
        config::SecondaryLocationConfig,
        debug_assert_current_span_has_tenant_and_timeline_id,
        ephemeral_file::is_ephemeral_file,
        remote_timeline_client::{
            index::LayerFileMetadata, is_temp_download_file, FAILED_DOWNLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
        },
        span::debug_assert_current_span_has_tenant_id,
        storage_layer::{layer::local_layer_path, LayerName, LayerVisibilityHint},
        tasks::{warn_when_period_overrun, BackgroundLoopKind},
    },
    virtual_file::{on_fatal_io_error, MaybeFatalIo, VirtualFile},
    TEMP_FILE_SUFFIX,
};

use super::{
    heatmap::HeatMapLayer,
    scheduler::{
        self, period_jitter, period_warmup, Completion, JobGenerator, SchedulingResult,
        TenantBackgroundJobs,
    },
    GetTenantError, SecondaryTenant, SecondaryTenantError,
};

use crate::tenant::{
    mgr::TenantShardManager,
    remote_timeline_client::{download::download_layer_file, remote_heatmap_path},
};

use camino::Utf8PathBuf;
use chrono::format::{DelayedFormat, StrftimeItems};
use futures::Future;
use metrics::UIntGauge;
use pageserver_api::models::SecondaryProgress;
use pageserver_api::shard::TenantShardId;
use remote_storage::{DownloadError, DownloadKind, DownloadOpts, Etag, GenericRemoteStorage};

use tokio_util::sync::CancellationToken;
use tracing::{info_span, instrument, warn, Instrument};
use utils::{
    backoff, completion::Barrier, crashsafe::path_with_suffix_extension, failpoint_support, fs_ext,
    id::TimelineId, pausable_failpoint, serde_system_time,
};

use super::{
    heatmap::{HeatMapTenant, HeatMapTimeline},
    CommandRequest, DownloadCommand,
};

/// For each tenant, default period for how long must have passed since the last download_tenant call before
/// calling it again.  This default is replaced with the value of [`HeatMapTenant::upload_period_ms`] after first
/// download, if the uploader populated it.
const DEFAULT_DOWNLOAD_INTERVAL: Duration = Duration::from_millis(60000);

pub(super) async fn downloader_task(
    tenant_manager: Arc<TenantShardManager>,
    remote_storage: GenericRemoteStorage,
    command_queue: tokio::sync::mpsc::Receiver<CommandRequest<DownloadCommand>>,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
    root_ctx: RequestContext,
) {
    let concurrency = tenant_manager.get_conf().secondary_download_concurrency;

    let generator = SecondaryDownloader {
        tenant_manager,
        remote_storage,
        root_ctx,
    };
    let mut scheduler = Scheduler::new(generator, concurrency);

    scheduler
        .run(command_queue, background_jobs_can_start, cancel)
        .instrument(info_span!("secondary_download_scheduler"))
        .await
}

struct SecondaryDownloader {
    tenant_manager: Arc<TenantShardManager>,
    remote_storage: GenericRemoteStorage,
    root_ctx: RequestContext,
}

#[derive(Debug, Clone)]
pub(super) struct OnDiskState {
    metadata: LayerFileMetadata,
    access_time: SystemTime,
    local_path: Utf8PathBuf,
}

impl OnDiskState {
    fn new(
        _conf: &'static PageServerConf,
        _tenant_shard_id: &TenantShardId,
        _imeline_id: &TimelineId,
        _ame: LayerName,
        metadata: LayerFileMetadata,
        access_time: SystemTime,
        local_path: Utf8PathBuf,
    ) -> Self {
        Self {
            metadata,
            access_time,
            local_path,
        }
    }

    // This is infallible, because all errors are either acceptable (ENOENT), or totally
    // unexpected (fatal).
    pub(super) fn remove_blocking(&self) {
        // We tolerate ENOENT, because between planning eviction and executing
        // it, the secondary downloader could have seen an updated heatmap that
        // resulted in a layer being deleted.
        // Other local I/O errors are process-fatal: these should never happen.
        std::fs::remove_file(&self.local_path)
            .or_else(fs_ext::ignore_not_found)
            .fatal_err("Deleting secondary layer")
    }

    pub(crate) fn file_size(&self) -> u64 {
        self.metadata.file_size
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct SecondaryDetailTimeline {
    on_disk_layers: HashMap<LayerName, OnDiskState>,

    /// We remember when layers were evicted, to prevent re-downloading them.
    pub(super) evicted_at: HashMap<LayerName, SystemTime>,
}

impl SecondaryDetailTimeline {
    pub(super) fn remove_layer(
        &mut self,
        name: &LayerName,
        resident_metric: &UIntGauge,
    ) -> Option<OnDiskState> {
        let removed = self.on_disk_layers.remove(name);
        if let Some(removed) = &removed {
            resident_metric.sub(removed.file_size());
        }
        removed
    }

    /// `local_path`
    fn touch_layer<F>(
        &mut self,
        conf: &'static PageServerConf,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
        touched: &HeatMapLayer,
        resident_metric: &UIntGauge,
        local_path: F,
    ) where
        F: FnOnce() -> Utf8PathBuf,
    {
        use std::collections::hash_map::Entry;
        match self.on_disk_layers.entry(touched.name.clone()) {
            Entry::Occupied(mut v) => {
                v.get_mut().access_time = touched.access_time;
            }
            Entry::Vacant(e) => {
                e.insert(OnDiskState::new(
                    conf,
                    tenant_shard_id,
                    timeline_id,
                    touched.name.clone(),
                    touched.metadata.clone(),
                    touched.access_time,
                    local_path(),
                ));
                resident_metric.add(touched.metadata.file_size);
            }
        }
    }
}

// Aspects of a heatmap that we remember after downloading it
#[derive(Clone, Debug)]
struct DownloadSummary {
    etag: Etag,
    #[allow(unused)]
    mtime: SystemTime,
    upload_period: Duration,
}

/// This state is written by the secondary downloader, it is opaque
/// to TenantManager
#[derive(Debug)]
pub(super) struct SecondaryDetail {
    pub(super) config: SecondaryLocationConfig,

    last_download: Option<DownloadSummary>,
    next_download: Option<Instant>,
    timelines: HashMap<TimelineId, SecondaryDetailTimeline>,
}

/// Helper for logging SystemTime
fn strftime(t: &'_ SystemTime) -> DelayedFormat<StrftimeItems<'_>> {
    let datetime: chrono::DateTime<chrono::Utc> = (*t).into();
    datetime.format("%d/%m/%Y %T")
}

/// Information returned from download function when it detects the heatmap has changed
struct HeatMapModified {
    etag: Etag,
    last_modified: SystemTime,
    bytes: Vec<u8>,
}

enum HeatMapDownload {
    // The heatmap's etag has changed: return the new etag, mtime and the body bytes
    Modified(HeatMapModified),
    // The heatmap's etag is unchanged
    Unmodified,
}

impl SecondaryDetail {
    pub(super) fn new(config: SecondaryLocationConfig) -> Self {
        Self {
            config,
            last_download: None,
            next_download: None,
            timelines: HashMap::new(),
        }
    }

    #[cfg(feature = "testing")]
    pub(crate) fn total_resident_size(&self) -> u64 {
        self.timelines
            .values()
            .map(|tl| {
                tl.on_disk_layers
                    .values()
                    .map(|v| v.metadata.file_size)
                    .sum::<u64>()
            })
            .sum::<u64>()
    }

    pub(super) fn evict_layer(
        &mut self,
        name: LayerName,
        timeline_id: &TimelineId,
        now: SystemTime,
        resident_metric: &UIntGauge,
    ) -> Option<OnDiskState> {
        let timeline = self.timelines.get_mut(timeline_id)?;
        let removed = timeline.remove_layer(&name, resident_metric);
        if removed.is_some() {
            timeline.evicted_at.insert(name, now);
        }
        removed
    }

    pub(super) fn remove_timeline(
        &mut self,
        timeline_id: &TimelineId,
        resident_metric: &UIntGauge,
    ) {
        let removed = self.timelines.remove(timeline_id);
        if let Some(removed) = removed {
            resident_metric.sub(
                removed
                    .on_disk_layers
                    .values()
                    .map(|l| l.metadata.file_size)
                    .sum(),
            );
        }
    }

    /// Additionally returns the total number of layers, used for more stable relative access time
    /// based eviction.
    pub(super) fn get_layers_for_eviction(
        &self,
        parent: &Arc<SecondaryTenant>,
    ) -> (DiskUsageEvictionInfo, usize) {
        let mut result = DiskUsageEvictionInfo::default();
        let mut total_layers = 0;

        for (timeline_id, timeline_detail) in &self.timelines {
            result
                .resident_layers
                .extend(timeline_detail.on_disk_layers.iter().map(|(name, ods)| {
                    EvictionCandidate {
                        layer: EvictionLayer::Secondary(EvictionSecondaryLayer {
                            secondary_tenant: parent.clone(),
                            timeline_id: *timeline_id,
                            name: name.clone(),
                            metadata: ods.metadata.clone(),
                        }),
                        last_activity_ts: ods.access_time,
                        relative_last_activity: finite_f32::FiniteF32::ZERO,
                        // Secondary location layers are presumed visible, because Covered layers
                        // are excluded from the heatmap
                        visibility: LayerVisibilityHint::Visible,
                    }
                }));

            // total might be missing currently downloading layers, but as a lower than actual
            // value it is good enough approximation.
            total_layers += timeline_detail.on_disk_layers.len() + timeline_detail.evicted_at.len();
        }
        result.max_layer_size = result
            .resident_layers
            .iter()
            .map(|l| l.layer.get_file_size())
            .max();

        tracing::debug!(
            "eviction: secondary tenant {} found {} timelines, {} layers",
            parent.get_tenant_shard_id(),
            self.timelines.len(),
            result.resident_layers.len()
        );

        (result, total_layers)
    }
}

struct PendingDownload {
    secondary_state: Arc<SecondaryTenant>,
    last_download: Option<DownloadSummary>,
    target_time: Option<Instant>,
}

impl scheduler::PendingJob for PendingDownload {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        self.secondary_state.get_tenant_shard_id()
    }
}

struct RunningDownload {
    barrier: Barrier,
}

impl scheduler::RunningJob for RunningDownload {
    fn get_barrier(&self) -> Barrier {
        self.barrier.clone()
    }
}

struct CompleteDownload {
    secondary_state: Arc<SecondaryTenant>,
    completed_at: Instant,
    result: Result<(), UpdateError>,
}

impl scheduler::Completion for CompleteDownload {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        self.secondary_state.get_tenant_shard_id()
    }
}

type Scheduler = TenantBackgroundJobs<
    SecondaryDownloader,
    PendingDownload,
    RunningDownload,
    CompleteDownload,
    DownloadCommand,
>;

impl JobGenerator<PendingDownload, RunningDownload, CompleteDownload, DownloadCommand>
    for SecondaryDownloader
{
    #[instrument(skip_all, fields(tenant_id=%completion.get_tenant_shard_id().tenant_id, shard_id=%completion.get_tenant_shard_id().shard_slug()))]
    fn on_completion(&mut self, completion: CompleteDownload) {
        let CompleteDownload {
            secondary_state,
            completed_at: _completed_at,
            result,
        } = completion;

        tracing::debug!("Secondary tenant download completed");

        let mut detail = secondary_state.detail.lock().unwrap();

        match result {
            Err(UpdateError::Restart) => {
                // Start downloading again as soon as we can.  This will involve waiting for the scheduler's
                // scheduling interval.  This slightly reduces the peak download speed of tenants that hit their
                // deadline and keep restarting, but that also helps give other tenants a chance to execute rather
                // that letting one big tenant dominate for a long time.
                detail.next_download = Some(Instant::now());
            }
            _ => {
                let period = detail
                    .last_download
                    .as_ref()
                    .map(|d| d.upload_period)
                    .unwrap_or(DEFAULT_DOWNLOAD_INTERVAL);

                // We advance next_download irrespective of errors: we don't want error cases to result in
                // expensive busy-polling.
                detail.next_download = Some(Instant::now() + period_jitter(period, 5));
            }
        }
    }

    async fn schedule(&mut self) -> SchedulingResult<PendingDownload> {
        let mut result = SchedulingResult {
            jobs: Vec::new(),
            want_interval: None,
        };

        // Step 1: identify some tenants that we may work on
        let mut tenants: Vec<Arc<SecondaryTenant>> = Vec::new();
        self.tenant_manager
            .foreach_secondary_tenants(|_id, secondary_state| {
                tenants.push(secondary_state.clone());
            });

        // Step 2: filter out tenants which are not yet elegible to run
        let now = Instant::now();
        result.jobs = tenants
            .into_iter()
            .filter_map(|secondary_tenant| {
                let (last_download, next_download) = {
                    let mut detail = secondary_tenant.detail.lock().unwrap();

                    if !detail.config.warm {
                        // Downloads are disabled for this tenant
                        detail.next_download = None;
                        return None;
                    }

                    if detail.next_download.is_none() {
                        // Initialize randomly in the range from 0 to our interval: this uniformly spreads the start times.  Subsequent
                        // rounds will use a smaller jitter to avoid accidentally synchronizing later.
                        detail.next_download = Some(now.checked_add(period_warmup(DEFAULT_DOWNLOAD_INTERVAL)).expect(
                        "Using our constant, which is known to be small compared with clock range",
                    ));
                    }
                    (detail.last_download.clone(), detail.next_download.unwrap())
                };

                if now > next_download {
                    Some(PendingDownload {
                        secondary_state: secondary_tenant,
                        last_download,
                        target_time: Some(next_download),
                    })
                } else {
                    None
                }
            })
            .collect();

        // Step 3: sort by target execution time to run most urgent first.
        result.jobs.sort_by_key(|j| j.target_time);

        result
    }

    fn on_command(
        &mut self,
        command: DownloadCommand,
    ) -> Result<PendingDownload, SecondaryTenantError> {
        let tenant_shard_id = command.get_tenant_shard_id();

        let tenant = self
            .tenant_manager
            .get_secondary_tenant_shard(*tenant_shard_id)
            .ok_or(GetTenantError::ShardNotFound(*tenant_shard_id))?;

        Ok(PendingDownload {
            target_time: None,
            last_download: None,
            secondary_state: tenant,
        })
    }

    fn spawn(
        &mut self,
        job: PendingDownload,
    ) -> (
        RunningDownload,
        Pin<Box<dyn Future<Output = CompleteDownload> + Send>>,
    ) {
        let PendingDownload {
            secondary_state,
            last_download,
            target_time,
        } = job;

        let (completion, barrier) = utils::completion::channel();
        let remote_storage = self.remote_storage.clone();
        let conf = self.tenant_manager.get_conf();
        let tenant_shard_id = *secondary_state.get_tenant_shard_id();
        let download_ctx = self.root_ctx.attached_child();
        (RunningDownload { barrier }, Box::pin(async move {
            let _completion = completion;

            let result = TenantDownloader::new(conf, &remote_storage, &secondary_state)
                .download(&download_ctx)
                .await;
            match &result
            {
                Err(UpdateError::NoData) => {
                    tracing::info!("No heatmap found for tenant.  This is fine if it is new.");
                },
                Err(UpdateError::NoSpace) => {
                    tracing::warn!("Insufficient space while downloading.  Will retry later.");
                }
                Err(UpdateError::Cancelled) => {
                    tracing::info!("Shut down while downloading");
                },
                Err(UpdateError::Deserialize(e)) => {
                    tracing::error!("Corrupt content while downloading tenant: {e}");
                },
                Err(e @ (UpdateError::DownloadError(_) | UpdateError::Other(_))) => {
                    tracing::error!("Error while downloading tenant: {e}");
                },
                Err(UpdateError::Restart) => {
                    tracing::info!("Download reached deadline & will restart to update heatmap")
                }
                Ok(()) => {}
            };

            // Irrespective of the result, we will reschedule ourselves to run after our usual period.

            // If the job had a target execution time, we may check our final execution
            // time against that for observability purposes.
            if let (Some(target_time), Some(last_download)) = (target_time, last_download) {
                // Elapsed time includes any scheduling lag as well as the execution of the job
                let elapsed = Instant::now().duration_since(target_time);

                warn_when_period_overrun(
                    elapsed,
                    last_download.upload_period,
                    BackgroundLoopKind::SecondaryDownload,
                );
            }

            CompleteDownload {
                secondary_state,
                completed_at: Instant::now(),
                result
            }
        }.instrument(info_span!(parent: None, "secondary_download", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()))))
    }
}

/// This type is a convenience to group together the various functions involved in
/// freshening a secondary tenant.
struct TenantDownloader<'a> {
    conf: &'static PageServerConf,
    remote_storage: &'a GenericRemoteStorage,
    secondary_state: &'a SecondaryTenant,
}

/// Errors that may be encountered while updating a tenant
#[derive(thiserror::Error, Debug)]
enum UpdateError {
    /// This is not a true failure, but it's how a download indicates that it would like to be restarted by
    /// the scheduler, to pick up the latest heatmap
    #[error("Reached deadline, restarting downloads")]
    Restart,

    #[error("No remote data found")]
    NoData,
    #[error("Insufficient local storage space")]
    NoSpace,
    #[error("Failed to download")]
    DownloadError(DownloadError),
    #[error(transparent)]
    Deserialize(#[from] serde_json::Error),
    #[error("Cancelled")]
    Cancelled,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<DownloadError> for UpdateError {
    fn from(value: DownloadError) -> Self {
        match &value {
            DownloadError::Cancelled => Self::Cancelled,
            DownloadError::NotFound => Self::NoData,
            _ => Self::DownloadError(value),
        }
    }
}

impl From<std::io::Error> for UpdateError {
    fn from(value: std::io::Error) -> Self {
        if let Some(nix::errno::Errno::ENOSPC) = value.raw_os_error().map(nix::errno::from_i32) {
            UpdateError::NoSpace
        } else if value
            .get_ref()
            .and_then(|x| x.downcast_ref::<DownloadError>())
            .is_some()
        {
            UpdateError::from(DownloadError::from(value))
        } else {
            // An I/O error from e.g. tokio::io::copy_buf is most likely a remote storage issue
            UpdateError::Other(anyhow::anyhow!(value))
        }
    }
}

impl<'a> TenantDownloader<'a> {
    fn new(
        conf: &'static PageServerConf,
        remote_storage: &'a GenericRemoteStorage,
        secondary_state: &'a SecondaryTenant,
    ) -> Self {
        Self {
            conf,
            remote_storage,
            secondary_state,
        }
    }

    async fn download(&self, ctx: &RequestContext) -> Result<(), UpdateError> {
        debug_assert_current_span_has_tenant_id();

        // For the duration of a download, we must hold the SecondaryTenant::gate, to ensure
        // cover our access to local storage.
        let Ok(_guard) = self.secondary_state.gate.enter() else {
            // Shutting down
            return Err(UpdateError::Cancelled);
        };

        let tenant_shard_id = self.secondary_state.get_tenant_shard_id();

        // We will use the etag from last successful download to make the download conditional on changes
        let last_download = self
            .secondary_state
            .detail
            .lock()
            .unwrap()
            .last_download
            .clone();

        // Download the tenant's heatmap
        let HeatMapModified {
            last_modified: heatmap_mtime,
            etag: heatmap_etag,
            bytes: heatmap_bytes,
        } = match tokio::select!(
            bytes = self.download_heatmap(last_download.as_ref().map(|d| &d.etag)) => {bytes?},
            _ = self.secondary_state.cancel.cancelled() => return Ok(())
        ) {
            HeatMapDownload::Unmodified => {
                tracing::info!("Heatmap unchanged since last successful download");
                return Ok(());
            }
            HeatMapDownload::Modified(m) => m,
        };

        let heatmap = serde_json::from_slice::<HeatMapTenant>(&heatmap_bytes)?;

        // Save the heatmap: this will be useful on restart, allowing us to reconstruct
        // layer metadata without having to re-download it.
        let heatmap_path = self.conf.tenant_heatmap_path(tenant_shard_id);

        let temp_path = path_with_suffix_extension(&heatmap_path, TEMP_FILE_SUFFIX);
        let context_msg = format!("write tenant {tenant_shard_id} heatmap to {heatmap_path}");
        let heatmap_path_bg = heatmap_path.clone();
        VirtualFile::crashsafe_overwrite(heatmap_path_bg, temp_path, heatmap_bytes)
            .await
            .maybe_fatal_err(&context_msg)?;

        tracing::debug!(
            "Wrote local heatmap to {}, with {} timelines",
            heatmap_path,
            heatmap.timelines.len()
        );

        // Get or initialize the local disk state for the timelines we will update
        let mut timeline_states = HashMap::new();
        for timeline in &heatmap.timelines {
            let timeline_state = self
                .secondary_state
                .detail
                .lock()
                .unwrap()
                .timelines
                .get(&timeline.timeline_id)
                .cloned();

            let timeline_state = match timeline_state {
                Some(t) => t,
                None => {
                    // We have no existing state: need to scan local disk for layers first.
                    let timeline_state = init_timeline_state(
                        self.conf,
                        tenant_shard_id,
                        timeline,
                        &self.secondary_state.resident_size_metric,
                    )
                    .await;

                    // Re-acquire detail lock now that we're done with async load from local FS
                    self.secondary_state
                        .detail
                        .lock()
                        .unwrap()
                        .timelines
                        .insert(timeline.timeline_id, timeline_state.clone());
                    timeline_state
                }
            };

            timeline_states.insert(timeline.timeline_id, timeline_state);
        }

        // Clean up any local layers that aren't in the heatmap.  We do this first for all timelines, on the general
        // principle that deletions should be done before writes wherever possible, and so that we can use this
        // phase to initialize our SecondaryProgress.
        {
            *self.secondary_state.progress.lock().unwrap() =
                self.prepare_timelines(&heatmap, heatmap_mtime).await?;
        }

        // Calculate a deadline for downloads: if downloading takes longer than this, it is useful to drop out and start again,
        // so that we are always using reasonably a fresh heatmap.  Otherwise, if we had really huge content to download, we might
        // spend 10s of minutes downloading layers we don't need.
        // (see https://github.com/neondatabase/neon/issues/8182)
        let deadline = {
            let period = self
                .secondary_state
                .detail
                .lock()
                .unwrap()
                .last_download
                .as_ref()
                .map(|d| d.upload_period)
                .unwrap_or(DEFAULT_DOWNLOAD_INTERVAL);

            // Use double the period: we are not promising to complete within the period, this is just a heuristic
            // to keep using a "reasonably fresh" heatmap.
            Instant::now() + period * 2
        };

        // Download the layers in the heatmap
        for timeline in heatmap.timelines {
            let timeline_state = timeline_states
                .remove(&timeline.timeline_id)
                .expect("Just populated above");

            if self.secondary_state.cancel.is_cancelled() {
                tracing::debug!(
                    "Cancelled before downloading timeline {}",
                    timeline.timeline_id
                );
                return Ok(());
            }

            let timeline_id = timeline.timeline_id;
            self.download_timeline(timeline, timeline_state, deadline, ctx)
                .instrument(tracing::info_span!(
                    "secondary_download_timeline",
                    tenant_id=%tenant_shard_id.tenant_id,
                    shard_id=%tenant_shard_id.shard_slug(),
                    %timeline_id
                ))
                .await?;
        }

        // Metrics consistency check in testing builds
        self.secondary_state.validate_metrics();
        // Only update last_etag after a full successful download: this way will not skip
        // the next download, even if the heatmap's actual etag is unchanged.
        self.secondary_state.detail.lock().unwrap().last_download = Some(DownloadSummary {
            etag: heatmap_etag,
            mtime: heatmap_mtime,
            upload_period: heatmap
                .upload_period_ms
                .map(|ms| Duration::from_millis(ms as u64))
                .unwrap_or(DEFAULT_DOWNLOAD_INTERVAL),
        });

        // Robustness: we should have updated progress properly, but in case we didn't, make sure
        // we don't leave the tenant in a state where we claim to have successfully downloaded
        // everything, but our progress is incomplete.  The invariant here should be that if
        // we have set `last_download` to this heatmap's etag, then the next time we see that
        // etag we can safely do no work (i.e. we must be complete).
        let mut progress = self.secondary_state.progress.lock().unwrap();
        debug_assert!(progress.layers_downloaded == progress.layers_total);
        debug_assert!(progress.bytes_downloaded == progress.bytes_total);
        if progress.layers_downloaded != progress.layers_total
            || progress.bytes_downloaded != progress.bytes_total
        {
            tracing::warn!("Correcting drift in progress stats ({progress:?})");
            progress.layers_downloaded = progress.layers_total;
            progress.bytes_downloaded = progress.bytes_total;
        }

        Ok(())
    }

    /// Do any fast local cleanup that comes before the much slower process of downloading
    /// layers from remote storage.  In the process, initialize the SecondaryProgress object
    /// that will later be updated incrementally as we download layers.
    async fn prepare_timelines(
        &self,
        heatmap: &HeatMapTenant,
        heatmap_mtime: SystemTime,
    ) -> Result<SecondaryProgress, UpdateError> {
        let heatmap_stats = heatmap.get_stats();
        // We will construct a progress object, and then populate its initial "downloaded" numbers
        // while iterating through local layer state in [`Self::prepare_timelines`]
        let mut progress = SecondaryProgress {
            layers_total: heatmap_stats.layers,
            bytes_total: heatmap_stats.bytes,
            heatmap_mtime: Some(serde_system_time::SystemTime(heatmap_mtime)),
            layers_downloaded: 0,
            bytes_downloaded: 0,
        };

        // Also expose heatmap bytes_total as a metric
        self.secondary_state
            .heatmap_total_size_metric
            .set(heatmap_stats.bytes);

        // Accumulate list of things to delete while holding the detail lock, for execution after dropping the lock
        let mut delete_layers = Vec::new();
        let mut delete_timelines = Vec::new();
        {
            let mut detail = self.secondary_state.detail.lock().unwrap();
            for (timeline_id, timeline_state) in &mut detail.timelines {
                let Some(heatmap_timeline_index) = heatmap
                    .timelines
                    .iter()
                    .position(|t| t.timeline_id == *timeline_id)
                else {
                    // This timeline is no longer referenced in the heatmap: delete it locally
                    delete_timelines.push(*timeline_id);
                    continue;
                };

                let heatmap_timeline = heatmap.timelines.get(heatmap_timeline_index).unwrap();

                let layers_in_heatmap = heatmap_timeline
                    .layers
                    .iter()
                    .map(|l| (&l.name, l.metadata.generation))
                    .collect::<HashSet<_>>();
                let layers_on_disk = timeline_state
                    .on_disk_layers
                    .iter()
                    .map(|l| (l.0, l.1.metadata.generation))
                    .collect::<HashSet<_>>();

                let mut layer_count = layers_on_disk.len();
                let mut layer_byte_count: u64 = timeline_state
                    .on_disk_layers
                    .values()
                    .map(|l| l.metadata.file_size)
                    .sum();

                // Remove on-disk layers that are no longer present in heatmap
                for (layer_file_name, generation) in layers_on_disk.difference(&layers_in_heatmap) {
                    layer_count -= 1;
                    layer_byte_count -= timeline_state
                        .on_disk_layers
                        .get(layer_file_name)
                        .unwrap()
                        .metadata
                        .file_size;

                    let local_path = local_layer_path(
                        self.conf,
                        self.secondary_state.get_tenant_shard_id(),
                        timeline_id,
                        layer_file_name,
                        generation,
                    );

                    delete_layers.push((*timeline_id, (*layer_file_name).clone(), local_path));
                }

                progress.bytes_downloaded += layer_byte_count;
                progress.layers_downloaded += layer_count;
            }

            for delete_timeline in &delete_timelines {
                // We haven't removed from disk yet, but optimistically remove from in-memory state: if removal
                // from disk fails that will be a fatal error.
                detail.remove_timeline(delete_timeline, &self.secondary_state.resident_size_metric);
            }
        }

        // Execute accumulated deletions
        for (timeline_id, layer_name, local_path) in delete_layers {
            tracing::info!(timeline_id=%timeline_id, "Removing secondary local layer {layer_name} because it's absent in heatmap",);

            tokio::fs::remove_file(&local_path)
                .await
                .or_else(fs_ext::ignore_not_found)
                .maybe_fatal_err("Removing secondary layer")?;

            // Update in-memory housekeeping to reflect the absence of the deleted layer
            let mut detail = self.secondary_state.detail.lock().unwrap();
            let Some(timeline_state) = detail.timelines.get_mut(&timeline_id) else {
                continue;
            };
            timeline_state.remove_layer(&layer_name, &self.secondary_state.resident_size_metric);
        }

        for timeline_id in delete_timelines {
            let timeline_path = self
                .conf
                .timeline_path(self.secondary_state.get_tenant_shard_id(), &timeline_id);
            tracing::info!(timeline_id=%timeline_id,
                "Timeline no longer in heatmap, removing from secondary location"
            );
            tokio::fs::remove_dir_all(&timeline_path)
                .await
                .or_else(fs_ext::ignore_not_found)
                .maybe_fatal_err("Removing secondary timeline")?;
        }

        Ok(progress)
    }

    /// Returns downloaded bytes if the etag differs from `prev_etag`, or None if the object
    /// still matches `prev_etag`.
    async fn download_heatmap(
        &self,
        prev_etag: Option<&Etag>,
    ) -> Result<HeatMapDownload, UpdateError> {
        debug_assert_current_span_has_tenant_id();
        let tenant_shard_id = self.secondary_state.get_tenant_shard_id();
        tracing::debug!("Downloading heatmap for secondary tenant",);

        let heatmap_path = remote_heatmap_path(tenant_shard_id);
        let cancel = &self.secondary_state.cancel;
        let opts = DownloadOpts {
            etag: prev_etag.cloned(),
            kind: DownloadKind::Small,
            ..Default::default()
        };

        backoff::retry(
            || async {
                let download = match self
                    .remote_storage
                    .download(&heatmap_path, &opts, cancel)
                    .await
                {
                    Ok(download) => download,
                    Err(DownloadError::Unmodified) => return Ok(HeatMapDownload::Unmodified),
                    Err(err) => return Err(err.into()),
                };

                let mut heatmap_bytes = Vec::new();
                let mut body = tokio_util::io::StreamReader::new(download.download_stream);
                let _size = tokio::io::copy_buf(&mut body, &mut heatmap_bytes).await?;
                Ok(HeatMapDownload::Modified(HeatMapModified {
                    etag: download.etag,
                    last_modified: download.last_modified,
                    bytes: heatmap_bytes,
                }))
            },
            |e| matches!(e, UpdateError::NoData | UpdateError::Cancelled),
            FAILED_DOWNLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "download heatmap",
            cancel,
        )
        .await
        .ok_or_else(|| UpdateError::Cancelled)
        .and_then(|x| x)
        .inspect(|_| SECONDARY_MODE.download_heatmap.inc())
    }

    /// Download heatmap layers that are not present on local disk, or update their
    /// access time if they are already present.
    async fn download_timeline_layers(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline: HeatMapTimeline,
        timeline_state: SecondaryDetailTimeline,
        deadline: Instant,
        ctx: &RequestContext,
    ) -> (Result<(), UpdateError>, Vec<HeatMapLayer>) {
        // Accumulate updates to the state
        let mut touched = Vec::new();

        for layer in timeline.layers {
            if self.secondary_state.cancel.is_cancelled() {
                tracing::debug!("Cancelled -- dropping out of layer loop");
                return (Err(UpdateError::Cancelled), touched);
            }

            if Instant::now() > deadline {
                // We've been running downloads for a while, restart to download latest heatmap.
                return (Err(UpdateError::Restart), touched);
            }

            // Existing on-disk layers: just update their access time.
            if let Some(on_disk) = timeline_state.on_disk_layers.get(&layer.name) {
                tracing::debug!("Layer {} is already on disk", layer.name);

                if cfg!(debug_assertions) {
                    // Debug for https://github.com/neondatabase/neon/issues/6966: check that the files we think
                    // are already present on disk are really there.
                    match tokio::fs::metadata(&on_disk.local_path).await {
                        Ok(meta) => {
                            tracing::debug!(
                                "Layer {} present at {}, size {}",
                                layer.name,
                                on_disk.local_path,
                                meta.len(),
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Layer {} not found at {} ({})",
                                layer.name,
                                on_disk.local_path,
                                e
                            );
                            debug_assert!(false);
                        }
                    }
                }

                if on_disk.metadata != layer.metadata || on_disk.access_time != layer.access_time {
                    // We already have this layer on disk.  Update its access time.
                    tracing::debug!(
                        "Access time updated for layer {}: {} -> {}",
                        layer.name,
                        strftime(&on_disk.access_time),
                        strftime(&layer.access_time)
                    );
                    touched.push(layer);
                }
                continue;
            } else {
                tracing::debug!("Layer {} not present on disk yet", layer.name);
            }

            // Eviction: if we evicted a layer, then do not re-download it unless it was accessed more
            // recently than it was evicted.
            if let Some(evicted_at) = timeline_state.evicted_at.get(&layer.name) {
                if &layer.access_time > evicted_at {
                    tracing::info!(
                        "Re-downloading evicted layer {}, accessed at {}, evicted at {}",
                        layer.name,
                        strftime(&layer.access_time),
                        strftime(evicted_at)
                    );
                } else {
                    tracing::trace!(
                        "Not re-downloading evicted layer {}, accessed at {}, evicted at {}",
                        layer.name,
                        strftime(&layer.access_time),
                        strftime(evicted_at)
                    );
                    self.skip_layer(layer);
                    continue;
                }
            }

            match self
                .download_layer(tenant_shard_id, &timeline.timeline_id, layer, ctx)
                .await
            {
                Ok(Some(layer)) => touched.push(layer),
                Ok(None) => {
                    // Not an error but we didn't download it: remote layer is missing.  Don't add it to the list of
                    // things to consider touched.
                }
                Err(e) => {
                    return (Err(e), touched);
                }
            }
        }

        (Ok(()), touched)
    }

    async fn download_timeline(
        &self,
        timeline: HeatMapTimeline,
        timeline_state: SecondaryDetailTimeline,
        deadline: Instant,
        ctx: &RequestContext,
    ) -> Result<(), UpdateError> {
        debug_assert_current_span_has_tenant_and_timeline_id();
        let tenant_shard_id = self.secondary_state.get_tenant_shard_id();
        let timeline_id = timeline.timeline_id;

        tracing::debug!(timeline_id=%timeline_id, "Downloading layers, {} in heatmap", timeline.layers.len());

        let (result, touched) = self
            .download_timeline_layers(tenant_shard_id, timeline, timeline_state, deadline, ctx)
            .await;

        // Write updates to state to record layers we just downloaded or touched, irrespective of whether the overall result was successful
        {
            let mut detail = self.secondary_state.detail.lock().unwrap();
            let timeline_detail = detail.timelines.entry(timeline_id).or_default();

            tracing::info!("Wrote timeline_detail for {} touched layers", touched.len());
            touched.into_iter().for_each(|t| {
                timeline_detail.touch_layer(
                    self.conf,
                    tenant_shard_id,
                    &timeline_id,
                    &t,
                    &self.secondary_state.resident_size_metric,
                    || {
                        local_layer_path(
                            self.conf,
                            tenant_shard_id,
                            &timeline_id,
                            &t.name,
                            &t.metadata.generation,
                        )
                    },
                )
            });
        }

        result
    }

    /// Call this during timeline download if a layer will _not_ be downloaded, to update progress statistics
    fn skip_layer(&self, layer: HeatMapLayer) {
        let mut progress = self.secondary_state.progress.lock().unwrap();
        progress.layers_total = progress.layers_total.saturating_sub(1);
        progress.bytes_total = progress
            .bytes_total
            .saturating_sub(layer.metadata.file_size);
    }

    async fn download_layer(
        &self,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
        layer: HeatMapLayer,
        ctx: &RequestContext,
    ) -> Result<Option<HeatMapLayer>, UpdateError> {
        // Failpoints for simulating slow remote storage
        failpoint_support::sleep_millis_async!(
            "secondary-layer-download-sleep",
            &self.secondary_state.cancel
        );

        pausable_failpoint!("secondary-layer-download-pausable");

        let local_path = local_layer_path(
            self.conf,
            tenant_shard_id,
            timeline_id,
            &layer.name,
            &layer.metadata.generation,
        );

        // Note: no backoff::retry wrapper here because download_layer_file does its own retries internally
        tracing::info!(
            "Starting download of layer {}, size {}",
            layer.name,
            layer.metadata.file_size
        );
        let downloaded_bytes = download_layer_file(
            self.conf,
            self.remote_storage,
            *tenant_shard_id,
            *timeline_id,
            &layer.name,
            &layer.metadata,
            &local_path,
            &self.secondary_state.gate,
            &self.secondary_state.cancel,
            ctx,
        )
        .await;

        let downloaded_bytes = match downloaded_bytes {
            Ok(bytes) => bytes,
            Err(DownloadError::NotFound) => {
                // A heatmap might be out of date and refer to a layer that doesn't exist any more.
                // This is harmless: continue to download the next layer. It is expected during compaction
                // GC.
                tracing::debug!(
                    "Skipped downloading missing layer {}, raced with compaction/gc?",
                    layer.name
                );
                self.skip_layer(layer);

                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        if downloaded_bytes != layer.metadata.file_size {
            let local_path = local_layer_path(
                self.conf,
                tenant_shard_id,
                timeline_id,
                &layer.name,
                &layer.metadata.generation,
            );

            tracing::warn!(
                "Downloaded layer {} with unexpected size {} != {}.  Removing download.",
                layer.name,
                downloaded_bytes,
                layer.metadata.file_size
            );

            tokio::fs::remove_file(&local_path)
                .await
                .or_else(fs_ext::ignore_not_found)?;
        } else {
            tracing::info!("Downloaded layer {}, size {}", layer.name, downloaded_bytes);
            let mut progress = self.secondary_state.progress.lock().unwrap();
            progress.bytes_downloaded += downloaded_bytes;
            progress.layers_downloaded += 1;
        }

        SECONDARY_MODE.download_layer.inc();

        Ok(Some(layer))
    }
}

/// Scan local storage and build up Layer objects based on the metadata in a HeatMapTimeline
async fn init_timeline_state(
    conf: &'static PageServerConf,
    tenant_shard_id: &TenantShardId,
    heatmap: &HeatMapTimeline,
    resident_metric: &UIntGauge,
) -> SecondaryDetailTimeline {
    let timeline_path = conf.timeline_path(tenant_shard_id, &heatmap.timeline_id);
    let mut detail = SecondaryDetailTimeline::default();

    let mut dir = match tokio::fs::read_dir(&timeline_path).await {
        Ok(d) => d,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                let context = format!("Creating timeline directory {timeline_path}");
                tracing::info!("{}", context);
                tokio::fs::create_dir_all(&timeline_path)
                    .await
                    .fatal_err(&context);

                // No entries to report: drop out.
                return detail;
            } else {
                on_fatal_io_error(&e, &format!("Reading timeline dir {timeline_path}"));
            }
        }
    };

    // As we iterate through layers found on disk, we will look up their metadata from this map.
    // Layers not present in metadata will be discarded.
    let heatmap_metadata: HashMap<&LayerName, &HeatMapLayer> =
        heatmap.layers.iter().map(|l| (&l.name, l)).collect();

    while let Some(dentry) = dir
        .next_entry()
        .await
        .fatal_err(&format!("Listing {timeline_path}"))
    {
        let Ok(file_path) = Utf8PathBuf::from_path_buf(dentry.path()) else {
            tracing::warn!("Malformed filename at {}", dentry.path().to_string_lossy());
            continue;
        };
        let local_meta = dentry
            .metadata()
            .await
            .fatal_err(&format!("Read metadata on {}", file_path));

        let file_name = file_path.file_name().expect("created it from the dentry");
        if crate::is_temporary(&file_path)
            || is_temp_download_file(&file_path)
            || is_ephemeral_file(file_name)
        {
            // Temporary files are frequently left behind from restarting during downloads
            tracing::info!("Cleaning up temporary file {file_path}");
            if let Err(e) = tokio::fs::remove_file(&file_path)
                .await
                .or_else(fs_ext::ignore_not_found)
            {
                tracing::error!("Failed to remove temporary file {file_path}: {e}");
            }
            continue;
        }

        match LayerName::from_str(file_name) {
            Ok(name) => {
                let remote_meta = heatmap_metadata.get(&name);
                match remote_meta {
                    Some(remote_meta) => {
                        // TODO: checksums for layers (https://github.com/neondatabase/neon/issues/2784)
                        if local_meta.len() != remote_meta.metadata.file_size {
                            // This should not happen, because we do crashsafe write-then-rename when downloading
                            // layers, and layers in remote storage are immutable.  Remove the local file because
                            // we cannot trust it.
                            tracing::warn!(
                                "Removing local layer {name} with unexpected local size {} != {}",
                                local_meta.len(),
                                remote_meta.metadata.file_size
                            );
                        } else {
                            // We expect the access time to be initialized immediately afterwards, when
                            // the latest heatmap is applied to the state.
                            detail.touch_layer(
                                conf,
                                tenant_shard_id,
                                &heatmap.timeline_id,
                                remote_meta,
                                resident_metric,
                                || file_path,
                            );
                        }
                    }
                    None => {
                        // FIXME: consider some optimization when transitioning from attached to secondary: maybe
                        // wait until we have seen a heatmap that is more recent than the most recent on-disk state?  Otherwise
                        // we will end up deleting any layers which were created+uploaded more recently than the heatmap.
                        tracing::info!(
                            "Removing secondary local layer {} because it's absent in heatmap",
                            name
                        );
                        tokio::fs::remove_file(&dentry.path())
                            .await
                            .or_else(fs_ext::ignore_not_found)
                            .fatal_err(&format!(
                                "Removing layer {}",
                                dentry.path().to_string_lossy()
                            ));
                    }
                }
            }
            Err(_) => {
                // Ignore it.
                tracing::warn!("Unexpected file in timeline directory: {file_name}");
            }
        }
    }

    detail
}
