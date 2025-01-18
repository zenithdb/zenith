mod downloader;
pub mod heatmap;
mod heatmap_uploader;
mod scheduler;

use std::{sync::Arc, time::SystemTime};

use crate::{
    context::RequestContext,
    disk_usage_eviction_task::DiskUsageEvictionInfo,
    metrics::SECONDARY_HEATMAP_TOTAL_SIZE,
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
};

use self::{
    downloader::{downloader_task, SecondaryDetail},
    heatmap_uploader::heatmap_uploader_task,
};

use super::{
    config::{SecondaryLocationConfig, TenantConfOpt},
    mgr::TenantShardManager,
    span::debug_assert_current_span_has_tenant_id,
    storage_layer::LayerName,
    GetTenantError,
};

use crate::metrics::SECONDARY_RESIDENT_PHYSICAL_SIZE;
use metrics::UIntGauge;
use pageserver_api::{
    models,
    shard::{ShardIdentity, TenantShardId},
};
use remote_storage::GenericRemoteStorage;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use utils::{completion::Barrier, id::TimelineId, sync::gate::Gate};

enum DownloadCommand {
    Download(TenantShardId),
}
enum UploadCommand {
    Upload(TenantShardId),
}

impl UploadCommand {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        match self {
            Self::Upload(id) => id,
        }
    }
}

impl DownloadCommand {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        match self {
            Self::Download(id) => id,
        }
    }
}

struct CommandRequest<T> {
    payload: T,
    response_tx: tokio::sync::oneshot::Sender<CommandResponse>,
}

struct CommandResponse {
    result: Result<(), SecondaryTenantError>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum SecondaryTenantError {
    #[error("{0}")]
    GetTenant(GetTenantError),
    #[error("shutting down")]
    ShuttingDown,
}

impl From<GetTenantError> for SecondaryTenantError {
    fn from(gte: GetTenantError) -> Self {
        Self::GetTenant(gte)
    }
}

// Whereas [`Tenant`] represents an attached tenant, this type represents the work
// we do for secondary tenant locations: where we are not serving clients or
// ingesting WAL, but we are maintaining a warm cache of layer files.
//
// This type is all about the _download_ path for secondary mode.  The upload path
// runs separately (see [`heatmap_uploader`]) while a regular attached `Tenant` exists.
//
// This structure coordinates TenantManager and SecondaryDownloader,
// so that the downloader can indicate which tenants it is currently
// operating on, and the manager can indicate when a particular
// secondary tenant should cancel any work in flight.
#[derive(Debug)]
pub(crate) struct SecondaryTenant {
    /// Carrying a tenant shard ID simplifies callers such as the downloader
    /// which need to organize many of these objects by ID.
    tenant_shard_id: TenantShardId,

    /// Cancellation token indicates to SecondaryDownloader that it should stop doing
    /// any work for this tenant at the next opportunity.
    pub(crate) cancel: CancellationToken,

    pub(crate) gate: Gate,

    // Secondary mode does not need the full shard identity or the TenantConfOpt.  However,
    // storing these enables us to report our full LocationConf, enabling convenient reconciliation
    // by the control plane (see [`Self::get_location_conf`])
    shard_identity: ShardIdentity,
    tenant_conf: std::sync::Mutex<TenantConfOpt>,

    // Internal state used by the Downloader.
    detail: std::sync::Mutex<SecondaryDetail>,

    // Public state indicating overall progress of downloads relative to the last heatmap seen
    pub(crate) progress: std::sync::Mutex<models::SecondaryProgress>,

    // Sum of layer sizes on local disk
    pub(super) resident_size_metric: UIntGauge,

    // Sum of layer sizes in the most recently downloaded heatmap
    pub(super) heatmap_total_size_metric: UIntGauge,
}

impl SecondaryTenant {
    pub(crate) fn new(
        tenant_shard_id: TenantShardId,
        shard_identity: ShardIdentity,
        tenant_conf: TenantConfOpt,
        config: &SecondaryLocationConfig,
    ) -> Arc<Self> {
        let tenant_id = tenant_shard_id.tenant_id.to_string();
        let shard_id = format!("{}", tenant_shard_id.shard_slug());
        let resident_size_metric = SECONDARY_RESIDENT_PHYSICAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id])
            .unwrap();

        let heatmap_total_size_metric = SECONDARY_HEATMAP_TOTAL_SIZE
            .get_metric_with_label_values(&[&tenant_id, &shard_id])
            .unwrap();

        Arc::new(Self {
            tenant_shard_id,
            // todo: shall we make this a descendent of the
            // main cancellation token, or is it sufficient that
            // on shutdown we walk the tenants and fire their
            // individual cancellations?
            cancel: CancellationToken::new(),
            gate: Gate::default(),

            shard_identity,
            tenant_conf: std::sync::Mutex::new(tenant_conf),

            detail: std::sync::Mutex::new(SecondaryDetail::new(config.clone())),

            progress: std::sync::Mutex::default(),

            resident_size_metric,
            heatmap_total_size_metric,
        })
    }

    pub(crate) fn tenant_shard_id(&self) -> TenantShardId {
        self.tenant_shard_id
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel.cancel();

        // Wait for any secondary downloader work to complete
        self.gate.close().await;

        self.validate_metrics();

        let tenant_id = self.tenant_shard_id.tenant_id.to_string();
        let shard_id = format!("{}", self.tenant_shard_id.shard_slug());
        let _ = SECONDARY_RESIDENT_PHYSICAL_SIZE.remove_label_values(&[&tenant_id, &shard_id]);
        let _ = SECONDARY_HEATMAP_TOTAL_SIZE.remove_label_values(&[&tenant_id, &shard_id]);
    }

    pub(crate) fn set_config(&self, config: &SecondaryLocationConfig) {
        self.detail.lock().unwrap().config = config.clone();
    }

    pub(crate) fn set_tenant_conf(&self, config: &TenantConfOpt) {
        *(self.tenant_conf.lock().unwrap()) = config.clone();
    }

    /// For API access: generate a LocationConfig equivalent to the one that would be used to
    /// create a Tenant in the same state.  Do not use this in hot paths: it's for relatively
    /// rare external API calls, like a reconciliation at startup.
    pub(crate) fn get_location_conf(&self) -> models::LocationConfig {
        let conf = self.detail.lock().unwrap().config.clone();

        let conf = models::LocationConfigSecondary { warm: conf.warm };

        let tenant_conf = self.tenant_conf.lock().unwrap().clone();
        models::LocationConfig {
            mode: models::LocationConfigMode::Secondary,
            generation: None,
            secondary_conf: Some(conf),
            shard_number: self.tenant_shard_id.shard_number.0,
            shard_count: self.tenant_shard_id.shard_count.literal(),
            shard_stripe_size: self.shard_identity.stripe_size.0,
            tenant_conf: tenant_conf.into(),
        }
    }

    pub(crate) fn get_tenant_shard_id(&self) -> &TenantShardId {
        &self.tenant_shard_id
    }

    pub(crate) fn get_layers_for_eviction(self: &Arc<Self>) -> (DiskUsageEvictionInfo, usize) {
        self.detail.lock().unwrap().get_layers_for_eviction(self)
    }

    /// Cancellation safe, but on cancellation the eviction will go through
    #[instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%timeline_id, name=%name))]
    pub(crate) async fn evict_layer(self: &Arc<Self>, timeline_id: TimelineId, name: LayerName) {
        debug_assert_current_span_has_tenant_id();

        let guard = match self.gate.enter() {
            Ok(g) => g,
            Err(_) => {
                tracing::debug!("Dropping layer evictions, secondary tenant shutting down",);
                return;
            }
        };

        let now = SystemTime::now();
        tracing::info!("Evicting secondary layer");

        let this = self.clone();

        // spawn it to be cancellation safe
        tokio::task::spawn_blocking(move || {
            let _guard = guard;

            // Update the timeline's state.  This does not have to be synchronized with
            // the download process, because:
            // - If downloader is racing with us to remove a file (e.g. because it is
            //   removed from heatmap), then our mutual .remove() operations will both
            //   succeed.
            // - If downloader is racing with us to download the object (this would require
            //   multiple eviction iterations to race with multiple download iterations), then
            //   if we remove it from the state, the worst that happens is the downloader
            //   downloads it again before re-inserting, or we delete the file but it remains
            //   in the state map (in which case it will be downloaded if this secondary
            //   tenant transitions to attached and tries to access it)
            //
            // The important assumption here is that the secondary timeline state does not
            // have to 100% match what is on disk, because it's a best-effort warming
            // of the cache.
            let mut detail = this.detail.lock().unwrap();
            if let Some(removed) =
                detail.evict_layer(name, &timeline_id, now, &this.resident_size_metric)
            {
                // We might race with removal of the same layer during downloads, so finding the layer we
                // were trying to remove is optional.  Only issue the disk I/O to remove it if we found it.
                removed.remove_blocking();
            }
        })
        .await
        .expect("secondary eviction should not have panicked");
    }

    /// Exhaustive check that incrementally updated metrics match the actual state.
    #[cfg(feature = "testing")]
    fn validate_metrics(&self) {
        let detail = self.detail.lock().unwrap();
        let resident_size = detail.total_resident_size();

        assert_eq!(resident_size, self.resident_size_metric.get());
    }

    #[cfg(not(feature = "testing"))]
    fn validate_metrics(&self) {
        // No-op in non-testing builds
    }
}

/// The SecondaryController is a pseudo-rpc client for administrative control of secondary mode downloads,
/// and heatmap uploads.  This is not a hot data path: it's used for:
/// - Live migrations, where we want to ensure a migration destination has the freshest possible
///   content before trying to cut over.
/// - Tests, where we want to immediately upload/download for a particular tenant.
///
/// In normal operations, outside of migrations, uploads & downloads are autonomous and not driven by this interface.
pub struct SecondaryController {
    upload_req_tx: tokio::sync::mpsc::Sender<CommandRequest<UploadCommand>>,
    download_req_tx: tokio::sync::mpsc::Sender<CommandRequest<DownloadCommand>>,
}

impl SecondaryController {
    async fn dispatch<T>(
        &self,
        queue: &tokio::sync::mpsc::Sender<CommandRequest<T>>,
        payload: T,
    ) -> Result<(), SecondaryTenantError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        queue
            .send(CommandRequest {
                payload,
                response_tx,
            })
            .await
            .map_err(|_| SecondaryTenantError::ShuttingDown)?;

        let response = response_rx
            .await
            .map_err(|_| SecondaryTenantError::ShuttingDown)?;

        response.result
    }

    pub(crate) async fn upload_tenant(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<(), SecondaryTenantError> {
        self.dispatch(&self.upload_req_tx, UploadCommand::Upload(tenant_shard_id))
            .await
    }
    pub(crate) async fn download_tenant(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> Result<(), SecondaryTenantError> {
        self.dispatch(
            &self.download_req_tx,
            DownloadCommand::Download(tenant_shard_id),
        )
        .await
    }
}

pub struct GlobalTasks {
    cancel: CancellationToken,
    uploader: JoinHandle<()>,
    downloader: JoinHandle<()>,
}

impl GlobalTasks {
    /// Caller is responsible for requesting shutdown via the cancellation token that was
    /// passed to [`spawn_tasks`].
    ///
    /// # Panics
    ///
    /// This method panics if that token is not cancelled.
    /// This is low-risk because we're calling this during process shutdown, so, a panic
    /// will be informative but not cause undue downtime.
    pub async fn wait(self) {
        let Self {
            cancel,
            uploader,
            downloader,
        } = self;
        assert!(
            cancel.is_cancelled(),
            "must cancel cancellation token, otherwise the tasks will not shut down"
        );

        let (uploader, downloader) = futures::future::join(uploader, downloader).await;
        uploader.expect(
            "unreachable: exit_on_panic_or_error would catch the panic and exit the process",
        );
        downloader.expect(
            "unreachable: exit_on_panic_or_error would catch the panic and exit the process",
        );
    }
}

pub fn spawn_tasks(
    tenant_manager: Arc<TenantShardManager>,
    remote_storage: GenericRemoteStorage,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> (SecondaryController, GlobalTasks) {
    let mgr_clone = tenant_manager.clone();
    let storage_clone = remote_storage.clone();
    let bg_jobs_clone = background_jobs_can_start.clone();

    let (download_req_tx, download_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<DownloadCommand>>(16);
    let (upload_req_tx, upload_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<UploadCommand>>(16);

    let cancel_clone = cancel.clone();
    let downloader = BACKGROUND_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
        "secondary tenant downloads",
        async move {
            downloader_task(
                mgr_clone,
                storage_clone,
                download_req_rx,
                bg_jobs_clone,
                cancel_clone,
                RequestContext::new(
                    TaskKind::SecondaryDownloads,
                    crate::context::DownloadBehavior::Download,
                ),
            )
            .await;
            anyhow::Ok(())
        },
    ));

    let cancel_clone = cancel.clone();
    let uploader = BACKGROUND_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
        "heatmap uploads",
        async move {
            heatmap_uploader_task(
                tenant_manager,
                remote_storage,
                upload_req_rx,
                background_jobs_can_start,
                cancel_clone,
            )
            .await;
            anyhow::Ok(())
        },
    ));

    (
        SecondaryController {
            upload_req_tx,
            download_req_tx,
        },
        GlobalTasks {
            cancel,
            uploader,
            downloader,
        },
    )
}
