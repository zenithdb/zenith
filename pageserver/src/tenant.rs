//!
//! Timeline repository implementation that keeps old data in files on disk, and
//! the recent changes in memory. See tenant/*_layer.rs files.
//! The functions here are responsible for locating the correct layer for the
//! get/put call, walking back the timeline branching history as needed.
//!
//! The files are stored in the .neon/tenants/<tenant_id>/timelines/<timeline_id>
//! directory. See docs/pageserver-storage.md for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{bail, ensure, Context};
use futures::FutureExt;
use pageserver_api::models::TimelineState;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use storage_broker::BrokerClientChannel;
use tokio::sync::watch;
use tokio::sync::OwnedMutexGuard;
use tokio::task::JoinSet;
use tracing::*;
use utils::completion;
use utils::crashsafe::path_with_suffix_extension;

use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::ops::Bound::Included;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use self::config::TenantConf;
use self::metadata::TimelineMetadata;
use self::remote_timeline_client::RemoteTimelineClient;
use self::timeline::EvictionTaskTenantState;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::import_datadir;
use crate::is_uninit_mark;
use crate::metrics::{remove_tenant_metrics, TENANT_STATE_METRIC, TENANT_SYNTHETIC_SIZE_METRIC};
use crate::repository::GcResult;
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::metadata::load_metadata;
use crate::tenant::remote_timeline_client::index::{IndexPart, LayerFileMetadata};
use crate::tenant::remote_timeline_client::{
    MaybeDeletedIndexPart, PersistIndexPartWithDeletedFlagError,
};
use crate::tenant::storage_layer::{DeltaLayer, ImageLayer, Layer, LayerFileName};
use crate::InitializationOrder;

use crate::virtual_file::VirtualFile;
use crate::walredo::PostgresRedoManager;
use crate::walredo::WalRedoManager;
use crate::TEMP_FILE_SUFFIX;
pub use pageserver_api::models::TenantState;

use toml_edit;
use utils::{
    crashsafe,
    id::{TenantId, TimelineId},
    lsn::{Lsn, RecordLsn},
};

pub mod blob_io;
pub mod block_io;
pub mod disk_btree;
pub(crate) mod ephemeral_file;
pub mod layer_map;
pub mod manifest;

pub mod metadata;
mod par_fsync;
mod remote_timeline_client;
pub mod storage_layer;

pub mod config;
pub mod mgr;
pub mod tasks;
pub mod upload_queue;

mod timeline;

pub mod size;

pub(crate) use timeline::debug_assert_current_span_has_tenant_and_timeline_id;
pub use timeline::{
    LocalLayerInfoForDiskUsageEviction, LogicalSizeCalculationCause, PageReconstructError, Timeline,
};

// re-export this function so that page_cache.rs can use it.
pub use crate::tenant::ephemeral_file::writeback as writeback_ephemeral_file;

// re-export for use in storage_sync.rs
pub use crate::tenant::metadata::save_metadata;

// re-export for use in walreceiver
pub use crate::tenant::timeline::WalReceiverInfo;

/// Parts of the `.neon/tenants/<tenant_id>/timelines/<timeline_id>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

pub const TENANT_ATTACHING_MARKER_FILENAME: &str = "attaching";

///
/// Tenant consists of multiple timelines. Keep them in a hash table.
///
pub struct Tenant {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

    /// The value creation timestamp, used to measure activation delay, see:
    /// <https://github.com/neondatabase/neon/issues/4025>
    loading_started_at: Instant,

    state: watch::Sender<TenantState>,

    // Overridden tenant-specific config parameters.
    // We keep TenantConfOpt sturct here to preserve the information
    // about parameters that are not set.
    // This is necessary to allow global config updates.
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    tenant_id: TenantId,
    timelines: Mutex<HashMap<TimelineId, Arc<Timeline>>>,
    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: tokio::sync::Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,

    // provides access to timeline data sitting in the remote storage
    remote_storage: Option<GenericRemoteStorage>,

    // for cross-region replication: provide access to master S3 bucket
    master_storage: Option<GenericRemoteStorage>,

    /// Cached logical sizes updated updated on each [`Tenant::gather_size_inputs`].
    cached_logical_sizes: tokio::sync::Mutex<HashMap<(TimelineId, Lsn), u64>>,
    cached_synthetic_tenant_size: Arc<AtomicU64>,

    eviction_task_tenant_state: tokio::sync::Mutex<EvictionTaskTenantState>,
}

/// A timeline with some of its files on disk, being initialized.
/// This struct ensures the atomicity of the timeline init: it's either properly created and inserted into pageserver's memory, or
/// its local files are removed. In the worst case of a crash, an uninit mark file is left behind, which causes the directory
/// to be removed on next restart.
///
/// The caller is responsible for proper timeline data filling before the final init.
#[must_use]
pub struct UninitializedTimeline<'t> {
    owning_tenant: &'t Tenant,
    timeline_id: TimelineId,
    raw_timeline: Option<(Arc<Timeline>, TimelineUninitMark)>,
}

/// An uninit mark file, created along the timeline dir to ensure the timeline either gets fully initialized and loaded into pageserver's memory,
/// or gets removed eventually.
///
/// XXX: it's important to create it near the timeline dir, not inside it to ensure timeline dir gets removed first.
#[must_use]
struct TimelineUninitMark {
    uninit_mark_deleted: bool,
    uninit_mark_path: PathBuf,
    timeline_path: PathBuf,
}

impl UninitializedTimeline<'_> {
    /// Finish timeline creation: insert it into the Tenant's timelines map and remove the
    /// uninit mark file.
    ///
    /// This function launches the flush loop if not already done.
    ///
    /// The caller is responsible for activating the timeline (function `.activate()`).
    fn finish_creation(mut self) -> anyhow::Result<Arc<Timeline>> {
        let timeline_id = self.timeline_id;
        let tenant_id = self.owning_tenant.tenant_id;

        let (new_timeline, uninit_mark) = self.raw_timeline.take().with_context(|| {
            format!("No timeline for initalization found for {tenant_id}/{timeline_id}")
        })?;

        // Check that the caller initialized disk_consistent_lsn
        let new_disk_consistent_lsn = new_timeline.get_disk_consistent_lsn();
        ensure!(
            new_disk_consistent_lsn.is_valid(),
            "new timeline {tenant_id}/{timeline_id} has invalid disk_consistent_lsn"
        );

        let mut timelines = self.owning_tenant.timelines.lock().unwrap();
        match timelines.entry(timeline_id) {
            Entry::Occupied(_) => anyhow::bail!(
                "Found freshly initialized timeline {tenant_id}/{timeline_id} in the tenant map"
            ),
            Entry::Vacant(v) => {
                uninit_mark.remove_uninit_mark().with_context(|| {
                    format!(
                        "Failed to remove uninit mark file for timeline {tenant_id}/{timeline_id}"
                    )
                })?;
                v.insert(Arc::clone(&new_timeline));

                new_timeline.maybe_spawn_flush_loop();
            }
        }

        Ok(new_timeline)
    }

    /// Prepares timeline data by loading it from the basebackup archive.
    pub async fn import_basebackup_from_tar(
        self,
        copyin_read: &mut (impl tokio::io::AsyncRead + Send + Sync + Unpin),
        base_lsn: Lsn,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let raw_timeline = self.raw_timeline()?;

        import_datadir::import_basebackup_from_tar(raw_timeline, copyin_read, base_lsn, ctx)
            .await
            .context("Failed to import basebackup")?;

        // Flush the new layer files to disk, before we make the timeline as available to
        // the outside world.
        //
        // Flush loop needs to be spawned in order to be able to flush.
        raw_timeline.maybe_spawn_flush_loop();

        fail::fail_point!("before-checkpoint-new-timeline", |_| {
            bail!("failpoint before-checkpoint-new-timeline");
        });

        raw_timeline
            .freeze_and_flush()
            .await
            .context("Failed to flush after basebackup import")?;

        // All the data has been imported. Insert the Timeline into the tenant's timelines
        // map and remove the uninit mark file.
        let tl = self.finish_creation()?;
        tl.activate(broker_client, None, ctx);
        Ok(tl)
    }

    fn raw_timeline(&self) -> anyhow::Result<&Arc<Timeline>> {
        Ok(&self
            .raw_timeline
            .as_ref()
            .with_context(|| {
                format!(
                    "No raw timeline {}/{} found",
                    self.owning_tenant.tenant_id, self.timeline_id
                )
            })?
            .0)
    }
}

impl Drop for UninitializedTimeline<'_> {
    fn drop(&mut self) {
        if let Some((_, uninit_mark)) = self.raw_timeline.take() {
            let _entered = info_span!("drop_uninitialized_timeline", tenant = %self.owning_tenant.tenant_id, timeline = %self.timeline_id).entered();
            error!("Timeline got dropped without initializing, cleaning its files");
            cleanup_timeline_directory(uninit_mark);
        }
    }
}

fn cleanup_timeline_directory(uninit_mark: TimelineUninitMark) {
    let timeline_path = &uninit_mark.timeline_path;
    match ignore_absent_files(|| fs::remove_dir_all(timeline_path)) {
        Ok(()) => {
            info!("Timeline dir {timeline_path:?} removed successfully, removing the uninit mark")
        }
        Err(e) => {
            error!("Failed to clean up uninitialized timeline directory {timeline_path:?}: {e:?}")
        }
    }
    drop(uninit_mark); // mark handles its deletion on drop, gets retained if timeline dir exists
}

impl TimelineUninitMark {
    fn new(uninit_mark_path: PathBuf, timeline_path: PathBuf) -> Self {
        Self {
            uninit_mark_deleted: false,
            uninit_mark_path,
            timeline_path,
        }
    }

    fn remove_uninit_mark(mut self) -> anyhow::Result<()> {
        if !self.uninit_mark_deleted {
            self.delete_mark_file_if_present()?;
        }

        Ok(())
    }

    fn delete_mark_file_if_present(&mut self) -> anyhow::Result<()> {
        let uninit_mark_file = &self.uninit_mark_path;
        let uninit_mark_parent = uninit_mark_file
            .parent()
            .with_context(|| format!("Uninit mark file {uninit_mark_file:?} has no parent"))?;
        ignore_absent_files(|| fs::remove_file(uninit_mark_file)).with_context(|| {
            format!("Failed to remove uninit mark file at path {uninit_mark_file:?}")
        })?;
        crashsafe::fsync(uninit_mark_parent).context("Failed to fsync uninit mark parent")?;
        self.uninit_mark_deleted = true;

        Ok(())
    }
}

impl Drop for TimelineUninitMark {
    fn drop(&mut self) {
        if !self.uninit_mark_deleted {
            if self.timeline_path.exists() {
                error!(
                    "Uninit mark {} is not removed, timeline {} stays uninitialized",
                    self.uninit_mark_path.display(),
                    self.timeline_path.display()
                )
            } else {
                // unblock later timeline creation attempts
                warn!(
                    "Removing intermediate uninit mark file {}",
                    self.uninit_mark_path.display()
                );
                if let Err(e) = self.delete_mark_file_if_present() {
                    error!("Failed to remove the uninit mark file: {e}")
                }
            }
        }
    }
}

// We should not blindly overwrite local metadata with remote one.
// For example, consider the following case:
//     Image layer is flushed to disk as a new delta layer, we update local metadata and start upload task but after that
//     pageserver crashes. During startup we'll load new metadata, and then reset it
//     to the state of remote one. But current layermap will have layers from the old
//     metadata which is inconsistent.
//     And with current logic it wont disgard them during load because during layermap
//     load it sees local disk consistent lsn which is ahead of layer lsns.
//     If we treat remote as source of truth we need to completely sync with it,
//     i e delete local files which are missing on the remote. This will add extra work,
//     wal for these layers needs to be reingested for example
//
// So the solution is to take remote metadata only when we're attaching.
pub fn merge_local_remote_metadata<'a>(
    local: Option<&'a TimelineMetadata>,
    remote: Option<&'a TimelineMetadata>,
) -> anyhow::Result<(&'a TimelineMetadata, bool)> {
    match (local, remote) {
        (None, None) => anyhow::bail!("we should have either local metadata or remote"),
        (Some(local), None) => Ok((local, true)),
        // happens if we crash during attach, before writing out the metadata file
        (None, Some(remote)) => Ok((remote, false)),
        // This is the regular case where we crash/exit before finishing queued uploads.
        // Also, it happens if we crash during attach after writing the metadata file
        // but before removing the attaching marker file.
        (Some(local), Some(remote)) => {
            let consistent_lsn_cmp = local
                .disk_consistent_lsn()
                .cmp(&remote.disk_consistent_lsn());
            let gc_cutoff_lsn_cmp = local
                .latest_gc_cutoff_lsn()
                .cmp(&remote.latest_gc_cutoff_lsn());
            use std::cmp::Ordering::*;
            match (consistent_lsn_cmp, gc_cutoff_lsn_cmp) {
                // It wouldn't matter, but pick the local one so that we don't rewrite the metadata file.
                (Equal, Equal) => Ok((local, true)),
                // Local state is clearly ahead of the remote.
                (Greater, Greater) => Ok((local, true)),
                // We have local layer files that aren't on the remote, but GC horizon is on par.
                (Greater, Equal) => Ok((local, true)),
                // Local GC started running but we couldn't sync it to the remote.
                (Equal, Greater) => Ok((local, true)),

                // We always update the local value first, so something else must have
                // updated the remote value, probably a different pageserver.
                // The control plane is supposed to prevent this from happening.
                // Bail out.
                (Less, Less)
                | (Less, Equal)
                | (Equal, Less)
                | (Less, Greater)
                | (Greater, Less) => {
                    anyhow::bail!(
                        r#"remote metadata appears to be ahead of local metadata:
local:
  {local:#?}
remote:
  {remote:#?}
"#
                    );
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GetTimelineError {
    #[error("Timeline {tenant_id}/{timeline_id} is not active, state: {state:?}")]
    NotActive {
        tenant_id: TenantId,
        timeline_id: TimelineId,
        state: TimelineState,
    },
    #[error("Timeline {tenant_id}/{timeline_id} was not found")]
    NotFound {
        tenant_id: TenantId,
        timeline_id: TimelineId,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum DeleteTimelineError {
    #[error("NotFound")]
    NotFound,
    #[error("HasChildren")]
    HasChildren(Vec<TimelineId>),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub enum SetStoppingError {
    AlreadyStopping,
    Broken,
}

struct RemoteStartupData {
    index_part: IndexPart,
    remote_metadata: TimelineMetadata,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WaitToBecomeActiveError {
    WillNotBecomeActive {
        tenant_id: TenantId,
        state: TenantState,
    },
    TenantDropped {
        tenant_id: TenantId,
    },
}

impl std::fmt::Display for WaitToBecomeActiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WaitToBecomeActiveError::WillNotBecomeActive { tenant_id, state } => {
                write!(
                    f,
                    "Tenant {} will not become active. Current state: {:?}",
                    tenant_id, state
                )
            }
            WaitToBecomeActiveError::TenantDropped { tenant_id } => {
                write!(f, "Tenant {tenant_id} will not become active (dropped)")
            }
        }
    }
}

pub(crate) enum ShutdownError {
    AlreadyStopping,
}

struct DeletionGuard(OwnedMutexGuard<bool>);

impl DeletionGuard {
    fn is_deleted(&self) -> bool {
        *self.0
    }
}

impl Tenant {
    /// Yet another helper for timeline initialization.
    /// Contains the common part of `load_local_timeline` and `load_remote_timeline`.
    ///
    /// - Initializes the Timeline struct and inserts it into the tenant's hash map
    /// - Scans the local timeline directory for layer files and builds the layer map
    /// - Downloads remote index file and adds remote files to the layer map
    /// - Schedules remote upload tasks for any files that are present locally but missing from remote storage.
    ///
    /// If the operation fails, the timeline is left in the tenant's hash map in Broken state. On success,
    /// it is marked as Active.
    #[allow(clippy::too_many_arguments)]
    async fn timeline_init_and_sync(
        &self,
        timeline_id: TimelineId,
        remote_client: Option<RemoteTimelineClient>,
        remote_startup_data: Option<RemoteStartupData>,
        local_metadata: Option<TimelineMetadata>,
        ancestor: Option<Arc<Timeline>>,
        first_save: bool,
        init_order: Option<&InitializationOrder>,
        _ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let tenant_id = self.tenant_id;

        let (up_to_date_metadata, picked_local) = merge_local_remote_metadata(
            local_metadata.as_ref(),
            remote_startup_data.as_ref().map(|r| &r.remote_metadata),
        )
        .context("merge_local_remote_metadata")?
        .to_owned();

        let timeline = self.create_timeline_struct(
            timeline_id,
            up_to_date_metadata,
            ancestor.clone(),
            remote_client,
            init_order,
        )?;
        let new_disk_consistent_lsn = timeline.get_disk_consistent_lsn();
        anyhow::ensure!(
            new_disk_consistent_lsn.is_valid(),
            "Timeline {tenant_id}/{timeline_id} has invalid disk_consistent_lsn"
        );
        timeline
            .load_layer_map(new_disk_consistent_lsn)
            .await
            .with_context(|| {
                format!("Failed to load layermap for timeline {tenant_id}/{timeline_id}")
            })?;

        {
            // avoiding holding it across awaits
            let mut timelines_accessor = self.timelines.lock().unwrap();
            match timelines_accessor.entry(timeline_id) {
                Entry::Occupied(_) => {
                    // The uninit mark file acts as a lock that prevents another task from
                    // initializing the timeline at the same time.
                    unreachable!(
                        "Timeline {tenant_id}/{timeline_id} already exists in the tenant map"
                    );
                }
                Entry::Vacant(v) => {
                    v.insert(Arc::clone(&timeline));
                    timeline.maybe_spawn_flush_loop();
                }
            }
        };

        if self.remote_storage.is_some() {
            // Reconcile local state with remote storage, downloading anything that's
            // missing locally, and scheduling uploads for anything that's missing
            // in remote storage.
            timeline
                .reconcile_with_remote(
                    up_to_date_metadata,
                    remote_startup_data.as_ref().map(|r| &r.index_part),
                )
                .await
                .context("failed to reconcile with remote")?
        }

        // Sanity check: a timeline should have some content.
        anyhow::ensure!(
            ancestor.is_some()
                || timeline
                    .layers
                    .read()
                    .await
                    .iter_historic_layers()
                    .next()
                    .is_some(),
            "Timeline has no ancestor and no layer files"
        );

        // Save the metadata file to local disk.
        if !picked_local {
            save_metadata(
                self.conf,
                timeline_id,
                tenant_id,
                up_to_date_metadata,
                first_save,
            )
            .context("save_metadata")?;
        }

        Ok(())
    }

    ///
    /// Attach a tenant that's available in cloud storage.
    ///
    /// This returns quickly, after just creating the in-memory object
    /// Tenant struct and launching a background task to download
    /// the remote index files.  On return, the tenant is most likely still in
    /// Attaching state, and it will become Active once the background task
    /// finishes. You can use wait_until_active() to wait for the task to
    /// complete.
    ///
    pub(crate) fn spawn_attach(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        broker_client: storage_broker::BrokerClientChannel,
        remote_storage: GenericRemoteStorage,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Tenant>> {
        // TODO dedup with spawn_load
        let tenant_conf =
            Self::load_tenant_config(conf, tenant_id).context("load tenant config")?;

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let tenant = Arc::new(Tenant::new(
            TenantState::Attaching,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            Some(remote_storage),
        ));

        // Do all the hard work in the background
        let tenant_clone = Arc::clone(&tenant);

        let ctx = ctx.detached_child(TaskKind::Attach, DownloadBehavior::Warn);
        task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::Attach,
            Some(tenant_id),
            None,
            "attach tenant",
            false,
            async move {
                match tenant_clone.attach(&ctx).await {
                    Ok(()) => {
                        info!("attach finished, activating");
                        tenant_clone.activate(broker_client, None, &ctx)?;
                    }
                    Err(e) => {
                        error!("attach failed, setting tenant state to Broken: {:?}", e);
                        tenant_clone.state.send_modify(|state| {
                            assert_eq!(*state, TenantState::Attaching, "the attach task owns the tenant state until activation is complete");
                            *state = TenantState::broken_from_reason(e.to_string());
                        });
                    }
                }
                Ok(())
            }
            .instrument({
                let span = tracing::info_span!(parent: None, "attach", tenant_id=%tenant_id);
                span.follows_from(Span::current());
                span
            }),
        );
        Ok(tenant)
    }

    ///
    /// Background task that downloads all data for a tenant and brings it to Active state.
    ///
    /// No background tasks are started as part of this routine.
    ///
    async fn attach(self: &Arc<Tenant>, ctx: &RequestContext) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        let marker_file = self.conf.tenant_attaching_mark_file_path(&self.tenant_id);
        if !tokio::fs::try_exists(&marker_file)
            .await
            .context("check for existence of marker file")?
        {
            anyhow::bail!(
                "implementation error: marker file should exist at beginning of this function"
            );
        }

        // Get list of remote timelines
        // download index files for every tenant timeline
        info!("listing remote timelines");

        let remote_storage = self
            .remote_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("cannot attach without remote storage"))?;

        let remote_timeline_ids = remote_timeline_client::list_remote_timelines(
            remote_storage,
            self.conf,
            self.tenant_id,
        )
        .await?;

        info!("found {} timelines", remote_timeline_ids.len());

        // Download & parse index parts
        let mut part_downloads = JoinSet::new();
        for timeline_id in remote_timeline_ids {
            let client = RemoteTimelineClient::new(
                remote_storage.clone(),
                self.conf,
                self.tenant_id,
                timeline_id,
            );
            part_downloads.spawn(
                async move {
                    debug!("starting index part download");

                    let index_part = client
                        .download_index_file()
                        .await
                        .context("download index file")?;

                    debug!("finished index part download");

                    Result::<_, anyhow::Error>::Ok((timeline_id, client, index_part))
                }
                .map(move |res| {
                    res.with_context(|| format!("download index part for timeline {timeline_id}"))
                })
                .instrument(info_span!("download_index_part", timeline=%timeline_id)),
            );
        }
        // Wait for all the download tasks to complete & collect results.
        let mut remote_index_and_client = HashMap::new();
        let mut timeline_ancestors = HashMap::new();
        while let Some(result) = part_downloads.join_next().await {
            // NB: we already added timeline_id as context to the error
            let result: Result<_, anyhow::Error> = result.context("joinset task join")?;
            let (timeline_id, client, index_part) = result?;
            debug!("successfully downloaded index part for timeline {timeline_id}");
            match index_part {
                MaybeDeletedIndexPart::IndexPart(index_part) => {
                    timeline_ancestors.insert(
                        timeline_id,
                        index_part.parse_metadata().context("parse_metadata")?,
                    );
                    remote_index_and_client.insert(timeline_id, (index_part, client));
                }
                MaybeDeletedIndexPart::Deleted(_) => {
                    info!("timeline {} is deleted, skipping", timeline_id);
                    continue;
                }
            }
        }

        // For every timeline, download the metadata file, scan the local directory,
        // and build a layer map that contains an entry for each remote and local
        // layer file.
        let sorted_timelines = tree_sort_timelines(timeline_ancestors)?;
        for (timeline_id, remote_metadata) in sorted_timelines {
            let (index_part, remote_client) = remote_index_and_client
                .remove(&timeline_id)
                .expect("just put it in above");

            // TODO again handle early failure
            self.load_remote_timeline(timeline_id, index_part, remote_metadata, remote_client, ctx)
                .await
                .with_context(|| {
                    format!(
                        "failed to load remote timeline {} for tenant {}",
                        timeline_id, self.tenant_id
                    )
                })?;
        }

        std::fs::remove_file(&marker_file)
            .with_context(|| format!("unlink attach marker file {}", marker_file.display()))?;
        crashsafe::fsync(marker_file.parent().expect("marker file has parent dir"))
            .context("fsync tenant directory after unlinking attach marker file")?;

        utils::failpoint_sleep_millis_async!("attach-before-activate");

        info!("Done");

        Ok(())
    }

    /// get size of all remote timelines
    ///
    /// This function relies on the index_part instead of listing the remote storage
    ///
    pub async fn get_remote_size(&self) -> anyhow::Result<u64> {
        let mut size = 0;

        for timeline in self.list_timelines().iter() {
            if let Some(remote_client) = &timeline.remote_client {
                size += remote_client.get_remote_physical_size();
            }
        }

        Ok(size)
    }

    #[instrument(skip_all, fields(timeline_id=%timeline_id))]
    async fn load_remote_timeline(
        &self,
        timeline_id: TimelineId,
        index_part: IndexPart,
        remote_metadata: TimelineMetadata,
        remote_client: RemoteTimelineClient,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        info!("downloading index file for timeline {}", timeline_id);
        tokio::fs::create_dir_all(self.conf.timeline_path(&timeline_id, &self.tenant_id))
            .await
            .context("Failed to create new timeline directory")?;

        let ancestor = if let Some(ancestor_id) = remote_metadata.ancestor_timeline() {
            let timelines = self.timelines.lock().unwrap();
            Some(Arc::clone(timelines.get(&ancestor_id).ok_or_else(
                || {
                    anyhow::anyhow!(
                        "cannot find ancestor timeline {ancestor_id} for timeline {timeline_id}"
                    )
                },
            )?))
        } else {
            None
        };

        // Even if there is local metadata it cannot be ahead of the remote one
        // since we're attaching. Even if we resume interrupted attach remote one
        // cannot be older than the local one
        let local_metadata = None;

        self.timeline_init_and_sync(
            timeline_id,
            Some(remote_client),
            Some(RemoteStartupData {
                index_part,
                remote_metadata,
            }),
            local_metadata,
            ancestor,
            true,
            None,
            ctx,
        )
        .await
    }

    /// Create a placeholder Tenant object for a broken tenant
    pub fn create_broken_tenant(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        reason: String,
    ) -> Arc<Tenant> {
        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        Arc::new(Tenant::new(
            TenantState::Broken {
                reason,
                backtrace: String::new(),
            },
            conf,
            TenantConfOpt::default(),
            wal_redo_manager,
            tenant_id,
            None,
        ))
    }

    /// Load a tenant that's available on local disk
    ///
    /// This is used at pageserver startup, to rebuild the in-memory
    /// structures from on-disk state. This is similar to attaching a tenant,
    /// but the index files already exist on local disk, as well as some layer
    /// files.
    ///
    /// If the loading fails for some reason, the Tenant will go into Broken
    /// state.
    #[instrument(skip_all, fields(tenant_id=%tenant_id))]
    pub fn spawn_load(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        broker_client: storage_broker::BrokerClientChannel,
        remote_storage: Option<GenericRemoteStorage>,
        init_order: Option<InitializationOrder>,
        ctx: &RequestContext,
    ) -> Arc<Tenant> {
        debug_assert_current_span_has_tenant_id();

        let tenant_conf = match Self::load_tenant_config(conf, tenant_id) {
            Ok(conf) => conf,
            Err(e) => {
                error!("load tenant config failed: {:?}", e);
                return Tenant::create_broken_tenant(conf, tenant_id, format!("{e:#}"));
            }
        };

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let tenant = Tenant::new(
            TenantState::Loading,
            conf,
            tenant_conf,
            wal_redo_manager,
            tenant_id,
            remote_storage,
        );
        let tenant = Arc::new(tenant);

        // Do all the hard work in a background task
        let tenant_clone = Arc::clone(&tenant);

        let ctx = ctx.detached_child(TaskKind::InitialLoad, DownloadBehavior::Warn);
        let _ = task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::InitialLoad,
            Some(tenant_id),
            None,
            "initial tenant load",
            false,
            async move {
                let mut init_order = init_order;

                // take the completion because initial tenant loading will complete when all of
                // these tasks complete.
                let _completion = init_order.as_mut().and_then(|x| x.initial_tenant_load.take());

                match tenant_clone.load(init_order.as_ref(), &ctx).await {
                    Ok(()) => {
                        debug!("load finished, activating");
                        let background_jobs_can_start = init_order.as_ref().map(|x| &x.background_jobs_can_start);
                        tenant_clone.activate(broker_client, background_jobs_can_start, &ctx)?;
                    }
                    Err(err) => {
                        error!("load failed, setting tenant state to Broken: {err:?}");
                        tenant_clone.state.send_modify(|state| {
                            assert_eq!(*state, TenantState::Loading, "the loading task owns the tenant state until activation is complete");
                            *state = TenantState::broken_from_reason(err.to_string());
                        });
                    }
                }
               Ok(())
            }
            .instrument({
                let span = tracing::info_span!(parent: None, "load", tenant_id=%tenant_id);
                span.follows_from(Span::current());
                span
            }),
        );

        tenant
    }

    pub fn scan_and_sort_timelines_dir(
        self: Arc<Tenant>,
    ) -> anyhow::Result<Vec<(TimelineId, TimelineMetadata)>> {
        let timelines_dir = self.conf.timelines_path(&self.tenant_id);
        let mut timelines_to_load: HashMap<TimelineId, TimelineMetadata> = HashMap::new();

        for entry in
            std::fs::read_dir(&timelines_dir).context("list timelines directory for tenant")?
        {
            let entry = entry.context("read timeline dir entry")?;
            let timeline_dir = entry.path();

            if crate::is_temporary(&timeline_dir) {
                info!(
                    "Found temporary timeline directory, removing: {}",
                    timeline_dir.display()
                );
                if let Err(e) = std::fs::remove_dir_all(&timeline_dir) {
                    error!(
                        "Failed to remove temporary directory '{}': {:?}",
                        timeline_dir.display(),
                        e
                    );
                }
            } else if is_uninit_mark(&timeline_dir) {
                if !timeline_dir.exists() {
                    warn!(
                        "Timeline dir entry become invalid: {}",
                        timeline_dir.display()
                    );
                    continue;
                }
                let timeline_uninit_mark_file = &timeline_dir;
                info!(
                    "Found an uninit mark file {}, removing the timeline and its uninit mark",
                    timeline_uninit_mark_file.display()
                );
                let timeline_id = timeline_uninit_mark_file
                    .file_stem()
                    .and_then(OsStr::to_str)
                    .unwrap_or_default()
                    .parse::<TimelineId>()
                    .with_context(|| {
                        format!(
                            "Could not parse timeline id out of the timeline uninit mark name {}",
                            timeline_uninit_mark_file.display()
                        )
                    })?;
                let timeline_dir = self.conf.timeline_path(&timeline_id, &self.tenant_id);
                if let Err(e) =
                    remove_timeline_and_uninit_mark(&timeline_dir, timeline_uninit_mark_file)
                {
                    error!("Failed to clean up uninit marked timeline: {e:?}");
                }
            } else {
                if !timeline_dir.exists() {
                    warn!(
                        "Timeline dir entry become invalid: {}",
                        timeline_dir.display()
                    );
                    continue;
                }
                let timeline_id = timeline_dir
                    .file_name()
                    .and_then(OsStr::to_str)
                    .unwrap_or_default()
                    .parse::<TimelineId>()
                    .with_context(|| {
                        format!(
                            "Could not parse timeline id out of the timeline dir name {}",
                            timeline_dir.display()
                        )
                    })?;
                let timeline_uninit_mark_file = self
                    .conf
                    .timeline_uninit_mark_file_path(self.tenant_id, timeline_id);
                if timeline_uninit_mark_file.exists() {
                    info!(
                        %timeline_id,
                        "Found an uninit mark file, removing the timeline and its uninit mark",
                    );
                    if let Err(e) =
                        remove_timeline_and_uninit_mark(&timeline_dir, &timeline_uninit_mark_file)
                    {
                        error!("Failed to clean up uninit marked timeline: {e:?}");
                    }
                    continue;
                }

                let file_name = entry.file_name();
                if let Ok(timeline_id) =
                    file_name.to_str().unwrap_or_default().parse::<TimelineId>()
                {
                    let metadata = load_metadata(self.conf, timeline_id, self.tenant_id)
                        .context("failed to load metadata")?;
                    timelines_to_load.insert(timeline_id, metadata);
                } else {
                    // A file or directory that doesn't look like a timeline ID
                    warn!(
                        "unexpected file or directory in timelines directory: {}",
                        file_name.to_string_lossy()
                    );
                }
            }
        }

        // Sort the array of timeline IDs into tree-order, so that parent comes before
        // all its children.
        tree_sort_timelines(timelines_to_load)
    }

    ///
    /// Background task to load in-memory data structures for this tenant, from
    /// files on disk. Used at pageserver startup.
    ///
    /// No background tasks are started as part of this routine.
    async fn load(
        self: &Arc<Tenant>,
        init_order: Option<&InitializationOrder>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        debug!("loading tenant task");

        utils::failpoint_sleep_millis_async!("before-loading-tenant");

        // Load in-memory state to reflect the local files on disk
        //
        // Scan the directory, peek into the metadata file of each timeline, and
        // collect a list of timelines and their ancestors.
        let span = info_span!("blocking");
        let cloned = Arc::clone(self);

        let sorted_timelines: Vec<(_, _)> = tokio::task::spawn_blocking(move || {
            let _g = span.entered();
            cloned.scan_and_sort_timelines_dir()
        })
        .await
        .context("load spawn_blocking")
        .and_then(|res| res)?;

        // FIXME original collect_timeline_files contained one more check:
        //    1. "Timeline has no ancestor and no layer files"

        for (timeline_id, local_metadata) in sorted_timelines {
            self.load_local_timeline(timeline_id, local_metadata, init_order, ctx)
                .await
                .with_context(|| format!("load local timeline {timeline_id}"))?;
        }

        trace!("Done");

        Ok(())
    }

    /// Subroutine of `load_tenant`, to load an individual timeline
    ///
    /// NB: The parent is assumed to be already loaded!
    #[instrument(skip(self, local_metadata, init_order, ctx))]
    async fn load_local_timeline(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        local_metadata: TimelineMetadata,
        init_order: Option<&InitializationOrder>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        let remote_client = self.remote_storage.as_ref().map(|remote_storage| {
            RemoteTimelineClient::new(
                remote_storage.clone(),
                self.conf,
                self.tenant_id,
                timeline_id,
            )
        });

        let ancestor = if let Some(ancestor_timeline_id) = local_metadata.ancestor_timeline() {
            let ancestor_timeline = self.get_timeline(ancestor_timeline_id, false)
                .with_context(|| anyhow::anyhow!("cannot find ancestor timeline {ancestor_timeline_id} for timeline {timeline_id}"))?;
            Some(ancestor_timeline)
        } else {
            None
        };

        let (remote_startup_data, remote_client) = match remote_client {
            Some(remote_client) => match remote_client.download_index_file().await {
                Ok(index_part) => {
                    let index_part = match index_part {
                        MaybeDeletedIndexPart::IndexPart(index_part) => index_part,
                        MaybeDeletedIndexPart::Deleted(index_part) => {
                            // TODO: we won't reach here if remote storage gets de-configured after start of the deletion operation.
                            // Example:
                            //  start deletion operation
                            //  finishes upload of index part
                            //  pageserver crashes
                            //  remote storage gets de-configured
                            //  pageserver starts
                            //
                            // We don't really anticipate remote storage to be de-configured, so, for now, this is fine.
                            // Also, maybe we'll remove that option entirely in the future, see https://github.com/neondatabase/neon/issues/4099.
                            info!("is_deleted is set on remote, resuming removal of timeline data originally done by timeline deletion handler");

                            remote_client
                                .init_upload_queue_stopped_to_continue_deletion(&index_part)?;

                            let timeline = self
                                .create_timeline_struct(
                                    timeline_id,
                                    &local_metadata,
                                    ancestor,
                                    Some(remote_client),
                                    init_order,
                                )
                                .context("create_timeline_struct")?;

                            let guard = DeletionGuard(
                                Arc::clone(&timeline.delete_lock)
                                    .try_lock_owned()
                                    .expect("cannot happen because we're the only owner"),
                            );

                            // Note: here we even skip populating layer map. Timeline is essentially uninitialized.
                            // RemoteTimelineClient is the only functioning part.
                            timeline.set_state(TimelineState::Stopping);
                            // We meed to do this because when console retries delete request we shouldnt answer with 404
                            // because 404 means successful deletion.
                            // FIXME consider TimelineState::Deleting.
                            let mut locked = self.timelines.lock().unwrap();
                            locked.insert(timeline_id, Arc::clone(&timeline));

                            Tenant::schedule_delete_timeline(
                                Arc::clone(self),
                                timeline_id,
                                timeline,
                                guard,
                            );

                            return Ok(());
                        }
                    };

                    let remote_metadata = index_part.parse_metadata().context("parse_metadata")?;
                    (
                        Some(RemoteStartupData {
                            index_part,
                            remote_metadata,
                        }),
                        Some(remote_client),
                    )
                }
                Err(DownloadError::NotFound) => {
                    info!("no index file was found on the remote");
                    (None, Some(remote_client))
                }
                Err(e) => return Err(anyhow::anyhow!(e)),
            },
            None => (None, remote_client),
        };

        self.timeline_init_and_sync(
            timeline_id,
            remote_client,
            remote_startup_data,
            Some(local_metadata),
            ancestor,
            false,
            init_order,
            ctx,
        )
        .await
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Get Timeline handle for given Neon timeline ID.
    /// This function is idempotent. It doesn't change internal state in any way.
    pub fn get_timeline(
        &self,
        timeline_id: TimelineId,
        active_only: bool,
    ) -> Result<Arc<Timeline>, GetTimelineError> {
        let timelines_accessor = self.timelines.lock().unwrap();
        let timeline = timelines_accessor
            .get(&timeline_id)
            .ok_or(GetTimelineError::NotFound {
                tenant_id: self.tenant_id,
                timeline_id,
            })?;

        if active_only && !timeline.is_active() {
            Err(GetTimelineError::NotActive {
                tenant_id: self.tenant_id,
                timeline_id,
                state: timeline.current_state(),
            })
        } else {
            Ok(Arc::clone(timeline))
        }
    }

    /// Lists timelines the tenant contains.
    /// Up to tenant's implementation to omit certain timelines that ar not considered ready for use.
    pub fn list_timelines(&self) -> Vec<Arc<Timeline>> {
        self.timelines
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect()
    }

    /// This is used to create the initial 'main' timeline during bootstrapping,
    /// or when importing a new base backup. The caller is expected to load an
    /// initial image of the datadir to the new timeline after this.
    ///
    /// Until that happens, the on-disk state is invalid (disk_consistent_lsn=Lsn(0))
    /// and the timeline will fail to load at a restart.
    ///
    /// That's why we add an uninit mark file, and wrap it together witht the Timeline
    /// in-memory object into UninitializedTimeline.
    /// Once the caller is done setting up the timeline, they should call
    /// `UninitializedTimeline::initialize_with_lock` to remove the uninit mark.
    ///
    /// For tests, use `DatadirModification::init_empty_test_timeline` + `commit` to setup the
    /// minimum amount of keys required to get a writable timeline.
    /// (Without it, `put` might fail due to `repartition` failing.)
    pub fn create_empty_timeline(
        &self,
        new_timeline_id: TimelineId,
        initdb_lsn: Lsn,
        pg_version: u32,
        _ctx: &RequestContext,
    ) -> anyhow::Result<UninitializedTimeline> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot create empty timelines on inactive tenant"
        );

        let timelines = self.timelines.lock().unwrap();
        let timeline_uninit_mark = self.create_timeline_uninit_mark(new_timeline_id, &timelines)?;
        drop(timelines);

        let new_metadata = TimelineMetadata::new(
            // Initialize disk_consistent LSN to 0, The caller must import some data to
            // make it valid, before calling finish_creation()
            Lsn(0),
            None,
            None,
            Lsn(0),
            initdb_lsn,
            initdb_lsn,
            pg_version,
            None,
        );
        self.prepare_new_timeline(
            new_timeline_id,
            &new_metadata,
            timeline_uninit_mark,
            initdb_lsn,
            None,
        )
    }

    /// Helper for unit tests to create an emtpy timeline.
    ///
    /// The timeline is has state value `Active` but its background loops are not running.
    // This makes the various functions which anyhow::ensure! for Active state work in tests.
    // Our current tests don't need the background loops.
    #[cfg(test)]
    pub async fn create_test_timeline(
        &self,
        new_timeline_id: TimelineId,
        initdb_lsn: Lsn,
        pg_version: u32,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let uninit_tl = self.create_empty_timeline(new_timeline_id, initdb_lsn, pg_version, ctx)?;
        let tline = uninit_tl.raw_timeline().expect("we just created it");
        assert_eq!(tline.get_last_record_lsn(), Lsn(0));

        // Setup minimum keys required for the timeline to be usable.
        let mut modification = tline.begin_modification(initdb_lsn);
        modification
            .init_empty_test_timeline()
            .context("init_empty_test_timeline")?;
        modification
            .commit()
            .await
            .context("commit init_empty_test_timeline modification")?;

        // Flush to disk so that uninit_tl's check for valid disk_consistent_lsn passes.
        tline.maybe_spawn_flush_loop();
        tline.freeze_and_flush().await.context("freeze_and_flush")?;

        let tl = uninit_tl.finish_creation()?;
        // The non-test code would call tl.activate() here.
        tl.set_state(TimelineState::Active);
        Ok(tl)
    }

    pub async fn create_timeline_replica(
        &self,
        timeline_id: TimelineId,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // We need to connect to broker in master's region to choose safekeeper to subscribe
        let master_broker_endpoint = self
            .tenant_conf
            .read()
            .unwrap()
            .master_broker_endpoint
            .as_ref()
            .unwrap()
            .clone();
        let broker_client =
            storage_broker::connect(master_broker_endpoint, self.conf.broker_keepalive_interval)?;

        // Access to S3 bucket in master's region
        let master_storage = self
            .master_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("master storage not specified"))?;
        let master_client = RemoteTimelineClient::new(
            master_storage.clone(),
            self.conf,
            self.tenant_id,
            timeline_id,
        );

        // Access to local S3 bucket in this region
        let remote_storage = self
            .master_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("remote storage not specified"))?;
        let remote_client = RemoteTimelineClient::new(
            remote_storage.clone(),
            self.conf,
            self.tenant_id,
            timeline_id,
        );

        // Get list of all timelines from master. We actually do not need all - only
        // ancestors of the target timeline.
        let remote_timeline_ids = remote_timeline_client::list_remote_timelines(
            master_storage,
            self.conf,
            self.tenant_id,
        )
        .await?;

        // Download & parse index parts
        let mut part_downloads = JoinSet::new();

        for timeline_id in remote_timeline_ids {
            let client = RemoteTimelineClient::new(
                master_storage.clone(),
                self.conf,
                self.tenant_id,
                timeline_id,
            );
            part_downloads.spawn(
                async move {
                    debug!("starting index part download");

                    let index_part = client
                        .download_index_file()
                        .await
                        .context("download index file")?;

                    debug!("finished index part download");

                    Result::<_, anyhow::Error>::Ok((timeline_id, client, index_part))
                }
                .map(move |res| {
                    res.with_context(|| format!("download index part for timeline {timeline_id}"))
                })
                .instrument(info_span!("download_index_part", timeline=%timeline_id)),
            );
        }
        // Wait for all the download tasks to complete & collect results.
        let mut remote_index_and_client = HashMap::new();
        while let Some(result) = part_downloads.join_next().await {
            // NB: we already added timeline_id as context to the error
            let result: Result<_, anyhow::Error> = result.context("joinset task join")?;
            let (timeline_id, client, index_part) = result?;
            debug!("successfully downloaded index part for timeline {timeline_id}");
            match index_part {
                MaybeDeletedIndexPart::IndexPart(index_part) => {
                    remote_index_and_client.insert(timeline_id, (index_part, client));
                }
                MaybeDeletedIndexPart::Deleted(_) => {
                    info!("timeline {} is deleted, skipping", timeline_id);
                    continue;
                }
            }
        }

        let (index_part, _client) = remote_index_and_client
            .get(&timeline_id)
            .expect("timeline found at master");
        let mut timeline_metadata = index_part.parse_metadata().context("parse_metadata")?;

        // Convert IndexLayerMetadata to LayerFileMetadata
        let mut layer_metadata: HashMap<LayerFileName, LayerFileMetadata> = index_part
            .layer_metadata
            .iter()
            .map(|(fname, meta)| (fname.clone(), LayerFileMetadata::from(meta)))
            .collect();

        // Let replic_lsn be the largest end LSN
        let replica_lsn = layer_metadata
            .keys()
            .map(|fname| fname.get_lsn_range().end)
            .max()
            .unwrap_or(timeline_metadata.ancestor_lsn());

        let old_metadata = timeline_metadata.clone();

        // Now collect layers of ancestor branches. We do not want to reconstruct exact branch
        // hierarhy at replica, because in this case we need to maintain several timelines.
        // Instead of it we just collect all layers which may be required for the current timeline.
        while let Some(ancestor_id) = timeline_metadata.ancestor_timeline() {
            let (index_part, _client) = remote_index_and_client
                .get(&ancestor_id)
                .expect("timeline found at master");
            for (fname, meta) in &index_part.layer_metadata {
                if fname.get_lsn_range().start < timeline_metadata.ancestor_lsn() {
                    layer_metadata.insert(fname.clone(), LayerFileMetadata::from(meta));
                }
            }
            timeline_metadata = index_part.parse_metadata().context("parse_metadata")?;
        }
        let new_metadata = TimelineMetadata::new(
            old_metadata.disk_consistent_lsn(),
            old_metadata.prev_record_lsn(),
            None,
            Lsn::INVALID,
            old_metadata.latest_gc_cutoff_lsn(),
            old_metadata.initdb_lsn(),
            old_metadata.pg_version(),
            Some(replica_lsn),
        );

        // Initialize data directories for new timeline
        tokio::fs::create_dir_all(self.conf.timeline_path(&timeline_id, &self.tenant_id))
            .await
            .context("Failed to create new timeline directory")?;

        // Save timeline metadata
        save_metadata(self.conf, timeline_id, self.tenant_id, &new_metadata, true)
            .context("Failed to create timeline metadata")?;

        // construct new index_part.json with combined list of layers
        let index_part = IndexPart::new(
            layer_metadata,
            old_metadata.disk_consistent_lsn(),
            new_metadata.to_bytes()?,
        );

        remote_client.init_upload_queue(&index_part)?;

        let timeline = Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            &new_metadata,
            None, // we do not need to restore branches hierarhy at replica
            timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            Some(remote_client),
            Some(master_client),
            Some(replica_lsn),
            old_metadata.pg_version(),
            None, // no need to calcuate logical size at replica
            None,
        );

        // Wait completion of index part upload */
        timeline
            .remote_client
            .as_ref()
            .unwrap()
            .wait_completion()
            .await
            .context("wait for index part upload to complete")?;

        /* Do we need to perform explicit upload?
        // Upload this index_part.json to S3 bucket
        upload_index_part(
            self.conf,
            &remote_storage,
            self.tenant_id,
            timeline_id,
            &index_part,
        )
        .await?;
         */

        timeline
            .create_remote_layers(
                &index_part,
                HashMap::new(), // no local layers
                replica_lsn,
            )
            .await?;

        // Start background works for this timeline
        timeline.activate(broker_client, None, ctx);

        Ok(())
    }

    /// Create a new timeline.
    ///
    /// Returns the new timeline ID and reference to its Timeline object.
    ///
    /// If the caller specified the timeline ID to use (`new_timeline_id`), and timeline with
    /// the same timeline ID already exists, returns None. If `new_timeline_id` is not given,
    /// a new unique ID is generated.
    pub async fn create_timeline(
        &self,
        new_timeline_id: TimelineId,
        ancestor_timeline_id: Option<TimelineId>,
        mut ancestor_start_lsn: Option<Lsn>,
        pg_version: u32,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> anyhow::Result<Option<Arc<Timeline>>> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot create timelines on inactive tenant"
        );

        if let Ok(existing) = self.get_timeline(new_timeline_id, false) {
            debug!("timeline {new_timeline_id} already exists");

            if let Some(remote_client) = existing.remote_client.as_ref() {
                // Wait for uploads to complete, so that when we return Ok, the timeline
                // is known to be durable on remote storage. Just like we do at the end of
                // this function, after we have created the timeline ourselves.
                //
                // We only really care that the initial version of `index_part.json` has
                // been uploaded. That's enough to remember that the timeline
                // exists. However, there is no function to wait specifically for that so
                // we just wait for all in-progress uploads to finish.
                remote_client
                    .wait_completion()
                    .await
                    .context("wait for timeline uploads to complete")?;
            }

            return Ok(None);
        }

        let loaded_timeline = match ancestor_timeline_id {
            Some(ancestor_timeline_id) => {
                let ancestor_timeline = self
                    .get_timeline(ancestor_timeline_id, false)
                    .context("Cannot branch off the timeline that's not present in pageserver")?;

                if let Some(lsn) = ancestor_start_lsn.as_mut() {
                    *lsn = lsn.align();

                    let ancestor_ancestor_lsn = ancestor_timeline.get_ancestor_lsn();
                    if ancestor_ancestor_lsn > *lsn {
                        // can we safely just branch from the ancestor instead?
                        bail!(
                            "invalid start lsn {} for ancestor timeline {}: less than timeline ancestor lsn {}",
                            lsn,
                            ancestor_timeline_id,
                            ancestor_ancestor_lsn,
                        );
                    }

                    // Wait for the WAL to arrive and be processed on the parent branch up
                    // to the requested branch point. The repository code itself doesn't
                    // require it, but if we start to receive WAL on the new timeline,
                    // decoding the new WAL might need to look up previous pages, relation
                    // sizes etc. and that would get confused if the previous page versions
                    // are not in the repository yet.
                    ancestor_timeline.wait_lsn(*lsn, ctx).await?;
                }

                self.branch_timeline(&ancestor_timeline, new_timeline_id, ancestor_start_lsn, ctx)
                    .await?
            }
            None => {
                self.bootstrap_timeline(new_timeline_id, pg_version, ctx)
                    .await?
            }
        };

        loaded_timeline.activate(self.get_broker_channel(broker_client)?, None, ctx);

        if let Some(remote_client) = loaded_timeline.remote_client.as_ref() {
            // Wait for the upload of the 'index_part.json` file to finish, so that when we return
            // Ok, the timeline is durable in remote storage.
            let kind = ancestor_timeline_id
                .map(|_| "branched")
                .unwrap_or("bootstrapped");
            remote_client.wait_completion().await.with_context(|| {
                format!("wait for {} timeline initial uploads to complete", kind)
            })?;
        }

        Ok(Some(loaded_timeline))
    }

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc task.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// `target_timeline_id` specifies the timeline to GC, or None for all.
    ///
    /// The `horizon` an `pitr` parameters determine how much WAL history needs to be retained.
    /// Also known as the retention period, or the GC cutoff point. `horizon` specifies
    /// the amount of history, as LSN difference from current latest LSN on each timeline.
    /// `pitr` specifies the same as a time difference from the current time. The effective
    /// GC cutoff point is determined conservatively by either `horizon` and `pitr`, whichever
    /// requires more history to be retained.
    //
    pub async fn gc_iteration(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        ctx: &RequestContext,
    ) -> anyhow::Result<GcResult> {
        // there is a global allowed_error for this
        anyhow::ensure!(
            self.is_active(),
            "Cannot run GC iteration on inactive tenant"
        );

        self.gc_iteration_internal(target_timeline_id, horizon, pitr, ctx)
            .await
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor task.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    pub async fn compaction_iteration(&self, ctx: &RequestContext) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot run compaction iteration on inactive tenant"
        );

        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // compactions.  We don't want to block everything else while the
        // compaction runs.
        let timelines_to_compact = {
            let timelines = self.timelines.lock().unwrap();
            let timelines_to_compact = timelines
                .iter()
                .filter_map(|(timeline_id, timeline)| {
                    if timeline.is_active() {
                        Some((*timeline_id, timeline.clone()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            drop(timelines);
            timelines_to_compact
        };

        for (timeline_id, timeline) in &timelines_to_compact {
            timeline
                .compact(ctx)
                .instrument(info_span!("compact_timeline", timeline = %timeline_id))
                .await?;
        }

        Ok(())
    }

    /// Flush all in-memory data to disk and remote storage, if any.
    ///
    /// Used at graceful shutdown.
    async fn freeze_and_flush_on_shutdown(&self) {
        let mut js = tokio::task::JoinSet::new();

        // execute on each timeline on the JoinSet, join after.
        let per_timeline = |timeline_id: TimelineId, timeline: Arc<Timeline>| {
            async move {
                debug_assert_current_span_has_tenant_and_timeline_id();

                match timeline.freeze_and_flush().await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("failed to freeze and flush: {e:#}");
                        return;
                    }
                }

                let res = if let Some(client) = timeline.remote_client.as_ref() {
                    // if we did not wait for completion here, it might be our shutdown process
                    // didn't wait for remote uploads to complete at all, as new tasks can forever
                    // be spawned.
                    //
                    // what is problematic is the shutting down of RemoteTimelineClient, because
                    // obviously it does not make sense to stop while we wait for it, but what
                    // about corner cases like s3 suddenly hanging up?
                    client.wait_completion().await
                } else {
                    Ok(())
                };

                if let Err(e) = res {
                    warn!("failed to await for frozen and flushed uploads: {e:#}");
                }
            }
            .instrument(tracing::info_span!("freeze_and_flush_on_shutdown", %timeline_id))
        };

        {
            let timelines = self.timelines.lock().unwrap();
            timelines
                .iter()
                .map(|(id, tl)| (*id, Arc::clone(tl)))
                .for_each(|(timeline_id, timeline)| {
                    js.spawn(per_timeline(timeline_id, timeline));
                })
        };

        while let Some(res) = js.join_next().await {
            match res {
                Ok(()) => {}
                Err(je) if je.is_cancelled() => unreachable!("no cancelling used"),
                Err(je) if je.is_panic() => { /* logged already */ }
                Err(je) => warn!("unexpected JoinError: {je:?}"),
            }
        }
    }

    /// Shuts down a timeline's tasks, removes its in-memory structures, and deletes its
    /// data from both disk and s3.
    async fn delete_timeline(
        &self,
        timeline_id: TimelineId,
        timeline: Arc<Timeline>,
        guard: DeletionGuard,
    ) -> anyhow::Result<()> {
        {
            // Grab the layer_removal_cs lock, and actually perform the deletion.
            //
            // This lock prevents prevents GC or compaction from running at the same time.
            // The GC task doesn't register itself with the timeline it's operating on,
            // so it might still be running even though we called `shutdown_tasks`.
            //
            // Note that there are still other race conditions between
            // GC, compaction and timeline deletion. See
            // https://github.com/neondatabase/neon/issues/2671
            //
            // No timeout here, GC & Compaction should be responsive to the
            // `TimelineState::Stopping` change.
            info!("waiting for layer_removal_cs.lock()");
            let layer_removal_guard = timeline.layer_removal_cs.lock().await;
            info!("got layer_removal_cs.lock(), deleting layer files");

            // NB: storage_sync upload tasks that reference these layers have been cancelled
            //     by the caller.

            let local_timeline_directory = self
                .conf
                .timeline_path(&timeline.timeline_id, &self.tenant_id);

            fail::fail_point!("timeline-delete-before-rm", |_| {
                Err(anyhow::anyhow!("failpoint: timeline-delete-before-rm"))?
            });

            // NB: This need not be atomic because the deleted flag in the IndexPart
            // will be observed during tenant/timeline load. The deletion will be resumed there.
            //
            // For configurations without remote storage, we tolerate that we're not crash-safe here.
            // The timeline may come up Active but with missing layer files, in such setups.
            // See https://github.com/neondatabase/neon/pull/3919#issuecomment-1531726720
            match std::fs::remove_dir_all(&local_timeline_directory) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // This can happen if we're called a second time, e.g.,
                    // because of a previous failure/cancellation at/after
                    // failpoint timeline-delete-after-rm.
                    //
                    // It can also happen if we race with tenant detach, because,
                    // it doesn't grab the layer_removal_cs lock.
                    //
                    // For now, log and continue.
                    // warn! level is technically not appropriate for the
                    // first case because we should expect retries to happen.
                    // But the error is so rare, it seems better to get attention if it happens.
                    let tenant_state = self.current_state();
                    warn!(
                        timeline_dir=?local_timeline_directory,
                        ?tenant_state,
                        "timeline directory not found, proceeding anyway"
                    );
                    // continue with the rest of the deletion
                }
                res => res.with_context(|| {
                    format!(
                        "Failed to remove local timeline directory '{}'",
                        local_timeline_directory.display()
                    )
                })?,
            }

            info!("finished deleting layer files, releasing layer_removal_cs.lock()");
            drop(layer_removal_guard);
        }

        fail::fail_point!("timeline-delete-after-rm", |_| {
            Err(anyhow::anyhow!("failpoint: timeline-delete-after-rm"))?
        });

        if let Some(remote_client) = &timeline.remote_client {
            remote_client.delete_all().await.context("delete_all")?
        };

        // Have a failpoint that can use the `pause` failpoint action.
        // We don't want to block the executor thread, hence, spawn_blocking + await.
        if cfg!(feature = "testing") {
            tokio::task::spawn_blocking({
                let current = tracing::Span::current();
                move || {
                    let _entered = current.entered();
                    tracing::info!("at failpoint in_progress_delete");
                    fail::fail_point!("in_progress_delete");
                }
            })
            .await
            .expect("spawn_blocking");
        }

        {
            // Remove the timeline from the map.
            let mut timelines = self.timelines.lock().unwrap();
            let children_exist = timelines
                .iter()
                .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline_id));
            // XXX this can happen because `branch_timeline` doesn't check `TimelineState::Stopping`.
            // We already deleted the layer files, so it's probably best to panic.
            // (Ideally, above remove_dir_all is atomic so we don't see this timeline after a restart)
            if children_exist {
                panic!("Timeline grew children while we removed layer files");
            }

            timelines.remove(&timeline_id).expect(
                "timeline that we were deleting was concurrently removed from 'timelines' map",
            );

            drop(timelines);
        }

        drop(guard);

        Ok(())
    }

    /// Removes timeline-related in-memory data and schedules removal from remote storage.
    #[instrument(skip(self, _ctx))]
    pub async fn prepare_and_schedule_delete_timeline(
        self: Arc<Self>,
        timeline_id: TimelineId,
        _ctx: &RequestContext,
    ) -> Result<(), DeleteTimelineError> {
        timeline::debug_assert_current_span_has_tenant_and_timeline_id();

        // Transition the timeline into TimelineState::Stopping.
        // This should prevent new operations from starting.
        //
        // Also grab the Timeline's delete_lock to prevent another deletion from starting.
        let timeline;
        let delete_lock_guard;
        {
            let mut timelines = self.timelines.lock().unwrap();

            // Ensure that there are no child timelines **attached to that pageserver**,
            // because detach removes files, which will break child branches
            let children: Vec<TimelineId> = timelines
                .iter()
                .filter_map(|(id, entry)| {
                    if entry.get_ancestor_timeline_id() == Some(timeline_id) {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect();

            if !children.is_empty() {
                return Err(DeleteTimelineError::HasChildren(children));
            }

            let timeline_entry = match timelines.entry(timeline_id) {
                Entry::Occupied(e) => e,
                Entry::Vacant(_) => return Err(DeleteTimelineError::NotFound),
            };

            timeline = Arc::clone(timeline_entry.get());

            // Prevent two tasks from trying to delete the timeline at the same time.
            delete_lock_guard =
                DeletionGuard(Arc::clone(&timeline.delete_lock).try_lock_owned().map_err(
                    |_| {
                        DeleteTimelineError::Other(anyhow::anyhow!(
                            "timeline deletion is already in progress"
                        ))
                    },
                )?);

            // If another task finished the deletion just before we acquired the lock,
            // return success.
            if delete_lock_guard.is_deleted() {
                return Ok(());
            }

            timeline.set_state(TimelineState::Stopping);

            drop(timelines);
        }

        // Now that the Timeline is in Stopping state, request all the related tasks to
        // shut down.
        //
        // NB: If this fails half-way through, and is retried, the retry will go through
        // all the same steps again. Make sure the code here is idempotent, and don't
        // error out if some of the shutdown tasks have already been completed!

        // Stop the walreceiver first.
        debug!("waiting for wal receiver to shutdown");
        let maybe_started_walreceiver = { timeline.walreceiver.lock().unwrap().take() };
        if let Some(walreceiver) = maybe_started_walreceiver {
            walreceiver.stop().await;
        }
        debug!("wal receiver shutdown confirmed");

        // Prevent new uploads from starting.
        if let Some(remote_client) = timeline.remote_client.as_ref() {
            let res = remote_client.stop();
            match res {
                Ok(()) => {}
                Err(e) => match e {
                    remote_timeline_client::StopError::QueueUninitialized => {
                        // This case shouldn't happen currently because the
                        // load and attach code bails out if _any_ of the timeline fails to fetch its IndexPart.
                        // That is, before we declare the Tenant as Active.
                        // But we only allow calls to delete_timeline on Active tenants.
                        return Err(DeleteTimelineError::Other(anyhow::anyhow!("upload queue is uninitialized, likely the timeline was in Broken state prior to this call because it failed to fetch IndexPart during load or attach, check the logs")));
                    }
                },
            }
        }

        // Stop & wait for the remaining timeline tasks, including upload tasks.
        // NB: This and other delete_timeline calls do not run as a task_mgr task,
        //     so, they are not affected by this shutdown_tasks() call.
        info!("waiting for timeline tasks to shutdown");
        task_mgr::shutdown_tasks(None, Some(self.tenant_id), Some(timeline_id)).await;

        // Mark timeline as deleted in S3 so we won't pick it up next time
        // during attach or pageserver restart.
        // See comment in persist_index_part_with_deleted_flag.
        if let Some(remote_client) = timeline.remote_client.as_ref() {
            match remote_client.persist_index_part_with_deleted_flag().await {
                // If we (now, or already) marked it successfully as deleted, we can proceed
                Ok(()) | Err(PersistIndexPartWithDeletedFlagError::AlreadyDeleted(_)) => (),
                // Bail out otherwise
                //
                // AlreadyInProgress shouldn't happen, because the 'delete_lock' prevents
                // two tasks from performing the deletion at the same time. The first task
                // that starts deletion should run it to completion.
                Err(e @ PersistIndexPartWithDeletedFlagError::AlreadyInProgress(_))
                | Err(e @ PersistIndexPartWithDeletedFlagError::Other(_)) => {
                    return Err(DeleteTimelineError::Other(anyhow::anyhow!(e)));
                }
            }
        }
        self.schedule_delete_timeline(timeline_id, timeline, delete_lock_guard);

        Ok(())
    }

    fn schedule_delete_timeline(
        self: Arc<Self>,
        timeline_id: TimelineId,
        timeline: Arc<Timeline>,
        guard: DeletionGuard,
    ) {
        let tenant_id = self.tenant_id;
        let timeline_clone = Arc::clone(&timeline);

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            Some(self.tenant_id),
            Some(timeline_id),
            "timeline_delete",
            false,
            async move {
                if let Err(err) = self.delete_timeline(timeline_id, timeline, guard).await {
                    error!("Error: {err:#}");
                    timeline_clone.set_broken(err.to_string())
                };
                Ok(())
            }
            .instrument({
                let span =
                    tracing::info_span!(parent: None, "delete_timeline", tenant_id=%tenant_id, timeline_id=%timeline_id);
                span.follows_from(Span::current());
                span
            }),
        );
    }

    pub fn current_state(&self) -> TenantState {
        self.state.borrow().clone()
    }

    pub fn is_active(&self) -> bool {
        self.current_state() == TenantState::Active
    }

    fn get_broker_channel(
        &self,
        broker_client: BrokerClientChannel,
    ) -> anyhow::Result<BrokerClientChannel> {
        let tenent_config_guard = self.tenant_conf.read().unwrap();
        if let Some(master_broker_endpoint) = &tenent_config_guard.master_broker_endpoint {
            storage_broker::connect(
                master_broker_endpoint.clone(),
                self.conf.broker_keepalive_interval,
            )
        } else {
            Ok(broker_client)
        }
    }

    /// Changes tenant status to active, unless shutdown was already requested.
    ///
    /// `background_jobs_can_start` is an optional barrier set to a value during pageserver startup
    /// to delay background jobs. Background jobs can be started right away when None is given.
    fn activate(
        self: &Arc<Self>,
        broker_client: BrokerClientChannel,
        background_jobs_can_start: Option<&completion::Barrier>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_id();

        let mut activating = false;
        self.state.send_modify(|current_state| {
            use pageserver_api::models::ActivatingFrom;
            match &*current_state {
                TenantState::Activating(_) | TenantState::Active | TenantState::Broken { .. } | TenantState::Stopping => {
                    panic!("caller is responsible for calling activate() only on Loading / Attaching tenants, got {state:?}", state = current_state);
                }
                TenantState::Loading => {
                    *current_state = TenantState::Activating(ActivatingFrom::Loading);
                }
                TenantState::Attaching => {
                    *current_state = TenantState::Activating(ActivatingFrom::Attaching);
                }
            }
            debug!(tenant_id = %self.tenant_id, "Activating tenant");
            activating = true;
            // Continue outside the closure. We need to grab timelines.lock()
            // and we plan to turn it into a tokio::sync::Mutex in a future patch.
        });

        if activating {
            let timelines_accessor = self.timelines.lock().unwrap();
            let timelines_to_activate = timelines_accessor
                .values()
                .filter(|timeline| !(timeline.is_broken() || timeline.is_stopping()));

            // Spawn gc and compaction loops. The loops will shut themselves
            // down when they notice that the tenant is inactive.
            tasks::start_background_loops(self, background_jobs_can_start);

            let mut activated_timelines = 0;

            for timeline in timelines_to_activate {
                timeline.activate(
                    self.get_broker_channel(broker_client.clone())?,
                    background_jobs_can_start,
                    ctx,
                );
                activated_timelines += 1;
            }

            self.state.send_modify(move |current_state| {
                assert!(
                    matches!(current_state, TenantState::Activating(_)),
                    "set_stopping and set_broken wait for us to leave Activating state",
                );
                *current_state = TenantState::Active;

                let elapsed = self.loading_started_at.elapsed();
                let total_timelines = timelines_accessor.len();

                // log a lot of stuff, because some tenants sometimes suffer from user-visible
                // times to activate. see https://github.com/neondatabase/neon/issues/4025
                info!(
                    since_creation_millis = elapsed.as_millis(),
                    tenant_id = %self.tenant_id,
                    activated_timelines,
                    total_timelines,
                    post_state = <&'static str>::from(&*current_state),
                    "activation attempt finished"
                );
            });
        }
        Ok(())
    }

    /// Shutdown the tenant and join all of the spawned tasks.
    ///
    /// The method caters for all use-cases:
    /// - pageserver shutdown (freeze_and_flush == true)
    /// - detach + ignore (freeze_and_flush == false)
    ///
    /// This will attempt to shutdown even if tenant is broken.
    pub(crate) async fn shutdown(&self, freeze_and_flush: bool) -> Result<(), ShutdownError> {
        debug_assert_current_span_has_tenant_id();
        // Set tenant (and its timlines) to Stoppping state.
        //
        // Since we can only transition into Stopping state after activation is complete,
        // run it in a JoinSet so all tenants have a chance to stop before we get SIGKILLed.
        //
        // Transitioning tenants to Stopping state has a couple of non-obvious side effects:
        // 1. Lock out any new requests to the tenants.
        // 2. Signal cancellation to WAL receivers (we wait on it below).
        // 3. Signal cancellation for other tenant background loops.
        // 4. ???
        //
        // The waiting for the cancellation is not done uniformly.
        // We certainly wait for WAL receivers to shut down.
        // That is necessary so that no new data comes in before the freeze_and_flush.
        // But the tenant background loops are joined-on in our caller.
        // It's mesed up.
        // we just ignore the failure to stop
        match self.set_stopping().await {
            Ok(()) => {}
            Err(SetStoppingError::Broken) => {
                // assume that this is acceptable
            }
            Err(SetStoppingError::AlreadyStopping) => return Err(ShutdownError::AlreadyStopping),
        };

        if freeze_and_flush {
            // walreceiver has already began to shutdown with TenantState::Stopping, but we need to
            // await for them to stop.
            task_mgr::shutdown_tasks(
                Some(TaskKind::WalReceiverManager),
                Some(self.tenant_id),
                None,
            )
            .await;

            // this will wait for uploads to complete; in the past, it was done outside tenant
            // shutdown in pageserver::shutdown_pageserver.
            self.freeze_and_flush_on_shutdown().await;
        }

        // shutdown all tenant and timeline tasks: gc, compaction, page service
        // No new tasks will be started for this tenant because it's in `Stopping` state.
        //
        // this will additionally shutdown and await all timeline tasks.
        task_mgr::shutdown_tasks(None, Some(self.tenant_id), None).await;

        Ok(())
    }

    /// Change tenant status to Stopping, to mark that it is being shut down.
    ///
    /// This function waits for the tenant to become active if it isn't already, before transitioning it into Stopping state.
    ///
    /// This function is not cancel-safe!
    async fn set_stopping(&self) -> Result<(), SetStoppingError> {
        let mut rx = self.state.subscribe();

        // cannot stop before we're done activating, so wait out until we're done activating
        rx.wait_for(|state| match state {
            TenantState::Activating(_) | TenantState::Loading | TenantState::Attaching => {
                info!(
                    "waiting for {} to turn Active|Broken|Stopping",
                    <&'static str>::from(state)
                );
                false
            }
            TenantState::Active | TenantState::Broken { .. } | TenantState::Stopping {} => true,
        })
        .await
        .expect("cannot drop self.state while on a &self method");

        // we now know we're done activating, let's see whether this task is the winner to transition into Stopping
        let mut err = None;
        let stopping = self.state.send_if_modified(|current_state| match current_state {
            TenantState::Activating(_) | TenantState::Loading | TenantState::Attaching => {
                unreachable!("we ensured above that we're done with activation, and, there is no re-activation")
            }
            TenantState::Active => {
                // FIXME: due to time-of-check vs time-of-use issues, it can happen that new timelines
                // are created after the transition to Stopping. That's harmless, as the Timelines
                // won't be accessible to anyone afterwards, because the Tenant is in Stopping state.
                *current_state = TenantState::Stopping;
                // Continue stopping outside the closure. We need to grab timelines.lock()
                // and we plan to turn it into a tokio::sync::Mutex in a future patch.
                true
            }
            TenantState::Broken { reason, .. } => {
                info!(
                    "Cannot set tenant to Stopping state, it is in Broken state due to: {reason}"
                );
                err = Some(SetStoppingError::Broken);
                false
            }
            TenantState::Stopping => {
                info!("Tenant is already in Stopping state");
                err = Some(SetStoppingError::AlreadyStopping);
                false
            }
        });
        match (stopping, err) {
            (true, None) => {} // continue
            (false, Some(err)) => return Err(err),
            (true, Some(_)) => unreachable!(
                "send_if_modified closure must error out if not transitioning to Stopping"
            ),
            (false, None) => unreachable!(
                "send_if_modified closure must return true if transitioning to Stopping"
            ),
        }

        let timelines_accessor = self.timelines.lock().unwrap();
        let not_broken_timelines = timelines_accessor
            .values()
            .filter(|timeline| !timeline.is_broken());
        for timeline in not_broken_timelines {
            timeline.set_state(TimelineState::Stopping);
        }
        Ok(())
    }

    /// Method for tenant::mgr to transition us into Broken state in case of a late failure in
    /// `remove_tenant_from_memory`
    ///
    /// This function waits for the tenant to become active if it isn't already, before transitioning it into Stopping state.
    ///
    /// In tests, we also use this to set tenants to Broken state on purpose.
    pub(crate) async fn set_broken(&self, reason: String) {
        let mut rx = self.state.subscribe();

        // The load & attach routines own the tenant state until it has reached `Active`.
        // So, wait until it's done.
        rx.wait_for(|state| match state {
            TenantState::Activating(_) | TenantState::Loading | TenantState::Attaching => {
                info!(
                    "waiting for {} to turn Active|Broken|Stopping",
                    <&'static str>::from(state)
                );
                false
            }
            TenantState::Active | TenantState::Broken { .. } | TenantState::Stopping {} => true,
        })
        .await
        .expect("cannot drop self.state while on a &self method");

        // we now know we're done activating, let's see whether this task is the winner to transition into Broken
        self.state.send_modify(|current_state| {
            match *current_state {
                TenantState::Activating(_) | TenantState::Loading | TenantState::Attaching => {
                    unreachable!("we ensured above that we're done with activation, and, there is no re-activation")
                }
                TenantState::Active => {
                    if cfg!(feature = "testing") {
                        warn!("Changing Active tenant to Broken state, reason: {}", reason);
                        *current_state = TenantState::broken_from_reason(reason);
                    } else {
                        unreachable!("not allowed to call set_broken on Active tenants in non-testing builds")
                    }
                }
                TenantState::Broken { .. } => {
                    warn!("Tenant is already in Broken state");
                }
                // This is the only "expected" path, any other path is a bug.
                TenantState::Stopping => {
                    warn!(
                        "Marking Stopping tenant as Broken state, reason: {}",
                        reason
                    );
                    *current_state = TenantState::broken_from_reason(reason);
                }
           }
        });
    }

    pub fn subscribe_for_state_updates(&self) -> watch::Receiver<TenantState> {
        self.state.subscribe()
    }

    pub(crate) async fn wait_to_become_active(&self) -> Result<(), WaitToBecomeActiveError> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = receiver.borrow_and_update().clone();
            match current_state {
                TenantState::Loading | TenantState::Attaching | TenantState::Activating(_) => {
                    // in these states, there's a chance that we can reach ::Active
                    receiver.changed().await.map_err(
                        |_e: tokio::sync::watch::error::RecvError| {
                            WaitToBecomeActiveError::TenantDropped {
                                tenant_id: self.tenant_id,
                            }
                        },
                    )?;
                }
                TenantState::Active { .. } => {
                    return Ok(());
                }
                TenantState::Broken { .. } | TenantState::Stopping => {
                    // There's no chance the tenant can transition back into ::Active
                    return Err(WaitToBecomeActiveError::WillNotBecomeActive {
                        tenant_id: self.tenant_id,
                        state: current_state,
                    });
                }
            }
        }
    }
}

/// Given a Vec of timelines and their ancestors (timeline_id, ancestor_id),
/// perform a topological sort, so that the parent of each timeline comes
/// before the children.
fn tree_sort_timelines(
    timelines: HashMap<TimelineId, TimelineMetadata>,
) -> anyhow::Result<Vec<(TimelineId, TimelineMetadata)>> {
    let mut result = Vec::with_capacity(timelines.len());

    let mut now = Vec::with_capacity(timelines.len());
    // (ancestor, children)
    let mut later: HashMap<TimelineId, Vec<(TimelineId, TimelineMetadata)>> =
        HashMap::with_capacity(timelines.len());

    for (timeline_id, metadata) in timelines {
        if let Some(ancestor_id) = metadata.ancestor_timeline() {
            let children = later.entry(ancestor_id).or_default();
            children.push((timeline_id, metadata));
        } else {
            now.push((timeline_id, metadata));
        }
    }

    while let Some((timeline_id, metadata)) = now.pop() {
        result.push((timeline_id, metadata));
        // All children of this can be loaded now
        if let Some(mut children) = later.remove(&timeline_id) {
            now.append(&mut children);
        }
    }

    // All timelines should be visited now. Unless there were timelines with missing ancestors.
    if !later.is_empty() {
        for (missing_id, orphan_ids) in later {
            for (orphan_id, _) in orphan_ids {
                error!("could not load timeline {orphan_id} because its ancestor timeline {missing_id} could not be loaded");
            }
        }
        bail!("could not load tenant because some timelines are missing ancestors");
    }

    Ok(result)
}

impl Tenant {
    pub fn tenant_specific_overrides(&self) -> TenantConfOpt {
        self.tenant_conf.read().unwrap().clone()
    }

    pub fn effective_config(&self) -> TenantConf {
        self.tenant_specific_overrides()
            .merge(self.conf.default_tenant_conf.clone())
    }

    pub fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    pub fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    pub fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    pub fn get_compaction_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_period
            .unwrap_or(self.conf.default_tenant_conf.compaction_period)
    }

    pub fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    pub fn get_gc_horizon(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .gc_horizon
            .unwrap_or(self.conf.default_tenant_conf.gc_horizon)
    }

    pub fn get_gc_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .gc_period
            .unwrap_or(self.conf.default_tenant_conf.gc_period)
    }

    pub fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    pub fn get_pitr_interval(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .pitr_interval
            .unwrap_or(self.conf.default_tenant_conf.pitr_interval)
    }

    pub fn get_trace_read_requests(&self) -> bool {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .trace_read_requests
            .unwrap_or(self.conf.default_tenant_conf.trace_read_requests)
    }

    pub fn get_min_resident_size_override(&self) -> Option<u64> {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .min_resident_size_override
            .or(self.conf.default_tenant_conf.min_resident_size_override)
    }

    pub fn set_new_tenant_config(&self, new_tenant_conf: TenantConfOpt) {
        *self.tenant_conf.write().unwrap() = new_tenant_conf;
        // Don't hold self.timelines.lock() during the notifies.
        // There's no risk of deadlock right now, but there could be if we consolidate
        // mutexes in struct Timeline in the future.
        let timelines = self.list_timelines();
        for timeline in timelines {
            timeline.tenant_conf_updated();
        }
    }

    /// Helper function to create a new Timeline struct.
    ///
    /// The returned Timeline is in Loading state. The caller is responsible for
    /// initializing any on-disk state, and for inserting the Timeline to the 'timelines'
    /// map.
    fn create_timeline_struct(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        remote_client: Option<RemoteTimelineClient>,
        init_order: Option<&InitializationOrder>,
    ) -> anyhow::Result<Arc<Timeline>> {
        if let Some(ancestor_timeline_id) = new_metadata.ancestor_timeline() {
            anyhow::ensure!(
                ancestor.is_some(),
                "Timeline's {new_timeline_id} ancestor {ancestor_timeline_id} was not found"
            )
        }

        let initial_logical_size_can_start = init_order.map(|x| &x.initial_logical_size_can_start);
        let initial_logical_size_attempt = init_order.map(|x| &x.initial_logical_size_attempt);

        let master_client = if let Some(master_storage) = &self.master_storage {
            Some(RemoteTimelineClient::new(
                master_storage.clone(),
                self.conf,
                self.tenant_id,
                new_timeline_id,
            ))
        } else {
            None
        };
        let pg_version = new_metadata.pg_version();
        Ok(Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            new_metadata,
            ancestor,
            new_timeline_id,
            self.tenant_id,
            Arc::clone(&self.walredo_mgr),
            remote_client,
            master_client,
            new_metadata.replica_lsn(),
            pg_version,
            initial_logical_size_can_start.cloned(),
            initial_logical_size_attempt.cloned(),
        ))
    }

    fn new(
        state: TenantState,
        conf: &'static PageServerConf,
        tenant_conf: TenantConfOpt,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenant_id: TenantId,
        remote_storage: Option<GenericRemoteStorage>,
    ) -> Tenant {
        let (state, mut rx) = watch::channel(state);

        let master_storage = if let Some(remote_storage_config) = &conf.remote_storage_config {
            if let Some(region) = &tenant_conf.master_region {
                let master_storage_config =
                    remote_storage_config.in_region(region.clone()).unwrap();
                Some(GenericRemoteStorage::from_config(&master_storage_config).unwrap())
            } else {
                None
            }
        } else {
            None
        };

        tokio::spawn(async move {
            let mut current_state: &'static str = From::from(&*rx.borrow_and_update());
            let tid = tenant_id.to_string();
            TENANT_STATE_METRIC
                .with_label_values(&[&tid, current_state])
                .inc();
            loop {
                match rx.changed().await {
                    Ok(()) => {
                        let new_state: &'static str = From::from(&*rx.borrow_and_update());
                        TENANT_STATE_METRIC
                            .with_label_values(&[&tid, current_state])
                            .dec();
                        TENANT_STATE_METRIC
                            .with_label_values(&[&tid, new_state])
                            .inc();

                        current_state = new_state;
                    }
                    Err(_sender_dropped_error) => {
                        info!("Tenant dropped the state updates sender, quitting waiting for tenant state change");
                        return;
                    }
                }
            }
        });

        Tenant {
            tenant_id,
            conf,
            // using now here is good enough approximation to catch tenants with really long
            // activation times.
            loading_started_at: Instant::now(),
            tenant_conf: Arc::new(RwLock::new(tenant_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: tokio::sync::Mutex::new(()),
            walredo_mgr,
            remote_storage,
            master_storage,
            state,
            cached_logical_sizes: tokio::sync::Mutex::new(HashMap::new()),
            cached_synthetic_tenant_size: Arc::new(AtomicU64::new(0)),
            eviction_task_tenant_state: tokio::sync::Mutex::new(EvictionTaskTenantState::default()),
        }
    }

    /// Locate and load config
    pub(super) fn load_tenant_config(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
    ) -> anyhow::Result<TenantConfOpt> {
        let target_config_path = conf.tenant_config_path(tenant_id);
        let target_config_display = target_config_path.display();

        info!("loading tenantconf from {target_config_display}");

        // FIXME If the config file is not found, assume that we're attaching
        // a detached tenant and config is passed via attach command.
        // https://github.com/neondatabase/neon/issues/1555
        if !target_config_path.exists() {
            info!("tenant config not found in {target_config_display}");
            return Ok(TenantConfOpt::default());
        }

        // load and parse file
        let config = fs::read_to_string(&target_config_path).with_context(|| {
            format!("Failed to load config from path '{target_config_display}'")
        })?;

        let toml = config.parse::<toml_edit::Document>().with_context(|| {
            format!("Failed to parse config from file '{target_config_display}' as toml file")
        })?;

        let mut tenant_conf = TenantConfOpt::default();
        for (key, item) in toml.iter() {
            match key {
                "tenant_config" => {
                    tenant_conf = PageServerConf::parse_toml_tenant_conf(item).with_context(|| {
                        format!("Failed to parse config from file '{target_config_display}' as pageserver config")
                    })?;
                }
                _ => bail!("config file {target_config_display} has unrecognized pageserver option '{key}'"),

            }
        }

        Ok(tenant_conf)
    }

    pub(super) fn persist_tenant_config(
        tenant_id: &TenantId,
        target_config_path: &Path,
        tenant_conf: TenantConfOpt,
        creating_tenant: bool,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("saving tenantconf").entered();

        // imitate a try-block with a closure
        let do_persist = |target_config_path: &Path| -> anyhow::Result<()> {
            let target_config_parent = target_config_path.parent().with_context(|| {
                format!(
                    "Config path does not have a parent: {}",
                    target_config_path.display()
                )
            })?;

            info!("persisting tenantconf to {}", target_config_path.display());

            let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.

[tenant_config]
"#
            .to_string();

            // Convert the config to a toml file.
            conf_content += &toml_edit::ser::to_string(&tenant_conf)?;

            let mut target_config_file = VirtualFile::open_with_options(
                target_config_path,
                OpenOptions::new()
                    .truncate(true) // This needed for overwriting with small config files
                    .write(true)
                    .create_new(creating_tenant)
                    // when creating a new tenant, first_save will be true and `.create(true)` will be
                    // ignored (per rust std docs).
                    //
                    // later when updating the config of created tenant, or persisting config for the
                    // first time for attached tenant, the `.create(true)` is used.
                    .create(true),
            )?;

            target_config_file
                .write(conf_content.as_bytes())
                .context("write toml bytes into file")
                .and_then(|_| target_config_file.sync_all().context("fsync config file"))
                .context("write config file")?;

            // fsync the parent directory to ensure the directory entry is durable.
            // before this was done conditionally on creating_tenant, but these management actions are rare
            // enough to just fsync it always.

            crashsafe::fsync(target_config_parent)?;
            // XXX we're not fsyncing the parent dir, need to do that in case `creating_tenant`
            Ok(())
        };

        // this function is called from creating the tenant and updating the tenant config, which
        // would otherwise share this context, so keep it here in one place.
        do_persist(target_config_path).with_context(|| {
            format!(
                "write tenant {tenant_id} config to {}",
                target_config_path.display()
            )
        })
    }

    //
    // How garbage collection works:
    //
    //                    +--bar------------->
    //                   /
    //             +----+-----foo---------------->
    //            /
    // ----main--+-------------------------->
    //                \
    //                 +-----baz-------->
    //
    //
    // 1. Grab 'gc_cs' mutex to prevent new timelines from being created while Timeline's
    //    `gc_infos` are being refreshed
    // 2. Scan collected timelines, and on each timeline, make note of the
    //    all the points where other timelines have been branched off.
    //    We will refrain from removing page versions at those LSNs.
    // 3. For each timeline, scan all layer files on the timeline.
    //    Remove all files for which a newer file exists and which
    //    don't cover any branch point LSNs.
    //
    // TODO:
    // - if a relation has a non-incremental persistent layer on a child branch, then we
    //   don't need to keep that in the parent anymore. But currently
    //   we do.
    async fn gc_iteration_internal(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        ctx: &RequestContext,
    ) -> anyhow::Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        let gc_timelines = self
            .refresh_gc_info_internal(target_timeline_id, horizon, pitr, ctx)
            .await?;

        utils::failpoint_sleep_millis_async!("gc_iteration_internal_after_getting_gc_timelines");

        // If there is nothing to GC, we don't want any messages in the INFO log.
        if !gc_timelines.is_empty() {
            info!("{} timelines need GC", gc_timelines.len());
        } else {
            debug!("{} timelines need GC", gc_timelines.len());
        }

        // Perform GC for each timeline.
        //
        // Note that we don't hold the GC lock here because we don't want
        // to delay the branch creation task, which requires the GC lock.
        // A timeline GC iteration can be slow because it may need to wait for
        // compaction (both require `layer_removal_cs` lock),
        // but the GC iteration can run concurrently with branch creation.
        //
        // See comments in [`Tenant::branch_timeline`] for more information
        // about why branch creation task can run concurrently with timeline's GC iteration.
        for timeline in gc_timelines {
            if task_mgr::is_shutdown_requested() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
            }
            let result = timeline.gc().await?;
            totals += result;
        }

        totals.elapsed = now.elapsed();
        Ok(totals)
    }

    /// Refreshes the Timeline::gc_info for all timelines, returning the
    /// vector of timelines which have [`Timeline::get_last_record_lsn`] past
    /// [`Tenant::get_gc_horizon`].
    ///
    /// This is usually executed as part of periodic gc, but can now be triggered more often.
    pub async fn refresh_gc_info(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<Arc<Timeline>>> {
        // since this method can now be called at different rates than the configured gc loop, it
        // might be that these configuration values get applied faster than what it was previously,
        // since these were only read from the gc task.
        let horizon = self.get_gc_horizon();
        let pitr = self.get_pitr_interval();

        // refresh all timelines
        let target_timeline_id = None;

        self.refresh_gc_info_internal(target_timeline_id, horizon, pitr, ctx)
            .await
    }

    async fn refresh_gc_info_internal(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<Arc<Timeline>>> {
        // grab mutex to prevent new timelines from being created here.
        let gc_cs = self.gc_cs.lock().await;

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        let (all_branchpoints, timeline_ids): (BTreeSet<(TimelineId, Lsn)>, _) = {
            let timelines = self.timelines.lock().unwrap();
            let mut all_branchpoints = BTreeSet::new();
            let timeline_ids = {
                if let Some(target_timeline_id) = target_timeline_id.as_ref() {
                    if timelines.get(target_timeline_id).is_none() {
                        bail!("gc target timeline does not exist")
                    }
                };

                timelines
                    .iter()
                    .map(|(timeline_id, timeline_entry)| {
                        if let Some(ancestor_timeline_id) =
                            &timeline_entry.get_ancestor_timeline_id()
                        {
                            // If target_timeline is specified, we only need to know branchpoints of its children
                            if let Some(timeline_id) = target_timeline_id {
                                if ancestor_timeline_id == &timeline_id {
                                    all_branchpoints.insert((
                                        *ancestor_timeline_id,
                                        timeline_entry.get_ancestor_lsn(),
                                    ));
                                }
                            }
                            // Collect branchpoints for all timelines
                            else {
                                all_branchpoints.insert((
                                    *ancestor_timeline_id,
                                    timeline_entry.get_ancestor_lsn(),
                                ));
                            }
                        }

                        *timeline_id
                    })
                    .collect::<Vec<_>>()
            };
            (all_branchpoints, timeline_ids)
        };

        // Ok, we now know all the branch points.
        // Update the GC information for each timeline.
        let mut gc_timelines = Vec::with_capacity(timeline_ids.len());
        for timeline_id in timeline_ids {
            // Timeline is known to be local and loaded.
            let timeline = self
                .get_timeline(timeline_id, false)
                .with_context(|| format!("Timeline {timeline_id} was not found"))?;

            // If target_timeline is specified, ignore all other timelines
            if let Some(target_timeline_id) = target_timeline_id {
                if timeline_id != target_timeline_id {
                    continue;
                }
            }

            if let Some(cutoff) = timeline.get_last_record_lsn().checked_sub(horizon) {
                let branchpoints: Vec<Lsn> = all_branchpoints
                    .range((
                        Included((timeline_id, Lsn(0))),
                        Included((timeline_id, Lsn(u64::MAX))),
                    ))
                    .map(|&x| x.1)
                    .collect();
                timeline
                    .update_gc_info(branchpoints, cutoff, pitr, ctx)
                    .await?;

                gc_timelines.push(timeline);
            }
        }
        drop(gc_cs);
        Ok(gc_timelines)
    }

    /// A substitute for `branch_timeline` for use in unit tests.
    /// The returned timeline will have state value `Active` to make various `anyhow::ensure!()`
    /// calls pass, but, we do not actually call `.activate()` under the hood. So, none of the
    /// timeline background tasks are launched, except the flush loop.
    #[cfg(test)]
    async fn branch_timeline_test(
        &self,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        start_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let tl = self
            .branch_timeline_impl(src_timeline, dst_id, start_lsn, ctx)
            .await?;
        tl.set_state(TimelineState::Active);
        Ok(tl)
    }

    /// Branch an existing timeline.
    ///
    /// The caller is responsible for activating the returned timeline.
    async fn branch_timeline(
        &self,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        start_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        self.branch_timeline_impl(src_timeline, dst_id, start_lsn, ctx)
            .await
    }

    async fn branch_timeline_impl(
        &self,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        start_lsn: Option<Lsn>,
        _ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let src_id = src_timeline.timeline_id;

        // If no start LSN is specified, we branch the new timeline from the source timeline's last record LSN
        let start_lsn = start_lsn.unwrap_or_else(|| {
            let lsn = src_timeline.get_last_record_lsn();
            info!("branching timeline {dst_id} from timeline {src_id} at last record LSN: {lsn}");
            lsn
        });

        // First acquire the GC lock so that another task cannot advance the GC
        // cutoff in 'gc_info', and make 'start_lsn' invalid, while we are
        // creating the branch.
        let _gc_cs = self.gc_cs.lock().await;

        // Create a placeholder for the new branch. This will error
        // out if the new timeline ID is already in use.
        let timeline_uninit_mark = {
            let timelines = self.timelines.lock().unwrap();
            self.create_timeline_uninit_mark(dst_id, &timelines)?
        };

        // Ensure that `start_lsn` is valid, i.e. the LSN is within the PITR
        // horizon on the source timeline
        //
        // We check it against both the planned GC cutoff stored in 'gc_info',
        // and the 'latest_gc_cutoff' of the last GC that was performed.  The
        // planned GC cutoff in 'gc_info' is normally larger than
        // 'latest_gc_cutoff_lsn', but beware of corner cases like if you just
        // changed the GC settings for the tenant to make the PITR window
        // larger, but some of the data was already removed by an earlier GC
        // iteration.

        // check against last actual 'latest_gc_cutoff' first
        let latest_gc_cutoff_lsn = src_timeline.get_latest_gc_cutoff_lsn();
        src_timeline
            .check_lsn_is_in_scope(start_lsn, &latest_gc_cutoff_lsn)
            .context(format!(
                "invalid branch start lsn: less than latest GC cutoff {}",
                *latest_gc_cutoff_lsn,
            ))?;

        // and then the planned GC cutoff
        {
            let gc_info = src_timeline.gc_info.read().unwrap();
            let cutoff = min(gc_info.pitr_cutoff, gc_info.horizon_cutoff);
            if start_lsn < cutoff {
                bail!(format!(
                    "invalid branch start lsn: less than planned GC cutoff {cutoff}"
                ));
            }
        }

        //
        // The branch point is valid, and we are still holding the 'gc_cs' lock
        // so that GC cannot advance the GC cutoff until we are finished.
        // Proceed with the branch creation.
        //

        // Determine prev-LSN for the new timeline. We can only determine it if
        // the timeline was branched at the current end of the source timeline.
        let RecordLsn {
            last: src_last,
            prev: src_prev,
        } = src_timeline.get_last_record_rlsn();
        let dst_prev = if src_last == start_lsn {
            Some(src_prev)
        } else {
            None
        };

        // Create the metadata file, noting the ancestor of the new timeline.
        // There is initially no data in it, but all the read-calls know to look
        // into the ancestor.
        let metadata = TimelineMetadata::new(
            start_lsn,
            dst_prev,
            Some(src_id),
            start_lsn,
            *src_timeline.latest_gc_cutoff_lsn.read(), // FIXME: should we hold onto this guard longer?
            src_timeline.initdb_lsn,
            src_timeline.pg_version,
            None, // no branches at replica
        );

        let uninitialized_timeline = self.prepare_new_timeline(
            dst_id,
            &metadata,
            timeline_uninit_mark,
            start_lsn + 1,
            Some(Arc::clone(src_timeline)),
        )?;

        let new_timeline = uninitialized_timeline.finish_creation()?;

        // Root timeline gets its layers during creation and uploads them along with the metadata.
        // A branch timeline though, when created, can get no writes for some time, hence won't get any layers created.
        // We still need to upload its metadata eagerly: if other nodes `attach` the tenant and miss this timeline, their GC
        // could get incorrect information and remove more layers, than needed.
        // See also https://github.com/neondatabase/neon/issues/3865
        if let Some(remote_client) = new_timeline.remote_client.as_ref() {
            remote_client
                .schedule_index_upload_for_metadata_update(&metadata)
                .context("branch initial metadata upload")?;
        }

        info!("branched timeline {dst_id} from {src_id} at {start_lsn}");

        Ok(new_timeline)
    }

    /// - run initdb to init temporary instance and get bootstrap data
    /// - after initialization complete, remove the temp dir.
    ///
    /// The caller is responsible for activating the returned timeline.
    async fn bootstrap_timeline(
        &self,
        timeline_id: TimelineId,
        pg_version: u32,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let timeline_uninit_mark = {
            let timelines = self.timelines.lock().unwrap();
            self.create_timeline_uninit_mark(timeline_id, &timelines)?
        };
        // create a `tenant/{tenant_id}/timelines/basebackup-{timeline_id}.{TEMP_FILE_SUFFIX}/`
        // temporary directory for basebackup files for the given timeline.
        let initdb_path = path_with_suffix_extension(
            self.conf
                .timelines_path(&self.tenant_id)
                .join(format!("basebackup-{timeline_id}")),
            TEMP_FILE_SUFFIX,
        );

        // an uninit mark was placed before, nothing else can access this timeline files
        // current initdb was not run yet, so remove whatever was left from the previous runs
        if initdb_path.exists() {
            fs::remove_dir_all(&initdb_path).with_context(|| {
                format!(
                    "Failed to remove already existing initdb directory: {}",
                    initdb_path.display()
                )
            })?;
        }
        // Init temporarily repo to get bootstrap data, this creates a directory in the `initdb_path` path
        run_initdb(self.conf, &initdb_path, pg_version)?;
        // this new directory is very temporary, set to remove it immediately after bootstrap, we don't need it
        scopeguard::defer! {
            if let Err(e) = fs::remove_dir_all(&initdb_path) {
                // this is unlikely, but we will remove the directory on pageserver restart or another bootstrap call
                error!("Failed to remove temporary initdb directory '{}': {}", initdb_path.display(), e);
            }
        }
        let pgdata_path = &initdb_path;
        let pgdata_lsn = import_datadir::get_lsn_from_controlfile(pgdata_path)?.align();

        // Import the contents of the data directory at the initial checkpoint
        // LSN, and any WAL after that.
        // Initdb lsn will be equal to last_record_lsn which will be set after import.
        // Because we know it upfront avoid having an option or dummy zero value by passing it to the metadata.
        let new_metadata = TimelineMetadata::new(
            Lsn(0),
            None,
            None,
            Lsn(0),
            pgdata_lsn,
            pgdata_lsn,
            pg_version,
            None,
        );
        let raw_timeline = self.prepare_new_timeline(
            timeline_id,
            &new_metadata,
            timeline_uninit_mark,
            pgdata_lsn,
            None,
        )?;

        let tenant_id = raw_timeline.owning_tenant.tenant_id;
        let unfinished_timeline = raw_timeline.raw_timeline()?;

        import_datadir::import_timeline_from_postgres_datadir(
            unfinished_timeline,
            pgdata_path,
            pgdata_lsn,
            ctx,
        )
        .await
        .with_context(|| {
            format!("Failed to import pgdatadir for timeline {tenant_id}/{timeline_id}")
        })?;

        // Flush the new layer files to disk, before we make the timeline as available to
        // the outside world.
        //
        // Flush loop needs to be spawned in order to be able to flush.
        unfinished_timeline.maybe_spawn_flush_loop();

        fail::fail_point!("before-checkpoint-new-timeline", |_| {
            anyhow::bail!("failpoint before-checkpoint-new-timeline");
        });

        unfinished_timeline
            .freeze_and_flush()
            .await
            .with_context(|| {
                format!(
                    "Failed to flush after pgdatadir import for timeline {tenant_id}/{timeline_id}"
                )
            })?;

        // All done!
        let timeline = raw_timeline.finish_creation()?;

        info!(
            "created root timeline {} timeline.lsn {}",
            timeline_id,
            timeline.get_last_record_lsn()
        );

        Ok(timeline)
    }

    /// Creates intermediate timeline structure and its files.
    ///
    /// An empty layer map is initialized, and new data and WAL can be imported starting
    /// at 'disk_consistent_lsn'. After any initial data has been imported, call
    /// `finish_creation` to insert the Timeline into the timelines map and to remove the
    /// uninit mark file.
    fn prepare_new_timeline(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        uninit_mark: TimelineUninitMark,
        start_lsn: Lsn,
        ancestor: Option<Arc<Timeline>>,
    ) -> anyhow::Result<UninitializedTimeline> {
        let tenant_id = self.tenant_id;

        let remote_client = if let Some(remote_storage) = self.remote_storage.as_ref() {
            let remote_client = RemoteTimelineClient::new(
                remote_storage.clone(),
                self.conf,
                tenant_id,
                new_timeline_id,
            );
            remote_client.init_upload_queue_for_empty_remote(new_metadata)?;
            Some(remote_client)
        } else {
            None
        };

        let timeline_struct = self
            .create_timeline_struct(new_timeline_id, new_metadata, ancestor, remote_client, None)
            .context("Failed to create timeline data structure")?;

        timeline_struct.init_empty_layer_map(start_lsn);

        if let Err(e) =
            self.create_timeline_files(&uninit_mark.timeline_path, new_timeline_id, new_metadata)
        {
            error!("Failed to create initial files for timeline {tenant_id}/{new_timeline_id}, cleaning up: {e:?}");
            cleanup_timeline_directory(uninit_mark);
            return Err(e);
        }

        debug!("Successfully created initial files for timeline {tenant_id}/{new_timeline_id}");

        Ok(UninitializedTimeline {
            owning_tenant: self,
            timeline_id: new_timeline_id,
            raw_timeline: Some((timeline_struct, uninit_mark)),
        })
    }

    fn create_timeline_files(
        &self,
        timeline_path: &Path,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        crashsafe::create_dir(timeline_path).context("Failed to create timeline directory")?;

        fail::fail_point!("after-timeline-uninit-mark-creation", |_| {
            anyhow::bail!("failpoint after-timeline-uninit-mark-creation");
        });

        save_metadata(
            self.conf,
            new_timeline_id,
            self.tenant_id,
            new_metadata,
            true,
        )
        .context("Failed to create timeline metadata")?;
        Ok(())
    }

    /// Attempts to create an uninit mark file for the timeline initialization.
    /// Bails, if the timeline is already loaded into the memory (i.e. initialized before), or the uninit mark file already exists.
    ///
    /// This way, we need to hold the timelines lock only for small amount of time during the mark check/creation per timeline init.
    fn create_timeline_uninit_mark(
        &self,
        timeline_id: TimelineId,
        timelines: &MutexGuard<HashMap<TimelineId, Arc<Timeline>>>,
    ) -> anyhow::Result<TimelineUninitMark> {
        let tenant_id = self.tenant_id;

        anyhow::ensure!(
            timelines.get(&timeline_id).is_none(),
            "Timeline {tenant_id}/{timeline_id} already exists in pageserver's memory"
        );
        let timeline_path = self.conf.timeline_path(&timeline_id, &tenant_id);
        anyhow::ensure!(
            !timeline_path.exists(),
            "Timeline {} already exists, cannot create its uninit mark file",
            timeline_path.display()
        );

        let uninit_mark_path = self
            .conf
            .timeline_uninit_mark_file_path(tenant_id, timeline_id);
        fs::File::create(&uninit_mark_path)
            .context("Failed to create uninit mark file")
            .and_then(|_| {
                crashsafe::fsync_file_and_parent(&uninit_mark_path)
                    .context("Failed to fsync uninit mark file")
            })
            .with_context(|| {
                format!("Failed to crate uninit mark for timeline {tenant_id}/{timeline_id}")
            })?;

        let uninit_mark = TimelineUninitMark::new(uninit_mark_path, timeline_path);

        Ok(uninit_mark)
    }

    /// Gathers inputs from all of the timelines to produce a sizing model input.
    ///
    /// Future is cancellation safe. Only one calculation can be running at once per tenant.
    #[instrument(skip_all, fields(tenant_id=%self.tenant_id))]
    pub async fn gather_size_inputs(
        &self,
        // `max_retention_period` overrides the cutoff that is used to calculate the size
        // (only if it is shorter than the real cutoff).
        max_retention_period: Option<u64>,
        cause: LogicalSizeCalculationCause,
        ctx: &RequestContext,
    ) -> anyhow::Result<size::ModelInputs> {
        let logical_sizes_at_once = self
            .conf
            .concurrent_tenant_size_logical_size_queries
            .inner();

        // TODO: Having a single mutex block concurrent reads is not great for performance.
        //
        // But the only case where we need to run multiple of these at once is when we
        // request a size for a tenant manually via API, while another background calculation
        // is in progress (which is not a common case).
        //
        // See more for on the issue #2748 condenced out of the initial PR review.
        let mut shared_cache = self.cached_logical_sizes.lock().await;

        size::gather_inputs(
            self,
            logical_sizes_at_once,
            max_retention_period,
            &mut shared_cache,
            cause,
            ctx,
        )
        .await
    }

    /// Calculate synthetic tenant size and cache the result.
    /// This is periodically called by background worker.
    /// result is cached in tenant struct
    #[instrument(skip_all, fields(tenant_id=%self.tenant_id))]
    pub async fn calculate_synthetic_size(
        &self,
        cause: LogicalSizeCalculationCause,
        ctx: &RequestContext,
    ) -> anyhow::Result<u64> {
        let inputs = self.gather_size_inputs(None, cause, ctx).await?;

        let size = inputs.calculate()?;

        self.set_cached_synthetic_size(size);

        Ok(size)
    }

    /// Cache given synthetic size and update the metric value
    pub fn set_cached_synthetic_size(&self, size: u64) {
        self.cached_synthetic_tenant_size
            .store(size, Ordering::Relaxed);

        TENANT_SYNTHETIC_SIZE_METRIC
            .get_metric_with_label_values(&[&self.tenant_id.to_string()])
            .unwrap()
            .set(size);
    }

    pub fn get_cached_synthetic_size(&self) -> u64 {
        self.cached_synthetic_tenant_size.load(Ordering::Relaxed)
    }
}

fn remove_timeline_and_uninit_mark(timeline_dir: &Path, uninit_mark: &Path) -> anyhow::Result<()> {
    fs::remove_dir_all(timeline_dir)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                // we can leave the uninit mark without a timeline dir,
                // just remove the mark then
                Ok(())
            } else {
                Err(e)
            }
        })
        .with_context(|| {
            format!(
                "Failed to remove unit marked timeline directory {}",
                timeline_dir.display()
            )
        })?;
    fs::remove_file(uninit_mark).with_context(|| {
        format!(
            "Failed to remove timeline uninit mark file {}",
            uninit_mark.display()
        )
    })?;

    Ok(())
}

pub(crate) enum CreateTenantFilesMode {
    Create,
    Attach,
}

pub(crate) fn create_tenant_files(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    mode: CreateTenantFilesMode,
) -> anyhow::Result<PathBuf> {
    let target_tenant_directory = conf.tenant_path(&tenant_id);
    anyhow::ensure!(
        !target_tenant_directory
            .try_exists()
            .context("check existence of tenant directory")?,
        "tenant directory already exists",
    );

    let temporary_tenant_dir =
        path_with_suffix_extension(&target_tenant_directory, TEMP_FILE_SUFFIX);
    debug!(
        "Creating temporary directory structure in {}",
        temporary_tenant_dir.display()
    );

    // top-level dir may exist if we are creating it through CLI
    crashsafe::create_dir_all(&temporary_tenant_dir).with_context(|| {
        format!(
            "could not create temporary tenant directory {}",
            temporary_tenant_dir.display()
        )
    })?;

    let creation_result = try_create_target_tenant_dir(
        conf,
        tenant_conf,
        tenant_id,
        mode,
        &temporary_tenant_dir,
        &target_tenant_directory,
    );

    if creation_result.is_err() {
        error!("Failed to create directory structure for tenant {tenant_id}, cleaning tmp data");
        if let Err(e) = fs::remove_dir_all(&temporary_tenant_dir) {
            error!("Failed to remove temporary tenant directory {temporary_tenant_dir:?}: {e}")
        } else if let Err(e) = crashsafe::fsync(&temporary_tenant_dir) {
            error!(
                "Failed to fsync removed temporary tenant directory {temporary_tenant_dir:?}: {e}"
            )
        }
    }

    creation_result?;

    Ok(target_tenant_directory)
}

fn try_create_target_tenant_dir(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    mode: CreateTenantFilesMode,
    temporary_tenant_dir: &Path,
    target_tenant_directory: &Path,
) -> Result<(), anyhow::Error> {
    match mode {
        CreateTenantFilesMode::Create => {} // needs no attach marker, writing tenant conf + atomic rename of dir is good enough
        CreateTenantFilesMode::Attach => {
            let attach_marker_path = temporary_tenant_dir.join(TENANT_ATTACHING_MARKER_FILENAME);
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&attach_marker_path)
                .with_context(|| {
                    format!("could not create attach marker file {attach_marker_path:?}")
                })?;
            file.sync_all().with_context(|| {
                format!("could not sync attach marker file: {attach_marker_path:?}")
            })?;
            // fsync of the directory in which the file resides comes later in this function
        }
    }

    let temporary_tenant_timelines_dir = rebase_directory(
        &conf.timelines_path(&tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("resolve tenant {tenant_id} temporary timelines dir"))?;
    let temporary_tenant_config_path = rebase_directory(
        &conf.tenant_config_path(tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("resolve tenant {tenant_id} temporary config path"))?;

    Tenant::persist_tenant_config(&tenant_id, &temporary_tenant_config_path, tenant_conf, true)?;

    crashsafe::create_dir(&temporary_tenant_timelines_dir).with_context(|| {
        format!(
            "create tenant {} temporary timelines directory {}",
            tenant_id,
            temporary_tenant_timelines_dir.display()
        )
    })?;
    fail::fail_point!("tenant-creation-before-tmp-rename", |_| {
        anyhow::bail!("failpoint tenant-creation-before-tmp-rename");
    });

    // Make sure the current tenant directory entries are durable before renaming.
    // Without this, a crash may reorder any of the directory entry creations above.
    crashsafe::fsync(temporary_tenant_dir)
        .with_context(|| format!("sync temporary tenant directory {temporary_tenant_dir:?}"))?;

    fs::rename(temporary_tenant_dir, target_tenant_directory).with_context(|| {
        format!(
            "move tenant {} temporary directory {} into the permanent one {}",
            tenant_id,
            temporary_tenant_dir.display(),
            target_tenant_directory.display()
        )
    })?;
    let target_dir_parent = target_tenant_directory.parent().with_context(|| {
        format!(
            "get tenant {} dir parent for {}",
            tenant_id,
            target_tenant_directory.display()
        )
    })?;
    crashsafe::fsync(target_dir_parent).with_context(|| {
        format!(
            "fsync renamed directory's parent {} for tenant {}",
            target_dir_parent.display(),
            tenant_id,
        )
    })?;

    Ok(())
}

fn rebase_directory(original_path: &Path, base: &Path, new_base: &Path) -> anyhow::Result<PathBuf> {
    let relative_path = original_path.strip_prefix(base).with_context(|| {
        format!(
            "Failed to strip base prefix '{}' off path '{}'",
            base.display(),
            original_path.display()
        )
    })?;
    Ok(new_base.join(relative_path))
}

/// Create the cluster temporarily in 'initdbpath' directory inside the repository
/// to get bootstrap data for timeline initialization.
fn run_initdb(
    conf: &'static PageServerConf,
    initdb_target_dir: &Path,
    pg_version: u32,
) -> anyhow::Result<()> {
    let initdb_bin_path = conf.pg_bin_dir(pg_version)?.join("initdb");
    let initdb_lib_dir = conf.pg_lib_dir(pg_version)?;
    info!(
        "running {} in {}, libdir: {}",
        initdb_bin_path.display(),
        initdb_target_dir.display(),
        initdb_lib_dir.display(),
    );

    let initdb_output = Command::new(&initdb_bin_path)
        .args(["-D", &initdb_target_dir.to_string_lossy()])
        .args(["-U", &conf.superuser])
        .args(["-E", "utf8"])
        .arg("--no-instructions")
        // This is only used for a temporary installation that is deleted shortly after,
        // so no need to fsync it
        .arg("--no-sync")
        .env_clear()
        .env("LD_LIBRARY_PATH", &initdb_lib_dir)
        .env("DYLD_LIBRARY_PATH", &initdb_lib_dir)
        .stdout(Stdio::null())
        .output()
        .with_context(|| {
            format!(
                "failed to execute {} at target dir {}",
                initdb_bin_path.display(),
                initdb_target_dir.display()
            )
        })?;
    if !initdb_output.status.success() {
        bail!(
            "initdb failed: '{}'",
            String::from_utf8_lossy(&initdb_output.stderr)
        );
    }

    Ok(())
}

impl Drop for Tenant {
    fn drop(&mut self) {
        remove_tenant_metrics(&self.tenant_id);
    }
}
/// Dump contents of a layer file to stdout.
pub fn dump_layerfile_from_path(
    path: &Path,
    verbose: bool,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    use std::os::unix::fs::FileExt;

    // All layer files start with a two-byte "magic" value, to identify the kind of
    // file.
    let file = File::open(path)?;
    let mut header_buf = [0u8; 2];
    file.read_exact_at(&mut header_buf, 0)?;

    match u16::from_be_bytes(header_buf) {
        crate::IMAGE_FILE_MAGIC => ImageLayer::new_for_path(path, file)?.dump(verbose, ctx)?,
        crate::DELTA_FILE_MAGIC => DeltaLayer::new_for_path(path, file)?.dump(verbose, ctx)?,
        magic => bail!("unrecognized magic identifier: {:?}", magic),
    }

    Ok(())
}

fn ignore_absent_files<F>(fs_operation: F) -> io::Result<()>
where
    F: Fn() -> io::Result<()>,
{
    fs_operation().or_else(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(e)
        }
    })
}

#[cfg(test)]
pub mod harness {
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::OnceCell;
    use std::sync::Arc;
    use std::{fs, path::PathBuf};
    use utils::logging;
    use utils::lsn::Lsn;

    use crate::{
        config::PageServerConf,
        repository::Key,
        tenant::Tenant,
        walrecord::NeonWalRecord,
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::*;
    use crate::tenant::config::{TenantConf, TenantConfOpt};
    use hex_literal::hex;
    use utils::id::{TenantId, TimelineId};

    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Convenience function to create a page image with given string as the only content
    #[allow(non_snake_case)]
    pub fn TEST_IMG(s: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        buf.resize(64, 0);

        buf.freeze()
    }

    impl From<TenantConf> for TenantConfOpt {
        fn from(tenant_conf: TenantConf) -> Self {
            Self {
                checkpoint_distance: Some(tenant_conf.checkpoint_distance),
                checkpoint_timeout: Some(tenant_conf.checkpoint_timeout),
                compaction_target_size: Some(tenant_conf.compaction_target_size),
                compaction_period: Some(tenant_conf.compaction_period),
                compaction_threshold: Some(tenant_conf.compaction_threshold),
                gc_horizon: Some(tenant_conf.gc_horizon),
                gc_period: Some(tenant_conf.gc_period),
                image_creation_threshold: Some(tenant_conf.image_creation_threshold),
                pitr_interval: Some(tenant_conf.pitr_interval),
                walreceiver_connect_timeout: Some(tenant_conf.walreceiver_connect_timeout),
                lagging_wal_timeout: Some(tenant_conf.lagging_wal_timeout),
                max_lsn_wal_lag: Some(tenant_conf.max_lsn_wal_lag),
                trace_read_requests: Some(tenant_conf.trace_read_requests),
                eviction_policy: Some(tenant_conf.eviction_policy),
                min_resident_size_override: tenant_conf.min_resident_size_override,
                evictions_low_residence_duration_metric_threshold: Some(
                    tenant_conf.evictions_low_residence_duration_metric_threshold,
                ),
                gc_feedback: Some(tenant_conf.gc_feedback),
            }
        }
    }

    pub struct TenantHarness {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_id: TenantId,
    }

    static LOG_HANDLE: OnceCell<()> = OnceCell::new();

    impl TenantHarness {
        pub fn create(test_name: &'static str) -> anyhow::Result<Self> {
            LOG_HANDLE.get_or_init(|| {
                logging::init(
                    logging::LogFormat::Test,
                    // enable it in case in case the tests exercise code paths that use
                    // debug_assert_current_span_has_tenant_and_timeline_id
                    logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
                )
                .expect("Failed to init test logging")
            });

            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            // Disable automatic GC and compaction to make the unit tests more deterministic.
            // The tests perform them manually if needed.
            let tenant_conf = TenantConf {
                gc_period: Duration::ZERO,
                compaction_period: Duration::ZERO,
                ..TenantConf::default()
            };

            let tenant_id = TenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
            })
        }

        pub async fn load(&self) -> (Arc<Tenant>, RequestContext) {
            let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
            (
                self.try_load(&ctx)
                    .await
                    .expect("failed to load test tenant"),
                ctx,
            )
        }

        pub async fn try_load(&self, ctx: &RequestContext) -> anyhow::Result<Arc<Tenant>> {
            let walredo_mgr = Arc::new(TestRedoManager);

            let tenant = Arc::new(Tenant::new(
                TenantState::Loading,
                self.conf,
                TenantConfOpt::from(self.tenant_conf),
                walredo_mgr,
                self.tenant_id,
                None,
            ));
            tenant
                .load(None, ctx)
                .instrument(info_span!("try_load", tenant_id=%self.tenant_id))
                .await?;

            // TODO reuse Tenant::activate (needs broker)
            tenant.state.send_replace(TenantState::Active);
            for timeline in tenant.timelines.lock().unwrap().values() {
                timeline.set_state(TimelineState::Active);
            }
            Ok(tenant)
        }

        pub fn timeline_path(&self, timeline_id: &TimelineId) -> PathBuf {
            self.conf.timeline_path(timeline_id, &self.tenant_id)
        }
    }

    // Mock WAL redo manager that doesn't do much
    pub struct TestRedoManager;

    impl WalRedoManager for TestRedoManager {
        fn request_redo(
            &self,
            key: Key,
            lsn: Lsn,
            base_img: Option<(Lsn, Bytes)>,
            records: Vec<(Lsn, NeonWalRecord)>,
            _pg_version: u32,
        ) -> Result<Bytes, WalRedoError> {
            let s = format!(
                "redo for {} to get to {}, with {} and {} records",
                key,
                lsn,
                if base_img.is_some() {
                    "base image"
                } else {
                    "no base image"
                },
                records.len()
            );
            println!("{s}");

            Ok(TEST_IMG(&s))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace::KeySpaceAccum;
    use crate::repository::{Key, Value};
    use crate::tenant::harness::*;
    use crate::DEFAULT_PG_VERSION;
    use crate::METADATA_FILE_NAME;
    use bytes::BytesMut;
    use hex_literal::hex;
    use once_cell::sync::Lazy;
    use rand::{thread_rng, Rng};

    static TEST_KEY: Lazy<Key> =
        Lazy::new(|| Key::from_slice(&hex!("112222222233333333444444445500000001")));

    #[tokio::test]
    async fn test_basic() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_basic")?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let writer = tline.writer().await;
        writer
            .put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))
            .await?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let writer = tline.writer().await;
        writer
            .put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))
            .await?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x10), &ctx).await?,
            TEST_IMG("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x1f), &ctx).await?,
            TEST_IMG("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x20), &ctx).await?,
            TEST_IMG("foo at 0x20")
        );

        Ok(())
    }

    #[tokio::test]
    async fn no_duplicate_timelines() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("no_duplicate_timelines")?
            .load()
            .await;
        let _ = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        match tenant.create_empty_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx) {
            Ok(_) => panic!("duplicate timeline creation should fail"),
            Err(e) => assert_eq!(
                e.to_string(),
                format!(
                    "Timeline {}/{} already exists in pageserver's memory",
                    tenant.tenant_id, TIMELINE_ID
                )
            ),
        }

        Ok(())
    }

    /// Convenience function to create a page image with given string as the only content
    pub fn test_value(s: &str) -> Value {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(s.as_bytes());
        Value::Image(buf.freeze())
    }

    ///
    /// Test branch creation
    ///
    #[tokio::test]
    async fn test_branch() -> anyhow::Result<()> {
        use std::str::from_utf8;

        let (tenant, ctx) = TenantHarness::create("test_branch")?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let writer = tline.writer().await;

        #[allow(non_snake_case)]
        let TEST_KEY_A: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();
        #[allow(non_snake_case)]
        let TEST_KEY_B: Key = Key::from_hex("112222222233333333444444445500000002").unwrap();

        // Insert a value on the timeline
        writer
            .put(TEST_KEY_A, Lsn(0x20), &test_value("foo at 0x20"))
            .await?;
        writer
            .put(TEST_KEY_B, Lsn(0x20), &test_value("foobar at 0x20"))
            .await?;
        writer.finish_write(Lsn(0x20));

        writer
            .put(TEST_KEY_A, Lsn(0x30), &test_value("foo at 0x30"))
            .await?;
        writer.finish_write(Lsn(0x30));
        writer
            .put(TEST_KEY_A, Lsn(0x40), &test_value("foo at 0x40"))
            .await?;
        writer.finish_write(Lsn(0x40));

        //assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x30)), &ctx)
            .await?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");
        let new_writer = newtline.writer().await;
        new_writer
            .put(TEST_KEY_A, Lsn(0x40), &test_value("bar at 0x40"))
            .await?;
        new_writer.finish_write(Lsn(0x40));

        // Check page contents on both branches
        assert_eq!(
            from_utf8(&tline.get(TEST_KEY_A, Lsn(0x40), &ctx).await?)?,
            "foo at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_A, Lsn(0x40), &ctx).await?)?,
            "bar at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_B, Lsn(0x40), &ctx).await?)?,
            "foobar at 0x20"
        );

        //assert_current_logical_size(&tline, Lsn(0x40));

        Ok(())
    }

    async fn make_some_layers(tline: &Timeline, start_lsn: Lsn) -> anyhow::Result<()> {
        let mut lsn = start_lsn;
        #[allow(non_snake_case)]
        {
            let writer = tline.writer().await;
            // Create a relation on the timeline
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
        }
        tline.freeze_and_flush().await?;
        {
            let writer = tline.writer().await;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
                )
                .await?;
            writer.finish_write(lsn);
        }
        tline.freeze_and_flush().await
    }

    #[tokio::test]
    async fn test_prohibit_branch_creation_on_garbage_collected_data() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?
                .load()
                .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the flushing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, &ctx)
            .await?;

        // try to branch at lsn 25, should fail because we already garbage collected the data
        match tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x25)), &ctx)
            .await
        {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(err.to_string().contains("invalid branch start lsn"));
                assert!(err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("we might've already garbage collected needed data"))
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_prohibit_branch_creation_on_pre_initdb_lsn() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?
                .load()
                .await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x50), DEFAULT_PG_VERSION, &ctx)
            .await?;
        // try to branch at lsn 0x25, should fail because initdb lsn is 0x50
        match tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x25)), &ctx)
            .await
        {
            Ok(_) => panic!("branching should have failed"),
            Err(err) => {
                assert!(&err.to_string().contains("invalid branch start lsn"));
                assert!(&err
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("is earlier than latest GC horizon"));
            }
        }

        Ok(())
    }

    /*
    // FIXME: This currently fails to error out. Calling GC doesn't currently
    // remove the old value, we'd need to work a little harder
    #[tokio::test]
    async fn test_prohibit_get_for_garbage_collected_data() -> anyhow::Result<()> {
        let repo =
            RepoHarness::create("test_prohibit_get_for_garbage_collected_data")?
            .load();

        let tline = repo.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO)?;
        let latest_gc_cutoff_lsn = tline.get_latest_gc_cutoff_lsn();
        assert!(*latest_gc_cutoff_lsn > Lsn(0x25));
        match tline.get(*TEST_KEY, Lsn(0x25)) {
            Ok(_) => panic!("request for page should have failed"),
            Err(err) => assert!(err.to_string().contains("not found at")),
        }
        Ok(())
    }
     */

    #[tokio::test]
    async fn test_get_branchpoints_from_an_inactive_timeline() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_get_branchpoints_from_an_inactive_timeline")?
                .load()
                .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
            .await?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60)).await?;

        tline.set_broken("test".to_owned());

        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, &ctx)
            .await?;

        // The branchpoints should contain all timelines, even ones marked
        // as Broken.
        {
            let branchpoints = &tline.gc_info.read().unwrap().retain_lsns;
            assert_eq!(branchpoints.len(), 1);
            assert_eq!(branchpoints[0], Lsn(0x40));
        }

        // You can read the key from the child branch even though the parent is
        // Broken, as long as you don't need to access data from the parent.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x70), &ctx).await?,
            TEST_IMG(&format!("foo at {}", Lsn(0x70)))
        );

        // This needs to traverse to the parent, and fails.
        let err = newtline.get(*TEST_KEY, Lsn(0x50), &ctx).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("will not become active. Current state: Broken"));

        Ok(())
    }

    #[tokio::test]
    async fn test_retain_data_in_parent_which_is_needed_for_child() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?
                .load()
                .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
            .await?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");
        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, &ctx)
            .await?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25), &ctx).await.is_ok());

        Ok(())
    }
    #[tokio::test]
    async fn test_parent_keeps_data_forever_after_branching() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_parent_keeps_data_forever_after_branching")?
                .load()
                .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
            .await?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60)).await?;

        // run gc on parent
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, &ctx)
            .await?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50), &ctx).await?,
            TEST_IMG(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn timeline_load() -> anyhow::Result<()> {
        const TEST_NAME: &str = "timeline_load";
        let harness = TenantHarness::create(TEST_NAME)?;
        {
            let (tenant, ctx) = harness.load().await;
            let tline = tenant
                .create_test_timeline(TIMELINE_ID, Lsn(0x7000), DEFAULT_PG_VERSION, &ctx)
                .await?;
            make_some_layers(tline.as_ref(), Lsn(0x8000)).await?;
        }

        let (tenant, _ctx) = harness.load().await;
        tenant
            .get_timeline(TIMELINE_ID, true)
            .expect("cannot load timeline");

        Ok(())
    }

    #[tokio::test]
    async fn timeline_load_with_ancestor() -> anyhow::Result<()> {
        const TEST_NAME: &str = "timeline_load_with_ancestor";
        let harness = TenantHarness::create(TEST_NAME)?;
        // create two timelines
        {
            let (tenant, ctx) = harness.load().await;
            let tline = tenant
                .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
                .await?;

            make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

            let child_tline = tenant
                .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
                .await?;
            child_tline.set_state(TimelineState::Active);

            let newtline = tenant
                .get_timeline(NEW_TIMELINE_ID, true)
                .expect("Should have a local timeline");

            make_some_layers(newtline.as_ref(), Lsn(0x60)).await?;
        }

        // check that both of them are initially unloaded
        let (tenant, _ctx) = harness.load().await;

        // check that both, child and ancestor are loaded
        let _child_tline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("cannot get child timeline loaded");

        let _ancestor_tline = tenant
            .get_timeline(TIMELINE_ID, true)
            .expect("cannot get ancestor timeline loaded");

        Ok(())
    }

    #[tokio::test]
    async fn corrupt_metadata() -> anyhow::Result<()> {
        const TEST_NAME: &str = "corrupt_metadata";
        let harness = TenantHarness::create(TEST_NAME)?;
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        drop(tline);
        drop(tenant);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[8] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let err = harness.try_load(&ctx).await.err().expect("should fail");
        // get all the stack with all .context, not tonly the last one
        let message = format!("{err:#}");
        let expected = "Failed to parse metadata bytes from path";
        assert!(
            message.contains(expected),
            "message '{message}' expected to contain {expected}"
        );

        let mut found_error_message = false;
        let mut err_source = err.source();
        while let Some(source) = err_source {
            if source.to_string() == "metadata checksum mismatch" {
                found_error_message = true;
                break;
            }
            err_source = source.source();
        }
        assert!(
            found_error_message,
            "didn't find the corrupted metadata error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_images() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_images")?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let writer = tline.writer().await;
        writer
            .put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))
            .await?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&ctx).await?;

        let writer = tline.writer().await;
        writer
            .put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))
            .await?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&ctx).await?;

        let writer = tline.writer().await;
        writer
            .put(*TEST_KEY, Lsn(0x30), &Value::Image(TEST_IMG("foo at 0x30")))
            .await?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&ctx).await?;

        let writer = tline.writer().await;
        writer
            .put(*TEST_KEY, Lsn(0x40), &Value::Image(TEST_IMG("foo at 0x40")))
            .await?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&ctx).await?;

        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x10), &ctx).await?,
            TEST_IMG("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x1f), &ctx).await?,
            TEST_IMG("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x20), &ctx).await?,
            TEST_IMG("foo at 0x20")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x30), &ctx).await?,
            TEST_IMG("foo at 0x30")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x40), &ctx).await?,
            TEST_IMG("foo at 0x40")
        );

        Ok(())
    }

    //
    // Insert 1000 key-value pairs with increasing keys, flush, compact, GC.
    // Repeat 50 times.
    //
    #[tokio::test]
    async fn test_bulk_insert() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_bulk_insert")?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let mut lsn = Lsn(0x10);

        let mut keyspace = KeySpaceAccum::new();

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        let mut blknum = 0;
        for _ in 0..50 {
            for _ in 0..10000 {
                test_key.field6 = blknum;
                let writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                    )
                    .await?;
                writer.finish_write(lsn);
                drop(writer);

                keyspace.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            let cutoff = tline.get_last_record_lsn();

            tline
                .update_gc_info(Vec::new(), cutoff, Duration::ZERO, &ctx)
                .await?;
            tline.freeze_and_flush().await?;
            tline.compact(&ctx).await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_random_updates() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_random_updates")?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0x10);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )
                .await?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        for _ in 0..50 {
            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                    )
                    .await?;
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn, &ctx).await?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of flush, compact, and GC
            let cutoff = tline.get_last_record_lsn();
            tline
                .update_gc_info(Vec::new(), cutoff, Duration::ZERO, &ctx)
                .await?;
            tline.freeze_and_flush().await?;
            tline.compact(&ctx).await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_branches() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_traverse_branches")?
            .load()
            .await;
        let mut tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0x10);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )
                .await?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        for _ in 0..50 {
            let new_tline_id = TimelineId::generate();
            tenant
                .branch_timeline_test(&tline, new_tline_id, Some(lsn), &ctx)
                .await?;
            tline = tenant
                .get_timeline(new_tline_id, true)
                .expect("Should have the branched timeline");

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                    )
                    .await?;
                println!("updating {} at {}", blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn, &ctx).await?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of flush, compact, and GC
            let cutoff = tline.get_last_record_lsn();
            tline
                .update_gc_info(Vec::new(), cutoff, Duration::ZERO, &ctx)
                .await?;
            tline.freeze_and_flush().await?;
            tline.compact(&ctx).await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_ancestors() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_traverse_ancestors")?
            .load()
            .await;
        let mut tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        // Track page mutation lsns across different timelines.
        let mut updated = [[Lsn(0); NUM_KEYS]; NUM_TLINES];

        let mut lsn = Lsn(0x10);

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_TLINES {
            let new_tline_id = TimelineId::generate();
            tenant
                .branch_timeline_test(&tline, new_tline_id, Some(lsn), &ctx)
                .await?;
            tline = tenant
                .get_timeline(new_tline_id, true)
                .expect("Should have the branched timeline");

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(TEST_IMG(&format!("{} {} at {}", idx, blknum, lsn))),
                    )
                    .await?;
                println!("updating [{}][{}] at {}", idx, blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[idx][blknum] = lsn;
            }
        }

        // Read pages from leaf timeline across all ancestors.
        for (idx, lsns) in updated.iter().enumerate() {
            for (blknum, lsn) in lsns.iter().enumerate() {
                // Skip empty mutations.
                if lsn.0 == 0 {
                    continue;
                }
                println!("checking [{idx}][{blknum}] at {lsn}");
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, *lsn, &ctx).await?,
                    TEST_IMG(&format!("{idx} {blknum} at {lsn}"))
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_write_at_initdb_lsn_takes_optimization_code_path() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_empty_test_timeline_is_usable")?
            .load()
            .await;

        let initdb_lsn = Lsn(0x20);
        let utline =
            tenant.create_empty_timeline(TIMELINE_ID, initdb_lsn, DEFAULT_PG_VERSION, &ctx)?;
        let tline = utline.raw_timeline().unwrap();

        // Spawn flush loop now so that we can set the `expect_initdb_optimization`
        tline.maybe_spawn_flush_loop();

        // Make sure the timeline has the minimum set of required keys for operation.
        // The only operation you can always do on an empty timeline is to `put` new data.
        // Except if you `put` at `initdb_lsn`.
        // In that case, there's an optimization to directly create image layers instead of delta layers.
        // It uses `repartition()`, which assumes some keys to be present.
        // Let's make sure the test timeline can handle that case.
        {
            let mut state = tline.flush_loop_state.lock().unwrap();
            assert_eq!(
                timeline::FlushLoopState::Running {
                    expect_initdb_optimization: false,
                    initdb_optimization_count: 0,
                },
                *state
            );
            *state = timeline::FlushLoopState::Running {
                expect_initdb_optimization: true,
                initdb_optimization_count: 0,
            };
        }

        // Make writes at the initdb_lsn. When we flush it below, it should be handled by the optimization.
        // As explained above, the optimization requires some keys to be present.
        // As per `create_empty_timeline` documentation, use init_empty to set them.
        // This is what `create_test_timeline` does, by the way.
        let mut modification = tline.begin_modification(initdb_lsn);
        modification
            .init_empty_test_timeline()
            .context("init_empty_test_timeline")?;
        modification
            .commit()
            .await
            .context("commit init_empty_test_timeline modification")?;

        // Do the flush. The flush code will check the expectations that we set above.
        tline.freeze_and_flush().await?;

        // assert freeze_and_flush exercised the initdb optimization
        {
            let state = tline.flush_loop_state.lock().unwrap();
            let
                timeline::FlushLoopState::Running {
                    expect_initdb_optimization,
                    initdb_optimization_count,
                } = *state else {
                    panic!("unexpected state: {:?}", *state);
                };
            assert!(expect_initdb_optimization);
            assert!(initdb_optimization_count > 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_uninit_mark_crash() -> anyhow::Result<()> {
        let name = "test_uninit_mark_crash";
        let harness = TenantHarness::create(name)?;
        {
            let (tenant, ctx) = harness.load().await;
            let tline =
                tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION, &ctx)?;
            // Keeps uninit mark in place
            std::mem::forget(tline);
        }

        let (tenant, _) = harness.load().await;
        match tenant.get_timeline(TIMELINE_ID, false) {
            Ok(_) => panic!("timeline should've been removed during load"),
            Err(e) => {
                assert_eq!(
                    e,
                    GetTimelineError::NotFound {
                        tenant_id: tenant.tenant_id,
                        timeline_id: TIMELINE_ID,
                    }
                )
            }
        }

        assert!(!harness
            .conf
            .timeline_path(&TIMELINE_ID, &tenant.tenant_id)
            .exists());

        assert!(!harness
            .conf
            .timeline_uninit_mark_file_path(tenant.tenant_id, TIMELINE_ID)
            .exists());

        Ok(())
    }
}

#[cfg(not(debug_assertions))]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_id() {}

#[cfg(debug_assertions)]
pub static TENANT_ID_EXTRACTOR: once_cell::sync::Lazy<
    utils::tracing_span_assert::MultiNameExtractor<2>,
> = once_cell::sync::Lazy::new(|| {
    utils::tracing_span_assert::MultiNameExtractor::new("TenantId", ["tenant_id", "tenant"])
});

#[cfg(debug_assertions)]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_id() {
    use utils::tracing_span_assert;

    match tracing_span_assert::check_fields_present([&*TENANT_ID_EXTRACTOR]) {
        Ok(()) => (),
        Err(missing) => panic!(
            "missing extractors: {:?}",
            missing.into_iter().map(|e| e.name()).collect::<Vec<_>>()
        ),
    }
}
