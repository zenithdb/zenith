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

use anyhow::{bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use futures::FutureExt;
use pageserver_api::models::TimelineState;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use storage_broker::BrokerClientChannel;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::completion;
use utils::crashsafe::path_with_suffix_extension;

use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fs;
use std::fs::File;
use std::io;
use std::ops::Bound::Included;
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use self::config::AttachedLocationConfig;
use self::config::LocationConf;
use self::config::TenantConf;
use self::delete::DeleteTenantFlow;
use self::metadata::LoadMetadataError;
use self::metadata::TimelineMetadata;
use self::mgr::TenantsMap;
use self::remote_timeline_client::RemoteTimelineClient;
use self::timeline::uninit::TimelineUninitMark;
use self::timeline::uninit::UninitializedTimeline;
use self::timeline::EvictionTaskTenantState;
use self::timeline::TimelineResources;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::deletion_queue::DeletionQueueClient;
use crate::import_datadir;
use crate::is_uninit_mark;
use crate::metrics::TENANT_ACTIVATION;
use crate::metrics::{remove_tenant_metrics, TENANT_STATE_METRIC, TENANT_SYNTHETIC_SIZE_METRIC};
use crate::repository::GcResult;
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::tenant::config::LocationMode;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::metadata::load_metadata;
pub use crate::tenant::remote_timeline_client::index::IndexPart;
use crate::tenant::remote_timeline_client::MaybeDeletedIndexPart;
use crate::tenant::storage_layer::DeltaLayer;
use crate::tenant::storage_layer::ImageLayer;
use crate::InitializationOrder;

use crate::tenant::timeline::delete::DeleteTimelineFlow;
use crate::tenant::timeline::uninit::cleanup_timeline_directory;
use crate::virtual_file::VirtualFile;
use crate::walredo::PostgresRedoManager;
use crate::walredo::WalRedoManager;
use crate::TEMP_FILE_SUFFIX;
pub use pageserver_api::models::TenantState;

use toml_edit;
use utils::{
    crashsafe,
    generation::Generation,
    id::{TenantId, TimelineId},
    lsn::{Lsn, RecordLsn},
};

/// Declare a failpoint that can use the `pause` failpoint action.
/// We don't want to block the executor thread, hence, spawn_blocking + await.
macro_rules! pausable_failpoint {
    ($name:literal) => {
        if cfg!(feature = "testing") {
            tokio::task::spawn_blocking({
                let current = tracing::Span::current();
                move || {
                    let _entered = current.entered();
                    tracing::info!("at failpoint {}", $name);
                    fail::fail_point!($name);
                }
            })
            .await
            .expect("spawn_blocking");
        }
    };
}

pub mod blob_io;
pub mod block_io;

pub mod disk_btree;
pub(crate) mod ephemeral_file;
pub mod layer_map;
mod span;

pub mod metadata;
mod par_fsync;
pub mod remote_timeline_client;
pub mod storage_layer;

pub mod config;
pub mod delete;
pub mod mgr;
pub mod tasks;
pub mod upload_queue;

pub(crate) mod timeline;

pub mod size;

pub(crate) use timeline::span::debug_assert_current_span_has_tenant_and_timeline_id;
pub use timeline::{
    LocalLayerInfoForDiskUsageEviction, LogicalSizeCalculationCause, PageReconstructError, Timeline,
};

// re-export for use in remote_timeline_client.rs
pub use crate::tenant::metadata::save_metadata;

// re-export for use in walreceiver
pub use crate::tenant::timeline::WalReceiverInfo;

/// The "tenants" part of `tenants/<tenant>/timelines...`
pub const TENANTS_SEGMENT_NAME: &str = "tenants";

/// Parts of the `.neon/tenants/<tenant_id>/timelines/<timeline_id>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

pub const TENANT_ATTACHING_MARKER_FILENAME: &str = "attaching";

pub const TENANT_DELETED_MARKER_FILE_NAME: &str = "deleted";

/// References to shared objects that are passed into each tenant, such
/// as the shared remote storage client and process initialization state.
#[derive(Clone)]
pub struct TenantSharedResources {
    pub broker_client: storage_broker::BrokerClientChannel,
    pub remote_storage: Option<GenericRemoteStorage>,
    pub deletion_queue_client: DeletionQueueClient,
}

/// A [`Tenant`] is really an _attached_ tenant.  The configuration
/// for an attached tenant is a subset of the [`LocationConf`], represented
/// in this struct.
pub(super) struct AttachedTenantConf {
    tenant_conf: TenantConfOpt,
    location: AttachedLocationConfig,
}

impl AttachedTenantConf {
    fn try_from(location_conf: LocationConf) -> anyhow::Result<Self> {
        match &location_conf.mode {
            LocationMode::Attached(attach_conf) => Ok(Self {
                tenant_conf: location_conf.tenant_conf,
                location: attach_conf.clone(),
            }),
            LocationMode::Secondary(_) => {
                anyhow::bail!("Attempted to construct AttachedTenantConf from a LocationConf in secondary mode")
            }
        }
    }
}

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
    // We keep TenantConfOpt struct here to preserve the information
    // about parameters that are not set.
    // This is necessary to allow global config updates.
    tenant_conf: Arc<RwLock<AttachedTenantConf>>,

    tenant_id: TenantId,

    /// The remote storage generation, used to protect S3 objects from split-brain.
    /// Does not change over the lifetime of the [`Tenant`] object.
    ///  
    /// This duplicates the generation stored in LocationConf, but that structure is mutable:
    /// this copy enforces the invariant that generatio doesn't change during a Tenant's lifetime.
    generation: Generation,

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
    pub(crate) remote_storage: Option<GenericRemoteStorage>,

    // Access to global deletion queue for when this tenant wants to schedule a deletion
    deletion_queue_client: DeletionQueueClient,

    /// Cached logical sizes updated updated on each [`Tenant::gather_size_inputs`].
    cached_logical_sizes: tokio::sync::Mutex<HashMap<(TimelineId, Lsn), u64>>,
    cached_synthetic_tenant_size: Arc<AtomicU64>,

    eviction_task_tenant_state: tokio::sync::Mutex<EvictionTaskTenantState>,

    pub(crate) delete_progress: Arc<tokio::sync::Mutex<DeleteTenantFlow>>,
}

// We should not blindly overwrite local metadata with remote one.
// For example, consider the following case:
//     Image layer is flushed to disk as a new delta layer, we update local metadata and start upload task but after that
//     pageserver crashes. During startup we'll load new metadata, and then reset it
//     to the state of remote one. But current layermap will have layers from the old
//     metadata which is inconsistent.
//     And with current logic it won't discard them during load because during layermap
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
pub enum LoadLocalTimelineError {
    #[error("FailedToLoad")]
    Load(#[source] anyhow::Error),
    #[error("FailedToResumeDeletion")]
    ResumeDeletion(#[source] anyhow::Error),
}

#[derive(thiserror::Error)]
pub enum DeleteTimelineError {
    #[error("NotFound")]
    NotFound,

    #[error("HasChildren")]
    HasChildren(Vec<TimelineId>),

    #[error("Timeline deletion is already in progress")]
    AlreadyInProgress(Arc<tokio::sync::Mutex<DeleteTimelineFlow>>),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Debug for DeleteTimelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "NotFound"),
            Self::HasChildren(c) => f.debug_tuple("HasChildren").field(c).finish(),
            Self::AlreadyInProgress(_) => f.debug_tuple("AlreadyInProgress").finish(),
            Self::Other(e) => f.debug_tuple("Other").field(e).finish(),
        }
    }
}

pub enum SetStoppingError {
    AlreadyStopping(completion::Barrier),
    Broken,
}

impl Debug for SetStoppingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyStopping(_) => f.debug_tuple("AlreadyStopping").finish(),
            Self::Broken => write!(f, "Broken"),
        }
    }
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

#[derive(thiserror::Error, Debug)]
pub enum CreateTimelineError {
    #[error("a timeline with the given ID already exists")]
    AlreadyExists,
    #[error(transparent)]
    AncestorLsn(anyhow::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

struct TenantDirectoryScan {
    sorted_timelines_to_load: Vec<(TimelineId, TimelineMetadata)>,
    timelines_to_resume_deletion: Vec<(TimelineId, Option<TimelineMetadata>)>,
}

enum CreateTimelineCause {
    Load,
    Delete,
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
        resources: TimelineResources,
        remote_startup_data: Option<RemoteStartupData>,
        local_metadata: Option<TimelineMetadata>,
        ancestor: Option<Arc<Timeline>>,
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
            resources,
            init_order,
            CreateTimelineCause::Load,
        )?;
        let disk_consistent_lsn = timeline.get_disk_consistent_lsn();
        anyhow::ensure!(
            disk_consistent_lsn.is_valid(),
            "Timeline {tenant_id}/{timeline_id} has invalid disk_consistent_lsn"
        );
        assert_eq!(
            disk_consistent_lsn,
            up_to_date_metadata.disk_consistent_lsn(),
            "these are used interchangeably"
        );

        // Save the metadata file to local disk.
        if !picked_local {
            save_metadata(self.conf, &tenant_id, &timeline_id, up_to_date_metadata)
                .await
                .context("save_metadata")?;
        }

        let index_part = remote_startup_data.as_ref().map(|x| &x.index_part);

        if let Some(index_part) = index_part {
            timeline
                .remote_client
                .as_ref()
                .unwrap()
                .init_upload_queue(index_part)?;
        } else if self.remote_storage.is_some() {
            // No data on the remote storage, but we have local metadata file. We can end up
            // here with timeline_create being interrupted before finishing index part upload.
            // By doing what we do here, the index part upload is retried.
            // If control plane retries timeline creation in the meantime, the mgmt API handler
            // for timeline creation will coalesce on the upload we queue here.
            let rtc = timeline.remote_client.as_ref().unwrap();
            rtc.init_upload_queue_for_empty_remote(up_to_date_metadata)?;
            rtc.schedule_index_upload_for_metadata_update(up_to_date_metadata)?;
        }

        timeline
            .load_layer_map(
                disk_consistent_lsn,
                remote_startup_data.map(|x| x.index_part),
            )
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

        // Sanity check: a timeline should have some content.
        anyhow::ensure!(
            ancestor.is_some()
                || timeline
                    .layers
                    .read()
                    .await
                    .layer_map()
                    .iter_historic_layers()
                    .next()
                    .is_some(),
            "Timeline has no ancestor and no layer files"
        );

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
        resources: TenantSharedResources,
        attached_conf: AttachedTenantConf,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Tenant>> {
        // TODO dedup with spawn_load
        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));

        let TenantSharedResources {
            broker_client,
            remote_storage,
            deletion_queue_client,
        } = resources;

        let tenant = Arc::new(Tenant::new(
            TenantState::Attaching,
            conf,
            attached_conf,
            wal_redo_manager,
            tenant_id,
            remote_storage.clone(),
            deletion_queue_client,
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
                // Ideally we should use Tenant::set_broken_no_wait, but it is not supposed to be used when tenant is in loading state.
                let make_broken = |t: &Tenant, err: anyhow::Error| {
                    error!("attach failed, setting tenant state to Broken: {err:?}");
                    t.state.send_modify(|state| {
                        assert_eq!(
                            *state,
                            TenantState::Attaching,
                            "the attach task owns the tenant state until activation is complete"
                        );
                        *state = TenantState::broken_from_reason(err.to_string());
                    });
                };

                let pending_deletion = {
                    match DeleteTenantFlow::should_resume_deletion(
                        conf,
                        remote_storage.as_ref(),
                        &tenant_clone,
                    )
                    .await
                    {
                        Ok(should_resume_deletion) => should_resume_deletion,
                        Err(err) => {
                            make_broken(&tenant_clone, anyhow::anyhow!(err));
                            return Ok(());
                        }
                    }
                };

                info!("pending_deletion {}", pending_deletion.is_some());

                if let Some(deletion) = pending_deletion {
                    match DeleteTenantFlow::resume_from_attach(
                        deletion,
                        &tenant_clone,
                        tenants,
                        &ctx,
                    )
                    .await
                    {
                        Err(err) => {
                            make_broken(&tenant_clone, anyhow::anyhow!(err));
                            return Ok(());
                        }
                        Ok(()) => return Ok(()),
                    }
                }

                match tenant_clone.attach(&ctx).await {
                    Ok(()) => {
                        info!("attach finished, activating");
                        tenant_clone.activate(broker_client, None, &ctx);
                    }
                    Err(e) => {
                        make_broken(&tenant_clone, anyhow::anyhow!(e));
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
        span::debug_assert_current_span_has_tenant_id();

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

        let remote_timeline_ids =
            remote_timeline_client::list_remote_timelines(remote_storage, self.tenant_id).await?;

        info!("found {} timelines", remote_timeline_ids.len());

        // Download & parse index parts
        let mut part_downloads = JoinSet::new();
        for timeline_id in remote_timeline_ids {
            let client = RemoteTimelineClient::new(
                remote_storage.clone(),
                self.deletion_queue_client.clone(),
                self.conf,
                self.tenant_id,
                timeline_id,
                self.generation,
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
                .instrument(info_span!("download_index_part", %timeline_id)),
            );
        }

        let mut timelines_to_resume_deletions = vec![];

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
                    timeline_ancestors.insert(timeline_id, index_part.metadata.clone());
                    remote_index_and_client.insert(timeline_id, (index_part, client));
                }
                MaybeDeletedIndexPart::Deleted(index_part) => {
                    info!(
                        "timeline {} is deleted, picking to resume deletion",
                        timeline_id
                    );
                    timelines_to_resume_deletions.push((timeline_id, index_part, client));
                }
            }
        }

        // For every timeline, download the metadata file, scan the local directory,
        // and build a layer map that contains an entry for each remote and local
        // layer file.
        let sorted_timelines = tree_sort_timelines(timeline_ancestors, |m| m.ancestor_timeline())?;
        for (timeline_id, remote_metadata) in sorted_timelines {
            let (index_part, remote_client) = remote_index_and_client
                .remove(&timeline_id)
                .expect("just put it in above");

            // TODO again handle early failure
            self.load_remote_timeline(
                timeline_id,
                index_part,
                remote_metadata,
                TimelineResources {
                    remote_client: Some(remote_client),
                    deletion_queue_client: self.deletion_queue_client.clone(),
                },
                ctx,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to load remote timeline {} for tenant {}",
                    timeline_id, self.tenant_id
                )
            })?;
        }

        // Walk through deleted timelines, resume deletion
        for (timeline_id, index_part, remote_timeline_client) in timelines_to_resume_deletions {
            remote_timeline_client
                .init_upload_queue_stopped_to_continue_deletion(&index_part)
                .context("init queue stopped")
                .map_err(LoadLocalTimelineError::ResumeDeletion)?;

            DeleteTimelineFlow::resume_deletion(
                Arc::clone(self),
                timeline_id,
                &index_part.metadata,
                Some(remote_timeline_client),
                self.deletion_queue_client.clone(),
                None,
            )
            .await
            .context("resume_deletion")
            .map_err(LoadLocalTimelineError::ResumeDeletion)?;
        }

        std::fs::remove_file(&marker_file)
            .with_context(|| format!("unlink attach marker file {marker_file}"))?;
        crashsafe::fsync(marker_file.parent().expect("marker file has parent dir"))
            .context("fsync tenant directory after unlinking attach marker file")?;

        crate::failpoint_support::sleep_millis_async!("attach-before-activate");

        info!("Done");

        Ok(())
    }

    /// Get sum of all remote timelines sizes
    ///
    /// This function relies on the index_part instead of listing the remote storage
    pub fn remote_size(&self) -> u64 {
        let mut size = 0;

        for timeline in self.list_timelines() {
            if let Some(remote_client) = &timeline.remote_client {
                size += remote_client.get_remote_physical_size();
            }
        }

        size
    }

    #[instrument(skip_all, fields(timeline_id=%timeline_id))]
    async fn load_remote_timeline(
        &self,
        timeline_id: TimelineId,
        index_part: IndexPart,
        remote_metadata: TimelineMetadata,
        resources: TimelineResources,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        span::debug_assert_current_span_has_tenant_id();

        info!("downloading index file for timeline {}", timeline_id);
        tokio::fs::create_dir_all(self.conf.timeline_path(&self.tenant_id, &timeline_id))
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
            resources,
            Some(RemoteStartupData {
                index_part,
                remote_metadata,
            }),
            local_metadata,
            ancestor,
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
            AttachedTenantConf::try_from(LocationConf::default()).unwrap(),
            wal_redo_manager,
            tenant_id,
            None,
            DeletionQueueClient::broken(),
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
    pub(crate) fn spawn_load(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        attached_conf: AttachedTenantConf,
        resources: TenantSharedResources,
        init_order: Option<InitializationOrder>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        ctx: &RequestContext,
    ) -> Arc<Tenant> {
        span::debug_assert_current_span_has_tenant_id();

        let broker_client = resources.broker_client;
        let remote_storage = resources.remote_storage;

        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        let tenant = Tenant::new(
            TenantState::Loading,
            conf,
            attached_conf,
            wal_redo_manager,
            tenant_id,
            remote_storage.clone(),
            resources.deletion_queue_client.clone(),
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
                // Ideally we should use Tenant::set_broken_no_wait, but it is not supposed to be used when tenant is in loading state.
                let make_broken = |t: &Tenant, err: anyhow::Error| {
                    error!("load failed, setting tenant state to Broken: {err:?}");
                    t.state.send_modify(|state| {
                        assert!(
                            matches!(*state, TenantState::Loading | TenantState::Stopping { .. }),
                            "the loading task owns the tenant state until activation is complete"
                        );
                        *state = TenantState::broken_from_reason(err.to_string());
                    });
                };

                let mut init_order = init_order;

                // take the completion because initial tenant loading will complete when all of
                // these tasks complete.
                let _completion = init_order
                    .as_mut()
                    .and_then(|x| x.initial_tenant_load.take());

                // Don't block pageserver startup on figuring out deletion status
                let pending_deletion = {
                    match DeleteTenantFlow::should_resume_deletion(
                        conf,
                        remote_storage.as_ref(),
                        &tenant_clone,
                    )
                    .await
                    {
                        Ok(should_resume_deletion) => should_resume_deletion,
                        Err(err) => {
                            make_broken(&tenant_clone, anyhow::anyhow!(err));
                            return Ok(());
                        }
                    }
                };

                info!("pending deletion {}", pending_deletion.is_some());

                if let Some(deletion) = pending_deletion {
                    // as we are no longer loading, signal completion by dropping
                    // the completion while we resume deletion
                    drop(_completion);
                    // do not hold to initial_logical_size_attempt as it will prevent loading from proceeding without timeout
                    let _ = init_order
                        .as_mut()
                        .and_then(|x| x.initial_logical_size_attempt.take());

                    match DeleteTenantFlow::resume_from_load(
                        deletion,
                        &tenant_clone,
                        init_order.as_ref(),
                        tenants,
                        &ctx,
                    )
                    .await
                    {
                        Err(err) => {
                            make_broken(&tenant_clone, anyhow::anyhow!(err));
                            return Ok(());
                        }
                        Ok(()) => return Ok(()),
                    }
                }

                let background_jobs_can_start =
                    init_order.as_ref().map(|x| &x.background_jobs_can_start);

                match tenant_clone.load(init_order.as_ref(), &ctx).await {
                    Ok(()) => {
                        debug!("load finished");

                        tenant_clone.activate(broker_client, background_jobs_can_start, &ctx);
                    }
                    Err(err) => make_broken(&tenant_clone, err),
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

    fn scan_and_sort_timelines_dir(self: Arc<Tenant>) -> anyhow::Result<TenantDirectoryScan> {
        let mut timelines_to_load: HashMap<TimelineId, TimelineMetadata> = HashMap::new();
        // Note timelines_to_resume_deletion needs to be separate because it can be not sortable
        // from the point of `tree_sort_timelines`. I e some parents can be missing because deletion
        // completed in non topological order (for example because parent has smaller number of layer files in it)
        let mut timelines_to_resume_deletion: Vec<(TimelineId, Option<TimelineMetadata>)> = vec![];

        let timelines_dir = self.conf.timelines_path(&self.tenant_id);

        for entry in timelines_dir
            .read_dir_utf8()
            .context("list timelines directory for tenant")?
        {
            let entry = entry.context("read timeline dir entry")?;
            let timeline_dir = entry.path();

            if crate::is_temporary(timeline_dir) {
                info!("Found temporary timeline directory, removing: {timeline_dir}");
                if let Err(e) = std::fs::remove_dir_all(timeline_dir) {
                    error!("Failed to remove temporary directory '{timeline_dir}': {e:?}");
                }
            } else if is_uninit_mark(timeline_dir) {
                if !timeline_dir.exists() {
                    warn!("Timeline dir entry become invalid: {timeline_dir}");
                    continue;
                }

                let timeline_uninit_mark_file = &timeline_dir;
                info!(
                    "Found an uninit mark file {timeline_uninit_mark_file}, removing the timeline and its uninit mark",
                );
                let timeline_id =
                    TimelineId::try_from(timeline_uninit_mark_file.file_stem())
                        .with_context(|| {
                            format!(
                            "Could not parse timeline id out of the timeline uninit mark name {timeline_uninit_mark_file}",
                        )
                        })?;
                let timeline_dir = self.conf.timeline_path(&self.tenant_id, &timeline_id);
                if let Err(e) =
                    remove_timeline_and_uninit_mark(&timeline_dir, timeline_uninit_mark_file)
                {
                    error!("Failed to clean up uninit marked timeline: {e:?}");
                }
            } else if crate::is_delete_mark(timeline_dir) {
                // If metadata exists, load as usual, continue deletion
                let timeline_id = TimelineId::try_from(timeline_dir.file_stem())
                    .with_context(|| {
                        format!(
                            "Could not parse timeline id out of the timeline uninit mark name {timeline_dir}",
                        )
                    })?;

                info!("Found deletion mark for timeline {}", timeline_id);

                match load_metadata(self.conf, &self.tenant_id, &timeline_id) {
                    Ok(metadata) => {
                        timelines_to_resume_deletion.push((timeline_id, Some(metadata)))
                    }
                    Err(e) => match &e {
                        LoadMetadataError::Read(r) => {
                            if r.kind() != io::ErrorKind::NotFound {
                                return Err(anyhow::anyhow!(e)).with_context(|| {
                                    format!("Failed to load metadata for timeline_id {timeline_id}")
                                });
                            }

                            // If metadata doesn't exist it means that we've crashed without
                            // completing cleanup_remaining_timeline_fs_traces in DeleteTimelineFlow.
                            // So save timeline_id for later call to `DeleteTimelineFlow::cleanup_remaining_timeline_fs_traces`.
                            // We cant do it here because the method is async so we'd need block_on
                            // and here we're in spawn_blocking. cleanup_remaining_timeline_fs_traces uses fs operations
                            // so that basically results in a cycle:
                            // spawn_blocking
                            // - block_on
                            //   - spawn_blocking
                            // which can lead to running out of threads in blocing pool.
                            timelines_to_resume_deletion.push((timeline_id, None));
                        }
                        _ => {
                            return Err(anyhow::anyhow!(e)).with_context(|| {
                                format!("Failed to load metadata for timeline_id {timeline_id}")
                            })
                        }
                    },
                }
            } else {
                if !timeline_dir.exists() {
                    warn!("Timeline dir entry become invalid: {timeline_dir}");
                    continue;
                }
                let timeline_id = TimelineId::try_from(timeline_dir.file_name())
                    .with_context(|| {
                        format!(
                            "Could not parse timeline id out of the timeline dir name {timeline_dir}",
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
                        remove_timeline_and_uninit_mark(timeline_dir, &timeline_uninit_mark_file)
                    {
                        error!("Failed to clean up uninit marked timeline: {e:?}");
                    }
                    continue;
                }

                let timeline_delete_mark_file = self
                    .conf
                    .timeline_delete_mark_file_path(self.tenant_id, timeline_id);
                if timeline_delete_mark_file.exists() {
                    // Cleanup should be done in `is_delete_mark` branch above
                    continue;
                }

                let file_name = entry.file_name();
                if let Ok(timeline_id) = file_name.parse::<TimelineId>() {
                    let metadata = load_metadata(self.conf, &self.tenant_id, &timeline_id)
                        .context("failed to load metadata")?;
                    timelines_to_load.insert(timeline_id, metadata);
                } else {
                    // A file or directory that doesn't look like a timeline ID
                    warn!("unexpected file or directory in timelines directory: {file_name}");
                }
            }
        }

        // Sort the array of timeline IDs into tree-order, so that parent comes before
        // all its children.
        tree_sort_timelines(timelines_to_load, |m| m.ancestor_timeline()).map(|sorted_timelines| {
            TenantDirectoryScan {
                sorted_timelines_to_load: sorted_timelines,
                timelines_to_resume_deletion,
            }
        })
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
        span::debug_assert_current_span_has_tenant_id();

        debug!("loading tenant task");

        crate::failpoint_support::sleep_millis_async!("before-loading-tenant");

        // Load in-memory state to reflect the local files on disk
        //
        // Scan the directory, peek into the metadata file of each timeline, and
        // collect a list of timelines and their ancestors.
        let span = info_span!("blocking");
        let cloned = Arc::clone(self);

        let scan = tokio::task::spawn_blocking(move || {
            let _g = span.entered();
            cloned.scan_and_sort_timelines_dir()
        })
        .await
        .context("load spawn_blocking")
        .and_then(|res| res)?;

        // FIXME original collect_timeline_files contained one more check:
        //    1. "Timeline has no ancestor and no layer files"

        // Process loadable timelines first
        for (timeline_id, local_metadata) in scan.sorted_timelines_to_load {
            if let Err(e) = self
                .load_local_timeline(timeline_id, local_metadata, init_order, ctx, false)
                .await
            {
                match e {
                    LoadLocalTimelineError::Load(source) => {
                        return Err(anyhow::anyhow!(source)).with_context(|| {
                            format!("Failed to load local timeline: {timeline_id}")
                        })
                    }
                    LoadLocalTimelineError::ResumeDeletion(source) => {
                        // Make sure resumed deletion won't fail loading for entire tenant.
                        error!("Failed to resume timeline deletion: {source:#}")
                    }
                }
            }
        }

        // Resume deletion ones with deleted_mark
        for (timeline_id, maybe_local_metadata) in scan.timelines_to_resume_deletion {
            match maybe_local_metadata {
                None => {
                    // See comment in `scan_and_sort_timelines_dir`.
                    if let Err(e) =
                        DeleteTimelineFlow::cleanup_remaining_timeline_fs_traces(self, timeline_id)
                            .await
                    {
                        warn!(
                            "cannot clean up deleted timeline dir timeline_id: {} error: {:#}",
                            timeline_id, e
                        );
                    }
                }
                Some(local_metadata) => {
                    if let Err(e) = self
                        .load_local_timeline(timeline_id, local_metadata, init_order, ctx, true)
                        .await
                    {
                        match e {
                            LoadLocalTimelineError::Load(source) => {
                                // We tried to load deleted timeline, this is a bug.
                                return Err(anyhow::anyhow!(source).context(
                                "This is a bug. We tried to load deleted timeline which is wrong and loading failed. Timeline: {timeline_id}"
                            ));
                            }
                            LoadLocalTimelineError::ResumeDeletion(source) => {
                                // Make sure resumed deletion won't fail loading for entire tenant.
                                error!("Failed to resume timeline deletion: {source:#}")
                            }
                        }
                    }
                }
            }
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
        found_delete_mark: bool,
    ) -> Result<(), LoadLocalTimelineError> {
        span::debug_assert_current_span_has_tenant_id();

        let mut resources = self.build_timeline_resources(timeline_id);

        let (remote_startup_data, remote_client) = match resources.remote_client {
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
                                .init_upload_queue_stopped_to_continue_deletion(&index_part)
                                .context("init queue stopped")
                                .map_err(LoadLocalTimelineError::ResumeDeletion)?;

                            DeleteTimelineFlow::resume_deletion(
                                Arc::clone(self),
                                timeline_id,
                                &local_metadata,
                                Some(remote_client),
                                self.deletion_queue_client.clone(),
                                init_order,
                            )
                            .await
                            .context("resume deletion")
                            .map_err(LoadLocalTimelineError::ResumeDeletion)?;

                            return Ok(());
                        }
                    };

                    let remote_metadata = index_part.metadata.clone();
                    (
                        Some(RemoteStartupData {
                            index_part,
                            remote_metadata,
                        }),
                        Some(remote_client),
                    )
                }
                Err(DownloadError::NotFound) => {
                    info!("no index file was found on the remote, found_delete_mark: {found_delete_mark}");

                    if found_delete_mark {
                        // We could've resumed at a point where remote index was deleted, but metadata file wasnt.
                        // Cleanup:
                        return DeleteTimelineFlow::cleanup_remaining_timeline_fs_traces(
                            self,
                            timeline_id,
                        )
                        .await
                        .context("cleanup_remaining_timeline_fs_traces")
                        .map_err(LoadLocalTimelineError::ResumeDeletion);
                    }

                    // We're loading fresh timeline that didnt yet make it into remote.
                    (None, Some(remote_client))
                }
                Err(e) => return Err(LoadLocalTimelineError::Load(anyhow::Error::new(e))),
            },
            None => {
                // No remote client
                if found_delete_mark {
                    // There is no remote client, we found local metadata.
                    // Continue cleaning up local disk.
                    DeleteTimelineFlow::resume_deletion(
                        Arc::clone(self),
                        timeline_id,
                        &local_metadata,
                        None,
                        self.deletion_queue_client.clone(),
                        init_order,
                    )
                    .await
                    .context("resume deletion")
                    .map_err(LoadLocalTimelineError::ResumeDeletion)?;
                    return Ok(());
                }

                (None, resources.remote_client)
            }
        };
        resources.remote_client = remote_client;

        let ancestor = if let Some(ancestor_timeline_id) = local_metadata.ancestor_timeline() {
            let ancestor_timeline = self.get_timeline(ancestor_timeline_id, false)
                .with_context(|| anyhow::anyhow!("cannot find ancestor timeline {ancestor_timeline_id} for timeline {timeline_id}"))
                .map_err(LoadLocalTimelineError::Load)?;
            Some(ancestor_timeline)
        } else {
            None
        };

        self.timeline_init_and_sync(
            timeline_id,
            resources,
            remote_startup_data,
            Some(local_metadata),
            ancestor,
            init_order,
            ctx,
        )
        .await
        .map_err(LoadLocalTimelineError::Load)
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
    /// That's why we add an uninit mark file, and wrap it together with the Timeline
    /// in-memory object into UninitializedTimeline.
    /// Once the caller is done setting up the timeline, they should call
    /// `UninitializedTimeline::initialize_with_lock` to remove the uninit mark.
    ///
    /// For tests, use `DatadirModification::init_empty_test_timeline` + `commit` to setup the
    /// minimum amount of keys required to get a writable timeline.
    /// (Without it, `put` might fail due to `repartition` failing.)
    pub async fn create_empty_timeline(
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

        let timeline_uninit_mark = {
            let timelines = self.timelines.lock().unwrap();
            self.create_timeline_uninit_mark(new_timeline_id, &timelines)?
        };
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
        );
        self.prepare_new_timeline(
            new_timeline_id,
            &new_metadata,
            timeline_uninit_mark,
            initdb_lsn,
            None,
        )
        .await
    }

    /// Helper for unit tests to create an empty timeline.
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
        let uninit_tl = self
            .create_empty_timeline(new_timeline_id, initdb_lsn, pg_version, ctx)
            .await?;
        let tline = uninit_tl.raw_timeline().expect("we just created it");
        assert_eq!(tline.get_last_record_lsn(), Lsn(0));

        // Setup minimum keys required for the timeline to be usable.
        let mut modification = tline.begin_modification(initdb_lsn);
        modification
            .init_empty_test_timeline()
            .context("init_empty_test_timeline")?;
        modification
            .commit(ctx)
            .await
            .context("commit init_empty_test_timeline modification")?;

        // Flush to disk so that uninit_tl's check for valid disk_consistent_lsn passes.
        tline.maybe_spawn_flush_loop();
        tline.freeze_and_flush().await.context("freeze_and_flush")?;

        // Make sure the freeze_and_flush reaches remote storage.
        tline
            .remote_client
            .as_ref()
            .unwrap()
            .wait_completion()
            .await
            .unwrap();

        let tl = uninit_tl.finish_creation()?;
        // The non-test code would call tl.activate() here.
        tl.set_state(TimelineState::Active);
        Ok(tl)
    }

    /// Create a new timeline.
    ///
    /// Returns the new timeline ID and reference to its Timeline object.
    ///
    /// If the caller specified the timeline ID to use (`new_timeline_id`), and timeline with
    /// the same timeline ID already exists, returns CreateTimelineError::AlreadyExists.
    pub async fn create_timeline(
        &self,
        new_timeline_id: TimelineId,
        ancestor_timeline_id: Option<TimelineId>,
        mut ancestor_start_lsn: Option<Lsn>,
        pg_version: u32,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> Result<Arc<Timeline>, CreateTimelineError> {
        if !self.is_active() {
            return Err(CreateTimelineError::Other(anyhow::anyhow!(
                "Cannot create timelines on inactive tenant"
            )));
        }

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

            return Err(CreateTimelineError::AlreadyExists);
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
                        return Err(CreateTimelineError::AncestorLsn(anyhow::anyhow!(
                            "invalid start lsn {} for ancestor timeline {}: less than timeline ancestor lsn {}",
                            lsn,
                            ancestor_timeline_id,
                            ancestor_ancestor_lsn,
                        )));
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

        loaded_timeline.activate(broker_client, None, ctx);

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

        Ok(loaded_timeline)
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

        {
            let conf = self.tenant_conf.read().unwrap();

            if !conf.location.may_delete_layers_hint() {
                info!("Skipping GC in location state {:?}", conf.location);
                return Ok(GcResult::default());
            }
        }

        self.gc_iteration_internal(target_timeline_id, horizon, pitr, ctx)
            .await
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor task.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    pub async fn compaction_iteration(
        &self,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot run compaction iteration on inactive tenant"
        );

        {
            let conf = self.tenant_conf.read().unwrap();
            if !conf.location.may_delete_layers_hint() || !conf.location.may_upload_layers_hint() {
                info!("Skipping compaction in location state {:?}", conf.location);
                return Ok(());
            }
        }

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
                .compact(cancel, ctx)
                .instrument(info_span!("compact_timeline", %timeline_id))
                .await?;
        }

        Ok(())
    }

    pub fn current_state(&self) -> TenantState {
        self.state.borrow().clone()
    }

    pub fn is_active(&self) -> bool {
        self.current_state() == TenantState::Active
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
    ) {
        span::debug_assert_current_span_has_tenant_id();

        let mut activating = false;
        self.state.send_modify(|current_state| {
            use pageserver_api::models::ActivatingFrom;
            match &*current_state {
                TenantState::Activating(_) | TenantState::Active | TenantState::Broken { .. } | TenantState::Stopping { .. } => {
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
                timeline.activate(broker_client.clone(), background_jobs_can_start, ctx);
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

                TENANT_ACTIVATION.observe(elapsed.as_secs_f64());
            });
        }
    }

    /// Shutdown the tenant and join all of the spawned tasks.
    ///
    /// The method caters for all use-cases:
    /// - pageserver shutdown (freeze_and_flush == true)
    /// - detach + ignore (freeze_and_flush == false)
    ///
    /// This will attempt to shutdown even if tenant is broken.
    ///
    /// `shutdown_progress` is a [`completion::Barrier`] for the shutdown initiated by this call.
    /// If the tenant is already shutting down, we return a clone of the first shutdown call's
    /// `Barrier` as an `Err`. This not-first caller can use the returned barrier to join with
    /// the ongoing shutdown.
    async fn shutdown(
        &self,
        shutdown_progress: completion::Barrier,
        freeze_and_flush: bool,
    ) -> Result<(), completion::Barrier> {
        span::debug_assert_current_span_has_tenant_id();
        // Set tenant (and its timlines) to Stopping state.
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

        match self.set_stopping(shutdown_progress, false, false).await {
            Ok(()) => {}
            Err(SetStoppingError::Broken) => {
                // assume that this is acceptable
            }
            Err(SetStoppingError::AlreadyStopping(other)) => {
                // give caller the option to wait for this this shutdown
                return Err(other);
            }
        };

        let mut js = tokio::task::JoinSet::new();
        {
            let timelines = self.timelines.lock().unwrap();
            timelines.values().for_each(|timeline| {
                let timeline = Arc::clone(timeline);
                let span = Span::current();
                js.spawn(async move { timeline.shutdown(freeze_and_flush).instrument(span).await });
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
    ///
    /// `allow_transition_from_loading` is needed for the special case of loading task deleting the tenant.
    /// `allow_transition_from_attaching` is needed for the special case of attaching deleted tenant.
    async fn set_stopping(
        &self,
        progress: completion::Barrier,
        allow_transition_from_loading: bool,
        allow_transition_from_attaching: bool,
    ) -> Result<(), SetStoppingError> {
        let mut rx = self.state.subscribe();

        // cannot stop before we're done activating, so wait out until we're done activating
        rx.wait_for(|state| match state {
            TenantState::Attaching if allow_transition_from_attaching => true,
            TenantState::Activating(_) | TenantState::Attaching => {
                info!(
                    "waiting for {} to turn Active|Broken|Stopping",
                    <&'static str>::from(state)
                );
                false
            }
            TenantState::Loading => allow_transition_from_loading,
            TenantState::Active | TenantState::Broken { .. } | TenantState::Stopping { .. } => true,
        })
        .await
        .expect("cannot drop self.state while on a &self method");

        // we now know we're done activating, let's see whether this task is the winner to transition into Stopping
        let mut err = None;
        let stopping = self.state.send_if_modified(|current_state| match current_state {
            TenantState::Activating(_) => {
                unreachable!("1we ensured above that we're done with activation, and, there is no re-activation")
            }
            TenantState::Attaching => {
                if !allow_transition_from_attaching {
                    unreachable!("2we ensured above that we're done with activation, and, there is no re-activation")
                };
                *current_state = TenantState::Stopping { progress };
                true
            }
            TenantState::Loading => {
                if !allow_transition_from_loading {
                    unreachable!("3we ensured above that we're done with activation, and, there is no re-activation")
                };
                *current_state = TenantState::Stopping { progress };
                true
            }
            TenantState::Active => {
                // FIXME: due to time-of-check vs time-of-use issues, it can happen that new timelines
                // are created after the transition to Stopping. That's harmless, as the Timelines
                // won't be accessible to anyone afterwards, because the Tenant is in Stopping state.
                *current_state = TenantState::Stopping { progress };
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
            TenantState::Stopping { progress } => {
                info!("Tenant is already in Stopping state");
                err = Some(SetStoppingError::AlreadyStopping(progress.clone()));
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
            TenantState::Active | TenantState::Broken { .. } | TenantState::Stopping { .. } => true,
        })
        .await
        .expect("cannot drop self.state while on a &self method");

        // we now know we're done activating, let's see whether this task is the winner to transition into Broken
        self.set_broken_no_wait(reason)
    }

    pub(crate) fn set_broken_no_wait(&self, reason: impl Display) {
        let reason = reason.to_string();
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
                TenantState::Stopping { .. } => {
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
                TenantState::Broken { .. } | TenantState::Stopping { .. } => {
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
/// E extracts the ancestor from T
/// This allows for T to be different. It can be TimelineMetadata, can be Timeline itself, etc.
fn tree_sort_timelines<T, E>(
    timelines: HashMap<TimelineId, T>,
    extractor: E,
) -> anyhow::Result<Vec<(TimelineId, T)>>
where
    E: Fn(&T) -> Option<TimelineId>,
{
    let mut result = Vec::with_capacity(timelines.len());

    let mut now = Vec::with_capacity(timelines.len());
    // (ancestor, children)
    let mut later: HashMap<TimelineId, Vec<(TimelineId, T)>> =
        HashMap::with_capacity(timelines.len());

    for (timeline_id, value) in timelines {
        if let Some(ancestor_id) = extractor(&value) {
            let children = later.entry(ancestor_id).or_default();
            children.push((timeline_id, value));
        } else {
            now.push((timeline_id, value));
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
        self.tenant_conf.read().unwrap().tenant_conf
    }

    pub fn effective_config(&self) -> TenantConf {
        self.tenant_specific_overrides()
            .merge(self.conf.default_tenant_conf)
    }

    pub fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    pub fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    pub fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    pub fn get_compaction_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .compaction_period
            .unwrap_or(self.conf.default_tenant_conf.compaction_period)
    }

    pub fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    pub fn get_gc_horizon(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .gc_horizon
            .unwrap_or(self.conf.default_tenant_conf.gc_horizon)
    }

    pub fn get_gc_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .gc_period
            .unwrap_or(self.conf.default_tenant_conf.gc_period)
    }

    pub fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    pub fn get_pitr_interval(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .pitr_interval
            .unwrap_or(self.conf.default_tenant_conf.pitr_interval)
    }

    pub fn get_trace_read_requests(&self) -> bool {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .trace_read_requests
            .unwrap_or(self.conf.default_tenant_conf.trace_read_requests)
    }

    pub fn get_min_resident_size_override(&self) -> Option<u64> {
        let tenant_conf = self.tenant_conf.read().unwrap().tenant_conf;
        tenant_conf
            .min_resident_size_override
            .or(self.conf.default_tenant_conf.min_resident_size_override)
    }

    pub fn set_new_tenant_config(&self, new_tenant_conf: TenantConfOpt) {
        self.tenant_conf.write().unwrap().tenant_conf = new_tenant_conf;
        // Don't hold self.timelines.lock() during the notifies.
        // There's no risk of deadlock right now, but there could be if we consolidate
        // mutexes in struct Timeline in the future.
        let timelines = self.list_timelines();
        for timeline in timelines {
            timeline.tenant_conf_updated();
        }
    }

    pub(crate) fn set_new_location_config(&self, new_conf: AttachedTenantConf) {
        *self.tenant_conf.write().unwrap() = new_conf;
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
    ///
    /// `validate_ancestor == false` is used when a timeline is created for deletion
    /// and we might not have the ancestor present anymore which is fine for to be
    /// deleted timelines.
    fn create_timeline_struct(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        resources: TimelineResources,
        init_order: Option<&InitializationOrder>,
        cause: CreateTimelineCause,
    ) -> anyhow::Result<Arc<Timeline>> {
        let state = match cause {
            CreateTimelineCause::Load => {
                let ancestor_id = new_metadata.ancestor_timeline();
                anyhow::ensure!(
                    ancestor_id == ancestor.as_ref().map(|t| t.timeline_id),
                    "Timeline's {new_timeline_id} ancestor {ancestor_id:?} was not found"
                );
                TimelineState::Loading
            }
            CreateTimelineCause::Delete => TimelineState::Stopping,
        };

        let initial_logical_size_can_start = init_order.map(|x| &x.initial_logical_size_can_start);
        let initial_logical_size_attempt = init_order.map(|x| &x.initial_logical_size_attempt);

        let pg_version = new_metadata.pg_version();

        let timeline = Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            new_metadata,
            ancestor,
            new_timeline_id,
            self.tenant_id,
            self.generation,
            Arc::clone(&self.walredo_mgr),
            resources,
            pg_version,
            initial_logical_size_can_start.cloned(),
            initial_logical_size_attempt.cloned().flatten(),
            state,
        );

        Ok(timeline)
    }

    // Allow too_many_arguments because a constructor's argument list naturally grows with the
    // number of attributes in the struct: breaking these out into a builder wouldn't be helpful.
    #[allow(clippy::too_many_arguments)]
    fn new(
        state: TenantState,
        conf: &'static PageServerConf,
        attached_conf: AttachedTenantConf,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        tenant_id: TenantId,
        remote_storage: Option<GenericRemoteStorage>,
        deletion_queue_client: DeletionQueueClient,
    ) -> Tenant {
        let (state, mut rx) = watch::channel(state);

        tokio::spawn(async move {
            let tid = tenant_id.to_string();

            fn inspect_state(state: &TenantState) -> ([&'static str; 1], bool) {
                ([state.into()], matches!(state, TenantState::Broken { .. }))
            }

            let mut tuple = inspect_state(&rx.borrow_and_update());

            let is_broken = tuple.1;
            let mut counted_broken = if !is_broken {
                // the tenant might be ignored and reloaded, so first remove any previous set
                // element. it most likely has already been scraped, as these are manual operations
                // right now. most likely we will add it back very soon.
                drop(crate::metrics::BROKEN_TENANTS_SET.remove_label_values(&[&tid]));
                false
            } else {
                // add the id to the set right away, there should not be any updates on the channel
                // after
                crate::metrics::BROKEN_TENANTS_SET
                    .with_label_values(&[&tid])
                    .set(1);
                true
            };

            loop {
                let labels = &tuple.0;
                let current = TENANT_STATE_METRIC.with_label_values(labels);
                current.inc();

                if rx.changed().await.is_err() {
                    // tenant has been dropped; decrement the counter because a tenant with that
                    // state is no longer in tenant map, but allow any broken set item to exist
                    // still.
                    current.dec();
                    break;
                }

                current.dec();
                tuple = inspect_state(&rx.borrow_and_update());

                let is_broken = tuple.1;
                if is_broken && !counted_broken {
                    counted_broken = true;
                    // insert the tenant_id (back) into the set
                    crate::metrics::BROKEN_TENANTS_SET
                        .with_label_values(&[&tid])
                        .inc();
                }
            }
        });

        Tenant {
            tenant_id,
            generation: attached_conf.location.generation,
            conf,
            // using now here is good enough approximation to catch tenants with really long
            // activation times.
            loading_started_at: Instant::now(),
            tenant_conf: Arc::new(RwLock::new(attached_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: tokio::sync::Mutex::new(()),
            walredo_mgr,
            remote_storage,
            deletion_queue_client,
            state,
            cached_logical_sizes: tokio::sync::Mutex::new(HashMap::new()),
            cached_synthetic_tenant_size: Arc::new(AtomicU64::new(0)),
            eviction_task_tenant_state: tokio::sync::Mutex::new(EvictionTaskTenantState::default()),
            delete_progress: Arc::new(tokio::sync::Mutex::new(DeleteTenantFlow::default())),
        }
    }

    /// Locate and load config
    pub(super) fn load_tenant_config(
        conf: &'static PageServerConf,
        tenant_id: &TenantId,
    ) -> anyhow::Result<LocationConf> {
        let legacy_config_path = conf.tenant_config_path(tenant_id);
        let config_path = conf.tenant_location_config_path(tenant_id);

        if config_path.exists() {
            // New-style config takes precedence
            let deserialized = Self::read_config(&config_path)?;
            Ok(toml_edit::de::from_document::<LocationConf>(deserialized)?)
        } else if legacy_config_path.exists() {
            // Upgrade path: found an old-style configuration only
            let deserialized = Self::read_config(&legacy_config_path)?;

            let mut tenant_conf = TenantConfOpt::default();
            for (key, item) in deserialized.iter() {
                match key {
                    "tenant_config" => {
                        tenant_conf = PageServerConf::parse_toml_tenant_conf(item).with_context(|| {
                            format!("Failed to parse config from file '{legacy_config_path}' as pageserver config")
                        })?;
                    }
                    _ => bail!(
                        "config file {legacy_config_path} has unrecognized pageserver option '{key}'"
                    ),
                }
            }

            // Legacy configs are implicitly in attached state
            Ok(LocationConf::attached_single(
                tenant_conf,
                Generation::none(),
            ))
        } else {
            // FIXME If the config file is not found, assume that we're attaching
            // a detached tenant and config is passed via attach command.
            // https://github.com/neondatabase/neon/issues/1555
            // OR: we're loading after incomplete deletion that managed to remove config.
            info!(
                "tenant config not found in {} or {}",
                config_path, legacy_config_path
            );
            Ok(LocationConf::default())
        }
    }

    fn read_config(path: &Utf8Path) -> anyhow::Result<toml_edit::Document> {
        info!("loading tenant configuration from {path}");

        // load and parse file
        let config = fs::read_to_string(path)
            .with_context(|| format!("Failed to load config from path '{path}'"))?;

        config
            .parse::<toml_edit::Document>()
            .with_context(|| format!("Failed to parse config from file '{path}' as toml file"))
    }

    #[tracing::instrument(skip_all, fields(%tenant_id))]
    pub(super) async fn persist_tenant_config(
        conf: &'static PageServerConf,
        tenant_id: &TenantId,
        location_conf: &LocationConf,
    ) -> anyhow::Result<()> {
        let legacy_config_path = conf.tenant_config_path(tenant_id);
        let config_path = conf.tenant_location_config_path(tenant_id);
        Self::persist_tenant_config_at(tenant_id, &config_path, &legacy_config_path, location_conf)
            .await
    }

    #[tracing::instrument(skip_all, fields(%tenant_id))]
    pub(super) async fn persist_tenant_config_at(
        tenant_id: &TenantId,
        config_path: &Utf8Path,
        legacy_config_path: &Utf8Path,
        location_conf: &LocationConf,
    ) -> anyhow::Result<()> {
        // Forward compat: write out an old-style configuration that old versions can read, in case we roll back
        Self::persist_tenant_config_legacy(
            tenant_id,
            legacy_config_path,
            &location_conf.tenant_conf,
        )
        .await?;

        if let LocationMode::Attached(attach_conf) = &location_conf.mode {
            // Once we use LocationMode, generations are mandatory.  If we aren't using generations,
            // then drop out after writing legacy-style config.
            if attach_conf.generation.is_none() {
                tracing::debug!("Running without generations, not writing new-style LocationConf");
                return Ok(());
            }
        }

        info!("persisting tenantconf to {config_path}");

        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.
"#
        .to_string();

        // Convert the config to a toml file.
        conf_content += &toml_edit::ser::to_string_pretty(&location_conf)?;

        let conf_content = conf_content.as_bytes();

        let temp_path = path_with_suffix_extension(config_path, TEMP_FILE_SUFFIX);
        VirtualFile::crashsafe_overwrite(config_path, &temp_path, conf_content)
            .await
            .with_context(|| format!("write tenant {tenant_id} config to {config_path}"))?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(%tenant_id))]
    async fn persist_tenant_config_legacy(
        tenant_id: &TenantId,
        target_config_path: &Utf8Path,
        tenant_conf: &TenantConfOpt,
    ) -> anyhow::Result<()> {
        info!("persisting tenantconf to {target_config_path}");

        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.

[tenant_config]
"#
        .to_string();

        // Convert the config to a toml file.
        conf_content += &toml_edit::ser::to_string(&tenant_conf)?;

        let conf_content = conf_content.as_bytes();

        let temp_path = path_with_suffix_extension(target_config_path, TEMP_FILE_SUFFIX);
        VirtualFile::crashsafe_overwrite(target_config_path, &temp_path, conf_content)
            .await
            .with_context(|| format!("write tenant {tenant_id} config to {target_config_path}"))?;
        Ok(())
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

        crate::failpoint_support::sleep_millis_async!(
            "gc_iteration_internal_after_getting_gc_timelines"
        );

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
    ) -> Result<Arc<Timeline>, CreateTimelineError> {
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
    ) -> Result<Arc<Timeline>, CreateTimelineError> {
        self.branch_timeline_impl(src_timeline, dst_id, start_lsn, ctx)
            .await
    }

    async fn branch_timeline_impl(
        &self,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        start_lsn: Option<Lsn>,
        _ctx: &RequestContext,
    ) -> Result<Arc<Timeline>, CreateTimelineError> {
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
            ))
            .map_err(CreateTimelineError::AncestorLsn)?;

        // and then the planned GC cutoff
        {
            let gc_info = src_timeline.gc_info.read().unwrap();
            let cutoff = min(gc_info.pitr_cutoff, gc_info.horizon_cutoff);
            if start_lsn < cutoff {
                return Err(CreateTimelineError::AncestorLsn(anyhow::anyhow!(
                    "invalid branch start lsn: less than planned GC cutoff {cutoff}"
                )));
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
        );

        let uninitialized_timeline = self
            .prepare_new_timeline(
                dst_id,
                &metadata,
                timeline_uninit_mark,
                start_lsn + 1,
                Some(Arc::clone(src_timeline)),
            )
            .await?;

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
                format!("Failed to remove already existing initdb directory: {initdb_path}")
            })?;
        }
        // Init temporarily repo to get bootstrap data, this creates a directory in the `initdb_path` path
        run_initdb(self.conf, &initdb_path, pg_version)?;
        // this new directory is very temporary, set to remove it immediately after bootstrap, we don't need it
        scopeguard::defer! {
            if let Err(e) = fs::remove_dir_all(&initdb_path) {
                // this is unlikely, but we will remove the directory on pageserver restart or another bootstrap call
                error!("Failed to remove temporary initdb directory '{initdb_path}': {e}");
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
        );
        let raw_timeline = self
            .prepare_new_timeline(
                timeline_id,
                &new_metadata,
                timeline_uninit_mark,
                pgdata_lsn,
                None,
            )
            .await?;

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

    /// Call this before constructing a timeline, to build its required structures
    fn build_timeline_resources(&self, timeline_id: TimelineId) -> TimelineResources {
        let remote_client = if let Some(remote_storage) = self.remote_storage.as_ref() {
            let remote_client = RemoteTimelineClient::new(
                remote_storage.clone(),
                self.deletion_queue_client.clone(),
                self.conf,
                self.tenant_id,
                timeline_id,
                self.generation,
            );
            Some(remote_client)
        } else {
            None
        };

        TimelineResources {
            remote_client,
            deletion_queue_client: self.deletion_queue_client.clone(),
        }
    }

    /// Creates intermediate timeline structure and its files.
    ///
    /// An empty layer map is initialized, and new data and WAL can be imported starting
    /// at 'disk_consistent_lsn'. After any initial data has been imported, call
    /// `finish_creation` to insert the Timeline into the timelines map and to remove the
    /// uninit mark file.
    async fn prepare_new_timeline(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        uninit_mark: TimelineUninitMark,
        start_lsn: Lsn,
        ancestor: Option<Arc<Timeline>>,
    ) -> anyhow::Result<UninitializedTimeline> {
        let tenant_id = self.tenant_id;

        let resources = self.build_timeline_resources(new_timeline_id);
        if let Some(remote_client) = &resources.remote_client {
            remote_client.init_upload_queue_for_empty_remote(new_metadata)?;
        }

        let timeline_struct = self
            .create_timeline_struct(
                new_timeline_id,
                new_metadata,
                ancestor,
                resources,
                None,
                CreateTimelineCause::Load,
            )
            .context("Failed to create timeline data structure")?;

        timeline_struct.init_empty_layer_map(start_lsn);

        if let Err(e) = self
            .create_timeline_files(&uninit_mark.timeline_path, &new_timeline_id, new_metadata)
            .await
        {
            error!("Failed to create initial files for timeline {tenant_id}/{new_timeline_id}, cleaning up: {e:?}");
            cleanup_timeline_directory(uninit_mark);
            return Err(e);
        }

        debug!("Successfully created initial files for timeline {tenant_id}/{new_timeline_id}");

        Ok(UninitializedTimeline::new(
            self,
            new_timeline_id,
            Some((timeline_struct, uninit_mark)),
        ))
    }

    async fn create_timeline_files(
        &self,
        timeline_path: &Utf8Path,
        new_timeline_id: &TimelineId,
        new_metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        crashsafe::create_dir(timeline_path).context("Failed to create timeline directory")?;

        fail::fail_point!("after-timeline-uninit-mark-creation", |_| {
            anyhow::bail!("failpoint after-timeline-uninit-mark-creation");
        });

        save_metadata(self.conf, &self.tenant_id, new_timeline_id, new_metadata)
            .await
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
        let timeline_path = self.conf.timeline_path(&tenant_id, &timeline_id);
        anyhow::ensure!(
            !timeline_path.exists(),
            "Timeline {timeline_path} already exists, cannot create its uninit mark file",
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

    pub fn cached_synthetic_size(&self) -> u64 {
        self.cached_synthetic_tenant_size.load(Ordering::Relaxed)
    }
}

fn remove_timeline_and_uninit_mark(
    timeline_dir: &Utf8Path,
    uninit_mark: &Utf8Path,
) -> anyhow::Result<()> {
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
            format!("Failed to remove unit marked timeline directory {timeline_dir}")
        })?;
    fs::remove_file(uninit_mark)
        .with_context(|| format!("Failed to remove timeline uninit mark file {uninit_mark}"))?;

    Ok(())
}

pub(crate) enum CreateTenantFilesMode {
    Create,
    Attach,
}

pub(crate) async fn create_tenant_files(
    conf: &'static PageServerConf,
    location_conf: &LocationConf,
    tenant_id: &TenantId,
    mode: CreateTenantFilesMode,
) -> anyhow::Result<Utf8PathBuf> {
    let target_tenant_directory = conf.tenant_path(tenant_id);
    anyhow::ensure!(
        !target_tenant_directory
            .try_exists()
            .context("check existence of tenant directory")?,
        "tenant directory already exists",
    );

    let temporary_tenant_dir =
        path_with_suffix_extension(&target_tenant_directory, TEMP_FILE_SUFFIX);
    debug!("Creating temporary directory structure in {temporary_tenant_dir}");

    // top-level dir may exist if we are creating it through CLI
    crashsafe::create_dir_all(&temporary_tenant_dir).with_context(|| {
        format!("could not create temporary tenant directory {temporary_tenant_dir}")
    })?;

    let creation_result = try_create_target_tenant_dir(
        conf,
        location_conf,
        tenant_id,
        mode,
        &temporary_tenant_dir,
        &target_tenant_directory,
    )
    .await;

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

async fn try_create_target_tenant_dir(
    conf: &'static PageServerConf,
    location_conf: &LocationConf,
    tenant_id: &TenantId,
    mode: CreateTenantFilesMode,
    temporary_tenant_dir: &Utf8Path,
    target_tenant_directory: &Utf8Path,
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
        &conf.timelines_path(tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("resolve tenant {tenant_id} temporary timelines dir"))?;
    let temporary_legacy_tenant_config_path = rebase_directory(
        &conf.tenant_config_path(tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("resolve tenant {tenant_id} temporary config path"))?;
    let temporary_tenant_config_path = rebase_directory(
        &conf.tenant_location_config_path(tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("resolve tenant {tenant_id} temporary config path"))?;

    Tenant::persist_tenant_config_at(
        tenant_id,
        &temporary_tenant_config_path,
        &temporary_legacy_tenant_config_path,
        location_conf,
    )
    .await?;

    crashsafe::create_dir(&temporary_tenant_timelines_dir).with_context(|| {
        format!(
            "create tenant {} temporary timelines directory {}",
            tenant_id, temporary_tenant_timelines_dir,
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
            tenant_id, temporary_tenant_dir, target_tenant_directory
        )
    })?;
    let target_dir_parent = target_tenant_directory.parent().with_context(|| {
        format!(
            "get tenant {} dir parent for {}",
            tenant_id, target_tenant_directory,
        )
    })?;
    crashsafe::fsync(target_dir_parent).with_context(|| {
        format!(
            "fsync renamed directory's parent {} for tenant {}",
            target_dir_parent, tenant_id,
        )
    })?;

    Ok(())
}

fn rebase_directory(
    original_path: &Utf8Path,
    base: &Utf8Path,
    new_base: &Utf8Path,
) -> anyhow::Result<Utf8PathBuf> {
    let relative_path = original_path.strip_prefix(base).with_context(|| {
        format!(
            "Failed to strip base prefix '{}' off path '{}'",
            base, original_path
        )
    })?;
    Ok(new_base.join(relative_path))
}

/// Create the cluster temporarily in 'initdbpath' directory inside the repository
/// to get bootstrap data for timeline initialization.
fn run_initdb(
    conf: &'static PageServerConf,
    initdb_target_dir: &Utf8Path,
    pg_version: u32,
) -> anyhow::Result<()> {
    let initdb_bin_path = conf.pg_bin_dir(pg_version)?.join("initdb");
    let initdb_lib_dir = conf.pg_lib_dir(pg_version)?;
    info!(
        "running {} in {}, libdir: {}",
        initdb_bin_path, initdb_target_dir, initdb_lib_dir,
    );

    let initdb_output = Command::new(&initdb_bin_path)
        .args(["-D", initdb_target_dir.as_ref()])
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
                initdb_bin_path, initdb_target_dir,
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
pub async fn dump_layerfile_from_path(
    path: &Utf8Path,
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
        crate::IMAGE_FILE_MAGIC => {
            ImageLayer::new_for_path(path, file)?
                .dump(verbose, ctx)
                .await?
        }
        crate::DELTA_FILE_MAGIC => {
            DeltaLayer::new_for_path(path, file)?
                .dump(verbose, ctx)
                .await?
        }
        magic => bail!("unrecognized magic identifier: {:?}", magic),
    }

    Ok(())
}

#[cfg(test)]
pub mod harness {
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::OnceCell;
    use std::fs;
    use std::sync::Arc;
    use utils::logging;
    use utils::lsn::Lsn;

    use crate::deletion_queue::mock::MockDeletionQueue;
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
        pub generation: Generation,
        pub remote_storage: GenericRemoteStorage,
        pub remote_fs_dir: Utf8PathBuf,
        pub deletion_queue: MockDeletionQueue,
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

            use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
            let remote_fs_dir = conf.workdir.join("localfs");
            std::fs::create_dir_all(&remote_fs_dir).unwrap();
            let config = RemoteStorageConfig {
                // TODO: why not remote_storage::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS,
                max_concurrent_syncs: std::num::NonZeroUsize::new(2_000_000).unwrap(),
                // TODO: why not remote_storage::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS,
                max_sync_errors: std::num::NonZeroU32::new(3_000_000).unwrap(),
                storage: RemoteStorageKind::LocalFs(remote_fs_dir.clone()),
            };
            let remote_storage = GenericRemoteStorage::from_config(&config).unwrap();
            let deletion_queue = MockDeletionQueue::new(Some(remote_storage.clone()));

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
                generation: Generation::new(0xdeadbeef),
                remote_storage,
                remote_fs_dir,
                deletion_queue,
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
                AttachedTenantConf::try_from(LocationConf::attached_single(
                    TenantConfOpt::from(self.tenant_conf),
                    self.generation,
                ))
                .unwrap(),
                walredo_mgr,
                self.tenant_id,
                Some(self.remote_storage.clone()),
                self.deletion_queue.new_client(),
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

        pub fn timeline_path(&self, timeline_id: &TimelineId) -> Utf8PathBuf {
            self.conf.timeline_path(&self.tenant_id, timeline_id)
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
    use tokio_util::sync::CancellationToken;

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
            .put(
                *TEST_KEY,
                Lsn(0x10),
                &Value::Image(TEST_IMG("foo at 0x10")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x20),
                &Value::Image(TEST_IMG("foo at 0x20")),
                &ctx,
            )
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

        match tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
        {
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
            .put(TEST_KEY_A, Lsn(0x20), &test_value("foo at 0x20"), &ctx)
            .await?;
        writer
            .put(TEST_KEY_B, Lsn(0x20), &test_value("foobar at 0x20"), &ctx)
            .await?;
        writer.finish_write(Lsn(0x20));

        writer
            .put(TEST_KEY_A, Lsn(0x30), &test_value("foo at 0x30"), &ctx)
            .await?;
        writer.finish_write(Lsn(0x30));
        writer
            .put(TEST_KEY_A, Lsn(0x40), &test_value("foo at 0x40"), &ctx)
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
            .put(TEST_KEY_A, Lsn(0x40), &test_value("bar at 0x40"), &ctx)
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

    async fn make_some_layers(
        tline: &Timeline,
        start_lsn: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
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
                    ctx,
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
                    ctx,
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
                    ctx,
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
                    ctx,
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
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

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
                let CreateTimelineError::AncestorLsn(err) = err else {
                    panic!("wrong error type")
                };
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
                let CreateTimelineError::AncestorLsn(err) = err else {
                    panic!("wrong error type");
                };
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
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

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
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

        tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
            .await?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60), &ctx).await?;

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
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

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
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

        tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
            .await?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60), &ctx).await?;

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
            make_some_layers(tline.as_ref(), Lsn(0x8000), &ctx).await?;
            // so that all uploads finish & we can call harness.load() below again
            tenant
                .shutdown(Default::default(), true)
                .instrument(info_span!("test_shutdown", tenant_id=%tenant.tenant_id))
                .await
                .ok()
                .unwrap();
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

            make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

            let child_tline = tenant
                .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(Lsn(0x40)), &ctx)
                .await?;
            child_tline.set_state(TimelineState::Active);

            let newtline = tenant
                .get_timeline(NEW_TIMELINE_ID, true)
                .expect("Should have a local timeline");

            make_some_layers(newtline.as_ref(), Lsn(0x60), &ctx).await?;

            // so that all uploads finish & we can call harness.load() below again
            tenant
                .shutdown(Default::default(), true)
                .instrument(info_span!("test_shutdown", tenant_id=%tenant.tenant_id))
                .await
                .ok()
                .unwrap();
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
    async fn delta_layer_dumping() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_layer_dumping")?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

        let layer_map = tline.layers.read().await;
        let level0_deltas = layer_map.layer_map().get_level0_deltas()?;

        assert!(!level0_deltas.is_empty());

        for delta in level0_deltas {
            let delta = layer_map.get_from_desc(&delta);
            // Ensure we are dumping a delta layer here
            let delta = delta.downcast_delta_layer().unwrap();

            delta.dump(false, &ctx).await.unwrap();
            delta.dump(true, &ctx).await.unwrap();
        }

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
        // so that all uploads finish & we can call harness.try_load() below again
        tenant
            .shutdown(Default::default(), true)
            .instrument(info_span!("test_shutdown", tenant_id=%tenant.tenant_id))
            .await
            .ok()
            .unwrap();
        drop(tenant);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[8] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let err = harness.try_load(&ctx).await.err().expect("should fail");
        // get all the stack with all .context, not only the last one
        let message = format!("{err:#}");
        let expected = "failed to load metadata";
        assert!(
            message.contains(expected),
            "message '{message}' expected to contain {expected}"
        );

        let mut found_error_message = false;
        let mut err_source = err.source();
        while let Some(source) = err_source {
            if source.to_string().contains("metadata checksum mismatch") {
                found_error_message = true;
                break;
            }
            err_source = source.source();
        }
        assert!(
            found_error_message,
            "didn't find the corrupted metadata error in {}",
            message
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
            .put(
                *TEST_KEY,
                Lsn(0x10),
                &Value::Image(TEST_IMG("foo at 0x10")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&CancellationToken::new(), &ctx).await?;

        let writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x20),
                &Value::Image(TEST_IMG("foo at 0x20")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&CancellationToken::new(), &ctx).await?;

        let writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x30),
                &Value::Image(TEST_IMG("foo at 0x30")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&CancellationToken::new(), &ctx).await?;

        let writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x40),
                &Value::Image(TEST_IMG("foo at 0x40")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline.compact(&CancellationToken::new(), &ctx).await?;

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
        let harness = TenantHarness::create("test_bulk_insert")?;
        let (tenant, ctx) = harness.load().await;
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
                        &ctx,
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
            tline.compact(&CancellationToken::new(), &ctx).await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_random_updates() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_random_updates")?;
        let (tenant, ctx) = harness.load().await;
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
                    &ctx,
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
                        &ctx,
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
            tline.compact(&CancellationToken::new(), &ctx).await?;
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
                    &ctx,
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
                        &ctx,
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
            tline.compact(&CancellationToken::new(), &ctx).await?;
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
                        &ctx,
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
        let utline = tenant
            .create_empty_timeline(TIMELINE_ID, initdb_lsn, DEFAULT_PG_VERSION, &ctx)
            .await?;
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
            .commit(&ctx)
            .await
            .context("commit init_empty_test_timeline modification")?;

        // Do the flush. The flush code will check the expectations that we set above.
        tline.freeze_and_flush().await?;

        // assert freeze_and_flush exercised the initdb optimization
        {
            let state = tline.flush_loop_state.lock().unwrap();
            let timeline::FlushLoopState::Running {
                expect_initdb_optimization,
                initdb_optimization_count,
            } = *state
            else {
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
            let tline = tenant
                .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION, &ctx)
                .await?;
            // Keeps uninit mark in place
            let raw_tline = tline.raw_timeline().unwrap();
            raw_tline
                .shutdown(false)
                .instrument(info_span!("test_shutdown", tenant_id=%raw_tline.tenant_id))
                .await;
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
            .timeline_path(&tenant.tenant_id, &TIMELINE_ID)
            .exists());

        assert!(!harness
            .conf
            .timeline_uninit_mark_file_path(tenant.tenant_id, TIMELINE_ID)
            .exists());

        Ok(())
    }
}
