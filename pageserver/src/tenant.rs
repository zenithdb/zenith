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
use bytes::Bytes;
use futures::Stream;
use pageserver_api::models::TimelineState;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use tokio::sync::watch;
use tokio_util::io::StreamReader;
use tokio_util::io::SyncIoBridge;
use tracing::*;
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
use std::pin::Pin;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use self::metadata::TimelineMetadata;
use crate::config::PageServerConf;
use crate::fail_point;
use crate::import_datadir;
use crate::is_uninit_mark;
use crate::metrics::{remove_tenant_metrics, STORAGE_TIME};
use crate::repository::GcResult;
use crate::storage_sync::create_remote_timeline_client;
use crate::storage_sync::index::IndexPart;
use crate::storage_sync::list_remote_timelines;
use crate::storage_sync::RemoteTimelineClient;
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::tenant::metadata::load_metadata;
use crate::tenant_config::TenantConfOpt;
use crate::virtual_file::VirtualFile;
use crate::walredo::PostgresRedoManager;
use crate::walredo::WalRedoManager;
use crate::{CheckpointConfig, TEMP_FILE_SUFFIX};
pub use pageserver_api::models::TenantState;

use toml_edit;
use utils::{
    crashsafe,
    id::{TenantId, TimelineId},
    lsn::{Lsn, RecordLsn},
};

mod blob_io;
pub mod block_io;
mod delta_layer;
mod disk_btree;
pub(crate) mod ephemeral_file;
pub mod filename;
mod image_layer;
mod inmemory_layer;
pub mod layer_map;

pub mod metadata;
mod par_fsync;
pub mod storage_layer;

mod timeline;

pub mod size;

use storage_layer::Layer;

pub use timeline::Timeline;

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
    // `timelines` mutex during all GC iteration (especially with enforced checkpoint)
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: Mutex<()>,
    walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,

    // provides access to timeline data sitting in the remote storage
    remote_storage: Option<GenericRemoteStorage>,

    /// Cached logical sizes updated updated on each [`Tenant::gather_size_inputs`].
    cached_logical_sizes: tokio::sync::Mutex<HashMap<(TimelineId, Lsn), u64>>,
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
    /// Ensures timeline data is valid, loads it into pageserver's memory and removes
    /// uninit mark file on success.
    ///
    /// The new timeline is initialized in Active state, and its background jobs are
    /// started
    pub fn initialize(self) -> anyhow::Result<Arc<Timeline>> {
        let mut timelines = self.owning_tenant.timelines.lock().unwrap();
        self.initialize_with_lock(&mut timelines, true, true)
    }

    /// Like `initialize`, but the caller is already holding lock on Tenant::timelines.
    /// If `launch_wal_receiver` is false, the WAL receiver not launched, even though
    /// timeline is initialized in Active state. This is used during tenant load and
    /// attach, where the WAL receivers are launched only after all the timelines have
    /// been initialized.
    fn initialize_with_lock(
        mut self,
        timelines: &mut HashMap<TimelineId, Arc<Timeline>>,
        load_layer_map: bool,
        launch_wal_receiver: bool,
    ) -> anyhow::Result<Arc<Timeline>> {
        let timeline_id = self.timeline_id;
        let tenant_id = self.owning_tenant.tenant_id;

        let (new_timeline, uninit_mark) = self.raw_timeline.take().with_context(|| {
            format!("No timeline for initalization found for {tenant_id}/{timeline_id}")
        })?;

        let new_disk_consistent_lsn = new_timeline.get_disk_consistent_lsn();
        // TODO it would be good to ensure that, but apparently a lot of our testing is dependend on that at least
        // ensure!(new_disk_consistent_lsn.is_valid(),
        //     "Timeline {tenant_id}/{timeline_id} has invalid disk_consistent_lsn and cannot be initialized");

        match timelines.entry(timeline_id) {
            Entry::Occupied(_) => anyhow::bail!(
                "Found freshly initialized timeline {tenant_id}/{timeline_id} in the tenant map"
            ),
            Entry::Vacant(v) => {
                if load_layer_map {
                    new_timeline
                        .load_layer_map(new_disk_consistent_lsn)
                        .with_context(|| {
                            format!(
                                "Failed to load layermap for timeline {tenant_id}/{timeline_id}"
                            )
                        })?;
                }
                uninit_mark.remove_uninit_mark().with_context(|| {
                    format!(
                        "Failed to remove uninit mark file for timeline {tenant_id}/{timeline_id}"
                    )
                })?;
                new_timeline.set_state(TimelineState::Active);
                v.insert(Arc::clone(&new_timeline));

                new_timeline.maybe_spawn_flush_loop();

                if launch_wal_receiver {
                    new_timeline.launch_wal_receiver();
                }
            }
        }

        Ok(new_timeline)
    }

    /// Prepares timeline data by loading it from the basebackup archive.
    pub async fn import_basebackup_from_tar(
        self,
        mut copyin_stream: &mut Pin<&mut impl Stream<Item = io::Result<Bytes>>>,
        base_lsn: Lsn,
    ) -> anyhow::Result<Arc<Timeline>> {
        let raw_timeline = self.raw_timeline()?;

        // import_basebackup_from_tar() is not async, mainly because the Tar crate
        // it uses is not async. So we need to jump through some hoops:
        // - convert the input from client connection to a synchronous Read
        // - use block_in_place()
        let reader = SyncIoBridge::new(StreamReader::new(&mut copyin_stream));

        tokio::task::block_in_place(|| {
            import_datadir::import_basebackup_from_tar(raw_timeline, reader, base_lsn)
                .context("Failed to import basebackup")
        })?;

        // Flush loop needs to be spawned in order for checkpoint to be able to flush.
        // We want to run proper checkpoint before we mark timeline as available to outside world
        // Thus spawning flush loop manually and skipping flush_loop setup in initialize_with_lock
        raw_timeline.maybe_spawn_flush_loop();

        fail_point!("before-checkpoint-new-timeline", |_| {
            bail!("failpoint before-checkpoint-new-timeline");
        });

        raw_timeline
            .checkpoint(CheckpointConfig::Flush)
            .await
            .context("Failed to checkpoint after basebackup import")?;

        let timeline = self.initialize()?;

        Ok(timeline)
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
    /// Useful for initializing timelines, existing on disk after the restart.
    pub fn dummy() -> Self {
        Self {
            uninit_mark_deleted: true,
            uninit_mark_path: PathBuf::new(),
            timeline_path: PathBuf::new(),
        }
    }

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
        ignore_absent_files(|| fs::remove_file(&uninit_mark_file)).with_context(|| {
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
//     Checkpoint comes, we update local metadata and start upload task but after that
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

struct RemoteStartupData {
    index_part: IndexPart,
    remote_metadata: TimelineMetadata,
}

impl Tenant {
    /// Yet another helper for timeline initialization.
    /// Contains common part for `load_local_timeline` and `load_remote_timeline`
    async fn setup_timeline(
        &self,
        timeline_id: TimelineId,
        remote_client: Option<RemoteTimelineClient>,
        remote_startup_data: Option<RemoteStartupData>,
        local_metadata: Option<TimelineMetadata>,
        ancestor: Option<Arc<Timeline>>,
        first_save: bool,
    ) -> anyhow::Result<()> {
        let tenant_id = self.tenant_id;

        let (up_to_date_metadata, picked_local) = merge_local_remote_metadata(
            local_metadata.as_ref(),
            remote_startup_data.as_ref().map(|r| &r.remote_metadata),
        )
        .context("merge_local_remote_metadata")?
        .to_owned();

        let timeline = {
            // avoiding holding it across awaits
            let mut timelines_accessor = self.timelines.lock().unwrap();
            if timelines_accessor.contains_key(&timeline_id) {
                anyhow::bail!(
                    "Timeline {tenant_id}/{timeline_id} already exists in the tenant map"
                );
            }

            let dummy_timeline = self.create_timeline_data(
                timeline_id,
                up_to_date_metadata.clone(),
                ancestor.clone(),
                remote_client,
            )?;

            let timeline = UninitializedTimeline {
                owning_tenant: self,
                timeline_id,
                raw_timeline: Some((Arc::new(dummy_timeline), TimelineUninitMark::dummy())),
            };
            // Do not start walreceiver here. We do need loaded layer map for reconcile_with_remote
            // But we shouldnt start walreceiver before we have all the data locally, because working walreceiver
            // will ingest data which may require looking at the layers which are not yet available locally
            match timeline.initialize_with_lock(&mut timelines_accessor, true, false) {
                Ok(initialized_timeline) => {
                    timelines_accessor.insert(timeline_id, initialized_timeline.clone());
                    Ok(initialized_timeline)
                }
                Err(e) => {
                    error!("Failed to initialize timeline {tenant_id}/{timeline_id}: {e:?}");
                    // FIXME using None is a hack, it wont hurt, just ugly.
                    //     Ideally initialize_with_lock error should return timeline in the error
                    //     Or return ownership of itself completely so somethin like into_broken
                    //     can be called directly on Uninitielized timeline
                    //     also leades to redundant .clone
                    let broken_timeline = self
                        .create_timeline_data(
                            timeline_id,
                            up_to_date_metadata.clone(),
                            ancestor.clone(),
                            None,
                        )
                        .with_context(|| {
                            format!(
                            "Failed to crate broken timeline data for {tenant_id}/{timeline_id}"
                        )
                        })?;
                    broken_timeline.set_state(TimelineState::Broken);
                    timelines_accessor.insert(timeline_id, Arc::new(broken_timeline));
                    Err(e)
                }
            }
        }?;

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
                    .unwrap()
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
    pub fn spawn_attach(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        remote_storage: GenericRemoteStorage,
    ) -> Arc<Tenant> {
        // XXX: Attach should provide the config, especially during tenant migration.
        //      See https://github.com/neondatabase/neon/issues/1555
        let tenant_conf = TenantConfOpt::default();

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

        task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::Attach,
            Some(tenant_id),
            None,
            "attach tenant",
            false,
            async move {
                match tenant_clone.attach().await {
                    Ok(_) => {}
                    Err(e) => {
                        tenant_clone.set_broken();
                        error!("error attaching tenant: {:?}", e);
                    }
                }
                Ok(())
            },
        );
        tenant
    }

    ///
    /// Background task that downloads all data for a tenant and brings it to Active state.
    ///
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id))]
    async fn attach(self: &Arc<Tenant>) -> anyhow::Result<()> {
        // Create directory with marker file to indicate attaching state.
        // The load_local_tenants() function in tenant_mgr relies on the marker file
        // to determine whether a tenant has finished attaching.
        let tenant_dir = self.conf.tenant_path(&self.tenant_id);
        let marker_file = self.conf.tenant_attaching_mark_file_path(&self.tenant_id);
        debug_assert_eq!(marker_file.parent().unwrap(), tenant_dir);
        if tenant_dir.exists() {
            if !marker_file.is_file() {
                anyhow::bail!(
                    "calling Tenant::attach with a tenant directory that doesn't have the attaching marker file:\ntenant_dir: {}\nmarker_file: {}",
                    tenant_dir.display(), marker_file.display());
            }
        } else {
            crashsafe::create_dir_all(&tenant_dir).context("create tenant directory")?;
            fs::File::create(&marker_file).context("create tenant attaching marker file")?;
            crashsafe::fsync_file_and_parent(&marker_file)
                .context("fsync tenant attaching marker file and parent")?;
        }
        debug_assert!(tenant_dir.is_dir());
        debug_assert!(marker_file.is_file());

        // Get list of remote timelines
        // download index files for every tenant timeline
        info!("listing remote timelines");

        let remote_storage = self
            .remote_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("cannot attach without remote storage"))?;

        let remote_timelines =
            list_remote_timelines(remote_storage, self.conf, self.tenant_id).await?;

        info!("found {} timelines", remote_timelines.len());

        let mut timeline_ancestors: HashMap<TimelineId, TimelineMetadata> = HashMap::new();
        let mut index_parts: HashMap<TimelineId, IndexPart> = HashMap::new();
        for (timeline_id, index_part) in remote_timelines {
            let remote_metadata = index_part.parse_metadata().with_context(|| {
                format!(
                    "Failed to parse metadata file from remote storage for tenant {} timeline {}",
                    self.tenant_id, timeline_id
                )
            })?;
            timeline_ancestors.insert(timeline_id, remote_metadata);
            index_parts.insert(timeline_id, index_part);
        }

        // For every timeline, download the metadata file, scan the local directory,
        // and build a layer map that contains an entry for each remote and local
        // layer file.
        let sorted_timelines = tree_sort_timelines(timeline_ancestors)?;
        for (timeline_id, remote_metadata) in sorted_timelines {
            // TODO again handle early failure
            self.load_remote_timeline(
                timeline_id,
                index_parts.remove(&timeline_id).unwrap(),
                remote_metadata,
                remote_storage.clone(),
            )
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

        // Start background operations and open the tenant for business.
        // The loops will shut themselves down when they notice that the tenant is inactive.
        self.activate()?;

        info!("Done");

        Ok(())
    }

    #[instrument(skip(self, index_part, remote_metadata, remote_storage), fields(timeline_id=%timeline_id))]
    async fn load_remote_timeline(
        &self,
        timeline_id: TimelineId,
        index_part: IndexPart,
        remote_metadata: TimelineMetadata,
        remote_storage: GenericRemoteStorage,
    ) -> anyhow::Result<()> {
        info!("downloading index file for timeline {}", timeline_id);
        tokio::fs::create_dir_all(self.conf.timeline_path(&timeline_id, &self.tenant_id))
            .await
            .context("Failed to create new timeline directory")?;

        let remote_client =
            create_remote_timeline_client(remote_storage, self.conf, self.tenant_id, timeline_id)?;

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

        self.setup_timeline(
            timeline_id,
            Some(remote_client),
            Some(RemoteStartupData {
                index_part,
                remote_metadata,
            }),
            local_metadata,
            ancestor,
            true,
        )
        .await
    }

    /// Create a placeholder Tenant object for a broken tenant
    pub fn create_broken_tenant(conf: &'static PageServerConf, tenant_id: TenantId) -> Arc<Tenant> {
        let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
        Arc::new(Tenant::new(
            TenantState::Broken,
            conf,
            TenantConfOpt::default(),
            wal_redo_manager,
            tenant_id,
            None,
        ))
    }

    ///
    /// Load a tenant that's available on local disk
    ///
    /// This is used at pageserver startup, to rebuild the in-memory
    /// structures from on-disk state. This is similar to attaching a tenant,
    /// but the index files already exist on local disk, as well as some layer
    /// files.
    ///
    /// If the loading fails for some reason, the Tenant will go into Broken
    /// state.
    ///
    #[instrument(skip(conf, remote_storage), fields(tenant_id=%tenant_id))]
    pub fn spawn_load(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        remote_storage: Option<GenericRemoteStorage>,
    ) -> Arc<Tenant> {
        let tenant_conf = match Self::load_tenant_config(conf, tenant_id) {
            Ok(conf) => conf,
            Err(e) => {
                error!("load tenant config failed: {:?}", e);
                return Tenant::create_broken_tenant(conf, tenant_id);
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

        let _ = task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::InitialLoad,
            Some(tenant_id),
            None,
            "initial tenant load",
            false,
            async move {
                match tenant_clone.load().await {
                    Ok(()) => {}
                    Err(err) => {
                        tenant_clone.set_broken();
                        error!("could not load tenant {tenant_id}: {err:?}");
                    }
                }
                info!("initial load for tenant {tenant_id} finished!");
                Ok(())
            },
        );

        info!("spawned load into background");

        tenant
    }

    ///
    /// Background task to load in-memory data structures for this tenant, from
    /// files on disk. Used at pageserver startup.
    ///
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id))]
    async fn load(self: &Arc<Tenant>) -> anyhow::Result<()> {
        info!("loading tenant task");

        utils::failpoint_sleep_millis_async!("before-loading-tenant");

        // TODO split this into two functions, scan and actual load

        // Load in-memory state to reflect the local files on disk
        //
        // Scan the directory, peek into the metadata file of each timeline, and
        // collect a list of timelines and their ancestors.
        let mut timelines_to_load: HashMap<TimelineId, TimelineMetadata> = HashMap::new();
        let timelines_dir = self.conf.timelines_path(&self.tenant_id);
        for entry in std::fs::read_dir(&timelines_dir).with_context(|| {
            format!(
                "Failed to list timelines directory for tenant {}",
                self.tenant_id
            )
        })? {
            let entry = entry.with_context(|| {
                format!("cannot read timeline dir entry for {}", self.tenant_id)
            })?;
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
                        "Found an uninit mark file for timeline {}/{}, removing the timeline and its uninit mark",
                        self.tenant_id, timeline_id
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
        let sorted_timelines = tree_sort_timelines(timelines_to_load)?;
        // FIXME original collect_timeline_files contained one more check:
        //    1. "Timeline has no ancestor and no layer files"

        for (timeline_id, local_metadata) in sorted_timelines {
            self.load_local_timeline(timeline_id, local_metadata)
                .await
                .with_context(|| format!("load local timeline {timeline_id}"))?;
        }

        // Start background operations and open the tenant for business.
        // The loops will shut themselves down when they notice that the tenant is inactive.
        self.activate()?;

        info!("Done");

        Ok(())
    }

    /// Subroutine of `load_tenant`, to load an individual timeline
    ///
    /// NB: The parent is assumed to be already loaded!
    #[instrument(skip(self, local_metadata), fields(timeline_id=%timeline_id))]
    async fn load_local_timeline(
        &self,
        timeline_id: TimelineId,
        local_metadata: TimelineMetadata,
    ) -> anyhow::Result<()> {
        let ancestor = if let Some(ancestor_timeline_id) = local_metadata.ancestor_timeline() {
            let ancestor_timeline = self.get_timeline(ancestor_timeline_id, false)
            .with_context(|| anyhow::anyhow!("cannot find ancestor timeline {ancestor_timeline_id} for timeline {timeline_id}"))?;
            Some(ancestor_timeline)
        } else {
            None
        };

        let remote_client = self
            .remote_storage
            .as_ref()
            .map(|remote_storage| {
                create_remote_timeline_client(
                    remote_storage.clone(),
                    self.conf,
                    self.tenant_id,
                    timeline_id,
                )
            })
            .transpose()?;

        let remote_startup_data = match &remote_client {
            Some(remote_client) => match remote_client.download_index_file().await {
                Ok(index_part) => {
                    let remote_metadata = index_part.parse_metadata().context("parse_metadata")?;
                    Some(RemoteStartupData {
                        index_part,
                        remote_metadata,
                    })
                }
                Err(DownloadError::NotFound) => {
                    info!("no index file was found on the remote");
                    None
                }
                Err(e) => return Err(anyhow::anyhow!(e)),
            },
            None => None,
        };

        self.setup_timeline(
            timeline_id,
            remote_client,
            remote_startup_data,
            Some(local_metadata),
            ancestor,
            false,
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
    ) -> anyhow::Result<Arc<Timeline>> {
        let timelines_accessor = self.timelines.lock().unwrap();
        let timeline = timelines_accessor.get(&timeline_id).with_context(|| {
            format!("Timeline {}/{} was not found", self.tenant_id, timeline_id)
        })?;

        if active_only && !timeline.is_active() {
            anyhow::bail!(
                "Timeline {}/{} is not active, state: {:?}",
                self.tenant_id,
                timeline_id,
                timeline.current_state()
            )
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
    pub fn create_empty_timeline(
        &self,
        new_timeline_id: TimelineId,
        initdb_lsn: Lsn,
        pg_version: u32,
    ) -> anyhow::Result<UninitializedTimeline> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot create empty timelines on inactive tenant"
        );

        let timelines = self.timelines.lock().unwrap();
        let timeline_uninit_mark = self.create_timeline_uninit_mark(new_timeline_id, &timelines)?;
        drop(timelines);

        let new_metadata = TimelineMetadata::new(
            Lsn(0),
            None,
            None,
            Lsn(0),
            initdb_lsn,
            initdb_lsn,
            pg_version,
        );
        self.prepare_timeline(
            new_timeline_id,
            new_metadata,
            timeline_uninit_mark,
            true,
            None,
        )
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
    ) -> anyhow::Result<Option<Arc<Timeline>>> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot create timelines on inactive tenant"
        );

        if self.get_timeline(new_timeline_id, false).is_ok() {
            debug!("timeline {new_timeline_id} already exists");
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
                    ancestor_timeline.wait_lsn(*lsn).await?;
                }

                self.branch_timeline(ancestor_timeline_id, new_timeline_id, ancestor_start_lsn)?
            }
            None => self.bootstrap_timeline(new_timeline_id, pg_version).await?,
        };

        Ok(Some(loaded_timeline))
    }

    /// perform one garbage collection iteration, removing old data files from disk.
    /// this function is periodically called by gc task.
    /// also it can be explicitly requested through page server api 'do_gc' command.
    ///
    /// 'target_timeline_id' specifies the timeline to GC, or None for all.
    /// `horizon` specifies delta from last lsn to preserve all object versions (pitr interval).
    /// `checkpoint_before_gc` parameter is used to force compaction of storage before GC
    /// to make tests more deterministic.
    /// TODO Do we still need it or we can call checkpoint explicitly in tests where needed?
    pub async fn gc_iteration(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        checkpoint_before_gc: bool,
    ) -> anyhow::Result<GcResult> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot run GC iteration on inactive tenant"
        );

        let timeline_str = target_timeline_id
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".to_string());

        {
            let _timer = STORAGE_TIME
                .with_label_values(&["gc", &self.tenant_id.to_string(), &timeline_str])
                .start_timer();
            self.gc_iteration_internal(target_timeline_id, horizon, pitr, checkpoint_before_gc)
                .await
        }
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor task.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    pub async fn compaction_iteration(&self) -> anyhow::Result<()> {
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
                .map(|(timeline_id, timeline)| (*timeline_id, timeline.clone()))
                .collect::<Vec<_>>();
            drop(timelines);
            timelines_to_compact
        };

        for (timeline_id, timeline) in &timelines_to_compact {
            timeline
                .compact()
                .instrument(info_span!("compact_timeline", timeline = %timeline_id))
                .await?;
        }

        Ok(())
    }

    /// Flush all in-memory data to disk.
    ///
    /// Used at graceful shutdown.
    ///
    pub async fn checkpoint(&self) -> anyhow::Result<()> {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // checkpoints. We don't want to block everything else while the
        // checkpoint runs.
        let timelines_to_checkpoint = {
            let timelines = self.timelines.lock().unwrap();
            timelines
                .iter()
                .map(|(id, timeline)| (*id, Arc::clone(timeline)))
                .collect::<Vec<_>>()
        };

        for (id, timeline) in &timelines_to_checkpoint {
            timeline
                .checkpoint(CheckpointConfig::Flush)
                .instrument(info_span!("checkpoint", timeline = %id, tenant = %self.tenant_id))
                .await?;
        }

        Ok(())
    }

    /// Removes timeline-related in-memory data
    pub async fn delete_timeline(&self, timeline_id: TimelineId) -> anyhow::Result<()> {
        // Transition the timeline into TimelineState::Stopping.
        // This should prevent new operations from starting.
        let timeline = {
            let mut timelines = self.timelines.lock().unwrap();

            // Ensure that there are no child timelines **attached to that pageserver**,
            // because detach removes files, which will break child branches
            let children_exist = timelines
                .iter()
                .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline_id));

            anyhow::ensure!(
                !children_exist,
                "Cannot delete timeline which has child timelines"
            );
            let timeline_entry = match timelines.entry(timeline_id) {
                Entry::Occupied(e) => e,
                Entry::Vacant(_) => bail!("timeline not found"),
            };

            let timeline = Arc::clone(timeline_entry.get());
            timeline.set_state(TimelineState::Stopping);

            drop(timelines);
            timeline
        };

        info!("waiting for layer_removal_cs.lock()");
        // No timeout here, GC & Compaction should be responsive to the `TimelineState::Stopping` change.
        let layer_removal_guard = timeline.layer_removal_cs.lock().await;
        info!("got layer_removal_cs.lock(), deleting layer files");

        // NB: storage_sync upload tasks that reference these layers have been cancelled
        //     by the caller.

        let local_timeline_directory = self.conf.timeline_path(&timeline_id, &self.tenant_id);
        // XXX make this atomic so that, if we crash-mid-way, the timeline won't be picked up
        // with some layers missing.
        std::fs::remove_dir_all(&local_timeline_directory).with_context(|| {
            format!(
                "Failed to remove local timeline directory '{}'",
                local_timeline_directory.display()
            )
        })?;
        info!("finished deleting layer files, releasing layer_removal_cs.lock()");

        drop(layer_removal_guard);

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
        let removed_timeline = timelines.remove(&timeline_id);
        if removed_timeline.is_none() {
            // This can legitimately happen if there's a concurrent call to this function.
            //   T1                                             T2
            //   lock
            //   unlock
            //                                                  lock
            //                                                  unlock
            //                                                  remove files
            //                                                  lock
            //                                                  remove from map
            //                                                  unlock
            //                                                  return
            //   remove files
            //   lock
            //   remove from map observes empty map
            //   unlock
            //   return
            debug!("concurrent call to this function won the race");
        }
        drop(timelines);

        Ok(())
    }

    pub fn current_state(&self) -> TenantState {
        *self.state.borrow()
    }

    pub fn is_active(&self) -> bool {
        self.current_state() == TenantState::Active
    }

    /// Changes tenant status to active, unless shutdown was already requested.
    fn activate(&self) -> anyhow::Result<()> {
        let mut result = Ok(());
        self.state.send_modify(|current_state| {
            match *current_state {
                TenantState::Active => {
                    // activate() was called on an already Active tenant. Shouldn't happen.
                    result = Err(anyhow::anyhow!("Tenant is already active"));
                }
                TenantState::Broken => {
                    // This shouldn't happen either
                    result = Err(anyhow::anyhow!(
                        "Could not activate tenant because it is in broken state"
                    ));
                }
                TenantState::Stopping => {
                    // The tenant was detached, or system shutdown was requested, while we were
                    // loading or attaching the tenant.
                    info!("Tenant is already in Stopping state, skipping activation");
                }
                TenantState::Loading | TenantState::Attaching => {
                    *current_state = TenantState::Active;

                    info!("Activating tenant {}", self.tenant_id);

                    let timelines_accessor = self.timelines.lock().unwrap();
                    let not_broken_timelines = timelines_accessor
                        .values()
                        .filter(|timeline| timeline.current_state() != TimelineState::Broken);

                    // Spawn gc and compaction loops. The loops will shut themselves
                    // down when they notice that the tenant is inactive.
                    crate::tenant_tasks::start_background_loops(self.tenant_id);

                    for timeline in not_broken_timelines {
                        timeline.set_state(TimelineState::Active);
                        timeline.launch_wal_receiver();
                    }
                }
            }
        });
        result
    }

    /// Change tenant status to Stopping, to mark that it is being shut down
    pub fn set_stopping(&self) {
        self.state.send_modify(|current_state| {
            match *current_state {
                TenantState::Active | TenantState::Loading | TenantState::Attaching => {
                    *current_state = TenantState::Stopping;

                    // FIXME: If the tenant is still Loading or Attaching, new timelines
                    // might be created after this. That's harmless, as the Timelines
                    // won't be accessible to anyone, when the Tenant is in Stopping
                    // state.
                    let timelines_accessor = self.timelines.lock().unwrap();
                    let not_broken_timelines = timelines_accessor
                        .values()
                        .filter(|timeline| timeline.current_state() != TimelineState::Broken);
                    for timeline in not_broken_timelines {
                        timeline.set_state(TimelineState::Suspended);
                    }
                }
                TenantState::Broken => {
                    info!("Cannot set tenant to Stopping state, it is already in Broken state");
                }
                TenantState::Stopping => {
                    // The tenant was detached, or system shutdown was requested, while we were
                    // loading or attaching the tenant.
                    info!("Tenant is already in Stopping state");
                }
            }
        });
    }

    pub fn set_broken(&self) {
        self.state.send_modify(|current_state| {
            match *current_state {
                TenantState::Active => {
                    // Broken tenants can currently only used for fatal errors that happen
                    // while loading or attaching a tenant. A tenant that has already been
                    // activated should never be marked as broken. We cope with it the best
                    // we can, but it shouldn't happen.
                    *current_state = TenantState::Broken;
                    warn!("Changing Active tenant to Broken state");
                }
                TenantState::Broken => {
                    // This shouldn't happen either
                    warn!("Tenant is already broken");
                }
                TenantState::Stopping => {
                    // This shouldn't happen either
                    *current_state = TenantState::Broken;
                    warn!("Marking Stopping tenant as Broken");
                }
                TenantState::Loading | TenantState::Attaching => {
                    *current_state = TenantState::Broken;
                }
            }
        });
    }

    pub fn subscribe_for_state_updates(&self) -> watch::Receiver<TenantState> {
        self.state.subscribe()
    }

    pub async fn wait_to_become_active(&self) -> anyhow::Result<()> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = *receiver.borrow_and_update();
            match current_state {
                TenantState::Loading | TenantState::Attaching => {
                    // in these states, there's a chance that we can reach ::Active
                    receiver.changed().await?;
                }
                TenantState::Active { .. } => {
                    return Ok(());
                }
                TenantState::Broken | TenantState::Stopping => {
                    // There's no chance the tenant can transition back into ::Active
                    anyhow::bail!(
                        "Tenant {} will not become active. Current state: {:?}",
                        self.tenant_id,
                        current_state,
                    );
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

/// Private functions
impl Tenant {
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

    pub fn update_tenant_config(&self, new_tenant_conf: TenantConfOpt) {
        self.tenant_conf.write().unwrap().update(&new_tenant_conf);
    }

    fn create_timeline_data(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        remote_client: Option<RemoteTimelineClient>,
    ) -> anyhow::Result<Timeline> {
        if let Some(ancestor_timeline_id) = new_metadata.ancestor_timeline() {
            anyhow::ensure!(
                ancestor.is_some(),
                "Timeline's {new_timeline_id} ancestor {ancestor_timeline_id} was not found"
            )
        }

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
            pg_version,
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
        let (state, _) = watch::channel(state);
        Tenant {
            tenant_id,
            conf,
            tenant_conf: Arc::new(RwLock::new(tenant_conf)),
            timelines: Mutex::new(HashMap::new()),
            gc_cs: Mutex::new(()),
            walredo_mgr,
            remote_storage,
            state,
            cached_logical_sizes: tokio::sync::Mutex::new(HashMap::new()),
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
        target_config_path: &Path,
        tenant_conf: TenantConfOpt,
        first_save: bool,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("saving tenantconf").entered();
        info!("persisting tenantconf to {}", target_config_path.display());

        // TODO this will prepend comments endlessly
        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.

[tenant_config]
"#
        .to_string();

        // Convert the config to a toml file.
        conf_content += &toml_edit::easy::to_string(&tenant_conf)?;

        let mut target_config_file = VirtualFile::open_with_options(
            target_config_path,
            OpenOptions::new().write(true).create_new(first_save),
        )?;

        target_config_file
            .write(conf_content.as_bytes())
            .context("Failed to write toml bytes into file")
            .and_then(|_| {
                target_config_file
                    .sync_all()
                    .context("Faile to fsync config file")
            })
            .with_context(|| {
                format!(
                    "Failed to write config file into path '{}'",
                    target_config_path.display()
                )
            })?;

        // fsync the parent directory to ensure the directory entry is durable
        if first_save {
            target_config_path
                .parent()
                .context("Config file does not have a parent")
                .and_then(|target_config_parent| {
                    File::open(target_config_parent).context("Failed to open config parent")
                })
                .and_then(|tenant_dir| {
                    tenant_dir
                        .sync_all()
                        .context("Failed to fsync config parent")
                })
                .with_context(|| {
                    format!(
                        "Failed to fsync on first save for config {}",
                        target_config_path.display()
                    )
                })?;
        }

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
        checkpoint_before_gc: bool,
    ) -> anyhow::Result<GcResult> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        let gc_timelines = self.refresh_gc_info_internal(target_timeline_id, horizon, pitr)?;

        utils::failpoint_sleep_millis_async!("gc_iteration_internal_after_getting_gc_timelines");

        info!("starting on {} timelines", gc_timelines.len());

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

            // If requested, force flush all in-memory layers to disk first,
            // so that they too can be garbage collected. That's
            // used in tests, so we want as deterministic results as possible.
            if checkpoint_before_gc {
                timeline.checkpoint(CheckpointConfig::Forced).await?;
                info!(
                    "timeline {} checkpoint_before_gc done",
                    timeline.timeline_id
                );
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
    pub fn refresh_gc_info(&self) -> anyhow::Result<Vec<Arc<Timeline>>> {
        // since this method can now be called at different rates than the configured gc loop, it
        // might be that these configuration values get applied faster than what it was previously,
        // since these were only read from the gc task.
        let horizon = self.get_gc_horizon();
        let pitr = self.get_pitr_interval();

        // refresh all timelines
        let target_timeline_id = None;

        self.refresh_gc_info_internal(target_timeline_id, horizon, pitr)
    }

    fn refresh_gc_info_internal(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
    ) -> anyhow::Result<Vec<Arc<Timeline>>> {
        // grab mutex to prevent new timelines from being created here.
        let gc_cs = self.gc_cs.lock().unwrap();

        let timelines = self.timelines.lock().unwrap();

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        let mut all_branchpoints: BTreeSet<(TimelineId, Lsn)> = BTreeSet::new();
        let timeline_ids = {
            if let Some(target_timeline_id) = target_timeline_id.as_ref() {
                if timelines.get(target_timeline_id).is_none() {
                    bail!("gc target timeline does not exist")
                }
            };

            timelines
                .iter()
                .map(|(timeline_id, timeline_entry)| {
                    if let Some(ancestor_timeline_id) = &timeline_entry.get_ancestor_timeline_id() {
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
                            all_branchpoints
                                .insert((*ancestor_timeline_id, timeline_entry.get_ancestor_lsn()));
                        }
                    }

                    *timeline_id
                })
                .collect::<Vec<_>>()
        };
        drop(timelines);

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
                timeline.update_gc_info(branchpoints, cutoff, pitr)?;

                gc_timelines.push(timeline);
            }
        }
        drop(gc_cs);
        Ok(gc_timelines)
    }

    /// Branch an existing timeline
    fn branch_timeline(
        &self,
        src: TimelineId,
        dst: TimelineId,
        start_lsn: Option<Lsn>,
    ) -> anyhow::Result<Arc<Timeline>> {
        // We need to hold this lock to prevent GC from starting at the same time. GC scans the directory to learn
        // about timelines, so otherwise a race condition is possible, where we create new timeline and GC
        // concurrently removes data that is needed by the new timeline.
        let _gc_cs = self.gc_cs.lock().unwrap();
        let timelines = self.timelines.lock().unwrap();
        let timeline_uninit_mark = self.create_timeline_uninit_mark(dst, &timelines)?;
        drop(timelines);

        // In order for the branch creation task to not wait for GC/compaction,
        // we need to make sure that the starting LSN of the child branch is not out of scope midway by
        //
        // 1. holding the GC lock to prevent overwritting timeline's GC data
        // 2. checking both the latest GC cutoff LSN and latest GC info of the source timeline
        //
        // Step 2 is to avoid initializing the new branch using data removed by past GC iterations
        // or in-queue GC iterations.

        let src_timeline = self.get_timeline(src, false).with_context(|| {
            format!(
                "No ancestor {} found for timeline {}/{}",
                src, self.tenant_id, dst
            )
        })?;

        let latest_gc_cutoff_lsn = src_timeline.get_latest_gc_cutoff_lsn();

        // If no start LSN is specified, we branch the new timeline from the source timeline's last record LSN
        let start_lsn = start_lsn.unwrap_or_else(|| {
            let lsn = src_timeline.get_last_record_lsn();
            info!("branching timeline {dst} from timeline {src} at last record LSN: {lsn}");
            lsn
        });

        // Check if the starting LSN is out of scope because it is less than
        // 1. the latest GC cutoff LSN or
        // 2. the planned GC cutoff LSN, which is from an in-queue GC iteration.
        src_timeline
            .check_lsn_is_in_scope(start_lsn, &latest_gc_cutoff_lsn)
            .context(format!(
                "invalid branch start lsn: less than latest GC cutoff {}",
                *latest_gc_cutoff_lsn,
            ))?;
        {
            let gc_info = src_timeline.gc_info.read().unwrap();
            let cutoff = min(gc_info.pitr_cutoff, gc_info.horizon_cutoff);
            if start_lsn < cutoff {
                bail!(format!(
                    "invalid branch start lsn: less than planned GC cutoff {cutoff}"
                ));
            }
        }

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
            Some(src),
            start_lsn,
            *src_timeline.latest_gc_cutoff_lsn.read(), // FIXME: should we hold onto this guard longer?
            src_timeline.initdb_lsn,
            src_timeline.pg_version,
        );
        let mut timelines = self.timelines.lock().unwrap();
        let new_timeline = self
            .prepare_timeline(
                dst,
                metadata,
                timeline_uninit_mark,
                false,
                Some(src_timeline),
            )?
            .initialize_with_lock(&mut timelines, true, true)?;
        drop(timelines);
        info!("branched timeline {dst} from {src} at {start_lsn}");

        Ok(new_timeline)
    }

    /// - run initdb to init temporary instance and get bootstrap data
    /// - after initialization complete, remove the temp dir.
    async fn bootstrap_timeline(
        &self,
        timeline_id: TimelineId,
        pg_version: u32,
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
        );
        let raw_timeline =
            self.prepare_timeline(timeline_id, new_metadata, timeline_uninit_mark, true, None)?;

        let tenant_id = raw_timeline.owning_tenant.tenant_id;
        let unfinished_timeline = raw_timeline.raw_timeline()?;

        tokio::task::block_in_place(|| {
            import_datadir::import_timeline_from_postgres_datadir(
                unfinished_timeline,
                pgdata_path,
                pgdata_lsn,
            )
        })
        .with_context(|| {
            format!("Failed to import pgdatadir for timeline {tenant_id}/{timeline_id}")
        })?;

        // Flush the new layer files to disk, before we mark the timeline as available to
        // the outside world.
        //
        // Thus spawn flush loop manually and skip flush_loop setup in initialize_with_lock
        unfinished_timeline.maybe_spawn_flush_loop();

        fail_point!("before-checkpoint-new-timeline", |_| {
            anyhow::bail!("failpoint before-checkpoint-new-timeline");
        });

        unfinished_timeline
            .checkpoint(CheckpointConfig::Flush).await
            .with_context(|| format!("Failed to checkpoint after pgdatadir import for timeline {tenant_id}/{timeline_id}"))?;

        let timeline = {
            let mut timelines = self.timelines.lock().unwrap();
            raw_timeline.initialize_with_lock(&mut timelines, false, true)?
        };

        info!(
            "created root timeline {} timeline.lsn {}",
            timeline_id,
            timeline.get_last_record_lsn()
        );

        Ok(timeline)
    }

    /// Creates intermediate timeline structure and its files, without loading it into memory.
    /// It's up to the caller to import the necesary data and import the timeline into memory.
    fn prepare_timeline(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: TimelineMetadata,
        uninit_mark: TimelineUninitMark,
        init_layers: bool,
        ancestor: Option<Arc<Timeline>>,
    ) -> anyhow::Result<UninitializedTimeline> {
        let tenant_id = self.tenant_id;

        let remote_client = if let Some(remote_storage) = self.remote_storage.as_ref() {
            let remote_client = create_remote_timeline_client(
                remote_storage.clone(),
                self.conf,
                tenant_id,
                new_timeline_id,
            )?;
            remote_client.init_upload_queue_for_empty_remote(&new_metadata)?;
            Some(remote_client)
        } else {
            None
        };

        match self.create_timeline_files(
            &uninit_mark.timeline_path,
            new_timeline_id,
            new_metadata,
            ancestor,
            remote_client,
        ) {
            Ok(new_timeline) => {
                if init_layers {
                    new_timeline.layers.write().unwrap().next_open_layer_at =
                        Some(new_timeline.initdb_lsn);
                }
                debug!(
                    "Successfully created initial files for timeline {tenant_id}/{new_timeline_id}"
                );
                Ok(UninitializedTimeline {
                    owning_tenant: self,
                    timeline_id: new_timeline_id,
                    raw_timeline: Some((Arc::new(new_timeline), uninit_mark)),
                })
            }
            Err(e) => {
                error!("Failed to create initial files for timeline {tenant_id}/{new_timeline_id}, cleaning up: {e:?}");
                cleanup_timeline_directory(uninit_mark);
                Err(e)
            }
        }
    }

    fn create_timeline_files(
        &self,
        timeline_path: &Path,
        new_timeline_id: TimelineId,
        new_metadata: TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        remote_client: Option<RemoteTimelineClient>,
    ) -> anyhow::Result<Timeline> {
        let timeline_data = self
            .create_timeline_data(
                new_timeline_id,
                new_metadata.clone(),
                ancestor,
                remote_client,
            )
            .context("Failed to create timeline data structure")?;
        crashsafe::create_dir_all(timeline_path).context("Failed to create timeline directory")?;

        fail_point!("after-timeline-uninit-mark-creation", |_| {
            anyhow::bail!("failpoint after-timeline-uninit-mark-creation");
        });

        save_metadata(
            self.conf,
            new_timeline_id,
            self.tenant_id,
            &new_metadata,
            true,
        )
        .context("Failed to create timeline metadata")?;

        Ok(timeline_data)
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
    pub async fn gather_size_inputs(&self) -> anyhow::Result<size::ModelInputs> {
        let logical_sizes_at_once = self
            .conf
            .concurrent_tenant_size_logical_size_queries
            .inner();

        // TODO: Having a single mutex block concurrent reads is unfortunate, but since the queries
        // are for testing/experimenting, we tolerate this.
        //
        // See more for on the issue #2748 condenced out of the initial PR review.
        let mut shared_cache = self.cached_logical_sizes.lock().await;

        size::gather_inputs(self, logical_sizes_at_once, &mut *shared_cache).await
    }
}

fn remove_timeline_and_uninit_mark(timeline_dir: &Path, uninit_mark: &Path) -> anyhow::Result<()> {
    fs::remove_dir_all(&timeline_dir)
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
    fs::remove_file(&uninit_mark).with_context(|| {
        format!(
            "Failed to remove timeline uninit mark file {}",
            uninit_mark.display()
        )
    })?;

    Ok(())
}

pub(crate) fn create_tenant_files(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> anyhow::Result<PathBuf> {
    let target_tenant_directory = conf.tenant_path(&tenant_id);
    anyhow::ensure!(
        !target_tenant_directory.exists(),
        "cannot create new tenant repo: '{tenant_id}' directory already exists",
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
    temporary_tenant_dir: &Path,
    target_tenant_directory: &Path,
) -> Result<(), anyhow::Error> {
    let temporary_tenant_timelines_dir = rebase_directory(
        &conf.timelines_path(&tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("Failed to resolve tenant {tenant_id} temporary timelines dir"))?;
    let temporary_tenant_config_path = rebase_directory(
        &conf.tenant_config_path(tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("Failed to resolve tenant {tenant_id} temporary config path"))?;

    Tenant::persist_tenant_config(&temporary_tenant_config_path, tenant_conf, true).with_context(
        || {
            format!(
                "Failed to write tenant {} config to {}",
                tenant_id,
                temporary_tenant_config_path.display()
            )
        },
    )?;
    crashsafe::create_dir(&temporary_tenant_timelines_dir).with_context(|| {
        format!(
            "could not create tenant {} temporary timelines directory {}",
            tenant_id,
            temporary_tenant_timelines_dir.display()
        )
    })?;
    fail_point!("tenant-creation-before-tmp-rename", |_| {
        anyhow::bail!("failpoint tenant-creation-before-tmp-rename");
    });

    fs::rename(&temporary_tenant_dir, target_tenant_directory).with_context(|| {
        format!(
            "failed to move tenant {} temporary directory {} into the permanent one {}",
            tenant_id,
            temporary_tenant_dir.display(),
            target_tenant_directory.display()
        )
    })?;
    let target_dir_parent = target_tenant_directory.parent().with_context(|| {
        format!(
            "Failed to get tenant {} dir parent for {}",
            tenant_id,
            target_tenant_directory.display()
        )
    })?;
    crashsafe::fsync(target_dir_parent).with_context(|| {
        format!(
            "Failed to fsync renamed directory's parent {} for tenant {}",
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
        .args(&["-D", &initdb_target_dir.to_string_lossy()])
        .args(&["-U", &conf.superuser])
        .args(&["-E", "utf8"])
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
pub fn dump_layerfile_from_path(path: &Path, verbose: bool) -> anyhow::Result<()> {
    use std::os::unix::fs::FileExt;

    // All layer files start with a two-byte "magic" value, to identify the kind of
    // file.
    let file = File::open(path)?;
    let mut header_buf = [0u8; 2];
    file.read_exact_at(&mut header_buf, 0)?;

    match u16::from_be_bytes(header_buf) {
        crate::IMAGE_FILE_MAGIC => {
            image_layer::ImageLayer::new_for_path(path, file)?.dump(verbose)?
        }
        crate::DELTA_FILE_MAGIC => {
            delta_layer::DeltaLayer::new_for_path(path, file)?.dump(verbose)?
        }
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
    use once_cell::sync::Lazy;
    use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
    use std::{fs, path::PathBuf};
    use utils::lsn::Lsn;

    use crate::{
        config::PageServerConf,
        repository::Key,
        tenant::Tenant,
        walrecord::NeonWalRecord,
        walredo::{WalRedoError, WalRedoManager},
    };

    use super::*;
    use crate::tenant_config::{TenantConf, TenantConfOpt};
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

    static LOCK: Lazy<RwLock<()>> = Lazy::new(|| RwLock::new(()));

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
            }
        }
    }

    pub struct TenantHarness<'a> {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_id: TenantId,

        pub lock_guard: (
            Option<RwLockReadGuard<'a, ()>>,
            Option<RwLockWriteGuard<'a, ()>>,
        ),
    }

    impl<'a> TenantHarness<'a> {
        pub fn create(test_name: &'static str) -> anyhow::Result<Self> {
            Self::create_internal(test_name, false)
        }
        pub fn create_exclusive(test_name: &'static str) -> anyhow::Result<Self> {
            Self::create_internal(test_name, true)
        }
        fn create_internal(test_name: &'static str, exclusive: bool) -> anyhow::Result<Self> {
            let lock_guard = if exclusive {
                (None, Some(LOCK.write().unwrap()))
            } else {
                (Some(LOCK.read().unwrap()), None)
            };

            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            // Disable automatic GC and compaction to make the unit tests more deterministic.
            // The tests perform them manually if needed.
            let mut tenant_conf = TenantConf::dummy_conf();
            tenant_conf.gc_period = Duration::ZERO;
            tenant_conf.compaction_period = Duration::ZERO;

            let tenant_id = TenantId::generate();
            fs::create_dir_all(conf.tenant_path(&tenant_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_id))?;

            Ok(Self {
                conf,
                tenant_conf,
                tenant_id,
                lock_guard,
            })
        }

        pub async fn load(&self) -> Arc<Tenant> {
            self.try_load().await.expect("failed to load test tenant")
        }

        pub async fn try_load(&self) -> anyhow::Result<Arc<Tenant>> {
            let walredo_mgr = Arc::new(TestRedoManager);

            let tenant = Arc::new(Tenant::new(
                TenantState::Loading,
                self.conf,
                TenantConfOpt::from(self.tenant_conf),
                walredo_mgr,
                self.tenant_id,
                None,
            ));
            // populate tenant with locally available timelines
            let mut timelines_to_load = HashMap::new();
            for timeline_dir_entry in fs::read_dir(self.conf.timelines_path(&self.tenant_id))
                .expect("should be able to read timelines dir")
            {
                let timeline_dir_entry = timeline_dir_entry?;
                let timeline_id: TimelineId = timeline_dir_entry
                    .path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .parse()?;

                let timeline_metadata = load_metadata(self.conf, timeline_id, self.tenant_id)?;
                timelines_to_load.insert(timeline_id, timeline_metadata);
            }
            // FIXME starts background jobs
            tenant.load().await?;

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
        let tenant = TenantHarness::create("test_basic")?.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));

        Ok(())
    }

    #[tokio::test]
    async fn no_duplicate_timelines() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("no_duplicate_timelines")?
            .load()
            .await;
        let _ = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        match tenant.create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION) {
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
        let tenant = TenantHarness::create("test_branch")?.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;
        let writer = tline.writer();
        use std::str::from_utf8;

        #[allow(non_snake_case)]
        let TEST_KEY_A: Key = Key::from_hex("112222222233333333444444445500000001").unwrap();
        #[allow(non_snake_case)]
        let TEST_KEY_B: Key = Key::from_hex("112222222233333333444444445500000002").unwrap();

        // Insert a value on the timeline
        writer.put(TEST_KEY_A, Lsn(0x20), &test_value("foo at 0x20"))?;
        writer.put(TEST_KEY_B, Lsn(0x20), &test_value("foobar at 0x20"))?;
        writer.finish_write(Lsn(0x20));

        writer.put(TEST_KEY_A, Lsn(0x30), &test_value("foo at 0x30"))?;
        writer.finish_write(Lsn(0x30));
        writer.put(TEST_KEY_A, Lsn(0x40), &test_value("foo at 0x40"))?;
        writer.finish_write(Lsn(0x40));

        //assert_current_logical_size(&tline, Lsn(0x40));

        // Branch the history, modify relation differently on the new timeline
        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x30)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");
        let new_writer = newtline.writer();
        new_writer.put(TEST_KEY_A, Lsn(0x40), &test_value("bar at 0x40"))?;
        new_writer.finish_write(Lsn(0x40));

        // Check page contents on both branches
        assert_eq!(
            from_utf8(&tline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "foo at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_A, Lsn(0x40))?)?,
            "bar at 0x40"
        );
        assert_eq!(
            from_utf8(&newtline.get(TEST_KEY_B, Lsn(0x40))?)?,
            "foobar at 0x20"
        );

        //assert_current_logical_size(&tline, Lsn(0x40));

        Ok(())
    }

    async fn make_some_layers(tline: &Timeline, start_lsn: Lsn) -> anyhow::Result<()> {
        let mut lsn = start_lsn;
        #[allow(non_snake_case)]
        {
            let writer = tline.writer();
            // Create a relation on the timeline
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
        }
        tline.checkpoint(CheckpointConfig::Forced).await?;
        {
            let writer = tline.writer();
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer.put(
                *TEST_KEY,
                lsn,
                &Value::Image(TEST_IMG(&format!("foo at {}", lsn))),
            )?;
            writer.finish_write(lsn);
        }
        tline.checkpoint(CheckpointConfig::Forced).await
    }

    #[tokio::test]
    async fn test_prohibit_branch_creation_on_garbage_collected_data() -> anyhow::Result<()> {
        let tenant =
            TenantHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")?
                .load()
                .await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the checkpointing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)
            .await?;

        // try to branch at lsn 25, should fail because we already garbage collected the data
        match tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
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
        let tenant = TenantHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")?
            .load()
            .await;

        tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0x50), DEFAULT_PG_VERSION)?
            .initialize()?;
        // try to branch at lsn 0x25, should fail because initdb lsn is 0x50
        match tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x25))) {
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

        repo.gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)?;
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
    async fn test_retain_data_in_parent_which_is_needed_for_child() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("test_retain_data_in_parent_which_is_needed_for_child")?
            .load()
            .await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");
        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)
            .await?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25)).is_ok());

        Ok(())
    }
    #[tokio::test]
    async fn test_parent_keeps_data_forever_after_branching() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("test_parent_keeps_data_forever_after_branching")?
            .load()
            .await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;
        make_some_layers(tline.as_ref(), Lsn(0x20)).await?;

        tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;
        let newtline = tenant
            .get_timeline(NEW_TIMELINE_ID, true)
            .expect("Should have a local timeline");

        make_some_layers(newtline.as_ref(), Lsn(0x60)).await?;

        // run gc on parent
        tenant
            .gc_iteration(Some(TIMELINE_ID), 0x10, Duration::ZERO, false)
            .await?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50))?,
            TEST_IMG(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn timeline_load() -> anyhow::Result<()> {
        const TEST_NAME: &str = "timeline_load";
        let harness = TenantHarness::create(TEST_NAME)?;
        {
            let tenant = harness.load().await;
            let tline = tenant
                .create_empty_timeline(TIMELINE_ID, Lsn(0x8000), DEFAULT_PG_VERSION)?
                .initialize()?;
            make_some_layers(tline.as_ref(), Lsn(0x8000)).await?;
            tline.checkpoint(CheckpointConfig::Forced).await?;
        }

        let tenant = harness.load().await;
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
            let tenant = harness.load().await;
            let tline = tenant
                .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
                .initialize()?;

            make_some_layers(tline.as_ref(), Lsn(0x20)).await?;
            tline.checkpoint(CheckpointConfig::Forced).await?;

            tenant.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Some(Lsn(0x40)))?;

            let newtline = tenant
                .get_timeline(NEW_TIMELINE_ID, true)
                .expect("Should have a local timeline");

            make_some_layers(newtline.as_ref(), Lsn(0x60)).await?;
            tline.checkpoint(CheckpointConfig::Forced).await?;
        }

        // check that both of them are initially unloaded
        let tenant = harness.load().await;

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
        let tenant = harness.load().await;

        tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;
        drop(tenant);

        let metadata_path = harness.timeline_path(&TIMELINE_ID).join(METADATA_FILE_NAME);

        assert!(metadata_path.is_file());

        let mut metadata_bytes = std::fs::read(&metadata_path)?;
        assert_eq!(metadata_bytes.len(), 512);
        metadata_bytes[8] ^= 1;
        std::fs::write(metadata_path, metadata_bytes)?;

        let err = harness.try_load().await.err().expect("should fail");
        assert!(err
            .to_string()
            .starts_with("Failed to parse metadata bytes from path"));

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
        let tenant = TenantHarness::create("test_images")?.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x10), &Value::Image(TEST_IMG("foo at 0x10")))?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x20), &Value::Image(TEST_IMG("foo at 0x20")))?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x30), &Value::Image(TEST_IMG("foo at 0x30")))?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        let writer = tline.writer();
        writer.put(*TEST_KEY, Lsn(0x40), &Value::Image(TEST_IMG("foo at 0x40")))?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.checkpoint(CheckpointConfig::Forced).await?;
        tline.compact().await?;

        assert_eq!(tline.get(*TEST_KEY, Lsn(0x10))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x1f))?, TEST_IMG("foo at 0x10"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x20))?, TEST_IMG("foo at 0x20"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x30))?, TEST_IMG("foo at 0x30"));
        assert_eq!(tline.get(*TEST_KEY, Lsn(0x40))?, TEST_IMG("foo at 0x40"));

        Ok(())
    }

    //
    // Insert 1000 key-value pairs with increasing keys, checkpoint,
    // repeat 50 times.
    //
    #[tokio::test]
    async fn test_bulk_insert() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("test_bulk_insert")?.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        let mut lsn = Lsn(0x10);

        let mut keyspace = KeySpaceAccum::new();

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        let mut blknum = 0;
        for _ in 0..50 {
            for _ in 0..10000 {
                test_key.field6 = blknum;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);

                keyspace.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            let cutoff = tline.get_last_record_lsn();

            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
            tline.checkpoint(CheckpointConfig::Forced).await?;
            tline.compact().await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_random_updates() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("test_random_updates")?.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
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
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
            tline.checkpoint(CheckpointConfig::Forced).await?;
            tline.compact().await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_branches() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("test_traverse_branches")?
            .load()
            .await;
        let mut tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let writer = tline.writer();
            writer.put(
                test_key,
                lsn,
                &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
            )?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);

            keyspace.add_key(test_key);
        }

        let mut tline_id = TIMELINE_ID;
        for _ in 0..50 {
            let new_tline_id = TimelineId::generate();
            tenant.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = tenant
                .get_timeline(new_tline_id, true)
                .expect("Should have the branched timeline");
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} at {}", blknum, lsn))),
                )?;
                println!("updating {} at {}", blknum, lsn);
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = blknum as u32;
                assert_eq!(
                    tline.get(test_key, lsn)?,
                    TEST_IMG(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of checkpoint, compaction, and GC
            println!("checkpointing {}", lsn);
            let cutoff = tline.get_last_record_lsn();
            tline.update_gc_info(Vec::new(), cutoff, Duration::ZERO)?;
            tline.checkpoint(CheckpointConfig::Forced).await?;
            tline.compact().await?;
            tline.gc().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_ancestors() -> anyhow::Result<()> {
        let tenant = TenantHarness::create("test_traverse_ancestors")?
            .load()
            .await;
        let mut tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION)?
            .initialize()?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("012222222233333333444444445500000000").unwrap();
        // Track page mutation lsns across different timelines.
        let mut updated = [[Lsn(0); NUM_KEYS]; NUM_TLINES];

        let mut lsn = Lsn(0);
        let mut tline_id = TIMELINE_ID;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..NUM_TLINES {
            let new_tline_id = TimelineId::generate();
            tenant.branch_timeline(tline_id, new_tline_id, Some(lsn))?;
            tline = tenant
                .get_timeline(new_tline_id, true)
                .expect("Should have the branched timeline");
            tline_id = new_tline_id;

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = blknum as u32;
                let writer = tline.writer();
                writer.put(
                    test_key,
                    lsn,
                    &Value::Image(TEST_IMG(&format!("{} {} at {}", idx, blknum, lsn))),
                )?;
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
                    tline.get(test_key, *lsn)?,
                    TEST_IMG(&format!("{idx} {blknum} at {lsn}"))
                );
            }
        }
        Ok(())
    }
}
