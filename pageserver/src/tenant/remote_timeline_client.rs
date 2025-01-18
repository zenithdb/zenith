//! This module manages synchronizing local FS with remote storage.
//!
//! # Overview
//!
//! * [`RemoteTimelineClient`] provides functions related to upload/download of a particular timeline.
//!   It contains a queue of pending uploads, and manages the queue, performing uploads in parallel
//!   when it's safe to do so.
//!
//! * Stand-alone function, [`list_remote_timelines`], to get list of timelines of a tenant.
//!
//! These functions use the low-level remote storage client, [`remote_storage::RemoteStorage`].
//!
//! # APIs & How To Use Them
//!
//! There is a [RemoteTimelineClient] for each [Timeline][`crate::tenant::Timeline`] in the system,
//! unless the pageserver is configured without remote storage.
//!
//! We allocate the client instance in [Timeline][`crate::tenant::Timeline`], i.e.,
//! either in [`crate::tenant::mgr`] during startup or when creating a new
//! timeline.
//! However, the client does not become ready for use until we've initialized its upload queue:
//!
//! - For timelines that already have some state on the remote storage, we use
//!   [`RemoteTimelineClient::init_upload_queue`] .
//! - For newly created timelines, we use
//!   [`RemoteTimelineClient::init_upload_queue_for_empty_remote`].
//!
//! The former takes the remote's [`IndexPart`] as an argument, possibly retrieved
//! using [`list_remote_timelines`]. We'll elaborate on [`IndexPart`] in the next section.
//!
//! Whenever we've created/updated/deleted a file in a timeline directory, we schedule
//! the corresponding remote operation with the timeline's [`RemoteTimelineClient`]:
//!
//! - [`RemoteTimelineClient::schedule_layer_file_upload`]  when we've created a new layer file.
//! - [`RemoteTimelineClient::schedule_index_upload_for_metadata_update`] when we've updated the timeline metadata file.
//! - [`RemoteTimelineClient::schedule_index_upload_for_file_changes`] to upload an updated index file, after we've scheduled file uploads
//! - [`RemoteTimelineClient::schedule_layer_file_deletion`] when we've deleted one or more layer files.
//!
//! Internally, these functions create [`UploadOp`]s and put them in a queue.
//!
//! There are also APIs for downloading files.
//! These are not part of the aforementioned queuing and will not be discussed
//! further here, except in the section covering tenant attach.
//!
//! # Remote Storage Structure & [`IndexPart`] Index File
//!
//! The "directory structure" in the remote storage mirrors the local directory structure, with paths
//! like `tenants/<tenant_id>/timelines/<timeline_id>/<layer filename>`.
//! Yet instead of keeping the `metadata` file remotely, we wrap it with more
//! data in an "index file" aka [`IndexPart`], containing the list of **all** remote
//! files for a given timeline.
//! If a file is not referenced from [`IndexPart`], it's not part of the remote storage state.
//!
//! Having the `IndexPart` also avoids expensive and slow `S3 list` commands.
//!
//! # Consistency
//!
//! To have a consistent remote structure, it's important that uploads and
//! deletions are performed in the right order. For example, the index file
//! contains a list of layer files, so it must not be uploaded until all the
//! layer files that are in its list have been successfully uploaded.
//!
//! The contract between client and its user is that the user is responsible of
//! scheduling operations in an order that keeps the remote consistent as
//! described above.
//!
//! From the user's perspective, the operations are executed sequentially.
//! Internally, the client knows which operations can be performed in parallel,
//! and which operations act like a "barrier" that require preceding operations
//! to finish. The calling code just needs to call the schedule-functions in the
//! correct order, and the client will parallelize the operations in a way that
//! is safe. For more details, see `UploadOp::can_bypass`.
//!
//! All of this relies on the following invariants:
//!
//! - We rely on read-after write consistency in the remote storage.
//! - Layer files are immutable.
//!
//! NB: Pageserver assumes that it has exclusive write access to the tenant in remote
//! storage. Different tenants can be attached to different pageservers, but if the
//! same tenant is attached to two pageservers at the same time, they will overwrite
//! each other's index file updates, and confusion will ensue. There's no interlock or
//! mechanism to detect that in the pageserver, we rely on the control plane to ensure
//! that that doesn't happen.
//!
//! ## Implementation Note
//!
//! The *actual* remote state lags behind the *desired* remote state while
//! there are in-flight operations.
//! We keep track of the desired remote state in [`UploadQueueInitialized::dirty`].
//! It is initialized based on the [`IndexPart`] that was passed during init
//! and updated with every `schedule_*` function call.
//! All this is necessary necessary to compute the future [`IndexPart`]s
//! when scheduling an operation while other operations that also affect the
//! remote [`IndexPart`] are in flight.
//!
//! # Retries & Error Handling
//!
//! The client retries operations indefinitely, using exponential back-off.
//! There is no way to force a retry, i.e., interrupt the back-off.
//! This could be built easily.
//!
//! # Cancellation
//!
//! The operations execute as plain [`task_mgr`] tasks, scoped to
//! the client's tenant and timeline.
//! Dropping the client will drop queued operations but not executing operations.
//! These will complete unless the `task_mgr` tasks are cancelled using `task_mgr`
//! APIs, e.g., during pageserver shutdown, timeline delete, or tenant detach.
//!
//! # Completion
//!
//! Once an operation has completed, we update [`UploadQueueInitialized::clean`] immediately,
//! and submit a request through the DeletionQueue to update
//! [`UploadQueueInitialized::visible_remote_consistent_lsn`] after it has
//! validated that our generation is not stale.  It is this visible value
//! that is advertized to safekeepers as a signal that that they can
//! delete the WAL up to that LSN.
//!
//! The [`RemoteTimelineClient::wait_completion`] method can be used to wait
//! for all pending operations to complete. It does not prevent more
//! operations from getting scheduled.
//!
//! # Crash Consistency
//!
//! We do not persist the upload queue state.
//! If we drop the client, or crash, all unfinished operations are lost.
//!
//! To recover, the following steps need to be taken:
//! - Retrieve the current remote [`IndexPart`]. This gives us a
//!   consistent remote state, assuming the user scheduled the operations in
//!   the correct order.
//! - Initiate upload queue with that [`IndexPart`].
//! - Reschedule all lost operations by comparing the local filesystem state
//!   and remote state as per [`IndexPart`]. This is done in
//!   [`TenantShard::timeline_init_and_sync`].
//!
//! Note that if we crash during file deletion between the index update
//! that removes the file from the list of files, and deleting the remote file,
//! the file is leaked in the remote storage. Similarly, if a new file is created
//! and uploaded, but the pageserver dies permanently before updating the
//! remote index file, the new file is leaked in remote storage. We accept and
//! tolerate that for now.
//! Note further that we cannot easily fix this by scheduling deletes for every
//! file that is present only on the remote, because we cannot distinguish the
//! following two cases:
//! - (1) We had the file locally, deleted it locally, scheduled a remote delete,
//!   but crashed before it finished remotely.
//! - (2) We never had the file locally because we haven't on-demand downloaded
//!   it yet.
//!
//! # Downloads
//!
//! In addition to the upload queue, [`RemoteTimelineClient`] has functions for
//! downloading files from the remote storage. Downloads are performed immediately
//! against the `RemoteStorage`, independently of the upload queue.
//!
//! When we attach a tenant, we perform the following steps:
//! - create `Tenant` object in `TenantState::Attaching` state
//! - List timelines that are present in remote storage, and for each:
//!   - download their remote [`IndexPart`]s
//!   - create `Timeline` struct and a `RemoteTimelineClient`
//!   - initialize the client's upload queue with its `IndexPart`
//!   - schedule uploads for layers that are only present locally.
//! - After the above is done for each timeline, open the tenant for business by
//!   transitioning it from `TenantState::Attaching` to `TenantState::Active` state.
//!   This starts the timelines' WAL-receivers and the tenant's GC & Compaction loops.
//!
//! # Operating Without Remote Storage
//!
//! If no remote storage configuration is provided, the [`RemoteTimelineClient`] is
//! not created and the uploads are skipped.
//!
//! [`TenantShard::timeline_init_and_sync`]: super::TenantShard::timeline_init_and_sync
//! [`Timeline::load_layer_map`]: super::Timeline::load_layer_map

pub(crate) mod download;
pub mod index;
pub mod manifest;
pub(crate) mod upload;

use anyhow::Context;
use camino::Utf8Path;
use chrono::{NaiveDateTime, Utc};

pub(crate) use download::download_initdb_tar_zst;
use pageserver_api::models::TimelineArchivalState;
use pageserver_api::shard::{ShardIndex, TenantShardId};
use regex::Regex;
use scopeguard::ScopeGuard;
use tokio_util::sync::CancellationToken;
use utils::backoff::{
    self, exponential_backoff, DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS,
};
use utils::pausable_failpoint;
use utils::shard::ShardNumber;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use remote_storage::{
    DownloadError, GenericRemoteStorage, ListingMode, RemotePath, TimeoutOrCancel,
};
use std::ops::DerefMut;
use tracing::{debug, error, info, instrument, warn};
use tracing::{info_span, Instrument};
use utils::lsn::Lsn;

use crate::context::RequestContext;
use crate::deletion_queue::{DeletionQueueClient, DeletionQueueError};
use crate::metrics::{
    MeasureRemoteOp, RemoteOpFileKind, RemoteOpKind, RemoteTimelineClientMetrics,
    RemoteTimelineClientMetricsCallTrackSize, REMOTE_ONDEMAND_DOWNLOADED_BYTES,
    REMOTE_ONDEMAND_DOWNLOADED_LAYERS,
};
use crate::task_mgr::shutdown_token;
use crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::download::download_retry;
use crate::tenant::storage_layer::AsLayerDesc;
use crate::tenant::upload_queue::{Delete, OpType, UploadQueueStoppedDeletable};
use crate::tenant::TIMELINES_SEGMENT_NAME;
use crate::{
    config::PageServerConf,
    task_mgr,
    task_mgr::TaskKind,
    task_mgr::BACKGROUND_RUNTIME,
    tenant::metadata::TimelineMetadata,
    tenant::upload_queue::{
        UploadOp, UploadQueue, UploadQueueInitialized, UploadQueueStopped, UploadTask,
    },
    TENANT_HEATMAP_BASENAME,
};

use utils::id::{TenantId, TimelineId};

use self::index::IndexPart;

use super::config::AttachedLocationConfig;
use super::metadata::MetadataUpdate;
use super::storage_layer::{Layer, LayerName, ResidentLayer};
use super::timeline::import_pgdata;
use super::upload_queue::{NotInitialized, SetDeletedFlagProgress};
use super::{DeleteTimelineError, Generation};

pub(crate) use download::{
    download_index_part, download_tenant_manifest, is_temp_download_file,
    list_remote_tenant_shards, list_remote_timelines,
};
pub(crate) use index::LayerFileMetadata;
pub(crate) use upload::upload_initdb_dir;

// Occasional network issues and such can cause remote operations to fail, and
// that's expected. If a download fails, we log it at info-level, and retry.
// But after FAILED_DOWNLOAD_WARN_THRESHOLD retries, we start to log it at WARN
// level instead, as repeated failures can mean a more serious problem. If it
// fails more than FAILED_DOWNLOAD_RETRIES times, we give up
pub(crate) const FAILED_DOWNLOAD_WARN_THRESHOLD: u32 = 3;
pub(crate) const FAILED_REMOTE_OP_RETRIES: u32 = 10;

// Similarly log failed uploads and deletions at WARN level, after this many
// retries. Uploads and deletions are retried forever, though.
pub(crate) const FAILED_UPLOAD_WARN_THRESHOLD: u32 = 3;

pub(crate) const INITDB_PATH: &str = "initdb.tar.zst";

pub(crate) const INITDB_PRESERVED_PATH: &str = "initdb-preserved.tar.zst";

/// Default buffer size when interfacing with [`tokio::fs::File`].
pub(crate) const BUFFER_SIZE: usize = 32 * 1024;

/// Doing non-essential flushes of deletion queue is subject to this timeout, after
/// which we warn and skip.
const DELETION_QUEUE_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

pub enum MaybeDeletedIndexPart {
    IndexPart(IndexPart),
    Deleted(IndexPart),
}

#[derive(Debug, thiserror::Error)]
pub enum PersistIndexPartWithDeletedFlagError {
    #[error("another task is already setting the deleted_flag, started at {0:?}")]
    AlreadyInProgress(NaiveDateTime),
    #[error("the deleted_flag was already set, value is {0:?}")]
    AlreadyDeleted(NaiveDateTime),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum WaitCompletionError {
    #[error(transparent)]
    NotInitialized(NotInitialized),
    #[error("wait_completion aborted because upload queue was stopped")]
    UploadQueueShutDownOrStopped,
}

#[derive(Debug, thiserror::Error)]
#[error("Upload queue either in unexpected state or hasn't downloaded manifest yet")]
pub struct UploadQueueNotReadyError;

#[derive(Debug, thiserror::Error)]
pub enum ShutdownIfArchivedError {
    #[error(transparent)]
    NotInitialized(NotInitialized),
    #[error("timeline is not archived")]
    NotArchived,
}

/// Behavioral modes that enable seamless live migration.
///
/// See docs/rfcs/028-pageserver-migration.md to understand how these fit in.
struct RemoteTimelineClientConfig {
    /// If this is false, then update to remote_consistent_lsn are dropped rather
    /// than being submitted to DeletionQueue for validation.  This behavior is
    /// used when a tenant attachment is known to have a stale generation number,
    /// such that validation attempts will always fail.  This is not necessary
    /// for correctness, but avoids spamming error statistics with failed validations
    /// when doing migrations of tenants.
    process_remote_consistent_lsn_updates: bool,

    /// If this is true, then object deletions are held in a buffer in RemoteTimelineClient
    /// rather than being submitted to the DeletionQueue.  This behavior is used when a tenant
    /// is known to be multi-attached, in order to avoid disrupting other attached tenants
    /// whose generations' metadata refers to the deleted objects.
    block_deletions: bool,
}

/// RemoteTimelineClientConfig's state is entirely driven by LocationConf, but we do
/// not carry the entire LocationConf structure: it's much more than we need.  The From
/// impl extracts the subset of the LocationConf that is interesting to RemoteTimelineClient.
impl From<&AttachedLocationConfig> for RemoteTimelineClientConfig {
    fn from(lc: &AttachedLocationConfig) -> Self {
        Self {
            block_deletions: !lc.may_delete_layers_hint(),
            process_remote_consistent_lsn_updates: lc.may_upload_layers_hint(),
        }
    }
}

/// A client for accessing a timeline's data in remote storage.
///
/// This takes care of managing the number of connections, and balancing them
/// across tenants. This also handles retries of failed uploads.
///
/// Upload and delete requests are ordered so that before a deletion is
/// performed, we wait for all preceding uploads to finish. This ensures sure
/// that if you perform a compaction operation that reshuffles data in layer
/// files, we don't have a transient state where the old files have already been
/// deleted, but new files have not yet been uploaded.
///
/// Similarly, this enforces an order between index-file uploads, and layer
/// uploads.  Before an index-file upload is performed, all preceding layer
/// uploads must be finished.
///
/// This also maintains a list of remote files, and automatically includes that
/// in the index part file, whenever timeline metadata is uploaded.
///
/// Downloads are not queued, they are performed immediately.
pub(crate) struct RemoteTimelineClient {
    conf: &'static PageServerConf,

    runtime: tokio::runtime::Handle,

    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    generation: Generation,

    upload_queue: Mutex<UploadQueue>,

    pub(crate) metrics: Arc<RemoteTimelineClientMetrics>,

    storage_impl: GenericRemoteStorage,

    deletion_queue_client: DeletionQueueClient,

    /// Subset of tenant configuration used to control upload behaviors during migrations
    config: std::sync::RwLock<RemoteTimelineClientConfig>,

    cancel: CancellationToken,
}

impl Drop for RemoteTimelineClient {
    fn drop(&mut self) {
        debug!("dropping RemoteTimelineClient");
    }
}

impl RemoteTimelineClient {
    ///
    /// Create a remote storage client for given timeline
    ///
    /// Note: the caller must initialize the upload queue before any uploads can be scheduled,
    /// by calling init_upload_queue.
    ///
    pub(crate) fn new(
        remote_storage: GenericRemoteStorage,
        deletion_queue_client: DeletionQueueClient,
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        generation: Generation,
        location_conf: &AttachedLocationConfig,
    ) -> RemoteTimelineClient {
        RemoteTimelineClient {
            conf,
            runtime: if cfg!(test) {
                // remote_timeline_client.rs tests rely on current-thread runtime
                tokio::runtime::Handle::current()
            } else {
                BACKGROUND_RUNTIME.handle().clone()
            },
            tenant_shard_id,
            timeline_id,
            generation,
            storage_impl: remote_storage,
            deletion_queue_client,
            upload_queue: Mutex::new(UploadQueue::Uninitialized),
            metrics: Arc::new(RemoteTimelineClientMetrics::new(
                &tenant_shard_id,
                &timeline_id,
            )),
            config: std::sync::RwLock::new(RemoteTimelineClientConfig::from(location_conf)),
            cancel: CancellationToken::new(),
        }
    }

    /// Initialize the upload queue for a remote storage that already received
    /// an index file upload, i.e., it's not empty.
    /// The given `index_part` must be the one on the remote.
    pub fn init_upload_queue(&self, index_part: &IndexPart) -> anyhow::Result<()> {
        // Set the maximum number of inprogress tasks to the remote storage concurrency. There's
        // certainly no point in starting more upload tasks than this.
        let inprogress_limit = self
            .conf
            .remote_storage_config
            .as_ref()
            .and_then(|r| r.concurrency_limit())
            .unwrap_or(0);
        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_with_current_remote_index_part(index_part, inprogress_limit)?;
        self.update_remote_physical_size_gauge(Some(index_part));
        info!(
            "initialized upload queue from remote index with {} layer files",
            index_part.layer_metadata.len()
        );
        Ok(())
    }

    /// Initialize the upload queue for the case where the remote storage is empty,
    /// i.e., it doesn't have an `IndexPart`.
    pub fn init_upload_queue_for_empty_remote(
        &self,
        local_metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        // Set the maximum number of inprogress tasks to the remote storage concurrency. There's
        // certainly no point in starting more upload tasks than this.
        let inprogress_limit = self
            .conf
            .remote_storage_config
            .as_ref()
            .and_then(|r| r.concurrency_limit())
            .unwrap_or(0);
        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_empty_remote(local_metadata, inprogress_limit)?;
        self.update_remote_physical_size_gauge(None);
        info!("initialized upload queue as empty");
        Ok(())
    }

    /// Initialize the queue in stopped state. Used in startup path
    /// to continue deletion operation interrupted by pageserver crash or restart.
    pub fn init_upload_queue_stopped_to_continue_deletion(
        &self,
        index_part: &IndexPart,
    ) -> anyhow::Result<()> {
        // FIXME: consider newtype for DeletedIndexPart.
        let deleted_at = index_part.deleted_at.ok_or(anyhow::anyhow!(
            "bug: it is responsibility of the caller to provide index part from MaybeDeletedIndexPart::Deleted"
        ))?;
        let inprogress_limit = self
            .conf
            .remote_storage_config
            .as_ref()
            .and_then(|r| r.concurrency_limit())
            .unwrap_or(0);

        let mut upload_queue = self.upload_queue.lock().unwrap();
        upload_queue.initialize_with_current_remote_index_part(index_part, inprogress_limit)?;
        self.update_remote_physical_size_gauge(Some(index_part));
        self.stop_impl(&mut upload_queue);

        upload_queue
            .stopped_mut()
            .expect("stopped above")
            .deleted_at = SetDeletedFlagProgress::Successful(deleted_at);

        Ok(())
    }

    /// Notify this client of a change to its parent tenant's config, as this may cause us to
    /// take action (unblocking deletions when transitioning from AttachedMulti to AttachedSingle)
    pub(super) fn update_config(&self, location_conf: &AttachedLocationConfig) {
        let new_conf = RemoteTimelineClientConfig::from(location_conf);
        let unblocked = !new_conf.block_deletions;

        // Update config before draining deletions, so that we don't race with more being
        // inserted.  This can result in deletions happening our of order, but that does not
        // violate any invariants: deletions only need to be ordered relative to upload of the index
        // that dereferences the deleted objects, and we are not changing that order.
        *self.config.write().unwrap() = new_conf;

        if unblocked {
            // If we may now delete layers, drain any that were blocked in our old
            // configuration state
            let mut queue_locked = self.upload_queue.lock().unwrap();

            if let Ok(queue) = queue_locked.initialized_mut() {
                let blocked_deletions = std::mem::take(&mut queue.blocked_deletions);
                for d in blocked_deletions {
                    if let Err(e) = self.deletion_queue_client.push_layers_sync(
                        self.tenant_shard_id,
                        self.timeline_id,
                        self.generation,
                        d.layers,
                    ) {
                        // This could happen if the pageserver is shut down while a tenant
                        // is transitioning from a deletion-blocked state: we will leak some
                        // S3 objects in this case.
                        warn!("Failed to drain blocked deletions: {}", e);
                        break;
                    }
                }
            }
        }
    }

    /// Returns `None` if nothing is yet uplodaded, `Some(disk_consistent_lsn)` otherwise.
    pub fn remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        match &mut *self.upload_queue.lock().unwrap() {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(q) => q.get_last_remote_consistent_lsn_projected(),
            UploadQueue::Stopped(UploadQueueStopped::Uninitialized) => None,
            UploadQueue::Stopped(UploadQueueStopped::Deletable(q)) => q
                .upload_queue_for_deletion
                .get_last_remote_consistent_lsn_projected(),
        }
    }

    pub fn remote_consistent_lsn_visible(&self) -> Option<Lsn> {
        match &mut *self.upload_queue.lock().unwrap() {
            UploadQueue::Uninitialized => None,
            UploadQueue::Initialized(q) => Some(q.get_last_remote_consistent_lsn_visible()),
            UploadQueue::Stopped(UploadQueueStopped::Uninitialized) => None,
            UploadQueue::Stopped(UploadQueueStopped::Deletable(q)) => Some(
                q.upload_queue_for_deletion
                    .get_last_remote_consistent_lsn_visible(),
            ),
        }
    }

    /// Returns true if this timeline was previously detached at this Lsn and the remote timeline
    /// client is currently initialized.
    pub(crate) fn is_previous_ancestor_lsn(&self, lsn: Lsn) -> bool {
        self.upload_queue
            .lock()
            .unwrap()
            .initialized_mut()
            .map(|uq| uq.clean.0.lineage.is_previous_ancestor_lsn(lsn))
            .unwrap_or(false)
    }

    /// Returns whether the timeline is archived.
    /// Return None if the remote index_part hasn't been downloaded yet.
    pub(crate) fn is_archived(&self) -> Option<bool> {
        self.upload_queue
            .lock()
            .unwrap()
            .initialized_mut()
            .map(|q| q.clean.0.archived_at.is_some())
            .ok()
    }

    /// Returns `Ok(Some(timestamp))` if the timeline has been archived, `Ok(None)` if the timeline hasn't been archived.
    ///
    /// Return Err(_) if the remote index_part hasn't been downloaded yet, or the timeline hasn't been stopped yet.
    pub(crate) fn archived_at_stopped_queue(
        &self,
    ) -> Result<Option<NaiveDateTime>, UploadQueueNotReadyError> {
        self.upload_queue
            .lock()
            .unwrap()
            .stopped_mut()
            .map(|q| q.upload_queue_for_deletion.clean.0.archived_at)
            .map_err(|_| UploadQueueNotReadyError)
    }

    fn update_remote_physical_size_gauge(&self, current_remote_index_part: Option<&IndexPart>) {
        let size: u64 = if let Some(current_remote_index_part) = current_remote_index_part {
            current_remote_index_part
                .layer_metadata
                .values()
                .map(|ilmd| ilmd.file_size)
                .sum()
        } else {
            0
        };
        self.metrics.remote_physical_size_gauge.set(size);
    }

    pub fn get_remote_physical_size(&self) -> u64 {
        self.metrics.remote_physical_size_gauge.get()
    }

    //
    // Download operations.
    //
    // These don't use the per-timeline queue. They do use the global semaphore in
    // S3Bucket, to limit the total number of concurrent operations, though.
    //

    /// Download index file
    pub async fn download_index_file(
        &self,
        cancel: &CancellationToken,
    ) -> Result<MaybeDeletedIndexPart, DownloadError> {
        let _unfinished_gauge_guard = self.metrics.call_begin(
            &RemoteOpFileKind::Index,
            &RemoteOpKind::Download,
            crate::metrics::RemoteTimelineClientMetricsCallTrackSize::DontTrackSize {
                reason: "no need for a downloads gauge",
            },
        );

        let (index_part, index_generation, index_last_modified) = download::download_index_part(
            &self.storage_impl,
            &self.tenant_shard_id,
            &self.timeline_id,
            self.generation,
            cancel,
        )
        .measure_remote_op(
            RemoteOpFileKind::Index,
            RemoteOpKind::Download,
            Arc::clone(&self.metrics),
        )
        .await?;

        // Defense in depth: monotonicity of generation numbers is an important correctness guarantee, so when we see a very
        // old index, we do extra checks in case this is the result of backward time-travel of the generation number (e.g.
        // in case of a bug in the service that issues generation numbers). Indices are allowed to be old, but we expect that
        // when we load an old index we are loading the _latest_ index: if we are asked to load an old index and there is
        // also a newer index available, that is surprising.
        const INDEX_AGE_CHECKS_THRESHOLD: Duration = Duration::from_secs(14 * 24 * 3600);
        let index_age = index_last_modified.elapsed().unwrap_or_else(|e| {
            if e.duration() > Duration::from_secs(5) {
                // We only warn if the S3 clock and our local clock are >5s out: because this is a low resolution
                // timestamp, it is common to be out by at least 1 second.
                tracing::warn!("Index has modification time in the future: {e}");
            }
            Duration::ZERO
        });
        if index_age > INDEX_AGE_CHECKS_THRESHOLD {
            tracing::info!(
                ?index_generation,
                age = index_age.as_secs_f64(),
                "Loaded an old index, checking for other indices..."
            );

            // Find the highest-generation index
            let (_latest_index_part, latest_index_generation, latest_index_mtime) =
                download::download_index_part(
                    &self.storage_impl,
                    &self.tenant_shard_id,
                    &self.timeline_id,
                    Generation::MAX,
                    cancel,
                )
                .await?;

            if latest_index_generation > index_generation {
                // Unexpected!  Why are we loading such an old index if a more recent one exists?
                // We will refuse to proceed, as there is no reasonable scenario where this should happen, but
                // there _is_ a clear bug/corruption scenario where it would happen (controller sets the generation
                // backwards).
                tracing::error!(
                    ?index_generation,
                    ?latest_index_generation,
                    ?latest_index_mtime,
                    "Found a newer index while loading an old one"
                );
                return Err(DownloadError::Fatal(
                    "Index age exceeds threshold and a newer index exists".into(),
                ));
            }
        }

        if index_part.deleted_at.is_some() {
            Ok(MaybeDeletedIndexPart::Deleted(index_part))
        } else {
            Ok(MaybeDeletedIndexPart::IndexPart(index_part))
        }
    }

    /// Download a (layer) file from `path`, into local filesystem.
    ///
    /// 'layer_metadata' is the metadata from the remote index file.
    ///
    /// On success, returns the size of the downloaded file.
    pub async fn download_layer_file(
        &self,
        layer_file_name: &LayerName,
        layer_metadata: &LayerFileMetadata,
        local_path: &Utf8Path,
        gate: &utils::sync::gate::Gate,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<u64, DownloadError> {
        let downloaded_size = {
            let _unfinished_gauge_guard = self.metrics.call_begin(
                &RemoteOpFileKind::Layer,
                &RemoteOpKind::Download,
                crate::metrics::RemoteTimelineClientMetricsCallTrackSize::DontTrackSize {
                    reason: "no need for a downloads gauge",
                },
            );
            download::download_layer_file(
                self.conf,
                &self.storage_impl,
                self.tenant_shard_id,
                self.timeline_id,
                layer_file_name,
                layer_metadata,
                local_path,
                gate,
                cancel,
                ctx,
            )
            .measure_remote_op(
                RemoteOpFileKind::Layer,
                RemoteOpKind::Download,
                Arc::clone(&self.metrics),
            )
            .await?
        };

        REMOTE_ONDEMAND_DOWNLOADED_LAYERS.inc();
        REMOTE_ONDEMAND_DOWNLOADED_BYTES.inc_by(downloaded_size);

        Ok(downloaded_size)
    }

    //
    // Upload operations.
    //

    /// Launch an index-file upload operation in the background, with
    /// fully updated metadata.
    ///
    /// This should only be used to upload initial metadata to remote storage.
    ///
    /// The upload will be added to the queue immediately, but it
    /// won't be performed until all previously scheduled layer file
    /// upload operations have completed successfully.  This is to
    /// ensure that when the index file claims that layers X, Y and Z
    /// exist in remote storage, they really do. To wait for the upload
    /// to complete, use `wait_completion`.
    ///
    /// If there were any changes to the list of files, i.e. if any
    /// layer file uploads were scheduled, since the last index file
    /// upload, those will be included too.
    pub fn schedule_index_upload_for_full_metadata_update(
        self: &Arc<Self>,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        // As documented in the struct definition, it's ok for latest_metadata to be
        // ahead of what's _actually_ on the remote during index upload.
        upload_queue.dirty.metadata = metadata.clone();

        self.schedule_index_upload(upload_queue);

        Ok(())
    }

    /// Launch an index-file upload operation in the background, with only parts of the metadata
    /// updated.
    ///
    /// This is the regular way of updating metadata on layer flushes or Gc.
    ///
    /// Using this lighter update mechanism allows for reparenting and detaching without changes to
    /// `index_part.json`, while being more clear on what values update regularly.
    pub(crate) fn schedule_index_upload_for_metadata_update(
        self: &Arc<Self>,
        update: &MetadataUpdate,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        upload_queue.dirty.metadata.apply(update);

        self.schedule_index_upload(upload_queue);

        Ok(())
    }

    /// Launch an index-file upload operation in the background, with only the `archived_at` field updated.
    ///
    /// Returns whether it is required to wait for the queue to be empty to ensure that the change is uploaded,
    /// so either if the change is already sitting in the queue, but not commited yet, or the change has not
    /// been in the queue yet.
    pub(crate) fn schedule_index_upload_for_timeline_archival_state(
        self: &Arc<Self>,
        state: TimelineArchivalState,
    ) -> anyhow::Result<bool> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        /// Returns Some(_) if a change is needed, and Some(true) if it's a
        /// change needed to set archived_at.
        fn need_change(
            archived_at: &Option<NaiveDateTime>,
            state: TimelineArchivalState,
        ) -> Option<bool> {
            match (archived_at, state) {
                (Some(_), TimelineArchivalState::Archived)
                | (None, TimelineArchivalState::Unarchived) => {
                    // Nothing to do
                    tracing::info!("intended state matches present state");
                    None
                }
                (None, TimelineArchivalState::Archived) => Some(true),
                (Some(_), TimelineArchivalState::Unarchived) => Some(false),
            }
        }
        let need_upload_scheduled = need_change(&upload_queue.dirty.archived_at, state);

        if let Some(archived_at_set) = need_upload_scheduled {
            let intended_archived_at = archived_at_set.then(|| Utc::now().naive_utc());
            upload_queue.dirty.archived_at = intended_archived_at;
            self.schedule_index_upload(upload_queue);
        }

        let need_wait = need_change(&upload_queue.clean.0.archived_at, state).is_some();
        Ok(need_wait)
    }

    /// Shuts the timeline client down, but only if the timeline is archived.
    ///
    /// This function and [`Self::schedule_index_upload_for_timeline_archival_state`] use the
    /// same lock to prevent races between unarchival and offloading: unarchival requires the
    /// upload queue to be initialized, and leaves behind an upload queue where either dirty
    /// or clean has archived_at of `None`. offloading leaves behind an uninitialized upload
    /// queue.
    pub(crate) async fn shutdown_if_archived(
        self: &Arc<Self>,
    ) -> Result<(), ShutdownIfArchivedError> {
        {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard
                .initialized_mut()
                .map_err(ShutdownIfArchivedError::NotInitialized)?;

            match (
                upload_queue.dirty.archived_at.is_none(),
                upload_queue.clean.0.archived_at.is_none(),
            ) {
                // The expected case: the timeline is archived and we don't want to unarchive
                (false, false) => {}
                (true, false) => {
                    tracing::info!("can't shut down timeline: timeline slated for unarchival");
                    return Err(ShutdownIfArchivedError::NotArchived);
                }
                (dirty_archived, true) => {
                    tracing::info!(%dirty_archived, "can't shut down timeline: timeline not archived in remote storage");
                    return Err(ShutdownIfArchivedError::NotArchived);
                }
            }

            // Set the shutting_down flag while the guard from the archival check is held.
            // This prevents a race with unarchival, as initialized_mut will not return
            // an upload queue from this point.
            // Also launch the queued tasks like shutdown() does.
            if !upload_queue.shutting_down {
                upload_queue.shutting_down = true;
                upload_queue.queued_operations.push_back(UploadOp::Shutdown);
                // this operation is not counted similar to Barrier
                self.launch_queued_tasks(upload_queue);
            }
        }

        self.shutdown().await;

        Ok(())
    }

    /// Launch an index-file upload operation in the background, setting `import_pgdata` field.
    pub(crate) fn schedule_index_upload_for_import_pgdata_state_update(
        self: &Arc<Self>,
        state: Option<import_pgdata::index_part_format::Root>,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;
        upload_queue.dirty.import_pgdata = state;
        self.schedule_index_upload(upload_queue);
        Ok(())
    }

    ///
    /// Launch an index-file upload operation in the background, if necessary.
    ///
    /// Use this function to schedule the update of the index file after
    /// scheduling file uploads or deletions. If no file uploads or deletions
    /// have been scheduled since the last index file upload, this does
    /// nothing.
    ///
    /// Like schedule_index_upload_for_metadata_update(), this merely adds
    /// the upload to the upload queue and returns quickly.
    pub fn schedule_index_upload_for_file_changes(self: &Arc<Self>) -> Result<(), NotInitialized> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        if upload_queue.latest_files_changes_since_metadata_upload_scheduled > 0 {
            self.schedule_index_upload(upload_queue);
        }

        Ok(())
    }

    /// Launch an index-file upload operation in the background (internal function)
    fn schedule_index_upload(self: &Arc<Self>, upload_queue: &mut UploadQueueInitialized) {
        let disk_consistent_lsn = upload_queue.dirty.metadata.disk_consistent_lsn();
        // fix up the duplicated field
        upload_queue.dirty.disk_consistent_lsn = disk_consistent_lsn;

        // make sure it serializes before doing it in perform_upload_task so that it doesn't
        // look like a retryable error
        let void = std::io::sink();
        serde_json::to_writer(void, &upload_queue.dirty).expect("serialize index_part.json");

        let index_part = &upload_queue.dirty;

        info!(
            "scheduling metadata upload up to consistent LSN {disk_consistent_lsn} with {} files ({} changed)",
            index_part.layer_metadata.len(),
            upload_queue.latest_files_changes_since_metadata_upload_scheduled,
        );

        let op = UploadOp::UploadMetadata {
            uploaded: Box::new(index_part.clone()),
        };
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
        upload_queue.latest_files_changes_since_metadata_upload_scheduled = 0;

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);
    }

    /// Reparent this timeline to a new parent.
    ///
    /// A retryable step of timeline ancestor detach.
    pub(crate) async fn schedule_reparenting_and_wait(
        self: &Arc<Self>,
        new_parent: &TimelineId,
    ) -> anyhow::Result<()> {
        let receiver = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;

            let Some(prev) = upload_queue.dirty.metadata.ancestor_timeline() else {
                return Err(anyhow::anyhow!(
                    "cannot reparent without a current ancestor"
                ));
            };

            let uploaded = &upload_queue.clean.0.metadata;

            if uploaded.ancestor_timeline().is_none() && !uploaded.ancestor_lsn().is_valid() {
                // nothing to do
                None
            } else {
                upload_queue.dirty.metadata.reparent(new_parent);
                upload_queue.dirty.lineage.record_previous_ancestor(&prev);

                self.schedule_index_upload(upload_queue);

                Some(self.schedule_barrier0(upload_queue))
            }
        };

        if let Some(receiver) = receiver {
            Self::wait_completion0(receiver).await?;
        }
        Ok(())
    }

    /// Schedules uploading a new version of `index_part.json` with the given layers added,
    /// detaching from ancestor and waits for it to complete.
    ///
    /// This is used with `Timeline::detach_ancestor` functionality.
    pub(crate) async fn schedule_adding_existing_layers_to_index_detach_and_wait(
        self: &Arc<Self>,
        layers: &[Layer],
        adopted: (TimelineId, Lsn),
    ) -> anyhow::Result<()> {
        let barrier = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;

            if upload_queue.clean.0.lineage.detached_previous_ancestor() == Some(adopted) {
                None
            } else {
                upload_queue.dirty.metadata.detach_from_ancestor(&adopted);
                upload_queue.dirty.lineage.record_detaching(&adopted);

                for layer in layers {
                    let prev = upload_queue
                        .dirty
                        .layer_metadata
                        .insert(layer.layer_desc().layer_name(), layer.metadata());
                    assert!(prev.is_none(), "copied layer existed already {layer}");
                }

                self.schedule_index_upload(upload_queue);

                Some(self.schedule_barrier0(upload_queue))
            }
        };

        if let Some(barrier) = barrier {
            Self::wait_completion0(barrier).await?;
        }
        Ok(())
    }

    /// Adds a gc blocking reason for this timeline if one does not exist already.
    ///
    /// A retryable step of timeline detach ancestor.
    ///
    /// Returns a future which waits until the completion of the upload.
    pub(crate) fn schedule_insert_gc_block_reason(
        self: &Arc<Self>,
        reason: index::GcBlockingReason,
    ) -> Result<impl std::future::Future<Output = Result<(), WaitCompletionError>>, NotInitialized>
    {
        let maybe_barrier = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;

            if let index::GcBlockingReason::DetachAncestor = reason {
                if upload_queue.dirty.metadata.ancestor_timeline().is_none() {
                    drop(guard);
                    panic!("cannot start detach ancestor if there is nothing to detach from");
                }
            }

            let wanted = |x: Option<&index::GcBlocking>| x.is_some_and(|x| x.blocked_by(reason));

            let current = upload_queue.dirty.gc_blocking.as_ref();
            let uploaded = upload_queue.clean.0.gc_blocking.as_ref();

            match (current, uploaded) {
                (x, y) if wanted(x) && wanted(y) => None,
                (x, y) if wanted(x) && !wanted(y) => Some(self.schedule_barrier0(upload_queue)),
                // Usual case: !wanted(x) && !wanted(y)
                //
                // Unusual: !wanted(x) && wanted(y) which means we have two processes waiting to
                // turn on and off some reason.
                (x, y) => {
                    if !wanted(x) && wanted(y) {
                        // this could be avoided by having external in-memory synchronization, like
                        // timeline detach ancestor
                        warn!(?reason, op="insert", "unexpected: two racing processes to enable and disable a gc blocking reason");
                    }

                    // at this point, the metadata must always show that there is a parent
                    upload_queue.dirty.gc_blocking = current
                        .map(|x| x.with_reason(reason))
                        .or_else(|| Some(index::GcBlocking::started_now_for(reason)));
                    self.schedule_index_upload(upload_queue);
                    Some(self.schedule_barrier0(upload_queue))
                }
            }
        };

        Ok(async move {
            if let Some(barrier) = maybe_barrier {
                Self::wait_completion0(barrier).await?;
            }
            Ok(())
        })
    }

    /// Removes a gc blocking reason for this timeline if one exists.
    ///
    /// A retryable step of timeline detach ancestor.
    ///
    /// Returns a future which waits until the completion of the upload.
    pub(crate) fn schedule_remove_gc_block_reason(
        self: &Arc<Self>,
        reason: index::GcBlockingReason,
    ) -> Result<impl std::future::Future<Output = Result<(), WaitCompletionError>>, NotInitialized>
    {
        let maybe_barrier = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut()?;

            if let index::GcBlockingReason::DetachAncestor = reason {
                if !upload_queue.clean.0.lineage.is_detached_from_ancestor() {
                    drop(guard);
                    panic!("cannot complete timeline_ancestor_detach while not detached");
                }
            }

            let wanted = |x: Option<&index::GcBlocking>| {
                x.is_none() || x.is_some_and(|b| !b.blocked_by(reason))
            };

            let current = upload_queue.dirty.gc_blocking.as_ref();
            let uploaded = upload_queue.clean.0.gc_blocking.as_ref();

            match (current, uploaded) {
                (x, y) if wanted(x) && wanted(y) => None,
                (x, y) if wanted(x) && !wanted(y) => Some(self.schedule_barrier0(upload_queue)),
                (x, y) => {
                    if !wanted(x) && wanted(y) {
                        warn!(?reason, op="remove", "unexpected: two racing processes to enable and disable a gc blocking reason (remove)");
                    }

                    upload_queue.dirty.gc_blocking =
                        current.as_ref().and_then(|x| x.without_reason(reason));
                    assert!(wanted(upload_queue.dirty.gc_blocking.as_ref()));
                    self.schedule_index_upload(upload_queue);
                    Some(self.schedule_barrier0(upload_queue))
                }
            }
        };

        Ok(async move {
            if let Some(barrier) = maybe_barrier {
                Self::wait_completion0(barrier).await?;
            }
            Ok(())
        })
    }

    /// Launch an upload operation in the background; the file is added to be included in next
    /// `index_part.json` upload.
    pub(crate) fn schedule_layer_file_upload(
        self: &Arc<Self>,
        layer: ResidentLayer,
    ) -> Result<(), NotInitialized> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        self.schedule_layer_file_upload0(upload_queue, layer);
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    fn schedule_layer_file_upload0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        layer: ResidentLayer,
    ) {
        let metadata = layer.metadata();

        upload_queue
            .dirty
            .layer_metadata
            .insert(layer.layer_desc().layer_name(), metadata.clone());
        upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;

        info!(
            gen=?metadata.generation,
            shard=?metadata.shard,
            "scheduled layer file upload {layer}",
        );

        let op = UploadOp::UploadLayer(layer, metadata, None);
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
    }

    /// Launch a delete operation in the background.
    ///
    /// The operation does not modify local filesystem state.
    ///
    /// Note: This schedules an index file upload before the deletions.  The
    /// deletion won't actually be performed, until all previously scheduled
    /// upload operations, and the index file upload, have completed
    /// successfully.
    pub fn schedule_layer_file_deletion(
        self: &Arc<Self>,
        names: &[LayerName],
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        let with_metadata =
            self.schedule_unlinking_of_layers_from_index_part0(upload_queue, names.iter().cloned());

        self.schedule_deletion_of_unlinked0(upload_queue, with_metadata);

        // Launch the tasks immediately, if possible
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    /// Unlinks the layer files from `index_part.json` but does not yet schedule deletion for the
    /// layer files, leaving them dangling.
    ///
    /// The files will be leaked in remote storage unless [`Self::schedule_deletion_of_unlinked`]
    /// is invoked on them.
    pub(crate) fn schedule_gc_update(
        self: &Arc<Self>,
        gc_layers: &[Layer],
    ) -> Result<(), NotInitialized> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        // just forget the return value; after uploading the next index_part.json, we can consider
        // the layer files as "dangling". this is fine, at worst case we create work for the
        // scrubber.

        let names = gc_layers.iter().map(|x| x.layer_desc().layer_name());

        self.schedule_unlinking_of_layers_from_index_part0(upload_queue, names);

        self.launch_queued_tasks(upload_queue);

        Ok(())
    }

    /// Update the remote index file, removing the to-be-deleted files from the index,
    /// allowing scheduling of actual deletions later.
    fn schedule_unlinking_of_layers_from_index_part0<I>(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        names: I,
    ) -> Vec<(LayerName, LayerFileMetadata)>
    where
        I: IntoIterator<Item = LayerName>,
    {
        // Decorate our list of names with each name's metadata, dropping
        // names that are unexpectedly missing from our metadata.  This metadata
        // is later used when physically deleting layers, to construct key paths.
        let with_metadata: Vec<_> = names
            .into_iter()
            .filter_map(|name| {
                let meta = upload_queue.dirty.layer_metadata.remove(&name);

                if let Some(meta) = meta {
                    upload_queue.latest_files_changes_since_metadata_upload_scheduled += 1;
                    Some((name, meta))
                } else {
                    // This can only happen if we forgot to to schedule the file upload
                    // before scheduling the delete. Log it because it is a rare/strange
                    // situation, and in case something is misbehaving, we'd like to know which
                    // layers experienced this.
                    info!("Deleting layer {name} not found in latest_files list, never uploaded?");
                    None
                }
            })
            .collect();

        #[cfg(feature = "testing")]
        for (name, metadata) in &with_metadata {
            let gen = metadata.generation;
            if let Some(unexpected) = upload_queue.dangling_files.insert(name.to_owned(), gen) {
                if unexpected == gen {
                    tracing::error!("{name} was unlinked twice with same generation");
                } else {
                    tracing::error!("{name} was unlinked twice with different generations {gen:?} and {unexpected:?}");
                }
            }
        }

        // after unlinking files from the upload_queue.latest_files we must always schedule an
        // index_part update, because that needs to be uploaded before we can actually delete the
        // files.
        if upload_queue.latest_files_changes_since_metadata_upload_scheduled > 0 {
            self.schedule_index_upload(upload_queue);
        }

        with_metadata
    }

    /// Schedules deletion for layer files which have previously been unlinked from the
    /// `index_part.json` with [`Self::schedule_gc_update`] or [`Self::schedule_compaction_update`].
    pub(crate) fn schedule_deletion_of_unlinked(
        self: &Arc<Self>,
        layers: Vec<(LayerName, LayerFileMetadata)>,
    ) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        self.schedule_deletion_of_unlinked0(upload_queue, layers);
        self.launch_queued_tasks(upload_queue);
        Ok(())
    }

    fn schedule_deletion_of_unlinked0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
        mut with_metadata: Vec<(LayerName, LayerFileMetadata)>,
    ) {
        // Filter out any layers which were not created by this tenant shard.  These are
        // layers that originate from some ancestor shard after a split, and may still
        // be referenced by other shards. We are free to delete them locally and remove
        // them from our index (and would have already done so when we reach this point
        // in the code), but we may not delete them remotely.
        with_metadata.retain(|(name, meta)| {
            let retain = meta.shard.shard_number == self.tenant_shard_id.shard_number
                && meta.shard.shard_count == self.tenant_shard_id.shard_count;
            if !retain {
                tracing::debug!(
                    "Skipping deletion of ancestor-shard layer {name}, from shard {}",
                    meta.shard
                );
            }
            retain
        });

        for (name, meta) in &with_metadata {
            info!(
                "scheduling deletion of layer {}{} (shard {})",
                name,
                meta.generation.get_suffix(),
                meta.shard
            );
        }

        #[cfg(feature = "testing")]
        for (name, meta) in &with_metadata {
            let gen = meta.generation;
            match upload_queue.dangling_files.remove(name) {
                Some(same) if same == gen => { /* expected */ }
                Some(other) => {
                    tracing::error!("{name} was unlinked with {other:?} but deleted with {gen:?}");
                }
                None => {
                    tracing::error!("{name} was unlinked but was not dangling");
                }
            }
        }

        // schedule the actual deletions
        if with_metadata.is_empty() {
            // avoid scheduling the op & bumping the metric
            return;
        }
        let op = UploadOp::Delete(Delete {
            layers: with_metadata,
        });
        self.metric_begin(&op);
        upload_queue.queued_operations.push_back(op);
    }

    /// Schedules a compaction update to the remote `index_part.json`.
    ///
    /// `compacted_from` represent the L0 names which have been `compacted_to` L1 layers.
    pub(crate) fn schedule_compaction_update(
        self: &Arc<Self>,
        compacted_from: &[Layer],
        compacted_to: &[ResidentLayer],
    ) -> Result<(), NotInitialized> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;

        for layer in compacted_to {
            self.schedule_layer_file_upload0(upload_queue, layer.clone());
        }

        let names = compacted_from.iter().map(|x| x.layer_desc().layer_name());

        self.schedule_unlinking_of_layers_from_index_part0(upload_queue, names);
        self.launch_queued_tasks(upload_queue);

        Ok(())
    }

    /// Wait for all previously scheduled uploads/deletions to complete
    pub(crate) async fn wait_completion(self: &Arc<Self>) -> Result<(), WaitCompletionError> {
        let receiver = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = guard
                .initialized_mut()
                .map_err(WaitCompletionError::NotInitialized)?;
            self.schedule_barrier0(upload_queue)
        };

        Self::wait_completion0(receiver).await
    }

    async fn wait_completion0(
        mut receiver: tokio::sync::watch::Receiver<()>,
    ) -> Result<(), WaitCompletionError> {
        if receiver.changed().await.is_err() {
            return Err(WaitCompletionError::UploadQueueShutDownOrStopped);
        }

        Ok(())
    }

    pub(crate) fn schedule_barrier(self: &Arc<Self>) -> anyhow::Result<()> {
        let mut guard = self.upload_queue.lock().unwrap();
        let upload_queue = guard.initialized_mut()?;
        self.schedule_barrier0(upload_queue);
        Ok(())
    }

    fn schedule_barrier0(
        self: &Arc<Self>,
        upload_queue: &mut UploadQueueInitialized,
    ) -> tokio::sync::watch::Receiver<()> {
        let (sender, receiver) = tokio::sync::watch::channel(());
        let barrier_op = UploadOp::Barrier(sender);

        upload_queue.queued_operations.push_back(barrier_op);
        // Don't count this kind of operation!

        // Launch the task immediately, if possible
        self.launch_queued_tasks(upload_queue);

        receiver
    }

    /// Wait for all previously scheduled operations to complete, and then stop.
    ///
    /// Not cancellation safe
    pub(crate) async fn shutdown(self: &Arc<Self>) {
        // On cancellation the queue is left in ackward state of refusing new operations but
        // proper stop is yet to be called. On cancel the original or some later task must call
        // `stop` or `shutdown`.
        let sg = scopeguard::guard((), |_| {
            tracing::error!("RemoteTimelineClient::shutdown was cancelled; this should not happen, do not make this into an allowed_error")
        });

        let fut = {
            let mut guard = self.upload_queue.lock().unwrap();
            let upload_queue = match &mut *guard {
                UploadQueue::Stopped(_) => {
                    scopeguard::ScopeGuard::into_inner(sg);
                    return;
                }
                UploadQueue::Uninitialized => {
                    // transition into Stopped state
                    self.stop_impl(&mut guard);
                    scopeguard::ScopeGuard::into_inner(sg);
                    return;
                }
                UploadQueue::Initialized(ref mut init) => init,
            };

            // if the queue is already stuck due to a shutdown operation which was cancelled, then
            // just don't add more of these as they would never complete.
            //
            // TODO: if launch_queued_tasks were to be refactored to accept a &mut UploadQueue
            // in every place we would not have to jump through this hoop, and this method could be
            // made cancellable.
            if !upload_queue.shutting_down {
                upload_queue.shutting_down = true;
                upload_queue.queued_operations.push_back(UploadOp::Shutdown);
                // this operation is not counted similar to Barrier

                self.launch_queued_tasks(upload_queue);
            }

            upload_queue.shutdown_ready.clone().acquire_owned()
        };

        let res = fut.await;

        scopeguard::ScopeGuard::into_inner(sg);

        match res {
            Ok(_permit) => unreachable!("shutdown_ready should not have been added permits"),
            Err(_closed) => {
                // expected
            }
        }

        self.stop();
    }

    /// Set the deleted_at field in the remote index file.
    ///
    /// This fails if the upload queue has not been `stop()`ed.
    ///
    /// The caller is responsible for calling `stop()` AND for waiting
    /// for any ongoing upload tasks to finish after `stop()` has succeeded.
    /// Check method [`RemoteTimelineClient::stop`] for details.
    #[instrument(skip_all)]
    pub(crate) async fn persist_index_part_with_deleted_flag(
        self: &Arc<Self>,
    ) -> Result<(), PersistIndexPartWithDeletedFlagError> {
        let index_part_with_deleted_at = {
            let mut locked = self.upload_queue.lock().unwrap();

            // We must be in stopped state because otherwise
            // we can have inprogress index part upload that can overwrite the file
            // with missing is_deleted flag that we going to set below
            let stopped = locked.stopped_mut()?;

            match stopped.deleted_at {
                SetDeletedFlagProgress::NotRunning => (), // proceed
                SetDeletedFlagProgress::InProgress(at) => {
                    return Err(PersistIndexPartWithDeletedFlagError::AlreadyInProgress(at));
                }
                SetDeletedFlagProgress::Successful(at) => {
                    return Err(PersistIndexPartWithDeletedFlagError::AlreadyDeleted(at));
                }
            };
            let deleted_at = Utc::now().naive_utc();
            stopped.deleted_at = SetDeletedFlagProgress::InProgress(deleted_at);

            let mut index_part = stopped.upload_queue_for_deletion.dirty.clone();
            index_part.deleted_at = Some(deleted_at);
            index_part
        };

        let undo_deleted_at = scopeguard::guard(Arc::clone(self), |self_clone| {
            let mut locked = self_clone.upload_queue.lock().unwrap();
            let stopped = locked
                .stopped_mut()
                .expect("there's no way out of Stopping, and we checked it's Stopping above");
            stopped.deleted_at = SetDeletedFlagProgress::NotRunning;
        });

        pausable_failpoint!("persist_deleted_index_part");

        backoff::retry(
            || {
                upload::upload_index_part(
                    &self.storage_impl,
                    &self.tenant_shard_id,
                    &self.timeline_id,
                    self.generation,
                    &index_part_with_deleted_at,
                    &self.cancel,
                )
            },
            |_e| false,
            1,
            // have just a couple of attempts
            // when executed as part of timeline deletion this happens in context of api call
            // when executed as part of tenant deletion this happens in the background
            2,
            "persist_index_part_with_deleted_flag",
            &self.cancel,
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)?;

        // all good, disarm the guard and mark as success
        ScopeGuard::into_inner(undo_deleted_at);
        {
            let mut locked = self.upload_queue.lock().unwrap();

            let stopped = locked
                .stopped_mut()
                .expect("there's no way out of Stopping, and we checked it's Stopping above");
            stopped.deleted_at = SetDeletedFlagProgress::Successful(
                index_part_with_deleted_at
                    .deleted_at
                    .expect("we set it above"),
            );
        }

        Ok(())
    }

    pub(crate) fn is_deleting(&self) -> bool {
        let mut locked = self.upload_queue.lock().unwrap();
        locked.stopped_mut().is_ok()
    }

    pub(crate) async fn preserve_initdb_archive(
        self: &Arc<Self>,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        backoff::retry(
            || async {
                upload::preserve_initdb_archive(&self.storage_impl, tenant_id, timeline_id, cancel)
                    .await
            },
            TimeoutOrCancel::caused_by_cancel,
            FAILED_DOWNLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "preserve_initdb_tar_zst",
            &cancel.clone(),
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)
        .context("backing up initdb archive")?;
        Ok(())
    }

    /// Uploads the given layer **without** adding it to be part of a future `index_part.json` upload.
    ///
    /// This is not normally needed.
    pub(crate) async fn upload_layer_file(
        self: &Arc<Self>,
        uploaded: &ResidentLayer,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let remote_path = remote_layer_path(
            &self.tenant_shard_id.tenant_id,
            &self.timeline_id,
            uploaded.metadata().shard,
            &uploaded.layer_desc().layer_name(),
            uploaded.metadata().generation,
        );

        backoff::retry(
            || async {
                upload::upload_timeline_layer(
                    &self.storage_impl,
                    uploaded.local_path(),
                    &remote_path,
                    uploaded.metadata().file_size,
                    cancel,
                )
                .await
            },
            TimeoutOrCancel::caused_by_cancel,
            FAILED_UPLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "upload a layer without adding it to latest files",
            cancel,
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)
        .context("upload a layer without adding it to latest files")
    }

    /// Copies the `adopted` remote existing layer to the remote path of `adopted_as`. The layer is
    /// not added to be part of a future `index_part.json` upload.
    pub(crate) async fn copy_timeline_layer(
        self: &Arc<Self>,
        adopted: &Layer,
        adopted_as: &Layer,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let source_remote_path = remote_layer_path(
            &self.tenant_shard_id.tenant_id,
            &adopted
                .get_timeline_id()
                .expect("Source timeline should be alive"),
            adopted.metadata().shard,
            &adopted.layer_desc().layer_name(),
            adopted.metadata().generation,
        );

        let target_remote_path = remote_layer_path(
            &self.tenant_shard_id.tenant_id,
            &self.timeline_id,
            adopted_as.metadata().shard,
            &adopted_as.layer_desc().layer_name(),
            adopted_as.metadata().generation,
        );

        backoff::retry(
            || async {
                upload::copy_timeline_layer(
                    &self.storage_impl,
                    &source_remote_path,
                    &target_remote_path,
                    cancel,
                )
                .await
            },
            TimeoutOrCancel::caused_by_cancel,
            FAILED_UPLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "copy timeline layer",
            cancel,
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)
        .context("remote copy timeline layer")
    }

    async fn flush_deletion_queue(&self) -> Result<(), DeletionQueueError> {
        match tokio::time::timeout(
            DELETION_QUEUE_FLUSH_TIMEOUT,
            self.deletion_queue_client.flush_immediate(),
        )
        .await
        {
            Ok(result) => result,
            Err(_timeout) => {
                // Flushing remote deletions is not mandatory: we flush here to make the system easier to test, and
                // to ensure that _usually_ objects are really gone after a DELETE is acked.  However, in case of deletion
                // queue issues (https://github.com/neondatabase/neon/issues/6440), we don't want to wait indefinitely here.
                tracing::warn!(
                    "Timed out waiting for deletion queue flush, acking deletion anyway"
                );
                Ok(())
            }
        }
    }

    /// Prerequisites: UploadQueue should be in stopped state and deleted_at should be successfuly set.
    /// The function deletes layer files one by one, then lists the prefix to see if we leaked something
    /// deletes leaked files if any and proceeds with deletion of index file at the end.
    pub(crate) async fn delete_all(self: &Arc<Self>) -> Result<(), DeleteTimelineError> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        let layers: Vec<RemotePath> = {
            let mut locked = self.upload_queue.lock().unwrap();
            let stopped = locked.stopped_mut().map_err(DeleteTimelineError::Other)?;

            if !matches!(stopped.deleted_at, SetDeletedFlagProgress::Successful(_)) {
                return Err(DeleteTimelineError::Other(anyhow::anyhow!(
                    "deleted_at is not set"
                )));
            }

            debug_assert!(stopped.upload_queue_for_deletion.no_pending_work());

            stopped
                .upload_queue_for_deletion
                .dirty
                .layer_metadata
                .drain()
                .filter(|(_file_name, meta)| {
                    // Filter out layers that belonged to an ancestor shard.  Since we are deleting the whole timeline from
                    // all shards anyway, we _could_ delete these, but
                    // - it creates a potential race if other shards are still
                    //   using the layers while this shard deletes them.
                    // - it means that if we rolled back the shard split, the ancestor shards would be in a state where
                    //   these timelines are present but corrupt (their index exists but some layers don't)
                    //
                    // These layers will eventually be cleaned up by the scrubber when it does physical GC.
                    meta.shard.shard_number == self.tenant_shard_id.shard_number
                        && meta.shard.shard_count == self.tenant_shard_id.shard_count
                })
                .map(|(file_name, meta)| {
                    remote_layer_path(
                        &self.tenant_shard_id.tenant_id,
                        &self.timeline_id,
                        meta.shard,
                        &file_name,
                        meta.generation,
                    )
                })
                .collect()
        };

        let layer_deletion_count = layers.len();
        self.deletion_queue_client
            .push_immediate(layers)
            .await
            .map_err(|_| DeleteTimelineError::Cancelled)?;

        // Delete the initdb.tar.zst, which is not always present, but deletion attempts of
        // inexistant objects are not considered errors.
        let initdb_path =
            remote_initdb_archive_path(&self.tenant_shard_id.tenant_id, &self.timeline_id);
        self.deletion_queue_client
            .push_immediate(vec![initdb_path])
            .await
            .map_err(|_| DeleteTimelineError::Cancelled)?;

        // Do not delete index part yet, it is needed for possible retry. If we remove it first
        // and retry will arrive to different pageserver there wont be any traces of it on remote storage
        let timeline_storage_path = remote_timeline_path(&self.tenant_shard_id, &self.timeline_id);

        // Execute all pending deletions, so that when we proceed to do a listing below, we aren't
        // taking the burden of listing all the layers that we already know we should delete.
        self.flush_deletion_queue()
            .await
            .map_err(|_| DeleteTimelineError::Cancelled)?;

        let cancel = shutdown_token();

        let remaining = download_retry(
            || async {
                self.storage_impl
                    .list(
                        Some(&timeline_storage_path),
                        ListingMode::NoDelimiter,
                        None,
                        &cancel,
                    )
                    .await
            },
            "list remaining files",
            &cancel,
        )
        .await
        .context("list files remaining files")?
        .keys;

        // We will delete the current index_part object last, since it acts as a deletion
        // marker via its deleted_at attribute
        let latest_index = remaining
            .iter()
            .filter(|o| {
                o.key
                    .object_name()
                    .map(|n| n.starts_with(IndexPart::FILE_NAME))
                    .unwrap_or(false)
            })
            .filter_map(|o| parse_remote_index_path(o.key.clone()).map(|gen| (o.key.clone(), gen)))
            .max_by_key(|i| i.1)
            .map(|i| i.0.clone())
            .unwrap_or(
                // No generation-suffixed indices, assume we are dealing with
                // a legacy index.
                remote_index_path(&self.tenant_shard_id, &self.timeline_id, Generation::none()),
            );

        let remaining_layers: Vec<RemotePath> = remaining
            .into_iter()
            .filter_map(|o| {
                if o.key == latest_index || o.key.object_name() == Some(INITDB_PRESERVED_PATH) {
                    None
                } else {
                    Some(o.key)
                }
            })
            .inspect(|path| {
                if let Some(name) = path.object_name() {
                    info!(%name, "deleting a file not referenced from index_part.json");
                } else {
                    warn!(%path, "deleting a nameless or non-utf8 object not referenced from index_part.json");
                }
            })
            .collect();

        let not_referenced_count = remaining_layers.len();
        if !remaining_layers.is_empty() {
            self.deletion_queue_client
                .push_immediate(remaining_layers)
                .await
                .map_err(|_| DeleteTimelineError::Cancelled)?;
        }

        fail::fail_point!("timeline-delete-before-index-delete", |_| {
            Err(DeleteTimelineError::Other(anyhow::anyhow!(
                "failpoint: timeline-delete-before-index-delete"
            )))?
        });

        debug!("enqueuing index part deletion");
        self.deletion_queue_client
            .push_immediate([latest_index].to_vec())
            .await
            .map_err(|_| DeleteTimelineError::Cancelled)?;

        // Timeline deletion is rare and we have probably emitted a reasonably number of objects: wait
        // for a flush to a persistent deletion list so that we may be sure deletion will occur.
        self.flush_deletion_queue()
            .await
            .map_err(|_| DeleteTimelineError::Cancelled)?;

        fail::fail_point!("timeline-delete-after-index-delete", |_| {
            Err(DeleteTimelineError::Other(anyhow::anyhow!(
                "failpoint: timeline-delete-after-index-delete"
            )))?
        });

        info!(prefix=%timeline_storage_path, referenced=layer_deletion_count, not_referenced=%not_referenced_count, "done deleting in timeline prefix, including index_part.json");

        Ok(())
    }

    /// Pick next tasks from the queue, and start as many of them as possible without violating
    /// the ordering constraints.
    ///
    /// TODO: consider limiting the number of in-progress tasks, beyond what remote_storage does.
    /// This can launch an unbounded number of queued tasks. `UploadQueue::next_ready()` also has
    /// worst-case quadratic cost in the number of tasks, and may struggle beyond 10,000 tasks.
    fn launch_queued_tasks(self: &Arc<Self>, upload_queue: &mut UploadQueueInitialized) {
        while let Some((mut next_op, coalesced_ops)) = upload_queue.next_ready() {
            debug!("starting op: {next_op}");

            // Prepare upload.
            match &mut next_op {
                UploadOp::UploadLayer(layer, meta, mode) => {
                    if upload_queue
                        .recently_deleted
                        .remove(&(layer.layer_desc().layer_name().clone(), meta.generation))
                    {
                        *mode = Some(OpType::FlushDeletion);
                    } else {
                        *mode = Some(OpType::MayReorder)
                    }
                }
                UploadOp::UploadMetadata { .. } => {}
                UploadOp::Delete(Delete { layers }) => {
                    for (name, meta) in layers {
                        upload_queue
                            .recently_deleted
                            .insert((name.clone(), meta.generation));
                    }
                }
                UploadOp::Barrier(sender) => {
                    sender.send_replace(());
                    continue;
                }
                UploadOp::Shutdown => unreachable!("shutdown is intentionally never popped off"),
            };

            // Assign unique ID to this task
            upload_queue.task_counter += 1;
            let upload_task_id = upload_queue.task_counter;

            // Add it to the in-progress map
            let task = Arc::new(UploadTask {
                task_id: upload_task_id,
                op: next_op,
                coalesced_ops,
                retries: AtomicU32::new(0),
            });
            upload_queue
                .inprogress_tasks
                .insert(task.task_id, Arc::clone(&task));

            // Spawn task to perform the task
            let self_rc = Arc::clone(self);
            let tenant_shard_id = self.tenant_shard_id;
            let timeline_id = self.timeline_id;
            task_mgr::spawn(
                &self.runtime,
                TaskKind::RemoteUploadTask,
                self.tenant_shard_id,
                Some(self.timeline_id),
                "remote upload",
                async move {
                    self_rc.perform_upload_task(task).await;
                    Ok(())
                }
                .instrument(info_span!(parent: None, "remote_upload", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %timeline_id, %upload_task_id)),
            );

            // Loop back to process next task
        }
    }

    ///
    /// Perform an upload task.
    ///
    /// The task is in the `inprogress_tasks` list. This function will try to
    /// execute it, retrying forever. On successful completion, the task is
    /// removed it from the `inprogress_tasks` list, and any next task(s) in the
    /// queue that were waiting by the completion are launched.
    ///
    /// The task can be shut down, however. That leads to stopping the whole
    /// queue.
    ///
    async fn perform_upload_task(self: &Arc<Self>, task: Arc<UploadTask>) {
        let cancel = shutdown_token();
        // Loop to retry until it completes.
        loop {
            // If we're requested to shut down, close up shop and exit.
            //
            // Note: We only check for the shutdown requests between retries, so
            // if a shutdown request arrives while we're busy uploading, in the
            // upload::upload:*() call below, we will wait not exit until it has
            // finished. We probably could cancel the upload by simply dropping
            // the Future, but we're not 100% sure if the remote storage library
            // is cancellation safe, so we don't dare to do that. Hopefully, the
            // upload finishes or times out soon enough.
            if cancel.is_cancelled() {
                info!("upload task cancelled by shutdown request");
                self.stop();
                return;
            }

            // Assert that we don't modify a layer that's referenced by the current index.
            if cfg!(debug_assertions) {
                let modified = match &task.op {
                    UploadOp::UploadLayer(layer, layer_metadata, _) => {
                        vec![(layer.layer_desc().layer_name(), layer_metadata)]
                    }
                    UploadOp::Delete(delete) => {
                        delete.layers.iter().map(|(n, m)| (n.clone(), m)).collect()
                    }
                    // These don't modify layers.
                    UploadOp::UploadMetadata { .. } => Vec::new(),
                    UploadOp::Barrier(_) => Vec::new(),
                    UploadOp::Shutdown => Vec::new(),
                };
                if let Ok(queue) = self.upload_queue.lock().unwrap().initialized_mut() {
                    for (ref name, metadata) in modified {
                        debug_assert!(
                            !queue.clean.0.references(name, metadata),
                            "layer {name} modified while referenced by index",
                        );
                    }
                }
            }

            let upload_result: anyhow::Result<()> = match &task.op {
                UploadOp::UploadLayer(ref layer, ref layer_metadata, mode) => {
                    // TODO: check if this mechanism can be removed now that can_bypass() performs
                    // conflict checks during scheduling.
                    if let Some(OpType::FlushDeletion) = mode {
                        if self.config.read().unwrap().block_deletions {
                            // Of course, this is not efficient... but usually the queue should be empty.
                            let mut queue_locked = self.upload_queue.lock().unwrap();
                            let mut detected = false;
                            if let Ok(queue) = queue_locked.initialized_mut() {
                                for list in queue.blocked_deletions.iter_mut() {
                                    list.layers.retain(|(name, meta)| {
                                        if name == &layer.layer_desc().layer_name()
                                            && meta.generation == layer_metadata.generation
                                        {
                                            detected = true;
                                            // remove the layer from deletion queue
                                            false
                                        } else {
                                            // keep the layer
                                            true
                                        }
                                    });
                                }
                            }
                            if detected {
                                info!(
                                    "cancelled blocked deletion of layer {} at gen {:?}",
                                    layer.layer_desc().layer_name(),
                                    layer_metadata.generation
                                );
                            }
                        } else {
                            // TODO: we did not guarantee that upload task starts after deletion task, so there could be possibly race conditions
                            // that we still get the layer deleted. But this only happens if someone creates a layer immediately after it's deleted,
                            // which is not possible in the current system.
                            info!(
                                "waiting for deletion queue flush to complete before uploading layer {} at gen {:?}",
                                layer.layer_desc().layer_name(),
                                layer_metadata.generation
                            );
                            {
                                // We are going to flush, we can clean up the recently deleted list.
                                let mut queue_locked = self.upload_queue.lock().unwrap();
                                if let Ok(queue) = queue_locked.initialized_mut() {
                                    queue.recently_deleted.clear();
                                }
                            }
                            if let Err(e) = self.deletion_queue_client.flush_execute().await {
                                warn!(
                                    "failed to flush the deletion queue before uploading layer {} at gen {:?}, still proceeding to upload: {e:#} ",
                                    layer.layer_desc().layer_name(),
                                    layer_metadata.generation
                                );
                            } else {
                                info!(
                                    "done flushing deletion queue before uploading layer {} at gen {:?}",
                                    layer.layer_desc().layer_name(),
                                    layer_metadata.generation
                                );
                            }
                        }
                    }
                    let local_path = layer.local_path();

                    // We should only be uploading layers created by this `Tenant`'s lifetime, so
                    // the metadata in the upload should always match our current generation.
                    assert_eq!(layer_metadata.generation, self.generation);

                    let remote_path = remote_layer_path(
                        &self.tenant_shard_id.tenant_id,
                        &self.timeline_id,
                        layer_metadata.shard,
                        &layer.layer_desc().layer_name(),
                        layer_metadata.generation,
                    );

                    upload::upload_timeline_layer(
                        &self.storage_impl,
                        local_path,
                        &remote_path,
                        layer_metadata.file_size,
                        &self.cancel,
                    )
                    .measure_remote_op(
                        RemoteOpFileKind::Layer,
                        RemoteOpKind::Upload,
                        Arc::clone(&self.metrics),
                    )
                    .await
                }
                UploadOp::UploadMetadata { ref uploaded } => {
                    let res = upload::upload_index_part(
                        &self.storage_impl,
                        &self.tenant_shard_id,
                        &self.timeline_id,
                        self.generation,
                        uploaded,
                        &self.cancel,
                    )
                    .measure_remote_op(
                        RemoteOpFileKind::Index,
                        RemoteOpKind::Upload,
                        Arc::clone(&self.metrics),
                    )
                    .await;
                    if res.is_ok() {
                        self.update_remote_physical_size_gauge(Some(uploaded));
                        let mention_having_future_layers = if cfg!(feature = "testing") {
                            uploaded
                                .layer_metadata
                                .keys()
                                .any(|x| x.is_in_future(uploaded.metadata.disk_consistent_lsn()))
                        } else {
                            false
                        };
                        if mention_having_future_layers {
                            // find rationale near crate::tenant::timeline::init::cleanup_future_layer
                            tracing::info!(
                                disk_consistent_lsn = %uploaded.metadata.disk_consistent_lsn(),
                                "uploaded an index_part.json with future layers -- this is ok! if shutdown now, expect future layer cleanup"
                            );
                        }
                    }
                    res
                }
                UploadOp::Delete(delete) => {
                    if self.config.read().unwrap().block_deletions {
                        let mut queue_locked = self.upload_queue.lock().unwrap();
                        if let Ok(queue) = queue_locked.initialized_mut() {
                            queue.blocked_deletions.push(delete.clone());
                        }
                        Ok(())
                    } else {
                        pausable_failpoint!("before-delete-layer-pausable");
                        self.deletion_queue_client
                            .push_layers(
                                self.tenant_shard_id,
                                self.timeline_id,
                                self.generation,
                                delete.layers.clone(),
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!(e))
                    }
                }
                unexpected @ UploadOp::Barrier(_) | unexpected @ UploadOp::Shutdown => {
                    // unreachable. Barrier operations are handled synchronously in
                    // launch_queued_tasks
                    warn!("unexpected {unexpected:?} operation in perform_upload_task");
                    break;
                }
            };

            match upload_result {
                Ok(()) => {
                    break;
                }
                Err(e) if TimeoutOrCancel::caused_by_cancel(&e) => {
                    // loop around to do the proper stopping
                    continue;
                }
                Err(e) => {
                    let retries = task.retries.fetch_add(1, Ordering::SeqCst);

                    // Uploads can fail due to rate limits (IAM, S3), spurious network problems,
                    // or other external reasons. Such issues are relatively regular, so log them
                    // at info level at first, and only WARN if the operation fails repeatedly.
                    //
                    // (See similar logic for downloads in `download::download_retry`)
                    if retries < FAILED_UPLOAD_WARN_THRESHOLD {
                        info!(
                            "failed to perform remote task {}, will retry (attempt {}): {:#}",
                            task.op, retries, e
                        );
                    } else {
                        warn!(
                            "failed to perform remote task {}, will retry (attempt {}): {:?}",
                            task.op, retries, e
                        );
                    }

                    // sleep until it's time to retry, or we're cancelled
                    exponential_backoff(
                        retries,
                        DEFAULT_BASE_BACKOFF_SECONDS,
                        DEFAULT_MAX_BACKOFF_SECONDS,
                        &cancel,
                    )
                    .await;
                }
            }
        }

        let retries = task.retries.load(Ordering::SeqCst);
        if retries > 0 {
            info!(
                "remote task {} completed successfully after {} retries",
                task.op, retries
            );
        } else {
            debug!("remote task {} completed successfully", task.op);
        }

        // The task has completed successfully. Remove it from the in-progress list.
        let lsn_update = {
            let mut upload_queue_guard = self.upload_queue.lock().unwrap();
            let upload_queue = match upload_queue_guard.deref_mut() {
                UploadQueue::Uninitialized => panic!("callers are responsible for ensuring this is only called on an initialized queue"),
                UploadQueue::Stopped(_stopped) => {
                    None
                },
                UploadQueue::Initialized(qi) => { Some(qi) }
            };

            let upload_queue = match upload_queue {
                Some(upload_queue) => upload_queue,
                None => {
                    info!("another concurrent task already stopped the queue");
                    return;
                }
            };

            upload_queue.inprogress_tasks.remove(&task.task_id);

            let lsn_update = match task.op {
                UploadOp::UploadLayer(_, _, _) => None,
                UploadOp::UploadMetadata { ref uploaded } => {
                    // the task id is reused as a monotonicity check for storing the "clean"
                    // IndexPart.
                    let last_updater = upload_queue.clean.1;
                    let is_later = last_updater.is_some_and(|task_id| task_id < task.task_id);
                    let monotone = is_later || last_updater.is_none();

                    assert!(monotone, "no two index uploads should be completing at the same time, prev={last_updater:?}, task.task_id={}", task.task_id);

                    // not taking ownership is wasteful
                    upload_queue.clean.0.clone_from(uploaded);
                    upload_queue.clean.1 = Some(task.task_id);

                    let lsn = upload_queue.clean.0.metadata.disk_consistent_lsn();
                    self.metrics
                        .projected_remote_consistent_lsn_gauge
                        .set(lsn.0);

                    if self.generation.is_none() {
                        // Legacy mode: skip validating generation
                        upload_queue.visible_remote_consistent_lsn.store(lsn);
                        None
                    } else if self
                        .config
                        .read()
                        .unwrap()
                        .process_remote_consistent_lsn_updates
                    {
                        Some((lsn, upload_queue.visible_remote_consistent_lsn.clone()))
                    } else {
                        // Our config disables remote_consistent_lsn updates: drop it.
                        None
                    }
                }
                UploadOp::Delete(_) => None,
                UploadOp::Barrier(..) | UploadOp::Shutdown => unreachable!(),
            };

            // Launch any queued tasks that were unblocked by this one.
            self.launch_queued_tasks(upload_queue);
            lsn_update
        };

        if let Some((lsn, slot)) = lsn_update {
            // Updates to the remote_consistent_lsn we advertise to pageservers
            // are all routed through the DeletionQueue, to enforce important
            // data safety guarantees (see docs/rfcs/025-generation-numbers.md)
            self.deletion_queue_client
                .update_remote_consistent_lsn(
                    self.tenant_shard_id,
                    self.timeline_id,
                    self.generation,
                    lsn,
                    slot,
                )
                .await;
        }

        self.metric_end(&task.op);
        for coalesced_op in &task.coalesced_ops {
            self.metric_end(coalesced_op);
        }
    }

    fn metric_impl(
        &self,
        op: &UploadOp,
    ) -> Option<(
        RemoteOpFileKind,
        RemoteOpKind,
        RemoteTimelineClientMetricsCallTrackSize,
    )> {
        use RemoteTimelineClientMetricsCallTrackSize::DontTrackSize;
        let res = match op {
            UploadOp::UploadLayer(_, m, _) => (
                RemoteOpFileKind::Layer,
                RemoteOpKind::Upload,
                RemoteTimelineClientMetricsCallTrackSize::Bytes(m.file_size),
            ),
            UploadOp::UploadMetadata { .. } => (
                RemoteOpFileKind::Index,
                RemoteOpKind::Upload,
                DontTrackSize {
                    reason: "metadata uploads are tiny",
                },
            ),
            UploadOp::Delete(_delete) => (
                RemoteOpFileKind::Layer,
                RemoteOpKind::Delete,
                DontTrackSize {
                    reason: "should we track deletes? positive or negative sign?",
                },
            ),
            UploadOp::Barrier(..) | UploadOp::Shutdown => {
                // we do not account these
                return None;
            }
        };
        Some(res)
    }

    fn metric_begin(&self, op: &UploadOp) {
        let (file_kind, op_kind, track_bytes) = match self.metric_impl(op) {
            Some(x) => x,
            None => return,
        };
        let guard = self.metrics.call_begin(&file_kind, &op_kind, track_bytes);
        guard.will_decrement_manually(); // in metric_end(), see right below
    }

    fn metric_end(&self, op: &UploadOp) {
        let (file_kind, op_kind, track_bytes) = match self.metric_impl(op) {
            Some(x) => x,
            None => return,
        };
        self.metrics.call_end(&file_kind, &op_kind, track_bytes);
    }

    /// Close the upload queue for new operations and cancel queued operations.
    ///
    /// Use [`RemoteTimelineClient::shutdown`] for graceful stop.
    ///
    /// In-progress operations will still be running after this function returns.
    /// Use `task_mgr::shutdown_tasks(Some(TaskKind::RemoteUploadTask), Some(self.tenant_shard_id), Some(timeline_id))`
    /// to wait for them to complete, after calling this function.
    pub(crate) fn stop(&self) {
        // Whichever *task* for this RemoteTimelineClient grabs the mutex first will transition the queue
        // into stopped state, thereby dropping all off the queued *ops* which haven't become *tasks* yet.
        // The other *tasks* will come here and observe an already shut down queue and hence simply wrap up their business.
        let mut guard = self.upload_queue.lock().unwrap();
        self.stop_impl(&mut guard);
    }

    fn stop_impl(&self, guard: &mut std::sync::MutexGuard<UploadQueue>) {
        match &mut **guard {
            UploadQueue::Uninitialized => {
                info!("UploadQueue is in state Uninitialized, nothing to do");
                **guard = UploadQueue::Stopped(UploadQueueStopped::Uninitialized);
            }
            UploadQueue::Stopped(_) => {
                // nothing to do
                info!("another concurrent task already shut down the queue");
            }
            UploadQueue::Initialized(initialized) => {
                info!("shutting down upload queue");

                // Replace the queue with the Stopped state, taking ownership of the old
                // Initialized queue. We will do some checks on it, and then drop it.
                let qi = {
                    // Here we preserve working version of the upload queue for possible use during deletions.
                    // In-place replace of Initialized to Stopped can be done with the help of https://github.com/Sgeo/take_mut
                    // but for this use case it doesnt really makes sense to bring unsafe code only for this usage point.
                    // Deletion is not really perf sensitive so there shouldnt be any problems with cloning a fraction of it.
                    let upload_queue_for_deletion = UploadQueueInitialized {
                        inprogress_limit: initialized.inprogress_limit,
                        task_counter: 0,
                        dirty: initialized.dirty.clone(),
                        clean: initialized.clean.clone(),
                        latest_files_changes_since_metadata_upload_scheduled: 0,
                        visible_remote_consistent_lsn: initialized
                            .visible_remote_consistent_lsn
                            .clone(),
                        inprogress_tasks: HashMap::default(),
                        queued_operations: VecDeque::default(),
                        #[cfg(feature = "testing")]
                        dangling_files: HashMap::default(),
                        blocked_deletions: Vec::new(),
                        shutting_down: false,
                        shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
                        recently_deleted: HashSet::new(),
                    };

                    let upload_queue = std::mem::replace(
                        &mut **guard,
                        UploadQueue::Stopped(UploadQueueStopped::Deletable(
                            UploadQueueStoppedDeletable {
                                upload_queue_for_deletion,
                                deleted_at: SetDeletedFlagProgress::NotRunning,
                            },
                        )),
                    );
                    if let UploadQueue::Initialized(qi) = upload_queue {
                        qi
                    } else {
                        unreachable!("we checked in the match above that it is Initialized");
                    }
                };

                // We don't need to do anything here for in-progress tasks. They will finish
                // on their own, decrement the unfinished-task counter themselves, and observe
                // that the queue is Stopped.
                drop(qi.inprogress_tasks);

                // Tear down queued ops
                for op in qi.queued_operations.into_iter() {
                    self.metric_end(&op);
                    // Dropping UploadOp::Barrier() here will make wait_completion() return with an Err()
                    // which is exactly what we want to happen.
                    drop(op);
                }
            }
        }
    }

    /// Returns an accessor which will hold the UploadQueue mutex for accessing the upload queue
    /// externally to RemoteTimelineClient.
    pub(crate) fn initialized_upload_queue(
        &self,
    ) -> Result<UploadQueueAccessor<'_>, NotInitialized> {
        let mut inner = self.upload_queue.lock().unwrap();
        inner.initialized_mut()?;
        Ok(UploadQueueAccessor { inner })
    }

    pub(crate) fn no_pending_work(&self) -> bool {
        let inner = self.upload_queue.lock().unwrap();
        match &*inner {
            UploadQueue::Uninitialized
            | UploadQueue::Stopped(UploadQueueStopped::Uninitialized) => true,
            UploadQueue::Stopped(UploadQueueStopped::Deletable(x)) => {
                x.upload_queue_for_deletion.no_pending_work()
            }
            UploadQueue::Initialized(x) => x.no_pending_work(),
        }
    }

    /// 'foreign' in the sense that it does not belong to this tenant shard.  This method
    /// is used during GC for other shards to get the index of shard zero.
    pub(crate) async fn download_foreign_index(
        &self,
        shard_number: ShardNumber,
        cancel: &CancellationToken,
    ) -> Result<(IndexPart, Generation, std::time::SystemTime), DownloadError> {
        let foreign_shard_id = TenantShardId {
            shard_number,
            shard_count: self.tenant_shard_id.shard_count,
            tenant_id: self.tenant_shard_id.tenant_id,
        };
        download_index_part(
            &self.storage_impl,
            &foreign_shard_id,
            &self.timeline_id,
            Generation::MAX,
            cancel,
        )
        .await
    }
}

pub(crate) struct UploadQueueAccessor<'a> {
    inner: std::sync::MutexGuard<'a, UploadQueue>,
}

impl UploadQueueAccessor<'_> {
    pub(crate) fn latest_uploaded_index_part(&self) -> &IndexPart {
        match &*self.inner {
            UploadQueue::Initialized(x) => &x.clean.0,
            UploadQueue::Uninitialized | UploadQueue::Stopped(_) => {
                unreachable!("checked before constructing")
            }
        }
    }
}

pub fn remote_tenant_path(tenant_shard_id: &TenantShardId) -> RemotePath {
    let path = format!("tenants/{tenant_shard_id}");
    RemotePath::from_string(&path).expect("Failed to construct path")
}

pub fn remote_tenant_manifest_path(
    tenant_shard_id: &TenantShardId,
    generation: Generation,
) -> RemotePath {
    let path = format!(
        "tenants/{tenant_shard_id}/tenant-manifest{}.json",
        generation.get_suffix()
    );
    RemotePath::from_string(&path).expect("Failed to construct path")
}

/// Prefix to all generations' manifest objects in a tenant shard
pub fn remote_tenant_manifest_prefix(tenant_shard_id: &TenantShardId) -> RemotePath {
    let path = format!("tenants/{tenant_shard_id}/tenant-manifest",);
    RemotePath::from_string(&path).expect("Failed to construct path")
}

pub fn remote_timelines_path(tenant_shard_id: &TenantShardId) -> RemotePath {
    let path = format!("tenants/{tenant_shard_id}/{TIMELINES_SEGMENT_NAME}");
    RemotePath::from_string(&path).expect("Failed to construct path")
}

fn remote_timelines_path_unsharded(tenant_id: &TenantId) -> RemotePath {
    let path = format!("tenants/{tenant_id}/{TIMELINES_SEGMENT_NAME}");
    RemotePath::from_string(&path).expect("Failed to construct path")
}

pub fn remote_timeline_path(
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
) -> RemotePath {
    remote_timelines_path(tenant_shard_id).join(Utf8Path::new(&timeline_id.to_string()))
}

/// Obtains the path of the given Layer in the remote
///
/// Note that the shard component of a remote layer path is _not_ always the same
/// as in the TenantShardId of the caller: tenants may reference layers from a different
/// ShardIndex.  Use the ShardIndex from the layer's metadata.
pub fn remote_layer_path(
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    shard: ShardIndex,
    layer_file_name: &LayerName,
    generation: Generation,
) -> RemotePath {
    // Generation-aware key format
    let path = format!(
        "tenants/{tenant_id}{0}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{1}{2}",
        shard.get_suffix(),
        layer_file_name,
        generation.get_suffix()
    );

    RemotePath::from_string(&path).expect("Failed to construct path")
}

/// Returns true if a and b have the same layer path within a tenant/timeline. This is essentially
/// remote_layer_path(a) == remote_layer_path(b) without the string allocations.
///
/// TODO: there should be a variant of LayerName for the physical path that contains information
/// about the shard and generation, such that this could be replaced by a simple comparison.
pub fn is_same_remote_layer_path(
    aname: &LayerName,
    ameta: &LayerFileMetadata,
    bname: &LayerName,
    bmeta: &LayerFileMetadata,
) -> bool {
    // NB: don't assert remote_layer_path(a) == remote_layer_path(b); too expensive even for debug.
    aname == bname && ameta.shard == bmeta.shard && ameta.generation == bmeta.generation
}

pub fn remote_initdb_archive_path(tenant_id: &TenantId, timeline_id: &TimelineId) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_id}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{INITDB_PATH}"
    ))
    .expect("Failed to construct path")
}

pub fn remote_initdb_preserved_archive_path(
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_id}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{INITDB_PRESERVED_PATH}"
    ))
    .expect("Failed to construct path")
}

pub fn remote_index_path(
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    generation: Generation,
) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_shard_id}/{TIMELINES_SEGMENT_NAME}/{timeline_id}/{0}{1}",
        IndexPart::FILE_NAME,
        generation.get_suffix()
    ))
    .expect("Failed to construct path")
}

pub(crate) fn remote_heatmap_path(tenant_shard_id: &TenantShardId) -> RemotePath {
    RemotePath::from_string(&format!(
        "tenants/{tenant_shard_id}/{TENANT_HEATMAP_BASENAME}"
    ))
    .expect("Failed to construct path")
}

/// Given the key of an index, parse out the generation part of the name
pub fn parse_remote_index_path(path: RemotePath) -> Option<Generation> {
    let file_name = match path.get_path().file_name() {
        Some(f) => f,
        None => {
            // Unexpected: we should be seeing index_part.json paths only
            tracing::warn!("Malformed index key {}", path);
            return None;
        }
    };

    match file_name.split_once('-') {
        Some((_, gen_suffix)) => Generation::parse_suffix(gen_suffix),
        None => None,
    }
}

/// Given the key of a tenant manifest, parse out the generation number
pub fn parse_remote_tenant_manifest_path(path: RemotePath) -> Option<Generation> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r".*tenant-manifest-([0-9a-f]{8}).json").unwrap());
    re.captures(path.get_path().as_str())
        .and_then(|c| c.get(1))
        .and_then(|m| Generation::parse_suffix(m.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::RequestContext,
        tenant::{
            config::AttachmentMode,
            harness::{TenantShardHarness, TIMELINE_ID},
            storage_layer::layer::local_layer_path,
            TenantShard, Timeline,
        },
        DEFAULT_PG_VERSION,
    };

    use std::collections::HashSet;

    pub(super) fn dummy_contents(name: &str) -> Vec<u8> {
        format!("contents for {name}").into()
    }

    pub(super) fn dummy_metadata(disk_consistent_lsn: Lsn) -> TimelineMetadata {
        let metadata = TimelineMetadata::new(
            disk_consistent_lsn,
            None,
            None,
            Lsn(0),
            Lsn(0),
            Lsn(0),
            // Any version will do
            // but it should be consistent with the one in the tests
            crate::DEFAULT_PG_VERSION,
        );

        // go through serialize + deserialize to fix the header, including checksum
        TimelineMetadata::from_bytes(&metadata.to_bytes().unwrap()).unwrap()
    }

    fn assert_file_list(a: &HashSet<LayerName>, b: &[&str]) {
        let mut avec: Vec<String> = a.iter().map(|x| x.to_string()).collect();
        avec.sort();

        let mut bvec = b.to_vec();
        bvec.sort_unstable();

        assert_eq!(avec, bvec);
    }

    fn assert_remote_files(expected: &[&str], remote_path: &Utf8Path, generation: Generation) {
        let mut expected: Vec<String> = expected
            .iter()
            .map(|x| format!("{}{}", x, generation.get_suffix()))
            .collect();
        expected.sort();

        let mut found: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(remote_path).unwrap().flatten() {
            let entry_name = entry.file_name();
            let fname = entry_name.to_str().unwrap();
            found.push(String::from(fname));
        }
        found.sort();

        assert_eq!(found, expected);
    }

    struct TestSetup {
        harness: TenantShardHarness,
        tenant: Arc<TenantShard>,
        timeline: Arc<Timeline>,
        tenant_ctx: RequestContext,
    }

    impl TestSetup {
        async fn new(test_name: &str) -> anyhow::Result<Self> {
            let test_name = Box::leak(Box::new(format!("remote_timeline_client__{test_name}")));
            let harness = TenantShardHarness::create(test_name).await?;
            let (tenant, ctx) = harness.load().await;

            let timeline = tenant
                .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
                .await?;

            Ok(Self {
                harness,
                tenant,
                timeline,
                tenant_ctx: ctx,
            })
        }

        /// Construct a RemoteTimelineClient in an arbitrary generation
        fn build_client(&self, generation: Generation) -> Arc<RemoteTimelineClient> {
            let location_conf = AttachedLocationConfig {
                generation,
                attach_mode: AttachmentMode::Single,
            };
            Arc::new(RemoteTimelineClient {
                conf: self.harness.conf,
                runtime: tokio::runtime::Handle::current(),
                tenant_shard_id: self.harness.tenant_shard_id,
                timeline_id: TIMELINE_ID,
                generation,
                storage_impl: self.harness.remote_storage.clone(),
                deletion_queue_client: self.harness.deletion_queue.new_client(),
                upload_queue: Mutex::new(UploadQueue::Uninitialized),
                metrics: Arc::new(RemoteTimelineClientMetrics::new(
                    &self.harness.tenant_shard_id,
                    &TIMELINE_ID,
                )),
                config: std::sync::RwLock::new(RemoteTimelineClientConfig::from(&location_conf)),
                cancel: CancellationToken::new(),
            })
        }

        /// A tracing::Span that satisfies remote_timeline_client methods that assert tenant_id
        /// and timeline_id are present.
        fn span(&self) -> tracing::Span {
            tracing::info_span!(
                "test",
                tenant_id = %self.harness.tenant_shard_id.tenant_id,
                shard_id = %self.harness.tenant_shard_id.shard_slug(),
                timeline_id = %TIMELINE_ID
            )
        }
    }

    // Test scheduling
    #[tokio::test]
    async fn upload_scheduling() {
        // Test outline:
        //
        // Schedule upload of a bunch of layers. Check that they are started immediately, not queued
        // Schedule upload of index. Check that it is queued
        // let the layer file uploads finish. Check that the index-upload is now started
        // let the index-upload finish.
        //
        // Download back the index.json. Check that the list of files is correct
        //
        // Schedule upload. Schedule deletion. Check that the deletion is queued
        // let upload finish. Check that deletion is now started
        // Schedule another deletion. Check that it's launched immediately.
        // Schedule index upload. Check that it's queued

        let test_setup = TestSetup::new("upload_scheduling").await.unwrap();
        let span = test_setup.span();
        let _guard = span.enter();

        let TestSetup {
            harness,
            tenant: _tenant,
            timeline,
            tenant_ctx: _tenant_ctx,
        } = test_setup;

        let client = &timeline.remote_client;

        // Download back the index.json, and check that the list of files is correct
        let initial_index_part = match client
            .download_index_file(&CancellationToken::new())
            .await
            .unwrap()
        {
            MaybeDeletedIndexPart::IndexPart(index_part) => index_part,
            MaybeDeletedIndexPart::Deleted(_) => panic!("unexpectedly got deleted index part"),
        };
        let initial_layers = initial_index_part
            .layer_metadata
            .keys()
            .map(|f| f.to_owned())
            .collect::<HashSet<LayerName>>();
        let initial_layer = {
            assert!(initial_layers.len() == 1);
            initial_layers.into_iter().next().unwrap()
        };

        let timeline_path = harness.timeline_path(&TIMELINE_ID);

        println!("workdir: {}", harness.conf.workdir);

        let remote_timeline_dir = harness
            .remote_fs_dir
            .join(timeline_path.strip_prefix(&harness.conf.workdir).unwrap());
        println!("remote_timeline_dir: {remote_timeline_dir}");

        let generation = harness.generation;
        let shard = harness.shard;

        // Create a couple of dummy files,  schedule upload for them

        let layers = [
            ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), dummy_contents("foo")),
            ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D9-00000000016B5A52".parse().unwrap(), dummy_contents("bar")),
            ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59DA-00000000016B5A53".parse().unwrap(), dummy_contents("baz"))
        ]
        .into_iter()
        .map(|(name, contents): (LayerName, Vec<u8>)| {

            let local_path = local_layer_path(
                harness.conf,
                &timeline.tenant_shard_id,
                &timeline.timeline_id,
                &name,
                &generation,
            );
            std::fs::write(&local_path, &contents).unwrap();

            Layer::for_resident(
                harness.conf,
                &timeline,
                local_path,
                name,
                LayerFileMetadata::new(contents.len() as u64, generation, shard),
            )
        }).collect::<Vec<_>>();

        client
            .schedule_layer_file_upload(layers[0].clone())
            .unwrap();
        client
            .schedule_layer_file_upload(layers[1].clone())
            .unwrap();

        // Check that they are started immediately, not queued
        //
        // this works because we running within block_on, so any futures are now queued up until
        // our next await point.
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.is_empty());
            assert_eq!(upload_queue.inprogress_tasks.len(), 2);
            assert_eq!(upload_queue.num_inprogress_layer_uploads(), 2);

            // also check that `latest_file_changes` was updated
            assert!(upload_queue.latest_files_changes_since_metadata_upload_scheduled == 2);
        }

        // Schedule upload of index. Check that it is queued
        let metadata = dummy_metadata(Lsn(0x20));
        client
            .schedule_index_upload_for_full_metadata_update(&metadata)
            .unwrap();
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();
            assert!(upload_queue.queued_operations.len() == 1);
            assert!(upload_queue.latest_files_changes_since_metadata_upload_scheduled == 0);
        }

        // Wait for the uploads to finish
        client.wait_completion().await.unwrap();
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            assert!(upload_queue.queued_operations.is_empty());
            assert!(upload_queue.inprogress_tasks.is_empty());
        }

        // Download back the index.json, and check that the list of files is correct
        let index_part = match client
            .download_index_file(&CancellationToken::new())
            .await
            .unwrap()
        {
            MaybeDeletedIndexPart::IndexPart(index_part) => index_part,
            MaybeDeletedIndexPart::Deleted(_) => panic!("unexpectedly got deleted index part"),
        };

        assert_file_list(
            &index_part
                .layer_metadata
                .keys()
                .map(|f| f.to_owned())
                .collect(),
            &[
                &initial_layer.to_string(),
                &layers[0].layer_desc().layer_name().to_string(),
                &layers[1].layer_desc().layer_name().to_string(),
            ],
        );
        assert_eq!(index_part.metadata, metadata);

        // Schedule upload and then a deletion. Check that the deletion is queued
        client
            .schedule_layer_file_upload(layers[2].clone())
            .unwrap();

        // this is no longer consistent with how deletion works with Layer::drop, but in this test
        // keep using schedule_layer_file_deletion because we don't have a way to wait for the
        // spawn_blocking started by the drop.
        client
            .schedule_layer_file_deletion(&[layers[0].layer_desc().layer_name()])
            .unwrap();
        {
            let mut guard = client.upload_queue.lock().unwrap();
            let upload_queue = guard.initialized_mut().unwrap();

            // Deletion schedules upload of the index file, and the file deletion itself
            assert_eq!(upload_queue.queued_operations.len(), 2);
            assert_eq!(upload_queue.inprogress_tasks.len(), 1);
            assert_eq!(upload_queue.num_inprogress_layer_uploads(), 1);
            assert_eq!(upload_queue.num_inprogress_deletions(), 0);
            assert_eq!(
                upload_queue.latest_files_changes_since_metadata_upload_scheduled,
                0
            );
        }
        assert_remote_files(
            &[
                &initial_layer.to_string(),
                &layers[0].layer_desc().layer_name().to_string(),
                &layers[1].layer_desc().layer_name().to_string(),
                "index_part.json",
            ],
            &remote_timeline_dir,
            generation,
        );

        // Finish them
        client.wait_completion().await.unwrap();
        harness.deletion_queue.pump().await;

        assert_remote_files(
            &[
                &initial_layer.to_string(),
                &layers[1].layer_desc().layer_name().to_string(),
                &layers[2].layer_desc().layer_name().to_string(),
                "index_part.json",
            ],
            &remote_timeline_dir,
            generation,
        );
    }

    #[tokio::test]
    async fn bytes_unfinished_gauge_for_layer_file_uploads() {
        // Setup

        let TestSetup {
            harness,
            tenant: _tenant,
            timeline,
            ..
        } = TestSetup::new("metrics").await.unwrap();
        let client = &timeline.remote_client;

        let layer_file_name_1: LayerName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let local_path = local_layer_path(
            harness.conf,
            &timeline.tenant_shard_id,
            &timeline.timeline_id,
            &layer_file_name_1,
            &harness.generation,
        );
        let content_1 = dummy_contents("foo");
        std::fs::write(&local_path, &content_1).unwrap();

        let layer_file_1 = Layer::for_resident(
            harness.conf,
            &timeline,
            local_path,
            layer_file_name_1.clone(),
            LayerFileMetadata::new(content_1.len() as u64, harness.generation, harness.shard),
        );

        #[derive(Debug, PartialEq, Clone, Copy)]
        struct BytesStartedFinished {
            started: Option<usize>,
            finished: Option<usize>,
        }
        impl std::ops::Add for BytesStartedFinished {
            type Output = Self;
            fn add(self, rhs: Self) -> Self::Output {
                Self {
                    started: self.started.map(|v| v + rhs.started.unwrap_or(0)),
                    finished: self.finished.map(|v| v + rhs.finished.unwrap_or(0)),
                }
            }
        }
        let get_bytes_started_stopped = || {
            let started = client
                .metrics
                .get_bytes_started_counter_value(&RemoteOpFileKind::Layer, &RemoteOpKind::Upload)
                .map(|v| v.try_into().unwrap());
            let stopped = client
                .metrics
                .get_bytes_finished_counter_value(&RemoteOpFileKind::Layer, &RemoteOpKind::Upload)
                .map(|v| v.try_into().unwrap());
            BytesStartedFinished {
                started,
                finished: stopped,
            }
        };

        // Test
        tracing::info!("now doing actual test");

        let actual_a = get_bytes_started_stopped();

        client
            .schedule_layer_file_upload(layer_file_1.clone())
            .unwrap();

        let actual_b = get_bytes_started_stopped();

        client.wait_completion().await.unwrap();

        let actual_c = get_bytes_started_stopped();

        // Validate

        let expected_b = actual_a
            + BytesStartedFinished {
                started: Some(content_1.len()),
                // assert that the _finished metric is created eagerly so that subtractions work on first sample
                finished: Some(0),
            };
        assert_eq!(actual_b, expected_b);

        let expected_c = actual_a
            + BytesStartedFinished {
                started: Some(content_1.len()),
                finished: Some(content_1.len()),
            };
        assert_eq!(actual_c, expected_c);
    }

    async fn inject_index_part(test_state: &TestSetup, generation: Generation) -> IndexPart {
        // An empty IndexPart, just sufficient to ensure deserialization will succeed
        let example_index_part = IndexPart::example();

        let index_part_bytes = serde_json::to_vec(&example_index_part).unwrap();

        let index_path = test_state.harness.remote_fs_dir.join(
            remote_index_path(
                &test_state.harness.tenant_shard_id,
                &TIMELINE_ID,
                generation,
            )
            .get_path(),
        );

        std::fs::create_dir_all(index_path.parent().unwrap())
            .expect("creating test dir should work");

        eprintln!("Writing {index_path}");
        std::fs::write(&index_path, index_part_bytes).unwrap();
        example_index_part
    }

    /// Assert that when a RemoteTimelineclient in generation `get_generation` fetches its
    /// index, the IndexPart returned is equal to `expected`
    async fn assert_got_index_part(
        test_state: &TestSetup,
        get_generation: Generation,
        expected: &IndexPart,
    ) {
        let client = test_state.build_client(get_generation);

        let download_r = client
            .download_index_file(&CancellationToken::new())
            .await
            .expect("download should always succeed");
        assert!(matches!(download_r, MaybeDeletedIndexPart::IndexPart(_)));
        match download_r {
            MaybeDeletedIndexPart::IndexPart(index_part) => {
                assert_eq!(&index_part, expected);
            }
            MaybeDeletedIndexPart::Deleted(_index_part) => panic!("Test doesn't set deleted_at"),
        }
    }

    #[tokio::test]
    async fn index_part_download_simple() -> anyhow::Result<()> {
        let test_state = TestSetup::new("index_part_download_simple").await.unwrap();
        let span = test_state.span();
        let _guard = span.enter();

        // Simple case: we are in generation N, load the index from generation N - 1
        let generation_n = 5;
        let injected = inject_index_part(&test_state, Generation::new(generation_n - 1)).await;

        assert_got_index_part(&test_state, Generation::new(generation_n), &injected).await;

        Ok(())
    }

    #[tokio::test]
    async fn index_part_download_ordering() -> anyhow::Result<()> {
        let test_state = TestSetup::new("index_part_download_ordering")
            .await
            .unwrap();

        let span = test_state.span();
        let _guard = span.enter();

        // A generation-less IndexPart exists in the bucket, we should find it
        let generation_n = 5;
        let injected_none = inject_index_part(&test_state, Generation::none()).await;
        assert_got_index_part(&test_state, Generation::new(generation_n), &injected_none).await;

        // If a more recent-than-none generation exists, we should prefer to load that
        let injected_1 = inject_index_part(&test_state, Generation::new(1)).await;
        assert_got_index_part(&test_state, Generation::new(generation_n), &injected_1).await;

        // If a more-recent-than-me generation exists, we should ignore it.
        let _injected_10 = inject_index_part(&test_state, Generation::new(10)).await;
        assert_got_index_part(&test_state, Generation::new(generation_n), &injected_1).await;

        // If a directly previous generation exists, _and_ an index exists in my own
        // generation, I should prefer my own generation.
        let _injected_prev =
            inject_index_part(&test_state, Generation::new(generation_n - 1)).await;
        let injected_current = inject_index_part(&test_state, Generation::new(generation_n)).await;
        assert_got_index_part(
            &test_state,
            Generation::new(generation_n),
            &injected_current,
        )
        .await;

        Ok(())
    }
}
