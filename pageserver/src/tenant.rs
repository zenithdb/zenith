//! Timeline repository implementation that keeps old data in layer files, and
//! the recent changes in ephemeral files.
//!
//! See tenant/*_layer.rs files. The functions here are responsible for locating
//! the correct layer for the get/put call, walking back the timeline branching
//! history as needed.
//!
//! The files are stored in the .neon/tenants/<tenant_id>/timelines/<timeline_id>
//! directory. See docs/pageserver-storage.md for how the files are managed.
//! In addition to the layer files, there is a metadata file in the same
//! directory that contains information about the timeline, in particular its
//! parent timeline, and the last LSN that has been written to disk.
//!

use anyhow::{bail, Context};
use arc_swap::ArcSwap;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::NaiveDateTime;
use enumset::EnumSet;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use pageserver_api::models;
use pageserver_api::models::CompactInfoResponse;
use pageserver_api::models::LsnLease;
use pageserver_api::models::TimelineArchivalState;
use pageserver_api::models::TimelineState;
use pageserver_api::models::TopTenantShardItem;
use pageserver_api::models::WalRedoManagerStatus;
use pageserver_api::shard::ShardIdentity;
use pageserver_api::shard::ShardStripeSize;
use pageserver_api::shard::TenantShardId;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use remote_storage::TimeoutOrCancel;
use remote_timeline_client::manifest::{
    OffloadedTimelineManifest, TenantManifest, LATEST_TENANT_MANIFEST_VERSION,
};
use remote_timeline_client::UploadQueueNotReadyError;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Weak;
use std::time::SystemTime;
use storage_broker::BrokerClientChannel;
use timeline::compaction::GcCompactionQueue;
use timeline::import_pgdata;
use timeline::offload::offload_timeline;
use timeline::offload::OffloadError;
use timeline::CompactOptions;
use timeline::ShutdownMode;
use tokio::io::BufReader;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;
use upload_queue::NotInitialized;
use utils::backoff;
use utils::circuit_breaker::CircuitBreaker;
use utils::completion;
use utils::crashsafe::path_with_suffix_extension;
use utils::failpoint_support;
use utils::fs_ext;
use utils::pausable_failpoint;
use utils::sync::gate::Gate;
use utils::sync::gate::GateGuard;
use utils::timeout::timeout_cancellable;
use utils::timeout::TimeoutCancellableError;
use utils::try_rcu::ArcSwapExt;
use utils::zstd::create_zst_tarball;
use utils::zstd::extract_zst_tarball;

use self::config::AttachedLocationConfig;
use self::config::AttachmentMode;
use self::config::LocationConf;
use self::config::TenantConf;
use self::metadata::TimelineMetadata;
use self::mgr::GetActiveTenantError;
use self::mgr::GetTenantError;
use self::remote_timeline_client::upload::{upload_index_part, upload_tenant_manifest};
use self::remote_timeline_client::{RemoteTimelineClient, WaitCompletionError};
use self::timeline::uninit::TimelineCreateGuard;
use self::timeline::uninit::TimelineExclusionError;
use self::timeline::uninit::UninitializedTimeline;
use self::timeline::EvictionTaskTenantState;
use self::timeline::GcCutoffs;
use self::timeline::TimelineDeleteProgress;
use self::timeline::TimelineResources;
use self::timeline::WaitLsnError;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::deletion_queue::DeletionQueueClient;
use crate::deletion_queue::DeletionQueueError;
use crate::import_datadir;
use crate::is_uninit_mark;
use crate::l0_flush::L0FlushGlobalState;
use crate::metrics::CONCURRENT_INITDBS;
use crate::metrics::INITDB_RUN_TIME;
use crate::metrics::INITDB_SEMAPHORE_ACQUISITION_TIME;
use crate::metrics::TENANT;
use crate::metrics::{
    remove_tenant_metrics, BROKEN_TENANTS_SET, CIRCUIT_BREAKERS_BROKEN, CIRCUIT_BREAKERS_UNBROKEN,
    TENANT_STATE_METRIC, TENANT_SYNTHETIC_SIZE_METRIC,
};
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::tenant::config::LocationMode;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::gc_result::GcResult;
pub use crate::tenant::remote_timeline_client::index::IndexPart;
use crate::tenant::remote_timeline_client::remote_initdb_archive_path;
use crate::tenant::remote_timeline_client::MaybeDeletedIndexPart;
use crate::tenant::remote_timeline_client::INITDB_PATH;
use crate::tenant::storage_layer::DeltaLayer;
use crate::tenant::storage_layer::ImageLayer;
use crate::walingest::WalLagCooldown;
use crate::walredo;
use crate::InitializationOrder;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::fs;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::span;
use crate::tenant::timeline::delete::DeleteTimelineFlow;
use crate::tenant::timeline::uninit::cleanup_timeline_directory;
use crate::virtual_file::VirtualFile;
use crate::walredo::PostgresRedoManager;
use crate::TEMP_FILE_SUFFIX;
use once_cell::sync::Lazy;
pub use pageserver_api::models::TenantState;
use tokio::sync::Semaphore;

static INIT_DB_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| Semaphore::new(8));
use utils::{
    crashsafe,
    generation::Generation,
    id::TimelineId,
    lsn::{Lsn, RecordLsn},
};

pub mod blob_io;
pub mod block_io;
pub mod vectored_blob_io;

pub mod disk_btree;
pub(crate) mod ephemeral_file;
pub mod layer_map;

pub mod metadata;
pub mod remote_timeline_client;
pub mod storage_layer;

pub mod checks;
pub mod config;
pub mod mgr;
pub mod secondary;
pub mod tasks;
pub mod upload_queue;

pub(crate) mod timeline;

pub mod size;

mod gc_block;
mod gc_result;
pub(crate) mod throttle;

pub(crate) use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
pub(crate) use timeline::{LogicalSizeCalculationCause, PageReconstructError, Timeline};

// re-export for use in walreceiver
pub use crate::tenant::timeline::WalReceiverInfo;

/// The "tenants" part of `tenants/<tenant>/timelines...`
pub const TENANTS_SEGMENT_NAME: &str = "tenants";

/// Parts of the `.neon/tenants/<tenant_id>/timelines/<timeline_id>` directory prefix.
pub const TIMELINES_SEGMENT_NAME: &str = "timelines";

/// References to shared objects that are passed into each tenant, such
/// as the shared remote storage client and process initialization state.
#[derive(Clone)]
pub struct TenantSharedResources {
    pub broker_client: storage_broker::BrokerClientChannel,
    pub remote_storage: GenericRemoteStorage,
    pub deletion_queue_client: DeletionQueueClient,
    pub l0_flush_global_state: L0FlushGlobalState,
}

/// A [`Tenant`] is really an _attached_ tenant.  The configuration
/// for an attached tenant is a subset of the [`LocationConf`], represented
/// in this struct.
#[derive(Clone)]
pub(super) struct AttachedTenantConf {
    tenant_conf: TenantConfOpt,
    location: AttachedLocationConfig,
    /// The deadline before which we are blocked from GC so that
    /// leases have a chance to be renewed.
    lsn_lease_deadline: Option<tokio::time::Instant>,
}

impl AttachedTenantConf {
    fn new(tenant_conf: TenantConfOpt, location: AttachedLocationConfig) -> Self {
        // Sets a deadline before which we cannot proceed to GC due to lsn lease.
        //
        // We do this as the leases mapping are not persisted to disk. By delaying GC by lease
        // length, we guarantee that all the leases we granted before will have a chance to renew
        // when we run GC for the first time after restart / transition from AttachedMulti to AttachedSingle.
        let lsn_lease_deadline = if location.attach_mode == AttachmentMode::Single {
            Some(
                tokio::time::Instant::now()
                    + tenant_conf
                        .lsn_lease_length
                        .unwrap_or(LsnLease::DEFAULT_LENGTH),
            )
        } else {
            // We don't use `lsn_lease_deadline` to delay GC in AttachedMulti and AttachedStale
            // because we don't do GC in these modes.
            None
        };

        Self {
            tenant_conf,
            location,
            lsn_lease_deadline,
        }
    }

    fn try_from(location_conf: LocationConf) -> anyhow::Result<Self> {
        match &location_conf.mode {
            LocationMode::Attached(attach_conf) => {
                Ok(Self::new(location_conf.tenant_conf, *attach_conf))
            }
            LocationMode::Secondary(_) => {
                anyhow::bail!("Attempted to construct AttachedTenantConf from a LocationConf in secondary mode")
            }
        }
    }

    fn is_gc_blocked_by_lsn_lease_deadline(&self) -> bool {
        self.lsn_lease_deadline
            .map(|d| tokio::time::Instant::now() < d)
            .unwrap_or(false)
    }
}
struct TimelinePreload {
    timeline_id: TimelineId,
    client: RemoteTimelineClient,
    index_part: Result<MaybeDeletedIndexPart, DownloadError>,
}

pub(crate) struct TenantPreload {
    tenant_manifest: TenantManifest,
    /// Map from timeline ID to a possible timeline preload. It is None iff the timeline is offloaded according to the manifest.
    timelines: HashMap<TimelineId, Option<TimelinePreload>>,
}

/// When we spawn a tenant, there is a special mode for tenant creation that
/// avoids trying to read anything from remote storage.
pub(crate) enum SpawnMode {
    /// Activate as soon as possible
    Eager,
    /// Lazy activation in the background, with the option to skip the queue if the need comes up
    Lazy,
}

///
/// Tenant consists of multiple timelines. Keep them in a hash table.
///
pub struct Tenant {
    // Global pageserver config parameters
    pub conf: &'static PageServerConf,

    /// The value creation timestamp, used to measure activation delay, see:
    /// <https://github.com/neondatabase/neon/issues/4025>
    constructed_at: Instant,

    state: watch::Sender<TenantState>,

    // Overridden tenant-specific config parameters.
    // We keep TenantConfOpt sturct here to preserve the information
    // about parameters that are not set.
    // This is necessary to allow global config updates.
    tenant_conf: Arc<ArcSwap<AttachedTenantConf>>,

    tenant_shard_id: TenantShardId,

    // The detailed sharding information, beyond the number/count in tenant_shard_id
    shard_identity: ShardIdentity,

    /// The remote storage generation, used to protect S3 objects from split-brain.
    /// Does not change over the lifetime of the [`Tenant`] object.
    ///
    /// This duplicates the generation stored in LocationConf, but that structure is mutable:
    /// this copy enforces the invariant that generatio doesn't change during a Tenant's lifetime.
    generation: Generation,

    timelines: Mutex<HashMap<TimelineId, Arc<Timeline>>>,

    /// During timeline creation, we first insert the TimelineId to the
    /// creating map, then `timelines`, then remove it from the creating map.
    /// **Lock order**: if acquiring all (or a subset), acquire them in order `timelines`, `timelines_offloaded`, `timelines_creating`
    timelines_creating: std::sync::Mutex<HashSet<TimelineId>>,

    /// Possibly offloaded and archived timelines
    /// **Lock order**: if acquiring all (or a subset), acquire them in order `timelines`, `timelines_offloaded`, `timelines_creating`
    timelines_offloaded: Mutex<HashMap<TimelineId, Arc<OffloadedTimeline>>>,

    /// Serialize writes of the tenant manifest to remote storage.  If there are concurrent operations
    /// affecting the manifest, such as timeline deletion and timeline offload, they must wait for
    /// each other (this could be optimized to coalesce writes if necessary).
    ///
    /// The contents of the Mutex are the last manifest we successfully uploaded
    tenant_manifest_upload: tokio::sync::Mutex<Option<TenantManifest>>,

    // This mutex prevents creation of new timelines during GC.
    // Adding yet another mutex (in addition to `timelines`) is needed because holding
    // `timelines` mutex during all GC iteration
    // may block for a long time `get_timeline`, `get_timelines_state`,... and other operations
    // with timelines, which in turn may cause dropping replication connection, expiration of wait_for_lsn
    // timeout...
    gc_cs: tokio::sync::Mutex<()>,
    walredo_mgr: Option<Arc<WalRedoManager>>,

    // provides access to timeline data sitting in the remote storage
    pub(crate) remote_storage: GenericRemoteStorage,

    // Access to global deletion queue for when this tenant wants to schedule a deletion
    deletion_queue_client: DeletionQueueClient,

    /// Cached logical sizes updated updated on each [`Tenant::gather_size_inputs`].
    cached_logical_sizes: tokio::sync::Mutex<HashMap<(TimelineId, Lsn), u64>>,
    cached_synthetic_tenant_size: Arc<AtomicU64>,

    eviction_task_tenant_state: tokio::sync::Mutex<EvictionTaskTenantState>,

    /// Track repeated failures to compact, so that we can back off.
    /// Overhead of mutex is acceptable because compaction is done with a multi-second period.
    compaction_circuit_breaker: std::sync::Mutex<CircuitBreaker>,

    /// Scheduled gc-compaction tasks.
    scheduled_compaction_tasks: std::sync::Mutex<HashMap<TimelineId, Arc<GcCompactionQueue>>>,

    /// If the tenant is in Activating state, notify this to encourage it
    /// to proceed to Active as soon as possible, rather than waiting for lazy
    /// background warmup.
    pub(crate) activate_now_sem: tokio::sync::Semaphore,

    /// Time it took for the tenant to activate. Zero if not active yet.
    attach_wal_lag_cooldown: Arc<std::sync::OnceLock<WalLagCooldown>>,

    // Cancellation token fires when we have entered shutdown().  This is a parent of
    // Timelines' cancellation token.
    pub(crate) cancel: CancellationToken,

    // Users of the Tenant such as the page service must take this Gate to avoid
    // trying to use a Tenant which is shutting down.
    pub(crate) gate: Gate,

    /// Throttle applied at the top of [`Timeline::get`].
    /// All [`Tenant::timelines`] of a given [`Tenant`] instance share the same [`throttle::Throttle`] instance.
    pub(crate) pagestream_throttle: Arc<throttle::Throttle>,

    pub(crate) pagestream_throttle_metrics: Arc<crate::metrics::tenant_throttling::Pagestream>,

    /// An ongoing timeline detach concurrency limiter.
    ///
    /// As a tenant will likely be restarted as part of timeline detach ancestor it makes no sense
    /// to have two running at the same time. A different one can be started if an earlier one
    /// has failed for whatever reason.
    ongoing_timeline_detach: std::sync::Mutex<Option<(TimelineId, utils::completion::Barrier)>>,

    /// `index_part.json` based gc blocking reason tracking.
    ///
    /// New gc iterations must start a new iteration by acquiring `GcBlock::start` before
    /// proceeding.
    pub(crate) gc_block: gc_block::GcBlock,

    l0_flush_global_state: L0FlushGlobalState,
}
impl std::fmt::Debug for Tenant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.tenant_shard_id, self.current_state())
    }
}

pub(crate) enum WalRedoManager {
    Prod(WalredoManagerId, PostgresRedoManager),
    #[cfg(test)]
    Test(harness::TestRedoManager),
}

#[derive(thiserror::Error, Debug)]
#[error("pageserver is shutting down")]
pub(crate) struct GlobalShutDown;

impl WalRedoManager {
    pub(crate) fn new(mgr: PostgresRedoManager) -> Result<Arc<Self>, GlobalShutDown> {
        let id = WalredoManagerId::next();
        let arc = Arc::new(Self::Prod(id, mgr));
        let mut guard = WALREDO_MANAGERS.lock().unwrap();
        match &mut *guard {
            Some(map) => {
                map.insert(id, Arc::downgrade(&arc));
                Ok(arc)
            }
            None => Err(GlobalShutDown),
        }
    }
}

impl Drop for WalRedoManager {
    fn drop(&mut self) {
        match self {
            Self::Prod(id, _) => {
                let mut guard = WALREDO_MANAGERS.lock().unwrap();
                if let Some(map) = &mut *guard {
                    map.remove(id).expect("new() registers, drop() unregisters");
                }
            }
            #[cfg(test)]
            Self::Test(_) => {
                // Not applicable to test redo manager
            }
        }
    }
}

/// Global registry of all walredo managers so that [`crate::shutdown_pageserver`] can shut down
/// the walredo processes outside of the regular order.
///
/// This is necessary to work around a systemd bug where it freezes if there are
/// walredo processes left => <https://github.com/neondatabase/cloud/issues/11387>
#[allow(clippy::type_complexity)]
pub(crate) static WALREDO_MANAGERS: once_cell::sync::Lazy<
    Mutex<Option<HashMap<WalredoManagerId, Weak<WalRedoManager>>>>,
> = once_cell::sync::Lazy::new(|| Mutex::new(Some(HashMap::new())));
#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub(crate) struct WalredoManagerId(u64);
impl WalredoManagerId {
    pub fn next() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if id == 0 {
            panic!("WalredoManagerId::new() returned 0, indicating wraparound, risking it's no longer unique");
        }
        Self(id)
    }
}

#[cfg(test)]
impl From<harness::TestRedoManager> for WalRedoManager {
    fn from(mgr: harness::TestRedoManager) -> Self {
        Self::Test(mgr)
    }
}

impl WalRedoManager {
    pub(crate) async fn shutdown(&self) -> bool {
        match self {
            Self::Prod(_, mgr) => mgr.shutdown().await,
            #[cfg(test)]
            Self::Test(_) => {
                // Not applicable to test redo manager
                true
            }
        }
    }

    pub(crate) fn maybe_quiesce(&self, idle_timeout: Duration) {
        match self {
            Self::Prod(_, mgr) => mgr.maybe_quiesce(idle_timeout),
            #[cfg(test)]
            Self::Test(_) => {
                // Not applicable to test redo manager
            }
        }
    }

    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    pub async fn request_redo(
        &self,
        key: pageserver_api::key::Key,
        lsn: Lsn,
        base_img: Option<(Lsn, bytes::Bytes)>,
        records: Vec<(Lsn, pageserver_api::record::NeonWalRecord)>,
        pg_version: u32,
    ) -> Result<bytes::Bytes, walredo::Error> {
        match self {
            Self::Prod(_, mgr) => {
                mgr.request_redo(key, lsn, base_img, records, pg_version)
                    .await
            }
            #[cfg(test)]
            Self::Test(mgr) => {
                mgr.request_redo(key, lsn, base_img, records, pg_version)
                    .await
            }
        }
    }

    pub(crate) fn status(&self) -> Option<WalRedoManagerStatus> {
        match self {
            WalRedoManager::Prod(_, m) => Some(m.status()),
            #[cfg(test)]
            WalRedoManager::Test(_) => None,
        }
    }
}

/// A very lightweight memory representation of an offloaded timeline.
///
/// We need to store the list of offloaded timelines so that we can perform operations on them,
/// like unoffloading them, or (at a later date), decide to perform flattening.
/// This type has a much smaller memory impact than [`Timeline`], and thus we can store many
/// more offloaded timelines than we can manage ones that aren't.
pub struct OffloadedTimeline {
    pub tenant_shard_id: TenantShardId,
    pub timeline_id: TimelineId,
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,

    /// When the timeline was archived.
    ///
    /// Present for future flattening deliberations.
    pub archived_at: NaiveDateTime,

    /// Prevent two tasks from deleting the timeline at the same time. If held, the
    /// timeline is being deleted. If 'true', the timeline has already been deleted.
    pub delete_progress: TimelineDeleteProgress,

    /// Part of the `OffloadedTimeline` object's lifecycle: this needs to be set before we drop it
    pub deleted_from_ancestor: AtomicBool,
}

impl OffloadedTimeline {
    /// Obtains an offloaded timeline from a given timeline object.
    ///
    /// Returns `None` if the `archived_at` flag couldn't be obtained, i.e.
    /// the timeline is not in a stopped state.
    /// Panics if the timeline is not archived.
    fn from_timeline(timeline: &Timeline) -> Result<Self, UploadQueueNotReadyError> {
        let (ancestor_retain_lsn, ancestor_timeline_id) =
            if let Some(ancestor_timeline) = timeline.ancestor_timeline() {
                let ancestor_lsn = timeline.get_ancestor_lsn();
                let ancestor_timeline_id = ancestor_timeline.timeline_id;
                let mut gc_info = ancestor_timeline.gc_info.write().unwrap();
                gc_info.insert_child(timeline.timeline_id, ancestor_lsn, MaybeOffloaded::Yes);
                (Some(ancestor_lsn), Some(ancestor_timeline_id))
            } else {
                (None, None)
            };
        let archived_at = timeline
            .remote_client
            .archived_at_stopped_queue()?
            .expect("must be called on an archived timeline");
        Ok(Self {
            tenant_shard_id: timeline.tenant_shard_id,
            timeline_id: timeline.timeline_id,
            ancestor_timeline_id,
            ancestor_retain_lsn,
            archived_at,

            delete_progress: timeline.delete_progress.clone(),
            deleted_from_ancestor: AtomicBool::new(false),
        })
    }
    fn from_manifest(tenant_shard_id: TenantShardId, manifest: &OffloadedTimelineManifest) -> Self {
        // We expect to reach this case in tenant loading, where the `retain_lsn` is populated in the parent's `gc_info`
        // by the `initialize_gc_info` function.
        let OffloadedTimelineManifest {
            timeline_id,
            ancestor_timeline_id,
            ancestor_retain_lsn,
            archived_at,
        } = *manifest;
        Self {
            tenant_shard_id,
            timeline_id,
            ancestor_timeline_id,
            ancestor_retain_lsn,
            archived_at,
            delete_progress: TimelineDeleteProgress::default(),
            deleted_from_ancestor: AtomicBool::new(false),
        }
    }
    fn manifest(&self) -> OffloadedTimelineManifest {
        let Self {
            timeline_id,
            ancestor_timeline_id,
            ancestor_retain_lsn,
            archived_at,
            ..
        } = self;
        OffloadedTimelineManifest {
            timeline_id: *timeline_id,
            ancestor_timeline_id: *ancestor_timeline_id,
            ancestor_retain_lsn: *ancestor_retain_lsn,
            archived_at: *archived_at,
        }
    }
    /// Delete this timeline's retain_lsn from its ancestor, if present in the given tenant
    fn delete_from_ancestor_with_timelines(
        &self,
        timelines: &std::sync::MutexGuard<'_, HashMap<TimelineId, Arc<Timeline>>>,
    ) {
        if let (Some(_retain_lsn), Some(ancestor_timeline_id)) =
            (self.ancestor_retain_lsn, self.ancestor_timeline_id)
        {
            if let Some((_, ancestor_timeline)) = timelines
                .iter()
                .find(|(tid, _tl)| **tid == ancestor_timeline_id)
            {
                let removal_happened = ancestor_timeline
                    .gc_info
                    .write()
                    .unwrap()
                    .remove_child_offloaded(self.timeline_id);
                if !removal_happened {
                    tracing::error!(tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id,
                        "Couldn't remove retain_lsn entry from offloaded timeline's parent: already removed");
                }
            }
        }
        self.deleted_from_ancestor.store(true, Ordering::Release);
    }
    /// Call [`Self::delete_from_ancestor_with_timelines`] instead if possible.
    ///
    /// As the entire tenant is being dropped, don't bother deregistering the `retain_lsn` from the ancestor.
    fn defuse_for_tenant_drop(&self) {
        self.deleted_from_ancestor.store(true, Ordering::Release);
    }
}

impl fmt::Debug for OffloadedTimeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OffloadedTimeline<{}>", self.timeline_id)
    }
}

impl Drop for OffloadedTimeline {
    fn drop(&mut self) {
        if !self.deleted_from_ancestor.load(Ordering::Acquire) {
            tracing::warn!(
                "offloaded timeline {} was dropped without having cleaned it up at the ancestor",
                self.timeline_id
            );
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum MaybeOffloaded {
    Yes,
    No,
}

#[derive(Clone, Debug)]
pub enum TimelineOrOffloaded {
    Timeline(Arc<Timeline>),
    Offloaded(Arc<OffloadedTimeline>),
}

impl TimelineOrOffloaded {
    pub fn arc_ref(&self) -> TimelineOrOffloadedArcRef<'_> {
        match self {
            TimelineOrOffloaded::Timeline(timeline) => {
                TimelineOrOffloadedArcRef::Timeline(timeline)
            }
            TimelineOrOffloaded::Offloaded(offloaded) => {
                TimelineOrOffloadedArcRef::Offloaded(offloaded)
            }
        }
    }
    pub fn tenant_shard_id(&self) -> TenantShardId {
        self.arc_ref().tenant_shard_id()
    }
    pub fn timeline_id(&self) -> TimelineId {
        self.arc_ref().timeline_id()
    }
    pub fn delete_progress(&self) -> &Arc<tokio::sync::Mutex<DeleteTimelineFlow>> {
        match self {
            TimelineOrOffloaded::Timeline(timeline) => &timeline.delete_progress,
            TimelineOrOffloaded::Offloaded(offloaded) => &offloaded.delete_progress,
        }
    }
    fn maybe_remote_client(&self) -> Option<Arc<RemoteTimelineClient>> {
        match self {
            TimelineOrOffloaded::Timeline(timeline) => Some(timeline.remote_client.clone()),
            TimelineOrOffloaded::Offloaded(_offloaded) => None,
        }
    }
}

pub enum TimelineOrOffloadedArcRef<'a> {
    Timeline(&'a Arc<Timeline>),
    Offloaded(&'a Arc<OffloadedTimeline>),
}

impl TimelineOrOffloadedArcRef<'_> {
    pub fn tenant_shard_id(&self) -> TenantShardId {
        match self {
            TimelineOrOffloadedArcRef::Timeline(timeline) => timeline.tenant_shard_id,
            TimelineOrOffloadedArcRef::Offloaded(offloaded) => offloaded.tenant_shard_id,
        }
    }
    pub fn timeline_id(&self) -> TimelineId {
        match self {
            TimelineOrOffloadedArcRef::Timeline(timeline) => timeline.timeline_id,
            TimelineOrOffloadedArcRef::Offloaded(offloaded) => offloaded.timeline_id,
        }
    }
}

impl<'a> From<&'a Arc<Timeline>> for TimelineOrOffloadedArcRef<'a> {
    fn from(timeline: &'a Arc<Timeline>) -> Self {
        Self::Timeline(timeline)
    }
}

impl<'a> From<&'a Arc<OffloadedTimeline>> for TimelineOrOffloadedArcRef<'a> {
    fn from(timeline: &'a Arc<OffloadedTimeline>) -> Self {
        Self::Offloaded(timeline)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GetTimelineError {
    #[error("Timeline is shutting down")]
    ShuttingDown,
    #[error("Timeline {tenant_id}/{timeline_id} is not active, state: {state:?}")]
    NotActive {
        tenant_id: TenantShardId,
        timeline_id: TimelineId,
        state: TimelineState,
    },
    #[error("Timeline {tenant_id}/{timeline_id} was not found")]
    NotFound {
        tenant_id: TenantShardId,
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

    #[error("Cancelled")]
    Cancelled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Debug for DeleteTimelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "NotFound"),
            Self::HasChildren(c) => f.debug_tuple("HasChildren").field(c).finish(),
            Self::AlreadyInProgress(_) => f.debug_tuple("AlreadyInProgress").finish(),
            Self::Cancelled => f.debug_tuple("Cancelled").finish(),
            Self::Other(e) => f.debug_tuple("Other").field(e).finish(),
        }
    }
}

#[derive(thiserror::Error)]
pub enum TimelineArchivalError {
    #[error("NotFound")]
    NotFound,

    #[error("Timeout")]
    Timeout,

    #[error("Cancelled")]
    Cancelled,

    #[error("ancestor is archived: {}", .0)]
    HasArchivedParent(TimelineId),

    #[error("HasUnarchivedChildren")]
    HasUnarchivedChildren(Vec<TimelineId>),

    #[error("Timeline archival is already in progress")]
    AlreadyInProgress,

    #[error(transparent)]
    Other(anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum TenantManifestError {
    #[error("Remote storage error: {0}")]
    RemoteStorage(anyhow::Error),

    #[error("Cancelled")]
    Cancelled,
}

impl From<TenantManifestError> for TimelineArchivalError {
    fn from(e: TenantManifestError) -> Self {
        match e {
            TenantManifestError::RemoteStorage(e) => Self::Other(e),
            TenantManifestError::Cancelled => Self::Cancelled,
        }
    }
}

impl Debug for TimelineArchivalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "NotFound"),
            Self::Timeout => write!(f, "Timeout"),
            Self::Cancelled => write!(f, "Cancelled"),
            Self::HasArchivedParent(p) => f.debug_tuple("HasArchivedParent").field(p).finish(),
            Self::HasUnarchivedChildren(c) => {
                f.debug_tuple("HasUnarchivedChildren").field(c).finish()
            }
            Self::AlreadyInProgress => f.debug_tuple("AlreadyInProgress").finish(),
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

/// Arguments to [`Tenant::create_timeline`].
///
/// Not usable as an idempotency key for timeline creation because if [`CreateTimelineParamsBranch::ancestor_start_lsn`]
/// is `None`, the result of the timeline create call is not deterministic.
///
/// See [`CreateTimelineIdempotency`] for an idempotency key.
#[derive(Debug)]
pub(crate) enum CreateTimelineParams {
    Bootstrap(CreateTimelineParamsBootstrap),
    Branch(CreateTimelineParamsBranch),
    ImportPgdata(CreateTimelineParamsImportPgdata),
}

#[derive(Debug)]
pub(crate) struct CreateTimelineParamsBootstrap {
    pub(crate) new_timeline_id: TimelineId,
    pub(crate) existing_initdb_timeline_id: Option<TimelineId>,
    pub(crate) pg_version: u32,
}

/// NB: See comment on [`CreateTimelineIdempotency::Branch`] for why there's no `pg_version` here.
#[derive(Debug)]
pub(crate) struct CreateTimelineParamsBranch {
    pub(crate) new_timeline_id: TimelineId,
    pub(crate) ancestor_timeline_id: TimelineId,
    pub(crate) ancestor_start_lsn: Option<Lsn>,
}

#[derive(Debug)]
pub(crate) struct CreateTimelineParamsImportPgdata {
    pub(crate) new_timeline_id: TimelineId,
    pub(crate) location: import_pgdata::index_part_format::Location,
    pub(crate) idempotency_key: import_pgdata::index_part_format::IdempotencyKey,
}

/// What is used to determine idempotency of a [`Tenant::create_timeline`] call in  [`Tenant::start_creating_timeline`] in  [`Tenant::start_creating_timeline`].
///
/// Each [`Timeline`] object holds [`Self`] as an immutable property in [`Timeline::create_idempotency`].
///
/// We lower timeline creation requests to [`Self`], and then use [`PartialEq::eq`] to compare [`Timeline::create_idempotency`] with the request.
/// If they are equal, we return a reference to the existing timeline, otherwise it's an idempotency conflict.
///
/// There is special treatment for [`Self::FailWithConflict`] to always return an idempotency conflict.
/// It would be nice to have more advanced derive macros to make that special treatment declarative.
///
/// Notes:
/// - Unlike [`CreateTimelineParams`], ancestor LSN is fixed, so, branching will be at a deterministic LSN.
/// - We make some trade-offs though, e.g., [`CreateTimelineParamsBootstrap::existing_initdb_timeline_id`]
///   is not considered for idempotency. We can improve on this over time if we deem it necessary.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateTimelineIdempotency {
    /// NB: special treatment, see comment in [`Self`].
    FailWithConflict,
    Bootstrap {
        pg_version: u32,
    },
    /// NB: branches always have the same `pg_version` as their ancestor.
    /// While [`pageserver_api::models::TimelineCreateRequestMode::Branch::pg_version`]
    /// exists as a field, and is set by cplane, it has always been ignored by pageserver when
    /// determining the child branch pg_version.
    Branch {
        ancestor_timeline_id: TimelineId,
        ancestor_start_lsn: Lsn,
    },
    ImportPgdata(CreatingTimelineIdempotencyImportPgdata),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreatingTimelineIdempotencyImportPgdata {
    idempotency_key: import_pgdata::index_part_format::IdempotencyKey,
}

/// What is returned by [`Tenant::start_creating_timeline`].
#[must_use]
enum StartCreatingTimelineResult {
    CreateGuard(TimelineCreateGuard),
    Idempotent(Arc<Timeline>),
}

enum TimelineInitAndSyncResult {
    ReadyToActivate(Arc<Timeline>),
    NeedsSpawnImportPgdata(TimelineInitAndSyncNeedsSpawnImportPgdata),
}

impl TimelineInitAndSyncResult {
    fn ready_to_activate(self) -> Option<Arc<Timeline>> {
        match self {
            Self::ReadyToActivate(timeline) => Some(timeline),
            _ => None,
        }
    }
}

#[must_use]
struct TimelineInitAndSyncNeedsSpawnImportPgdata {
    timeline: Arc<Timeline>,
    import_pgdata: import_pgdata::index_part_format::Root,
    guard: TimelineCreateGuard,
}

/// What is returned by [`Tenant::create_timeline`].
enum CreateTimelineResult {
    Created(Arc<Timeline>),
    Idempotent(Arc<Timeline>),
    /// IMPORTANT: This [`Arc<Timeline>`] object is not in [`Tenant::timelines`] when
    /// we return this result, nor will this concrete object ever be added there.
    /// Cf method comment on [`Tenant::create_timeline_import_pgdata`].
    ImportSpawned(Arc<Timeline>),
}

impl CreateTimelineResult {
    fn discriminant(&self) -> &'static str {
        match self {
            Self::Created(_) => "Created",
            Self::Idempotent(_) => "Idempotent",
            Self::ImportSpawned(_) => "ImportSpawned",
        }
    }
    fn timeline(&self) -> &Arc<Timeline> {
        match self {
            Self::Created(t) | Self::Idempotent(t) | Self::ImportSpawned(t) => t,
        }
    }
    /// Unit test timelines aren't activated, test has to do it if it needs to.
    #[cfg(test)]
    fn into_timeline_for_test(self) -> Arc<Timeline> {
        match self {
            Self::Created(t) | Self::Idempotent(t) | Self::ImportSpawned(t) => t,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CreateTimelineError {
    #[error("creation of timeline with the given ID is in progress")]
    AlreadyCreating,
    #[error("timeline already exists with different parameters")]
    Conflict,
    #[error(transparent)]
    AncestorLsn(anyhow::Error),
    #[error("ancestor timeline is not active")]
    AncestorNotActive,
    #[error("ancestor timeline is archived")]
    AncestorArchived,
    #[error("tenant shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum InitdbError {
    #[error("Operation was cancelled")]
    Cancelled,
    #[error(transparent)]
    Other(anyhow::Error),
    #[error(transparent)]
    Inner(postgres_initdb::Error),
}

enum CreateTimelineCause {
    Load,
    Delete,
}

enum LoadTimelineCause {
    Attach,
    Unoffload,
    ImportPgdata {
        create_guard: TimelineCreateGuard,
        activate: ActivateTimelineArgs,
    },
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum GcError {
    // The tenant is shutting down
    #[error("tenant shutting down")]
    TenantCancelled,

    // The tenant is shutting down
    #[error("timeline shutting down")]
    TimelineCancelled,

    // The tenant is in a state inelegible to run GC
    #[error("not active")]
    NotActive,

    // A requested GC cutoff LSN was invalid, for example it tried to move backwards
    #[error("not active")]
    BadLsn { why: String },

    // A remote storage error while scheduling updates after compaction
    #[error(transparent)]
    Remote(anyhow::Error),

    // An error reading while calculating GC cutoffs
    #[error(transparent)]
    GcCutoffs(PageReconstructError),

    // If GC was invoked for a particular timeline, this error means it didn't exist
    #[error("timeline not found")]
    TimelineNotFound,
}

impl From<PageReconstructError> for GcError {
    fn from(value: PageReconstructError) -> Self {
        match value {
            PageReconstructError::Cancelled => Self::TimelineCancelled,
            other => Self::GcCutoffs(other),
        }
    }
}

impl From<NotInitialized> for GcError {
    fn from(value: NotInitialized) -> Self {
        match value {
            NotInitialized::Uninitialized => GcError::Remote(value.into()),
            NotInitialized::Stopped | NotInitialized::ShuttingDown => GcError::TimelineCancelled,
        }
    }
}

impl From<timeline::layer_manager::Shutdown> for GcError {
    fn from(_: timeline::layer_manager::Shutdown) -> Self {
        GcError::TimelineCancelled
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum LoadConfigError {
    #[error("TOML deserialization error: '{0}'")]
    DeserializeToml(#[from] toml_edit::de::Error),

    #[error("Config not found at {0}")]
    NotFound(Utf8PathBuf),
}

impl Tenant {
    /// Yet another helper for timeline initialization.
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
        self: &Arc<Self>,
        timeline_id: TimelineId,
        resources: TimelineResources,
        mut index_part: IndexPart,
        metadata: TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        cause: LoadTimelineCause,
        ctx: &RequestContext,
    ) -> anyhow::Result<TimelineInitAndSyncResult> {
        let tenant_id = self.tenant_shard_id;

        let import_pgdata = index_part.import_pgdata.take();
        let idempotency = match &import_pgdata {
            Some(import_pgdata) => {
                CreateTimelineIdempotency::ImportPgdata(CreatingTimelineIdempotencyImportPgdata {
                    idempotency_key: import_pgdata.idempotency_key().clone(),
                })
            }
            None => {
                if metadata.ancestor_timeline().is_none() {
                    CreateTimelineIdempotency::Bootstrap {
                        pg_version: metadata.pg_version(),
                    }
                } else {
                    CreateTimelineIdempotency::Branch {
                        ancestor_timeline_id: metadata.ancestor_timeline().unwrap(),
                        ancestor_start_lsn: metadata.ancestor_lsn(),
                    }
                }
            }
        };

        let timeline = self.create_timeline_struct(
            timeline_id,
            &metadata,
            ancestor.clone(),
            resources,
            CreateTimelineCause::Load,
            idempotency.clone(),
            index_part.l2_lsn,
        )?;
        let disk_consistent_lsn = timeline.get_disk_consistent_lsn();
        anyhow::ensure!(
            disk_consistent_lsn.is_valid(),
            "Timeline {tenant_id}/{timeline_id} has invalid disk_consistent_lsn"
        );
        assert_eq!(
            disk_consistent_lsn,
            metadata.disk_consistent_lsn(),
            "these are used interchangeably"
        );

        timeline.remote_client.init_upload_queue(&index_part)?;

        timeline
            .load_layer_map(disk_consistent_lsn, index_part)
            .await
            .with_context(|| {
                format!("Failed to load layermap for timeline {tenant_id}/{timeline_id}")
            })?;

        match import_pgdata {
            Some(import_pgdata) if !import_pgdata.is_done() => {
                match cause {
                    LoadTimelineCause::Attach | LoadTimelineCause::Unoffload => (),
                    LoadTimelineCause::ImportPgdata { .. } => {
                        unreachable!("ImportPgdata should not be reloading timeline import is done and persisted as such in s3")
                    }
                }
                let mut guard = self.timelines_creating.lock().unwrap();
                if !guard.insert(timeline_id) {
                    // We should never try and load the same timeline twice during startup
                    unreachable!("Timeline {tenant_id}/{timeline_id} is already being created")
                }
                let timeline_create_guard = TimelineCreateGuard {
                    _tenant_gate_guard: self.gate.enter()?,
                    owning_tenant: self.clone(),
                    timeline_id,
                    idempotency,
                    // The users of this specific return value don't need the timline_path in there.
                    timeline_path: timeline
                        .conf
                        .timeline_path(&timeline.tenant_shard_id, &timeline.timeline_id),
                };
                Ok(TimelineInitAndSyncResult::NeedsSpawnImportPgdata(
                    TimelineInitAndSyncNeedsSpawnImportPgdata {
                        timeline,
                        import_pgdata,
                        guard: timeline_create_guard,
                    },
                ))
            }
            Some(_) | None => {
                {
                    let mut timelines_accessor = self.timelines.lock().unwrap();
                    match timelines_accessor.entry(timeline_id) {
                        // We should never try and load the same timeline twice during startup
                        Entry::Occupied(_) => {
                            unreachable!(
                            "Timeline {tenant_id}/{timeline_id} already exists in the tenant map"
                        );
                        }
                        Entry::Vacant(v) => {
                            v.insert(Arc::clone(&timeline));
                            timeline.maybe_spawn_flush_loop();
                        }
                    }
                }

                // Sanity check: a timeline should have some content.
                anyhow::ensure!(
                    ancestor.is_some()
                        || timeline
                            .layers
                            .read()
                            .await
                            .layer_map()
                            .expect("currently loading, layer manager cannot be shutdown already")
                            .iter_historic_layers()
                            .next()
                            .is_some(),
                    "Timeline has no ancestor and no layer files"
                );

                match cause {
                    LoadTimelineCause::Attach | LoadTimelineCause::Unoffload => (),
                    LoadTimelineCause::ImportPgdata {
                        create_guard,
                        activate,
                    } => {
                        // TODO: see the comment in the task code above how I'm not so certain
                        // it is safe to activate here because of concurrent shutdowns.
                        match activate {
                            ActivateTimelineArgs::Yes { broker_client } => {
                                info!("activating timeline after reload from pgdata import task");
                                timeline.activate(self.clone(), broker_client, None, ctx);
                            }
                            ActivateTimelineArgs::No => (),
                        }
                        drop(create_guard);
                    }
                }

                Ok(TimelineInitAndSyncResult::ReadyToActivate(timeline))
            }
        }
    }

    /// Attach a tenant that's available in cloud storage.
    ///
    /// This returns quickly, after just creating the in-memory object
    /// Tenant struct and launching a background task to download
    /// the remote index files.  On return, the tenant is most likely still in
    /// Attaching state, and it will become Active once the background task
    /// finishes. You can use wait_until_active() to wait for the task to
    /// complete.
    ///
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        resources: TenantSharedResources,
        attached_conf: AttachedTenantConf,
        shard_identity: ShardIdentity,
        init_order: Option<InitializationOrder>,
        mode: SpawnMode,
        ctx: &RequestContext,
    ) -> Result<Arc<Tenant>, GlobalShutDown> {
        let wal_redo_manager =
            WalRedoManager::new(PostgresRedoManager::new(conf, tenant_shard_id))?;

        let TenantSharedResources {
            broker_client,
            remote_storage,
            deletion_queue_client,
            l0_flush_global_state,
        } = resources;

        let attach_mode = attached_conf.location.attach_mode;
        let generation = attached_conf.location.generation;

        let tenant = Arc::new(Tenant::new(
            TenantState::Attaching,
            conf,
            attached_conf,
            shard_identity,
            Some(wal_redo_manager),
            tenant_shard_id,
            remote_storage.clone(),
            deletion_queue_client,
            l0_flush_global_state,
        ));

        // The attach task will carry a GateGuard, so that shutdown() reliably waits for it to drop out if
        // we shut down while attaching.
        let attach_gate_guard = tenant
            .gate
            .enter()
            .expect("We just created the Tenant: nothing else can have shut it down yet");

        // Do all the hard work in the background
        let tenant_clone = Arc::clone(&tenant);
        let ctx = ctx.detached_child(TaskKind::Attach, DownloadBehavior::Warn);
        task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::Attach,
            tenant_shard_id,
            None,
            "attach tenant",
            async move {

                info!(
                    ?attach_mode,
                    "Attaching tenant"
                );

                let _gate_guard = attach_gate_guard;

                // Is this tenant being spawned as part of process startup?
                let starting_up = init_order.is_some();
                scopeguard::defer! {
                    if starting_up {
                        TENANT.startup_complete.inc();
                    }
                }

                // Ideally we should use Tenant::set_broken_no_wait, but it is not supposed to be used when tenant is in loading state.
                enum BrokenVerbosity {
                    Error,
                    Info
                }
                let make_broken =
                    |t: &Tenant, err: anyhow::Error, verbosity: BrokenVerbosity| {
                        match verbosity {
                            BrokenVerbosity::Info => {
                                info!("attach cancelled, setting tenant state to Broken: {err}");
                            },
                            BrokenVerbosity::Error => {
                                error!("attach failed, setting tenant state to Broken: {err:?}");
                            }
                        }
                        t.state.send_modify(|state| {
                            // The Stopping case is for when we have passed control on to DeleteTenantFlow:
                            // if it errors, we will call make_broken when tenant is already in Stopping.
                            assert!(
                                matches!(*state, TenantState::Attaching | TenantState::Stopping { .. }),
                                "the attach task owns the tenant state until activation is complete"
                            );

                            *state = TenantState::broken_from_reason(err.to_string());
                        });
                    };

                // TODO: should also be rejecting tenant conf changes that violate this check.
                if let Err(e) = crate::tenant::storage_layer::inmemory_layer::IndexEntry::validate_checkpoint_distance(tenant_clone.get_checkpoint_distance()) {
                    make_broken(&tenant_clone, anyhow::anyhow!(e), BrokenVerbosity::Error);
                    return Ok(());
                }

                let mut init_order = init_order;
                // take the completion because initial tenant loading will complete when all of
                // these tasks complete.
                let _completion = init_order
                    .as_mut()
                    .and_then(|x| x.initial_tenant_load.take());
                let remote_load_completion = init_order
                    .as_mut()
                    .and_then(|x| x.initial_tenant_load_remote.take());

                enum AttachType<'a> {
                    /// We are attaching this tenant lazily in the background.
                    Warmup {
                        _permit: tokio::sync::SemaphorePermit<'a>,
                        during_startup: bool
                    },
                    /// We are attaching this tenant as soon as we can, because for example an
                    /// endpoint tried to access it.
                    OnDemand,
                    /// During normal operations after startup, we are attaching a tenant, and
                    /// eager attach was requested.
                    Normal,
                }

                let attach_type = if matches!(mode, SpawnMode::Lazy) {
                    // Before doing any I/O, wait for at least one of:
                    // - A client attempting to access to this tenant (on-demand loading)
                    // - A permit becoming available in the warmup semaphore (background warmup)

                    tokio::select!(
                        permit = tenant_clone.activate_now_sem.acquire() => {
                            let _ = permit.expect("activate_now_sem is never closed");
                            tracing::info!("Activating tenant (on-demand)");
                            AttachType::OnDemand
                        },
                        permit = conf.concurrent_tenant_warmup.inner().acquire() => {
                            let _permit = permit.expect("concurrent_tenant_warmup semaphore is never closed");
                            tracing::info!("Activating tenant (warmup)");
                            AttachType::Warmup {
                                _permit,
                                during_startup: init_order.is_some()
                            }
                        }
                        _ = tenant_clone.cancel.cancelled() => {
                            // This is safe, but should be pretty rare: it is interesting if a tenant
                            // stayed in Activating for such a long time that shutdown found it in
                            // that state.
                            tracing::info!(state=%tenant_clone.current_state(), "Tenant shut down before activation");
                            // Make the tenant broken so that set_stopping will not hang waiting for it to leave
                            // the Attaching state.  This is an over-reaction (nothing really broke, the tenant is
                            // just shutting down), but ensures progress.
                            make_broken(&tenant_clone, anyhow::anyhow!("Shut down while Attaching"), BrokenVerbosity::Info);
                            return Ok(());
                        },
                    )
                } else {
                    // SpawnMode::{Create,Eager} always cause jumping ahead of the
                    // concurrent_tenant_warmup queue
                    AttachType::Normal
                };

                let preload = match &mode {
                    SpawnMode::Eager | SpawnMode::Lazy => {
                        let _preload_timer = TENANT.preload.start_timer();
                        let res = tenant_clone
                            .preload(&remote_storage, task_mgr::shutdown_token())
                            .await;
                        match res {
                            Ok(p) => Some(p),
                            Err(e) => {
                                make_broken(&tenant_clone, anyhow::anyhow!(e), BrokenVerbosity::Error);
                                return Ok(());
                            }
                        }
                    }

                };

                // Remote preload is complete.
                drop(remote_load_completion);


                // We will time the duration of the attach phase unless this is a creation (attach will do no work)
                let attach_start = std::time::Instant::now();
                let attached = {
                    let _attach_timer = Some(TENANT.attach.start_timer());
                    tenant_clone.attach(preload, &ctx).await
                };
                let attach_duration = attach_start.elapsed();
                _ = tenant_clone.attach_wal_lag_cooldown.set(WalLagCooldown::new(attach_start, attach_duration));

                match attached {
                    Ok(()) => {
                        info!("attach finished, activating");
                        tenant_clone.activate(broker_client, None, &ctx);
                    }
                    Err(e) => {
                        make_broken(&tenant_clone, anyhow::anyhow!(e), BrokenVerbosity::Error);
                    }
                }

                // If we are doing an opportunistic warmup attachment at startup, initialize
                // logical size at the same time.  This is better than starting a bunch of idle tenants
                // with cold caches and then coming back later to initialize their logical sizes.
                //
                // It also prevents the warmup proccess competing with the concurrency limit on
                // logical size calculations: if logical size calculation semaphore is saturated,
                // then warmup will wait for that before proceeding to the next tenant.
                if matches!(attach_type, AttachType::Warmup { during_startup: true, .. }) {
                    let mut futs: FuturesUnordered<_> = tenant_clone.timelines.lock().unwrap().values().cloned().map(|t| t.await_initial_logical_size()).collect();
                    tracing::info!("Waiting for initial logical sizes while warming up...");
                    while futs.next().await.is_some() {}
                    tracing::info!("Warm-up complete");
                }

                Ok(())
            }
            .instrument(tracing::info_span!(parent: None, "attach", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), gen=?generation)),
        );
        Ok(tenant)
    }

    #[instrument(skip_all)]
    pub(crate) async fn preload(
        self: &Arc<Self>,
        remote_storage: &GenericRemoteStorage,
        cancel: CancellationToken,
    ) -> anyhow::Result<TenantPreload> {
        span::debug_assert_current_span_has_tenant_id();
        // Get list of remote timelines
        // download index files for every tenant timeline
        info!("listing remote timelines");
        let (mut remote_timeline_ids, other_keys) = remote_timeline_client::list_remote_timelines(
            remote_storage,
            self.tenant_shard_id,
            cancel.clone(),
        )
        .await?;
        let (offloaded_add, tenant_manifest) =
            match remote_timeline_client::download_tenant_manifest(
                remote_storage,
                &self.tenant_shard_id,
                self.generation,
                &cancel,
            )
            .await
            {
                Ok((tenant_manifest, _generation, _manifest_mtime)) => (
                    format!("{} offloaded", tenant_manifest.offloaded_timelines.len()),
                    tenant_manifest,
                ),
                Err(DownloadError::NotFound) => {
                    ("no manifest".to_string(), TenantManifest::empty())
                }
                Err(e) => Err(e)?,
            };

        info!(
            "found {} timelines, and {offloaded_add}",
            remote_timeline_ids.len()
        );

        for k in other_keys {
            warn!("Unexpected non timeline key {k}");
        }

        // Avoid downloading IndexPart of offloaded timelines.
        let mut offloaded_with_prefix = HashSet::new();
        for offloaded in tenant_manifest.offloaded_timelines.iter() {
            if remote_timeline_ids.remove(&offloaded.timeline_id) {
                offloaded_with_prefix.insert(offloaded.timeline_id);
            } else {
                // We'll take care later of timelines in the manifest without a prefix
            }
        }

        let timelines = self
            .load_timelines_metadata(remote_timeline_ids, remote_storage, cancel)
            .await?;

        Ok(TenantPreload {
            tenant_manifest,
            timelines: timelines
                .into_iter()
                .map(|(id, tl)| (id, Some(tl)))
                .chain(offloaded_with_prefix.into_iter().map(|id| (id, None)))
                .collect(),
        })
    }

    ///
    /// Background task that downloads all data for a tenant and brings it to Active state.
    ///
    /// No background tasks are started as part of this routine.
    ///
    async fn attach(
        self: &Arc<Tenant>,
        preload: Option<TenantPreload>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        span::debug_assert_current_span_has_tenant_id();

        failpoint_support::sleep_millis_async!("before-attaching-tenant");

        let Some(preload) = preload else {
            anyhow::bail!("local-only deployment is no longer supported, https://github.com/neondatabase/neon/issues/5624");
        };

        let mut offloaded_timeline_ids = HashSet::new();
        let mut offloaded_timelines_list = Vec::new();
        for timeline_manifest in preload.tenant_manifest.offloaded_timelines.iter() {
            let timeline_id = timeline_manifest.timeline_id;
            let offloaded_timeline =
                OffloadedTimeline::from_manifest(self.tenant_shard_id, timeline_manifest);
            offloaded_timelines_list.push((timeline_id, Arc::new(offloaded_timeline)));
            offloaded_timeline_ids.insert(timeline_id);
        }
        // Complete deletions for offloaded timeline id's from manifest.
        // The manifest will be uploaded later in this function.
        offloaded_timelines_list
            .retain(|(offloaded_id, offloaded)| {
                // Existence of a timeline is finally determined by the existence of an index-part.json in remote storage.
                // If there is dangling references in another location, they need to be cleaned up.
                let delete = !preload.timelines.contains_key(offloaded_id);
                if delete {
                    tracing::info!("Removing offloaded timeline {offloaded_id} from manifest as no remote prefix was found");
                    offloaded.defuse_for_tenant_drop();
                }
                !delete
        });

        let mut timelines_to_resume_deletions = vec![];

        let mut remote_index_and_client = HashMap::new();
        let mut timeline_ancestors = HashMap::new();
        let mut existent_timelines = HashSet::new();
        for (timeline_id, preload) in preload.timelines {
            let Some(preload) = preload else { continue };
            // This is an invariant of the `preload` function's API
            assert!(!offloaded_timeline_ids.contains(&timeline_id));
            let index_part = match preload.index_part {
                Ok(i) => {
                    debug!("remote index part exists for timeline {timeline_id}");
                    // We found index_part on the remote, this is the standard case.
                    existent_timelines.insert(timeline_id);
                    i
                }
                Err(DownloadError::NotFound) => {
                    // There is no index_part on the remote. We only get here
                    // if there is some prefix for the timeline in the remote storage.
                    // This can e.g. be the initdb.tar.zst archive, maybe a
                    // remnant from a prior incomplete creation or deletion attempt.
                    // Delete the local directory as the deciding criterion for a
                    // timeline's existence is presence of index_part.
                    info!(%timeline_id, "index_part not found on remote");
                    continue;
                }
                Err(DownloadError::Fatal(why)) => {
                    // If, while loading one remote timeline, we saw an indication that our generation
                    // number is likely invalid, then we should not load the whole tenant.
                    error!(%timeline_id, "Fatal error loading timeline: {why}");
                    anyhow::bail!(why.to_string());
                }
                Err(e) => {
                    // Some (possibly ephemeral) error happened during index_part download.
                    // Pretend the timeline exists to not delete the timeline directory,
                    // as it might be a temporary issue and we don't want to re-download
                    // everything after it resolves.
                    warn!(%timeline_id, "Failed to load index_part from remote storage, failed creation? ({e})");

                    existent_timelines.insert(timeline_id);
                    continue;
                }
            };
            match index_part {
                MaybeDeletedIndexPart::IndexPart(index_part) => {
                    timeline_ancestors.insert(timeline_id, index_part.metadata.clone());
                    remote_index_and_client.insert(timeline_id, (index_part, preload.client));
                }
                MaybeDeletedIndexPart::Deleted(index_part) => {
                    info!(
                        "timeline {} is deleted, picking to resume deletion",
                        timeline_id
                    );
                    timelines_to_resume_deletions.push((timeline_id, index_part, preload.client));
                }
            }
        }

        let mut gc_blocks = HashMap::new();

        // For every timeline, download the metadata file, scan the local directory,
        // and build a layer map that contains an entry for each remote and local
        // layer file.
        let sorted_timelines = tree_sort_timelines(timeline_ancestors, |m| m.ancestor_timeline())?;
        for (timeline_id, remote_metadata) in sorted_timelines {
            let (index_part, remote_client) = remote_index_and_client
                .remove(&timeline_id)
                .expect("just put it in above");

            if let Some(blocking) = index_part.gc_blocking.as_ref() {
                // could just filter these away, but it helps while testing
                anyhow::ensure!(
                    !blocking.reasons.is_empty(),
                    "index_part for {timeline_id} is malformed: it should not have gc blocking with zero reasons"
                );
                let prev = gc_blocks.insert(timeline_id, blocking.reasons);
                assert!(prev.is_none());
            }

            // TODO again handle early failure
            let effect = self
                .load_remote_timeline(
                    timeline_id,
                    index_part,
                    remote_metadata,
                    TimelineResources {
                        remote_client,
                        pagestream_throttle: self.pagestream_throttle.clone(),
                        pagestream_throttle_metrics: self.pagestream_throttle_metrics.clone(),
                        l0_flush_global_state: self.l0_flush_global_state.clone(),
                    },
                    LoadTimelineCause::Attach,
                    ctx,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to load remote timeline {} for tenant {}",
                        timeline_id, self.tenant_shard_id
                    )
                })?;

            match effect {
                TimelineInitAndSyncResult::ReadyToActivate(_) => {
                    // activation happens later, on Tenant::activate
                }
                TimelineInitAndSyncResult::NeedsSpawnImportPgdata(
                    TimelineInitAndSyncNeedsSpawnImportPgdata {
                        timeline,
                        import_pgdata,
                        guard,
                    },
                ) => {
                    tokio::task::spawn(self.clone().create_timeline_import_pgdata_task(
                        timeline,
                        import_pgdata,
                        ActivateTimelineArgs::No,
                        guard,
                    ));
                }
            }
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
                remote_timeline_client,
            )
            .instrument(tracing::info_span!("timeline_delete", %timeline_id))
            .await
            .context("resume_deletion")
            .map_err(LoadLocalTimelineError::ResumeDeletion)?;
        }
        let needs_manifest_upload =
            offloaded_timelines_list.len() != preload.tenant_manifest.offloaded_timelines.len();
        {
            let mut offloaded_timelines_accessor = self.timelines_offloaded.lock().unwrap();
            offloaded_timelines_accessor.extend(offloaded_timelines_list.into_iter());
        }
        if needs_manifest_upload {
            self.store_tenant_manifest().await?;
        }

        // The local filesystem contents are a cache of what's in the remote IndexPart;
        // IndexPart is the source of truth.
        self.clean_up_timelines(&existent_timelines)?;

        self.gc_block.set_scanned(gc_blocks);

        fail::fail_point!("attach-before-activate", |_| {
            anyhow::bail!("attach-before-activate");
        });
        failpoint_support::sleep_millis_async!("attach-before-activate-sleep", &self.cancel);

        info!("Done");

        Ok(())
    }

    /// Check for any local timeline directories that are temporary, or do not correspond to a
    /// timeline that still exists: this can happen if we crashed during a deletion/creation, or
    /// if a timeline was deleted while the tenant was attached to a different pageserver.
    fn clean_up_timelines(&self, existent_timelines: &HashSet<TimelineId>) -> anyhow::Result<()> {
        let timelines_dir = self.conf.timelines_path(&self.tenant_shard_id);

        let entries = match timelines_dir.read_dir_utf8() {
            Ok(d) => d,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(());
                } else {
                    return Err(e).context("list timelines directory for tenant");
                }
            }
        };

        for entry in entries {
            let entry = entry.context("read timeline dir entry")?;
            let entry_path = entry.path();

            let purge = if crate::is_temporary(entry_path)
                // TODO: remove uninit mark code (https://github.com/neondatabase/neon/issues/5718)
                || is_uninit_mark(entry_path)
                || crate::is_delete_mark(entry_path)
            {
                true
            } else {
                match TimelineId::try_from(entry_path.file_name()) {
                    Ok(i) => {
                        // Purge if the timeline ID does not exist in remote storage: remote storage is the authority.
                        !existent_timelines.contains(&i)
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Unparseable directory in timelines directory: {entry_path}, ignoring ({e})"
                        );
                        // Do not purge junk: if we don't recognize it, be cautious and leave it for a human.
                        false
                    }
                }
            };

            if purge {
                tracing::info!("Purging stale timeline dentry {entry_path}");
                if let Err(e) = match entry.file_type() {
                    Ok(t) => if t.is_dir() {
                        std::fs::remove_dir_all(entry_path)
                    } else {
                        std::fs::remove_file(entry_path)
                    }
                    .or_else(fs_ext::ignore_not_found),
                    Err(e) => Err(e),
                } {
                    tracing::warn!("Failed to purge stale timeline dentry {entry_path}: {e}");
                }
            }
        }

        Ok(())
    }

    /// Get sum of all remote timelines sizes
    ///
    /// This function relies on the index_part instead of listing the remote storage
    pub fn remote_size(&self) -> u64 {
        let mut size = 0;

        for timeline in self.list_timelines() {
            size += timeline.remote_client.get_remote_physical_size();
        }

        size
    }

    #[instrument(skip_all, fields(timeline_id=%timeline_id))]
    async fn load_remote_timeline(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        index_part: IndexPart,
        remote_metadata: TimelineMetadata,
        resources: TimelineResources,
        cause: LoadTimelineCause,
        ctx: &RequestContext,
    ) -> anyhow::Result<TimelineInitAndSyncResult> {
        span::debug_assert_current_span_has_tenant_id();

        info!("downloading index file for timeline {}", timeline_id);
        tokio::fs::create_dir_all(self.conf.timeline_path(&self.tenant_shard_id, &timeline_id))
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

        self.timeline_init_and_sync(
            timeline_id,
            resources,
            index_part,
            remote_metadata,
            ancestor,
            cause,
            ctx,
        )
        .await
    }

    async fn load_timelines_metadata(
        self: &Arc<Tenant>,
        timeline_ids: HashSet<TimelineId>,
        remote_storage: &GenericRemoteStorage,
        cancel: CancellationToken,
    ) -> anyhow::Result<HashMap<TimelineId, TimelinePreload>> {
        let mut part_downloads = JoinSet::new();
        for timeline_id in timeline_ids {
            let cancel_clone = cancel.clone();
            part_downloads.spawn(
                self.load_timeline_metadata(timeline_id, remote_storage.clone(), cancel_clone)
                    .instrument(info_span!("download_index_part", %timeline_id)),
            );
        }

        let mut timeline_preloads: HashMap<TimelineId, TimelinePreload> = HashMap::new();

        loop {
            tokio::select!(
                next = part_downloads.join_next() => {
                    match next {
                        Some(result) => {
                            let preload = result.context("join preload task")?;
                            timeline_preloads.insert(preload.timeline_id, preload);
                        },
                        None => {
                            break;
                        }
                    }
                },
                _ = cancel.cancelled() => {
                    anyhow::bail!("Cancelled while waiting for remote index download")
                }
            )
        }

        Ok(timeline_preloads)
    }

    fn build_timeline_client(
        &self,
        timeline_id: TimelineId,
        remote_storage: GenericRemoteStorage,
    ) -> RemoteTimelineClient {
        RemoteTimelineClient::new(
            remote_storage.clone(),
            self.deletion_queue_client.clone(),
            self.conf,
            self.tenant_shard_id,
            timeline_id,
            self.generation,
            &self.tenant_conf.load().location,
        )
    }

    fn load_timeline_metadata(
        self: &Arc<Tenant>,
        timeline_id: TimelineId,
        remote_storage: GenericRemoteStorage,
        cancel: CancellationToken,
    ) -> impl Future<Output = TimelinePreload> {
        let client = self.build_timeline_client(timeline_id, remote_storage);
        async move {
            debug_assert_current_span_has_tenant_and_timeline_id();
            debug!("starting index part download");

            let index_part = client.download_index_file(&cancel).await;

            debug!("finished index part download");

            TimelinePreload {
                client,
                timeline_id,
                index_part,
            }
        }
    }

    fn check_to_be_archived_has_no_unarchived_children(
        timeline_id: TimelineId,
        timelines: &std::sync::MutexGuard<'_, HashMap<TimelineId, Arc<Timeline>>>,
    ) -> Result<(), TimelineArchivalError> {
        let children: Vec<TimelineId> = timelines
            .iter()
            .filter_map(|(id, entry)| {
                if entry.get_ancestor_timeline_id() != Some(timeline_id) {
                    return None;
                }
                if entry.is_archived() == Some(true) {
                    return None;
                }
                Some(*id)
            })
            .collect();

        if !children.is_empty() {
            return Err(TimelineArchivalError::HasUnarchivedChildren(children));
        }
        Ok(())
    }

    fn check_ancestor_of_to_be_unarchived_is_not_archived(
        ancestor_timeline_id: TimelineId,
        timelines: &std::sync::MutexGuard<'_, HashMap<TimelineId, Arc<Timeline>>>,
        offloaded_timelines: &std::sync::MutexGuard<
            '_,
            HashMap<TimelineId, Arc<OffloadedTimeline>>,
        >,
    ) -> Result<(), TimelineArchivalError> {
        let has_archived_parent =
            if let Some(ancestor_timeline) = timelines.get(&ancestor_timeline_id) {
                ancestor_timeline.is_archived() == Some(true)
            } else if offloaded_timelines.contains_key(&ancestor_timeline_id) {
                true
            } else {
                error!("ancestor timeline {ancestor_timeline_id} not found");
                if cfg!(debug_assertions) {
                    panic!("ancestor timeline {ancestor_timeline_id} not found");
                }
                return Err(TimelineArchivalError::NotFound);
            };
        if has_archived_parent {
            return Err(TimelineArchivalError::HasArchivedParent(
                ancestor_timeline_id,
            ));
        }
        Ok(())
    }

    fn check_to_be_unarchived_timeline_has_no_archived_parent(
        timeline: &Arc<Timeline>,
    ) -> Result<(), TimelineArchivalError> {
        if let Some(ancestor_timeline) = timeline.ancestor_timeline() {
            if ancestor_timeline.is_archived() == Some(true) {
                return Err(TimelineArchivalError::HasArchivedParent(
                    ancestor_timeline.timeline_id,
                ));
            }
        }
        Ok(())
    }

    /// Loads the specified (offloaded) timeline from S3 and attaches it as a loaded timeline
    ///
    /// Counterpart to [`offload_timeline`].
    async fn unoffload_timeline(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: RequestContext,
    ) -> Result<Arc<Timeline>, TimelineArchivalError> {
        info!("unoffloading timeline");

        // We activate the timeline below manually, so this must be called on an active tenant.
        // We expect callers of this function to ensure this.
        match self.current_state() {
            TenantState::Activating { .. }
            | TenantState::Attaching
            | TenantState::Broken { .. } => {
                panic!("Timeline expected to be active")
            }
            TenantState::Stopping { .. } => return Err(TimelineArchivalError::Cancelled),
            TenantState::Active => {}
        }
        let cancel = self.cancel.clone();

        // Protect against concurrent attempts to use this TimelineId
        // We don't care much about idempotency, as it's ensured a layer above.
        let allow_offloaded = true;
        let _create_guard = self
            .create_timeline_create_guard(
                timeline_id,
                CreateTimelineIdempotency::FailWithConflict,
                allow_offloaded,
            )
            .map_err(|err| match err {
                TimelineExclusionError::AlreadyCreating => TimelineArchivalError::AlreadyInProgress,
                TimelineExclusionError::AlreadyExists { .. } => {
                    TimelineArchivalError::Other(anyhow::anyhow!("Timeline already exists"))
                }
                TimelineExclusionError::Other(e) => TimelineArchivalError::Other(e),
                TimelineExclusionError::ShuttingDown => TimelineArchivalError::Cancelled,
            })?;

        let timeline_preload = self
            .load_timeline_metadata(timeline_id, self.remote_storage.clone(), cancel.clone())
            .await;

        let index_part = match timeline_preload.index_part {
            Ok(index_part) => {
                debug!("remote index part exists for timeline {timeline_id}");
                index_part
            }
            Err(DownloadError::NotFound) => {
                error!(%timeline_id, "index_part not found on remote");
                return Err(TimelineArchivalError::NotFound);
            }
            Err(DownloadError::Cancelled) => return Err(TimelineArchivalError::Cancelled),
            Err(e) => {
                // Some (possibly ephemeral) error happened during index_part download.
                warn!(%timeline_id, "Failed to load index_part from remote storage, failed creation? ({e})");
                return Err(TimelineArchivalError::Other(
                    anyhow::Error::new(e).context("downloading index_part from remote storage"),
                ));
            }
        };
        let index_part = match index_part {
            MaybeDeletedIndexPart::IndexPart(index_part) => index_part,
            MaybeDeletedIndexPart::Deleted(_index_part) => {
                info!("timeline is deleted according to index_part.json");
                return Err(TimelineArchivalError::NotFound);
            }
        };
        let remote_metadata = index_part.metadata.clone();
        let timeline_resources = self.build_timeline_resources(timeline_id);
        self.load_remote_timeline(
            timeline_id,
            index_part,
            remote_metadata,
            timeline_resources,
            LoadTimelineCause::Unoffload,
            &ctx,
        )
        .await
        .with_context(|| {
            format!(
                "failed to load remote timeline {} for tenant {}",
                timeline_id, self.tenant_shard_id
            )
        })
        .map_err(TimelineArchivalError::Other)?;

        let timeline = {
            let timelines = self.timelines.lock().unwrap();
            let Some(timeline) = timelines.get(&timeline_id) else {
                warn!("timeline not available directly after attach");
                // This is not a panic because no locks are held between `load_remote_timeline`
                // which puts the timeline into timelines, and our look into the timeline map.
                return Err(TimelineArchivalError::Other(anyhow::anyhow!(
                    "timeline not available directly after attach"
                )));
            };
            let mut offloaded_timelines = self.timelines_offloaded.lock().unwrap();
            match offloaded_timelines.remove(&timeline_id) {
                Some(offloaded) => {
                    offloaded.delete_from_ancestor_with_timelines(&timelines);
                }
                None => warn!("timeline already removed from offloaded timelines"),
            }

            self.initialize_gc_info(&timelines, &offloaded_timelines, Some(timeline_id));

            Arc::clone(timeline)
        };

        // Upload new list of offloaded timelines to S3
        self.store_tenant_manifest().await?;

        // Activate the timeline (if it makes sense)
        if !(timeline.is_broken() || timeline.is_stopping()) {
            let background_jobs_can_start = None;
            timeline.activate(
                self.clone(),
                broker_client.clone(),
                background_jobs_can_start,
                &ctx,
            );
        }

        info!("timeline unoffloading complete");
        Ok(timeline)
    }

    pub(crate) async fn apply_timeline_archival_config(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        new_state: TimelineArchivalState,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: RequestContext,
    ) -> Result<(), TimelineArchivalError> {
        info!("setting timeline archival config");
        // First part: figure out what is needed to do, and do validation
        let timeline_or_unarchive_offloaded = 'outer: {
            let timelines = self.timelines.lock().unwrap();

            let Some(timeline) = timelines.get(&timeline_id) else {
                let offloaded_timelines = self.timelines_offloaded.lock().unwrap();
                let Some(offloaded) = offloaded_timelines.get(&timeline_id) else {
                    return Err(TimelineArchivalError::NotFound);
                };
                if new_state == TimelineArchivalState::Archived {
                    // It's offloaded already, so nothing to do
                    return Ok(());
                }
                if let Some(ancestor_timeline_id) = offloaded.ancestor_timeline_id {
                    Self::check_ancestor_of_to_be_unarchived_is_not_archived(
                        ancestor_timeline_id,
                        &timelines,
                        &offloaded_timelines,
                    )?;
                }
                break 'outer None;
            };

            // Do some validation. We release the timelines lock below, so there is potential
            // for race conditions: these checks are more present to prevent misunderstandings of
            // the API's capabilities, instead of serving as the sole way to defend their invariants.
            match new_state {
                TimelineArchivalState::Unarchived => {
                    Self::check_to_be_unarchived_timeline_has_no_archived_parent(timeline)?
                }
                TimelineArchivalState::Archived => {
                    Self::check_to_be_archived_has_no_unarchived_children(timeline_id, &timelines)?
                }
            }
            Some(Arc::clone(timeline))
        };

        // Second part: unoffload timeline (if needed)
        let timeline = if let Some(timeline) = timeline_or_unarchive_offloaded {
            timeline
        } else {
            // Turn offloaded timeline into a non-offloaded one
            self.unoffload_timeline(timeline_id, broker_client, ctx)
                .await?
        };

        // Third part: upload new timeline archival state and block until it is present in S3
        let upload_needed = match timeline
            .remote_client
            .schedule_index_upload_for_timeline_archival_state(new_state)
        {
            Ok(upload_needed) => upload_needed,
            Err(e) => {
                if timeline.cancel.is_cancelled() {
                    return Err(TimelineArchivalError::Cancelled);
                } else {
                    return Err(TimelineArchivalError::Other(e));
                }
            }
        };

        if upload_needed {
            info!("Uploading new state");
            const MAX_WAIT: Duration = Duration::from_secs(10);
            let Ok(v) =
                tokio::time::timeout(MAX_WAIT, timeline.remote_client.wait_completion()).await
            else {
                tracing::warn!("reached timeout for waiting on upload queue");
                return Err(TimelineArchivalError::Timeout);
            };
            v.map_err(|e| match e {
                WaitCompletionError::NotInitialized(e) => {
                    TimelineArchivalError::Other(anyhow::anyhow!(e))
                }
                WaitCompletionError::UploadQueueShutDownOrStopped => {
                    TimelineArchivalError::Cancelled
                }
            })?;
        }
        Ok(())
    }

    pub fn get_offloaded_timeline(
        &self,
        timeline_id: TimelineId,
    ) -> Result<Arc<OffloadedTimeline>, GetTimelineError> {
        self.timelines_offloaded
            .lock()
            .unwrap()
            .get(&timeline_id)
            .map(Arc::clone)
            .ok_or(GetTimelineError::NotFound {
                tenant_id: self.tenant_shard_id,
                timeline_id,
            })
    }

    pub(crate) fn tenant_shard_id(&self) -> TenantShardId {
        self.tenant_shard_id
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
                tenant_id: self.tenant_shard_id,
                timeline_id,
            })?;

        if active_only && !timeline.is_active() {
            Err(GetTimelineError::NotActive {
                tenant_id: self.tenant_shard_id,
                timeline_id,
                state: timeline.current_state(),
            })
        } else {
            Ok(Arc::clone(timeline))
        }
    }

    /// Lists timelines the tenant contains.
    /// It's up to callers to omit certain timelines that are not considered ready for use.
    pub fn list_timelines(&self) -> Vec<Arc<Timeline>> {
        self.timelines
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect()
    }

    /// Lists timelines the tenant manages, including offloaded ones.
    ///
    /// It's up to callers to omit certain timelines that are not considered ready for use.
    pub fn list_timelines_and_offloaded(
        &self,
    ) -> (Vec<Arc<Timeline>>, Vec<Arc<OffloadedTimeline>>) {
        let timelines = self
            .timelines
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect();
        let offloaded = self
            .timelines_offloaded
            .lock()
            .unwrap()
            .values()
            .map(Arc::clone)
            .collect();
        (timelines, offloaded)
    }

    pub fn list_timeline_ids(&self) -> Vec<TimelineId> {
        self.timelines.lock().unwrap().keys().cloned().collect()
    }

    /// This is used by tests & import-from-basebackup.
    ///
    /// The returned [`UninitializedTimeline`] contains no data nor metadata and it is in
    /// a state that will fail [`Tenant::load_remote_timeline`] because `disk_consistent_lsn=Lsn(0)`.
    ///
    /// The caller is responsible for getting the timeline into a state that will be accepted
    /// by [`Tenant::load_remote_timeline`] / [`Tenant::attach`].
    /// Then they may call [`UninitializedTimeline::finish_creation`] to add the timeline
    /// to the [`Tenant::timelines`].
    ///
    /// Tests should use `Tenant::create_test_timeline` to set up the minimum required metadata keys.
    pub(crate) async fn create_empty_timeline(
        self: &Arc<Self>,
        new_timeline_id: TimelineId,
        initdb_lsn: Lsn,
        pg_version: u32,
        _ctx: &RequestContext,
    ) -> anyhow::Result<UninitializedTimeline> {
        anyhow::ensure!(
            self.is_active(),
            "Cannot create empty timelines on inactive tenant"
        );

        // Protect against concurrent attempts to use this TimelineId
        let create_guard = match self
            .start_creating_timeline(new_timeline_id, CreateTimelineIdempotency::FailWithConflict)
            .await?
        {
            StartCreatingTimelineResult::CreateGuard(guard) => guard,
            StartCreatingTimelineResult::Idempotent(_) => {
                unreachable!("FailWithConflict implies we get an error instead")
            }
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
            create_guard,
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
        self: &Arc<Self>,
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
        tline.remote_client.wait_completion().await.unwrap();

        let tl = uninit_tl.finish_creation()?;
        // The non-test code would call tl.activate() here.
        tl.set_state(TimelineState::Active);
        Ok(tl)
    }

    /// Helper for unit tests to create a timeline with some pre-loaded states.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub async fn create_test_timeline_with_layers(
        self: &Arc<Self>,
        new_timeline_id: TimelineId,
        initdb_lsn: Lsn,
        pg_version: u32,
        ctx: &RequestContext,
        delta_layer_desc: Vec<timeline::DeltaLayerTestDesc>,
        image_layer_desc: Vec<(Lsn, Vec<(pageserver_api::key::Key, bytes::Bytes)>)>,
        end_lsn: Lsn,
    ) -> anyhow::Result<Arc<Timeline>> {
        use checks::check_valid_layermap;
        use itertools::Itertools;

        let tline = self
            .create_test_timeline(new_timeline_id, initdb_lsn, pg_version, ctx)
            .await?;
        tline.force_advance_lsn(end_lsn);
        for deltas in delta_layer_desc {
            tline
                .force_create_delta_layer(deltas, Some(initdb_lsn), ctx)
                .await?;
        }
        for (lsn, images) in image_layer_desc {
            tline
                .force_create_image_layer(lsn, images, Some(initdb_lsn), ctx)
                .await?;
        }
        let layer_names = tline
            .layers
            .read()
            .await
            .layer_map()
            .unwrap()
            .iter_historic_layers()
            .map(|layer| layer.layer_name())
            .collect_vec();
        if let Some(err) = check_valid_layermap(&layer_names) {
            bail!("invalid layermap: {err}");
        }
        Ok(tline)
    }

    /// Create a new timeline.
    ///
    /// Returns the new timeline ID and reference to its Timeline object.
    ///
    /// If the caller specified the timeline ID to use (`new_timeline_id`), and timeline with
    /// the same timeline ID already exists, returns CreateTimelineError::AlreadyExists.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_timeline(
        self: &Arc<Tenant>,
        params: CreateTimelineParams,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> Result<Arc<Timeline>, CreateTimelineError> {
        if !self.is_active() {
            if matches!(self.current_state(), TenantState::Stopping { .. }) {
                return Err(CreateTimelineError::ShuttingDown);
            } else {
                return Err(CreateTimelineError::Other(anyhow::anyhow!(
                    "Cannot create timelines on inactive tenant"
                )));
            }
        }

        let _gate = self
            .gate
            .enter()
            .map_err(|_| CreateTimelineError::ShuttingDown)?;

        let result: CreateTimelineResult = match params {
            CreateTimelineParams::Bootstrap(CreateTimelineParamsBootstrap {
                new_timeline_id,
                existing_initdb_timeline_id,
                pg_version,
            }) => {
                self.bootstrap_timeline(
                    new_timeline_id,
                    pg_version,
                    existing_initdb_timeline_id,
                    ctx,
                )
                .await?
            }
            CreateTimelineParams::Branch(CreateTimelineParamsBranch {
                new_timeline_id,
                ancestor_timeline_id,
                mut ancestor_start_lsn,
            }) => {
                let ancestor_timeline = self
                    .get_timeline(ancestor_timeline_id, false)
                    .context("Cannot branch off the timeline that's not present in pageserver")?;

                // instead of waiting around, just deny the request because ancestor is not yet
                // ready for other purposes either.
                if !ancestor_timeline.is_active() {
                    return Err(CreateTimelineError::AncestorNotActive);
                }

                if ancestor_timeline.is_archived() == Some(true) {
                    info!("tried to branch archived timeline");
                    return Err(CreateTimelineError::AncestorArchived);
                }

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
                    ancestor_timeline
                        .wait_lsn(*lsn, timeline::WaitLsnWaiter::Tenant, ctx)
                        .await
                        .map_err(|e| match e {
                            e @ (WaitLsnError::Timeout(_) | WaitLsnError::BadState { .. }) => {
                                CreateTimelineError::AncestorLsn(anyhow::anyhow!(e))
                            }
                            WaitLsnError::Shutdown => CreateTimelineError::ShuttingDown,
                        })?;
                }

                self.branch_timeline(&ancestor_timeline, new_timeline_id, ancestor_start_lsn, ctx)
                    .await?
            }
            CreateTimelineParams::ImportPgdata(params) => {
                self.create_timeline_import_pgdata(
                    params,
                    ActivateTimelineArgs::Yes {
                        broker_client: broker_client.clone(),
                    },
                    ctx,
                )
                .await?
            }
        };

        // At this point we have dropped our guard on [`Self::timelines_creating`], and
        // the timeline is visible in [`Self::timelines`], but it is _not_ durable yet.  We must
        // not send a success to the caller until it is.  The same applies to idempotent retries.
        //
        // TODO: the timeline is already visible in [`Self::timelines`]; a caller could incorrectly
        // assume that, because they can see the timeline via API, that the creation is done and
        // that it is durable. Ideally, we would keep the timeline hidden (in [`Self::timelines_creating`])
        // until it is durable, e.g., by extending the time we hold the creation guard. This also
        // interacts with UninitializedTimeline and is generally a bit tricky.
        //
        // To re-emphasize: the only correct way to create a timeline is to repeat calling the
        // creation API until it returns success. Only then is durability guaranteed.
        info!(creation_result=%result.discriminant(), "waiting for timeline to be durable");
        result
            .timeline()
            .remote_client
            .wait_completion()
            .await
            .map_err(|e| match e {
                WaitCompletionError::NotInitialized(
                    e, // If the queue is already stopped, it's a shutdown error.
                ) if e.is_stopping() => CreateTimelineError::ShuttingDown,
                WaitCompletionError::NotInitialized(_) => {
                    // This is a bug: we should never try to wait for uploads before initializing the timeline
                    debug_assert!(false);
                    CreateTimelineError::Other(anyhow::anyhow!("timeline not initialized"))
                }
                WaitCompletionError::UploadQueueShutDownOrStopped => {
                    CreateTimelineError::ShuttingDown
                }
            })?;

        // The creating task is responsible for activating the timeline.
        // We do this after `wait_completion()` so that we don't spin up tasks that start
        // doing stuff before the IndexPart is durable in S3, which is done by the previous section.
        let activated_timeline = match result {
            CreateTimelineResult::Created(timeline) => {
                timeline.activate(self.clone(), broker_client, None, ctx);
                timeline
            }
            CreateTimelineResult::Idempotent(timeline) => {
                info!(
                    "request was deemed idempotent, activation will be done by the creating task"
                );
                timeline
            }
            CreateTimelineResult::ImportSpawned(timeline) => {
                info!("import task spawned, timeline will become visible and activated once the import is done");
                timeline
            }
        };

        Ok(activated_timeline)
    }

    /// The returned [`Arc<Timeline>`] is NOT in the [`Tenant::timelines`] map until the import
    /// completes in the background. A DIFFERENT [`Arc<Timeline>`] will be inserted into the
    /// [`Tenant::timelines`] map when the import completes.
    /// We only return an [`Arc<Timeline>`] here so the API handler can create a [`pageserver_api::models::TimelineInfo`]
    /// for the response.
    async fn create_timeline_import_pgdata(
        self: &Arc<Tenant>,
        params: CreateTimelineParamsImportPgdata,
        activate: ActivateTimelineArgs,
        ctx: &RequestContext,
    ) -> Result<CreateTimelineResult, CreateTimelineError> {
        let CreateTimelineParamsImportPgdata {
            new_timeline_id,
            location,
            idempotency_key,
        } = params;

        let started_at = chrono::Utc::now().naive_utc();

        //
        // There's probably a simpler way to upload an index part, but, remote_timeline_client
        // is the canonical way we do it.
        // - create an empty timeline in-memory
        // - use its remote_timeline_client to do the upload
        // - dispose of the uninit timeline
        // - keep the creation guard alive

        let timeline_create_guard = match self
            .start_creating_timeline(
                new_timeline_id,
                CreateTimelineIdempotency::ImportPgdata(CreatingTimelineIdempotencyImportPgdata {
                    idempotency_key: idempotency_key.clone(),
                }),
            )
            .await?
        {
            StartCreatingTimelineResult::CreateGuard(guard) => guard,
            StartCreatingTimelineResult::Idempotent(timeline) => {
                return Ok(CreateTimelineResult::Idempotent(timeline))
            }
        };

        let mut uninit_timeline = {
            let this = &self;
            let initdb_lsn = Lsn(0);
            let _ctx = ctx;
            async move {
                let new_metadata = TimelineMetadata::new(
                    // Initialize disk_consistent LSN to 0, The caller must import some data to
                    // make it valid, before calling finish_creation()
                    Lsn(0),
                    None,
                    None,
                    Lsn(0),
                    initdb_lsn,
                    initdb_lsn,
                    15,
                );
                this.prepare_new_timeline(
                    new_timeline_id,
                    &new_metadata,
                    timeline_create_guard,
                    initdb_lsn,
                    None,
                )
                .await
            }
        }
        .await?;

        let in_progress = import_pgdata::index_part_format::InProgress {
            idempotency_key,
            location,
            started_at,
        };
        let index_part = import_pgdata::index_part_format::Root::V1(
            import_pgdata::index_part_format::V1::InProgress(in_progress),
        );
        uninit_timeline
            .raw_timeline()
            .unwrap()
            .remote_client
            .schedule_index_upload_for_import_pgdata_state_update(Some(index_part.clone()))?;

        // wait_completion happens in caller

        let (timeline, timeline_create_guard) = uninit_timeline.finish_creation_myself();

        tokio::spawn(self.clone().create_timeline_import_pgdata_task(
            timeline.clone(),
            index_part,
            activate,
            timeline_create_guard,
        ));

        // NB: the timeline doesn't exist in self.timelines at this point
        Ok(CreateTimelineResult::ImportSpawned(timeline))
    }

    #[instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), timeline_id=%timeline.timeline_id))]
    async fn create_timeline_import_pgdata_task(
        self: Arc<Tenant>,
        timeline: Arc<Timeline>,
        index_part: import_pgdata::index_part_format::Root,
        activate: ActivateTimelineArgs,
        timeline_create_guard: TimelineCreateGuard,
    ) {
        debug_assert_current_span_has_tenant_and_timeline_id();
        info!("starting");
        scopeguard::defer! {info!("exiting")};

        let res = self
            .create_timeline_import_pgdata_task_impl(
                timeline,
                index_part,
                activate,
                timeline_create_guard,
            )
            .await;
        if let Err(err) = &res {
            error!(?err, "task failed");
            // TODO sleep & retry, sensitive to tenant shutdown
            // TODO: allow timeline deletion requests => should cancel the task
        }
    }

    async fn create_timeline_import_pgdata_task_impl(
        self: Arc<Tenant>,
        timeline: Arc<Timeline>,
        index_part: import_pgdata::index_part_format::Root,
        activate: ActivateTimelineArgs,
        timeline_create_guard: TimelineCreateGuard,
    ) -> Result<(), anyhow::Error> {
        let ctx = RequestContext::new(TaskKind::ImportPgdata, DownloadBehavior::Warn);

        info!("importing pgdata");
        import_pgdata::doit(&timeline, index_part, &ctx, self.cancel.clone())
            .await
            .context("import")?;
        info!("import done");

        //
        // Reload timeline from remote.
        // This proves that the remote state is attachable, and it reuses the code.
        //
        // TODO: think about whether this is safe to do with concurrent Tenant::shutdown.
        // timeline_create_guard hols the tenant gate open, so, shutdown cannot _complete_ until we exit.
        // But our activate() call might launch new background tasks after Tenant::shutdown
        // already went past shutting down the Tenant::timelines, which this timeline here is no part of.
        // I think the same problem exists with the bootstrap & branch mgmt API tasks (tenant shutting
        // down while bootstrapping/branching + activating), but, the race condition is much more likely
        // to manifest because of the long runtime of this import task.

        //        in theory this shouldn't even .await anything except for coop yield
        info!("shutting down timeline");
        timeline.shutdown(ShutdownMode::Hard).await;
        info!("timeline shut down, reloading from remote");
        // TODO: we can't do the following check because create_timeline_import_pgdata must return an Arc<Timeline>
        // let Some(timeline) = Arc::into_inner(timeline) else {
        //     anyhow::bail!("implementation error: timeline that we shut down was still referenced from somewhere");
        // };
        let timeline_id = timeline.timeline_id;

        // load from object storage like Tenant::attach does
        let resources = self.build_timeline_resources(timeline_id);
        let index_part = resources
            .remote_client
            .download_index_file(&self.cancel)
            .await?;
        let index_part = match index_part {
            MaybeDeletedIndexPart::Deleted(_) => {
                // likely concurrent delete call, cplane should prevent this
                anyhow::bail!("index part says deleted but we are not done creating yet, this should not happen but")
            }
            MaybeDeletedIndexPart::IndexPart(p) => p,
        };
        let metadata = index_part.metadata.clone();
        self
            .load_remote_timeline(timeline_id, index_part, metadata, resources, LoadTimelineCause::ImportPgdata{
                create_guard: timeline_create_guard, activate, }, &ctx)
            .await?
            .ready_to_activate()
            .context("implementation error: reloaded timeline still needs import after import reported success")?;

        anyhow::Ok(())
    }

    pub(crate) async fn delete_timeline(
        self: Arc<Self>,
        timeline_id: TimelineId,
    ) -> Result<(), DeleteTimelineError> {
        DeleteTimelineFlow::run(&self, timeline_id).await?;

        Ok(())
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
    pub(crate) async fn gc_iteration(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<GcResult, GcError> {
        // Don't start doing work during shutdown
        if let TenantState::Stopping { .. } = self.current_state() {
            return Ok(GcResult::default());
        }

        // there is a global allowed_error for this
        if !self.is_active() {
            return Err(GcError::NotActive);
        }

        {
            let conf = self.tenant_conf.load();

            // If we may not delete layers, then simply skip GC.  Even though a tenant
            // in AttachedMulti state could do GC and just enqueue the blocked deletions,
            // the only advantage to doing it is to perhaps shrink the LayerMap metadata
            // a bit sooner than we would achieve by waiting for AttachedSingle status.
            if !conf.location.may_delete_layers_hint() {
                info!("Skipping GC in location state {:?}", conf.location);
                return Ok(GcResult::default());
            }

            if conf.is_gc_blocked_by_lsn_lease_deadline() {
                info!("Skipping GC because lsn lease deadline is not reached");
                return Ok(GcResult::default());
            }
        }

        let _guard = match self.gc_block.start().await {
            Ok(guard) => guard,
            Err(reasons) => {
                info!("Skipping GC: {reasons}");
                return Ok(GcResult::default());
            }
        };

        self.gc_iteration_internal(target_timeline_id, horizon, pitr, cancel, ctx)
            .await
    }

    /// Perform one compaction iteration.
    /// This function is periodically called by compactor task.
    /// Also it can be explicitly requested per timeline through page server
    /// api's 'compact' command.
    ///
    /// Returns whether we have pending compaction task.
    async fn compaction_iteration(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<bool, timeline::CompactionError> {
        // Don't start doing work during shutdown, or when broken, we do not need those in the logs
        if !self.is_active() {
            return Ok(false);
        }

        {
            let conf = self.tenant_conf.load();

            // Note that compaction usually requires deletions, but we don't respect
            // may_delete_layers_hint here: that is because tenants in AttachedMulti
            // should proceed with compaction even if they can't do deletion, to avoid
            // accumulating dangerously deep stacks of L0 layers.  Deletions will be
            // enqueued inside RemoteTimelineClient, and executed layer if/when we transition
            // to AttachedSingle state.
            if !conf.location.may_upload_layers_hint() {
                info!("Skipping compaction in location state {:?}", conf.location);
                return Ok(false);
            }
        }

        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // compactions.  We don't want to block everything else while the
        // compaction runs.
        let timelines_to_compact_or_offload;
        {
            let timelines = self.timelines.lock().unwrap();
            timelines_to_compact_or_offload = timelines
                .iter()
                .filter_map(|(timeline_id, timeline)| {
                    let (is_active, (can_offload, _)) =
                        (timeline.is_active(), timeline.can_offload());
                    let has_no_unoffloaded_children = {
                        !timelines
                            .iter()
                            .any(|(_id, tl)| tl.get_ancestor_timeline_id() == Some(*timeline_id))
                    };
                    let config_allows_offload = self.conf.timeline_offloading
                        || self
                            .tenant_conf
                            .load()
                            .tenant_conf
                            .timeline_offloading
                            .unwrap_or_default();
                    let can_offload =
                        can_offload && has_no_unoffloaded_children && config_allows_offload;
                    if (is_active, can_offload) == (false, false) {
                        None
                    } else {
                        Some((*timeline_id, timeline.clone(), (is_active, can_offload)))
                    }
                })
                .collect::<Vec<_>>();
            drop(timelines);
        }

        // Before doing any I/O work, check our circuit breaker
        if self.compaction_circuit_breaker.lock().unwrap().is_broken() {
            info!("Skipping compaction due to previous failures");
            return Ok(false);
        }

        let mut has_pending_task = false;

        for (timeline_id, timeline, (can_compact, can_offload)) in &timelines_to_compact_or_offload
        {
            // pending_task_left == None: cannot compact, maybe still pending tasks
            // pending_task_left == Some(true): compaction task left
            // pending_task_left == Some(false): no compaction task left
            let pending_task_left = if *can_compact {
                let has_pending_l0_compaction_task = timeline
                    .compact(cancel, EnumSet::empty(), ctx)
                    .instrument(info_span!("compact_timeline", %timeline_id))
                    .await
                    .inspect_err(|e| match e {
                        timeline::CompactionError::ShuttingDown => (),
                        timeline::CompactionError::Offload(_) => {
                            // Failures to offload timelines do not trip the circuit breaker, because
                            // they do not do lots of writes the way compaction itself does: it is cheap
                            // to retry, and it would be bad to stop all compaction because of an issue with offloading.
                        }
                        timeline::CompactionError::Other(e) => {
                            self.compaction_circuit_breaker
                                .lock()
                                .unwrap()
                                .fail(&CIRCUIT_BREAKERS_BROKEN, e);
                        }
                    })?;
                if has_pending_l0_compaction_task {
                    Some(true)
                } else {
                    let queue = {
                        let guard = self.scheduled_compaction_tasks.lock().unwrap();
                        guard.get(timeline_id).cloned()
                    };
                    if let Some(queue) = queue {
                        let has_pending_tasks = queue
                            .iteration(cancel, ctx, &self.gc_block, timeline)
                            .await?;
                        Some(has_pending_tasks)
                    } else {
                        Some(false)
                    }
                }
            } else {
                None
            };
            has_pending_task |= pending_task_left.unwrap_or(false);
            if pending_task_left == Some(false) && *can_offload {
                pausable_failpoint!("before-timeline-auto-offload");
                match offload_timeline(self, timeline)
                    .instrument(info_span!("offload_timeline", %timeline_id))
                    .await
                {
                    Err(OffloadError::NotArchived) => {
                        // Ignore this, we likely raced with unarchival
                        Ok(())
                    }
                    other => other,
                }?;
            }
        }

        self.compaction_circuit_breaker
            .lock()
            .unwrap()
            .success(&CIRCUIT_BREAKERS_UNBROKEN);

        Ok(has_pending_task)
    }

    /// Cancel scheduled compaction tasks
    pub(crate) fn cancel_scheduled_compaction(&self, timeline_id: TimelineId) {
        let mut guard = self.scheduled_compaction_tasks.lock().unwrap();
        if let Some(q) = guard.get_mut(&timeline_id) {
            q.cancel_scheduled();
        }
    }

    pub(crate) fn get_scheduled_compaction_tasks(
        &self,
        timeline_id: TimelineId,
    ) -> Vec<CompactInfoResponse> {
        let res = {
            let guard = self.scheduled_compaction_tasks.lock().unwrap();
            guard.get(&timeline_id).map(|q| q.remaining_jobs())
        };
        let Some((running, remaining)) = res else {
            return Vec::new();
        };
        let mut result = Vec::new();
        if let Some((id, running)) = running {
            result.extend(running.into_compact_info_resp(id, true));
        }
        for (id, job) in remaining {
            result.extend(job.into_compact_info_resp(id, false));
        }
        result
    }

    /// Schedule a compaction task for a timeline.
    pub(crate) async fn schedule_compaction(
        &self,
        timeline_id: TimelineId,
        options: CompactOptions,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<()>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut guard = self.scheduled_compaction_tasks.lock().unwrap();
        let q = guard
            .entry(timeline_id)
            .or_insert_with(|| Arc::new(GcCompactionQueue::new()));
        q.schedule_manual_compaction(options, Some(tx));
        Ok(rx)
    }

    // Call through to all timelines to freeze ephemeral layers if needed.  Usually
    // this happens during ingest: this background housekeeping is for freezing layers
    // that are open but haven't been written to for some time.
    async fn ingest_housekeeping(&self) {
        // Scan through the hashmap and collect a list of all the timelines,
        // while holding the lock. Then drop the lock and actually perform the
        // compactions.  We don't want to block everything else while the
        // compaction runs.
        let timelines = {
            self.timelines
                .lock()
                .unwrap()
                .values()
                .filter_map(|timeline| {
                    if timeline.is_active() {
                        Some(timeline.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        for timeline in &timelines {
            timeline.maybe_freeze_ephemeral_layer().await;
        }
    }

    pub fn timeline_has_no_attached_children(&self, timeline_id: TimelineId) -> bool {
        let timelines = self.timelines.lock().unwrap();
        !timelines
            .iter()
            .any(|(_id, tl)| tl.get_ancestor_timeline_id() == Some(timeline_id))
    }

    pub fn current_state(&self) -> TenantState {
        self.state.borrow().clone()
    }

    pub fn is_active(&self) -> bool {
        self.current_state() == TenantState::Active
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub(crate) fn wal_redo_manager_status(&self) -> Option<WalRedoManagerStatus> {
        self.walredo_mgr.as_ref().and_then(|mgr| mgr.status())
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
                TenantState::Attaching => {
                    *current_state = TenantState::Activating(ActivatingFrom::Attaching);
                }
            }
            debug!(tenant_id = %self.tenant_shard_id.tenant_id, shard_id = %self.tenant_shard_id.shard_slug(), "Activating tenant");
            activating = true;
            // Continue outside the closure. We need to grab timelines.lock()
            // and we plan to turn it into a tokio::sync::Mutex in a future patch.
        });

        if activating {
            let timelines_accessor = self.timelines.lock().unwrap();
            let timelines_offloaded_accessor = self.timelines_offloaded.lock().unwrap();
            let timelines_to_activate = timelines_accessor
                .values()
                .filter(|timeline| !(timeline.is_broken() || timeline.is_stopping()));

            // Before activation, populate each Timeline's GcInfo with information about its children
            self.initialize_gc_info(&timelines_accessor, &timelines_offloaded_accessor, None);

            // Spawn gc and compaction loops. The loops will shut themselves
            // down when they notice that the tenant is inactive.
            tasks::start_background_loops(self, background_jobs_can_start);

            let mut activated_timelines = 0;

            for timeline in timelines_to_activate {
                timeline.activate(
                    self.clone(),
                    broker_client.clone(),
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

                let elapsed = self.constructed_at.elapsed();
                let total_timelines = timelines_accessor.len();

                // log a lot of stuff, because some tenants sometimes suffer from user-visible
                // times to activate. see https://github.com/neondatabase/neon/issues/4025
                info!(
                    since_creation_millis = elapsed.as_millis(),
                    tenant_id = %self.tenant_shard_id.tenant_id,
                    shard_id = %self.tenant_shard_id.shard_slug(),
                    activated_timelines,
                    total_timelines,
                    post_state = <&'static str>::from(&*current_state),
                    "activation attempt finished"
                );

                TENANT.activation.observe(elapsed.as_secs_f64());
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
        shutdown_mode: timeline::ShutdownMode,
    ) -> Result<(), completion::Barrier> {
        span::debug_assert_current_span_has_tenant_id();

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

        // If we're still attaching, fire the cancellation token early to drop out: this
        // will prevent us flushing, but ensures timely shutdown if some I/O during attach
        // is very slow.
        let shutdown_mode = if matches!(self.current_state(), TenantState::Attaching) {
            self.cancel.cancel();

            // Having fired our cancellation token, do not try and flush timelines: their cancellation tokens
            // are children of ours, so their flush loops will have shut down already
            timeline::ShutdownMode::Hard
        } else {
            shutdown_mode
        };

        match self.set_stopping(shutdown_progress, false, false).await {
            Ok(()) => {}
            Err(SetStoppingError::Broken) => {
                // assume that this is acceptable
            }
            Err(SetStoppingError::AlreadyStopping(other)) => {
                // give caller the option to wait for this this shutdown
                info!("Tenant::shutdown: AlreadyStopping");
                return Err(other);
            }
        };

        let mut js = tokio::task::JoinSet::new();
        {
            let timelines = self.timelines.lock().unwrap();
            timelines.values().for_each(|timeline| {
                let timeline = Arc::clone(timeline);
                let timeline_id = timeline.timeline_id;
                let span = tracing::info_span!("timeline_shutdown", %timeline_id, ?shutdown_mode);
                js.spawn(async move { timeline.shutdown(shutdown_mode).instrument(span).await });
            });
        }
        {
            let timelines_offloaded = self.timelines_offloaded.lock().unwrap();
            timelines_offloaded.values().for_each(|timeline| {
                timeline.defuse_for_tenant_drop();
            });
        }
        // test_long_timeline_create_then_tenant_delete is leaning on this message
        tracing::info!("Waiting for timelines...");
        while let Some(res) = js.join_next().await {
            match res {
                Ok(()) => {}
                Err(je) if je.is_cancelled() => unreachable!("no cancelling used"),
                Err(je) if je.is_panic() => { /* logged already */ }
                Err(je) => warn!("unexpected JoinError: {je:?}"),
            }
        }

        if let ShutdownMode::Reload = shutdown_mode {
            tracing::info!("Flushing deletion queue");
            if let Err(e) = self.deletion_queue_client.flush().await {
                match e {
                    DeletionQueueError::ShuttingDown => {
                        // This is the only error we expect for now. In the future, if more error
                        // variants are added, we should handle them here.
                    }
                }
            }
        }

        // We cancel the Tenant's cancellation token _after_ the timelines have all shut down.  This permits
        // them to continue to do work during their shutdown methods, e.g. flushing data.
        tracing::debug!("Cancelling CancellationToken");
        self.cancel.cancel();

        // shutdown all tenant and timeline tasks: gc, compaction, page service
        // No new tasks will be started for this tenant because it's in `Stopping` state.
        //
        // this will additionally shutdown and await all timeline tasks.
        tracing::debug!("Waiting for tasks...");
        task_mgr::shutdown_tasks(None, Some(self.tenant_shard_id), None).await;

        if let Some(walredo_mgr) = self.walredo_mgr.as_ref() {
            walredo_mgr.shutdown().await;
        }

        // Wait for any in-flight operations to complete
        self.gate.close().await;

        remove_tenant_metrics(&self.tenant_shard_id);

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
        _allow_transition_from_loading: bool,
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
            TenantState::Activating(_) | TenantState::Attaching => {
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
                TenantState::Activating(_) | TenantState::Attaching => {
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

    /// The activate_now semaphore is initialized with zero units.  As soon as
    /// we add a unit, waiters will be able to acquire a unit and proceed.
    pub(crate) fn activate_now(&self) {
        self.activate_now_sem.add_permits(1);
    }

    pub(crate) async fn wait_to_become_active(
        &self,
        timeout: Duration,
    ) -> Result<(), GetActiveTenantError> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = receiver.borrow_and_update().clone();
            match current_state {
                TenantState::Attaching | TenantState::Activating(_) => {
                    // in these states, there's a chance that we can reach ::Active
                    self.activate_now();
                    match timeout_cancellable(timeout, &self.cancel, receiver.changed()).await {
                        Ok(r) => {
                            r.map_err(
                            |_e: tokio::sync::watch::error::RecvError|
                                // Tenant existed but was dropped: report it as non-existent
                                GetActiveTenantError::NotFound(GetTenantError::ShardNotFound(self.tenant_shard_id))
                        )?
                        }
                        Err(TimeoutCancellableError::Cancelled) => {
                            return Err(GetActiveTenantError::Cancelled);
                        }
                        Err(TimeoutCancellableError::Timeout) => {
                            return Err(GetActiveTenantError::WaitForActiveTimeout {
                                latest_state: Some(self.current_state()),
                                wait_time: timeout,
                            });
                        }
                    }
                }
                TenantState::Active { .. } => {
                    return Ok(());
                }
                TenantState::Broken { reason, .. } => {
                    // This is fatal, and reported distinctly from the general case of "will never be active" because
                    // it's logically a 500 to external API users (broken is always a bug).
                    return Err(GetActiveTenantError::Broken(reason));
                }
                TenantState::Stopping { .. } => {
                    // There's no chance the tenant can transition back into ::Active
                    return Err(GetActiveTenantError::WillNotBecomeActive(current_state));
                }
            }
        }
    }

    pub(crate) fn get_attach_mode(&self) -> AttachmentMode {
        self.tenant_conf.load().location.attach_mode
    }

    /// For API access: generate a LocationConfig equivalent to the one that would be used to
    /// create a Tenant in the same state.  Do not use this in hot paths: it's for relatively
    /// rare external API calls, like a reconciliation at startup.
    pub(crate) fn get_location_conf(&self) -> models::LocationConfig {
        let conf = self.tenant_conf.load();

        let location_config_mode = match conf.location.attach_mode {
            AttachmentMode::Single => models::LocationConfigMode::AttachedSingle,
            AttachmentMode::Multi => models::LocationConfigMode::AttachedMulti,
            AttachmentMode::Stale => models::LocationConfigMode::AttachedStale,
        };

        // We have a pageserver TenantConf, we need the API-facing TenantConfig.
        let tenant_config: models::TenantConfig = conf.tenant_conf.clone().into();

        models::LocationConfig {
            mode: location_config_mode,
            generation: self.generation.into(),
            secondary_conf: None,
            shard_number: self.shard_identity.number.0,
            shard_count: self.shard_identity.count.literal(),
            shard_stripe_size: self.shard_identity.stripe_size.0,
            tenant_conf: tenant_config,
        }
    }

    pub(crate) fn get_tenant_shard_id(&self) -> &TenantShardId {
        &self.tenant_shard_id
    }

    pub(crate) fn get_shard_stripe_size(&self) -> ShardStripeSize {
        self.shard_identity.stripe_size
    }

    pub(crate) fn get_generation(&self) -> Generation {
        self.generation
    }

    /// This function partially shuts down the tenant (it shuts down the Timelines) and is fallible,
    /// and can leave the tenant in a bad state if it fails.  The caller is responsible for
    /// resetting this tenant to a valid state if we fail.
    pub(crate) async fn split_prepare(
        &self,
        child_shards: &Vec<TenantShardId>,
    ) -> anyhow::Result<()> {
        let (timelines, offloaded) = {
            let timelines = self.timelines.lock().unwrap();
            let offloaded = self.timelines_offloaded.lock().unwrap();
            (timelines.clone(), offloaded.clone())
        };
        let timelines_iter = timelines
            .values()
            .map(TimelineOrOffloadedArcRef::<'_>::from)
            .chain(
                offloaded
                    .values()
                    .map(TimelineOrOffloadedArcRef::<'_>::from),
            );
        for timeline in timelines_iter {
            // We do not block timeline creation/deletion during splits inside the pageserver: it is up to higher levels
            // to ensure that they do not start a split if currently in the process of doing these.

            let timeline_id = timeline.timeline_id();

            if let TimelineOrOffloadedArcRef::Timeline(timeline) = timeline {
                // Upload an index from the parent: this is partly to provide freshness for the
                // child tenants that will copy it, and partly for general ease-of-debugging: there will
                // always be a parent shard index in the same generation as we wrote the child shard index.
                tracing::info!(%timeline_id, "Uploading index");
                timeline
                    .remote_client
                    .schedule_index_upload_for_file_changes()?;
                timeline.remote_client.wait_completion().await?;
            }

            let remote_client = match timeline {
                TimelineOrOffloadedArcRef::Timeline(timeline) => timeline.remote_client.clone(),
                TimelineOrOffloadedArcRef::Offloaded(offloaded) => {
                    let remote_client = self
                        .build_timeline_client(offloaded.timeline_id, self.remote_storage.clone());
                    Arc::new(remote_client)
                }
            };

            // Shut down the timeline's remote client: this means that the indices we write
            // for child shards will not be invalidated by the parent shard deleting layers.
            tracing::info!(%timeline_id, "Shutting down remote storage client");
            remote_client.shutdown().await;

            // Download methods can still be used after shutdown, as they don't flow through the remote client's
            // queue.  In principal the RemoteTimelineClient could provide this without downloading it, but this
            // operation is rare, so it's simpler to just download it (and robustly guarantees that the index
            // we use here really is the remotely persistent one).
            tracing::info!(%timeline_id, "Downloading index_part from parent");
            let result = remote_client
                .download_index_file(&self.cancel)
                .instrument(info_span!("download_index_file", tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), %timeline_id))
                .await?;
            let index_part = match result {
                MaybeDeletedIndexPart::Deleted(_) => {
                    anyhow::bail!("Timeline deletion happened concurrently with split")
                }
                MaybeDeletedIndexPart::IndexPart(p) => p,
            };

            for child_shard in child_shards {
                tracing::info!(%timeline_id, "Uploading index_part for child {}", child_shard.to_index());
                upload_index_part(
                    &self.remote_storage,
                    child_shard,
                    &timeline_id,
                    self.generation,
                    &index_part,
                    &self.cancel,
                )
                .await?;
            }
        }

        let tenant_manifest = self.build_tenant_manifest();
        for child_shard in child_shards {
            tracing::info!(
                "Uploading tenant manifest for child {}",
                child_shard.to_index()
            );
            upload_tenant_manifest(
                &self.remote_storage,
                child_shard,
                self.generation,
                &tenant_manifest,
                &self.cancel,
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) fn get_sizes(&self) -> TopTenantShardItem {
        let mut result = TopTenantShardItem {
            id: self.tenant_shard_id,
            resident_size: 0,
            physical_size: 0,
            max_logical_size: 0,
        };

        for timeline in self.timelines.lock().unwrap().values() {
            result.resident_size += timeline.metrics.resident_physical_size_gauge.get();

            result.physical_size += timeline
                .remote_client
                .metrics
                .remote_physical_size_gauge
                .get();
            result.max_logical_size = std::cmp::max(
                result.max_logical_size,
                timeline.metrics.current_logical_size_gauge.get(),
            );
        }

        result
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

enum ActivateTimelineArgs {
    Yes {
        broker_client: storage_broker::BrokerClientChannel,
    },
    No,
}

impl Tenant {
    pub fn tenant_specific_overrides(&self) -> TenantConfOpt {
        self.tenant_conf.load().tenant_conf.clone()
    }

    pub fn effective_config(&self) -> TenantConf {
        self.tenant_specific_overrides()
            .merge(self.conf.default_tenant_conf.clone())
    }

    pub fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    pub fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    pub fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    pub fn get_compaction_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .compaction_period
            .unwrap_or(self.conf.default_tenant_conf.compaction_period)
    }

    pub fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    pub fn get_gc_horizon(&self) -> u64 {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .gc_horizon
            .unwrap_or(self.conf.default_tenant_conf.gc_horizon)
    }

    pub fn get_gc_period(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .gc_period
            .unwrap_or(self.conf.default_tenant_conf.gc_period)
    }

    pub fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    pub fn get_pitr_interval(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .pitr_interval
            .unwrap_or(self.conf.default_tenant_conf.pitr_interval)
    }

    pub fn get_min_resident_size_override(&self) -> Option<u64> {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .min_resident_size_override
            .or(self.conf.default_tenant_conf.min_resident_size_override)
    }

    pub fn get_heatmap_period(&self) -> Option<Duration> {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        let heatmap_period = tenant_conf
            .heatmap_period
            .unwrap_or(self.conf.default_tenant_conf.heatmap_period);
        if heatmap_period.is_zero() {
            None
        } else {
            Some(heatmap_period)
        }
    }

    pub fn get_lsn_lease_length(&self) -> Duration {
        let tenant_conf = self.tenant_conf.load().tenant_conf.clone();
        tenant_conf
            .lsn_lease_length
            .unwrap_or(self.conf.default_tenant_conf.lsn_lease_length)
    }

    /// Generate an up-to-date TenantManifest based on the state of this Tenant.
    fn build_tenant_manifest(&self) -> TenantManifest {
        let timelines_offloaded = self.timelines_offloaded.lock().unwrap();

        let mut timeline_manifests = timelines_offloaded
            .iter()
            .map(|(_timeline_id, offloaded)| offloaded.manifest())
            .collect::<Vec<_>>();
        // Sort the manifests so that our output is deterministic
        timeline_manifests.sort_by_key(|timeline_manifest| timeline_manifest.timeline_id);

        TenantManifest {
            version: LATEST_TENANT_MANIFEST_VERSION,
            offloaded_timelines: timeline_manifests,
        }
    }

    pub fn update_tenant_config<F: Fn(TenantConfOpt) -> anyhow::Result<TenantConfOpt>>(
        &self,
        update: F,
    ) -> anyhow::Result<TenantConfOpt> {
        // Use read-copy-update in order to avoid overwriting the location config
        // state if this races with [`Tenant::set_new_location_config`]. Note that
        // this race is not possible if both request types come from the storage
        // controller (as they should!) because an exclusive op lock is required
        // on the storage controller side.

        self.tenant_conf
            .try_rcu(|attached_conf| -> Result<_, anyhow::Error> {
                Ok(Arc::new(AttachedTenantConf {
                    tenant_conf: update(attached_conf.tenant_conf.clone())?,
                    location: attached_conf.location,
                    lsn_lease_deadline: attached_conf.lsn_lease_deadline,
                }))
            })?;

        let updated = self.tenant_conf.load();

        self.tenant_conf_updated(&updated.tenant_conf);
        // Don't hold self.timelines.lock() during the notifies.
        // There's no risk of deadlock right now, but there could be if we consolidate
        // mutexes in struct Timeline in the future.
        let timelines = self.list_timelines();
        for timeline in timelines {
            timeline.tenant_conf_updated(&updated);
        }

        Ok(updated.tenant_conf.clone())
    }

    pub(crate) fn set_new_location_config(&self, new_conf: AttachedTenantConf) {
        let new_tenant_conf = new_conf.tenant_conf.clone();

        self.tenant_conf.store(Arc::new(new_conf.clone()));

        self.tenant_conf_updated(&new_tenant_conf);
        // Don't hold self.timelines.lock() during the notifies.
        // There's no risk of deadlock right now, but there could be if we consolidate
        // mutexes in struct Timeline in the future.
        let timelines = self.list_timelines();
        for timeline in timelines {
            timeline.tenant_conf_updated(&new_conf);
        }
    }

    fn get_pagestream_throttle_config(
        psconf: &'static PageServerConf,
        overrides: &TenantConfOpt,
    ) -> throttle::Config {
        overrides
            .timeline_get_throttle
            .clone()
            .unwrap_or(psconf.default_tenant_conf.timeline_get_throttle.clone())
    }

    pub(crate) fn tenant_conf_updated(&self, new_conf: &TenantConfOpt) {
        let conf = Self::get_pagestream_throttle_config(self.conf, new_conf);
        self.pagestream_throttle.reconfigure(conf)
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
    #[allow(clippy::too_many_arguments)]
    fn create_timeline_struct(
        &self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        resources: TimelineResources,
        cause: CreateTimelineCause,
        create_idempotency: CreateTimelineIdempotency,
        l2_lsn: Option<Lsn>,
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

        let pg_version = new_metadata.pg_version();

        let timeline = Timeline::new(
            self.conf,
            Arc::clone(&self.tenant_conf),
            new_metadata,
            ancestor,
            new_timeline_id,
            self.tenant_shard_id,
            self.generation,
            self.shard_identity,
            self.walredo_mgr.clone(),
            resources,
            pg_version,
            state,
            self.attach_wal_lag_cooldown.clone(),
            create_idempotency,
            l2_lsn,
            self.cancel.child_token(),
        );

        Ok(timeline)
    }

    /// [`Tenant::shutdown`] must be called before dropping the returned [`Tenant`] object
    /// to ensure proper cleanup of background tasks and metrics.
    //
    // Allow too_many_arguments because a constructor's argument list naturally grows with the
    // number of attributes in the struct: breaking these out into a builder wouldn't be helpful.
    #[allow(clippy::too_many_arguments)]
    fn new(
        state: TenantState,
        conf: &'static PageServerConf,
        attached_conf: AttachedTenantConf,
        shard_identity: ShardIdentity,
        walredo_mgr: Option<Arc<WalRedoManager>>,
        tenant_shard_id: TenantShardId,
        remote_storage: GenericRemoteStorage,
        deletion_queue_client: DeletionQueueClient,
        l0_flush_global_state: L0FlushGlobalState,
    ) -> Tenant {
        debug_assert!(
            !attached_conf.location.generation.is_none() || conf.control_plane_api.is_none()
        );

        let (state, mut rx) = watch::channel(state);

        tokio::spawn(async move {
            // reflect tenant state in metrics:
            // - global per tenant state: TENANT_STATE_METRIC
            // - "set" of broken tenants: BROKEN_TENANTS_SET
            //
            // set of broken tenants should not have zero counts so that it remains accessible for
            // alerting.

            let tid = tenant_shard_id.to_string();
            let shard_id = tenant_shard_id.shard_slug().to_string();
            let set_key = &[tid.as_str(), shard_id.as_str()][..];

            fn inspect_state(state: &TenantState) -> ([&'static str; 1], bool) {
                ([state.into()], matches!(state, TenantState::Broken { .. }))
            }

            let mut tuple = inspect_state(&rx.borrow_and_update());

            let is_broken = tuple.1;
            let mut counted_broken = if is_broken {
                // add the id to the set right away, there should not be any updates on the channel
                // after before tenant is removed, if ever
                BROKEN_TENANTS_SET.with_label_values(set_key).set(1);
                true
            } else {
                false
            };

            loop {
                let labels = &tuple.0;
                let current = TENANT_STATE_METRIC.with_label_values(labels);
                current.inc();

                if rx.changed().await.is_err() {
                    // tenant has been dropped
                    current.dec();
                    drop(BROKEN_TENANTS_SET.remove_label_values(set_key));
                    break;
                }

                current.dec();
                tuple = inspect_state(&rx.borrow_and_update());

                let is_broken = tuple.1;
                if is_broken && !counted_broken {
                    counted_broken = true;
                    // insert the tenant_id (back) into the set while avoiding needless counter
                    // access
                    BROKEN_TENANTS_SET.with_label_values(set_key).set(1);
                }
            }
        });

        Tenant {
            tenant_shard_id,
            shard_identity,
            generation: attached_conf.location.generation,
            conf,
            // using now here is good enough approximation to catch tenants with really long
            // activation times.
            constructed_at: Instant::now(),
            timelines: Mutex::new(HashMap::new()),
            timelines_creating: Mutex::new(HashSet::new()),
            timelines_offloaded: Mutex::new(HashMap::new()),
            tenant_manifest_upload: Default::default(),
            gc_cs: tokio::sync::Mutex::new(()),
            walredo_mgr,
            remote_storage,
            deletion_queue_client,
            state,
            cached_logical_sizes: tokio::sync::Mutex::new(HashMap::new()),
            cached_synthetic_tenant_size: Arc::new(AtomicU64::new(0)),
            eviction_task_tenant_state: tokio::sync::Mutex::new(EvictionTaskTenantState::default()),
            compaction_circuit_breaker: std::sync::Mutex::new(CircuitBreaker::new(
                format!("compaction-{tenant_shard_id}"),
                5,
                // Compaction can be a very expensive operation, and might leak disk space.  It also ought
                // to be infallible, as long as remote storage is available.  So if it repeatedly fails,
                // use an extremely long backoff.
                Some(Duration::from_secs(3600 * 24)),
            )),
            scheduled_compaction_tasks: Mutex::new(Default::default()),
            activate_now_sem: tokio::sync::Semaphore::new(0),
            attach_wal_lag_cooldown: Arc::new(std::sync::OnceLock::new()),
            cancel: CancellationToken::default(),
            gate: Gate::default(),
            pagestream_throttle: Arc::new(throttle::Throttle::new(
                Tenant::get_pagestream_throttle_config(conf, &attached_conf.tenant_conf),
            )),
            pagestream_throttle_metrics: Arc::new(
                crate::metrics::tenant_throttling::Pagestream::new(&tenant_shard_id),
            ),
            tenant_conf: Arc::new(ArcSwap::from_pointee(attached_conf)),
            ongoing_timeline_detach: std::sync::Mutex::default(),
            gc_block: Default::default(),
            l0_flush_global_state,
        }
    }

    /// Locate and load config
    pub(super) fn load_tenant_config(
        conf: &'static PageServerConf,
        tenant_shard_id: &TenantShardId,
    ) -> Result<LocationConf, LoadConfigError> {
        let config_path = conf.tenant_location_config_path(tenant_shard_id);

        info!("loading tenant configuration from {config_path}");

        // load and parse file
        let config = fs::read_to_string(&config_path).map_err(|e| {
            match e.kind() {
                std::io::ErrorKind::NotFound => {
                    // The config should almost always exist for a tenant directory:
                    //  - When attaching a tenant, the config is the first thing we write
                    //  - When detaching a tenant, we atomically move the directory to a tmp location
                    //    before deleting contents.
                    //
                    // The very rare edge case that can result in a missing config is if we crash during attach
                    // between creating directory and writing config.  Callers should handle that as if the
                    // directory didn't exist.

                    LoadConfigError::NotFound(config_path)
                }
                _ => {
                    // No IO errors except NotFound are acceptable here: other kinds of error indicate local storage or permissions issues
                    // that we cannot cleanly recover
                    crate::virtual_file::on_fatal_io_error(&e, "Reading tenant config file")
                }
            }
        })?;

        Ok(toml_edit::de::from_str::<LocationConf>(&config)?)
    }

    #[tracing::instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()))]
    pub(super) async fn persist_tenant_config(
        conf: &'static PageServerConf,
        tenant_shard_id: &TenantShardId,
        location_conf: &LocationConf,
    ) -> std::io::Result<()> {
        let config_path = conf.tenant_location_config_path(tenant_shard_id);

        Self::persist_tenant_config_at(tenant_shard_id, &config_path, location_conf).await
    }

    #[tracing::instrument(skip_all, fields(tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug()))]
    pub(super) async fn persist_tenant_config_at(
        tenant_shard_id: &TenantShardId,
        config_path: &Utf8Path,
        location_conf: &LocationConf,
    ) -> std::io::Result<()> {
        debug!("persisting tenantconf to {config_path}");

        let mut conf_content = r#"# This file contains a specific per-tenant's config.
#  It is read in case of pageserver restart.
"#
        .to_string();

        fail::fail_point!("tenant-config-before-write", |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "tenant-config-before-write",
            ))
        });

        // Convert the config to a toml file.
        conf_content +=
            &toml_edit::ser::to_string_pretty(&location_conf).expect("Config serialization failed");

        let temp_path = path_with_suffix_extension(config_path, TEMP_FILE_SUFFIX);

        let conf_content = conf_content.into_bytes();
        VirtualFile::crashsafe_overwrite(config_path.to_owned(), temp_path, conf_content).await
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
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<GcResult, GcError> {
        let mut totals: GcResult = Default::default();
        let now = Instant::now();

        let gc_timelines = self
            .refresh_gc_info_internal(target_timeline_id, horizon, pitr, cancel, ctx)
            .await?;

        failpoint_support::sleep_millis_async!("gc_iteration_internal_after_getting_gc_timelines");

        // If there is nothing to GC, we don't want any messages in the INFO log.
        if !gc_timelines.is_empty() {
            info!("{} timelines need GC", gc_timelines.len());
        } else {
            debug!("{} timelines need GC", gc_timelines.len());
        }

        // Perform GC for each timeline.
        //
        // Note that we don't hold the `Tenant::gc_cs` lock here because we don't want to delay the
        // branch creation task, which requires the GC lock. A GC iteration can run concurrently
        // with branch creation.
        //
        // See comments in [`Tenant::branch_timeline`] for more information about why branch
        // creation task can run concurrently with timeline's GC iteration.
        for timeline in gc_timelines {
            if cancel.is_cancelled() {
                // We were requested to shut down. Stop and return with the progress we
                // made.
                break;
            }
            let result = match timeline.gc().await {
                Err(GcError::TimelineCancelled) => {
                    if target_timeline_id.is_some() {
                        // If we were targetting this specific timeline, surface cancellation to caller
                        return Err(GcError::TimelineCancelled);
                    } else {
                        // A timeline may be shutting down independently of the tenant's lifecycle: we should
                        // skip past this and proceed to try GC on other timelines.
                        continue;
                    }
                }
                r => r?,
            };
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
    pub(crate) async fn refresh_gc_info(
        &self,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<Vec<Arc<Timeline>>, GcError> {
        // since this method can now be called at different rates than the configured gc loop, it
        // might be that these configuration values get applied faster than what it was previously,
        // since these were only read from the gc task.
        let horizon = self.get_gc_horizon();
        let pitr = self.get_pitr_interval();

        // refresh all timelines
        let target_timeline_id = None;

        self.refresh_gc_info_internal(target_timeline_id, horizon, pitr, cancel, ctx)
            .await
    }

    /// Populate all Timelines' `GcInfo` with information about their children.  We do not set the
    /// PITR cutoffs here, because that requires I/O: this is done later, before GC, by [`Self::refresh_gc_info_internal`]
    ///
    /// Subsequently, parent-child relationships are updated incrementally inside [`Timeline::new`] and [`Timeline::drop`].
    fn initialize_gc_info(
        &self,
        timelines: &std::sync::MutexGuard<HashMap<TimelineId, Arc<Timeline>>>,
        timelines_offloaded: &std::sync::MutexGuard<HashMap<TimelineId, Arc<OffloadedTimeline>>>,
        restrict_to_timeline: Option<TimelineId>,
    ) {
        if restrict_to_timeline.is_none() {
            // This function must be called before activation: after activation timeline create/delete operations
            // might happen, and this function is not safe to run concurrently with those.
            assert!(!self.is_active());
        }

        // Scan all timelines. For each timeline, remember the timeline ID and
        // the branch point where it was created.
        let mut all_branchpoints: BTreeMap<TimelineId, Vec<(Lsn, TimelineId, MaybeOffloaded)>> =
            BTreeMap::new();
        timelines.iter().for_each(|(timeline_id, timeline_entry)| {
            if let Some(ancestor_timeline_id) = &timeline_entry.get_ancestor_timeline_id() {
                let ancestor_children = all_branchpoints.entry(*ancestor_timeline_id).or_default();
                ancestor_children.push((
                    timeline_entry.get_ancestor_lsn(),
                    *timeline_id,
                    MaybeOffloaded::No,
                ));
            }
        });
        timelines_offloaded
            .iter()
            .for_each(|(timeline_id, timeline_entry)| {
                let Some(ancestor_timeline_id) = &timeline_entry.ancestor_timeline_id else {
                    return;
                };
                let Some(retain_lsn) = timeline_entry.ancestor_retain_lsn else {
                    return;
                };
                let ancestor_children = all_branchpoints.entry(*ancestor_timeline_id).or_default();
                ancestor_children.push((retain_lsn, *timeline_id, MaybeOffloaded::Yes));
            });

        // The number of bytes we always keep, irrespective of PITR: this is a constant across timelines
        let horizon = self.get_gc_horizon();

        // Populate each timeline's GcInfo with information about its child branches
        let timelines_to_write = if let Some(timeline_id) = restrict_to_timeline {
            itertools::Either::Left(timelines.get(&timeline_id).into_iter())
        } else {
            itertools::Either::Right(timelines.values())
        };
        for timeline in timelines_to_write {
            let mut branchpoints: Vec<(Lsn, TimelineId, MaybeOffloaded)> = all_branchpoints
                .remove(&timeline.timeline_id)
                .unwrap_or_default();

            branchpoints.sort_by_key(|b| b.0);

            let mut target = timeline.gc_info.write().unwrap();

            target.retain_lsns = branchpoints;

            let space_cutoff = timeline
                .get_last_record_lsn()
                .checked_sub(horizon)
                .unwrap_or(Lsn(0));

            target.cutoffs = GcCutoffs {
                space: space_cutoff,
                time: Lsn::INVALID,
            };
        }
    }

    async fn refresh_gc_info_internal(
        &self,
        target_timeline_id: Option<TimelineId>,
        horizon: u64,
        pitr: Duration,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<Vec<Arc<Timeline>>, GcError> {
        // before taking the gc_cs lock, do the heavier weight finding of gc_cutoff points for
        // currently visible timelines.
        let timelines = self
            .timelines
            .lock()
            .unwrap()
            .values()
            .filter(|tl| match target_timeline_id.as_ref() {
                Some(target) => &tl.timeline_id == target,
                None => true,
            })
            .cloned()
            .collect::<Vec<_>>();

        if target_timeline_id.is_some() && timelines.is_empty() {
            // We were to act on a particular timeline and it wasn't found
            return Err(GcError::TimelineNotFound);
        }

        let mut gc_cutoffs: HashMap<TimelineId, GcCutoffs> =
            HashMap::with_capacity(timelines.len());

        // Ensures all timelines use the same start time when computing the time cutoff.
        let now_ts_for_pitr_calc = SystemTime::now();
        for timeline in timelines.iter() {
            let cutoff = timeline
                .get_last_record_lsn()
                .checked_sub(horizon)
                .unwrap_or(Lsn(0));

            let cutoffs = timeline
                .find_gc_cutoffs(now_ts_for_pitr_calc, cutoff, pitr, cancel, ctx)
                .await?;
            let old = gc_cutoffs.insert(timeline.timeline_id, cutoffs);
            assert!(old.is_none());
        }

        if !self.is_active() || self.cancel.is_cancelled() {
            return Err(GcError::TenantCancelled);
        }

        // grab mutex to prevent new timelines from being created here; avoid doing long operations
        // because that will stall branch creation.
        let gc_cs = self.gc_cs.lock().await;

        // Ok, we now know all the branch points.
        // Update the GC information for each timeline.
        let mut gc_timelines = Vec::with_capacity(timelines.len());
        for timeline in timelines {
            // We filtered the timeline list above
            if let Some(target_timeline_id) = target_timeline_id {
                assert_eq!(target_timeline_id, timeline.timeline_id);
            }

            {
                let mut target = timeline.gc_info.write().unwrap();

                // Cull any expired leases
                let now = SystemTime::now();
                target.leases.retain(|_, lease| !lease.is_expired(&now));

                timeline
                    .metrics
                    .valid_lsn_lease_count_gauge
                    .set(target.leases.len() as u64);

                // Look up parent's PITR cutoff to update the child's knowledge of whether it is within parent's PITR
                if let Some(ancestor_id) = timeline.get_ancestor_timeline_id() {
                    if let Some(ancestor_gc_cutoffs) = gc_cutoffs.get(&ancestor_id) {
                        target.within_ancestor_pitr =
                            timeline.get_ancestor_lsn() >= ancestor_gc_cutoffs.time;
                    }
                }

                // Update metrics that depend on GC state
                timeline
                    .metrics
                    .archival_size
                    .set(if target.within_ancestor_pitr {
                        timeline.metrics.current_logical_size_gauge.get()
                    } else {
                        0
                    });
                timeline.metrics.pitr_history_size.set(
                    timeline
                        .get_last_record_lsn()
                        .checked_sub(target.cutoffs.time)
                        .unwrap_or(Lsn(0))
                        .0,
                );

                // Apply the cutoffs we found to the Timeline's GcInfo.  Why might we _not_ have cutoffs for a timeline?
                // - this timeline was created while we were finding cutoffs
                // - lsn for timestamp search fails for this timeline repeatedly
                if let Some(cutoffs) = gc_cutoffs.get(&timeline.timeline_id) {
                    let original_cutoffs = target.cutoffs.clone();
                    // GC cutoffs should never go back
                    target.cutoffs = GcCutoffs {
                        space: Lsn(cutoffs.space.0.max(original_cutoffs.space.0)),
                        time: Lsn(cutoffs.time.0.max(original_cutoffs.time.0)),
                    }
                }
            }

            gc_timelines.push(timeline);
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
        self: &Arc<Self>,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        ancestor_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> Result<Arc<Timeline>, CreateTimelineError> {
        let tl = self
            .branch_timeline_impl(src_timeline, dst_id, ancestor_lsn, ctx)
            .await?
            .into_timeline_for_test();
        tl.set_state(TimelineState::Active);
        Ok(tl)
    }

    /// Helper for unit tests to branch a timeline with some pre-loaded states.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub async fn branch_timeline_test_with_layers(
        self: &Arc<Self>,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        ancestor_lsn: Option<Lsn>,
        ctx: &RequestContext,
        delta_layer_desc: Vec<timeline::DeltaLayerTestDesc>,
        image_layer_desc: Vec<(Lsn, Vec<(pageserver_api::key::Key, bytes::Bytes)>)>,
        end_lsn: Lsn,
    ) -> anyhow::Result<Arc<Timeline>> {
        use checks::check_valid_layermap;
        use itertools::Itertools;

        let tline = self
            .branch_timeline_test(src_timeline, dst_id, ancestor_lsn, ctx)
            .await?;
        let ancestor_lsn = if let Some(ancestor_lsn) = ancestor_lsn {
            ancestor_lsn
        } else {
            tline.get_last_record_lsn()
        };
        assert!(end_lsn >= ancestor_lsn);
        tline.force_advance_lsn(end_lsn);
        for deltas in delta_layer_desc {
            tline
                .force_create_delta_layer(deltas, Some(ancestor_lsn), ctx)
                .await?;
        }
        for (lsn, images) in image_layer_desc {
            tline
                .force_create_image_layer(lsn, images, Some(ancestor_lsn), ctx)
                .await?;
        }
        let layer_names = tline
            .layers
            .read()
            .await
            .layer_map()
            .unwrap()
            .iter_historic_layers()
            .map(|layer| layer.layer_name())
            .collect_vec();
        if let Some(err) = check_valid_layermap(&layer_names) {
            bail!("invalid layermap: {err}");
        }
        Ok(tline)
    }

    /// Branch an existing timeline.
    async fn branch_timeline(
        self: &Arc<Self>,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        start_lsn: Option<Lsn>,
        ctx: &RequestContext,
    ) -> Result<CreateTimelineResult, CreateTimelineError> {
        self.branch_timeline_impl(src_timeline, dst_id, start_lsn, ctx)
            .await
    }

    async fn branch_timeline_impl(
        self: &Arc<Self>,
        src_timeline: &Arc<Timeline>,
        dst_id: TimelineId,
        start_lsn: Option<Lsn>,
        _ctx: &RequestContext,
    ) -> Result<CreateTimelineResult, CreateTimelineError> {
        let src_id = src_timeline.timeline_id;

        // We will validate our ancestor LSN in this function.  Acquire the GC lock so that
        // this check cannot race with GC, and the ancestor LSN is guaranteed to remain
        // valid while we are creating the branch.
        let _gc_cs = self.gc_cs.lock().await;

        // If no start LSN is specified, we branch the new timeline from the source timeline's last record LSN
        let start_lsn = start_lsn.unwrap_or_else(|| {
            let lsn = src_timeline.get_last_record_lsn();
            info!("branching timeline {dst_id} from timeline {src_id} at last record LSN: {lsn}");
            lsn
        });

        // we finally have determined the ancestor_start_lsn, so we can get claim exclusivity now
        let timeline_create_guard = match self
            .start_creating_timeline(
                dst_id,
                CreateTimelineIdempotency::Branch {
                    ancestor_timeline_id: src_timeline.timeline_id,
                    ancestor_start_lsn: start_lsn,
                },
            )
            .await?
        {
            StartCreatingTimelineResult::CreateGuard(guard) => guard,
            StartCreatingTimelineResult::Idempotent(timeline) => {
                return Ok(CreateTimelineResult::Idempotent(timeline));
            }
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
            let cutoff = gc_info.min_cutoff();
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
                timeline_create_guard,
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
        new_timeline
            .remote_client
            .schedule_index_upload_for_full_metadata_update(&metadata)
            .context("branch initial metadata upload")?;

        // Callers are responsible to wait for uploads to complete and for activating the timeline.

        Ok(CreateTimelineResult::Created(new_timeline))
    }

    /// For unit tests, make this visible so that other modules can directly create timelines
    #[cfg(test)]
    #[tracing::instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug(), %timeline_id))]
    pub(crate) async fn bootstrap_timeline_test(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        pg_version: u32,
        load_existing_initdb: Option<TimelineId>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        self.bootstrap_timeline(timeline_id, pg_version, load_existing_initdb, ctx)
            .await
            .map_err(anyhow::Error::new)
            .map(|r| r.into_timeline_for_test())
    }

    /// Get exclusive access to the timeline ID for creation.
    ///
    /// Timeline-creating code paths must use this function before making changes
    /// to in-memory or persistent state.
    ///
    /// The `state` parameter is a description of the timeline creation operation
    /// we intend to perform.
    /// If the timeline was already created in the meantime, we check whether this
    /// request conflicts or is idempotent , based on `state`.
    async fn start_creating_timeline(
        self: &Arc<Self>,
        new_timeline_id: TimelineId,
        idempotency: CreateTimelineIdempotency,
    ) -> Result<StartCreatingTimelineResult, CreateTimelineError> {
        let allow_offloaded = false;
        match self.create_timeline_create_guard(new_timeline_id, idempotency, allow_offloaded) {
            Ok(create_guard) => {
                pausable_failpoint!("timeline-creation-after-uninit");
                Ok(StartCreatingTimelineResult::CreateGuard(create_guard))
            }
            Err(TimelineExclusionError::ShuttingDown) => Err(CreateTimelineError::ShuttingDown),
            Err(TimelineExclusionError::AlreadyCreating) => {
                // Creation is in progress, we cannot create it again, and we cannot
                // check if this request matches the existing one, so caller must try
                // again later.
                Err(CreateTimelineError::AlreadyCreating)
            }
            Err(TimelineExclusionError::Other(e)) => Err(CreateTimelineError::Other(e)),
            Err(TimelineExclusionError::AlreadyExists {
                existing: TimelineOrOffloaded::Offloaded(_existing),
                ..
            }) => {
                info!("timeline already exists but is offloaded");
                Err(CreateTimelineError::Conflict)
            }
            Err(TimelineExclusionError::AlreadyExists {
                existing: TimelineOrOffloaded::Timeline(existing),
                arg,
            }) => {
                {
                    let existing = &existing.create_idempotency;
                    let _span = info_span!("idempotency_check", ?existing, ?arg).entered();
                    debug!("timeline already exists");

                    match (existing, &arg) {
                        // FailWithConflict => no idempotency check
                        (CreateTimelineIdempotency::FailWithConflict, _)
                        | (_, CreateTimelineIdempotency::FailWithConflict) => {
                            warn!("timeline already exists, failing request");
                            return Err(CreateTimelineError::Conflict);
                        }
                        // Idempotent <=> CreateTimelineIdempotency is identical
                        (x, y) if x == y => {
                            info!("timeline already exists and idempotency matches, succeeding request");
                            // fallthrough
                        }
                        (_, _) => {
                            warn!("idempotency conflict, failing request");
                            return Err(CreateTimelineError::Conflict);
                        }
                    }
                }

                Ok(StartCreatingTimelineResult::Idempotent(existing))
            }
        }
    }

    async fn upload_initdb(
        &self,
        timelines_path: &Utf8PathBuf,
        pgdata_path: &Utf8PathBuf,
        timeline_id: &TimelineId,
    ) -> anyhow::Result<()> {
        let temp_path = timelines_path.join(format!(
            "{INITDB_PATH}.upload-{timeline_id}.{TEMP_FILE_SUFFIX}"
        ));

        scopeguard::defer! {
            if let Err(e) = fs::remove_file(&temp_path) {
                error!("Failed to remove temporary initdb archive '{temp_path}': {e}");
            }
        }

        let (pgdata_zstd, tar_zst_size) = create_zst_tarball(pgdata_path, &temp_path).await?;
        const INITDB_TAR_ZST_WARN_LIMIT: u64 = 2 * 1024 * 1024;
        if tar_zst_size > INITDB_TAR_ZST_WARN_LIMIT {
            warn!(
                "compressed {temp_path} size of {tar_zst_size} is above limit {INITDB_TAR_ZST_WARN_LIMIT}."
            );
        }

        pausable_failpoint!("before-initdb-upload");

        backoff::retry(
            || async {
                self::remote_timeline_client::upload_initdb_dir(
                    &self.remote_storage,
                    &self.tenant_shard_id.tenant_id,
                    timeline_id,
                    pgdata_zstd.try_clone().await?,
                    tar_zst_size,
                    &self.cancel,
                )
                .await
            },
            |_| false,
            3,
            u32::MAX,
            "persist_initdb_tar_zst",
            &self.cancel,
        )
        .await
        .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
        .and_then(|x| x)
    }

    /// - run initdb to init temporary instance and get bootstrap data
    /// - after initialization completes, tar up the temp dir and upload it to S3.
    async fn bootstrap_timeline(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        pg_version: u32,
        load_existing_initdb: Option<TimelineId>,
        ctx: &RequestContext,
    ) -> Result<CreateTimelineResult, CreateTimelineError> {
        let timeline_create_guard = match self
            .start_creating_timeline(
                timeline_id,
                CreateTimelineIdempotency::Bootstrap { pg_version },
            )
            .await?
        {
            StartCreatingTimelineResult::CreateGuard(guard) => guard,
            StartCreatingTimelineResult::Idempotent(timeline) => {
                return Ok(CreateTimelineResult::Idempotent(timeline))
            }
        };

        // create a `tenant/{tenant_id}/timelines/basebackup-{timeline_id}.{TEMP_FILE_SUFFIX}/`
        // temporary directory for basebackup files for the given timeline.

        let timelines_path = self.conf.timelines_path(&self.tenant_shard_id);
        let pgdata_path = path_with_suffix_extension(
            timelines_path.join(format!("basebackup-{timeline_id}")),
            TEMP_FILE_SUFFIX,
        );

        // Remove whatever was left from the previous runs: safe because TimelineCreateGuard guarantees
        // we won't race with other creations or existent timelines with the same path.
        if pgdata_path.exists() {
            fs::remove_dir_all(&pgdata_path).with_context(|| {
                format!("Failed to remove already existing initdb directory: {pgdata_path}")
            })?;
        }

        // this new directory is very temporary, set to remove it immediately after bootstrap, we don't need it
        scopeguard::defer! {
            if let Err(e) = fs::remove_dir_all(&pgdata_path) {
                // this is unlikely, but we will remove the directory on pageserver restart or another bootstrap call
                error!("Failed to remove temporary initdb directory '{pgdata_path}': {e}");
            }
        }
        if let Some(existing_initdb_timeline_id) = load_existing_initdb {
            if existing_initdb_timeline_id != timeline_id {
                let source_path = &remote_initdb_archive_path(
                    &self.tenant_shard_id.tenant_id,
                    &existing_initdb_timeline_id,
                );
                let dest_path =
                    &remote_initdb_archive_path(&self.tenant_shard_id.tenant_id, &timeline_id);

                // if this fails, it will get retried by retried control plane requests
                self.remote_storage
                    .copy_object(source_path, dest_path, &self.cancel)
                    .await
                    .context("copy initdb tar")?;
            }
            let (initdb_tar_zst_path, initdb_tar_zst) =
                self::remote_timeline_client::download_initdb_tar_zst(
                    self.conf,
                    &self.remote_storage,
                    &self.tenant_shard_id,
                    &existing_initdb_timeline_id,
                    &self.cancel,
                )
                .await
                .context("download initdb tar")?;

            scopeguard::defer! {
                if let Err(e) = fs::remove_file(&initdb_tar_zst_path) {
                    error!("Failed to remove temporary initdb archive '{initdb_tar_zst_path}': {e}");
                }
            }

            let buf_read =
                BufReader::with_capacity(remote_timeline_client::BUFFER_SIZE, initdb_tar_zst);
            extract_zst_tarball(&pgdata_path, buf_read)
                .await
                .context("extract initdb tar")?;
        } else {
            // Init temporarily repo to get bootstrap data, this creates a directory in the `pgdata_path` path
            run_initdb(self.conf, &pgdata_path, pg_version, &self.cancel)
                .await
                .context("run initdb")?;

            // Upload the created data dir to S3
            if self.tenant_shard_id().is_shard_zero() {
                self.upload_initdb(&timelines_path, &pgdata_path, &timeline_id)
                    .await?;
            }
        }
        let pgdata_lsn = import_datadir::get_lsn_from_controlfile(&pgdata_path)?.align();

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
                timeline_create_guard,
                pgdata_lsn,
                None,
            )
            .await?;

        let tenant_shard_id = raw_timeline.owning_tenant.tenant_shard_id;
        let unfinished_timeline = raw_timeline.raw_timeline()?;

        // Flush the new layer files to disk, before we make the timeline as available to
        // the outside world.
        //
        // Flush loop needs to be spawned in order to be able to flush.
        unfinished_timeline.maybe_spawn_flush_loop();

        import_datadir::import_timeline_from_postgres_datadir(
            unfinished_timeline,
            &pgdata_path,
            pgdata_lsn,
            ctx,
        )
        .await
        .with_context(|| {
            format!("Failed to import pgdatadir for timeline {tenant_shard_id}/{timeline_id}")
        })?;

        fail::fail_point!("before-checkpoint-new-timeline", |_| {
            Err(CreateTimelineError::Other(anyhow::anyhow!(
                "failpoint before-checkpoint-new-timeline"
            )))
        });

        unfinished_timeline
            .freeze_and_flush()
            .await
            .with_context(|| {
                format!(
                    "Failed to flush after pgdatadir import for timeline {tenant_shard_id}/{timeline_id}"
                )
            })?;

        // All done!
        let timeline = raw_timeline.finish_creation()?;

        // Callers are responsible to wait for uploads to complete and for activating the timeline.

        Ok(CreateTimelineResult::Created(timeline))
    }

    fn build_timeline_remote_client(&self, timeline_id: TimelineId) -> RemoteTimelineClient {
        RemoteTimelineClient::new(
            self.remote_storage.clone(),
            self.deletion_queue_client.clone(),
            self.conf,
            self.tenant_shard_id,
            timeline_id,
            self.generation,
            &self.tenant_conf.load().location,
        )
    }

    /// Call this before constructing a timeline, to build its required structures
    fn build_timeline_resources(&self, timeline_id: TimelineId) -> TimelineResources {
        TimelineResources {
            remote_client: self.build_timeline_remote_client(timeline_id),
            pagestream_throttle: self.pagestream_throttle.clone(),
            pagestream_throttle_metrics: self.pagestream_throttle_metrics.clone(),
            l0_flush_global_state: self.l0_flush_global_state.clone(),
        }
    }

    /// Creates intermediate timeline structure and its files.
    ///
    /// An empty layer map is initialized, and new data and WAL can be imported starting
    /// at 'disk_consistent_lsn'. After any initial data has been imported, call
    /// `finish_creation` to insert the Timeline into the timelines map.
    async fn prepare_new_timeline<'a>(
        &'a self,
        new_timeline_id: TimelineId,
        new_metadata: &TimelineMetadata,
        create_guard: TimelineCreateGuard,
        start_lsn: Lsn,
        ancestor: Option<Arc<Timeline>>,
    ) -> anyhow::Result<UninitializedTimeline<'a>> {
        let tenant_shard_id = self.tenant_shard_id;

        let resources = self.build_timeline_resources(new_timeline_id);
        resources
            .remote_client
            .init_upload_queue_for_empty_remote(new_metadata)?;

        let timeline_struct = self
            .create_timeline_struct(
                new_timeline_id,
                new_metadata,
                ancestor,
                resources,
                CreateTimelineCause::Load,
                create_guard.idempotency.clone(),
                None,
            )
            .context("Failed to create timeline data structure")?;

        timeline_struct.init_empty_layer_map(start_lsn);

        if let Err(e) = self
            .create_timeline_files(&create_guard.timeline_path)
            .await
        {
            error!("Failed to create initial files for timeline {tenant_shard_id}/{new_timeline_id}, cleaning up: {e:?}");
            cleanup_timeline_directory(create_guard);
            return Err(e);
        }

        debug!(
            "Successfully created initial files for timeline {tenant_shard_id}/{new_timeline_id}"
        );

        Ok(UninitializedTimeline::new(
            self,
            new_timeline_id,
            Some((timeline_struct, create_guard)),
        ))
    }

    async fn create_timeline_files(&self, timeline_path: &Utf8Path) -> anyhow::Result<()> {
        crashsafe::create_dir(timeline_path).context("Failed to create timeline directory")?;

        fail::fail_point!("after-timeline-dir-creation", |_| {
            anyhow::bail!("failpoint after-timeline-dir-creation");
        });

        Ok(())
    }

    /// Get a guard that provides exclusive access to the timeline directory, preventing
    /// concurrent attempts to create the same timeline.
    ///
    /// The `allow_offloaded` parameter controls whether to tolerate the existence of
    /// offloaded timelines or not.
    fn create_timeline_create_guard(
        self: &Arc<Self>,
        timeline_id: TimelineId,
        idempotency: CreateTimelineIdempotency,
        allow_offloaded: bool,
    ) -> Result<TimelineCreateGuard, TimelineExclusionError> {
        let tenant_shard_id = self.tenant_shard_id;

        let timeline_path = self.conf.timeline_path(&tenant_shard_id, &timeline_id);

        let create_guard = TimelineCreateGuard::new(
            self,
            timeline_id,
            timeline_path.clone(),
            idempotency,
            allow_offloaded,
        )?;

        // At this stage, we have got exclusive access to in-memory state for this timeline ID
        // for creation.
        // A timeline directory should never exist on disk already:
        // - a previous failed creation would have cleaned up after itself
        // - a pageserver restart would clean up timeline directories that don't have valid remote state
        //
        // Therefore it is an unexpected internal error to encounter a timeline directory already existing here,
        // this error may indicate a bug in cleanup on failed creations.
        if timeline_path.exists() {
            return Err(TimelineExclusionError::Other(anyhow::anyhow!(
                "Timeline directory already exists! This is a bug."
            )));
        }

        Ok(create_guard)
    }

    /// Gathers inputs from all of the timelines to produce a sizing model input.
    ///
    /// Future is cancellation safe. Only one calculation can be running at once per tenant.
    #[instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug()))]
    pub async fn gather_size_inputs(
        &self,
        // `max_retention_period` overrides the cutoff that is used to calculate the size
        // (only if it is shorter than the real cutoff).
        max_retention_period: Option<u64>,
        cause: LogicalSizeCalculationCause,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<size::ModelInputs, size::CalculateSyntheticSizeError> {
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
        let mut shared_cache = tokio::select! {
            locked = self.cached_logical_sizes.lock() => locked,
            _ = cancel.cancelled() => return Err(size::CalculateSyntheticSizeError::Cancelled),
            _ = self.cancel.cancelled() => return Err(size::CalculateSyntheticSizeError::Cancelled),
        };

        size::gather_inputs(
            self,
            logical_sizes_at_once,
            max_retention_period,
            &mut shared_cache,
            cause,
            cancel,
            ctx,
        )
        .await
    }

    /// Calculate synthetic tenant size and cache the result.
    /// This is periodically called by background worker.
    /// result is cached in tenant struct
    #[instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug()))]
    pub async fn calculate_synthetic_size(
        &self,
        cause: LogicalSizeCalculationCause,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<u64, size::CalculateSyntheticSizeError> {
        let inputs = self.gather_size_inputs(None, cause, cancel, ctx).await?;

        let size = inputs.calculate();

        self.set_cached_synthetic_size(size);

        Ok(size)
    }

    /// Cache given synthetic size and update the metric value
    pub fn set_cached_synthetic_size(&self, size: u64) {
        self.cached_synthetic_tenant_size
            .store(size, Ordering::Relaxed);

        // Only shard zero should be calculating synthetic sizes
        debug_assert!(self.shard_identity.is_shard_zero());

        TENANT_SYNTHETIC_SIZE_METRIC
            .get_metric_with_label_values(&[&self.tenant_shard_id.tenant_id.to_string()])
            .unwrap()
            .set(size);
    }

    pub fn cached_synthetic_size(&self) -> u64 {
        self.cached_synthetic_tenant_size.load(Ordering::Relaxed)
    }

    /// Flush any in-progress layers, schedule uploads, and wait for uploads to complete.
    ///
    /// This function can take a long time: callers should wrap it in a timeout if calling
    /// from an external API handler.
    ///
    /// Cancel-safety: cancelling this function may leave I/O running, but such I/O is
    /// still bounded by tenant/timeline shutdown.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn flush_remote(&self) -> anyhow::Result<()> {
        let timelines = self.timelines.lock().unwrap().clone();

        async fn flush_timeline(_gate: GateGuard, timeline: Arc<Timeline>) -> anyhow::Result<()> {
            tracing::info!(timeline_id=%timeline.timeline_id, "Flushing...");
            timeline.freeze_and_flush().await?;
            tracing::info!(timeline_id=%timeline.timeline_id, "Waiting for uploads...");
            timeline.remote_client.wait_completion().await?;

            Ok(())
        }

        // We do not use a JoinSet for these tasks, because we don't want them to be
        // aborted when this function's future is cancelled: they should stay alive
        // holding their GateGuard until they complete, to ensure their I/Os complete
        // before Timeline shutdown completes.
        let mut results = FuturesUnordered::new();

        for (_timeline_id, timeline) in timelines {
            // Run each timeline's flush in a task holding the timeline's gate: this
            // means that if this function's future is cancelled, the Timeline shutdown
            // will still wait for any I/O in here to complete.
            let Ok(gate) = timeline.gate.enter() else {
                continue;
            };
            let jh = tokio::task::spawn(async move { flush_timeline(gate, timeline).await });
            results.push(jh);
        }

        while let Some(r) = results.next().await {
            if let Err(e) = r {
                if !e.is_cancelled() && !e.is_panic() {
                    tracing::error!("unexpected join error: {e:?}");
                }
            }
        }

        // The flushes we did above were just writes, but the Tenant might have had
        // pending deletions as well from recent compaction/gc: we want to flush those
        // as well.  This requires flushing the global delete queue.  This is cheap
        // because it's typically a no-op.
        match self.deletion_queue_client.flush_execute().await {
            Ok(_) => {}
            Err(DeletionQueueError::ShuttingDown) => {}
        }

        Ok(())
    }

    pub(crate) fn get_tenant_conf(&self) -> TenantConfOpt {
        self.tenant_conf.load().tenant_conf.clone()
    }

    /// How much local storage would this tenant like to have?  It can cope with
    /// less than this (via eviction and on-demand downloads), but this function enables
    /// the Tenant to advertise how much storage it would prefer to have to provide fast I/O
    /// by keeping important things on local disk.
    ///
    /// This is a heuristic, not a guarantee: tenants that are long-idle will actually use less
    /// than they report here, due to layer eviction.  Tenants with many active branches may
    /// actually use more than they report here.
    pub(crate) fn local_storage_wanted(&self) -> u64 {
        let timelines = self.timelines.lock().unwrap();

        // Heuristic: we use the max() of the timelines' visible sizes, rather than the sum.  This
        // reflects the observation that on tenants with multiple large branches, typically only one
        // of them is used actively enough to occupy space on disk.
        timelines
            .values()
            .map(|t| t.metrics.visible_physical_size_gauge.get())
            .max()
            .unwrap_or(0)
    }

    /// Serialize and write the latest TenantManifest to remote storage.
    pub(crate) async fn store_tenant_manifest(&self) -> Result<(), TenantManifestError> {
        // Only one manifest write may be done at at time, and the contents of the manifest
        // must be loaded while holding this lock. This makes it safe to call this function
        // from anywhere without worrying about colliding updates.
        let mut guard = tokio::select! {
            g = self.tenant_manifest_upload.lock() => {
                g
            },
            _ = self.cancel.cancelled() => {
                return Err(TenantManifestError::Cancelled);
            }
        };

        let manifest = self.build_tenant_manifest();
        if Some(&manifest) == (*guard).as_ref() {
            // Optimisation: skip uploads that don't change anything.
            return Ok(());
        }

        upload_tenant_manifest(
            &self.remote_storage,
            &self.tenant_shard_id,
            self.generation,
            &manifest,
            &self.cancel,
        )
        .await
        .map_err(|e| {
            if self.cancel.is_cancelled() {
                TenantManifestError::Cancelled
            } else {
                TenantManifestError::RemoteStorage(e)
            }
        })?;

        // Store the successfully uploaded manifest, so that future callers can avoid
        // re-uploading the same thing.
        *guard = Some(manifest);

        Ok(())
    }
}

/// Create the cluster temporarily in 'initdbpath' directory inside the repository
/// to get bootstrap data for timeline initialization.
async fn run_initdb(
    conf: &'static PageServerConf,
    initdb_target_dir: &Utf8Path,
    pg_version: u32,
    cancel: &CancellationToken,
) -> Result<(), InitdbError> {
    let initdb_bin_path = conf
        .pg_bin_dir(pg_version)
        .map_err(InitdbError::Other)?
        .join("initdb");
    let initdb_lib_dir = conf.pg_lib_dir(pg_version).map_err(InitdbError::Other)?;
    info!(
        "running {} in {}, libdir: {}",
        initdb_bin_path, initdb_target_dir, initdb_lib_dir,
    );

    let _permit = {
        let _timer = INITDB_SEMAPHORE_ACQUISITION_TIME.start_timer();
        INIT_DB_SEMAPHORE.acquire().await
    };

    CONCURRENT_INITDBS.inc();
    scopeguard::defer! {
        CONCURRENT_INITDBS.dec();
    }

    let _timer = INITDB_RUN_TIME.start_timer();
    let res = postgres_initdb::do_run_initdb(postgres_initdb::RunInitdbArgs {
        superuser: &conf.superuser,
        locale: &conf.locale,
        initdb_bin: &initdb_bin_path,
        pg_version,
        library_search_path: &initdb_lib_dir,
        pgdata: initdb_target_dir,
    })
    .await
    .map_err(InitdbError::Inner);

    // This isn't true cancellation support, see above. Still return an error to
    // excercise the cancellation code path.
    if cancel.is_cancelled() {
        return Err(InitdbError::Cancelled);
    }

    res
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
pub(crate) mod harness {
    use bytes::{Bytes, BytesMut};
    use once_cell::sync::OnceCell;
    use pageserver_api::models::ShardParameters;
    use pageserver_api::shard::ShardIndex;
    use utils::logging;

    use crate::deletion_queue::mock::MockDeletionQueue;
    use crate::l0_flush::L0FlushConfig;
    use crate::walredo::apply_neon;
    use pageserver_api::key::Key;
    use pageserver_api::record::NeonWalRecord;

    use super::*;
    use hex_literal::hex;
    use utils::id::TenantId;

    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));
    pub const NEW_TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("AA223344556677881122334455667788"));

    /// Convenience function to create a page image with given string as the only content
    pub fn test_img(s: &str) -> Bytes {
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
                compaction_algorithm: Some(tenant_conf.compaction_algorithm),
                gc_horizon: Some(tenant_conf.gc_horizon),
                gc_period: Some(tenant_conf.gc_period),
                image_creation_threshold: Some(tenant_conf.image_creation_threshold),
                pitr_interval: Some(tenant_conf.pitr_interval),
                walreceiver_connect_timeout: Some(tenant_conf.walreceiver_connect_timeout),
                lagging_wal_timeout: Some(tenant_conf.lagging_wal_timeout),
                max_lsn_wal_lag: Some(tenant_conf.max_lsn_wal_lag),
                eviction_policy: Some(tenant_conf.eviction_policy),
                min_resident_size_override: tenant_conf.min_resident_size_override,
                evictions_low_residence_duration_metric_threshold: Some(
                    tenant_conf.evictions_low_residence_duration_metric_threshold,
                ),
                heatmap_period: Some(tenant_conf.heatmap_period),
                lazy_slru_download: Some(tenant_conf.lazy_slru_download),
                timeline_get_throttle: Some(tenant_conf.timeline_get_throttle),
                image_layer_creation_check_threshold: Some(
                    tenant_conf.image_layer_creation_check_threshold,
                ),
                lsn_lease_length: Some(tenant_conf.lsn_lease_length),
                lsn_lease_length_for_ts: Some(tenant_conf.lsn_lease_length_for_ts),
                timeline_offloading: Some(tenant_conf.timeline_offloading),
                wal_receiver_protocol_override: tenant_conf.wal_receiver_protocol_override,
                gc_compaction_enabled: Some(tenant_conf.gc_compaction_enabled),
                gc_compaction_initial_threshold_mb: Some(
                    tenant_conf.gc_compaction_initial_threshold_mb,
                ),
                gc_compaction_ratio_percent: Some(tenant_conf.gc_compaction_ratio_percent),
            }
        }
    }

    pub struct TenantHarness {
        pub conf: &'static PageServerConf,
        pub tenant_conf: TenantConf,
        pub tenant_shard_id: TenantShardId,
        pub generation: Generation,
        pub shard: ShardIndex,
        pub remote_storage: GenericRemoteStorage,
        pub remote_fs_dir: Utf8PathBuf,
        pub deletion_queue: MockDeletionQueue,
    }

    static LOG_HANDLE: OnceCell<()> = OnceCell::new();

    pub(crate) fn setup_logging() {
        LOG_HANDLE.get_or_init(|| {
            logging::init(
                logging::LogFormat::Test,
                // enable it in case the tests exercise code paths that use
                // debug_assert_current_span_has_tenant_and_timeline_id
                logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
                logging::Output::Stdout,
            )
            .expect("Failed to init test logging")
        });
    }

    impl TenantHarness {
        pub async fn create_custom(
            test_name: &'static str,
            tenant_conf: TenantConf,
            tenant_id: TenantId,
            shard_identity: ShardIdentity,
            generation: Generation,
        ) -> anyhow::Result<Self> {
            setup_logging();

            let repo_dir = PageServerConf::test_repo_dir(test_name);
            let _ = fs::remove_dir_all(&repo_dir);
            fs::create_dir_all(&repo_dir)?;

            let conf = PageServerConf::dummy_conf(repo_dir);
            // Make a static copy of the config. This can never be free'd, but that's
            // OK in a test.
            let conf: &'static PageServerConf = Box::leak(Box::new(conf));

            let shard = shard_identity.shard_index();
            let tenant_shard_id = TenantShardId {
                tenant_id,
                shard_number: shard.shard_number,
                shard_count: shard.shard_count,
            };
            fs::create_dir_all(conf.tenant_path(&tenant_shard_id))?;
            fs::create_dir_all(conf.timelines_path(&tenant_shard_id))?;

            use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
            let remote_fs_dir = conf.workdir.join("localfs");
            std::fs::create_dir_all(&remote_fs_dir).unwrap();
            let config = RemoteStorageConfig {
                storage: RemoteStorageKind::LocalFs {
                    local_path: remote_fs_dir.clone(),
                },
                timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
                small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT,
            };
            let remote_storage = GenericRemoteStorage::from_config(&config).await.unwrap();
            let deletion_queue = MockDeletionQueue::new(Some(remote_storage.clone()));

            Ok(Self {
                conf,
                tenant_conf,
                tenant_shard_id,
                generation,
                shard,
                remote_storage,
                remote_fs_dir,
                deletion_queue,
            })
        }

        pub async fn create(test_name: &'static str) -> anyhow::Result<Self> {
            // Disable automatic GC and compaction to make the unit tests more deterministic.
            // The tests perform them manually if needed.
            let tenant_conf = TenantConf {
                gc_period: Duration::ZERO,
                compaction_period: Duration::ZERO,
                ..TenantConf::default()
            };
            let tenant_id = TenantId::generate();
            let shard = ShardIdentity::unsharded();
            Self::create_custom(
                test_name,
                tenant_conf,
                tenant_id,
                shard,
                Generation::new(0xdeadbeef),
            )
            .await
        }

        pub fn span(&self) -> tracing::Span {
            info_span!("TenantHarness", tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug())
        }

        pub(crate) async fn load(&self) -> (Arc<Tenant>, RequestContext) {
            let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
            (
                self.do_try_load(&ctx)
                    .await
                    .expect("failed to load test tenant"),
                ctx,
            )
        }

        #[instrument(skip_all, fields(tenant_id=%self.tenant_shard_id.tenant_id, shard_id=%self.tenant_shard_id.shard_slug()))]
        pub(crate) async fn do_try_load(
            &self,
            ctx: &RequestContext,
        ) -> anyhow::Result<Arc<Tenant>> {
            let walredo_mgr = Arc::new(WalRedoManager::from(TestRedoManager));

            let tenant = Arc::new(Tenant::new(
                TenantState::Attaching,
                self.conf,
                AttachedTenantConf::try_from(LocationConf::attached_single(
                    TenantConfOpt::from(self.tenant_conf.clone()),
                    self.generation,
                    &ShardParameters::default(),
                ))
                .unwrap(),
                // This is a legacy/test code path: sharding isn't supported here.
                ShardIdentity::unsharded(),
                Some(walredo_mgr),
                self.tenant_shard_id,
                self.remote_storage.clone(),
                self.deletion_queue.new_client(),
                // TODO: ideally we should run all unit tests with both configs
                L0FlushGlobalState::new(L0FlushConfig::default()),
            ));

            let preload = tenant
                .preload(&self.remote_storage, CancellationToken::new())
                .await?;
            tenant.attach(Some(preload), ctx).await?;

            tenant.state.send_replace(TenantState::Active);
            for timeline in tenant.timelines.lock().unwrap().values() {
                timeline.set_state(TimelineState::Active);
            }
            Ok(tenant)
        }

        pub fn timeline_path(&self, timeline_id: &TimelineId) -> Utf8PathBuf {
            self.conf.timeline_path(&self.tenant_shard_id, timeline_id)
        }
    }

    // Mock WAL redo manager that doesn't do much
    pub(crate) struct TestRedoManager;

    impl TestRedoManager {
        /// # Cancel-Safety
        ///
        /// This method is cancellation-safe.
        pub async fn request_redo(
            &self,
            key: Key,
            lsn: Lsn,
            base_img: Option<(Lsn, Bytes)>,
            records: Vec<(Lsn, NeonWalRecord)>,
            _pg_version: u32,
        ) -> Result<Bytes, walredo::Error> {
            let records_neon = records.iter().all(|r| apply_neon::can_apply_in_neon(&r.1));
            if records_neon {
                // For Neon wal records, we can decode without spawning postgres, so do so.
                let mut page = match (base_img, records.first()) {
                    (Some((_lsn, img)), _) => {
                        let mut page = BytesMut::new();
                        page.extend_from_slice(&img);
                        page
                    }
                    (_, Some((_lsn, rec))) if rec.will_init() => BytesMut::new(),
                    _ => {
                        panic!("Neon WAL redo requires base image or will init record");
                    }
                };

                for (record_lsn, record) in records {
                    apply_neon::apply_in_neon(&record, record_lsn, key, &mut page)?;
                }
                Ok(page.freeze())
            } else {
                // We never spawn a postgres walredo process in unit tests: just log what we might have done.
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

                Ok(test_img(&s))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::keyspace::KeySpaceAccum;
    use crate::tenant::harness::*;
    use crate::tenant::timeline::CompactFlags;
    use crate::DEFAULT_PG_VERSION;
    use bytes::{Bytes, BytesMut};
    use hex_literal::hex;
    use itertools::Itertools;
    use pageserver_api::key::{Key, AUX_KEY_PREFIX, NON_INHERITED_RANGE, RELATION_SIZE_PREFIX};
    use pageserver_api::keyspace::KeySpace;
    use pageserver_api::models::{CompactionAlgorithm, CompactionAlgorithmSettings};
    use pageserver_api::value::Value;
    use pageserver_compaction::helpers::overlaps_with;
    use rand::{thread_rng, Rng};
    use storage_layer::PersistentLayerKey;
    use tests::storage_layer::ValuesReconstructState;
    use tests::timeline::{GetVectoredError, ShutdownMode};
    use timeline::{CompactOptions, DeltaLayerTestDesc};
    use utils::id::TenantId;

    #[cfg(feature = "testing")]
    use models::CompactLsnRange;
    #[cfg(feature = "testing")]
    use pageserver_api::record::NeonWalRecord;
    #[cfg(feature = "testing")]
    use timeline::compaction::{KeyHistoryRetention, KeyLogAtLsn};
    #[cfg(feature = "testing")]
    use timeline::GcInfo;

    static TEST_KEY: Lazy<Key> =
        Lazy::new(|| Key::from_slice(&hex!("010000000033333333444444445500000001")));

    #[tokio::test]
    async fn test_basic() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_basic").await?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let mut writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x10),
                &Value::Image(test_img("foo at 0x10")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        let mut writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x20),
                &Value::Image(test_img("foo at 0x20")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x10), &ctx).await?,
            test_img("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x1f), &ctx).await?,
            test_img("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x20), &ctx).await?,
            test_img("foo at 0x20")
        );

        Ok(())
    }

    #[tokio::test]
    async fn no_duplicate_timelines() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("no_duplicate_timelines")
            .await?
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
                "timeline already exists with different parameters".to_string()
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

        let (tenant, ctx) = TenantHarness::create("test_branch").await?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let mut writer = tline.writer().await;

        #[allow(non_snake_case)]
        let TEST_KEY_A: Key = Key::from_hex("110000000033333333444444445500000001").unwrap();
        #[allow(non_snake_case)]
        let TEST_KEY_B: Key = Key::from_hex("110000000033333333444444445500000002").unwrap();

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
        let mut new_writer = newtline.writer().await;
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
        {
            let mut writer = tline.writer().await;
            // Create a relation on the timeline
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(test_img(&format!("foo at {}", lsn))),
                    ctx,
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(test_img(&format!("foo at {}", lsn))),
                    ctx,
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
        }
        tline.freeze_and_flush().await?;
        {
            let mut writer = tline.writer().await;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(test_img(&format!("foo at {}", lsn))),
                    ctx,
                )
                .await?;
            writer.finish_write(lsn);
            lsn += 0x10;
            writer
                .put(
                    *TEST_KEY,
                    lsn,
                    &Value::Image(test_img(&format!("foo at {}", lsn))),
                    ctx,
                )
                .await?;
            writer.finish_write(lsn);
        }
        tline.freeze_and_flush().await.map_err(|e| e.into())
    }

    #[tokio::test(start_paused = true)]
    async fn test_prohibit_branch_creation_on_garbage_collected_data() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_prohibit_branch_creation_on_garbage_collected_data")
                .await?
                .load()
                .await;
        // Advance to the lsn lease deadline so that GC is not blocked by
        // initial transition into AttachedSingle.
        tokio::time::advance(tenant.get_lsn_lease_length()).await;
        tokio::time::resume();
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

        // this removes layers before lsn 40 (50 minus 10), so there are two remaining layers, image and delta for 31-50
        // FIXME: this doesn't actually remove any layer currently, given how the flushing
        // and compaction works. But it does set the 'cutoff' point so that the cross check
        // below should fail.
        tenant
            .gc_iteration(
                Some(TIMELINE_ID),
                0x10,
                Duration::ZERO,
                &CancellationToken::new(),
                &ctx,
            )
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
            TenantHarness::create("test_prohibit_branch_creation_on_pre_initdb_lsn")
                .await?
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
                    .contains("is earlier than latest GC cutoff"));
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
            TenantHarness::create("test_get_branchpoints_from_an_inactive_timeline")
                .await?
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
            .gc_iteration(
                Some(TIMELINE_ID),
                0x10,
                Duration::ZERO,
                &CancellationToken::new(),
                &ctx,
            )
            .await?;

        // The branchpoints should contain all timelines, even ones marked
        // as Broken.
        {
            let branchpoints = &tline.gc_info.read().unwrap().retain_lsns;
            assert_eq!(branchpoints.len(), 1);
            assert_eq!(
                branchpoints[0],
                (Lsn(0x40), NEW_TIMELINE_ID, MaybeOffloaded::No)
            );
        }

        // You can read the key from the child branch even though the parent is
        // Broken, as long as you don't need to access data from the parent.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x70), &ctx).await?,
            test_img(&format!("foo at {}", Lsn(0x70)))
        );

        // This needs to traverse to the parent, and fails.
        let err = newtline.get(*TEST_KEY, Lsn(0x50), &ctx).await.unwrap_err();
        assert!(
            err.to_string().starts_with(&format!(
                "bad state on timeline {}: Broken",
                tline.timeline_id
            )),
            "{err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_retain_data_in_parent_which_is_needed_for_child() -> anyhow::Result<()> {
        let (tenant, ctx) =
            TenantHarness::create("test_retain_data_in_parent_which_is_needed_for_child")
                .await?
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
            .gc_iteration(
                Some(TIMELINE_ID),
                0x10,
                Duration::ZERO,
                &CancellationToken::new(),
                &ctx,
            )
            .await?;
        assert!(newtline.get(*TEST_KEY, Lsn(0x25), &ctx).await.is_ok());

        Ok(())
    }
    #[tokio::test]
    async fn test_parent_keeps_data_forever_after_branching() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_parent_keeps_data_forever_after_branching")
            .await?
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
            .gc_iteration(
                Some(TIMELINE_ID),
                0x10,
                Duration::ZERO,
                &CancellationToken::new(),
                &ctx,
            )
            .await?;

        // Check that the data is still accessible on the branch.
        assert_eq!(
            newtline.get(*TEST_KEY, Lsn(0x50), &ctx).await?,
            test_img(&format!("foo at {}", Lsn(0x40)))
        );

        Ok(())
    }

    #[tokio::test]
    async fn timeline_load() -> anyhow::Result<()> {
        const TEST_NAME: &str = "timeline_load";
        let harness = TenantHarness::create(TEST_NAME).await?;
        {
            let (tenant, ctx) = harness.load().await;
            let tline = tenant
                .create_test_timeline(TIMELINE_ID, Lsn(0x7000), DEFAULT_PG_VERSION, &ctx)
                .await?;
            make_some_layers(tline.as_ref(), Lsn(0x8000), &ctx).await?;
            // so that all uploads finish & we can call harness.load() below again
            tenant
                .shutdown(Default::default(), ShutdownMode::FreezeAndFlush)
                .instrument(harness.span())
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
        let harness = TenantHarness::create(TEST_NAME).await?;
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
                .shutdown(Default::default(), ShutdownMode::FreezeAndFlush)
                .instrument(harness.span())
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
        use storage_layer::AsLayerDesc;
        let (tenant, ctx) = TenantHarness::create("test_layer_dumping")
            .await?
            .load()
            .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        make_some_layers(tline.as_ref(), Lsn(0x20), &ctx).await?;

        let layer_map = tline.layers.read().await;
        let level0_deltas = layer_map
            .layer_map()?
            .level0_deltas()
            .iter()
            .map(|desc| layer_map.get_from_desc(desc))
            .collect::<Vec<_>>();

        assert!(!level0_deltas.is_empty());

        for delta in level0_deltas {
            // Ensure we are dumping a delta layer here
            assert!(delta.layer_desc().is_delta);
            delta.dump(true, &ctx).await.unwrap();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_images() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_images").await?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let mut writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x10),
                &Value::Image(test_img("foo at 0x10")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x10));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline
            .compact(&CancellationToken::new(), EnumSet::empty(), &ctx)
            .await?;

        let mut writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x20),
                &Value::Image(test_img("foo at 0x20")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x20));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline
            .compact(&CancellationToken::new(), EnumSet::empty(), &ctx)
            .await?;

        let mut writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x30),
                &Value::Image(test_img("foo at 0x30")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x30));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline
            .compact(&CancellationToken::new(), EnumSet::empty(), &ctx)
            .await?;

        let mut writer = tline.writer().await;
        writer
            .put(
                *TEST_KEY,
                Lsn(0x40),
                &Value::Image(test_img("foo at 0x40")),
                &ctx,
            )
            .await?;
        writer.finish_write(Lsn(0x40));
        drop(writer);

        tline.freeze_and_flush().await?;
        tline
            .compact(&CancellationToken::new(), EnumSet::empty(), &ctx)
            .await?;

        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x10), &ctx).await?,
            test_img("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x1f), &ctx).await?,
            test_img("foo at 0x10")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x20), &ctx).await?,
            test_img("foo at 0x20")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x30), &ctx).await?,
            test_img("foo at 0x30")
        );
        assert_eq!(
            tline.get(*TEST_KEY, Lsn(0x40), &ctx).await?,
            test_img("foo at 0x40")
        );

        Ok(())
    }

    async fn bulk_insert_compact_gc(
        tenant: &Tenant,
        timeline: &Arc<Timeline>,
        ctx: &RequestContext,
        lsn: Lsn,
        repeat: usize,
        key_count: usize,
    ) -> anyhow::Result<HashMap<Key, BTreeSet<Lsn>>> {
        let compact = true;
        bulk_insert_maybe_compact_gc(tenant, timeline, ctx, lsn, repeat, key_count, compact).await
    }

    async fn bulk_insert_maybe_compact_gc(
        tenant: &Tenant,
        timeline: &Arc<Timeline>,
        ctx: &RequestContext,
        mut lsn: Lsn,
        repeat: usize,
        key_count: usize,
        compact: bool,
    ) -> anyhow::Result<HashMap<Key, BTreeSet<Lsn>>> {
        let mut inserted: HashMap<Key, BTreeSet<Lsn>> = Default::default();

        let mut test_key = Key::from_hex("010000000033333333444444445500000000").unwrap();
        let mut blknum = 0;

        // Enforce that key range is monotonously increasing
        let mut keyspace = KeySpaceAccum::new();

        let cancel = CancellationToken::new();

        for _ in 0..repeat {
            for _ in 0..key_count {
                test_key.field6 = blknum;
                let mut writer = timeline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
                        ctx,
                    )
                    .await?;
                inserted.entry(test_key).or_default().insert(lsn);
                writer.finish_write(lsn);
                drop(writer);

                keyspace.add_key(test_key);

                lsn = Lsn(lsn.0 + 0x10);
                blknum += 1;
            }

            timeline.freeze_and_flush().await?;
            if compact {
                // this requires timeline to be &Arc<Timeline>
                timeline.compact(&cancel, EnumSet::empty(), ctx).await?;
            }

            // this doesn't really need to use the timeline_id target, but it is closer to what it
            // originally was.
            let res = tenant
                .gc_iteration(Some(timeline.timeline_id), 0, Duration::ZERO, &cancel, ctx)
                .await?;

            assert_eq!(res.layers_removed, 0, "this never removes anything");
        }

        Ok(inserted)
    }

    //
    // Insert 1000 key-value pairs with increasing keys, flush, compact, GC.
    // Repeat 50 times.
    //
    #[tokio::test]
    async fn test_bulk_insert() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_bulk_insert").await?;
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let lsn = Lsn(0x10);
        bulk_insert_compact_gc(&tenant, &tline, &ctx, lsn, 50, 10000).await?;

        Ok(())
    }

    // Test the vectored get real implementation against a simple sequential implementation.
    //
    // The test generates a keyspace by repeatedly flushing the in-memory layer and compacting.
    // Projected to 2D the key space looks like below. Lsn grows upwards on the Y axis and keys
    // grow to the right on the X axis.
    //                       [Delta]
    //                 [Delta]
    //           [Delta]
    //    [Delta]
    // ------------ Image ---------------
    //
    // After layer generation we pick the ranges to query as follows:
    // 1. The beginning of each delta layer
    // 2. At the seam between two adjacent delta layers
    //
    // There's one major downside to this test: delta layers only contains images,
    // so the search can stop at the first delta layer and doesn't traverse any deeper.
    #[tokio::test]
    async fn test_get_vectored() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_get_vectored").await?;
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let lsn = Lsn(0x10);
        let inserted = bulk_insert_compact_gc(&tenant, &tline, &ctx, lsn, 50, 10000).await?;

        let guard = tline.layers.read().await;
        let lm = guard.layer_map()?;

        lm.dump(true, &ctx).await?;

        let mut reads = Vec::new();
        let mut prev = None;
        lm.iter_historic_layers().for_each(|desc| {
            if !desc.is_delta() {
                prev = Some(desc.clone());
                return;
            }

            let start = desc.key_range.start;
            let end = desc
                .key_range
                .start
                .add(Timeline::MAX_GET_VECTORED_KEYS.try_into().unwrap());
            reads.push(KeySpace {
                ranges: vec![start..end],
            });

            if let Some(prev) = &prev {
                if !prev.is_delta() {
                    return;
                }

                let first_range = Key {
                    field6: prev.key_range.end.field6 - 4,
                    ..prev.key_range.end
                }..prev.key_range.end;

                let second_range = desc.key_range.start..Key {
                    field6: desc.key_range.start.field6 + 4,
                    ..desc.key_range.start
                };

                reads.push(KeySpace {
                    ranges: vec![first_range, second_range],
                });
            };

            prev = Some(desc.clone());
        });

        drop(guard);

        // Pick a big LSN such that we query over all the changes.
        let reads_lsn = Lsn(u64::MAX - 1);

        for read in reads {
            info!("Doing vectored read on {:?}", read);

            let vectored_res = tline
                .get_vectored_impl(
                    read.clone(),
                    reads_lsn,
                    &mut ValuesReconstructState::new(),
                    &ctx,
                )
                .await;

            let mut expected_lsns: HashMap<Key, Lsn> = Default::default();
            let mut expect_missing = false;
            let mut key = read.start().unwrap();
            while key != read.end().unwrap() {
                if let Some(lsns) = inserted.get(&key) {
                    let expected_lsn = lsns.iter().rfind(|lsn| **lsn <= reads_lsn);
                    match expected_lsn {
                        Some(lsn) => {
                            expected_lsns.insert(key, *lsn);
                        }
                        None => {
                            expect_missing = true;
                            break;
                        }
                    }
                } else {
                    expect_missing = true;
                    break;
                }

                key = key.next();
            }

            if expect_missing {
                assert!(matches!(vectored_res, Err(GetVectoredError::MissingKey(_))));
            } else {
                for (key, image) in vectored_res? {
                    let expected_lsn = expected_lsns.get(&key).expect("determined above");
                    let expected_image = test_img(&format!("{} at {}", key.field6, expected_lsn));
                    assert_eq!(image?, expected_image);
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_vectored_aux_files() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_get_vectored_aux_files").await?;

        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let tline = tline.raw_timeline().unwrap();

        let mut modification = tline.begin_modification(Lsn(0x1000));
        modification.put_file("foo/bar1", b"content1", &ctx).await?;
        modification.set_lsn(Lsn(0x1008))?;
        modification.put_file("foo/bar2", b"content2", &ctx).await?;
        modification.commit(&ctx).await?;

        let child_timeline_id = TimelineId::generate();
        tenant
            .branch_timeline_test(
                tline,
                child_timeline_id,
                Some(tline.get_last_record_lsn()),
                &ctx,
            )
            .await?;

        let child_timeline = tenant
            .get_timeline(child_timeline_id, true)
            .expect("Should have the branched timeline");

        let aux_keyspace = KeySpace {
            ranges: vec![NON_INHERITED_RANGE],
        };
        let read_lsn = child_timeline.get_last_record_lsn();

        let vectored_res = child_timeline
            .get_vectored_impl(
                aux_keyspace.clone(),
                read_lsn,
                &mut ValuesReconstructState::new(),
                &ctx,
            )
            .await;

        let images = vectored_res?;
        assert!(images.is_empty());
        Ok(())
    }

    // Test that vectored get handles layer gaps correctly
    // by advancing into the next ancestor timeline if required.
    //
    // The test generates timelines that look like the diagram below.
    // We leave a gap in one of the L1 layers at `gap_at_key` (`/` in the diagram).
    // The reconstruct data for that key lies in the ancestor timeline (`X` in the diagram).
    //
    // ```
    //-------------------------------+
    //                          ...  |
    //               [   L1   ]      |
    //     [ / L1   ]                | Child Timeline
    // ...                           |
    // ------------------------------+
    //     [ X L1   ]                | Parent Timeline
    // ------------------------------+
    // ```
    #[tokio::test]
    async fn test_get_vectored_key_gap() -> anyhow::Result<()> {
        let tenant_conf = TenantConf {
            // Make compaction deterministic
            gc_period: Duration::ZERO,
            compaction_period: Duration::ZERO,
            // Encourage creation of L1 layers
            checkpoint_distance: 16 * 1024,
            compaction_target_size: 8 * 1024,
            ..TenantConf::default()
        };

        let harness = TenantHarness::create_custom(
            "test_get_vectored_key_gap",
            tenant_conf,
            TenantId::generate(),
            ShardIdentity::unsharded(),
            Generation::new(0xdeadbeef),
        )
        .await?;
        let (tenant, ctx) = harness.load().await;

        let mut current_key = Key::from_hex("010000000033333333444444445500000000").unwrap();
        let gap_at_key = current_key.add(100);
        let mut current_lsn = Lsn(0x10);

        const KEY_COUNT: usize = 10_000;

        let timeline_id = TimelineId::generate();
        let current_timeline = tenant
            .create_test_timeline(timeline_id, current_lsn, DEFAULT_PG_VERSION, &ctx)
            .await?;

        current_lsn += 0x100;

        let mut writer = current_timeline.writer().await;
        writer
            .put(
                gap_at_key,
                current_lsn,
                &Value::Image(test_img(&format!("{} at {}", gap_at_key, current_lsn))),
                &ctx,
            )
            .await?;
        writer.finish_write(current_lsn);
        drop(writer);

        let mut latest_lsns = HashMap::new();
        latest_lsns.insert(gap_at_key, current_lsn);

        current_timeline.freeze_and_flush().await?;

        let child_timeline_id = TimelineId::generate();

        tenant
            .branch_timeline_test(
                &current_timeline,
                child_timeline_id,
                Some(current_lsn),
                &ctx,
            )
            .await?;
        let child_timeline = tenant
            .get_timeline(child_timeline_id, true)
            .expect("Should have the branched timeline");

        for i in 0..KEY_COUNT {
            if current_key == gap_at_key {
                current_key = current_key.next();
                continue;
            }

            current_lsn += 0x10;

            let mut writer = child_timeline.writer().await;
            writer
                .put(
                    current_key,
                    current_lsn,
                    &Value::Image(test_img(&format!("{} at {}", current_key, current_lsn))),
                    &ctx,
                )
                .await?;
            writer.finish_write(current_lsn);
            drop(writer);

            latest_lsns.insert(current_key, current_lsn);
            current_key = current_key.next();

            // Flush every now and then to encourage layer file creation.
            if i % 500 == 0 {
                child_timeline.freeze_and_flush().await?;
            }
        }

        child_timeline.freeze_and_flush().await?;
        let mut flags = EnumSet::new();
        flags.insert(CompactFlags::ForceRepartition);
        child_timeline
            .compact(&CancellationToken::new(), flags, &ctx)
            .await?;

        let key_near_end = {
            let mut tmp = current_key;
            tmp.field6 -= 10;
            tmp
        };

        let key_near_gap = {
            let mut tmp = gap_at_key;
            tmp.field6 -= 10;
            tmp
        };

        let read = KeySpace {
            ranges: vec![key_near_gap..gap_at_key.next(), key_near_end..current_key],
        };
        let results = child_timeline
            .get_vectored_impl(
                read.clone(),
                current_lsn,
                &mut ValuesReconstructState::new(),
                &ctx,
            )
            .await?;

        for (key, img_res) in results {
            let expected = test_img(&format!("{} at {}", key, latest_lsns[&key]));
            assert_eq!(img_res?, expected);
        }

        Ok(())
    }

    // Test that vectored get descends into ancestor timelines correctly and
    // does not return an image that's newer than requested.
    //
    // The diagram below ilustrates an interesting case. We have a parent timeline
    // (top of the Lsn range) and a child timeline. The request key cannot be reconstructed
    // from the child timeline, so the parent timeline must be visited. When advacing into
    // the child timeline, the read path needs to remember what the requested Lsn was in
    // order to avoid returning an image that's too new. The test below constructs such
    // a timeline setup and does a few queries around the Lsn of each page image.
    // ```
    //    LSN
    //     ^
    //     |
    //     |
    // 500 | --------------------------------------> branch point
    // 400 |        X
    // 300 |        X
    // 200 | --------------------------------------> requested lsn
    // 100 |        X
    //     |---------------------------------------> Key
    //              |
    //              ------> requested key
    //
    // Legend:
    // * X - page images
    // ```
    #[tokio::test]
    async fn test_get_vectored_ancestor_descent() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_get_vectored_on_lsn_axis").await?;
        let (tenant, ctx) = harness.load().await;

        let start_key = Key::from_hex("010000000033333333444444445500000000").unwrap();
        let end_key = start_key.add(1000);
        let child_gap_at_key = start_key.add(500);
        let mut parent_gap_lsns: BTreeMap<Lsn, String> = BTreeMap::new();

        let mut current_lsn = Lsn(0x10);

        let timeline_id = TimelineId::generate();
        let parent_timeline = tenant
            .create_test_timeline(timeline_id, current_lsn, DEFAULT_PG_VERSION, &ctx)
            .await?;

        current_lsn += 0x100;

        for _ in 0..3 {
            let mut key = start_key;
            while key < end_key {
                current_lsn += 0x10;

                let image_value = format!("{} at {}", child_gap_at_key, current_lsn);

                let mut writer = parent_timeline.writer().await;
                writer
                    .put(
                        key,
                        current_lsn,
                        &Value::Image(test_img(&image_value)),
                        &ctx,
                    )
                    .await?;
                writer.finish_write(current_lsn);

                if key == child_gap_at_key {
                    parent_gap_lsns.insert(current_lsn, image_value);
                }

                key = key.next();
            }

            parent_timeline.freeze_and_flush().await?;
        }

        let child_timeline_id = TimelineId::generate();

        let child_timeline = tenant
            .branch_timeline_test(&parent_timeline, child_timeline_id, Some(current_lsn), &ctx)
            .await?;

        let mut key = start_key;
        while key < end_key {
            if key == child_gap_at_key {
                key = key.next();
                continue;
            }

            current_lsn += 0x10;

            let mut writer = child_timeline.writer().await;
            writer
                .put(
                    key,
                    current_lsn,
                    &Value::Image(test_img(&format!("{} at {}", key, current_lsn))),
                    &ctx,
                )
                .await?;
            writer.finish_write(current_lsn);

            key = key.next();
        }

        child_timeline.freeze_and_flush().await?;

        let lsn_offsets: [i64; 5] = [-10, -1, 0, 1, 10];
        let mut query_lsns = Vec::new();
        for image_lsn in parent_gap_lsns.keys().rev() {
            for offset in lsn_offsets {
                query_lsns.push(Lsn(image_lsn
                    .0
                    .checked_add_signed(offset)
                    .expect("Shouldn't overflow")));
            }
        }

        for query_lsn in query_lsns {
            let results = child_timeline
                .get_vectored_impl(
                    KeySpace {
                        ranges: vec![child_gap_at_key..child_gap_at_key.next()],
                    },
                    query_lsn,
                    &mut ValuesReconstructState::new(),
                    &ctx,
                )
                .await;

            let expected_item = parent_gap_lsns
                .iter()
                .rev()
                .find(|(lsn, _)| **lsn <= query_lsn);

            info!(
                "Doing vectored read at LSN {}. Expecting image to be: {:?}",
                query_lsn, expected_item
            );

            match expected_item {
                Some((_, img_value)) => {
                    let key_results = results.expect("No vectored get error expected");
                    let key_result = &key_results[&child_gap_at_key];
                    let returned_img = key_result
                        .as_ref()
                        .expect("No page reconstruct error expected");

                    info!(
                        "Vectored read at LSN {} returned image {}",
                        query_lsn,
                        std::str::from_utf8(returned_img)?
                    );
                    assert_eq!(*returned_img, test_img(img_value));
                }
                None => {
                    assert!(matches!(results, Err(GetVectoredError::MissingKey(_))));
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_random_updates() -> anyhow::Result<()> {
        let names_algorithms = [
            ("test_random_updates_legacy", CompactionAlgorithm::Legacy),
            ("test_random_updates_tiered", CompactionAlgorithm::Tiered),
        ];
        for (name, algorithm) in names_algorithms {
            test_random_updates_algorithm(name, algorithm).await?;
        }
        Ok(())
    }

    async fn test_random_updates_algorithm(
        name: &'static str,
        compaction_algorithm: CompactionAlgorithm,
    ) -> anyhow::Result<()> {
        let mut harness = TenantHarness::create(name).await?;
        harness.tenant_conf.compaction_algorithm = CompactionAlgorithmSettings {
            kind: compaction_algorithm,
        };
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 1000;
        let cancel = CancellationToken::new();

        let mut test_key = Key::from_hex("010000000033333333444444445500000000").unwrap();
        let mut test_key_end = test_key;
        test_key_end.field6 = NUM_KEYS as u32;
        tline.add_extra_test_dense_keyspace(KeySpace::single(test_key..test_key_end));

        let mut keyspace = KeySpaceAccum::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0x10);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let mut writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
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
                let mut writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
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
                    test_img(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of flush, and GC
            tline.freeze_and_flush().await?;
            tenant
                .gc_iteration(Some(tline.timeline_id), 0, Duration::ZERO, &cancel, &ctx)
                .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_branches() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_traverse_branches")
            .await?
            .load()
            .await;
        let mut tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 1000;

        let mut test_key = Key::from_hex("010000000033333333444444445500000000").unwrap();

        let mut keyspace = KeySpaceAccum::new();

        let cancel = CancellationToken::new();

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0x10);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = blknum as u32;
            let mut writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
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
                let mut writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
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
                    test_img(&format!("{} at {}", blknum, last_lsn))
                );
            }

            // Perform a cycle of flush, compact, and GC
            tline.freeze_and_flush().await?;
            tline.compact(&cancel, EnumSet::empty(), &ctx).await?;
            tenant
                .gc_iteration(Some(tline.timeline_id), 0, Duration::ZERO, &cancel, &ctx)
                .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_traverse_ancestors() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_traverse_ancestors")
            .await?
            .load()
            .await;
        let mut tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 100;
        const NUM_TLINES: usize = 50;

        let mut test_key = Key::from_hex("010000000033333333444444445500000000").unwrap();
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
                let mut writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(test_img(&format!("{} {} at {}", idx, blknum, lsn))),
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
                    test_img(&format!("{idx} {blknum} at {lsn}"))
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_write_at_initdb_lsn_takes_optimization_code_path() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_empty_test_timeline_is_usable")
            .await?
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
    async fn test_create_guard_crash() -> anyhow::Result<()> {
        let name = "test_create_guard_crash";
        let harness = TenantHarness::create(name).await?;
        {
            let (tenant, ctx) = harness.load().await;
            let tline = tenant
                .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION, &ctx)
                .await?;
            // Leave the timeline ID in [`Tenant::timelines_creating`] to exclude attempting to create it again
            let raw_tline = tline.raw_timeline().unwrap();
            raw_tline
                .shutdown(super::timeline::ShutdownMode::Hard)
                .instrument(info_span!("test_shutdown", tenant_id=%raw_tline.tenant_shard_id, shard_id=%raw_tline.tenant_shard_id.shard_slug(), timeline_id=%TIMELINE_ID))
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
                        tenant_id: tenant.tenant_shard_id,
                        timeline_id: TIMELINE_ID,
                    }
                )
            }
        }

        assert!(!harness
            .conf
            .timeline_path(&tenant.tenant_shard_id, &TIMELINE_ID)
            .exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_read_at_max_lsn() -> anyhow::Result<()> {
        let names_algorithms = [
            ("test_read_at_max_lsn_legacy", CompactionAlgorithm::Legacy),
            ("test_read_at_max_lsn_tiered", CompactionAlgorithm::Tiered),
        ];
        for (name, algorithm) in names_algorithms {
            test_read_at_max_lsn_algorithm(name, algorithm).await?;
        }
        Ok(())
    }

    async fn test_read_at_max_lsn_algorithm(
        name: &'static str,
        compaction_algorithm: CompactionAlgorithm,
    ) -> anyhow::Result<()> {
        let mut harness = TenantHarness::create(name).await?;
        harness.tenant_conf.compaction_algorithm = CompactionAlgorithmSettings {
            kind: compaction_algorithm,
        };
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x08), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let lsn = Lsn(0x10);
        let compact = false;
        bulk_insert_maybe_compact_gc(&tenant, &tline, &ctx, lsn, 50, 10000, compact).await?;

        let test_key = Key::from_hex("010000000033333333444444445500000000").unwrap();
        let read_lsn = Lsn(u64::MAX - 1);

        let result = tline.get(test_key, read_lsn, &ctx).await;
        assert!(result.is_ok(), "result is not Ok: {}", result.unwrap_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_scan() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_metadata_scan").await?;
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 1000;
        const STEP: usize = 10000; // random update + scan base_key + idx * STEP

        let cancel = CancellationToken::new();

        let mut base_key = Key::from_hex("000000000033333333444444445500000000").unwrap();
        base_key.field1 = AUX_KEY_PREFIX;
        let mut test_key = base_key;

        // Track when each page was last modified. Used to assert that
        // a read sees the latest page version.
        let mut updated = [Lsn(0); NUM_KEYS];

        let mut lsn = Lsn(0x10);
        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = (blknum * STEP) as u32;
            let mut writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
                    &ctx,
                )
                .await?;
            writer.finish_write(lsn);
            updated[blknum] = lsn;
            drop(writer);
        }

        let keyspace = KeySpace::single(base_key..base_key.add((NUM_KEYS * STEP) as u32));

        for iter in 0..=10 {
            // Read all the blocks
            for (blknum, last_lsn) in updated.iter().enumerate() {
                test_key.field6 = (blknum * STEP) as u32;
                assert_eq!(
                    tline.get(test_key, lsn, &ctx).await?,
                    test_img(&format!("{} at {}", blknum, last_lsn))
                );
            }

            let mut cnt = 0;
            for (key, value) in tline
                .get_vectored_impl(
                    keyspace.clone(),
                    lsn,
                    &mut ValuesReconstructState::default(),
                    &ctx,
                )
                .await?
            {
                let blknum = key.field6 as usize;
                let value = value?;
                assert!(blknum % STEP == 0);
                let blknum = blknum / STEP;
                assert_eq!(
                    value,
                    test_img(&format!("{} at {}", blknum, updated[blknum]))
                );
                cnt += 1;
            }

            assert_eq!(cnt, NUM_KEYS);

            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = (blknum * STEP) as u32;
                let mut writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
                        &ctx,
                    )
                    .await?;
                writer.finish_write(lsn);
                drop(writer);
                updated[blknum] = lsn;
            }

            // Perform two cycles of flush, compact, and GC
            for round in 0..2 {
                tline.freeze_and_flush().await?;
                tline
                    .compact(
                        &cancel,
                        if iter % 5 == 0 && round == 0 {
                            let mut flags = EnumSet::new();
                            flags.insert(CompactFlags::ForceImageLayerCreation);
                            flags.insert(CompactFlags::ForceRepartition);
                            flags
                        } else {
                            EnumSet::empty()
                        },
                        &ctx,
                    )
                    .await?;
                tenant
                    .gc_iteration(Some(tline.timeline_id), 0, Duration::ZERO, &cancel, &ctx)
                    .await?;
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_compaction_trigger() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_metadata_compaction_trigger").await?;
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        let cancel = CancellationToken::new();

        let mut base_key = Key::from_hex("000000000033333333444444445500000000").unwrap();
        base_key.field1 = AUX_KEY_PREFIX;
        let test_key = base_key;
        let mut lsn = Lsn(0x10);

        for _ in 0..20 {
            lsn = Lsn(lsn.0 + 0x10);
            let mut writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(test_img(&format!("{} at {}", 0, lsn))),
                    &ctx,
                )
                .await?;
            writer.finish_write(lsn);
            drop(writer);
            tline.freeze_and_flush().await?; // force create a delta layer
        }

        let before_num_l0_delta_files =
            tline.layers.read().await.layer_map()?.level0_deltas().len();

        tline.compact(&cancel, EnumSet::empty(), &ctx).await?;

        let after_num_l0_delta_files = tline.layers.read().await.layer_map()?.level0_deltas().len();

        assert!(after_num_l0_delta_files < before_num_l0_delta_files, "after_num_l0_delta_files={after_num_l0_delta_files}, before_num_l0_delta_files={before_num_l0_delta_files}");

        assert_eq!(
            tline.get(test_key, lsn, &ctx).await?,
            test_img(&format!("{} at {}", 0, lsn))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aux_file_e2e() {
        let harness = TenantHarness::create("test_aux_file_e2e").await.unwrap();

        let (tenant, ctx) = harness.load().await;

        let mut lsn = Lsn(0x08);

        let tline: Arc<Timeline> = tenant
            .create_test_timeline(TIMELINE_ID, lsn, DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        {
            lsn += 8;
            let mut modification = tline.begin_modification(lsn);
            modification
                .put_file("pg_logical/mappings/test1", b"first", &ctx)
                .await
                .unwrap();
            modification.commit(&ctx).await.unwrap();
        }

        // we can read everything from the storage
        let files = tline.list_aux_files(lsn, &ctx).await.unwrap();
        assert_eq!(
            files.get("pg_logical/mappings/test1"),
            Some(&bytes::Bytes::from_static(b"first"))
        );

        {
            lsn += 8;
            let mut modification = tline.begin_modification(lsn);
            modification
                .put_file("pg_logical/mappings/test2", b"second", &ctx)
                .await
                .unwrap();
            modification.commit(&ctx).await.unwrap();
        }

        let files = tline.list_aux_files(lsn, &ctx).await.unwrap();
        assert_eq!(
            files.get("pg_logical/mappings/test2"),
            Some(&bytes::Bytes::from_static(b"second"))
        );

        let child = tenant
            .branch_timeline_test(&tline, NEW_TIMELINE_ID, Some(lsn), &ctx)
            .await
            .unwrap();

        let files = child.list_aux_files(lsn, &ctx).await.unwrap();
        assert_eq!(files.get("pg_logical/mappings/test1"), None);
        assert_eq!(files.get("pg_logical/mappings/test2"), None);
    }

    #[tokio::test]
    async fn test_metadata_image_creation() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_metadata_image_creation").await?;
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;

        const NUM_KEYS: usize = 1000;
        const STEP: usize = 10000; // random update + scan base_key + idx * STEP

        let cancel = CancellationToken::new();

        let base_key = Key::from_hex("620000000033333333444444445500000000").unwrap();
        assert_eq!(base_key.field1, AUX_KEY_PREFIX); // in case someone accidentally changed the prefix...
        let mut test_key = base_key;
        let mut lsn = Lsn(0x10);

        async fn scan_with_statistics(
            tline: &Timeline,
            keyspace: &KeySpace,
            lsn: Lsn,
            ctx: &RequestContext,
        ) -> anyhow::Result<(BTreeMap<Key, Result<Bytes, PageReconstructError>>, usize)> {
            let mut reconstruct_state = ValuesReconstructState::default();
            let res = tline
                .get_vectored_impl(keyspace.clone(), lsn, &mut reconstruct_state, ctx)
                .await?;
            Ok((res, reconstruct_state.get_delta_layers_visited() as usize))
        }

        #[allow(clippy::needless_range_loop)]
        for blknum in 0..NUM_KEYS {
            lsn = Lsn(lsn.0 + 0x10);
            test_key.field6 = (blknum * STEP) as u32;
            let mut writer = tline.writer().await;
            writer
                .put(
                    test_key,
                    lsn,
                    &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
                    &ctx,
                )
                .await?;
            writer.finish_write(lsn);
            drop(writer);
        }

        let keyspace = KeySpace::single(base_key..base_key.add((NUM_KEYS * STEP) as u32));

        for iter in 1..=10 {
            for _ in 0..NUM_KEYS {
                lsn = Lsn(lsn.0 + 0x10);
                let blknum = thread_rng().gen_range(0..NUM_KEYS);
                test_key.field6 = (blknum * STEP) as u32;
                let mut writer = tline.writer().await;
                writer
                    .put(
                        test_key,
                        lsn,
                        &Value::Image(test_img(&format!("{} at {}", blknum, lsn))),
                        &ctx,
                    )
                    .await?;
                writer.finish_write(lsn);
                drop(writer);
            }

            tline.freeze_and_flush().await?;

            if iter % 5 == 0 {
                let (_, before_delta_file_accessed) =
                    scan_with_statistics(&tline, &keyspace, lsn, &ctx).await?;
                tline
                    .compact(
                        &cancel,
                        {
                            let mut flags = EnumSet::new();
                            flags.insert(CompactFlags::ForceImageLayerCreation);
                            flags.insert(CompactFlags::ForceRepartition);
                            flags
                        },
                        &ctx,
                    )
                    .await?;
                let (_, after_delta_file_accessed) =
                    scan_with_statistics(&tline, &keyspace, lsn, &ctx).await?;
                assert!(after_delta_file_accessed < before_delta_file_accessed, "after_delta_file_accessed={after_delta_file_accessed}, before_delta_file_accessed={before_delta_file_accessed}");
                // Given that we already produced an image layer, there should be no delta layer needed for the scan, but still setting a low threshold there for unforeseen circumstances.
                assert!(
                    after_delta_file_accessed <= 2,
                    "after_delta_file_accessed={after_delta_file_accessed}"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_vectored_missing_data_key_reads() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_vectored_missing_data_key_reads").await?;
        let (tenant, ctx) = harness.load().await;

        let base_key = Key::from_hex("000000000033333333444444445500000000").unwrap();
        let base_key_child = Key::from_hex("000000000033333333444444445500000001").unwrap();
        let base_key_nonexist = Key::from_hex("000000000033333333444444445500000002").unwrap();

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                Vec::new(), // delta layers
                vec![(Lsn(0x20), vec![(base_key, test_img("data key 1"))])], // image layers
                Lsn(0x20), // it's fine to not advance LSN to 0x30 while using 0x30 to get below because `get_vectored_impl` does not wait for LSN
            )
            .await?;
        tline.add_extra_test_dense_keyspace(KeySpace::single(base_key..(base_key_nonexist.next())));

        let child = tenant
            .branch_timeline_test_with_layers(
                &tline,
                NEW_TIMELINE_ID,
                Some(Lsn(0x20)),
                &ctx,
                Vec::new(), // delta layers
                vec![(Lsn(0x30), vec![(base_key_child, test_img("data key 2"))])], // image layers
                Lsn(0x30),
            )
            .await
            .unwrap();

        let lsn = Lsn(0x30);

        // test vectored get on parent timeline
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_key, lsn, &ctx).await?,
            Some(test_img("data key 1"))
        );
        assert!(get_vectored_impl_wrapper(&tline, base_key_child, lsn, &ctx)
            .await
            .unwrap_err()
            .is_missing_key_error());
        assert!(
            get_vectored_impl_wrapper(&tline, base_key_nonexist, lsn, &ctx)
                .await
                .unwrap_err()
                .is_missing_key_error()
        );

        // test vectored get on child timeline
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_key, lsn, &ctx).await?,
            Some(test_img("data key 1"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_key_child, lsn, &ctx).await?,
            Some(test_img("data key 2"))
        );
        assert!(
            get_vectored_impl_wrapper(&child, base_key_nonexist, lsn, &ctx)
                .await
                .unwrap_err()
                .is_missing_key_error()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_vectored_missing_metadata_key_reads() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_vectored_missing_metadata_key_reads").await?;
        let (tenant, ctx) = harness.load().await;

        let base_key = Key::from_hex("620000000033333333444444445500000000").unwrap();
        let base_key_child = Key::from_hex("620000000033333333444444445500000001").unwrap();
        let base_key_nonexist = Key::from_hex("620000000033333333444444445500000002").unwrap();
        let base_key_overwrite = Key::from_hex("620000000033333333444444445500000003").unwrap();

        let base_inherited_key = Key::from_hex("610000000033333333444444445500000000").unwrap();
        let base_inherited_key_child =
            Key::from_hex("610000000033333333444444445500000001").unwrap();
        let base_inherited_key_nonexist =
            Key::from_hex("610000000033333333444444445500000002").unwrap();
        let base_inherited_key_overwrite =
            Key::from_hex("610000000033333333444444445500000003").unwrap();

        assert_eq!(base_key.field1, AUX_KEY_PREFIX); // in case someone accidentally changed the prefix...
        assert_eq!(base_inherited_key.field1, RELATION_SIZE_PREFIX);

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                Vec::new(), // delta layers
                vec![(
                    Lsn(0x20),
                    vec![
                        (base_inherited_key, test_img("metadata inherited key 1")),
                        (
                            base_inherited_key_overwrite,
                            test_img("metadata key overwrite 1a"),
                        ),
                        (base_key, test_img("metadata key 1")),
                        (base_key_overwrite, test_img("metadata key overwrite 1b")),
                    ],
                )], // image layers
                Lsn(0x20), // it's fine to not advance LSN to 0x30 while using 0x30 to get below because `get_vectored_impl` does not wait for LSN
            )
            .await?;

        let child = tenant
            .branch_timeline_test_with_layers(
                &tline,
                NEW_TIMELINE_ID,
                Some(Lsn(0x20)),
                &ctx,
                Vec::new(), // delta layers
                vec![(
                    Lsn(0x30),
                    vec![
                        (
                            base_inherited_key_child,
                            test_img("metadata inherited key 2"),
                        ),
                        (
                            base_inherited_key_overwrite,
                            test_img("metadata key overwrite 2a"),
                        ),
                        (base_key_child, test_img("metadata key 2")),
                        (base_key_overwrite, test_img("metadata key overwrite 2b")),
                    ],
                )], // image layers
                Lsn(0x30),
            )
            .await
            .unwrap();

        let lsn = Lsn(0x30);

        // test vectored get on parent timeline
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_key, lsn, &ctx).await?,
            Some(test_img("metadata key 1"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_key_child, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_key_nonexist, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_key_overwrite, lsn, &ctx).await?,
            Some(test_img("metadata key overwrite 1b"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_inherited_key, lsn, &ctx).await?,
            Some(test_img("metadata inherited key 1"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_inherited_key_child, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_inherited_key_nonexist, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, base_inherited_key_overwrite, lsn, &ctx).await?,
            Some(test_img("metadata key overwrite 1a"))
        );

        // test vectored get on child timeline
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_key, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_key_child, lsn, &ctx).await?,
            Some(test_img("metadata key 2"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_key_nonexist, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_inherited_key, lsn, &ctx).await?,
            Some(test_img("metadata inherited key 1"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_inherited_key_child, lsn, &ctx).await?,
            Some(test_img("metadata inherited key 2"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_inherited_key_nonexist, lsn, &ctx).await?,
            None
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_key_overwrite, lsn, &ctx).await?,
            Some(test_img("metadata key overwrite 2b"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&child, base_inherited_key_overwrite, lsn, &ctx).await?,
            Some(test_img("metadata key overwrite 2a"))
        );

        // test vectored scan on parent timeline
        let mut reconstruct_state = ValuesReconstructState::new();
        let res = tline
            .get_vectored_impl(
                KeySpace::single(Key::metadata_key_range()),
                lsn,
                &mut reconstruct_state,
                &ctx,
            )
            .await?;

        assert_eq!(
            res.into_iter()
                .map(|(k, v)| (k, v.unwrap()))
                .collect::<Vec<_>>(),
            vec![
                (base_inherited_key, test_img("metadata inherited key 1")),
                (
                    base_inherited_key_overwrite,
                    test_img("metadata key overwrite 1a")
                ),
                (base_key, test_img("metadata key 1")),
                (base_key_overwrite, test_img("metadata key overwrite 1b")),
            ]
        );

        // test vectored scan on child timeline
        let mut reconstruct_state = ValuesReconstructState::new();
        let res = child
            .get_vectored_impl(
                KeySpace::single(Key::metadata_key_range()),
                lsn,
                &mut reconstruct_state,
                &ctx,
            )
            .await?;

        assert_eq!(
            res.into_iter()
                .map(|(k, v)| (k, v.unwrap()))
                .collect::<Vec<_>>(),
            vec![
                (base_inherited_key, test_img("metadata inherited key 1")),
                (
                    base_inherited_key_child,
                    test_img("metadata inherited key 2")
                ),
                (
                    base_inherited_key_overwrite,
                    test_img("metadata key overwrite 2a")
                ),
                (base_key_child, test_img("metadata key 2")),
                (base_key_overwrite, test_img("metadata key overwrite 2b")),
            ]
        );

        Ok(())
    }

    async fn get_vectored_impl_wrapper(
        tline: &Arc<Timeline>,
        key: Key,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Option<Bytes>, GetVectoredError> {
        let mut reconstruct_state = ValuesReconstructState::new();
        let mut res = tline
            .get_vectored_impl(
                KeySpace::single(key..key.next()),
                lsn,
                &mut reconstruct_state,
                ctx,
            )
            .await?;
        Ok(res.pop_last().map(|(k, v)| {
            assert_eq!(k, key);
            v.unwrap()
        }))
    }

    #[tokio::test]
    async fn test_metadata_tombstone_reads() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_metadata_tombstone_reads").await?;
        let (tenant, ctx) = harness.load().await;
        let key0 = Key::from_hex("620000000033333333444444445500000000").unwrap();
        let key1 = Key::from_hex("620000000033333333444444445500000001").unwrap();
        let key2 = Key::from_hex("620000000033333333444444445500000002").unwrap();
        let key3 = Key::from_hex("620000000033333333444444445500000003").unwrap();

        // We emulate the situation that the compaction algorithm creates an image layer that removes the tombstones
        // Lsn 0x30 key0, key3, no key1+key2
        // Lsn 0x20 key1+key2 tomestones
        // Lsn 0x10 key1 in image, key2 in delta
        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                // delta layers
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x10)..Lsn(0x20),
                        vec![(key2, Lsn(0x10), Value::Image(test_img("metadata key 2")))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x20)..Lsn(0x30),
                        vec![(key1, Lsn(0x20), Value::Image(Bytes::new()))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x20)..Lsn(0x30),
                        vec![(key2, Lsn(0x20), Value::Image(Bytes::new()))],
                    ),
                ],
                // image layers
                vec![
                    (Lsn(0x10), vec![(key1, test_img("metadata key 1"))]),
                    (
                        Lsn(0x30),
                        vec![
                            (key0, test_img("metadata key 0")),
                            (key3, test_img("metadata key 3")),
                        ],
                    ),
                ],
                Lsn(0x30),
            )
            .await?;

        let lsn = Lsn(0x30);
        let old_lsn = Lsn(0x20);

        assert_eq!(
            get_vectored_impl_wrapper(&tline, key0, lsn, &ctx).await?,
            Some(test_img("metadata key 0"))
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, key1, lsn, &ctx).await?,
            None,
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, key2, lsn, &ctx).await?,
            None,
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, key1, old_lsn, &ctx).await?,
            Some(Bytes::new()),
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, key2, old_lsn, &ctx).await?,
            Some(Bytes::new()),
        );
        assert_eq!(
            get_vectored_impl_wrapper(&tline, key3, lsn, &ctx).await?,
            Some(test_img("metadata key 3"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_tombstone_image_creation() {
        let harness = TenantHarness::create("test_metadata_tombstone_image_creation")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let key0 = Key::from_hex("620000000033333333444444445500000000").unwrap();
        let key1 = Key::from_hex("620000000033333333444444445500000001").unwrap();
        let key2 = Key::from_hex("620000000033333333444444445500000002").unwrap();
        let key3 = Key::from_hex("620000000033333333444444445500000003").unwrap();

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                // delta layers
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x10)..Lsn(0x20),
                        vec![(key2, Lsn(0x10), Value::Image(test_img("metadata key 2")))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x20)..Lsn(0x30),
                        vec![(key1, Lsn(0x20), Value::Image(Bytes::new()))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x20)..Lsn(0x30),
                        vec![(key2, Lsn(0x20), Value::Image(Bytes::new()))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x30)..Lsn(0x40),
                        vec![
                            (key0, Lsn(0x30), Value::Image(test_img("metadata key 0"))),
                            (key3, Lsn(0x30), Value::Image(test_img("metadata key 3"))),
                        ],
                    ),
                ],
                // image layers
                vec![(Lsn(0x10), vec![(key1, test_img("metadata key 1"))])],
                Lsn(0x40),
            )
            .await
            .unwrap();

        let cancel = CancellationToken::new();

        tline
            .compact(
                &cancel,
                {
                    let mut flags = EnumSet::new();
                    flags.insert(CompactFlags::ForceImageLayerCreation);
                    flags.insert(CompactFlags::ForceRepartition);
                    flags
                },
                &ctx,
            )
            .await
            .unwrap();

        // Image layers are created at last_record_lsn
        let images = tline
            .inspect_image_layers(Lsn(0x40), &ctx)
            .await
            .unwrap()
            .into_iter()
            .filter(|(k, _)| k.is_metadata_key())
            .collect::<Vec<_>>();
        assert_eq!(images.len(), 2); // the image layer should only contain two existing keys, tombstones should be removed.
    }

    #[tokio::test]
    async fn test_metadata_tombstone_empty_image_creation() {
        let harness = TenantHarness::create("test_metadata_tombstone_empty_image_creation")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let key1 = Key::from_hex("620000000033333333444444445500000001").unwrap();
        let key2 = Key::from_hex("620000000033333333444444445500000002").unwrap();

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                // delta layers
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x10)..Lsn(0x20),
                        vec![(key2, Lsn(0x10), Value::Image(test_img("metadata key 2")))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x20)..Lsn(0x30),
                        vec![(key1, Lsn(0x20), Value::Image(Bytes::new()))],
                    ),
                    DeltaLayerTestDesc::new_with_inferred_key_range(
                        Lsn(0x20)..Lsn(0x30),
                        vec![(key2, Lsn(0x20), Value::Image(Bytes::new()))],
                    ),
                ],
                // image layers
                vec![(Lsn(0x10), vec![(key1, test_img("metadata key 1"))])],
                Lsn(0x30),
            )
            .await
            .unwrap();

        let cancel = CancellationToken::new();

        tline
            .compact(
                &cancel,
                {
                    let mut flags = EnumSet::new();
                    flags.insert(CompactFlags::ForceImageLayerCreation);
                    flags.insert(CompactFlags::ForceRepartition);
                    flags
                },
                &ctx,
            )
            .await
            .unwrap();

        // Image layers are created at last_record_lsn
        let images = tline
            .inspect_image_layers(Lsn(0x30), &ctx)
            .await
            .unwrap()
            .into_iter()
            .filter(|(k, _)| k.is_metadata_key())
            .collect::<Vec<_>>();
        assert_eq!(images.len(), 0); // the image layer should not contain tombstones, or it is not created
    }

    #[tokio::test]
    async fn test_simple_bottom_most_compaction_images() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_simple_bottom_most_compaction_images").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        // We create
        // - one bottom-most image layer,
        // - a delta layer D1 crossing the GC horizon with data below and above the horizon,
        // - a delta layer D2 crossing the GC horizon with data only below the horizon,
        // - a delta layer D3 above the horizon.
        //
        //                             | D3 |
        //  | D1 |
        // -|    |-- gc horizon -----------------
        //  |    |                | D2 |
        // --------- img layer ------------------
        //
        // What we should expact from this compaction is:
        //                             | D3 |
        //  | Part of D1 |
        // --------- img layer with D1+D2 at GC horizon------------------

        // img layer at 0x10
        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::Image(Bytes::from("value 1@0x20")),
            ),
            (
                get_key(2),
                Lsn(0x30),
                Value::Image(Bytes::from("value 2@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x40),
                Value::Image(Bytes::from("value 3@0x40")),
            ),
        ];
        let delta2 = vec![
            (
                get_key(5),
                Lsn(0x20),
                Value::Image(Bytes::from("value 5@0x20")),
            ),
            (
                get_key(6),
                Lsn(0x20),
                Value::Image(Bytes::from("value 6@0x20")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::Image(Bytes::from("value 8@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::Image(Bytes::from("value 9@0x48")),
            ),
        ];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x48), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x48), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x48)..Lsn(0x50), delta3),
                ], // delta layers
                vec![(Lsn(0x10), img_layer)], // image layers
                Lsn(0x50),
            )
            .await?;
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            guard.cutoffs.time = Lsn(0x30);
            guard.cutoffs.space = Lsn(0x30);
        }

        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x20"),
            Bytes::from_static(b"value 2@0x30"),
            Bytes::from_static(b"value 3@0x40"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x20"),
            Bytes::from_static(b"value 6@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x48"),
            Bytes::from_static(b"value 9@0x48"),
        ];

        for (idx, expected) in expected_result.iter().enumerate() {
            assert_eq!(
                tline
                    .get(get_key(idx as u32), Lsn(0x50), &ctx)
                    .await
                    .unwrap(),
                expected
            );
        }

        let cancel = CancellationToken::new();
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();

        for (idx, expected) in expected_result.iter().enumerate() {
            assert_eq!(
                tline
                    .get(get_key(idx as u32), Lsn(0x50), &ctx)
                    .await
                    .unwrap(),
                expected
            );
        }

        // Check if the image layer at the GC horizon contains exactly what we want
        let image_at_gc_horizon = tline
            .inspect_image_layers(Lsn(0x30), &ctx)
            .await
            .unwrap()
            .into_iter()
            .filter(|(k, _)| k.is_metadata_key())
            .collect::<Vec<_>>();

        assert_eq!(image_at_gc_horizon.len(), 10);
        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x20"),
            Bytes::from_static(b"value 2@0x30"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x20"),
            Bytes::from_static(b"value 6@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];
        for idx in 0..10 {
            assert_eq!(
                image_at_gc_horizon[idx],
                (get_key(idx as u32), expected_result[idx].clone())
            );
        }

        // Check if old layers are removed / new layers have the expected LSN
        let all_layers = inspect_and_sort(&tline, None).await;
        assert_eq!(
            all_layers,
            vec![
                // Image layer at GC horizon
                PersistentLayerKey {
                    key_range: Key::MIN..Key::MAX,
                    lsn_range: Lsn(0x30)..Lsn(0x31),
                    is_delta: false
                },
                // The delta layer below the horizon
                PersistentLayerKey {
                    key_range: get_key(3)..get_key(4),
                    lsn_range: Lsn(0x30)..Lsn(0x48),
                    is_delta: true
                },
                // The delta3 layer that should not be picked for the compaction
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x48)..Lsn(0x50),
                    is_delta: true
                }
            ]
        );

        // increase GC horizon and compact again
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x40))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            guard.cutoffs.time = Lsn(0x40);
            guard.cutoffs.space = Lsn(0x40);
        }
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_neon_test_record() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_neon_test_record").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append(",0x20")),
            ),
            (
                get_key(1),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append(",0x30")),
            ),
            (get_key(2), Lsn(0x10), Value::Image("0x10".into())),
            (
                get_key(2),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append(",0x20")),
            ),
            (
                get_key(2),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append(",0x30")),
            ),
            (get_key(3), Lsn(0x10), Value::Image("0x10".into())),
            (
                get_key(3),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_clear("c")),
            ),
            (get_key(4), Lsn(0x10), Value::Image("0x10".into())),
            (
                get_key(4),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_init("i")),
            ),
        ];
        let image1 = vec![(get_key(1), "0x10".into())];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![DeltaLayerTestDesc::new_with_inferred_key_range(
                    Lsn(0x10)..Lsn(0x40),
                    delta1,
                )], // delta layers
                vec![(Lsn(0x10), image1)], // image layers
                Lsn(0x50),
            )
            .await?;

        assert_eq!(
            tline.get(get_key(1), Lsn(0x50), &ctx).await?,
            Bytes::from_static(b"0x10,0x20,0x30")
        );
        assert_eq!(
            tline.get(get_key(2), Lsn(0x50), &ctx).await?,
            Bytes::from_static(b"0x10,0x20,0x30")
        );

        // Need to remove the limit of "Neon WAL redo requires base image".

        // assert_eq!(tline.get(get_key(3), Lsn(0x50), &ctx).await?, Bytes::new());
        // assert_eq!(tline.get(get_key(4), Lsn(0x50), &ctx).await?, Bytes::new());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_lsn_lease() -> anyhow::Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_lsn_lease")
            .await
            .unwrap()
            .load()
            .await;
        // Advance to the lsn lease deadline so that GC is not blocked by
        // initial transition into AttachedSingle.
        tokio::time::advance(tenant.get_lsn_lease_length()).await;
        tokio::time::resume();
        let key = Key::from_hex("010000000033333333444444445500000000").unwrap();

        let end_lsn = Lsn(0x100);
        let image_layers = (0x20..=0x90)
            .step_by(0x10)
            .map(|n| {
                (
                    Lsn(n),
                    vec![(key, test_img(&format!("data key at {:x}", n)))],
                )
            })
            .collect();

        let timeline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                Vec::new(),
                image_layers,
                end_lsn,
            )
            .await?;

        let leased_lsns = [0x30, 0x50, 0x70];
        let mut leases = Vec::new();
        leased_lsns.iter().for_each(|n| {
            leases.push(
                timeline
                    .init_lsn_lease(Lsn(*n), timeline.get_lsn_lease_length(), &ctx)
                    .expect("lease request should succeed"),
            );
        });

        let updated_lease_0 = timeline
            .renew_lsn_lease(Lsn(leased_lsns[0]), Duration::from_secs(0), &ctx)
            .expect("lease renewal should succeed");
        assert_eq!(
            updated_lease_0.valid_until, leases[0].valid_until,
            " Renewing with shorter lease should not change the lease."
        );

        let updated_lease_1 = timeline
            .renew_lsn_lease(
                Lsn(leased_lsns[1]),
                timeline.get_lsn_lease_length() * 2,
                &ctx,
            )
            .expect("lease renewal should succeed");
        assert!(
            updated_lease_1.valid_until > leases[1].valid_until,
            "Renewing with a long lease should renew lease with later expiration time."
        );

        // Force set disk consistent lsn so we can get the cutoff at `end_lsn`.
        info!(
            "latest_gc_cutoff_lsn: {}",
            *timeline.get_latest_gc_cutoff_lsn()
        );
        timeline.force_set_disk_consistent_lsn(end_lsn);

        let res = tenant
            .gc_iteration(
                Some(TIMELINE_ID),
                0,
                Duration::ZERO,
                &CancellationToken::new(),
                &ctx,
            )
            .await
            .unwrap();

        // Keeping everything <= Lsn(0x80) b/c leases:
        // 0/10: initdb layer
        // (0/20..=0/70).step_by(0x10): image layers added when creating the timeline.
        assert_eq!(res.layers_needed_by_leases, 7);
        // Keeping 0/90 b/c it is the latest layer.
        assert_eq!(res.layers_not_updated, 1);
        // Removed 0/80.
        assert_eq!(res.layers_removed, 1);

        // Make lease on a already GC-ed LSN.
        // 0/80 does not have a valid lease + is below latest_gc_cutoff
        assert!(Lsn(0x80) < *timeline.get_latest_gc_cutoff_lsn());
        timeline
            .init_lsn_lease(Lsn(0x80), timeline.get_lsn_lease_length(), &ctx)
            .expect_err("lease request on GC-ed LSN should fail");

        // Should still be able to renew a currently valid lease
        // Assumption: original lease to is still valid for 0/50.
        // (use `Timeline::init_lsn_lease` for testing so it always does validation)
        timeline
            .init_lsn_lease(Lsn(leased_lsns[1]), timeline.get_lsn_lease_length(), &ctx)
            .expect("lease renewal with validation should succeed");

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_deltas_1() -> anyhow::Result<()> {
        test_simple_bottom_most_compaction_deltas_helper(
            "test_simple_bottom_most_compaction_deltas_1",
            false,
        )
        .await
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_deltas_2() -> anyhow::Result<()> {
        test_simple_bottom_most_compaction_deltas_helper(
            "test_simple_bottom_most_compaction_deltas_2",
            true,
        )
        .await
    }

    #[cfg(feature = "testing")]
    async fn test_simple_bottom_most_compaction_deltas_helper(
        test_name: &'static str,
        use_delta_bottom_layer: bool,
    ) -> anyhow::Result<()> {
        let harness = TenantHarness::create(test_name).await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        // We create
        // - one bottom-most image layer,
        // - a delta layer D1 crossing the GC horizon with data below and above the horizon,
        // - a delta layer D2 crossing the GC horizon with data only below the horizon,
        // - a delta layer D3 above the horizon.
        //
        //                             | D3 |
        //  | D1 |
        // -|    |-- gc horizon -----------------
        //  |    |                | D2 |
        // --------- img layer ------------------
        //
        // What we should expact from this compaction is:
        //                             | D3 |
        //  | Part of D1 |
        // --------- img layer with D1+D2 at GC horizon------------------

        // img layer at 0x10
        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();
        // or, delta layer at 0x10 if `use_delta_bottom_layer` is true
        let delta4 = (0..10)
            .map(|id| {
                (
                    get_key(id),
                    Lsn(0x08),
                    Value::WalRecord(NeonWalRecord::wal_init(format!("value {id}@0x10"))),
                )
            })
            .collect_vec();

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(2),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x28),
                Value::WalRecord(NeonWalRecord::wal_append("@0x28")),
            ),
            (
                get_key(3),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x40),
                Value::WalRecord(NeonWalRecord::wal_append("@0x40")),
            ),
        ];
        let delta2 = vec![
            (
                get_key(5),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(6),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
        ];

        let tline = if use_delta_bottom_layer {
            tenant
                .create_test_timeline_with_layers(
                    TIMELINE_ID,
                    Lsn(0x08),
                    DEFAULT_PG_VERSION,
                    &ctx,
                    vec![
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x08)..Lsn(0x10),
                            delta4,
                        ),
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x20)..Lsn(0x48),
                            delta1,
                        ),
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x20)..Lsn(0x48),
                            delta2,
                        ),
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x48)..Lsn(0x50),
                            delta3,
                        ),
                    ], // delta layers
                    vec![], // image layers
                    Lsn(0x50),
                )
                .await?
        } else {
            tenant
                .create_test_timeline_with_layers(
                    TIMELINE_ID,
                    Lsn(0x10),
                    DEFAULT_PG_VERSION,
                    &ctx,
                    vec![
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x10)..Lsn(0x48),
                            delta1,
                        ),
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x10)..Lsn(0x48),
                            delta2,
                        ),
                        DeltaLayerTestDesc::new_with_inferred_key_range(
                            Lsn(0x48)..Lsn(0x50),
                            delta3,
                        ),
                    ], // delta layers
                    vec![(Lsn(0x10), img_layer)], // image layers
                    Lsn(0x50),
                )
                .await?
        };
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![],
                cutoffs: GcCutoffs {
                    time: Lsn(0x30),
                    space: Lsn(0x30),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10@0x30"),
            Bytes::from_static(b"value 3@0x10@0x28@0x30@0x40"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10@0x48"),
            Bytes::from_static(b"value 9@0x10@0x48"),
        ];

        let expected_result_at_gc_horizon = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10@0x30"),
            Bytes::from_static(b"value 3@0x10@0x28@0x30"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        for idx in 0..10 {
            assert_eq!(
                tline
                    .get(get_key(idx as u32), Lsn(0x50), &ctx)
                    .await
                    .unwrap(),
                &expected_result[idx]
            );
            assert_eq!(
                tline
                    .get(get_key(idx as u32), Lsn(0x30), &ctx)
                    .await
                    .unwrap(),
                &expected_result_at_gc_horizon[idx]
            );
        }

        let cancel = CancellationToken::new();
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();

        for idx in 0..10 {
            assert_eq!(
                tline
                    .get(get_key(idx as u32), Lsn(0x50), &ctx)
                    .await
                    .unwrap(),
                &expected_result[idx]
            );
            assert_eq!(
                tline
                    .get(get_key(idx as u32), Lsn(0x30), &ctx)
                    .await
                    .unwrap(),
                &expected_result_at_gc_horizon[idx]
            );
        }

        // increase GC horizon and compact again
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x40))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            guard.cutoffs.time = Lsn(0x40);
            guard.cutoffs.space = Lsn(0x40);
        }
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_generate_key_retention() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_generate_key_retention").await?;
        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        tline.force_advance_lsn(Lsn(0x70));
        let key = Key::from_hex("010000000033333333444444445500000000").unwrap();
        let history = vec![
            (
                key,
                Lsn(0x10),
                Value::WalRecord(NeonWalRecord::wal_init("0x10")),
            ),
            (
                key,
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append(";0x20")),
            ),
            (
                key,
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append(";0x30")),
            ),
            (
                key,
                Lsn(0x40),
                Value::WalRecord(NeonWalRecord::wal_append(";0x40")),
            ),
            (
                key,
                Lsn(0x50),
                Value::WalRecord(NeonWalRecord::wal_append(";0x50")),
            ),
            (
                key,
                Lsn(0x60),
                Value::WalRecord(NeonWalRecord::wal_append(";0x60")),
            ),
            (
                key,
                Lsn(0x70),
                Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
            ),
            (
                key,
                Lsn(0x80),
                Value::Image(Bytes::copy_from_slice(
                    b"0x10;0x20;0x30;0x40;0x50;0x60;0x70;0x80",
                )),
            ),
            (
                key,
                Lsn(0x90),
                Value::WalRecord(NeonWalRecord::wal_append(";0x90")),
            ),
        ];
        let res = tline
            .generate_key_retention(
                key,
                &history,
                Lsn(0x60),
                &[Lsn(0x20), Lsn(0x40), Lsn(0x50)],
                3,
                None,
            )
            .await
            .unwrap();
        let expected_res = KeyHistoryRetention {
            below_horizon: vec![
                (
                    Lsn(0x20),
                    KeyLogAtLsn(vec![(
                        Lsn(0x20),
                        Value::Image(Bytes::from_static(b"0x10;0x20")),
                    )]),
                ),
                (
                    Lsn(0x40),
                    KeyLogAtLsn(vec![
                        (
                            Lsn(0x30),
                            Value::WalRecord(NeonWalRecord::wal_append(";0x30")),
                        ),
                        (
                            Lsn(0x40),
                            Value::WalRecord(NeonWalRecord::wal_append(";0x40")),
                        ),
                    ]),
                ),
                (
                    Lsn(0x50),
                    KeyLogAtLsn(vec![(
                        Lsn(0x50),
                        Value::Image(Bytes::copy_from_slice(b"0x10;0x20;0x30;0x40;0x50")),
                    )]),
                ),
                (
                    Lsn(0x60),
                    KeyLogAtLsn(vec![(
                        Lsn(0x60),
                        Value::WalRecord(NeonWalRecord::wal_append(";0x60")),
                    )]),
                ),
            ],
            above_horizon: KeyLogAtLsn(vec![
                (
                    Lsn(0x70),
                    Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
                ),
                (
                    Lsn(0x80),
                    Value::Image(Bytes::copy_from_slice(
                        b"0x10;0x20;0x30;0x40;0x50;0x60;0x70;0x80",
                    )),
                ),
                (
                    Lsn(0x90),
                    Value::WalRecord(NeonWalRecord::wal_append(";0x90")),
                ),
            ]),
        };
        assert_eq!(res, expected_res);

        // We expect GC-compaction to run with the original GC. This would create a situation that
        // the original GC algorithm removes some delta layers b/c there are full image coverage,
        // therefore causing some keys to have an incomplete history below the lowest retain LSN.
        // For example, we have
        // ```plain
        // init delta @ 0x10, image @ 0x20, delta @ 0x30 (gc_horizon), image @ 0x40.
        // ```
        // Now the GC horizon moves up, and we have
        // ```plain
        // init delta @ 0x10, image @ 0x20, delta @ 0x30, image @ 0x40 (gc_horizon)
        // ```
        // The original GC algorithm kicks in, and removes delta @ 0x10, image @ 0x20.
        // We will end up with
        // ```plain
        // delta @ 0x30, image @ 0x40 (gc_horizon)
        // ```
        // Now we run the GC-compaction, and this key does not have a full history.
        // We should be able to handle this partial history and drop everything before the
        // gc_horizon image.

        let history = vec![
            (
                key,
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append(";0x20")),
            ),
            (
                key,
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append(";0x30")),
            ),
            (
                key,
                Lsn(0x40),
                Value::Image(Bytes::copy_from_slice(b"0x10;0x20;0x30;0x40")),
            ),
            (
                key,
                Lsn(0x50),
                Value::WalRecord(NeonWalRecord::wal_append(";0x50")),
            ),
            (
                key,
                Lsn(0x60),
                Value::WalRecord(NeonWalRecord::wal_append(";0x60")),
            ),
            (
                key,
                Lsn(0x70),
                Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
            ),
            (
                key,
                Lsn(0x80),
                Value::Image(Bytes::copy_from_slice(
                    b"0x10;0x20;0x30;0x40;0x50;0x60;0x70;0x80",
                )),
            ),
            (
                key,
                Lsn(0x90),
                Value::WalRecord(NeonWalRecord::wal_append(";0x90")),
            ),
        ];
        let res = tline
            .generate_key_retention(key, &history, Lsn(0x60), &[Lsn(0x40), Lsn(0x50)], 3, None)
            .await
            .unwrap();
        let expected_res = KeyHistoryRetention {
            below_horizon: vec![
                (
                    Lsn(0x40),
                    KeyLogAtLsn(vec![(
                        Lsn(0x40),
                        Value::Image(Bytes::copy_from_slice(b"0x10;0x20;0x30;0x40")),
                    )]),
                ),
                (
                    Lsn(0x50),
                    KeyLogAtLsn(vec![(
                        Lsn(0x50),
                        Value::WalRecord(NeonWalRecord::wal_append(";0x50")),
                    )]),
                ),
                (
                    Lsn(0x60),
                    KeyLogAtLsn(vec![(
                        Lsn(0x60),
                        Value::WalRecord(NeonWalRecord::wal_append(";0x60")),
                    )]),
                ),
            ],
            above_horizon: KeyLogAtLsn(vec![
                (
                    Lsn(0x70),
                    Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
                ),
                (
                    Lsn(0x80),
                    Value::Image(Bytes::copy_from_slice(
                        b"0x10;0x20;0x30;0x40;0x50;0x60;0x70;0x80",
                    )),
                ),
                (
                    Lsn(0x90),
                    Value::WalRecord(NeonWalRecord::wal_append(";0x90")),
                ),
            ]),
        };
        assert_eq!(res, expected_res);

        // In case of branch compaction, the branch itself does not have the full history, and we need to provide
        // the ancestor image in the test case.

        let history = vec![
            (
                key,
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append(";0x20")),
            ),
            (
                key,
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append(";0x30")),
            ),
            (
                key,
                Lsn(0x40),
                Value::WalRecord(NeonWalRecord::wal_append(";0x40")),
            ),
            (
                key,
                Lsn(0x70),
                Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
            ),
        ];
        let res = tline
            .generate_key_retention(
                key,
                &history,
                Lsn(0x60),
                &[],
                3,
                Some((key, Lsn(0x10), Bytes::copy_from_slice(b"0x10"))),
            )
            .await
            .unwrap();
        let expected_res = KeyHistoryRetention {
            below_horizon: vec![(
                Lsn(0x60),
                KeyLogAtLsn(vec![(
                    Lsn(0x60),
                    Value::Image(Bytes::copy_from_slice(b"0x10;0x20;0x30;0x40")), // use the ancestor image to reconstruct the page
                )]),
            )],
            above_horizon: KeyLogAtLsn(vec![(
                Lsn(0x70),
                Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
            )]),
        };
        assert_eq!(res, expected_res);

        let history = vec![
            (
                key,
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append(";0x20")),
            ),
            (
                key,
                Lsn(0x40),
                Value::WalRecord(NeonWalRecord::wal_append(";0x40")),
            ),
            (
                key,
                Lsn(0x60),
                Value::WalRecord(NeonWalRecord::wal_append(";0x60")),
            ),
            (
                key,
                Lsn(0x70),
                Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
            ),
        ];
        let res = tline
            .generate_key_retention(
                key,
                &history,
                Lsn(0x60),
                &[Lsn(0x30)],
                3,
                Some((key, Lsn(0x10), Bytes::copy_from_slice(b"0x10"))),
            )
            .await
            .unwrap();
        let expected_res = KeyHistoryRetention {
            below_horizon: vec![
                (
                    Lsn(0x30),
                    KeyLogAtLsn(vec![(
                        Lsn(0x20),
                        Value::WalRecord(NeonWalRecord::wal_append(";0x20")),
                    )]),
                ),
                (
                    Lsn(0x60),
                    KeyLogAtLsn(vec![(
                        Lsn(0x60),
                        Value::Image(Bytes::copy_from_slice(b"0x10;0x20;0x40;0x60")),
                    )]),
                ),
            ],
            above_horizon: KeyLogAtLsn(vec![(
                Lsn(0x70),
                Value::WalRecord(NeonWalRecord::wal_append(";0x70")),
            )]),
        };
        assert_eq!(res, expected_res);

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_with_retain_lsns() -> anyhow::Result<()> {
        let harness =
            TenantHarness::create("test_simple_bottom_most_compaction_with_retain_lsns").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(2),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x28),
                Value::WalRecord(NeonWalRecord::wal_append("@0x28")),
            ),
            (
                get_key(3),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x40),
                Value::WalRecord(NeonWalRecord::wal_append("@0x40")),
            ),
        ];
        let delta2 = vec![
            (
                get_key(5),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(6),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
        ];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x10)..Lsn(0x48), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x10)..Lsn(0x48), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x48)..Lsn(0x50), delta3),
                ], // delta layers
                vec![(Lsn(0x10), img_layer)], // image layers
                Lsn(0x50),
            )
            .await?;
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![
                    (Lsn(0x10), tline.timeline_id, MaybeOffloaded::No),
                    (Lsn(0x20), tline.timeline_id, MaybeOffloaded::No),
                ],
                cutoffs: GcCutoffs {
                    time: Lsn(0x30),
                    space: Lsn(0x30),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10@0x30"),
            Bytes::from_static(b"value 3@0x10@0x28@0x30@0x40"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10@0x48"),
            Bytes::from_static(b"value 9@0x10@0x48"),
        ];

        let expected_result_at_gc_horizon = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10@0x30"),
            Bytes::from_static(b"value 3@0x10@0x28@0x30"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_20 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_10 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let verify_result = || async {
            let gc_horizon = {
                let gc_info = tline.gc_info.read().unwrap();
                gc_info.cutoffs.time
            };
            for idx in 0..10 {
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x50), &ctx)
                        .await
                        .unwrap(),
                    &expected_result[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), gc_horizon, &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_gc_horizon[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x20), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_20[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x10), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_10[idx]
                );
            }
        };

        verify_result().await;

        let cancel = CancellationToken::new();
        let mut dryrun_flags = EnumSet::new();
        dryrun_flags.insert(CompactFlags::DryRun);

        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: dryrun_flags,
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        // We expect layer map to be the same b/c the dry run flag, but we don't know whether there will be other background jobs
        // cleaning things up, and therefore, we don't do sanity checks on the layer map during unit tests.
        verify_result().await;

        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        // compact again
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        // increase GC horizon and compact again
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x38))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            guard.cutoffs.time = Lsn(0x38);
            guard.cutoffs.space = Lsn(0x38);
        }
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await; // no wals between 0x30 and 0x38, so we should obtain the same result

        // not increasing the GC horizon and compact again
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_with_retain_lsns_single_key() -> anyhow::Result<()>
    {
        let harness =
            TenantHarness::create("test_simple_bottom_most_compaction_with_retain_lsns_single_key")
                .await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(1),
                Lsn(0x28),
                Value::WalRecord(NeonWalRecord::wal_append("@0x28")),
            ),
        ];
        let delta2 = vec![
            (
                get_key(1),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(1),
                Lsn(0x38),
                Value::WalRecord(NeonWalRecord::wal_append("@0x38")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
        ];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![
                    // delta1 and delta 2 only contain a single key but multiple updates
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x10)..Lsn(0x30), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x30)..Lsn(0x50), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x10)..Lsn(0x50), delta3),
                ], // delta layers
                vec![(Lsn(0x10), img_layer)], // image layers
                Lsn(0x50),
            )
            .await?;
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![
                    (Lsn(0x10), tline.timeline_id, MaybeOffloaded::No),
                    (Lsn(0x20), tline.timeline_id, MaybeOffloaded::No),
                ],
                cutoffs: GcCutoffs {
                    time: Lsn(0x30),
                    space: Lsn(0x30),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20@0x28@0x30@0x38"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10@0x48"),
            Bytes::from_static(b"value 9@0x10@0x48"),
        ];

        let expected_result_at_gc_horizon = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20@0x28@0x30"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_20 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_10 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let verify_result = || async {
            let gc_horizon = {
                let gc_info = tline.gc_info.read().unwrap();
                gc_info.cutoffs.time
            };
            for idx in 0..10 {
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x50), &ctx)
                        .await
                        .unwrap(),
                    &expected_result[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), gc_horizon, &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_gc_horizon[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x20), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_20[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x10), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_10[idx]
                );
            }
        };

        verify_result().await;

        let cancel = CancellationToken::new();
        let mut dryrun_flags = EnumSet::new();
        dryrun_flags.insert(CompactFlags::DryRun);

        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: dryrun_flags,
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        // We expect layer map to be the same b/c the dry run flag, but we don't know whether there will be other background jobs
        // cleaning things up, and therefore, we don't do sanity checks on the layer map during unit tests.
        verify_result().await;

        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        // compact again
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_on_branch() -> anyhow::Result<()> {
        use models::CompactLsnRange;

        let harness = TenantHarness::create("test_simple_bottom_most_compaction_on_branch").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            let mut key = Key::from_hex("000000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(2),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x28),
                Value::WalRecord(NeonWalRecord::wal_append("@0x28")),
            ),
            (
                get_key(3),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x40),
                Value::WalRecord(NeonWalRecord::wal_append("@0x40")),
            ),
        ];
        let delta2 = vec![
            (
                get_key(5),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
            (
                get_key(6),
                Lsn(0x20),
                Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
        ];

        let parent_tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![],                       // delta layers
                vec![(Lsn(0x18), img_layer)], // image layers
                Lsn(0x18),
            )
            .await?;

        parent_tline.add_extra_test_dense_keyspace(KeySpace::single(get_key(0)..get_key(10)));

        let branch_tline = tenant
            .branch_timeline_test_with_layers(
                &parent_tline,
                NEW_TIMELINE_ID,
                Some(Lsn(0x18)),
                &ctx,
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x48), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x48), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x48)..Lsn(0x50), delta3),
                ], // delta layers
                vec![], // image layers
                Lsn(0x50),
            )
            .await?;

        branch_tline.add_extra_test_dense_keyspace(KeySpace::single(get_key(0)..get_key(10)));

        {
            parent_tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x10))
                .wait()
                .await;
            // Update GC info
            let mut guard = parent_tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![(Lsn(0x18), branch_tline.timeline_id, MaybeOffloaded::No)],
                cutoffs: GcCutoffs {
                    time: Lsn(0x10),
                    space: Lsn(0x10),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        {
            branch_tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x50))
                .wait()
                .await;
            // Update GC info
            let mut guard = branch_tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![(Lsn(0x40), branch_tline.timeline_id, MaybeOffloaded::No)],
                cutoffs: GcCutoffs {
                    time: Lsn(0x50),
                    space: Lsn(0x50),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let expected_result_at_gc_horizon = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10@0x30"),
            Bytes::from_static(b"value 3@0x10@0x28@0x30@0x40"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10@0x48"),
            Bytes::from_static(b"value 9@0x10@0x48"),
        ];

        let expected_result_at_lsn_40 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10@0x30"),
            Bytes::from_static(b"value 3@0x10@0x28@0x30@0x40"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10@0x20"),
            Bytes::from_static(b"value 6@0x10@0x20"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let verify_result = || async {
            for idx in 0..10 {
                assert_eq!(
                    branch_tline
                        .get(get_key(idx as u32), Lsn(0x50), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_gc_horizon[idx]
                );
                assert_eq!(
                    branch_tline
                        .get(get_key(idx as u32), Lsn(0x40), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_40[idx]
                );
            }
        };

        verify_result().await;

        let cancel = CancellationToken::new();
        branch_tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();

        verify_result().await;

        // Piggyback a compaction with above_lsn. Ensure it works correctly when the specified LSN intersects with the layer files.
        // Now we already have a single large delta layer, so the compaction min_layer_lsn should be the same as ancestor LSN (0x18).
        branch_tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    compact_lsn_range: Some(CompactLsnRange::above(Lsn(0x40))),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();

        verify_result().await;

        Ok(())
    }

    // Regression test for https://github.com/neondatabase/neon/issues/9012
    // Create an image arrangement where we have to read at different LSN ranges
    // from a delta layer. This is achieved by overlapping an image layer on top of
    // a delta layer. Like so:
    //
    //     A      B
    // +----------------+ -> delta_layer
    // |                |                           ^ lsn
    // |       =========|-> nested_image_layer      |
    // |       C        |                           |
    // +----------------+                           |
    // ======== -> baseline_image_layer             +-------> key
    //
    //
    // When querying the key range [A, B) we need to read at different LSN ranges
    // for [A, C) and [C, B). This test checks that the described edge case is handled correctly.
    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_vectored_read_with_nested_image_layer() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_vectored_read_with_nested_image_layer").await?;
        let (tenant, ctx) = harness.load().await;

        let will_init_keys = [2, 6];
        fn get_key(id: u32) -> Key {
            let mut key = Key::from_hex("110000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let mut expected_key_values = HashMap::new();

        let baseline_image_layer_lsn = Lsn(0x10);
        let mut baseline_img_layer = Vec::new();
        for i in 0..5 {
            let key = get_key(i);
            let value = format!("value {i}@{baseline_image_layer_lsn}");

            let removed = expected_key_values.insert(key, value.clone());
            assert!(removed.is_none());

            baseline_img_layer.push((key, Bytes::from(value)));
        }

        let nested_image_layer_lsn = Lsn(0x50);
        let mut nested_img_layer = Vec::new();
        for i in 5..10 {
            let key = get_key(i);
            let value = format!("value {i}@{nested_image_layer_lsn}");

            let removed = expected_key_values.insert(key, value.clone());
            assert!(removed.is_none());

            nested_img_layer.push((key, Bytes::from(value)));
        }

        let mut delta_layer_spec = Vec::default();
        let delta_layer_start_lsn = Lsn(0x20);
        let mut delta_layer_end_lsn = delta_layer_start_lsn;

        for i in 0..10 {
            let key = get_key(i);
            let key_in_nested = nested_img_layer
                .iter()
                .any(|(key_with_img, _)| *key_with_img == key);
            let lsn = {
                if key_in_nested {
                    Lsn(nested_image_layer_lsn.0 + 0x10)
                } else {
                    delta_layer_start_lsn
                }
            };

            let will_init = will_init_keys.contains(&i);
            if will_init {
                delta_layer_spec.push((key, lsn, Value::WalRecord(NeonWalRecord::wal_init(""))));

                expected_key_values.insert(key, "".to_string());
            } else {
                let delta = format!("@{lsn}");
                delta_layer_spec.push((
                    key,
                    lsn,
                    Value::WalRecord(NeonWalRecord::wal_append(&delta)),
                ));

                expected_key_values
                    .get_mut(&key)
                    .expect("An image exists for each key")
                    .push_str(delta.as_str());
            }
            delta_layer_end_lsn = std::cmp::max(delta_layer_start_lsn, lsn);
        }

        delta_layer_end_lsn = Lsn(delta_layer_end_lsn.0 + 1);

        assert!(
            nested_image_layer_lsn > delta_layer_start_lsn
                && nested_image_layer_lsn < delta_layer_end_lsn
        );

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                baseline_image_layer_lsn,
                DEFAULT_PG_VERSION,
                &ctx,
                vec![DeltaLayerTestDesc::new_with_inferred_key_range(
                    delta_layer_start_lsn..delta_layer_end_lsn,
                    delta_layer_spec,
                )], // delta layers
                vec![
                    (baseline_image_layer_lsn, baseline_img_layer),
                    (nested_image_layer_lsn, nested_img_layer),
                ], // image layers
                delta_layer_end_lsn,
            )
            .await?;

        let keyspace = KeySpace::single(get_key(0)..get_key(10));
        let results = tline
            .get_vectored(keyspace, delta_layer_end_lsn, &ctx)
            .await
            .expect("No vectored errors");
        for (key, res) in results {
            let value = res.expect("No key errors");
            let expected_value = expected_key_values.remove(&key).expect("No unknown keys");
            assert_eq!(value, Bytes::from(expected_value));
        }

        Ok(())
    }

    fn sort_layer_key(k1: &PersistentLayerKey, k2: &PersistentLayerKey) -> std::cmp::Ordering {
        (
            k1.is_delta,
            k1.key_range.start,
            k1.key_range.end,
            k1.lsn_range.start,
            k1.lsn_range.end,
        )
            .cmp(&(
                k2.is_delta,
                k2.key_range.start,
                k2.key_range.end,
                k2.lsn_range.start,
                k2.lsn_range.end,
            ))
    }

    async fn inspect_and_sort(
        tline: &Arc<Timeline>,
        filter: Option<std::ops::Range<Key>>,
    ) -> Vec<PersistentLayerKey> {
        let mut all_layers = tline.inspect_historic_layers().await.unwrap();
        if let Some(filter) = filter {
            all_layers.retain(|layer| overlaps_with(&layer.key_range, &filter));
        }
        all_layers.sort_by(sort_layer_key);
        all_layers
    }

    #[cfg(feature = "testing")]
    fn check_layer_map_key_eq(
        mut left: Vec<PersistentLayerKey>,
        mut right: Vec<PersistentLayerKey>,
    ) {
        left.sort_by(sort_layer_key);
        right.sort_by(sort_layer_key);
        if left != right {
            eprintln!("---LEFT---");
            for left in left.iter() {
                eprintln!("{}", left);
            }
            eprintln!("---RIGHT---");
            for right in right.iter() {
                eprintln!("{}", right);
            }
            assert_eq!(left, right);
        }
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_partial_bottom_most_compaction() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_simple_partial_bottom_most_compaction").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        // img layer at 0x10
        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![
            (
                get_key(1),
                Lsn(0x20),
                Value::Image(Bytes::from("value 1@0x20")),
            ),
            (
                get_key(2),
                Lsn(0x30),
                Value::Image(Bytes::from("value 2@0x30")),
            ),
            (
                get_key(3),
                Lsn(0x40),
                Value::Image(Bytes::from("value 3@0x40")),
            ),
        ];
        let delta2 = vec![
            (
                get_key(5),
                Lsn(0x20),
                Value::Image(Bytes::from("value 5@0x20")),
            ),
            (
                get_key(6),
                Lsn(0x20),
                Value::Image(Bytes::from("value 6@0x20")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::Image(Bytes::from("value 8@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::Image(Bytes::from("value 9@0x48")),
            ),
        ];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x48), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x48), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x48)..Lsn(0x50), delta3),
                ], // delta layers
                vec![(Lsn(0x10), img_layer)], // image layers
                Lsn(0x50),
            )
            .await?;

        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![(Lsn(0x20), tline.timeline_id, MaybeOffloaded::No)],
                cutoffs: GcCutoffs {
                    time: Lsn(0x30),
                    space: Lsn(0x30),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let cancel = CancellationToken::new();

        // Do a partial compaction on key range 0..2
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: EnumSet::new(),
                    compact_key_range: Some((get_key(0)..get_key(2)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // newly-generated image layer for the partial compaction range 0-2
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // delta1 is split and the second part is rewritten
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                PersistentLayerKey {
                    key_range: get_key(5)..get_key(7),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x48)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // Do a partial compaction on key range 2..4
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: EnumSet::new(),
                    compact_key_range: Some((get_key(2)..get_key(4)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // image layer generated for the compaction range 2-4
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                // we have key2/key3 above the retain_lsn, so we still need this delta layer
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                PersistentLayerKey {
                    key_range: get_key(5)..get_key(7),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x48)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // Do a partial compaction on key range 4..9
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: EnumSet::new(),
                    compact_key_range: Some((get_key(4)..get_key(9)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                // image layer generated for this compaction range
                PersistentLayerKey {
                    key_range: get_key(4)..get_key(9),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x48)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // Do a partial compaction on key range 9..10
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: EnumSet::new(),
                    compact_key_range: Some((get_key(9)..get_key(10)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                PersistentLayerKey {
                    key_range: get_key(4)..get_key(9),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                // image layer generated for the compaction range
                PersistentLayerKey {
                    key_range: get_key(9)..get_key(10),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x48)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // Do a partial compaction on key range 0..10, all image layers below LSN 20 can be replaced with new ones.
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    flags: EnumSet::new(),
                    compact_key_range: Some((get_key(0)..get_key(10)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // aha, we removed all unnecessary image/delta layers and got a very clean layer map!
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x20)..Lsn(0x21),
                    is_delta: false,
                },
                PersistentLayerKey {
                    key_range: get_key(2)..get_key(4),
                    lsn_range: Lsn(0x20)..Lsn(0x48),
                    is_delta: true,
                },
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x48)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );
        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_timeline_offload_retain_lsn() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_timeline_offload_retain_lsn")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;
        let tline_parent = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();
        let tline_child = tenant
            .branch_timeline_test(&tline_parent, NEW_TIMELINE_ID, Some(Lsn(0x20)), &ctx)
            .await
            .unwrap();
        {
            let gc_info_parent = tline_parent.gc_info.read().unwrap();
            assert_eq!(
                gc_info_parent.retain_lsns,
                vec![(Lsn(0x20), tline_child.timeline_id, MaybeOffloaded::No)]
            );
        }
        // We have to directly call the remote_client instead of using the archive function to avoid constructing broker client...
        tline_child
            .remote_client
            .schedule_index_upload_for_timeline_archival_state(TimelineArchivalState::Archived)
            .unwrap();
        tline_child.remote_client.wait_completion().await.unwrap();
        offload_timeline(&tenant, &tline_child)
            .instrument(tracing::info_span!(parent: None, "offload_test", tenant_id=%"test", shard_id=%"test", timeline_id=%"test"))
            .await.unwrap();
        let child_timeline_id = tline_child.timeline_id;
        Arc::try_unwrap(tline_child).unwrap();

        {
            let gc_info_parent = tline_parent.gc_info.read().unwrap();
            assert_eq!(
                gc_info_parent.retain_lsns,
                vec![(Lsn(0x20), child_timeline_id, MaybeOffloaded::Yes)]
            );
        }

        tenant
            .get_offloaded_timeline(child_timeline_id)
            .unwrap()
            .defuse_for_tenant_drop();

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_above_lsn() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_simple_bottom_most_compaction_above_lsn").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![(
            get_key(1),
            Lsn(0x20),
            Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
        )];
        let delta4 = vec![(
            get_key(1),
            Lsn(0x28),
            Value::WalRecord(NeonWalRecord::wal_append("@0x28")),
        )];
        let delta2 = vec![
            (
                get_key(1),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(1),
                Lsn(0x38),
                Value::WalRecord(NeonWalRecord::wal_append("@0x38")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
        ];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![
                    // delta1/2/4 only contain a single key but multiple updates
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x28), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x30)..Lsn(0x50), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x28)..Lsn(0x30), delta4),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x30)..Lsn(0x50), delta3),
                ], // delta layers
                vec![(Lsn(0x10), img_layer)], // image layers
                Lsn(0x50),
            )
            .await?;
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![
                    (Lsn(0x10), tline.timeline_id, MaybeOffloaded::No),
                    (Lsn(0x20), tline.timeline_id, MaybeOffloaded::No),
                ],
                cutoffs: GcCutoffs {
                    time: Lsn(0x30),
                    space: Lsn(0x30),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20@0x28@0x30@0x38"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10@0x48"),
            Bytes::from_static(b"value 9@0x10@0x48"),
        ];

        let expected_result_at_gc_horizon = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20@0x28@0x30"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_20 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_10 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let verify_result = || async {
            let gc_horizon = {
                let gc_info = tline.gc_info.read().unwrap();
                gc_info.cutoffs.time
            };
            for idx in 0..10 {
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x50), &ctx)
                        .await
                        .unwrap(),
                    &expected_result[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), gc_horizon, &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_gc_horizon[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x20), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_20[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x10), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_10[idx]
                );
            }
        };

        verify_result().await;

        let cancel = CancellationToken::new();
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    compact_lsn_range: Some(CompactLsnRange::above(Lsn(0x28))),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        verify_result().await;

        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // The original image layer, not compacted
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // Delta layer below the specified above_lsn not compacted
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x28),
                    is_delta: true,
                },
                // Delta layer compacted above the LSN
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(10),
                    lsn_range: Lsn(0x28)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // compact again
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // The compacted image layer (full key range)
                PersistentLayerKey {
                    key_range: Key::MIN..Key::MAX,
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // All other data in the delta layer
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        Ok(())
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn test_simple_bottom_most_compaction_rectangle() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_simple_bottom_most_compaction_rectangle").await?;
        let (tenant, ctx) = harness.load().await;

        fn get_key(id: u32) -> Key {
            // using aux key here b/c they are guaranteed to be inside `collect_keyspace`.
            let mut key = Key::from_hex("620000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }

        let img_layer = (0..10)
            .map(|id| (get_key(id), Bytes::from(format!("value {id}@0x10"))))
            .collect_vec();

        let delta1 = vec![(
            get_key(1),
            Lsn(0x20),
            Value::WalRecord(NeonWalRecord::wal_append("@0x20")),
        )];
        let delta4 = vec![(
            get_key(1),
            Lsn(0x28),
            Value::WalRecord(NeonWalRecord::wal_append("@0x28")),
        )];
        let delta2 = vec![
            (
                get_key(1),
                Lsn(0x30),
                Value::WalRecord(NeonWalRecord::wal_append("@0x30")),
            ),
            (
                get_key(1),
                Lsn(0x38),
                Value::WalRecord(NeonWalRecord::wal_append("@0x38")),
            ),
        ];
        let delta3 = vec![
            (
                get_key(8),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
            (
                get_key(9),
                Lsn(0x48),
                Value::WalRecord(NeonWalRecord::wal_append("@0x48")),
            ),
        ];

        let tline = tenant
            .create_test_timeline_with_layers(
                TIMELINE_ID,
                Lsn(0x10),
                DEFAULT_PG_VERSION,
                &ctx,
                vec![
                    // delta1/2/4 only contain a single key but multiple updates
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x20)..Lsn(0x28), delta1),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x30)..Lsn(0x50), delta2),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x28)..Lsn(0x30), delta4),
                    DeltaLayerTestDesc::new_with_inferred_key_range(Lsn(0x30)..Lsn(0x50), delta3),
                ], // delta layers
                vec![(Lsn(0x10), img_layer)], // image layers
                Lsn(0x50),
            )
            .await?;
        {
            tline
                .latest_gc_cutoff_lsn
                .lock_for_write()
                .store_and_unlock(Lsn(0x30))
                .wait()
                .await;
            // Update GC info
            let mut guard = tline.gc_info.write().unwrap();
            *guard = GcInfo {
                retain_lsns: vec![
                    (Lsn(0x10), tline.timeline_id, MaybeOffloaded::No),
                    (Lsn(0x20), tline.timeline_id, MaybeOffloaded::No),
                ],
                cutoffs: GcCutoffs {
                    time: Lsn(0x30),
                    space: Lsn(0x30),
                },
                leases: Default::default(),
                within_ancestor_pitr: false,
            };
        }

        let expected_result = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20@0x28@0x30@0x38"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10@0x48"),
            Bytes::from_static(b"value 9@0x10@0x48"),
        ];

        let expected_result_at_gc_horizon = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20@0x28@0x30"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_20 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10@0x20"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let expected_result_at_lsn_10 = [
            Bytes::from_static(b"value 0@0x10"),
            Bytes::from_static(b"value 1@0x10"),
            Bytes::from_static(b"value 2@0x10"),
            Bytes::from_static(b"value 3@0x10"),
            Bytes::from_static(b"value 4@0x10"),
            Bytes::from_static(b"value 5@0x10"),
            Bytes::from_static(b"value 6@0x10"),
            Bytes::from_static(b"value 7@0x10"),
            Bytes::from_static(b"value 8@0x10"),
            Bytes::from_static(b"value 9@0x10"),
        ];

        let verify_result = || async {
            let gc_horizon = {
                let gc_info = tline.gc_info.read().unwrap();
                gc_info.cutoffs.time
            };
            for idx in 0..10 {
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x50), &ctx)
                        .await
                        .unwrap(),
                    &expected_result[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), gc_horizon, &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_gc_horizon[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x20), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_20[idx]
                );
                assert_eq!(
                    tline
                        .get(get_key(idx as u32), Lsn(0x10), &ctx)
                        .await
                        .unwrap(),
                    &expected_result_at_lsn_10[idx]
                );
            }
        };

        verify_result().await;

        let cancel = CancellationToken::new();

        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    compact_key_range: Some((get_key(0)..get_key(2)).into()),
                    compact_lsn_range: Some((Lsn(0x20)..Lsn(0x28)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        verify_result().await;

        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // The original image layer, not compacted
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // According the selection logic, we select all layers with start key <= 0x28, so we would merge the layer 0x20-0x28 and
                // the layer 0x28-0x30 into one.
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x30),
                    is_delta: true,
                },
                // Above the upper bound and untouched
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(2),
                    lsn_range: Lsn(0x30)..Lsn(0x50),
                    is_delta: true,
                },
                // This layer is untouched
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x30)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    compact_key_range: Some((get_key(3)..get_key(8)).into()),
                    compact_lsn_range: Some((Lsn(0x28)..Lsn(0x40)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        verify_result().await;

        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // The original image layer, not compacted
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // Not in the compaction key range, uncompacted
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x30),
                    is_delta: true,
                },
                // Not in the compaction key range, uncompacted but need rewrite because the delta layer overlaps with the range
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(2),
                    lsn_range: Lsn(0x30)..Lsn(0x50),
                    is_delta: true,
                },
                // Note that when we specify the LSN upper bound to be 0x40, the compaction algorithm will not try to cut the layer
                // horizontally in half. Instead, it will include all LSNs that overlap with 0x40. So the real max_lsn of the compaction
                // becomes 0x50.
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x30)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // compact again
        tline
            .compact_with_gc(
                &cancel,
                CompactOptions {
                    compact_key_range: Some((get_key(0)..get_key(5)).into()),
                    compact_lsn_range: Some((Lsn(0x20)..Lsn(0x50)).into()),
                    ..Default::default()
                },
                &ctx,
            )
            .await
            .unwrap();
        verify_result().await;

        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // The original image layer, not compacted
                PersistentLayerKey {
                    key_range: get_key(0)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // The range gets compacted
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(2),
                    lsn_range: Lsn(0x20)..Lsn(0x50),
                    is_delta: true,
                },
                // Not touched during this iteration of compaction
                PersistentLayerKey {
                    key_range: get_key(8)..get_key(10),
                    lsn_range: Lsn(0x30)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        // final full compaction
        tline
            .compact_with_gc(&cancel, CompactOptions::default(), &ctx)
            .await
            .unwrap();
        verify_result().await;

        let all_layers = inspect_and_sort(&tline, Some(get_key(0)..get_key(10))).await;
        check_layer_map_key_eq(
            all_layers,
            vec![
                // The compacted image layer (full key range)
                PersistentLayerKey {
                    key_range: Key::MIN..Key::MAX,
                    lsn_range: Lsn(0x10)..Lsn(0x11),
                    is_delta: false,
                },
                // All other data in the delta layer
                PersistentLayerKey {
                    key_range: get_key(1)..get_key(10),
                    lsn_range: Lsn(0x10)..Lsn(0x50),
                    is_delta: true,
                },
            ],
        );

        Ok(())
    }
}
