//!

mod eviction_task;
mod walreceiver;

use anyhow::{anyhow, bail, ensure, Context};
use bytes::Bytes;
use fail::fail_point;
use futures::StreamExt;
use itertools::Itertools;
use pageserver_api::models::{
    DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskSpawnRequest,
    DownloadRemoteLayersTaskState, LayerMapInfo, LayerResidenceEventReason, LayerResidenceStatus,
    TimelineState,
};
use remote_storage::GenericRemoteStorage;
use storage_broker::BrokerClientChannel;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::id::TenantTimelineId;

use std::cmp::{max, min, Ordering};
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use crate::broker_client::{get_broker_client, is_broker_client_initialized};
use crate::context::{DownloadBehavior, RequestContext};
use crate::tenant::remote_timeline_client::{self, index::LayerFileMetadata};
use crate::tenant::storage_layer::{
    DeltaFileName, DeltaLayerWriter, ImageFileName, ImageLayerWriter, InMemoryLayer,
    LayerAccessStats, LayerFileName, RemoteLayer,
};
use crate::tenant::{
    ephemeral_file::is_ephemeral_file,
    layer_map::{LayerMap, SearchResult},
    metadata::{save_metadata, TimelineMetadata},
    par_fsync,
    storage_layer::{PersistentLayer, ValueReconstructResult, ValueReconstructState},
};

use crate::config::PageServerConf;
use crate::keyspace::{KeyPartitioning, KeySpace, KeySpaceRandomAccum};
use crate::metrics::{TimelineMetrics, UNEXPECTED_ONDEMAND_DOWNLOADS};
use crate::pgdatadir_mapping::BlockNumber;
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::pgdatadir_mapping::{is_rel_fsm_block_key, is_rel_vm_block_key};
use crate::tenant::config::{EvictionPolicy, TenantConfOpt};
use pageserver_api::reltag::RelTag;

use postgres_connection::PgConnectionConfig;
use postgres_ffi::to_pg_timestamp;
use utils::{
    id::{TenantId, TimelineId},
    lsn::{AtomicLsn, Lsn, RecordLsn},
    seqwait::SeqWait,
    simple_rcu::{Rcu, RcuReadGuard},
};

use crate::page_cache;
use crate::repository::GcResult;
use crate::repository::{Key, Value};
use crate::task_mgr::TaskKind;
use crate::walredo::WalRedoManager;
use crate::METADATA_FILE_NAME;
use crate::ZERO_PAGE;
use crate::{is_temporary, task_mgr};

pub(super) use self::eviction_task::EvictionTaskTenantState;
use self::eviction_task::EvictionTaskTimelineState;
use self::walreceiver::{WalReceiver, WalReceiverConf};

use super::config::TenantConf;
use super::layer_map::BatchedUpdates;
use super::remote_timeline_client::index::IndexPart;
use super::remote_timeline_client::RemoteTimelineClient;
use super::storage_layer::{DeltaLayer, ImageLayer, Layer, LayerAccessStatsReset};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum FlushLoopState {
    NotStarted,
    Running,
    Exited,
}

/// Wrapper for key range to provide reverse ordering by range length for BinaryHeap
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hole {
    key_range: Range<Key>,
    coverage_size: usize,
}

impl Ord for Hole {
    fn cmp(&self, other: &Self) -> Ordering {
        other.coverage_size.cmp(&self.coverage_size) // inverse order
    }
}

impl PartialOrd for Hole {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Timeline {
    conf: &'static PageServerConf,
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    myself: Weak<Self>,

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,

    pub pg_version: u32,

    pub(super) layers: RwLock<LayerMap<dyn PersistentLayer>>,

    /// Set of key ranges which should be covered by image layers to
    /// allow GC to remove old layers. This set is created by GC and its cutoff LSN is also stored.
    /// It is used by compaction task when it checks if new image layer should be created.
    /// Newly created image layer doesn't help to remove the delta layer, until the
    /// newly created image layer falls off the PITR horizon. So on next GC cycle,
    /// gc_timeline may still want the new image layer to be created. To avoid redundant
    /// image layers creation we should check if image layer exists but beyond PITR horizon.
    /// This is why we need remember GC cutoff LSN.
    ///
    wanted_image_layers: Mutex<Option<(Lsn, KeySpace)>>,

    last_freeze_at: AtomicLsn,
    // Atomic would be more appropriate here.
    last_freeze_ts: RwLock<Instant>,

    // WAL redo manager
    walredo_mgr: Arc<dyn WalRedoManager + Sync + Send>,

    /// Remote storage client.
    /// See [`storage_sync`] module comment for details.
    pub remote_client: Option<Arc<RemoteTimelineClient>>,

    // What page versions do we hold in the repository? If we get a
    // request > last_record_lsn, we need to wait until we receive all
    // the WAL up to the request. The SeqWait provides functions for
    // that. TODO: If we get a request for an old LSN, such that the
    // versions have already been garbage collected away, we should
    // throw an error, but we don't track that currently.
    //
    // last_record_lsn.load().last points to the end of last processed WAL record.
    //
    // We also remember the starting point of the previous record in
    // 'last_record_lsn.load().prev'. It's used to set the xl_prev pointer of the
    // first WAL record when the node is started up. But here, we just
    // keep track of it.
    last_record_lsn: SeqWait<RecordLsn, Lsn>,

    // All WAL records have been processed and stored durably on files on
    // local disk, up to this LSN. On crash and restart, we need to re-process
    // the WAL starting from this point.
    //
    // Some later WAL records might have been processed and also flushed to disk
    // already, so don't be surprised to see some, but there's no guarantee on
    // them yet.
    disk_consistent_lsn: AtomicLsn,

    // Parent timeline that this timeline was branched from, and the LSN
    // of the branch point.
    ancestor_timeline: Option<Arc<Timeline>>,
    ancestor_lsn: Lsn,

    pub(super) metrics: TimelineMetrics,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`Timeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`TimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,

    /// Used to avoid multiple `flush_loop` tasks running
    flush_loop_state: Mutex<FlushLoopState>,

    /// layer_flush_start_tx can be used to wake up the layer-flushing task.
    /// The value is a counter, incremented every time a new flush cycle is requested.
    /// The flush cycle counter is sent back on the layer_flush_done channel when
    /// the flush finishes. You can use that to wait for the flush to finish.
    layer_flush_start_tx: tokio::sync::watch::Sender<u64>,
    /// to be notified when layer flushing has finished, subscribe to the layer_flush_done channel
    layer_flush_done_tx: tokio::sync::watch::Sender<(u64, anyhow::Result<()>)>,

    /// Layer removal lock.
    /// A lock to ensure that no layer of the timeline is removed concurrently by other tasks.
    /// This lock is acquired in [`Timeline::gc`], [`Timeline::compact`],
    /// and [`Tenant::delete_timeline`].
    pub(super) layer_removal_cs: tokio::sync::Mutex<()>,

    // Needed to ensure that we can't create a branch at a point that was already garbage collected
    pub latest_gc_cutoff_lsn: Rcu<Lsn>,

    // List of child timelines and their branch points. This is needed to avoid
    // garbage collecting data that is still needed by the child timelines.
    pub gc_info: std::sync::RwLock<GcInfo>,

    // It may change across major versions so for simplicity
    // keep it after running initdb for a timeline.
    // It is needed in checks when we want to error on some operations
    // when they are requested for pre-initdb lsn.
    // It can be unified with latest_gc_cutoff_lsn under some "first_valid_lsn",
    // though let's keep them both for better error visibility.
    pub initdb_lsn: Lsn,

    /// When did we last calculate the partitioning?
    partitioning: Mutex<(KeyPartitioning, Lsn)>,

    /// Configuration: how often should the partitioning be recalculated.
    repartition_threshold: u64,

    /// Current logical size of the "datadir", at the last LSN.
    current_logical_size: AtomicI64,

    /// Information about the last processed message by the WAL receiver,
    /// or None if WAL receiver has not received anything for this timeline
    /// yet.
    pub last_received_wal: Mutex<Option<WalReceiverInfo>>,
    pub walreceiver: WalReceiver,

    /// Relation size cache
    pub rel_size_cache: RwLock<HashMap<RelTag, (Lsn, BlockNumber)>>,

    download_all_remote_layers_task_info: RwLock<Option<DownloadRemoteLayersTaskInfo>>,

    state: watch::Sender<TimelineState>,

    eviction_task_timeline_state: tokio::sync::Mutex<EvictionTaskTimelineState>,
}

pub struct WalReceiverInfo {
    pub wal_source_connconf: PgConnectionConfig,
    pub last_received_msg_lsn: Lsn,
    pub last_received_msg_ts: u128,
}

///
/// Information about how much history needs to be retained, needed by
/// Garbage Collection.
///
pub struct GcInfo {
    /// Specific LSNs that are needed.
    ///
    /// Currently, this includes all points where child branches have
    /// been forked off from. In the future, could also include
    /// explicit user-defined snapshot points.
    pub retain_lsns: Vec<Lsn>,

    /// In addition to 'retain_lsns', keep everything newer than this
    /// point.
    ///
    /// This is calculated by subtracting 'gc_horizon' setting from
    /// last-record LSN
    ///
    /// FIXME: is this inclusive or exclusive?
    pub horizon_cutoff: Lsn,

    /// In addition to 'retain_lsns' and 'horizon_cutoff', keep everything newer than this
    /// point.
    ///
    /// This is calculated by finding a number such that a record is needed for PITR
    /// if only if its LSN is larger than 'pitr_cutoff'.
    pub pitr_cutoff: Lsn,
}

/// An error happened in a get() operation.
#[derive(thiserror::Error)]
pub enum PageReconstructError {
    #[error(transparent)]
    Other(#[from] anyhow::Error), // source and Display delegate to anyhow::Error

    /// The operation would require downloading a layer that is missing locally.
    NeedsDownload(TenantTimelineId, LayerFileName),

    /// The operation was cancelled
    Cancelled,

    /// The ancestor of this is being stopped
    AncestorStopping(TimelineId),

    /// An error happened replaying WAL records
    #[error(transparent)]
    WalRedo(#[from] crate::walredo::WalRedoError),
}

impl std::fmt::Debug for PageReconstructError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Other(err) => err.fmt(f),
            Self::NeedsDownload(tenant_timeline_id, layer_file_name) => {
                write!(
                    f,
                    "layer {}/{} needs download",
                    tenant_timeline_id,
                    layer_file_name.file_name()
                )
            }
            Self::Cancelled => write!(f, "cancelled"),
            Self::AncestorStopping(timeline_id) => {
                write!(f, "ancestor timeline {timeline_id} is being stopped")
            }
            Self::WalRedo(err) => err.fmt(f),
        }
    }
}

impl std::fmt::Display for PageReconstructError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Other(err) => err.fmt(f),
            Self::NeedsDownload(tenant_timeline_id, layer_file_name) => {
                write!(
                    f,
                    "layer {}/{} needs download",
                    tenant_timeline_id,
                    layer_file_name.file_name()
                )
            }
            Self::Cancelled => write!(f, "cancelled"),
            Self::AncestorStopping(timeline_id) => {
                write!(f, "ancestor timeline {timeline_id} is being stopped")
            }
            Self::WalRedo(err) => err.fmt(f),
        }
    }
}

/// Public interface functions
impl Timeline {
    /// Get the LSN where this branch was created
    pub fn get_ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }

    /// Get the ancestor's timeline id
    pub fn get_ancestor_timeline_id(&self) -> Option<TimelineId> {
        self.ancestor_timeline
            .as_ref()
            .map(|ancestor| ancestor.timeline_id)
    }

    /// Lock and get timeline's GC cuttof
    pub fn get_latest_gc_cutoff_lsn(&self) -> RcuReadGuard<Lsn> {
        self.latest_gc_cutoff_lsn.read()
    }

    /// Look up given page version.
    ///
    /// If a remote layer file is needed, it is downloaded as part of this
    /// call.
    ///
    /// NOTE: It is considered an error to 'get' a key that doesn't exist. The
    /// abstraction above this needs to store suitable metadata to track what
    /// data exists with what keys, in separate metadata entries. If a
    /// non-existent key is requested, we may incorrectly return a value from
    /// an ancestor branch, for example, or waste a lot of cycles chasing the
    /// non-existing key.
    ///
    pub async fn get(
        &self,
        key: Key,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        if !lsn.is_valid() {
            return Err(PageReconstructError::Other(anyhow::anyhow!("Invalid LSN")));
        }

        // XXX: structured stats collection for layer eviction here.
        trace!(
            "get page request for {}@{} from task kind {:?}",
            key,
            lsn,
            ctx.task_kind()
        );

        // Check the page cache. We will get back the most recent page with lsn <= `lsn`.
        // The cached image can be returned directly if there is no WAL between the cached image
        // and requested LSN. The cached image can also be used to reduce the amount of WAL needed
        // for redo.
        let cached_page_img = match self.lookup_cached_page(&key, lsn) {
            Some((cached_lsn, cached_img)) => {
                match cached_lsn.cmp(&lsn) {
                    Ordering::Less => {} // there might be WAL between cached_lsn and lsn, we need to check
                    Ordering::Equal => return Ok(cached_img), // exact LSN match, return the image
                    Ordering::Greater => {
                        unreachable!("the returned lsn should never be after the requested lsn")
                    }
                }
                Some((cached_lsn, cached_img))
            }
            None => None,
        };

        let mut reconstruct_state = ValueReconstructState {
            records: Vec::new(),
            img: cached_page_img,
        };

        self.get_reconstruct_data(key, lsn, &mut reconstruct_state, ctx)
            .await?;

        self.metrics
            .reconstruct_time_histo
            .observe_closure_duration(|| self.reconstruct_value(key, lsn, reconstruct_state))
    }

    /// Get last or prev record separately. Same as get_last_record_rlsn().last/prev.
    pub fn get_last_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().last
    }

    pub fn get_prev_record_lsn(&self) -> Lsn {
        self.last_record_lsn.load().prev
    }

    /// Atomically get both last and prev.
    pub fn get_last_record_rlsn(&self) -> RecordLsn {
        self.last_record_lsn.load()
    }

    pub fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn.load()
    }

    pub fn get_remote_consistent_lsn(&self) -> Option<Lsn> {
        if let Some(remote_client) = &self.remote_client {
            remote_client.last_uploaded_consistent_lsn()
        } else {
            None
        }
    }

    /// The sum of the file size of all historic layers in the layer map.
    /// This method makes no distinction between local and remote layers.
    /// Hence, the result **does not represent local filesystem usage**.
    pub fn layer_size_sum(&self) -> u64 {
        let layer_map = self.layers.read().unwrap();
        let mut size = 0;
        for l in layer_map.iter_historic_layers() {
            size += l.file_size();
        }
        size
    }

    pub fn get_resident_physical_size(&self) -> u64 {
        self.metrics.resident_physical_size_gauge.get()
    }

    ///
    /// Wait until WAL has been received and processed up to this LSN.
    ///
    /// You should call this before any of the other get_* or list_* functions. Calling
    /// those functions with an LSN that has been processed yet is an error.
    ///
    pub async fn wait_lsn(
        &self,
        lsn: Lsn,
        _ctx: &RequestContext, /* Prepare for use by cancellation */
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.is_active(), "Cannot wait for Lsn on inactive timeline");

        // This should never be called from the WAL receiver, because that could lead
        // to a deadlock.
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverManager),
            "wait_lsn cannot be called in WAL receiver"
        );
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverConnectionHandler),
            "wait_lsn cannot be called in WAL receiver"
        );
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverConnectionPoller),
            "wait_lsn cannot be called in WAL receiver"
        );

        let _timer = self.metrics.wait_lsn_time_histo.start_timer();

        match self
            .last_record_lsn
            .wait_for_timeout(lsn, self.conf.wait_lsn_timeout)
            .await
        {
            Ok(()) => Ok(()),
            seqwait_error => {
                drop(_timer);
                let walreceiver_status = self.walreceiver.status().await;
                seqwait_error.with_context(|| format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}, {}",
                    lsn,
                    self.get_last_record_lsn(),
                    self.get_disk_consistent_lsn(),
                    walreceiver_status.map(|status| status.to_human_readable_string())
                            .unwrap_or_else(|| "WalReceiver status: Not active".to_string()),
                ))
            }
        }
    }

    /// Check that it is valid to request operations with that lsn.
    pub fn check_lsn_is_in_scope(
        &self,
        lsn: Lsn,
        latest_gc_cutoff_lsn: &RcuReadGuard<Lsn>,
    ) -> anyhow::Result<()> {
        ensure!(
            lsn >= **latest_gc_cutoff_lsn,
            "LSN {} is earlier than latest GC horizon {} (we might've already garbage collected needed data)",
            lsn,
            **latest_gc_cutoff_lsn,
        );
        Ok(())
    }

    /// Flush to disk all data that was written with the put_* functions
    #[instrument(skip(self), fields(tenant_id=%self.tenant_id, timeline_id=%self.timeline_id))]
    pub async fn freeze_and_flush(&self) -> anyhow::Result<()> {
        self.freeze_inmem_layer(false);
        self.flush_frozen_layers_and_wait().await
    }

    /// Outermost timeline compaction operation; downloads needed layers.
    pub async fn compact(&self, ctx: &RequestContext) -> anyhow::Result<()> {
        const ROUNDS: usize = 2;

        let last_record_lsn = self.get_last_record_lsn();

        // Last record Lsn could be zero in case the timeline was just created
        if !last_record_lsn.is_valid() {
            warn!("Skipping compaction for potentially just initialized timeline, it has invalid last record lsn: {last_record_lsn}");
            return Ok(());
        }

        // retry two times to allow first round to find layers which need to be downloaded, then
        // download them, then retry compaction
        for round in 0..ROUNDS {
            // should we error out with the most specific error?
            let last_round = round == ROUNDS - 1;

            let res = self.compact_inner(ctx).await;

            // If `create_image_layers' or `compact_level0` scheduled any
            // uploads or deletions, but didn't update the index file yet,
            // do it now.
            //
            // This isn't necessary for correctness, the remote state is
            // consistent without the uploads and deletions, and we would
            // update the index file on next flush iteration too. But it
            // could take a while until that happens.
            //
            // Additionally, only do this once before we return from this function.
            if last_round || res.is_ok() {
                if let Some(remote_client) = &self.remote_client {
                    remote_client.schedule_index_upload_for_file_changes()?;
                }
            }

            let rls = match res {
                Ok(()) => return Ok(()),
                Err(CompactionError::DownloadRequired(rls)) if !last_round => {
                    // this can be done at most one time before exiting, waiting
                    rls
                }
                Err(CompactionError::DownloadRequired(rls)) => {
                    anyhow::bail!("Compaction requires downloading multiple times (last was {} layers), possibly battling against eviction", rls.len())
                }
                Err(CompactionError::Other(e)) => {
                    return Err(e);
                }
            };

            // this path can be visited in the second round of retrying, if first one found that we
            // must first download some remote layers
            let total = rls.len();

            let mut downloads = rls
                .into_iter()
                .map(|rl| self.download_remote_layer(rl))
                .collect::<futures::stream::FuturesUnordered<_>>();

            let mut failed = 0;

            let mut cancelled = pin!(task_mgr::shutdown_watcher());

            loop {
                tokio::select! {
                    _ = &mut cancelled => anyhow::bail!("Cancelled while downloading remote layers"),
                    res = downloads.next() => {
                        match res {
                            Some(Ok(())) => {},
                            Some(Err(e)) => {
                                warn!("Downloading remote layer for compaction failed: {e:#}");
                                failed += 1;
                            }
                            None => break,
                        }
                    }
                }
            }

            if failed != 0 {
                anyhow::bail!("{failed} out of {total} layers failed to download, retrying later");
            }

            // if everything downloaded fine, lets try again
        }

        unreachable!("retry loop exits")
    }

    /// Compaction which might need to be retried after downloading remote layers.
    async fn compact_inner(&self, ctx: &RequestContext) -> Result<(), CompactionError> {
        //
        // High level strategy for compaction / image creation:
        //
        // 1. First, calculate the desired "partitioning" of the
        // currently in-use key space. The goal is to partition the
        // key space into roughly fixed-size chunks, but also take into
        // account any existing image layers, and try to align the
        // chunk boundaries with the existing image layers to avoid
        // too much churn. Also try to align chunk boundaries with
        // relation boundaries.  In principle, we don't know about
        // relation boundaries here, we just deal with key-value
        // pairs, and the code in pgdatadir_mapping.rs knows how to
        // map relations into key-value pairs. But in practice we know
        // that 'field6' is the block number, and the fields 1-5
        // identify a relation. This is just an optimization,
        // though.
        //
        // 2. Once we know the partitioning, for each partition,
        // decide if it's time to create a new image layer. The
        // criteria is: there has been too much "churn" since the last
        // image layer? The "churn" is fuzzy concept, it's a
        // combination of too many delta files, or too much WAL in
        // total in the delta file. Or perhaps: if creating an image
        // file would allow to delete some older files.
        //
        // 3. After that, we compact all level0 delta files if there
        // are too many of them.  While compacting, we also garbage
        // collect any page versions that are no longer needed because
        // of the new image layers we created in step 2.
        //
        // TODO: This high level strategy hasn't been implemented yet.
        // Below are functions compact_level0() and create_image_layers()
        // but they are a bit ad hoc and don't quite work like it's explained
        // above. Rewrite it.
        let layer_removal_cs = self.layer_removal_cs.lock().await;
        // Is the timeline being deleted?
        let state = *self.state.borrow();
        if state == TimelineState::Stopping {
            return Err(anyhow::anyhow!("timeline is Stopping").into());
        }

        let target_file_size = self.get_checkpoint_distance();

        // Define partitioning schema if needed

        match self
            .repartition(
                self.get_last_record_lsn(),
                self.get_compaction_target_size(),
                ctx,
            )
            .await
        {
            Ok((partitioning, lsn)) => {
                // 2. Create new image layers for partitions that have been modified
                // "enough".
                let layer_paths_to_upload = self
                    .create_image_layers(&partitioning, lsn, false, ctx)
                    .await
                    .map_err(anyhow::Error::from)?;
                if let Some(remote_client) = &self.remote_client {
                    for (path, layer_metadata) in layer_paths_to_upload {
                        remote_client.schedule_layer_file_upload(&path, &layer_metadata)?;
                    }
                }

                // 3. Compact
                let timer = self.metrics.compact_time_histo.start_timer();
                self.compact_level0(&layer_removal_cs, target_file_size, ctx)
                    .await?;
                timer.stop_and_record();
            }
            Err(err) => {
                // no partitioning? This is normal, if the timeline was just created
                // as an empty timeline. Also in unit tests, when we use the timeline
                // as a simple key-value store, ignoring the datadir layout. Log the
                // error but continue.
                error!("could not compact, repartitioning keyspace failed: {err:?}");
            }
        };

        Ok(())
    }

    /// Mutate the timeline with a [`TimelineWriter`].
    pub fn writer(&self) -> TimelineWriter<'_> {
        TimelineWriter {
            tl: self,
            _write_guard: self.write_lock.lock().unwrap(),
        }
    }

    /// Retrieve current logical size of the timeline.
    ///
    /// The size could be lagging behind the actual number, in case
    /// the initial size calculation has not been run (gets triggered on the first size access).
    ///
    /// return size and boolean flag that shows if the size is exact
    pub fn get_current_logical_size(self: &Arc<Self>) -> u64 {
        self.current_logical_size.load(AtomicOrdering::Relaxed) as u64
    }

    /// Load from KV storage value of logical timeline size and store it in inmemory atomic variable
    pub async fn load_inmem_logical_size(&self) -> anyhow::Result<()> {
        let lsn = self.get_disk_consistent_lsn();
        if lsn != Lsn::INVALID {
            let ctx = RequestContext::todo_child(TaskKind::Startup, DownloadBehavior::Error);
            match self.get_logical_size(lsn, &ctx).await {
                Ok(size) => self
                    .current_logical_size
                    .store(size as i64, AtomicOrdering::Relaxed),
                Err(e) => info!("Failed to load logical size: {:?}", e),
            }
        }
        Ok(())
    }

    /// Check if more than 'checkpoint_distance' of WAL has been accumulated in
    /// the in-memory layer, and initiate flushing it if so.
    ///
    /// Also flush after a period of time without new data -- it helps
    /// safekeepers to regard pageserver as caught up and suspend activity.
    pub fn check_checkpoint_distance(self: &Arc<Timeline>) -> anyhow::Result<()> {
        let last_lsn = self.get_last_record_lsn();
        let layers = self.layers.read().unwrap();
        if let Some(open_layer) = &layers.open_layer {
            let open_layer_size = open_layer.size()?;
            drop(layers);
            let last_freeze_at = self.last_freeze_at.load();
            let last_freeze_ts = *(self.last_freeze_ts.read().unwrap());
            let distance = last_lsn.widening_sub(last_freeze_at);
            // Checkpointing the open layer can be triggered by layer size or LSN range.
            // S3 has a 5 GB limit on the size of one upload (without multi-part upload), and
            // we want to stay below that with a big margin.  The LSN distance determines how
            // much WAL the safekeepers need to store.
            if distance >= self.get_checkpoint_distance().into()
                || open_layer_size > self.get_checkpoint_distance()
                || (distance > 0 && last_freeze_ts.elapsed() >= self.get_checkpoint_timeout())
            {
                info!(
                    "check_checkpoint_distance {}, layer size {}, elapsed since last flush {:?}",
                    distance,
                    open_layer_size,
                    last_freeze_ts.elapsed()
                );

                self.freeze_inmem_layer(true);
                self.last_freeze_at.store(last_lsn);
                *(self.last_freeze_ts.write().unwrap()) = Instant::now();

                // Wake up the layer flusher
                self.flush_frozen_layers();
            }
        }
        Ok(())
    }

    pub fn activate(self: &Arc<Self>, ctx: &RequestContext) -> anyhow::Result<()> {
        if is_broker_client_initialized() {
            self.launch_wal_receiver(ctx, get_broker_client().clone())?;
        } else if cfg!(test) {
            info!("not launching WAL receiver because broker client hasn't been initialized");
        } else {
            anyhow::bail!("broker client not initialized");
        }

        self.set_state(TimelineState::Active);
        self.launch_eviction_task();
        Ok(())
    }

    pub fn set_state(&self, new_state: TimelineState) {
        match (self.current_state(), new_state) {
            (equal_state_1, equal_state_2) if equal_state_1 == equal_state_2 => {
                warn!("Ignoring new state, equal to the existing one: {equal_state_2:?}");
            }
            (st, TimelineState::Loading) => {
                error!("ignoring transition from {st:?} into Loading state");
            }
            (TimelineState::Broken, _) => {
                error!("Ignoring state update {new_state:?} for broken tenant");
            }
            (TimelineState::Stopping, TimelineState::Active) => {
                error!("Not activating a Stopping timeline");
            }
            (_, new_state) => {
                self.state.send_replace(new_state);
            }
        }
    }

    pub fn current_state(&self) -> TimelineState {
        *self.state.borrow()
    }

    pub fn is_active(&self) -> bool {
        self.current_state() == TimelineState::Active
    }

    pub fn subscribe_for_state_updates(&self) -> watch::Receiver<TimelineState> {
        self.state.subscribe()
    }

    pub async fn wait_to_become_active(
        &self,
        _ctx: &RequestContext, // Prepare for use by cancellation
    ) -> Result<(), TimelineState> {
        let mut receiver = self.state.subscribe();
        loop {
            let current_state = *receiver.borrow_and_update();
            match current_state {
                TimelineState::Loading => {
                    receiver
                        .changed()
                        .await
                        .expect("holding a reference to self");
                }
                TimelineState::Active { .. } => {
                    return Ok(());
                }
                TimelineState::Broken { .. } | TimelineState::Stopping => {
                    // There's no chance the timeline can transition back into ::Active
                    return Err(current_state);
                }
            }
        }
    }

    pub fn layer_map_info(&self, reset: LayerAccessStatsReset) -> LayerMapInfo {
        let layer_map = self.layers.read().unwrap();
        let mut in_memory_layers = Vec::with_capacity(layer_map.frozen_layers.len() + 1);
        if let Some(open_layer) = &layer_map.open_layer {
            in_memory_layers.push(open_layer.info());
        }
        for frozen_layer in &layer_map.frozen_layers {
            in_memory_layers.push(frozen_layer.info());
        }

        let mut historic_layers = Vec::new();
        for historic_layer in layer_map.iter_historic_layers() {
            historic_layers.push(historic_layer.info(reset));
        }

        LayerMapInfo {
            in_memory_layers,
            historic_layers,
        }
    }

    #[instrument(skip_all, fields(tenant = %self.tenant_id, timeline = %self.timeline_id))]
    pub async fn download_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
        let Some(layer) = self.find_layer(layer_file_name) else { return Ok(None) };
        let Some(remote_layer) = layer.downcast_remote_layer() else { return  Ok(Some(false)) };
        if self.remote_client.is_none() {
            return Ok(Some(false));
        }

        self.download_remote_layer(remote_layer).await?;
        Ok(Some(true))
    }

    /// Like [`evict_layer_batch`], but for just one layer.
    /// Additional case `Ok(None)` covers the case where the layer could not be found by its `layer_file_name`.
    pub async fn evict_layer(&self, layer_file_name: &str) -> anyhow::Result<Option<bool>> {
        let Some(local_layer) = self.find_layer(layer_file_name) else { return Ok(None) };
        let remote_client = self
            .remote_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("remote storage not configured; cannot evict"))?;

        let cancel = CancellationToken::new();
        let results = self
            .evict_layer_batch(remote_client, &[local_layer], cancel)
            .await?;
        assert_eq!(results.len(), 1);
        let result: Option<anyhow::Result<bool>> = results.into_iter().next().unwrap();
        match result {
            None => anyhow::bail!("task_mgr shutdown requested"),
            Some(Ok(b)) => Ok(Some(b)),
            Some(Err(e)) => Err(e),
        }
    }

    /// Evict a batch of layers.
    ///
    /// GenericRemoteStorage reference is required as a witness[^witness_article] for "remote storage is configured."
    ///
    /// [^witness_article]: https://willcrichton.net/rust-api-type-patterns/witnesses.html
    pub async fn evict_layers(
        &self,
        _: &GenericRemoteStorage,
        layers_to_evict: &[Arc<dyn PersistentLayer>],
        cancel: CancellationToken,
    ) -> anyhow::Result<Vec<Option<anyhow::Result<bool>>>> {
        let remote_client = self.remote_client.clone().expect(
            "GenericRemoteStorage is configured, so timeline must have RemoteTimelineClient",
        );

        self.evict_layer_batch(&remote_client, layers_to_evict, cancel)
            .await
    }

    /// Evict multiple layers at once, continuing through errors.
    ///
    /// Try to evict the given `layers_to_evict` by
    ///
    /// 1. Replacing the given layer object in the layer map with a corresponding [`RemoteLayer`] object.
    /// 2. Deleting the now unreferenced layer file from disk.
    ///
    /// The `remote_client` should be this timeline's `self.remote_client`.
    /// We make the caller provide it so that they are responsible for handling the case
    /// where someone wants to evict the layer but no remote storage is configured.
    ///
    /// Returns either `Err()` or `Ok(results)` where `results.len() == layers_to_evict.len()`.
    /// If `Err()` is returned, no eviction was attempted.
    /// Each position of `Ok(results)` corresponds to the layer in `layers_to_evict`.
    /// Meaning of each `result[i]`:
    /// - `Some(Err(...))` if layer replacement failed for an unexpected reason
    /// - `Some(Ok(true))` if everything went well.
    /// - `Some(Ok(false))` if there was an expected reason why the layer could not be replaced, e.g.:
    ///    - evictee was not yet downloaded
    ///    - replacement failed for an expectable reason (e.g., layer removed by GC before we grabbed all locks)
    /// - `None` if no eviction attempt was made for the layer because `cancel.is_cancelled() == true`.
    async fn evict_layer_batch(
        &self,
        remote_client: &Arc<RemoteTimelineClient>,
        layers_to_evict: &[Arc<dyn PersistentLayer>],
        cancel: CancellationToken,
    ) -> anyhow::Result<Vec<Option<anyhow::Result<bool>>>> {
        // ensure that the layers have finished uploading
        // (don't hold the layer_removal_cs while we do it, we're not removing anything yet)
        remote_client
            .wait_completion()
            .await
            .context("wait for layer upload ops to complete")?;

        // now lock out layer removal (compaction, gc, timeline deletion)
        let layer_removal_guard = self.layer_removal_cs.lock().await;

        {
            // to avoid racing with detach and delete_timeline
            let state = self.current_state();
            anyhow::ensure!(
                state == TimelineState::Active,
                "timeline is not active but {state:?}"
            );
        }

        // start the batch update
        let mut layer_map = self.layers.write().unwrap();
        let mut batch_updates = layer_map.batch_update();

        let mut results = Vec::with_capacity(layers_to_evict.len());

        for l in layers_to_evict.iter() {
            let res = if cancel.is_cancelled() {
                None
            } else {
                Some(self.evict_layer_batch_impl(&layer_removal_guard, l, &mut batch_updates))
            };
            results.push(res);
        }

        // commit the updates & release locks
        batch_updates.flush();
        drop(layer_map);
        drop(layer_removal_guard);

        assert_eq!(results.len(), layers_to_evict.len());
        Ok(results)
    }

    fn evict_layer_batch_impl(
        &self,
        _layer_removal_cs: &tokio::sync::MutexGuard<'_, ()>,
        local_layer: &Arc<dyn PersistentLayer>,
        batch_updates: &mut BatchedUpdates<'_, dyn PersistentLayer>,
    ) -> anyhow::Result<bool> {
        use super::layer_map::Replacement;

        if local_layer.is_remote_layer() {
            // TODO(issue #3851): consider returning an err here instead of false,
            // which is the same out the match later
            return Ok(false);
        }

        let layer_file_size = local_layer.file_size();

        let local_layer_mtime = local_layer
            .local_path()
            .expect("local layer should have a local path")
            .metadata()
            .context("get local layer file stat")?
            .modified()
            .context("get mtime of layer file")?;
        let local_layer_residence_duration =
            match SystemTime::now().duration_since(local_layer_mtime) {
                Err(e) => {
                    warn!("layer mtime is in the future: {}", e);
                    None
                }
                Ok(delta) => Some(delta),
            };

        let layer_metadata = LayerFileMetadata::new(layer_file_size);

        let new_remote_layer = Arc::new(match local_layer.filename() {
            LayerFileName::Image(image_name) => RemoteLayer::new_img(
                self.tenant_id,
                self.timeline_id,
                &image_name,
                &layer_metadata,
                local_layer
                    .access_stats()
                    .clone_for_residence_change(batch_updates, LayerResidenceStatus::Evicted),
            ),
            LayerFileName::Delta(delta_name) => RemoteLayer::new_delta(
                self.tenant_id,
                self.timeline_id,
                &delta_name,
                &layer_metadata,
                local_layer
                    .access_stats()
                    .clone_for_residence_change(batch_updates, LayerResidenceStatus::Evicted),
            ),
        });

        let replaced = match batch_updates.replace_historic(local_layer, new_remote_layer)? {
            Replacement::Replaced { .. } => {
                if let Err(e) = local_layer.delete_resident_layer_file() {
                    error!("failed to remove layer file on evict after replacement: {e:#?}");
                }
                // Always decrement the physical size gauge, even if we failed to delete the file.
                // Rationale: we already replaced the layer with a remote layer in the layer map,
                // and any subsequent download_remote_layer will
                // 1. overwrite the file on disk and
                // 2. add the downloaded size to the resident size gauge.
                //
                // If there is no re-download, and we restart the pageserver, then load_layer_map
                // will treat the file as a local layer again, count it towards resident size,
                // and it'll be like the layer removal never happened.
                // The bump in resident size is perhaps unexpected but overall a robust behavior.
                self.metrics
                    .resident_physical_size_gauge
                    .sub(layer_file_size);

                self.metrics.evictions.inc();

                if let Some(delta) = local_layer_residence_duration {
                    self.metrics
                        .evictions_with_low_residence_duration
                        .read()
                        .unwrap()
                        .observe(delta);
                    info!(layer=%local_layer.short_id(), residence_millis=delta.as_millis(), "evicted layer after known residence period");
                } else {
                    info!(layer=%local_layer.short_id(), "evicted layer after unknown residence period");
                }

                true
            }
            Replacement::NotFound => {
                debug!(evicted=?local_layer, "layer was no longer in layer map");
                false
            }
            Replacement::RemovalBuffered => {
                unreachable!("not doing anything else in this batch")
            }
            Replacement::Unexpected(other) => {
                error!(
                    local_layer.ptr=?Arc::as_ptr(local_layer),
                    other.ptr=?Arc::as_ptr(&other),
                    ?other,
                    "failed to replace");
                false
            }
        };

        Ok(replaced)
    }
}

// Private functions
impl Timeline {
    fn get_checkpoint_distance(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_distance
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_distance)
    }

    fn get_checkpoint_timeout(&self) -> Duration {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .checkpoint_timeout
            .unwrap_or(self.conf.default_tenant_conf.checkpoint_timeout)
    }

    fn get_compaction_target_size(&self) -> u64 {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_target_size
            .unwrap_or(self.conf.default_tenant_conf.compaction_target_size)
    }

    fn get_compaction_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .compaction_threshold
            .unwrap_or(self.conf.default_tenant_conf.compaction_threshold)
    }

    fn get_image_creation_threshold(&self) -> usize {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .image_creation_threshold
            .unwrap_or(self.conf.default_tenant_conf.image_creation_threshold)
    }

    fn get_eviction_policy(&self) -> EvictionPolicy {
        let tenant_conf = self.tenant_conf.read().unwrap();
        tenant_conf
            .eviction_policy
            .unwrap_or(self.conf.default_tenant_conf.eviction_policy)
    }

    fn get_evictions_low_residence_duration_metric_threshold(
        tenant_conf: &TenantConfOpt,
        default_tenant_conf: &TenantConf,
    ) -> Duration {
        tenant_conf
            .evictions_low_residence_duration_metric_threshold
            .unwrap_or(default_tenant_conf.evictions_low_residence_duration_metric_threshold)
    }

    pub(super) fn tenant_conf_updated(&self) {
        // NB: Most tenant conf options are read by background loops, so,
        // changes will automatically be picked up.

        // The threshold is embedded in the metric. So, we need to update it.
        {
            let new_threshold = Self::get_evictions_low_residence_duration_metric_threshold(
                &self.tenant_conf.read().unwrap(),
                &self.conf.default_tenant_conf,
            );
            let tenant_id_str = self.tenant_id.to_string();
            let timeline_id_str = self.timeline_id.to_string();
            self.metrics
                .evictions_with_low_residence_duration
                .write()
                .unwrap()
                .change_threshold(&tenant_id_str, &timeline_id_str, new_threshold);
        }
    }

    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        conf: &'static PageServerConf,
        tenant_conf: Arc<RwLock<TenantConfOpt>>,
        metadata: &TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        remote_client: Option<RemoteTimelineClient>,
        pg_version: u32,
    ) -> Arc<Self> {
        let disk_consistent_lsn = metadata.disk_consistent_lsn();
        let (state, _) = watch::channel(TimelineState::Loading);

        let (layer_flush_start_tx, _) = tokio::sync::watch::channel(0);
        let (layer_flush_done_tx, _) = tokio::sync::watch::channel((0, Ok(())));

        let tenant_conf_guard = tenant_conf.read().unwrap();
        let wal_connect_timeout = tenant_conf_guard
            .walreceiver_connect_timeout
            .unwrap_or(conf.default_tenant_conf.walreceiver_connect_timeout);
        let lagging_wal_timeout = tenant_conf_guard
            .lagging_wal_timeout
            .unwrap_or(conf.default_tenant_conf.lagging_wal_timeout);
        let max_lsn_wal_lag = tenant_conf_guard
            .max_lsn_wal_lag
            .unwrap_or(conf.default_tenant_conf.max_lsn_wal_lag);
        let evictions_low_residence_duration_metric_threshold =
            Self::get_evictions_low_residence_duration_metric_threshold(
                &tenant_conf_guard,
                &conf.default_tenant_conf,
            );
        drop(tenant_conf_guard);

        Arc::new_cyclic(|myself| {
            let walreceiver = WalReceiver::new(
                TenantTimelineId::new(tenant_id, timeline_id),
                Weak::clone(myself),
                WalReceiverConf {
                    wal_connect_timeout,
                    lagging_wal_timeout,
                    max_lsn_wal_lag,
                    auth_token: crate::config::SAFEKEEPER_AUTH_TOKEN.get().cloned(),
                    availability_zone: conf.availability_zone.clone(),
                },
            );

            let mut result = Timeline {
                conf,
                tenant_conf,
                myself: myself.clone(),
                timeline_id,
                tenant_id,
                pg_version,
                layers: RwLock::new(LayerMap::default()),
                wanted_image_layers: Mutex::new(None),

                walredo_mgr,
                walreceiver,

                remote_client: remote_client.map(Arc::new),

                // initialize in-memory 'last_record_lsn' from 'disk_consistent_lsn'.
                last_record_lsn: SeqWait::new(RecordLsn {
                    last: disk_consistent_lsn,
                    prev: metadata.prev_record_lsn().unwrap_or(Lsn(0)),
                }),
                disk_consistent_lsn: AtomicLsn::new(disk_consistent_lsn.0),

                last_freeze_at: AtomicLsn::new(disk_consistent_lsn.0),
                last_freeze_ts: RwLock::new(Instant::now()),

                ancestor_timeline: ancestor,
                ancestor_lsn: metadata.ancestor_lsn(),

                metrics: TimelineMetrics::new(
                    &tenant_id,
                    &timeline_id,
                    crate::metrics::EvictionsWithLowResidenceDurationBuilder::new(
                        "mtime",
                        evictions_low_residence_duration_metric_threshold,
                    ),
                ),

                flush_loop_state: Mutex::new(FlushLoopState::NotStarted),

                layer_flush_start_tx,
                layer_flush_done_tx,

                write_lock: Mutex::new(()),
                layer_removal_cs: Default::default(),

                gc_info: std::sync::RwLock::new(GcInfo {
                    retain_lsns: Vec::new(),
                    horizon_cutoff: Lsn(0),
                    pitr_cutoff: Lsn(0),
                }),

                latest_gc_cutoff_lsn: Rcu::new(metadata.latest_gc_cutoff_lsn()),
                initdb_lsn: metadata.initdb_lsn(),

                current_logical_size: AtomicI64::new(0),
                partitioning: Mutex::new((KeyPartitioning::new(), Lsn(0))),
                repartition_threshold: 0,

                last_received_wal: Mutex::new(None),
                rel_size_cache: RwLock::new(HashMap::new()),

                download_all_remote_layers_task_info: RwLock::new(None),

                state,

                eviction_task_timeline_state: tokio::sync::Mutex::new(
                    EvictionTaskTimelineState::default(),
                ),
            };
            result.repartition_threshold = result.get_checkpoint_distance() / 10;
            result
                .metrics
                .last_record_gauge
                .set(disk_consistent_lsn.0 as i64);
            result
        })
    }

    pub(super) fn maybe_spawn_flush_loop(self: &Arc<Self>) {
        let mut flush_loop_state = self.flush_loop_state.lock().unwrap();
        match *flush_loop_state {
            FlushLoopState::NotStarted => (),
            FlushLoopState::Running => {
                info!(
                    "skipping attempt to start flush_loop twice {}/{}",
                    self.tenant_id, self.timeline_id
                );
                return;
            }
            FlushLoopState::Exited => {
                warn!(
                    "ignoring attempt to restart exited flush_loop {}/{}",
                    self.tenant_id, self.timeline_id
                );
                return;
            }
        }

        let layer_flush_start_rx = self.layer_flush_start_tx.subscribe();
        let self_clone = Arc::clone(self);

        info!("spawning flush loop");
        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::LayerFlushTask,
            Some(self.tenant_id),
            Some(self.timeline_id),
            "layer flush task",
            false,
            async move {
                let background_ctx = RequestContext::todo_child(TaskKind::LayerFlushTask, DownloadBehavior::Error);
                self_clone.flush_loop(layer_flush_start_rx, &background_ctx).await;
                let mut flush_loop_state = self_clone.flush_loop_state.lock().unwrap();
                assert_eq!(*flush_loop_state, FlushLoopState::Running);
                *flush_loop_state  = FlushLoopState::Exited;
                Ok(())
            }
            .instrument(info_span!(parent: None, "layer flush task", tenant = %self.tenant_id, timeline = %self.timeline_id))
        );

        *flush_loop_state = FlushLoopState::Running;
    }

    pub(super) fn launch_wal_receiver(
        &self,
        ctx: &RequestContext,
        broker_client: BrokerClientChannel,
    ) -> anyhow::Result<()> {
        info!(
            "launching WAL receiver for timeline {} of tenant {}",
            self.timeline_id, self.tenant_id
        );
        self.walreceiver.start(ctx, broker_client)?;
        Ok(())
    }

    ///
    /// Scan the timeline directory to populate the layer map.
    /// Returns all timeline-related files that were found and loaded.
    ///
    pub(super) fn load_layer_map(&self, disk_consistent_lsn: Lsn) -> anyhow::Result<()> {
        let mut layers = self.layers.write().unwrap();
        let mut updates = layers.batch_update();
        let mut num_layers = 0;

        let timer = self.metrics.load_layer_map_histo.start_timer();

        // Scan timeline directory and create ImageFileName and DeltaFilename
        // structs representing all files on disk
        let timeline_path = self.conf.timeline_path(&self.timeline_id, &self.tenant_id);
        // total size of layer files in the current timeline directory
        let mut total_physical_size = 0;

        for direntry in fs::read_dir(timeline_path)? {
            let direntry = direntry?;
            let direntry_path = direntry.path();
            let fname = direntry.file_name();
            let fname = fname.to_string_lossy();

            if let Some(imgfilename) = ImageFileName::parse_str(&fname) {
                // create an ImageLayer struct for each image file.
                if imgfilename.lsn > disk_consistent_lsn {
                    warn!(
                        "found future image layer {} on timeline {} disk_consistent_lsn is {}",
                        imgfilename, self.timeline_id, disk_consistent_lsn
                    );

                    rename_to_backup(&direntry_path)?;
                    continue;
                }

                let file_size = direntry_path.metadata()?.len();

                let layer = ImageLayer::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_id,
                    &imgfilename,
                    file_size,
                    LayerAccessStats::for_loading_layer(&updates, LayerResidenceStatus::Resident),
                );

                trace!("found layer {}", layer.path().display());
                total_physical_size += file_size;
                updates.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if let Some(deltafilename) = DeltaFileName::parse_str(&fname) {
                // Create a DeltaLayer struct for each delta file.
                // The end-LSN is exclusive, while disk_consistent_lsn is
                // inclusive. For example, if disk_consistent_lsn is 100, it is
                // OK for a delta layer to have end LSN 101, but if the end LSN
                // is 102, then it might not have been fully flushed to disk
                // before crash.
                if deltafilename.lsn_range.end > disk_consistent_lsn + 1 {
                    warn!(
                        "found future delta layer {} on timeline {} disk_consistent_lsn is {}",
                        deltafilename, self.timeline_id, disk_consistent_lsn
                    );

                    rename_to_backup(&direntry_path)?;
                    continue;
                }

                let file_size = direntry_path.metadata()?.len();

                let layer = DeltaLayer::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_id,
                    &deltafilename,
                    file_size,
                    LayerAccessStats::for_loading_layer(&updates, LayerResidenceStatus::Resident),
                );

                trace!("found layer {}", layer.path().display());
                total_physical_size += file_size;
                updates.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if fname == METADATA_FILE_NAME || fname.ends_with(".old") {
                // ignore these
            } else if remote_timeline_client::is_temp_download_file(&direntry_path) {
                info!(
                    "skipping temp download file, reconcile_with_remote will resume / clean up: {}",
                    fname
                );
            } else if is_ephemeral_file(&fname) {
                // Delete any old ephemeral files
                trace!("deleting old ephemeral file in timeline dir: {}", fname);
                fs::remove_file(&direntry_path)?;
            } else if is_temporary(&direntry_path) {
                info!("removing temp timeline file at {}", direntry_path.display());
                fs::remove_file(&direntry_path).with_context(|| {
                    format!(
                        "failed to remove temp download file at {}",
                        direntry_path.display()
                    )
                })?;
            } else {
                warn!("unrecognized filename in timeline dir: {}", fname);
            }
        }

        updates.flush();
        layers.next_open_layer_at = Some(Lsn(disk_consistent_lsn.0) + 1);

        info!(
            "loaded layer map with {} layers at {}, total physical size: {}",
            num_layers, disk_consistent_lsn, total_physical_size
        );
        self.metrics
            .resident_physical_size_gauge
            .set(total_physical_size);

        timer.stop_and_record();

        Ok(())
    }

    async fn create_remote_layers(
        &self,
        index_part: &IndexPart,
        local_layers: HashMap<LayerFileName, Arc<dyn PersistentLayer>>,
        up_to_date_disk_consistent_lsn: Lsn,
    ) -> anyhow::Result<HashMap<LayerFileName, Arc<dyn PersistentLayer>>> {
        // Are we missing some files that are present in remote storage?
        // Create RemoteLayer instances for them.
        let mut local_only_layers = local_layers;

        // We're holding a layer map lock for a while but this
        // method is only called during init so it's fine.
        let mut layer_map = self.layers.write().unwrap();
        let mut updates = layer_map.batch_update();
        for remote_layer_name in &index_part.timeline_layers {
            let local_layer = local_only_layers.remove(remote_layer_name);

            let remote_layer_metadata = index_part
                .layer_metadata
                .get(remote_layer_name)
                .map(LayerFileMetadata::from)
                .with_context(|| {
                    format!(
                        "No remote layer metadata found for layer {}",
                        remote_layer_name.file_name()
                    )
                })?;

            // Is the local layer's size different from the size stored in the
            // remote index file?
            // If so, rename_to_backup those files & replace their local layer with
            // a RemoteLayer in the layer map so that we re-download them on-demand.
            if let Some(local_layer) = local_layer {
                let local_layer_path = local_layer
                    .local_path()
                    .expect("caller must ensure that local_layers only contains local layers");
                ensure!(
                    local_layer_path.exists(),
                    "every layer from local_layers must exist on disk: {}",
                    local_layer_path.display()
                );

                let remote_size = remote_layer_metadata.file_size();
                let metadata = local_layer_path.metadata().with_context(|| {
                    format!(
                        "get file size of local layer {}",
                        local_layer_path.display()
                    )
                })?;
                let local_size = metadata.len();
                if local_size != remote_size {
                    warn!("removing local file {local_layer_path:?} because it has unexpected length {local_size}; length in remote index is {remote_size}");
                    if let Err(err) = rename_to_backup(&local_layer_path) {
                        assert!(local_layer_path.exists(), "we would leave the local_layer without a file if this does not hold: {}", local_layer_path.display());
                        anyhow::bail!("could not rename file {local_layer_path:?}: {err:?}");
                    } else {
                        self.metrics.resident_physical_size_gauge.sub(local_size);
                        updates.remove_historic(local_layer);
                        // fall-through to adding the remote layer
                    }
                } else {
                    debug!(
                        "layer is present locally and file size matches remote, using it: {}",
                        local_layer_path.display()
                    );
                    continue;
                }
            }

            info!(
                "remote layer does not exist locally, creating remote layer: {}",
                remote_layer_name.file_name()
            );

            match remote_layer_name {
                LayerFileName::Image(imgfilename) => {
                    if imgfilename.lsn > up_to_date_disk_consistent_lsn {
                        warn!(
                        "found future image layer {} on timeline {} remote_consistent_lsn is {}",
                        imgfilename, self.timeline_id, up_to_date_disk_consistent_lsn
                    );
                        continue;
                    }

                    let remote_layer = RemoteLayer::new_img(
                        self.tenant_id,
                        self.timeline_id,
                        imgfilename,
                        &remote_layer_metadata,
                        LayerAccessStats::for_loading_layer(
                            &updates,
                            LayerResidenceStatus::Evicted,
                        ),
                    );
                    let remote_layer = Arc::new(remote_layer);

                    updates.insert_historic(remote_layer);
                }
                LayerFileName::Delta(deltafilename) => {
                    // Create a RemoteLayer for the delta file.
                    // The end-LSN is exclusive, while disk_consistent_lsn is
                    // inclusive. For example, if disk_consistent_lsn is 100, it is
                    // OK for a delta layer to have end LSN 101, but if the end LSN
                    // is 102, then it might not have been fully flushed to disk
                    // before crash.
                    if deltafilename.lsn_range.end > up_to_date_disk_consistent_lsn + 1 {
                        warn!(
                            "found future delta layer {} on timeline {} remote_consistent_lsn is {}",
                            deltafilename, self.timeline_id, up_to_date_disk_consistent_lsn
                        );
                        continue;
                    }
                    let remote_layer = RemoteLayer::new_delta(
                        self.tenant_id,
                        self.timeline_id,
                        deltafilename,
                        &remote_layer_metadata,
                        LayerAccessStats::for_loading_layer(
                            &updates,
                            LayerResidenceStatus::Evicted,
                        ),
                    );
                    let remote_layer = Arc::new(remote_layer);
                    updates.insert_historic(remote_layer);
                }
            }
        }

        updates.flush();
        Ok(local_only_layers)
    }

    /// This function will synchronize local state with what we have in remote storage.
    ///
    /// Steps taken:
    /// 1. Initialize upload queue based on `index_part`.
    /// 2. Create `RemoteLayer` instances for layers that exist only on the remote.
    ///    The list of layers on the remote comes from `index_part`.
    ///    The list of local layers is given by the layer map's `iter_historic_layers()`.
    ///    So, the layer map must have been loaded already.
    /// 3. Schedule upload of local-only layer files (which will then also update the remote
    ///    IndexPart to include the new layer files).
    ///
    /// Refer to the `storage_sync` module comment for more context.
    ///
    /// # TODO
    /// May be a bit cleaner to do things based on populated remote client,
    /// and then do things based on its upload_queue.latest_files.
    #[instrument(skip(self, index_part, up_to_date_metadata))]
    pub async fn reconcile_with_remote(
        &self,
        up_to_date_metadata: &TimelineMetadata,
        index_part: Option<&IndexPart>,
    ) -> anyhow::Result<()> {
        info!("starting");
        let remote_client = self
            .remote_client
            .as_ref()
            .ok_or_else(|| anyhow!("cannot download without remote storage"))?;

        let disk_consistent_lsn = up_to_date_metadata.disk_consistent_lsn();

        let local_layers = self
            .layers
            .read()
            .unwrap()
            .iter_historic_layers()
            .map(|l| (l.filename(), l))
            .collect::<HashMap<_, _>>();

        // If no writes happen, new branches do not have any layers, only the metadata file.
        let has_local_layers = !local_layers.is_empty();
        let local_only_layers = match index_part {
            Some(index_part) => {
                info!(
                    "initializing upload queue from remote index with {} layer files",
                    index_part.timeline_layers.len()
                );
                remote_client.init_upload_queue(index_part)?;
                self.create_remote_layers(index_part, local_layers, disk_consistent_lsn)
                    .await?
            }
            None => {
                info!("initializing upload queue as empty");
                remote_client.init_upload_queue_for_empty_remote(up_to_date_metadata)?;
                local_layers
            }
        };

        if has_local_layers {
            // Are there local files that don't exist remotely? Schedule uploads for them.
            // Local timeline metadata will get uploaded to remove along witht he layers.
            for (layer_name, layer) in &local_only_layers {
                // XXX solve this in the type system
                let layer_path = layer
                    .local_path()
                    .expect("local_only_layers only contains local layers");
                let layer_size = layer_path
                    .metadata()
                    .with_context(|| format!("failed to get file {layer_path:?} metadata"))?
                    .len();
                info!("scheduling {layer_path:?} for upload");
                remote_client
                    .schedule_layer_file_upload(layer_name, &LayerFileMetadata::new(layer_size))?;
            }
            remote_client.schedule_index_upload_for_file_changes()?;
        } else if index_part.is_none() {
            // No data on the remote storage, no local layers, local metadata file.
            //
            // TODO https://github.com/neondatabase/neon/issues/3865
            // Currently, console does not wait for the timeline data upload to the remote storage
            // and considers the timeline created, expecting other pageserver nodes to work with it.
            // Branch metadata upload could get interrupted (e.g pageserver got killed),
            // hence any locally existing branch metadata with no remote counterpart should be uploaded,
            // otherwise any other pageserver won't see the branch on `attach`.
            //
            // After the issue gets implemented, pageserver should rather remove the branch,
            // since absence on S3 means we did not acknowledge the branch creation and console will have to retry,
            // no need to keep the old files.
            remote_client.schedule_index_upload_for_metadata_update(up_to_date_metadata)?;
        } else {
            // Local timeline has a metadata file, remote one too, both have no layers to sync.
        }

        info!("Done");

        Ok(())
    }

    /// Update current logical size, adding `delta' to the old value.
    fn update_current_logical_size(&self, delta: i64) -> u64 {
        let prev_size = self
            .current_logical_size
            .fetch_add(delta, AtomicOrdering::SeqCst);
        (prev_size + delta) as u64
    }

    fn find_layer(&self, layer_file_name: &str) -> Option<Arc<dyn PersistentLayer>> {
        for historic_layer in self.layers.read().unwrap().iter_historic_layers() {
            let historic_layer_name = historic_layer.filename().file_name();
            if layer_file_name == historic_layer_name {
                return Some(historic_layer);
            }
        }

        None
    }

    /// Removes the layer from local FS (if present) and from memory.
    /// Remote storage is not affected by this operation.
    fn delete_historic_layer(
        &self,
        // we cannot remove layers otherwise, since gc and compaction will race
        _layer_removal_cs: &tokio::sync::MutexGuard<'_, ()>,
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_, dyn PersistentLayer>,
    ) -> anyhow::Result<()> {
        if !layer.is_remote_layer() {
            layer.delete_resident_layer_file()?;
            let layer_file_size = layer.file_size();
            self.metrics
                .resident_physical_size_gauge
                .sub(layer_file_size);
        }

        // TODO Removing from the bottom of the layer map is expensive.
        //      Maybe instead discard all layer map historic versions that
        //      won't be needed for page reconstruction for this timeline,
        //      and mark what we can't delete yet as deleted from the layer
        //      map index without actually rebuilding the index.
        updates.remove_historic(layer);

        Ok(())
    }
}

type TraversalId = String;

trait TraversalLayerExt {
    fn traversal_id(&self) -> TraversalId;
}

impl TraversalLayerExt for Arc<dyn PersistentLayer> {
    fn traversal_id(&self) -> TraversalId {
        match self.local_path() {
            Some(local_path) => {
                debug_assert!(local_path.to_str().unwrap().contains(&format!("{}", self.get_timeline_id())),
                    "need timeline ID to uniquely identify the layer when traversal crosses ancestor boundary",
                );
                format!("{}", local_path.display())
            }
            None => {
                format!(
                    "remote {}/{}",
                    self.get_timeline_id(),
                    self.filename().file_name()
                )
            }
        }
    }
}

impl TraversalLayerExt for Arc<InMemoryLayer> {
    fn traversal_id(&self) -> TraversalId {
        format!(
            "timeline {} in-memory {}",
            self.get_timeline_id(),
            self.short_id()
        )
    }
}

impl Timeline {
    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    async fn get_reconstruct_data(
        &self,
        key: Key,
        request_lsn: Lsn,
        reconstruct_state: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), PageReconstructError> {
        // Start from the current timeline.
        let mut timeline_owned;
        let mut timeline = self;

        // For debugging purposes, collect the path of layers that we traversed
        // through. It's included in the error message if we fail to find the key.
        let mut traversal_path = Vec::<TraversalPathItem>::new();

        let cached_lsn = if let Some((cached_lsn, _)) = &reconstruct_state.img {
            *cached_lsn
        } else {
            Lsn(0)
        };

        // 'prev_lsn' tracks the last LSN that we were at in our search. It's used
        // to check that each iteration make some progress, to break infinite
        // looping if something goes wrong.
        let mut prev_lsn = Lsn(u64::MAX);

        let mut result = ValueReconstructResult::Continue;
        let mut cont_lsn = Lsn(request_lsn.0 + 1);

        'outer: loop {
            // The function should have updated 'state'
            //info!("CALLED for {} at {}: {:?} with {} records, cached {}", key, cont_lsn, result, reconstruct_state.records.len(), cached_lsn);
            match result {
                ValueReconstructResult::Complete => return Ok(()),
                ValueReconstructResult::Continue => {
                    // If we reached an earlier cached page image, we're done.
                    if cont_lsn == cached_lsn + 1 {
                        self.metrics.materialized_page_cache_hit_counter.inc_by(1);
                        return Ok(());
                    }
                    if prev_lsn <= cont_lsn {
                        // Didn't make any progress in last iteration. Error out to avoid
                        // getting stuck in the loop.
                        return Err(layer_traversal_error(format!(
                            "could not find layer with more data for key {} at LSN {}, request LSN {}, ancestor {}",
                            key,
                            Lsn(cont_lsn.0 - 1),
                            request_lsn,
                            timeline.ancestor_lsn
                        ), traversal_path));
                    }
                    prev_lsn = cont_lsn;
                }
                ValueReconstructResult::Missing => {
                    return Err(layer_traversal_error(
                        format!(
                            "could not find data for key {} at LSN {}, for request at LSN {}",
                            key, cont_lsn, request_lsn
                        ),
                        traversal_path,
                    ));
                }
            }

            // Recurse into ancestor if needed
            if Lsn(cont_lsn.0 - 1) <= timeline.ancestor_lsn {
                trace!(
                    "going into ancestor {}, cont_lsn is {}",
                    timeline.ancestor_lsn,
                    cont_lsn
                );
                let ancestor = match timeline.get_ancestor_timeline() {
                    Ok(timeline) => timeline,
                    Err(e) => return Err(PageReconstructError::from(e)),
                };

                // It's possible that the ancestor timeline isn't active yet, or
                // is active but hasn't yet caught up to the branch point. Wait
                // for it.
                //
                // This cannot happen while the pageserver is running normally,
                // because you cannot create a branch from a point that isn't
                // present in the pageserver yet. However, we don't wait for the
                // branch point to be uploaded to cloud storage before creating
                // a branch. I.e., the branch LSN need not be remote consistent
                // for the branching operation to succeed.
                //
                // Hence, if we try to load a tenant in such a state where
                // 1. the existence of the branch was persisted (in IndexPart and/or locally)
                // 2. but the ancestor state is behind branch_lsn because it was not yet persisted
                // then we will need to wait for the ancestor timeline to
                // re-stream WAL up to branch_lsn before we access it.
                //
                // How can a tenant get in such a state?
                // - ungraceful pageserver process exit
                // - detach+attach => this is a bug, https://github.com/neondatabase/neon/issues/4219
                //
                // NB: this could be avoided by requiring
                //   branch_lsn >= remote_consistent_lsn
                // during branch creation.
                match ancestor.wait_to_become_active(ctx).await {
                    Ok(()) => {}
                    Err(state) if state == TimelineState::Stopping => {
                        return Err(PageReconstructError::AncestorStopping(ancestor.timeline_id));
                    }
                    Err(state) => {
                        return Err(PageReconstructError::Other(anyhow::anyhow!(
                            "Timeline {} will not become active. Current state: {:?}",
                            ancestor.timeline_id,
                            &state,
                        )));
                    }
                }
                ancestor.wait_lsn(timeline.ancestor_lsn, ctx).await?;

                timeline_owned = ancestor;
                timeline = &*timeline_owned;
                prev_lsn = Lsn(u64::MAX);
                continue 'outer;
            }

            #[allow(clippy::never_loop)] // see comment at bottom of this loop
            'layer_map_search: loop {
                let remote_layer = {
                    let layers = timeline.layers.read().unwrap();

                    // Check the open and frozen in-memory layers first, in order from newest
                    // to oldest.
                    if let Some(open_layer) = &layers.open_layer {
                        let start_lsn = open_layer.get_lsn_range().start;
                        if cont_lsn > start_lsn {
                            //info!("CHECKING for {} at {} on open layer {}", key, cont_lsn, open_layer.filename().display());
                            // Get all the data needed to reconstruct the page version from this layer.
                            // But if we have an older cached page image, no need to go past that.
                            let lsn_floor = max(cached_lsn + 1, start_lsn);
                            result = match open_layer.get_value_reconstruct_data(
                                key,
                                lsn_floor..cont_lsn,
                                reconstruct_state,
                                ctx,
                            ) {
                                Ok(result) => result,
                                Err(e) => return Err(PageReconstructError::from(e)),
                            };
                            cont_lsn = lsn_floor;
                            traversal_path.push((
                                result,
                                cont_lsn,
                                Box::new({
                                    let open_layer = Arc::clone(open_layer);
                                    move || open_layer.traversal_id()
                                }),
                            ));
                            continue 'outer;
                        }
                    }
                    for frozen_layer in layers.frozen_layers.iter().rev() {
                        let start_lsn = frozen_layer.get_lsn_range().start;
                        if cont_lsn > start_lsn {
                            //info!("CHECKING for {} at {} on frozen layer {}", key, cont_lsn, frozen_layer.filename().display());
                            let lsn_floor = max(cached_lsn + 1, start_lsn);
                            result = match frozen_layer.get_value_reconstruct_data(
                                key,
                                lsn_floor..cont_lsn,
                                reconstruct_state,
                                ctx,
                            ) {
                                Ok(result) => result,
                                Err(e) => return Err(PageReconstructError::from(e)),
                            };
                            cont_lsn = lsn_floor;
                            traversal_path.push((
                                result,
                                cont_lsn,
                                Box::new({
                                    let frozen_layer = Arc::clone(frozen_layer);
                                    move || frozen_layer.traversal_id()
                                }),
                            ));
                            continue 'outer;
                        }
                    }

                    if let Some(SearchResult { lsn_floor, layer }) = layers.search(key, cont_lsn) {
                        // If it's a remote layer, download it and retry.
                        if let Some(remote_layer) =
                            super::storage_layer::downcast_remote_layer(&layer)
                        {
                            // TODO: push a breadcrumb to 'traversal_path' to record the fact that
                            // we downloaded / would need to download this layer.
                            remote_layer // download happens outside the scope of `layers` guard object
                        } else {
                            // Get all the data needed to reconstruct the page version from this layer.
                            // But if we have an older cached page image, no need to go past that.
                            let lsn_floor = max(cached_lsn + 1, lsn_floor);
                            result = match layer.get_value_reconstruct_data(
                                key,
                                lsn_floor..cont_lsn,
                                reconstruct_state,
                                ctx,
                            ) {
                                Ok(result) => result,
                                Err(e) => return Err(PageReconstructError::from(e)),
                            };
                            cont_lsn = lsn_floor;
                            traversal_path.push((
                                result,
                                cont_lsn,
                                Box::new({
                                    let layer = Arc::clone(&layer);
                                    move || layer.traversal_id()
                                }),
                            ));
                            continue 'outer;
                        }
                    } else if timeline.ancestor_timeline.is_some() {
                        // Nothing on this timeline. Traverse to parent
                        result = ValueReconstructResult::Continue;
                        cont_lsn = Lsn(timeline.ancestor_lsn.0 + 1);
                        continue 'outer;
                    } else {
                        // Nothing found
                        result = ValueReconstructResult::Missing;
                        continue 'outer;
                    }
                };
                // Download the remote_layer and replace it in the layer map.
                // For that, we need to release the mutex. Otherwise, we'd deadlock.
                //
                // The control flow is so weird here because `drop(layers)` inside
                // the if stmt above is not enough for current rustc: it requires
                // that the layers lock guard is not in scope across the download
                // await point.
                let remote_layer_as_persistent: Arc<dyn PersistentLayer> =
                    Arc::clone(&remote_layer) as Arc<dyn PersistentLayer>;
                let id = remote_layer_as_persistent.traversal_id();
                info!(
                    "need remote layer {} for task kind {:?}",
                    id,
                    ctx.task_kind()
                );

                // The next layer doesn't exist locally. Need to download it.
                // (The control flow is a bit complicated here because we must drop the 'layers'
                // lock before awaiting on the Future.)
                match (
                    ctx.download_behavior(),
                    self.conf.ondemand_download_behavior_treat_error_as_warn,
                ) {
                    (DownloadBehavior::Download, _) => {
                        info!(
                            "on-demand downloading remote layer {id} for task kind {:?}",
                            ctx.task_kind()
                        );
                        timeline.download_remote_layer(remote_layer).await?;
                        continue 'layer_map_search;
                    }
                    (DownloadBehavior::Warn, _) | (DownloadBehavior::Error, true) => {
                        warn!(
                            "unexpectedly on-demand downloading remote layer {} for task kind {:?}",
                            id,
                            ctx.task_kind()
                        );
                        UNEXPECTED_ONDEMAND_DOWNLOADS.inc();
                        timeline.download_remote_layer(remote_layer).await?;
                        continue 'layer_map_search;
                    }
                    (DownloadBehavior::Error, false) => {
                        return Err(PageReconstructError::NeedsDownload(
                            TenantTimelineId::new(self.tenant_id, self.timeline_id),
                            remote_layer.file_name.clone(),
                        ))
                    }
                }
            }
        }
    }

    fn lookup_cached_page(&self, key: &Key, lsn: Lsn) -> Option<(Lsn, Bytes)> {
        let cache = page_cache::get();

        // FIXME: It's pointless to check the cache for things that are not 8kB pages.
        // We should look at the key to determine if it's a cacheable object
        let (lsn, read_guard) =
            cache.lookup_materialized_page(self.tenant_id, self.timeline_id, key, lsn)?;
        let img = Bytes::from(read_guard.to_vec());
        Some((lsn, img))
    }

    fn get_ancestor_timeline(&self) -> anyhow::Result<Arc<Timeline>> {
        let ancestor = self.ancestor_timeline.as_ref().with_context(|| {
            format!(
                "Ancestor is missing. Timeline id: {} Ancestor id {:?}",
                self.timeline_id,
                self.get_ancestor_timeline_id(),
            )
        })?;
        Ok(Arc::clone(ancestor))
    }

    ///
    /// Get a handle to the latest layer for appending.
    ///
    fn get_layer_for_write(&self, lsn: Lsn) -> anyhow::Result<Arc<InMemoryLayer>> {
        let mut layers = self.layers.write().unwrap();

        ensure!(lsn.is_aligned());

        let last_record_lsn = self.get_last_record_lsn();
        ensure!(
            lsn > last_record_lsn,
            "cannot modify relation after advancing last_record_lsn (incoming_lsn={}, last_record_lsn={})",
            lsn,
            last_record_lsn,
        );

        // Do we have a layer open for writing already?
        let layer;
        if let Some(open_layer) = &layers.open_layer {
            if open_layer.get_lsn_range().start > lsn {
                bail!("unexpected open layer in the future");
            }

            layer = Arc::clone(open_layer);
        } else {
            // No writeable layer yet. Create one.
            let start_lsn = layers
                .next_open_layer_at
                .context("No next open layer found")?;

            trace!(
                "creating layer for write at {}/{} for record at {}",
                self.timeline_id,
                start_lsn,
                lsn
            );
            let new_layer =
                InMemoryLayer::create(self.conf, self.timeline_id, self.tenant_id, start_lsn)?;
            let layer_rc = Arc::new(new_layer);

            layers.open_layer = Some(Arc::clone(&layer_rc));
            layers.next_open_layer_at = None;

            layer = layer_rc;
        }
        Ok(layer)
    }

    fn put_value(&self, key: Key, lsn: Lsn, val: &Value) -> anyhow::Result<()> {
        //info!("PUT: key {} at {}", key, lsn);
        let layer = self.get_layer_for_write(lsn)?;
        layer.put_value(key, lsn, val)?;
        Ok(())
    }

    fn put_tombstone(&self, key_range: Range<Key>, lsn: Lsn) -> anyhow::Result<()> {
        let layer = self.get_layer_for_write(lsn)?;
        layer.put_tombstone(key_range, lsn)?;

        Ok(())
    }

    fn finish_write(&self, new_lsn: Lsn) {
        assert!(new_lsn.is_aligned());

        self.metrics.last_record_gauge.set(new_lsn.0 as i64);
        self.last_record_lsn.advance(new_lsn);
    }

    fn freeze_inmem_layer(&self, write_lock_held: bool) {
        // Freeze the current open in-memory layer. It will be written to disk on next
        // iteration.
        let _write_guard = if write_lock_held {
            None
        } else {
            Some(self.write_lock.lock().unwrap())
        };
        let mut layers = self.layers.write().unwrap();
        if let Some(open_layer) = &layers.open_layer {
            let open_layer_rc = Arc::clone(open_layer);
            // Does this layer need freezing?
            let end_lsn = Lsn(self.get_last_record_lsn().0 + 1);
            open_layer.freeze(end_lsn);

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            layers.frozen_layers.push_back(open_layer_rc);
            layers.open_layer = None;
            layers.next_open_layer_at = Some(end_lsn);
            self.last_freeze_at.store(end_lsn);
        }
        drop(layers);
    }

    /// Layer flusher task's main loop.
    async fn flush_loop(
        &self,
        mut layer_flush_start_rx: tokio::sync::watch::Receiver<u64>,
        ctx: &RequestContext,
    ) {
        info!("started flush loop");
        loop {
            tokio::select! {
                _ = task_mgr::shutdown_watcher() => {
                    info!("shutting down layer flush task");
                    break;
                },
                _ = layer_flush_start_rx.changed() => {}
            }

            trace!("waking up");
            let timer = self.metrics.flush_time_histo.start_timer();
            let flush_counter = *layer_flush_start_rx.borrow();
            let result = loop {
                let layer_to_flush = {
                    let layers = self.layers.read().unwrap();
                    layers.frozen_layers.front().cloned()
                    // drop 'layers' lock to allow concurrent reads and writes
                };
                if let Some(layer_to_flush) = layer_to_flush {
                    if let Err(err) = self.flush_frozen_layer(layer_to_flush, ctx).await {
                        error!("could not flush frozen layer: {err:?}");
                        break Err(err);
                    }
                    continue;
                } else {
                    break Ok(());
                }
            };
            // Notify any listeners that we're done
            let _ = self
                .layer_flush_done_tx
                .send_replace((flush_counter, result));

            timer.stop_and_record();
        }
    }

    async fn flush_frozen_layers_and_wait(&self) -> anyhow::Result<()> {
        let mut rx = self.layer_flush_done_tx.subscribe();

        // Increment the flush cycle counter and wake up the flush task.
        // Remember the new value, so that when we listen for the flush
        // to finish, we know when the flush that we initiated has
        // finished, instead of some other flush that was started earlier.
        let mut my_flush_request = 0;

        let flush_loop_state = { *self.flush_loop_state.lock().unwrap() };
        if flush_loop_state != FlushLoopState::Running {
            anyhow::bail!("cannot flush frozen layers when flush_loop is not running, state is {flush_loop_state:?}")
        }

        self.layer_flush_start_tx.send_modify(|counter| {
            my_flush_request = *counter + 1;
            *counter = my_flush_request;
        });

        loop {
            {
                let (last_result_counter, last_result) = &*rx.borrow();
                if *last_result_counter >= my_flush_request {
                    if let Err(_err) = last_result {
                        // We already logged the original error in
                        // flush_loop. We cannot propagate it to the caller
                        // here, because it might not be Cloneable
                        anyhow::bail!(
                            "Could not flush frozen layer. Request id: {}",
                            my_flush_request
                        );
                    } else {
                        return Ok(());
                    }
                }
            }
            trace!("waiting for flush to complete");
            rx.changed().await?;
            trace!("done")
        }
    }

    fn flush_frozen_layers(&self) {
        self.layer_flush_start_tx.send_modify(|val| *val += 1);
    }

    /// Flush one frozen in-memory layer to disk, as a new delta layer.
    #[instrument(skip(self, frozen_layer, ctx), fields(tenant_id=%self.tenant_id, timeline_id=%self.timeline_id, layer=%frozen_layer.short_id()))]
    async fn flush_frozen_layer(
        &self,
        frozen_layer: Arc<InMemoryLayer>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // As a special case, when we have just imported an image into the repository,
        // instead of writing out a L0 delta layer, we directly write out image layer
        // files instead. This is possible as long as *all* the data imported into the
        // repository have the same LSN.
        let lsn_range = frozen_layer.get_lsn_range();
        let layer_paths_to_upload =
            if lsn_range.start == self.initdb_lsn && lsn_range.end == Lsn(self.initdb_lsn.0 + 1) {
                // Note: The 'ctx' in use here has DownloadBehavior::Error. We should not
                // require downloading anything during initial import.
                let (partitioning, _lsn) = self
                    .repartition(self.initdb_lsn, self.get_compaction_target_size(), ctx)
                    .await?;
                self.create_image_layers(&partitioning, self.initdb_lsn, true, ctx)
                    .await?
            } else {
                // normal case, write out a L0 delta layer file.
                let (delta_path, metadata) = self.create_delta_layer(&frozen_layer)?;
                HashMap::from([(delta_path, metadata)])
            };

        fail_point!("flush-frozen-before-sync");

        // The new on-disk layers are now in the layer map. We can remove the
        // in-memory layer from the map now.
        {
            let mut layers = self.layers.write().unwrap();
            let l = layers.frozen_layers.pop_front();

            // Only one thread may call this function at a time (for this
            // timeline). If two threads tried to flush the same frozen
            // layer to disk at the same time, that would not work.
            assert!(LayerMap::compare_arced_layers(&l.unwrap(), &frozen_layer));

            // release lock on 'layers'
        }

        fail_point!("checkpoint-after-sync");

        // Update the metadata file, with new 'disk_consistent_lsn'
        //
        // TODO: This perhaps should be done in 'flush_frozen_layers', after flushing
        // *all* the layers, to avoid fsyncing the file multiple times.
        let disk_consistent_lsn = Lsn(lsn_range.end.0 - 1);
        let old_disk_consistent_lsn = self.disk_consistent_lsn.load();

        // If we were able to advance 'disk_consistent_lsn', save it the metadata file.
        // After crash, we will restart WAL streaming and processing from that point.
        if disk_consistent_lsn != old_disk_consistent_lsn {
            assert!(disk_consistent_lsn > old_disk_consistent_lsn);
            self.update_metadata_file(disk_consistent_lsn, layer_paths_to_upload)
                .context("update_metadata_file")?;
            // Also update the in-memory copy
            self.disk_consistent_lsn.store(disk_consistent_lsn);
        }
        Ok(())
    }

    /// Update metadata file
    fn update_metadata_file(
        &self,
        disk_consistent_lsn: Lsn,
        layer_paths_to_upload: HashMap<LayerFileName, LayerFileMetadata>,
    ) -> anyhow::Result<()> {
        // We can only save a valid 'prev_record_lsn' value on disk if we
        // flushed *all* in-memory changes to disk. We only track
        // 'prev_record_lsn' in memory for the latest processed record, so we
        // don't remember what the correct value that corresponds to some old
        // LSN is. But if we flush everything, then the value corresponding
        // current 'last_record_lsn' is correct and we can store it on disk.
        let RecordLsn {
            last: last_record_lsn,
            prev: prev_record_lsn,
        } = self.last_record_lsn.load();
        let ondisk_prev_record_lsn = if disk_consistent_lsn == last_record_lsn {
            Some(prev_record_lsn)
        } else {
            None
        };

        let ancestor_timeline_id = self
            .ancestor_timeline
            .as_ref()
            .map(|ancestor| ancestor.timeline_id);

        let metadata = TimelineMetadata::new(
            disk_consistent_lsn,
            ondisk_prev_record_lsn,
            ancestor_timeline_id,
            self.ancestor_lsn,
            *self.latest_gc_cutoff_lsn.read(),
            self.initdb_lsn,
            self.pg_version,
        );

        fail_point!("checkpoint-before-saving-metadata", |x| bail!(
            "{}",
            x.unwrap()
        ));

        save_metadata(
            self.conf,
            self.timeline_id,
            self.tenant_id,
            &metadata,
            false,
        )
        .context("save_metadata")?;

        if let Some(remote_client) = &self.remote_client {
            for (path, layer_metadata) in layer_paths_to_upload {
                remote_client.schedule_layer_file_upload(&path, &layer_metadata)?;
            }
            remote_client.schedule_index_upload_for_metadata_update(&metadata)?;
        }

        Ok(())
    }

    // Write out the given frozen in-memory layer as a new L0 delta file
    fn create_delta_layer(
        &self,
        frozen_layer: &InMemoryLayer,
    ) -> anyhow::Result<(LayerFileName, LayerFileMetadata)> {
        // Write it out
        let new_delta = frozen_layer.write_to_disk()?;
        let new_delta_path = new_delta.path();
        let new_delta_filename = new_delta.filename();

        // Sync it to disk.
        //
        // We must also fsync the timeline dir to ensure the directory entries for
        // new layer files are durable
        //
        // TODO: If we're running inside 'flush_frozen_layers' and there are multiple
        // files to flush, it might be better to first write them all, and then fsync
        // them all in parallel.
        par_fsync::par_fsync(&[
            new_delta_path.clone(),
            self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
        ])?;

        // Add it to the layer map
        let l = Arc::new(new_delta);
        let mut layers = self.layers.write().unwrap();
        let mut batch_updates = layers.batch_update();
        l.access_stats().record_residence_event(
            &batch_updates,
            LayerResidenceStatus::Resident,
            LayerResidenceEventReason::LayerCreate,
        );
        batch_updates.insert_historic(l);
        batch_updates.flush();

        // update the timeline's physical size
        let sz = new_delta_path.metadata()?.len();

        self.metrics.resident_physical_size_gauge.add(sz);
        // update metrics
        self.metrics.num_persistent_files_created.inc_by(1);
        self.metrics.persistent_bytes_written.inc_by(sz);

        Ok((new_delta_filename, LayerFileMetadata::new(sz)))
    }

    async fn repartition(
        &self,
        lsn: Lsn,
        partition_size: u64,
        ctx: &RequestContext,
    ) -> anyhow::Result<(KeyPartitioning, Lsn)> {
        {
            let partitioning_guard = self.partitioning.lock().unwrap();
            let distance = lsn.0 - partitioning_guard.1 .0;
            if partitioning_guard.1 != Lsn(0) && distance <= self.repartition_threshold {
                debug!(
                    distance,
                    threshold = self.repartition_threshold,
                    "no repartitioning needed"
                );
                return Ok((partitioning_guard.0.clone(), partitioning_guard.1));
            }
        }
        let keyspace = self.collect_keyspace(lsn, ctx).await?;
        let partitioning = keyspace.partition(partition_size);

        let mut partitioning_guard = self.partitioning.lock().unwrap();
        if lsn > partitioning_guard.1 {
            *partitioning_guard = (partitioning, lsn);
        } else {
            warn!("Concurrent repartitioning of keyspace. This unexpected, but probably harmless");
        }
        Ok((partitioning_guard.0.clone(), partitioning_guard.1))
    }

    // Is it time to create a new image layer for the given partition?
    fn time_for_new_image_layer(&self, partition: &KeySpace, lsn: Lsn) -> anyhow::Result<bool> {
        let threshold = self.get_image_creation_threshold();

        let layers = self.layers.read().unwrap();

        let mut max_deltas = 0;
        {
            let wanted_image_layers = self.wanted_image_layers.lock().unwrap();
            if let Some((cutoff_lsn, wanted)) = &*wanted_image_layers {
                let img_range =
                    partition.ranges.first().unwrap().start..partition.ranges.last().unwrap().end;
                if wanted.overlaps(&img_range) {
                    //
                    // gc_timeline only pays attention to image layers that are older than the GC cutoff,
                    // but create_image_layers creates image layers at last-record-lsn.
                    // So it's possible that gc_timeline wants a new image layer to be created for a key range,
                    // but the range is already covered by image layers at more recent LSNs. Before we
                    // create a new image layer, check if the range is already covered at more recent LSNs.
                    if !layers
                        .image_layer_exists(&img_range, &(Lsn::min(lsn, *cutoff_lsn)..lsn + 1))?
                    {
                        debug!(
                            "Force generation of layer {}-{} wanted by GC, cutoff={}, lsn={})",
                            img_range.start, img_range.end, cutoff_lsn, lsn
                        );
                        return Ok(true);
                    }
                }
            }
        }

        for part_range in &partition.ranges {
            let image_coverage = layers.image_coverage(part_range, lsn)?;
            for (img_range, last_img) in image_coverage {
                let img_lsn = if let Some(last_img) = last_img {
                    last_img.get_lsn_range().end
                } else {
                    Lsn(0)
                };
                // Let's consider an example:
                //
                // delta layer with LSN range 71-81
                // delta layer with LSN range 81-91
                // delta layer with LSN range 91-101
                // image layer at LSN 100
                //
                // If 'lsn' is still 100, i.e. no new WAL has been processed since the last image layer,
                // there's no need to create a new one. We check this case explicitly, to avoid passing
                // a bogus range to count_deltas below, with start > end. It's even possible that there
                // are some delta layers *later* than current 'lsn', if more WAL was processed and flushed
                // after we read last_record_lsn, which is passed here in the 'lsn' argument.
                if img_lsn < lsn {
                    let num_deltas =
                        layers.count_deltas(&img_range, &(img_lsn..lsn), Some(threshold))?;

                    max_deltas = max_deltas.max(num_deltas);
                    if num_deltas >= threshold {
                        debug!(
                            "key range {}-{}, has {} deltas on this timeline in LSN range {}..{}",
                            img_range.start, img_range.end, num_deltas, img_lsn, lsn
                        );
                        return Ok(true);
                    }
                }
            }
        }

        debug!(
            max_deltas,
            "none of the partitioned ranges had >= {threshold} deltas"
        );
        Ok(false)
    }

    async fn create_image_layers(
        &self,
        partitioning: &KeyPartitioning,
        lsn: Lsn,
        force: bool,
        ctx: &RequestContext,
    ) -> Result<HashMap<LayerFileName, LayerFileMetadata>, PageReconstructError> {
        let timer = self.metrics.create_images_time_histo.start_timer();
        let mut image_layers: Vec<ImageLayer> = Vec::new();

        // We need to avoid holes between generated image layers.
        // Otherwise LayerMap::image_layer_exists will return false if key range of some layer is covered by more than one
        // image layer with hole between them. In this case such layer can not be utilized by GC.
        //
        // How such hole between partitions can appear?
        // if we have relation with relid=1 and size 100 and relation with relid=2 with size 200 then result of
        // KeySpace::partition may contain partitions <100000000..100000099> and <200000000..200000199>.
        // If there is delta layer <100000000..300000000> then it never be garbage collected because
        // image layers  <100000000..100000099> and <200000000..200000199> are not completely covering it.
        let mut start = Key::MIN;

        for partition in partitioning.parts.iter() {
            let img_range = start..partition.ranges.last().unwrap().end;
            start = img_range.end;
            if force || self.time_for_new_image_layer(partition, lsn)? {
                let mut image_layer_writer = ImageLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_id,
                    &img_range,
                    lsn,
                )?;

                fail_point!("image-layer-writer-fail-before-finish", |_| {
                    Err(PageReconstructError::Other(anyhow::anyhow!(
                        "failpoint image-layer-writer-fail-before-finish"
                    )))
                });
                for range in &partition.ranges {
                    let mut key = range.start;
                    while key < range.end {
                        let img = match self.get(key, lsn, ctx).await {
                            Ok(img) => img,
                            Err(err) => {
                                // If we fail to reconstruct a VM or FSM page, we can zero the
                                // page without losing any actual user data. That seems better
                                // than failing repeatedly and getting stuck.
                                //
                                // We had a bug at one point, where we truncated the FSM and VM
                                // in the pageserver, but the Postgres didn't know about that
                                // and continued to generate incremental WAL records for pages
                                // that didn't exist in the pageserver. Trying to replay those
                                // WAL records failed to find the previous image of the page.
                                // This special case allows us to recover from that situation.
                                // See https://github.com/neondatabase/neon/issues/2601.
                                //
                                // Unfortunately we cannot do this for the main fork, or for
                                // any metadata keys, keys, as that would lead to actual data
                                // loss.
                                if is_rel_fsm_block_key(key) || is_rel_vm_block_key(key) {
                                    warn!("could not reconstruct FSM or VM key {key}, filling with zeros: {err:?}");
                                    ZERO_PAGE.clone()
                                } else {
                                    return Err(err);
                                }
                            }
                        };
                        image_layer_writer.put_image(key, &img)?;
                        key = key.next();
                    }
                }
                let image_layer = image_layer_writer.finish()?;
                image_layers.push(image_layer);
            }
        }
        // All layers that the GC wanted us to create have now been created.
        //
        // It's possible that another GC cycle happened while we were compacting, and added
        // something new to wanted_image_layers, and we now clear that before processing it.
        // That's OK, because the next GC iteration will put it back in.
        *self.wanted_image_layers.lock().unwrap() = None;

        // Sync the new layer to disk before adding it to the layer map, to make sure
        // we don't garbage collect something based on the new layer, before it has
        // reached the disk.
        //
        // We must also fsync the timeline dir to ensure the directory entries for
        // new layer files are durable
        //
        // Compaction creates multiple image layers. It would be better to create them all
        // and fsync them all in parallel.
        let all_paths = image_layers
            .iter()
            .map(|layer| layer.path())
            .chain(std::iter::once(
                self.conf.timeline_path(&self.timeline_id, &self.tenant_id),
            ))
            .collect::<Vec<_>>();
        par_fsync::par_fsync(&all_paths).context("fsync of newly created layer files")?;

        let mut layer_paths_to_upload = HashMap::with_capacity(image_layers.len());

        let mut layers = self.layers.write().unwrap();
        let mut updates = layers.batch_update();
        let timeline_path = self.conf.timeline_path(&self.timeline_id, &self.tenant_id);
        for l in image_layers {
            let path = l.filename();
            let metadata = timeline_path
                .join(path.file_name())
                .metadata()
                .with_context(|| format!("reading metadata of layer file {}", path.file_name()))?;

            layer_paths_to_upload.insert(path, LayerFileMetadata::new(metadata.len()));

            self.metrics
                .resident_physical_size_gauge
                .add(metadata.len());
            let l = Arc::new(l);
            l.access_stats().record_residence_event(
                &updates,
                LayerResidenceStatus::Resident,
                LayerResidenceEventReason::LayerCreate,
            );
            updates.insert_historic(l);
        }
        updates.flush();
        drop(layers);
        timer.stop_and_record();

        Ok(layer_paths_to_upload)
    }
}

#[derive(Default)]
struct CompactLevel0Phase1Result {
    new_layers: Vec<DeltaLayer>,
    deltas_to_compact: Vec<Arc<dyn PersistentLayer>>,
}

/// Top-level failure to compact.
#[derive(Debug)]
enum CompactionError {
    /// L0 compaction requires layers to be downloaded.
    ///
    /// This should not happen repeatedly, but will be retried once by top-level
    /// `Timeline::compact`.
    DownloadRequired(Vec<Arc<RemoteLayer>>),
    /// Compaction cannot be done right now; page reconstruction and so on.
    Other(anyhow::Error),
}

impl From<anyhow::Error> for CompactionError {
    fn from(value: anyhow::Error) -> Self {
        CompactionError::Other(value)
    }
}

impl Timeline {
    /// Level0 files first phase of compaction, explained in the [`compact_inner`] comment.
    ///
    /// This method takes the `_layer_removal_cs` guard to highlight it required downloads are
    /// returned as an error. If the `layer_removal_cs` boundary is changed not to be taken in the
    /// start of level0 files compaction, the on-demand download should be revisited as well.
    async fn compact_level0_phase1(
        &self,
        _layer_removal_cs: &tokio::sync::MutexGuard<'_, ()>,
        target_file_size: u64,
        ctx: &RequestContext,
    ) -> Result<CompactLevel0Phase1Result, CompactionError> {
        let layers = self.layers.read().unwrap();
        let mut level0_deltas = layers.get_level0_deltas()?;
        drop(layers);

        // Only compact if enough layers have accumulated.
        let threshold = self.get_compaction_threshold();
        if level0_deltas.is_empty() || level0_deltas.len() < threshold {
            debug!(
                level0_deltas = level0_deltas.len(),
                threshold, "too few deltas to compact"
            );
            return Ok(CompactLevel0Phase1Result::default());
        }

        // Gather the files to compact in this iteration.
        //
        // Start with the oldest Level 0 delta file, and collect any other
        // level 0 files that form a contiguous sequence, such that the end
        // LSN of previous file matches the start LSN of the next file.
        //
        // Note that if the files don't form such a sequence, we might
        // "compact" just a single file. That's a bit pointless, but it allows
        // us to get rid of the level 0 file, and compact the other files on
        // the next iteration. This could probably made smarter, but such
        // "gaps" in the sequence of level 0 files should only happen in case
        // of a crash, partial download from cloud storage, or something like
        // that, so it's not a big deal in practice.
        level0_deltas.sort_by_key(|l| l.get_lsn_range().start);
        let mut level0_deltas_iter = level0_deltas.iter();

        let first_level0_delta = level0_deltas_iter.next().unwrap();
        let mut prev_lsn_end = first_level0_delta.get_lsn_range().end;
        let mut deltas_to_compact = vec![Arc::clone(first_level0_delta)];
        for l in level0_deltas_iter {
            let lsn_range = l.get_lsn_range();

            if lsn_range.start != prev_lsn_end {
                break;
            }
            deltas_to_compact.push(Arc::clone(l));
            prev_lsn_end = lsn_range.end;
        }
        let lsn_range = Range {
            start: deltas_to_compact.first().unwrap().get_lsn_range().start,
            end: deltas_to_compact.last().unwrap().get_lsn_range().end,
        };

        let remotes = deltas_to_compact
            .iter()
            .filter(|l| l.is_remote_layer())
            .inspect(|l| info!("compact requires download of {}", l.filename().file_name()))
            .map(|l| {
                l.clone()
                    .downcast_remote_layer()
                    .expect("just checked it is remote layer")
            })
            .collect::<Vec<_>>();

        if !remotes.is_empty() {
            // caller is holding the lock to layer_removal_cs, and we don't want to download while
            // holding that; in future download_remote_layer might take it as well. this is
            // regardless of earlier image creation downloading on-demand, while holding the lock.
            return Err(CompactionError::DownloadRequired(remotes));
        }

        info!(
            "Starting Level0 compaction in LSN range {}-{} for {} layers ({} deltas in total)",
            lsn_range.start,
            lsn_range.end,
            deltas_to_compact.len(),
            level0_deltas.len()
        );

        for l in deltas_to_compact.iter() {
            info!("compact includes {}", l.filename().file_name());
        }

        // We don't need the original list of layers anymore. Drop it so that
        // we don't accidentally use it later in the function.
        drop(level0_deltas);

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let all_values_iter = itertools::process_results(
            deltas_to_compact.iter().map(|l| l.iter(ctx)),
            |iter_iter| {
                iter_iter.kmerge_by(|a, b| {
                    if let Ok((a_key, a_lsn, _)) = a {
                        if let Ok((b_key, b_lsn, _)) = b {
                            match a_key.cmp(b_key) {
                                Ordering::Less => true,
                                Ordering::Equal => a_lsn <= b_lsn,
                                Ordering::Greater => false,
                            }
                        } else {
                            false
                        }
                    } else {
                        true
                    }
                })
            },
        )?;

        // This iterator walks through all keys and is needed to calculate size used by each key
        let mut all_keys_iter = itertools::process_results(
            deltas_to_compact.iter().map(|l| l.key_iter(ctx)),
            |iter_iter| {
                iter_iter.kmerge_by(|a, b| {
                    let (a_key, a_lsn, _) = a;
                    let (b_key, b_lsn, _) = b;
                    match a_key.cmp(b_key) {
                        Ordering::Less => true,
                        Ordering::Equal => a_lsn <= b_lsn,
                        Ordering::Greater => false,
                    }
                })
            },
        )?;

        // Determine N largest holes where N is number of compacted layers.
        let max_holes = deltas_to_compact.len();
        let last_record_lsn = self.get_last_record_lsn();
        let layers = self.layers.read().unwrap(); // Is'n it better to hold original layers lock till here?
        let min_hole_range = (target_file_size / page_cache::PAGE_SZ as u64) as i128;
        let min_hole_coverage_size = 3; // TODO: something more flexible?

        // min-heap (reserve space for one more element added before eviction)
        let mut heap: BinaryHeap<Hole> = BinaryHeap::with_capacity(max_holes + 1);
        let mut prev: Option<Key> = None;
        for (next_key, _next_lsn, _size) in itertools::process_results(
            deltas_to_compact.iter().map(|l| l.key_iter(ctx)),
            |iter_iter| iter_iter.kmerge_by(|a, b| a.0 <= b.0),
        )? {
            if let Some(prev_key) = prev {
                // just first fast filter
                if next_key.to_i128() - prev_key.to_i128() >= min_hole_range {
                    let key_range = prev_key..next_key;
                    // Measuring hole by just subtraction of i128 representation of key range boundaries
                    // has not so much sense, because largest holes will corresponds field1/field2 changes.
                    // But we are mostly interested to eliminate holes which cause generation of excessive image layers.
                    // That is why it is better to measure size of hole as number of covering image layers.
                    let coverage_size = layers.image_coverage(&key_range, last_record_lsn)?.len();
                    if coverage_size >= min_hole_coverage_size {
                        heap.push(Hole {
                            key_range,
                            coverage_size,
                        });
                        if heap.len() > max_holes {
                            heap.pop(); // remove smallest hole
                        }
                    }
                }
            }
            prev = Some(next_key.next());
        }
        drop(layers);
        let mut holes = heap.into_vec();
        holes.sort_unstable_by_key(|hole| hole.key_range.start);
        let mut next_hole = 0; // index of next hole in holes vector

        // Merge the contents of all the input delta layers into a new set
        // of delta layers, based on the current partitioning.
        //
        // We split the new delta layers on the key dimension. We iterate through the key space, and for each key, check if including the next key to the current output layer we're building would cause the layer to become too large. If so, dump the current output layer and start new one.
        // It's possible that there is a single key with so many page versions that storing all of them in a single layer file
        // would be too large. In that case, we also split on the LSN dimension.
        //
        // LSN
        //  ^
        //  |
        //  | +-----------+            +--+--+--+--+
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+     ==>    |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            +--+--+--+--+
        //  |
        //  +--------------> key
        //
        //
        // If one key (X) has a lot of page versions:
        //
        // LSN
        //  ^
        //  |                                 (X)
        //  | +-----------+            +--+--+--+--+
        //  | |           |            |  |  |  |  |
        //  | +-----------+            |  |  +--+  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+     ==>    |  |  |  |  |
        //  | |           |            |  |  +--+  |
        //  | +-----------+            |  |  |  |  |
        //  | |           |            |  |  |  |  |
        //  | +-----------+            +--+--+--+--+
        //  |
        //  +--------------> key
        // TODO: this actually divides the layers into fixed-size chunks, not
        // based on the partitioning.
        //
        // TODO: we should also opportunistically materialize and
        // garbage collect what we can.
        let mut new_layers = Vec::new();
        let mut prev_key: Option<Key> = None;
        let mut writer: Option<DeltaLayerWriter> = None;
        let mut key_values_total_size = 0u64;
        let mut dup_start_lsn: Lsn = Lsn::INVALID; // start LSN of layer containing values of the single key
        let mut dup_end_lsn: Lsn = Lsn::INVALID; // end LSN of layer containing values of the single key
        for x in all_values_iter {
            let (key, lsn, value) = x?;
            let same_key = prev_key.map_or(false, |prev_key| prev_key == key);
            // We need to check key boundaries once we reach next key or end of layer with the same key
            if !same_key || lsn == dup_end_lsn {
                let mut next_key_size = 0u64;
                let is_dup_layer = dup_end_lsn.is_valid();
                dup_start_lsn = Lsn::INVALID;
                if !same_key {
                    dup_end_lsn = Lsn::INVALID;
                }
                // Determine size occupied by this key. We stop at next key or when size becomes larger than target_file_size
                for (next_key, next_lsn, next_size) in all_keys_iter.by_ref() {
                    next_key_size = next_size;
                    if key != next_key {
                        if dup_end_lsn.is_valid() {
                            // We are writting segment with duplicates:
                            // place all remaining values of this key in separate segment
                            dup_start_lsn = dup_end_lsn; // new segments starts where old stops
                            dup_end_lsn = lsn_range.end; // there are no more values of this key till end of LSN range
                        }
                        break;
                    }
                    key_values_total_size += next_size;
                    // Check if it is time to split segment: if total keys size is larger than target file size.
                    // We need to avoid generation of empty segments if next_size > target_file_size.
                    if key_values_total_size > target_file_size && lsn != next_lsn {
                        // Split key between multiple layers: such layer can contain only single key
                        dup_start_lsn = if dup_end_lsn.is_valid() {
                            dup_end_lsn // new segment with duplicates starts where old one stops
                        } else {
                            lsn // start with the first LSN for this key
                        };
                        dup_end_lsn = next_lsn; // upper LSN boundary is exclusive
                        break;
                    }
                }
                // handle case when loop reaches last key: in this case dup_end is non-zero but dup_start is not set.
                if dup_end_lsn.is_valid() && !dup_start_lsn.is_valid() {
                    dup_start_lsn = dup_end_lsn;
                    dup_end_lsn = lsn_range.end;
                }
                if writer.is_some() {
                    let written_size = writer.as_mut().unwrap().size();
                    let contains_hole =
                        next_hole < holes.len() && key >= holes[next_hole].key_range.end;
                    // check if key cause layer overflow or contains hole...
                    if is_dup_layer
                        || dup_end_lsn.is_valid()
                        || written_size + key_values_total_size > target_file_size
                        || contains_hole
                    {
                        // ... if so, flush previous layer and prepare to write new one
                        new_layers.push(writer.take().unwrap().finish(prev_key.unwrap().next())?);
                        writer = None;

                        if contains_hole {
                            // skip hole
                            next_hole += 1;
                        }
                    }
                }
                // Remember size of key value because at next iteration we will access next item
                key_values_total_size = next_key_size;
            }
            if writer.is_none() {
                // Create writer if not initiaized yet
                writer = Some(DeltaLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_id,
                    key,
                    if dup_end_lsn.is_valid() {
                        // this is a layer containing slice of values of the same key
                        debug!("Create new dup layer {}..{}", dup_start_lsn, dup_end_lsn);
                        dup_start_lsn..dup_end_lsn
                    } else {
                        debug!("Create new layer {}..{}", lsn_range.start, lsn_range.end);
                        lsn_range.clone()
                    },
                )?);
            }

            fail_point!("delta-layer-writer-fail-before-finish", |_| {
                Err(anyhow::anyhow!("failpoint delta-layer-writer-fail-before-finish").into())
            });

            writer.as_mut().unwrap().put_value(key, lsn, value)?;
            prev_key = Some(key);
        }
        if let Some(writer) = writer {
            new_layers.push(writer.finish(prev_key.unwrap().next())?);
        }

        // Sync layers
        if !new_layers.is_empty() {
            let mut layer_paths: Vec<PathBuf> = new_layers.iter().map(|l| l.path()).collect();

            // also sync the directory
            layer_paths.push(self.conf.timeline_path(&self.timeline_id, &self.tenant_id));

            // Fsync all the layer files and directory using multiple threads to
            // minimize latency.
            par_fsync::par_fsync(&layer_paths).context("fsync all new layers")?;

            layer_paths.pop().unwrap();
        }

        drop(all_keys_iter); // So that deltas_to_compact is no longer borrowed

        Ok(CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact,
        })
    }

    ///
    /// Collect a bunch of Level 0 layer files, and compact and reshuffle them as
    /// as Level 1 files.
    ///
    async fn compact_level0(
        &self,
        layer_removal_cs: &tokio::sync::MutexGuard<'_, ()>,
        target_file_size: u64,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact,
        } = self
            .compact_level0_phase1(layer_removal_cs, target_file_size, ctx)
            .await?;

        if new_layers.is_empty() && deltas_to_compact.is_empty() {
            // nothing to do
            return Ok(());
        }

        // Before deleting any layers, we need to wait for their upload ops to finish.
        // See storage_sync module level comment on consistency.
        // Do it here because we don't want to hold self.layers.write() while waiting.
        if let Some(remote_client) = &self.remote_client {
            debug!("waiting for upload ops to complete");
            remote_client
                .wait_completion()
                .await
                .context("wait for layer upload ops to complete")?;
        }

        let mut layers = self.layers.write().unwrap();
        let mut updates = layers.batch_update();
        let mut new_layer_paths = HashMap::with_capacity(new_layers.len());
        for l in new_layers {
            let new_delta_path = l.path();

            let metadata = new_delta_path.metadata().with_context(|| {
                format!(
                    "read file metadata for new created layer {}",
                    new_delta_path.display()
                )
            })?;

            if let Some(remote_client) = &self.remote_client {
                remote_client.schedule_layer_file_upload(
                    &l.filename(),
                    &LayerFileMetadata::new(metadata.len()),
                )?;
            }

            // update the timeline's physical size
            self.metrics
                .resident_physical_size_gauge
                .add(metadata.len());

            new_layer_paths.insert(new_delta_path, LayerFileMetadata::new(metadata.len()));
            let x: Arc<dyn PersistentLayer + 'static> = Arc::new(l);
            x.access_stats().record_residence_event(
                &updates,
                LayerResidenceStatus::Resident,
                LayerResidenceEventReason::LayerCreate,
            );
            updates.insert_historic(x);
        }

        // Now that we have reshuffled the data to set of new delta layers, we can
        // delete the old ones
        let mut layer_names_to_delete = Vec::with_capacity(deltas_to_compact.len());
        for l in deltas_to_compact {
            layer_names_to_delete.push(l.filename());
            self.delete_historic_layer(layer_removal_cs, l, &mut updates)?;
        }
        updates.flush();
        drop(layers);

        // Also schedule the deletions in remote storage
        if let Some(remote_client) = &self.remote_client {
            remote_client.schedule_layer_file_deletion(&layer_names_to_delete)?;
        }

        Ok(())
    }

    /// Update information about which layer files need to be retained on
    /// garbage collection. This is separate from actually performing the GC,
    /// and is updated more frequently, so that compaction can remove obsolete
    /// page versions more aggressively.
    ///
    /// TODO: that's wishful thinking, compaction doesn't actually do that
    /// currently.
    ///
    /// The caller specifies how much history is needed with the 3 arguments:
    ///
    /// retain_lsns: keep a version of each page at these LSNs
    /// cutoff_horizon: also keep everything newer than this LSN
    /// pitr: the time duration required to keep data for PITR
    ///
    /// The 'retain_lsns' list is currently used to prevent removing files that
    /// are needed by child timelines. In the future, the user might be able to
    /// name additional points in time to retain. The caller is responsible for
    /// collecting that information.
    ///
    /// The 'cutoff_horizon' point is used to retain recent versions that might still be
    /// needed by read-only nodes. (As of this writing, the caller just passes
    /// the latest LSN subtracted by a constant, and doesn't do anything smart
    /// to figure out what read-only nodes might actually need.)
    ///
    /// The 'pitr' duration is used to calculate a 'pitr_cutoff', which can be used to determine
    /// whether a record is needed for PITR.
    ///
    /// NOTE: This function holds a short-lived lock to protect the 'gc_info'
    /// field, so that the three values passed as argument are stored
    /// atomically. But the caller is responsible for ensuring that no new
    /// branches are created that would need to be included in 'retain_lsns',
    /// for example. The caller should hold `Tenant::gc_cs` lock to ensure
    /// that.
    ///
    pub(super) async fn update_gc_info(
        &self,
        retain_lsns: Vec<Lsn>,
        cutoff_horizon: Lsn,
        pitr: Duration,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // First, calculate pitr_cutoff_timestamp and then convert it to LSN.
        //
        // Some unit tests depend on garbage-collection working even when
        // CLOG data is missing, so that find_lsn_for_timestamp() doesn't
        // work, so avoid calling it altogether if time-based retention is not
        // configured. It would be pointless anyway.
        let pitr_cutoff = if pitr != Duration::ZERO {
            let now = SystemTime::now();
            if let Some(pitr_cutoff_timestamp) = now.checked_sub(pitr) {
                let pitr_timestamp = to_pg_timestamp(pitr_cutoff_timestamp);

                match self.find_lsn_for_timestamp(pitr_timestamp, ctx).await? {
                    LsnForTimestamp::Present(lsn) => lsn,
                    LsnForTimestamp::Future(lsn) => {
                        // The timestamp is in the future. That sounds impossible,
                        // but what it really means is that there hasn't been
                        // any commits since the cutoff timestamp.
                        debug!("future({})", lsn);
                        cutoff_horizon
                    }
                    LsnForTimestamp::Past(lsn) => {
                        debug!("past({})", lsn);
                        // conservative, safe default is to remove nothing, when we
                        // have no commit timestamp data available
                        *self.get_latest_gc_cutoff_lsn()
                    }
                    LsnForTimestamp::NoData(lsn) => {
                        debug!("nodata({})", lsn);
                        // conservative, safe default is to remove nothing, when we
                        // have no commit timestamp data available
                        *self.get_latest_gc_cutoff_lsn()
                    }
                }
            } else {
                // If we don't have enough data to convert to LSN,
                // play safe and don't remove any layers.
                *self.get_latest_gc_cutoff_lsn()
            }
        } else {
            // No time-based retention was configured. Set time-based cutoff to
            // same as LSN based.
            cutoff_horizon
        };

        // Grab the lock and update the values
        *self.gc_info.write().unwrap() = GcInfo {
            retain_lsns,
            horizon_cutoff: cutoff_horizon,
            pitr_cutoff,
        };

        Ok(())
    }

    ///
    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    pub(super) async fn gc(&self) -> anyhow::Result<GcResult> {
        let timer = self.metrics.garbage_collect_histo.start_timer();

        fail_point!("before-timeline-gc");

        let layer_removal_cs = self.layer_removal_cs.lock().await;
        // Is the timeline being deleted?
        let state = *self.state.borrow();
        if state == TimelineState::Stopping {
            anyhow::bail!("timeline is Stopping");
        }

        let (horizon_cutoff, pitr_cutoff, retain_lsns) = {
            let gc_info = self.gc_info.read().unwrap();

            let horizon_cutoff = min(gc_info.horizon_cutoff, self.get_disk_consistent_lsn());
            let pitr_cutoff = gc_info.pitr_cutoff;
            let retain_lsns = gc_info.retain_lsns.clone();
            (horizon_cutoff, pitr_cutoff, retain_lsns)
        };

        let new_gc_cutoff = Lsn::min(horizon_cutoff, pitr_cutoff);

        let res = self
            .gc_timeline(
                &layer_removal_cs,
                horizon_cutoff,
                pitr_cutoff,
                retain_lsns,
                new_gc_cutoff,
            )
            .instrument(
                info_span!("gc_timeline", timeline = %self.timeline_id, cutoff = %new_gc_cutoff),
            )
            .await?;

        // only record successes
        timer.stop_and_record();

        Ok(res)
    }

    async fn gc_timeline(
        &self,
        layer_removal_cs: &tokio::sync::MutexGuard<'_, ()>,
        horizon_cutoff: Lsn,
        pitr_cutoff: Lsn,
        retain_lsns: Vec<Lsn>,
        new_gc_cutoff: Lsn,
    ) -> anyhow::Result<GcResult> {
        let now = SystemTime::now();
        let mut result: GcResult = GcResult::default();

        // Nothing to GC. Return early.
        let latest_gc_cutoff = *self.get_latest_gc_cutoff_lsn();
        if latest_gc_cutoff >= new_gc_cutoff {
            info!(
                "Nothing to GC: new_gc_cutoff_lsn {new_gc_cutoff}, latest_gc_cutoff_lsn {latest_gc_cutoff}",
            );
            return Ok(result);
        }

        // We need to ensure that no one tries to read page versions or create
        // branches at a point before latest_gc_cutoff_lsn. See branch_timeline()
        // for details. This will block until the old value is no longer in use.
        //
        // The GC cutoff should only ever move forwards.
        {
            let write_guard = self.latest_gc_cutoff_lsn.lock_for_write();
            ensure!(
                *write_guard <= new_gc_cutoff,
                "Cannot move GC cutoff LSN backwards (was {}, new {})",
                *write_guard,
                new_gc_cutoff
            );
            write_guard.store_and_unlock(new_gc_cutoff).wait();
        }

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        // Before deleting any layers, we need to wait for their upload ops to finish.
        // See storage_sync module level comment on consistency.
        // Do it here because we don't want to hold self.layers.write() while waiting.
        if let Some(remote_client) = &self.remote_client {
            debug!("waiting for upload ops to complete");
            remote_client
                .wait_completion()
                .await
                .context("wait for layer upload ops to complete")?;
        }

        let mut layers_to_remove = Vec::new();
        let mut wanted_image_layers = KeySpaceRandomAccum::default();

        // Scan all layers in the timeline (remote or on-disk).
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it is older than PITR interval;
        // 3. it doesn't need to be retained for 'retain_lsns';
        // 4. newer on-disk image layers cover the layer's whole key range
        //
        // TODO holding a write lock is too agressive and avoidable
        let mut layers = self.layers.write().unwrap();
        'outer: for l in layers.iter_historic_layers() {
            result.layers_total += 1;

            // 1. Is it newer than GC horizon cutoff point?
            if l.get_lsn_range().end > horizon_cutoff {
                debug!(
                    "keeping {} because it's newer than horizon_cutoff {}",
                    l.filename().file_name(),
                    horizon_cutoff
                );
                result.layers_needed_by_cutoff += 1;
                continue 'outer;
            }

            // 2. It is newer than PiTR cutoff point?
            if l.get_lsn_range().end > pitr_cutoff {
                debug!(
                    "keeping {} because it's newer than pitr_cutoff {}",
                    l.filename().file_name(),
                    pitr_cutoff
                );
                result.layers_needed_by_pitr += 1;
                continue 'outer;
            }

            // 3. Is it needed by a child branch?
            // NOTE With that we would keep data that
            // might be referenced by child branches forever.
            // We can track this in child timeline GC and delete parent layers when
            // they are no longer needed. This might be complicated with long inheritance chains.
            //
            // TODO Vec is not a great choice for `retain_lsns`
            for retain_lsn in &retain_lsns {
                // start_lsn is inclusive
                if &l.get_lsn_range().start <= retain_lsn {
                    debug!(
                        "keeping {} because it's still might be referenced by child branch forked at {} is_dropped: xx is_incremental: {}",
                        l.filename().file_name(),
                        retain_lsn,
                        l.is_incremental(),
                    );
                    result.layers_needed_by_branches += 1;
                    continue 'outer;
                }
            }

            // 4. Is there a later on-disk layer for this relation?
            //
            // The end-LSN is exclusive, while disk_consistent_lsn is
            // inclusive. For example, if disk_consistent_lsn is 100, it is
            // OK for a delta layer to have end LSN 101, but if the end LSN
            // is 102, then it might not have been fully flushed to disk
            // before crash.
            //
            // For example, imagine that the following layers exist:
            //
            // 1000      - image (A)
            // 1000-2000 - delta (B)
            // 2000      - image (C)
            // 2000-3000 - delta (D)
            // 3000      - image (E)
            //
            // If GC horizon is at 2500, we can remove layers A and B, but
            // we cannot remove C, even though it's older than 2500, because
            // the delta layer 2000-3000 depends on it.
            if !layers
                .image_layer_exists(&l.get_key_range(), &(l.get_lsn_range().end..new_gc_cutoff))?
            {
                debug!(
                    "keeping {} because it is the latest layer",
                    l.filename().file_name()
                );
                // Collect delta key ranges that need image layers to allow garbage
                // collecting the layers.
                // It is not so obvious whether we need to propagate information only about
                // delta layers. Image layers can form "stairs" preventing old image from been deleted.
                // But image layers are in any case less sparse than delta layers. Also we need some
                // protection from replacing recent image layers with new one after each GC iteration.
                if l.is_incremental() && !LayerMap::is_l0(&*l) {
                    wanted_image_layers.add_range(l.get_key_range());
                }
                result.layers_not_updated += 1;
                continue 'outer;
            }

            // We didn't find any reason to keep this file, so remove it.
            debug!(
                "garbage collecting {} is_dropped: xx is_incremental: {}",
                l.filename().file_name(),
                l.is_incremental(),
            );
            layers_to_remove.push(Arc::clone(&l));
        }
        self.wanted_image_layers
            .lock()
            .unwrap()
            .replace((new_gc_cutoff, wanted_image_layers.to_keyspace()));

        let mut updates = layers.batch_update();
        if !layers_to_remove.is_empty() {
            // Persist the new GC cutoff value in the metadata file, before
            // we actually remove anything.
            self.update_metadata_file(self.disk_consistent_lsn.load(), HashMap::new())?;

            // Actually delete the layers from disk and remove them from the map.
            // (couldn't do this in the loop above, because you cannot modify a collection
            // while iterating it. BTreeMap::retain() would be another option)
            let mut layer_names_to_delete = Vec::with_capacity(layers_to_remove.len());
            {
                for doomed_layer in layers_to_remove {
                    layer_names_to_delete.push(doomed_layer.filename());
                    self.delete_historic_layer(layer_removal_cs, doomed_layer, &mut updates)?; // FIXME: schedule succeeded deletions before returning?
                    result.layers_removed += 1;
                }
            }

            if result.layers_removed != 0 {
                fail_point!("after-timeline-gc-removed-layers");
            }

            if let Some(remote_client) = &self.remote_client {
                remote_client.schedule_layer_file_deletion(&layer_names_to_delete)?;
            }
        }
        updates.flush();

        info!(
            "GC completed removing {} layers, cutoff {}",
            result.layers_removed, new_gc_cutoff
        );

        result.elapsed = now.elapsed()?;
        Ok(result)
    }

    ///
    /// Reconstruct a value, using the given base image and WAL records in 'data'.
    ///
    fn reconstruct_value(
        &self,
        key: Key,
        request_lsn: Lsn,
        mut data: ValueReconstructState,
    ) -> Result<Bytes, PageReconstructError> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some((img_lsn, img)) = &data.img {
                trace!(
                    "found page image for key {} at {}, no WAL redo required, req LSN {}",
                    key,
                    img_lsn,
                    request_lsn,
                );
                Ok(img.clone())
            } else {
                Err(PageReconstructError::from(anyhow!(
                    "base image for {key} at {request_lsn} not found"
                )))
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.img.is_none() && !data.records.first().unwrap().1.will_init() {
                Err(PageReconstructError::from(anyhow!(
                    "Base image for {} at {} not found, but got {} WAL records",
                    key,
                    request_lsn,
                    data.records.len()
                )))
            } else {
                if data.img.is_some() {
                    trace!(
                        "found {} WAL records and a base image for {} at {}, performing WAL redo",
                        data.records.len(),
                        key,
                        request_lsn
                    );
                } else {
                    trace!("found {} WAL records that will init the page for {} at {}, performing WAL redo", data.records.len(), key, request_lsn);
                };

                let last_rec_lsn = data.records.last().unwrap().0;

                let img = match self
                    .walredo_mgr
                    .request_redo(key, request_lsn, data.img, data.records, self.pg_version)
                    .context("Failed to reconstruct a page image:")
                {
                    Ok(img) => img,
                    Err(e) => return Err(PageReconstructError::from(e)),
                };

                if img.len() == page_cache::PAGE_SZ {
                    let cache = page_cache::get();
                    if let Err(e) = cache
                        .memorize_materialized_page(
                            self.tenant_id,
                            self.timeline_id,
                            key,
                            last_rec_lsn,
                            &img,
                        )
                        .context("Materialized page memoization failed")
                    {
                        return Err(PageReconstructError::from(e));
                    }
                }

                Ok(img)
            }
        }
    }

    /// Download a layer file from remote storage and insert it into the layer map.
    ///
    /// It's safe to call this function for the same layer concurrently. In that case:
    /// - If the layer has already been downloaded, `OK(...)` is returned.
    /// - If the layer is currently being downloaded, we wait until that download succeeded / failed.
    ///     - If it succeeded, we return `Ok(...)`.
    ///     - If it failed, we or another concurrent caller will initiate a new download attempt.
    ///
    /// Download errors are classified and retried if appropriate by the underlying RemoteTimelineClient function.
    /// It has an internal limit for the maximum number of retries and prints appropriate log messages.
    /// If we exceed the limit, it returns an error, and this function passes it through.
    /// The caller _could_ retry further by themselves by calling this function again, but _should not_ do it.
    /// The reason is that they cannot distinguish permanent errors from temporary ones, whereas
    /// the underlying RemoteTimelineClient can.
    ///
    /// There is no internal timeout or slowness detection.
    /// If the caller has a deadline or needs a timeout, they can simply stop polling:
    /// we're **cancellation-safe** because the download happens in a separate task_mgr task.
    /// So, the current download attempt will run to completion even if we stop polling.
    #[instrument(skip_all, fields(layer=%remote_layer.short_id()))]
    pub async fn download_remote_layer(
        &self,
        remote_layer: Arc<RemoteLayer>,
    ) -> anyhow::Result<()> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        use std::sync::atomic::Ordering::Relaxed;

        let permit = match Arc::clone(&remote_layer.ongoing_download)
            .acquire_owned()
            .await
        {
            Ok(permit) => permit,
            Err(_closed) => {
                if remote_layer.download_replacement_failure.load(Relaxed) {
                    // this path will be hit often, in case there are upper retries. however
                    // hitting this error will prevent a busy loop between get_reconstruct_data and
                    // download, so an error is prefered.
                    //
                    // TODO: we really should poison the timeline, but panicking is not yet
                    // supported. Related: https://github.com/neondatabase/neon/issues/3621
                    anyhow::bail!("an earlier download succeeded but LayerMap::replace failed")
                } else {
                    info!("download of layer has already finished");
                    return Ok(());
                }
            }
        };

        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Spawn a task so that download does not outlive timeline when we detach tenant / delete timeline.
        let self_clone = self.myself.upgrade().expect("timeline is gone");
        task_mgr::spawn(
            &tokio::runtime::Handle::current(),
            TaskKind::RemoteDownloadTask,
            Some(self.tenant_id),
            Some(self.timeline_id),
            &format!("download layer {}", remote_layer.short_id()),
            false,
            async move {
                let remote_client = self_clone.remote_client.as_ref().unwrap();

                // Does retries + exponential back-off internally.
                // When this fails, don't layer further retry attempts here.
                let result = remote_client
                    .download_layer_file(&remote_layer.file_name, &remote_layer.layer_metadata)
                    .await;

                if let Ok(size) = &result {
                    info!("layer file download finished");

                    // XXX the temp file is still around in Err() case
                    // and consumes space until we clean up upon pageserver restart.
                    self_clone.metrics.resident_physical_size_gauge.add(*size);

                    // Download complete. Replace the RemoteLayer with the corresponding
                    // Delta- or ImageLayer in the layer map.
                    let mut layers = self_clone.layers.write().unwrap();
                    let mut updates = layers.batch_update();
                    let new_layer = remote_layer.create_downloaded_layer(&updates, self_clone.conf, *size);
                    {
                        use crate::tenant::layer_map::Replacement;
                        let l: Arc<dyn PersistentLayer> = remote_layer.clone();
                        let failure = match updates.replace_historic(&l, new_layer) {
                            Ok(Replacement::Replaced { .. }) => false,
                            Ok(Replacement::NotFound) => {
                                // TODO: the downloaded file should probably be removed, otherwise
                                // it will be added to the layermap on next load? we should
                                // probably restart any get_reconstruct_data search as well.
                                //
                                // See: https://github.com/neondatabase/neon/issues/3533
                                error!("replacing downloaded layer into layermap failed because layer was not found");
                                true
                            }
                            Ok(Replacement::RemovalBuffered) => {
                                unreachable!("current implementation does not remove anything")
                            }
                            Ok(Replacement::Unexpected(other)) => {
                                // if the other layer would have the same pointer value as
                                // expected, it means they differ only on vtables.
                                //
                                // otherwise there's no known reason for this to happen as
                                // compacted layers should have different covering rectangle
                                // leading to produce Replacement::NotFound.

                                error!(
                                    expected.ptr = ?Arc::as_ptr(&l),
                                    other.ptr = ?Arc::as_ptr(&other),
                                    ?other,
                                    "replacing downloaded layer into layermap failed because another layer was found instead of expected"
                                );
                                true
                            }
                            Err(e) => {
                                // this is a precondition failure, the layer filename derived
                                // attributes didn't match up, which doesn't seem likely.
                                error!("replacing downloaded layer into layermap failed: {e:#?}");
                                true
                            }
                        };

                        if failure {
                            // mark the remote layer permanently failed; the timeline is most
                            // likely unusable after this. sadly we cannot just poison the layermap
                            // lock with panic, because that would create an issue with shutdown.
                            //
                            // this does not change the retry semantics on failed downloads.
                            //
                            // use of Relaxed is valid because closing of the semaphore gives
                            // happens-before and wakes up any waiters; we write this value before
                            // and any waiters (or would be waiters) will load it after closing
                            // semaphore.
                            //
                            // See: https://github.com/neondatabase/neon/issues/3533
                            remote_layer
                                .download_replacement_failure
                                .store(true, Relaxed);
                        }
                    }
                    updates.flush();
                    drop(layers);

                    info!("on-demand download successful");

                    // Now that we've inserted the download into the layer map,
                    // close the semaphore. This will make other waiters for
                    // this download return Ok(()).
                    assert!(!remote_layer.ongoing_download.is_closed());
                    remote_layer.ongoing_download.close();
                } else {
                    // Keep semaphore open. We'll drop the permit at the end of the function.
                    error!("layer file download failed: {:?}", result.as_ref().unwrap_err());
                }

                // Don't treat it as an error if the task that triggered the download
                // is no longer interested in the result.
                sender.send(result.map(|_sz| ())).ok();

                // In case we failed and there are other waiters, this will make one
                // of them retry the download in a new task.
                // XXX: This resets the exponential backoff because it's a new call to
                // download_layer file.
                drop(permit);

                Ok(())
            }.in_current_span(),
        );

        receiver.await.context("download task cancelled")?
    }

    pub async fn spawn_download_all_remote_layers(
        self: Arc<Self>,
        request: DownloadRemoteLayersTaskSpawnRequest,
    ) -> Result<DownloadRemoteLayersTaskInfo, DownloadRemoteLayersTaskInfo> {
        let mut status_guard = self.download_all_remote_layers_task_info.write().unwrap();
        if let Some(st) = &*status_guard {
            match &st.state {
                DownloadRemoteLayersTaskState::Running => {
                    return Err(st.clone());
                }
                DownloadRemoteLayersTaskState::ShutDown
                | DownloadRemoteLayersTaskState::Completed => {
                    *status_guard = None;
                }
            }
        }

        let self_clone = Arc::clone(&self);
        let task_id = task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            task_mgr::TaskKind::DownloadAllRemoteLayers,
            Some(self.tenant_id),
            Some(self.timeline_id),
            "download all remote layers task",
            false,
            async move {
                self_clone.download_all_remote_layers(request).await;
                let mut status_guard = self_clone.download_all_remote_layers_task_info.write().unwrap();
                 match &mut *status_guard {
                    None => {
                        warn!("tasks status is supposed to be Some(), since we are running");
                    }
                    Some(st) => {
                        let exp_task_id = format!("{}", task_mgr::current_task_id().unwrap());
                        if st.task_id != exp_task_id {
                            warn!("task id changed while we were still running, expecting {} but have {}", exp_task_id, st.task_id);
                        } else {
                            st.state = DownloadRemoteLayersTaskState::Completed;
                        }
                    }
                };
                Ok(())
            }
            .instrument(info_span!(parent: None, "download_all_remote_layers", tenant = %self.tenant_id, timeline = %self.timeline_id))
        );

        let initial_info = DownloadRemoteLayersTaskInfo {
            task_id: format!("{task_id}"),
            state: DownloadRemoteLayersTaskState::Running,
            total_layer_count: 0,
            successful_download_count: 0,
            failed_download_count: 0,
        };
        *status_guard = Some(initial_info.clone());

        Ok(initial_info)
    }

    async fn download_all_remote_layers(
        self: &Arc<Self>,
        request: DownloadRemoteLayersTaskSpawnRequest,
    ) {
        let mut downloads = Vec::new();
        {
            let layers = self.layers.read().unwrap();
            layers
                .iter_historic_layers()
                .filter_map(|l| l.downcast_remote_layer())
                .map(|l| self.download_remote_layer(l))
                .for_each(|dl| downloads.push(dl))
        }
        let total_layer_count = downloads.len();
        // limit download concurrency as specified in request
        let downloads = futures::stream::iter(downloads);
        let mut downloads = downloads.buffer_unordered(request.max_concurrent_downloads.get());

        macro_rules! lock_status {
            ($st:ident) => {
                let mut st = self.download_all_remote_layers_task_info.write().unwrap();
                let st = st
                    .as_mut()
                    .expect("this function is only called after the task has been spawned");
                assert_eq!(
                    st.task_id,
                    format!(
                        "{}",
                        task_mgr::current_task_id().expect("we run inside a task_mgr task")
                    )
                );
                let $st = st;
            };
        }

        {
            lock_status!(st);
            st.total_layer_count = total_layer_count as u64;
        }
        loop {
            tokio::select! {
                dl = downloads.next() => {
                    lock_status!(st);
                    match dl {
                        None => break,
                        Some(Ok(())) => {
                            st.successful_download_count += 1;
                        },
                        Some(Err(e)) => {
                            error!(error = %e, "layer download failed");
                            st.failed_download_count += 1;
                        }
                    }
                }
                _ = task_mgr::shutdown_watcher() => {
                    // Kind of pointless to watch for shutdowns here,
                    // as download_remote_layer spawns other task_mgr tasks internally.
                    lock_status!(st);
                    st.state = DownloadRemoteLayersTaskState::ShutDown;
                }
            }
        }
        {
            lock_status!(st);
            st.state = DownloadRemoteLayersTaskState::Completed;
        }
    }

    pub fn get_download_all_remote_layers_task_info(&self) -> Option<DownloadRemoteLayersTaskInfo> {
        self.download_all_remote_layers_task_info
            .read()
            .unwrap()
            .clone()
    }
}

pub struct DiskUsageEvictionInfo {
    /// Timeline's largest layer (remote or resident)
    pub max_layer_size: Option<u64>,
    /// Timeline's resident layers
    pub resident_layers: Vec<LocalLayerInfoForDiskUsageEviction>,
}

pub struct LocalLayerInfoForDiskUsageEviction {
    pub layer: Arc<dyn PersistentLayer>,
    pub last_activity_ts: SystemTime,
}

impl std::fmt::Debug for LocalLayerInfoForDiskUsageEviction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // format the tv_sec, tv_nsec into rfc3339 in case someone is looking at it
        // having to allocate a string to this is bad, but it will rarely be formatted
        let ts = chrono::DateTime::<chrono::Utc>::from(self.last_activity_ts);
        let ts = ts.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        f.debug_struct("LocalLayerInfoForDiskUsageEviction")
            .field("layer", &self.layer)
            .field("last_activity", &ts)
            .finish()
    }
}

impl LocalLayerInfoForDiskUsageEviction {
    pub fn file_size(&self) -> u64 {
        self.layer.file_size()
    }
}

impl Timeline {
    pub(crate) fn get_local_layers_for_disk_usage_eviction(&self) -> DiskUsageEvictionInfo {
        let layers = self.layers.read().unwrap();

        let mut max_layer_size: Option<u64> = None;
        let mut resident_layers = Vec::new();

        for l in layers.iter_historic_layers() {
            let file_size = l.file_size();
            max_layer_size = max_layer_size.map_or(Some(file_size), |m| Some(m.max(file_size)));

            if l.is_remote_layer() {
                continue;
            }

            let last_activity_ts = l
                .access_stats()
                .latest_activity()
                .unwrap_or_else(|| {
                    // We only use this fallback if there's an implementation error.
                    // `latest_activity` already does rate-limited warn!() log.
                    debug!(layer=%l.filename().file_name(), "last_activity returns None, using SystemTime::now");
                    SystemTime::now()
                });

            resident_layers.push(LocalLayerInfoForDiskUsageEviction {
                layer: l,
                last_activity_ts,
            });
        }

        DiskUsageEvictionInfo {
            max_layer_size,
            resident_layers,
        }
    }
}

type TraversalPathItem = (
    ValueReconstructResult,
    Lsn,
    Box<dyn Send + FnOnce() -> TraversalId>,
);

/// Helper function for get_reconstruct_data() to add the path of layers traversed
/// to an error, as anyhow context information.
fn layer_traversal_error(msg: String, path: Vec<TraversalPathItem>) -> PageReconstructError {
    // We want the original 'msg' to be the outermost context. The outermost context
    // is the most high-level information, which also gets propagated to the client.
    let mut msg_iter = path
        .into_iter()
        .map(|(r, c, l)| {
            format!(
                "layer traversal: result {:?}, cont_lsn {}, layer: {}",
                r,
                c,
                l(),
            )
        })
        .chain(std::iter::once(msg));
    // Construct initial message from the first traversed layer
    let err = anyhow!(msg_iter.next().unwrap());

    // Append all subsequent traversals, and the error message 'msg', as contexts.
    let msg = msg_iter.fold(err, |err, msg| err.context(msg));
    PageReconstructError::from(msg)
}

/// Various functions to mutate the timeline.
// TODO Currently, Deref is used to allow easy access to read methods from this trait.
// This is probably considered a bad practice in Rust and should be fixed eventually,
// but will cause large code changes.
pub struct TimelineWriter<'a> {
    tl: &'a Timeline,
    _write_guard: MutexGuard<'a, ()>,
}

impl Deref for TimelineWriter<'_> {
    type Target = Timeline;

    fn deref(&self) -> &Self::Target {
        self.tl
    }
}

impl<'a> TimelineWriter<'a> {
    /// Put a new page version that can be constructed from a WAL record
    ///
    /// This will implicitly extend the relation, if the page is beyond the
    /// current end-of-file.
    pub fn put(&self, key: Key, lsn: Lsn, value: &Value) -> anyhow::Result<()> {
        self.tl.put_value(key, lsn, value)
    }

    pub fn delete(&self, key_range: Range<Key>, lsn: Lsn) -> anyhow::Result<()> {
        self.tl.put_tombstone(key_range, lsn)
    }

    /// Track the end of the latest digested WAL record.
    /// Remember the (end of) last valid WAL record remembered in the timeline.
    ///
    /// Call this after you have finished writing all the WAL up to 'lsn'.
    ///
    /// 'lsn' must be aligned. This wakes up any wait_lsn() callers waiting for
    /// the 'lsn' or anything older. The previous last record LSN is stored alongside
    /// the latest and can be read.
    pub fn finish_write(&self, new_lsn: Lsn) {
        self.tl.finish_write(new_lsn);
    }

    pub fn update_current_logical_size(&self, delta: i64) -> u64 {
        self.tl.update_current_logical_size(delta)
    }
}

/// Add a suffix to a layer file's name: .{num}.old
/// Uses the first available num (starts at 0)
fn rename_to_backup(path: &Path) -> anyhow::Result<()> {
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow!("Path {} don't have a file name", path.display()))?
        .to_string_lossy();
    let mut new_path = path.to_owned();

    for i in 0u32.. {
        new_path.set_file_name(format!("{filename}.{i}.old"));
        if !new_path.exists() {
            std::fs::rename(path, &new_path)?;
            return Ok(());
        }
    }

    bail!("couldn't find an unused backup number for {:?}", path)
}

#[cfg(not(debug_assertions))]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {}

#[cfg(debug_assertions)]
#[inline]
pub(crate) fn debug_assert_current_span_has_tenant_and_timeline_id() {
    use utils::tracing_span_assert;

    pub static TIMELINE_ID_EXTRACTOR: once_cell::sync::Lazy<
        tracing_span_assert::MultiNameExtractor<2>,
    > = once_cell::sync::Lazy::new(|| {
        tracing_span_assert::MultiNameExtractor::new("TimelineId", ["timeline_id", "timeline"])
    });

    match tracing_span_assert::check_fields_present([
        &*super::TENANT_ID_EXTRACTOR,
        &*TIMELINE_ID_EXTRACTOR,
    ]) {
        Ok(()) => (),
        Err(missing) => panic!(
            "missing extractors: {:?}",
            missing.into_iter().map(|e| e.name()).collect::<Vec<_>>()
        ),
    }
}
