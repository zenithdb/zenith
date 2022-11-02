//!

use anyhow::{anyhow, bail, ensure, Context};
use bytes::Bytes;
use fail::fail_point;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pageserver_api::models::TimelineState;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tracing::*;

use std::cmp::{max, min, Ordering};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::ops::{Deref, Range};
use std::path::PathBuf;
use std::sync::atomic::{self, AtomicBool, AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, TryLockError};
use std::time::{Duration, Instant, SystemTime};

use crate::tenant::{
    delta_layer::{DeltaLayer, DeltaLayerWriter},
    ephemeral_file::is_ephemeral_file,
    filename::{DeltaFileName, ImageFileName},
    image_layer::{ImageLayer, ImageLayerWriter},
    inmemory_layer::InMemoryLayer,
    layer_map::{LayerMap, SearchResult},
    metadata::{save_metadata, TimelineMetadata},
    par_fsync,
    storage_layer::{Layer, ValueReconstructResult, ValueReconstructState},
};

use crate::config::{PageServerConf, METADATA_FILE_NAME};
use crate::keyspace::{KeyPartitioning, KeySpace};
use crate::metrics::TimelineMetrics;
use crate::page_image_cache;
use crate::pgdatadir_mapping::BlockNumber;
use crate::pgdatadir_mapping::LsnForTimestamp;
use crate::pgdatadir_mapping::{is_rel_fsm_block_key, is_rel_vm_block_key};
use crate::tenant_config::TenantConfOpt;
use pageserver_api::reltag::RelTag;

use postgres_ffi::to_pg_timestamp;
use utils::{
    id::{TenantId, TimelineId},
    lsn::{AtomicLsn, Lsn, RecordLsn},
    seqwait::SeqWait,
    simple_rcu::{Rcu, RcuReadGuard},
};

use crate::repository::GcResult;
use crate::repository::{Key, Value};
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::walreceiver::{is_etcd_client_initialized, spawn_connection_manager_task};
use crate::walredo::WalRedoManager;
use crate::CheckpointConfig;
use crate::ZERO_PAGE;
use crate::{
    page_cache,
    storage_sync::{self, index::LayerFileMetadata},
};

pub struct Timeline {
    conf: &'static PageServerConf,
    tenant_conf: Arc<RwLock<TenantConfOpt>>,

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,

    pub pg_version: u32,

    pub layers: RwLock<LayerMap>,

    last_freeze_at: AtomicLsn,
    // Atomic would be more appropriate here.
    last_freeze_ts: RwLock<Instant>,

    // WAL redo manager
    walredo_mgr: Arc<dyn WalRedoManager + Sync + Send>,

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

    // Metrics
    metrics: TimelineMetrics,

    /// If `true`, will backup its files that appear after each checkpointing to the remote storage.
    upload_layers: AtomicBool,

    /// Ensures layers aren't frozen by checkpointer between
    /// [`Timeline::get_layer_for_write`] and layer reads.
    /// Locked automatically by [`TimelineWriter`] and checkpointer.
    /// Must always be acquired before the layer map/individual layer lock
    /// to avoid deadlock.
    write_lock: Mutex<()>,

    /// Used to ensure that there is only task performing flushing at a time
    layer_flush_lock: Mutex<()>,

    /// Layer removal lock.
    /// A lock to ensure that no layer of the timeline is removed concurrently by other tasks.
    /// This lock is acquired in [`Timeline::gc`], [`Timeline::compact`],
    /// and [`Tenant::delete_timeline`].
    layer_removal_cs: Mutex<()>,

    // Needed to ensure that we can't create a branch at a point that was already garbage collected
    pub latest_gc_cutoff_lsn: Rcu<Lsn>,

    // List of child timelines and their branch points. This is needed to avoid
    // garbage collecting data that is still needed by the child timelines.
    pub gc_info: RwLock<GcInfo>,

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
    current_logical_size: LogicalSize,
    initial_size_computation_started: AtomicBool,

    /// Information about the last processed message by the WAL receiver,
    /// or None if WAL receiver has not received anything for this timeline
    /// yet.
    pub last_received_wal: Mutex<Option<WalReceiverInfo>>,

    /// Relation size cache
    pub rel_size_cache: RwLock<HashMap<RelTag, (Lsn, BlockNumber)>>,

    state: watch::Sender<TimelineState>,
}

/// Internal structure to hold all data needed for logical size calculation.
/// Calculation consists of two parts:
/// 1.  Initial size calculation. That might take a long time, because it requires
/// reading all layers containing relation sizes up to the `initial_part_end`.
/// 2. Collecting an incremental part and adding that to the initial size.
/// Increments are appended on walreceiver writing new timeline data,
/// which result in increase or decrease of the logical size.
struct LogicalSize {
    /// Size, potentially slow to compute, derived from all layers located locally on this node's FS.
    /// Might require reading multiple layers, and even ancestor's layers, to collect the size.
    ///
    /// NOTE: initial size is not a constant and will change between restarts.
    initial_logical_size: OnceCell<u64>,
    /// Latest Lsn that has its size uncalculated, could be absent for freshly created timelines.
    initial_part_end: Option<Lsn>,
    /// All other size changes after startup, combined together.
    ///
    /// Size shouldn't ever be negative, but this is signed for two reasons:
    ///
    /// 1. If we initialized the "baseline" size lazily, while we already
    /// process incoming WAL, the incoming WAL records could decrement the
    /// variable and temporarily make it negative. (This is just future-proofing;
    /// the initialization is currently not done lazily.)
    ///
    /// 2. If there is a bug and we e.g. forget to increment it in some cases
    /// when size grows, but remember to decrement it when it shrinks again, the
    /// variable could go negative. In that case, it seems better to at least
    /// try to keep tracking it, rather than clamp or overflow it. Note that
    /// get_current_logical_size() will clamp the returned value to zero if it's
    /// negative, and log an error. Could set it permanently to zero or some
    /// special value to indicate "broken" instead, but this will do for now.
    ///
    /// Note that we also expose a copy of this value as a prometheus metric,
    /// see `current_logical_size_gauge`. Use the `update_current_logical_size`
    /// to modify this, it will also keep the prometheus metric in sync.
    size_added_after_initial: AtomicI64,
}

/// Normalized current size, that the data in pageserver occupies.
#[derive(Debug, Clone, Copy)]
enum CurrentLogicalSize {
    /// The size is not yet calculated to the end, this is an intermediate result,
    /// constructed from walreceiver increments and normalized: logical data could delete some objects, hence be negative,
    /// yet total logical size cannot be below 0.
    Approximate(u64),
    // Fully calculated logical size, only other future walreceiver increments are changing it, and those changes are
    // available for observation without any calculations.
    Exact(u64),
}

impl CurrentLogicalSize {
    fn size(&self) -> u64 {
        *match self {
            Self::Approximate(size) => size,
            Self::Exact(size) => size,
        }
    }
}

impl LogicalSize {
    fn empty_initial() -> Self {
        Self {
            initial_logical_size: OnceCell::with_value(0),
            initial_part_end: None,
            size_added_after_initial: AtomicI64::new(0),
        }
    }

    fn deferred_initial(compute_to: Lsn) -> Self {
        Self {
            initial_logical_size: OnceCell::new(),
            initial_part_end: Some(compute_to),
            size_added_after_initial: AtomicI64::new(0),
        }
    }

    fn current_size(&self) -> anyhow::Result<CurrentLogicalSize> {
        let size_increment: i64 = self.size_added_after_initial.load(AtomicOrdering::Acquire);
        //                  ^^^ keep this type explicit so that the casts in this function break if
        //                  we change the type.
        match self.initial_logical_size.get() {
            Some(initial_size) => {
                let absolute_size_increment = u64::try_from(
                    size_increment
                        .checked_abs()
                        .with_context(|| format!("Size added after initial {size_increment} is not expected to be i64::MIN"))?,
                    ).expect("casting nonnegative i64 to u64 should not fail");

                if size_increment < 0 {
                    initial_size.checked_sub(absolute_size_increment)
                } else {
                    initial_size.checked_add(absolute_size_increment)
                }.with_context(|| format!("Overflow during logical size calculation, initial_size: {initial_size}, size_increment: {size_increment}"))
                .map(CurrentLogicalSize::Exact)
            }
            None => {
                let non_negative_size_increment = u64::try_from(size_increment).unwrap_or(0);
                Ok(CurrentLogicalSize::Approximate(non_negative_size_increment))
            }
        }
    }

    fn increment_size(&self, delta: i64) {
        self.size_added_after_initial
            .fetch_add(delta, AtomicOrdering::SeqCst);
    }
}

pub struct WalReceiverInfo {
    pub wal_source_connstr: String,
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
    /// NOTE: It is considered an error to 'get' a key that doesn't exist. The abstraction
    /// above this needs to store suitable metadata to track what data exists with
    /// what keys, in separate metadata entries. If a non-existent key is requested,
    /// the Repository implementation may incorrectly return a value from an ancestor
    /// branch, for example, or waste a lot of cycles chasing the non-existing key.
    ///
    pub fn get(&self, key: Key, lsn: Lsn) -> anyhow::Result<Bytes> {
        anyhow::ensure!(lsn.is_valid(), "Invalid LSN");

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

        self.get_reconstruct_data(key, lsn, &mut reconstruct_state)?;

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

    /// Get the physical size of the timeline at the latest LSN
    pub fn get_physical_size(&self) -> u64 {
        self.metrics.current_physical_size_gauge.get()
    }

    /// Get the physical size of the timeline at the latest LSN non incrementally
    pub fn get_physical_size_non_incremental(&self) -> anyhow::Result<u64> {
        let timeline_path = self.conf.timeline_path(&self.timeline_id, &self.tenant_id);
        // total size of layer files in the current timeline directory
        let mut total_physical_size = 0;

        for direntry in fs::read_dir(timeline_path)? {
            let direntry = direntry?;
            let fname = direntry.file_name();
            let fname = fname.to_string_lossy();

            if ImageFileName::parse_str(&fname).is_some()
                || DeltaFileName::parse_str(&fname).is_some()
            {
                total_physical_size += direntry.metadata()?.len();
            }
        }

        Ok(total_physical_size)
    }

    ///
    /// Wait until WAL has been received and processed up to this LSN.
    ///
    /// You should call this before any of the other get_* or list_* functions. Calling
    /// those functions with an LSN that has been processed yet is an error.
    ///
    pub async fn wait_lsn(&self, lsn: Lsn) -> anyhow::Result<()> {
        anyhow::ensure!(self.is_active(), "Cannot wait for Lsn on inactive timeline");

        // This should never be called from the WAL receiver, because that could lead
        // to a deadlock.
        anyhow::ensure!(
            task_mgr::current_task_kind() != Some(TaskKind::WalReceiverConnection),
            "wait_lsn cannot be called in WAL receiver"
        );

        let _timer = self.metrics.wait_lsn_time_histo.start_timer();

        self.last_record_lsn.wait_for_timeout(lsn, self.conf.wait_lsn_timeout).await
            .with_context(||
                format!(
                    "Timed out while waiting for WAL record at LSN {} to arrive, last_record_lsn {} disk consistent LSN={}",
                    lsn, self.get_last_record_lsn(), self.get_disk_consistent_lsn()
                )
            )?;

        Ok(())
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
    ///
    /// NOTE: This has nothing to do with checkpoint in PostgreSQL. We don't
    /// know anything about them here in the repository.
    pub fn checkpoint(&self, cconf: CheckpointConfig) -> anyhow::Result<()> {
        match cconf {
            CheckpointConfig::Flush => {
                self.freeze_inmem_layer(false);
                self.flush_frozen_layers(true)
            }
            CheckpointConfig::Forced => {
                self.freeze_inmem_layer(false);
                self.flush_frozen_layers(true)?;
                self.compact()
            }
        }
    }

    pub fn compact(&self) -> anyhow::Result<()> {
        let last_record_lsn = self.get_last_record_lsn();

        // Last record Lsn could be zero in case the timelie was just created
        if !last_record_lsn.is_valid() {
            warn!("Skipping compaction for potentially just initialized timeline, it has invalid last record lsn: {last_record_lsn}");
            return Ok(());
        }

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
        let _layer_removal_cs = self.layer_removal_cs.lock().unwrap();

        let target_file_size = self.get_checkpoint_distance();

        // Define partitioning schema if needed

        match self.repartition(
            self.get_last_record_lsn(),
            self.get_compaction_target_size(),
        ) {
            Ok((partitioning, lsn)) => {
                // 2. Create new image layers for partitions that have been modified
                // "enough".
                let layer_paths_to_upload = self.create_image_layers(&partitioning, lsn, false)?;
                if !layer_paths_to_upload.is_empty()
                    && self.upload_layers.load(atomic::Ordering::Relaxed)
                {
                    storage_sync::schedule_layer_upload(
                        self.tenant_id,
                        self.timeline_id,
                        layer_paths_to_upload,
                        None,
                    );
                }

                // 3. Compact
                let timer = self.metrics.compact_time_histo.start_timer();
                self.compact_level0(target_file_size)?;
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
    pub fn get_current_logical_size(self: &Arc<Self>) -> anyhow::Result<u64> {
        let current_size = self.current_logical_size.current_size()?;
        debug!("Current size: {current_size:?}");

        let size = current_size.size();
        if let (CurrentLogicalSize::Approximate(_), Some(init_lsn)) =
            (current_size, self.current_logical_size.initial_part_end)
        {
            self.try_spawn_size_init_task(init_lsn);
        }

        Ok(size)
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

                // Launch a task to flush the frozen layer to disk, unless
                // a task was already running. (If the task was running
                // at the time that we froze the layer, it must've seen the
                // the layer we just froze before it exited; see comments
                // in flush_frozen_layers())
                if let Ok(guard) = self.layer_flush_lock.try_lock() {
                    drop(guard);
                    let self_clone = Arc::clone(self);
                    task_mgr::spawn(
                        task_mgr::BACKGROUND_RUNTIME.handle(),
                        task_mgr::TaskKind::LayerFlushTask,
                        Some(self.tenant_id),
                        Some(self.timeline_id),
                        "layer flush task",
                        false,
                        async move { self_clone.flush_frozen_layers(false) },
                    );
                }
            }
        }
        Ok(())
    }

    pub fn set_state(&self, new_state: TimelineState) {
        match (self.current_state(), new_state) {
            (equal_state_1, equal_state_2) if equal_state_1 == equal_state_2 => {
                debug!("Ignoring new state, equal to the existing one: {equal_state_2:?}");
            }
            (TimelineState::Broken, _) => {
                error!("Ignoring state update {new_state:?} for broken tenant");
            }
            (TimelineState::Paused, TimelineState::Active) => {
                debug!("Not activating a paused timeline");
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

    /// Open a Timeline handle.
    ///
    /// Loads the metadata for the timeline into memory, but not the layer map.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        conf: &'static PageServerConf,
        tenant_conf: Arc<RwLock<TenantConfOpt>>,
        metadata: TimelineMetadata,
        ancestor: Option<Arc<Timeline>>,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        walredo_mgr: Arc<dyn WalRedoManager + Send + Sync>,
        upload_layers: bool,
        pg_version: u32,
    ) -> Self {
        let disk_consistent_lsn = metadata.disk_consistent_lsn();
        let (state, _) = watch::channel(TimelineState::Suspended);

        let mut result = Timeline {
            conf,
            tenant_conf,
            timeline_id,
            tenant_id,
            pg_version,
            layers: RwLock::new(LayerMap::default()),

            walredo_mgr,

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

            metrics: TimelineMetrics::new(&tenant_id, &timeline_id),

            upload_layers: AtomicBool::new(upload_layers),

            write_lock: Mutex::new(()),
            layer_flush_lock: Mutex::new(()),
            layer_removal_cs: Mutex::new(()),

            gc_info: RwLock::new(GcInfo {
                retain_lsns: Vec::new(),
                horizon_cutoff: Lsn(0),
                pitr_cutoff: Lsn(0),
            }),

            latest_gc_cutoff_lsn: Rcu::new(metadata.latest_gc_cutoff_lsn()),
            initdb_lsn: metadata.initdb_lsn(),

            current_logical_size: if disk_consistent_lsn.is_valid() {
                // we're creating timeline data with some layer files existing locally,
                // need to recalculate timeline's logical size based on data in the layers.
                LogicalSize::deferred_initial(disk_consistent_lsn)
            } else {
                // we're creating timeline data without any layers existing locally,
                // initial logical size is 0.
                LogicalSize::empty_initial()
            },
            initial_size_computation_started: AtomicBool::new(false),
            partitioning: Mutex::new((KeyPartitioning::new(), Lsn(0))),
            repartition_threshold: 0,

            last_received_wal: Mutex::new(None),
            rel_size_cache: RwLock::new(HashMap::new()),
            state,
        };
        result.repartition_threshold = result.get_checkpoint_distance() / 10;
        result
    }

    pub(super) fn launch_wal_receiver(self: &Arc<Self>) {
        if !is_etcd_client_initialized() {
            if cfg!(test) {
                info!("not launching WAL receiver because etcd client hasn't been initialized");
                return;
            } else {
                panic!("etcd client not initialized");
            }
        }

        info!(
            "launching WAL receiver for timeline {} of tenant {}",
            self.timeline_id, self.tenant_id
        );
        let tenant_conf_guard = self.tenant_conf.read().unwrap();
        let lagging_wal_timeout = tenant_conf_guard
            .lagging_wal_timeout
            .unwrap_or(self.conf.default_tenant_conf.lagging_wal_timeout);
        let walreceiver_connect_timeout = tenant_conf_guard
            .walreceiver_connect_timeout
            .unwrap_or(self.conf.default_tenant_conf.walreceiver_connect_timeout);
        let max_lsn_wal_lag = tenant_conf_guard
            .max_lsn_wal_lag
            .unwrap_or(self.conf.default_tenant_conf.max_lsn_wal_lag);
        drop(tenant_conf_guard);
        let self_clone = Arc::clone(self);
        spawn_connection_manager_task(
            self.conf.broker_etcd_prefix.clone(),
            self_clone,
            walreceiver_connect_timeout,
            lagging_wal_timeout,
            max_lsn_wal_lag,
        );
    }

    ///
    /// Scan the timeline directory to populate the layer map.
    /// Returns all timeline-related files that were found and loaded.
    ///
    pub(super) fn load_layer_map(&self, disk_consistent_lsn: Lsn) -> anyhow::Result<()> {
        let mut layers = self.layers.write().unwrap();
        let mut num_layers = 0;

        let timer = self.metrics.load_layer_map_histo.start_timer();

        // Scan timeline directory and create ImageFileName and DeltaFilename
        // structs representing all files on disk
        let timeline_path = self.conf.timeline_path(&self.timeline_id, &self.tenant_id);
        // total size of layer files in the current timeline directory
        let mut total_physical_size = 0;

        for direntry in fs::read_dir(timeline_path)? {
            let direntry = direntry?;
            let fname = direntry.file_name();
            let fname = fname.to_string_lossy();

            if let Some(imgfilename) = ImageFileName::parse_str(&fname) {
                // create an ImageLayer struct for each image file.
                if imgfilename.lsn > disk_consistent_lsn {
                    warn!(
                        "found future image layer {} on timeline {} disk_consistent_lsn is {}",
                        imgfilename, self.timeline_id, disk_consistent_lsn
                    );

                    rename_to_backup(direntry.path())?;
                    continue;
                }

                let layer =
                    ImageLayer::new(self.conf, self.timeline_id, self.tenant_id, &imgfilename);

                trace!("found layer {}", layer.filename().display());
                total_physical_size += layer.path().metadata()?.len();
                layers.insert_historic(Arc::new(layer));
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

                    rename_to_backup(direntry.path())?;
                    continue;
                }

                let layer =
                    DeltaLayer::new(self.conf, self.timeline_id, self.tenant_id, &deltafilename);

                trace!("found layer {}", layer.filename().display());
                total_physical_size += layer.path().metadata()?.len();
                layers.insert_historic(Arc::new(layer));
                num_layers += 1;
            } else if fname == METADATA_FILE_NAME || fname.ends_with(".old") {
                // ignore these
            } else if is_ephemeral_file(&fname) {
                // Delete any old ephemeral files
                trace!("deleting old ephemeral file in timeline dir: {}", fname);
                fs::remove_file(direntry.path())?;
            } else {
                warn!("unrecognized filename in timeline dir: {}", fname);
            }
        }

        layers.next_open_layer_at = Some(Lsn(disk_consistent_lsn.0) + 1);

        info!(
            "loaded layer map with {} layers at {}, total physical size: {}",
            num_layers, disk_consistent_lsn, total_physical_size
        );
        self.metrics
            .current_physical_size_gauge
            .set(total_physical_size);

        timer.stop_and_record();

        Ok(())
    }

    pub(super) fn layer_removal_guard(&self) -> anyhow::Result<MutexGuard<()>> {
        self.layer_removal_cs
            .try_lock()
            .map_err(|e| anyhow!("cannot lock compaction critical section {e}"))
    }

    fn try_spawn_size_init_task(self: &Arc<Self>, init_lsn: Lsn) {
        // Atomically check if the timeline size calculation had already started.
        // If the flag was not already set, this sets it.
        if !self
            .initial_size_computation_started
            .swap(true, AtomicOrdering::SeqCst)
        {
            // We need to start the computation task.
            let self_clone = Arc::clone(self);
            task_mgr::spawn(
                task_mgr::BACKGROUND_RUNTIME.handle(),
                task_mgr::TaskKind::InitialLogicalSizeCalculation,
                Some(self.tenant_id),
                Some(self.timeline_id),
                "initial size calculation",
                false,
                async move {
                    let mut timeline_state_updates = self_clone.subscribe_for_state_updates();
                    let self_calculation = Arc::clone(&self_clone);
                    tokio::select! {
                        calculation_result = spawn_blocking(move || self_calculation.calculate_logical_size(init_lsn)) => {
                            let calculated_size = calculation_result
                                .context("Failed to spawn calculation result task")?
                                .context("Failed to calculate logical size")?;
                            match self_clone.current_logical_size.initial_logical_size.set(calculated_size) {
                                Ok(()) => info!("Successfully calculated initial logical size"),
                                Err(existing_size) => error!("Tried to update initial timeline size value to {calculated_size}, but the size was already set to {existing_size}, not changing"),
                            }
                            Ok(())
                        },
                        new_event = async {
                            loop {
                                match timeline_state_updates.changed().await {
                                    Ok(()) => {
                                        let new_state = *timeline_state_updates.borrow();
                                        match new_state {
                                            // we're running this job for active timelines only
                                            TimelineState::Active => continue,
                                            TimelineState::Broken | TimelineState::Paused | TimelineState::Suspended => return Some(new_state),
                                        }
                                    }
                                    Err(_sender_dropped_error) => return None,
                                }
                            }
                        } => {
                            match new_event {
                                Some(new_state) => info!("Timeline became inactive (new state: {new_state:?}), dropping current connections until it reactivates"),
                                None => info!("Timeline dropped state updates sender, stopping init size calculation"),
                            }
                            Ok(())
                        },
                    }
                }.instrument(info_span!("initial_logical_size_calculation", tenant = %self.tenant_id, timeline = %self.timeline_id)),
            );
        }
    }

    /// Calculate the logical size of the database at the latest LSN.
    ///
    /// NOTE: counted incrementally, includes ancestors, this can be a slow operation.
    fn calculate_logical_size(&self, up_to_lsn: Lsn) -> anyhow::Result<u64> {
        info!("Calculating logical size for timeline {}", self.timeline_id);
        let timer = self.metrics.init_logical_size_histo.start_timer();
        let logical_size = self.get_current_logical_size_non_incremental(up_to_lsn)?;
        debug!("calculated logical size: {logical_size}");
        timer.stop_and_record();
        Ok(logical_size)
    }

    /// Update current logical size, adding `delta' to the old value.
    fn update_current_logical_size(&self, delta: i64) {
        let logical_size = &self.current_logical_size;
        logical_size.increment_size(delta);

        // Also set the value in the prometheus gauge. Note that
        // there is a race condition here: if this is is called by two
        // threads concurrently, the prometheus gauge might be set to
        // one value while current_logical_size is set to the
        // other.
        match logical_size.current_size() {
            Ok(new_current_size) => self
                .metrics
                .current_logical_size_gauge
                .set(new_current_size.size()),
            Err(e) => error!("Failed to compute current logical size for metrics update: {e:?}"),
        }
    }

    ///
    /// Get a handle to a Layer for reading.
    ///
    /// The returned Layer might be from an ancestor timeline, if the
    /// segment hasn't been updated on this timeline yet.
    ///
    /// This function takes the current timeline's locked LayerMap as an argument,
    /// so callers can avoid potential race conditions.
    fn get_reconstruct_data(
        &self,
        key: Key,
        request_lsn: Lsn,
        reconstruct_state: &mut ValueReconstructState,
    ) -> anyhow::Result<()> {
        // Start from the current timeline.
        let mut timeline_owned;
        let mut timeline = self;

        // For debugging purposes, collect the path of layers that we traversed
        // through. It's included in the error message if we fail to find the key.
        let mut traversal_path: Vec<(ValueReconstructResult, Lsn, Arc<dyn Layer>)> = Vec::new();

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
                        return layer_traversal_error(format!(
                            "could not find layer with more data for key {} at LSN {}, request LSN {}, ancestor {}",
                            key,
                            Lsn(cont_lsn.0 - 1),
                            request_lsn,
                            timeline.ancestor_lsn
                        ), traversal_path);
                    }
                    prev_lsn = cont_lsn;
                }
                ValueReconstructResult::Missing => {
                    return layer_traversal_error(
                        format!(
                            "could not find data for key {} at LSN {}, for request at LSN {}",
                            key, cont_lsn, request_lsn
                        ),
                        traversal_path,
                    );
                }
            }

            // Recurse into ancestor if needed
            if Lsn(cont_lsn.0 - 1) <= timeline.ancestor_lsn {
                trace!(
                    "going into ancestor {}, cont_lsn is {}",
                    timeline.ancestor_lsn,
                    cont_lsn
                );
                let ancestor = timeline.get_ancestor_timeline()?;
                timeline_owned = ancestor;
                timeline = &*timeline_owned;
                prev_lsn = Lsn(u64::MAX);
                continue;
            }

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
                    result = open_layer.get_value_reconstruct_data(
                        key,
                        lsn_floor..cont_lsn,
                        reconstruct_state,
                    )?;
                    cont_lsn = lsn_floor;
                    traversal_path.push((result, cont_lsn, open_layer.clone()));
                    continue;
                }
            }
            for frozen_layer in layers.frozen_layers.iter().rev() {
                let start_lsn = frozen_layer.get_lsn_range().start;
                if cont_lsn > start_lsn {
                    //info!("CHECKING for {} at {} on frozen layer {}", key, cont_lsn, frozen_layer.filename().display());
                    let lsn_floor = max(cached_lsn + 1, start_lsn);
                    result = frozen_layer.get_value_reconstruct_data(
                        key,
                        lsn_floor..cont_lsn,
                        reconstruct_state,
                    )?;
                    cont_lsn = lsn_floor;
                    traversal_path.push((result, cont_lsn, frozen_layer.clone()));
                    continue 'outer;
                }
            }

            if let Some(SearchResult { lsn_floor, layer }) = layers.search(key, cont_lsn)? {
                //info!("CHECKING for {} at {} on historic layer {}", key, cont_lsn, layer.filename().display());

                let lsn_floor = max(cached_lsn + 1, lsn_floor);
                result = layer.get_value_reconstruct_data(
                    key,
                    lsn_floor..cont_lsn,
                    reconstruct_state,
                )?;
                cont_lsn = lsn_floor;
                traversal_path.push((result, cont_lsn, layer));
            } else if timeline.ancestor_timeline.is_some() {
                // Nothing on this timeline. Traverse to parent
                result = ValueReconstructResult::Continue;
                cont_lsn = Lsn(timeline.ancestor_lsn.0 + 1);
            } else {
                // Nothing found
                result = ValueReconstructResult::Missing;
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

    /// Flush all frozen layers to disk.
    ///
    /// Only one task at a time can be doing layer-flushing for a
    /// given timeline. If 'wait' is true, and another task is
    /// currently doing the flushing, this function will wait for it
    /// to finish. If 'wait' is false, this function will return
    /// immediately instead.
    fn flush_frozen_layers(&self, wait: bool) -> anyhow::Result<()> {
        let flush_lock_guard = if wait {
            self.layer_flush_lock.lock().unwrap()
        } else {
            match self.layer_flush_lock.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => return Ok(()),
                Err(TryLockError::Poisoned(err)) => panic!("{:?}", err),
            }
        };

        let timer = self.metrics.flush_time_histo.start_timer();

        loop {
            let layers = self.layers.read().unwrap();
            if let Some(frozen_layer) = layers.frozen_layers.front() {
                let frozen_layer = Arc::clone(frozen_layer);
                drop(layers); // to allow concurrent reads and writes
                self.flush_frozen_layer(frozen_layer)?;
            } else {
                // Drop the 'layer_flush_lock' *before* 'layers'. That
                // way, if you freeze a layer, and then call
                // flush_frozen_layers(false), it is guaranteed that
                // if another thread was busy flushing layers and the
                // call therefore returns immediately, the other
                // thread will have seen the newly-frozen layer and
                // will flush that too (assuming no errors).
                drop(flush_lock_guard);
                drop(layers);
                break;
            }
        }

        timer.stop_and_record();

        Ok(())
    }

    /// Flush one frozen in-memory layer to disk, as a new delta layer.
    fn flush_frozen_layer(&self, frozen_layer: Arc<InMemoryLayer>) -> anyhow::Result<()> {
        // As a special case, when we have just imported an image into the repository,
        // instead of writing out a L0 delta layer, we directly write out image layer
        // files instead. This is possible as long as *all* the data imported into the
        // repository have the same LSN.
        let lsn_range = frozen_layer.get_lsn_range();
        let layer_paths_to_upload =
            if lsn_range.start == self.initdb_lsn && lsn_range.end == Lsn(self.initdb_lsn.0 + 1) {
                let (partitioning, _lsn) =
                    self.repartition(self.initdb_lsn, self.get_compaction_target_size())?;
                self.create_image_layers(&partitioning, self.initdb_lsn, true)?
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
            assert!(Arc::ptr_eq(&l.unwrap(), &frozen_layer));

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
            self.update_metadata_file(disk_consistent_lsn, layer_paths_to_upload)?;
            // Also update the in-memory copy
            self.disk_consistent_lsn.store(disk_consistent_lsn);
        }
        Ok(())
    }

    /// Update metadata file
    fn update_metadata_file(
        &self,
        disk_consistent_lsn: Lsn,
        layer_paths_to_upload: HashMap<PathBuf, LayerFileMetadata>,
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
        )?;

        if self.can_upload_layers() {
            storage_sync::schedule_layer_upload(
                self.tenant_id,
                self.timeline_id,
                layer_paths_to_upload,
                Some(metadata),
            );
        }

        Ok(())
    }

    // Write out the given frozen in-memory layer as a new L0 delta file
    fn create_delta_layer(
        &self,
        frozen_layer: &InMemoryLayer,
    ) -> anyhow::Result<(PathBuf, LayerFileMetadata)> {
        // Write it out
        let new_delta = frozen_layer.write_to_disk()?;
        let new_delta_path = new_delta.path();

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
        {
            let mut layers = self.layers.write().unwrap();
            layers.insert_historic(Arc::new(new_delta));
        }

        // update the timeline's physical size
        let sz = new_delta_path.metadata()?.len();

        self.metrics.current_physical_size_gauge.add(sz);
        // update metrics
        self.metrics.num_persistent_files_created.inc_by(1);
        self.metrics.persistent_bytes_written.inc_by(sz);

        Ok((new_delta_path, LayerFileMetadata::new(sz)))
    }

    fn repartition(&self, lsn: Lsn, partition_size: u64) -> anyhow::Result<(KeyPartitioning, Lsn)> {
        let mut partitioning_guard = self.partitioning.lock().unwrap();
        if partitioning_guard.1 == Lsn(0)
            || lsn.0 - partitioning_guard.1 .0 > self.repartition_threshold
        {
            let keyspace = self.collect_keyspace(lsn)?;
            let partitioning = keyspace.partition(partition_size);
            *partitioning_guard = (partitioning, lsn);
            return Ok((partitioning_guard.0.clone(), lsn));
        }
        Ok((partitioning_guard.0.clone(), partitioning_guard.1))
    }

    // Is it time to create a new image layer for the given partition?
    fn time_for_new_image_layer(&self, partition: &KeySpace, lsn: Lsn) -> anyhow::Result<bool> {
        let layers = self.layers.read().unwrap();

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
                    let num_deltas = layers.count_deltas(&img_range, &(img_lsn..lsn))?;

                    debug!(
                        "key range {}-{}, has {} deltas on this timeline in LSN range {}..{}",
                        img_range.start, img_range.end, num_deltas, img_lsn, lsn
                    );
                    if num_deltas >= self.get_image_creation_threshold() {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn create_image_layers(
        &self,
        partitioning: &KeyPartitioning,
        lsn: Lsn,
        force: bool,
    ) -> anyhow::Result<HashMap<PathBuf, LayerFileMetadata>> {
        let timer = self.metrics.create_images_time_histo.start_timer();
        let mut image_layers: Vec<ImageLayer> = Vec::new();
        for partition in partitioning.parts.iter() {
            if force || self.time_for_new_image_layer(partition, lsn)? {
                let img_range =
                    partition.ranges.first().unwrap().start..partition.ranges.last().unwrap().end;
                let mut image_layer_writer = ImageLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_id,
                    &img_range,
                    lsn,
                )?;

                fail_point!("image-layer-writer-fail-before-finish", |_| {
                    anyhow::bail!("failpoint image-layer-writer-fail-before-finish");
                });

                for range in &partition.ranges {
                    let mut key = range.start;
                    while key < range.end {
                        let img = match self.get(key, lsn) {
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
        par_fsync::par_fsync(&all_paths)?;

        let mut layer_paths_to_upload = HashMap::with_capacity(image_layers.len());

        let mut layers = self.layers.write().unwrap();
        for l in image_layers {
            let path = l.path();
            let metadata = path.metadata()?;

            layer_paths_to_upload.insert(path, LayerFileMetadata::new(metadata.len()));

            self.metrics.current_physical_size_gauge.add(metadata.len());
            layers.insert_historic(Arc::new(l));
        }
        drop(layers);
        timer.stop_and_record();

        Ok(layer_paths_to_upload)
    }

    ///
    /// Collect a bunch of Level 0 layer files, and compact and reshuffle them as
    /// as Level 1 files.
    ///
    fn compact_level0(&self, target_file_size: u64) -> anyhow::Result<()> {
        let layers = self.layers.read().unwrap();
        let mut level0_deltas = layers.get_level0_deltas()?;
        drop(layers);

        // Only compact if enough layers have accumulated.
        if level0_deltas.is_empty() || level0_deltas.len() < self.get_compaction_threshold() {
            return Ok(());
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

        info!(
            "Starting Level0 compaction in LSN range {}-{} for {} layers ({} deltas in total)",
            lsn_range.start,
            lsn_range.end,
            deltas_to_compact.len(),
            level0_deltas.len()
        );
        for l in deltas_to_compact.iter() {
            info!("compact includes {}", l.filename().display());
        }
        // We don't need the original list of layers anymore. Drop it so that
        // we don't accidentally use it later in the function.
        drop(level0_deltas);

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let all_values_iter = deltas_to_compact
            .iter()
            .map(|l| l.iter())
            .kmerge_by(|a, b| {
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
            });

        // This iterator walks through all keys and is needed to calculate size used by each key
        let mut all_keys_iter = deltas_to_compact
            .iter()
            .map(|l| l.key_iter())
            .kmerge_by(|a, b| {
                let (a_key, a_lsn, _) = a;
                let (b_key, b_lsn, _) = b;
                match a_key.cmp(b_key) {
                    Ordering::Less => true,
                    Ordering::Equal => a_lsn <= b_lsn,
                    Ordering::Greater => false,
                }
            });

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
                    // check if key cause layer overflow...
                    if is_dup_layer
                        || dup_end_lsn.is_valid()
                        || written_size + key_values_total_size > target_file_size
                    {
                        // ... if so, flush previous layer and prepare to write new one
                        new_layers.push(writer.take().unwrap().finish(prev_key.unwrap().next())?);
                        writer = None;
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
                anyhow::bail!("failpoint delta-layer-writer-fail-before-finish");
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
            par_fsync::par_fsync(&layer_paths)?;

            layer_paths.pop().unwrap();
        }

        let mut layers = self.layers.write().unwrap();
        let mut new_layer_paths = HashMap::with_capacity(new_layers.len());
        for l in new_layers {
            let new_delta_path = l.path();

            let metadata = new_delta_path.metadata()?;

            // update the timeline's physical size
            self.metrics.current_physical_size_gauge.add(metadata.len());

            new_layer_paths.insert(new_delta_path, LayerFileMetadata::new(metadata.len()));
            layers.insert_historic(Arc::new(l));
        }

        // Now that we have reshuffled the data to set of new delta layers, we can
        // delete the old ones
        let mut layer_paths_do_delete = HashSet::with_capacity(deltas_to_compact.len());
        drop(all_keys_iter);
        for l in deltas_to_compact {
            if let Some(path) = l.local_path() {
                self.metrics
                    .current_physical_size_gauge
                    .sub(path.metadata()?.len());
                layer_paths_do_delete.insert(path);
            }
            l.delete()?;
            layers.remove_historic(l);
        }
        drop(layers);

        if self.can_upload_layers() {
            storage_sync::schedule_layer_upload(
                self.tenant_id,
                self.timeline_id,
                new_layer_paths,
                None,
            );
            storage_sync::schedule_layer_delete(
                self.tenant_id,
                self.timeline_id,
                layer_paths_do_delete,
            );
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
    pub(super) fn update_gc_info(
        &self,
        retain_lsns: Vec<Lsn>,
        cutoff_horizon: Lsn,
        pitr: Duration,
    ) -> anyhow::Result<()> {
        let mut gc_info = self.gc_info.write().unwrap();

        gc_info.horizon_cutoff = cutoff_horizon;
        gc_info.retain_lsns = retain_lsns;

        // Calculate pitr cutoff point.
        // If we cannot determine a cutoff LSN, be conservative and don't GC anything.
        let mut pitr_cutoff_lsn: Lsn;

        if pitr != Duration::ZERO {
            // conservative, safe default is to remove nothing, when we have no
            // commit timestamp data available
            pitr_cutoff_lsn = *self.get_latest_gc_cutoff_lsn();

            // First, calculate pitr_cutoff_timestamp and then convert it to LSN.
            // If we don't have enough data to convert to LSN,
            // play safe and don't remove any layers.
            let now = SystemTime::now();
            if let Some(pitr_cutoff_timestamp) = now.checked_sub(pitr) {
                let pitr_timestamp = to_pg_timestamp(pitr_cutoff_timestamp);

                match self.find_lsn_for_timestamp(pitr_timestamp)? {
                    LsnForTimestamp::Present(lsn) => pitr_cutoff_lsn = lsn,
                    LsnForTimestamp::Future(lsn) => {
                        debug!("future({})", lsn);
                        pitr_cutoff_lsn = gc_info.horizon_cutoff;
                    }
                    LsnForTimestamp::Past(lsn) => {
                        debug!("past({})", lsn);
                    }
                    LsnForTimestamp::NoData(lsn) => {
                        debug!("nodata({})", lsn);
                    }
                }
                debug!("pitr_cutoff_lsn = {:?}", pitr_cutoff_lsn)
            }
        } else {
            // No time-based retention. (Some unit tests depend on garbage-collection
            // working even when CLOG data is missing, so that find_lsn_for_timestamp()
            // above doesn't work.)
            pitr_cutoff_lsn = gc_info.horizon_cutoff;
        }
        gc_info.pitr_cutoff = pitr_cutoff_lsn;

        Ok(())
    }

    ///
    /// Garbage collect layer files on a timeline that are no longer needed.
    ///
    /// Currently, we don't make any attempt at removing unneeded page versions
    /// within a layer file. We can only remove the whole file if it's fully
    /// obsolete.
    ///
    pub(super) fn gc(&self) -> anyhow::Result<GcResult> {
        let mut result: GcResult = GcResult::default();
        let now = SystemTime::now();

        fail_point!("before-timeline-gc");

        let _layer_removal_cs = self.layer_removal_cs.lock().unwrap();

        let gc_info = self.gc_info.read().unwrap();

        let horizon_cutoff = min(gc_info.horizon_cutoff, self.get_disk_consistent_lsn());
        let pitr_cutoff = gc_info.pitr_cutoff;
        let retain_lsns = &gc_info.retain_lsns;

        let new_gc_cutoff = Lsn::min(horizon_cutoff, pitr_cutoff);

        let _enter =
            info_span!("gc_timeline", timeline = %self.timeline_id, cutoff = %new_gc_cutoff)
                .entered();

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
        // Persist the new GC cutoff value in the metadata file, before
        // we actually remove anything.
        self.update_metadata_file(self.disk_consistent_lsn.load(), HashMap::new())?;

        info!("GC starting");

        debug!("retain_lsns: {:?}", retain_lsns);

        let mut layers_to_remove = Vec::new();

        // Scan all on-disk layers in the timeline.
        //
        // Garbage collect the layer if all conditions are satisfied:
        // 1. it is older than cutoff LSN;
        // 2. it is older than PITR interval;
        // 3. it doesn't need to be retained for 'retain_lsns';
        // 4. newer on-disk image layers cover the layer's whole key range
        //
        let mut layers = self.layers.write().unwrap();
        'outer: for l in layers.iter_historic_layers() {
            // This layer is in the process of being flushed to disk.
            // It will be swapped out of the layer map, replaced with
            // on-disk layers containing the same data.
            // We can't GC it, as it's not on disk. We can't remove it
            // from the layer map yet, as it would make its data
            // inaccessible.
            if l.is_in_memory() {
                continue;
            }

            result.layers_total += 1;

            // 1. Is it newer than GC horizon cutoff point?
            if l.get_lsn_range().end > horizon_cutoff {
                debug!(
                    "keeping {} because it's newer than horizon_cutoff {}",
                    l.filename().display(),
                    horizon_cutoff
                );
                result.layers_needed_by_cutoff += 1;
                continue 'outer;
            }

            // 2. It is newer than PiTR cutoff point?
            if l.get_lsn_range().end > pitr_cutoff {
                debug!(
                    "keeping {} because it's newer than pitr_cutoff {}",
                    l.filename().display(),
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
            for retain_lsn in retain_lsns {
                // start_lsn is inclusive
                if &l.get_lsn_range().start <= retain_lsn {
                    debug!(
                        "keeping {} because it's still might be referenced by child branch forked at {} is_dropped: xx is_incremental: {}",
                        l.filename().display(),
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
                    l.filename().display()
                );
                result.layers_not_updated += 1;
                continue 'outer;
            }

            // We didn't find any reason to keep this file, so remove it.
            debug!(
                "garbage collecting {} is_dropped: xx is_incremental: {}",
                l.filename().display(),
                l.is_incremental(),
            );
            layers_to_remove.push(Arc::clone(&l));
        }

        // Actually delete the layers from disk and remove them from the map.
        // (couldn't do this in the loop above, because you cannot modify a collection
        // while iterating it. BTreeMap::retain() would be another option)
        let mut layer_paths_to_delete = HashSet::with_capacity(layers_to_remove.len());
        for doomed_layer in layers_to_remove {
            if let Some(path) = doomed_layer.local_path() {
                self.metrics
                    .current_physical_size_gauge
                    .sub(path.metadata()?.len());
                layer_paths_to_delete.insert(path);
            }
            doomed_layer.delete()?;
            layers.remove_historic(doomed_layer);
            result.layers_removed += 1;
        }

        info!(
            "GC completed removing {} layers, cutoff {}",
            result.layers_removed, new_gc_cutoff
        );

        if result.layers_removed != 0 {
            fail_point!("after-timeline-gc-removed-layers");
        }

        if self.can_upload_layers() {
            storage_sync::schedule_layer_delete(
                self.tenant_id,
                self.timeline_id,
                layer_paths_to_delete,
            );
        }

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
    ) -> anyhow::Result<Bytes> {
        // Perform WAL redo if needed
        data.records.reverse();

        // If we have a page image, and no WAL, we're all set
        if data.records.is_empty() {
            if let Some((img_lsn, img)) = &data.img {
                trace!(
                    "found page image for key {} at {}, no WAL redo required",
                    key,
                    img_lsn
                );
                Ok(img.clone())
            } else {
                bail!("base image for {} at {} not found", key, request_lsn);
            }
        } else {
            // We need to do WAL redo.
            //
            // If we don't have a base image, then the oldest WAL record better initialize
            // the page
            if data.img.is_none() && !data.records.first().unwrap().1.will_init() {
                bail!(
                    "Base image for {} at {} not found, but got {} WAL records",
                    key,
                    request_lsn,
                    data.records.len()
                );
            } else {
                let base_img = if let Some((_lsn, img)) = data.img {
                    trace!(
                        "found {} WAL records and a base image for {} at {}, performing WAL redo",
                        data.records.len(),
                        key,
                        request_lsn
                    );
                    Some(img)
                } else {
                    trace!("found {} WAL records that will init the page for {} at {}, performing WAL redo", data.records.len(), key, request_lsn);
                    None
                };

                let last_rec_lsn = data.records.last().unwrap().0;

                let img = self.walredo_mgr.request_redo(
                    key,
                    request_lsn,
                    base_img,
                    data.records,
                    self.pg_version,
                )?;

                if img.len() == page_cache::PAGE_SZ {
                    let cache = page_cache::get();
                    cache
                        .memorize_materialized_page(
                            self.tenant_id,
                            self.timeline_id,
                            key,
                            last_rec_lsn,
                            &img,
                        )
                        .context("Materialized page memoization failed")?;
                }

                Ok(img)
            }
        }
    }

    fn can_upload_layers(&self) -> bool {
        self.upload_layers.load(atomic::Ordering::Relaxed)
            && self.current_state() != TimelineState::Broken
    }
}

/// Helper function for get_reconstruct_data() to add the path of layers traversed
/// to an error, as anyhow context information.
fn layer_traversal_error(
    msg: String,
    path: Vec<(ValueReconstructResult, Lsn, Arc<dyn Layer>)>,
) -> anyhow::Result<()> {
    // We want the original 'msg' to be the outermost context. The outermost context
    // is the most high-level information, which also gets propagated to the client.
    let mut msg_iter = path
        .iter()
        .map(|(r, c, l)| {
            format!(
                "layer traversal: result {:?}, cont_lsn {}, layer: {}",
                r,
                c,
                l.filename().display()
            )
        })
        .chain(std::iter::once(msg));
    // Construct initial message from the first traversed layer
    let err = anyhow!(msg_iter.next().unwrap());

    // Append all subsequent traversals, and the error message 'msg', as contexts.
    Err(msg_iter.fold(err, |err, msg| err.context(msg)))
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
        page_image_cache::remove(key, self.tenant_id, self.timeline_id);
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

    pub fn update_current_logical_size(&self, delta: i64) {
        self.tl.update_current_logical_size(delta)
    }
}

/// Add a suffix to a layer file's name: .{num}.old
/// Uses the first available num (starts at 0)
fn rename_to_backup(path: PathBuf) -> anyhow::Result<()> {
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow!("Path {} don't have a file name", path.display()))?
        .to_string_lossy();
    let mut new_path = path.clone();

    for i in 0u32.. {
        new_path.set_file_name(format!("{}.{}.old", filename, i));
        if !new_path.exists() {
            std::fs::rename(&path, &new_path)?;
            return Ok(());
        }
    }

    bail!("couldn't find an unused backup number for {:?}", path)
}
