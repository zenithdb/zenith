//! New compaction implementation. The algorithm itself is implemented in the
//! compaction crate. This file implements the callbacks and structs that allow
//! the algorithm to drive the process.
//!
//! The old legacy algorithm is implemented directly in `timeline.rs`.

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::ops::{Deref, Range};
use std::sync::Arc;

use super::layer_manager::LayerManager;
use super::{
    CompactFlags, CompactOptions, CreateImageLayersError, DurationRecorder, ImageLayerCreationMode,
    LastImageLayerCreationStatus, RecordedDuration, Timeline,
};

use anyhow::{anyhow, bail, Context};
use bytes::Bytes;
use enumset::EnumSet;
use fail::fail_point;
use itertools::Itertools;
use pageserver_api::key::KEY_SIZE;
use pageserver_api::keyspace::ShardedRange;
use pageserver_api::models::CompactInfoResponse;
use pageserver_api::shard::{ShardCount, ShardIdentity, TenantShardId};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, info_span, trace, warn, Instrument};
use utils::id::TimelineId;

use crate::context::{AccessStatsBehavior, RequestContext, RequestContextBuilder};
use crate::page_cache;
use crate::statvfs::Statvfs;
use crate::tenant::checks::check_valid_layermap;
use crate::tenant::gc_block::GcBlock;
use crate::tenant::remote_timeline_client::WaitCompletionError;
use crate::tenant::storage_layer::batch_split_writer::{
    BatchWriterResult, SplitDeltaLayerWriter, SplitImageLayerWriter,
};
use crate::tenant::storage_layer::filter_iterator::FilterIterator;
use crate::tenant::storage_layer::merge_iterator::MergeIterator;
use crate::tenant::storage_layer::{
    AsLayerDesc, PersistentLayerDesc, PersistentLayerKey, ValueReconstructState,
};
use crate::tenant::timeline::{drop_rlock, DeltaLayerWriter, ImageLayerWriter};
use crate::tenant::timeline::{ImageLayerCreationOutcome, IoConcurrency};
use crate::tenant::timeline::{Layer, ResidentLayer};
use crate::tenant::{gc_block, DeltaLayer, MaybeOffloaded};
use crate::virtual_file::{MaybeFatalIo, VirtualFile};
use pageserver_api::config::tenant_conf_defaults::DEFAULT_CHECKPOINT_DISTANCE;

use pageserver_api::key::Key;
use pageserver_api::keyspace::KeySpace;
use pageserver_api::record::NeonWalRecord;
use pageserver_api::value::Value;

use utils::lsn::Lsn;

use pageserver_compaction::helpers::{fully_contains, overlaps_with};
use pageserver_compaction::interface::*;

use super::CompactionError;

/// Maximum number of deltas before generating an image layer in bottom-most compaction.
const COMPACTION_DELTA_THRESHOLD: usize = 5;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct GcCompactionJobId(pub usize);

impl std::fmt::Display for GcCompactionJobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum GcCompactionQueueItem {
    Manual(CompactOptions),
    SubCompactionJob(CompactOptions),
    #[allow(dead_code)]
    UpdateL2Lsn(Lsn),
    Notify(GcCompactionJobId),
}

impl GcCompactionQueueItem {
    pub fn into_compact_info_resp(
        self,
        id: GcCompactionJobId,
        running: bool,
    ) -> Option<CompactInfoResponse> {
        match self {
            GcCompactionQueueItem::Manual(options) => Some(CompactInfoResponse {
                compact_key_range: options.compact_key_range,
                compact_lsn_range: options.compact_lsn_range,
                sub_compaction: options.sub_compaction,
                running,
                job_id: id.0,
            }),
            GcCompactionQueueItem::SubCompactionJob(options) => Some(CompactInfoResponse {
                compact_key_range: options.compact_key_range,
                compact_lsn_range: options.compact_lsn_range,
                sub_compaction: options.sub_compaction,
                running,
                job_id: id.0,
            }),
            GcCompactionQueueItem::UpdateL2Lsn(_) => None,
            GcCompactionQueueItem::Notify(_) => None,
        }
    }
}

struct GcCompactionQueueInner {
    running: Option<(GcCompactionJobId, GcCompactionQueueItem)>,
    queued: VecDeque<(GcCompactionJobId, GcCompactionQueueItem)>,
    notify: HashMap<GcCompactionJobId, tokio::sync::oneshot::Sender<()>>,
    gc_guards: HashMap<GcCompactionJobId, gc_block::Guard>,
    last_id: GcCompactionJobId,
}

impl GcCompactionQueueInner {
    fn next_id(&mut self) -> GcCompactionJobId {
        let id = self.last_id;
        self.last_id = GcCompactionJobId(id.0 + 1);
        id
    }
}

/// A structure to store gc_compaction jobs.
pub struct GcCompactionQueue {
    /// All items in the queue, and the currently-running job.
    inner: std::sync::Mutex<GcCompactionQueueInner>,
    /// Ensure only one thread is consuming the queue.
    consumer_lock: tokio::sync::Mutex<()>,
}

impl GcCompactionQueue {
    pub fn new() -> Self {
        GcCompactionQueue {
            inner: std::sync::Mutex::new(GcCompactionQueueInner {
                running: None,
                queued: VecDeque::new(),
                notify: HashMap::new(),
                gc_guards: HashMap::new(),
                last_id: GcCompactionJobId(0),
            }),
            consumer_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn cancel_scheduled(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.queued.clear();
        guard.notify.clear();
        guard.gc_guards.clear();
    }

    /// Schedule a manual compaction job.
    pub fn schedule_manual_compaction(
        &self,
        options: CompactOptions,
        notify: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> GcCompactionJobId {
        let mut guard = self.inner.lock().unwrap();
        let id = guard.next_id();
        guard
            .queued
            .push_back((id, GcCompactionQueueItem::Manual(options)));
        if let Some(notify) = notify {
            guard.notify.insert(id, notify);
        }
        info!("scheduled compaction job id={}", id);
        id
    }

    /// Trigger an auto compaction.
    #[allow(dead_code)]
    pub fn trigger_auto_compaction(&self, _: &Arc<Timeline>) {}

    /// Notify the caller the job has finished and unblock GC.
    fn notify_and_unblock(&self, id: GcCompactionJobId) {
        info!("compaction job id={} finished", id);
        let mut guard = self.inner.lock().unwrap();
        if let Some(blocking) = guard.gc_guards.remove(&id) {
            drop(blocking)
        }
        if let Some(tx) = guard.notify.remove(&id) {
            let _ = tx.send(());
        }
    }

    async fn handle_sub_compaction(
        &self,
        id: GcCompactionJobId,
        options: CompactOptions,
        timeline: &Arc<Timeline>,
        gc_block: &GcBlock,
    ) -> Result<(), CompactionError> {
        info!("running scheduled enhanced gc bottom-most compaction with sub-compaction, splitting compaction jobs");
        let jobs: Vec<GcCompactJob> = timeline
            .gc_compaction_split_jobs(
                GcCompactJob::from_compact_options(options.clone()),
                options.sub_compaction_max_job_size_mb,
            )
            .await
            .map_err(CompactionError::Other)?;
        if jobs.is_empty() {
            info!("no jobs to run, skipping scheduled compaction task");
            self.notify_and_unblock(id);
        } else {
            let gc_guard = match gc_block.start().await {
                Ok(guard) => guard,
                Err(e) => {
                    return Err(CompactionError::Other(anyhow!(
                        "cannot run gc-compaction because gc is blocked: {}",
                        e
                    )));
                }
            };

            let jobs_len = jobs.len();
            let mut pending_tasks = Vec::new();
            for job in jobs {
                // Unfortunately we need to convert the `GcCompactJob` back to `CompactionOptions`
                // until we do further refactors to allow directly call `compact_with_gc`.
                let mut flags: EnumSet<CompactFlags> = EnumSet::default();
                flags |= CompactFlags::EnhancedGcBottomMostCompaction;
                if job.dry_run {
                    flags |= CompactFlags::DryRun;
                }
                let options = CompactOptions {
                    flags,
                    sub_compaction: false,
                    compact_key_range: Some(job.compact_key_range.into()),
                    compact_lsn_range: Some(job.compact_lsn_range.into()),
                    sub_compaction_max_job_size_mb: None,
                };
                pending_tasks.push(GcCompactionQueueItem::SubCompactionJob(options));
            }
            pending_tasks.push(GcCompactionQueueItem::Notify(id));
            {
                let mut guard = self.inner.lock().unwrap();
                guard.gc_guards.insert(id, gc_guard);
                let mut tasks = Vec::new();
                for task in pending_tasks {
                    let id = guard.next_id();
                    tasks.push((id, task));
                }
                tasks.reverse();
                for item in tasks {
                    guard.queued.push_front(item);
                }
            }
            info!("scheduled enhanced gc bottom-most compaction with sub-compaction, split into {} jobs", jobs_len);
        }
        Ok(())
    }

    /// Take a job from the queue and process it. Returns if there are still pending tasks.
    pub async fn iteration(
        &self,
        cancel: &CancellationToken,
        ctx: &RequestContext,
        gc_block: &GcBlock,
        timeline: &Arc<Timeline>,
    ) -> Result<bool, CompactionError> {
        let _one_op_at_a_time_guard = self.consumer_lock.lock().await;
        let has_pending_tasks;
        let (id, item) = {
            let mut guard = self.inner.lock().unwrap();
            let Some((id, item)) = guard.queued.pop_front() else {
                return Ok(false);
            };
            guard.running = Some((id, item.clone()));
            has_pending_tasks = !guard.queued.is_empty();
            (id, item)
        };

        match item {
            GcCompactionQueueItem::Manual(options) => {
                if !options
                    .flags
                    .contains(CompactFlags::EnhancedGcBottomMostCompaction)
                {
                    warn!("ignoring scheduled compaction task: scheduled task must be gc compaction: {:?}", options);
                } else if options.sub_compaction {
                    self.handle_sub_compaction(id, options, timeline, gc_block)
                        .await?;
                } else {
                    let gc_guard = match gc_block.start().await {
                        Ok(guard) => guard,
                        Err(e) => {
                            return Err(CompactionError::Other(anyhow!(
                                "cannot run gc-compaction because gc is blocked: {}",
                                e
                            )));
                        }
                    };
                    {
                        let mut guard = self.inner.lock().unwrap();
                        guard.gc_guards.insert(id, gc_guard);
                    }
                    let _ = timeline
                        .compact_with_options(cancel, options, ctx)
                        .instrument(info_span!("scheduled_compact_timeline", %timeline.timeline_id))
                        .await?;
                    self.notify_and_unblock(id);
                }
            }
            GcCompactionQueueItem::SubCompactionJob(options) => {
                let _ = timeline
                    .compact_with_options(cancel, options, ctx)
                    .instrument(info_span!("scheduled_compact_timeline", %timeline.timeline_id))
                    .await?;
            }
            GcCompactionQueueItem::Notify(id) => {
                self.notify_and_unblock(id);
            }
            GcCompactionQueueItem::UpdateL2Lsn(_) => {
                unreachable!()
            }
        }
        {
            let mut guard = self.inner.lock().unwrap();
            guard.running = None;
        }
        Ok(has_pending_tasks)
    }

    #[allow(clippy::type_complexity)]
    pub fn remaining_jobs(
        &self,
    ) -> (
        Option<(GcCompactionJobId, GcCompactionQueueItem)>,
        VecDeque<(GcCompactionJobId, GcCompactionQueueItem)>,
    ) {
        let guard = self.inner.lock().unwrap();
        (guard.running.clone(), guard.queued.clone())
    }

    #[allow(dead_code)]
    pub fn remaining_jobs_num(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.queued.len() + if guard.running.is_some() { 1 } else { 0 }
    }
}

/// A job description for the gc-compaction job. This structure describes the rectangle range that the job will
/// process. The exact layers that need to be compacted/rewritten will be generated when `compact_with_gc` gets
/// called.
#[derive(Debug, Clone)]
pub(crate) struct GcCompactJob {
    pub dry_run: bool,
    /// The key range to be compacted. The compaction algorithm will only regenerate key-value pairs within this range
    /// [left inclusive, right exclusive), and other pairs will be rewritten into new files if necessary.
    pub compact_key_range: Range<Key>,
    /// The LSN range to be compacted. The compaction algorithm will use this range to determine the layers to be
    /// selected for the compaction, and it does not guarantee the generated layers will have exactly the same LSN range
    /// as specified here. The true range being compacted is `min_lsn/max_lsn` in [`GcCompactionJobDescription`].
    /// min_lsn will always <= the lower bound specified here, and max_lsn will always >= the upper bound specified here.
    pub compact_lsn_range: Range<Lsn>,
}

impl GcCompactJob {
    pub fn from_compact_options(options: CompactOptions) -> Self {
        GcCompactJob {
            dry_run: options.flags.contains(CompactFlags::DryRun),
            compact_key_range: options
                .compact_key_range
                .map(|x| x.into())
                .unwrap_or(Key::MIN..Key::MAX),
            compact_lsn_range: options
                .compact_lsn_range
                .map(|x| x.into())
                .unwrap_or(Lsn::INVALID..Lsn::MAX),
        }
    }
}

/// A job description for the gc-compaction job. This structure is generated when `compact_with_gc` is called
/// and contains the exact layers we want to compact.
pub struct GcCompactionJobDescription {
    /// All layers to read in the compaction job
    selected_layers: Vec<Layer>,
    /// GC cutoff of the job. This is the lowest LSN that will be accessed by the read/GC path and we need to
    /// keep all deltas <= this LSN or generate an image == this LSN.
    gc_cutoff: Lsn,
    /// LSNs to retain for the job. Read path will use this LSN so we need to keep deltas <= this LSN or
    /// generate an image == this LSN.
    retain_lsns_below_horizon: Vec<Lsn>,
    /// Maximum layer LSN processed in this compaction, that is max(end_lsn of layers). Exclusive. All data
    /// \>= this LSN will be kept and will not be rewritten.
    max_layer_lsn: Lsn,
    /// Minimum layer LSN processed in this compaction, that is min(start_lsn of layers). Inclusive.
    /// All access below (strict lower than `<`) this LSN will be routed through the normal read path instead of
    /// k-merge within gc-compaction.
    min_layer_lsn: Lsn,
    /// Only compact layers overlapping with this range.
    compaction_key_range: Range<Key>,
    /// When partial compaction is enabled, these layers need to be rewritten to ensure no overlap.
    /// This field is here solely for debugging. The field will not be read once the compaction
    /// description is generated.
    rewrite_layers: Vec<Arc<PersistentLayerDesc>>,
}

/// The result of bottom-most compaction for a single key at each LSN.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct KeyLogAtLsn(pub Vec<(Lsn, Value)>);

/// The result of bottom-most compaction.
#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct KeyHistoryRetention {
    /// Stores logs to reconstruct the value at the given LSN, that is to say, logs <= LSN or image == LSN.
    pub(crate) below_horizon: Vec<(Lsn, KeyLogAtLsn)>,
    /// Stores logs to reconstruct the value at any LSN above the horizon, that is to say, log > LSN.
    pub(crate) above_horizon: KeyLogAtLsn,
}

impl KeyHistoryRetention {
    /// Hack: skip delta layer if we need to produce a layer of a same key-lsn.
    ///
    /// This can happen if we have removed some deltas in "the middle" of some existing layer's key-lsn-range.
    /// For example, consider the case where a single delta with range [0x10,0x50) exists.
    /// And we have branches at LSN 0x10, 0x20, 0x30.
    /// Then we delete branch @ 0x20.
    /// Bottom-most compaction may now delete the delta [0x20,0x30).
    /// And that wouldnt' change the shape of the layer.
    ///
    /// Note that bottom-most-gc-compaction never _adds_ new data in that case, only removes.
    ///
    /// `discard_key` will only be called when the writer reaches its target (instead of for every key), so it's fine to grab a lock inside.
    async fn discard_key(key: &PersistentLayerKey, tline: &Arc<Timeline>, dry_run: bool) -> bool {
        if dry_run {
            return true;
        }
        let layer_generation;
        {
            let guard = tline.layers.read().await;
            if !guard.contains_key(key) {
                return false;
            }
            layer_generation = guard.get_from_key(key).metadata().generation;
        }
        if layer_generation == tline.generation {
            info!(
                key=%key,
                ?layer_generation,
                "discard layer due to duplicated layer key in the same generation",
            );
            true
        } else {
            false
        }
    }

    /// Pipe a history of a single key to the writers.
    ///
    /// If `image_writer` is none, the images will be placed into the delta layers.
    /// The delta writer will contain all images and deltas (below and above the horizon) except the bottom-most images.
    #[allow(clippy::too_many_arguments)]
    async fn pipe_to(
        self,
        key: Key,
        delta_writer: &mut SplitDeltaLayerWriter,
        mut image_writer: Option<&mut SplitImageLayerWriter>,
        stat: &mut CompactionStatistics,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let mut first_batch = true;
        for (cutoff_lsn, KeyLogAtLsn(logs)) in self.below_horizon {
            if first_batch {
                if logs.len() == 1 && logs[0].1.is_image() {
                    let Value::Image(img) = &logs[0].1 else {
                        unreachable!()
                    };
                    stat.produce_image_key(img);
                    if let Some(image_writer) = image_writer.as_mut() {
                        image_writer.put_image(key, img.clone(), ctx).await?;
                    } else {
                        delta_writer
                            .put_value(key, cutoff_lsn, Value::Image(img.clone()), ctx)
                            .await?;
                    }
                } else {
                    for (lsn, val) in logs {
                        stat.produce_key(&val);
                        delta_writer.put_value(key, lsn, val, ctx).await?;
                    }
                }
                first_batch = false;
            } else {
                for (lsn, val) in logs {
                    stat.produce_key(&val);
                    delta_writer.put_value(key, lsn, val, ctx).await?;
                }
            }
        }
        let KeyLogAtLsn(above_horizon_logs) = self.above_horizon;
        for (lsn, val) in above_horizon_logs {
            stat.produce_key(&val);
            delta_writer.put_value(key, lsn, val, ctx).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Default)]
struct CompactionStatisticsNumSize {
    num: u64,
    size: u64,
}

#[derive(Debug, Serialize, Default)]
pub struct CompactionStatistics {
    delta_layer_visited: CompactionStatisticsNumSize,
    image_layer_visited: CompactionStatisticsNumSize,
    delta_layer_produced: CompactionStatisticsNumSize,
    image_layer_produced: CompactionStatisticsNumSize,
    num_delta_layer_discarded: usize,
    num_image_layer_discarded: usize,
    num_unique_keys_visited: usize,
    wal_keys_visited: CompactionStatisticsNumSize,
    image_keys_visited: CompactionStatisticsNumSize,
    wal_produced: CompactionStatisticsNumSize,
    image_produced: CompactionStatisticsNumSize,
}

impl CompactionStatistics {
    fn estimated_size_of_value(val: &Value) -> usize {
        match val {
            Value::Image(img) => img.len(),
            Value::WalRecord(NeonWalRecord::Postgres { rec, .. }) => rec.len(),
            _ => std::mem::size_of::<NeonWalRecord>(),
        }
    }
    fn estimated_size_of_key() -> usize {
        KEY_SIZE // TODO: distinguish image layer and delta layer (count LSN in delta layer)
    }
    fn visit_delta_layer(&mut self, size: u64) {
        self.delta_layer_visited.num += 1;
        self.delta_layer_visited.size += size;
    }
    fn visit_image_layer(&mut self, size: u64) {
        self.image_layer_visited.num += 1;
        self.image_layer_visited.size += size;
    }
    fn on_unique_key_visited(&mut self) {
        self.num_unique_keys_visited += 1;
    }
    fn visit_wal_key(&mut self, val: &Value) {
        self.wal_keys_visited.num += 1;
        self.wal_keys_visited.size +=
            Self::estimated_size_of_value(val) as u64 + Self::estimated_size_of_key() as u64;
    }
    fn visit_image_key(&mut self, val: &Value) {
        self.image_keys_visited.num += 1;
        self.image_keys_visited.size +=
            Self::estimated_size_of_value(val) as u64 + Self::estimated_size_of_key() as u64;
    }
    fn produce_key(&mut self, val: &Value) {
        match val {
            Value::Image(img) => self.produce_image_key(img),
            Value::WalRecord(_) => self.produce_wal_key(val),
        }
    }
    fn produce_wal_key(&mut self, val: &Value) {
        self.wal_produced.num += 1;
        self.wal_produced.size +=
            Self::estimated_size_of_value(val) as u64 + Self::estimated_size_of_key() as u64;
    }
    fn produce_image_key(&mut self, val: &Bytes) {
        self.image_produced.num += 1;
        self.image_produced.size += val.len() as u64 + Self::estimated_size_of_key() as u64;
    }
    fn discard_delta_layer(&mut self) {
        self.num_delta_layer_discarded += 1;
    }
    fn discard_image_layer(&mut self) {
        self.num_image_layer_discarded += 1;
    }
    fn produce_delta_layer(&mut self, size: u64) {
        self.delta_layer_produced.num += 1;
        self.delta_layer_produced.size += size;
    }
    fn produce_image_layer(&mut self, size: u64) {
        self.image_layer_produced.num += 1;
        self.image_layer_produced.size += size;
    }
}

impl Timeline {
    /// TODO: cancellation
    ///
    /// Returns whether the compaction has pending tasks.
    pub(crate) async fn compact_legacy(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        options: CompactOptions,
        ctx: &RequestContext,
    ) -> Result<bool, CompactionError> {
        if options
            .flags
            .contains(CompactFlags::EnhancedGcBottomMostCompaction)
        {
            self.compact_with_gc(cancel, options, ctx)
                .await
                .map_err(CompactionError::Other)?;
            return Ok(false);
        }

        if options.flags.contains(CompactFlags::DryRun) {
            return Err(CompactionError::Other(anyhow!(
                "dry-run mode is not supported for legacy compaction for now"
            )));
        }

        if options.compact_key_range.is_some() || options.compact_lsn_range.is_some() {
            // maybe useful in the future? could implement this at some point
            return Err(CompactionError::Other(anyhow!(
                "compaction range is not supported for legacy compaction for now"
            )));
        }

        // High level strategy for compaction / image creation:
        //
        // 1. First, do a L0 compaction to ensure we move the L0
        // layers into the historic layer map get flat levels of
        // layers. If we did not compact all L0 layers, we will
        // prioritize compacting the timeline again and not do
        // any of the compactions below.
        //
        // 2. Then, calculate the desired "partitioning" of the
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
        // 3. Once we know the partitioning, for each partition,
        // decide if it's time to create a new image layer. The
        // criteria is: there has been too much "churn" since the last
        // image layer? The "churn" is fuzzy concept, it's a
        // combination of too many delta files, or too much WAL in
        // total in the delta file. Or perhaps: if creating an image
        // file would allow to delete some older files.
        //
        // 4. In the end, if the tenant gets auto-sharded, we will run
        // a shard-ancestor compaction.

        // Is the timeline being deleted?
        if self.is_stopping() {
            trace!("Dropping out of compaction on timeline shutdown");
            return Err(CompactionError::ShuttingDown);
        }

        let target_file_size = self.get_checkpoint_distance();

        // Define partitioning schema if needed

        // 1. L0 Compact
        let fully_compacted = {
            let timer = self.metrics.compact_time_histo.start_timer();
            let fully_compacted = self
                .compact_level0(
                    target_file_size,
                    options.flags.contains(CompactFlags::ForceL0Compaction),
                    ctx,
                )
                .await?;
            timer.stop_and_record();
            fully_compacted
        };

        if !fully_compacted {
            // Yield and do not do any other kind of compaction. True means
            // that we have pending L0 compaction tasks and the compaction scheduler
            // will prioritize compacting this tenant/timeline again.
            info!("skipping image layer generation and shard ancestor compaction due to L0 compaction did not include all layers.");
            return Ok(true);
        }

        // 2. Repartition and create image layers if necessary
        let partition_count = match self
            .repartition(
                self.get_last_record_lsn(), // TODO: use L0-L1 boundary
                self.get_compaction_target_size(),
                options.flags,
                ctx,
            )
            .await
        {
            Ok(((dense_partitioning, sparse_partitioning), lsn)) => {
                // Disables access_stats updates, so that the files we read remain candidates for eviction after we're done with them
                let image_ctx = RequestContextBuilder::extend(ctx)
                    .access_stats_behavior(AccessStatsBehavior::Skip)
                    .build();

                let mut partitioning = dense_partitioning;
                partitioning
                    .parts
                    .extend(sparse_partitioning.into_dense().parts);

                // 3. Create new image layers for partitions that have been modified "enough".
                let (image_layers, is_complete) = self
                    .create_image_layers(
                        &partitioning,
                        lsn,
                        if options
                            .flags
                            .contains(CompactFlags::ForceImageLayerCreation)
                        {
                            ImageLayerCreationMode::Force
                        } else {
                            ImageLayerCreationMode::Try
                        },
                        &image_ctx,
                        self.last_image_layer_creation_status
                            .load()
                            .as_ref()
                            .clone(),
                    )
                    .await?;

                self.last_image_layer_creation_status
                    .store(Arc::new(is_complete.clone()));

                self.upload_new_image_layers(image_layers)?;
                if let LastImageLayerCreationStatus::Incomplete = is_complete {
                    // Yield and do not do any other kind of compaction.
                    info!("skipping shard ancestor compaction due to pending image layer generation tasks (preempted by L0 compaction).");
                    return Ok(true);
                }
                partitioning.parts.len()
            }
            Err(err) => {
                // no partitioning? This is normal, if the timeline was just created
                // as an empty timeline. Also in unit tests, when we use the timeline
                // as a simple key-value store, ignoring the datadir layout. Log the
                // error but continue.
                //
                // Suppress error when it's due to cancellation
                if !self.cancel.is_cancelled() && !err.is_cancelled() {
                    tracing::error!("could not compact, repartitioning keyspace failed: {err:?}");
                }
                1
            }
        };

        // 4. Shard ancestor compaction

        if self.shard_identity.count >= ShardCount::new(2) {
            // Limit the number of layer rewrites to the number of partitions: this means its
            // runtime should be comparable to a full round of image layer creations, rather than
            // being potentially much longer.
            let rewrite_max = partition_count;

            self.compact_shard_ancestors(rewrite_max, ctx).await?;
        }

        Ok(false)
    }

    /// Check for layers that are elegible to be rewritten:
    /// - Shard splitting: After a shard split, ancestor layers beyond pitr_interval, so that
    ///   we don't indefinitely retain keys in this shard that aren't needed.
    /// - For future use: layers beyond pitr_interval that are in formats we would
    ///   rather not maintain compatibility with indefinitely.
    ///
    /// Note: this phase may read and write many gigabytes of data: use rewrite_max to bound
    /// how much work it will try to do in each compaction pass.
    async fn compact_shard_ancestors(
        self: &Arc<Self>,
        rewrite_max: usize,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let mut drop_layers = Vec::new();
        let mut layers_to_rewrite: Vec<Layer> = Vec::new();

        // We will use the Lsn cutoff of the last GC as a threshold for rewriting layers: if a
        // layer is behind this Lsn, it indicates that the layer is being retained beyond the
        // pitr_interval, for example because a branchpoint references it.
        //
        // Holding this read guard also blocks [`Self::gc_timeline`] from entering while we
        // are rewriting layers.
        let latest_gc_cutoff = self.get_latest_gc_cutoff_lsn();

        tracing::info!(
            "latest_gc_cutoff: {}, pitr cutoff {}",
            *latest_gc_cutoff,
            self.gc_info.read().unwrap().cutoffs.time
        );

        let layers = self.layers.read().await;
        for layer_desc in layers.layer_map()?.iter_historic_layers() {
            let layer = layers.get_from_desc(&layer_desc);
            if layer.metadata().shard.shard_count == self.shard_identity.count {
                // This layer does not belong to a historic ancestor, no need to re-image it.
                continue;
            }

            // This layer was created on an ancestor shard: check if it contains any data for this shard.
            let sharded_range = ShardedRange::new(layer_desc.get_key_range(), &self.shard_identity);
            let layer_local_page_count = sharded_range.page_count();
            let layer_raw_page_count = ShardedRange::raw_size(&layer_desc.get_key_range());
            if layer_local_page_count == 0 {
                // This ancestral layer only covers keys that belong to other shards.
                // We include the full metadata in the log: if we had some critical bug that caused
                // us to incorrectly drop layers, this would simplify manually debugging + reinstating those layers.
                info!(%layer, old_metadata=?layer.metadata(),
                    "dropping layer after shard split, contains no keys for this shard.",
                );

                if cfg!(debug_assertions) {
                    // Expensive, exhaustive check of keys in this layer: this guards against ShardedRange's calculations being
                    // wrong.  If ShardedRange claims the local page count is zero, then no keys in this layer
                    // should be !is_key_disposable()
                    let range = layer_desc.get_key_range();
                    let mut key = range.start;
                    while key < range.end {
                        debug_assert!(self.shard_identity.is_key_disposable(&key));
                        key = key.next();
                    }
                }

                drop_layers.push(layer);
                continue;
            } else if layer_local_page_count != u32::MAX
                && layer_local_page_count == layer_raw_page_count
            {
                debug!(%layer,
                    "layer is entirely shard local ({} keys), no need to filter it",
                    layer_local_page_count
                );
                continue;
            }

            // Don't bother re-writing a layer unless it will at least halve its size
            if layer_local_page_count != u32::MAX
                && layer_local_page_count > layer_raw_page_count / 2
            {
                debug!(%layer,
                    "layer is already mostly local ({}/{}), not rewriting",
                    layer_local_page_count,
                    layer_raw_page_count
                );
            }

            // Don't bother re-writing a layer if it is within the PITR window: it will age-out eventually
            // without incurring the I/O cost of a rewrite.
            if layer_desc.get_lsn_range().end >= *latest_gc_cutoff {
                debug!(%layer, "Skipping rewrite of layer still in GC window ({} >= {})",
                    layer_desc.get_lsn_range().end, *latest_gc_cutoff);
                continue;
            }

            if layer_desc.is_delta() {
                // We do not yet implement rewrite of delta layers
                debug!(%layer, "Skipping rewrite of delta layer");
                continue;
            }

            // Only rewrite layers if their generations differ.  This guarantees:
            //  - that local rewrite is safe, as local layer paths will differ between existing layer and rewritten one
            //  - that the layer is persistent in remote storage, as we only see old-generation'd layer via loading from remote storage
            if layer.metadata().generation == self.generation {
                debug!(%layer, "Skipping rewrite, is not from old generation");
                continue;
            }

            if layers_to_rewrite.len() >= rewrite_max {
                tracing::info!(%layer, "Will rewrite layer on a future compaction, already rewrote {}",
                    layers_to_rewrite.len()
                );
                continue;
            }

            // Fall through: all our conditions for doing a rewrite passed.
            layers_to_rewrite.push(layer);
        }

        // Drop read lock on layer map before we start doing time-consuming I/O
        drop(layers);

        let mut replace_image_layers = Vec::new();

        for layer in layers_to_rewrite {
            tracing::info!(layer=%layer, "Rewriting layer after shard split...");
            let mut image_layer_writer = ImageLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &layer.layer_desc().key_range,
                layer.layer_desc().image_layer_lsn(),
                ctx,
            )
            .await
            .map_err(CompactionError::Other)?;

            // Safety of layer rewrites:
            // - We are writing to a different local file path than we are reading from, so the old Layer
            //   cannot interfere with the new one.
            // - In the page cache, contents for a particular VirtualFile are stored with a file_id that
            //   is different for two layers with the same name (in `ImageLayerInner::new` we always
            //   acquire a fresh id from [`crate::page_cache::next_file_id`].  So readers do not risk
            //   reading the index from one layer file, and then data blocks from the rewritten layer file.
            // - Any readers that have a reference to the old layer will keep it alive until they are done
            //   with it. If they are trying to promote from remote storage, that will fail, but this is the same
            //   as for compaction generally: compaction is allowed to delete layers that readers might be trying to use.
            // - We do not run concurrently with other kinds of compaction, so the only layer map writes we race with are:
            //    - GC, which at worst witnesses us "undelete" a layer that they just deleted.
            //    - ingestion, which only inserts layers, therefore cannot collide with us.
            let resident = layer.download_and_keep_resident().await?;

            let keys_written = resident
                .filter(&self.shard_identity, &mut image_layer_writer, ctx)
                .await?;

            if keys_written > 0 {
                let (desc, path) = image_layer_writer
                    .finish(ctx)
                    .await
                    .map_err(CompactionError::Other)?;
                let new_layer = Layer::finish_creating(self.conf, self, desc, &path)
                    .map_err(CompactionError::Other)?;
                tracing::info!(layer=%new_layer, "Rewrote layer, {} -> {} bytes",
                    layer.metadata().file_size,
                    new_layer.metadata().file_size);

                replace_image_layers.push((layer, new_layer));
            } else {
                // Drop the old layer.  Usually for this case we would already have noticed that
                // the layer has no data for us with the ShardedRange check above, but
                drop_layers.push(layer);
            }
        }

        // At this point, we have replaced local layer files with their rewritten form, but not yet uploaded
        // metadata to reflect that. If we restart here, the replaced layer files will look invalid (size mismatch
        // to remote index) and be removed. This is inefficient but safe.
        fail::fail_point!("compact-shard-ancestors-localonly");

        // Update the LayerMap so that readers will use the new layers, and enqueue it for writing to remote storage
        self.rewrite_layers(replace_image_layers, drop_layers)
            .await?;

        fail::fail_point!("compact-shard-ancestors-enqueued");

        // We wait for all uploads to complete before finishing this compaction stage.  This is not
        // necessary for correctness, but it simplifies testing, and avoids proceeding with another
        // Timeline's compaction while this timeline's uploads may be generating lots of disk I/O
        // load.
        match self.remote_client.wait_completion().await {
            Ok(()) => (),
            Err(WaitCompletionError::NotInitialized(ni)) => return Err(CompactionError::from(ni)),
            Err(WaitCompletionError::UploadQueueShutDownOrStopped) => {
                return Err(CompactionError::ShuttingDown)
            }
        }

        fail::fail_point!("compact-shard-ancestors-persistent");

        Ok(())
    }

    /// Update the LayerVisibilityHint of layers covered by image layers, based on whether there is
    /// an image layer between them and the most recent readable LSN (branch point or tip of timeline).  The
    /// purpose of the visibility hint is to record which layers need to be available to service reads.
    ///
    /// The result may be used as an input to eviction and secondary downloads to de-prioritize layers
    /// that we know won't be needed for reads.
    pub(super) async fn update_layer_visibility(
        &self,
    ) -> Result<(), super::layer_manager::Shutdown> {
        let head_lsn = self.get_last_record_lsn();

        // We will sweep through layers in reverse-LSN order.  We only do historic layers.  L0 deltas
        // are implicitly left visible, because LayerVisibilityHint's default is Visible, and we never modify it here.
        // Note that L0 deltas _can_ be covered by image layers, but we consider them 'visible' because we anticipate that
        // they will be subject to L0->L1 compaction in the near future.
        let layer_manager = self.layers.read().await;
        let layer_map = layer_manager.layer_map()?;

        let readable_points = {
            let children = self.gc_info.read().unwrap().retain_lsns.clone();

            let mut readable_points = Vec::with_capacity(children.len() + 1);
            for (child_lsn, _child_timeline_id, is_offloaded) in &children {
                if *is_offloaded == MaybeOffloaded::Yes {
                    continue;
                }
                readable_points.push(*child_lsn);
            }
            readable_points.push(head_lsn);
            readable_points
        };

        let (layer_visibility, covered) = layer_map.get_visibility(readable_points);
        for (layer_desc, visibility) in layer_visibility {
            // FIXME: a more efficiency bulk zip() through the layers rather than NlogN getting each one
            let layer = layer_manager.get_from_desc(&layer_desc);
            layer.set_visibility(visibility);
        }

        // TODO: publish our covered KeySpace to our parent, so that when they update their visibility, they can
        // avoid assuming that everything at a branch point is visible.
        drop(covered);
        Ok(())
    }

    /// Collect a bunch of Level 0 layer files, and compact and reshuffle them as
    /// as Level 1 files. Returns whether the L0 layers are fully compacted.
    async fn compact_level0(
        self: &Arc<Self>,
        target_file_size: u64,
        force_compaction_ignore_threshold: bool,
        ctx: &RequestContext,
    ) -> Result<bool, CompactionError> {
        let CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact,
            fully_compacted,
        } = {
            let phase1_span = info_span!("compact_level0_phase1");
            let ctx = ctx.attached_child();
            let mut stats = CompactLevel0Phase1StatsBuilder {
                version: Some(2),
                tenant_id: Some(self.tenant_shard_id),
                timeline_id: Some(self.timeline_id),
                ..Default::default()
            };

            let begin = tokio::time::Instant::now();
            let phase1_layers_locked = self.layers.read().await;
            let now = tokio::time::Instant::now();
            stats.read_lock_acquisition_micros =
                DurationRecorder::Recorded(RecordedDuration(now - begin), now);
            self.compact_level0_phase1(
                phase1_layers_locked,
                stats,
                target_file_size,
                force_compaction_ignore_threshold,
                &ctx,
            )
            .instrument(phase1_span)
            .await?
        };

        if new_layers.is_empty() && deltas_to_compact.is_empty() {
            // nothing to do
            return Ok(true);
        }

        self.finish_compact_batch(&new_layers, &Vec::new(), &deltas_to_compact)
            .await?;
        Ok(fully_compacted)
    }

    /// Level0 files first phase of compaction, explained in the [`Self::compact_legacy`] comment.
    async fn compact_level0_phase1<'a>(
        self: &'a Arc<Self>,
        guard: tokio::sync::RwLockReadGuard<'a, LayerManager>,
        mut stats: CompactLevel0Phase1StatsBuilder,
        target_file_size: u64,
        force_compaction_ignore_threshold: bool,
        ctx: &RequestContext,
    ) -> Result<CompactLevel0Phase1Result, CompactionError> {
        stats.read_lock_held_spawn_blocking_startup_micros =
            stats.read_lock_acquisition_micros.till_now(); // set by caller
        let layers = guard.layer_map()?;
        let level0_deltas = layers.level0_deltas();
        stats.level0_deltas_count = Some(level0_deltas.len());

        // Only compact if enough layers have accumulated.
        let threshold = self.get_compaction_threshold();
        if level0_deltas.is_empty() || level0_deltas.len() < threshold {
            if force_compaction_ignore_threshold {
                if !level0_deltas.is_empty() {
                    info!(
                        level0_deltas = level0_deltas.len(),
                        threshold, "too few deltas to compact, but forcing compaction"
                    );
                } else {
                    info!(
                        level0_deltas = level0_deltas.len(),
                        threshold, "too few deltas to compact, cannot force compaction"
                    );
                    return Ok(CompactLevel0Phase1Result::default());
                }
            } else {
                debug!(
                    level0_deltas = level0_deltas.len(),
                    threshold, "too few deltas to compact"
                );
                return Ok(CompactLevel0Phase1Result::default());
            }
        }

        let mut level0_deltas = level0_deltas
            .iter()
            .map(|x| guard.get_from_desc(x))
            .collect::<Vec<_>>();

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
        level0_deltas.sort_by_key(|l| l.layer_desc().lsn_range.start);
        let mut level0_deltas_iter = level0_deltas.iter();

        let first_level0_delta = level0_deltas_iter.next().unwrap();
        let mut prev_lsn_end = first_level0_delta.layer_desc().lsn_range.end;
        let mut deltas_to_compact = Vec::with_capacity(level0_deltas.len());

        // Accumulate the size of layers in `deltas_to_compact`
        let mut deltas_to_compact_bytes = 0;

        // Under normal circumstances, we will accumulate up to compaction_upper_limit L0s of size
        // checkpoint_distance each.  To avoid edge cases using extra system resources, bound our
        // work in this function to only operate on this much delta data at once.
        //
        // In general, compaction_threshold should be <= compaction_upper_limit, but in case that
        // the constraint is not respected, we use the larger of the two.
        let delta_size_limit = std::cmp::max(
            self.get_compaction_upper_limit(),
            self.get_compaction_threshold(),
        ) as u64
            * std::cmp::max(self.get_checkpoint_distance(), DEFAULT_CHECKPOINT_DISTANCE);

        let mut fully_compacted = true;

        deltas_to_compact.push(first_level0_delta.download_and_keep_resident().await?);
        for l in level0_deltas_iter {
            let lsn_range = &l.layer_desc().lsn_range;

            if lsn_range.start != prev_lsn_end {
                break;
            }
            deltas_to_compact.push(l.download_and_keep_resident().await?);
            deltas_to_compact_bytes += l.metadata().file_size;
            prev_lsn_end = lsn_range.end;

            if deltas_to_compact_bytes >= delta_size_limit {
                info!(
                    l0_deltas_selected = deltas_to_compact.len(),
                    l0_deltas_total = level0_deltas.len(),
                    "L0 compaction picker hit max delta layer size limit: {}",
                    delta_size_limit
                );
                fully_compacted = false;

                // Proceed with compaction, but only a subset of L0s
                break;
            }
        }
        let lsn_range = Range {
            start: deltas_to_compact
                .first()
                .unwrap()
                .layer_desc()
                .lsn_range
                .start,
            end: deltas_to_compact.last().unwrap().layer_desc().lsn_range.end,
        };

        info!(
            "Starting Level0 compaction in LSN range {}-{} for {} layers ({} deltas in total)",
            lsn_range.start,
            lsn_range.end,
            deltas_to_compact.len(),
            level0_deltas.len()
        );

        for l in deltas_to_compact.iter() {
            info!("compact includes {l}");
        }

        // We don't need the original list of layers anymore. Drop it so that
        // we don't accidentally use it later in the function.
        drop(level0_deltas);

        stats.read_lock_held_prerequisites_micros = stats
            .read_lock_held_spawn_blocking_startup_micros
            .till_now();

        // TODO: replace with streaming k-merge
        let all_keys = {
            let mut all_keys = Vec::new();
            for l in deltas_to_compact.iter() {
                if self.cancel.is_cancelled() {
                    return Err(CompactionError::ShuttingDown);
                }
                let delta = l.get_as_delta(ctx).await.map_err(CompactionError::Other)?;
                let keys = delta
                    .index_entries(ctx)
                    .await
                    .map_err(CompactionError::Other)?;
                all_keys.extend(keys);
            }
            // The current stdlib sorting implementation is designed in a way where it is
            // particularly fast where the slice is made up of sorted sub-ranges.
            all_keys.sort_by_key(|DeltaEntry { key, lsn, .. }| (*key, *lsn));
            all_keys
        };

        stats.read_lock_held_key_sort_micros = stats.read_lock_held_prerequisites_micros.till_now();

        // Determine N largest holes where N is number of compacted layers. The vec is sorted by key range start.
        //
        // A hole is a key range for which this compaction doesn't have any WAL records.
        // Our goal in this compaction iteration is to avoid creating L1s that, in terms of their key range,
        // cover the hole, but actually don't contain any WAL records for that key range.
        // The reason is that the mere stack of L1s (`count_deltas`) triggers image layer creation (`create_image_layers`).
        // That image layer creation would be useless for a hole range covered by L1s that don't contain any WAL records.
        //
        // The algorithm chooses holes as follows.
        // - Slide a 2-window over the keys in key orde to get the hole range (=distance between two keys).
        // - Filter: min threshold on range length
        // - Rank: by coverage size (=number of image layers required to reconstruct each key in the range for which we have any data)
        //
        // For more details, intuition, and some ASCII art see https://github.com/neondatabase/neon/pull/3597#discussion_r1112704451
        #[derive(PartialEq, Eq)]
        struct Hole {
            key_range: Range<Key>,
            coverage_size: usize,
        }
        let holes: Vec<Hole> = {
            use std::cmp::Ordering;
            impl Ord for Hole {
                fn cmp(&self, other: &Self) -> Ordering {
                    self.coverage_size.cmp(&other.coverage_size).reverse()
                }
            }
            impl PartialOrd for Hole {
                fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                    Some(self.cmp(other))
                }
            }
            let max_holes = deltas_to_compact.len();
            let last_record_lsn = self.get_last_record_lsn();
            let min_hole_range = (target_file_size / page_cache::PAGE_SZ as u64) as i128;
            let min_hole_coverage_size = 3; // TODO: something more flexible?
                                            // min-heap (reserve space for one more element added before eviction)
            let mut heap: BinaryHeap<Hole> = BinaryHeap::with_capacity(max_holes + 1);
            let mut prev: Option<Key> = None;

            for &DeltaEntry { key: next_key, .. } in all_keys.iter() {
                if let Some(prev_key) = prev {
                    // just first fast filter, do not create hole entries for metadata keys. The last hole in the
                    // compaction is the gap between data key and metadata keys.
                    if next_key.to_i128() - prev_key.to_i128() >= min_hole_range
                        && !Key::is_metadata_key(&prev_key)
                    {
                        let key_range = prev_key..next_key;
                        // Measuring hole by just subtraction of i128 representation of key range boundaries
                        // has not so much sense, because largest holes will corresponds field1/field2 changes.
                        // But we are mostly interested to eliminate holes which cause generation of excessive image layers.
                        // That is why it is better to measure size of hole as number of covering image layers.
                        let coverage_size =
                            layers.image_coverage(&key_range, last_record_lsn).len();
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
            let mut holes = heap.into_vec();
            holes.sort_unstable_by_key(|hole| hole.key_range.start);
            holes
        };
        stats.read_lock_held_compute_holes_micros = stats.read_lock_held_key_sort_micros.till_now();
        drop_rlock(guard);

        if self.cancel.is_cancelled() {
            return Err(CompactionError::ShuttingDown);
        }

        stats.read_lock_drop_micros = stats.read_lock_held_compute_holes_micros.till_now();

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        // If there's both a Value::Image and Value::WalRecord for the same (key,lsn),
        // then the Value::Image is ordered before Value::WalRecord.
        let mut all_values_iter = {
            let mut deltas = Vec::with_capacity(deltas_to_compact.len());
            for l in deltas_to_compact.iter() {
                let l = l.get_as_delta(ctx).await.map_err(CompactionError::Other)?;
                deltas.push(l);
            }
            MergeIterator::create(&deltas, &[], ctx)
        };

        // This iterator walks through all keys and is needed to calculate size used by each key
        let mut all_keys_iter = all_keys
            .iter()
            .map(|DeltaEntry { key, lsn, size, .. }| (*key, *lsn, *size))
            .coalesce(|mut prev, cur| {
                // Coalesce keys that belong to the same key pair.
                // This ensures that compaction doesn't put them
                // into different layer files.
                // Still limit this by the target file size,
                // so that we keep the size of the files in
                // check.
                if prev.0 == cur.0 && prev.2 < target_file_size {
                    prev.2 += cur.2;
                    Ok(prev)
                } else {
                    Err((prev, cur))
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
        let mut next_hole = 0; // index of next hole in holes vector

        let mut keys = 0;

        while let Some((key, lsn, value)) = all_values_iter
            .next()
            .await
            .map_err(CompactionError::Other)?
        {
            keys += 1;

            if keys % 32_768 == 0 && self.cancel.is_cancelled() {
                // avoid hitting the cancellation token on every key. in benches, we end up
                // shuffling an order of million keys per layer, this means we'll check it
                // around tens of times per layer.
                return Err(CompactionError::ShuttingDown);
            }

            let same_key = prev_key == Some(key);
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
                        let (desc, path) = writer
                            .take()
                            .unwrap()
                            .finish(prev_key.unwrap().next(), ctx)
                            .await
                            .map_err(CompactionError::Other)?;
                        let new_delta = Layer::finish_creating(self.conf, self, desc, &path)
                            .map_err(CompactionError::Other)?;

                        new_layers.push(new_delta);
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
            fail_point!("delta-layer-writer-fail-before-finish", |_| {
                Err(CompactionError::Other(anyhow::anyhow!(
                    "failpoint delta-layer-writer-fail-before-finish"
                )))
            });

            if !self.shard_identity.is_key_disposable(&key) {
                if writer.is_none() {
                    if self.cancel.is_cancelled() {
                        // to be somewhat responsive to cancellation, check for each new layer
                        return Err(CompactionError::ShuttingDown);
                    }
                    // Create writer if not initiaized yet
                    writer = Some(
                        DeltaLayerWriter::new(
                            self.conf,
                            self.timeline_id,
                            self.tenant_shard_id,
                            key,
                            if dup_end_lsn.is_valid() {
                                // this is a layer containing slice of values of the same key
                                debug!("Create new dup layer {}..{}", dup_start_lsn, dup_end_lsn);
                                dup_start_lsn..dup_end_lsn
                            } else {
                                debug!("Create new layer {}..{}", lsn_range.start, lsn_range.end);
                                lsn_range.clone()
                            },
                            ctx,
                        )
                        .await
                        .map_err(CompactionError::Other)?,
                    );

                    keys = 0;
                }

                writer
                    .as_mut()
                    .unwrap()
                    .put_value(key, lsn, value, ctx)
                    .await
                    .map_err(CompactionError::Other)?;
            } else {
                let owner = self.shard_identity.get_shard_number(&key);

                // This happens after a shard split, when we're compacting an L0 created by our parent shard
                debug!("dropping key {key} during compaction (it belongs on shard {owner})");
            }

            if !new_layers.is_empty() {
                fail_point!("after-timeline-compacted-first-L1");
            }

            prev_key = Some(key);
        }
        if let Some(writer) = writer {
            let (desc, path) = writer
                .finish(prev_key.unwrap().next(), ctx)
                .await
                .map_err(CompactionError::Other)?;
            let new_delta = Layer::finish_creating(self.conf, self, desc, &path)
                .map_err(CompactionError::Other)?;
            new_layers.push(new_delta);
        }

        // Sync layers
        if !new_layers.is_empty() {
            // Print a warning if the created layer is larger than double the target size
            // Add two pages for potential overhead. This should in theory be already
            // accounted for in the target calculation, but for very small targets,
            // we still might easily hit the limit otherwise.
            let warn_limit = target_file_size * 2 + page_cache::PAGE_SZ as u64 * 2;
            for layer in new_layers.iter() {
                if layer.layer_desc().file_size > warn_limit {
                    warn!(
                        %layer,
                        "created delta file of size {} larger than double of target of {target_file_size}", layer.layer_desc().file_size
                    );
                }
            }

            // The writer.finish() above already did the fsync of the inodes.
            // We just need to fsync the directory in which these inodes are linked,
            // which we know to be the timeline directory.
            //
            // We use fatal_err() below because the after writer.finish() returns with success,
            // the in-memory state of the filesystem already has the layer file in its final place,
            // and subsequent pageserver code could think it's durable while it really isn't.
            let timeline_dir = VirtualFile::open(
                &self
                    .conf
                    .timeline_path(&self.tenant_shard_id, &self.timeline_id),
                ctx,
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }

        stats.write_layer_files_micros = stats.read_lock_drop_micros.till_now();
        stats.new_deltas_count = Some(new_layers.len());
        stats.new_deltas_size = Some(new_layers.iter().map(|l| l.layer_desc().file_size).sum());

        match TryInto::<CompactLevel0Phase1Stats>::try_into(stats)
            .and_then(|stats| serde_json::to_string(&stats).context("serde_json::to_string"))
        {
            Ok(stats_json) => {
                info!(
                    stats_json = stats_json.as_str(),
                    "compact_level0_phase1 stats available"
                )
            }
            Err(e) => {
                warn!("compact_level0_phase1 stats failed to serialize: {:#}", e);
            }
        }

        // Without this, rustc complains about deltas_to_compact still
        // being borrowed when we `.into_iter()` below.
        drop(all_values_iter);

        Ok(CompactLevel0Phase1Result {
            new_layers,
            deltas_to_compact: deltas_to_compact
                .into_iter()
                .map(|x| x.drop_eviction_guard())
                .collect::<Vec<_>>(),
            fully_compacted,
        })
    }
}

#[derive(Default)]
struct CompactLevel0Phase1Result {
    new_layers: Vec<ResidentLayer>,
    deltas_to_compact: Vec<Layer>,
    // Whether we have included all L0 layers, or selected only part of them due to the
    // L0 compaction size limit.
    fully_compacted: bool,
}

#[derive(Default)]
struct CompactLevel0Phase1StatsBuilder {
    version: Option<u64>,
    tenant_id: Option<TenantShardId>,
    timeline_id: Option<TimelineId>,
    read_lock_acquisition_micros: DurationRecorder,
    read_lock_held_spawn_blocking_startup_micros: DurationRecorder,
    read_lock_held_key_sort_micros: DurationRecorder,
    read_lock_held_prerequisites_micros: DurationRecorder,
    read_lock_held_compute_holes_micros: DurationRecorder,
    read_lock_drop_micros: DurationRecorder,
    write_layer_files_micros: DurationRecorder,
    level0_deltas_count: Option<usize>,
    new_deltas_count: Option<usize>,
    new_deltas_size: Option<u64>,
}

#[derive(serde::Serialize)]
struct CompactLevel0Phase1Stats {
    version: u64,
    tenant_id: TenantShardId,
    timeline_id: TimelineId,
    read_lock_acquisition_micros: RecordedDuration,
    read_lock_held_spawn_blocking_startup_micros: RecordedDuration,
    read_lock_held_key_sort_micros: RecordedDuration,
    read_lock_held_prerequisites_micros: RecordedDuration,
    read_lock_held_compute_holes_micros: RecordedDuration,
    read_lock_drop_micros: RecordedDuration,
    write_layer_files_micros: RecordedDuration,
    level0_deltas_count: usize,
    new_deltas_count: usize,
    new_deltas_size: u64,
}

impl TryFrom<CompactLevel0Phase1StatsBuilder> for CompactLevel0Phase1Stats {
    type Error = anyhow::Error;

    fn try_from(value: CompactLevel0Phase1StatsBuilder) -> Result<Self, Self::Error> {
        Ok(Self {
            version: value.version.ok_or_else(|| anyhow!("version not set"))?,
            tenant_id: value
                .tenant_id
                .ok_or_else(|| anyhow!("tenant_id not set"))?,
            timeline_id: value
                .timeline_id
                .ok_or_else(|| anyhow!("timeline_id not set"))?,
            read_lock_acquisition_micros: value
                .read_lock_acquisition_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_acquisition_micros not set"))?,
            read_lock_held_spawn_blocking_startup_micros: value
                .read_lock_held_spawn_blocking_startup_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_spawn_blocking_startup_micros not set"))?,
            read_lock_held_key_sort_micros: value
                .read_lock_held_key_sort_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_key_sort_micros not set"))?,
            read_lock_held_prerequisites_micros: value
                .read_lock_held_prerequisites_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_prerequisites_micros not set"))?,
            read_lock_held_compute_holes_micros: value
                .read_lock_held_compute_holes_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_held_compute_holes_micros not set"))?,
            read_lock_drop_micros: value
                .read_lock_drop_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("read_lock_drop_micros not set"))?,
            write_layer_files_micros: value
                .write_layer_files_micros
                .into_recorded()
                .ok_or_else(|| anyhow!("write_layer_files_micros not set"))?,
            level0_deltas_count: value
                .level0_deltas_count
                .ok_or_else(|| anyhow!("level0_deltas_count not set"))?,
            new_deltas_count: value
                .new_deltas_count
                .ok_or_else(|| anyhow!("new_deltas_count not set"))?,
            new_deltas_size: value
                .new_deltas_size
                .ok_or_else(|| anyhow!("new_deltas_size not set"))?,
        })
    }
}

impl Timeline {
    /// Entry point for new tiered compaction algorithm.
    ///
    /// All the real work is in the implementation in the pageserver_compaction
    /// crate. The code here would apply to any algorithm implemented by the
    /// same interface, but tiered is the only one at the moment.
    ///
    /// TODO: cancellation
    pub(crate) async fn compact_tiered(
        self: &Arc<Self>,
        _cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<(), CompactionError> {
        let fanout = self.get_compaction_threshold() as u64;
        let target_file_size = self.get_checkpoint_distance();

        // Find the top of the historical layers
        let end_lsn = {
            let guard = self.layers.read().await;
            let layers = guard.layer_map()?;

            let l0_deltas = layers.level0_deltas();

            // As an optimization, if we find that there are too few L0 layers,
            // bail out early. We know that the compaction algorithm would do
            // nothing in that case.
            if l0_deltas.len() < fanout as usize {
                // doesn't need compacting
                return Ok(());
            }
            l0_deltas.iter().map(|l| l.lsn_range.end).max().unwrap()
        };

        // Is the timeline being deleted?
        if self.is_stopping() {
            trace!("Dropping out of compaction on timeline shutdown");
            return Err(CompactionError::ShuttingDown);
        }

        let (dense_ks, _sparse_ks) = self.collect_keyspace(end_lsn, ctx).await?;
        // TODO(chi): ignore sparse_keyspace for now, compact it in the future.
        let mut adaptor = TimelineAdaptor::new(self, (end_lsn, dense_ks));

        pageserver_compaction::compact_tiered::compact_tiered(
            &mut adaptor,
            end_lsn,
            target_file_size,
            fanout,
            ctx,
        )
        .await
        // TODO: compact_tiered needs to return CompactionError
        .map_err(CompactionError::Other)?;

        adaptor.flush_updates().await?;
        Ok(())
    }

    /// Take a list of images and deltas, produce images and deltas according to GC horizon and retain_lsns.
    ///
    /// It takes a key, the values of the key within the compaction process, a GC horizon, and all retain_lsns below the horizon.
    /// For now, it requires the `accumulated_values` contains the full history of the key (i.e., the key with the lowest LSN is
    /// an image or a WAL not requiring a base image). This restriction will be removed once we implement gc-compaction on branch.
    ///
    /// The function returns the deltas and the base image that need to be placed at each of the retain LSN. For example, we have:
    ///
    /// A@0x10, +B@0x20, +C@0x30, +D@0x40, +E@0x50, +F@0x60
    /// horizon = 0x50, retain_lsn = 0x20, 0x40, delta_threshold=3
    ///
    /// The function will produce:
    ///
    /// ```plain
    /// 0x20(retain_lsn) -> img=AB@0x20                  always produce a single image below the lowest retain LSN
    /// 0x40(retain_lsn) -> deltas=[+C@0x30, +D@0x40]    two deltas since the last base image, keeping the deltas
    /// 0x50(horizon)    -> deltas=[ABCDE@0x50]          three deltas since the last base image, generate an image but put it in the delta
    /// above_horizon    -> deltas=[+F@0x60]             full history above the horizon
    /// ```
    ///
    /// Note that `accumulated_values` must be sorted by LSN and should belong to a single key.
    pub(crate) async fn generate_key_retention(
        self: &Arc<Timeline>,
        key: Key,
        full_history: &[(Key, Lsn, Value)],
        horizon: Lsn,
        retain_lsn_below_horizon: &[Lsn],
        delta_threshold_cnt: usize,
        base_img_from_ancestor: Option<(Key, Lsn, Bytes)>,
    ) -> anyhow::Result<KeyHistoryRetention> {
        // Pre-checks for the invariants

        let debug_mode = cfg!(debug_assertions) || cfg!(feature = "testing");

        if debug_mode {
            for (log_key, _, _) in full_history {
                assert_eq!(log_key, &key, "mismatched key");
            }
            for i in 1..full_history.len() {
                assert!(full_history[i - 1].1 <= full_history[i].1, "unordered LSN");
                if full_history[i - 1].1 == full_history[i].1 {
                    assert!(
                        matches!(full_history[i - 1].2, Value::Image(_)),
                        "unordered delta/image, or duplicated delta"
                    );
                }
            }
            // There was an assertion for no base image that checks if the first
            // record in the history is `will_init` before, but it was removed.
            // This is explained in the test cases for generate_key_retention.
            // Search "incomplete history" for more information.
            for lsn in retain_lsn_below_horizon {
                assert!(lsn < &horizon, "retain lsn must be below horizon")
            }
            for i in 1..retain_lsn_below_horizon.len() {
                assert!(
                    retain_lsn_below_horizon[i - 1] <= retain_lsn_below_horizon[i],
                    "unordered LSN"
                );
            }
        }
        let has_ancestor = base_img_from_ancestor.is_some();
        // Step 1: split history into len(retain_lsn_below_horizon) + 2 buckets, where the last bucket is for all deltas above the horizon,
        // and the second-to-last bucket is for the horizon. Each bucket contains lsn_last_bucket < deltas <= lsn_this_bucket.
        let (mut split_history, lsn_split_points) = {
            let mut split_history = Vec::new();
            split_history.resize_with(retain_lsn_below_horizon.len() + 2, Vec::new);
            let mut lsn_split_points = Vec::with_capacity(retain_lsn_below_horizon.len() + 1);
            for lsn in retain_lsn_below_horizon {
                lsn_split_points.push(*lsn);
            }
            lsn_split_points.push(horizon);
            let mut current_idx = 0;
            for item @ (_, lsn, _) in full_history {
                while current_idx < lsn_split_points.len() && *lsn > lsn_split_points[current_idx] {
                    current_idx += 1;
                }
                split_history[current_idx].push(item);
            }
            (split_history, lsn_split_points)
        };
        // Step 2: filter out duplicated records due to the k-merge of image/delta layers
        for split_for_lsn in &mut split_history {
            let mut prev_lsn = None;
            let mut new_split_for_lsn = Vec::with_capacity(split_for_lsn.len());
            for record @ (_, lsn, _) in std::mem::take(split_for_lsn) {
                if let Some(prev_lsn) = &prev_lsn {
                    if *prev_lsn == lsn {
                        // The case that we have an LSN with both data from the delta layer and the image layer. As
                        // `ValueWrapper` ensures that an image is ordered before a delta at the same LSN, we simply
                        // drop this delta and keep the image.
                        //
                        // For example, we have delta layer key1@0x10, key1@0x20, and image layer key1@0x10, we will
                        // keep the image for key1@0x10 and the delta for key1@0x20. key1@0x10 delta will be simply
                        // dropped.
                        //
                        // TODO: in case we have both delta + images for a given LSN and it does not exceed the delta
                        // threshold, we could have kept delta instead to save space. This is an optimization for the future.
                        continue;
                    }
                }
                prev_lsn = Some(lsn);
                new_split_for_lsn.push(record);
            }
            *split_for_lsn = new_split_for_lsn;
        }
        // Step 3: generate images when necessary
        let mut retention = Vec::with_capacity(split_history.len());
        let mut records_since_last_image = 0;
        let batch_cnt = split_history.len();
        assert!(
            batch_cnt >= 2,
            "should have at least below + above horizon batches"
        );
        let mut replay_history: Vec<(Key, Lsn, Value)> = Vec::new();
        if let Some((key, lsn, img)) = base_img_from_ancestor {
            replay_history.push((key, lsn, Value::Image(img)));
        }

        /// Generate debug information for the replay history
        fn generate_history_trace(replay_history: &[(Key, Lsn, Value)]) -> String {
            use std::fmt::Write;
            let mut output = String::new();
            if let Some((key, _, _)) = replay_history.first() {
                write!(output, "key={} ", key).unwrap();
                let mut cnt = 0;
                for (_, lsn, val) in replay_history {
                    if val.is_image() {
                        write!(output, "i@{} ", lsn).unwrap();
                    } else if val.will_init() {
                        write!(output, "di@{} ", lsn).unwrap();
                    } else {
                        write!(output, "d@{} ", lsn).unwrap();
                    }
                    cnt += 1;
                    if cnt >= 128 {
                        write!(output, "... and more").unwrap();
                        break;
                    }
                }
            } else {
                write!(output, "<no history>").unwrap();
            }
            output
        }

        fn generate_debug_trace(
            replay_history: Option<&[(Key, Lsn, Value)]>,
            full_history: &[(Key, Lsn, Value)],
            lsns: &[Lsn],
            horizon: Lsn,
        ) -> String {
            use std::fmt::Write;
            let mut output = String::new();
            if let Some(replay_history) = replay_history {
                writeln!(
                    output,
                    "replay_history: {}",
                    generate_history_trace(replay_history)
                )
                .unwrap();
            } else {
                writeln!(output, "replay_history: <disabled>",).unwrap();
            }
            writeln!(
                output,
                "full_history: {}",
                generate_history_trace(full_history)
            )
            .unwrap();
            writeln!(
                output,
                "when processing: [{}] horizon={}",
                lsns.iter().map(|l| format!("{l}")).join(","),
                horizon
            )
            .unwrap();
            output
        }

        let mut key_exists = false;
        for (i, split_for_lsn) in split_history.into_iter().enumerate() {
            // TODO: there could be image keys inside the splits, and we can compute records_since_last_image accordingly.
            records_since_last_image += split_for_lsn.len();
            // Whether to produce an image into the final layer files
            let produce_image = if i == 0 && !has_ancestor {
                // We always generate images for the first batch (below horizon / lowest retain_lsn)
                true
            } else if i == batch_cnt - 1 {
                // Do not generate images for the last batch (above horizon)
                false
            } else if records_since_last_image == 0 {
                false
            } else if records_since_last_image >= delta_threshold_cnt {
                // Generate images when there are too many records
                true
            } else {
                false
            };
            replay_history.extend(split_for_lsn.iter().map(|x| (*x).clone()));
            // Only retain the items after the last image record
            for idx in (0..replay_history.len()).rev() {
                if replay_history[idx].2.will_init() {
                    replay_history = replay_history[idx..].to_vec();
                    break;
                }
            }
            if replay_history.is_empty() && !key_exists {
                // The key does not exist at earlier LSN, we can skip this iteration.
                retention.push(Vec::new());
                continue;
            } else {
                key_exists = true;
            }
            let Some((_, _, val)) = replay_history.first() else {
                unreachable!("replay history should not be empty once it exists")
            };
            if !val.will_init() {
                return Err(anyhow::anyhow!("invalid history, no base image")).with_context(|| {
                    generate_debug_trace(
                        Some(&replay_history),
                        full_history,
                        retain_lsn_below_horizon,
                        horizon,
                    )
                });
            }
            // Whether to reconstruct the image. In debug mode, we will generate an image
            // at every retain_lsn to ensure data is not corrupted, but we won't put the
            // image into the final layer.
            let generate_image = produce_image || debug_mode;
            if produce_image {
                records_since_last_image = 0;
            }
            let img_and_lsn = if generate_image {
                let replay_history_for_debug = if debug_mode {
                    Some(replay_history.clone())
                } else {
                    None
                };
                let replay_history_for_debug_ref = replay_history_for_debug.as_deref();
                let history = if produce_image {
                    std::mem::take(&mut replay_history)
                } else {
                    replay_history.clone()
                };
                let mut img = None;
                let mut records = Vec::with_capacity(history.len());
                if let (_, lsn, Value::Image(val)) = history.first().as_ref().unwrap() {
                    img = Some((*lsn, val.clone()));
                    for (_, lsn, val) in history.into_iter().skip(1) {
                        let Value::WalRecord(rec) = val else {
                            return Err(anyhow::anyhow!(
                                "invalid record, first record is image, expect walrecords"
                            ))
                            .with_context(|| {
                                generate_debug_trace(
                                    replay_history_for_debug_ref,
                                    full_history,
                                    retain_lsn_below_horizon,
                                    horizon,
                                )
                            });
                        };
                        records.push((lsn, rec));
                    }
                } else {
                    for (_, lsn, val) in history.into_iter() {
                        let Value::WalRecord(rec) = val else {
                            return Err(anyhow::anyhow!("invalid record, first record is walrecord, expect rest are walrecord"))
                                .with_context(|| generate_debug_trace(
                                    replay_history_for_debug_ref,
                                    full_history,
                                    retain_lsn_below_horizon,
                                    horizon,
                                ));
                        };
                        records.push((lsn, rec));
                    }
                }
                records.reverse();
                let state = ValueReconstructState { img, records };
                // last batch does not generate image so i is always in range, unless we force generate
                // an image during testing
                let request_lsn = if i >= lsn_split_points.len() {
                    Lsn::MAX
                } else {
                    lsn_split_points[i]
                };
                let img = self.reconstruct_value(key, request_lsn, state).await?;
                Some((request_lsn, img))
            } else {
                None
            };
            if produce_image {
                let (request_lsn, img) = img_and_lsn.unwrap();
                replay_history.push((key, request_lsn, Value::Image(img.clone())));
                retention.push(vec![(request_lsn, Value::Image(img))]);
            } else {
                let deltas = split_for_lsn
                    .iter()
                    .map(|(_, lsn, value)| (*lsn, value.clone()))
                    .collect_vec();
                retention.push(deltas);
            }
        }
        let mut result = Vec::with_capacity(retention.len());
        assert_eq!(retention.len(), lsn_split_points.len() + 1);
        for (idx, logs) in retention.into_iter().enumerate() {
            if idx == lsn_split_points.len() {
                return Ok(KeyHistoryRetention {
                    below_horizon: result,
                    above_horizon: KeyLogAtLsn(logs),
                });
            } else {
                result.push((lsn_split_points[idx], KeyLogAtLsn(logs)));
            }
        }
        unreachable!("key retention is empty")
    }

    /// Check how much space is left on the disk
    async fn check_available_space(self: &Arc<Self>) -> anyhow::Result<u64> {
        let tenants_dir = self.conf.tenants_path();

        let stat = Statvfs::get(&tenants_dir, None)
            .context("statvfs failed, presumably directory got unlinked")?;

        let (avail_bytes, _) = stat.get_avail_total_bytes();

        Ok(avail_bytes)
    }

    /// Check if the compaction can proceed safely without running out of space. We assume the size
    /// upper bound of the produced files of a compaction job is the same as all layers involved in
    /// the compaction. Therefore, we need `2 * layers_to_be_compacted_size` at least to do a
    /// compaction.
    async fn check_compaction_space(
        self: &Arc<Self>,
        layer_selection: &[Layer],
    ) -> anyhow::Result<()> {
        let available_space = self.check_available_space().await?;
        let mut remote_layer_size = 0;
        let mut all_layer_size = 0;
        for layer in layer_selection {
            let needs_download = layer.needs_download().await?;
            if needs_download.is_some() {
                remote_layer_size += layer.layer_desc().file_size;
            }
            all_layer_size += layer.layer_desc().file_size;
        }
        let allocated_space = (available_space as f64 * 0.8) as u64; /* reserve 20% space for other tasks */
        if all_layer_size /* space needed for newly-generated file */ + remote_layer_size /* space for downloading layers */ > allocated_space
        {
            return Err(anyhow!("not enough space for compaction: available_space={}, allocated_space={}, all_layer_size={}, remote_layer_size={}, required_space={}",
                available_space, allocated_space, all_layer_size, remote_layer_size, all_layer_size + remote_layer_size));
        }
        Ok(())
    }

    /// Get a watermark for gc-compaction, that is the lowest LSN that we can use as the `gc_horizon` for
    /// the compaction algorithm. It is min(space_cutoff, time_cutoff, latest_gc_cutoff, standby_horizon).
    /// Leases and retain_lsns are considered in the gc-compaction job itself so we don't need to account for them
    /// here.
    pub(crate) fn get_gc_compaction_watermark(self: &Arc<Self>) -> Lsn {
        let gc_cutoff_lsn = {
            let gc_info = self.gc_info.read().unwrap();
            gc_info.min_cutoff()
        };

        // TODO: standby horizon should use leases so we don't really need to consider it here.
        // let watermark = watermark.min(self.standby_horizon.load());

        // TODO: ensure the child branches will not use anything below the watermark, or consider
        // them when computing the watermark.
        gc_cutoff_lsn.min(*self.get_latest_gc_cutoff_lsn())
    }

    /// Split a gc-compaction job into multiple compaction jobs. The split is based on the key range and the estimated size of the compaction job.
    /// The function returns a list of compaction jobs that can be executed separately. If the upper bound of the compact LSN
    /// range is not specified, we will use the latest gc_cutoff as the upper bound, so that all jobs in the jobset acts
    /// like a full compaction of the specified keyspace.
    pub(crate) async fn gc_compaction_split_jobs(
        self: &Arc<Self>,
        job: GcCompactJob,
        sub_compaction_max_job_size_mb: Option<u64>,
    ) -> anyhow::Result<Vec<GcCompactJob>> {
        let compact_below_lsn = if job.compact_lsn_range.end != Lsn::MAX {
            job.compact_lsn_range.end
        } else {
            self.get_gc_compaction_watermark()
        };

        if compact_below_lsn == Lsn::INVALID {
            tracing::warn!("no layers to compact with gc: gc_cutoff not generated yet, skipping gc bottom-most compaction");
            return Ok(vec![]);
        }

        // Split compaction job to about 4GB each
        const GC_COMPACT_MAX_SIZE_MB: u64 = 4 * 1024;
        let sub_compaction_max_job_size_mb =
            sub_compaction_max_job_size_mb.unwrap_or(GC_COMPACT_MAX_SIZE_MB);

        let mut compact_jobs = Vec::new();
        // For now, we simply use the key partitioning information; we should do a more fine-grained partitioning
        // by estimating the amount of files read for a compaction job. We should also partition on LSN.
        let ((dense_ks, sparse_ks), _) = self.partitioning.read().as_ref().clone();
        // Truncate the key range to be within user specified compaction range.
        fn truncate_to(
            source_start: &Key,
            source_end: &Key,
            target_start: &Key,
            target_end: &Key,
        ) -> Option<(Key, Key)> {
            let start = source_start.max(target_start);
            let end = source_end.min(target_end);
            if start < end {
                Some((*start, *end))
            } else {
                None
            }
        }
        let mut split_key_ranges = Vec::new();
        let ranges = dense_ks
            .parts
            .iter()
            .map(|partition| partition.ranges.iter())
            .chain(sparse_ks.parts.iter().map(|x| x.0.ranges.iter()))
            .flatten()
            .cloned()
            .collect_vec();
        for range in ranges.iter() {
            let Some((start, end)) = truncate_to(
                &range.start,
                &range.end,
                &job.compact_key_range.start,
                &job.compact_key_range.end,
            ) else {
                continue;
            };
            split_key_ranges.push((start, end));
        }
        split_key_ranges.sort();
        let guard = self.layers.read().await;
        let layer_map = guard.layer_map()?;
        let mut current_start = None;
        let ranges_num = split_key_ranges.len();
        for (idx, (start, end)) in split_key_ranges.into_iter().enumerate() {
            if current_start.is_none() {
                current_start = Some(start);
            }
            let start = current_start.unwrap();
            if start >= end {
                // We have already processed this partition.
                continue;
            }
            let res = layer_map.range_search(start..end, compact_below_lsn);
            let total_size = res.found.keys().map(|x| x.layer.file_size()).sum::<u64>();
            if total_size > sub_compaction_max_job_size_mb * 1024 * 1024 || ranges_num == idx + 1 {
                // Try to extend the compaction range so that we include at least one full layer file.
                let extended_end = res
                    .found
                    .keys()
                    .map(|layer| layer.layer.key_range.end)
                    .min();
                // It is possible that the search range does not contain any layer files when we reach the end of the loop.
                // In this case, we simply use the specified key range end.
                let end = if let Some(extended_end) = extended_end {
                    extended_end.max(end)
                } else {
                    end
                };
                let end = if ranges_num == idx + 1 {
                    // extend the compaction range to the end of the key range if it's the last partition
                    end.max(job.compact_key_range.end)
                } else {
                    end
                };
                info!(
                    "splitting compaction job: {}..{}, estimated_size={}",
                    start, end, total_size
                );
                compact_jobs.push(GcCompactJob {
                    dry_run: job.dry_run,
                    compact_key_range: start..end,
                    compact_lsn_range: job.compact_lsn_range.start..compact_below_lsn,
                });
                current_start = Some(end);
            }
        }
        drop(guard);
        Ok(compact_jobs)
    }

    /// An experimental compaction building block that combines compaction with garbage collection.
    ///
    /// The current implementation picks all delta + image layers that are below or intersecting with
    /// the GC horizon without considering retain_lsns. Then, it does a full compaction over all these delta
    /// layers and image layers, which generates image layers on the gc horizon, drop deltas below gc horizon,
    /// and create delta layers with all deltas >= gc horizon.
    ///
    /// If `options.compact_range` is provided, it will only compact the keys within the range, aka partial compaction.
    /// Partial compaction will read and process all layers overlapping with the key range, even if it might
    /// contain extra keys. After the gc-compaction phase completes, delta layers that are not fully contained
    /// within the key range will be rewritten to ensure they do not overlap with the delta layers. Providing
    /// Key::MIN..Key..MAX to the function indicates a full compaction, though technically, `Key::MAX` is not
    /// part of the range.
    ///
    /// If `options.compact_lsn_range.end` is provided, the compaction will only compact layers below or intersect with
    /// the LSN. Otherwise, it will use the gc cutoff by default.
    pub(crate) async fn compact_with_gc(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        options: CompactOptions,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let sub_compaction = options.sub_compaction;
        let job = GcCompactJob::from_compact_options(options.clone());
        if sub_compaction {
            info!("running enhanced gc bottom-most compaction with sub-compaction, splitting compaction jobs");
            let jobs = self
                .gc_compaction_split_jobs(job, options.sub_compaction_max_job_size_mb)
                .await?;
            let jobs_len = jobs.len();
            for (idx, job) in jobs.into_iter().enumerate() {
                info!(
                    "running enhanced gc bottom-most compaction, sub-compaction {}/{}",
                    idx + 1,
                    jobs_len
                );
                self.compact_with_gc_inner(cancel, job, ctx).await?;
            }
            if jobs_len == 0 {
                info!("no jobs to run, skipping gc bottom-most compaction");
            }
            return Ok(());
        }
        self.compact_with_gc_inner(cancel, job, ctx).await
    }

    async fn compact_with_gc_inner(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        job: GcCompactJob,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Block other compaction/GC tasks from running for now. GC-compaction could run along
        // with legacy compaction tasks in the future. Always ensure the lock order is compaction -> gc.
        // Note that we already acquired the compaction lock when the outer `compact` function gets called.

        let gc_lock = async {
            tokio::select! {
                guard = self.gc_lock.lock() => Ok(guard),
                // TODO: refactor to CompactionError to correctly pass cancelled error
                _ = cancel.cancelled() => Err(anyhow!("cancelled")),
            }
        };

        let gc_lock = crate::timed(
            gc_lock,
            "acquires gc lock",
            std::time::Duration::from_secs(5),
        )
        .await?;

        let dry_run = job.dry_run;
        let compact_key_range = job.compact_key_range;
        let compact_lsn_range = job.compact_lsn_range;

        let debug_mode = cfg!(debug_assertions) || cfg!(feature = "testing");

        info!("running enhanced gc bottom-most compaction, dry_run={dry_run}, compact_key_range={}..{}, compact_lsn_range={}..{}", compact_key_range.start, compact_key_range.end, compact_lsn_range.start, compact_lsn_range.end);

        scopeguard::defer! {
            info!("done enhanced gc bottom-most compaction");
        };

        let mut stat = CompactionStatistics::default();

        // Step 0: pick all delta layers + image layers below/intersect with the GC horizon.
        // The layer selection has the following properties:
        // 1. If a layer is in the selection, all layers below it are in the selection.
        // 2. Inferred from (1), for each key in the layer selection, the value can be reconstructed only with the layers in the layer selection.
        let job_desc = {
            let guard = self.layers.read().await;
            let layers = guard.layer_map()?;
            let gc_info = self.gc_info.read().unwrap();
            let mut retain_lsns_below_horizon = Vec::new();
            let gc_cutoff = {
                // Currently, gc-compaction only kicks in after the legacy gc has updated the gc_cutoff.
                // Therefore, it can only clean up data that cannot be cleaned up with legacy gc, instead of
                // cleaning everything that theoritically it could. In the future, it should use `self.gc_info`
                // to get the truth data.
                let real_gc_cutoff = self.get_gc_compaction_watermark();
                // The compaction algorithm will keep all keys above the gc_cutoff while keeping only necessary keys below the gc_cutoff for
                // each of the retain_lsn. Therefore, if the user-provided `compact_lsn_range.end` is larger than the real gc cutoff, we will use
                // the real cutoff.
                let mut gc_cutoff = if compact_lsn_range.end == Lsn::MAX {
                    if real_gc_cutoff == Lsn::INVALID {
                        // If the gc_cutoff is not generated yet, we should not compact anything.
                        tracing::warn!("no layers to compact with gc: gc_cutoff not generated yet, skipping gc bottom-most compaction");
                        return Ok(());
                    }
                    real_gc_cutoff
                } else {
                    compact_lsn_range.end
                };
                if gc_cutoff > real_gc_cutoff {
                    warn!("provided compact_lsn_range.end={} is larger than the real_gc_cutoff={}, using the real gc cutoff", gc_cutoff, real_gc_cutoff);
                    gc_cutoff = real_gc_cutoff;
                }
                gc_cutoff
            };
            for (lsn, _timeline_id, _is_offloaded) in &gc_info.retain_lsns {
                if lsn < &gc_cutoff {
                    retain_lsns_below_horizon.push(*lsn);
                }
            }
            for lsn in gc_info.leases.keys() {
                if lsn < &gc_cutoff {
                    retain_lsns_below_horizon.push(*lsn);
                }
            }
            let mut selected_layers: Vec<Layer> = Vec::new();
            drop(gc_info);
            // Firstly, pick all the layers intersect or below the gc_cutoff, get the largest LSN in the selected layers.
            let Some(max_layer_lsn) = layers
                .iter_historic_layers()
                .filter(|desc| desc.get_lsn_range().start <= gc_cutoff)
                .map(|desc| desc.get_lsn_range().end)
                .max()
            else {
                info!("no layers to compact with gc: no historic layers below gc_cutoff, gc_cutoff={}", gc_cutoff);
                return Ok(());
            };
            // Next, if the user specifies compact_lsn_range.start, we need to filter some layers out. All the layers (strictly) below
            // the min_layer_lsn computed as below will be filtered out and the data will be accessed using the normal read path, as if
            // it is a branch.
            let Some(min_layer_lsn) = layers
                .iter_historic_layers()
                .filter(|desc| {
                    if compact_lsn_range.start == Lsn::INVALID {
                        true // select all layers below if start == Lsn(0)
                    } else {
                        desc.get_lsn_range().end > compact_lsn_range.start // strictly larger than compact_above_lsn
                    }
                })
                .map(|desc| desc.get_lsn_range().start)
                .min()
            else {
                info!("no layers to compact with gc: no historic layers above compact_above_lsn, compact_above_lsn={}", compact_lsn_range.end);
                return Ok(());
            };
            // Then, pick all the layers that are below the max_layer_lsn. This is to ensure we can pick all single-key
            // layers to compact.
            let mut rewrite_layers = Vec::new();
            for desc in layers.iter_historic_layers() {
                if desc.get_lsn_range().end <= max_layer_lsn
                    && desc.get_lsn_range().start >= min_layer_lsn
                    && overlaps_with(&desc.get_key_range(), &compact_key_range)
                {
                    // If the layer overlaps with the compaction key range, we need to read it to obtain all keys within the range,
                    // even if it might contain extra keys
                    selected_layers.push(guard.get_from_desc(&desc));
                    // If the layer is not fully contained within the key range, we need to rewrite it if it's a delta layer (it's fine
                    // to overlap image layers)
                    if desc.is_delta() && !fully_contains(&compact_key_range, &desc.get_key_range())
                    {
                        rewrite_layers.push(desc);
                    }
                }
            }
            if selected_layers.is_empty() {
                info!("no layers to compact with gc: no layers within the key range, gc_cutoff={}, key_range={}..{}", gc_cutoff, compact_key_range.start, compact_key_range.end);
                return Ok(());
            }
            retain_lsns_below_horizon.sort();
            GcCompactionJobDescription {
                selected_layers,
                gc_cutoff,
                retain_lsns_below_horizon,
                min_layer_lsn,
                max_layer_lsn,
                compaction_key_range: compact_key_range,
                rewrite_layers,
            }
        };
        let (has_data_below, lowest_retain_lsn) = if compact_lsn_range.start != Lsn::INVALID {
            // If we only compact above some LSN, we should get the history from the current branch below the specified LSN.
            // We use job_desc.min_layer_lsn as if it's the lowest branch point.
            (true, job_desc.min_layer_lsn)
        } else if self.ancestor_timeline.is_some() {
            // In theory, we can also use min_layer_lsn here, but using ancestor LSN makes sure the delta layers cover the
            // LSN ranges all the way to the ancestor timeline.
            (true, self.ancestor_lsn)
        } else {
            let res = job_desc
                .retain_lsns_below_horizon
                .first()
                .copied()
                .unwrap_or(job_desc.gc_cutoff);
            if debug_mode {
                assert_eq!(
                    res,
                    job_desc
                        .retain_lsns_below_horizon
                        .iter()
                        .min()
                        .copied()
                        .unwrap_or(job_desc.gc_cutoff)
                );
            }
            (false, res)
        };
        info!(
            "picked {} layers for compaction ({} layers need rewriting) with max_layer_lsn={} min_layer_lsn={} gc_cutoff={} lowest_retain_lsn={}, key_range={}..{}, has_data_below={}",
            job_desc.selected_layers.len(),
            job_desc.rewrite_layers.len(),
            job_desc.max_layer_lsn,
            job_desc.min_layer_lsn,
            job_desc.gc_cutoff,
            lowest_retain_lsn,
            job_desc.compaction_key_range.start,
            job_desc.compaction_key_range.end,
            has_data_below,
        );

        for layer in &job_desc.selected_layers {
            debug!("read layer: {}", layer.layer_desc().key());
        }
        for layer in &job_desc.rewrite_layers {
            debug!("rewrite layer: {}", layer.key());
        }

        self.check_compaction_space(&job_desc.selected_layers)
            .await?;

        // Generate statistics for the compaction
        for layer in &job_desc.selected_layers {
            let desc = layer.layer_desc();
            if desc.is_delta() {
                stat.visit_delta_layer(desc.file_size());
            } else {
                stat.visit_image_layer(desc.file_size());
            }
        }

        // Step 1: construct a k-merge iterator over all layers.
        // Also, verify if the layer map can be split by drawing a horizontal line at every LSN start/end split point.
        let layer_names = job_desc
            .selected_layers
            .iter()
            .map(|layer| layer.layer_desc().layer_name())
            .collect_vec();
        if let Some(err) = check_valid_layermap(&layer_names) {
            bail!("gc-compaction layer map check failed because {}, cannot proceed with compaction due to potential data loss", err);
        }
        // The maximum LSN we are processing in this compaction loop
        let end_lsn = job_desc
            .selected_layers
            .iter()
            .map(|l| l.layer_desc().lsn_range.end)
            .max()
            .unwrap();
        let mut delta_layers = Vec::new();
        let mut image_layers = Vec::new();
        let mut downloaded_layers = Vec::new();
        let mut total_downloaded_size = 0;
        let mut total_layer_size = 0;
        for layer in &job_desc.selected_layers {
            if layer.needs_download().await?.is_some() {
                total_downloaded_size += layer.layer_desc().file_size;
            }
            total_layer_size += layer.layer_desc().file_size;
            let resident_layer = layer.download_and_keep_resident().await?;
            downloaded_layers.push(resident_layer);
        }
        info!(
            "finish downloading layers, downloaded={}, total={}, ratio={:.2}",
            total_downloaded_size,
            total_layer_size,
            total_downloaded_size as f64 / total_layer_size as f64
        );
        for resident_layer in &downloaded_layers {
            if resident_layer.layer_desc().is_delta() {
                let layer = resident_layer.get_as_delta(ctx).await?;
                delta_layers.push(layer);
            } else {
                let layer = resident_layer.get_as_image(ctx).await?;
                image_layers.push(layer);
            }
        }
        let (dense_ks, sparse_ks) = self.collect_gc_compaction_keyspace().await?;
        let mut merge_iter = FilterIterator::create(
            MergeIterator::create(&delta_layers, &image_layers, ctx),
            dense_ks,
            sparse_ks,
        )?;

        // Step 2: Produce images+deltas.
        let mut accumulated_values = Vec::new();
        let mut last_key: Option<Key> = None;

        // Only create image layers when there is no ancestor branches. TODO: create covering image layer
        // when some condition meet.
        let mut image_layer_writer = if !has_data_below {
            Some(
                SplitImageLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_shard_id,
                    job_desc.compaction_key_range.start,
                    lowest_retain_lsn,
                    self.get_compaction_target_size(),
                    ctx,
                )
                .await?,
            )
        } else {
            None
        };

        let mut delta_layer_writer = SplitDeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_shard_id,
            lowest_retain_lsn..end_lsn,
            self.get_compaction_target_size(),
        )
        .await?;

        #[derive(Default)]
        struct RewritingLayers {
            before: Option<DeltaLayerWriter>,
            after: Option<DeltaLayerWriter>,
        }
        let mut delta_layer_rewriters = HashMap::<Arc<PersistentLayerKey>, RewritingLayers>::new();

        /// When compacting not at a bottom range (=`[0,X)`) of the root branch, we "have data below" (`has_data_below=true`).
        /// The two cases are compaction in ancestor branches and when `compact_lsn_range.start` is set.
        /// In those cases, we need to pull up data from below the LSN range we're compaction.
        ///
        /// This function unifies the cases so that later code doesn't have to think about it.
        ///
        /// Currently, we always get the ancestor image for each key in the child branch no matter whether the image
        /// is needed for reconstruction. This should be fixed in the future.
        ///
        /// Furthermore, we should do vectored get instead of a single get, or better, use k-merge for ancestor
        /// images.
        async fn get_ancestor_image(
            this_tline: &Arc<Timeline>,
            key: Key,
            ctx: &RequestContext,
            has_data_below: bool,
            history_lsn_point: Lsn,
        ) -> anyhow::Result<Option<(Key, Lsn, Bytes)>> {
            if !has_data_below {
                return Ok(None);
            };
            // This function is implemented as a get of the current timeline at ancestor LSN, therefore reusing
            // as much existing code as possible.
            let img = this_tline.get(key, history_lsn_point, ctx).await?;
            Ok(Some((key, history_lsn_point, img)))
        }

        // Actually, we can decide not to write to the image layer at all at this point because
        // the key and LSN range are determined. However, to keep things simple here, we still
        // create this writer, and discard the writer in the end.

        while let Some(((key, lsn, val), desc)) = merge_iter.next_with_trace().await? {
            if cancel.is_cancelled() {
                return Err(anyhow!("cancelled")); // TODO: refactor to CompactionError and pass cancel error
            }
            if self.shard_identity.is_key_disposable(&key) {
                // If this shard does not need to store this key, simply skip it.
                //
                // This is not handled in the filter iterator because shard is determined by hash.
                // Therefore, it does not give us any performance benefit to do things like skip
                // a whole layer file as handling key spaces (ranges).
                if cfg!(debug_assertions) {
                    let shard = self.shard_identity.shard_index();
                    let owner = self.shard_identity.get_shard_number(&key);
                    panic!("key {key} does not belong on shard {shard}, owned by {owner}");
                }
                continue;
            }
            if !job_desc.compaction_key_range.contains(&key) {
                if !desc.is_delta {
                    continue;
                }
                let rewriter = delta_layer_rewriters.entry(desc.clone()).or_default();
                let rewriter = if key < job_desc.compaction_key_range.start {
                    if rewriter.before.is_none() {
                        rewriter.before = Some(
                            DeltaLayerWriter::new(
                                self.conf,
                                self.timeline_id,
                                self.tenant_shard_id,
                                desc.key_range.start,
                                desc.lsn_range.clone(),
                                ctx,
                            )
                            .await?,
                        );
                    }
                    rewriter.before.as_mut().unwrap()
                } else if key >= job_desc.compaction_key_range.end {
                    if rewriter.after.is_none() {
                        rewriter.after = Some(
                            DeltaLayerWriter::new(
                                self.conf,
                                self.timeline_id,
                                self.tenant_shard_id,
                                job_desc.compaction_key_range.end,
                                desc.lsn_range.clone(),
                                ctx,
                            )
                            .await?,
                        );
                    }
                    rewriter.after.as_mut().unwrap()
                } else {
                    unreachable!()
                };
                rewriter.put_value(key, lsn, val, ctx).await?;
                continue;
            }
            match val {
                Value::Image(_) => stat.visit_image_key(&val),
                Value::WalRecord(_) => stat.visit_wal_key(&val),
            }
            if last_key.is_none() || last_key.as_ref() == Some(&key) {
                if last_key.is_none() {
                    last_key = Some(key);
                }
                accumulated_values.push((key, lsn, val));
            } else {
                let last_key: &mut Key = last_key.as_mut().unwrap();
                stat.on_unique_key_visited(); // TODO: adjust statistics for partial compaction
                let retention = self
                    .generate_key_retention(
                        *last_key,
                        &accumulated_values,
                        job_desc.gc_cutoff,
                        &job_desc.retain_lsns_below_horizon,
                        COMPACTION_DELTA_THRESHOLD,
                        get_ancestor_image(self, *last_key, ctx, has_data_below, lowest_retain_lsn)
                            .await?,
                    )
                    .await?;
                retention
                    .pipe_to(
                        *last_key,
                        &mut delta_layer_writer,
                        image_layer_writer.as_mut(),
                        &mut stat,
                        ctx,
                    )
                    .await?;
                accumulated_values.clear();
                *last_key = key;
                accumulated_values.push((key, lsn, val));
            }
        }

        // TODO: move the below part to the loop body
        let last_key = last_key.expect("no keys produced during compaction");
        stat.on_unique_key_visited();

        let retention = self
            .generate_key_retention(
                last_key,
                &accumulated_values,
                job_desc.gc_cutoff,
                &job_desc.retain_lsns_below_horizon,
                COMPACTION_DELTA_THRESHOLD,
                get_ancestor_image(self, last_key, ctx, has_data_below, lowest_retain_lsn).await?,
            )
            .await?;
        retention
            .pipe_to(
                last_key,
                &mut delta_layer_writer,
                image_layer_writer.as_mut(),
                &mut stat,
                ctx,
            )
            .await?;
        // end: move the above part to the loop body

        let mut rewrote_delta_layers = Vec::new();
        for (key, writers) in delta_layer_rewriters {
            if let Some(delta_writer_before) = writers.before {
                let (desc, path) = delta_writer_before
                    .finish(job_desc.compaction_key_range.start, ctx)
                    .await?;
                let layer = Layer::finish_creating(self.conf, self, desc, &path)?;
                rewrote_delta_layers.push(layer);
            }
            if let Some(delta_writer_after) = writers.after {
                let (desc, path) = delta_writer_after.finish(key.key_range.end, ctx).await?;
                let layer = Layer::finish_creating(self.conf, self, desc, &path)?;
                rewrote_delta_layers.push(layer);
            }
        }

        let discard = |key: &PersistentLayerKey| {
            let key = key.clone();
            async move { KeyHistoryRetention::discard_key(&key, self, dry_run).await }
        };

        let produced_image_layers = if let Some(writer) = image_layer_writer {
            if !dry_run {
                let end_key = job_desc.compaction_key_range.end;
                writer
                    .finish_with_discard_fn(self, ctx, end_key, discard)
                    .await?
            } else {
                drop(writer);
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let produced_delta_layers = if !dry_run {
            delta_layer_writer
                .finish_with_discard_fn(self, ctx, discard)
                .await?
        } else {
            drop(delta_layer_writer);
            Vec::new()
        };

        // TODO: make image/delta/rewrote_delta layers generation atomic. At this point, we already generated resident layers, and if
        // compaction is cancelled at this point, we might have some layers that are not cleaned up.
        let mut compact_to = Vec::new();
        let mut keep_layers = HashSet::new();
        let produced_delta_layers_len = produced_delta_layers.len();
        let produced_image_layers_len = produced_image_layers.len();
        for action in produced_delta_layers {
            match action {
                BatchWriterResult::Produced(layer) => {
                    if cfg!(debug_assertions) {
                        info!("produced delta layer: {}", layer.layer_desc().key());
                    }
                    stat.produce_delta_layer(layer.layer_desc().file_size());
                    compact_to.push(layer);
                }
                BatchWriterResult::Discarded(l) => {
                    if cfg!(debug_assertions) {
                        info!("discarded delta layer: {}", l);
                    }
                    keep_layers.insert(l);
                    stat.discard_delta_layer();
                }
            }
        }
        for layer in &rewrote_delta_layers {
            debug!(
                "produced rewritten delta layer: {}",
                layer.layer_desc().key()
            );
        }
        compact_to.extend(rewrote_delta_layers);
        for action in produced_image_layers {
            match action {
                BatchWriterResult::Produced(layer) => {
                    debug!("produced image layer: {}", layer.layer_desc().key());
                    stat.produce_image_layer(layer.layer_desc().file_size());
                    compact_to.push(layer);
                }
                BatchWriterResult::Discarded(l) => {
                    debug!("discarded image layer: {}", l);
                    keep_layers.insert(l);
                    stat.discard_image_layer();
                }
            }
        }

        let mut layer_selection = job_desc.selected_layers;

        // Partial compaction might select more data than it processes, e.g., if
        // the compaction_key_range only partially overlaps:
        //
        //         [---compaction_key_range---]
        //   [---A----][----B----][----C----][----D----]
        //
        // For delta layers, we will rewrite the layers so that it is cut exactly at
        // the compaction key range, so we can always discard them. However, for image
        // layers, as we do not rewrite them for now, we need to handle them differently.
        // Assume image layers  A, B, C, D are all in the `layer_selection`.
        //
        // The created image layers contain whatever is needed from B, C, and from
        // `----]` of A, and from  `[---` of D.
        //
        // In contrast, `[---A` and `D----]` have not been processed, so, we must
        // keep that data.
        //
        // The solution for now is to keep A and D completely if they are image layers.
        // (layer_selection is what we'll remove from the layer map, so, retain what
        // is _not_ fully covered by compaction_key_range).
        for layer in &layer_selection {
            if !layer.layer_desc().is_delta() {
                if !overlaps_with(
                    &layer.layer_desc().key_range,
                    &job_desc.compaction_key_range,
                ) {
                    bail!("violated constraint: image layer outside of compaction key range");
                }
                if !fully_contains(
                    &job_desc.compaction_key_range,
                    &layer.layer_desc().key_range,
                ) {
                    keep_layers.insert(layer.layer_desc().key());
                }
            }
        }

        layer_selection.retain(|x| !keep_layers.contains(&x.layer_desc().key()));

        info!(
            "gc-compaction statistics: {}",
            serde_json::to_string(&stat)?
        );

        if dry_run {
            return Ok(());
        }

        info!(
            "produced {} delta layers and {} image layers, {} layers are kept",
            produced_delta_layers_len,
            produced_image_layers_len,
            keep_layers.len()
        );

        // Step 3: Place back to the layer map.

        // First, do a sanity check to ensure the newly-created layer map does not contain overlaps.
        let all_layers = {
            let guard = self.layers.read().await;
            let layer_map = guard.layer_map()?;
            layer_map.iter_historic_layers().collect_vec()
        };

        let mut final_layers = all_layers
            .iter()
            .map(|layer| layer.layer_name())
            .collect::<HashSet<_>>();
        for layer in &layer_selection {
            final_layers.remove(&layer.layer_desc().layer_name());
        }
        for layer in &compact_to {
            final_layers.insert(layer.layer_desc().layer_name());
        }
        let final_layers = final_layers.into_iter().collect_vec();

        // TODO: move this check before we call `finish` on image layer writers. However, this will require us to get the layer name before we finish
        // the writer, so potentially, we will need a function like `ImageLayerBatchWriter::get_all_pending_layer_keys` to get all the keys that are
        // in the writer before finalizing the persistent layers. Now we would leave some dangling layers on the disk if the check fails.
        if let Some(err) = check_valid_layermap(&final_layers) {
            bail!("gc-compaction layer map check failed after compaction because {}, compaction result not applied to the layer map due to potential data loss", err);
        }

        // Between the sanity check and this compaction update, there could be new layers being flushed, but it should be fine because we only
        // operate on L1 layers.
        {
            // Gc-compaction will rewrite the history of a key. This could happen in two ways:
            //
            // 1. We create an image layer to replace all the deltas below the compact LSN. In this case, assume
            // we have 2 delta layers A and B, both below the compact LSN. We create an image layer I to replace
            // A and B at the compact LSN. If the read path finishes reading A, yields, and now we update the layer
            // map, the read path then cannot find any keys below A, reporting a missing key error, while the key
            // now gets stored in I at the compact LSN.
            //
            // ---------------                                       ---------------
            //   delta1@LSN20                                         image1@LSN20
            // ---------------  (read path collects delta@LSN20,  => ---------------  (read path cannot find anything
            //   delta1@LSN10    yields)                                               below LSN 20)
            // ---------------
            //
            // 2. We create a delta layer to replace all the deltas below the compact LSN, and in the delta layers,
            // we combines the history of a key into a single image. For example, we have deltas at LSN 1, 2, 3, 4,
            // Assume one delta layer contains LSN 1, 2, 3 and the other contains LSN 4.
            //
            // We let gc-compaction combine delta 2, 3, 4 into an image at LSN 4, which produces a delta layer that
            // contains the delta at LSN 1, the image at LSN 4. If the read path finishes reading the original delta
            // layer containing 4, yields, and we update the layer map to put the delta layer.
            //
            // ---------------                                      ---------------
            //   delta1@LSN4                                          image1@LSN4
            // ---------------  (read path collects delta@LSN4,  => ---------------  (read path collects LSN4 and LSN1,
            //  delta1@LSN1-3    yields)                              delta1@LSN1     which is an invalid history)
            // ---------------                                      ---------------
            //
            // Therefore, the gc-compaction layer update operation should wait for all ongoing reads, block all pending reads,
            // and only allow reads to continue after the update is finished.

            let update_guard = self.gc_compaction_layer_update_lock.write().await;
            // Acquiring the update guard ensures current read operations end and new read operations are blocked.
            // TODO: can we use `latest_gc_cutoff` Rcu to achieve the same effect?
            let mut guard = self.layers.write().await;
            guard
                .open_mut()?
                .finish_gc_compaction(&layer_selection, &compact_to, &self.metrics);
            drop(update_guard); // Allow new reads to start ONLY after we finished updating the layer map.
        };

        // Schedule an index-only upload to update the `latest_gc_cutoff` in the index_part.json.
        // Otherwise, after restart, the index_part only contains the old `latest_gc_cutoff` and
        // find_gc_cutoffs will try accessing things below the cutoff. TODO: ideally, this should
        // be batched into `schedule_compaction_update`.
        let disk_consistent_lsn = self.disk_consistent_lsn.load();
        self.schedule_uploads(disk_consistent_lsn, None)?;
        // If a layer gets rewritten throughout gc-compaction, we need to keep that layer only in `compact_to` instead
        // of `compact_from`.
        let compact_from = {
            let mut compact_from = Vec::new();
            let mut compact_to_set = HashMap::new();
            for layer in &compact_to {
                compact_to_set.insert(layer.layer_desc().key(), layer);
            }
            for layer in &layer_selection {
                if let Some(to) = compact_to_set.get(&layer.layer_desc().key()) {
                    tracing::info!(
                        "skipping delete {} because found same layer key at different generation {}",
                        layer, to
                    );
                } else {
                    compact_from.push(layer.clone());
                }
            }
            compact_from
        };
        self.remote_client
            .schedule_compaction_update(&compact_from, &compact_to)?;

        drop(gc_lock);

        Ok(())
    }
}

struct TimelineAdaptor {
    timeline: Arc<Timeline>,

    keyspace: (Lsn, KeySpace),

    new_deltas: Vec<ResidentLayer>,
    new_images: Vec<ResidentLayer>,
    layers_to_delete: Vec<Arc<PersistentLayerDesc>>,
}

impl TimelineAdaptor {
    pub fn new(timeline: &Arc<Timeline>, keyspace: (Lsn, KeySpace)) -> Self {
        Self {
            timeline: timeline.clone(),
            keyspace,
            new_images: Vec::new(),
            new_deltas: Vec::new(),
            layers_to_delete: Vec::new(),
        }
    }

    pub async fn flush_updates(&mut self) -> Result<(), CompactionError> {
        let layers_to_delete = {
            let guard = self.timeline.layers.read().await;
            self.layers_to_delete
                .iter()
                .map(|x| guard.get_from_desc(x))
                .collect::<Vec<Layer>>()
        };
        self.timeline
            .finish_compact_batch(&self.new_deltas, &self.new_images, &layers_to_delete)
            .await?;

        self.timeline
            .upload_new_image_layers(std::mem::take(&mut self.new_images))?;

        self.new_deltas.clear();
        self.layers_to_delete.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct ResidentDeltaLayer(ResidentLayer);
#[derive(Clone)]
struct ResidentImageLayer(ResidentLayer);

impl CompactionJobExecutor for TimelineAdaptor {
    type Key = pageserver_api::key::Key;

    type Layer = OwnArc<PersistentLayerDesc>;
    type DeltaLayer = ResidentDeltaLayer;
    type ImageLayer = ResidentImageLayer;

    type RequestContext = crate::context::RequestContext;

    fn get_shard_identity(&self) -> &ShardIdentity {
        self.timeline.get_shard_identity()
    }

    async fn get_layers(
        &mut self,
        key_range: &Range<Key>,
        lsn_range: &Range<Lsn>,
        _ctx: &RequestContext,
    ) -> anyhow::Result<Vec<OwnArc<PersistentLayerDesc>>> {
        self.flush_updates().await?;

        let guard = self.timeline.layers.read().await;
        let layer_map = guard.layer_map()?;

        let result = layer_map
            .iter_historic_layers()
            .filter(|l| {
                overlaps_with(&l.lsn_range, lsn_range) && overlaps_with(&l.key_range, key_range)
            })
            .map(OwnArc)
            .collect();
        Ok(result)
    }

    async fn get_keyspace(
        &mut self,
        key_range: &Range<Key>,
        lsn: Lsn,
        _ctx: &RequestContext,
    ) -> anyhow::Result<Vec<Range<Key>>> {
        if lsn == self.keyspace.0 {
            Ok(pageserver_compaction::helpers::intersect_keyspace(
                &self.keyspace.1.ranges,
                key_range,
            ))
        } else {
            // The current compaction implementation only ever requests the key space
            // at the compaction end LSN.
            anyhow::bail!("keyspace not available for requested lsn");
        }
    }

    async fn downcast_delta_layer(
        &self,
        layer: &OwnArc<PersistentLayerDesc>,
    ) -> anyhow::Result<Option<ResidentDeltaLayer>> {
        // this is a lot more complex than a simple downcast...
        if layer.is_delta() {
            let l = {
                let guard = self.timeline.layers.read().await;
                guard.get_from_desc(layer)
            };
            let result = l.download_and_keep_resident().await?;

            Ok(Some(ResidentDeltaLayer(result)))
        } else {
            Ok(None)
        }
    }

    async fn create_image(
        &mut self,
        lsn: Lsn,
        key_range: &Range<Key>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        Ok(self.create_image_impl(lsn, key_range, ctx).await?)
    }

    async fn create_delta(
        &mut self,
        lsn_range: &Range<Lsn>,
        key_range: &Range<Key>,
        input_layers: &[ResidentDeltaLayer],
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        debug!("Create new layer {}..{}", lsn_range.start, lsn_range.end);

        let mut all_entries = Vec::new();
        for dl in input_layers.iter() {
            all_entries.extend(dl.load_keys(ctx).await?);
        }

        // The current stdlib sorting implementation is designed in a way where it is
        // particularly fast where the slice is made up of sorted sub-ranges.
        all_entries.sort_by_key(|DeltaEntry { key, lsn, .. }| (*key, *lsn));

        let mut writer = DeltaLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            key_range.start,
            lsn_range.clone(),
            ctx,
        )
        .await?;

        let mut dup_values = 0;

        // This iterator walks through all key-value pairs from all the layers
        // we're compacting, in key, LSN order.
        let mut prev: Option<(Key, Lsn)> = None;
        for &DeltaEntry {
            key, lsn, ref val, ..
        } in all_entries.iter()
        {
            if prev == Some((key, lsn)) {
                // This is a duplicate. Skip it.
                //
                // It can happen if compaction is interrupted after writing some
                // layers but not all, and we are compacting the range again.
                // The calculations in the algorithm assume that there are no
                // duplicates, so the math on targeted file size is likely off,
                // and we will create smaller files than expected.
                dup_values += 1;
                continue;
            }

            let value = val.load(ctx).await?;

            writer.put_value(key, lsn, value, ctx).await?;

            prev = Some((key, lsn));
        }

        if dup_values > 0 {
            warn!("delta layer created with {} duplicate values", dup_values);
        }

        fail_point!("delta-layer-writer-fail-before-finish", |_| {
            Err(anyhow::anyhow!(
                "failpoint delta-layer-writer-fail-before-finish"
            ))
        });

        let (desc, path) = writer.finish(prev.unwrap().0.next(), ctx).await?;
        let new_delta_layer =
            Layer::finish_creating(self.timeline.conf, &self.timeline, desc, &path)?;

        self.new_deltas.push(new_delta_layer);
        Ok(())
    }

    async fn delete_layer(
        &mut self,
        layer: &OwnArc<PersistentLayerDesc>,
        _ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.layers_to_delete.push(layer.clone().0);
        Ok(())
    }
}

impl TimelineAdaptor {
    async fn create_image_impl(
        &mut self,
        lsn: Lsn,
        key_range: &Range<Key>,
        ctx: &RequestContext,
    ) -> Result<(), CreateImageLayersError> {
        let timer = self.timeline.metrics.create_images_time_histo.start_timer();

        let image_layer_writer = ImageLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            key_range,
            lsn,
            ctx,
        )
        .await?;

        fail_point!("image-layer-writer-fail-before-finish", |_| {
            Err(CreateImageLayersError::Other(anyhow::anyhow!(
                "failpoint image-layer-writer-fail-before-finish"
            )))
        });

        let keyspace = KeySpace {
            ranges: self.get_keyspace(key_range, lsn, ctx).await?,
        };
        // TODO set proper (stateful) start. The create_image_layer_for_rel_blocks function mostly
        let outcome = self
            .timeline
            .create_image_layer_for_rel_blocks(
                &keyspace,
                image_layer_writer,
                lsn,
                ctx,
                key_range.clone(),
                IoConcurrency::sequential(),
            )
            .await?;

        if let ImageLayerCreationOutcome::Generated {
            unfinished_image_layer,
        } = outcome
        {
            let (desc, path) = unfinished_image_layer.finish(ctx).await?;
            let image_layer =
                Layer::finish_creating(self.timeline.conf, &self.timeline, desc, &path)?;
            self.new_images.push(image_layer);
        }

        timer.stop_and_record();

        Ok(())
    }
}

impl CompactionRequestContext for crate::context::RequestContext {}

#[derive(Debug, Clone)]
pub struct OwnArc<T>(pub Arc<T>);

impl<T> Deref for OwnArc<T> {
    type Target = <Arc<T> as Deref>::Target;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for OwnArc<T> {
    fn as_ref(&self) -> &T {
        self.0.as_ref()
    }
}

impl CompactionLayer<Key> for OwnArc<PersistentLayerDesc> {
    fn key_range(&self) -> &Range<Key> {
        &self.key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.lsn_range
    }
    fn file_size(&self) -> u64 {
        self.file_size
    }
    fn short_id(&self) -> std::string::String {
        self.as_ref().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        self.as_ref().is_delta()
    }
}

impl CompactionLayer<Key> for OwnArc<DeltaLayer> {
    fn key_range(&self) -> &Range<Key> {
        &self.layer_desc().key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.layer_desc().lsn_range
    }
    fn file_size(&self) -> u64 {
        self.layer_desc().file_size
    }
    fn short_id(&self) -> std::string::String {
        self.layer_desc().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        true
    }
}

use crate::tenant::timeline::DeltaEntry;

impl CompactionLayer<Key> for ResidentDeltaLayer {
    fn key_range(&self) -> &Range<Key> {
        &self.0.layer_desc().key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.0.layer_desc().lsn_range
    }
    fn file_size(&self) -> u64 {
        self.0.layer_desc().file_size
    }
    fn short_id(&self) -> std::string::String {
        self.0.layer_desc().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        true
    }
}

impl CompactionDeltaLayer<TimelineAdaptor> for ResidentDeltaLayer {
    type DeltaEntry<'a> = DeltaEntry<'a>;

    async fn load_keys(&self, ctx: &RequestContext) -> anyhow::Result<Vec<DeltaEntry<'_>>> {
        self.0.get_as_delta(ctx).await?.index_entries(ctx).await
    }
}

impl CompactionLayer<Key> for ResidentImageLayer {
    fn key_range(&self) -> &Range<Key> {
        &self.0.layer_desc().key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.0.layer_desc().lsn_range
    }
    fn file_size(&self) -> u64 {
        self.0.layer_desc().file_size
    }
    fn short_id(&self) -> std::string::String {
        self.0.layer_desc().short_id().to_string()
    }
    fn is_delta(&self) -> bool {
        false
    }
}
impl CompactionImageLayer<TimelineAdaptor> for ResidentImageLayer {}
