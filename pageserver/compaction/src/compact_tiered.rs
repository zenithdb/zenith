//! # Tiered compaction algorithm.
//!
//! Read all the input delta files, and write a new set of delta files that
//! include all the input WAL records. See retile_deltas().
//!
//! In a "normal" LSM tree, you get to remove any values that are overwritten by
//! later values, but in our system, we keep all the history. So the reshuffling
//! doesn't remove any garbage, it just reshuffles the records to reduce read
//! amplification, i.e. the number of files that you need to access to find the
//! WAL records for a given key.
//!
//! If the new delta files would be very "narrow", i.e. each file would cover
//! only a narrow key range, then we create a new set of image files
//! instead. The current threshold is that if the estimated total size of the
//! image layers is smaller than the size of the deltas, then we create image
//! layers. That amounts to 2x storage amplification, and it means that the
//! distance of image layers in LSN dimension is roughly equal to the logical
//! database size. For example, if the logical database size is 10 GB, we would
//! generate new image layers every 10 GB of WAL.
use futures::StreamExt;
use pageserver_api::shard::ShardIdentity;
use tracing::{debug, info};

use std::collections::{HashSet, VecDeque};
use std::ops::Range;

use crate::helpers::{
    accum_key_values, keyspace_total_size, merge_delta_keys_buffered, overlaps_with, PAGE_SZ,
};
use crate::interface::*;
use utils::lsn::Lsn;

use crate::identify_levels::identify_level;

/// Main entry point to compaction.
///
/// The starting point is a cutoff LSN (`end_lsn`). The compaction is run on
/// everything below that point, that needs compaction. The cutoff LSN must
/// partition the layers so that there are no layers that span across that
/// LSN. To start compaction at the top of the tree, pass the end LSN of the
/// written last L0 layer.
pub async fn compact_tiered<E: CompactionJobExecutor>(
    executor: &mut E,
    end_lsn: Lsn,
    target_file_size: u64,
    fanout: u64,
    ctx: &E::RequestContext,
) -> anyhow::Result<()> {
    assert!(fanout >= 1, "fanout needs to be at least 1 but is {fanout}");
    let exp_base = fanout.max(2);
    // Start at L0
    let mut current_level_no = 0;
    let mut current_level_target_height = target_file_size;
    loop {
        // end LSN +1 to include possible image layers exactly at 'end_lsn'.
        let all_layers = executor
            .get_layers(
                &(E::Key::MIN..E::Key::MAX),
                &(Lsn(u64::MIN)..end_lsn + 1),
                ctx,
            )
            .await?;
        info!(
            "Compacting L{}, total # of layers: {}",
            current_level_no,
            all_layers.len()
        );

        // Identify the range of LSNs that belong to this level. We assume that
        // each file in this level spans an LSN range up to 1.75x target file
        // size. That should give us enough slop that if we created a slightly
        // oversized L0 layer, e.g. because flushing the in-memory layer was
        // delayed for some reason, we don't consider the oversized layer to
        // belong to L1. But not too much slop, that we don't accidentally
        // "skip" levels.
        let max_height = (current_level_target_height as f64 * 1.75) as u64;
        let Some(level) = identify_level(all_layers, end_lsn, max_height).await? else {
            break;
        };

        // Calculate the height of this level. If the # of tiers exceeds the
        // fanout parameter, it's time to compact it.
        let depth = level.depth();
        info!(
            "Level {} identified as LSN range {}-{}: depth {}",
            current_level_no, level.lsn_range.start, level.lsn_range.end, depth
        );
        for l in &level.layers {
            debug!("LEVEL {} layer: {}", current_level_no, l.short_id());
        }
        if depth < fanout {
            debug!(
                level = current_level_no,
                depth = depth,
                fanout,
                "too few deltas to compact"
            );
            break;
        }

        compact_level(
            &level.lsn_range,
            &level.layers,
            executor,
            target_file_size,
            ctx,
        )
        .await?;
        if current_level_target_height == u64::MAX {
            // our target height includes all possible lsns
            info!(
                level = current_level_no,
                depth = depth,
                "compaction loop reached max current_level_target_height"
            );
            break;
        }
        current_level_no += 1;
        current_level_target_height = current_level_target_height.saturating_mul(exp_base);
    }
    Ok(())
}

async fn compact_level<E: CompactionJobExecutor>(
    lsn_range: &Range<Lsn>,
    layers: &[E::Layer],
    executor: &mut E,
    target_file_size: u64,
    ctx: &E::RequestContext,
) -> anyhow::Result<bool> {
    let mut layer_fragments = Vec::new();
    for l in layers {
        layer_fragments.push(LayerFragment::new(l.clone()));
    }

    let mut state = LevelCompactionState {
        shard_identity: *executor.get_shard_identity(),
        target_file_size,
        _lsn_range: lsn_range.clone(),
        layers: layer_fragments,
        jobs: Vec::new(),
        job_queue: Vec::new(),
        next_level: false,
        executor,
    };

    let first_job = CompactionJob {
        key_range: E::Key::MIN..E::Key::MAX,
        lsn_range: lsn_range.clone(),
        strategy: CompactionStrategy::Divide,
        input_layers: state
            .layers
            .iter()
            .enumerate()
            .map(|i| LayerId(i.0))
            .collect(),
        completed: false,
    };

    state.jobs.push(first_job);
    state.job_queue.push(JobId(0));
    state.execute(ctx).await?;

    info!(
        "compaction completed! Need to process next level: {}",
        state.next_level
    );

    Ok(state.next_level)
}

/// Blackboard that keeps track of the state of all the jobs and work remaining
struct LevelCompactionState<'a, E>
where
    E: CompactionJobExecutor,
{
    shard_identity: ShardIdentity,

    // parameters
    target_file_size: u64,

    _lsn_range: Range<Lsn>,
    layers: Vec<LayerFragment<E>>,

    // job queue
    jobs: Vec<CompactionJob<E>>,
    job_queue: Vec<JobId>,

    /// If false, no need to compact levels below this
    next_level: bool,

    /// Interface to the outside world
    executor: &'a mut E,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct LayerId(usize);
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
struct JobId(usize);

struct PendingJobSet {
    pending: HashSet<JobId>,
    completed: HashSet<JobId>,
}

impl PendingJobSet {
    fn new() -> Self {
        PendingJobSet {
            pending: HashSet::new(),
            completed: HashSet::new(),
        }
    }

    fn complete_job(&mut self, job_id: JobId) {
        self.pending.remove(&job_id);
        self.completed.insert(job_id);
    }

    fn all_completed(&self) -> bool {
        self.pending.is_empty()
    }
}

// When we decide to rewrite a set of layers, LayerFragment is used to keep
// track which new layers supersede an old layer. When all the stakeholder jobs
// have completed, this layer can be deleted.
struct LayerFragment<E>
where
    E: CompactionJobExecutor,
{
    layer: E::Layer,

    // If we will write new layers to replace this one, this keeps track of the
    // jobs that need to complete before this layer can be deleted. As the jobs
    // complete, they are moved from 'pending' to 'completed' set. Once the
    // 'pending' set becomes empty, the layer can be deleted.
    //
    // If None, this layer is not rewritten and must not be deleted.
    deletable_after: Option<PendingJobSet>,

    deleted: bool,
}

impl<E> LayerFragment<E>
where
    E: CompactionJobExecutor,
{
    fn new(layer: E::Layer) -> Self {
        LayerFragment {
            layer,
            deletable_after: None,
            deleted: false,
        }
    }
}

#[derive(PartialEq)]
enum CompactionStrategy {
    Divide,
    CreateDelta,
    CreateImage,
}

struct CompactionJob<E: CompactionJobExecutor> {
    key_range: Range<E::Key>,
    lsn_range: Range<Lsn>,

    strategy: CompactionStrategy,

    input_layers: Vec<LayerId>,

    completed: bool,
}

impl<E> LevelCompactionState<'_, E>
where
    E: CompactionJobExecutor,
{
    /// Main loop of the executor.
    ///
    /// In each iteration, we take the next job from the queue, and execute it.
    /// The execution might add new jobs to the queue. Keep going until the
    /// queue is empty.
    ///
    /// Initially, the job queue consists of one Divide job over the whole
    /// level. On first call, it is divided into smaller jobs.
    async fn execute(&mut self, ctx: &E::RequestContext) -> anyhow::Result<()> {
        // TODO: this would be pretty straightforward to parallelize with FuturesUnordered
        while let Some(next_job_id) = self.job_queue.pop() {
            info!("executing job {}", next_job_id.0);
            self.execute_job(next_job_id, ctx).await?;
        }

        // all done!
        Ok(())
    }

    async fn execute_job(&mut self, job_id: JobId, ctx: &E::RequestContext) -> anyhow::Result<()> {
        let job = &self.jobs[job_id.0];
        match job.strategy {
            CompactionStrategy::Divide => {
                self.divide_job(job_id, ctx).await?;
                Ok(())
            }
            CompactionStrategy::CreateDelta => {
                let mut deltas: Vec<E::DeltaLayer> = Vec::new();
                let mut layer_ids: Vec<LayerId> = Vec::new();
                for layer_id in &job.input_layers {
                    let layer = &self.layers[layer_id.0].layer;
                    if let Some(dl) = self.executor.downcast_delta_layer(layer).await? {
                        deltas.push(dl.clone());
                        layer_ids.push(*layer_id);
                    }
                }

                self.executor
                    .create_delta(&job.lsn_range, &job.key_range, &deltas, ctx)
                    .await?;
                self.jobs[job_id.0].completed = true;

                // did we complete any fragments?
                for layer_id in layer_ids {
                    let l = &mut self.layers[layer_id.0];
                    if let Some(deletable_after) = l.deletable_after.as_mut() {
                        deletable_after.complete_job(job_id);
                        if deletable_after.all_completed() {
                            self.executor.delete_layer(&l.layer, ctx).await?;
                            l.deleted = true;
                        }
                    }
                }

                self.next_level = true;

                Ok(())
            }
            CompactionStrategy::CreateImage => {
                self.executor
                    .create_image(job.lsn_range.end, &job.key_range, ctx)
                    .await?;
                self.jobs[job_id.0].completed = true;

                // TODO: we could check if any layers < PITR horizon became deletable
                Ok(())
            }
        }
    }

    fn push_job(&mut self, job: CompactionJob<E>) -> JobId {
        let job_id = JobId(self.jobs.len());
        self.jobs.push(job);
        self.job_queue.push(job_id);
        job_id
    }

    /// Take a partition of the key space, and decide how to compact it.
    ///
    /// TODO: Currently, this is called exactly once for the level, and we
    /// decide whether to create new image layers to cover the whole level, or
    /// write a new set of deltas. In the future, this should try to partition
    /// the key space, and make the decision separately for each partition.
    async fn divide_job(&mut self, job_id: JobId, ctx: &E::RequestContext) -> anyhow::Result<()> {
        let job = &self.jobs[job_id.0];
        assert!(job.strategy == CompactionStrategy::Divide);

        // Check for dummy cases
        if job.input_layers.is_empty() {
            return Ok(());
        }

        let job = &self.jobs[job_id.0];
        assert!(job.strategy == CompactionStrategy::Divide);

        // Would it be better to create images for this partition?
        // Decide based on the average density of the level
        let keyspace_size = keyspace_total_size(
            &self
                .executor
                .get_keyspace(&job.key_range, job.lsn_range.end, ctx)
                .await?,
            &self.shard_identity,
        ) * PAGE_SZ;

        let wal_size = job
            .input_layers
            .iter()
            .filter(|layer_id| self.layers[layer_id.0].layer.is_delta())
            .map(|layer_id| self.layers[layer_id.0].layer.file_size())
            .sum::<u64>();
        if keyspace_size < wal_size {
            // seems worth it
            info!(
                "covering with images, because keyspace_size is {}, size of deltas between {}-{} is {}",
                keyspace_size, job.lsn_range.start, job.lsn_range.end, wal_size
            );
            self.cover_with_images(job_id, ctx).await
        } else {
            // do deltas
            info!(
                "coverage not worth it, keyspace_size {}, wal_size {}",
                keyspace_size, wal_size
            );
            self.retile_deltas(job_id, ctx).await
        }
    }

    // LSN
    //  ^
    //  |
    //  |                          ###|###|#####
    //  | +--+-----+--+            +--+-----+--+
    //  | |  |     |  |            |  |     |  |
    //  | +--+--+--+--+            +--+--+--+--+
    //  | |     |     |            |     |     |
    //  | +---+-+-+---+     ==>    +---+-+-+---+
    //  | |   |   |   |            |   |   |   |
    //  | +---+-+-++--+            +---+-+-++--+
    //  | |     |  |  |            |     |  |  |
    //  | +-----+--+--+            +-----+--+--+
    //  |
    //  +--------------> key
    //
    async fn cover_with_images(
        &mut self,
        job_id: JobId,
        ctx: &E::RequestContext,
    ) -> anyhow::Result<()> {
        let job = &self.jobs[job_id.0];
        assert!(job.strategy == CompactionStrategy::Divide);

        // XXX: do we still need the "holes" stuff?

        let mut new_jobs = Vec::new();

        // Slide a window through the keyspace
        let keyspace = self
            .executor
            .get_keyspace(&job.key_range, job.lsn_range.end, ctx)
            .await?;

        let mut window = KeyspaceWindow::new(
            E::Key::MIN..E::Key::MAX,
            keyspace,
            self.target_file_size / PAGE_SZ,
        );
        while let Some(key_range) = window.choose_next_image(&self.shard_identity) {
            new_jobs.push(CompactionJob::<E> {
                key_range,
                lsn_range: job.lsn_range.clone(),
                strategy: CompactionStrategy::CreateImage,
                input_layers: Vec::new(), // XXX: Is it OK for  this to be empty for image layer?
                completed: false,
            });
        }

        for j in new_jobs.into_iter().rev() {
            let _job_id = self.push_job(j);

            // TODO: image layers don't let us delete anything. unless < PITR horizon
            //let j = &self.jobs[job_id.0];
            // for layer_id in j.input_layers.iter() {
            //    self.layers[layer_id.0].pending_stakeholders.insert(job_id);
            //}
        }

        Ok(())
    }

    // Merge the contents of all the input delta layers into a new set
    // of delta layers, based on the current partitioning.
    //
    // We split the new delta layers on the key dimension. We iterate through
    // the key space, and for each key, check if including the next key to the
    // current output layer we're building would cause the layer to become too
    // large. If so, dump the current output layer and start new one.  It's
    // possible that there is a single key with so many page versions that
    // storing all of them in a single layer file would be too large. In that
    // case, we also split on the LSN dimension.
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
    //
    // TODO: this actually divides the layers into fixed-size chunks, not
    // based on the partitioning.
    //
    // TODO: we should also opportunistically materialize and
    // garbage collect what we can.
    async fn retile_deltas(
        &mut self,
        job_id: JobId,
        ctx: &E::RequestContext,
    ) -> anyhow::Result<()> {
        let job = &self.jobs[job_id.0];
        assert!(job.strategy == CompactionStrategy::Divide);

        // Sweep the key space left to right, running an estimate of how much
        // disk size and keyspace we have accumulated
        //
        // Once the disk size reaches the target threshold, stop and think.
        // If we have accumulated only a narrow band of keyspace, create an
        // image layer. Otherwise write a delta layer.

        // FIXME: we are ignoring images here. Did we already divide the work
        // so that we won't encounter them here?

        let mut deltas: Vec<E::DeltaLayer> = Vec::new();
        for layer_id in &job.input_layers {
            let l = &self.layers[layer_id.0];
            if let Some(dl) = self.executor.downcast_delta_layer(&l.layer).await? {
                deltas.push(dl.clone());
            }
        }
        // Open stream
        let key_value_stream =
            std::pin::pin!(merge_delta_keys_buffered::<E>(deltas.as_slice(), ctx)
                .await?
                .map(Result::<_, anyhow::Error>::Ok));
        let mut new_jobs = Vec::new();

        // Slide a window through the keyspace
        let mut key_accum =
            std::pin::pin!(accum_key_values(key_value_stream, self.target_file_size));
        let mut all_in_window: bool = false;
        let mut window = Window::new();

        // Helper function to create a job for a new delta layer with given key-lsn
        // rectangle.
        let create_delta_job = |key_range, lsn_range: &Range<Lsn>, new_jobs: &mut Vec<_>| {
            // The inputs for the job are all the input layers of the original job that
            // overlap with the rectangle.
            let batch_layers: Vec<LayerId> = job
                .input_layers
                .iter()
                .filter(|layer_id| {
                    overlaps_with(self.layers[layer_id.0].layer.key_range(), &key_range)
                })
                .cloned()
                .collect();
            assert!(!batch_layers.is_empty());
            new_jobs.push(CompactionJob {
                key_range,
                lsn_range: lsn_range.clone(),
                strategy: CompactionStrategy::CreateDelta,
                input_layers: batch_layers,
                completed: false,
            });
        };

        loop {
            if all_in_window && window.is_empty() {
                // All done!
                break;
            }

            // If we now have enough keyspace for next delta layer in the window, create a
            // new delta layer
            if let Some(key_range) = window.choose_next_delta(self.target_file_size, !all_in_window)
            {
                create_delta_job(key_range, &job.lsn_range, &mut new_jobs);
                continue;
            }
            assert!(!all_in_window);

            // Process next key in the key space
            match key_accum.next().await.transpose()? {
                None => {
                    all_in_window = true;
                }
                Some(next_key) if next_key.partition_lsns.is_empty() => {
                    // Normal case: extend the window by the key
                    window.feed(next_key.key, next_key.size);
                }
                Some(next_key) => {
                    // A key with too large size impact for a single delta layer. This
                    // case occurs if you make a huge number of updates for a single key.
                    //
                    // Drain the window with has_more = false to make a clean cut before
                    // the key, and then make dedicated delta layers for the single key.
                    //
                    // We cannot cluster the key with the others, because we don't want
                    // layer files to overlap with each other in the lsn,key space (no
                    // overlaps for the rectangles).
                    let key = next_key.key;
                    debug!("key {key} with size impact larger than the layer size");
                    while !window.is_empty() {
                        let has_more = false;
                        let key_range = window.choose_next_delta(self.target_file_size, has_more)
                            .expect("with has_more==false, choose_next_delta always returns something for a non-empty Window");
                        create_delta_job(key_range, &job.lsn_range, &mut new_jobs);
                    }

                    // Not really required: but here for future resilience:
                    // We make a "gap" here, so any structure the window holds should
                    // probably be reset.
                    window = Window::new();

                    let mut prior_lsn = job.lsn_range.start;
                    let mut lsn_ranges = Vec::new();
                    for (lsn, _size) in next_key.partition_lsns.iter() {
                        lsn_ranges.push(prior_lsn..*lsn);
                        prior_lsn = *lsn;
                    }
                    lsn_ranges.push(prior_lsn..job.lsn_range.end);
                    for lsn_range in lsn_ranges {
                        let key_range = key..key.next();
                        create_delta_job(key_range, &lsn_range, &mut new_jobs);
                    }
                }
            }
        }

        // All the input files are rewritten. Set up the tracking for when they can
        // be deleted.
        for layer_id in job.input_layers.iter() {
            let l = &mut self.layers[layer_id.0];
            assert!(l.deletable_after.is_none());
            l.deletable_after = Some(PendingJobSet::new());
        }
        for j in new_jobs.into_iter().rev() {
            let job_id = self.push_job(j);
            let j = &self.jobs[job_id.0];
            for layer_id in j.input_layers.iter() {
                self.layers[layer_id.0]
                    .deletable_after
                    .as_mut()
                    .unwrap()
                    .pending
                    .insert(job_id);
            }
        }

        Ok(())
    }
}

/// Sliding window through keyspace and values for image layer
/// This is used by [`LevelCompactionState::cover_with_images`] to decide on good split points
struct KeyspaceWindow<K> {
    head: KeyspaceWindowHead<K>,

    start_pos: KeyspaceWindowPos<K>,
}
struct KeyspaceWindowHead<K> {
    // overall key range to cover
    key_range: Range<K>,

    keyspace: Vec<Range<K>>,
    target_keysize: u64,
}

#[derive(Clone)]
struct KeyspaceWindowPos<K> {
    end_key: K,

    keyspace_idx: usize,

    accum_keysize: u64,
}
impl<K: CompactionKey> KeyspaceWindowPos<K> {
    fn reached_end(&self, w: &KeyspaceWindowHead<K>) -> bool {
        self.keyspace_idx == w.keyspace.len()
    }

    // Advance the cursor until it reaches 'target_keysize'.
    fn advance_until_size(
        &mut self,
        w: &KeyspaceWindowHead<K>,
        max_size: u64,
        shard_identity: &ShardIdentity,
    ) {
        while self.accum_keysize < max_size && !self.reached_end(w) {
            let curr_range = &w.keyspace[self.keyspace_idx];
            if self.end_key < curr_range.start {
                // skip over any unused space
                self.end_key = curr_range.start;
            }

            // We're now within 'curr_range'. Can we advance past it completely?
            let distance = K::key_range_size(&(self.end_key..curr_range.end), shard_identity);
            if (self.accum_keysize + distance as u64) < max_size {
                // oh yeah, it fits
                self.end_key = curr_range.end;
                self.keyspace_idx += 1;
                self.accum_keysize += distance as u64;
            } else {
                // advance within the range
                let skip_key = self.end_key.skip_some();
                let distance = K::key_range_size(&(self.end_key..skip_key), shard_identity);
                if (self.accum_keysize + distance as u64) < max_size {
                    self.end_key = skip_key;
                    self.accum_keysize += distance as u64;
                } else {
                    self.end_key = self.end_key.next();
                    self.accum_keysize += 1;
                }
            }
        }
    }
}

impl<K> KeyspaceWindow<K>
where
    K: CompactionKey,
{
    fn new(key_range: Range<K>, keyspace: CompactionKeySpace<K>, target_keysize: u64) -> Self {
        assert!(keyspace.first().unwrap().start >= key_range.start);

        let start_key = key_range.start;
        let start_pos = KeyspaceWindowPos::<K> {
            end_key: start_key,
            keyspace_idx: 0,
            accum_keysize: 0,
        };
        Self {
            head: KeyspaceWindowHead::<K> {
                key_range,
                keyspace,
                target_keysize,
            },
            start_pos,
        }
    }

    fn choose_next_image(&mut self, shard_identity: &ShardIdentity) -> Option<Range<K>> {
        if self.start_pos.keyspace_idx == self.head.keyspace.len() {
            // we've reached the end
            return None;
        }

        let mut next_pos = self.start_pos.clone();
        next_pos.advance_until_size(
            &self.head,
            self.start_pos.accum_keysize + self.head.target_keysize,
            shard_identity,
        );

        // See if we can gobble up the rest of the keyspace if we stretch out the layer, up to
        // 1.25x target size
        let mut end_pos = next_pos.clone();
        end_pos.advance_until_size(
            &self.head,
            self.start_pos.accum_keysize + (self.head.target_keysize * 5 / 4),
            shard_identity,
        );
        if end_pos.reached_end(&self.head) {
            // gobble up any unused keyspace between the last used key and end of the range
            assert!(end_pos.end_key <= self.head.key_range.end);
            end_pos.end_key = self.head.key_range.end;
            next_pos = end_pos;
        }

        let start_key = self.start_pos.end_key;
        self.start_pos = next_pos;
        Some(start_key..self.start_pos.end_key)
    }
}

// Take previous partitioning, based on the image layers below.
//
// Candidate is at the front:
//
// Consider stretching an image layer to next divider? If it's close enough,
// that's the image candidate
//
// If it's too far, consider splitting at a reasonable point
//
// Is the image candidate smaller than the equivalent delta? If so,
// split off the image. Otherwise, split off one delta.
// Try to snap off the delta at a reasonable point

struct WindowElement<K> {
    start_key: K, // inclusive
    last_key: K,  // inclusive
    accum_size: u64,
}

/// Sliding window through keyspace and values for delta layer tiling
///
/// This is used to decide which delta layer to write next.
struct Window<K> {
    elems: VecDeque<WindowElement<K>>,

    // last key that was split off, inclusive
    splitoff_key: Option<K>,
    splitoff_size: u64,
}

impl<K> Window<K>
where
    K: CompactionKey,
{
    fn new() -> Self {
        Self {
            elems: VecDeque::new(),
            splitoff_key: None,
            splitoff_size: 0,
        }
    }

    fn feed(&mut self, key: K, size: u64) {
        let last_size;
        if let Some(last) = self.elems.back_mut() {
            // We require the keys to be strictly increasing for the window.
            // Keys should already have been deduplicated by `accum_key_values`
            assert!(
                last.last_key < key,
                "last_key(={}) >= key(={key})",
                last.last_key
            );
            last_size = last.accum_size;
        } else {
            last_size = 0;
        }
        // This is a new key.
        let elem = WindowElement {
            start_key: key,
            last_key: key,
            accum_size: last_size + size,
        };
        self.elems.push_back(elem);
    }

    fn remain_size(&self) -> u64 {
        self.elems.back().unwrap().accum_size - self.splitoff_size
    }

    fn peek_size(&self) -> u64 {
        self.elems.front().unwrap().accum_size - self.splitoff_size
    }

    fn is_empty(&self) -> bool {
        self.elems.is_empty()
    }

    fn commit_upto(&mut self, mut upto: usize) {
        while upto > 1 {
            let popped = self.elems.pop_front().unwrap();
            self.elems.front_mut().unwrap().start_key = popped.start_key;
            upto -= 1;
        }
    }

    fn find_size_split(&self, target_size: u64) -> usize {
        self.elems
            .partition_point(|elem| elem.accum_size - self.splitoff_size < target_size)
    }

    fn pop(&mut self) {
        let first = self.elems.pop_front().unwrap();
        self.splitoff_size = first.accum_size;

        self.splitoff_key = Some(first.last_key);
    }

    // the difference between delta and image is that an image covers
    // any unused keyspace before and after, while a delta tries to
    // minimize that. TODO: difference not implemented
    fn pop_delta(&mut self) -> Range<K> {
        let first = self.elems.front().unwrap();
        let key_range = first.start_key..first.last_key.next();

        self.pop();
        key_range
    }

    // Prerequisite: we have enough input in the window
    //
    // On return None, the caller should feed more data and call again
    fn choose_next_delta(&mut self, target_size: u64, has_more: bool) -> Option<Range<K>> {
        if has_more && self.elems.is_empty() {
            // Starting up
            return None;
        }

        // If we still have an undersized candidate, just keep going
        while self.peek_size() < target_size {
            if self.elems.len() > 1 {
                self.commit_upto(2);
            } else if has_more {
                return None;
            } else {
                break;
            }
        }

        // Ensure we have enough input in the window to make a good decision
        if has_more && self.remain_size() < target_size * 5 / 4 {
            return None;
        }

        // The candidate on the front is now large enough, for a delta.
        // And we have enough data in the window to decide.

        // If we're willing to stretch it up to 1.25 target size, could we
        // gobble up the rest of the work? This avoids creating very small
        // "tail" layers at the end of the keyspace
        if !has_more && self.remain_size() < target_size * 5 / 4 {
            self.commit_upto(self.elems.len());
        } else {
            let delta_split_at = self.find_size_split(target_size);
            self.commit_upto(delta_split_at);

            // If it's still not large enough, request the caller to fill the window
            if self.elems.len() == 1 && has_more {
                return None;
            }
        }
        Some(self.pop_delta())
    }
}
