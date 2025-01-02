use super::remote_timeline_client::is_same_remote_layer_path;
use super::storage_layer::AsLayerDesc as _;
use super::storage_layer::LayerName;
use super::storage_layer::ResidentLayer;
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::remote_timeline_client::index::IndexPart;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

use chrono::NaiveDateTime;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tracing::info;
use utils::generation::Generation;
use utils::lsn::AtomicLsn;

use std::sync::atomic::AtomicU32;
use utils::lsn::Lsn;

/// Kill switch for upload queue reordering in case it causes problems.
/// NB: enabling this will block all uploads in AttachedMulti tenants with a blocked deletion.
/// TODO: remove this once we have confidence in it.
static DISABLE_UPLOAD_QUEUE_REORDERING: Lazy<bool> =
    Lazy::new(|| std::env::var("DISABLE_UPLOAD_QUEUE_REORDERING").as_deref() == Ok("true"));

// clippy warns that Uninitialized is much smaller than Initialized, which wastes
// memory for Uninitialized variants. Doesn't matter in practice, there are not
// that many upload queues in a running pageserver, and most of them are initialized
// anyway.
#[allow(clippy::large_enum_variant)]
pub(super) enum UploadQueue {
    Uninitialized,
    Initialized(UploadQueueInitialized),
    Stopped(UploadQueueStopped),
}

impl UploadQueue {
    pub fn as_str(&self) -> &'static str {
        match self {
            UploadQueue::Uninitialized => "Uninitialized",
            UploadQueue::Initialized(_) => "Initialized",
            UploadQueue::Stopped(_) => "Stopped",
        }
    }
}

/// This keeps track of queued and in-progress tasks.
pub(crate) struct UploadQueueInitialized {
    /// Counter to assign task IDs
    pub(crate) task_counter: u64,

    /// The next uploaded index_part.json; assumed to be dirty.
    ///
    /// Should not be read, directly except for layer file updates. Instead you should add a
    /// projected field.
    pub(crate) dirty: IndexPart,

    /// The latest remote persisted IndexPart.
    ///
    /// Each completed metadata upload will update this. The second item is the task_id which last
    /// updated the value, used to ensure we never store an older value over a newer one.
    pub(crate) clean: (IndexPart, Option<u64>),

    /// How many file uploads or deletions been scheduled, since the
    /// last (scheduling of) metadata index upload?
    pub(crate) latest_files_changes_since_metadata_upload_scheduled: u64,

    /// The Lsn is only updated after our generation has been validated with
    /// the control plane (unlesss a timeline's generation is None, in which case
    /// we skip validation)
    pub(crate) visible_remote_consistent_lsn: Arc<AtomicLsn>,

    /// Tasks that are currently in-progress. In-progress means that a tokio Task
    /// has been launched for it. An in-progress task can be busy uploading, but it can
    /// also be waiting on the `concurrency_limiter` Semaphore in S3Bucket, or it can
    /// be waiting for retry in `exponential_backoff`.
    pub(crate) inprogress_tasks: HashMap<u64, Arc<UploadTask>>,

    /// Queued operations that have not been launched yet. They might depend on previous
    /// tasks to finish. For example, metadata upload cannot be performed before all
    /// preceding layer file uploads have completed.
    pub(crate) queued_operations: VecDeque<UploadOp>,

    /// If set to a valid generation, deletions at generations below this are blocked.
    pub(crate) block_deletions_below: Generation,

    /// Files which have been unlinked but not yet had scheduled a deletion for. Only kept around
    /// for error logging.
    ///
    /// Putting this behind a testing feature to catch problems in tests, but assuming we could have a
    /// bug causing leaks, then it's better to not leave this enabled for production builds.
    #[cfg(feature = "testing")]
    pub(crate) dangling_files: HashMap<LayerName, Generation>,

    /// Set to true when we have inserted the `UploadOp::Shutdown` into the `inprogress_tasks`.
    pub(crate) shutting_down: bool,

    /// Permitless semaphore on which any number of `RemoteTimelineClient::shutdown` futures can
    /// wait on until one of them stops the queue. The semaphore is closed when
    /// `RemoteTimelineClient::launch_queued_tasks` encounters `UploadOp::Shutdown`.
    pub(crate) shutdown_ready: Arc<tokio::sync::Semaphore>,
}

impl UploadQueueInitialized {
    pub(super) fn no_pending_work(&self) -> bool {
        self.inprogress_tasks.is_empty() && self.queued_operations.is_empty()
    }

    pub(super) fn get_last_remote_consistent_lsn_visible(&self) -> Lsn {
        self.visible_remote_consistent_lsn.load()
    }

    pub(super) fn get_last_remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        let lsn = self.clean.0.metadata.disk_consistent_lsn();
        self.clean.1.map(|_| lsn)
    }

    /// Returns and removes the next ready operation from the queue, if any. This isn't necessarily
    /// the first operation in the queue, to avoid head-of-line blocking -- an operation can jump
    /// the queue if it doesn't conflict with operations ahead of it.
    ///
    /// None may be returned even if the queue isn't empty, if no operations are ready yet.
    pub(crate) fn next_ready(&mut self) -> Option<UploadOp> {
        // NB: this is quadratic, but queues are expected to be small.
        for (i, candidate) in self.queued_operations.iter().enumerate() {
            // If this candidate is ready, go for it. Otherwise, try the next one.
            if self.is_ready(i) {
                // Shutdown operations are left at the head of the queue, to prevent further
                // operations from starting. Signal that we're ready to shut down.
                if matches!(candidate, UploadOp::Shutdown) {
                    assert!(self.inprogress_tasks.is_empty(), "shutdown with tasks");
                    assert_eq!(i, 0, "shutdown not at head of queue");
                    self.shutdown_ready.close();
                    return None;
                }

                return self.queued_operations.remove(i);
            }

            // Nothing can bypass a barrier or shutdown. If it wasn't scheduled above, give up.
            if matches!(candidate, UploadOp::Barrier(_) | UploadOp::Shutdown) {
                return None;
            }

            // If upload queue reordering is disabled, bail out after the first operation.
            if *DISABLE_UPLOAD_QUEUE_REORDERING {
                return None;
            }
        }
        None
    }

    /// Returns true if the queued operation at the given position is ready to be uploaded, i.e. if
    /// it doesn't conflict with any in-progress or queued operations ahead of it. Operations are
    /// allowed to skip the queue when it's safe to do so, to increase parallelism.
    ///
    /// The position must be valid for the queue size.
    fn is_ready(&self, pos: usize) -> bool {
        let candidate = self.queued_operations.get(pos).expect("invalid position");

        // Don't allow deletions for past generations if blocked.
        // TODO: document motivation.
        if self.block_deletions_below != Generation::None {
            if let UploadOp::Delete(delete) = candidate {
                if delete
                    .layers
                    .iter()
                    .any(|(_, metadata)| metadata.generation < self.block_deletions_below)
                {
                    return false;
                }
            }
        }

        self
            // Look at in-progress operations, in random order.
            .inprogress_tasks
            .values()
            .map(|task| &task.op)
            // Then queued operations ahead of the candidate, front-to-back.
            .chain(self.queued_operations.iter().take(pos))
            // Keep track of the active index ahead of each operation. This is used to ensure that
            // an upload doesn't skip the queue too far, such that it modifies a layer that's
            // referenced by an active index.
            //
            // It's okay that in-progress operations are emitted in random order above, since at
            // most one of them can be an index upload (enforced by can_bypass).
            .scan(&self.clean.0, |next_active_index, op| {
                let active_index = *next_active_index;
                if let UploadOp::UploadMetadata { ref uploaded } = op {
                    *next_active_index = uploaded; // stash index for next operation after this
                }
                Some((op, active_index))
            })
            // Check if the candidate can bypass all of them.
            .all(|(op, active_index)| candidate.can_bypass(op, active_index))
    }

    /// Returns the number of in-progress deletion operations.
    #[cfg(test)]
    pub(crate) fn num_inprogress_deletions(&self) -> usize {
        self.inprogress_tasks
            .iter()
            .filter(|(_, t)| matches!(t.op, UploadOp::Delete(_)))
            .count()
    }

    /// Returns the number of in-progress layer uploads.
    #[cfg(test)]
    pub(crate) fn num_inprogress_layer_uploads(&self) -> usize {
        self.inprogress_tasks
            .iter()
            .filter(|(_, t)| matches!(t.op, UploadOp::UploadLayer(_, _)))
            .count()
    }

    /// Test helper that schedules all ready operations into inprogress_tasks, and returns
    /// references to them.
    ///
    /// TODO: the corresponding production logic should be moved from RemoteTimelineClient into
    /// UploadQueue, so we can use the same code path.
    #[cfg(test)]
    fn schedule_ready(&mut self) -> Vec<Arc<UploadTask>> {
        let mut tasks = Vec::new();
        // NB: schedule operations one by one, to handle conflicts with inprogress_tasks.
        while let Some(op) = self.next_ready() {
            self.task_counter += 1;
            let task = Arc::new(UploadTask {
                task_id: self.task_counter,
                op,
                retries: 0.into(),
            });
            self.inprogress_tasks.insert(task.task_id, task.clone());
            tasks.push(task);
        }
        tasks
    }

    /// Test helper that marks an operation as completed, removing it from inprogress_tasks.
    ///
    /// TODO: the corresponding production logic should be moved from RemoteTimelineClient into
    /// UploadQueue, so we can use the same code path.
    #[cfg(test)]
    fn complete(&mut self, task_id: u64) {
        let Some(task) = self.inprogress_tasks.remove(&task_id) else {
            return;
        };
        // Update the clean index on uploads.
        if let UploadOp::UploadMetadata { ref uploaded } = task.op {
            if task.task_id > self.clean.1.unwrap_or_default() {
                self.clean = (*uploaded.clone(), Some(task.task_id));
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum SetDeletedFlagProgress {
    NotRunning,
    InProgress(NaiveDateTime),
    Successful(NaiveDateTime),
}

pub(super) struct UploadQueueStoppedDeletable {
    pub(super) upload_queue_for_deletion: UploadQueueInitialized,
    pub(super) deleted_at: SetDeletedFlagProgress,
}

pub(super) enum UploadQueueStopped {
    Deletable(UploadQueueStoppedDeletable),
    Uninitialized,
}

#[derive(thiserror::Error, Debug)]
pub enum NotInitialized {
    #[error("queue is in state Uninitialized")]
    Uninitialized,
    #[error("queue is in state Stopped")]
    Stopped,
    #[error("queue is shutting down")]
    ShuttingDown,
}

impl NotInitialized {
    pub(crate) fn is_stopping(&self) -> bool {
        use NotInitialized::*;
        match self {
            Uninitialized => false,
            Stopped => true,
            ShuttingDown => true,
        }
    }
}

impl UploadQueue {
    pub(crate) fn initialize_empty_remote(
        &mut self,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) | UploadQueue::Stopped(_) => {
                anyhow::bail!("already initialized, state {}", self.as_str())
            }
        }

        info!("initializing upload queue for empty remote");

        let index_part = IndexPart::empty(metadata.clone());

        let state = UploadQueueInitialized {
            dirty: index_part.clone(),
            clean: (index_part, None),
            latest_files_changes_since_metadata_upload_scheduled: 0,
            visible_remote_consistent_lsn: Arc::new(AtomicLsn::new(0)),
            // what follows are boring default initializations
            task_counter: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
            block_deletions_below: Generation::None,
            #[cfg(feature = "testing")]
            dangling_files: HashMap::new(),
            shutting_down: false,
            shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    pub(crate) fn initialize_with_current_remote_index_part(
        &mut self,
        index_part: &IndexPart,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) | UploadQueue::Stopped(_) => {
                anyhow::bail!("already initialized, state {}", self.as_str())
            }
        }

        info!(
            "initializing upload queue with remote index_part.disk_consistent_lsn: {}",
            index_part.metadata.disk_consistent_lsn()
        );

        let state = UploadQueueInitialized {
            dirty: index_part.clone(),
            clean: (index_part.clone(), None),
            latest_files_changes_since_metadata_upload_scheduled: 0,
            visible_remote_consistent_lsn: Arc::new(
                index_part.metadata.disk_consistent_lsn().into(),
            ),
            // what follows are boring default initializations
            task_counter: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
            block_deletions_below: Generation::None,
            #[cfg(feature = "testing")]
            dangling_files: HashMap::new(),
            shutting_down: false,
            shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    pub(crate) fn initialized_mut(
        &mut self,
    ) -> Result<&mut UploadQueueInitialized, NotInitialized> {
        use UploadQueue::*;
        match self {
            Uninitialized => Err(NotInitialized::Uninitialized),
            Initialized(x) => {
                if x.shutting_down {
                    Err(NotInitialized::ShuttingDown)
                } else {
                    Ok(x)
                }
            }
            Stopped(_) => Err(NotInitialized::Stopped),
        }
    }

    pub(crate) fn stopped_mut(&mut self) -> anyhow::Result<&mut UploadQueueStoppedDeletable> {
        match self {
            UploadQueue::Initialized(_) | UploadQueue::Uninitialized => {
                anyhow::bail!("queue is in state {}", self.as_str())
            }
            UploadQueue::Stopped(UploadQueueStopped::Uninitialized) => {
                anyhow::bail!("queue is in state Stopped(Uninitialized)")
            }
            UploadQueue::Stopped(UploadQueueStopped::Deletable(deletable)) => Ok(deletable),
        }
    }
}

/// An in-progress upload or delete task.
#[derive(Debug)]
pub(crate) struct UploadTask {
    /// Unique ID of this task. Used as the key in `inprogress_tasks` above.
    pub(crate) task_id: u64,
    pub(crate) retries: AtomicU32,

    pub(crate) op: UploadOp,
}

/// A deletion of some layers within the lifetime of a timeline.  This is not used
/// for timeline deletion, which skips this queue and goes directly to DeletionQueue.
#[derive(Debug, Clone)]
pub(crate) struct Delete {
    pub(crate) layers: Vec<(LayerName, LayerFileMetadata)>,
}

#[derive(Clone, Debug)]
pub(crate) enum UploadOp {
    /// Upload a layer file. The last field indicates the last operation for thie file.
    UploadLayer(ResidentLayer, LayerFileMetadata),

    /// Upload a index_part.json file
    UploadMetadata {
        /// The next [`UploadQueueInitialized::clean`] after this upload succeeds.
        uploaded: Box<IndexPart>,
    },

    /// Delete layer files
    Delete(Delete),

    /// Barrier. When the barrier operation is reached, the channel is closed.
    Barrier(tokio::sync::watch::Sender<()>),

    /// Shutdown; upon encountering this operation no new operations will be spawned, otherwise
    /// this is the same as a Barrier.
    Shutdown,
}

impl std::fmt::Display for UploadOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UploadOp::UploadLayer(layer, metadata) => {
                write!(
                    f,
                    "UploadLayer({}, size={:?}, gen={:?})",
                    layer, metadata.file_size, metadata.generation
                )
            }
            UploadOp::UploadMetadata { uploaded, .. } => {
                write!(
                    f,
                    "UploadMetadata(lsn: {})",
                    uploaded.metadata.disk_consistent_lsn()
                )
            }
            UploadOp::Delete(delete) => {
                write!(f, "Delete({} layers)", delete.layers.len())
            }
            UploadOp::Barrier(_) => write!(f, "Barrier"),
            UploadOp::Shutdown => write!(f, "Shutdown"),
        }
    }
}

impl UploadOp {
    /// Returns true if self can bypass other, i.e. if the operations don't conflict. index is the
    /// active index when other would be uploaded -- if we allow self to bypass other, this would
    /// be the active index when self is uploaded.
    pub fn can_bypass(&self, other: &UploadOp, index: &IndexPart) -> bool {
        match (self, other) {
            // Nothing can bypass a barrier or shutdown, and it can't bypass anything.
            (UploadOp::Barrier(_), _) | (_, UploadOp::Barrier(_)) => false,
            (UploadOp::Shutdown, _) | (_, UploadOp::Shutdown) => false,

            // Uploads and deletes can bypass each other unless they're for the same file.
            (UploadOp::UploadLayer(a, ameta), UploadOp::UploadLayer(b, bmeta)) => {
                let aname = &a.layer_desc().layer_name();
                let bname = &b.layer_desc().layer_name();
                !is_same_remote_layer_path(aname, ameta, bname, bmeta)
            }
            (UploadOp::UploadLayer(u, umeta), UploadOp::Delete(d))
            | (UploadOp::Delete(d), UploadOp::UploadLayer(u, umeta)) => {
                d.layers.iter().all(|(dname, dmeta)| {
                    !is_same_remote_layer_path(&u.layer_desc().layer_name(), umeta, dname, dmeta)
                })
            }

            // Deletes are idempotent and can always bypass each other.
            (UploadOp::Delete(_), UploadOp::Delete(_)) => true,

            // Uploads and deletes can bypass an index upload as long as neither the uploaded index
            // nor the active index below it references the file. A layer can't be modified or
            // deleted while referenced by an index.
            //
            // Similarly, index uploads can bypass uploads and deletes as long as neither the
            // uploaded index nor the active index references the file (the latter would be
            // incorrect use by the caller).
            (UploadOp::UploadLayer(u, umeta), UploadOp::UploadMetadata { uploaded: i })
            | (UploadOp::UploadMetadata { uploaded: i }, UploadOp::UploadLayer(u, umeta)) => {
                let uname = u.layer_desc().layer_name();
                !i.references(&uname, umeta) && !index.references(&uname, umeta)
            }
            (UploadOp::Delete(d), UploadOp::UploadMetadata { uploaded: i })
            | (UploadOp::UploadMetadata { uploaded: i }, UploadOp::Delete(d)) => {
                d.layers.iter().all(|(dname, dmeta)| {
                    !i.references(dname, dmeta) && !index.references(dname, dmeta)
                })
            }

            // Indexes can never bypass each other.
            // TODO: we could coalesce them though, by only uploading the newest ready index. This
            // is left for later, out of caution.
            (UploadOp::UploadMetadata { .. }, UploadOp::UploadMetadata { .. }) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::{TenantHarness, TIMELINE_ID};
    use crate::tenant::storage_layer::layer::local_layer_path;
    use crate::tenant::storage_layer::Layer;
    use crate::tenant::Timeline;
    use crate::DEFAULT_PG_VERSION;
    use itertools::Itertools as _;
    use std::str::FromStr as _;
    use utils::generation::Generation;
    use utils::shard::{ShardCount, ShardIndex, ShardNumber};

    /// Test helper which asserts that two operations are the same, in lieu of UploadOp PartialEq.
    #[track_caller]
    fn assert_same_op(a: &UploadOp, b: &UploadOp) {
        use UploadOp::*;
        match (a, b) {
            (UploadLayer(a, ameta), UploadLayer(b, bmeta)) => {
                assert_eq!(a.layer_desc().layer_name(), b.layer_desc().layer_name());
                assert_eq!(ameta, bmeta);
            }
            (Delete(a), Delete(b)) => assert_eq!(a.layers, b.layers),
            (UploadMetadata { uploaded: a }, UploadMetadata { uploaded: b }) => assert_eq!(a, b),
            (Barrier(_), Barrier(_)) => {}
            (Shutdown, Shutdown) => {}
            (a, b) => panic!("{a:?} != {b:?}"),
        }
    }

    /// Test helper which asserts that two sets of operations are the same.
    #[track_caller]
    fn assert_same_ops<'a>(
        a: impl IntoIterator<Item = &'a UploadOp>,
        b: impl IntoIterator<Item = &'a UploadOp>,
    ) {
        a.into_iter()
            .zip_eq(b)
            .for_each(|(a, b)| assert_same_op(a, b))
    }

    /// Test helper to construct a test timeline.
    ///
    /// TODO: it really shouldn't be necessary to construct an entire tenant and timeline just to
    /// test the upload queue -- decouple ResidentLayer from Timeline.
    ///
    /// TODO: the upload queue uses TimelineMetadata::example() instead, because there's no way to
    /// obtain a TimelineMetadata from a Timeline.
    fn make_timeline() -> Arc<Timeline> {
        // Grab the current test name from the current thread name.
        // TODO: TenantHarness shouldn't take a &'static str, but just leak the test name for now.
        let test_name = std::thread::current().name().unwrap().to_string();
        let test_name = Box::leak(test_name.into_boxed_str());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create runtime");

        runtime
            .block_on(async {
                let harness = TenantHarness::create(test_name).await?;
                let (tenant, ctx) = harness.load().await;
                tenant
                    .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
                    .await
            })
            .expect("failed to create timeline")
    }

    /// Test helper to construct an (empty) resident layer.
    fn make_layer(timeline: &Arc<Timeline>, name: &str) -> ResidentLayer {
        make_layer_with_size(timeline, name, 0)
    }

    /// Test helper to construct a resident layer with the given size.
    fn make_layer_with_size(timeline: &Arc<Timeline>, name: &str, size: usize) -> ResidentLayer {
        let metadata = LayerFileMetadata {
            generation: timeline.generation,
            shard: timeline.get_shard_index(),
            file_size: size as u64,
        };
        make_layer_with_metadata(timeline, name, metadata)
    }

    /// Test helper to construct a layer with the given metadata.
    fn make_layer_with_metadata(
        timeline: &Arc<Timeline>,
        name: &str,
        metadata: LayerFileMetadata,
    ) -> ResidentLayer {
        let name = LayerName::from_str(name).expect("invalid name");
        let local_path = local_layer_path(
            timeline.conf,
            &timeline.tenant_shard_id,
            &timeline.timeline_id,
            &name,
            &metadata.generation,
        );
        std::fs::write(&local_path, vec![0; metadata.file_size as usize])
            .expect("failed to write file");
        Layer::for_resident(timeline.conf, timeline, local_path, name, metadata)
    }

    /// Test helper to add a layer to an index and return a new index.
    fn index_with(index: &IndexPart, layer: &ResidentLayer) -> Box<IndexPart> {
        let mut index = index.clone();
        index
            .layer_metadata
            .insert(layer.layer_desc().layer_name(), layer.metadata());
        Box::new(index)
    }

    /// Test helper to remove a layer from an index and return a new index.
    fn index_without(index: &IndexPart, layer: &ResidentLayer) -> Box<IndexPart> {
        let mut index = index.clone();
        index
            .layer_metadata
            .remove(&layer.layer_desc().layer_name());
        Box::new(index)
    }

    /// Nothing can bypass a barrier, and it can't bypass inprogress tasks.
    #[test]
    fn schedule_barrier() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_empty_remote(&TimelineMetadata::example())?;
        let tli = make_timeline();

        let index = Box::new(queue.clean.0.clone()); // empty, doesn't matter
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer2 = make_layer(&tli, "200000000000000000000000000000000000-300000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer3 = make_layer(&tli, "300000000000000000000000000000000000-400000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let (barrier, _) = tokio::sync::watch::channel(());

        // Enqueue non-conflicting upload, delete, and index before and after a barrier.
        let ops = [
            UploadOp::UploadLayer(layer0.clone(), layer0.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![(layer1.layer_desc().layer_name(), layer1.metadata())],
            }),
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
            UploadOp::Barrier(barrier),
            UploadOp::UploadLayer(layer2.clone(), layer2.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![(layer3.layer_desc().layer_name(), layer3.metadata())],
            }),
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
        ];

        queue.queued_operations.extend(ops.clone());

        // Schedule the initial operations ahead of the barrier.
        let tasks = queue.schedule_ready();

        assert_same_ops(tasks.iter().map(|t| &t.op), &ops[0..3]);
        assert!(matches!(
            queue.queued_operations.front(),
            Some(&UploadOp::Barrier(_))
        ));

        // Complete the initial operations. The barrier isn't scheduled while they're pending.
        for task in tasks {
            assert!(queue.schedule_ready().is_empty());
            queue.complete(task.task_id);
        }

        // Schedule the barrier. The later tasks won't schedule until it completes.
        let tasks = queue.schedule_ready();

        assert_eq!(tasks.len(), 1);
        assert!(matches!(tasks[0].op, UploadOp::Barrier(_)));
        assert_eq!(queue.queued_operations.len(), 3);

        // Complete the barrier. The rest of the tasks schedule immediately.
        queue.complete(tasks[0].task_id);

        let tasks = queue.schedule_ready();
        assert_same_ops(tasks.iter().map(|t| &t.op), &ops[4..]);
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Deletes can be scheduled in parallel, even if they're for the same file.
    #[test]
    fn schedule_delete_parallel() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_empty_remote(&TimelineMetadata::example())?;
        let tli = make_timeline();

        // Enqueue a bunch of deletes, some with conflicting names.
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer2 = make_layer(&tli, "200000000000000000000000000000000000-300000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer3 = make_layer(&tli, "300000000000000000000000000000000000-400000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");

        let ops = [
            UploadOp::Delete(Delete {
                layers: vec![(layer0.layer_desc().layer_name(), layer0.metadata())],
            }),
            UploadOp::Delete(Delete {
                layers: vec![(layer1.layer_desc().layer_name(), layer1.metadata())],
            }),
            UploadOp::Delete(Delete {
                layers: vec![
                    (layer1.layer_desc().layer_name(), layer1.metadata()),
                    (layer2.layer_desc().layer_name(), layer2.metadata()),
                ],
            }),
            UploadOp::Delete(Delete {
                layers: vec![(layer2.layer_desc().layer_name(), layer2.metadata())],
            }),
            UploadOp::Delete(Delete {
                layers: vec![(layer3.layer_desc().layer_name(), layer3.metadata())],
            }),
        ];

        queue.queued_operations.extend(ops.clone());

        // Schedule all ready operations. Since deletes don't conflict, they're all scheduled.
        let tasks = queue.schedule_ready();

        assert_same_ops(tasks.iter().map(|t| &t.op), &ops);
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Conflicting uploads are serialized.
    #[test]
    fn schedule_upload_conflicts() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Enqueue three versions of the same layer, with different file sizes.
        let layer0a = make_layer_with_size(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51", 1);
        let layer0b = make_layer_with_size(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51", 2);
        let layer0c = make_layer_with_size(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51", 3);

        let ops = [
            UploadOp::UploadLayer(layer0a.clone(), layer0a.metadata()),
            UploadOp::UploadLayer(layer0b.clone(), layer0b.metadata()),
            UploadOp::UploadLayer(layer0c.clone(), layer0c.metadata()),
        ];

        queue.queued_operations.extend(ops.clone());

        // Only one version should be scheduled and uploaded at a time.
        for op in ops {
            let tasks = queue.schedule_ready();
            assert_eq!(tasks.len(), 1);
            assert_same_op(&tasks[0].op, &op);
            queue.complete(tasks[0].task_id);
        }
        assert!(queue.schedule_ready().is_empty());
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Conflicting uploads and deletes are serialized.
    #[test]
    fn schedule_upload_delete_conflicts() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Enqueue two layer uploads, with a delete of both layers in between them. These should be
        // scheduled one at a time, since deletes can't bypass uploads and vice versa.
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");

        let ops = [
            UploadOp::UploadLayer(layer0.clone(), layer0.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![
                    (layer0.layer_desc().layer_name(), layer0.metadata()),
                    (layer1.layer_desc().layer_name(), layer1.metadata()),
                ],
            }),
            UploadOp::UploadLayer(layer1.clone(), layer1.metadata()),
        ];

        queue.queued_operations.extend(ops.clone());

        // Only one version should be scheduled and uploaded at a time.
        for op in ops {
            let tasks = queue.schedule_ready();
            assert_eq!(tasks.len(), 1);
            assert_same_op(&tasks[0].op, &op);
            queue.complete(tasks[0].task_id);
        }
        assert!(queue.schedule_ready().is_empty());
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Non-conflicting uploads and deletes can bypass the queue, avoiding the conflicting
    /// delete/upload operations at the head of the queue.
    #[test]
    fn schedule_upload_delete_conflicts_bypass() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Enqueue two layer uploads, with a delete of both layers in between them. These should be
        // scheduled one at a time, since deletes can't bypass uploads and vice versa.
        //
        // Also enqueue non-conflicting uploads and deletes at the end. These can bypass the queue
        // and run immediately.
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer2 = make_layer(&tli, "200000000000000000000000000000000000-300000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer3 = make_layer(&tli, "300000000000000000000000000000000000-400000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");

        let ops = [
            UploadOp::UploadLayer(layer0.clone(), layer0.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![
                    (layer0.layer_desc().layer_name(), layer0.metadata()),
                    (layer1.layer_desc().layer_name(), layer1.metadata()),
                ],
            }),
            UploadOp::UploadLayer(layer1.clone(), layer1.metadata()),
            UploadOp::UploadLayer(layer2.clone(), layer2.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![(layer3.layer_desc().layer_name(), layer3.metadata())],
            }),
        ];

        queue.queued_operations.extend(ops.clone());

        // Operations 0, 3, and 4 are scheduled immediately.
        let tasks = queue.schedule_ready();
        assert_same_ops(tasks.iter().map(|t| &t.op), [&ops[0], &ops[3], &ops[4]]);
        assert_eq!(queue.queued_operations.len(), 2);

        Ok(())
    }

    /// Non-conflicting uploads are parallelized.
    #[test]
    fn schedule_upload_parallel() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Enqueue three different layer uploads.
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer2 = make_layer(&tli, "200000000000000000000000000000000000-300000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");

        let ops = [
            UploadOp::UploadLayer(layer0.clone(), layer0.metadata()),
            UploadOp::UploadLayer(layer1.clone(), layer1.metadata()),
            UploadOp::UploadLayer(layer2.clone(), layer2.metadata()),
        ];

        queue.queued_operations.extend(ops.clone());

        // All uploads should be scheduled concurrently.
        let tasks = queue.schedule_ready();

        assert_same_ops(tasks.iter().map(|t| &t.op), &ops);
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Index uploads are serialized.
    #[test]
    fn schedule_index_serial() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;

        // Enqueue three uploads of the current empty index.
        let index = Box::new(queue.clean.0.clone());

        let ops = [
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
        ];

        queue.queued_operations.extend(ops.clone());

        // The uploads should run serially.
        for op in ops {
            let tasks = queue.schedule_ready();
            assert_eq!(tasks.len(), 1);
            assert_same_op(&tasks[0].op, &op);
            queue.complete(tasks[0].task_id);
        }

        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Chains of upload/index operations lead to parallel layer uploads and serial index uploads.
    /// This is the common case with layer flushes.
    #[test]
    fn schedule_index_upload_chain() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Enqueue three uploads of the current empty index.
        let index = Box::new(queue.clean.0.clone());
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let index0 = index_with(&index, &layer0);
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let index1 = index_with(&index0, &layer1);
        let layer2 = make_layer(&tli, "200000000000000000000000000000000000-300000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let index2 = index_with(&index1, &layer2);

        let ops = [
            UploadOp::UploadLayer(layer0.clone(), layer0.metadata()),
            UploadOp::UploadMetadata {
                uploaded: index0.clone(),
            },
            UploadOp::UploadLayer(layer1.clone(), layer1.metadata()),
            UploadOp::UploadMetadata {
                uploaded: index1.clone(),
            },
            UploadOp::UploadLayer(layer2.clone(), layer2.metadata()),
            UploadOp::UploadMetadata {
                uploaded: index2.clone(),
            },
        ];

        queue.queued_operations.extend(ops.clone());

        // The layer uploads should be scheduled immediately. The indexes must wait.
        let upload_tasks = queue.schedule_ready();
        assert_same_ops(
            upload_tasks.iter().map(|t| &t.op),
            [&ops[0], &ops[2], &ops[4]],
        );

        // layer2 completes first. None of the indexes can upload yet.
        queue.complete(upload_tasks[2].task_id);
        assert!(queue.schedule_ready().is_empty());

        // layer0 completes. index0 can upload. It completes.
        queue.complete(upload_tasks[0].task_id);
        let index_tasks = queue.schedule_ready();
        assert_eq!(index_tasks.len(), 1);
        assert_same_op(&index_tasks[0].op, &ops[1]);
        queue.complete(index_tasks[0].task_id);

        // layer 1 completes. This unblocks index 1 then index 2.
        queue.complete(upload_tasks[1].task_id);

        let index_tasks = queue.schedule_ready();
        assert_eq!(index_tasks.len(), 1);
        assert_same_op(&index_tasks[0].op, &ops[3]);
        queue.complete(index_tasks[0].task_id);

        let index_tasks = queue.schedule_ready();
        assert_eq!(index_tasks.len(), 1);
        assert_same_op(&index_tasks[0].op, &ops[5]);
        queue.complete(index_tasks[0].task_id);

        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// A delete can't bypass an index upload if an index ahead of it still references it.
    #[test]
    fn schedule_index_delete_dereferenced() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Create a layer to upload.
        let layer = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let index_upload = index_with(&queue.clean.0, &layer);

        // Remove the layer reference in a new index, then delete the layer.
        let index_deref = index_without(&index_upload, &layer);

        let ops = [
            // Initial upload.
            UploadOp::UploadLayer(layer.clone(), layer.metadata()),
            UploadOp::UploadMetadata {
                uploaded: index_upload.clone(),
            },
            // Dereference the layer and delete it.
            UploadOp::UploadMetadata {
                uploaded: index_deref.clone(),
            },
            UploadOp::Delete(Delete {
                layers: vec![(layer.layer_desc().layer_name(), layer.metadata())],
            }),
        ];

        queue.queued_operations.extend(ops.clone());

        // Operations are serialized.
        for op in ops {
            let tasks = queue.schedule_ready();
            assert_eq!(tasks.len(), 1);
            assert_same_op(&tasks[0].op, &op);
            queue.complete(tasks[0].task_id);
        }
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// An upload with a reused layer name doesn't clobber the previous layer. Specifically, a
    /// dereference/upload/reference cycle can't allow the upload to bypass the reference.
    #[test]
    fn schedule_index_upload_dereferenced() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&IndexPart::example())?;
        let tli = make_timeline();

        // Create a layer to upload.
        let layer = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");

        // Upload the layer. Then dereference the layer, and upload/reference it again.
        let index_upload = index_with(&queue.clean.0, &layer);
        let index_deref = index_without(&index_upload, &layer);
        let index_ref = index_with(&index_deref, &layer);

        let ops = [
            // Initial upload.
            UploadOp::UploadLayer(layer.clone(), layer.metadata()),
            UploadOp::UploadMetadata {
                uploaded: index_upload.clone(),
            },
            // Dereference the layer.
            UploadOp::UploadMetadata {
                uploaded: index_deref.clone(),
            },
            // Replace and reference the layer.
            UploadOp::UploadLayer(layer.clone(), layer.metadata()),
            UploadOp::UploadMetadata {
                uploaded: index_ref.clone(),
            },
        ];

        queue.queued_operations.extend(ops.clone());

        // Operations are serialized.
        for op in ops {
            let tasks = queue.schedule_ready();
            assert_eq!(tasks.len(), 1);
            assert_same_op(&tasks[0].op, &op);
            queue.complete(tasks[0].task_id);
        }
        assert!(queue.queued_operations.is_empty());

        Ok(())
    }

    /// Nothing can bypass a shutdown, and it waits for inprogress tasks. It's never returned from
    /// next_ready(), but is left at the head of the queue.
    #[test]
    fn schedule_shutdown() -> anyhow::Result<()> {
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_empty_remote(&TimelineMetadata::example())?;
        let tli = make_timeline();

        let index = Box::new(queue.clean.0.clone()); // empty, doesn't matter
        let layer0 = make_layer(&tli, "000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer1 = make_layer(&tli, "100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer2 = make_layer(&tli, "200000000000000000000000000000000000-300000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");
        let layer3 = make_layer(&tli, "300000000000000000000000000000000000-400000000000000000000000000000000000__00000000016B59D8-00000000016B5A51");

        // Enqueue non-conflicting upload, delete, and index before and after a shutdown.
        let ops = [
            UploadOp::UploadLayer(layer0.clone(), layer0.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![(layer1.layer_desc().layer_name(), layer1.metadata())],
            }),
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
            UploadOp::Shutdown,
            UploadOp::UploadLayer(layer2.clone(), layer2.metadata()),
            UploadOp::Delete(Delete {
                layers: vec![(layer3.layer_desc().layer_name(), layer3.metadata())],
            }),
            UploadOp::UploadMetadata {
                uploaded: index.clone(),
            },
        ];

        queue.queued_operations.extend(ops.clone());

        // Schedule the initial operations ahead of the shutdown.
        let tasks = queue.schedule_ready();

        assert_same_ops(tasks.iter().map(|t| &t.op), &ops[0..3]);
        assert!(matches!(
            queue.queued_operations.front(),
            Some(&UploadOp::Shutdown)
        ));

        // Complete the initial operations. The shutdown isn't triggered while they're pending.
        for task in tasks {
            assert!(queue.schedule_ready().is_empty());
            queue.complete(task.task_id);
        }

        // The shutdown is triggered the next time we try to pull an operation. It isn't returned,
        // but is left in the queue.
        assert!(!queue.shutdown_ready.is_closed());
        assert!(queue.next_ready().is_none());
        assert!(queue.shutdown_ready.is_closed());

        Ok(())
    }

    /// Tests that can_bypass takes name, generation and shard index into account for all operations.
    #[test]
    fn can_bypass_path() -> anyhow::Result<()> {
        let tli = make_timeline();

        let name0 = &"000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51";
        let name1 = &"100000000000000000000000000000000000-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51";

        // Asserts that layers a and b either can or can't bypass each other, for all combinations
        // of operations (except Delete and UploadMetadata which are special-cased).
        #[track_caller]
        fn assert_can_bypass(a: ResidentLayer, b: ResidentLayer, can_bypass: bool) {
            let index = IndexPart::empty(TimelineMetadata::example());
            for (a, b) in make_ops(a).into_iter().zip(make_ops(b)) {
                match (&a, &b) {
                    // Deletes can always bypass each other.
                    (UploadOp::Delete(_), UploadOp::Delete(_)) => assert!(a.can_bypass(&b, &index)),
                    // Indexes can never bypass each other.
                    (UploadOp::UploadMetadata { .. }, UploadOp::UploadMetadata { .. }) => {
                        assert!(!a.can_bypass(&b, &index))
                    }
                    // For other operations, assert as requested.
                    (a, b) => assert_eq!(a.can_bypass(b, &index), can_bypass),
                }
            }
        }

        fn make_ops(layer: ResidentLayer) -> Vec<UploadOp> {
            let mut index = IndexPart::empty(TimelineMetadata::example());
            index
                .layer_metadata
                .insert(layer.layer_desc().layer_name(), layer.metadata());
            vec![
                UploadOp::UploadLayer(layer.clone(), layer.metadata()),
                UploadOp::Delete(Delete {
                    layers: vec![(layer.layer_desc().layer_name(), layer.metadata())],
                }),
                UploadOp::UploadMetadata {
                    uploaded: Box::new(index),
                },
            ]
        }

        // Makes a ResidentLayer.
        let layer = |name: &'static str, shard: Option<u8>, generation: u32| -> ResidentLayer {
            let shard = shard
                .map(|n| ShardIndex::new(ShardNumber(n), ShardCount(8)))
                .unwrap_or(ShardIndex::unsharded());
            let metadata = LayerFileMetadata {
                shard,
                generation: Generation::Valid(generation),
                file_size: 0,
            };
            make_layer_with_metadata(&tli, name, metadata)
        };

        // Same name and metadata can't bypass. This goes both for unsharded and sharded, as well as
        // 0 or >0 generation.
        assert_can_bypass(layer(name0, None, 0), layer(name0, None, 0), false);
        assert_can_bypass(layer(name0, Some(0), 0), layer(name0, Some(0), 0), false);
        assert_can_bypass(layer(name0, None, 1), layer(name0, None, 1), false);

        // Different names can bypass.
        assert_can_bypass(layer(name0, None, 0), layer(name1, None, 0), true);

        // Different shards can bypass. Shard 0 is different from unsharded.
        assert_can_bypass(layer(name0, Some(0), 0), layer(name0, Some(1), 0), true);
        assert_can_bypass(layer(name0, Some(0), 0), layer(name0, None, 0), true);

        // Different generations can bypass, both sharded and unsharded.
        assert_can_bypass(layer(name0, None, 0), layer(name0, None, 1), true);
        assert_can_bypass(layer(name0, Some(1), 0), layer(name0, Some(1), 1), true);

        Ok(())
    }
}
