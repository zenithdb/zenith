mod deleter;
mod list_writer;
mod validator;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::controller_upcall_client::ControlPlaneGenerationsApi;
use crate::metrics;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::remote_timeline_client::remote_timeline_path;
use crate::tenant::remote_timeline_client::LayerFileMetadata;
use crate::virtual_file::MaybeFatalIo;
use crate::virtual_file::VirtualFile;
use anyhow::Context;
use camino::Utf8PathBuf;
use pageserver_api::shard::TenantShardId;
use remote_storage::{GenericRemoteStorage, RemotePath};
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::{debug, error};
use utils::crashsafe::path_with_suffix_extension;
use utils::generation::Generation;
use utils::id::TimelineId;
use utils::lsn::AtomicLsn;
use utils::lsn::Lsn;

use self::deleter::Deleter;
use self::list_writer::DeletionOp;
use self::list_writer::ListWriter;
use self::list_writer::RecoverOp;
use self::validator::Validator;
use deleter::DeleterMessage;
use list_writer::ListWriterQueueMessage;
use validator::ValidatorQueueMessage;

use crate::{config::PageServerConf, tenant::storage_layer::LayerName};

// TODO: configurable for how long to wait before executing deletions

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
/// - Enable gating deletions on validation of a tenant's generation number, to make
///   it safe to multi-attach tenants (see docs/rfcs/025-generation-numbers.md)
///
/// There are two kinds of deletion: deferred and immediate.  A deferred deletion
/// may be intentionally delayed to protect passive readers of S3 data, and is
/// subject to a generation number validation step.  An immediate deletion is
/// ready to execute immediately, and is only queued up so that it can be coalesced
/// with other deletions in flight.
///
/// Deferred deletions pass through three steps:
/// - ListWriter: accumulate deletion requests from Timelines, and batch them up into
///   DeletionLists, which are persisted to disk.
/// - Validator: accumulate deletion lists, and validate them en-masse prior to passing
///   the keys in the list onward for actual deletion.  Also validate remote_consistent_lsn
///   updates for running timelines.
/// - Deleter: accumulate object keys that the validator has validated, and execute them in
///   batches of 1000 keys via DeleteObjects.
///
/// Non-deferred deletions, such as during timeline deletion, bypass the first
/// two stages and are passed straight into the Deleter.
///
/// Internally, each stage is joined by a channel to the next.  On disk, there is only
/// one queue (of DeletionLists), which is written by the frontend and consumed
/// by the backend.
#[derive(Clone)]
pub struct DeletionQueue {
    client: DeletionQueueClient,

    // Parent cancellation token for the tokens passed into background workers
    cancel: CancellationToken,
}

/// Opaque wrapper around individual worker tasks, to avoid making the
/// worker objects themselves public
pub struct DeletionQueueWorkers<C>
where
    C: ControlPlaneGenerationsApi + Send + Sync,
{
    frontend: ListWriter,
    backend: Validator<C>,
    executor: Deleter,
}

impl<C> DeletionQueueWorkers<C>
where
    C: ControlPlaneGenerationsApi + Send + Sync + 'static,
{
    pub fn spawn_with(mut self, runtime: &tokio::runtime::Handle) -> tokio::task::JoinHandle<()> {
        let jh_frontend = runtime.spawn(async move {
            self.frontend
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion frontend"))
                .await
        });
        let jh_backend = runtime.spawn(async move {
            self.backend
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion backend"))
                .await
        });
        let jh_executor = runtime.spawn(async move {
            self.executor
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion executor"))
                .await
        });

        runtime.spawn({
            async move {
                jh_frontend.await.expect("error joining frontend worker");
                jh_backend.await.expect("error joining backend worker");
                drop(jh_executor.await.expect("error joining executor worker"));
            }
        })
    }
}

/// A FlushOp is just a oneshot channel, where we send the transmit side down
/// another channel, and the receive side will receive a message when the channel
/// we're flushing has reached the FlushOp we sent into it.
///
/// The only extra behavior beyond the channel is that the notify() method does not
/// return an error when the receive side has been dropped, because in this use case
/// it is harmless (the code that initiated the flush no longer cares about the result).
#[derive(Debug)]
struct FlushOp {
    tx: tokio::sync::oneshot::Sender<()>,
}

impl FlushOp {
    fn new() -> (Self, tokio::sync::oneshot::Receiver<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        (Self { tx }, rx)
    }

    fn notify(self) {
        if self.tx.send(()).is_err() {
            // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
            debug!("deletion queue flush from dropped client");
        };
    }
}

#[derive(Clone, Debug)]
pub struct DeletionQueueClient {
    tx: tokio::sync::mpsc::UnboundedSender<ListWriterQueueMessage>,
    executor_tx: tokio::sync::mpsc::Sender<DeleterMessage>,

    lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TenantDeletionList {
    /// For each Timeline, a list of key fragments to append to the timeline remote path
    /// when reconstructing a full key
    timelines: HashMap<TimelineId, Vec<String>>,

    /// The generation in which this deletion was emitted: note that this may not be the
    /// same as the generation of any layers being deleted.  The generation of the layer
    /// has already been absorbed into the keys in `objects`
    generation: Generation,
}

impl TenantDeletionList {
    pub(crate) fn len(&self) -> usize {
        self.timelines.values().map(|v| v.len()).sum()
    }
}

/// Files ending with this suffix will be ignored and erased
/// during recovery as startup.
const TEMP_SUFFIX: &str = "tmp";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct DeletionList {
    /// Serialization version, for future use
    version: u8,

    /// Used for constructing a unique key for each deletion list we write out.
    sequence: u64,

    /// To avoid repeating tenant/timeline IDs in every key, we store keys in
    /// nested HashMaps by TenantTimelineID.  Each Tenant only appears once
    /// with one unique generation ID: if someone tries to push a second generation
    /// ID for the same tenant, we will start a new DeletionList.
    tenants: HashMap<TenantShardId, TenantDeletionList>,

    /// Avoid having to walk `tenants` to calculate the number of keys in
    /// the nested deletion lists
    size: usize,

    /// Set to true when the list has undergone validation with the control
    /// plane and the remaining contents of `tenants` are valid.  A list may
    /// also be implicitly marked valid by DeletionHeader.validated_sequence
    /// advancing to >= DeletionList.sequence
    #[serde(default)]
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    validated: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeletionHeader {
    /// Serialization version, for future use
    version: u8,

    /// The highest sequence number (inclusive) that has been validated.  All deletion
    /// lists on disk with a sequence <= this value are safe to execute.
    validated_sequence: u64,
}

impl DeletionHeader {
    const VERSION_LATEST: u8 = 1;

    fn new(validated_sequence: u64) -> Self {
        Self {
            version: Self::VERSION_LATEST,
            validated_sequence,
        }
    }

    async fn save(&self, conf: &'static PageServerConf) -> anyhow::Result<()> {
        debug!("Saving deletion list header {:?}", self);
        let header_bytes = serde_json::to_vec(self).context("serialize deletion header")?;
        let header_path = conf.deletion_header_path();
        let temp_path = path_with_suffix_extension(&header_path, TEMP_SUFFIX);
        VirtualFile::crashsafe_overwrite(header_path, temp_path, header_bytes)
            .await
            .maybe_fatal_err("save deletion header")?;

        Ok(())
    }
}

impl DeletionList {
    const VERSION_LATEST: u8 = 1;
    fn new(sequence: u64) -> Self {
        Self {
            version: Self::VERSION_LATEST,
            sequence,
            tenants: HashMap::new(),
            size: 0,
            validated: false,
        }
    }

    fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the push was accepted, false if the caller must start a new
    /// deletion list.
    fn push(
        &mut self,
        tenant: &TenantShardId,
        timeline: &TimelineId,
        generation: Generation,
        objects: &mut Vec<RemotePath>,
    ) -> bool {
        if objects.is_empty() {
            // Avoid inserting an empty TimelineDeletionList: this preserves the property
            // that if we have no keys, then self.objects is empty (used in Self::is_empty)
            return true;
        }

        let tenant_entry = self
            .tenants
            .entry(*tenant)
            .or_insert_with(|| TenantDeletionList {
                timelines: HashMap::new(),
                generation,
            });

        if tenant_entry.generation != generation {
            // Only one generation per tenant per list: signal to
            // caller to start a new list.
            return false;
        }

        let timeline_entry = tenant_entry.timelines.entry(*timeline).or_default();

        let timeline_remote_path = remote_timeline_path(tenant, timeline);

        self.size += objects.len();
        timeline_entry.extend(objects.drain(..).map(|p| {
            p.strip_prefix(&timeline_remote_path)
                .expect("Timeline paths always start with the timeline prefix")
                .to_string()
        }));
        true
    }

    fn into_remote_paths(self) -> Vec<RemotePath> {
        let mut result = Vec::new();
        for (tenant, tenant_deletions) in self.tenants.into_iter() {
            for (timeline, timeline_layers) in tenant_deletions.timelines.into_iter() {
                let timeline_remote_path = remote_timeline_path(&tenant, &timeline);
                result.extend(
                    timeline_layers
                        .into_iter()
                        .map(|l| timeline_remote_path.join(Utf8PathBuf::from(l))),
                );
            }
        }

        result
    }

    async fn save(&self, conf: &'static PageServerConf) -> anyhow::Result<()> {
        let path = conf.deletion_list_path(self.sequence);
        let temp_path = path_with_suffix_extension(&path, TEMP_SUFFIX);

        let bytes = serde_json::to_vec(self).expect("Failed to serialize deletion list");

        VirtualFile::crashsafe_overwrite(path, temp_path, bytes)
            .await
            .maybe_fatal_err("save deletion list")
            .map_err(Into::into)
    }
}

impl std::fmt::Display for DeletionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeletionList<seq={}, tenants={}, keys={}>",
            self.sequence,
            self.tenants.len(),
            self.size
        )
    }
}

struct PendingLsn {
    projected: Lsn,
    result_slot: Arc<AtomicLsn>,
}

struct TenantLsnState {
    timelines: HashMap<TimelineId, PendingLsn>,

    // In what generation was the most recent update proposed?
    generation: Generation,
}

#[derive(Default)]
struct VisibleLsnUpdates {
    tenants: HashMap<TenantShardId, TenantLsnState>,
}

impl VisibleLsnUpdates {
    fn new() -> Self {
        Self {
            tenants: HashMap::new(),
        }
    }
}

impl std::fmt::Debug for VisibleLsnUpdates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VisibleLsnUpdates({} tenants)", self.tenants.len())
    }
}

#[derive(Error, Debug)]
pub enum DeletionQueueError {
    #[error("Deletion queue unavailable during shutdown")]
    ShuttingDown,
}

impl DeletionQueueClient {
    /// This is cancel-safe.  If you drop the future before it completes, the message
    /// is not pushed, although in the context of the deletion queue it doesn't matter: once
    /// we decide to do a deletion the decision is always final.
    fn do_push<T>(
        &self,
        queue: &tokio::sync::mpsc::UnboundedSender<T>,
        msg: T,
    ) -> Result<(), DeletionQueueError> {
        match queue.send(msg) {
            Ok(_) => Ok(()),
            Err(e) => {
                // This shouldn't happen, we should shut down all tenants before
                // we shut down the global delete queue.  If we encounter a bug like this,
                // we may leak objects as deletions won't be processed.
                error!("Deletion queue closed while pushing, shutting down? ({e})");
                Err(DeletionQueueError::ShuttingDown)
            }
        }
    }

    pub(crate) fn recover(
        &self,
        attached_tenants: HashMap<TenantShardId, Generation>,
    ) -> Result<(), DeletionQueueError> {
        self.do_push(
            &self.tx,
            ListWriterQueueMessage::Recover(RecoverOp { attached_tenants }),
        )
    }

    /// When a Timeline wishes to update the remote_consistent_lsn that it exposes to the outside
    /// world, it must validate its generation number before doing so.  Rather than do this synchronously,
    /// we allow the timeline to publish updates at will via this API, and then read back what LSN was most
    /// recently validated separately.
    ///
    /// In this function we publish the LSN to the `projected` field of the timeline's entry in the VisibleLsnUpdates.  The
    /// backend will later wake up and notice that the tenant's generation requires validation.
    pub(crate) async fn update_remote_consistent_lsn(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        current_generation: Generation,
        lsn: Lsn,
        result_slot: Arc<AtomicLsn>,
    ) {
        let mut locked = self
            .lsn_table
            .write()
            .expect("Lock should never be poisoned");

        let tenant_entry = locked
            .tenants
            .entry(tenant_shard_id)
            .or_insert(TenantLsnState {
                timelines: HashMap::new(),
                generation: current_generation,
            });

        if tenant_entry.generation != current_generation {
            // Generation might have changed if we were detached and then re-attached: in this case,
            // state from the previous generation cannot be trusted.
            tenant_entry.timelines.clear();
            tenant_entry.generation = current_generation;
        }

        tenant_entry.timelines.insert(
            timeline_id,
            PendingLsn {
                projected: lsn,
                result_slot,
            },
        );
    }

    /// Submit a list of layers for deletion: this function will return before the deletion is
    /// persistent, but it may be executed at any time after this function enters: do not push
    /// layers until you're sure they can be deleted safely (i.e. remote metadata no longer
    /// references them).
    ///
    /// The `current_generation` is the generation of this pageserver's current attachment.  The
    /// generations in `layers` are the generations in which those layers were written.
    pub(crate) async fn push_layers(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        current_generation: Generation,
        layers: Vec<(LayerName, LayerFileMetadata)>,
    ) -> Result<(), DeletionQueueError> {
        if current_generation.is_none() {
            debug!("Enqueuing deletions in legacy mode, skipping queue");

            let mut layer_paths = Vec::new();
            for (layer, meta) in layers {
                layer_paths.push(remote_layer_path(
                    &tenant_shard_id.tenant_id,
                    &timeline_id,
                    meta.shard,
                    &layer,
                    meta.generation,
                ));
            }
            self.push_immediate(layer_paths).await?;
            return self.flush_immediate().await;
        }

        self.push_layers_sync(tenant_shard_id, timeline_id, current_generation, layers)
    }

    /// When a Tenant has a generation, push_layers is always synchronous because
    /// the ListValidator channel is an unbounded channel.
    ///
    /// This can be merged into push_layers when we remove the Generation-less mode
    /// support (`<https://github.com/neondatabase/neon/issues/5395>`)
    pub(crate) fn push_layers_sync(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        current_generation: Generation,
        layers: Vec<(LayerName, LayerFileMetadata)>,
    ) -> Result<(), DeletionQueueError> {
        metrics::DELETION_QUEUE
            .keys_submitted
            .inc_by(layers.len() as u64);
        self.do_push(
            &self.tx,
            ListWriterQueueMessage::Delete(DeletionOp {
                tenant_shard_id,
                timeline_id,
                layers,
                generation: current_generation,
                objects: Vec::new(),
            }),
        )
    }

    /// This is cancel-safe.  If you drop the future the flush may still happen in the background.
    async fn do_flush<T>(
        &self,
        queue: &tokio::sync::mpsc::UnboundedSender<T>,
        msg: T,
        rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), DeletionQueueError> {
        self.do_push(queue, msg)?;
        if rx.await.is_err() {
            // This shouldn't happen if tenants are shut down before deletion queue.  If we
            // encounter a bug like this, then a flusher will incorrectly believe it has flushed
            // when it hasn't, possibly leading to leaking objects.
            error!("Deletion queue dropped flush op while client was still waiting");
            Err(DeletionQueueError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    /// Wait until all previous deletions are persistent (either executed, or written to a DeletionList)
    ///
    /// This is cancel-safe.  If you drop the future the flush may still happen in the background.
    pub async fn flush(&self) -> Result<(), DeletionQueueError> {
        let (flush_op, rx) = FlushOp::new();
        self.do_flush(&self.tx, ListWriterQueueMessage::Flush(flush_op), rx)
            .await
    }

    /// Issue a flush without waiting for it to complete.  This is useful on advisory flushes where
    /// the caller wants to avoid the risk of waiting for lots of enqueued work, such as on tenant
    /// detach where flushing is nice but not necessary.
    ///
    /// This function provides no guarantees of work being done.
    pub fn flush_advisory(&self) {
        let (flush_op, _) = FlushOp::new();

        // Transmit the flush message, ignoring any result (such as a closed channel during shutdown).
        drop(self.tx.send(ListWriterQueueMessage::FlushExecute(flush_op)));
    }

    // Wait until all previous deletions are executed
    pub(crate) async fn flush_execute(&self) -> Result<(), DeletionQueueError> {
        debug!("flush_execute: flushing to deletion lists...");
        // Flush any buffered work to deletion lists
        self.flush().await?;

        // Flush the backend into the executor of deletion lists
        let (flush_op, rx) = FlushOp::new();
        debug!("flush_execute: flushing backend...");
        self.do_flush(&self.tx, ListWriterQueueMessage::FlushExecute(flush_op), rx)
            .await?;
        debug!("flush_execute: finished flushing backend...");

        // Flush any immediate-mode deletions (the above backend flush will only flush
        // the executor if deletions had flowed through the backend)
        debug!("flush_execute: flushing execution...");
        self.flush_immediate().await?;
        debug!("flush_execute: finished flushing execution...");
        Ok(())
    }

    /// This interface bypasses the persistent deletion queue, and any validation
    /// that this pageserver is still elegible to execute the deletions.  It is for
    /// use in timeline deletions, where the control plane is telling us we may
    /// delete everything in the timeline.
    ///
    /// DO NOT USE THIS FROM GC OR COMPACTION CODE.  Use the regular `push_layers`.
    pub(crate) async fn push_immediate(
        &self,
        objects: Vec<RemotePath>,
    ) -> Result<(), DeletionQueueError> {
        metrics::DELETION_QUEUE
            .keys_submitted
            .inc_by(objects.len() as u64);
        self.executor_tx
            .send(DeleterMessage::Delete(objects))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)
    }

    /// Companion to push_immediate.  When this returns Ok, all prior objects sent
    /// into push_immediate have been deleted from remote storage.
    pub(crate) async fn flush_immediate(&self) -> Result<(), DeletionQueueError> {
        let (flush_op, rx) = FlushOp::new();
        self.executor_tx
            .send(DeleterMessage::Flush(flush_op))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)?;

        rx.await.map_err(|_| DeletionQueueError::ShuttingDown)
    }
}

impl DeletionQueue {
    pub fn new_client(&self) -> DeletionQueueClient {
        self.client.clone()
    }

    /// Caller may use the returned object to construct clients with new_client.
    /// Caller should tokio::spawn the background() members of the two worker objects returned:
    /// we don't spawn those inside new() so that the caller can use their runtime/spans of choice.
    pub fn new<C>(
        remote_storage: GenericRemoteStorage,
        controller_upcall_client: Option<C>,
        conf: &'static PageServerConf,
    ) -> (Self, DeletionQueueWorkers<C>)
    where
        C: ControlPlaneGenerationsApi + Send + Sync,
    {
        // Unbounded channel: enables non-async functions to submit deletions.  The actual length is
        // constrained by how promptly the ListWriter wakes up and drains it, which should be frequent
        // enough to avoid this taking pathologically large amount of memory.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Shallow channel: it carries DeletionLists which each contain up to thousands of deletions
        let (backend_tx, backend_rx) = tokio::sync::mpsc::channel(16);

        // Shallow channel: it carries lists of paths, and we expect the main queueing to
        // happen in the backend (persistent), not in this queue.
        let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16);

        let lsn_table = Arc::new(std::sync::RwLock::new(VisibleLsnUpdates::new()));

        // The deletion queue has an independent cancellation token to
        // the general pageserver shutdown token, because it stays alive a bit
        // longer to flush after Tenants have all been torn down.
        let cancel = CancellationToken::new();

        (
            Self {
                client: DeletionQueueClient {
                    tx,
                    executor_tx: executor_tx.clone(),
                    lsn_table: lsn_table.clone(),
                },
                cancel: cancel.clone(),
            },
            DeletionQueueWorkers {
                frontend: ListWriter::new(conf, rx, backend_tx, cancel.clone()),
                backend: Validator::new(
                    conf,
                    backend_rx,
                    executor_tx,
                    controller_upcall_client,
                    lsn_table.clone(),
                    cancel.clone(),
                ),
                executor: Deleter::new(remote_storage, executor_rx, cancel.clone()),
            },
        )
    }

    pub async fn shutdown(&mut self, timeout: Duration) {
        match tokio::time::timeout(timeout, self.client.flush()).await {
            Ok(Ok(())) => {
                tracing::info!("Deletion queue flushed successfully on shutdown")
            }
            Ok(Err(DeletionQueueError::ShuttingDown)) => {
                // This is not harmful for correctness, but is unexpected: the deletion
                // queue's workers should stay alive as long as there are any client handles instantiated.
                tracing::warn!("Deletion queue stopped prematurely");
            }
            Err(_timeout) => {
                tracing::warn!("Timed out flushing deletion queue on shutdown")
            }
        }

        // We only cancel _after_ flushing: otherwise we would be shutting down the
        // components that do the flush.
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod test {
    use camino::Utf8Path;
    use hex_literal::hex;
    use pageserver_api::{key::Key, shard::ShardIndex, upcall_api::ReAttachResponseTenant};
    use std::{io::ErrorKind, time::Duration};
    use tracing::info;

    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use tokio::task::JoinHandle;

    use crate::{
        controller_upcall_client::RetryForeverError,
        tenant::{harness::TenantShardHarness, storage_layer::DeltaLayerName},
    };

    use super::*;
    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));

    pub const EXAMPLE_LAYER_NAME: LayerName = LayerName::Delta(DeltaLayerName {
        key_range: Key::from_i128(0x0)..Key::from_i128(0xFFFFFFFFFFFFFFFF),
        lsn_range: Lsn(0x00000000016B59D8)..Lsn(0x00000000016B5A51),
    });

    // When you need a second layer in a test.
    pub const EXAMPLE_LAYER_NAME_ALT: LayerName = LayerName::Delta(DeltaLayerName {
        key_range: Key::from_i128(0x0)..Key::from_i128(0xFFFFFFFFFFFFFFFF),
        lsn_range: Lsn(0x00000000016B5A51)..Lsn(0x00000000016B5A61),
    });

    struct TestSetup {
        harness: TenantShardHarness,
        remote_fs_dir: Utf8PathBuf,
        storage: GenericRemoteStorage,
        mock_control_plane: MockControlPlane,
        deletion_queue: DeletionQueue,
        worker_join: JoinHandle<()>,
    }

    impl TestSetup {
        /// Simulate a pageserver restart by destroying and recreating the deletion queue
        async fn restart(&mut self) {
            let (deletion_queue, workers) = DeletionQueue::new(
                self.storage.clone(),
                Some(self.mock_control_plane.clone()),
                self.harness.conf,
            );

            tracing::debug!("Spawning worker for new queue queue");
            let worker_join = workers.spawn_with(&tokio::runtime::Handle::current());

            let old_worker_join = std::mem::replace(&mut self.worker_join, worker_join);
            let old_deletion_queue = std::mem::replace(&mut self.deletion_queue, deletion_queue);

            tracing::debug!("Joining worker from previous queue");
            old_deletion_queue.cancel.cancel();
            old_worker_join
                .await
                .expect("Failed to join workers for previous deletion queue");
        }

        fn set_latest_generation(&self, gen: Generation) {
            let tenant_shard_id = self.harness.tenant_shard_id;
            self.mock_control_plane
                .latest_generation
                .lock()
                .unwrap()
                .insert(tenant_shard_id, gen);
        }

        /// Returns remote layer file name, suitable for use in assert_remote_files
        fn write_remote_layer(
            &self,
            file_name: LayerName,
            gen: Generation,
        ) -> anyhow::Result<String> {
            let tenant_shard_id = self.harness.tenant_shard_id;
            let relative_remote_path = remote_timeline_path(&tenant_shard_id, &TIMELINE_ID);
            let remote_timeline_path = self.remote_fs_dir.join(relative_remote_path.get_path());
            std::fs::create_dir_all(&remote_timeline_path)?;
            let remote_layer_file_name = format!("{}{}", file_name, gen.get_suffix());

            let content: Vec<u8> = format!("placeholder contents of {file_name}").into();

            std::fs::write(
                remote_timeline_path.join(remote_layer_file_name.clone()),
                content,
            )?;

            Ok(remote_layer_file_name)
        }
    }

    #[derive(Debug, Clone)]
    struct MockControlPlane {
        pub latest_generation: std::sync::Arc<std::sync::Mutex<HashMap<TenantShardId, Generation>>>,
    }

    impl MockControlPlane {
        fn new() -> Self {
            Self {
                latest_generation: Arc::default(),
            }
        }
    }

    impl ControlPlaneGenerationsApi for MockControlPlane {
        async fn re_attach(
            &self,
            _conf: &PageServerConf,
        ) -> Result<HashMap<TenantShardId, ReAttachResponseTenant>, RetryForeverError> {
            unimplemented!()
        }

        async fn validate(
            &self,
            tenants: Vec<(TenantShardId, Generation)>,
        ) -> Result<HashMap<TenantShardId, bool>, RetryForeverError> {
            let mut result = HashMap::new();

            let latest_generation = self.latest_generation.lock().unwrap();

            for (tenant_shard_id, generation) in tenants {
                if let Some(latest) = latest_generation.get(&tenant_shard_id) {
                    result.insert(tenant_shard_id, *latest == generation);
                }
            }

            Ok(result)
        }
    }

    async fn setup(test_name: &str) -> anyhow::Result<TestSetup> {
        let test_name = Box::leak(Box::new(format!("deletion_queue__{test_name}")));
        let harness = TenantShardHarness::create(test_name).await?;

        // We do not load() the harness: we only need its config and remote_storage

        // Set up a GenericRemoteStorage targetting a directory
        let remote_fs_dir = harness.conf.workdir.join("remote_fs");
        std::fs::create_dir_all(remote_fs_dir)?;
        let remote_fs_dir = harness.conf.workdir.join("remote_fs").canonicalize_utf8()?;
        let storage_config = RemoteStorageConfig {
            storage: RemoteStorageKind::LocalFs {
                local_path: remote_fs_dir.clone(),
            },
            timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
            small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT,
        };
        let storage = GenericRemoteStorage::from_config(&storage_config)
            .await
            .unwrap();

        let mock_control_plane = MockControlPlane::new();

        let (deletion_queue, worker) = DeletionQueue::new(
            storage.clone(),
            Some(mock_control_plane.clone()),
            harness.conf,
        );

        let worker_join = worker.spawn_with(&tokio::runtime::Handle::current());

        Ok(TestSetup {
            harness,
            remote_fs_dir,
            storage,
            mock_control_plane,
            deletion_queue,
            worker_join,
        })
    }

    // TODO: put this in a common location so that we can share with remote_timeline_client's tests
    fn assert_remote_files(expected: &[&str], remote_path: &Utf8Path) {
        let mut expected: Vec<String> = expected.iter().map(|x| String::from(*x)).collect();
        expected.sort();

        let mut found: Vec<String> = Vec::new();
        let dir = match std::fs::read_dir(remote_path) {
            Ok(d) => d,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    if expected.is_empty() {
                        // We are asserting prefix is empty: it is expected that the dir is missing
                        return;
                    } else {
                        assert_eq!(expected, Vec::<String>::new());
                        unreachable!();
                    }
                } else {
                    panic!("Unexpected error listing {remote_path}: {e}");
                }
            }
        };

        for entry in dir.flatten() {
            let entry_name = entry.file_name();
            let fname = entry_name.to_str().unwrap();
            found.push(String::from(fname));
        }
        found.sort();

        assert_eq!(expected, found);
    }

    fn assert_local_files(expected: &[&str], directory: &Utf8Path) {
        let dir = match std::fs::read_dir(directory) {
            Ok(d) => d,
            Err(_) => {
                assert_eq!(expected, &Vec::<String>::new());
                return;
            }
        };
        let mut found = Vec::new();
        for dentry in dir {
            let dentry = dentry.unwrap();
            let file_name = dentry.file_name();
            let file_name_str = file_name.to_string_lossy();
            found.push(file_name_str.to_string());
        }
        found.sort();
        assert_eq!(expected, found);
    }

    #[tokio::test]
    async fn deletion_queue_smoke() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let ctx = setup("deletion_queue_smoke")
            .await
            .expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();
        client.recover(HashMap::new())?;

        let layer_file_name_1: LayerName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let tenant_shard_id = ctx.harness.tenant_shard_id;

        let content: Vec<u8> = "victim1 contents".into();
        let relative_remote_path = remote_timeline_path(&tenant_shard_id, &TIMELINE_ID);
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());
        let deletion_prefix = ctx.harness.conf.deletion_prefix();

        // Exercise the distinction between the generation of the layers
        // we delete, and the generation of the running Tenant.
        let layer_generation = Generation::new(0xdeadbeef);
        let now_generation = Generation::new(0xfeedbeef);
        let layer_metadata =
            LayerFileMetadata::new(0xf00, layer_generation, ShardIndex::unsharded());

        let remote_layer_file_name_1 =
            format!("{}{}", layer_file_name_1, layer_generation.get_suffix());

        // Set mock control plane state to valid for our generation
        ctx.set_latest_generation(now_generation);

        // Inject a victim file to remote storage
        info!("Writing");
        std::fs::create_dir_all(&remote_timeline_path)?;
        std::fs::write(
            remote_timeline_path.join(remote_layer_file_name_1.clone()),
            content,
        )?;
        assert_remote_files(&[&remote_layer_file_name_1], &remote_timeline_path);

        // File should still be there after we push it to the queue (we haven't pushed enough to flush anything)
        info!("Pushing");
        client
            .push_layers(
                tenant_shard_id,
                TIMELINE_ID,
                now_generation,
                [(layer_file_name_1.clone(), layer_metadata)].to_vec(),
            )
            .await?;
        assert_remote_files(&[&remote_layer_file_name_1], &remote_timeline_path);

        assert_local_files(&[], &deletion_prefix);

        // File should still be there after we write a deletion list (we haven't pushed enough to execute anything)
        info!("Flushing");
        client.flush().await?;
        assert_remote_files(&[&remote_layer_file_name_1], &remote_timeline_path);
        assert_local_files(&["0000000000000001-01.list"], &deletion_prefix);

        // File should go away when we execute
        info!("Flush-executing");
        client.flush_execute().await?;
        assert_remote_files(&[], &remote_timeline_path);
        assert_local_files(&["header-01"], &deletion_prefix);

        // Flushing on an empty queue should succeed immediately, and not write any lists
        info!("Flush-executing on empty");
        client.flush_execute().await?;
        assert_local_files(&["header-01"], &deletion_prefix);

        Ok(())
    }

    #[tokio::test]
    async fn deletion_queue_validation() -> anyhow::Result<()> {
        let ctx = setup("deletion_queue_validation")
            .await
            .expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();
        client.recover(HashMap::new())?;

        // Generation that the control plane thinks is current
        let latest_generation = Generation::new(0xdeadbeef);
        // Generation that our DeletionQueue thinks the tenant is running with
        let stale_generation = latest_generation.previous();
        // Generation that our example layer file was written with
        let layer_generation = stale_generation.previous();
        let layer_metadata =
            LayerFileMetadata::new(0xf00, layer_generation, ShardIndex::unsharded());

        ctx.set_latest_generation(latest_generation);

        let tenant_shard_id = ctx.harness.tenant_shard_id;
        let relative_remote_path = remote_timeline_path(&tenant_shard_id, &TIMELINE_ID);
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());

        // Initial state: a remote layer exists
        let remote_layer_name = ctx.write_remote_layer(EXAMPLE_LAYER_NAME, layer_generation)?;
        assert_remote_files(&[&remote_layer_name], &remote_timeline_path);

        tracing::debug!("Pushing...");
        client
            .push_layers(
                tenant_shard_id,
                TIMELINE_ID,
                stale_generation,
                [(EXAMPLE_LAYER_NAME.clone(), layer_metadata.clone())].to_vec(),
            )
            .await?;

        // We enqueued the operation in a stale generation: it should have failed validation
        tracing::debug!("Flushing...");
        tokio::time::timeout(Duration::from_secs(5), client.flush_execute()).await??;
        assert_remote_files(&[&remote_layer_name], &remote_timeline_path);

        tracing::debug!("Pushing...");
        client
            .push_layers(
                tenant_shard_id,
                TIMELINE_ID,
                latest_generation,
                [(EXAMPLE_LAYER_NAME.clone(), layer_metadata.clone())].to_vec(),
            )
            .await?;

        // We enqueued the operation in a fresh generation: it should have passed validation
        tracing::debug!("Flushing...");
        tokio::time::timeout(Duration::from_secs(5), client.flush_execute()).await??;
        assert_remote_files(&[], &remote_timeline_path);

        Ok(())
    }

    #[tokio::test]
    async fn deletion_queue_recovery() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let mut ctx = setup("deletion_queue_recovery")
            .await
            .expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();
        client.recover(HashMap::new())?;

        let tenant_shard_id = ctx.harness.tenant_shard_id;

        let relative_remote_path = remote_timeline_path(&tenant_shard_id, &TIMELINE_ID);
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());
        let deletion_prefix = ctx.harness.conf.deletion_prefix();

        let layer_generation = Generation::new(0xdeadbeef);
        let now_generation = Generation::new(0xfeedbeef);
        let layer_metadata =
            LayerFileMetadata::new(0xf00, layer_generation, ShardIndex::unsharded());

        // Inject a deletion in the generation before generation_now: after restart,
        // this deletion should _not_ get executed (only the immediately previous
        // generation gets that treatment)
        let remote_layer_file_name_historical =
            ctx.write_remote_layer(EXAMPLE_LAYER_NAME, layer_generation)?;
        client
            .push_layers(
                tenant_shard_id,
                TIMELINE_ID,
                now_generation.previous(),
                [(EXAMPLE_LAYER_NAME.clone(), layer_metadata.clone())].to_vec(),
            )
            .await?;

        // Inject a deletion in the generation before generation_now: after restart,
        // this deletion should get executed, because we execute deletions in the
        // immediately previous generation on the same node.
        let remote_layer_file_name_previous =
            ctx.write_remote_layer(EXAMPLE_LAYER_NAME_ALT, layer_generation)?;
        client
            .push_layers(
                tenant_shard_id,
                TIMELINE_ID,
                now_generation,
                [(EXAMPLE_LAYER_NAME_ALT.clone(), layer_metadata.clone())].to_vec(),
            )
            .await?;

        client.flush().await?;
        assert_remote_files(
            &[
                &remote_layer_file_name_historical,
                &remote_layer_file_name_previous,
            ],
            &remote_timeline_path,
        );

        // Different generatinos for the same tenant will cause two separate
        // deletion lists to be emitted.
        assert_local_files(
            &["0000000000000001-01.list", "0000000000000002-01.list"],
            &deletion_prefix,
        );

        // Simulate a node restart: the latest generation advances
        let now_generation = now_generation.next();
        ctx.set_latest_generation(now_generation);

        // Restart the deletion queue
        drop(client);
        ctx.restart().await;
        let client = ctx.deletion_queue.new_client();
        client.recover(HashMap::from([(tenant_shard_id, now_generation)]))?;

        info!("Flush-executing");
        client.flush_execute().await?;
        // The deletion from immediately prior generation was executed, the one from
        // an older generation was not.
        assert_remote_files(&[&remote_layer_file_name_historical], &remote_timeline_path);
        Ok(())
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
#[cfg(test)]
pub(crate) mod mock {
    use tracing::info;

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct ConsumerState {
        rx: tokio::sync::mpsc::UnboundedReceiver<ListWriterQueueMessage>,
        executor_rx: tokio::sync::mpsc::Receiver<DeleterMessage>,
        cancel: CancellationToken,
        executed: Arc<AtomicUsize>,
    }

    impl ConsumerState {
        async fn consume(&mut self, remote_storage: &GenericRemoteStorage) {
            info!("Executing all pending deletions");

            // Transform all executor messages to generic frontend messages
            loop {
                use either::Either;
                let msg = tokio::select! {
                    left = self.executor_rx.recv() => Either::Left(left),
                    right = self.rx.recv() => Either::Right(right),
                };
                match msg {
                    Either::Left(None) => break,
                    Either::Right(None) => break,
                    Either::Left(Some(DeleterMessage::Delete(objects))) => {
                        for path in objects {
                            match remote_storage.delete(&path, &self.cancel).await {
                                Ok(_) => {
                                    debug!("Deleted {path}");
                                }
                                Err(e) => {
                                    error!("Failed to delete {path}, leaking object! ({e})");
                                }
                            }
                            self.executed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Either::Left(Some(DeleterMessage::Flush(flush_op))) => {
                        flush_op.notify();
                    }
                    Either::Right(Some(ListWriterQueueMessage::Delete(op))) => {
                        let mut objects = op.objects;
                        for (layer, meta) in op.layers {
                            objects.push(remote_layer_path(
                                &op.tenant_shard_id.tenant_id,
                                &op.timeline_id,
                                meta.shard,
                                &layer,
                                meta.generation,
                            ));
                        }

                        for path in objects {
                            info!("Executing deletion {path}");
                            match remote_storage.delete(&path, &self.cancel).await {
                                Ok(_) => {
                                    debug!("Deleted {path}");
                                }
                                Err(e) => {
                                    error!("Failed to delete {path}, leaking object! ({e})");
                                }
                            }
                            self.executed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Either::Right(Some(ListWriterQueueMessage::Flush(op))) => {
                        op.notify();
                    }
                    Either::Right(Some(ListWriterQueueMessage::FlushExecute(op))) => {
                        // We have already executed all prior deletions because mock does them inline
                        op.notify();
                    }
                    Either::Right(Some(ListWriterQueueMessage::Recover(_))) => {
                        // no-op in mock
                    }
                }
            }
        }
    }

    pub struct MockDeletionQueue {
        tx: tokio::sync::mpsc::UnboundedSender<ListWriterQueueMessage>,
        executor_tx: tokio::sync::mpsc::Sender<DeleterMessage>,
        lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,
    }

    impl MockDeletionQueue {
        pub fn new(remote_storage: Option<GenericRemoteStorage>) -> Self {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16384);

            let executed = Arc::new(AtomicUsize::new(0));

            let mut consumer = ConsumerState {
                rx,
                executor_rx,
                cancel: CancellationToken::new(),
                executed: executed.clone(),
            };

            tokio::spawn(async move {
                if let Some(remote_storage) = &remote_storage {
                    consumer.consume(remote_storage).await;
                }
            });

            Self {
                tx,
                executor_tx,
                lsn_table: Arc::new(std::sync::RwLock::new(VisibleLsnUpdates::new())),
            }
        }

        #[allow(clippy::await_holding_lock)]
        pub async fn pump(&self) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.executor_tx
                .send(DeleterMessage::Flush(FlushOp { tx }))
                .await
                .expect("Failed to send flush message");
            rx.await.ok();
        }

        pub(crate) fn new_client(&self) -> DeletionQueueClient {
            DeletionQueueClient {
                tx: self.tx.clone(),
                executor_tx: self.executor_tx.clone(),
                lsn_table: self.lsn_table.clone(),
            }
        }
    }

    /// Test round-trip serialization/deserialization, and test stability of the format
    /// vs. a static expected string for the serialized version.
    #[test]
    fn deletion_list_serialization() -> anyhow::Result<()> {
        let tenant_id = "ad6c1a56f5680419d3a16ff55d97ec3c"
            .to_string()
            .parse::<TenantShardId>()?;
        let timeline_id = "be322c834ed9e709e63b5c9698691910"
            .to_string()
            .parse::<TimelineId>()?;
        let generation = Generation::new(123);

        let object =
            RemotePath::from_string(&format!("tenants/{tenant_id}/timelines/{timeline_id}/foo"))?;
        let mut objects = [object].to_vec();

        let mut example = DeletionList::new(1);
        example.push(&tenant_id, &timeline_id, generation, &mut objects);

        let encoded = serde_json::to_string(&example)?;

        let expected = "{\"version\":1,\"sequence\":1,\"tenants\":{\"ad6c1a56f5680419d3a16ff55d97ec3c\":{\"timelines\":{\"be322c834ed9e709e63b5c9698691910\":[\"foo\"]},\"generation\":123}},\"size\":1}".to_string();
        assert_eq!(encoded, expected);

        let decoded = serde_json::from_str::<DeletionList>(&encoded)?;
        assert_eq!(example, decoded);

        Ok(())
    }
}
