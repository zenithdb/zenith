use remote_storage::{GenericRemoteStorage, RemotePath};
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use tokio;
use tokio::time::{Duration, Instant};
use tracing::{self, debug, error, info, warn};
use utils::id::{TenantId, TimelineId};

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

// TODO: small value is just for testing, make this bigger
const DELETION_LIST_TARGET_SIZE: usize = 16;

// Ordinarily, we only flush to DeletionList periodically, to bound the window during
// which we might leak objects from not flushing a DeletionList after
// the objects are already unlinked from timeline metadata.
const FLUSH_DEFAULT_DEADLINE: Duration = Duration::from_millis(10000);

// If someone is waiting for a flush to DeletionList, only delay a little to accumulate
// more objects before doing the flush.
const FLUSH_EXPLICIT_DEADLINE: Duration = Duration::from_millis(100);

// TODO: metrics for queue length, deletions executed, deletion errors

// TODO: adminstrative "panic button" config property to disable all deletions

// TODO: implement admin API hook to flush deletion queue, for use in integration tests
//       that would like to assert deleted objects are gone

// TODO: configurable for how long to wait before executing deletions

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
///
/// There are two parts ot this, frontend and backend, joined by channels:
/// - DeletionQueueWorker consumes the frontend queue: the "DeletionQueue" that makes up
///   the public interface and accepts deletion requests.
/// - BackendQueueWorker consumes the backend queue: a queue of DeletionList that have
///   already been written to S3 and are now eligible for final deletion.
///   
///
///
///
/// There are three queues internally:
/// - Incoming deletes (the DeletionQueue that the outside world sees)
/// - Persistent deletion blocks: these represent deletion lists that have already been written to S3 and
///   are pending execution.
/// - Deletions read back frorm the persistent deletion blocks, which are batched up into groups
///   of 1024 for execution via a DeleteObjects call.
#[derive(Clone)]
pub struct DeletionQueue {
    tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
}

#[derive(Debug)]
enum FrontendQueueMessage {
    Delete(DeletionOp),
    // Wait until all prior deletions make it into a persistent DeletionList
    Flush(FlushOp),
    // Wait until all prior deletions have been executed (i.e. objects are actually deleted)
    FlushExecute(FlushOp),
}

#[derive(Debug)]
struct DeletionOp {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layers: Vec<LayerFileName>,
}

#[derive(Debug)]
struct FlushOp {
    tx: tokio::sync::oneshot::Sender<()>,
}

impl FlushOp {
    fn fire(self) {
        if let Err(_) = self.tx.send(()) {
            // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
            debug!("deletion queue flush from dropped client");
        };
    }
}

#[derive(Clone)]
pub struct DeletionQueueClient {
    tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct DeletionList {
    /// Used for constructing a unique key for each deletion list we write out.
    sequence: u64,

    /// These objects are elegible for deletion: they are unlinked from timeline metadata, and
    /// we are free to delete them at any time from their presence in this data structure onwards.
    objects: Vec<RemotePath>,
}

impl DeletionList {
    fn new(sequence: u64) -> Self {
        Self {
            sequence,
            objects: Vec::new(),
        }
    }
}

impl DeletionQueueClient {
    async fn do_push(&self, msg: FrontendQueueMessage) {
        match self.tx.send(msg).await {
            Ok(_) => {}
            Err(e) => {
                // This shouldn't happen, we should shut down all tenants before
                // we shut down the global delete queue.  If we encounter a bug like this,
                // we may leak objects as deletions won't be processed.
                error!("Deletion queue closed while pushing, shutting down? ({e})");
            }
        }
    }

    /// Submit a list of layers for deletion: this function will return before the deletion is
    /// persistent, but it may be executed at any time after this function enters: do not push
    /// layers until you're sure they can be deleted safely (i.e. remote metadata no longer
    /// references them).
    pub async fn push(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        layers: Vec<LayerFileName>,
    ) {
        self.do_push(FrontendQueueMessage::Delete(DeletionOp {
            tenant_id,
            timeline_id,
            layers,
        }))
        .await;
    }

    async fn do_flush(&self, msg: FrontendQueueMessage, rx: tokio::sync::oneshot::Receiver<()>) {
        self.do_push(msg).await;
        if let Err(_) = rx.await {
            // This shouldn't happen if tenants are shut down before deletion queue.  If we
            // encounter a bug like this, then a flusher will incorrectly believe it has flushed
            // when it hasn't, possibly leading to leaking objects.
            error!("Deletion queue dropped flush op while client was still waiting");
        }
    }

    /// Wait until all previous deletions are persistent (either executed, or written to a DeletionList)
    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.do_flush(FrontendQueueMessage::Flush(FlushOp { tx }), rx)
            .await
    }

    // Wait until all previous deletions are executed
    pub async fn flush_execute(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.do_flush(FrontendQueueMessage::FlushExecute(FlushOp { tx }), rx)
            .await
    }
}

pub struct BackendQueueWorker {
    remote_storage: GenericRemoteStorage,
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,

    // Accumulate up to 1024 keys for the next deletion operation
    accumulator: Vec<RemotePath>,

    // DeletionLists we have fully ingested but might still have
    // some keys in accumulator.
    pending_lists: Vec<DeletionList>,

    // DeletionLists we have fully executed, which may be deleted
    // from remote storage.
    executed_lists: Vec<DeletionList>,
}

impl BackendQueueWorker {
    async fn maybe_execute(&mut self) {
        match self.remote_storage.delete_objects(&self.accumulator).await {
            Ok(()) => {
                self.accumulator.clear();
                self.executed_lists.append(&mut self.pending_lists);
            }
            Err(e) => {
                warn!("Batch deletion failed: {e}, will retry");
                // TODO: increment error counter
            }
        }
    }

    pub async fn background(&mut self) {
        let _span = tracing::info_span!("deletion_backend");

        // TODO: if we would like to be able to defer deletions while a Layer still has
        // refs (but it will be elegible for deletion after process ends), then we may
        // add an ephemeral part to BackendQueueMessage::Delete that tracks which keys
        // in the deletion list may not be deleted yet, with guards to block on while
        // we wait to proceed.

        // From the S3 spec
        const MAX_KEYS_PER_DELETE: usize = 1024;

        self.accumulator.reserve(MAX_KEYS_PER_DELETE);

        while let Some(msg) = self.rx.recv().await {
            match msg {
                BackendQueueMessage::Delete(mut list) => {
                    if list.objects.is_empty() {
                        // This shouldn't happen, but is harmless.  warn so that
                        // tests will fail if we have such a bug, but proceed with
                        // processing subsequent messages.
                        warn!("Empty DeletionList passed to deletion backend");
                        self.executed_lists.push(list);
                        continue;
                    }

                    // This loop handles deletion lists that require multiple DeleteObjects requests,
                    // and also handles retries if a deletion fails: we will keep going around until
                    // we have either deleted everything, or we have a remainder in accumulator.
                    while !list.objects.is_empty() || self.accumulator.len() == MAX_KEYS_PER_DELETE
                    {
                        let take_count = if self.accumulator.len() == MAX_KEYS_PER_DELETE {
                            0
                        } else {
                            let available_slots = MAX_KEYS_PER_DELETE - self.accumulator.len();
                            std::cmp::min(available_slots, list.objects.len())
                        };

                        for object in list.objects.drain(list.objects.len() - take_count..) {
                            self.accumulator.push(object);
                        }

                        if self.accumulator.len() == MAX_KEYS_PER_DELETE {
                            // Great, we got a full request: issue it.
                            self.maybe_execute().await;
                        }
                    }

                    if !self.accumulator.is_empty() {
                        // We have a remainder, deletion list is not fully processed yet
                        self.pending_lists.push(list);
                    } else {
                        // We fully processed this list, it is ready for purge
                        self.executed_lists.push(list);
                    }

                    let executed_keys: Vec<RemotePath> = self
                        .executed_lists
                        .iter()
                        .rev()
                        .take(MAX_KEYS_PER_DELETE)
                        .map(|l| {
                            RemotePath::new(&self.conf.remote_deletion_list_path(l.sequence))
                                .expect("Failed to compose deletion list path")
                        })
                        .collect();

                    match self.remote_storage.delete_objects(&executed_keys).await {
                        Ok(()) => {
                            // Retain any lists that couldn't be deleted in that request
                            self.executed_lists
                                .truncate(self.executed_lists.len() - executed_keys.len());
                        }
                        Err(e) => {
                            warn!("Failed to purge deletion lists: {e}");
                            // Do nothing: the elements remain in executed_lists, and purge will be retried
                            // next time we process some deletions and go around the loop.
                        }
                    }
                }
                BackendQueueMessage::Flush(op) => {
                    while !self.accumulator.is_empty() {
                        self.maybe_execute().await;
                    }

                    op.fire();
                }
            }
        }
    }
}

#[derive(Debug)]
enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}

pub struct FrontendQueueWorker {
    remote_storage: GenericRemoteStorage,
    conf: &'static PageServerConf,

    // Incoming frontend requests to delete some keys
    rx: tokio::sync::mpsc::Receiver<FrontendQueueMessage>,

    // Outbound requests to the backend to execute deletion lists we have composed.
    tx: tokio::sync::mpsc::Sender<BackendQueueMessage>,

    // The list we are currently building, contains a buffer of keys to delete
    // and our next sequence number
    pending: DeletionList,

    // When we should next proactively flush if we have pending deletions, even if
    // the target deletion list size has not been reached.
    deadline: Instant,

    // These FlushOps should fire the next time we flush
    pending_flushes: Vec<FlushOp>,
}

impl FrontendQueueWorker {
    /// Try to flush `list` to persistent storage
    ///
    /// This does not return errors, because on failure to flush we do not lose
    /// any state: flushing will be retried implicitly on the next deadline
    async fn flush(&mut self) {
        let key = RemotePath::new(&self.conf.remote_deletion_list_path(self.pending.sequence))
            .expect("Failed to compose deletion list path");

        let bytes = serde_json::to_vec(&self.pending).expect("Failed to serialize deletion list");
        let size = bytes.len();
        let source = tokio::io::BufReader::new(std::io::Cursor::new(bytes));

        match self.remote_storage.upload(source, size, &key, None).await {
            Ok(_) => {
                for f in self.pending_flushes.drain(..) {
                    f.fire();
                }

                let mut onward_list = DeletionList {
                    sequence: self.pending.sequence,
                    objects: Vec::new(),
                };
                std::mem::swap(&mut onward_list.objects, &mut self.pending.objects);
                self.pending.sequence += 1;

                if let Err(e) = self.tx.send(BackendQueueMessage::Delete(onward_list)).await {
                    // This is allowed to fail: it will only happen if the backend worker is shut down,
                    // so we can just drop this on the floor.
                    info!("Deletion list dropped, this is normal during shutdown ({e})");
                }
            }
            Err(e) => {
                warn!(
                    sequence = self.pending.sequence,
                    "Failed to flush deletion list, will retry later ({e})"
                )
            }
        }
    }

    /// This is the front-end ingest, where we bundle up deletion requests into DeletionList
    /// and write them out, for later
    pub async fn background(&mut self) {
        loop {
            let flush_delay = self.deadline.duration_since(Instant::now());

            // Wait for the next message, or to hit self.deadline
            let msg = tokio::select! {
                msg_opt = self.rx.recv() => {
                    match msg_opt {
                        None => {
                            break;
                        },
                        Some(msg)=> {msg}
                    }
                },
                _ = tokio::time::sleep(flush_delay) => {
                    self.deadline = Instant::now() + FLUSH_DEFAULT_DEADLINE;
                    if !self.pending.objects.is_empty() {
                        debug!("Flushing for deadline");
                        self.flush().await;
                    }
                    continue;
                }
            };

            match msg {
                FrontendQueueMessage::Delete(op) => {
                    let timeline_path = self.conf.timeline_path(&op.tenant_id, &op.timeline_id);

                    let _span = tracing::info_span!(
                        "execute_deletion",
                        tenant_id = %op.tenant_id,
                        timeline_id = %op.timeline_id,
                    );

                    for layer in op.layers {
                        // TODO go directly to remote path without composing local path
                        let local_path = timeline_path.join(layer.file_name());
                        let path = match self.conf.remote_path(&local_path) {
                            Ok(p) => p,
                            Err(e) => {
                                panic!("Can't make a timeline path! {e}");
                            }
                        };
                        self.pending.objects.push(path);
                    }
                }
                FrontendQueueMessage::Flush(op) => {
                    if self.pending.objects.is_empty() {
                        // Execute immediately
                        op.fire()
                    } else {
                        // Execute next time we flush
                        self.pending_flushes.push(op);

                        // Move up the deadline since we have been explicitly asked to flush
                        let flush_delay = self.deadline.duration_since(Instant::now());
                        if flush_delay > FLUSH_EXPLICIT_DEADLINE {
                            self.deadline = Instant::now() + FLUSH_EXPLICIT_DEADLINE;
                        }
                    }
                }
                FrontendQueueMessage::FlushExecute(op) => {
                    // We do not flush to a deletion list here: the client sends a Flush before the FlushExecute
                    if let Err(e) = self.tx.send(BackendQueueMessage::Flush(op)).await {
                        info!("Can't flush, shutting down ({e})");
                        // Caller will get error when their oneshot sender was dropped.
                    }
                }
            }

            if self.pending.objects.len() > DELETION_LIST_TARGET_SIZE {
                debug!(sequence = self.pending.sequence, "Flushing for deadline");
                self.flush().await;
            }
        }
        info!("Deletion queue shut down.");
    }
}
impl DeletionQueue {
    pub fn new_client(&self) -> DeletionQueueClient {
        DeletionQueueClient {
            tx: self.tx.clone(),
        }
    }

    /// Caller may use the returned object to construct clients with new_client.
    /// Caller should tokio::spawn the background() members of the two worker objects returned:
    /// we don't spawn those inside new() so that the caller can use their runtime/spans of choice.
    ///
    /// If remote_storage is None, then the returned workers will also be None.
    pub fn new(
        remote_storage: Option<GenericRemoteStorage>,
        conf: &'static PageServerConf,
    ) -> (
        Self,
        Option<FrontendQueueWorker>,
        Option<BackendQueueWorker>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(16384);

        let remote_storage = match remote_storage {
            None => return (Self { tx }, None, None),
            Some(r) => r,
        };

        let (backend_tx, backend_rx) = tokio::sync::mpsc::channel(16384);

        (
            Self { tx },
            Some(FrontendQueueWorker {
                // TODO: on startup, recover sequence number by listing persistent list objects,
                // *or* if we implement generation numbers, we may start from 0 every time
                pending: DeletionList::new(0xdeadbeef),
                remote_storage: remote_storage.clone(),
                conf,
                rx,
                tx: backend_tx,
                deadline: Instant::now() + FLUSH_DEFAULT_DEADLINE,
                pending_flushes: Vec::new(),
            }),
            Some(BackendQueueWorker {
                remote_storage,
                conf,
                rx: backend_rx,
                accumulator: Vec::new(),
                pending_lists: Vec::new(),
                executed_lists: Vec::new(),
            }),
        )
    }
}

#[cfg(test)]
mod test {
    use hex_literal::hex;
    use std::path::{Path, PathBuf};

    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use tokio::{runtime::EnterGuard, task::JoinHandle};

    use crate::tenant::harness::TenantHarness;

    use super::*;
    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));

    struct TestSetup {
        runtime: &'static tokio::runtime::Runtime,
        entered_runtime: EnterGuard<'static>,
        harness: TenantHarness,
        remote_fs_dir: PathBuf,
        deletion_queue: DeletionQueue,
        fe_worker: JoinHandle<()>,
        be_worker: JoinHandle<()>,
    }

    fn setup(test_name: &str) -> anyhow::Result<TestSetup> {
        let test_name = Box::leak(Box::new(format!("deletion_queue__{test_name}")));
        let harness = TenantHarness::create(test_name)?;

        // We do not load() the harness: we only need its config and remote_storage

        // Set up a GenericRemoteStorage targetting a directory
        let remote_fs_dir = harness.conf.workdir.join("remote_fs");
        std::fs::create_dir_all(remote_fs_dir)?;
        let remote_fs_dir = std::fs::canonicalize(harness.conf.workdir.join("remote_fs"))?;
        let storage_config = RemoteStorageConfig {
            max_concurrent_syncs: std::num::NonZeroUsize::new(
                remote_storage::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS,
            )
            .unwrap(),
            max_sync_errors: std::num::NonZeroU32::new(
                remote_storage::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS,
            )
            .unwrap(),
            storage: RemoteStorageKind::LocalFs(remote_fs_dir.clone()),
        };
        let storage = GenericRemoteStorage::from_config(&storage_config).unwrap();

        let runtime = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        ));
        let entered_runtime = runtime.enter();

        let (deletion_queue, fe_worker, be_worker) =
            DeletionQueue::new(Some(storage), harness.conf);

        let mut fe_worker = fe_worker.unwrap();
        let mut be_worker = be_worker.unwrap();
        let fe_worker_join = runtime.spawn(async move { fe_worker.background().await });
        let be_worker_join = runtime.spawn(async move { be_worker.background().await });

        Ok(TestSetup {
            runtime,
            entered_runtime,
            harness,
            remote_fs_dir,
            deletion_queue,
            fe_worker: fe_worker_join,
            be_worker: be_worker_join,
        })
    }

    // TODO: put this in a common location so that we can share with remote_timeline_client's tests
    fn assert_remote_files(expected: &[&str], remote_path: &Path) {
        let mut expected: Vec<String> = expected.iter().map(|x| String::from(*x)).collect();
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

    #[test]
    fn deletion_queue_smoke() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let ctx = setup("deletion_queue_smoke").expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();

        let layer_file_name_1: LayerFileName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let tenant_id = ctx.harness.tenant_id;

        let content: Vec<u8> = "victim1 contents".into();
        let relative_remote_path = ctx
            .harness
            .conf
            .remote_path(&ctx.harness.timeline_path(&TIMELINE_ID))
            .expect("Failed to construct remote path");
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());

        // Inject a victim file to remote storage
        info!("Writing");
        std::fs::create_dir_all(&remote_timeline_path)?;
        std::fs::write(
            remote_timeline_path.join(layer_file_name_1.to_string()),
            &content,
        )?;
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);

        // File should still be there after we push it to the queue (we haven't pushed enough to flush anything)
        info!("Pushing");
        ctx.runtime.block_on(client.push(
            tenant_id,
            TIMELINE_ID,
            [layer_file_name_1.clone()].to_vec(),
        ));
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);

        // File should still be there after we write a deletion list (we haven't pushed enough to execute anything)
        info!("Flushing");
        ctx.runtime.block_on(client.flush());
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);

        // File should go away when we execute
        info!("Flush-executing");
        ctx.runtime.block_on(client.flush_execute());
        assert_remote_files(&[], &remote_timeline_path);
        Ok(())
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    pub struct MockDeletionQueue {
        tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
        tx_pump: tokio::sync::mpsc::Sender<FlushOp>,
        executed: Arc<AtomicUsize>,
    }

    impl MockDeletionQueue {
        pub fn new(
            remote_storage: Option<GenericRemoteStorage>,
            conf: &'static PageServerConf,
        ) -> Self {
            let (tx, mut rx) = tokio::sync::mpsc::channel(16384);
            let (tx_pump, mut rx_pump) = tokio::sync::mpsc::channel::<FlushOp>(1);

            let executed = Arc::new(AtomicUsize::new(0));
            let executed_bg = executed.clone();

            tokio::spawn(async move {
                let _span = tracing::info_span!("mock_deletion_queue");
                let remote_storage = match &remote_storage {
                    Some(rs) => rs,
                    None => {
                        info!("No remote storage configured, deletion queue will not run");
                        return;
                    }
                };
                info!("Running mock deletion queue");
                // Each time we are asked to pump, drain the queue of deletions
                while let Some(flush_op) = rx_pump.recv().await {
                    info!("Executing all pending deletions");
                    while let Ok(msg) = rx.try_recv() {
                        match msg {
                            FrontendQueueMessage::Delete(op) => {
                                let timeline_path =
                                    conf.timeline_path(&op.tenant_id, &op.timeline_id);

                                let _span = tracing::info_span!(
                                    "execute_deletion",
                                    tenant_id = %op.tenant_id,
                                    timeline_id = %op.timeline_id,
                                );

                                for layer in op.layers {
                                    let local_path = timeline_path.join(layer.file_name());
                                    let path = match conf.remote_path(&local_path) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            panic!("Can't make a timeline path! {e}");
                                        }
                                    };
                                    info!("Executing deletion {path}");
                                    match remote_storage.delete(&path).await {
                                        Ok(_) => {
                                            debug!("Deleted {path}");
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to delete {path}, leaking object! ({e})"
                                            );
                                        }
                                    }
                                    executed_bg.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            FrontendQueueMessage::Flush(op) => {
                                op.fire();
                            }
                            FrontendQueueMessage::FlushExecute(op) => {
                                // We have already executed all prior deletions because mock does them inline
                                op.fire();
                            }
                        }
                        info!("All pending deletions have been executed");
                    }
                    flush_op
                        .tx
                        .send(())
                        .expect("Test called flush but dropped before finishing");
                }
            });

            Self {
                tx: tx,
                tx_pump,
                executed,
            }
        }

        pub fn get_executed(&self) -> usize {
            self.executed.load(Ordering::Relaxed)
        }

        pub async fn pump(&self) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx_pump
                .send(FlushOp { tx })
                .await
                .expect("pump called after deletion queue loop stopped");
            rx.await
                .expect("Mock delete queue shutdown while waiting to pump");
        }

        pub fn new_client(&self) -> DeletionQueueClient {
            DeletionQueueClient {
                tx: self.tx.clone(),
            }
        }
    }
}
