//! Common traits and structs for layers

pub mod batch_split_writer;
pub mod delta_layer;
pub mod filter_iterator;
pub mod image_layer;
pub mod inmemory_layer;
pub(crate) mod layer;
mod layer_desc;
mod layer_name;
pub mod merge_iterator;

use crate::config::PageServerConf;
use crate::context::{AccessStatsBehavior, RequestContext, RequestContextBuilder};
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use pageserver_api::key::Key;
use pageserver_api::keyspace::{KeySpace, KeySpaceRandomAccum};
use pageserver_api::record::NeonWalRecord;
use pageserver_api::value::Value;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info_span, trace, Instrument};
use utils::sync::gate::GateGuard;

use utils::lsn::Lsn;

pub use batch_split_writer::{BatchLayerWriter, SplitDeltaLayerWriter, SplitImageLayerWriter};
pub use delta_layer::{DeltaLayer, DeltaLayerWriter, ValueRef};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use layer_desc::{PersistentLayerDesc, PersistentLayerKey};
pub use layer_name::{DeltaLayerName, ImageLayerName, LayerName};

pub(crate) use layer::{EvictionError, Layer, ResidentLayer};

use self::inmemory_layer::InMemoryLayerFileId;

use super::timeline::GetVectoredError;
use super::PageReconstructError;

pub fn range_overlaps<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialOrd<T>,
{
    if a.start < b.start {
        a.end > b.start
    } else {
        b.end > a.start
    }
}

/// Struct used to communicate across calls to 'get_value_reconstruct_data'.
///
/// Before first call, you can fill in 'page_img' if you have an older cached
/// version of the page available. That can save work in
/// 'get_value_reconstruct_data', as it can stop searching for page versions
/// when all the WAL records going back to the cached image have been collected.
///
/// When get_value_reconstruct_data returns Complete, 'img' is set to an image
/// of the page, or the oldest WAL record in 'records' is a will_init-type
/// record that initializes the page without requiring a previous image.
///
/// If 'get_page_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same ValueReconstructState struct in the next 'get_value_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug, Default)]
pub(crate) struct ValueReconstructState {
    pub(crate) records: Vec<(Lsn, NeonWalRecord)>,
    pub(crate) img: Option<(Lsn, Bytes)>,
}

impl ValueReconstructState {
    pub(crate) fn depth(&self) -> usize {
        self.records.len() + self.img.as_ref().map_or(0, |_| 1)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ValueReconstructSituation {
    Complete,
    #[default]
    Continue,
}

/// On disk representation of a value loaded in a buffer
#[derive(Debug)]
pub(crate) enum OnDiskValue {
    /// Unencoded [`Value::Image`]
    RawImage(Bytes),
    /// Encoded [`Value`]. Can deserialize into an image or a WAL record
    WalRecordOrImage(Bytes),
}

/// Reconstruct data accumulated for a single key during a vectored get
#[derive(Debug, Default)]
pub(crate) struct VectoredValueReconstructState {
    pub(crate) on_disk_values: Vec<(Lsn, OnDiskValueIoWaiter)>,

    pub(crate) situation: ValueReconstructSituation,
}

#[derive(Debug)]
pub(crate) struct OnDiskValueIoWaiter {
    rx: tokio::sync::oneshot::Receiver<OnDiskValueIoResult>,
}

#[derive(Debug)]
#[must_use]
pub(crate) enum OnDiskValueIo {
    /// Traversal identified this IO as required to complete the vectored get.
    Required {
        num_active_ios: Arc<AtomicUsize>,
        tx: tokio::sync::oneshot::Sender<OnDiskValueIoResult>,
    },
    /// Sparse keyspace reads always read all the values for a given key,
    /// even though only the first value is needed.
    ///
    /// This variant represents the unnecessary IOs for those values at lower LSNs
    /// that aren't needed, but are currently still being done.
    ///
    /// The execution of unnecessary IOs was a pre-existing behavior before concurrent IO.
    /// We added this explicit representation here so that we can drop
    /// unnecessary IO results immediately, instead of buffering them in
    /// `oneshot` channels inside [`VectoredValueReconstructState`] until
    /// [`VectoredValueReconstructState::collect_pending_ios`] gets called.
    Unnecessary,
}

type OnDiskValueIoResult = Result<OnDiskValue, std::io::Error>;

impl OnDiskValueIo {
    pub(crate) fn complete(self, res: OnDiskValueIoResult) {
        match self {
            OnDiskValueIo::Required { num_active_ios, tx } => {
                num_active_ios.fetch_sub(1, std::sync::atomic::Ordering::Release);
                let _ = tx.send(res);
            }
            OnDiskValueIo::Unnecessary => {
                // Nobody cared, see variant doc comment.
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WaitCompletionError {
    #[error("OnDiskValueIo was dropped without completing, likely the sidecar task panicked")]
    IoDropped,
}

impl OnDiskValueIoWaiter {
    pub(crate) async fn wait_completion(self) -> Result<OnDiskValueIoResult, WaitCompletionError> {
        // NB: for Unnecessary IOs, this method never gets called because we don't add them to `on_disk_values`.
        self.rx.await.map_err(|_| WaitCompletionError::IoDropped)
    }
}

impl VectoredValueReconstructState {
    /// # Cancel-Safety
    ///
    /// Technically fine to stop polling this future, but, the IOs will still
    /// be executed to completion by the sidecar task and hold on to / consume resources.
    /// Better not do it to make reasonsing about the system easier.
    pub(crate) async fn collect_pending_ios(
        self,
    ) -> Result<ValueReconstructState, PageReconstructError> {
        use utils::bin_ser::BeSer;

        let mut res = Ok(ValueReconstructState::default());

        // We should try hard not to bail early, so that by the time we return from this
        // function, all IO for this value is done. It's not required -- we could totally
        // stop polling the IO futures in the sidecar task, they need to support that,
        // but just stopping to poll doesn't reduce the IO load on the disk. It's easier
        // to reason about the system if we just wait for all IO to complete, even if
        // we're no longer interested in the result.
        //
        // Revisit this when IO futures are replaced with a more sophisticated IO system
        // and an IO scheduler, where we know which IOs were submitted and which ones
        // just queued. Cf the comment on IoConcurrency::spawn_io.
        for (lsn, waiter) in self.on_disk_values {
            let value_recv_res = waiter
                .wait_completion()
                // we rely on the caller to poll us to completion, so this is not a bail point
                .await;
            // Force not bailing early by wrapping the code into a closure.
            #[allow(clippy::redundant_closure_call)]
            let _: () = (|| {
                match (&mut res, value_recv_res) {
                    (Err(_), _) => {
                        // We've already failed, no need to process more.
                    }
                    (Ok(_), Err(wait_err)) => {
                        // This shouldn't happen - likely the sidecar task panicked.
                        res = Err(PageReconstructError::Other(wait_err.into()));
                    }
                    (Ok(_), Ok(Err(err))) => {
                        let err: std::io::Error = err;
                        // TODO: returning IO error here will fail a compute query.
                        // Probably not what we want, we're not doing `maybe_fatal_err`
                        // in the IO futures.
                        // But it's been like that for a long time, not changing it
                        // as part of concurrent IO.
                        // => https://github.com/neondatabase/neon/issues/10454
                        res = Err(PageReconstructError::Other(err.into()));
                    }
                    (Ok(ok), Ok(Ok(OnDiskValue::RawImage(img)))) => {
                        assert!(ok.img.is_none());
                        ok.img = Some((lsn, img));
                    }
                    (Ok(ok), Ok(Ok(OnDiskValue::WalRecordOrImage(buf)))) => {
                        match Value::des(&buf) {
                            Ok(Value::WalRecord(rec)) => {
                                ok.records.push((lsn, rec));
                            }
                            Ok(Value::Image(img)) => {
                                assert!(ok.img.is_none());
                                ok.img = Some((lsn, img));
                            }
                            Err(err) => {
                                res = Err(PageReconstructError::Other(err.into()));
                            }
                        }
                    }
                }
            })();
        }

        res
    }
}

/// Bag of data accumulated during a vectored get..
pub(crate) struct ValuesReconstructState {
    /// The keys will be removed after `get_vectored` completes. The caller outside `Timeline`
    /// should not expect to get anything from this hashmap.
    pub(crate) keys: HashMap<Key, VectoredValueReconstructState>,
    /// The keys which are already retrieved
    keys_done: KeySpaceRandomAccum,

    /// The keys covered by the image layers
    keys_with_image_coverage: Option<Range<Key>>,

    // Statistics that are still accessible as a caller of `get_vectored_impl`.
    layers_visited: u32,
    delta_layers_visited: u32,

    pub(crate) io_concurrency: IoConcurrency,
    num_active_ios: Arc<AtomicUsize>,
}

/// The level of IO concurrency to be used on the read path
///
/// The desired end state is that we always do parallel IO.
/// This struct and the dispatching in the impl will be removed once
/// we've built enough confidence.
pub(crate) enum IoConcurrency {
    Sequential,
    SidecarTask {
        task_id: usize,
        ios_tx: tokio::sync::mpsc::UnboundedSender<IoFuture>,
    },
}

type IoFuture = Pin<Box<dyn Send + Future<Output = ()>>>;

pub(crate) enum SelectedIoConcurrency {
    Sequential,
    SidecarTask(GateGuard),
}

impl std::fmt::Debug for IoConcurrency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IoConcurrency::Sequential => write!(f, "Sequential"),
            IoConcurrency::SidecarTask { .. } => write!(f, "SidecarTask"),
        }
    }
}

impl std::fmt::Debug for SelectedIoConcurrency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectedIoConcurrency::Sequential => write!(f, "Sequential"),
            SelectedIoConcurrency::SidecarTask(_) => write!(f, "SidecarTask"),
        }
    }
}

impl IoConcurrency {
    /// Force sequential IO. This is a temporary workaround until we have
    /// moved plumbing-through-the-call-stack
    /// of IoConcurrency into `RequestContextq.
    ///
    /// DO NOT USE for new code.
    ///
    /// Tracking issue: <https://github.com/neondatabase/neon/issues/10460>.
    pub(crate) fn sequential() -> Self {
        Self::spawn(SelectedIoConcurrency::Sequential)
    }

    pub(crate) fn spawn_from_conf(
        conf: &'static PageServerConf,
        gate_guard: GateGuard,
    ) -> IoConcurrency {
        use pageserver_api::config::GetVectoredConcurrentIo;
        let selected = match conf.get_vectored_concurrent_io {
            GetVectoredConcurrentIo::Sequential => SelectedIoConcurrency::Sequential,
            GetVectoredConcurrentIo::SidecarTask => SelectedIoConcurrency::SidecarTask(gate_guard),
        };
        Self::spawn(selected)
    }

    pub(crate) fn spawn(io_concurrency: SelectedIoConcurrency) -> Self {
        match io_concurrency {
            SelectedIoConcurrency::Sequential => IoConcurrency::Sequential,
            SelectedIoConcurrency::SidecarTask(gate_guard) => {
                let (ios_tx, ios_rx) = tokio::sync::mpsc::unbounded_channel();
                static TASK_ID: AtomicUsize = AtomicUsize::new(0);
                let task_id = TASK_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // TODO: enrich the span with more context (tenant,shard,timeline) + (basebackup|pagestream|...)
                let span =
                    tracing::info_span!(parent: None, "IoConcurrency_sidecar", task_id = task_id);
                trace!(task_id, "spawning sidecar task");
                tokio::spawn(async move {
                    trace!("start");
                    scopeguard::defer!{ trace!("end") };
                    type IosRx = tokio::sync::mpsc::UnboundedReceiver<IoFuture>;
                    enum State {
                        Waiting {
                            // invariant: is_empty(), but we recycle the allocation
                            empty_futures: FuturesUnordered<IoFuture>,
                            ios_rx: IosRx,
                        },
                        Executing {
                            futures: FuturesUnordered<IoFuture>,
                            ios_rx: IosRx,
                        },
                        ShuttingDown {
                            futures: FuturesUnordered<IoFuture>,
                        },
                    }
                    let mut state = State::Waiting {
                        empty_futures: FuturesUnordered::new(),
                        ios_rx,
                    };
                    loop {
                        match state {
                            State::Waiting {
                                empty_futures,
                                mut ios_rx,
                            } => {
                                assert!(empty_futures.is_empty());
                                tokio::select! {
                                    fut = ios_rx.recv() => {
                                        if let Some(fut) = fut {
                                            trace!("received new io future");
                                            empty_futures.push(fut);
                                            state = State::Executing { futures: empty_futures, ios_rx };
                                        } else {
                                            state = State::ShuttingDown { futures: empty_futures }
                                        }
                                    }
                                }
                            }
                            State::Executing {
                                mut futures,
                                mut ios_rx,
                            } => {
                                tokio::select! {
                                    res = futures.next() => {
                                        trace!("io future completed");
                                        assert!(res.is_some());
                                        if futures.is_empty() {
                                            state = State::Waiting { empty_futures: futures, ios_rx};
                                        } else {
                                            state = State::Executing { futures, ios_rx };
                                        }
                                    }
                                    fut = ios_rx.recv() => {
                                        if let Some(fut) = fut {
                                            trace!("received new io future");
                                            futures.push(fut);
                                            state =  State::Executing { futures, ios_rx};
                                        } else {
                                            state = State::ShuttingDown { futures };
                                        }
                                    }
                                }
                            }
                            State::ShuttingDown {
                                mut futures,
                            } => {
                                trace!("shutting down");
                                while let Some(()) = futures.next().await {
                                    trace!("io future completed (shutdown)");
                                    // drain
                                }
                                trace!("shutdown complete");
                                break;
                            }
                        }
                    }
                    drop(gate_guard); // drop it right before we exit
                }.instrument(span));
                IoConcurrency::SidecarTask { task_id, ios_tx }
            }
        }
    }

    pub(crate) fn clone(&self) -> Self {
        match self {
            IoConcurrency::Sequential => IoConcurrency::Sequential,
            IoConcurrency::SidecarTask { task_id, ios_tx } => IoConcurrency::SidecarTask {
                task_id: *task_id,
                ios_tx: ios_tx.clone(),
            },
        }
    }

    /// Submit an IO to be executed in the background. DEADLOCK RISK, read the full doc string.
    ///
    /// The IO is represented as an opaque future.
    /// IO completion must be handled inside the future, e.g., through a oneshot channel.
    ///
    /// The API seems simple but there are multiple **pitfalls** involving
    /// DEADLOCK RISK.
    ///
    /// First, there are no guarantees about the exexecution of the IO.
    /// It may be `await`ed in-place before this function returns.
    /// It may be polled partially by this task and handed off to another task to be finished.
    /// It may be polled and then dropped before returning ready.
    ///
    /// This means that submitted IOs must not be interedependent.
    /// Interdependence may be through shared limited resources, e.g.,
    /// - VirtualFile file descriptor cache slot acquisition
    /// - tokio-epoll-uring slot
    ///
    /// # Why current usage is safe from deadlocks
    ///
    /// Textbook condition for a deadlock is that _all_ of the following be given
    /// - Mutual exclusion
    /// - Hold and wait
    /// - No preemption
    /// - Circular wait
    ///
    /// The current usage is safe because:
    /// - Mutual exclusion: IO futures definitely use mutexes, no way around that for now
    /// - Hold and wait: IO futures currently hold two kinds of locks/resources while waiting
    ///   for acquisition of other resources:
    ///    - VirtualFile file descriptor cache slot tokio mutex
    ///    - tokio-epoll-uring slot (uses tokio notify => wait queue, much like mutex)
    /// - No preemption: there's no taking-away of acquired locks/resources => given
    /// - Circular wait: this is the part of the condition that isn't met: all IO futures
    ///   first acquire VirtualFile mutex, then tokio-epoll-uring slot.
    ///   There is no IO future that acquires slot before VirtualFile.
    ///   Hence there can be no circular waiting.
    ///   Hence there cannot be a deadlock.
    ///
    /// This is a very fragile situation and must be revisited whenver any code called from
    /// inside the IO futures is changed.
    ///
    /// We will move away from opaque IO futures towards well-defined IOs at some point in
    /// the future when we have shipped this first version of concurrent IO to production
    /// and are ready to retire the Sequential mode which runs the futures in place.
    /// Right now, while brittle, the opaque IO approach allows us to ship the feature
    /// with minimal changes to the code and minimal changes to existing behavior in Sequential mode.
    ///
    /// Also read the comment in `collect_pending_ios`.
    pub(crate) async fn spawn_io<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        match self {
            IoConcurrency::Sequential => fut.await,
            IoConcurrency::SidecarTask { ios_tx, .. } => {
                let fut = Box::pin(fut);
                // NB: experiments showed that doing an opportunistic poll of `fut` here was bad for throughput
                // while insignificant for latency.
                // It would make sense to revisit the tokio-epoll-uring API in the future such that we can try
                // a submission here, but never poll the future. That way, io_uring can make proccess while
                // the future sits in the ios_tx queue.
                match ios_tx.send(fut) {
                    Ok(()) => {}
                    Err(_) => {
                        unreachable!("the io task must have exited, likely it panicked")
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn spawn_for_test() -> impl std::ops::DerefMut<Target = Self> {
        use std::ops::{Deref, DerefMut};
        use tracing::info;
        use utils::sync::gate::Gate;

        // Spawn needs a Gate, give it one.
        struct Wrapper {
            inner: IoConcurrency,
            #[allow(dead_code)]
            gate: Box<Gate>,
        }
        impl Deref for Wrapper {
            type Target = IoConcurrency;

            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }
        impl DerefMut for Wrapper {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }
        let gate = Box::new(Gate::default());

        // The default behavior when running Rust unit tests without any further
        // flags is to use the new behavior.
        // The CI uses the following environment variable to unit test both old
        // and new behavior.
        // NB: the Python regression & perf tests take the `else` branch
        // below and have their own defaults management.
        let selected = {
            // The pageserver_api::config type is unsuitable because it's internally tagged.
            #[derive(serde::Deserialize)]
            #[serde(rename_all = "kebab-case")]
            enum TestOverride {
                Sequential,
                SidecarTask,
            }
            use once_cell::sync::Lazy;
            static TEST_OVERRIDE: Lazy<TestOverride> = Lazy::new(|| {
                utils::env::var_serde_json_string(
                    "NEON_PAGESERVER_UNIT_TEST_GET_VECTORED_CONCURRENT_IO",
                )
                .unwrap_or(TestOverride::SidecarTask)
            });

            match *TEST_OVERRIDE {
                TestOverride::Sequential => SelectedIoConcurrency::Sequential,
                TestOverride::SidecarTask => {
                    SelectedIoConcurrency::SidecarTask(gate.enter().expect("just created it"))
                }
            }
        };

        info!(?selected, "get_vectored_concurrent_io test");

        Wrapper {
            inner: Self::spawn(selected),
            gate,
        }
    }
}

/// Make noise in case the [`ValuesReconstructState`] gets dropped while
/// there are still IOs in flight.
/// Refer to `collect_pending_ios` for why we prefer not to do that.
//
/// We log from here instead of from the sidecar task because the [`ValuesReconstructState`]
/// gets dropped in a tracing span with more context.
/// We repeat the sidecar tasks's `task_id` so we can correlate what we emit here with
/// the logs / panic handler logs from the sidecar task, which also logs the `task_id`.
impl Drop for ValuesReconstructState {
    fn drop(&mut self) {
        let num_active_ios = self
            .num_active_ios
            .load(std::sync::atomic::Ordering::Acquire);
        if num_active_ios == 0 {
            return;
        }
        let sidecar_task_id = match &self.io_concurrency {
            IoConcurrency::Sequential => None,
            IoConcurrency::SidecarTask { task_id, .. } => Some(*task_id),
        };
        tracing::warn!(
            num_active_ios,
            ?sidecar_task_id,
            backtrace=%std::backtrace::Backtrace::force_capture(),
            "dropping ValuesReconstructState while some IOs have not been completed",
        );
    }
}

impl ValuesReconstructState {
    pub(crate) fn new(io_concurrency: IoConcurrency) -> Self {
        Self {
            keys: HashMap::new(),
            keys_done: KeySpaceRandomAccum::new(),
            keys_with_image_coverage: None,
            layers_visited: 0,
            delta_layers_visited: 0,
            io_concurrency,
            num_active_ios: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Absolutely read [`IoConcurrency::spawn_io`] to learn about assumptions & pitfalls.
    pub(crate) async fn spawn_io<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.io_concurrency.spawn_io(fut).await;
    }

    pub(crate) fn on_layer_visited(&mut self, layer: &ReadableLayer) {
        self.layers_visited += 1;
        if let ReadableLayer::PersistentLayer(layer) = layer {
            if layer.layer_desc().is_delta() {
                self.delta_layers_visited += 1;
            }
        }
    }

    pub(crate) fn get_delta_layers_visited(&self) -> u32 {
        self.delta_layers_visited
    }

    pub(crate) fn get_layers_visited(&self) -> u32 {
        self.layers_visited
    }

    /// On hitting image layer, we can mark all keys in this range as done, because
    /// if the image layer does not contain a key, it is deleted/never added.
    pub(crate) fn on_image_layer_visited(&mut self, key_range: &Range<Key>) {
        let prev_val = self.keys_with_image_coverage.replace(key_range.clone());
        assert_eq!(
            prev_val, None,
            "should consume the keyspace before the next iteration"
        );
    }

    /// Update the state collected for a given key.
    /// Returns true if this was the last value needed for the key and false otherwise.
    ///
    /// If the key is done after the update, mark it as such.
    ///
    /// If the key is in the sparse keyspace (i.e., aux files), we do not track them in
    /// `key_done`.
    // TODO: rename this method & update description.
    pub(crate) fn update_key(&mut self, key: &Key, lsn: Lsn, completes: bool) -> OnDiskValueIo {
        let state = self.keys.entry(*key).or_default();

        let is_sparse_key = key.is_sparse();

        let required_io = match state.situation {
            ValueReconstructSituation::Complete => {
                if is_sparse_key {
                    // Sparse keyspace might be visited multiple times because
                    // we don't track unmapped keyspaces.
                    return OnDiskValueIo::Unnecessary;
                } else {
                    unreachable!()
                }
            }
            ValueReconstructSituation::Continue => {
                self.num_active_ios
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
                let (tx, rx) = tokio::sync::oneshot::channel();
                state.on_disk_values.push((lsn, OnDiskValueIoWaiter { rx }));
                OnDiskValueIo::Required {
                    tx,
                    num_active_ios: Arc::clone(&self.num_active_ios),
                }
            }
        };

        if completes && state.situation == ValueReconstructSituation::Continue {
            state.situation = ValueReconstructSituation::Complete;
            if !is_sparse_key {
                self.keys_done.add_key(*key);
            }
        }

        required_io
    }

    /// Returns the key space describing the keys that have
    /// been marked as completed since the last call to this function.
    /// Returns individual keys done, and the image layer coverage.
    pub(crate) fn consume_done_keys(&mut self) -> (KeySpace, Option<Range<Key>>) {
        (
            self.keys_done.consume_keyspace(),
            self.keys_with_image_coverage.take(),
        )
    }
}

/// A key that uniquely identifies a layer in a timeline
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum LayerId {
    PersitentLayerId(PersistentLayerKey),
    InMemoryLayerId(InMemoryLayerFileId),
}

/// Uniquely identify a layer visit by the layer
/// and LSN floor (or start LSN) of the reads.
/// The layer itself is not enough since we may
/// have different LSN lower bounds for delta layer reads.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
struct LayerToVisitId {
    layer_id: LayerId,
    lsn_floor: Lsn,
}

/// Layer wrapper for the read path. Note that it is valid
/// to use these layers even after external operations have
/// been performed on them (compaction, freeze, etc.).
#[derive(Debug)]
pub(crate) enum ReadableLayer {
    PersistentLayer(Layer),
    InMemoryLayer(Arc<InMemoryLayer>),
}

/// A partial description of a read to be done.
#[derive(Debug, Clone)]
struct LayerVisit {
    /// An id used to resolve the readable layer within the fringe
    layer_to_visit_id: LayerToVisitId,
    /// Lsn range for the read, used for selecting the next read
    lsn_range: Range<Lsn>,
}

/// Data structure which maintains a fringe of layers for the
/// read path. The fringe is the set of layers which intersects
/// the current keyspace that the search is descending on.
/// Each layer tracks the keyspace that intersects it.
///
/// The fringe must appear sorted by Lsn. Hence, it uses
/// a two layer indexing scheme.
#[derive(Debug)]
pub(crate) struct LayerFringe {
    planned_visits_by_lsn: BinaryHeap<LayerVisit>,
    visit_reads: HashMap<LayerToVisitId, LayerVisitReads>,
}

#[derive(Debug)]
struct LayerVisitReads {
    layer: ReadableLayer,
    target_keyspace: KeySpaceRandomAccum,
}

impl LayerFringe {
    pub(crate) fn new() -> Self {
        LayerFringe {
            planned_visits_by_lsn: BinaryHeap::new(),
            visit_reads: HashMap::new(),
        }
    }

    pub(crate) fn next_layer(&mut self) -> Option<(ReadableLayer, KeySpace, Range<Lsn>)> {
        let read_desc = self.planned_visits_by_lsn.pop()?;

        let removed = self.visit_reads.remove_entry(&read_desc.layer_to_visit_id);

        match removed {
            Some((
                _,
                LayerVisitReads {
                    layer,
                    mut target_keyspace,
                },
            )) => Some((
                layer,
                target_keyspace.consume_keyspace(),
                read_desc.lsn_range,
            )),
            None => unreachable!("fringe internals are always consistent"),
        }
    }

    pub(crate) fn update(
        &mut self,
        layer: ReadableLayer,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
    ) {
        let layer_to_visit_id = LayerToVisitId {
            layer_id: layer.id(),
            lsn_floor: lsn_range.start,
        };

        let entry = self.visit_reads.entry(layer_to_visit_id.clone());
        match entry {
            Entry::Occupied(mut entry) => {
                entry.get_mut().target_keyspace.add_keyspace(keyspace);
            }
            Entry::Vacant(entry) => {
                self.planned_visits_by_lsn.push(LayerVisit {
                    lsn_range,
                    layer_to_visit_id: layer_to_visit_id.clone(),
                });
                let mut accum = KeySpaceRandomAccum::new();
                accum.add_keyspace(keyspace);
                entry.insert(LayerVisitReads {
                    layer,
                    target_keyspace: accum,
                });
            }
        }
    }
}

impl Default for LayerFringe {
    fn default() -> Self {
        Self::new()
    }
}

impl Ord for LayerVisit {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.lsn_range.end.cmp(&other.lsn_range.end);
        if ord == std::cmp::Ordering::Equal {
            self.lsn_range.start.cmp(&other.lsn_range.start).reverse()
        } else {
            ord
        }
    }
}

impl PartialOrd for LayerVisit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LayerVisit {
    fn eq(&self, other: &Self) -> bool {
        self.lsn_range == other.lsn_range
    }
}

impl Eq for LayerVisit {}

impl ReadableLayer {
    pub(crate) fn id(&self) -> LayerId {
        match self {
            Self::PersistentLayer(layer) => LayerId::PersitentLayerId(layer.layer_desc().key()),
            Self::InMemoryLayer(layer) => LayerId::InMemoryLayerId(layer.file_id()),
        }
    }

    pub(crate) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        match self {
            ReadableLayer::PersistentLayer(layer) => {
                let persistent_context = RequestContextBuilder::from(ctx)
                    .perf_span(|crnt_perf_span| {
                        info_span!(
                            target: "get_page",
                            parent: crnt_perf_span,
                            "PLAN_LAYER",
                            layer = %layer
                        )
                    })
                    .attached_child();

                persistent_context
                    .maybe_instrument(
                        layer.get_values_reconstruct_data(
                            keyspace,
                            lsn_range,
                            reconstruct_state,
                            &persistent_context,
                        ),
                        |crnt_perf_span| crnt_perf_span.clone(),
                    )
                    .await
            }
            ReadableLayer::InMemoryLayer(layer) => {
                let in_mem_context = RequestContextBuilder::from(ctx)
                    .perf_span(|crnt_perf_span| {
                        info_span!(
                            target: "get_page",
                            parent: crnt_perf_span,
                            "PLAN_LAYER",
                            layer = %layer
                        )
                    })
                    .attached_child();

                in_mem_context
                    .maybe_instrument(
                        layer.get_values_reconstruct_data(
                            keyspace,
                            lsn_range.end,
                            reconstruct_state,
                            &in_mem_context,
                        ),
                        |crnt_perf_span| crnt_perf_span.clone(),
                    )
                    .await
            }
        }
    }
}

/// Layers contain a hint indicating whether they are likely to be used for reads.
///
/// This is a hint rather than an authoritative value, so that we do not have to update it synchronously
/// when changing the visibility of layers (for example when creating a branch that makes some previously
/// covered layers visible).  It should be used for cache management but not for correctness-critical checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerVisibilityHint {
    /// A Visible layer might be read while serving a read, because there is not an image layer between it
    /// and a readable LSN (the tip of the branch or a child's branch point)
    Visible,
    /// A Covered layer probably won't be read right now, but _can_ be read in future if someone creates
    /// a branch or ephemeral endpoint at an LSN below the layer that covers this.
    Covered,
}

pub(crate) struct LayerAccessStats(std::sync::atomic::AtomicU64);

#[derive(Clone, Copy, strum_macros::EnumString)]
pub(crate) enum LayerAccessStatsReset {
    NoReset,
    AllStats,
}

impl Default for LayerAccessStats {
    fn default() -> Self {
        // Default value is to assume resident since creation time, and visible.
        let (_mask, mut value) = Self::to_low_res_timestamp(Self::RTIME_SHIFT, SystemTime::now());
        value |= 0x1 << Self::VISIBILITY_SHIFT;

        Self(std::sync::atomic::AtomicU64::new(value))
    }
}

// Efficient store of two very-low-resolution timestamps and some bits.  Used for storing last access time and
// last residence change time.
impl LayerAccessStats {
    // How many high bits to drop from a u32 timestamp?
    // - Only storing up to a u32 timestamp will work fine until 2038 (if this code is still in use
    //   after that, this software has been very successful!)
    // - Dropping the top bit is implicitly safe because unix timestamps are meant to be
    // stored in an i32, so they never used it.
    // - Dropping the next two bits is safe because this code is only running on systems in
    // years >= 2024, and these bits have been 1 since 2021
    //
    // Therefore we may store only 28 bits for a timestamp with one second resolution.  We do
    // this truncation to make space for some flags in the high bits of our u64.
    const TS_DROP_HIGH_BITS: u32 = u32::count_ones(Self::TS_ONES) + 1;
    const TS_MASK: u32 = 0x1f_ff_ff_ff;
    const TS_ONES: u32 = 0x60_00_00_00;

    const ATIME_SHIFT: u32 = 0;
    const RTIME_SHIFT: u32 = 32 - Self::TS_DROP_HIGH_BITS;
    const VISIBILITY_SHIFT: u32 = 64 - 2 * Self::TS_DROP_HIGH_BITS;

    fn write_bits(&self, mask: u64, value: u64) -> u64 {
        self.0
            .fetch_update(
                // TODO: decide what orderings are correct
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |v| Some((v & !mask) | (value & mask)),
            )
            .expect("Inner function is infallible")
    }

    fn to_low_res_timestamp(shift: u32, time: SystemTime) -> (u64, u64) {
        // Drop the low three bits of the timestamp, for an ~8s accuracy
        let timestamp = time.duration_since(UNIX_EPOCH).unwrap().as_secs() & (Self::TS_MASK as u64);

        ((Self::TS_MASK as u64) << shift, timestamp << shift)
    }

    fn read_low_res_timestamp(&self, shift: u32) -> Option<SystemTime> {
        let read = self.0.load(std::sync::atomic::Ordering::Relaxed);

        let ts_bits = (read & ((Self::TS_MASK as u64) << shift)) >> shift;
        if ts_bits == 0 {
            None
        } else {
            Some(UNIX_EPOCH + Duration::from_secs(ts_bits | (Self::TS_ONES as u64)))
        }
    }

    /// Record a change in layer residency.
    ///
    /// Recording the event must happen while holding the layer map lock to
    /// ensure that latest-activity-threshold-based layer eviction (eviction_task.rs)
    /// can do an "imitate access" to this layer, before it observes `now-latest_activity() > threshold`.
    ///
    /// If we instead recorded the residence event with a timestamp from before grabbing the layer map lock,
    /// the following race could happen:
    ///
    /// - Compact: Write out an L1 layer from several L0 layers. This records residence event LayerCreate with the current timestamp.
    /// - Eviction: imitate access logical size calculation. This accesses the L0 layers because the L1 layer is not yet in the layer map.
    /// - Compact: Grab layer map lock, add the new L1 to layer map and remove the L0s, release layer map lock.
    /// - Eviction: observes the new L1 layer whose only activity timestamp is the LayerCreate event.
    pub(crate) fn record_residence_event_at(&self, now: SystemTime) {
        let (mask, value) = Self::to_low_res_timestamp(Self::RTIME_SHIFT, now);
        self.write_bits(mask, value);
    }

    pub(crate) fn record_residence_event(&self) {
        self.record_residence_event_at(SystemTime::now())
    }

    fn record_access_at(&self, now: SystemTime) -> bool {
        let (mut mask, mut value) = Self::to_low_res_timestamp(Self::ATIME_SHIFT, now);

        // A layer which is accessed must be visible.
        mask |= 0x1 << Self::VISIBILITY_SHIFT;
        value |= 0x1 << Self::VISIBILITY_SHIFT;

        let old_bits = self.write_bits(mask, value);
        !matches!(
            self.decode_visibility(old_bits),
            LayerVisibilityHint::Visible
        )
    }

    /// Returns true if we modified the layer's visibility to set it to Visible implicitly
    /// as a result of this access
    pub(crate) fn record_access(&self, ctx: &RequestContext) -> bool {
        if ctx.access_stats_behavior() == AccessStatsBehavior::Skip {
            return false;
        }

        self.record_access_at(SystemTime::now())
    }

    fn as_api_model(
        &self,
        reset: LayerAccessStatsReset,
    ) -> pageserver_api::models::LayerAccessStats {
        let ret = pageserver_api::models::LayerAccessStats {
            access_time: self
                .read_low_res_timestamp(Self::ATIME_SHIFT)
                .unwrap_or(UNIX_EPOCH),
            residence_time: self
                .read_low_res_timestamp(Self::RTIME_SHIFT)
                .unwrap_or(UNIX_EPOCH),
            visible: matches!(self.visibility(), LayerVisibilityHint::Visible),
        };
        match reset {
            LayerAccessStatsReset::NoReset => {}
            LayerAccessStatsReset::AllStats => {
                self.write_bits((Self::TS_MASK as u64) << Self::ATIME_SHIFT, 0x0);
                self.write_bits((Self::TS_MASK as u64) << Self::RTIME_SHIFT, 0x0);
            }
        }
        ret
    }

    /// Get the latest access timestamp, falling back to latest residence event.  The latest residence event
    /// will be this Layer's construction time, if its residence hasn't changed since then.
    pub(crate) fn latest_activity(&self) -> SystemTime {
        if let Some(t) = self.read_low_res_timestamp(Self::ATIME_SHIFT) {
            t
        } else {
            self.read_low_res_timestamp(Self::RTIME_SHIFT)
                .expect("Residence time is set on construction")
        }
    }

    /// Whether this layer has been accessed (excluding in [`AccessStatsBehavior::Skip`]).
    ///
    /// This indicates whether the layer has been used for some purpose that would motivate
    /// us to keep it on disk, such as for serving a getpage request.
    fn accessed(&self) -> bool {
        // Consider it accessed if the most recent access is more recent than
        // the most recent change in residence status.
        match (
            self.read_low_res_timestamp(Self::ATIME_SHIFT),
            self.read_low_res_timestamp(Self::RTIME_SHIFT),
        ) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(a), Some(r)) => a >= r,
        }
    }

    /// Helper for extracting the visibility hint from the literal value of our inner u64
    fn decode_visibility(&self, bits: u64) -> LayerVisibilityHint {
        match (bits >> Self::VISIBILITY_SHIFT) & 0x1 {
            1 => LayerVisibilityHint::Visible,
            0 => LayerVisibilityHint::Covered,
            _ => unreachable!(),
        }
    }

    /// Returns the old value which has been replaced
    pub(crate) fn set_visibility(&self, visibility: LayerVisibilityHint) -> LayerVisibilityHint {
        let value = match visibility {
            LayerVisibilityHint::Visible => 0x1 << Self::VISIBILITY_SHIFT,
            LayerVisibilityHint::Covered => 0x0,
        };

        let old_bits = self.write_bits(0x1 << Self::VISIBILITY_SHIFT, value);
        self.decode_visibility(old_bits)
    }

    pub(crate) fn visibility(&self) -> LayerVisibilityHint {
        let read = self.0.load(std::sync::atomic::Ordering::Relaxed);
        self.decode_visibility(read)
    }
}

/// Get a layer descriptor from a layer.
pub(crate) trait AsLayerDesc {
    /// Get the layer descriptor.
    fn layer_desc(&self) -> &PersistentLayerDesc;
}

pub mod tests {
    use pageserver_api::shard::TenantShardId;
    use utils::id::TimelineId;

    use super::*;

    impl From<DeltaLayerName> for PersistentLayerDesc {
        fn from(value: DeltaLayerName) -> Self {
            PersistentLayerDesc::new_delta(
                TenantShardId::from([0; 18]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn_range,
                233,
            )
        }
    }

    impl From<ImageLayerName> for PersistentLayerDesc {
        fn from(value: ImageLayerName) -> Self {
            PersistentLayerDesc::new_img(
                TenantShardId::from([0; 18]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn,
                233,
            )
        }
    }

    impl From<LayerName> for PersistentLayerDesc {
        fn from(value: LayerName) -> Self {
            match value {
                LayerName::Delta(d) => Self::from(d),
                LayerName::Image(i) => Self::from(i),
            }
        }
    }
}

/// Range wrapping newtype, which uses display to render Debug.
///
/// Useful with `Key`, which has too verbose `{:?}` for printing multiple layers.
struct RangeDisplayDebug<'a, T: std::fmt::Display>(&'a Range<T>);

impl<T: std::fmt::Display> std::fmt::Debug for RangeDisplayDebug<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.0.start, self.0.end)
    }
}

#[cfg(test)]
mod tests2 {
    use pageserver_api::key::DBDIR_KEY;
    use tracing::info;

    use super::*;
    use crate::tenant::storage_layer::IoConcurrency;

    /// TODO: currently this test relies on manual visual inspection of the --no-capture output.
    /// Should look like so:
    /// ```text
    /// RUST_LOG=trace cargo nextest run  --features testing  --no-capture test_io_concurrency_noise
    /// running 1 test
    /// 2025-01-21T17:42:01.335679Z  INFO get_vectored_concurrent_io test selected=SidecarTask
    /// 2025-01-21T17:42:01.335680Z TRACE spawning sidecar task task_id=0
    /// 2025-01-21T17:42:01.335937Z TRACE IoConcurrency_sidecar{task_id=0}: start
    /// 2025-01-21T17:42:01.335972Z TRACE IoConcurrency_sidecar{task_id=0}: received new io future
    /// 2025-01-21T17:42:01.335999Z  INFO IoConcurrency_sidecar{task_id=0}: waiting for signal to complete IO
    /// 2025-01-21T17:42:01.336229Z  WARN dropping ValuesReconstructState while some IOs have not been completed num_active_ios=1 sidecar_task_id=Some(0) backtrace=   0: <pageserver::tenant::storage_layer::ValuesReconstructState as core::ops::drop::Drop>::drop
    ///              at ./src/tenant/storage_layer.rs:553:24
    ///    1: core::ptr::drop_in_place<pageserver::tenant::storage_layer::ValuesReconstructState>
    ///              at /home/christian/.rustup/toolchains/1.84.0-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/library/core/src/ptr/mod.rs:521:1
    ///    2: core::mem::drop
    ///              at /home/christian/.rustup/toolchains/1.84.0-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/library/core/src/mem/mod.rs:942:24
    ///    3: pageserver::tenant::storage_layer::tests2::test_io_concurrency_noise::{{closure}}
    ///              at ./src/tenant/storage_layer.rs:1159:9
    ///   ...
    ///   49: <unknown>
    /// 2025-01-21T17:42:01.452293Z  INFO IoConcurrency_sidecar{task_id=0}: completing IO
    /// 2025-01-21T17:42:01.452357Z TRACE IoConcurrency_sidecar{task_id=0}: io future completed
    /// 2025-01-21T17:42:01.452473Z TRACE IoConcurrency_sidecar{task_id=0}: end
    /// test tenant::storage_layer::tests2::test_io_concurrency_noise ... ok
    ///
    /// ```
    #[tokio::test]
    async fn test_io_concurrency_noise() {
        crate::tenant::harness::setup_logging();

        let io_concurrency = IoConcurrency::spawn_for_test();
        match *io_concurrency {
            IoConcurrency::Sequential => {
                // This test asserts behavior in sidecar mode, doesn't make sense in sequential mode.
                return;
            }
            IoConcurrency::SidecarTask { .. } => {}
        }
        let mut reconstruct_state = ValuesReconstructState::new(io_concurrency.clone());

        let (io_fut_is_waiting_tx, io_fut_is_waiting) = tokio::sync::oneshot::channel();
        let (do_complete_io, should_complete_io) = tokio::sync::oneshot::channel();
        let (io_fut_exiting_tx, io_fut_exiting) = tokio::sync::oneshot::channel();

        let io = reconstruct_state.update_key(&DBDIR_KEY, Lsn(8), true);
        reconstruct_state
            .spawn_io(async move {
                info!("waiting for signal to complete IO");
                io_fut_is_waiting_tx.send(()).unwrap();
                should_complete_io.await.unwrap();
                info!("completing IO");
                io.complete(Ok(OnDiskValue::RawImage(Bytes::new())));
                io_fut_exiting_tx.send(()).unwrap();
            })
            .await;

        io_fut_is_waiting.await.unwrap();

        // this is what makes the noise
        drop(reconstruct_state);

        do_complete_io.send(()).unwrap();

        io_fut_exiting.await.unwrap();
    }
}
