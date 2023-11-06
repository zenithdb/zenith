//!
//! This module provides centralized handling of tokio tasks in the Page Server.
//!
//! We provide a few basic facilities:
//! - A global registry of tasks that lists what kind of tasks they are, and
//!   which tenant or timeline they are working on
//!
//! - The ability to request a task to shut down.
//!
//!
//! # How it works?
//!
//! There is a global hashmap of all the tasks (`TASKS`). Whenever a new
//! task is spawned, a PageServerTask entry is added there, and when a
//! task dies, it removes itself from the hashmap. If you want to kill a
//! task, you can scan the hashmap to find it.
//!
//! # Task shutdown
//!
//! To kill a task, we rely on co-operation from the victim. Each task is
//! expected to periodically call the `is_shutdown_requested()` function, and
//! if it returns true, exit gracefully. In addition to that, when waiting for
//! the network or other long-running operation, you can use
//! `shutdown_watcher()` function to get a Future that will become ready if
//! the current task has been requested to shut down. You can use that with
//! Tokio select!().
//!
//! TODO: This would be a good place to also handle panics in a somewhat sane way.
//! Depending on what task panics, we might want to kill the whole server, or
//! only a single tenant or timeline.
//!

// Clippy 1.60 incorrectly complains about the tokio::task_local!() macro.
// Silence it. See https://github.com/rust-lang/rust-clippy/issues/9224.
#![allow(clippy::declare_interior_mutable_const)]

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use metrics::tokio_metrics::TokioCollector;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::task_local;
use tokio_util::sync::CancellationToken;

use tracing::{debug, error, info, warn};

use once_cell::sync::Lazy;

use utils::id::{TenantId, TimelineId};

use crate::shutdown_pageserver;

//
// There are four runtimes:
//
// Compute request runtime
//  - used to handle connections from compute nodes. Any tasks related to satisfying
//    GetPage requests, base backups, import, and other such compute node operations
//    are handled by the Compute request runtime
//  - page_service.rs
//  - this includes layer downloads from remote storage, if a layer is needed to
//    satisfy a GetPage request
//
// Management request runtime
//  - used to handle HTTP API requests
//
// WAL receiver runtime:
//  - used to handle WAL receiver connections.
//  - and to receiver updates from storage_broker
//
// Background runtime
//  - layer flushing
//  - garbage collection
//  - compaction
//  - remote storage uploads
//  - initial tenant loading
//
// Everything runs in a tokio task. If you spawn new tasks, spawn it using the correct
// runtime.
//
// There might be situations when one task needs to wait for a task running in another
// Runtime to finish. For example, if a background operation needs a layer from remote
// storage, it will start to download it. If a background operation needs a remote layer,
// and the download was already initiated by a GetPage request, the background task
// will wait for the download - running in the Page server runtime - to finish.
// Another example: the initial tenant loading tasks are launched in the background ops
// runtime. If a GetPage request comes in before the load of a tenant has finished, the
// GetPage request will wait for the tenant load to finish.
//
// The core Timeline code is synchronous, and uses a bunch of std Mutexes and RWLocks to
// protect data structures. Let's keep it that way. Synchronous code is easier to debug
// and analyze, and there's a lot of hairy, low-level, performance critical code there.
//
// It's nice to have different runtimes, so that you can quickly eyeball how much CPU
// time each class of operations is taking, with 'top -H' or similar.
//
// It's also good to avoid hogging all threads that would be needed to process
// other operations, if the upload tasks e.g. get blocked on locks. It shouldn't
// happen, but still.
//
pub static COMPUTE_REQUEST_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("compute request worker")
        .enable_all()
        .build()
        .expect("Failed to create compute request runtime")
});

pub static MGMT_REQUEST_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("mgmt request worker")
        .enable_all()
        .build()
        .expect("Failed to create mgmt request runtime")
});

pub static WALRECEIVER_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("walreceiver worker")
        .enable_all()
        .build()
        .expect("Failed to create walreceiver runtime")
});

pub static BACKGROUND_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("background op worker")
        // if you change the number of worker threads please change the constant below
        .enable_all()
        .build()
        .expect("Failed to create background op runtime")
});

pub(crate) static BACKGROUND_RUNTIME_WORKER_THREADS: Lazy<usize> = Lazy::new(|| {
    // force init and thus panics
    let _ = BACKGROUND_RUNTIME.handle();
    // replicates tokio-1.28.1::loom::sys::num_cpus which is not available publicly
    // tokio would had already panicked for parsing errors or NotUnicode
    //
    // this will be wrong if any of the runtimes gets their worker threads configured to something
    // else, but that has not been needed in a long time.
    std::env::var("TOKIO_WORKER_THREADS")
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or_else(|_e| usize::max(1, num_cpus::get()))
});

pub fn runtime_collector() -> TokioCollector {
    TokioCollector::default()
        .add_runtime(
            COMPUTE_REQUEST_RUNTIME.handle().clone(),
            "compute".to_owned(),
        )
        .add_runtime(MGMT_REQUEST_RUNTIME.handle().clone(), "mgmt".to_owned())
        .add_runtime(
            WALRECEIVER_RUNTIME.handle().clone(),
            "walreceiver".to_owned(),
        )
        .add_runtime(BACKGROUND_RUNTIME.handle().clone(), "background".to_owned())
}

#[derive(Debug, Clone, Copy)]
pub struct PageserverTaskId(u64);

impl fmt::Display for PageserverTaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Each task that we track is associated with a "task ID". It's just an
/// increasing number that we assign. Note that it is different from tokio::task::Id.
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

/// Global registry of tasks
static TASKS: Lazy<Mutex<HashMap<u64, Arc<PageServerTask>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

task_local! {
    // This is a cancellation token which will be cancelled when a task needs to shut down. The
    // root token is kept in the global registry, so that anyone can send the signal to request
    // task shutdown.
    static SHUTDOWN_TOKEN: CancellationToken;

    // Each task holds reference to its own PageServerTask here.
    static CURRENT_TASK: Arc<PageServerTask>;
}

///
/// There are many kinds of tasks in the system. Some are associated with a particular
/// tenant or timeline, while others are global.
///
/// Note that we don't try to limit how many task of a certain kind can be running
/// at the same time.
///
#[derive(
    Debug,
    // NB: enumset::EnumSetType derives PartialEq, Eq, Clone, Copy
    enumset::EnumSetType,
    enum_map::Enum,
    serde::Serialize,
    serde::Deserialize,
    strum_macros::IntoStaticStr,
)]
pub enum TaskKind {
    // Pageserver startup, i.e., `main`
    Startup,

    // libpq listener task. It just accepts connection and spawns a
    // PageRequestHandler task for each connection.
    LibpqEndpointListener,

    // HTTP endpoint listener.
    HttpEndpointListener,

    // Task that handles a single connection. A PageRequestHandler task
    // starts detached from any particular tenant or timeline, but it can be
    // associated with one later, after receiving a command from the client.
    PageRequestHandler,

    /// Manages the WAL receiver connection for one timeline.
    /// It subscribes to events from storage_broker and decides which safekeeper to connect to.
    /// Once the decision has been made, it establishes the connection using the `tokio-postgres` library.
    /// There is at most one connection at any given time.
    ///
    /// That `tokio-postgres` library represents a connection as two objects: a `Client` and a `Connection`.
    /// The `Client` object is what library users use to make requests & get responses.
    /// Internally, `Client` hands over requests to the `Connection` object.
    /// The `Connection` object is responsible for speaking the wire protocol.
    ///
    /// Walreceiver uses its own abstraction called `TaskHandle` to represent the activity of establishing and handling a connection.
    /// That abstraction doesn't use `task_mgr`.
    /// The `WalReceiverManager` task ensures that this `TaskHandle` task does not outlive the `WalReceiverManager` task.
    /// For the `RequestContext` that we hand to the TaskHandle, we use the [`WalReceiverConnectionHandler`] task kind.
    ///
    /// Once the connection is established, the `TaskHandle` task creates a
    /// [`WalReceiverConnectionPoller`] task_mgr task that is responsible for polling
    /// the `Connection` object.
    /// A `CancellationToken` created by the `TaskHandle` task ensures
    /// that the [`WalReceiverConnectionPoller`] task will cancel soon after as the `TaskHandle` is dropped.
    ///
    /// [`WalReceiverConnectionHandler`]: Self::WalReceiverConnectionHandler
    /// [`WalReceiverConnectionPoller`]: Self::WalReceiverConnectionPoller
    WalReceiverManager,

    /// The `TaskHandle` task that executes `handle_walreceiver_connection`.
    /// Not a `task_mgr` task, but we use this `TaskKind` for its `RequestContext`.
    /// See the comment on [`WalReceiverManager`].
    ///
    /// [`WalReceiverManager`]: Self::WalReceiverManager
    WalReceiverConnectionHandler,

    /// The task that polls the `tokio-postgres::Connection` object.
    /// Spawned by task [`WalReceiverConnectionHandler`](Self::WalReceiverConnectionHandler).
    /// See the comment on [`WalReceiverManager`](Self::WalReceiverManager).
    WalReceiverConnectionPoller,

    // Garbage collection worker. One per tenant
    GarbageCollector,

    // Compaction. One per tenant.
    Compaction,

    // Eviction. One per timeline.
    Eviction,

    /// See [`crate::disk_usage_eviction_task`].
    DiskUsageEviction,

    // Initial logical size calculation
    InitialLogicalSizeCalculation,

    OndemandLogicalSizeCalculation,

    // Task that flushes frozen in-memory layers to disk
    LayerFlushTask,

    // Task that uploads a file to remote storage
    RemoteUploadTask,

    // Task that downloads a file from remote storage
    RemoteDownloadTask,

    // task that handles the initial downloading of all tenants
    InitialLoad,

    // task that handles attaching a tenant
    Attach,

    // Used mostly for background deletion from s3
    TimelineDeletionWorker,

    // task that handhes metrics collection
    MetricsCollection,

    // task that drives downloading layers
    DownloadAllRemoteLayers,
    // Task that calculates synthetis size for all active tenants
    CalculateSyntheticSize,

    // A request that comes in via the pageserver HTTP API.
    MgmtRequest,

    DebugTool,

    #[cfg(test)]
    UnitTest,
}

#[derive(Default)]
struct MutableTaskState {
    /// Handle for waiting for the task to exit. It can be None, if the
    /// the task has already exited.
    join_handle: Option<JoinHandle<()>>,
}

struct PageServerTask {
    #[allow(dead_code)] // unused currently
    task_id: PageserverTaskId,

    kind: TaskKind,

    name: String,

    // To request task shutdown, just cancel this token.
    cancel: CancellationToken,

    /// Tasks may optionally be launched for a particular tenant/timeline, enabling
    /// later cancelling tasks for that tenant/timeline in [`shutdown_tasks`]
    tenant_id: Option<TenantId>,
    timeline_id: Option<TimelineId>,

    mutable: Mutex<MutableTaskState>,
}

/// Launch a new task
/// Note: if shutdown_process_on_error is set to true failure
///   of the task will lead to shutdown of entire process
pub fn spawn<F>(
    runtime: &tokio::runtime::Handle,
    kind: TaskKind,
    tenant_id: Option<TenantId>,
    timeline_id: Option<TimelineId>,
    name: &str,
    shutdown_process_on_error: bool,
    future: F,
) -> PageserverTaskId
where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let cancel = CancellationToken::new();
    let task_id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
    let task = Arc::new(PageServerTask {
        task_id: PageserverTaskId(task_id),
        kind,
        name: name.to_string(),
        cancel: cancel.clone(),
        tenant_id,
        timeline_id,
        mutable: Mutex::new(MutableTaskState { join_handle: None }),
    });

    TASKS.lock().unwrap().insert(task_id, Arc::clone(&task));

    let mut task_mut = task.mutable.lock().unwrap();

    let task_name = name.to_string();
    let task_cloned = Arc::clone(&task);
    let join_handle = runtime.spawn(task_wrapper(
        task_name,
        task_id,
        task_cloned,
        cancel,
        shutdown_process_on_error,
        future,
    ));
    task_mut.join_handle = Some(join_handle);
    drop(task_mut);

    // The task is now running. Nothing more to do here
    PageserverTaskId(task_id)
}

/// This wrapper function runs in a newly-spawned task. It initializes the
/// task-local variables and calls the payload function.
async fn task_wrapper<F>(
    task_name: String,
    task_id: u64,
    task: Arc<PageServerTask>,
    shutdown_token: CancellationToken,
    shutdown_process_on_error: bool,
    future: F,
) where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    debug!("Starting task '{}'", task_name);

    let result = SHUTDOWN_TOKEN
        .scope(
            shutdown_token,
            CURRENT_TASK.scope(task, {
                // We use AssertUnwindSafe here so that the payload function
                // doesn't need to be UnwindSafe. We don't do anything after the
                // unwinding that would expose us to unwind-unsafe behavior.
                AssertUnwindSafe(future).catch_unwind()
            }),
        )
        .await;
    task_finish(result, task_name, task_id, shutdown_process_on_error).await;
}

async fn task_finish(
    result: std::result::Result<
        anyhow::Result<()>,
        std::boxed::Box<dyn std::any::Any + std::marker::Send>,
    >,
    task_name: String,
    task_id: u64,
    shutdown_process_on_error: bool,
) {
    // Remove our entry from the global hashmap.
    let task = TASKS
        .lock()
        .unwrap()
        .remove(&task_id)
        .expect("no task in registry");

    let mut shutdown_process = false;
    {
        match result {
            Ok(Ok(())) => {
                debug!("Task '{}' exited normally", task_name);
            }
            Ok(Err(err)) => {
                if shutdown_process_on_error {
                    error!(
                        "Shutting down: task '{}' tenant_id: {:?}, timeline_id: {:?} exited with error: {:?}",
                        task_name, task.tenant_id, task.timeline_id, err
                    );
                    shutdown_process = true;
                } else {
                    error!(
                        "Task '{}' tenant_id: {:?}, timeline_id: {:?} exited with error: {:?}",
                        task_name, task.tenant_id, task.timeline_id, err
                    );
                }
            }
            Err(err) => {
                if shutdown_process_on_error {
                    error!(
                        "Shutting down: task '{}' tenant_id: {:?}, timeline_id: {:?} panicked: {:?}",
                        task_name, task.tenant_id, task.timeline_id, err
                    );
                    shutdown_process = true;
                } else {
                    error!(
                        "Task '{}' tenant_id: {:?}, timeline_id: {:?} panicked: {:?}",
                        task_name, task.tenant_id, task.timeline_id, err
                    );
                }
            }
        }
    }

    if shutdown_process {
        shutdown_pageserver(None, 1).await;
    }
}

/// Signal and wait for tasks to shut down.
///
///
/// The arguments are used to select the tasks to kill. Any None arguments are
/// ignored. For example, to shut down all WalReceiver tasks:
///
///   shutdown_tasks(Some(TaskKind::WalReceiver), None, None)
///
/// Or to shut down all tasks for given timeline:
///
///   shutdown_tasks(None, Some(tenant_id), Some(timeline_id))
///
pub async fn shutdown_tasks(
    kind: Option<TaskKind>,
    tenant_id: Option<TenantId>,
    timeline_id: Option<TimelineId>,
) {
    let mut victim_tasks = Vec::new();

    {
        let tasks = TASKS.lock().unwrap();
        for task in tasks.values() {
            if (kind.is_none() || Some(task.kind) == kind)
                && (tenant_id.is_none() || task.tenant_id == tenant_id)
                && (timeline_id.is_none() || task.timeline_id == timeline_id)
            {
                task.cancel.cancel();
                victim_tasks.push((
                    Arc::clone(task),
                    task.kind,
                    task.tenant_id,
                    task.timeline_id,
                ));
            }
        }
    }

    let log_all = kind.is_none() && tenant_id.is_none() && timeline_id.is_none();

    for (task, task_kind, tenant_id, timeline_id) in victim_tasks {
        let join_handle = {
            let mut task_mut = task.mutable.lock().unwrap();
            task_mut.join_handle.take()
        };
        if let Some(mut join_handle) = join_handle {
            if log_all {
                if tenant_id.is_none() {
                    // there are quite few of these
                    info!(name = task.name, kind = ?task_kind, "stopping global task");
                } else {
                    // warn to catch these in tests; there shouldn't be any
                    warn!(name = task.name, tenant_id = ?tenant_id, timeline_id = ?timeline_id, kind = ?task_kind, "stopping left-over");
                }
            }
            if tokio::time::timeout(std::time::Duration::from_secs(1), &mut join_handle)
                .await
                .is_err()
            {
                // allow some time to elapse before logging to cut down the number of log
                // lines.
                info!("waiting for {} to shut down", task.name);
                // we never handled this return value, but:
                // - we don't deschedule which would lead to is_cancelled
                // - panics are already logged (is_panicked)
                // - task errors are already logged in the wrapper
                let _ = join_handle.await;
            }
        } else {
            // Possibly one of:
            //  * The task had not even fully started yet.
            //  * It was shut down concurrently and already exited
        }
    }
}

pub fn current_task_kind() -> Option<TaskKind> {
    CURRENT_TASK.try_with(|ct| ct.kind).ok()
}

pub fn current_task_id() -> Option<PageserverTaskId> {
    CURRENT_TASK.try_with(|ct| ct.task_id).ok()
}

/// A Future that can be used to check if the current task has been requested to
/// shut down.
pub async fn shutdown_watcher() {
    let token = SHUTDOWN_TOKEN
        .try_with(|t| t.clone())
        .expect("shutdown_watcher() called in an unexpected task or thread");

    token.cancelled().await;
}

/// Clone the current task's cancellation token, which can be moved across tasks.
///
/// When the task which is currently executing is shutdown, the cancellation token will be
/// cancelled. It can however be moved to other tasks, such as `tokio::task::spawn_blocking` or
/// `tokio::task::JoinSet::spawn`.
pub fn shutdown_token() -> CancellationToken {
    SHUTDOWN_TOKEN
        .try_with(|t| t.clone())
        .expect("shutdown_token() called in an unexpected task or thread")
}

/// Has the current task been requested to shut down?
pub fn is_shutdown_requested() -> bool {
    if let Ok(cancel) = SHUTDOWN_TOKEN.try_with(|t| t.clone()) {
        cancel.is_cancelled()
    } else {
        if !cfg!(test) {
            warn!("is_shutdown_requested() called in an unexpected task or thread");
        }
        false
    }
}
