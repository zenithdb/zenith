#![recursion_limit = "300"]
#![deny(clippy::undocumented_unsafe_blocks)]

mod auth;
pub mod basebackup;
pub mod config;
pub mod consumption_metrics;
pub mod context;
pub mod controller_upcall_client;
pub mod deletion_queue;
pub mod disk_usage_eviction_task;
pub mod http;
pub mod import_datadir;
pub mod l0_flush;

extern crate hyper0 as hyper;

use futures::{stream::FuturesUnordered, StreamExt};
pub use pageserver_api::keyspace;
use tokio_util::sync::CancellationToken;
mod assert_u64_eq_usize;
pub mod aux_file;
pub mod metrics;
pub mod page_cache;
pub mod page_service;
pub mod pgdatadir_mapping;
pub mod span;
pub(crate) mod statvfs;
pub mod task_mgr;
pub mod tenant;
pub mod utilization;
pub mod virtual_file;
pub mod walingest;
pub mod walredo;

use camino::Utf8Path;
use deletion_queue::DeletionQueue;
use tenant::{
    mgr::{BackgroundPurges, TenantManager},
    secondary,
};
use tracing::{info, info_span};

/// Current storage format version
///
/// This is embedded in the header of all the layer files.
/// If you make any backwards-incompatible changes to the storage
/// format, bump this!
/// Note that TimelineMetadata uses its own version number to track
/// backwards-compatible changes to the metadata format.
pub const STORAGE_FORMAT_VERSION: u16 = 3;

pub const DEFAULT_PG_VERSION: u32 = 16;

// Magic constants used to identify different kinds of files
pub const IMAGE_FILE_MAGIC: u16 = 0x5A60;
pub const DELTA_FILE_MAGIC: u16 = 0x5A61;

static ZERO_PAGE: bytes::Bytes = bytes::Bytes::from_static(&[0u8; 8192]);

pub use crate::metrics::preinitialize_metrics;

pub struct CancellableTask {
    pub task: tokio::task::JoinHandle<()>,
    pub cancel: CancellationToken,
}
pub struct HttpEndpointListener(pub CancellableTask);
pub struct ConsumptionMetricsTasks(pub CancellableTask);
pub struct DiskUsageEvictionTask(pub CancellableTask);
impl CancellableTask {
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}

#[tracing::instrument(skip_all, fields(%exit_code))]
#[allow(clippy::too_many_arguments)]
pub async fn shutdown_pageserver(
    http_listener: HttpEndpointListener,
    page_service: page_service::Listener,
    consumption_metrics_worker: ConsumptionMetricsTasks,
    disk_usage_eviction_task: Option<DiskUsageEvictionTask>,
    tenant_manager: &TenantManager,
    background_purges: BackgroundPurges,
    mut deletion_queue: DeletionQueue,
    secondary_controller_tasks: secondary::GlobalTasks,
    exit_code: i32,
) {
    use std::time::Duration;

    let started_at = std::time::Instant::now();

    // If the orderly shutdown below takes too long, we still want to make
    // sure that all walredo processes are killed and wait()ed on by us, not systemd.
    //
    // (Leftover walredo processes are the hypothesized trigger for the systemd freezes
    //  that we keep seeing in prod => https://github.com/neondatabase/cloud/issues/11387.
    //
    // We use a thread instead of a tokio task because the background runtime is likely busy
    // with the final flushing / uploads. This activity here has priority, and due to lack
    // of scheduling priority feature sin the tokio scheduler, using a separate thread is
    // an effective priority booster.
    let walredo_extraordinary_shutdown_thread_span = {
        let span = info_span!(parent: None, "walredo_extraordinary_shutdown_thread");
        span.follows_from(tracing::Span::current());
        span
    };
    let walredo_extraordinary_shutdown_thread_cancel = CancellationToken::new();
    let walredo_extraordinary_shutdown_thread = std::thread::spawn({
        let walredo_extraordinary_shutdown_thread_cancel =
            walredo_extraordinary_shutdown_thread_cancel.clone();
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let _entered = rt.enter();
            let _entered = walredo_extraordinary_shutdown_thread_span.enter();
            if let Ok(()) = rt.block_on(tokio::time::timeout(
                Duration::from_secs(8),
                walredo_extraordinary_shutdown_thread_cancel.cancelled(),
            )) {
                info!("cancellation requested");
                return;
            }
            let managers = tenant::WALREDO_MANAGERS
                .lock()
                .unwrap()
                // prevents new walredo managers from being inserted
                .take()
                .expect("only we take()");
            // Use FuturesUnordered to get in queue early for each manager's
            // heavier_once_cell semaphore wait list.
            // Also, for idle tenants that for some reason haven't
            // shut down yet, it's quite likely that we're not going
            // to get Poll::Pending once.
            let mut futs: FuturesUnordered<_> = managers
                .into_iter()
                .filter_map(|(_, mgr)| mgr.upgrade())
                .map(|mgr| async move { tokio::task::unconstrained(mgr.shutdown()).await })
                .collect();
            info!(count=%futs.len(), "built FuturesUnordered");
            let mut last_log_at = std::time::Instant::now();
            #[derive(Debug, Default)]
            struct Results {
                initiated: u64,
                already: u64,
            }
            let mut results = Results::default();
            while let Some(we_initiated) = rt.block_on(futs.next()) {
                if we_initiated {
                    results.initiated += 1;
                } else {
                    results.already += 1;
                }
                if last_log_at.elapsed() > Duration::from_millis(100) {
                    info!(remaining=%futs.len(), ?results, "progress");
                    last_log_at = std::time::Instant::now();
                }
            }
            info!(?results, "done");
        }
    });

    // Shut down the libpq endpoint task. This prevents new connections from
    // being accepted.
    let remaining_connections = timed(
        page_service.stop_accepting(),
        "shutdown LibpqEndpointListener",
        Duration::from_secs(1),
    )
    .await;

    // Shut down all the tenants. This flushes everything to disk and kills
    // the checkpoint and GC tasks.
    timed(
        tenant_manager.shutdown(),
        "shutdown all tenants",
        Duration::from_secs(5),
    )
    .await;

    // Shut down any page service tasks: any in-progress work for particular timelines or tenants
    // should already have been canclled via mgr::shutdown_all_tenants
    timed(
        remaining_connections.shutdown(),
        "shutdown PageRequestHandlers",
        Duration::from_secs(1),
    )
    .await;

    // Best effort to persist any outstanding deletions, to avoid leaking objects
    deletion_queue.shutdown(Duration::from_secs(5)).await;

    timed(
        consumption_metrics_worker.0.shutdown(),
        "shutdown consumption metrics",
        Duration::from_secs(1),
    )
    .await;

    timed(
        futures::future::OptionFuture::from(disk_usage_eviction_task.map(|t| t.0.shutdown())),
        "shutdown disk usage eviction",
        Duration::from_secs(1),
    )
    .await;

    timed(
        background_purges.shutdown(),
        "shutdown background purges",
        Duration::from_secs(1),
    )
    .await;

    // Shut down the HTTP endpoint last, so that you can still check the server's
    // status while it's shutting down.
    // FIXME: We should probably stop accepting commands like attach/detach earlier.
    timed(
        http_listener.0.shutdown(),
        "shutdown http",
        Duration::from_secs(1),
    )
    .await;

    timed(
        secondary_controller_tasks.wait(), // cancellation happened in caller
        "secondary controller wait",
        Duration::from_secs(1),
    )
    .await;

    // There should be nothing left, but let's be sure
    timed(
        task_mgr::shutdown_tasks(None, None, None),
        "shutdown leftovers",
        Duration::from_secs(1),
    )
    .await;

    info!("cancel & join walredo_extraordinary_shutdown_thread");
    walredo_extraordinary_shutdown_thread_cancel.cancel();
    walredo_extraordinary_shutdown_thread.join().unwrap();
    info!("walredo_extraordinary_shutdown_thread done");

    info!(
        elapsed_ms = started_at.elapsed().as_millis(),
        "Shut down successfully completed"
    );
    std::process::exit(exit_code);
}

/// Per-tenant configuration file.
/// Full path: `tenants/<tenant_id>/config-v1`.
pub(crate) const TENANT_LOCATION_CONFIG_NAME: &str = "config-v1";

/// Per-tenant copy of their remote heatmap, downloaded into the local
/// tenant path while in secondary mode.
pub(crate) const TENANT_HEATMAP_BASENAME: &str = "heatmap-v1.json";

/// A suffix used for various temporary files. Any temporary files found in the
/// data directory at pageserver startup can be automatically removed.
pub(crate) const TEMP_FILE_SUFFIX: &str = "___temp";

/// A marker file to mark that a timeline directory was not fully initialized.
/// If a timeline directory with this marker is encountered at pageserver startup,
/// the timeline directory and the marker file are both removed.
/// Full path: `tenants/<tenant_id>/timelines/<timeline_id>___uninit`.
pub(crate) const TIMELINE_UNINIT_MARK_SUFFIX: &str = "___uninit";

pub(crate) const TIMELINE_DELETE_MARK_SUFFIX: &str = "___delete";

pub fn is_temporary(path: &Utf8Path) -> bool {
    match path.file_name() {
        Some(name) => name.ends_with(TEMP_FILE_SUFFIX),
        None => false,
    }
}

fn ends_with_suffix(path: &Utf8Path, suffix: &str) -> bool {
    match path.file_name() {
        Some(name) => name.ends_with(suffix),
        None => false,
    }
}

// FIXME: DO NOT ADD new query methods like this, which will have a next step of parsing timelineid
// from the directory name. Instead create type "UninitMark(TimelineId)" and only parse it once
// from the name.

pub(crate) fn is_uninit_mark(path: &Utf8Path) -> bool {
    ends_with_suffix(path, TIMELINE_UNINIT_MARK_SUFFIX)
}

pub(crate) fn is_delete_mark(path: &Utf8Path) -> bool {
    ends_with_suffix(path, TIMELINE_DELETE_MARK_SUFFIX)
}

/// During pageserver startup, we need to order operations not to exhaust tokio worker threads by
/// blocking.
///
/// The instances of this value exist only during startup, otherwise `None` is provided, meaning no
/// delaying is needed.
#[derive(Clone)]
pub struct InitializationOrder {
    /// Each initial tenant load task carries this until it is done loading timelines from remote storage
    pub initial_tenant_load_remote: Option<utils::completion::Completion>,

    /// Each initial tenant load task carries this until completion.
    pub initial_tenant_load: Option<utils::completion::Completion>,

    /// Barrier for when we can start any background jobs.
    ///
    /// This can be broken up later on, but right now there is just one class of a background job.
    pub background_jobs_can_start: utils::completion::Barrier,
}

/// Time the future with a warning when it exceeds a threshold.
async fn timed<Fut: std::future::Future>(
    fut: Fut,
    name: &str,
    warn_at: std::time::Duration,
) -> <Fut as std::future::Future>::Output {
    let started = std::time::Instant::now();

    let mut fut = std::pin::pin!(fut);

    match tokio::time::timeout(warn_at, &mut fut).await {
        Ok(ret) => {
            tracing::info!(
                stage = name,
                elapsed_ms = started.elapsed().as_millis(),
                "completed"
            );
            ret
        }
        Err(_) => {
            tracing::info!(
                stage = name,
                elapsed_ms = started.elapsed().as_millis(),
                "still waiting, taking longer than expected..."
            );

            let ret = fut.await;

            // this has a global allowed_errors
            tracing::warn!(
                stage = name,
                elapsed_ms = started.elapsed().as_millis(),
                "completed, took longer than expected"
            );

            ret
        }
    }
}

/// Like [`timed`], but the warning timeout only starts after `cancel` has been cancelled.
async fn timed_after_cancellation<Fut: std::future::Future>(
    fut: Fut,
    name: &str,
    warn_at: std::time::Duration,
    cancel: &CancellationToken,
) -> <Fut as std::future::Future>::Output {
    let mut fut = std::pin::pin!(fut);

    tokio::select! {
        _ = cancel.cancelled() => {
            timed(fut, name, warn_at).await
        }
        ret = &mut fut => {
            ret
        }
    }
}

#[cfg(test)]
mod timed_tests {
    use super::timed;
    use std::time::Duration;

    #[tokio::test]
    async fn timed_completes_when_inner_future_completes() {
        // A future that completes on time should have its result returned
        let r1 = timed(
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                123
            },
            "test 1",
            Duration::from_millis(50),
        )
        .await;
        assert_eq!(r1, 123);

        // A future that completes too slowly should also have its result returned
        let r1 = timed(
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                456
            },
            "test 1",
            Duration::from_millis(10),
        )
        .await;
        assert_eq!(r1, 456);
    }
}
