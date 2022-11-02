pub mod basebackup;
pub mod config;
pub mod http;
pub mod import_datadir;
pub mod keyspace;
pub mod metrics;
pub mod page_cache;
pub mod page_image_cache;
pub mod page_service;
pub mod pgdatadir_mapping;
pub mod profiling;
pub mod repository;
pub mod storage_sync;
pub mod task_mgr;
pub mod tenant;
pub mod tenant_config;
pub mod tenant_mgr;
pub mod tenant_tasks;
pub mod virtual_file;
pub mod walingest;
pub mod walreceiver;
pub mod walrecord;
pub mod walredo;

use std::collections::HashMap;

use tracing::info;
use utils::id::{TenantId, TimelineId};

use crate::task_mgr::TaskKind;

/// Current storage format version
///
/// This is embedded in the header of all the layer files.
/// If you make any backwards-incompatible changes to the storage
/// format, bump this!
/// Note that TimelineMetadata uses its own version number to track
/// backwards-compatible changes to the metadata format.
pub const STORAGE_FORMAT_VERSION: u16 = 3;

pub const DEFAULT_PG_VERSION: u32 = 14;

// Magic constants used to identify different kinds of files
pub const IMAGE_FILE_MAGIC: u16 = 0x5A60;
pub const DELTA_FILE_MAGIC: u16 = 0x5A61;

static ZERO_PAGE: bytes::Bytes = bytes::Bytes::from_static(&[0u8; 8192]);

/// Config for the Repository checkpointer
#[derive(Debug, Clone, Copy)]
pub enum CheckpointConfig {
    // Flush all in-memory data
    Flush,
    // Flush all in-memory data and reconstruct all page images
    Forced,
}

pub async fn shutdown_pageserver(exit_code: i32) {
    // Shut down the libpq endpoint task. This prevents new connections from
    // being accepted.
    task_mgr::shutdown_tasks(Some(TaskKind::LibpqEndpointListener), None, None).await;

    // Shut down any page service tasks.
    task_mgr::shutdown_tasks(Some(TaskKind::PageRequestHandler), None, None).await;

    // Shut down all the tenants. This flushes everything to disk and kills
    // the checkpoint and GC tasks.
    tenant_mgr::shutdown_all_tenants().await;

    // Stop syncing with remote storage.
    //
    // FIXME: Does this wait for the sync tasks to finish syncing what's queued up?
    // Should it?
    task_mgr::shutdown_tasks(Some(TaskKind::StorageSync), None, None).await;

    // Shut down the HTTP endpoint last, so that you can still check the server's
    // status while it's shutting down.
    // FIXME: We should probably stop accepting commands like attach/detach earlier.
    task_mgr::shutdown_tasks(Some(TaskKind::HttpEndpointListener), None, None).await;

    // There should be nothing left, but let's be sure
    task_mgr::shutdown_tasks(None, None, None).await;
    info!("Shut down successfully completed");
    std::process::exit(exit_code);
}

const DEFAULT_BASE_BACKOFF_SECONDS: f64 = 0.1;
const DEFAULT_MAX_BACKOFF_SECONDS: f64 = 3.0;

async fn exponential_backoff(n: u32, base_increment: f64, max_seconds: f64) {
    let backoff_duration_seconds =
        exponential_backoff_duration_seconds(n, base_increment, max_seconds);
    if backoff_duration_seconds > 0.0 {
        info!(
            "Backoff: waiting {backoff_duration_seconds} seconds before processing with the task",
        );
        tokio::time::sleep(std::time::Duration::from_secs_f64(backoff_duration_seconds)).await;
    }
}

fn exponential_backoff_duration_seconds(n: u32, base_increment: f64, max_seconds: f64) -> f64 {
    if n == 0 {
        0.0
    } else {
        (1.0 + base_increment).powf(f64::from(n)).min(max_seconds)
    }
}

/// A newtype to store arbitrary data grouped by tenant and timeline ids.
/// One could use [`utils::id::TenantTimelineId`] for grouping, but that would
/// not include the cases where a certain tenant has zero timelines.
/// This is sometimes important: a tenant could be registered during initial load from FS,
/// even if he has no timelines on disk.
#[derive(Debug)]
pub struct TenantTimelineValues<T>(HashMap<TenantId, HashMap<TimelineId, T>>);

impl<T> TenantTimelineValues<T> {
    fn new() -> Self {
        Self(HashMap::new())
    }
}

/// A suffix to be used during file sync from the remote storage,
/// to ensure that we do not leave corrupted files that pretend to be layers.
const TEMP_FILE_SUFFIX: &str = "___temp";

#[cfg(test)]
mod backoff_defaults_tests {
    use super::*;

    #[test]
    fn backoff_defaults_produce_growing_backoff_sequence() {
        let mut current_backoff_value = None;

        for i in 0..10_000 {
            let new_backoff_value = exponential_backoff_duration_seconds(
                i,
                DEFAULT_BASE_BACKOFF_SECONDS,
                DEFAULT_MAX_BACKOFF_SECONDS,
            );

            if let Some(old_backoff_value) = current_backoff_value.replace(new_backoff_value) {
                assert!(
                    old_backoff_value <= new_backoff_value,
                    "{i}th backoff value {new_backoff_value} is smaller than the previous one {old_backoff_value}"
                )
            }
        }

        assert_eq!(
            current_backoff_value.expect("Should have produced backoff values to compare"),
            DEFAULT_MAX_BACKOFF_SECONDS,
            "Given big enough of retries, backoff should reach its allowed max value"
        );
    }
}
