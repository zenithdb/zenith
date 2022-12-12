//! Helper functions to download files from remote storage with a RemoteStorage
use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::config::PageServerConf;
use crate::storage_sync::index::LayerFileMetadata;
use remote_storage::{DownloadError, GenericRemoteStorage, RemotePath};
use utils::crashsafe::path_with_suffix_extension;
use utils::id::{TenantId, TimelineId};

use super::index::IndexPart;

async fn fsync_path(path: impl AsRef<std::path::Path>) -> Result<(), std::io::Error> {
    fs::File::open(path).await?.sync_all().await
}

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
pub async fn download_layer_file<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    remote_path: &'a RemotePath,
    layer_metadata: &'a LayerFileMetadata,
) -> anyhow::Result<u64> {
    let local_path = conf.local_path(remote_path);

    // Perform a rename inspired by durable_rename from file_utils.c.
    // The sequence:
    //     write(tmp)
    //     fsync(tmp)
    //     rename(tmp, new)
    //     fsync(new)
    //     fsync(parent)
    // For more context about durable_rename check this email from postgres mailing list:
    // https://www.postgresql.org/message-id/56583BDD.9060302@2ndquadrant.com
    // If pageserver crashes the temp file will be deleted on startup and re-downloaded.
    let temp_file_path = path_with_suffix_extension(&local_path, TEMP_DOWNLOAD_EXTENSION);

    // TODO: this doesn't use the cached fd for some reason?
    let mut destination_file = fs::File::create(&temp_file_path).await.with_context(|| {
        format!(
            "Failed to create a destination file for layer '{}'",
            temp_file_path.display()
        )
    })?;
    let mut download = storage.download(remote_path).await.with_context(|| {
        format!(
            "Failed to open a download stream for layer with remote storage path '{remote_path:?}'"
        )
    })?;
    let bytes_amount = tokio::io::copy(&mut download.download_stream, &mut destination_file).await.with_context(|| {
        format!("Failed to download layer with remote storage path '{remote_path:?}' into file {temp_file_path:?}")
    })?;

    // Tokio doc here: https://docs.rs/tokio/1.17.0/tokio/fs/struct.File.html states that:
    // A file will not be closed immediately when it goes out of scope if there are any IO operations
    // that have not yet completed. To ensure that a file is closed immediately when it is dropped,
    // you should call flush before dropping it.
    //
    // From the tokio code I see that it waits for pending operations to complete. There shouldt be any because
    // we assume that `destination_file` file is fully written. I e there is no pending .write(...).await operations.
    // But for additional safety lets check/wait for any pending operations.
    destination_file.flush().await.with_context(|| {
        format!(
            "failed to flush source file at {}",
            temp_file_path.display()
        )
    })?;

    match layer_metadata.file_size() {
        Some(expected) if expected != bytes_amount => {
            anyhow::bail!(
                "According to layer file metadata should had downloaded {expected} bytes but downloaded {bytes_amount} bytes into file '{}'",
                temp_file_path.display()
            );
        }
        Some(_) | None => {
            // matches, or upgrading from an earlier IndexPart version
        }
    }

    // not using sync_data because it can lose file size update
    destination_file.sync_all().await.with_context(|| {
        format!(
            "failed to fsync source file at {}",
            temp_file_path.display()
        )
    })?;
    drop(destination_file);

    crate::fail_point!("remote-storage-download-pre-rename", |_| {
        bail!("remote-storage-download-pre-rename failpoint triggered")
    });

    fs::rename(&temp_file_path, &local_path).await?;

    fsync_path(&local_path)
        .await
        .with_context(|| format!("Could not fsync layer file {}", local_path.display(),))?;

    tracing::info!("download complete: {}", local_path.display());

    Ok(bytes_amount)
}

const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

pub fn is_temp_download_file(path: &Path) -> bool {
    let extension = path.extension().map(|pname| {
        pname
            .to_str()
            .expect("paths passed to this function must be valid Rust strings")
    });
    match extension {
        Some(TEMP_DOWNLOAD_EXTENSION) => true,
        Some(_) => false,
        None => false,
    }
}

/// List timelines of given tenant in remote storage
pub async fn list_remote_timelines<'a>(
    storage: &'a GenericRemoteStorage,
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> anyhow::Result<Vec<(TimelineId, IndexPart)>> {
    let tenant_path = conf.timelines_path(&tenant_id);
    let tenant_storage_path = conf.remote_path(&tenant_path)?;

    let timelines = storage
        .list_prefixes(Some(&tenant_storage_path))
        .await
        .with_context(|| {
            format!(
                "Failed to list tenant storage path {tenant_storage_path:?} to get remote timelines to download"
            )
        })?;

    if timelines.is_empty() {
        anyhow::bail!("no timelines found on the remote storage")
    }

    let mut timeline_ids = HashSet::new();
    let mut part_downloads = FuturesUnordered::new();

    for timeline_remote_storage_key in timelines {
        let object_name = timeline_remote_storage_key.object_name().ok_or_else(|| {
            anyhow::anyhow!("failed to get timeline id for remote tenant {tenant_id}")
        })?;

        let timeline_id: TimelineId = object_name.parse().with_context(|| {
            format!("failed to parse object name into timeline id '{object_name}'")
        })?;

        // list_prefixes returns all files with the prefix. If we haven't seen this timeline ID
        // yet, launch a download task for it.
        if !timeline_ids.contains(&timeline_id) {
            timeline_ids.insert(timeline_id);
            let storage_clone = storage.clone();
            part_downloads.push(async move {
                (
                    timeline_id,
                    download_index_part(conf, &storage_clone, tenant_id, timeline_id).await,
                )
            });
        }
    }

    // Wait for all the download tasks to complete.
    let mut timeline_parts = Vec::new();
    while let Some((timeline_id, part_upload_result)) = part_downloads.next().await {
        let index_part = part_upload_result
            .with_context(|| format!("Failed to fetch index part for timeline {timeline_id}"))?;

        debug!("Successfully fetched index part for timeline {timeline_id}");
        timeline_parts.push((timeline_id, index_part));
    }
    Ok(timeline_parts)
}

pub async fn download_index_part(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<IndexPart, DownloadError> {
    let index_part_path = conf
        .metadata_path(timeline_id, tenant_id)
        .with_file_name(IndexPart::FILE_NAME);
    let part_storage_path = conf
        .remote_path(&index_part_path)
        .map_err(DownloadError::BadInput)?;

    let mut index_part_download = storage.download(&part_storage_path).await?;

    let mut index_part_bytes = Vec::new();
    tokio::io::copy(
        &mut index_part_download.download_stream,
        &mut index_part_bytes,
    )
    .await
    .with_context(|| format!("Failed to download an index part into file {index_part_path:?}"))
    .map_err(DownloadError::Other)?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| {
            format!("Failed to deserialize index part file into file {index_part_path:?}")
        })
        .map_err(DownloadError::Other)?;

    Ok(index_part)
}
