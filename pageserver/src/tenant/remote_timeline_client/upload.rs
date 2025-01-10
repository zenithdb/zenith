//! Helper functions to upload files to remote storage with a RemoteStorage

use anyhow::{bail, Context};
use bytes::Bytes;
use camino::Utf8Path;
use fail::fail_point;
use pageserver_api::shard::TenantShardId;
use std::io::{ErrorKind, SeekFrom};
use std::time::SystemTime;
use tokio::fs::{self, File};
use tokio::io::AsyncSeekExt;
use tokio_util::sync::CancellationToken;
use utils::{backoff, pausable_failpoint};

use super::index::IndexPart;
use super::manifest::TenantManifest;
use super::Generation;
use crate::tenant::remote_timeline_client::{
    remote_index_path, remote_initdb_archive_path, remote_initdb_preserved_archive_path,
    remote_tenant_manifest_path,
};
use remote_storage::{GenericRemoteStorage, RemotePath, TimeTravelError};
use utils::id::{TenantId, TimelineId};

use tracing::info;

/// Serializes and uploads the given index part data to the remote storage.
pub(crate) async fn upload_index_part(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    generation: Generation,
    index_part: &IndexPart,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    tracing::trace!("uploading new index part");

    fail_point!("before-upload-index", |_| {
        bail!("failpoint before-upload-index")
    });
    pausable_failpoint!("before-upload-index-pausable");

    // FIXME: this error comes too late
    let serialized = index_part.to_json_bytes()?;
    let serialized = Bytes::from(serialized);

    let index_part_size = serialized.len();

    let remote_path = remote_index_path(tenant_shard_id, timeline_id, generation);
    storage
        .upload_storage_object(
            futures::stream::once(futures::future::ready(Ok(serialized))),
            index_part_size,
            &remote_path,
            cancel,
        )
        .await
        .with_context(|| format!("upload index part for '{tenant_shard_id} / {timeline_id}'"))
}
/// Serializes and uploads the given tenant manifest data to the remote storage.
pub(crate) async fn upload_tenant_manifest(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    generation: Generation,
    tenant_manifest: &TenantManifest,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    tracing::trace!("uploading new tenant manifest");

    fail_point!("before-upload-manifest", |_| {
        bail!("failpoint before-upload-manifest")
    });
    pausable_failpoint!("before-upload-manifest-pausable");

    let serialized = tenant_manifest.to_json_bytes()?;
    let serialized = Bytes::from(serialized);

    let tenant_manifest_site = serialized.len();

    let remote_path = remote_tenant_manifest_path(tenant_shard_id, generation);
    storage
        .upload_storage_object(
            futures::stream::once(futures::future::ready(Ok(serialized))),
            tenant_manifest_site,
            &remote_path,
            cancel,
        )
        .await
        .with_context(|| format!("upload tenant manifest for '{tenant_shard_id}'"))
}

/// Attempts to upload given layer files.
/// No extra checks for overlapping files is made and any files that are already present remotely will be overwritten, if submitted during the upload.
///
/// On an error, bumps the retries count and reschedules the entire task.
pub(super) async fn upload_timeline_layer<'a>(
    storage: &'a GenericRemoteStorage,
    local_path: &'a Utf8Path,
    remote_path: &'a RemotePath,
    metadata_size: u64,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    fail_point!("before-upload-layer", |_| {
        bail!("failpoint before-upload-layer")
    });

    pausable_failpoint!("before-upload-layer-pausable");

    let source_file_res = fs::File::open(&local_path).await;
    let source_file = match source_file_res {
        Ok(source_file) => source_file,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // If we encounter this arm, it wasn't intended, but it's also not
            // a big problem, if it's because the file was deleted before an
            // upload. However, a nonexistent file can also be indicative of
            // something worse, like when a file is scheduled for upload before
            // it has been written to disk yet.
            //
            // This is tested against `test_compaction_delete_before_upload`
            info!(path = %local_path, "File to upload doesn't exist. Likely the file has been deleted and an upload is not required any more.");
            return Ok(());
        }
        Err(e) => Err(e).with_context(|| format!("open a source file for layer {local_path:?}"))?,
    };

    let fs_size = source_file
        .metadata()
        .await
        .with_context(|| format!("get the source file metadata for layer {local_path:?}"))?
        .len();

    if metadata_size != fs_size {
        bail!("File {local_path:?} has its current FS size {fs_size} diferent from initially determined {metadata_size}");
    }

    let fs_size = usize::try_from(fs_size)
        .with_context(|| format!("convert {local_path:?} size {fs_size} usize"))?;

    let reader = tokio_util::io::ReaderStream::with_capacity(source_file, super::BUFFER_SIZE);

    storage
        .upload(reader, fs_size, remote_path, None, cancel)
        .await
        .with_context(|| format!("upload layer from local path '{local_path}'"))
}

pub(super) async fn copy_timeline_layer(
    storage: &GenericRemoteStorage,
    source_path: &RemotePath,
    target_path: &RemotePath,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    fail_point!("before-copy-layer", |_| {
        bail!("failpoint before-copy-layer")
    });

    pausable_failpoint!("before-copy-layer-pausable");

    storage
        .copy_object(source_path, target_path, cancel)
        .await
        .with_context(|| format!("copy layer {source_path} to {target_path}"))
}

/// Uploads the given `initdb` data to the remote storage.
pub(crate) async fn upload_initdb_dir(
    storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    mut initdb_tar_zst: File,
    size: u64,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    tracing::trace!("uploading initdb dir");

    // We might have read somewhat into the file already in the prior retry attempt
    initdb_tar_zst.seek(SeekFrom::Start(0)).await?;

    let file = tokio_util::io::ReaderStream::with_capacity(initdb_tar_zst, super::BUFFER_SIZE);

    let remote_path = remote_initdb_archive_path(tenant_id, timeline_id);
    storage
        .upload_storage_object(file, size as usize, &remote_path, cancel)
        .await
        .with_context(|| format!("upload initdb dir for '{tenant_id} / {timeline_id}'"))
}

pub(crate) async fn preserve_initdb_archive(
    storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let source_path = remote_initdb_archive_path(tenant_id, timeline_id);
    let dest_path = remote_initdb_preserved_archive_path(tenant_id, timeline_id);
    storage
        .copy_object(&source_path, &dest_path, cancel)
        .await
        .with_context(|| format!("backing up initdb archive for '{tenant_id} / {timeline_id}'"))
}

pub(crate) async fn time_travel_recover_tenant(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timestamp: SystemTime,
    done_if_after: SystemTime,
    cancel: &CancellationToken,
) -> Result<(), TimeTravelError> {
    let warn_after = 3;
    let max_attempts = 10;
    let mut prefixes = Vec::with_capacity(2);
    if tenant_shard_id.is_shard_zero() {
        // Also recover the unsharded prefix for a shard of zero:
        // - if the tenant is totally unsharded, the unsharded prefix contains all the data
        // - if the tenant is sharded, we still want to recover the initdb data, but we only
        //   want to do it once, so let's do it on the 0 shard
        let timelines_path_unsharded =
            super::remote_timelines_path_unsharded(&tenant_shard_id.tenant_id);
        prefixes.push(timelines_path_unsharded);
    }
    if !tenant_shard_id.is_unsharded() {
        // If the tenant is sharded, we need to recover the sharded prefix
        let timelines_path = super::remote_timelines_path(tenant_shard_id);
        prefixes.push(timelines_path);
    }
    for prefix in &prefixes {
        backoff::retry(
            || async {
                storage
                    .time_travel_recover(Some(prefix), timestamp, done_if_after, cancel)
                    .await
            },
            |e| !matches!(e, TimeTravelError::Other(_)),
            warn_after,
            max_attempts,
            "time travel recovery of tenant prefix",
            cancel,
        )
        .await
        .ok_or_else(|| TimeTravelError::Cancelled)
        .and_then(|x| x)?;
    }
    Ok(())
}
