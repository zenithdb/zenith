//! Helper functions to download files from remote storage with a RemoteStorage
//!
//! The functions in this module retry failed operations automatically, according
//! to the FAILED_DOWNLOAD_RETRIES constant.

use std::collections::HashSet;
use std::future::Future;
use std::str::FromStr;
use std::time::SystemTime;

use anyhow::{anyhow, Context};
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::shard::TenantShardId;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use utils::backoff;

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::span::{
    debug_assert_current_span_has_tenant_and_timeline_id, debug_assert_current_span_has_tenant_id,
};
use crate::tenant::remote_timeline_client::{remote_layer_path, remote_timelines_path};
use crate::tenant::storage_layer::LayerName;
use crate::tenant::Generation;
use crate::virtual_file::{on_fatal_io_error, MaybeFatalIo, VirtualFile};
use crate::TEMP_FILE_SUFFIX;
use remote_storage::{
    DownloadError, DownloadKind, DownloadOpts, GenericRemoteStorage, ListingMode, RemotePath,
};
use utils::crashsafe::path_with_suffix_extension;
use utils::id::{TenantId, TimelineId};
use utils::pausable_failpoint;

use super::index::{IndexPart, LayerFileMetadata};
use super::manifest::TenantManifest;
use super::{
    parse_remote_index_path, parse_remote_tenant_manifest_path, remote_index_path,
    remote_initdb_archive_path, remote_initdb_preserved_archive_path, remote_tenant_manifest_path,
    remote_tenant_manifest_prefix, remote_tenant_path, FAILED_DOWNLOAD_WARN_THRESHOLD,
    FAILED_REMOTE_OP_RETRIES, INITDB_PATH,
};

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
#[allow(clippy::too_many_arguments)]
pub async fn download_layer_file<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    layer_file_name: &'a LayerName,
    layer_metadata: &'a LayerFileMetadata,
    local_path: &Utf8Path,
    gate: &utils::sync::gate::Gate,
    cancel: &CancellationToken,
    ctx: &RequestContext,
) -> Result<u64, DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let timeline_path = conf.timeline_path(&tenant_shard_id, &timeline_id);

    let remote_path = remote_layer_path(
        &tenant_shard_id.tenant_id,
        &timeline_id,
        layer_metadata.shard,
        layer_file_name,
        layer_metadata.generation,
    );

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
    let temp_file_path = path_with_suffix_extension(local_path, TEMP_DOWNLOAD_EXTENSION);

    let bytes_amount = download_retry(
        || async {
            download_object(storage, &remote_path, &temp_file_path, gate, cancel, ctx).await
        },
        &format!("download {remote_path:?}"),
        cancel,
    )
    .await?;

    let expected = layer_metadata.file_size;
    if expected != bytes_amount {
        return Err(DownloadError::Other(anyhow!(
            "According to layer file metadata should have downloaded {expected} bytes but downloaded {bytes_amount} bytes into file {temp_file_path:?}",
        )));
    }

    fail::fail_point!("remote-storage-download-pre-rename", |_| {
        Err(DownloadError::Other(anyhow!(
            "remote-storage-download-pre-rename failpoint triggered"
        )))
    });

    fs::rename(&temp_file_path, &local_path)
        .await
        .with_context(|| format!("rename download layer file to {local_path}"))
        .map_err(DownloadError::Other)?;

    // We use fatal_err() below because the after the rename above,
    // the in-memory state of the filesystem already has the layer file in its final place,
    // and subsequent pageserver code could think it's durable while it really isn't.
    let work = {
        let ctx = ctx.detached_child(ctx.task_kind(), ctx.download_behavior());
        async move {
            let timeline_dir = VirtualFile::open(&timeline_path, &ctx)
                .await
                .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }
    };
    crate::virtual_file::io_engine::get()
        .spawn_blocking_and_block_on_if_std(work)
        .await;

    tracing::debug!("download complete: {local_path}");

    Ok(bytes_amount)
}

/// Download the object `src_path` in the remote `storage` to local path `dst_path`.
///
/// If Ok() is returned, the download succeeded and the inode & data have been made durable.
/// (Note that the directory entry for the inode is not made durable.)
/// The file size in bytes is returned.
///
/// If Err() is returned, there was some error. The file at `dst_path` has been unlinked.
/// The unlinking has _not_ been made durable.
async fn download_object(
    storage: &GenericRemoteStorage,
    src_path: &RemotePath,
    dst_path: &Utf8PathBuf,
    #[cfg_attr(target_os = "macos", allow(unused_variables))] gate: &utils::sync::gate::Gate,
    cancel: &CancellationToken,
    #[cfg_attr(target_os = "macos", allow(unused_variables))] ctx: &RequestContext,
) -> Result<u64, DownloadError> {
    let res = match crate::virtual_file::io_engine::get() {
        crate::virtual_file::io_engine::IoEngine::NotSet => panic!("unset"),
        crate::virtual_file::io_engine::IoEngine::StdFs => {
            async {
                let destination_file = tokio::fs::File::create(dst_path)
                    .await
                    .with_context(|| format!("create a destination file for layer '{dst_path}'"))
                    .map_err(DownloadError::Other)?;

                let download = storage
                    .download(src_path, &DownloadOpts::default(), cancel)
                    .await?;

                pausable_failpoint!("before-downloading-layer-stream-pausable");

                let mut buf_writer =
                    tokio::io::BufWriter::with_capacity(super::BUFFER_SIZE, destination_file);

                let mut reader = tokio_util::io::StreamReader::new(download.download_stream);

                let bytes_amount = tokio::io::copy_buf(&mut reader, &mut buf_writer).await?;
                buf_writer.flush().await?;

                let mut destination_file = buf_writer.into_inner();

                // Tokio doc here: https://docs.rs/tokio/1.17.0/tokio/fs/struct.File.html states that:
                // A file will not be closed immediately when it goes out of scope if there are any IO operations
                // that have not yet completed. To ensure that a file is closed immediately when it is dropped,
                // you should call flush before dropping it.
                //
                // From the tokio code I see that it waits for pending operations to complete. There shouldt be any because
                // we assume that `destination_file` file is fully written. I e there is no pending .write(...).await operations.
                // But for additional safety lets check/wait for any pending operations.
                destination_file
                    .flush()
                    .await
                    .maybe_fatal_err("download_object sync_all")
                    .with_context(|| format!("flush source file at {dst_path}"))
                    .map_err(DownloadError::Other)?;

                // not using sync_data because it can lose file size update
                destination_file
                    .sync_all()
                    .await
                    .maybe_fatal_err("download_object sync_all")
                    .with_context(|| format!("failed to fsync source file at {dst_path}"))
                    .map_err(DownloadError::Other)?;

                Ok(bytes_amount)
            }
            .await
        }
        #[cfg(target_os = "linux")]
        crate::virtual_file::io_engine::IoEngine::TokioEpollUring => {
            use crate::virtual_file::owned_buffers_io;
            use crate::virtual_file::IoBufferMut;
            use std::sync::Arc;
            async {
                let destination_file = Arc::new(
                    VirtualFile::create(dst_path, ctx)
                        .await
                        .with_context(|| {
                            format!("create a destination file for layer '{dst_path}'")
                        })
                        .map_err(DownloadError::Other)?,
                );

                let mut download = storage
                    .download(src_path, &DownloadOpts::default(), cancel)
                    .await?;

                pausable_failpoint!("before-downloading-layer-stream-pausable");

                let mut buffered = owned_buffers_io::write::BufferedWriter::<IoBufferMut, _>::new(
                    destination_file,
                    || IoBufferMut::with_capacity(super::BUFFER_SIZE),
                    gate.enter().map_err(|_| DownloadError::Cancelled)?,
                    ctx,
                );

                // TODO: use vectored write (writev) once supported by tokio-epoll-uring.
                // There's chunks_vectored() on the stream.
                let (bytes_amount, destination_file) = async {
                    while let Some(res) =
                        futures::StreamExt::next(&mut download.download_stream).await
                    {
                        let chunk = match res {
                            Ok(chunk) => chunk,
                            Err(e) => return Err(e),
                        };
                        buffered.write_buffered_borrowed(&chunk, ctx).await?;
                    }
                    let inner = buffered.flush_and_into_inner(ctx).await?;
                    Ok(inner)
                }
                .await?;

                // not using sync_data because it can lose file size update
                destination_file
                    .sync_all()
                    .await
                    .maybe_fatal_err("download_object sync_all")
                    .with_context(|| format!("failed to fsync source file at {dst_path}"))
                    .map_err(DownloadError::Other)?;

                Ok(bytes_amount)
            }
            .await
        }
    };

    // in case the download failed, clean up
    match res {
        Ok(bytes_amount) => Ok(bytes_amount),
        Err(e) => {
            if let Err(e) = tokio::fs::remove_file(dst_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    on_fatal_io_error(&e, &format!("Removing temporary file {dst_path}"));
                }
            }
            Err(e)
        }
    }
}

const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

pub(crate) fn is_temp_download_file(path: &Utf8Path) -> bool {
    let extension = path.extension();
    match extension {
        Some(TEMP_DOWNLOAD_EXTENSION) => true,
        Some(_) => false,
        None => false,
    }
}

async fn list_identifiers<T>(
    storage: &GenericRemoteStorage,
    prefix: RemotePath,
    cancel: CancellationToken,
) -> anyhow::Result<(HashSet<T>, HashSet<String>)>
where
    T: FromStr + Eq + std::hash::Hash,
{
    let listing = download_retry_forever(
        || storage.list(Some(&prefix), ListingMode::WithDelimiter, None, &cancel),
        &format!("list identifiers in prefix {prefix}"),
        &cancel,
    )
    .await?;

    let mut parsed_ids = HashSet::new();
    let mut other_prefixes = HashSet::new();

    for id_remote_storage_key in listing.prefixes {
        let object_name = id_remote_storage_key.object_name().ok_or_else(|| {
            anyhow::anyhow!("failed to get object name for key {id_remote_storage_key}")
        })?;

        match object_name.parse::<T>() {
            Ok(t) => parsed_ids.insert(t),
            Err(_) => other_prefixes.insert(object_name.to_string()),
        };
    }

    for object in listing.keys {
        let object_name = object
            .key
            .object_name()
            .ok_or_else(|| anyhow::anyhow!("object name for key {}", object.key))?;
        other_prefixes.insert(object_name.to_string());
    }

    Ok((parsed_ids, other_prefixes))
}

/// List shards of given tenant in remote storage
pub(crate) async fn list_remote_tenant_shards(
    storage: &GenericRemoteStorage,
    tenant_id: TenantId,
    cancel: CancellationToken,
) -> anyhow::Result<(HashSet<TenantShardId>, HashSet<String>)> {
    let remote_path = remote_tenant_path(&TenantShardId::unsharded(tenant_id));
    list_identifiers::<TenantShardId>(storage, remote_path, cancel).await
}

/// List timelines of given tenant shard in remote storage
pub async fn list_remote_timelines(
    storage: &GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
    cancel: CancellationToken,
) -> anyhow::Result<(HashSet<TimelineId>, HashSet<String>)> {
    fail::fail_point!("storage-sync-list-remote-timelines", |_| {
        anyhow::bail!("storage-sync-list-remote-timelines");
    });

    let remote_path = remote_timelines_path(&tenant_shard_id).add_trailing_slash();
    list_identifiers::<TimelineId>(storage, remote_path, cancel).await
}

async fn do_download_remote_path_retry_forever(
    storage: &GenericRemoteStorage,
    remote_path: &RemotePath,
    download_opts: DownloadOpts,
    cancel: &CancellationToken,
) -> Result<(Vec<u8>, SystemTime), DownloadError> {
    download_retry_forever(
        || async {
            let download = storage
                .download(remote_path, &download_opts, cancel)
                .await?;

            let mut bytes = Vec::new();

            let stream = download.download_stream;
            let mut stream = StreamReader::new(stream);

            tokio::io::copy_buf(&mut stream, &mut bytes).await?;

            Ok((bytes, download.last_modified))
        },
        &format!("download {remote_path:?}"),
        cancel,
    )
    .await
}

async fn do_download_tenant_manifest(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    _timeline_id: Option<&TimelineId>,
    generation: Generation,
    cancel: &CancellationToken,
) -> Result<(TenantManifest, Generation, SystemTime), DownloadError> {
    let remote_path = remote_tenant_manifest_path(tenant_shard_id, generation);

    let download_opts = DownloadOpts {
        kind: DownloadKind::Small,
        ..Default::default()
    };

    let (manifest_bytes, manifest_bytes_mtime) =
        do_download_remote_path_retry_forever(storage, &remote_path, download_opts, cancel).await?;

    let tenant_manifest = TenantManifest::from_json_bytes(&manifest_bytes)
        .with_context(|| format!("deserialize tenant manifest file at {remote_path:?}"))
        .map_err(DownloadError::Other)?;

    Ok((tenant_manifest, generation, manifest_bytes_mtime))
}

async fn do_download_index_part(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timeline_id: Option<&TimelineId>,
    index_generation: Generation,
    cancel: &CancellationToken,
) -> Result<(IndexPart, Generation, SystemTime), DownloadError> {
    let timeline_id =
        timeline_id.expect("A timeline ID is always provided when downloading an index");
    let remote_path = remote_index_path(tenant_shard_id, timeline_id, index_generation);

    let download_opts = DownloadOpts {
        kind: DownloadKind::Small,
        ..Default::default()
    };

    let (index_part_bytes, index_part_mtime) =
        do_download_remote_path_retry_forever(storage, &remote_path, download_opts, cancel).await?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| format!("deserialize index part file at {remote_path:?}"))
        .map_err(DownloadError::Other)?;

    Ok((index_part, index_generation, index_part_mtime))
}

/// Metadata objects are "generationed", meaning that they include a generation suffix.  This
/// function downloads the object with the highest generation <= `my_generation`.
///
/// Data objects (layer files) also include a generation in their path, but there is no equivalent
/// search process, because their reference from an index includes the generation.
///
/// An expensive object listing operation is only done if necessary: the typical fast path is to issue two
/// GET operations, one to our own generation (stale attachment case), and one to the immediately preceding
/// generation (normal case when migrating/restarting).  Only if both of these return 404 do we fall back
/// to listing objects.
///
/// * `my_generation`: the value of `[crate::tenant::Tenant::generation]`
/// * `what`: for logging, what object are we downloading
/// * `prefix`: when listing objects, use this prefix (i.e. the part of the object path before the generation)
/// * `do_download`: a GET of the object in a particular generation, which should **retry indefinitely** unless
///                  `cancel`` has fired.  This function does not do its own retries of GET operations, and relies
///                  on the function passed in to do so.
/// * `parse_path`: parse a fully qualified remote storage path to get the generation of the object.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(generation=?my_generation))]
pub(crate) async fn download_generation_object<'a, T, DF, DFF, PF>(
    storage: &'a GenericRemoteStorage,
    tenant_shard_id: &'a TenantShardId,
    timeline_id: Option<&'a TimelineId>,
    my_generation: Generation,
    what: &str,
    prefix: RemotePath,
    do_download: DF,
    parse_path: PF,
    cancel: &'a CancellationToken,
) -> Result<(T, Generation, SystemTime), DownloadError>
where
    DF: Fn(
        &'a GenericRemoteStorage,
        &'a TenantShardId,
        Option<&'a TimelineId>,
        Generation,
        &'a CancellationToken,
    ) -> DFF,
    DFF: Future<Output = Result<(T, Generation, SystemTime), DownloadError>>,
    PF: Fn(RemotePath) -> Option<Generation>,
    T: 'static,
{
    debug_assert_current_span_has_tenant_id();

    if my_generation.is_none() {
        // Operating without generations: just fetch the generation-less path
        return do_download(storage, tenant_shard_id, timeline_id, my_generation, cancel).await;
    }

    // Stale case: If we were intentionally attached in a stale generation, the remote object may already
    // exist in our generation.
    //
    // This is an optimization to avoid doing the listing for the general case below.
    let res = do_download(storage, tenant_shard_id, timeline_id, my_generation, cancel).await;
    match res {
        Ok(decoded) => {
            tracing::debug!("Found {what} from current generation (this is a stale attachment)");
            return Ok(decoded);
        }
        Err(DownloadError::NotFound) => {}
        Err(e) => return Err(e),
    };

    // Typical case: the previous generation of this tenant was running healthily, and had uploaded the object
    // we are seeking in that generation.  We may safely start from this index without doing a listing, because:
    //  - We checked for current generation case above
    //  - generations > my_generation are to be ignored
    //  - any other objects that exist would have an older generation than `previous_gen`, and
    //    we want to find the most recent object from a previous generation.
    //
    // This is an optimization to avoid doing the listing for the general case below.
    let res = do_download(
        storage,
        tenant_shard_id,
        timeline_id,
        my_generation.previous(),
        cancel,
    )
    .await;
    match res {
        Ok(decoded) => {
            tracing::debug!("Found {what} from previous generation");
            return Ok(decoded);
        }
        Err(DownloadError::NotFound) => {
            tracing::debug!("No {what} found from previous generation, falling back to listing");
        }
        Err(e) => {
            return Err(e);
        }
    }

    // General case/fallback: if there is no index at my_generation or prev_generation, then list all index_part.json
    // objects, and select the highest one with a generation <= my_generation.  Constructing the prefix is equivalent
    // to constructing a full index path with no generation, because the generation is a suffix.
    let paths = download_retry(
        || async {
            storage
                .list(Some(&prefix), ListingMode::NoDelimiter, None, cancel)
                .await
        },
        "list index_part files",
        cancel,
    )
    .await?
    .keys;

    // General case logic for which index to use: the latest index whose generation
    // is <= our own.  See "Finding the remote indices for timelines" in docs/rfcs/025-generation-numbers.md
    let max_previous_generation = paths
        .into_iter()
        .filter_map(|o| parse_path(o.key))
        .filter(|g| g <= &my_generation)
        .max();

    match max_previous_generation {
        Some(g) => {
            tracing::debug!("Found {what} in generation {g:?}");
            do_download(storage, tenant_shard_id, timeline_id, g, cancel).await
        }
        None => {
            // Migration from legacy pre-generation state: we have a generation but no prior
            // attached pageservers did.  Try to load from a no-generation path.
            tracing::debug!("No {what}* found");
            do_download(
                storage,
                tenant_shard_id,
                timeline_id,
                Generation::none(),
                cancel,
            )
            .await
        }
    }
}

/// index_part.json objects are suffixed with a generation number, so we cannot
/// directly GET the latest index part without doing some probing.
///
/// In this function we probe for the most recent index in a generation <= our current generation.
/// See "Finding the remote indices for timelines" in docs/rfcs/025-generation-numbers.md
pub(crate) async fn download_index_part(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    my_generation: Generation,
    cancel: &CancellationToken,
) -> Result<(IndexPart, Generation, SystemTime), DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let index_prefix = remote_index_path(tenant_shard_id, timeline_id, Generation::none());
    download_generation_object(
        storage,
        tenant_shard_id,
        Some(timeline_id),
        my_generation,
        "index_part",
        index_prefix,
        do_download_index_part,
        parse_remote_index_path,
        cancel,
    )
    .await
}

pub(crate) async fn download_tenant_manifest(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    my_generation: Generation,
    cancel: &CancellationToken,
) -> Result<(TenantManifest, Generation, SystemTime), DownloadError> {
    let manifest_prefix = remote_tenant_manifest_prefix(tenant_shard_id);

    download_generation_object(
        storage,
        tenant_shard_id,
        None,
        my_generation,
        "tenant-manifest",
        manifest_prefix,
        do_download_tenant_manifest,
        parse_remote_tenant_manifest_path,
        cancel,
    )
    .await
}

pub(crate) async fn download_initdb_tar_zst(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    cancel: &CancellationToken,
) -> Result<(Utf8PathBuf, File), DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let remote_path = remote_initdb_archive_path(&tenant_shard_id.tenant_id, timeline_id);

    let remote_preserved_path =
        remote_initdb_preserved_archive_path(&tenant_shard_id.tenant_id, timeline_id);

    let timeline_path = conf.timelines_path(tenant_shard_id);

    if !timeline_path.exists() {
        tokio::fs::create_dir_all(&timeline_path)
            .await
            .with_context(|| format!("timeline dir creation {timeline_path}"))
            .map_err(DownloadError::Other)?;
    }
    let temp_path = timeline_path.join(format!(
        "{INITDB_PATH}.download-{timeline_id}.{TEMP_FILE_SUFFIX}"
    ));

    let file = download_retry(
        || async {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&temp_path)
                .await
                .with_context(|| format!("tempfile creation {temp_path}"))
                .map_err(DownloadError::Other)?;

            let download = match storage
                .download(&remote_path, &DownloadOpts::default(), cancel)
                .await
            {
                Ok(dl) => dl,
                Err(DownloadError::NotFound) => {
                    storage
                        .download(&remote_preserved_path, &DownloadOpts::default(), cancel)
                        .await?
                }
                Err(other) => Err(other)?,
            };
            let mut download = tokio_util::io::StreamReader::new(download.download_stream);
            let mut writer = tokio::io::BufWriter::with_capacity(super::BUFFER_SIZE, file);

            tokio::io::copy_buf(&mut download, &mut writer).await?;

            let mut file = writer.into_inner();

            file.seek(std::io::SeekFrom::Start(0))
                .await
                .with_context(|| format!("rewinding initdb.tar.zst at: {remote_path:?}"))
                .map_err(DownloadError::Other)?;

            Ok(file)
        },
        &format!("download {remote_path}"),
        cancel,
    )
    .await
    .inspect_err(|_e| {
        // Do a best-effort attempt at deleting the temporary file upon encountering an error.
        // We don't have async here nor do we want to pile on any extra errors.
        if let Err(e) = std::fs::remove_file(&temp_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("error deleting temporary file {temp_path}: {e}");
            }
        }
    })?;

    Ok((temp_path, file))
}

/// Helper function to handle retries for a download operation.
///
/// Remote operations can fail due to rate limits (S3), spurious network
/// problems, or other external reasons. Retry FAILED_DOWNLOAD_RETRIES times,
/// with backoff.
///
/// (See similar logic for uploads in `perform_upload_task`)
pub(super) async fn download_retry<T, O, F>(
    op: O,
    description: &str,
    cancel: &CancellationToken,
) -> Result<T, DownloadError>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, DownloadError>>,
{
    backoff::retry(
        op,
        DownloadError::is_permanent,
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        description,
        cancel,
    )
    .await
    .ok_or_else(|| DownloadError::Cancelled)
    .and_then(|x| x)
}

pub(crate) async fn download_retry_forever<T, O, F>(
    op: O,
    description: &str,
    cancel: &CancellationToken,
) -> Result<T, DownloadError>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, DownloadError>>,
{
    backoff::retry(
        op,
        DownloadError::is_permanent,
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        u32::MAX,
        description,
        cancel,
    )
    .await
    .ok_or_else(|| DownloadError::Cancelled)
    .and_then(|x| x)
}
