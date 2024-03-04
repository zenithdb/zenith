use super::super::super::index::LayerFileMetadata;
use super::super::download_retry;
use super::super::TEMP_DOWNLOAD_EXTENSION;
use crate::config::PageServerConf;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::remote_timeline_client::BUFFER_SIZE;
use crate::tenant::storage_layer::LayerFileName;
use crate::virtual_file::{on_fatal_io_error, MaybeFatalIo, VirtualFile};
use anyhow::anyhow;
use anyhow::Context;
use bytes::BytesMut;
use futures::StreamExt;
use pageserver_api::shard::TenantShardId;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use tokio::fs;
use tokio_util::sync::CancellationToken;
use utils::crashsafe::path_with_suffix_extension;
use utils::id::TimelineId;

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
pub(crate) async fn download_layer_file<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    layer_file_name: &'a LayerFileName,
    layer_metadata: &'a LayerFileMetadata,
    cancel: &CancellationToken,
) -> Result<u64, DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let timeline_path = conf.timeline_path(&tenant_shard_id, &timeline_id);
    let local_path = timeline_path.join(layer_file_name.file_name());

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
    let temp_file_path = path_with_suffix_extension(&local_path, TEMP_DOWNLOAD_EXTENSION);

    let (destination_file, bytes_amount) = download_retry(
        || async {
            let mut destination_file = VirtualFile::create(&temp_file_path)
                .await
                .with_context(|| format!("create a destination file for layer '{temp_file_path}'"))
                .map_err(DownloadError::Other)?;

            let mut download = storage.download(&remote_path, cancel).await?;

            // This async block is the tokio-epoll-uring version of tokio::io::copy_buf
            // TODO: abstract away & unit test.
            let bytes_amount = async {
                // TODO: use vectored write (writev) once supported by tokio-epoll-uring.
                // There's chunks_vectored()
                let mut buf = BytesMut::with_capacity(*BUFFER_SIZE);
                let mut bytes_amount: u64 = 0;
                while let Some(chunk) = download.download_stream.next().await {
                    let mut chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(e) => return Err(e),
                    };
                    // pad previous iteration's `buf` with head of this chunk
                    {
                        let have = chunk.len();
                        let need = *BUFFER_SIZE - buf.len();
                        let n = std::cmp::min(need, have);
                        let head = chunk.split_to(n);
                        buf.extend_from_slice(&head);
                        if buf.len() >= *BUFFER_SIZE {
                            assert_eq!(buf.len(), *BUFFER_SIZE);
                            {
                                // flush buf
                                let res;
                                let buf_pre_write = buf.len();
                                (buf, res) = destination_file.write_all(buf).await;
                                let nwritten = res?;
                                assert_eq!(nwritten, buf_pre_write);
                                buf.clear();
                                bytes_amount += u64::try_from(nwritten).unwrap();
                            }
                            // fallthrough
                        } else {
                            assert_eq!(chunk.len(), 0, "if the chunk wasn't sufficient to fill `buf`, it should be empty now");
                            continue; // next chunk
                        }
                    }

                    // avoid memcpy for the middle of the chunk
                    while chunk.len() >= *BUFFER_SIZE {
                        let mut head = chunk.split_to(*BUFFER_SIZE);
                        {
                            // flush head
                            let res;
                            let buf_pre_write = head.len();
                            (head, res) = destination_file.write_all(head).await;
                            let nwritten = res?;
                            assert_eq!(nwritten, buf_pre_write);
                            bytes_amount += u64::try_from(nwritten).unwrap();
                        }
                        drop(head);
                    }
                    // in-memory copy the < BUFFER_SIZED tail of the chunk
                    assert!(chunk.len() < *BUFFER_SIZE);
                    let mut chunk = &chunk[..];
                    while !chunk.is_empty() {
                        let need = *BUFFER_SIZE - buf.len();
                        let have = chunk.len();
                        let n = std::cmp::min(need, have);
                        buf.extend_from_slice(&chunk[..n]);
                        chunk = &chunk[n..];
                        if buf.len() >= *BUFFER_SIZE {
                            assert_eq!(buf.len(), *BUFFER_SIZE);
                            {
                                // flush buf
                                let res;
                                let buf_pre_write = buf.len();
                                (buf, res) = destination_file.write_all(buf).await;
                                let nwritten = res?;
                                assert_eq!(nwritten, buf_pre_write);
                                buf.clear();
                                bytes_amount += u64::try_from(nwritten).unwrap();
                            }
                        }
                    }
                    assert!(chunk.is_empty(), "by now we should have drained the chunk");
                }
                {
                    // flush buf
                    let res;
                    let buf_pre_write = buf.len();
                    (buf, res) = destination_file.write_all(buf).await;
                    let nwritten = res?;
                    assert_eq!(nwritten, buf_pre_write);
                    buf.clear();
                    bytes_amount += u64::try_from(nwritten).unwrap();
                }
                Ok(bytes_amount)
            }
            .await;

            match bytes_amount {
                Ok(bytes_amount) => Ok((destination_file, bytes_amount)),
                Err(e) => {
                    if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
                        on_fatal_io_error(&e, &format!("Removing temporary file {temp_file_path}"));
                    }
                    Err(e.into())
                }
            }
        },
        &format!("download {remote_path:?}"),
        cancel,
    )
    .await?;

    let expected = layer_metadata.file_size();
    if expected != bytes_amount {
        return Err(DownloadError::Other(anyhow!(
            "According to layer file metadata should have downloaded {expected} bytes but downloaded {bytes_amount} bytes into file {temp_file_path:?}",
        )));
    }

    // not using sync_data because it can lose file size update
    destination_file
        .sync_all()
        .await
        .with_context(|| format!("failed to fsync source file at {temp_file_path}"))
        .map_err(DownloadError::Other)?;
    drop(destination_file);

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
    let work = async move {
        let timeline_dir = VirtualFile::open(&timeline_path)
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
        timeline_dir
            .sync_all()
            .await
            .fatal_err("VirtualFile::sync_all timeline dir");
    };
    crate::virtual_file::io_engine::get()
        .spawn_blocking_and_block_on_if_std(work)
        .await;

    tracing::debug!("download complete: {local_path}");

    Ok(bytes_amount)
}
