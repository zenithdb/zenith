//! Helper functions to upload files to remote storage with a RemoteStorage

use anyhow::{bail, Context};
use std::path::Path;
use tokio::fs;

use super::index::IndexPart;
use crate::config::PageServerConf;
use crate::fail_point;
use crate::storage_sync::LayerFileMetadata;
use remote_storage::GenericRemoteStorage;
use utils::id::{TenantId, TimelineId};

/// Serializes and uploads the given index part data to the remote storage.
pub(super) async fn upload_index_part<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    index_part: &'a IndexPart,
) -> anyhow::Result<()> {
    fail_point!("before-upload-index", |_| {
        bail!("failpoint before-upload-index")
    });
    let index_part_bytes = serde_json::to_vec(&index_part)
        .context("Failed to serialize index part file into bytes")?;
    let index_part_size = index_part_bytes.len();
    let index_part_bytes = tokio::io::BufReader::new(std::io::Cursor::new(index_part_bytes));

    let index_part_path = conf
        .metadata_path(timeline_id, tenant_id)
        .with_file_name(IndexPart::FILE_NAME);
    let storage_path = conf.remote_path(&index_part_path)?;
    storage
        .upload_storage_object(Box::new(index_part_bytes), index_part_size, &storage_path)
        .await
        .with_context(|| format!("Failed to upload index part for '{tenant_id} / {timeline_id}'"))
}

/// Attempts to upload given layer files.
/// No extra checks for overlapping files is made and any files that are already present remotely will be overwritten, if submitted during the upload.
///
/// On an error, bumps the retries count and reschedules the entire task.
pub(super) async fn upload_timeline_layer<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    source_path: &'a Path,
    known_metadata: &'a LayerFileMetadata,
) -> anyhow::Result<()> {
    fail_point!("before-upload-layer", |_| {
        bail!("failpoint before-upload-layer")
    });
    let storage_path = conf.remote_path(source_path)?;

    let source_file = fs::File::open(&source_path)
        .await
        .with_context(|| format!("Failed to open a source file for layer {source_path:?}"))?;

    let fs_size = source_file
        .metadata()
        .await
        .with_context(|| {
            format!("Failed to get the source file metadata for layer {source_path:?}")
        })?
        .len();

    // FIXME: this looks bad
    if let Some(metadata_size) = known_metadata.file_size() {
        if metadata_size != fs_size {
            bail!("File {source_path:?} has its current FS size {fs_size} diferent from initially determined {metadata_size}");
        }
    } else {
        // this is a silly state we would like to avoid
    }

    let fs_size = usize::try_from(fs_size).with_context(|| {
        format!("File {source_path:?} size {fs_size} could not be converted to usize")
    })?;

    storage
        .upload(Box::new(source_file), fs_size, &storage_path, None)
        .await
        .with_context(|| {
            format!(
                "Failed to upload a layer from local path '{}'",
                source_path.display()
            )
        })?;

    Ok(())
}
