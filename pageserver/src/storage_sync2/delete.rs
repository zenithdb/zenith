//! Helper functions to delete files from remote storage with a RemoteStorage
use anyhow::Context;
use std::path::Path;
use tracing::debug;

use remote_storage::GenericRemoteStorage;

use crate::config::PageServerConf;

pub(super) async fn delete_layer<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    local_layer_path: &'a Path,
) -> anyhow::Result<()> {
    crate::fail_point!("before-delete-layer", |_| {
        anyhow::bail!("failpoint before-delete-layer")
    });
    debug!("Deleting layer from remote storage: {local_layer_path:?}",);

    let path_to_delete = conf.remote_path(local_layer_path)?;

    // XXX: If the deletion fails because the object already didn't exist,
    // it would be good to just issue a warning but consider it success.
    // https://github.com/neondatabase/neon/issues/2934
    storage.delete(&path_to_delete).await.with_context(|| {
        format!("Failed to delete remote layer from storage at {path_to_delete:?}")
    })
}
