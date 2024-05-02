use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use utils::{generation::Generation, lsn::Lsn};

use super::{layer_manager::LayerManager, DetachFromAncestorError, Timeline};
use crate::{
    context::RequestContext,
    tenant::storage_layer::{AsLayerDesc as _, DeltaLayerWriter, Layer, ResidentLayer},
};

pub(super) fn partition_work(
    ancestor_lsn: Lsn,
    source_layermap: &LayerManager,
) -> (usize, Vec<Layer>, Vec<Layer>) {
    let mut straddling_branchpoint = vec![];
    let mut rest_of_historic = vec![];

    let mut later_by_lsn = 0;

    for desc in source_layermap.layer_map().iter_historic_layers() {
        // off by one chances here:
        // - start is inclusive
        // - end is exclusive
        if desc.lsn_range.start > ancestor_lsn {
            later_by_lsn += 1;
            continue;
        }

        let target = if desc.lsn_range.start <= ancestor_lsn
            && desc.lsn_range.end > ancestor_lsn
            && desc.is_delta
        {
            // TODO: image layer at Lsn optimization
            &mut straddling_branchpoint
        } else {
            &mut rest_of_historic
        };

        target.push(source_layermap.get_from_desc(&desc));
    }

    (later_by_lsn, straddling_branchpoint, rest_of_historic)
}

pub(super) async fn upload_rewritten_layer(
    end_lsn: Lsn,
    layer: &Layer,
    target: &Arc<Timeline>,
    cancel: &CancellationToken,
    ctx: &RequestContext,
) -> Result<Option<Layer>, DetachFromAncestorError> {
    use DetachFromAncestorError::UploadRewritten;
    let copied = copy_lsn_prefix(end_lsn, layer, target, ctx).await?;

    let Some(copied) = copied else {
        return Ok(None);
    };

    // FIXME: better shuttingdown error
    target
        .remote_client
        .as_ref()
        .unwrap()
        .upload_layer_file(&copied, cancel)
        .await
        .map_err(UploadRewritten)?;

    Ok(Some(copied.into()))
}

async fn copy_lsn_prefix(
    end_lsn: Lsn,
    layer: &Layer,
    target_timeline: &Arc<Timeline>,
    ctx: &RequestContext,
) -> Result<Option<ResidentLayer>, DetachFromAncestorError> {
    use DetachFromAncestorError::{CopyDeltaPrefix, RewrittenDeltaDownloadFailed};

    tracing::debug!(%layer, %end_lsn, "copying lsn prefix");

    let mut writer = DeltaLayerWriter::new(
        target_timeline.conf,
        target_timeline.timeline_id,
        target_timeline.tenant_shard_id,
        layer.layer_desc().key_range.start,
        layer.layer_desc().lsn_range.start..end_lsn,
    )
    .await
    .map_err(CopyDeltaPrefix)?;

    let resident = layer
        .download_and_keep_resident()
        .await
        // likely shutdown
        .map_err(RewrittenDeltaDownloadFailed)?;

    let records = resident
        .copy_delta_prefix(&mut writer, end_lsn, ctx)
        .await
        .map_err(CopyDeltaPrefix)?;

    drop(resident);

    tracing::debug!(%layer, records, "copied records");

    if records == 0 {
        drop(writer);
        // TODO: we might want to store an empty marker in remote storage for this
        // layer so that we will not needlessly walk `layer` on repeated attempts.
        Ok(None)
    } else {
        // reuse the key instead of adding more holes between layers by using the real
        // highest key in the layer.
        let reused_highest_key = layer.layer_desc().key_range.end;
        let copied = writer
            .finish(reused_highest_key, target_timeline)
            .await
            .map_err(CopyDeltaPrefix)?;

        tracing::debug!(%layer, %copied, "new layer produced");

        Ok(Some(copied))
    }
}

/// Creates a new Layer instance for the adopted layer, and ensures it is found from the remote
/// storage on successful return without the adopted layer being added to `index_part.json`.
pub(super) async fn remote_copy(
    adopted: &Layer,
    adoptee: &Arc<Timeline>,
    generation: Generation,
    cancel: &CancellationToken,
) -> Result<Layer, DetachFromAncestorError> {
    use DetachFromAncestorError::CopyFailed;

    // depending if Layer::keep_resident we could hardlink

    let mut metadata = adopted.metadata();
    debug_assert!(metadata.generation <= generation);
    metadata.generation = generation;

    let owned = crate::tenant::storage_layer::Layer::for_evicted(
        adoptee.conf,
        adoptee,
        adopted.layer_desc().filename(),
        metadata,
    );

    // FIXME: better shuttingdown error
    adoptee
        .remote_client
        .as_ref()
        .unwrap()
        .copy_timeline_layer(adopted, &owned, cancel)
        .await
        .map(move |()| owned)
        .map_err(CopyFailed)
}
