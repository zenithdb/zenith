use std::sync::Arc;

use utils::generation::Generation;

use super::delete::{delete_local_timeline_directory, DeleteTimelineFlow, DeletionGuard};
use super::Timeline;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::{remote_timeline_client, OffloadedTimeline, Tenant, TimelineOrOffloaded};

pub(crate) async fn offload_timeline(
    tenant: &Tenant,
    timeline: &Arc<Timeline>,
) -> anyhow::Result<()> {
    debug_assert_current_span_has_tenant_and_timeline_id();
    tracing::info!("offloading archived timeline");

    let (timeline, guard) = DeleteTimelineFlow::prepare(tenant, timeline.timeline_id)?;

    let TimelineOrOffloaded::Timeline(timeline) = timeline else {
        tracing::error!("timeline already offloaded, but given timeline object");
        return Ok(());
    };

    let is_archived = timeline.is_archived();
    match is_archived {
        Some(true) => (),
        Some(false) => {
            tracing::warn!(?is_archived, "tried offloading a non-archived timeline");
            anyhow::bail!("timeline isn't archived");
        }
        None => {
            tracing::warn!(
                ?is_archived,
                "tried offloading a timeline where manifest is not yet available"
            );
            anyhow::bail!("timeline manifest hasn't been loaded yet");
        }
    }

    // Now that the Timeline is in Stopping state, request all the related tasks to shut down.
    timeline.shutdown(super::ShutdownMode::Hard).await;

    // TODO extend guard mechanism above with method
    // to make deletions possible while offloading is in progress

    let conf = &tenant.conf;
    delete_local_timeline_directory(conf, tenant.tenant_shard_id, &timeline).await?;

    remove_timeline_from_tenant(tenant, &timeline, &guard).await?;

    {
        let mut offloaded_timelines = tenant.timelines_offloaded.lock().unwrap();
        offloaded_timelines.insert(
            timeline.timeline_id,
            Arc::new(
                OffloadedTimeline::from_timeline(&timeline)
                    .expect("we checked above that timeline was ready"),
            ),
        );
    }

    // Last step: mark timeline as offloaded in S3
    // TODO: maybe move this step above, right above deletion of the local timeline directory,
    // then there is no potential race condition where we partially offload a timeline, and
    // at the next restart attach it again.
    // For that to happen, we'd need to make the manifest reflect our *intended* state,
    // not our actual state of offloaded timelines.
    let manifest = tenant.tenant_manifest();
    // TODO: generation support
    let generation = Generation::none();
    remote_timeline_client::upload_tenant_manifest(
        &tenant.remote_storage,
        &tenant.tenant_shard_id,
        generation,
        &manifest,
        &tenant.cancel,
    )
    .await?;

    Ok(())
}

/// It is important that this gets called when DeletionGuard is being held.
/// For more context see comments in [`DeleteTimelineFlow::prepare`]
async fn remove_timeline_from_tenant(
    tenant: &Tenant,
    timeline: &Timeline,
    _: &DeletionGuard, // using it as a witness
) -> anyhow::Result<()> {
    // Remove the timeline from the map.
    let mut timelines = tenant.timelines.lock().unwrap();
    let children_exist = timelines
        .iter()
        .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline.timeline_id));
    // XXX this can happen because `branch_timeline` doesn't check `TimelineState::Stopping`.
    // We already deleted the layer files, so it's probably best to panic.
    // (Ideally, above remove_dir_all is atomic so we don't see this timeline after a restart)
    if children_exist {
        panic!("Timeline grew children while we removed layer files");
    }

    timelines
        .remove(&timeline.timeline_id)
        .expect("timeline that we were deleting was concurrently removed from 'timelines' map");

    drop(timelines);

    Ok(())
}
