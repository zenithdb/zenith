use std::sync::Arc;

use pageserver_api::models::{TenantState, TimelineState};

use super::delete::{delete_local_timeline_directory, DeleteTimelineFlow, DeletionGuard};
use super::Timeline;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::ShutdownIfArchivedError;
use crate::tenant::{OffloadedTimeline, Tenant, TenantManifestError, TimelineOrOffloaded};

#[derive(thiserror::Error, Debug)]
pub(crate) enum OffloadError {
    #[error("Cancelled")]
    Cancelled,
    #[error("Timeline is not archived")]
    NotArchived,
    #[error(transparent)]
    RemoteStorage(anyhow::Error),
    #[error("Unexpected offload error: {0}")]
    Other(anyhow::Error),
}

impl From<TenantManifestError> for OffloadError {
    fn from(e: TenantManifestError) -> Self {
        match e {
            TenantManifestError::Cancelled => Self::Cancelled,
            TenantManifestError::RemoteStorage(e) => Self::RemoteStorage(e),
        }
    }
}

pub(crate) async fn offload_timeline(
    tenant: &Tenant,
    timeline: &Arc<Timeline>,
) -> Result<(), OffloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();
    tracing::info!("offloading archived timeline");

    let allow_offloaded_children = true;
    let set_stopping = false;
    let (timeline, guard) = DeleteTimelineFlow::prepare(
        tenant,
        timeline.timeline_id,
        allow_offloaded_children,
        set_stopping,
    )
    .map_err(|e| OffloadError::Other(anyhow::anyhow!(e)))?;

    let TimelineOrOffloaded::Timeline(timeline) = timeline else {
        tracing::error!("timeline already offloaded, but given timeline object");
        return Ok(());
    };

    match timeline.remote_client.shutdown_if_archived().await {
        Ok(()) => {}
        Err(ShutdownIfArchivedError::NotInitialized(_)) => {
            // Either the timeline is being deleted, the operation is being retried, or we are shutting down.
            // Don't return cancelled here to keep it idempotent.
        }
        Err(ShutdownIfArchivedError::NotArchived) => return Err(OffloadError::NotArchived),
    }
    timeline.set_state(TimelineState::Stopping);

    // Now that the Timeline is in Stopping state, request all the related tasks to shut down.
    timeline.shutdown(super::ShutdownMode::Reload).await;

    // TODO extend guard mechanism above with method
    // to make deletions possible while offloading is in progress

    let conf = &tenant.conf;
    delete_local_timeline_directory(conf, tenant.tenant_shard_id, &timeline).await;

    let remaining_refcount = remove_timeline_from_tenant(tenant, &timeline, &guard);

    {
        let mut offloaded_timelines = tenant.timelines_offloaded.lock().unwrap();
        if matches!(
            tenant.current_state(),
            TenantState::Stopping { .. } | TenantState::Broken { .. }
        ) {
            // Cancel the operation if the tenant is shutting down. Do this while the
            // timelines_offloaded lock is held to prevent a race with Tenant::shutdown
            // for defusing the lock
            return Err(OffloadError::Cancelled);
        }
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
    tenant.store_tenant_manifest().await?;

    tracing::info!("Timeline offload complete (remaining arc refcount: {remaining_refcount})");

    Ok(())
}

/// It is important that this gets called when DeletionGuard is being held.
/// For more context see comments in [`DeleteTimelineFlow::prepare`]
///
/// Returns the strong count of the timeline `Arc`
fn remove_timeline_from_tenant(
    tenant: &Tenant,
    timeline: &Timeline,
    _: &DeletionGuard, // using it as a witness
) -> usize {
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

    let timeline = timelines
        .remove(&timeline.timeline_id)
        .expect("timeline that we were deleting was concurrently removed from 'timelines' map");

    Arc::strong_count(&timeline)
}
