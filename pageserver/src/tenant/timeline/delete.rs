use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use anyhow::Context;
use pageserver_api::{models::TimelineState, shard::TenantShardId};
use remote_storage::DownloadError;
use tokio::sync::OwnedMutexGuard;
use tracing::{error, info, info_span, instrument, Instrument};
use utils::{crashsafe, fs_ext, id::TimelineId, pausable_failpoint};

use crate::{
    config::PageServerConf,
    task_mgr::{self, TaskKind},
    tenant::{
        metadata::TimelineMetadata,
        remote_timeline_client::{PersistIndexPartWithDeletedFlagError, RemoteTimelineClient},
        CreateTimelineCause, DeleteTimelineError, MaybeDeletedIndexPart, Tenant,
        TenantManifestError, TimelineOrOffloaded,
    },
    virtual_file::MaybeFatalIo,
};

use super::{Timeline, TimelineResources};

/// Mark timeline as deleted in S3 so we won't pick it up next time
/// during attach or pageserver restart.
/// See comment in persist_index_part_with_deleted_flag.
async fn set_deleted_in_remote_index(
    remote_client: &Arc<RemoteTimelineClient>,
) -> Result<(), DeleteTimelineError> {
    let res = remote_client.persist_index_part_with_deleted_flag().await;
    match res {
        // If we (now, or already) marked it successfully as deleted, we can proceed
        Ok(()) | Err(PersistIndexPartWithDeletedFlagError::AlreadyDeleted(_)) => (),
        // Bail out otherwise
        //
        // AlreadyInProgress shouldn't happen, because the 'delete_lock' prevents
        // two tasks from performing the deletion at the same time. The first task
        // that starts deletion should run it to completion.
        Err(e @ PersistIndexPartWithDeletedFlagError::AlreadyInProgress(_))
        | Err(e @ PersistIndexPartWithDeletedFlagError::Other(_)) => {
            return Err(DeleteTimelineError::Other(anyhow::anyhow!(e)));
        }
    }
    Ok(())
}

/// Grab the compaction and gc locks, and actually perform the deletion.
///
/// The locks prevent GC or compaction from running at the same time. The background tasks do not
/// register themselves with the timeline it's operating on, so it might still be running even
/// though we called `shutdown_tasks`.
///
/// Note that there are still other race conditions between
/// GC, compaction and timeline deletion. See
/// <https://github.com/neondatabase/neon/issues/2671>
///
/// No timeout here, GC & Compaction should be responsive to the
/// `TimelineState::Stopping` change.
// pub(super): documentation link
pub(super) async fn delete_local_timeline_directory(
    conf: &PageServerConf,
    tenant_shard_id: TenantShardId,
    timeline: &Timeline,
) {
    // Always ensure the lock order is compaction -> gc.
    let compaction_lock = timeline.compaction_lock.lock();
    let _compaction_lock = crate::timed(
        compaction_lock,
        "acquires compaction lock",
        std::time::Duration::from_secs(5),
    )
    .await;

    let gc_lock = timeline.gc_lock.lock();
    let _gc_lock = crate::timed(
        gc_lock,
        "acquires gc lock",
        std::time::Duration::from_secs(5),
    )
    .await;

    // NB: storage_sync upload tasks that reference these layers have been cancelled
    //     by the caller.

    let local_timeline_directory = conf.timeline_path(&tenant_shard_id, &timeline.timeline_id);

    // NB: This need not be atomic because the deleted flag in the IndexPart
    // will be observed during tenant/timeline load. The deletion will be resumed there.
    //
    // ErrorKind::NotFound can happen e.g. if we race with tenant detach, because,
    // no locks are shared.
    tokio::fs::remove_dir_all(local_timeline_directory)
        .await
        .or_else(fs_ext::ignore_not_found)
        .fatal_err("removing timeline directory");

    // Make sure previous deletions are ordered before mark removal.
    // Otherwise there is no guarantee that they reach the disk before mark deletion.
    // So its possible for mark to reach disk first and for other deletions
    // to be reordered later and thus missed if a crash occurs.
    // Note that we dont need to sync after mark file is removed
    // because we can tolerate the case when mark file reappears on startup.
    let timeline_path = conf.timelines_path(&tenant_shard_id);
    crashsafe::fsync_async(timeline_path)
        .await
        .fatal_err("fsync after removing timeline directory");

    info!("finished deleting layer files, releasing locks");
}

/// It is important that this gets called when DeletionGuard is being held.
/// For more context see comments in [`DeleteTimelineFlow::prepare`]
async fn remove_maybe_offloaded_timeline_from_tenant(
    tenant: &Tenant,
    timeline: &TimelineOrOffloaded,
    _: &DeletionGuard, // using it as a witness
) -> anyhow::Result<()> {
    // Remove the timeline from the map.
    // This observes the locking order between timelines and timelines_offloaded
    let mut timelines = tenant.timelines.lock().unwrap();
    let mut timelines_offloaded = tenant.timelines_offloaded.lock().unwrap();
    let offloaded_children_exist = timelines_offloaded
        .iter()
        .any(|(_, entry)| entry.ancestor_timeline_id == Some(timeline.timeline_id()));
    let children_exist = timelines
        .iter()
        .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline.timeline_id()));
    // XXX this can happen because of race conditions with branch creation.
    // We already deleted the remote layer files, so it's probably best to panic.
    if children_exist || offloaded_children_exist {
        panic!("Timeline grew children while we removed layer files");
    }

    match timeline {
        TimelineOrOffloaded::Timeline(timeline) => {
            timelines.remove(&timeline.timeline_id).expect(
                "timeline that we were deleting was concurrently removed from 'timelines' map",
            );
        }
        TimelineOrOffloaded::Offloaded(timeline) => {
            let offloaded_timeline = timelines_offloaded
                .remove(&timeline.timeline_id)
                .expect("timeline that we were deleting was concurrently removed from 'timelines_offloaded' map");
            offloaded_timeline.delete_from_ancestor_with_timelines(&timelines);
        }
    }

    drop(timelines_offloaded);
    drop(timelines);

    Ok(())
}

/// Orchestrates timeline shut down of all timeline tasks, removes its in-memory structures,
/// and deletes its data from both disk and s3.
/// The sequence of steps:
/// 1. Set deleted_at in remote index part.
/// 2. Create local mark file.
/// 3. Delete local files except metadata (it is simpler this way, to be able to reuse timeline initialization code that expects metadata)
/// 4. Delete remote layers
/// 5. Delete index part
/// 6. Delete meta, timeline directory
/// 7. Delete mark file
///
/// It is resumable from any step in case a crash/restart occurs.
/// There are two entrypoints to the process:
/// 1. [`DeleteTimelineFlow::run`] this is the main one called by a management api handler.
/// 2. [`DeleteTimelineFlow::resume_deletion`] is called during restarts when local metadata is still present
///    and we possibly neeed to continue deletion of remote files.
///
/// Note the only other place that messes around timeline delete mark is the logic that scans directory with timelines during tenant load.
#[derive(Default)]
pub enum DeleteTimelineFlow {
    #[default]
    NotStarted,
    InProgress,
    Finished,
}

impl DeleteTimelineFlow {
    // These steps are run in the context of management api request handler.
    // Long running steps are continued to run in the background.
    // NB: If this fails half-way through, and is retried, the retry will go through
    // all the same steps again. Make sure the code here is idempotent, and don't
    // error out if some of the shutdown tasks have already been completed!
    #[instrument(skip_all)]
    pub async fn run(
        tenant: &Arc<Tenant>,
        timeline_id: TimelineId,
    ) -> Result<(), DeleteTimelineError> {
        super::debug_assert_current_span_has_tenant_and_timeline_id();

        let allow_offloaded_children = false;
        let set_stopping = true;
        let (timeline, mut guard) =
            Self::prepare(tenant, timeline_id, allow_offloaded_children, set_stopping)?;

        guard.mark_in_progress()?;

        // Now that the Timeline is in Stopping state, request all the related tasks to shut down.
        if let TimelineOrOffloaded::Timeline(timeline) = &timeline {
            timeline.shutdown(super::ShutdownMode::Hard).await;
        }

        tenant.gc_block.before_delete(&timeline.timeline_id());

        fail::fail_point!("timeline-delete-before-index-deleted-at", |_| {
            Err(anyhow::anyhow!(
                "failpoint: timeline-delete-before-index-deleted-at"
            ))?
        });

        let remote_client = match timeline.maybe_remote_client() {
            Some(remote_client) => remote_client,
            None => {
                let remote_client = tenant
                    .build_timeline_client(timeline.timeline_id(), tenant.remote_storage.clone());
                let result = match remote_client
                    .download_index_file(&tenant.cancel)
                    .instrument(info_span!("download_index_file"))
                    .await
                {
                    Ok(r) => r,
                    Err(DownloadError::NotFound) => {
                        // Deletion is already complete
                        tracing::info!("Timeline already deleted in remote storage");
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(DeleteTimelineError::Other(anyhow::anyhow!(
                            "error: {:?}",
                            e
                        )));
                    }
                };
                let index_part = match result {
                    MaybeDeletedIndexPart::Deleted(p) => {
                        tracing::info!("Timeline already set as deleted in remote index");
                        p
                    }
                    MaybeDeletedIndexPart::IndexPart(p) => p,
                };
                let remote_client = Arc::new(remote_client);

                remote_client
                    .init_upload_queue(&index_part)
                    .map_err(DeleteTimelineError::Other)?;
                remote_client.shutdown().await;
                remote_client
            }
        };
        set_deleted_in_remote_index(&remote_client).await?;

        fail::fail_point!("timeline-delete-before-schedule", |_| {
            Err(anyhow::anyhow!(
                "failpoint: timeline-delete-before-schedule"
            ))?
        });

        Self::schedule_background(
            guard,
            tenant.conf,
            Arc::clone(tenant),
            timeline,
            remote_client,
        );

        Ok(())
    }

    fn mark_in_progress(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Finished => anyhow::bail!("Bug. Is in finished state"),
            Self::InProgress { .. } => { /* We're in a retry */ }
            Self::NotStarted => { /* Fresh start */ }
        }

        *self = Self::InProgress;

        Ok(())
    }

    /// Shortcut to create Timeline in stopping state and spawn deletion task.
    #[instrument(skip_all, fields(%timeline_id))]
    pub(crate) async fn resume_deletion(
        tenant: Arc<Tenant>,
        timeline_id: TimelineId,
        local_metadata: &TimelineMetadata,
        remote_client: RemoteTimelineClient,
    ) -> anyhow::Result<()> {
        // Note: here we even skip populating layer map. Timeline is essentially uninitialized.
        // RemoteTimelineClient is the only functioning part.
        let timeline = tenant
            .create_timeline_struct(
                timeline_id,
                local_metadata,
                None, // Ancestor is not needed for deletion.
                TimelineResources {
                    remote_client,
                    pagestream_throttle: tenant.pagestream_throttle.clone(),
                    pagestream_throttle_metrics: tenant.pagestream_throttle_metrics.clone(),
                    l0_flush_global_state: tenant.l0_flush_global_state.clone(),
                },
                // Important. We dont pass ancestor above because it can be missing.
                // Thus we need to skip the validation here.
                CreateTimelineCause::Delete,
                crate::tenant::CreateTimelineIdempotency::FailWithConflict, // doesn't matter what we put here
            )
            .context("create_timeline_struct")?;

        let mut guard = DeletionGuard(
            Arc::clone(&timeline.delete_progress)
                .try_lock_owned()
                .expect("cannot happen because we're the only owner"),
        );

        // We meed to do this because when console retries delete request we shouldnt answer with 404
        // because 404 means successful deletion.
        {
            let mut locked = tenant.timelines.lock().unwrap();
            locked.insert(timeline_id, Arc::clone(&timeline));
        }

        guard.mark_in_progress()?;

        let remote_client = timeline.remote_client.clone();
        let timeline = TimelineOrOffloaded::Timeline(timeline);
        Self::schedule_background(guard, tenant.conf, tenant, timeline, remote_client);

        Ok(())
    }

    pub(super) fn prepare(
        tenant: &Tenant,
        timeline_id: TimelineId,
        allow_offloaded_children: bool,
        set_stopping: bool,
    ) -> Result<(TimelineOrOffloaded, DeletionGuard), DeleteTimelineError> {
        // Note the interaction between this guard and deletion guard.
        // Here we attempt to lock deletion guard when we're holding a lock on timelines.
        // This is important because when you take into account `remove_timeline_from_tenant`
        // we remove timeline from memory when we still hold the deletion guard.
        // So here when timeline deletion is finished timeline wont be present in timelines map at all
        // which makes the following sequence impossible:
        // T1: get preempted right before the try_lock on `Timeline::delete_progress`
        // T2: do a full deletion, acquire and drop `Timeline::delete_progress`
        // T1: acquire deletion lock, do another `DeleteTimelineFlow::run`
        // For more context see this discussion: `https://github.com/neondatabase/neon/pull/4552#discussion_r1253437346`
        let timelines = tenant.timelines.lock().unwrap();
        let timelines_offloaded = tenant.timelines_offloaded.lock().unwrap();

        let timeline = match timelines.get(&timeline_id) {
            Some(t) => TimelineOrOffloaded::Timeline(Arc::clone(t)),
            None => match timelines_offloaded.get(&timeline_id) {
                Some(t) => TimelineOrOffloaded::Offloaded(Arc::clone(t)),
                None => return Err(DeleteTimelineError::NotFound),
            },
        };

        // Ensure that there are no child timelines, because we are about to remove files,
        // which will break child branches
        let mut children = Vec::new();
        if !allow_offloaded_children {
            children.extend(timelines_offloaded.iter().filter_map(|(id, entry)| {
                (entry.ancestor_timeline_id == Some(timeline_id)).then_some(*id)
            }));
        }
        children.extend(timelines.iter().filter_map(|(id, entry)| {
            (entry.get_ancestor_timeline_id() == Some(timeline_id)).then_some(*id)
        }));

        if !children.is_empty() {
            return Err(DeleteTimelineError::HasChildren(children));
        }

        // Note that using try_lock here is important to avoid a deadlock.
        // Here we take lock on timelines and then the deletion guard.
        // At the end of the operation we're holding the guard and need to lock timelines map
        // to remove the timeline from it.
        // Always if you have two locks that are taken in different order this can result in a deadlock.

        let delete_progress = Arc::clone(timeline.delete_progress());
        let delete_lock_guard = match delete_progress.try_lock_owned() {
            Ok(guard) => DeletionGuard(guard),
            Err(_) => {
                // Unfortunately if lock fails arc is consumed.
                return Err(DeleteTimelineError::AlreadyInProgress(Arc::clone(
                    timeline.delete_progress(),
                )));
            }
        };

        if set_stopping {
            if let TimelineOrOffloaded::Timeline(timeline) = &timeline {
                timeline.set_state(TimelineState::Stopping);
            }
        }

        Ok((timeline, delete_lock_guard))
    }

    fn schedule_background(
        guard: DeletionGuard,
        conf: &'static PageServerConf,
        tenant: Arc<Tenant>,
        timeline: TimelineOrOffloaded,
        remote_client: Arc<RemoteTimelineClient>,
    ) {
        let tenant_shard_id = timeline.tenant_shard_id();
        let timeline_id = timeline.timeline_id();

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            tenant_shard_id,
            Some(timeline_id),
            "timeline_delete",
            async move {
                if let Err(err) = Self::background(guard, conf, &tenant, &timeline, remote_client).await {
                    // Only log as an error if it's not a cancellation.
                    if matches!(err, DeleteTimelineError::Cancelled) {
                        info!("Shutdown during timeline deletion");
                    }else {
                        error!("Error: {err:#}");
                    }
                    if let TimelineOrOffloaded::Timeline(timeline) = timeline {
                        timeline.set_broken(format!("{err:#}"))
                    }
                };
                Ok(())
            }
            .instrument(tracing::info_span!(parent: None, "delete_timeline", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),timeline_id=%timeline_id)),
        );
    }

    async fn background(
        mut guard: DeletionGuard,
        conf: &PageServerConf,
        tenant: &Tenant,
        timeline: &TimelineOrOffloaded,
        remote_client: Arc<RemoteTimelineClient>,
    ) -> Result<(), DeleteTimelineError> {
        fail::fail_point!("timeline-delete-before-rm", |_| {
            Err(anyhow::anyhow!("failpoint: timeline-delete-before-rm"))?
        });

        // Offloaded timelines have no local state
        // TODO: once we persist offloaded information, delete the timeline from there, too
        if let TimelineOrOffloaded::Timeline(timeline) = timeline {
            delete_local_timeline_directory(conf, tenant.tenant_shard_id, timeline).await;
        }

        fail::fail_point!("timeline-delete-after-rm", |_| {
            Err(anyhow::anyhow!("failpoint: timeline-delete-after-rm"))?
        });

        remote_client.delete_all().await?;

        pausable_failpoint!("in_progress_delete");

        remove_maybe_offloaded_timeline_from_tenant(tenant, timeline, &guard).await?;

        // This is susceptible to race conditions, i.e. we won't continue deletions if there is a crash
        // between the deletion of the index-part.json and reaching of this code.
        // So indeed, the tenant manifest might refer to an offloaded timeline which has already been deleted.
        // However, we handle this case in tenant loading code so the next time we attach, the issue is
        // resolved.
        tenant.store_tenant_manifest().await.map_err(|e| match e {
            TenantManifestError::Cancelled => DeleteTimelineError::Cancelled,
            _ => DeleteTimelineError::Other(e.into()),
        })?;

        *guard = Self::Finished;

        Ok(())
    }

    pub(crate) fn is_not_started(&self) -> bool {
        matches!(self, Self::NotStarted)
    }
}

pub(super) struct DeletionGuard(OwnedMutexGuard<DeleteTimelineFlow>);

impl Deref for DeletionGuard {
    type Target = DeleteTimelineFlow;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DeletionGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
