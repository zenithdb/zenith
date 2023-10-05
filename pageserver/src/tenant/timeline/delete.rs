use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use anyhow::Context;
use pageserver_api::models::TimelineState;
use tokio::sync::OwnedMutexGuard;
use tracing::{debug, error, info, instrument, warn, Instrument, Span};
use utils::{
    crashsafe, fs_ext,
    id::{TenantId, TimelineId},
};

use crate::{
    config::PageServerConf,
    deletion_queue::DeletionQueueClient,
    task_mgr::{self, TaskKind},
    tenant::{
        metadata::TimelineMetadata,
        remote_timeline_client::{
            self, PersistIndexPartWithDeletedFlagError, RemoteTimelineClient,
        },
        CreateTimelineCause, DeleteTimelineError, Tenant,
    },
    InitializationOrder,
};

use super::{Timeline, TimelineResources};

/// Now that the Timeline is in Stopping state, request all the related tasks to shut down.
async fn stop_tasks(timeline: &Timeline) -> Result<(), DeleteTimelineError> {
    // Stop the walreceiver first.
    debug!("waiting for wal receiver to shutdown");
    let maybe_started_walreceiver = { timeline.walreceiver.lock().unwrap().take() };
    if let Some(walreceiver) = maybe_started_walreceiver {
        walreceiver.stop().await;
    }
    debug!("wal receiver shutdown confirmed");

    // Prevent new uploads from starting.
    if let Some(remote_client) = timeline.remote_client.as_ref() {
        let res = remote_client.stop();
        match res {
            Ok(()) => {}
            Err(e) => match e {
                remote_timeline_client::StopError::QueueUninitialized => {
                    // This case shouldn't happen currently because the
                    // load and attach code bails out if _any_ of the timeline fails to fetch its IndexPart.
                    // That is, before we declare the Tenant as Active.
                    // But we only allow calls to delete_timeline on Active tenants.
                    return Err(DeleteTimelineError::Other(anyhow::anyhow!("upload queue is uninitialized, likely the timeline was in Broken state prior to this call because it failed to fetch IndexPart during load or attach, check the logs")));
                }
            },
        }
    }

    // Stop & wait for the remaining timeline tasks, including upload tasks.
    // NB: This and other delete_timeline calls do not run as a task_mgr task,
    //     so, they are not affected by this shutdown_tasks() call.
    info!("waiting for timeline tasks to shutdown");
    task_mgr::shutdown_tasks(None, Some(timeline.tenant_id), Some(timeline.timeline_id)).await;

    fail::fail_point!("timeline-delete-before-index-deleted-at", |_| {
        Err(anyhow::anyhow!(
            "failpoint: timeline-delete-before-index-deleted-at"
        ))?
    });
    Ok(())
}

/// Mark timeline as deleted in S3 so we won't pick it up next time
/// during attach or pageserver restart.
/// See comment in persist_index_part_with_deleted_flag.
async fn set_deleted_in_remote_index(timeline: &Timeline) -> Result<(), DeleteTimelineError> {
    if let Some(remote_client) = timeline.remote_client.as_ref() {
        match remote_client.persist_index_part_with_deleted_flag().await {
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
    }
    Ok(())
}

// We delete local files first, so if pageserver restarts after local files deletion then remote deletion is not continued.
// This can be solved with inversion of these steps. But even if these steps are inverted then, when index_part.json
// gets deleted there is no way to distinguish between "this timeline is good, we just didnt upload it to remote"
// and "this timeline is deleted we should continue with removal of local state". So to avoid the ambiguity we use a mark file.
// After index part is deleted presence of this mark file identifies that it was a deletion intention.
// So we can just remove the mark file.
async fn create_delete_mark(
    conf: &PageServerConf,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Result<(), DeleteTimelineError> {
    fail::fail_point!("timeline-delete-before-delete-mark", |_| {
        Err(anyhow::anyhow!(
            "failpoint: timeline-delete-before-delete-mark"
        ))?
    });
    let marker_path = conf.timeline_delete_mark_file_path(tenant_id, timeline_id);

    // Note: we're ok to replace existing file.
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&marker_path)
        .with_context(|| format!("could not create delete marker file {marker_path:?}"))?;

    crashsafe::fsync_file_and_parent(&marker_path).context("sync_mark")?;
    Ok(())
}

/// Grab the layer_removal_cs lock, and actually perform the deletion.
///
/// This lock prevents prevents GC or compaction from running at the same time.
/// The GC task doesn't register itself with the timeline it's operating on,
/// so it might still be running even though we called `shutdown_tasks`.
///
/// Note that there are still other race conditions between
/// GC, compaction and timeline deletion. See
/// <https://github.com/neondatabase/neon/issues/2671>
///
/// No timeout here, GC & Compaction should be responsive to the
/// `TimelineState::Stopping` change.
async fn delete_local_layer_files(
    conf: &PageServerConf,
    tenant_id: TenantId,
    timeline: &Timeline,
) -> anyhow::Result<()> {
    info!("waiting for layer_removal_cs.lock()");
    let layer_removal_guard = timeline.layer_removal_cs.lock().await;
    info!("got layer_removal_cs.lock(), deleting layer files");

    // NB: storage_sync upload tasks that reference these layers have been cancelled
    //     by the caller.

    let local_timeline_directory = conf.timeline_path(&tenant_id, &timeline.timeline_id);

    fail::fail_point!("timeline-delete-before-rm", |_| {
        Err(anyhow::anyhow!("failpoint: timeline-delete-before-rm"))?
    });

    // NB: This need not be atomic because the deleted flag in the IndexPart
    // will be observed during tenant/timeline load. The deletion will be resumed there.
    //
    // For configurations without remote storage, we guarantee crash-safety by persising delete mark file.
    //
    // Note that here we do not bail out on std::io::ErrorKind::NotFound.
    // This can happen if we're called a second time, e.g.,
    // because of a previous failure/cancellation at/after
    // failpoint timeline-delete-after-rm.
    //
    // It can also happen if we race with tenant detach, because,
    // it doesn't grab the layer_removal_cs lock.
    //
    // For now, log and continue.
    // warn! level is technically not appropriate for the
    // first case because we should expect retries to happen.
    // But the error is so rare, it seems better to get attention if it happens.
    //
    // Note that metadata removal is skipped, this is not technically needed,
    // but allows to reuse timeline loading code during resumed deletion.
    // (we always expect that metadata is in place when timeline is being loaded)

    #[cfg(feature = "testing")]
    let mut counter = 0;

    // Timeline directory may not exist if we failed to delete mark file and request was retried.
    if !local_timeline_directory.exists() {
        return Ok(());
    }

    let metadata_path = conf.metadata_path(&tenant_id, &timeline.timeline_id);

    for entry in walkdir::WalkDir::new(&local_timeline_directory).contents_first(true) {
        #[cfg(feature = "testing")]
        {
            counter += 1;
            if counter == 2 {
                fail::fail_point!("timeline-delete-during-rm", |_| {
                    Err(anyhow::anyhow!("failpoint: timeline-delete-during-rm"))?
                });
            }
        }

        let entry = entry?;
        if entry.path() == metadata_path {
            debug!("found metadata, skipping");
            continue;
        }

        if entry.path() == local_timeline_directory {
            // Keeping directory because metedata file is still there
            debug!("found timeline dir itself, skipping");
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(e) => {
                if crate::is_walkdir_io_not_found(&e) {
                    warn!(
                        timeline_dir=?local_timeline_directory,
                        path=?entry.path().display(),
                        "got not found err while removing timeline dir, proceeding anyway"
                    );
                    continue;
                }
                anyhow::bail!(e);
            }
        };

        if metadata.is_dir() {
            warn!(path=%entry.path().display(), "unexpected directory under timeline dir");
            tokio::fs::remove_dir(entry.path()).await
        } else {
            tokio::fs::remove_file(entry.path()).await
        }
        .with_context(|| format!("Failed to remove: {}", entry.path().display()))?;
    }

    info!("finished deleting layer files, releasing layer_removal_cs.lock()");
    drop(layer_removal_guard);

    fail::fail_point!("timeline-delete-after-rm", |_| {
        Err(anyhow::anyhow!("failpoint: timeline-delete-after-rm"))?
    });

    Ok(())
}

/// Removes remote layers and an index file after them.
async fn delete_remote_layers_and_index(timeline: &Timeline) -> anyhow::Result<()> {
    if let Some(remote_client) = &timeline.remote_client {
        remote_client.delete_all().await.context("delete_all")?
    };

    Ok(())
}

// This function removs remaining traces of a timeline on disk.
// Namely: metadata file, timeline directory, delete mark.
// Note: io::ErrorKind::NotFound are ignored for metadata and timeline dir.
// delete mark should be present because it is the last step during deletion.
// (nothing can fail after its deletion)
async fn cleanup_remaining_timeline_fs_traces(
    conf: &PageServerConf,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> anyhow::Result<()> {
    // Remove local metadata
    tokio::fs::remove_file(conf.metadata_path(&tenant_id, &timeline_id))
        .await
        .or_else(fs_ext::ignore_not_found)
        .context("remove metadata")?;

    fail::fail_point!("timeline-delete-after-rm-metadata", |_| {
        Err(anyhow::anyhow!(
            "failpoint: timeline-delete-after-rm-metadata"
        ))?
    });

    // Remove timeline dir
    tokio::fs::remove_dir(conf.timeline_path(&tenant_id, &timeline_id))
        .await
        .or_else(fs_ext::ignore_not_found)
        .context("timeline dir")?;

    fail::fail_point!("timeline-delete-after-rm-dir", |_| {
        Err(anyhow::anyhow!("failpoint: timeline-delete-after-rm-dir"))?
    });

    // Make sure previous deletions are ordered before mark removal.
    // Otherwise there is no guarantee that they reach the disk before mark deletion.
    // So its possible for mark to reach disk first and for other deletions
    // to be reordered later and thus missed if a crash occurs.
    // Note that we dont need to sync after mark file is removed
    // because we can tolerate the case when mark file reappears on startup.
    let timeline_path = conf.timelines_path(&tenant_id);
    crashsafe::fsync_async(timeline_path)
        .await
        .context("fsync_pre_mark_remove")?;

    // Remove delete mark
    tokio::fs::remove_file(conf.timeline_delete_mark_file_path(tenant_id, timeline_id))
        .await
        .context("remove delete mark")
}

/// It is important that this gets called when DeletionGuard is being held.
/// For more context see comments in [`DeleteTimelineFlow::prepare`]
async fn remove_timeline_from_tenant(
    tenant: &Tenant,
    timeline_id: TimelineId,
    _: &DeletionGuard, // using it as a witness
) -> anyhow::Result<()> {
    // Remove the timeline from the map.
    let mut timelines = tenant.timelines.lock().unwrap();
    let children_exist = timelines
        .iter()
        .any(|(_, entry)| entry.get_ancestor_timeline_id() == Some(timeline_id));
    // XXX this can happen because `branch_timeline` doesn't check `TimelineState::Stopping`.
    // We already deleted the layer files, so it's probably best to panic.
    // (Ideally, above remove_dir_all is atomic so we don't see this timeline after a restart)
    if children_exist {
        panic!("Timeline grew children while we removed layer files");
    }

    timelines
        .remove(&timeline_id)
        .expect("timeline that we were deleting was concurrently removed from 'timelines' map");

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
/// It is resumable from any step in case a crash/restart occurs.
/// There are three entrypoints to the process:
/// 1. [`DeleteTimelineFlow::run`] this is the main one called by a management api handler.
/// 2. [`DeleteTimelineFlow::resume_deletion`] is called during restarts when local metadata is still present
/// and we possibly need to continue deletion of remote files.
/// 3. [`DeleteTimelineFlow::cleanup_remaining_timeline_fs_traces`] is used when we deleted remote
/// index but still have local metadata, timeline directory and delete mark.
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
    #[instrument(skip(tenant), fields(tenant_id=%tenant.tenant_id))]
    pub async fn run(
        tenant: &Arc<Tenant>,
        timeline_id: TimelineId,
        inplace: bool,
    ) -> Result<(), DeleteTimelineError> {
        let (timeline, mut guard) = Self::prepare(tenant, timeline_id)?;

        guard.mark_in_progress()?;

        stop_tasks(&timeline).await?;

        set_deleted_in_remote_index(&timeline).await?;

        create_delete_mark(tenant.conf, timeline.tenant_id, timeline.timeline_id).await?;

        fail::fail_point!("timeline-delete-before-schedule", |_| {
            Err(anyhow::anyhow!(
                "failpoint: timeline-delete-before-schedule"
            ))?
        });

        if inplace {
            Self::background(guard, tenant.conf, tenant, &timeline).await?
        } else {
            Self::schedule_background(guard, tenant.conf, Arc::clone(tenant), timeline);
        }

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
    /// See corresponding parts of [`crate::tenant::delete::DeleteTenantFlow`]
    #[instrument(skip_all, fields(%timeline_id))]
    pub async fn resume_deletion(
        tenant: Arc<Tenant>,
        timeline_id: TimelineId,
        local_metadata: &TimelineMetadata,
        remote_client: Option<RemoteTimelineClient>,
        deletion_queue_client: DeletionQueueClient,
        init_order: Option<&InitializationOrder>,
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
                    deletion_queue_client,
                },
                init_order,
                // Important. We dont pass ancestor above because it can be missing.
                // Thus we need to skip the validation here.
                CreateTimelineCause::Delete,
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

        // Note that delete mark can be missing on resume
        // because we create delete mark after we set deleted_at in the index part.
        create_delete_mark(tenant.conf, tenant.tenant_id, timeline_id).await?;

        Self::schedule_background(guard, tenant.conf, tenant, timeline);

        Ok(())
    }

    #[instrument(skip_all, fields(%timeline_id))]
    pub async fn cleanup_remaining_timeline_fs_traces(
        tenant: &Tenant,
        timeline_id: TimelineId,
    ) -> anyhow::Result<()> {
        let r =
            cleanup_remaining_timeline_fs_traces(tenant.conf, tenant.tenant_id, timeline_id).await;
        info!("Done");
        r
    }

    fn prepare(
        tenant: &Tenant,
        timeline_id: TimelineId,
    ) -> Result<(Arc<Timeline>, DeletionGuard), DeleteTimelineError> {
        // Note the interaction between this guard and deletion guard.
        // Here we attempt to lock deletion guard when we're holding a lock on timelines.
        // This is important because when you take into account `remove_timeline_from_tenant`
        // we remove timeline from memory when we still hold the deletion guard.
        // So here when timeline deletion is finished timeline won't be present in timelines map at all
        // which makes the following sequence impossible:
        // T1: get preempted right before the try_lock on `Timeline::delete_progress`
        // T2: do a full deletion, acquire and drop `Timeline::delete_progress`
        // T1: acquire deletion lock, do another `DeleteTimelineFlow::run`
        // For more context see this discussion: `https://github.com/neondatabase/neon/pull/4552#discussion_r1253437346`
        let timelines = tenant.timelines.lock().unwrap();

        let timeline = match timelines.get(&timeline_id) {
            Some(t) => t,
            None => return Err(DeleteTimelineError::NotFound),
        };

        // Ensure that there are no child timelines **attached to that pageserver**,
        // because detach removes files, which will break child branches
        let children: Vec<TimelineId> = timelines
            .iter()
            .filter_map(|(id, entry)| {
                if entry.get_ancestor_timeline_id() == Some(timeline_id) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        if !children.is_empty() {
            return Err(DeleteTimelineError::HasChildren(children));
        }

        // Note that using try_lock here is important to avoid a deadlock.
        // Here we take lock on timelines and then the deletion guard.
        // At the end of the operation we're holding the guard and need to lock timelines map
        // to remove the timeline from it.
        // Always if you have two locks that are taken in different order this can result in a deadlock.

        let delete_progress = Arc::clone(&timeline.delete_progress);
        let delete_lock_guard = match delete_progress.try_lock_owned() {
            Ok(guard) => DeletionGuard(guard),
            Err(_) => {
                // Unfortunately if lock fails arc is consumed.
                return Err(DeleteTimelineError::AlreadyInProgress(Arc::clone(
                    &timeline.delete_progress,
                )));
            }
        };

        timeline.set_state(TimelineState::Stopping);

        Ok((Arc::clone(timeline), delete_lock_guard))
    }

    fn schedule_background(
        guard: DeletionGuard,
        conf: &'static PageServerConf,
        tenant: Arc<Tenant>,
        timeline: Arc<Timeline>,
    ) {
        let tenant_id = timeline.tenant_id;
        let timeline_id = timeline.timeline_id;

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            Some(tenant_id),
            Some(timeline_id),
            "timeline_delete",
            false,
            async move {
                if let Err(err) = Self::background(guard, conf, &tenant, &timeline).await {
                    error!("Error: {err:#}");
                    timeline.set_broken(format!("{err:#}"))
                };
                Ok(())
            }
            .instrument({
                let span =
                    tracing::info_span!(parent: None, "delete_timeline", tenant_id=%tenant_id, timeline_id=%timeline_id);
                span.follows_from(Span::current());
                span
            }),
        );
    }

    async fn background(
        mut guard: DeletionGuard,
        conf: &PageServerConf,
        tenant: &Tenant,
        timeline: &Timeline,
    ) -> Result<(), DeleteTimelineError> {
        delete_local_layer_files(conf, tenant.tenant_id, timeline).await?;

        delete_remote_layers_and_index(timeline).await?;

        pausable_failpoint!("in_progress_delete");

        cleanup_remaining_timeline_fs_traces(conf, tenant.tenant_id, timeline.timeline_id).await?;

        remove_timeline_from_tenant(tenant, timeline.timeline_id, &guard).await?;

        *guard = Self::Finished;

        Ok(())
    }

    pub(crate) fn is_finished(&self) -> bool {
        matches!(self, Self::Finished)
    }
}

struct DeletionGuard(OwnedMutexGuard<DeleteTimelineFlow>);

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
