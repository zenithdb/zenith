use std::sync::Arc;

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::models::TenantState;
use remote_storage::{DownloadError, GenericRemoteStorage, RemotePath};
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, warn, Instrument, Span};

use utils::{
    backoff, completion, crashsafe, fs_ext,
    id::{TenantId, TimelineId},
};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    task_mgr::{self, TaskKind},
    InitializationOrder,
};

use super::{
    mgr::{GetTenantError, TenantsMap},
    remote_timeline_client::{FAILED_REMOTE_OP_RETRIES, FAILED_UPLOAD_WARN_THRESHOLD},
    span,
    timeline::delete::DeleteTimelineFlow,
    tree_sort_timelines, DeleteTimelineError, Tenant,
};

const SHOULD_RESUME_DELETION_FETCH_MARK_ATTEMPTS: u32 = 3;

#[derive(Debug, thiserror::Error)]
pub enum DeleteTenantError {
    #[error("GetTenant {0}")]
    Get(#[from] GetTenantError),

    #[error("Invalid state {0}. Expected Active or Broken")]
    InvalidState(TenantState),

    #[error("Tenant deletion is already in progress")]
    AlreadyInProgress,

    #[error("Timeline {0}")]
    Timeline(#[from] DeleteTimelineError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

type DeletionGuard = tokio::sync::OwnedMutexGuard<DeleteTenantFlow>;

fn remote_tenant_delete_mark_path(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> anyhow::Result<RemotePath> {
    let tenant_remote_path = conf
        .tenant_path(tenant_id)
        .strip_prefix(&conf.workdir)
        .context("Failed to strip workdir prefix")
        .and_then(RemotePath::new)
        .context("tenant path")?;
    Ok(tenant_remote_path.join(Utf8Path::new("deleted")))
}

async fn create_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    let remote_mark_path = remote_tenant_delete_mark_path(conf, tenant_id)?;

    let data: &[u8] = &[];
    backoff::retry(
        || async {
            remote_storage
                .upload(data, 0, &remote_mark_path, None)
                .await
        },
        |_e| false,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        "mark_upload",
        // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
        backoff::Cancel::new(CancellationToken::new(), || unreachable!()),
    )
    .await
    .context("mark_upload")?;

    Ok(())
}

async fn create_local_delete_mark(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    let marker_path = conf.tenant_deleted_mark_file_path(tenant_id);

    // Note: we're ok to replace existing file.
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&marker_path)
        .with_context(|| format!("could not create delete marker file {marker_path:?}"))?;

    crashsafe::fsync_file_and_parent(&marker_path).context("sync_mark")?;

    Ok(())
}

async fn schedule_ordered_timeline_deletions(
    tenant: &Arc<Tenant>,
) -> Result<Vec<(Arc<tokio::sync::Mutex<DeleteTimelineFlow>>, TimelineId)>, DeleteTenantError> {
    // Tenant is stopping at this point. We know it will be deleted.
    // No new timelines should be created.
    // Tree sort timelines to delete from leafs to the root.
    // NOTE: by calling clone we release the mutex which creates a possibility for a race: pending deletion
    // can complete and remove timeline from the map in between our call to clone
    // and `DeleteTimelineFlow::run`, so `run` won't find timeline in `timelines` map.
    // timelines.lock is currently synchronous so we cant hold it across await point.
    // So just ignore NotFound error if we get it from `run`.
    // Beware: in case it becomes async and we try to hold it here, `run` also locks it, which can create a deadlock.
    let timelines = tenant.timelines.lock().unwrap().clone();
    let sorted =
        tree_sort_timelines(timelines, |t| t.get_ancestor_timeline_id()).context("tree sort")?;

    let mut already_running_deletions = vec![];

    for (timeline_id, _) in sorted.into_iter().rev() {
        if let Err(e) = DeleteTimelineFlow::run(tenant, timeline_id, true).await {
            match e {
                DeleteTimelineError::NotFound => {
                    // Timeline deletion finished after call to clone above but before call
                    // to `DeleteTimelineFlow::run` and removed timeline from the map.
                    continue;
                }
                DeleteTimelineError::AlreadyInProgress(guard) => {
                    already_running_deletions.push((guard, timeline_id));
                    continue;
                }
                e => return Err(DeleteTenantError::Timeline(e)),
            }
        }
    }

    Ok(already_running_deletions)
}

async fn ensure_timelines_dir_empty(timelines_path: &Utf8Path) -> Result<(), DeleteTenantError> {
    // Assert timelines dir is empty.
    if !fs_ext::is_directory_empty(timelines_path).await? {
        // Display first 10 items in directory
        let list = &fs_ext::list_dir(timelines_path).await.context("list_dir")?[..10];
        return Err(DeleteTenantError::Other(anyhow::anyhow!(
            "Timelines directory is not empty after all timelines deletion: {list:?}"
        )));
    }

    Ok(())
}

async fn remove_tenant_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: Option<&GenericRemoteStorage>,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    if let Some(remote_storage) = remote_storage {
        let path = remote_tenant_delete_mark_path(conf, tenant_id)?;
        backoff::retry(
            || async { remote_storage.delete(&path).await },
            |_e| false,
            FAILED_UPLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "remove_tenant_remote_delete_mark",
            // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
            backoff::Cancel::new(CancellationToken::new(), || unreachable!()),
        )
        .await
        .context("remove_tenant_remote_delete_mark")?;
    }
    Ok(())
}

// Cleanup fs traces: tenant config, timelines dir local delete mark, tenant dir
async fn cleanup_remaining_fs_traces(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    let rm = |p: Utf8PathBuf, is_dir: bool| async move {
        if is_dir {
            tokio::fs::remove_dir(&p).await
        } else {
            tokio::fs::remove_file(&p).await
        }
        .or_else(fs_ext::ignore_not_found)
        .with_context(|| format!("failed to delete {p}"))
    };

    rm(conf.tenant_config_path(tenant_id), false).await?;
    rm(conf.tenant_location_config_path(tenant_id), false).await?;

    fail::fail_point!("tenant-delete-before-remove-timelines-dir", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-timelines-dir"
        ))?
    });

    rm(conf.timelines_path(tenant_id), true).await?;

    fail::fail_point!("tenant-delete-before-remove-deleted-mark", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-deleted-mark"
        ))?
    });

    // Make sure previous deletions are ordered before mark removal.
    // Otherwise there is no guarantee that they reach the disk before mark deletion.
    // So its possible for mark to reach disk first and for other deletions
    // to be reordered later and thus missed if a crash occurs.
    // Note that we don't need to sync after mark file is removed
    // because we can tolerate the case when mark file reappears on startup.
    let tenant_path = &conf.tenant_path(tenant_id);
    if tenant_path.exists() {
        crashsafe::fsync_async(&conf.tenant_path(tenant_id))
            .await
            .context("fsync_pre_mark_remove")?;
    }

    rm(conf.tenant_deleted_mark_file_path(tenant_id), false).await?;

    fail::fail_point!("tenant-delete-before-remove-tenant-dir", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-tenant-dir"
        ))?
    });

    rm(conf.tenant_path(tenant_id), true).await?;

    Ok(())
}

pub(crate) async fn remote_delete_mark_exists(
    conf: &PageServerConf,
    tenant_id: &TenantId,
    remote_storage: &GenericRemoteStorage,
) -> anyhow::Result<bool> {
    // If remote storage is there we rely on it
    let remote_mark_path = remote_tenant_delete_mark_path(conf, tenant_id).context("path")?;

    let result = backoff::retry(
        || async { remote_storage.download(&remote_mark_path).await },
        |e| matches!(e, DownloadError::NotFound),
        SHOULD_RESUME_DELETION_FETCH_MARK_ATTEMPTS,
        SHOULD_RESUME_DELETION_FETCH_MARK_ATTEMPTS,
        "fetch_tenant_deletion_mark",
        // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
        backoff::Cancel::new(CancellationToken::new(), || unreachable!()),
    )
    .await;

    match result {
        Ok(_) => Ok(true),
        Err(DownloadError::NotFound) => Ok(false),
        Err(e) => Err(anyhow::anyhow!(e)).context("remote_delete_mark_exists")?,
    }
}

/// Orchestrates tenant shut down of all tasks, removes its in-memory structures,
/// and deletes its data from both disk and s3.
/// The sequence of steps:
/// 1. Upload remote deletion mark.
/// 2. Create local mark file.
/// 3. Shutdown tasks
/// 4. Run ordered timeline deletions
/// 5. Wait for timeline deletion operations that were scheduled before tenant deletion was requested
/// 6. Remove remote mark
/// 7. Cleanup remaining fs traces, tenant dir, config, timelines dir, local delete mark
/// It is resumable from any step in case a crash/restart occurs.
/// There are three entrypoints to the process:
/// 1. [`DeleteTenantFlow::run`] this is the main one called by a management api handler.
/// 2. [`DeleteTenantFlow::resume_from_load`] is called during restarts when local or remote deletion marks are still there.
/// 3. [`DeleteTenantFlow::resume_from_attach`] is called when deletion is resumed tenant is found to be deleted during attach process.
///  Note the only other place that messes around timeline delete mark is the `Tenant::spawn_load` function.
#[derive(Default)]
pub enum DeleteTenantFlow {
    #[default]
    NotStarted,
    InProgress,
    Finished,
}

impl DeleteTenantFlow {
    // These steps are run in the context of management api request handler.
    // Long running steps are continued to run in the background.
    // NB: If this fails half-way through, and is retried, the retry will go through
    // all the same steps again. Make sure the code here is idempotent, and don't
    // error out if some of the shutdown tasks have already been completed!
    // NOTE: static needed for background part.
    // We assume that calling code sets up the span with tenant_id.
    #[instrument(skip_all)]
    pub(crate) async fn run(
        conf: &'static PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        tenant_id: TenantId,
    ) -> Result<(), DeleteTenantError> {
        span::debug_assert_current_span_has_tenant_id();

        let (tenant, mut guard) = Self::prepare(tenants, tenant_id).await?;

        if let Err(e) = Self::run_inner(&mut guard, conf, remote_storage.as_ref(), &tenant).await {
            tenant.set_broken(format!("{e:#}")).await;
            return Err(e);
        }

        Self::schedule_background(guard, conf, remote_storage, tenants, tenant);

        Ok(())
    }

    // Helper function needed to be able to match once on returned error and transition tenant into broken state.
    // This is needed because tenant.shutwodn is not idempotent. If tenant state is set to stopping another call to tenant.shutdown
    // will result in an error, but here we need to be able to retry shutdown when tenant deletion is retried.
    // So the solution is to set tenant state to broken.
    async fn run_inner(
        guard: &mut OwnedMutexGuard<Self>,
        conf: &'static PageServerConf,
        remote_storage: Option<&GenericRemoteStorage>,
        tenant: &Tenant,
    ) -> Result<(), DeleteTenantError> {
        guard.mark_in_progress()?;

        fail::fail_point!("tenant-delete-before-create-remote-mark", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-create-remote-mark"
            ))?
        });

        // IDEA: implement detach as delete without remote storage. Then they would use the same lock (deletion_progress) so won't contend.
        // Though sounds scary, different mark name?
        // Detach currently uses remove_dir_all so in case of a crash we can end up in a weird state.
        if let Some(remote_storage) = &remote_storage {
            create_remote_delete_mark(conf, remote_storage, &tenant.tenant_id)
                .await
                .context("remote_mark")?
        }

        fail::fail_point!("tenant-delete-before-create-local-mark", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-create-local-mark"
            ))?
        });

        create_local_delete_mark(conf, &tenant.tenant_id)
            .await
            .context("local delete mark")?;

        fail::fail_point!("tenant-delete-before-background", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-background"
            ))?
        });

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

    pub async fn should_resume_deletion(
        conf: &'static PageServerConf,
        remote_storage: Option<&GenericRemoteStorage>,
        tenant: &Tenant,
    ) -> Result<Option<DeletionGuard>, DeleteTenantError> {
        let acquire = |t: &Tenant| {
            Some(
                Arc::clone(&t.delete_progress)
                    .try_lock_owned()
                    .expect("we're the only owner during init"),
            )
        };

        let tenant_id = tenant.tenant_id;
        // Check local mark first, if its there there is no need to go to s3 to check whether remote one exists.
        if conf.tenant_deleted_mark_file_path(&tenant_id).exists() {
            return Ok(acquire(tenant));
        }

        let remote_storage = match remote_storage {
            Some(remote_storage) => remote_storage,
            None => return Ok(None),
        };

        if remote_delete_mark_exists(conf, &tenant_id, remote_storage).await? {
            Ok(acquire(tenant))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn resume_from_load(
        guard: DeletionGuard,
        tenant: &Arc<Tenant>,
        init_order: Option<&InitializationOrder>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        ctx: &RequestContext,
    ) -> Result<(), DeleteTenantError> {
        let (_, progress) = completion::channel();

        tenant
            .set_stopping(progress, true, false)
            .await
            .expect("cant be stopping or broken");

        // Do not consume valuable resources during the load phase, continue deletion once init phase is complete.
        let background_jobs_can_start = init_order.as_ref().map(|x| &x.background_jobs_can_start);
        if let Some(background) = background_jobs_can_start {
            info!("waiting for background jobs barrier");
            background.clone().wait().await;
            info!("ready for background jobs barrier");
        }

        // Tenant may not be loadable if we fail late in cleanup_remaining_fs_traces (e g remove timelines dir)
        let timelines_path = tenant.conf.timelines_path(&tenant.tenant_id);
        if timelines_path.exists() {
            tenant.load(init_order, ctx).await.context("load")?;
        }

        Self::background(
            guard,
            tenant.conf,
            tenant.remote_storage.clone(),
            tenants,
            tenant,
        )
        .await
    }

    pub(crate) async fn resume_from_attach(
        guard: DeletionGuard,
        tenant: &Arc<Tenant>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        ctx: &RequestContext,
    ) -> Result<(), DeleteTenantError> {
        let (_, progress) = completion::channel();

        tenant
            .set_stopping(progress, false, true)
            .await
            .expect("cant be stopping or broken");

        tenant.attach(ctx).await.context("attach")?;

        Self::background(
            guard,
            tenant.conf,
            tenant.remote_storage.clone(),
            tenants,
            tenant,
        )
        .await
    }

    async fn prepare(
        tenants: &tokio::sync::RwLock<TenantsMap>,
        tenant_id: TenantId,
    ) -> Result<(Arc<Tenant>, tokio::sync::OwnedMutexGuard<Self>), DeleteTenantError> {
        let m = tenants.read().await;

        let tenant = m
            .get(&tenant_id)
            .ok_or(GetTenantError::NotFound(tenant_id))?;

        // FIXME: unsure about active only. Our init jobs may not be cancellable properly,
        // so at least for now allow deletions only for active tenants. TODO recheck
        // Broken and Stopping is needed for retries.
        if !matches!(
            tenant.current_state(),
            TenantState::Active | TenantState::Broken { .. }
        ) {
            return Err(DeleteTenantError::InvalidState(tenant.current_state()));
        }

        let guard = Arc::clone(&tenant.delete_progress)
            .try_lock_owned()
            .map_err(|_| DeleteTenantError::AlreadyInProgress)?;

        fail::fail_point!("tenant-delete-before-shutdown", |_| {
            Err(anyhow::anyhow!("failpoint: tenant-delete-before-shutdown"))?
        });

        // make pageserver shutdown not to wait for our completion
        let (_, progress) = completion::channel();

        // It would be good to only set stopping here and continue shutdown in the background, but shutdown is not idempotent.
        // i e it is an error to do:
        // tenant.set_stopping
        // tenant.shutdown
        // Its also bad that we're holding tenants.read here.
        // TODO relax set_stopping to be idempotent?
        if tenant.shutdown(progress, false).await.is_err() {
            return Err(DeleteTenantError::Other(anyhow::anyhow!(
                "tenant shutdown is already in progress"
            )));
        }

        Ok((Arc::clone(tenant), guard))
    }

    fn schedule_background(
        guard: OwnedMutexGuard<Self>,
        conf: &'static PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        tenant: Arc<Tenant>,
    ) {
        let tenant_id = tenant.tenant_id;

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            Some(tenant_id),
            None,
            "tenant_delete",
            false,
            async move {
                if let Err(err) =
                    Self::background(guard, conf, remote_storage, tenants, &tenant).await
                {
                    error!("Error: {err:#}");
                    tenant.set_broken(format!("{err:#}")).await;
                };
                Ok(())
            }
            .instrument({
                let span = tracing::info_span!(parent: None, "delete_tenant", tenant_id=%tenant_id);
                span.follows_from(Span::current());
                span
            }),
        );
    }

    async fn background(
        mut guard: OwnedMutexGuard<Self>,
        conf: &PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        tenant: &Arc<Tenant>,
    ) -> Result<(), DeleteTenantError> {
        // Tree sort timelines, schedule delete for them. Mention retries from the console side.
        // Note that if deletion fails we don't mark timelines as broken,
        // the whole tenant will become broken as by `Self::schedule_background` logic
        let already_running_timeline_deletions = schedule_ordered_timeline_deletions(tenant)
            .await
            .context("schedule_ordered_timeline_deletions")?;

        fail::fail_point!("tenant-delete-before-polling-ongoing-deletions", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-polling-ongoing-deletions"
            ))?
        });

        // Wait for deletions that were already running at the moment when tenant deletion was requested.
        // When we can lock deletion guard it means that corresponding timeline deletion finished.
        for (guard, timeline_id) in already_running_timeline_deletions {
            let flow = guard.lock().await;
            if !flow.is_finished() {
                return Err(DeleteTenantError::Other(anyhow::anyhow!(
                    "already running timeline deletion failed: {timeline_id}"
                )));
            }
        }

        let timelines_path = conf.timelines_path(&tenant.tenant_id);
        // May not exist if we fail in cleanup_remaining_fs_traces after removing it
        if timelines_path.exists() {
            // sanity check to guard against layout changes
            ensure_timelines_dir_empty(&timelines_path)
                .await
                .context("timelines dir not empty")?;
        }

        remove_tenant_remote_delete_mark(conf, remote_storage.as_ref(), &tenant.tenant_id).await?;

        fail::fail_point!("tenant-delete-before-cleanup-remaining-fs-traces", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-cleanup-remaining-fs-traces"
            ))?
        });

        cleanup_remaining_fs_traces(conf, &tenant.tenant_id)
            .await
            .context("cleanup_remaining_fs_traces")?;

        let mut locked = tenants.write().await;
        if locked.remove(&tenant.tenant_id).is_none() {
            warn!("Tenant got removed from tenants map during deletion");
        };

        *guard = Self::Finished;

        Ok(())
    }
}
