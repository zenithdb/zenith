use std::{collections::HashSet, sync::Arc};

use super::{layer_manager::LayerManager, FlushLayerError, Timeline};
use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::{
        mgr::GetActiveTenantError,
        storage_layer::{AsLayerDesc as _, DeltaLayerWriter, Layer, ResidentLayer},
        Tenant,
    },
    virtual_file::{MaybeFatalIo, VirtualFile},
};
use anyhow::Context;
use pageserver_api::models::detach_ancestor::{self, AncestorDetached};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{
    completion, generation::Generation, http::error::ApiError, id::TimelineId, lsn::Lsn, sync::gate,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("no ancestors")]
    NoAncestor,
    #[error("too many ancestors")]
    TooManyAncestors,
    #[error("shutting down, please retry later")]
    ShuttingDown,
    #[error("flushing failed")]
    FlushAncestor(#[source] FlushLayerError),
    #[error("layer download failed")]
    RewrittenDeltaDownloadFailed(#[source] crate::tenant::storage_layer::layer::DownloadError),
    #[error("copying LSN prefix locally failed")]
    CopyDeltaPrefix(#[source] anyhow::Error),
    #[error("upload rewritten layer")]
    UploadRewritten(#[source] anyhow::Error),

    #[error("ancestor is already being detached by: {}", .0)]
    OtherTimelineDetachOngoing(TimelineId),

    #[error("remote copying layer failed")]
    CopyFailed(#[source] anyhow::Error),

    #[error("wait for tenant to activate after restarting")]
    WaitToActivate(#[source] GetActiveTenantError),

    #[error("detached timeline was not found after restart")]
    DetachedNotFoundAfterRestart,

    #[error("unexpected error")]
    Unexpected(#[source] anyhow::Error),

    #[error("failpoint: {}", .0)]
    Failpoint(&'static str),
}

impl From<Error> for ApiError {
    fn from(value: Error) -> Self {
        match value {
            e @ Error::NoAncestor => ApiError::Conflict(e.to_string()),
            // TODO: ApiError converts the anyhow using debug formatting ... just stop using ApiError?
            e @ Error::TooManyAncestors => ApiError::BadRequest(anyhow::anyhow!("{}", e)),
            Error::ShuttingDown => ApiError::ShuttingDown,
            Error::OtherTimelineDetachOngoing(_) => {
                ApiError::ResourceUnavailable("other timeline detach is already ongoing".into())
            }
            e @ Error::WaitToActivate(_) => {
                let s = utils::error::report_compact_sources(&e).to_string();
                ApiError::ResourceUnavailable(s.into())
            }
            // All of these contain shutdown errors, in fact, it's the most common
            e @ Error::FlushAncestor(_)
            | e @ Error::RewrittenDeltaDownloadFailed(_)
            | e @ Error::CopyDeltaPrefix(_)
            | e @ Error::UploadRewritten(_)
            | e @ Error::CopyFailed(_)
            | e @ Error::Unexpected(_)
            | e @ Error::Failpoint(_) => ApiError::InternalServerError(e.into()),
            Error::DetachedNotFoundAfterRestart => ApiError::NotFound(value.into()),
        }
    }
}

impl From<crate::tenant::upload_queue::NotInitialized> for Error {
    fn from(_: crate::tenant::upload_queue::NotInitialized) -> Self {
        // treat all as shutting down signals, even though that is not entirely correct
        // (uninitialized state)
        Error::ShuttingDown
    }
}
impl From<super::layer_manager::Shutdown> for Error {
    fn from(_: super::layer_manager::Shutdown) -> Self {
        Error::ShuttingDown
    }
}

impl From<FlushLayerError> for Error {
    fn from(value: FlushLayerError) -> Self {
        match value {
            FlushLayerError::Cancelled => Error::ShuttingDown,
            FlushLayerError::NotRunning(_) => {
                // FIXME(#6424): technically statically unreachable right now, given how we never
                // drop the sender
                Error::ShuttingDown
            }
            FlushLayerError::CreateImageLayersError(_) | FlushLayerError::Other(_) => {
                Error::FlushAncestor(value)
            }
        }
    }
}

impl From<GetActiveTenantError> for Error {
    fn from(value: GetActiveTenantError) -> Self {
        use pageserver_api::models::TenantState;
        use GetActiveTenantError::*;

        match value {
            Cancelled | WillNotBecomeActive(TenantState::Stopping { .. }) => Error::ShuttingDown,
            WaitForActiveTimeout { .. } | NotFound(_) | Broken(_) | WillNotBecomeActive(_) => {
                // NotFound seems out-of-place
                Error::WaitToActivate(value)
            }
        }
    }
}

pub(crate) enum Progress {
    Prepared(Attempt, PreparedTimelineDetach),
    Done(AncestorDetached),
}

pub(crate) struct PreparedTimelineDetach {
    layers: Vec<Layer>,
}

/// TODO: this should be part of PageserverConf because we cannot easily modify cplane arguments.
#[derive(Debug)]
pub(crate) struct Options {
    pub(crate) rewrite_concurrency: std::num::NonZeroUsize,
    pub(crate) copy_concurrency: std::num::NonZeroUsize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rewrite_concurrency: std::num::NonZeroUsize::new(2).unwrap(),
            copy_concurrency: std::num::NonZeroUsize::new(100).unwrap(),
        }
    }
}

/// Represents an across tenant reset exclusive single attempt to detach ancestor.
#[derive(Debug)]
pub(crate) struct Attempt {
    pub(crate) timeline_id: TimelineId,

    _guard: completion::Completion,
    gate_entered: Option<utils::sync::gate::GateGuard>,
}

impl Attempt {
    pub(crate) fn before_shutdown(&mut self) {
        let taken = self.gate_entered.take();
        assert!(taken.is_some());
    }
}

/// See [`Timeline::prepare_to_detach_from_ancestor`]
pub(super) async fn prepare(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    options: Options,
    ctx: &RequestContext,
) -> Result<Progress, Error> {
    use Error::*;

    let Some((ancestor, ancestor_lsn)) = detached
        .ancestor_timeline
        .as_ref()
        .map(|tl| (tl.clone(), detached.ancestor_lsn))
    else {
        let still_in_progress = {
            let accessor = detached.remote_client.initialized_upload_queue()?;

            // we are safe to inspect the latest uploaded, because we can only witness this after
            // restart is complete and ancestor is no more.
            let latest = accessor.latest_uploaded_index_part();
            if latest.lineage.detached_previous_ancestor().is_none() {
                return Err(NoAncestor);
            };

            latest.gc_blocking.as_ref().is_some_and(|b| {
                b.blocked_by(
                    crate::tenant::remote_timeline_client::index::GcBlockingReason::DetachAncestor,
                )
            })
        };

        if still_in_progress {
            // gc is still blocked, we can still reparent and complete.
            // we are safe to reparent remaining, because they were locked in in the beginning.
            let attempt = continue_with_blocked_gc(detached, tenant).await?;

            // because the ancestor of detached is already set to none, we have published all
            // of the layers, so we are still "prepared."
            return Ok(Progress::Prepared(
                attempt,
                PreparedTimelineDetach { layers: Vec::new() },
            ));
        }

        let reparented_timelines = reparented_direct_children(detached, tenant)?;
        return Ok(Progress::Done(AncestorDetached {
            reparented_timelines,
        }));
    };

    if !ancestor_lsn.is_valid() {
        // rare case, probably wouldn't even load
        tracing::error!("ancestor is set, but ancestor_lsn is invalid, this timeline needs fixing");
        return Err(NoAncestor);
    }

    if ancestor.ancestor_timeline.is_some() {
        // non-technical requirement; we could flatten N ancestors just as easily but we chose
        // not to, at least initially
        return Err(TooManyAncestors);
    }

    let attempt = start_new_attempt(detached, tenant).await?;

    utils::pausable_failpoint!("timeline-detach-ancestor::before_starting_after_locking_pausable");

    fail::fail_point!(
        "timeline-detach-ancestor::before_starting_after_locking",
        |_| Err(Error::Failpoint(
            "timeline-detach-ancestor::before_starting_after_locking"
        ))
    );

    if ancestor_lsn >= ancestor.get_disk_consistent_lsn() {
        let span =
            tracing::info_span!("freeze_and_flush", ancestor_timeline_id=%ancestor.timeline_id);
        async {
            let started_at = std::time::Instant::now();
            let freeze_and_flush = ancestor.freeze_and_flush0();
            let mut freeze_and_flush = std::pin::pin!(freeze_and_flush);

            let res =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut freeze_and_flush)
                    .await;

            let res = match res {
                Ok(res) => res,
                Err(_elapsed) => {
                    tracing::info!("freezing and flushing ancestor is still ongoing");
                    freeze_and_flush.await
                }
            };

            res?;

            // we do not need to wait for uploads to complete but we do need `struct Layer`,
            // copying delta prefix is unsupported currently for `InMemoryLayer`.
            tracing::info!(
                elapsed_ms = started_at.elapsed().as_millis(),
                "froze and flushed the ancestor"
            );
            Ok::<_, Error>(())
        }
        .instrument(span)
        .await?;
    }

    let end_lsn = ancestor_lsn + 1;

    let (filtered_layers, straddling_branchpoint, rest_of_historic) = {
        // we do not need to start from our layers, because they can only be layers that come
        // *after* ancestor_lsn
        let layers = tokio::select! {
            guard = ancestor.layers.read() => guard,
            _ = detached.cancel.cancelled() => {
                return Err(ShuttingDown);
            }
            _ = ancestor.cancel.cancelled() => {
                return Err(ShuttingDown);
            }
        };

        // between retries, these can change if compaction or gc ran in between. this will mean
        // we have to redo work.
        partition_work(ancestor_lsn, &layers)?
    };

    // TODO: layers are already sorted by something: use that to determine how much of remote
    // copies are already done -- gc is blocked, but a compaction could had happened on ancestor,
    // which is something to keep in mind if copy skipping is implemented.
    tracing::info!(filtered=%filtered_layers, to_rewrite = straddling_branchpoint.len(), historic=%rest_of_historic.len(), "collected layers");

    // TODO: copying and lsn prefix copying could be done at the same time with a single fsync after
    let mut new_layers: Vec<Layer> =
        Vec::with_capacity(straddling_branchpoint.len() + rest_of_historic.len());

    {
        tracing::debug!(to_rewrite = %straddling_branchpoint.len(), "copying prefix of delta layers");

        let mut tasks = tokio::task::JoinSet::new();

        let mut wrote_any = false;

        let limiter = Arc::new(Semaphore::new(options.rewrite_concurrency.get()));

        for layer in straddling_branchpoint {
            let limiter = limiter.clone();
            let timeline = detached.clone();
            let ctx = ctx.detached_child(TaskKind::DetachAncestor, DownloadBehavior::Download);

            let span = tracing::info_span!("upload_rewritten_layer", %layer);
            tasks.spawn(
                async move {
                    let _permit = limiter.acquire().await;
                    let copied =
                        upload_rewritten_layer(end_lsn, &layer, &timeline, &timeline.cancel, &ctx)
                            .await?;
                    if let Some(copied) = copied.as_ref() {
                        tracing::info!(%copied, "rewrote and uploaded");
                    }
                    Ok(copied)
                }
                .instrument(span),
            );
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(Ok(Some(copied))) => {
                    wrote_any = true;
                    new_layers.push(copied);
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => return Err(e),
                Err(je) => return Err(Unexpected(je.into())),
            }
        }

        // FIXME: the fsync should be mandatory, after both rewrites and copies
        if wrote_any {
            let timeline_dir = VirtualFile::open(
                &detached
                    .conf
                    .timeline_path(&detached.tenant_shard_id, &detached.timeline_id),
                ctx,
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }
    }

    let mut tasks = tokio::task::JoinSet::new();
    let limiter = Arc::new(Semaphore::new(options.copy_concurrency.get()));

    for adopted in rest_of_historic {
        let limiter = limiter.clone();
        let timeline = detached.clone();

        tasks.spawn(
            async move {
                let _permit = limiter.acquire().await;
                let owned =
                    remote_copy(&adopted, &timeline, timeline.generation, &timeline.cancel).await?;
                tracing::info!(layer=%owned, "remote copied");
                Ok(owned)
            }
            .in_current_span(),
        );
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(owned)) => {
                new_layers.push(owned);
            }
            Ok(Err(failed)) => {
                return Err(failed);
            }
            Err(je) => return Err(Unexpected(je.into())),
        }
    }

    // TODO: fsync directory again if we hardlinked something

    let prepared = PreparedTimelineDetach { layers: new_layers };

    Ok(Progress::Prepared(attempt, prepared))
}

async fn start_new_attempt(detached: &Timeline, tenant: &Tenant) -> Result<Attempt, Error> {
    let attempt = obtain_exclusive_attempt(detached, tenant)?;

    // insert the block in the index_part.json, if not already there.
    let _dont_care = tenant
        .gc_block
        .insert(
            detached,
            crate::tenant::remote_timeline_client::index::GcBlockingReason::DetachAncestor,
        )
        .await
        .map_err(|e| Error::launder(e, Error::Prepare))?;

    Ok(attempt)
}

async fn continue_with_blocked_gc(detached: &Timeline, tenant: &Tenant) -> Result<Attempt, Error> {
    // FIXME: it would be nice to confirm that there is an in-memory version, since we've just
    // verified there is a persistent one?
    obtain_exclusive_attempt(detached, tenant)
}

fn obtain_exclusive_attempt(detached: &Timeline, tenant: &Tenant) -> Result<Attempt, Error> {
    use Error::{OtherTimelineDetachOngoing, ShuttingDown};

    // ensure we are the only active attempt for this tenant
    let (guard, barrier) = completion::channel();
    {
        let mut guard = tenant.ongoing_timeline_detach.lock().unwrap();
        if let Some((tl, other)) = guard.as_ref() {
            if !other.is_ready() {
                return Err(OtherTimelineDetachOngoing(*tl));
            }
            // FIXME: no test enters here
        }
        *guard = Some((detached.timeline_id, barrier));
    }

    // ensure the gate is still open
    let _gate_entered = detached.gate.enter().map_err(|_| ShuttingDown)?;

    Ok(Attempt {
        timeline_id: detached.timeline_id,
        _guard: guard,
        gate_entered: Some(_gate_entered),
    })
}

fn reparented_direct_children(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
) -> Result<HashSet<TimelineId>, Error> {
    let mut all_direct_children = tenant
        .timelines
        .lock()
        .unwrap()
        .values()
        .filter_map(|tl| {
            let is_direct_child = matches!(tl.ancestor_timeline.as_ref(), Some(ancestor) if Arc::ptr_eq(ancestor, detached));

            if is_direct_child {
                Some(tl.clone())
            } else {
                if let Some(timeline) = tl.ancestor_timeline.as_ref() {
                    assert_ne!(timeline.timeline_id, detached.timeline_id, "we cannot have two timelines with the same timeline_id live");
                }
                None
            }
        })
        // Collect to avoid lock taking order problem with Tenant::timelines and
        // Timeline::remote_client
        .collect::<Vec<_>>();

    let mut any_shutdown = false;

    all_direct_children.retain(|tl| match tl.remote_client.initialized_upload_queue() {
        Ok(accessor) => accessor
            .latest_uploaded_index_part()
            .lineage
            .is_reparented(),
        Err(_shutdownalike) => {
            // not 100% a shutdown, but let's bail early not to give inconsistent results in
            // sharded enviroment.
            any_shutdown = true;
            true
        }
    });

    if any_shutdown {
        // it could be one or many being deleted; have client retry
        return Err(Error::ShuttingDown);
    }

    Ok(all_direct_children
        .into_iter()
        .map(|tl| tl.timeline_id)
        .collect())
}

fn partition_work(
    ancestor_lsn: Lsn,
    source: &LayerManager,
) -> Result<(usize, Vec<Layer>, Vec<Layer>), Error> {
    let mut straddling_branchpoint = vec![];
    let mut rest_of_historic = vec![];

    let mut later_by_lsn = 0;

    for desc in source.layer_map()?.iter_historic_layers() {
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

        target.push(source.get_from_desc(&desc));
    }

    Ok((later_by_lsn, straddling_branchpoint, rest_of_historic))
}

async fn upload_rewritten_layer(
    end_lsn: Lsn,
    layer: &Layer,
    target: &Arc<Timeline>,
    cancel: &CancellationToken,
    ctx: &RequestContext,
) -> Result<Option<Layer>, Error> {
    use Error::UploadRewritten;
    let copied = copy_lsn_prefix(end_lsn, layer, target, ctx).await?;

    let Some(copied) = copied else {
        return Ok(None);
    };

    // FIXME: better shuttingdown error
    target
        .remote_client
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
) -> Result<Option<ResidentLayer>, Error> {
    use Error::{CopyDeltaPrefix, RewrittenDeltaDownloadFailed, ShuttingDown};

    if target_timeline.cancel.is_cancelled() {
        return Err(ShuttingDown);
    }

    tracing::debug!(%layer, %end_lsn, "copying lsn prefix");

    let mut writer = DeltaLayerWriter::new(
        target_timeline.conf,
        target_timeline.timeline_id,
        target_timeline.tenant_shard_id,
        layer.layer_desc().key_range.start,
        layer.layer_desc().lsn_range.start..end_lsn,
        ctx,
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
        let (desc, path) = writer
            .finish(reused_highest_key, ctx)
            .await
            .map_err(CopyDeltaPrefix)?;
        let copied = Layer::finish_creating(target_timeline.conf, target_timeline, desc, &path)
            .map_err(CopyDeltaPrefix)?;

        tracing::debug!(%layer, %copied, "new layer produced");

        Ok(Some(copied))
    }
}

/// Creates a new Layer instance for the adopted layer, and ensures it is found from the remote
/// storage on successful return without the adopted layer being added to `index_part.json`.
async fn remote_copy(
    adopted: &Layer,
    adoptee: &Arc<Timeline>,
    generation: Generation,
    cancel: &CancellationToken,
) -> Result<Layer, Error> {
    use Error::CopyFailed;

    // depending if Layer::keep_resident we could hardlink

    let mut metadata = adopted.metadata();
    debug_assert!(metadata.generation <= generation);
    metadata.generation = generation;

    let owned = crate::tenant::storage_layer::Layer::for_evicted(
        adoptee.conf,
        adoptee,
        adopted.layer_desc().layer_name(),
        metadata,
    );

    // FIXME: better shuttingdown error
    adoptee
        .remote_client
        .copy_timeline_layer(adopted, &owned, cancel)
        .await
        .map(move |()| owned)
        .map_err(CopyFailed)
}

pub(crate) enum DetachingAndReparenting {
    /// All of the following timeline ids were reparented and the timeline ancestor detach must be
    /// marked as completed.
    Reparented(HashSet<TimelineId>),

    /// Some of the reparentings failed. The timeline ancestor detach must **not** be marked as
    /// completed.
    ///
    /// Nested `must_restart` is set to true when any restart requiring changes were made.
    SomeReparentingFailed { must_restart: bool },

    /// Detaching and reparentings were completed in a previous attempt. Timeline ancestor detach
    /// must be marked as completed.
    AlreadyDone(HashSet<TimelineId>),
}

impl DetachingAndReparenting {
    pub(crate) fn reset_tenant_required(&self) -> bool {
        use DetachingAndReparenting::*;
        match self {
            Reparented(_) => true,
            SomeReparentingFailed { must_restart } => *must_restart,
            AlreadyDone(_) => false,
        }
    }

    pub(crate) fn completed(self) -> Option<HashSet<TimelineId>> {
        use DetachingAndReparenting::*;
        match self {
            Reparented(x) | AlreadyDone(x) => Some(x),
            SomeReparentingFailed { .. } => None,
        }
    }
}

/// See [`Timeline::detach_from_ancestor_and_reparent`].
pub(super) async fn detach_and_reparent(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    prepared: PreparedTimelineDetach,
    _ctx: &RequestContext,
) -> Result<DetachingAndReparenting, anyhow::Error> {
    let PreparedTimelineDetach { layers } = prepared;

    #[derive(Debug)]
    enum Ancestor {
        NotDetached(Arc<Timeline>, Lsn),
        Detached(Arc<Timeline>, Lsn),
    }

    let (recorded_branchpoint, still_ongoing) = {
        let access = detached.remote_client.initialized_upload_queue()?;
        let latest = access.latest_uploaded_index_part();

        (
            latest.lineage.detached_previous_ancestor(),
            latest.gc_blocking.as_ref().is_some_and(|b| {
                b.blocked_by(
                    crate::tenant::remote_timeline_client::index::GcBlockingReason::DetachAncestor,
                )
            }),
        )
    };
    assert!(
        still_ongoing,
        "cannot (detach? reparent)? complete if the operation is not still ongoing"
    );

    let ancestor = match (detached.ancestor_timeline.as_ref(), recorded_branchpoint) {
        (Some(ancestor), None) => {
            assert!(
                !layers.is_empty(),
                "there should always be at least one layer to inherit"
            );
            Ancestor::NotDetached(ancestor.clone(), detached.ancestor_lsn)
        }
        (Some(_), Some(_)) => {
            panic!(
                "it should be impossible to get to here without having gone through the tenant reset; if the tenant was reset, then the ancestor_timeline would be None"
            );
        }
        (None, Some((ancestor_id, ancestor_lsn))) => {
            // it has been either:
            // - detached but still exists => we can try reparenting
            // - detached and deleted
            //
            // either way, we must complete
            assert!(
                layers.is_empty(),
                "no layers should had been copied as detach is done"
            );

            let existing = tenant.timelines.lock().unwrap().get(&ancestor_id).cloned();

            if let Some(ancestor) = existing {
                Ancestor::Detached(ancestor, ancestor_lsn)
            } else {
                let direct_children = reparented_direct_children(detached, tenant)?;
                return Ok(DetachingAndReparenting::AlreadyDone(direct_children));
            }
        }
        (None, None) => {
            // TODO: make sure there are no `?` before tenant_reset from after a questionmark from
            // here.
            panic!(
            "bug: detach_and_reparent called on a timeline which has not been detached or which has no live ancestor"
            );
        }
    };

    // publish the prepared layers before we reparent any of the timelines, so that on restart
    // reparented timelines find layers. also do the actual detaching.
    //
    // if we crash after this operation, a retry will allow reparenting the remaining timelines as
    // gc is blocked.

    let (ancestor, ancestor_lsn, was_detached) = match ancestor {
        Ancestor::NotDetached(ancestor, ancestor_lsn) => {
            // this has to complete before any reparentings because otherwise they would not have
            // layers on the new parent.
            detached
                .remote_client
                .schedule_adding_existing_layers_to_index_detach_and_wait(
                    &layers,
                    (ancestor.timeline_id, ancestor_lsn),
                )
                .await
                .context("publish layers and detach ancestor")?;

            tracing::info!(
                ancestor=%ancestor.timeline_id,
                %ancestor_lsn,
                inherited_layers=%layers.len(),
                "detached from ancestor"
            );
            (ancestor, ancestor_lsn, true)
        }
        Ancestor::Detached(ancestor, ancestor_lsn) => (ancestor, ancestor_lsn, false),
    };

    let mut tasks = tokio::task::JoinSet::new();

    // Returns a single permit semaphore which will be used to make one reparenting succeed,
    // others will fail as if those timelines had been stopped for whatever reason.
    #[cfg(feature = "testing")]
    let failpoint_sem = || -> Option<Arc<Semaphore>> {
        fail::fail_point!("timeline-detach-ancestor::allow_one_reparented", |_| Some(
            Arc::new(Semaphore::new(1))
        ));
        None
    }();

    // because we are now keeping the slot in progress, it is unlikely that there will be any
    // timeline deletions during this time. if we raced one, then we'll just ignore it.
    {
        let g = tenant.timelines.lock().unwrap();
        reparentable_timelines(g.values(), detached, &ancestor, ancestor_lsn)
            .cloned()
            .for_each(|timeline| {
                // important in this scope: we are holding the Tenant::timelines lock
                let span = tracing::info_span!("reparent", reparented=%timeline.timeline_id);
                let new_parent = detached.timeline_id;
                #[cfg(feature = "testing")]
                let failpoint_sem = failpoint_sem.clone();

                tasks.spawn(
                    async move {
                        let res = async {
                            #[cfg(feature = "testing")]
                            if let Some(failpoint_sem) = failpoint_sem {
                                let _permit = failpoint_sem.acquire().await.map_err(|_| {
                                    anyhow::anyhow!(
                                        "failpoint: timeline-detach-ancestor::allow_one_reparented",
                                    )
                                })?;
                                failpoint_sem.close();
                            }

                            timeline
                                .remote_client
                                .schedule_reparenting_and_wait(&new_parent)
                                .await
                        }
                        .await;

                        match res {
                            Ok(()) => {
                                tracing::info!("reparented");
                                Some(timeline)
                            }
                            Err(e) => {
                                // with the use of tenant slot, raced timeline deletion is the most
                                // likely reason.
                                tracing::warn!("reparenting failed: {e:#}");
                                None
                            }
                        }
                    }
                    .instrument(span),
                );
            });
    }

    let reparenting_candidates = tasks.len();
    let mut reparented = HashSet::with_capacity(tasks.len());

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Some(timeline)) => {
                assert!(
                    reparented.insert(timeline.timeline_id),
                    "duplicate reparenting? timeline_id={}",
                    timeline.timeline_id
                );
            }
            Err(je) if je.is_cancelled() => unreachable!("not used"),
            // just ignore failures now, we can retry
            Ok(None) => {}
            Err(je) if je.is_panic() => {}
            Err(je) => tracing::error!("unexpected join error: {je:?}"),
        }
    }

    let reparented_all = reparenting_candidates == reparented.len();

    if reparented_all {
        Ok(DetachingAndReparenting::Reparented(reparented))
    } else {
        tracing::info!(
            reparented = reparented.len(),
            candidates = reparenting_candidates,
            "failed to reparent all candidates; they can be retried after the tenant_reset",
        );
        let must_restart = !reparented.is_empty() || was_detached;
        Ok(DetachingAndReparenting::SomeReparentingFailed { must_restart })
    }
}

pub(super) async fn complete(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    mut attempt: Attempt,
    _ctx: &RequestContext,
) -> Result<(), Error> {
    assert_eq!(detached.timeline_id, attempt.timeline_id);

    if attempt.gate_entered.is_none() {
        let entered = detached.gate.enter().map_err(|_| Error::ShuttingDown)?;
        attempt.gate_entered = Some(entered);
    } else {
        // Some(gate_entered) means the tenant was not restarted, as is not required
    }

    assert!(detached.ancestor_timeline.is_none());

    // this should be an 503 at least...?
    fail::fail_point!(
        "timeline-detach-ancestor::complete_before_uploading",
        |_| Err(Error::Failpoint(
            "timeline-detach-ancestor::complete_before_uploading"
        ))
    );

    tenant
        .gc_block
        .remove(
            detached,
            crate::tenant::remote_timeline_client::index::GcBlockingReason::DetachAncestor,
        )
        .await
        .map_err(|e| Error::launder(e, Error::Complete))?;

    Ok(())
}

/// Query against a locked `Tenant::timelines`.
fn reparentable_timelines<'a, I>(
    timelines: I,
    detached: &'a Arc<Timeline>,
    ancestor: &'a Arc<Timeline>,
    ancestor_lsn: Lsn,
) -> impl Iterator<Item = &'a Arc<Timeline>> + 'a
where
    I: Iterator<Item = &'a Arc<Timeline>> + 'a,
{
    timelines.filter_map(move |tl| {
        if Arc::ptr_eq(tl, detached) {
            return None;
        }

        let tl_ancestor = tl.ancestor_timeline.as_ref()?;
        let is_same = Arc::ptr_eq(ancestor, tl_ancestor);
        let is_earlier = tl.get_ancestor_lsn() <= ancestor_lsn;

        let is_deleting = tl
            .delete_progress
            .try_lock()
            .map(|flow| !flow.is_not_started())
            .unwrap_or(true);

        if is_same && is_earlier && !is_deleting {
            Some(tl)
        } else {
            None
        }
    })
}
