use std::{collections::hash_map::Entry, fs, sync::Arc};

use anyhow::Context;
use camino::Utf8PathBuf;
use tracing::{error, info, info_span};
use utils::{fs_ext, id::TimelineId, lsn::Lsn, sync::gate::GateGuard};

use crate::{
    context::RequestContext,
    import_datadir,
    tenant::{CreateTimelineIdempotency, TenantShard, TimelineOrOffloaded},
};

use super::Timeline;

/// A timeline with some of its files on disk, being initialized.
/// This struct ensures the atomicity of the timeline init: it's either properly created and inserted into pageserver's memory, or
/// its local files are removed.  If we crash while this class exists, then the timeline's local
/// state is cleaned up during [`TenantShard::clean_up_timelines`], because the timeline's content isn't in remote storage.
///
/// The caller is responsible for proper timeline data filling before the final init.
#[must_use]
pub struct UninitializedTimeline<'t> {
    pub(crate) owning_tenant: &'t TenantShard,
    timeline_id: TimelineId,
    raw_timeline: Option<(Arc<Timeline>, TimelineCreateGuard)>,
}

impl<'t> UninitializedTimeline<'t> {
    pub(crate) fn new(
        owning_tenant: &'t TenantShard,
        timeline_id: TimelineId,
        raw_timeline: Option<(Arc<Timeline>, TimelineCreateGuard)>,
    ) -> Self {
        Self {
            owning_tenant,
            timeline_id,
            raw_timeline,
        }
    }

    /// Finish timeline creation: insert it into the Tenant's timelines map
    ///
    /// This function launches the flush loop if not already done.
    ///
    /// The caller is responsible for activating the timeline (function `.activate()`).
    pub(crate) fn finish_creation(mut self) -> anyhow::Result<Arc<Timeline>> {
        let timeline_id = self.timeline_id;
        let tenant_shard_id = self.owning_tenant.tenant_shard_id;

        if self.raw_timeline.is_none() {
            return Err(anyhow::anyhow!(
                "No timeline for initialization found for {tenant_shard_id}/{timeline_id}"
            ));
        }

        // Check that the caller initialized disk_consistent_lsn
        let new_disk_consistent_lsn = self
            .raw_timeline
            .as_ref()
            .expect("checked above")
            .0
            .get_disk_consistent_lsn();

        anyhow::ensure!(
            new_disk_consistent_lsn.is_valid(),
            "new timeline {tenant_shard_id}/{timeline_id} has invalid disk_consistent_lsn"
        );

        let mut timelines = self.owning_tenant.timelines.lock().unwrap();
        match timelines.entry(timeline_id) {
            Entry::Occupied(_) => anyhow::bail!(
                "Found freshly initialized timeline {tenant_shard_id}/{timeline_id} in the tenant map"
            ),
            Entry::Vacant(v) => {
                // after taking here should be no fallible operations, because the drop guard will not
                // cleanup after and would block for example the tenant deletion
                let (new_timeline, _create_guard) =
                    self.raw_timeline.take().expect("already checked");

                v.insert(Arc::clone(&new_timeline));

                new_timeline.maybe_spawn_flush_loop();

                Ok(new_timeline)
            }
        }
    }

    pub(crate) fn finish_creation_myself(&mut self) -> (Arc<Timeline>, TimelineCreateGuard) {
        self.raw_timeline.take().expect("already checked")
    }

    /// Prepares timeline data by loading it from the basebackup archive.
    pub(crate) async fn import_basebackup_from_tar(
        self,
        tenant: Arc<TenantShard>,
        copyin_read: &mut (impl tokio::io::AsyncRead + Send + Sync + Unpin),
        base_lsn: Lsn,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let raw_timeline = self.raw_timeline()?;

        import_datadir::import_basebackup_from_tar(raw_timeline, copyin_read, base_lsn, ctx)
            .await
            .context("Failed to import basebackup")?;

        // Flush the new layer files to disk, before we make the timeline as available to
        // the outside world.
        //
        // Flush loop needs to be spawned in order to be able to flush.
        raw_timeline.maybe_spawn_flush_loop();

        fail::fail_point!("before-checkpoint-new-timeline", |_| {
            anyhow::bail!("failpoint before-checkpoint-new-timeline");
        });

        raw_timeline
            .freeze_and_flush()
            .await
            .context("Failed to flush after basebackup import")?;

        // All the data has been imported. Insert the Timeline into the tenant's timelines map
        let tl = self.finish_creation()?;
        tl.activate(tenant, broker_client, None, ctx);
        Ok(tl)
    }

    pub(crate) fn raw_timeline(&self) -> anyhow::Result<&Arc<Timeline>> {
        Ok(&self
            .raw_timeline
            .as_ref()
            .with_context(|| {
                format!(
                    "No raw timeline {}/{} found",
                    self.owning_tenant.tenant_shard_id, self.timeline_id
                )
            })?
            .0)
    }
}

impl Drop for UninitializedTimeline<'_> {
    fn drop(&mut self) {
        if let Some((_, create_guard)) = self.raw_timeline.take() {
            let _entered = info_span!("drop_uninitialized_timeline", tenant_id = %self.owning_tenant.tenant_shard_id.tenant_id, shard_id = %self.owning_tenant.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id).entered();
            // This is unusual, but can happen harmlessly if the pageserver is stopped while
            // creating a timeline.
            info!("Timeline got dropped without initializing, cleaning its files");
            cleanup_timeline_directory(create_guard);
        }
    }
}

pub(crate) fn cleanup_timeline_directory(create_guard: TimelineCreateGuard) {
    let timeline_path = &create_guard.timeline_path;
    match fs_ext::ignore_absent_files(|| fs::remove_dir_all(timeline_path)) {
        Ok(()) => {
            info!("Timeline dir {timeline_path:?} removed successfully")
        }
        Err(e) => {
            error!("Failed to clean up uninitialized timeline directory {timeline_path:?}: {e:?}")
        }
    }
    // Having cleaned up, we can release this TimelineId in `[TenantShard::timelines_creating]` to allow other
    // timeline creation attempts under this TimelineId to proceed
    drop(create_guard);
}

/// A guard for timeline creations in process: as long as this object exists, the timeline ID
/// is kept in `[TenantShard::timelines_creating]` to exclude concurrent attempts to create the same timeline.
#[must_use]
pub(crate) struct TimelineCreateGuard {
    pub(crate) _tenant_gate_guard: GateGuard,
    pub(crate) owning_tenant: Arc<TenantShard>,
    pub(crate) timeline_id: TimelineId,
    pub(crate) timeline_path: Utf8PathBuf,
    pub(crate) idempotency: CreateTimelineIdempotency,
}

/// Errors when acquiring exclusive access to a timeline ID for creation
#[derive(thiserror::Error, Debug)]
pub(crate) enum TimelineExclusionError {
    #[error("Already exists")]
    AlreadyExists {
        existing: TimelineOrOffloaded,
        arg: CreateTimelineIdempotency,
    },
    #[error("Already creating")]
    AlreadyCreating,
    #[error("Shutting down")]
    ShuttingDown,

    // e.g. I/O errors, or some failure deep in postgres initdb
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl TimelineCreateGuard {
    pub(crate) fn new(
        owning_tenant: &Arc<TenantShard>,
        timeline_id: TimelineId,
        timeline_path: Utf8PathBuf,
        idempotency: CreateTimelineIdempotency,
        allow_offloaded: bool,
    ) -> Result<Self, TimelineExclusionError> {
        let _tenant_gate_guard = owning_tenant
            .gate
            .enter()
            .map_err(|_| TimelineExclusionError::ShuttingDown)?;

        // Lock order: this is the only place we take both locks.  During drop() we only
        // lock creating_timelines
        let timelines = owning_tenant.timelines.lock().unwrap();
        let timelines_offloaded = owning_tenant.timelines_offloaded.lock().unwrap();
        let mut creating_timelines: std::sync::MutexGuard<
            '_,
            std::collections::HashSet<TimelineId>,
        > = owning_tenant.timelines_creating.lock().unwrap();

        if let Some(existing) = timelines.get(&timeline_id) {
            return Err(TimelineExclusionError::AlreadyExists {
                existing: TimelineOrOffloaded::Timeline(existing.clone()),
                arg: idempotency,
            });
        }
        if !allow_offloaded {
            if let Some(existing) = timelines_offloaded.get(&timeline_id) {
                return Err(TimelineExclusionError::AlreadyExists {
                    existing: TimelineOrOffloaded::Offloaded(existing.clone()),
                    arg: idempotency,
                });
            }
        }
        if creating_timelines.contains(&timeline_id) {
            return Err(TimelineExclusionError::AlreadyCreating);
        }
        creating_timelines.insert(timeline_id);
        drop(creating_timelines);
        drop(timelines_offloaded);
        drop(timelines);
        Ok(Self {
            _tenant_gate_guard,
            owning_tenant: Arc::clone(owning_tenant),
            timeline_id,
            timeline_path,
            idempotency,
        })
    }
}

impl Drop for TimelineCreateGuard {
    fn drop(&mut self) {
        self.owning_tenant
            .timelines_creating
            .lock()
            .unwrap()
            .remove(&self.timeline_id);
    }
}
