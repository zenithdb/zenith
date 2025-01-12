use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::keyspace::KeySpace;
use pageserver_api::models::HistoricLayerInfo;
use pageserver_api::shard::{ShardIdentity, ShardIndex, TenantShardId};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};
use tracing::Instrument;
use utils::id::TimelineId;
use utils::lsn::Lsn;
use utils::sync::{gate, heavier_once_cell};

use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext, RequestContextBuilder};
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::task_mgr::TaskKind;
use crate::tenant::timeline::{CompactionError, GetVectoredError};
use crate::tenant::{remote_timeline_client::LayerFileMetadata, Timeline};

use super::delta_layer::{self};
use super::image_layer::{self};
use super::{
    AsLayerDesc, ImageLayerWriter, LayerAccessStats, LayerAccessStatsReset, LayerName,
    LayerVisibilityHint, PersistentLayerDesc, ValuesReconstructState,
};

use utils::generation::Generation;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod failpoints;

pub const S3_UPLOAD_LIMIT: u64 = 4_500_000_000;

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This type represents the on-disk kind while in-memory kind are represented by
/// [`InMemoryLayer`].
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN.
///
/// This type models the on-disk layers, which can be evicted and on-demand downloaded. As a
/// general goal, read accesses should always win eviction and eviction should not wait for
/// download.
///
/// ### State transitions
///
/// The internal state of `Layer` is composed of most importantly the on-filesystem state and the
/// [`ResidentOrWantedEvicted`] enum. On-filesystem state can be either present (fully downloaded,
/// right size) or deleted.
///
/// Reads will always win requests to evict until `wait_for_turn_and_evict` has acquired the
/// `heavier_once_cell::InitPermit` and has started to `evict_blocking`. Before the
/// `heavier_once_cell::InitPermit` has been acquired, any read request
/// (`get_or_maybe_download`) can "re-initialize" using the existing downloaded file and thus
/// cancelling the eviction.
///
/// ```text
///  +-----------------+   get_or_maybe_download    +--------------------------------+
///  | not initialized |--------------------------->| Resident(Arc<DownloadedLayer>) |
///  |     ENOENT      |                         /->|                                |
///  +-----------------+                         |  +--------------------------------+
///                  ^                           |                         |       ^
///                  |    get_or_maybe_download  |                         |       | get_or_maybe_download, either:
///   evict_blocking | /-------------------------/                         |       | - upgrade weak to strong
///                  | |                                                   |       | - re-initialize without download
///                  | |                                    evict_and_wait |       |
///  +-----------------+                                                   v       |
///  | not initialized |  on_downloaded_layer_drop  +--------------------------------------+
///  | file is present |<---------------------------| WantedEvicted(Weak<DownloadedLayer>) |
///  +-----------------+                            +--------------------------------------+
/// ```
///
/// ### Unsupported
///
/// - Evicting by the operator deleting files from the filesystem
///
/// [`InMemoryLayer`]: super::inmemory_layer::InMemoryLayer
#[derive(Clone)]
pub(crate) struct Layer(Arc<LayerInner>);

impl std::fmt::Display for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.layer_desc().short_id(),
            self.0.generation.get_suffix()
        )
    }
}

impl std::fmt::Debug for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl AsLayerDesc for Layer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.0.layer_desc()
    }
}

impl PartialEq for Layer {
    fn eq(&self, other: &Self) -> bool {
        Arc::as_ptr(&self.0) == Arc::as_ptr(&other.0)
    }
}

pub(crate) fn local_layer_path(
    conf: &PageServerConf,
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    layer_file_name: &LayerName,
    generation: &Generation,
) -> Utf8PathBuf {
    let timeline_path = conf.timeline_path(tenant_shard_id, timeline_id);

    if generation.is_none() {
        // Without a generation, we may only use legacy path style
        timeline_path.join(layer_file_name.to_string())
    } else {
        timeline_path.join(format!("{}-v1{}", layer_file_name, generation.get_suffix()))
    }
}

impl Layer {
    /// Creates a layer value for a file we know to not be resident.
    pub(crate) fn for_evicted(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        file_name: LayerName,
        metadata: LayerFileMetadata,
    ) -> Self {
        let local_path = local_layer_path(
            conf,
            &timeline.tenant_shard_id,
            &timeline.timeline_id,
            &file_name,
            &metadata.generation,
        );

        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_shard_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size,
        );

        let owner = Layer(Arc::new(LayerInner::new(
            conf,
            timeline,
            local_path,
            desc,
            None,
            metadata.generation,
            metadata.shard,
        )));

        debug_assert!(owner.0.needs_download_blocking().unwrap().is_some());

        owner
    }

    /// Creates a Layer value for a file we know to be resident in timeline directory.
    pub(crate) fn for_resident(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        local_path: Utf8PathBuf,
        file_name: LayerName,
        metadata: LayerFileMetadata,
    ) -> ResidentLayer {
        let desc = PersistentLayerDesc::from_filename(
            timeline.tenant_shard_id,
            timeline.timeline_id,
            file_name,
            metadata.file_size,
        );

        let mut resident = None;

        let owner = Layer(Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
                version: 0,
            });
            resident = Some(inner.clone());

            LayerInner::new(
                conf,
                timeline,
                local_path,
                desc,
                Some(inner),
                metadata.generation,
                metadata.shard,
            )
        }));

        let downloaded = resident.expect("just initialized");

        debug_assert!(owner.0.needs_download_blocking().unwrap().is_none());

        timeline
            .metrics
            .resident_physical_size_add(metadata.file_size);

        ResidentLayer { downloaded, owner }
    }

    /// Creates a Layer value for freshly written out new layer file by renaming it from a
    /// temporary path.
    pub(crate) fn finish_creating(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        desc: PersistentLayerDesc,
        temp_path: &Utf8Path,
    ) -> anyhow::Result<ResidentLayer> {
        let mut resident = None;

        let owner = Layer(Arc::new_cyclic(|owner| {
            let inner = Arc::new(DownloadedLayer {
                owner: owner.clone(),
                kind: tokio::sync::OnceCell::default(),
                version: 0,
            });
            resident = Some(inner.clone());

            let local_path = local_layer_path(
                conf,
                &timeline.tenant_shard_id,
                &timeline.timeline_id,
                &desc.layer_name(),
                &timeline.generation,
            );

            LayerInner::new(
                conf,
                timeline,
                local_path,
                desc,
                Some(inner),
                timeline.generation,
                timeline.get_shard_index(),
            )
        }));

        let downloaded = resident.expect("just initialized");

        // We never want to overwrite an existing file, so we use `RENAME_NOREPLACE`.
        // TODO: this leaves the temp file in place if the rename fails, risking us running
        // out of space. Should we clean it up here or does the calling context deal with this?
        utils::fs_ext::rename_noreplace(temp_path.as_std_path(), owner.local_path().as_std_path())
            .with_context(|| format!("rename temporary file as correct path for {owner}"))?;

        Ok(ResidentLayer { downloaded, owner })
    }

    /// Requests the layer to be evicted and waits for this to be done.
    ///
    /// If the file is not resident, an [`EvictionError::NotFound`] is returned.
    ///
    /// If for a bad luck or blocking of the executor, we miss the actual eviction and the layer is
    /// re-downloaded, [`EvictionError::Downloaded`] is returned.
    ///
    /// Timeout is mandatory, because waiting for eviction is only needed for our tests; eviction
    /// will happen regardless the future returned by this method completing unless there is a
    /// read access before eviction gets to complete.
    ///
    /// Technically cancellation safe, but cancelling might shift the viewpoint of what generation
    /// of download-evict cycle on retry.
    pub(crate) async fn evict_and_wait(&self, timeout: Duration) -> Result<(), EvictionError> {
        self.0.evict_and_wait(timeout).await
    }

    /// Delete the layer file when the `self` gets dropped, also try to schedule a remote index upload
    /// then.
    ///
    /// On drop, this will cause a call to [`crate::tenant::remote_timeline_client::RemoteTimelineClient::schedule_deletion_of_unlinked`].
    /// This means that the unlinking by [gc] or [compaction] must have happened strictly before
    /// the value this is called on gets dropped.
    ///
    /// This is ensured by both of those methods accepting references to Layer.
    ///
    /// [gc]: [`RemoteTimelineClient::schedule_gc_update`]
    /// [compaction]: [`RemoteTimelineClient::schedule_compaction_update`]
    pub(crate) fn delete_on_drop(&self) {
        self.0.delete_on_drop();
    }

    pub(crate) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let layer = self
            .0
            .get_or_maybe_download(true, Some(ctx))
            .await
            .map_err(|err| match err {
                DownloadError::TimelineShutdown | DownloadError::DownloadCancelled => {
                    GetVectoredError::Cancelled
                }
                other => GetVectoredError::Other(anyhow::anyhow!(other)),
            })?;

        self.record_access(ctx);

        layer
            .get_values_reconstruct_data(keyspace, lsn_range, reconstruct_data, &self.0, ctx)
            .instrument(tracing::debug_span!("get_values_reconstruct_data", layer=%self))
            .await
            .map_err(|err| match err {
                GetVectoredError::Other(err) => GetVectoredError::Other(
                    err.context(format!("get_values_reconstruct_data for layer {self}")),
                ),
                err => err,
            })
    }

    /// Download the layer if evicted.
    ///
    /// Will not error when the layer is already downloaded.
    pub(crate) async fn download(&self) -> anyhow::Result<()> {
        self.0.get_or_maybe_download(true, None).await?;
        Ok(())
    }

    pub(crate) async fn needs_download(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        self.0.needs_download().await
    }

    /// Assuming the layer is already downloaded, returns a guard which will prohibit eviction
    /// while the guard exists.
    ///
    /// Returns None if the layer is currently evicted or becoming evicted.
    #[cfg(test)]
    pub(crate) async fn keep_resident(&self) -> Option<ResidentLayer> {
        let downloaded = self.0.inner.get().and_then(|rowe| rowe.get())?;

        Some(ResidentLayer {
            downloaded,
            owner: self.clone(),
        })
    }

    /// Weak indicator of is the layer resident or not. Good enough for eviction, which can deal
    /// with `EvictionError::NotFound`.
    ///
    /// Returns `true` if this layer might be resident, or `false`, if it most likely evicted or
    /// will be unless a read happens soon.
    pub(crate) fn is_likely_resident(&self) -> bool {
        self.0
            .inner
            .get()
            .map(|rowe| rowe.is_likely_resident())
            .unwrap_or(false)
    }

    /// Downloads if necessary and creates a guard, which will keep this layer from being evicted.
    pub(crate) async fn download_and_keep_resident(&self) -> Result<ResidentLayer, DownloadError> {
        let downloaded = self.0.get_or_maybe_download(true, None).await?;

        Ok(ResidentLayer {
            downloaded,
            owner: self.clone(),
        })
    }

    pub(crate) fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        self.0.info(reset)
    }

    pub(crate) fn latest_activity(&self) -> SystemTime {
        self.0.access_stats.latest_activity()
    }

    pub(crate) fn visibility(&self) -> LayerVisibilityHint {
        self.0.access_stats.visibility()
    }

    pub(crate) fn local_path(&self) -> &Utf8Path {
        &self.0.path
    }

    pub(crate) fn metadata(&self) -> LayerFileMetadata {
        self.0.metadata()
    }

    pub(crate) fn get_timeline_id(&self) -> Option<TimelineId> {
        self.0
            .timeline
            .upgrade()
            .map(|timeline| timeline.timeline_id)
    }

    /// Traditional debug dumping facility
    #[allow(unused)]
    pub(crate) async fn dump(&self, verbose: bool, ctx: &RequestContext) -> anyhow::Result<()> {
        self.0.desc.dump();

        if verbose {
            // for now, unconditionally download everything, even if that might not be wanted.
            let l = self.0.get_or_maybe_download(true, Some(ctx)).await?;
            l.dump(&self.0, ctx).await?
        }

        Ok(())
    }

    /// Waits until this layer has been dropped (and if needed, local file deletion and remote
    /// deletion scheduling has completed).
    ///
    /// Does not start local deletion, use [`Self::delete_on_drop`] for that
    /// separatedly.
    #[cfg(any(feature = "testing", test))]
    pub(crate) fn wait_drop(&self) -> impl std::future::Future<Output = ()> + 'static {
        let mut rx = self.0.status.as_ref().unwrap().subscribe();

        async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    fn record_access(&self, ctx: &RequestContext) {
        if self.0.access_stats.record_access(ctx) {
            // Visibility was modified to Visible: maybe log about this
            match ctx.task_kind() {
                TaskKind::CalculateSyntheticSize
                | TaskKind::OndemandLogicalSizeCalculation
                | TaskKind::GarbageCollector
                | TaskKind::MgmtRequest => {
                    // This situation is expected in code paths do binary searches of the LSN space to resolve
                    // an LSN to a timestamp, which happens during GC, during GC cutoff calculations in synthetic size,
                    // and on-demand for certain HTTP API requests. On-demand logical size calculation is also included
                    // because it is run as a sub-task of synthetic size.
                }
                _ => {
                    // In all other contexts, it is unusual to do I/O involving layers which are not visible at
                    // some branch tip, so we log the fact that we are accessing something that the visibility
                    // calculation thought should not be visible.
                    //
                    // This case is legal in brief time windows: for example an in-flight getpage request can hold on to a layer object
                    // which was covered by a concurrent compaction.
                    tracing::info!(
                        layer=%self,
                        "became visible as a result of access",
                    );
                }
            }

            // Update the timeline's visible bytes count
            if let Some(tl) = self.0.timeline.upgrade() {
                tl.metrics
                    .visible_physical_size_gauge
                    .add(self.0.desc.file_size)
            }
        }
    }

    pub(crate) fn set_visibility(&self, visibility: LayerVisibilityHint) {
        let old_visibility = self.0.access_stats.set_visibility(visibility.clone());
        use LayerVisibilityHint::*;
        match (old_visibility, visibility) {
            (Visible, Covered) => {
                // Subtract this layer's contribution to the visible size metric
                if let Some(tl) = self.0.timeline.upgrade() {
                    debug_assert!(
                        tl.metrics.visible_physical_size_gauge.get() >= self.0.desc.file_size
                    );
                    tl.metrics
                        .visible_physical_size_gauge
                        .sub(self.0.desc.file_size)
                }
            }
            (Covered, Visible) => {
                // Add this layer's contribution to the visible size metric
                if let Some(tl) = self.0.timeline.upgrade() {
                    tl.metrics
                        .visible_physical_size_gauge
                        .add(self.0.desc.file_size)
                }
            }
            (Covered, Covered) | (Visible, Visible) => {
                // no change
            }
        }
    }
}

/// The download-ness ([`DownloadedLayer`]) can be either resident or wanted evicted.
///
/// However when we want something evicted, we cannot evict it right away as there might be current
/// reads happening on it. For example: it has been searched from [`LayerMap::search`] but not yet
/// read with [`Layer::get_values_reconstruct_data`].
///
/// [`LayerMap::search`]: crate::tenant::layer_map::LayerMap::search
#[derive(Debug)]
enum ResidentOrWantedEvicted {
    Resident(Arc<DownloadedLayer>),
    WantedEvicted(Weak<DownloadedLayer>, usize),
}

impl ResidentOrWantedEvicted {
    /// Non-mutating access to the a DownloadedLayer, if possible.
    ///
    /// This is not used on the read path (anything that calls
    /// [`LayerInner::get_or_maybe_download`]) because it was decided that reads always win
    /// evictions, and part of that winning is using [`ResidentOrWantedEvicted::get_and_upgrade`].
    #[cfg(test)]
    fn get(&self) -> Option<Arc<DownloadedLayer>> {
        match self {
            ResidentOrWantedEvicted::Resident(strong) => Some(strong.clone()),
            ResidentOrWantedEvicted::WantedEvicted(weak, _) => weak.upgrade(),
        }
    }

    /// Best-effort query for residency right now, not as strong guarantee as receiving a strong
    /// reference from `ResidentOrWantedEvicted::get`.
    fn is_likely_resident(&self) -> bool {
        match self {
            ResidentOrWantedEvicted::Resident(_) => true,
            ResidentOrWantedEvicted::WantedEvicted(weak, _) => weak.strong_count() > 0,
        }
    }

    /// Upgrades any weak to strong if possible.
    ///
    /// Returns a strong reference if possible, along with a boolean telling if an upgrade
    /// happened.
    fn get_and_upgrade(&mut self) -> Option<(Arc<DownloadedLayer>, bool)> {
        match self {
            ResidentOrWantedEvicted::Resident(strong) => Some((strong.clone(), false)),
            ResidentOrWantedEvicted::WantedEvicted(weak, _) => match weak.upgrade() {
                Some(strong) => {
                    LAYER_IMPL_METRICS.inc_raced_wanted_evicted_accesses();

                    *self = ResidentOrWantedEvicted::Resident(strong.clone());

                    Some((strong, true))
                }
                None => None,
            },
        }
    }

    /// When eviction is first requested, drop down to holding a [`Weak`].
    ///
    /// Returns `Some` if this was the first time eviction was requested. Care should be taken to
    /// drop the possibly last strong reference outside of the mutex of
    /// [`heavier_once_cell::OnceCell`].
    fn downgrade(&mut self) -> Option<Arc<DownloadedLayer>> {
        match self {
            ResidentOrWantedEvicted::Resident(strong) => {
                let weak = Arc::downgrade(strong);
                let mut temp = ResidentOrWantedEvicted::WantedEvicted(weak, strong.version);
                std::mem::swap(self, &mut temp);
                match temp {
                    ResidentOrWantedEvicted::Resident(strong) => Some(strong),
                    ResidentOrWantedEvicted::WantedEvicted(..) => unreachable!("just swapped"),
                }
            }
            ResidentOrWantedEvicted::WantedEvicted(..) => None,
        }
    }
}

struct LayerInner {
    /// Only needed to check ondemand_download_behavior_treat_error_as_warn and creation of
    /// [`Self::path`].
    conf: &'static PageServerConf,

    /// Full path to the file; unclear if this should exist anymore.
    path: Utf8PathBuf,

    desc: PersistentLayerDesc,

    /// Timeline access is needed for remote timeline client and metrics.
    ///
    /// There should not be an access to timeline for any reason without entering the
    /// [`Timeline::gate`] at the same time.
    timeline: Weak<Timeline>,

    access_stats: LayerAccessStats,

    /// This custom OnceCell is backed by std mutex, but only held for short time periods.
    ///
    /// Filesystem changes (download, evict) are only done while holding a permit which the
    /// `heavier_once_cell` provides.
    ///
    /// A number of fields in `Layer` are meant to only be updated when holding the InitPermit, but
    /// possibly read while not holding it.
    inner: heavier_once_cell::OnceCell<ResidentOrWantedEvicted>,

    /// Do we want to delete locally and remotely this when `LayerInner` is dropped
    wanted_deleted: AtomicBool,

    /// Version is to make sure we will only evict a specific initialization of the downloaded file.
    ///
    /// Incremented for each initialization, stored in `DownloadedLayer::version` or
    /// `ResidentOrWantedEvicted::WantedEvicted`.
    version: AtomicUsize,

    /// Allow subscribing to when the layer actually gets evicted, a non-cancellable download
    /// starts, or completes.
    ///
    /// Updates must only be posted while holding the InitPermit or the heavier_once_cell::Guard.
    /// Holding the InitPermit is the only time we can do state transitions, but we also need to
    /// cancel a pending eviction on upgrading a [`ResidentOrWantedEvicted::WantedEvicted`] back to
    /// [`ResidentOrWantedEvicted::Resident`] on access.
    ///
    /// The sender is wrapped in an Option to facilitate moving it out on [`LayerInner::drop`].
    status: Option<tokio::sync::watch::Sender<Status>>,

    /// Counter for exponential backoff with the download.
    ///
    /// This is atomic only for the purposes of having additional data only accessed while holding
    /// the InitPermit.
    consecutive_failures: AtomicUsize,

    /// The generation of this Layer.
    ///
    /// For loaded layers (resident or evicted) this comes from [`LayerFileMetadata::generation`],
    /// for created layers from [`Timeline::generation`].
    generation: Generation,

    /// The shard of this Layer.
    ///
    /// For layers created in this process, this will always be the [`ShardIndex`] of the
    /// current `ShardIdentity`` (TODO: add link once it's introduced).
    ///
    /// For loaded layers, this may be some other value if the tenant has undergone
    /// a shard split since the layer was originally written.
    shard: ShardIndex,

    /// When the Layer was last evicted but has not been downloaded since.
    ///
    /// This is used solely for updating metrics. See [`LayerImplMetrics::redownload_after`].
    last_evicted_at: std::sync::Mutex<Option<std::time::Instant>>,

    #[cfg(test)]
    failpoints: std::sync::Mutex<Vec<failpoints::Failpoint>>,
}

impl std::fmt::Display for LayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for LayerInner {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

#[derive(Debug, Clone, Copy)]
enum Status {
    Resident,
    Evicted,
    Downloading,
}

impl Drop for LayerInner {
    fn drop(&mut self) {
        // if there was a pending eviction, mark it cancelled here to balance metrics
        if let Some((ResidentOrWantedEvicted::WantedEvicted(..), _)) = self.inner.take_and_deinit()
        {
            // eviction has already been started
            LAYER_IMPL_METRICS.inc_eviction_cancelled(EvictionCancelled::LayerGone);

            // eviction request is intentionally not honored as no one is present to wait for it
            // and we could be delaying shutdown for nothing.
        }

        let timeline = self.timeline.upgrade();

        if let Some(timeline) = timeline.as_ref() {
            // Only need to decrement metrics if the timeline still exists: otherwise
            // it will have already de-registered these metrics via TimelineMetrics::shutdown
            if self.desc.is_delta() {
                timeline.metrics.layer_count_delta.dec();
                timeline.metrics.layer_size_delta.sub(self.desc.file_size);
            } else {
                timeline.metrics.layer_count_image.dec();
                timeline.metrics.layer_size_image.sub(self.desc.file_size);
            }

            if matches!(self.access_stats.visibility(), LayerVisibilityHint::Visible) {
                debug_assert!(
                    timeline.metrics.visible_physical_size_gauge.get() >= self.desc.file_size
                );
                timeline
                    .metrics
                    .visible_physical_size_gauge
                    .sub(self.desc.file_size);
            }
        }

        if !*self.wanted_deleted.get_mut() {
            return;
        }

        let span = tracing::info_span!(parent: None, "layer_delete", tenant_id = %self.layer_desc().tenant_shard_id.tenant_id, shard_id=%self.layer_desc().tenant_shard_id.shard_slug(), timeline_id = %self.layer_desc().timeline_id);

        let path = std::mem::take(&mut self.path);
        let file_name = self.layer_desc().layer_name();
        let file_size = self.layer_desc().file_size;
        let meta = self.metadata();
        let status = self.status.take();

        Self::spawn_blocking(move || {
            let _g = span.entered();

            // carry this until we are finished for [`Layer::wait_drop`] support
            let _status = status;

            let Some(timeline) = timeline else {
                // no need to nag that timeline is gone: under normal situation on
                // task_mgr::remove_tenant_from_memory the timeline is gone before we get dropped.
                LAYER_IMPL_METRICS.inc_deletes_failed(DeleteFailed::TimelineGone);
                return;
            };

            let Ok(_guard) = timeline.gate.enter() else {
                LAYER_IMPL_METRICS.inc_deletes_failed(DeleteFailed::TimelineGone);
                return;
            };

            let removed = match std::fs::remove_file(path) {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // until we no longer do detaches by removing all local files before removing the
                    // tenant from the global map, we will always get these errors even if we knew what
                    // is the latest state.
                    //
                    // we currently do not track the latest state, so we'll also end up here on evicted
                    // layers.
                    false
                }
                Err(e) => {
                    tracing::error!("failed to remove wanted deleted layer: {e}");
                    LAYER_IMPL_METRICS.inc_delete_removes_failed();
                    false
                }
            };

            if removed {
                timeline.metrics.resident_physical_size_sub(file_size);
            }
            let res = timeline
                .remote_client
                .schedule_deletion_of_unlinked(vec![(file_name, meta)]);

            if let Err(e) = res {
                // test_timeline_deletion_with_files_stuck_in_upload_queue is good at
                // demonstrating this deadlock (without spawn_blocking): stop will drop
                // queued items, which will have ResidentLayer's, and those drops would try
                // to re-entrantly lock the RemoteTimelineClient inner state.
                if !timeline.is_active() {
                    tracing::info!("scheduling deletion on drop failed: {e:#}");
                } else {
                    tracing::warn!("scheduling deletion on drop failed: {e:#}");
                }
                LAYER_IMPL_METRICS.inc_deletes_failed(DeleteFailed::DeleteSchedulingFailed);
            } else {
                LAYER_IMPL_METRICS.inc_completed_deletes();
            }
        });
    }
}

impl LayerInner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        conf: &'static PageServerConf,
        timeline: &Arc<Timeline>,
        local_path: Utf8PathBuf,
        desc: PersistentLayerDesc,
        downloaded: Option<Arc<DownloadedLayer>>,
        generation: Generation,
        shard: ShardIndex,
    ) -> Self {
        let (inner, version, init_status) = if let Some(inner) = downloaded {
            let version = inner.version;
            let resident = ResidentOrWantedEvicted::Resident(inner);
            (
                heavier_once_cell::OnceCell::new(resident),
                version,
                Status::Resident,
            )
        } else {
            (heavier_once_cell::OnceCell::default(), 0, Status::Evicted)
        };

        // This object acts as a RAII guard on these metrics: increment on construction
        if desc.is_delta() {
            timeline.metrics.layer_count_delta.inc();
            timeline.metrics.layer_size_delta.add(desc.file_size);
        } else {
            timeline.metrics.layer_count_image.inc();
            timeline.metrics.layer_size_image.add(desc.file_size);
        }

        // New layers are visible by default. This metric is later updated on drop or in set_visibility
        timeline
            .metrics
            .visible_physical_size_gauge
            .add(desc.file_size);

        LayerInner {
            conf,
            path: local_path,
            desc,
            timeline: Arc::downgrade(timeline),
            access_stats: Default::default(),
            wanted_deleted: AtomicBool::new(false),
            inner,
            version: AtomicUsize::new(version),
            status: Some(tokio::sync::watch::channel(init_status).0),
            consecutive_failures: AtomicUsize::new(0),
            generation,
            shard,
            last_evicted_at: std::sync::Mutex::default(),
            #[cfg(test)]
            failpoints: Default::default(),
        }
    }

    fn delete_on_drop(&self) {
        let res =
            self.wanted_deleted
                .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed);

        if res.is_ok() {
            LAYER_IMPL_METRICS.inc_started_deletes();
        }
    }

    /// Cancellation safe, however dropping the future and calling this method again might result
    /// in a new attempt to evict OR join the previously started attempt.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all, ret, err(level = tracing::Level::DEBUG), fields(layer=%self))]
    pub(crate) async fn evict_and_wait(&self, timeout: Duration) -> Result<(), EvictionError> {
        let mut rx = self.status.as_ref().unwrap().subscribe();

        {
            let current = rx.borrow_and_update();
            match &*current {
                Status::Resident => {
                    // we might get lucky and evict this; continue
                }
                Status::Evicted | Status::Downloading => {
                    // it is already evicted
                    return Err(EvictionError::NotFound);
                }
            }
        }

        let strong = {
            match self.inner.get() {
                Some(mut either) => either.downgrade(),
                None => {
                    // we already have a scheduled eviction, which just has not gotten to run yet.
                    // it might still race with a read access, but that could also get cancelled,
                    // so let's say this is not evictable.
                    return Err(EvictionError::NotFound);
                }
            }
        };

        if strong.is_some() {
            // drop the DownloadedLayer outside of the holding the guard
            drop(strong);

            // idea here is that only one evicter should ever get to witness a strong reference,
            // which means whenever get_or_maybe_download upgrades a weak, it must mark up a
            // cancelled eviction and signal us, like it currently does.
            //
            // a second concurrent evict_and_wait will not see a strong reference.
            LAYER_IMPL_METRICS.inc_started_evictions();
        }

        let changed = rx.changed();
        let changed = tokio::time::timeout(timeout, changed).await;

        let Ok(changed) = changed else {
            return Err(EvictionError::Timeout);
        };

        let _: () = changed.expect("cannot be closed, because we are holding a strong reference");

        let current = rx.borrow_and_update();

        match &*current {
            // the easiest case
            Status::Evicted => Ok(()),
            // it surely was evicted in between, but then there was a new access now; we can't know
            // if it'll succeed so lets just call it evicted
            Status::Downloading => Ok(()),
            // either the download which was started after eviction completed already, or it was
            // never evicted
            Status::Resident => Err(EvictionError::Downloaded),
        }
    }

    /// Cancellation safe.
    async fn get_or_maybe_download(
        self: &Arc<Self>,
        allow_download: bool,
        ctx: Option<&RequestContext>,
    ) -> Result<Arc<DownloadedLayer>, DownloadError> {
        let (weak, permit) = {
            // get_or_init_detached can:
            // - be fast (mutex lock) OR uncontested semaphore permit acquire
            // - be slow (wait for semaphore permit or closing)
            let init_cancelled = scopeguard::guard((), |_| LAYER_IMPL_METRICS.inc_init_cancelled());

            let locked = self
                .inner
                .get_or_init_detached()
                .await
                .map(|mut guard| guard.get_and_upgrade().ok_or(guard));

            scopeguard::ScopeGuard::into_inner(init_cancelled);

            match locked {
                // this path could had been a RwLock::read
                Ok(Ok((strong, upgraded))) if !upgraded => return Ok(strong),
                Ok(Ok((strong, _))) => {
                    // when upgraded back, the Arc<DownloadedLayer> is still available, but
                    // previously a `evict_and_wait` was received. this is the only place when we
                    // send out an update without holding the InitPermit.
                    //
                    // note that we also have dropped the Guard; this is fine, because we just made
                    // a state change and are holding a strong reference to be returned.
                    self.status.as_ref().unwrap().send_replace(Status::Resident);
                    LAYER_IMPL_METRICS
                        .inc_eviction_cancelled(EvictionCancelled::UpgradedBackOnAccess);

                    return Ok(strong);
                }
                Ok(Err(guard)) => {
                    // path to here: we won the eviction, the file should still be on the disk.
                    let (weak, permit) = guard.take_and_deinit();
                    (Some(weak), permit)
                }
                Err(permit) => (None, permit),
            }
        };

        if let Some(weak) = weak {
            // only drop the weak after dropping the heavier_once_cell guard
            assert!(
                matches!(weak, ResidentOrWantedEvicted::WantedEvicted(..)),
                "unexpected {weak:?}, ResidentOrWantedEvicted::get_and_upgrade has a bug"
            );
        }

        let timeline = self
            .timeline
            .upgrade()
            .ok_or(DownloadError::TimelineShutdown)?;

        // count cancellations, which currently remain largely unexpected
        let init_cancelled = scopeguard::guard((), |_| LAYER_IMPL_METRICS.inc_init_cancelled());

        // check if we really need to be downloaded: this can happen if a read access won the
        // semaphore before eviction.
        //
        // if we are cancelled while doing this `stat` the `self.inner` will be uninitialized. a
        // pending eviction will try to evict even upon finding an uninitialized `self.inner`.
        let needs_download = self
            .needs_download()
            .await
            .map_err(DownloadError::PreStatFailed);

        scopeguard::ScopeGuard::into_inner(init_cancelled);

        let needs_download = needs_download?;

        let Some(reason) = needs_download else {
            // the file is present locally because eviction has not had a chance to run yet

            #[cfg(test)]
            self.failpoint(failpoints::FailpointKind::AfterDeterminingLayerNeedsNoDownload)
                .await?;

            LAYER_IMPL_METRICS.inc_init_needed_no_download();

            return Ok(self.initialize_after_layer_is_on_disk(permit));
        };

        // we must download; getting cancelled before spawning the download is not an issue as
        // any still running eviction would not find anything to evict.

        if let NeedsDownload::NotFile(ft) = reason {
            return Err(DownloadError::NotFile(ft));
        }

        if let Some(ctx) = ctx {
            self.check_expected_download(ctx)?;
        }

        if !allow_download {
            // this is only used from tests, but it is hard to test without the boolean
            return Err(DownloadError::DownloadRequired);
        }

        let download_ctx = ctx
            .map(|ctx| ctx.detached_child(TaskKind::LayerDownload, DownloadBehavior::Download))
            .unwrap_or(RequestContext::new(
                TaskKind::LayerDownload,
                DownloadBehavior::Download,
            ));

        async move {
            tracing::info!(%reason, "downloading on-demand");

            let init_cancelled = scopeguard::guard((), |_| LAYER_IMPL_METRICS.inc_init_cancelled());
            let res = self
                .download_init_and_wait(timeline, permit, download_ctx)
                .await?;
            scopeguard::ScopeGuard::into_inner(init_cancelled);
            Ok(res)
        }
        .instrument(tracing::info_span!("get_or_maybe_download", layer=%self))
        .await
    }

    /// Nag or fail per RequestContext policy
    fn check_expected_download(&self, ctx: &RequestContext) -> Result<(), DownloadError> {
        use crate::context::DownloadBehavior::*;
        let b = ctx.download_behavior();
        match b {
            Download => Ok(()),
            Warn | Error => {
                tracing::info!(
                    "unexpectedly on-demand downloading for task kind {:?}",
                    ctx.task_kind()
                );
                crate::metrics::UNEXPECTED_ONDEMAND_DOWNLOADS.inc();

                let really_error =
                    matches!(b, Error) && !self.conf.ondemand_download_behavior_treat_error_as_warn;

                if really_error {
                    // this check is only probablistic, seems like flakyness footgun
                    Err(DownloadError::ContextAndConfigReallyDeniesDownloads)
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Actual download, at most one is executed at the time.
    async fn download_init_and_wait(
        self: &Arc<Self>,
        timeline: Arc<Timeline>,
        permit: heavier_once_cell::InitPermit,
        ctx: RequestContext,
    ) -> Result<Arc<DownloadedLayer>, DownloadError> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        let (tx, rx) = tokio::sync::oneshot::channel();

        let this: Arc<Self> = self.clone();

        let guard = timeline
            .gate
            .enter()
            .map_err(|_| DownloadError::DownloadCancelled)?;

        Self::spawn(
            async move {
                let _guard = guard;

                // now that we have commited to downloading, send out an update to:
                // - unhang any pending eviction
                // - break out of evict_and_wait
                this.status
                    .as_ref()
                    .unwrap()
                    .send_replace(Status::Downloading);

                #[cfg(test)]
                this.failpoint(failpoints::FailpointKind::WaitBeforeDownloading)
                    .await
                    .unwrap();

                let res = this.download_and_init(timeline, permit, &ctx).await;

                if let Err(res) = tx.send(res) {
                    match res {
                        Ok(_res) => {
                            tracing::debug!("layer initialized, but caller has been cancelled");
                            LAYER_IMPL_METRICS.inc_init_completed_without_requester();
                        }
                        Err(e) => {
                            tracing::info!(
                                "layer file download failed, and caller has been cancelled: {e:?}"
                            );
                            LAYER_IMPL_METRICS.inc_download_failed_without_requester();
                        }
                    }
                }
            }
            .in_current_span(),
        );

        match rx.await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(remote_storage::DownloadError::Cancelled)) => {
                Err(DownloadError::DownloadCancelled)
            }
            Ok(Err(_)) => Err(DownloadError::DownloadFailed),
            Err(_gone) => Err(DownloadError::DownloadCancelled),
        }
    }

    async fn download_and_init(
        self: &Arc<LayerInner>,
        timeline: Arc<Timeline>,
        permit: heavier_once_cell::InitPermit,
        ctx: &RequestContext,
    ) -> Result<Arc<DownloadedLayer>, remote_storage::DownloadError> {
        let result = timeline
            .remote_client
            .download_layer_file(
                &self.desc.layer_name(),
                &self.metadata(),
                &self.path,
                &timeline.gate,
                &timeline.cancel,
                ctx,
            )
            .await;

        match result {
            Ok(size) => {
                assert_eq!(size, self.desc.file_size);

                match self.needs_download().await {
                    Ok(Some(reason)) => {
                        // this is really a bug in needs_download or remote timeline client
                        panic!("post-condition failed: needs_download returned {reason:?}");
                    }
                    Ok(None) => {
                        // as expected
                    }
                    Err(e) => {
                        panic!("post-condition failed: needs_download errored: {e:?}");
                    }
                }

                tracing::info!(size=%self.desc.file_size, "on-demand download successful");
                timeline
                    .metrics
                    .resident_physical_size_add(self.desc.file_size);
                self.consecutive_failures.store(0, Ordering::Relaxed);

                let since_last_eviction = self
                    .last_evicted_at
                    .lock()
                    .unwrap()
                    .take()
                    .map(|ts| ts.elapsed());
                if let Some(since_last_eviction) = since_last_eviction {
                    LAYER_IMPL_METRICS.record_redownloaded_after(since_last_eviction);
                }

                self.access_stats.record_residence_event();

                Ok(self.initialize_after_layer_is_on_disk(permit))
            }
            Err(e) => {
                let consecutive_failures =
                    1 + self.consecutive_failures.fetch_add(1, Ordering::Relaxed);

                if timeline.cancel.is_cancelled() {
                    // If we're shutting down, drop out before logging the error
                    return Err(e);
                }

                tracing::error!(consecutive_failures, "layer file download failed: {e:#}");

                let backoff = utils::backoff::exponential_backoff_duration_seconds(
                    consecutive_failures.min(u32::MAX as usize) as u32,
                    1.5,
                    60.0,
                );

                let backoff = std::time::Duration::from_secs_f64(backoff);

                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {},
                    _ = timeline.cancel.cancelled() => {},
                };

                Err(e)
            }
        }
    }

    /// Initializes the `Self::inner` to a "resident" state.
    ///
    /// Callers are assumed to ensure that the file is actually on disk with `Self::needs_download`
    /// before calling this method.
    ///
    /// If this method is ever made async, it needs to be cancellation safe so that no state
    /// changes are made before we can write to the OnceCell in non-cancellable fashion.
    fn initialize_after_layer_is_on_disk(
        self: &Arc<LayerInner>,
        permit: heavier_once_cell::InitPermit,
    ) -> Arc<DownloadedLayer> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        // disable any scheduled but not yet running eviction deletions for this initialization
        let next_version = 1 + self.version.fetch_add(1, Ordering::Relaxed);
        self.status.as_ref().unwrap().send_replace(Status::Resident);

        let res = Arc::new(DownloadedLayer {
            owner: Arc::downgrade(self),
            kind: tokio::sync::OnceCell::default(),
            version: next_version,
        });

        let waiters = self.inner.initializer_count();
        if waiters > 0 {
            tracing::info!(waiters, "completing layer init for other tasks");
        }

        let value = ResidentOrWantedEvicted::Resident(res.clone());

        self.inner.set(value, permit);

        res
    }

    async fn needs_download(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match tokio::fs::metadata(&self.path).await {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m).err()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn needs_download_blocking(&self) -> Result<Option<NeedsDownload>, std::io::Error> {
        match self.path.metadata() {
            Ok(m) => Ok(self.is_file_present_and_good_size(&m).err()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(NeedsDownload::NotFound)),
            Err(e) => Err(e),
        }
    }

    fn is_file_present_and_good_size(&self, m: &std::fs::Metadata) -> Result<(), NeedsDownload> {
        // in future, this should include sha2-256 validation of the file.
        if !m.is_file() {
            Err(NeedsDownload::NotFile(m.file_type()))
        } else if m.len() != self.desc.file_size {
            Err(NeedsDownload::WrongSize {
                actual: m.len(),
                expected: self.desc.file_size,
            })
        } else {
            Ok(())
        }
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo {
        let layer_name = self.desc.layer_name().to_string();

        let resident = self
            .inner
            .get()
            .map(|rowe| rowe.is_likely_resident())
            .unwrap_or(false);

        let access_stats = self.access_stats.as_api_model(reset);

        if self.desc.is_delta {
            let lsn_range = &self.desc.lsn_range;

            HistoricLayerInfo::Delta {
                layer_file_name: layer_name,
                layer_file_size: self.desc.file_size,
                lsn_start: lsn_range.start,
                lsn_end: lsn_range.end,
                remote: !resident,
                access_stats,
                l0: crate::tenant::layer_map::LayerMap::is_l0(
                    &self.layer_desc().key_range,
                    self.layer_desc().is_delta,
                ),
            }
        } else {
            let lsn = self.desc.image_layer_lsn();

            HistoricLayerInfo::Image {
                layer_file_name: layer_name,
                layer_file_size: self.desc.file_size,
                lsn_start: lsn,
                remote: !resident,
                access_stats,
            }
        }
    }

    /// `DownloadedLayer` is being dropped, so it calls this method.
    fn on_downloaded_layer_drop(self: Arc<LayerInner>, only_version: usize) {
        // we cannot know without inspecting LayerInner::inner if we should evict or not, even
        // though here it is very likely
        let span = tracing::info_span!(parent: None, "layer_evict", tenant_id = %self.desc.tenant_shard_id.tenant_id, shard_id = %self.desc.tenant_shard_id.shard_slug(), timeline_id = %self.desc.timeline_id, layer=%self, version=%only_version);

        // NOTE: this scope *must* never call `self.inner.get` because evict_and_wait might
        // drop while the `self.inner` is being locked, leading to a deadlock.

        let start_evicting = async move {
            #[cfg(test)]
            self.failpoint(failpoints::FailpointKind::WaitBeforeStartingEvicting)
                .await
                .expect("failpoint should not have errored");

            tracing::debug!("eviction started");

            let res = self.wait_for_turn_and_evict(only_version).await;
            // metrics: ignore the Ok branch, it is not done yet
            if let Err(e) = res {
                tracing::debug!(res=?Err::<(), _>(&e), "eviction completed");
                LAYER_IMPL_METRICS.inc_eviction_cancelled(e);
            }
        };

        Self::spawn(start_evicting.instrument(span));
    }

    async fn wait_for_turn_and_evict(
        self: Arc<LayerInner>,
        only_version: usize,
    ) -> Result<(), EvictionCancelled> {
        fn is_good_to_continue(status: &Status) -> Result<(), EvictionCancelled> {
            use Status::*;
            match status {
                Resident => Ok(()),
                Evicted => Err(EvictionCancelled::UnexpectedEvictedState),
                Downloading => Err(EvictionCancelled::LostToDownload),
            }
        }

        let timeline = self
            .timeline
            .upgrade()
            .ok_or(EvictionCancelled::TimelineGone)?;

        let mut rx = self
            .status
            .as_ref()
            .expect("LayerInner cannot be dropped, holding strong ref")
            .subscribe();

        is_good_to_continue(&rx.borrow_and_update())?;

        let Ok(gate) = timeline.gate.enter() else {
            return Err(EvictionCancelled::TimelineGone);
        };

        let permit = {
            // we cannot just `std::fs::remove_file` because there might already be an
            // get_or_maybe_download which will inspect filesystem and reinitialize. filesystem
            // operations must be done while holding the heavier_once_cell::InitPermit
            let mut wait = std::pin::pin!(self.inner.get_or_init_detached());

            let waited = loop {
                // we must race to the Downloading starting, otherwise we would have to wait until the
                // completion of the download. waiting for download could be long and hinder our
                // efforts to alert on "hanging" evictions.
                tokio::select! {
                    res = &mut wait => break res,
                    _ = rx.changed() => {
                        is_good_to_continue(&rx.borrow_and_update())?;
                        // two possibilities for Status::Resident:
                        // - the layer was found locally from disk by a read
                        // - we missed a bunch of updates and now the layer is
                        // again downloaded -- assume we'll fail later on with
                        // version check or AlreadyReinitialized
                    }
                }
            };

            // re-check now that we have the guard or permit; all updates should have happened
            // while holding the permit.
            is_good_to_continue(&rx.borrow_and_update())?;

            // the term deinitialize is used here, because we clearing out the Weak will eventually
            // lead to deallocating the reference counted value, and the value we
            // `Guard::take_and_deinit` is likely to be the last because the Weak is never cloned.
            let (_weak, permit) = match waited {
                Ok(guard) => {
                    match &*guard {
                        ResidentOrWantedEvicted::WantedEvicted(_weak, version)
                            if *version == only_version =>
                        {
                            tracing::debug!(version, "deinitializing matching WantedEvicted");
                            let (weak, permit) = guard.take_and_deinit();
                            (Some(weak), permit)
                        }
                        ResidentOrWantedEvicted::WantedEvicted(_, version) => {
                            // if we were not doing the version check, we would need to try to
                            // upgrade the weak here to see if it really is dropped. version check
                            // is done instead assuming that it is cheaper.
                            tracing::debug!(
                                version,
                                only_version,
                                "version mismatch, not deinitializing"
                            );
                            return Err(EvictionCancelled::VersionCheckFailed);
                        }
                        ResidentOrWantedEvicted::Resident(_) => {
                            return Err(EvictionCancelled::AlreadyReinitialized);
                        }
                    }
                }
                Err(permit) => {
                    tracing::debug!("continuing after cancelled get_or_maybe_download or eviction");
                    (None, permit)
                }
            };

            permit
        };

        let span = tracing::Span::current();

        let spawned_at = std::time::Instant::now();

        // this is on purpose a detached spawn; we don't need to wait for it
        //
        // eviction completion reporting is the only thing hinging on this, and it can be just as
        // well from a spawn_blocking thread.
        //
        // important to note that now that we've acquired the permit we have made sure the evicted
        // file is either the exact `WantedEvicted` we wanted to evict, or uninitialized in case
        // there are multiple evictions. The rest is not cancellable, and we've now commited to
        // evicting.
        //
        // If spawn_blocking has a queue and maximum number of threads are in use, we could stall
        // reads. We will need to add cancellation for that if necessary.
        Self::spawn_blocking(move || {
            let _span = span.entered();

            let res = self.evict_blocking(&timeline, &gate, &permit);

            let waiters = self.inner.initializer_count();

            if waiters > 0 {
                LAYER_IMPL_METRICS.inc_evicted_with_waiters();
            }

            let completed_in = spawned_at.elapsed();
            LAYER_IMPL_METRICS.record_time_to_evict(completed_in);

            match res {
                Ok(()) => LAYER_IMPL_METRICS.inc_completed_evictions(),
                Err(e) => LAYER_IMPL_METRICS.inc_eviction_cancelled(e),
            }

            tracing::debug!(?res, elapsed_ms=%completed_in.as_millis(), %waiters, "eviction completed");
        });

        Ok(())
    }

    /// This is blocking only to do just one spawn_blocking hop compared to multiple via tokio::fs.
    fn evict_blocking(
        &self,
        timeline: &Timeline,
        _gate: &gate::GateGuard,
        _permit: &heavier_once_cell::InitPermit,
    ) -> Result<(), EvictionCancelled> {
        // now accesses to `self.inner.get_or_init*` wait on the semaphore or the `_permit`

        match capture_mtime_and_remove(&self.path) {
            Ok(local_layer_mtime) => {
                let duration = SystemTime::now().duration_since(local_layer_mtime);
                match duration {
                    Ok(elapsed) => {
                        let accessed_and_visible = self.access_stats.accessed()
                            && self.access_stats.visibility() == LayerVisibilityHint::Visible;
                        if accessed_and_visible {
                            // Only layers used for reads contribute to our "low residence" metric that is used
                            // to detect thrashing.  Layers promoted for other reasons (e.g. compaction) are allowed
                            // to be rapidly evicted without contributing to this metric.
                            timeline
                                .metrics
                                .evictions_with_low_residence_duration
                                .read()
                                .unwrap()
                                .observe(elapsed);
                        }

                        tracing::info!(
                            residence_millis = elapsed.as_millis(),
                            accessed_and_visible,
                            "evicted layer after known residence period"
                        );
                    }
                    Err(_) => {
                        tracing::info!("evicted layer after unknown residence period");
                    }
                }
                timeline.metrics.evictions.inc();
                timeline
                    .metrics
                    .resident_physical_size_sub(self.desc.file_size);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::error!(
                    layer_size = %self.desc.file_size,
                    "failed to evict layer from disk, it was already gone"
                );
                return Err(EvictionCancelled::FileNotFound);
            }
            Err(e) => {
                // FIXME: this should probably be an abort
                tracing::error!("failed to evict file from disk: {e:#}");
                return Err(EvictionCancelled::RemoveFailed);
            }
        }

        self.access_stats.record_residence_event();

        self.status.as_ref().unwrap().send_replace(Status::Evicted);

        *self.last_evicted_at.lock().unwrap() = Some(std::time::Instant::now());

        Ok(())
    }

    fn metadata(&self) -> LayerFileMetadata {
        LayerFileMetadata::new(self.desc.file_size, self.generation, self.shard)
    }

    /// Needed to use entered runtime in tests, but otherwise use BACKGROUND_RUNTIME.
    ///
    /// Synchronizing with spawned tasks is very complicated otherwise.
    fn spawn<F>(fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        #[cfg(test)]
        tokio::task::spawn(fut);
        #[cfg(not(test))]
        crate::task_mgr::BACKGROUND_RUNTIME.spawn(fut);
    }

    /// Needed to use entered runtime in tests, but otherwise use BACKGROUND_RUNTIME.
    fn spawn_blocking<F>(f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        #[cfg(test)]
        tokio::task::spawn_blocking(f);
        #[cfg(not(test))]
        crate::task_mgr::BACKGROUND_RUNTIME.spawn_blocking(f);
    }
}

fn capture_mtime_and_remove(path: &Utf8Path) -> Result<SystemTime, std::io::Error> {
    let m = path.metadata()?;
    let local_layer_mtime = m.modified()?;
    std::fs::remove_file(path)?;
    Ok(local_layer_mtime)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EvictionError {
    #[error("layer was already evicted")]
    NotFound,

    /// Evictions must always lose to downloads in races, and this time it happened.
    #[error("layer was downloaded instead")]
    Downloaded,

    #[error("eviction did not happen within timeout")]
    Timeout,
}

/// Error internal to the [`LayerInner::get_or_maybe_download`]
#[derive(Debug, thiserror::Error)]
pub(crate) enum DownloadError {
    #[error("timeline has already shutdown")]
    TimelineShutdown,
    #[error("context denies downloading")]
    ContextAndConfigReallyDeniesDownloads,
    #[error("downloading is really required but not allowed by this method")]
    DownloadRequired,
    #[error("layer path exists, but it is not a file: {0:?}")]
    NotFile(std::fs::FileType),
    /// Why no error here? Because it will be reported by page_service. We should had also done
    /// retries already.
    #[error("downloading evicted layer file failed")]
    DownloadFailed,
    #[error("downloading failed, possibly for shutdown")]
    DownloadCancelled,
    #[error("pre-condition: stat before download failed")]
    PreStatFailed(#[source] std::io::Error),

    #[cfg(test)]
    #[error("failpoint: {0:?}")]
    Failpoint(failpoints::FailpointKind),
}

impl DownloadError {
    pub(crate) fn is_cancelled(&self) -> bool {
        matches!(self, DownloadError::DownloadCancelled)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum NeedsDownload {
    NotFound,
    NotFile(std::fs::FileType),
    WrongSize { actual: u64, expected: u64 },
}

impl std::fmt::Display for NeedsDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NeedsDownload::NotFound => write!(f, "file was not found"),
            NeedsDownload::NotFile(ft) => write!(f, "path is not a file; {ft:?}"),
            NeedsDownload::WrongSize { actual, expected } => {
                write!(f, "file size mismatch {actual} vs. {expected}")
            }
        }
    }
}

/// Existence of `DownloadedLayer` means that we have the file locally, and can later evict it.
pub(crate) struct DownloadedLayer {
    owner: Weak<LayerInner>,
    // Use tokio OnceCell as we do not need to deinitialize this, it'll just get dropped with the
    // DownloadedLayer
    kind: tokio::sync::OnceCell<anyhow::Result<LayerKind>>,
    version: usize,
}

impl std::fmt::Debug for DownloadedLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadedLayer")
            // owner omitted because it is always "Weak"
            .field("kind", &self.kind)
            .field("version", &self.version)
            .finish()
    }
}

impl Drop for DownloadedLayer {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.on_downloaded_layer_drop(self.version);
        } else {
            // Layer::drop will handle cancelling the eviction; because of drop order and
            // `DownloadedLayer` never leaking, we cannot know here if eviction was requested.
        }
    }
}

impl DownloadedLayer {
    /// Initializes the `DeltaLayerInner` or `ImageLayerInner` within [`LayerKind`].
    /// Failure to load the layer is sticky, i.e., future `get()` calls will return
    /// the initial load failure immediately.
    ///
    /// `owner` parameter is a strong reference at the same `LayerInner` as the
    /// `DownloadedLayer::owner` would be when upgraded. Given how this method ends up called,
    /// we will always have the LayerInner on the callstack, so we can just use it.
    async fn get<'a>(
        &'a self,
        owner: &Arc<LayerInner>,
        ctx: &RequestContext,
    ) -> anyhow::Result<&'a LayerKind> {
        let init = || async {
            assert_eq!(
                Weak::as_ptr(&self.owner),
                Arc::as_ptr(owner),
                "these are the same, just avoiding the upgrade"
            );

            let res = if owner.desc.is_delta {
                let ctx = RequestContextBuilder::extend(ctx)
                    .page_content_kind(crate::context::PageContentKind::DeltaLayerSummary)
                    .build();
                let summary = Some(delta_layer::Summary::expected(
                    owner.desc.tenant_shard_id.tenant_id,
                    owner.desc.timeline_id,
                    owner.desc.key_range.clone(),
                    owner.desc.lsn_range.clone(),
                ));
                delta_layer::DeltaLayerInner::load(
                    &owner.path,
                    summary,
                    Some(owner.conf.max_vectored_read_bytes),
                    &ctx,
                )
                .await
                .map(LayerKind::Delta)
            } else {
                let ctx = RequestContextBuilder::extend(ctx)
                    .page_content_kind(crate::context::PageContentKind::ImageLayerSummary)
                    .build();
                let lsn = owner.desc.image_layer_lsn();
                let summary = Some(image_layer::Summary::expected(
                    owner.desc.tenant_shard_id.tenant_id,
                    owner.desc.timeline_id,
                    owner.desc.key_range.clone(),
                    lsn,
                ));
                image_layer::ImageLayerInner::load(
                    &owner.path,
                    lsn,
                    summary,
                    Some(owner.conf.max_vectored_read_bytes),
                    &ctx,
                )
                .await
                .map(LayerKind::Image)
            };

            match res {
                Ok(layer) => Ok(layer),
                Err(err) => {
                    LAYER_IMPL_METRICS.inc_permanent_loading_failures();
                    // We log this message once over the lifetime of `Self`
                    // => Ok and good to log backtrace and path here.
                    tracing::error!(
                        "layer load failed, assuming permanent failure: {}: {err:?}",
                        owner.path
                    );
                    Err(err)
                }
            }
        };
        self.kind
            .get_or_init(init)
            .await
            .as_ref()
            // We already logged the full backtrace above, once. Don't repeat that here.
            .map_err(|e| anyhow::anyhow!("layer load failed earlier: {e}"))
    }

    async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValuesReconstructState,
        owner: &Arc<LayerInner>,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        use LayerKind::*;

        match self
            .get(owner, ctx)
            .await
            .map_err(GetVectoredError::Other)?
        {
            Delta(d) => {
                d.get_values_reconstruct_data(keyspace, lsn_range, reconstruct_data, ctx)
                    .await
            }
            Image(i) => {
                i.get_values_reconstruct_data(keyspace, reconstruct_data, ctx)
                    .await
            }
        }
    }

    async fn dump(&self, owner: &Arc<LayerInner>, ctx: &RequestContext) -> anyhow::Result<()> {
        use LayerKind::*;
        match self.get(owner, ctx).await? {
            Delta(d) => d.dump(ctx).await?,
            Image(i) => i.dump(ctx).await?,
        }

        Ok(())
    }
}

/// Wrapper around an actual layer implementation.
#[derive(Debug)]
enum LayerKind {
    Delta(delta_layer::DeltaLayerInner),
    Image(image_layer::ImageLayerInner),
}

/// Guard for forcing a layer be resident while it exists.
#[derive(Clone)]
pub struct ResidentLayer {
    owner: Layer,
    downloaded: Arc<DownloadedLayer>,
}

impl std::fmt::Display for ResidentLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.owner)
    }
}

impl std::fmt::Debug for ResidentLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.owner)
    }
}

impl ResidentLayer {
    /// Release the eviction guard, converting back into a plain [`Layer`].
    ///
    /// You can access the [`Layer`] also by using `as_ref`.
    pub(crate) fn drop_eviction_guard(self) -> Layer {
        self.into()
    }

    /// Loads all keys stored in the layer. Returns key, lsn and value size.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all, fields(layer=%self))]
    pub(crate) async fn load_keys<'a>(
        &'a self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<pageserver_api::key::Key>> {
        use LayerKind::*;

        let owner = &self.owner.0;
        let inner = self.downloaded.get(owner, ctx).await?;

        // this is valid because the DownloadedLayer::kind is a OnceCell, not a
        // Mutex<OnceCell>, so we cannot go and deinitialize the value with OnceCell::take
        // while it's being held.
        self.owner.record_access(ctx);

        let res = match inner {
            Delta(ref d) => delta_layer::DeltaLayerInner::load_keys(d, ctx).await,
            Image(ref i) => image_layer::ImageLayerInner::load_keys(i, ctx).await,
        };
        res.with_context(|| format!("Layer index is corrupted for {self}"))
    }

    /// Read all they keys in this layer which match the ShardIdentity, and write them all to
    /// the provided writer.  Return the number of keys written.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all, fields(layer=%self))]
    pub(crate) async fn filter(
        &self,
        shard_identity: &ShardIdentity,
        writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> Result<usize, CompactionError> {
        use LayerKind::*;

        match self
            .downloaded
            .get(&self.owner.0, ctx)
            .await
            .map_err(CompactionError::Other)?
        {
            Delta(_) => {
                return Err(CompactionError::Other(anyhow::anyhow!(format!(
                    "cannot filter() on a delta layer {self}"
                ))));
            }
            Image(i) => i
                .filter(shard_identity, writer, ctx)
                .await
                .map_err(CompactionError::Other),
        }
    }

    /// Returns the amount of keys and values written to the writer.
    pub(crate) async fn copy_delta_prefix(
        &self,
        writer: &mut super::delta_layer::DeltaLayerWriter,
        until: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        use LayerKind::*;

        let owner = &self.owner.0;

        match self.downloaded.get(owner, ctx).await? {
            Delta(ref d) => d
                .copy_prefix(writer, until, ctx)
                .await
                .with_context(|| format!("copy_delta_prefix until {until} of {self}")),
            Image(_) => anyhow::bail!(format!("cannot copy_lsn_prefix of image layer {self}")),
        }
    }

    pub(crate) fn local_path(&self) -> &Utf8Path {
        &self.owner.0.path
    }

    pub(crate) fn metadata(&self) -> LayerFileMetadata {
        self.owner.metadata()
    }

    /// Cast the layer to a delta, return an error if it is an image layer.
    pub(crate) async fn get_as_delta(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<&delta_layer::DeltaLayerInner> {
        use LayerKind::*;
        match self.downloaded.get(&self.owner.0, ctx).await? {
            Delta(ref d) => Ok(d),
            Image(_) => Err(anyhow::anyhow!("image layer")),
        }
    }

    /// Cast the layer to an image, return an error if it is a delta layer.
    pub(crate) async fn get_as_image(
        &self,
        ctx: &RequestContext,
    ) -> anyhow::Result<&image_layer::ImageLayerInner> {
        use LayerKind::*;
        match self.downloaded.get(&self.owner.0, ctx).await? {
            Image(ref d) => Ok(d),
            Delta(_) => Err(anyhow::anyhow!("delta layer")),
        }
    }
}

impl AsLayerDesc for ResidentLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        self.owner.layer_desc()
    }
}

impl AsRef<Layer> for ResidentLayer {
    fn as_ref(&self) -> &Layer {
        &self.owner
    }
}

/// Drop the eviction guard.
impl From<ResidentLayer> for Layer {
    fn from(value: ResidentLayer) -> Self {
        value.owner
    }
}

use metrics::IntCounter;

pub(crate) struct LayerImplMetrics {
    started_evictions: IntCounter,
    completed_evictions: IntCounter,
    cancelled_evictions: enum_map::EnumMap<EvictionCancelled, IntCounter>,

    started_deletes: IntCounter,
    completed_deletes: IntCounter,
    failed_deletes: enum_map::EnumMap<DeleteFailed, IntCounter>,

    rare_counters: enum_map::EnumMap<RareEvent, IntCounter>,
    inits_cancelled: metrics::core::GenericCounter<metrics::core::AtomicU64>,
    redownload_after: metrics::Histogram,
    time_to_evict: metrics::Histogram,
}

impl Default for LayerImplMetrics {
    fn default() -> Self {
        use enum_map::Enum;

        // reminder: these will be pageserver_layer_* with "_total" suffix

        let started_evictions = metrics::register_int_counter!(
            "pageserver_layer_started_evictions",
            "Evictions started in the Layer implementation"
        )
        .unwrap();
        let completed_evictions = metrics::register_int_counter!(
            "pageserver_layer_completed_evictions",
            "Evictions completed in the Layer implementation"
        )
        .unwrap();

        let cancelled_evictions = metrics::register_int_counter_vec!(
            "pageserver_layer_cancelled_evictions_count",
            "Different reasons for evictions to have been cancelled or failed",
            &["reason"]
        )
        .unwrap();

        let cancelled_evictions = enum_map::EnumMap::from_array(std::array::from_fn(|i| {
            let reason = EvictionCancelled::from_usize(i);
            let s = reason.as_str();
            cancelled_evictions.with_label_values(&[s])
        }));

        let started_deletes = metrics::register_int_counter!(
            "pageserver_layer_started_deletes",
            "Deletions on drop pending in the Layer implementation"
        )
        .unwrap();
        let completed_deletes = metrics::register_int_counter!(
            "pageserver_layer_completed_deletes",
            "Deletions on drop completed in the Layer implementation"
        )
        .unwrap();

        let failed_deletes = metrics::register_int_counter_vec!(
            "pageserver_layer_failed_deletes_count",
            "Different reasons for deletions on drop to have failed",
            &["reason"]
        )
        .unwrap();

        let failed_deletes = enum_map::EnumMap::from_array(std::array::from_fn(|i| {
            let reason = DeleteFailed::from_usize(i);
            let s = reason.as_str();
            failed_deletes.with_label_values(&[s])
        }));

        let rare_counters = metrics::register_int_counter_vec!(
            "pageserver_layer_assumed_rare_count",
            "Times unexpected or assumed rare event happened",
            &["event"]
        )
        .unwrap();

        let rare_counters = enum_map::EnumMap::from_array(std::array::from_fn(|i| {
            let event = RareEvent::from_usize(i);
            let s = event.as_str();
            rare_counters.with_label_values(&[s])
        }));

        let inits_cancelled = metrics::register_int_counter!(
            "pageserver_layer_inits_cancelled_count",
            "Times Layer initialization was cancelled",
        )
        .unwrap();

        let redownload_after = {
            let minute = 60.0;
            let hour = 60.0 * minute;
            metrics::register_histogram!(
                "pageserver_layer_redownloaded_after",
                "Time between evicting and re-downloading.",
                vec![
                    10.0,
                    30.0,
                    minute,
                    5.0 * minute,
                    15.0 * minute,
                    30.0 * minute,
                    hour,
                    12.0 * hour,
                ]
            )
            .unwrap()
        };

        let time_to_evict = metrics::register_histogram!(
            "pageserver_layer_eviction_held_permit_seconds",
            "Time eviction held the permit.",
            vec![0.001, 0.010, 0.100, 0.500, 1.000, 5.000]
        )
        .unwrap();

        Self {
            started_evictions,
            completed_evictions,
            cancelled_evictions,

            started_deletes,
            completed_deletes,
            failed_deletes,

            rare_counters,
            inits_cancelled,
            redownload_after,
            time_to_evict,
        }
    }
}

impl LayerImplMetrics {
    fn inc_started_evictions(&self) {
        self.started_evictions.inc();
    }
    fn inc_completed_evictions(&self) {
        self.completed_evictions.inc();
    }
    fn inc_eviction_cancelled(&self, reason: EvictionCancelled) {
        self.cancelled_evictions[reason].inc()
    }

    fn inc_started_deletes(&self) {
        self.started_deletes.inc();
    }
    fn inc_completed_deletes(&self) {
        self.completed_deletes.inc();
    }
    fn inc_deletes_failed(&self, reason: DeleteFailed) {
        self.failed_deletes[reason].inc();
    }

    /// Counted separatedly from failed layer deletes because we will complete the layer deletion
    /// attempt regardless of failure to delete local file.
    fn inc_delete_removes_failed(&self) {
        self.rare_counters[RareEvent::RemoveOnDropFailed].inc();
    }

    /// Expected rare just as cancellations are rare, but we could have cancellations separate from
    /// the single caller which can start the download, so use this counter to separte them.
    fn inc_init_completed_without_requester(&self) {
        self.rare_counters[RareEvent::InitCompletedWithoutRequester].inc();
    }

    /// Expected rare because cancellations are unexpected, and failures are unexpected
    fn inc_download_failed_without_requester(&self) {
        self.rare_counters[RareEvent::DownloadFailedWithoutRequester].inc();
    }

    /// The Weak in ResidentOrWantedEvicted::WantedEvicted was successfully upgraded.
    ///
    /// If this counter is always zero, we should replace ResidentOrWantedEvicted type with an
    /// Option.
    fn inc_raced_wanted_evicted_accesses(&self) {
        self.rare_counters[RareEvent::UpgradedWantedEvicted].inc();
    }

    /// These are only expected for [`Self::inc_init_cancelled`] amount when
    /// running with remote storage.
    fn inc_init_needed_no_download(&self) {
        self.rare_counters[RareEvent::InitWithoutDownload].inc();
    }

    /// Expected rare because all layer files should be readable and good
    fn inc_permanent_loading_failures(&self) {
        self.rare_counters[RareEvent::PermanentLoadingFailure].inc();
    }

    fn inc_init_cancelled(&self) {
        self.inits_cancelled.inc()
    }

    fn record_redownloaded_after(&self, duration: std::time::Duration) {
        self.redownload_after.observe(duration.as_secs_f64())
    }

    /// This would be bad if it ever happened, or mean extreme disk pressure. We should probably
    /// instead cancel eviction if we would have read waiters. We cannot however separate reads
    /// from other evictions, so this could have noise as well.
    fn inc_evicted_with_waiters(&self) {
        self.rare_counters[RareEvent::EvictedWithWaiters].inc();
    }

    /// Recorded at least initially as the permit is now acquired in async context before
    /// spawn_blocking action.
    fn record_time_to_evict(&self, duration: std::time::Duration) {
        self.time_to_evict.observe(duration.as_secs_f64())
    }
}

#[derive(Debug, Clone, Copy, enum_map::Enum)]
enum EvictionCancelled {
    LayerGone,
    TimelineGone,
    VersionCheckFailed,
    FileNotFound,
    RemoveFailed,
    AlreadyReinitialized,
    /// Not evicted because of a pending reinitialization
    LostToDownload,
    /// After eviction, there was a new layer access which cancelled the eviction.
    UpgradedBackOnAccess,
    UnexpectedEvictedState,
}

impl EvictionCancelled {
    fn as_str(&self) -> &'static str {
        match self {
            EvictionCancelled::LayerGone => "layer_gone",
            EvictionCancelled::TimelineGone => "timeline_gone",
            EvictionCancelled::VersionCheckFailed => "version_check_fail",
            EvictionCancelled::FileNotFound => "file_not_found",
            EvictionCancelled::RemoveFailed => "remove_failed",
            EvictionCancelled::AlreadyReinitialized => "already_reinitialized",
            EvictionCancelled::LostToDownload => "lost_to_download",
            EvictionCancelled::UpgradedBackOnAccess => "upgraded_back_on_access",
            EvictionCancelled::UnexpectedEvictedState => "unexpected_evicted_state",
        }
    }
}

#[derive(enum_map::Enum)]
enum DeleteFailed {
    TimelineGone,
    DeleteSchedulingFailed,
}

impl DeleteFailed {
    fn as_str(&self) -> &'static str {
        match self {
            DeleteFailed::TimelineGone => "timeline_gone",
            DeleteFailed::DeleteSchedulingFailed => "delete_scheduling_failed",
        }
    }
}

#[derive(enum_map::Enum)]
enum RareEvent {
    RemoveOnDropFailed,
    InitCompletedWithoutRequester,
    DownloadFailedWithoutRequester,
    UpgradedWantedEvicted,
    InitWithoutDownload,
    PermanentLoadingFailure,
    EvictedWithWaiters,
}

impl RareEvent {
    fn as_str(&self) -> &'static str {
        use RareEvent::*;

        match self {
            RemoveOnDropFailed => "remove_on_drop_failed",
            InitCompletedWithoutRequester => "init_completed_without",
            DownloadFailedWithoutRequester => "download_failed_without",
            UpgradedWantedEvicted => "raced_wanted_evicted",
            InitWithoutDownload => "init_needed_no_download",
            PermanentLoadingFailure => "permanent_loading_failure",
            EvictedWithWaiters => "evicted_with_waiters",
        }
    }
}

pub(crate) static LAYER_IMPL_METRICS: once_cell::sync::Lazy<LayerImplMetrics> =
    once_cell::sync::Lazy::new(LayerImplMetrics::default);
