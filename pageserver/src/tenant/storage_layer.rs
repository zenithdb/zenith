//! Common traits and structs for layers

pub mod delta_layer;
mod filename;
mod image_layer;
mod inmemory_layer;
mod layer_desc;
mod remote_layer;

use crate::config::PageServerConf;
use crate::context::{AccessStatsBehavior, RequestContext};
use crate::repository::Key;
use crate::task_mgr::TaskKind;
use crate::walrecord::NeonWalRecord;
use anyhow::Result;
use bytes::Bytes;
use camino::Utf8PathBuf;
use enum_map::EnumMap;
use enumset::EnumSet;
use once_cell::sync::Lazy;
use pageserver_api::models::LayerAccessKind;
use pageserver_api::models::{
    HistoricLayerInfo, LayerResidenceEvent, LayerResidenceEventReason, LayerResidenceStatus,
};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use utils::history_buffer::HistoryBufferWithDropCounter;
use utils::rate_limit::RateLimit;

use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub use delta_layer::{DeltaLayer, DeltaLayerWriter, ValueRef};
pub use filename::{DeltaFileName, ImageFileName, LayerFileName};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use layer_desc::{PersistentLayerDesc, PersistentLayerKey};
pub use remote_layer::RemoteLayer;

pub fn range_overlaps<T>(a: &Range<T>, b: &Range<T>) -> bool
where
    T: PartialOrd<T>,
{
    if a.start < b.start {
        a.end > b.start
    } else {
        b.end > a.start
    }
}

/// Struct used to communicate across calls to 'get_value_reconstruct_data'.
///
/// Before first call, you can fill in 'page_img' if you have an older cached
/// version of the page available. That can save work in
/// 'get_value_reconstruct_data', as it can stop searching for page versions
/// when all the WAL records going back to the cached image have been collected.
///
/// When get_value_reconstruct_data returns Complete, 'img' is set to an image
/// of the page, or the oldest WAL record in 'records' is a will_init-type
/// record that initializes the page without requiring a previous image.
///
/// If 'get_page_reconstruct_data' returns Continue, some 'records' may have
/// been collected, but there are more records outside the current layer. Pass
/// the same ValueReconstructState struct in the next 'get_value_reconstruct_data'
/// call, to collect more records.
///
#[derive(Debug)]
pub struct ValueReconstructState {
    pub records: Vec<(Lsn, NeonWalRecord)>,
    pub img: Option<(Lsn, Bytes)>,
}

/// Return value from Layer::get_page_reconstruct_data
#[derive(Clone, Copy, Debug)]
pub enum ValueReconstructResult {
    /// Got all the data needed to reconstruct the requested page
    Complete,
    /// This layer didn't contain all the required data, the caller should look up
    /// the predecessor layer at the returned LSN and collect more data from there.
    Continue,

    /// This layer didn't contain data needed to reconstruct the page version at
    /// the returned LSN. This is usually considered an error, but might be OK
    /// in some circumstances.
    Missing,
}

#[derive(Debug)]
pub struct LayerAccessStats(Mutex<LayerAccessStatsLocked>);

/// This struct holds two instances of [`LayerAccessStatsInner`].
/// Accesses are recorded to both instances.
/// The `for_scraping_api`instance can be reset from the management API via [`LayerAccessStatsReset`].
/// The `for_eviction_policy` is never reset.
#[derive(Debug, Default, Clone)]
struct LayerAccessStatsLocked {
    for_scraping_api: LayerAccessStatsInner,
    for_eviction_policy: LayerAccessStatsInner,
}

impl LayerAccessStatsLocked {
    fn iter_mut(&mut self) -> impl Iterator<Item = &mut LayerAccessStatsInner> {
        [&mut self.for_scraping_api, &mut self.for_eviction_policy].into_iter()
    }
}

#[derive(Debug, Default, Clone)]
struct LayerAccessStatsInner {
    first_access: Option<LayerAccessStatFullDetails>,
    count_by_access_kind: EnumMap<LayerAccessKind, u64>,
    task_kind_flag: EnumSet<TaskKind>,
    last_accesses: HistoryBufferWithDropCounter<LayerAccessStatFullDetails, 16>,
    last_residence_changes: HistoryBufferWithDropCounter<LayerResidenceEvent, 16>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LayerAccessStatFullDetails {
    pub(crate) when: SystemTime,
    pub(crate) task_kind: TaskKind,
    pub(crate) access_kind: LayerAccessKind,
}

#[derive(Clone, Copy, strum_macros::EnumString)]
pub enum LayerAccessStatsReset {
    NoReset,
    JustTaskKindFlags,
    AllStats,
}

fn system_time_to_millis_since_epoch(ts: &SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .expect("better to die in this unlikely case than report false stats")
        .as_millis()
        .try_into()
        .expect("64 bits is enough for few more years")
}

impl LayerAccessStatFullDetails {
    fn as_api_model(&self) -> pageserver_api::models::LayerAccessStatFullDetails {
        let Self {
            when,
            task_kind,
            access_kind,
        } = self;
        pageserver_api::models::LayerAccessStatFullDetails {
            when_millis_since_epoch: system_time_to_millis_since_epoch(when),
            task_kind: task_kind.into(), // into static str, powered by strum_macros
            access_kind: *access_kind,
        }
    }
}

impl LayerAccessStats {
    /// Create an empty stats object.
    ///
    /// The caller is responsible for recording a residence event
    /// using [`record_residence_event`] before calling `latest_activity`.
    /// If they don't, [`latest_activity`] will return `None`.
    ///
    /// [`record_residence_event`]: Self::record_residence_event
    /// [`latest_activity`]: Self::latest_activity
    pub(crate) fn empty_will_record_residence_event_later() -> Self {
        LayerAccessStats(Mutex::default())
    }

    /// Create an empty stats object and record a [`LayerLoad`] event with the given residence status.
    ///
    /// See [`record_residence_event`] for why you need to do this while holding the layer map lock.
    ///
    /// [`LayerLoad`]: LayerResidenceEventReason::LayerLoad
    /// [`record_residence_event`]: Self::record_residence_event
    pub(crate) fn for_loading_layer(status: LayerResidenceStatus) -> Self {
        let new = LayerAccessStats(Mutex::new(LayerAccessStatsLocked::default()));
        new.record_residence_event(status, LayerResidenceEventReason::LayerLoad);
        new
    }

    /// Creates a clone of `self` and records `new_status` in the clone.
    ///
    /// The `new_status` is not recorded in `self`.
    ///
    /// See [`record_residence_event`] for why you need to do this while holding the layer map lock.
    ///
    /// [`record_residence_event`]: Self::record_residence_event
    pub(crate) fn clone_for_residence_change(
        &self,
        new_status: LayerResidenceStatus,
    ) -> LayerAccessStats {
        let clone = {
            let inner = self.0.lock().unwrap();
            inner.clone()
        };
        let new = LayerAccessStats(Mutex::new(clone));
        new.record_residence_event(new_status, LayerResidenceEventReason::ResidenceChange);
        new
    }

    /// Record a change in layer residency.
    ///
    /// Recording the event must happen while holding the layer map lock to
    /// ensure that latest-activity-threshold-based layer eviction (eviction_task.rs)
    /// can do an "imitate access" to this layer, before it observes `now-latest_activity() > threshold`.
    ///
    /// If we instead recorded the residence event with a timestamp from before grabbing the layer map lock,
    /// the following race could happen:
    ///
    /// - Compact: Write out an L1 layer from several L0 layers. This records residence event LayerCreate with the current timestamp.
    /// - Eviction: imitate access logical size calculation. This accesses the L0 layers because the L1 layer is not yet in the layer map.
    /// - Compact: Grab layer map lock, add the new L1 to layer map and remove the L0s, release layer map lock.
    /// - Eviction: observes the new L1 layer whose only activity timestamp is the LayerCreate event.
    ///
    pub(crate) fn record_residence_event(
        &self,
        status: LayerResidenceStatus,
        reason: LayerResidenceEventReason,
    ) {
        let mut locked = self.0.lock().unwrap();
        locked.iter_mut().for_each(|inner| {
            inner
                .last_residence_changes
                .write(LayerResidenceEvent::new(status, reason))
        });
    }

    fn record_access(&self, access_kind: LayerAccessKind, ctx: &RequestContext) {
        if ctx.access_stats_behavior() == AccessStatsBehavior::Skip {
            return;
        }

        let this_access = LayerAccessStatFullDetails {
            when: SystemTime::now(),
            task_kind: ctx.task_kind(),
            access_kind,
        };

        let mut locked = self.0.lock().unwrap();
        locked.iter_mut().for_each(|inner| {
            inner.first_access.get_or_insert(this_access);
            inner.count_by_access_kind[access_kind] += 1;
            inner.task_kind_flag |= ctx.task_kind();
            inner.last_accesses.write(this_access);
        })
    }

    fn as_api_model(
        &self,
        reset: LayerAccessStatsReset,
    ) -> pageserver_api::models::LayerAccessStats {
        let mut locked = self.0.lock().unwrap();
        let inner = &mut locked.for_scraping_api;
        let LayerAccessStatsInner {
            first_access,
            count_by_access_kind,
            task_kind_flag,
            last_accesses,
            last_residence_changes,
        } = inner;
        let ret = pageserver_api::models::LayerAccessStats {
            access_count_by_access_kind: count_by_access_kind
                .iter()
                .map(|(kind, count)| (kind, *count))
                .collect(),
            task_kind_access_flag: task_kind_flag
                .iter()
                .map(|task_kind| task_kind.into()) // into static str, powered by strum_macros
                .collect(),
            first: first_access.as_ref().map(|a| a.as_api_model()),
            accesses_history: last_accesses.map(|m| m.as_api_model()),
            residence_events_history: last_residence_changes.clone(),
        };
        match reset {
            LayerAccessStatsReset::NoReset => (),
            LayerAccessStatsReset::JustTaskKindFlags => {
                inner.task_kind_flag.clear();
            }
            LayerAccessStatsReset::AllStats => {
                *inner = LayerAccessStatsInner::default();
            }
        }
        ret
    }

    /// Get the latest access timestamp, falling back to latest residence event.
    ///
    /// This function can only return `None` if there has not yet been a call to the
    /// [`record_residence_event`] method. That would generally be considered an
    /// implementation error. This function logs a rate-limited warning in that case.
    ///
    /// TODO: use type system to avoid the need for `fallback`.
    /// The approach in <https://github.com/neondatabase/neon/pull/3775>
    /// could be used to enforce that a residence event is recorded
    /// before a layer is added to the layer map. We could also have
    /// a layer wrapper type that holds the LayerAccessStats, and ensure
    /// that that type can only be produced by inserting into the layer map.
    ///
    /// [`record_residence_event`]: Self::record_residence_event
    pub(crate) fn latest_activity(&self) -> Option<SystemTime> {
        let locked = self.0.lock().unwrap();
        let inner = &locked.for_eviction_policy;
        match inner.last_accesses.recent() {
            Some(a) => Some(a.when),
            None => match inner.last_residence_changes.recent() {
                Some(e) => Some(e.timestamp),
                None => {
                    static WARN_RATE_LIMIT: Lazy<Mutex<(usize, RateLimit)>> =
                        Lazy::new(|| Mutex::new((0, RateLimit::new(Duration::from_secs(10)))));
                    let mut guard = WARN_RATE_LIMIT.lock().unwrap();
                    guard.0 += 1;
                    let occurrences = guard.0;
                    guard.1.call(move || {
                        warn!(parent: None, occurrences, "latest_activity not available, this is an implementation bug, using fallback value");
                    });
                    None
                }
            },
        }
    }
}

/// Supertrait of the [`Layer`] trait that captures the bare minimum interface
/// required by [`LayerMap`](super::layer_map::LayerMap).
///
/// All layers should implement a minimal `std::fmt::Debug` without tenant or
/// timeline names, because those are known in the context of which the layers
/// are used in (timeline).
#[async_trait::async_trait]
pub trait Layer: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static {
    ///
    /// Return data needed to reconstruct given page at LSN.
    ///
    /// It is up to the caller to collect more data from previous layer and
    /// perform WAL redo, if necessary.
    ///
    /// See PageReconstructResult for possible return values. The collected data
    /// is appended to reconstruct_data; the caller should pass an empty struct
    /// on first call, or a struct with a cached older image of the page if one
    /// is available. If this returns ValueReconstructResult::Continue, look up
    /// the predecessor layer and call again with the same 'reconstruct_data' to
    /// collect more data.
    async fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_data: &mut ValueReconstructState,
        ctx: &RequestContext,
    ) -> Result<ValueReconstructResult>;
}

/// Get a layer descriptor from a layer.
pub trait AsLayerDesc {
    /// Get the layer descriptor.
    fn layer_desc(&self) -> &PersistentLayerDesc;
}

/// A Layer contains all data in a "rectangle" consisting of a range of keys and
/// range of LSNs.
///
/// There are two kinds of layers, in-memory and on-disk layers. In-memory
/// layers are used to ingest incoming WAL, and provide fast access to the
/// recent page versions. On-disk layers are stored as files on disk, and are
/// immutable. This trait presents the common functionality of in-memory and
/// on-disk layers.
///
/// Furthermore, there are two kinds of on-disk layers: delta and image layers.
/// A delta layer contains all modifications within a range of LSNs and keys.
/// An image layer is a snapshot of all the data in a key-range, at a single
/// LSN.
pub trait PersistentLayer: Layer + AsLayerDesc {
    /// File name used for this layer, both in the pageserver's local filesystem
    /// state as well as in the remote storage.
    fn filename(&self) -> LayerFileName {
        self.layer_desc().filename()
    }

    // Path to the layer file in the local filesystem.
    // `None` for `RemoteLayer`.
    fn local_path(&self) -> Option<Utf8PathBuf>;

    /// Permanently remove this layer from disk.
    fn delete_resident_layer_file(&self) -> Result<()>;

    fn downcast_remote_layer(self: Arc<Self>) -> Option<std::sync::Arc<RemoteLayer>> {
        None
    }

    fn downcast_delta_layer(self: Arc<Self>) -> Option<std::sync::Arc<DeltaLayer>> {
        None
    }

    fn is_remote_layer(&self) -> bool {
        false
    }

    fn info(&self, reset: LayerAccessStatsReset) -> HistoricLayerInfo;

    fn access_stats(&self) -> &LayerAccessStats;
}

pub fn downcast_remote_layer(
    layer: &Arc<dyn PersistentLayer>,
) -> Option<std::sync::Arc<RemoteLayer>> {
    if layer.is_remote_layer() {
        Arc::clone(layer).downcast_remote_layer()
    } else {
        None
    }
}

pub mod tests {
    use super::*;

    impl From<DeltaFileName> for PersistentLayerDesc {
        fn from(value: DeltaFileName) -> Self {
            PersistentLayerDesc::new_delta(
                TenantId::from_array([0; 16]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn_range,
                233,
            )
        }
    }

    impl From<ImageFileName> for PersistentLayerDesc {
        fn from(value: ImageFileName) -> Self {
            PersistentLayerDesc::new_img(
                TenantId::from_array([0; 16]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn,
                233,
            )
        }
    }

    impl From<LayerFileName> for PersistentLayerDesc {
        fn from(value: LayerFileName) -> Self {
            match value {
                LayerFileName::Delta(d) => Self::from(d),
                LayerFileName::Image(i) => Self::from(i),
            }
        }
    }
}

/// Helper enum to hold a PageServerConf, or a path
///
/// This is used by DeltaLayer and ImageLayer. Normally, this holds a reference to the
/// global config, and paths to layer files are constructed using the tenant/timeline
/// path from the config. But in the 'pagectl' binary, we need to construct a Layer
/// struct for a file on disk, without having a page server running, so that we have no
/// config. In that case, we use the Path variant to hold the full path to the file on
/// disk.
enum PathOrConf {
    Path(Utf8PathBuf),
    Conf(&'static PageServerConf),
}

/// Range wrapping newtype, which uses display to render Debug.
///
/// Useful with `Key`, which has too verbose `{:?}` for printing multiple layers.
struct RangeDisplayDebug<'a, T: std::fmt::Display>(&'a Range<T>);

impl<'a, T: std::fmt::Display> std::fmt::Debug for RangeDisplayDebug<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.0.start, self.0.end)
    }
}
