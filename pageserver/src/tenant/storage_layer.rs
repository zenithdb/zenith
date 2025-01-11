//! Common traits and structs for layers

pub mod batch_split_writer;
pub mod delta_layer;
pub mod filter_iterator;
pub mod image_layer;
pub mod inmemory_layer;
pub(crate) mod layer;
mod layer_desc;
mod layer_name;
pub mod merge_iterator;

use crate::context::{AccessStatsBehavior, RequestContext};
use bytes::Bytes;
use pageserver_api::key::Key;
use pageserver_api::keyspace::{KeySpace, KeySpaceRandomAccum};
use pageserver_api::record::NeonWalRecord;
use pageserver_api::value::Value;
use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use utils::lsn::Lsn;

pub use delta_layer::{DeltaLayer, DeltaLayerWriter, ValueRef};
pub use image_layer::{ImageLayer, ImageLayerWriter};
pub use inmemory_layer::InMemoryLayer;
pub use layer_desc::{PersistentLayerDesc, PersistentLayerKey};
pub use layer_name::{DeltaLayerName, ImageLayerName, LayerName};

pub(crate) use layer::{EvictionError, Layer, ResidentLayer};

use self::inmemory_layer::InMemoryLayerFileId;

use super::timeline::GetVectoredError;
use super::PageReconstructError;

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
#[derive(Debug, Default)]
pub(crate) struct ValueReconstructState {
    pub(crate) records: Vec<(Lsn, NeonWalRecord)>,
    pub(crate) img: Option<(Lsn, Bytes)>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ValueReconstructSituation {
    Complete,
    #[default]
    Continue,
}

/// Reconstruct data accumulated for a single key during a vectored get
#[derive(Debug, Default, Clone)]
pub(crate) struct VectoredValueReconstructState {
    pub(crate) records: Vec<(Lsn, NeonWalRecord)>,
    pub(crate) img: Option<(Lsn, Bytes)>,

    situation: ValueReconstructSituation,
}

impl VectoredValueReconstructState {
    fn get_cached_lsn(&self) -> Option<Lsn> {
        self.img.as_ref().map(|img| img.0)
    }
}

impl From<VectoredValueReconstructState> for ValueReconstructState {
    fn from(mut state: VectoredValueReconstructState) -> Self {
        // walredo expects the records to be descending in terms of Lsn
        state.records.sort_by_key(|(lsn, _)| Reverse(*lsn));

        ValueReconstructState {
            records: state.records,
            img: state.img,
        }
    }
}

/// Bag of data accumulated during a vectored get..
pub(crate) struct ValuesReconstructState {
    /// The keys will be removed after `get_vectored` completes. The caller outside `Timeline`
    /// should not expect to get anything from this hashmap.
    pub(crate) keys: HashMap<Key, Result<VectoredValueReconstructState, PageReconstructError>>,
    /// The keys which are already retrieved
    keys_done: KeySpaceRandomAccum,

    /// The keys covered by the image layers
    keys_with_image_coverage: Option<Range<Key>>,

    // Statistics that are still accessible as a caller of `get_vectored_impl`.
    layers_visited: u32,
    delta_layers_visited: u32,
}

impl ValuesReconstructState {
    pub(crate) fn new() -> Self {
        Self {
            keys: HashMap::new(),
            keys_done: KeySpaceRandomAccum::new(),
            keys_with_image_coverage: None,
            layers_visited: 0,
            delta_layers_visited: 0,
        }
    }

    /// Associate a key with the error which it encountered and mark it as done
    pub(crate) fn on_key_error(&mut self, key: Key, err: PageReconstructError) {
        let previous = self.keys.insert(key, Err(err));
        if let Some(Ok(state)) = previous {
            if state.situation == ValueReconstructSituation::Continue {
                self.keys_done.add_key(key);
            }
        }
    }

    pub(crate) fn on_layer_visited(&mut self, layer: &ReadableLayer) {
        self.layers_visited += 1;
        if let ReadableLayer::PersistentLayer(layer) = layer {
            if layer.layer_desc().is_delta() {
                self.delta_layers_visited += 1;
            }
        }
    }

    pub(crate) fn get_delta_layers_visited(&self) -> u32 {
        self.delta_layers_visited
    }

    pub(crate) fn get_layers_visited(&self) -> u32 {
        self.layers_visited
    }

    /// This function is called after reading a keyspace from a layer.
    /// It checks if the read path has now moved past the cached Lsn for any keys.
    ///
    /// Implementation note: We intentionally iterate over the keys for which we've
    /// already collected some reconstruct data. This avoids scaling complexity with
    /// the size of the search space.
    pub(crate) fn on_lsn_advanced(&mut self, keyspace: &KeySpace, advanced_to: Lsn) {
        for (key, value) in self.keys.iter_mut() {
            if !keyspace.contains(key) {
                continue;
            }

            if let Ok(state) = value {
                if state.situation != ValueReconstructSituation::Complete
                    && state.get_cached_lsn() >= Some(advanced_to)
                {
                    state.situation = ValueReconstructSituation::Complete;
                    self.keys_done.add_key(*key);
                }
            }
        }
    }

    /// On hitting image layer, we can mark all keys in this range as done, because
    /// if the image layer does not contain a key, it is deleted/never added.
    pub(crate) fn on_image_layer_visited(&mut self, key_range: &Range<Key>) {
        let prev_val = self.keys_with_image_coverage.replace(key_range.clone());
        assert_eq!(
            prev_val, None,
            "should consume the keyspace before the next iteration"
        );
    }

    /// Update the state collected for a given key.
    /// Returns true if this was the last value needed for the key and false otherwise.
    ///
    /// If the key is done after the update, mark it as such.
    ///
    /// If the key is in the sparse keyspace (i.e., aux files), we do not track them in
    /// `key_done`.
    pub(crate) fn update_key(
        &mut self,
        key: &Key,
        lsn: Lsn,
        value: Value,
    ) -> ValueReconstructSituation {
        let state = self
            .keys
            .entry(*key)
            .or_insert(Ok(VectoredValueReconstructState::default()));
        let is_sparse_key = key.is_sparse();
        if let Ok(state) = state {
            let key_done = match state.situation {
                ValueReconstructSituation::Complete => {
                    if is_sparse_key {
                        // Sparse keyspace might be visited multiple times because
                        // we don't track unmapped keyspaces.
                        return ValueReconstructSituation::Complete;
                    } else {
                        unreachable!()
                    }
                }
                ValueReconstructSituation::Continue => match value {
                    Value::Image(img) => {
                        state.img = Some((lsn, img));
                        true
                    }
                    Value::WalRecord(rec) => {
                        debug_assert!(
                            Some(lsn) > state.get_cached_lsn(),
                            "Attempt to collect a record below cached LSN for walredo: {} < {}",
                            lsn,
                            state
                                .get_cached_lsn()
                                .expect("Assertion can only fire if a cached lsn is present")
                        );

                        let will_init = rec.will_init();
                        state.records.push((lsn, rec));
                        will_init
                    }
                },
            };

            if key_done && state.situation == ValueReconstructSituation::Continue {
                state.situation = ValueReconstructSituation::Complete;
                if !is_sparse_key {
                    self.keys_done.add_key(*key);
                }
            }

            state.situation
        } else {
            ValueReconstructSituation::Complete
        }
    }

    /// Returns the Lsn at which this key is cached if one exists.
    /// The read path should go no further than this Lsn for the given key.
    pub(crate) fn get_cached_lsn(&self, key: &Key) -> Option<Lsn> {
        self.keys
            .get(key)
            .and_then(|k| k.as_ref().ok())
            .and_then(|state| state.get_cached_lsn())
    }

    /// Returns the key space describing the keys that have
    /// been marked as completed since the last call to this function.
    /// Returns individual keys done, and the image layer coverage.
    pub(crate) fn consume_done_keys(&mut self) -> (KeySpace, Option<Range<Key>>) {
        (
            self.keys_done.consume_keyspace(),
            self.keys_with_image_coverage.take(),
        )
    }
}

impl Default for ValuesReconstructState {
    fn default() -> Self {
        Self::new()
    }
}

/// A key that uniquely identifies a layer in a timeline
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) enum LayerId {
    PersitentLayerId(PersistentLayerKey),
    InMemoryLayerId(InMemoryLayerFileId),
}

/// Uniquely identify a layer visit by the layer
/// and LSN floor (or start LSN) of the reads.
/// The layer itself is not enough since we may
/// have different LSN lower bounds for delta layer reads.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
struct LayerToVisitId {
    layer_id: LayerId,
    lsn_floor: Lsn,
}

/// Layer wrapper for the read path. Note that it is valid
/// to use these layers even after external operations have
/// been performed on them (compaction, freeze, etc.).
#[derive(Debug)]
pub(crate) enum ReadableLayer {
    PersistentLayer(Layer),
    InMemoryLayer(Arc<InMemoryLayer>),
}

/// A partial description of a read to be done.
#[derive(Debug, Clone)]
struct LayerVisit {
    /// An id used to resolve the readable layer within the fringe
    layer_to_visit_id: LayerToVisitId,
    /// Lsn range for the read, used for selecting the next read
    lsn_range: Range<Lsn>,
}

/// Data structure which maintains a fringe of layers for the
/// read path. The fringe is the set of layers which intersects
/// the current keyspace that the search is descending on.
/// Each layer tracks the keyspace that intersects it.
///
/// The fringe must appear sorted by Lsn. Hence, it uses
/// a two layer indexing scheme.
#[derive(Debug)]
pub(crate) struct LayerFringe {
    planned_visits_by_lsn: BinaryHeap<LayerVisit>,
    visit_reads: HashMap<LayerToVisitId, LayerVisitReads>,
}

#[derive(Debug)]
struct LayerVisitReads {
    layer: ReadableLayer,
    target_keyspace: KeySpaceRandomAccum,
}

impl LayerFringe {
    pub(crate) fn new() -> Self {
        LayerFringe {
            planned_visits_by_lsn: BinaryHeap::new(),
            visit_reads: HashMap::new(),
        }
    }

    pub(crate) fn next_layer(&mut self) -> Option<(ReadableLayer, KeySpace, Range<Lsn>)> {
        let read_desc = self.planned_visits_by_lsn.pop()?;

        let removed = self.visit_reads.remove_entry(&read_desc.layer_to_visit_id);

        match removed {
            Some((
                _,
                LayerVisitReads {
                    layer,
                    mut target_keyspace,
                },
            )) => Some((
                layer,
                target_keyspace.consume_keyspace(),
                read_desc.lsn_range,
            )),
            None => unreachable!("fringe internals are always consistent"),
        }
    }

    pub(crate) fn update(
        &mut self,
        layer: ReadableLayer,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
    ) {
        let layer_to_visit_id = LayerToVisitId {
            layer_id: layer.id(),
            lsn_floor: lsn_range.start,
        };

        let entry = self.visit_reads.entry(layer_to_visit_id.clone());
        match entry {
            Entry::Occupied(mut entry) => {
                entry.get_mut().target_keyspace.add_keyspace(keyspace);
            }
            Entry::Vacant(entry) => {
                self.planned_visits_by_lsn.push(LayerVisit {
                    lsn_range,
                    layer_to_visit_id: layer_to_visit_id.clone(),
                });
                let mut accum = KeySpaceRandomAccum::new();
                accum.add_keyspace(keyspace);
                entry.insert(LayerVisitReads {
                    layer,
                    target_keyspace: accum,
                });
            }
        }
    }
}

impl Default for LayerFringe {
    fn default() -> Self {
        Self::new()
    }
}

impl Ord for LayerVisit {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.lsn_range.end.cmp(&other.lsn_range.end);
        if ord == std::cmp::Ordering::Equal {
            self.lsn_range.start.cmp(&other.lsn_range.start).reverse()
        } else {
            ord
        }
    }
}

impl PartialOrd for LayerVisit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LayerVisit {
    fn eq(&self, other: &Self) -> bool {
        self.lsn_range == other.lsn_range
    }
}

impl Eq for LayerVisit {}

impl ReadableLayer {
    pub(crate) fn id(&self) -> LayerId {
        match self {
            Self::PersistentLayer(layer) => LayerId::PersitentLayerId(layer.layer_desc().key()),
            Self::InMemoryLayer(layer) => LayerId::InMemoryLayerId(layer.file_id()),
        }
    }

    pub(crate) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        match self {
            ReadableLayer::PersistentLayer(layer) => {
                layer
                    .get_values_reconstruct_data(keyspace, lsn_range, reconstruct_state, ctx)
                    .await
            }
            ReadableLayer::InMemoryLayer(layer) => {
                layer
                    .get_values_reconstruct_data(keyspace, lsn_range.end, reconstruct_state, ctx)
                    .await
            }
        }
    }
}

/// Layers contain a hint indicating whether they are likely to be used for reads.
///
/// This is a hint rather than an authoritative value, so that we do not have to update it synchronously
/// when changing the visibility of layers (for example when creating a branch that makes some previously
/// covered layers visible).  It should be used for cache management but not for correctness-critical checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerVisibilityHint {
    /// A Visible layer might be read while serving a read, because there is not an image layer between it
    /// and a readable LSN (the tip of the branch or a child's branch point)
    Visible,
    /// A Covered layer probably won't be read right now, but _can_ be read in future if someone creates
    /// a branch or ephemeral endpoint at an LSN below the layer that covers this.
    Covered,
}

pub(crate) struct LayerAccessStats(std::sync::atomic::AtomicU64);

#[derive(Clone, Copy, strum_macros::EnumString)]
pub(crate) enum LayerAccessStatsReset {
    NoReset,
    AllStats,
}

impl Default for LayerAccessStats {
    fn default() -> Self {
        // Default value is to assume resident since creation time, and visible.
        let (_mask, mut value) = Self::to_low_res_timestamp(Self::RTIME_SHIFT, SystemTime::now());
        value |= 0x1 << Self::VISIBILITY_SHIFT;

        Self(std::sync::atomic::AtomicU64::new(value))
    }
}

// Efficient store of two very-low-resolution timestamps and some bits.  Used for storing last access time and
// last residence change time.
impl LayerAccessStats {
    // How many high bits to drop from a u32 timestamp?
    // - Only storing up to a u32 timestamp will work fine until 2038 (if this code is still in use
    //   after that, this software has been very successful!)
    // - Dropping the top bit is implicitly safe because unix timestamps are meant to be
    // stored in an i32, so they never used it.
    // - Dropping the next two bits is safe because this code is only running on systems in
    // years >= 2024, and these bits have been 1 since 2021
    //
    // Therefore we may store only 28 bits for a timestamp with one second resolution.  We do
    // this truncation to make space for some flags in the high bits of our u64.
    const TS_DROP_HIGH_BITS: u32 = u32::count_ones(Self::TS_ONES) + 1;
    const TS_MASK: u32 = 0x1f_ff_ff_ff;
    const TS_ONES: u32 = 0x60_00_00_00;

    const ATIME_SHIFT: u32 = 0;
    const RTIME_SHIFT: u32 = 32 - Self::TS_DROP_HIGH_BITS;
    const VISIBILITY_SHIFT: u32 = 64 - 2 * Self::TS_DROP_HIGH_BITS;

    fn write_bits(&self, mask: u64, value: u64) -> u64 {
        self.0
            .fetch_update(
                // TODO: decide what orderings are correct
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |v| Some((v & !mask) | (value & mask)),
            )
            .expect("Inner function is infallible")
    }

    fn to_low_res_timestamp(shift: u32, time: SystemTime) -> (u64, u64) {
        // Drop the low three bits of the timestamp, for an ~8s accuracy
        let timestamp = time.duration_since(UNIX_EPOCH).unwrap().as_secs() & (Self::TS_MASK as u64);

        ((Self::TS_MASK as u64) << shift, timestamp << shift)
    }

    fn read_low_res_timestamp(&self, shift: u32) -> Option<SystemTime> {
        let read = self.0.load(std::sync::atomic::Ordering::Relaxed);

        let ts_bits = (read & ((Self::TS_MASK as u64) << shift)) >> shift;
        if ts_bits == 0 {
            None
        } else {
            Some(UNIX_EPOCH + Duration::from_secs(ts_bits | (Self::TS_ONES as u64)))
        }
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
    pub(crate) fn record_residence_event_at(&self, now: SystemTime) {
        let (mask, value) = Self::to_low_res_timestamp(Self::RTIME_SHIFT, now);
        self.write_bits(mask, value);
    }

    pub(crate) fn record_residence_event(&self) {
        self.record_residence_event_at(SystemTime::now())
    }

    fn record_access_at(&self, now: SystemTime) -> bool {
        let (mut mask, mut value) = Self::to_low_res_timestamp(Self::ATIME_SHIFT, now);

        // A layer which is accessed must be visible.
        mask |= 0x1 << Self::VISIBILITY_SHIFT;
        value |= 0x1 << Self::VISIBILITY_SHIFT;

        let old_bits = self.write_bits(mask, value);
        !matches!(
            self.decode_visibility(old_bits),
            LayerVisibilityHint::Visible
        )
    }

    /// Returns true if we modified the layer's visibility to set it to Visible implicitly
    /// as a result of this access
    pub(crate) fn record_access(&self, ctx: &RequestContext) -> bool {
        if ctx.access_stats_behavior() == AccessStatsBehavior::Skip {
            return false;
        }

        self.record_access_at(SystemTime::now())
    }

    fn as_api_model(
        &self,
        reset: LayerAccessStatsReset,
    ) -> pageserver_api::models::LayerAccessStats {
        let ret = pageserver_api::models::LayerAccessStats {
            access_time: self
                .read_low_res_timestamp(Self::ATIME_SHIFT)
                .unwrap_or(UNIX_EPOCH),
            residence_time: self
                .read_low_res_timestamp(Self::RTIME_SHIFT)
                .unwrap_or(UNIX_EPOCH),
            visible: matches!(self.visibility(), LayerVisibilityHint::Visible),
        };
        match reset {
            LayerAccessStatsReset::NoReset => {}
            LayerAccessStatsReset::AllStats => {
                self.write_bits((Self::TS_MASK as u64) << Self::ATIME_SHIFT, 0x0);
                self.write_bits((Self::TS_MASK as u64) << Self::RTIME_SHIFT, 0x0);
            }
        }
        ret
    }

    /// Get the latest access timestamp, falling back to latest residence event.  The latest residence event
    /// will be this Layer's construction time, if its residence hasn't changed since then.
    pub(crate) fn latest_activity(&self) -> SystemTime {
        if let Some(t) = self.read_low_res_timestamp(Self::ATIME_SHIFT) {
            t
        } else {
            self.read_low_res_timestamp(Self::RTIME_SHIFT)
                .expect("Residence time is set on construction")
        }
    }

    /// Whether this layer has been accessed (excluding in [`AccessStatsBehavior::Skip`]).
    ///
    /// This indicates whether the layer has been used for some purpose that would motivate
    /// us to keep it on disk, such as for serving a getpage request.
    fn accessed(&self) -> bool {
        // Consider it accessed if the most recent access is more recent than
        // the most recent change in residence status.
        match (
            self.read_low_res_timestamp(Self::ATIME_SHIFT),
            self.read_low_res_timestamp(Self::RTIME_SHIFT),
        ) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(a), Some(r)) => a >= r,
        }
    }

    /// Helper for extracting the visibility hint from the literal value of our inner u64
    fn decode_visibility(&self, bits: u64) -> LayerVisibilityHint {
        match (bits >> Self::VISIBILITY_SHIFT) & 0x1 {
            1 => LayerVisibilityHint::Visible,
            0 => LayerVisibilityHint::Covered,
            _ => unreachable!(),
        }
    }

    /// Returns the old value which has been replaced
    pub(crate) fn set_visibility(&self, visibility: LayerVisibilityHint) -> LayerVisibilityHint {
        let value = match visibility {
            LayerVisibilityHint::Visible => 0x1 << Self::VISIBILITY_SHIFT,
            LayerVisibilityHint::Covered => 0x0,
        };

        let old_bits = self.write_bits(0x1 << Self::VISIBILITY_SHIFT, value);
        self.decode_visibility(old_bits)
    }

    pub(crate) fn visibility(&self) -> LayerVisibilityHint {
        let read = self.0.load(std::sync::atomic::Ordering::Relaxed);
        self.decode_visibility(read)
    }
}

/// Get a layer descriptor from a layer.
pub(crate) trait AsLayerDesc {
    /// Get the layer descriptor.
    fn layer_desc(&self) -> &PersistentLayerDesc;
}

pub mod tests {
    use pageserver_api::shard::TenantShardId;
    use utils::id::TimelineId;

    use super::*;

    impl From<DeltaLayerName> for PersistentLayerDesc {
        fn from(value: DeltaLayerName) -> Self {
            PersistentLayerDesc::new_delta(
                TenantShardId::from([0; 18]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn_range,
                233,
            )
        }
    }

    impl From<ImageLayerName> for PersistentLayerDesc {
        fn from(value: ImageLayerName) -> Self {
            PersistentLayerDesc::new_img(
                TenantShardId::from([0; 18]),
                TimelineId::from_array([0; 16]),
                value.key_range,
                value.lsn,
                233,
            )
        }
    }

    impl From<LayerName> for PersistentLayerDesc {
        fn from(value: LayerName) -> Self {
            match value {
                LayerName::Delta(d) => Self::from(d),
                LayerName::Image(i) => Self::from(i),
            }
        }
    }
}

/// Range wrapping newtype, which uses display to render Debug.
///
/// Useful with `Key`, which has too verbose `{:?}` for printing multiple layers.
struct RangeDisplayDebug<'a, T: std::fmt::Display>(&'a Range<T>);

impl<T: std::fmt::Display> std::fmt::Debug for RangeDisplayDebug<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.0.start, self.0.end)
    }
}
