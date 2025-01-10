//!
//! The layer map tracks what layers exist in a timeline.
//!
//! When the timeline is first accessed, the server lists of all layer files
//! in the timelines/<timeline_id> directory, and populates this map with
//! ImageLayer and DeltaLayer structs corresponding to each file. When the first
//! new WAL record is received, we create an InMemoryLayer to hold the incoming
//! records. Now and then, in the checkpoint() function, the in-memory layer is
//! are frozen, and it is split up into new image and delta layers and the
//! corresponding files are written to disk.
//!
//! Design overview:
//!
//! The `search` method of the layer map is on the read critical path, so we've
//! built an efficient data structure for fast reads, stored in `LayerMap::historic`.
//! Other read methods are less critical but still impact performance of background tasks.
//!
//! This data structure relies on a persistent/immutable binary search tree. See the
//! following lecture for an introduction <https://www.youtube.com/watch?v=WqCWghETNDc&t=581s>
//! Summary: A persistent/immutable BST (and persistent data structures in general) allows
//! you to modify the tree in such a way that each modification creates a new "version"
//! of the tree. When you modify it, you get a new version, but all previous versions are
//! still accessible too. So if someone is still holding a reference to an older version,
//! they continue to see the tree as it was then. The persistent BST stores all the
//! different versions in an efficient way.
//!
//! Our persistent BST maintains a map of which layer file "covers" each key. It has only
//! one dimension, the key. See `layer_coverage.rs`. We use the persistent/immutable property
//! to handle the LSN dimension.
//!
//! To build the layer map, we insert each layer to the persistent BST in LSN.start order,
//! starting from the oldest one. After each insertion, we grab a reference to that "version"
//! of the tree, and store it in another tree, a BtreeMap keyed by the LSN. See
//! `historic_layer_coverage.rs`.
//!
//! To search for a particular key-LSN pair, you first look up the right "version" in the
//! BTreeMap. Then you search that version of the BST with the key.
//!
//! The persistent BST keeps all the versions, but there is no way to change the old versions
//! afterwards. We can add layers as long as they have larger LSNs than any previous layer in
//! the map, but if we need to remove a layer, or insert anything with an older LSN, we need
//! to throw away most of the persistent BST and build a new one, starting from the oldest
//! LSN. See [`LayerMap::flush_updates()`].
//!

mod historic_layer_coverage;
mod layer_coverage;

use crate::context::RequestContext;
use crate::keyspace::KeyPartitioning;
use crate::tenant::storage_layer::InMemoryLayer;
use anyhow::Result;
use pageserver_api::key::Key;
use pageserver_api::keyspace::{KeySpace, KeySpaceAccum};
use range_set_blaze::{CheckSortedDisjoint, RangeSetBlaze};
use std::collections::{HashMap, VecDeque};
use std::iter::Peekable;
use std::ops::Range;
use std::sync::Arc;
use utils::lsn::Lsn;

use historic_layer_coverage::BufferedHistoricLayerCoverage;
pub use historic_layer_coverage::LayerKey;

use super::storage_layer::{LayerVisibilityHint, PersistentLayerDesc};

///
/// LayerMap tracks what layers exist on a timeline.
///
#[derive(Default)]
pub struct LayerMap {
    //
    // 'open_layer' holds the current InMemoryLayer that is accepting new
    // records. If it is None, 'next_open_layer_at' will be set instead, indicating
    // where the start LSN of the next InMemoryLayer that is to be created.
    //
    pub open_layer: Option<Arc<InMemoryLayer>>,
    pub next_open_layer_at: Option<Lsn>,

    ///
    /// Frozen layers, if any. Frozen layers are in-memory layers that
    /// are no longer added to, but haven't been written out to disk
    /// yet. They contain WAL older than the current 'open_layer' or
    /// 'next_open_layer_at', but newer than any historic layer.
    /// The frozen layers are in order from oldest to newest, so that
    /// the newest one is in the 'back' of the VecDeque, and the oldest
    /// in the 'front'.
    ///
    pub frozen_layers: VecDeque<Arc<InMemoryLayer>>,

    /// Index of the historic layers optimized for search
    historic: BufferedHistoricLayerCoverage<Arc<PersistentLayerDesc>>,

    /// L0 layers have key range Key::MIN..Key::MAX, and locating them using R-Tree search is very inefficient.
    /// So L0 layers are held in l0_delta_layers vector, in addition to the R-tree.
    l0_delta_layers: Vec<Arc<PersistentLayerDesc>>,
}

/// The primary update API for the layer map.
///
/// Batching historic layer insertions and removals is good for
/// performance and this struct helps us do that correctly.
#[must_use]
pub struct BatchedUpdates<'a> {
    // While we hold this exclusive reference to the layer map the type checker
    // will prevent us from accidentally reading any unflushed updates.
    layer_map: &'a mut LayerMap,
}

/// Provide ability to batch more updates while hiding the read
/// API so we don't accidentally read without flushing.
impl BatchedUpdates<'_> {
    ///
    /// Insert an on-disk layer.
    ///
    // TODO remove the `layer` argument when `mapping` is refactored out of `LayerMap`
    pub fn insert_historic(&mut self, layer_desc: PersistentLayerDesc) {
        self.layer_map.insert_historic_noflush(layer_desc)
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer_desc: &PersistentLayerDesc) {
        self.layer_map.remove_historic_noflush(layer_desc)
    }

    // We will flush on drop anyway, but this method makes it
    // more explicit that there is some work being done.
    /// Apply all updates
    pub fn flush(self) {
        // Flush happens on drop
    }
}

// Ideally the flush() method should be called explicitly for more
// controlled execution. But if we forget we'd rather flush on drop
// than panic later or read without flushing.
//
// TODO maybe warn if flush hasn't explicitly been called
impl Drop for BatchedUpdates<'_> {
    fn drop(&mut self) {
        self.layer_map.flush_updates();
    }
}

/// Return value of LayerMap::search
#[derive(Eq, PartialEq, Debug, Hash)]
pub struct SearchResult {
    pub layer: Arc<PersistentLayerDesc>,
    pub lsn_floor: Lsn,
}

/// Return value of [`LayerMap::range_search`]
///
/// Contains a mapping from a layer description to a keyspace
/// accumulator that contains all the keys which intersect the layer
/// from the original search space. Keys that were not found are accumulated
/// in a separate key space accumulator.
#[derive(Debug)]
pub struct RangeSearchResult {
    pub found: HashMap<SearchResult, KeySpaceAccum>,
    pub not_found: KeySpaceAccum,
}

impl RangeSearchResult {
    fn new() -> Self {
        Self {
            found: HashMap::new(),
            not_found: KeySpaceAccum::new(),
        }
    }
}

/// Collector for results of range search queries on the LayerMap.
/// It should be provided with two iterators for the delta and image coverage
/// that contain all the changes for layers which intersect the range.
struct RangeSearchCollector<Iter>
where
    Iter: Iterator<Item = (i128, Option<Arc<PersistentLayerDesc>>)>,
{
    delta_coverage: Peekable<Iter>,
    image_coverage: Peekable<Iter>,
    key_range: Range<Key>,
    end_lsn: Lsn,

    current_delta: Option<Arc<PersistentLayerDesc>>,
    current_image: Option<Arc<PersistentLayerDesc>>,

    result: RangeSearchResult,
}

#[derive(Debug)]
enum NextLayerType {
    Delta(i128),
    Image(i128),
    Both(i128),
}

impl NextLayerType {
    fn next_change_at_key(&self) -> Key {
        match self {
            NextLayerType::Delta(at) => Key::from_i128(*at),
            NextLayerType::Image(at) => Key::from_i128(*at),
            NextLayerType::Both(at) => Key::from_i128(*at),
        }
    }
}

impl<Iter> RangeSearchCollector<Iter>
where
    Iter: Iterator<Item = (i128, Option<Arc<PersistentLayerDesc>>)>,
{
    fn new(
        key_range: Range<Key>,
        end_lsn: Lsn,
        delta_coverage: Iter,
        image_coverage: Iter,
    ) -> Self {
        Self {
            delta_coverage: delta_coverage.peekable(),
            image_coverage: image_coverage.peekable(),
            key_range,
            end_lsn,
            current_delta: None,
            current_image: None,
            result: RangeSearchResult::new(),
        }
    }

    /// Run the collector. Collection is implemented via a two pointer algorithm.
    /// One pointer tracks the start of the current range and the other tracks
    /// the beginning of the next range which will overlap with the next change
    /// in coverage across both image and delta.
    fn collect(mut self) -> RangeSearchResult {
        let next_layer_type = self.choose_next_layer_type();
        let mut current_range_start = match next_layer_type {
            None => {
                // No changes for the range
                self.pad_range(self.key_range.clone());
                return self.result;
            }
            Some(layer_type) if self.key_range.end <= layer_type.next_change_at_key() => {
                // Changes only after the end of the range
                self.pad_range(self.key_range.clone());
                return self.result;
            }
            Some(layer_type) => {
                // Changes for the range exist. Record anything before the first
                // coverage change as not found.
                let coverage_start = layer_type.next_change_at_key();
                let range_before = self.key_range.start..coverage_start;
                self.pad_range(range_before);

                self.advance(&layer_type);
                coverage_start
            }
        };

        while current_range_start < self.key_range.end {
            let next_layer_type = self.choose_next_layer_type();
            match next_layer_type {
                Some(t) => {
                    let current_range_end = t.next_change_at_key();
                    self.add_range(current_range_start..current_range_end);
                    current_range_start = current_range_end;

                    self.advance(&t);
                }
                None => {
                    self.add_range(current_range_start..self.key_range.end);
                    current_range_start = self.key_range.end;
                }
            }
        }

        self.result
    }

    /// Mark a range as not found (i.e. no layers intersect it)
    fn pad_range(&mut self, key_range: Range<Key>) {
        if !key_range.is_empty() {
            self.result.not_found.add_range(key_range);
        }
    }

    /// Select the appropiate layer for the given range and update
    /// the collector.
    fn add_range(&mut self, covered_range: Range<Key>) {
        let selected = LayerMap::select_layer(
            self.current_delta.clone(),
            self.current_image.clone(),
            self.end_lsn,
        );

        match selected {
            Some(search_result) => self
                .result
                .found
                .entry(search_result)
                .or_default()
                .add_range(covered_range),
            None => self.pad_range(covered_range),
        }
    }

    /// Move to the next coverage change.
    fn advance(&mut self, layer_type: &NextLayerType) {
        match layer_type {
            NextLayerType::Delta(_) => {
                let (_, layer) = self.delta_coverage.next().unwrap();
                self.current_delta = layer;
            }
            NextLayerType::Image(_) => {
                let (_, layer) = self.image_coverage.next().unwrap();
                self.current_image = layer;
            }
            NextLayerType::Both(_) => {
                let (_, image_layer) = self.image_coverage.next().unwrap();
                let (_, delta_layer) = self.delta_coverage.next().unwrap();

                self.current_image = image_layer;
                self.current_delta = delta_layer;
            }
        }
    }

    /// Pick the next coverage change: the one at the lesser key or both if they're alligned.
    fn choose_next_layer_type(&mut self) -> Option<NextLayerType> {
        let next_delta_at = self.delta_coverage.peek().map(|(key, _)| key);
        let next_image_at = self.image_coverage.peek().map(|(key, _)| key);

        match (next_delta_at, next_image_at) {
            (None, None) => None,
            (Some(next_delta_at), None) => Some(NextLayerType::Delta(*next_delta_at)),
            (None, Some(next_image_at)) => Some(NextLayerType::Image(*next_image_at)),
            (Some(next_delta_at), Some(next_image_at)) if next_image_at < next_delta_at => {
                Some(NextLayerType::Image(*next_image_at))
            }
            (Some(next_delta_at), Some(next_image_at)) if next_delta_at < next_image_at => {
                Some(NextLayerType::Delta(*next_delta_at))
            }
            (Some(next_delta_at), Some(_)) => Some(NextLayerType::Both(*next_delta_at)),
        }
    }
}

impl LayerMap {
    ///
    /// Find the latest layer (by lsn.end) that covers the given
    /// 'key', with lsn.start < 'end_lsn'.
    ///
    /// The caller of this function is the page reconstruction
    /// algorithm looking for the next relevant delta layer, or
    /// the terminal image layer. The caller will pass the lsn_floor
    /// value as end_lsn in the next call to search.
    ///
    /// If there's an image layer exactly below the given end_lsn,
    /// search should return that layer regardless if there are
    /// overlapping deltas.
    ///
    /// If the latest layer is a delta and there is an overlapping
    /// image with it below, the lsn_floor returned should be right
    /// above that image so we don't skip it in the search. Otherwise
    /// the lsn_floor returned should be the bottom of the delta layer
    /// because we should make as much progress down the lsn axis
    /// as possible. It's fine if this way we skip some overlapping
    /// deltas, because the delta we returned would contain the same
    /// wal content.
    ///
    /// TODO: This API is convoluted and inefficient. If the caller
    /// makes N search calls, we'll end up finding the same latest
    /// image layer N times. We should either cache the latest image
    /// layer result, or simplify the api to `get_latest_image` and
    /// `get_latest_delta`, and only call `get_latest_image` once.
    ///
    /// NOTE: This only searches the 'historic' layers, *not* the
    /// 'open' and 'frozen' layers!
    ///
    pub fn search(&self, key: Key, end_lsn: Lsn) -> Option<SearchResult> {
        let version = self.historic.get().unwrap().get_version(end_lsn.0 - 1)?;
        let latest_delta = version.delta_coverage.query(key.to_i128());
        let latest_image = version.image_coverage.query(key.to_i128());

        Self::select_layer(latest_delta, latest_image, end_lsn)
    }

    fn select_layer(
        delta_layer: Option<Arc<PersistentLayerDesc>>,
        image_layer: Option<Arc<PersistentLayerDesc>>,
        end_lsn: Lsn,
    ) -> Option<SearchResult> {
        assert!(delta_layer.as_ref().is_none_or(|l| l.is_delta()));
        assert!(image_layer.as_ref().is_none_or(|l| !l.is_delta()));

        match (delta_layer, image_layer) {
            (None, None) => None,
            (None, Some(image)) => {
                let lsn_floor = image.get_lsn_range().start;
                Some(SearchResult {
                    layer: image,
                    lsn_floor,
                })
            }
            (Some(delta), None) => {
                let lsn_floor = delta.get_lsn_range().start;
                Some(SearchResult {
                    layer: delta,
                    lsn_floor,
                })
            }
            (Some(delta), Some(image)) => {
                let img_lsn = image.get_lsn_range().start;
                let image_is_newer = image.get_lsn_range().end >= delta.get_lsn_range().end;
                let image_exact_match = img_lsn + 1 == end_lsn;
                if image_is_newer || image_exact_match {
                    Some(SearchResult {
                        layer: image,
                        lsn_floor: img_lsn,
                    })
                } else {
                    let lsn_floor =
                        std::cmp::max(delta.get_lsn_range().start, image.get_lsn_range().start + 1);
                    Some(SearchResult {
                        layer: delta,
                        lsn_floor,
                    })
                }
            }
        }
    }

    pub fn range_search(&self, key_range: Range<Key>, end_lsn: Lsn) -> RangeSearchResult {
        let version = match self.historic.get().unwrap().get_version(end_lsn.0 - 1) {
            Some(version) => version,
            None => {
                let mut result = RangeSearchResult::new();
                result.not_found.add_range(key_range);
                return result;
            }
        };

        let raw_range = key_range.start.to_i128()..key_range.end.to_i128();
        let delta_changes = version.delta_coverage.range_overlaps(&raw_range);
        let image_changes = version.image_coverage.range_overlaps(&raw_range);

        let collector = RangeSearchCollector::new(key_range, end_lsn, delta_changes, image_changes);
        collector.collect()
    }

    /// Start a batch of updates, applied on drop
    pub fn batch_update(&mut self) -> BatchedUpdates<'_> {
        BatchedUpdates { layer_map: self }
    }

    ///
    /// Insert an on-disk layer
    ///
    /// Helper function for BatchedUpdates::insert_historic
    ///
    /// TODO(chi): remove L generic so that we do not need to pass layer object.
    pub(self) fn insert_historic_noflush(&mut self, layer_desc: PersistentLayerDesc) {
        // TODO: See #3869, resulting #4088, attempted fix and repro #4094

        if Self::is_l0(&layer_desc.key_range, layer_desc.is_delta) {
            self.l0_delta_layers.push(layer_desc.clone().into());
        }

        self.historic.insert(
            historic_layer_coverage::LayerKey::from(&layer_desc),
            layer_desc.into(),
        );
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// Helper function for BatchedUpdates::remove_historic
    ///
    pub fn remove_historic_noflush(&mut self, layer_desc: &PersistentLayerDesc) {
        self.historic
            .remove(historic_layer_coverage::LayerKey::from(layer_desc));
        let layer_key = layer_desc.key();
        if Self::is_l0(&layer_desc.key_range, layer_desc.is_delta) {
            let len_before = self.l0_delta_layers.len();
            let mut l0_delta_layers = std::mem::take(&mut self.l0_delta_layers);
            l0_delta_layers.retain(|other| other.key() != layer_key);
            self.l0_delta_layers = l0_delta_layers;
            // this assertion is related to use of Arc::ptr_eq in Self::compare_arced_layers,
            // there's a chance that the comparison fails at runtime due to it comparing (pointer,
            // vtable) pairs.
            assert_eq!(
                self.l0_delta_layers.len(),
                len_before - 1,
                "failed to locate removed historic layer from l0_delta_layers"
            );
        }
    }

    /// Helper function for BatchedUpdates::drop.
    pub(self) fn flush_updates(&mut self) {
        self.historic.rebuild();
    }

    /// Is there a newer image layer for given key- and LSN-range? Or a set
    /// of image layers within the specified lsn range that cover the entire
    /// specified key range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn image_layer_exists(&self, key: &Range<Key>, lsn: &Range<Lsn>) -> bool {
        if key.is_empty() {
            // Vacuously true. There's a newer image for all 0 of the kerys in the range.
            return true;
        }

        let version = match self.historic.get().unwrap().get_version(lsn.end.0 - 1) {
            Some(v) => v,
            None => return false,
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        let layer_covers = |layer: Option<Arc<PersistentLayerDesc>>| match layer {
            Some(layer) => layer.get_lsn_range().start >= lsn.start,
            None => false,
        };

        // Check the start is covered
        if !layer_covers(version.image_coverage.query(start)) {
            return false;
        }

        // Check after all changes of coverage
        for (_, change_val) in version.image_coverage.range(start..end) {
            if !layer_covers(change_val) {
                return false;
            }
        }

        true
    }

    pub fn iter_historic_layers(&self) -> impl '_ + Iterator<Item = Arc<PersistentLayerDesc>> {
        self.historic.iter()
    }

    /// Get a ref counted pointer for the first in memory layer that matches the provided predicate.
    pub fn find_in_memory_layer<Pred>(&self, mut pred: Pred) -> Option<Arc<InMemoryLayer>>
    where
        Pred: FnMut(&Arc<InMemoryLayer>) -> bool,
    {
        if let Some(open) = &self.open_layer {
            if pred(open) {
                return Some(open.clone());
            }
        }

        self.frozen_layers.iter().rfind(|l| pred(l)).cloned()
    }

    ///
    /// Divide the whole given range of keys into sub-ranges based on the latest
    /// image layer that covers each range at the specified lsn (inclusive).
    /// This is used when creating  new image layers.
    pub fn image_coverage(
        &self,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Vec<(Range<Key>, Option<Arc<PersistentLayerDesc>>)> {
        let version = match self.historic.get().unwrap().get_version(lsn.0) {
            Some(v) => v,
            None => return vec![],
        };

        let start = key_range.start.to_i128();
        let end = key_range.end.to_i128();

        // Initialize loop variables
        let mut coverage: Vec<(Range<Key>, Option<Arc<PersistentLayerDesc>>)> = vec![];
        let mut current_key = start;
        let mut current_val = version.image_coverage.query(start);

        // Loop through the change events and push intervals
        for (change_key, change_val) in version.image_coverage.range(start..end) {
            let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
            coverage.push((kr, current_val.take()));
            current_key = change_key;
            current_val.clone_from(&change_val);
        }

        // Add the final interval
        let kr = Key::from_i128(current_key)..Key::from_i128(end);
        coverage.push((kr, current_val.take()));

        coverage
    }

    /// Check if the key range resembles that of an L0 layer.
    pub fn is_l0(key_range: &Range<Key>, is_delta_layer: bool) -> bool {
        is_delta_layer && key_range == &(Key::MIN..Key::MAX)
    }

    /// This function determines which layers are counted in `count_deltas`:
    /// layers that should count towards deciding whether or not to reimage
    /// a certain partition range.
    ///
    /// There are two kinds of layers we currently consider reimage-worthy:
    ///
    /// Case 1: Non-L0 layers are currently reimage-worthy by default.
    /// TODO Some of these layers are very sparse and cover the entire key
    ///      range. Replacing 256MB of data (or less!) with terabytes of
    ///      images doesn't seem wise. We need a better heuristic, possibly
    ///      based on some of these factors:
    ///      a) whether this layer has any wal in this partition range
    ///      b) the size of the layer
    ///      c) the number of images needed to cover it
    ///      d) the estimated time until we'll have to reimage over it for GC
    ///
    /// Case 2: Since L0 layers by definition cover the entire key space, we consider
    /// them reimage-worthy only when the entire key space can be covered by very few
    /// images (currently 1).
    /// TODO The optimal number should probably be slightly higher than 1, but to
    ///      implement that we need to plumb a lot more context into this function
    ///      than just the current partition_range.
    pub fn is_reimage_worthy(layer: &PersistentLayerDesc, partition_range: &Range<Key>) -> bool {
        // Case 1
        if !Self::is_l0(&layer.key_range, layer.is_delta) {
            return true;
        }

        // Case 2
        if partition_range == &(Key::MIN..Key::MAX) {
            return true;
        }

        false
    }

    /// Count the height of the tallest stack of reimage-worthy deltas
    /// in this 2d region.
    ///
    /// If `limit` is provided we don't try to count above that number.
    ///
    /// This number is used to compute the largest number of deltas that
    /// we'll need to visit for any page reconstruction in this region.
    /// We use this heuristic to decide whether to create an image layer.
    pub fn count_deltas(&self, key: &Range<Key>, lsn: &Range<Lsn>, limit: Option<usize>) -> usize {
        // We get the delta coverage of the region, and for each part of the coverage
        // we recurse right underneath the delta. The recursion depth is limited by
        // the largest result this function could return, which is in practice between
        // 3 and 10 (since we usually try to create an image when the number gets larger).

        if lsn.is_empty() || key.is_empty() || limit == Some(0) {
            return 0;
        }

        let version = match self.historic.get().unwrap().get_version(lsn.end.0 - 1) {
            Some(v) => v,
            None => return 0,
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        // Initialize loop variables
        let mut max_stacked_deltas = 0;
        let mut current_key = start;
        let mut current_val = version.delta_coverage.query(start);

        // Loop through the delta coverage and recurse on each part
        for (change_key, change_val) in version.delta_coverage.range(start..end) {
            // If there's a relevant delta in this part, add 1 and recurse down
            if let Some(val) = &current_val {
                if val.get_lsn_range().end > lsn.start {
                    let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
                    let lr = lsn.start..val.get_lsn_range().start;
                    if !kr.is_empty() {
                        let base_count = Self::is_reimage_worthy(val, key) as usize;
                        let new_limit = limit.map(|l| l - base_count);
                        let max_stacked_deltas_underneath = self.count_deltas(&kr, &lr, new_limit);
                        max_stacked_deltas = std::cmp::max(
                            max_stacked_deltas,
                            base_count + max_stacked_deltas_underneath,
                        );
                    }
                }
            }

            current_key = change_key;
            current_val.clone_from(&change_val);
        }

        // Consider the last part
        if let Some(val) = &current_val {
            if val.get_lsn_range().end > lsn.start {
                let kr = Key::from_i128(current_key)..Key::from_i128(end);
                let lr = lsn.start..val.get_lsn_range().start;

                if !kr.is_empty() {
                    let base_count = Self::is_reimage_worthy(val, key) as usize;
                    let new_limit = limit.map(|l| l - base_count);
                    let max_stacked_deltas_underneath = self.count_deltas(&kr, &lr, new_limit);
                    max_stacked_deltas = std::cmp::max(
                        max_stacked_deltas,
                        base_count + max_stacked_deltas_underneath,
                    );
                }
            }
        }

        max_stacked_deltas
    }

    /// Count how many reimage-worthy layers we need to visit for given key-lsn pair.
    ///
    /// The `partition_range` argument is used as context for the reimage-worthiness decision.
    ///
    /// Used as a helper for correctness checks only. Performance not critical.
    pub fn get_difficulty(&self, lsn: Lsn, key: Key, partition_range: &Range<Key>) -> usize {
        match self.search(key, lsn) {
            Some(search_result) => {
                if search_result.layer.is_incremental() {
                    (Self::is_reimage_worthy(&search_result.layer, partition_range) as usize)
                        + self.get_difficulty(search_result.lsn_floor, key, partition_range)
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    /// Used for correctness checking. Results are expected to be identical to
    /// self.get_difficulty_map. Assumes self.search is correct.
    pub fn get_difficulty_map_bruteforce(
        &self,
        lsn: Lsn,
        partitioning: &KeyPartitioning,
    ) -> Vec<usize> {
        // Looking at the difficulty as a function of key, it could only increase
        // when a delta layer starts or an image layer ends. Therefore it's sufficient
        // to check the difficulties at:
        // - the key.start for each non-empty part range
        // - the key.start for each delta
        // - the key.end for each image
        let keys_iter: Box<dyn Iterator<Item = Key>> = {
            let mut keys: Vec<Key> = self
                .iter_historic_layers()
                .map(|layer| {
                    if layer.is_incremental() {
                        layer.get_key_range().start
                    } else {
                        layer.get_key_range().end
                    }
                })
                .collect();
            keys.sort();
            Box::new(keys.into_iter())
        };
        let mut keys_iter = keys_iter.peekable();

        // Iter the partition and keys together and query all the necessary
        // keys, computing the max difficulty for each part.
        partitioning
            .parts
            .iter()
            .map(|part| {
                let mut difficulty = 0;
                // Partition ranges are assumed to be sorted and disjoint
                // TODO assert it
                for range in &part.ranges {
                    if !range.is_empty() {
                        difficulty =
                            std::cmp::max(difficulty, self.get_difficulty(lsn, range.start, range));
                    }
                    while let Some(key) = keys_iter.peek() {
                        if key >= &range.end {
                            break;
                        }
                        let key = keys_iter.next().unwrap();
                        if key < range.start {
                            continue;
                        }
                        difficulty =
                            std::cmp::max(difficulty, self.get_difficulty(lsn, key, range));
                    }
                }
                difficulty
            })
            .collect()
    }

    /// For each part of a keyspace partitioning, return the maximum number of layers
    /// that would be needed for page reconstruction in that part at the given LSN.
    ///
    /// If `limit` is provided we don't try to count above that number.
    ///
    /// This method is used to decide where to create new image layers. Computing the
    /// result for the entire partitioning at once allows this function to be more
    /// efficient, and further optimization is possible by using iterators instead,
    /// to allow early return.
    ///
    /// TODO actually use this method instead of count_deltas. Currently we only use
    ///      it for benchmarks.
    pub fn get_difficulty_map(
        &self,
        lsn: Lsn,
        partitioning: &KeyPartitioning,
        limit: Option<usize>,
    ) -> Vec<usize> {
        // TODO This is a naive implementation. Perf improvements to do:
        // 1. Instead of calling self.image_coverage and self.count_deltas,
        //    iterate the image and delta coverage only once.
        partitioning
            .parts
            .iter()
            .map(|part| {
                let mut difficulty = 0;
                for range in &part.ranges {
                    if limit == Some(difficulty) {
                        break;
                    }
                    for (img_range, last_img) in self.image_coverage(range, lsn) {
                        if limit == Some(difficulty) {
                            break;
                        }
                        let img_lsn = if let Some(last_img) = last_img {
                            last_img.get_lsn_range().end
                        } else {
                            Lsn(0)
                        };

                        if img_lsn < lsn {
                            let num_deltas = self.count_deltas(&img_range, &(img_lsn..lsn), limit);
                            difficulty = std::cmp::max(difficulty, num_deltas);
                        }
                    }
                }
                difficulty
            })
            .collect()
    }

    /// Return all L0 delta layers
    pub fn level0_deltas(&self) -> &Vec<Arc<PersistentLayerDesc>> {
        &self.l0_delta_layers
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub async fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        println!("Begin dump LayerMap");

        println!("open_layer:");
        if let Some(open_layer) = &self.open_layer {
            open_layer.dump(verbose, ctx).await?;
        }

        println!("frozen_layers:");
        for frozen_layer in self.frozen_layers.iter() {
            frozen_layer.dump(verbose, ctx).await?;
        }

        println!("historic_layers:");
        for desc in self.iter_historic_layers() {
            desc.dump();
        }
        println!("End dump LayerMap");
        Ok(())
    }

    /// `read_points` represent the tip of a timeline and any branch points, i.e. the places
    /// where we expect to serve reads.
    ///
    /// This function is O(N) and should be called infrequently.  The caller is responsible for
    /// looking up and updating the Layer objects for these layer descriptors.
    pub fn get_visibility(
        &self,
        mut read_points: Vec<Lsn>,
    ) -> (
        Vec<(Arc<PersistentLayerDesc>, LayerVisibilityHint)>,
        KeySpace,
    ) {
        // This is like a KeySpace, but this type is intended for efficient unions with image layer ranges, whereas
        // KeySpace is intended to be composed statically and iterated over.
        struct KeyShadow {
            // Map of range start to range end
            inner: RangeSetBlaze<i128>,
        }

        impl KeyShadow {
            fn new() -> Self {
                Self {
                    inner: Default::default(),
                }
            }

            fn contains(&self, range: Range<Key>) -> bool {
                let range_incl = range.start.to_i128()..=range.end.to_i128() - 1;
                self.inner.is_superset(&RangeSetBlaze::from_sorted_disjoint(
                    CheckSortedDisjoint::from([range_incl]),
                ))
            }

            /// Add the input range to the keys covered by self.
            ///
            /// Return true if inserting this range covered some keys that were previously not covered
            fn cover(&mut self, insert: Range<Key>) -> bool {
                let range_incl = insert.start.to_i128()..=insert.end.to_i128() - 1;
                self.inner.ranges_insert(range_incl)
            }

            fn reset(&mut self) {
                self.inner = Default::default();
            }

            fn to_keyspace(&self) -> KeySpace {
                let mut accum = KeySpaceAccum::new();
                for range_incl in self.inner.ranges() {
                    let range = Range {
                        start: Key::from_i128(*range_incl.start()),
                        end: Key::from_i128(range_incl.end() + 1),
                    };
                    accum.add_range(range)
                }

                accum.to_keyspace()
            }
        }

        // The 'shadow' will be updated as we sweep through the layers: an image layer subtracts from the shadow,
        // and a ReadPoint
        read_points.sort_by_key(|rp| rp.0);
        let mut shadow = KeyShadow::new();

        // We will interleave all our read points and layers into a sorted collection
        enum Item {
            ReadPoint { lsn: Lsn },
            Layer(Arc<PersistentLayerDesc>),
        }

        let mut items = Vec::with_capacity(self.historic.len() + read_points.len());
        items.extend(self.iter_historic_layers().map(Item::Layer));
        items.extend(
            read_points
                .into_iter()
                .map(|rp| Item::ReadPoint { lsn: rp }),
        );

        // Ordering: we want to iterate like this:
        // 1. Highest LSNs first
        // 2. Consider images before deltas if they end at the same LSNs (images cover deltas)
        // 3. Consider ReadPoints before image layers if they're at the same LSN (readpoints make that image visible)
        items.sort_by_key(|item| {
            std::cmp::Reverse(match item {
                Item::Layer(layer) => {
                    if layer.is_delta() {
                        (Lsn(layer.get_lsn_range().end.0 - 1), 0)
                    } else {
                        (layer.image_layer_lsn(), 1)
                    }
                }
                Item::ReadPoint { lsn } => (*lsn, 2),
            })
        });

        let mut results = Vec::with_capacity(self.historic.len());

        let mut maybe_covered_deltas: Vec<Arc<PersistentLayerDesc>> = Vec::new();

        for item in items {
            let (reached_lsn, is_readpoint) = match &item {
                Item::ReadPoint { lsn } => (lsn, true),
                Item::Layer(layer) => (&layer.lsn_range.start, false),
            };
            maybe_covered_deltas.retain(|d| {
                if *reached_lsn >= d.lsn_range.start && is_readpoint {
                    // We encountered a readpoint within the delta layer: it is visible

                    results.push((d.clone(), LayerVisibilityHint::Visible));
                    false
                } else if *reached_lsn < d.lsn_range.start {
                    // We passed the layer's range without encountering a read point: it is not visible
                    results.push((d.clone(), LayerVisibilityHint::Covered));
                    false
                } else {
                    // We're still in the delta layer: continue iterating
                    true
                }
            });

            match item {
                Item::ReadPoint { lsn: _lsn } => {
                    // TODO: propagate the child timeline's shadow from their own run of this function, so that we don't have
                    // to assume that the whole key range is visible at the branch point.
                    shadow.reset();
                }
                Item::Layer(layer) => {
                    let visibility = if layer.is_delta() {
                        if shadow.contains(layer.get_key_range()) {
                            // If a layer isn't visible based on current state, we must defer deciding whether
                            // it is truly not visible until we have advanced past the delta's range: we might
                            // encounter another branch point within this delta layer's LSN range.
                            maybe_covered_deltas.push(layer);
                            continue;
                        } else {
                            LayerVisibilityHint::Visible
                        }
                    } else {
                        let modified = shadow.cover(layer.get_key_range());
                        if modified {
                            // An image layer in a region which wasn't fully covered yet: this layer is visible, but layers below it will be covered
                            LayerVisibilityHint::Visible
                        } else {
                            // An image layer in a region that was already covered
                            LayerVisibilityHint::Covered
                        }
                    };

                    results.push((layer, visibility));
                }
            }
        }

        // Drain any remaining maybe_covered deltas
        results.extend(
            maybe_covered_deltas
                .into_iter()
                .map(|d| (d, LayerVisibilityHint::Covered)),
        );

        (results, shadow.to_keyspace())
    }
}

#[cfg(test)]
mod tests {
    use crate::tenant::{storage_layer::LayerName, IndexPart};
    use pageserver_api::{
        key::DBDIR_KEY,
        keyspace::{KeySpace, KeySpaceRandomAccum},
    };
    use std::{collections::HashMap, path::PathBuf};
    use utils::{
        id::{TenantId, TimelineId},
        shard::TenantShardId,
    };

    use super::*;

    #[derive(Clone)]
    struct LayerDesc {
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
        is_delta: bool,
    }

    fn create_layer_map(layers: Vec<LayerDesc>) -> LayerMap {
        let mut layer_map = LayerMap::default();

        for layer in layers {
            layer_map.insert_historic_noflush(PersistentLayerDesc::new_test(
                layer.key_range,
                layer.lsn_range,
                layer.is_delta,
            ));
        }

        layer_map.flush_updates();
        layer_map
    }

    fn assert_range_search_result_eq(lhs: RangeSearchResult, rhs: RangeSearchResult) {
        assert_eq!(lhs.not_found.to_keyspace(), rhs.not_found.to_keyspace());
        let lhs: HashMap<SearchResult, KeySpace> = lhs
            .found
            .into_iter()
            .map(|(search_result, accum)| (search_result, accum.to_keyspace()))
            .collect();
        let rhs: HashMap<SearchResult, KeySpace> = rhs
            .found
            .into_iter()
            .map(|(search_result, accum)| (search_result, accum.to_keyspace()))
            .collect();

        assert_eq!(lhs, rhs);
    }

    #[cfg(test)]
    fn brute_force_range_search(
        layer_map: &LayerMap,
        key_range: Range<Key>,
        end_lsn: Lsn,
    ) -> RangeSearchResult {
        let mut range_search_result = RangeSearchResult::new();

        let mut key = key_range.start;
        while key != key_range.end {
            let res = layer_map.search(key, end_lsn);
            match res {
                Some(res) => {
                    range_search_result
                        .found
                        .entry(res)
                        .or_default()
                        .add_key(key);
                }
                None => {
                    range_search_result.not_found.add_key(key);
                }
            }

            key = key.next();
        }

        range_search_result
    }

    #[test]
    fn ranged_search_on_empty_layer_map() {
        let layer_map = LayerMap::default();
        let range = Key::from_i128(100)..Key::from_i128(200);

        let res = layer_map.range_search(range.clone(), Lsn(100));
        assert_eq!(
            res.not_found.to_keyspace(),
            KeySpace {
                ranges: vec![range]
            }
        );
    }

    #[test]
    fn ranged_search() {
        let layers = vec![
            LayerDesc {
                key_range: Key::from_i128(15)..Key::from_i128(50),
                lsn_range: Lsn(0)..Lsn(5),
                is_delta: false,
            },
            LayerDesc {
                key_range: Key::from_i128(10)..Key::from_i128(20),
                lsn_range: Lsn(5)..Lsn(20),
                is_delta: true,
            },
            LayerDesc {
                key_range: Key::from_i128(15)..Key::from_i128(25),
                lsn_range: Lsn(20)..Lsn(30),
                is_delta: true,
            },
            LayerDesc {
                key_range: Key::from_i128(35)..Key::from_i128(40),
                lsn_range: Lsn(25)..Lsn(35),
                is_delta: true,
            },
            LayerDesc {
                key_range: Key::from_i128(35)..Key::from_i128(40),
                lsn_range: Lsn(35)..Lsn(40),
                is_delta: false,
            },
        ];

        let layer_map = create_layer_map(layers.clone());
        for start in 0..60 {
            for end in (start + 1)..60 {
                let range = Key::from_i128(start)..Key::from_i128(end);
                let result = layer_map.range_search(range.clone(), Lsn(100));
                let expected = brute_force_range_search(&layer_map, range, Lsn(100));

                assert_range_search_result_eq(result, expected);
            }
        }
    }

    #[test]
    fn layer_visibility_basic() {
        // A simple synthetic input, as a smoke test.
        let tenant_shard_id = TenantShardId::unsharded(TenantId::generate());
        let timeline_id = TimelineId::generate();
        let mut layer_map = LayerMap::default();
        let mut updates = layer_map.batch_update();

        const FAKE_LAYER_SIZE: u64 = 1024;

        let inject_delta = |updates: &mut BatchedUpdates,
                            key_start: i128,
                            key_end: i128,
                            lsn_start: u64,
                            lsn_end: u64| {
            let desc = PersistentLayerDesc::new_delta(
                tenant_shard_id,
                timeline_id,
                Range {
                    start: Key::from_i128(key_start),
                    end: Key::from_i128(key_end),
                },
                Range {
                    start: Lsn(lsn_start),
                    end: Lsn(lsn_end),
                },
                1024,
            );
            updates.insert_historic(desc.clone());
            desc
        };

        let inject_image =
            |updates: &mut BatchedUpdates, key_start: i128, key_end: i128, lsn: u64| {
                let desc = PersistentLayerDesc::new_img(
                    tenant_shard_id,
                    timeline_id,
                    Range {
                        start: Key::from_i128(key_start),
                        end: Key::from_i128(key_end),
                    },
                    Lsn(lsn),
                    FAKE_LAYER_SIZE,
                );
                updates.insert_historic(desc.clone());
                desc
            };

        //
        // Construct our scenario: the following lines go in backward-LSN order, constructing the various scenarios
        // we expect to handle.  You can follow these examples through in the same order as they would be processed
        // by the function under test.
        //

        let mut read_points = vec![Lsn(1000)];

        // A delta ahead of any image layer
        let ahead_layer = inject_delta(&mut updates, 10, 20, 101, 110);

        // An image layer is visible and covers some layers beneath itself
        let visible_covering_img = inject_image(&mut updates, 5, 25, 99);

        // A delta layer covered by the image layer: should be covered
        let covered_delta = inject_delta(&mut updates, 10, 20, 90, 100);

        // A delta layer partially covered by an image layer: should be visible
        let partially_covered_delta = inject_delta(&mut updates, 1, 7, 90, 100);

        // A delta layer not covered by an image layer: should be visible
        let not_covered_delta = inject_delta(&mut updates, 1, 4, 90, 100);

        // An image layer covered by the image layer above: should be covered
        let covered_image = inject_image(&mut updates, 10, 20, 89);

        // An image layer partially covered by an image layer: should be visible
        let partially_covered_image = inject_image(&mut updates, 1, 7, 89);

        // An image layer not covered by an image layer: should be visible
        let not_covered_image = inject_image(&mut updates, 1, 4, 89);

        // A read point: this will make subsequent layers below here visible, even if there are
        // more recent layers covering them.
        read_points.push(Lsn(80));

        // A delta layer covered by an earlier image layer, but visible to a readpoint below that covering layer
        let covered_delta_below_read_point = inject_delta(&mut updates, 10, 20, 70, 79);

        // A delta layer whose end LSN is covered, but where a read point is present partway through its LSN range:
        // the read point should make it visible, even though its end LSN is covered
        let covering_img_between_read_points = inject_image(&mut updates, 10, 20, 69);
        let covered_delta_between_read_points = inject_delta(&mut updates, 10, 15, 67, 69);
        read_points.push(Lsn(65));
        let covered_delta_intersects_read_point = inject_delta(&mut updates, 15, 20, 60, 69);

        let visible_img_after_last_read_point = inject_image(&mut updates, 10, 20, 65);

        updates.flush();

        let (layer_visibilities, shadow) = layer_map.get_visibility(read_points);
        let layer_visibilities = layer_visibilities.into_iter().collect::<HashMap<_, _>>();

        assert_eq!(
            layer_visibilities.get(&ahead_layer),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&visible_covering_img),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&covered_delta),
            Some(&LayerVisibilityHint::Covered)
        );
        assert_eq!(
            layer_visibilities.get(&partially_covered_delta),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&not_covered_delta),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&covered_image),
            Some(&LayerVisibilityHint::Covered)
        );
        assert_eq!(
            layer_visibilities.get(&partially_covered_image),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&not_covered_image),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&covered_delta_below_read_point),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&covering_img_between_read_points),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&covered_delta_between_read_points),
            Some(&LayerVisibilityHint::Covered)
        );
        assert_eq!(
            layer_visibilities.get(&covered_delta_intersects_read_point),
            Some(&LayerVisibilityHint::Visible)
        );
        assert_eq!(
            layer_visibilities.get(&visible_img_after_last_read_point),
            Some(&LayerVisibilityHint::Visible)
        );

        // Shadow should include all the images below the last read point
        let expected_shadow = KeySpace {
            ranges: vec![Key::from_i128(10)..Key::from_i128(20)],
        };
        assert_eq!(shadow, expected_shadow);
    }

    fn fixture_path(relative: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
    }

    #[test]
    fn layer_visibility_realistic() {
        // Load a large example layermap
        let index_raw = std::fs::read_to_string(fixture_path(
            "test_data/indices/mixed_workload/index_part.json",
        ))
        .unwrap();
        let index: IndexPart = serde_json::from_str::<IndexPart>(&index_raw).unwrap();

        let tenant_id = TenantId::generate();
        let tenant_shard_id = TenantShardId::unsharded(tenant_id);
        let timeline_id = TimelineId::generate();

        let mut layer_map = LayerMap::default();
        let mut updates = layer_map.batch_update();
        for (layer_name, layer_metadata) in index.layer_metadata {
            let layer_desc = match layer_name {
                LayerName::Image(layer_name) => PersistentLayerDesc {
                    key_range: layer_name.key_range.clone(),
                    lsn_range: layer_name.lsn_as_range(),
                    tenant_shard_id,
                    timeline_id,
                    is_delta: false,
                    file_size: layer_metadata.file_size,
                },
                LayerName::Delta(layer_name) => PersistentLayerDesc {
                    key_range: layer_name.key_range,
                    lsn_range: layer_name.lsn_range,
                    tenant_shard_id,
                    timeline_id,
                    is_delta: true,
                    file_size: layer_metadata.file_size,
                },
            };
            updates.insert_historic(layer_desc);
        }
        updates.flush();

        let read_points = vec![index.metadata.disk_consistent_lsn()];
        let (layer_visibilities, shadow) = layer_map.get_visibility(read_points);
        for (layer_desc, visibility) in &layer_visibilities {
            tracing::info!("{layer_desc:?}: {visibility:?}");
            eprintln!("{layer_desc:?}: {visibility:?}");
        }

        // The shadow should be non-empty, since there were some image layers
        assert!(!shadow.ranges.is_empty());

        // At least some layers should be marked covered
        assert!(layer_visibilities
            .iter()
            .any(|i| matches!(i.1, LayerVisibilityHint::Covered)));

        let layer_visibilities = layer_visibilities.into_iter().collect::<HashMap<_, _>>();

        // Brute force validation: a layer should be marked covered if and only if there are image layers above it in LSN order which cover it
        for (layer_desc, visible) in &layer_visibilities {
            let mut coverage = KeySpaceRandomAccum::new();
            let mut covered_by = Vec::new();

            for other_layer in layer_map.iter_historic_layers() {
                if &other_layer == layer_desc {
                    continue;
                }
                if !other_layer.is_delta()
                    && other_layer.image_layer_lsn() >= Lsn(layer_desc.get_lsn_range().end.0 - 1)
                    && other_layer.key_range.start <= layer_desc.key_range.end
                    && layer_desc.key_range.start <= other_layer.key_range.end
                {
                    coverage.add_range(other_layer.get_key_range());
                    covered_by.push((*other_layer).clone());
                }
            }
            let coverage = coverage.to_keyspace();

            let expect_visible = if coverage.ranges.len() == 1
                && coverage.contains(&layer_desc.key_range.start)
                && coverage.contains(&Key::from_i128(layer_desc.key_range.end.to_i128() - 1))
            {
                LayerVisibilityHint::Covered
            } else {
                LayerVisibilityHint::Visible
            };

            if expect_visible != *visible {
                eprintln!(
                    "Layer {}..{} @ {}..{} (delta={}) is {visible:?}, should be {expect_visible:?}",
                    layer_desc.key_range.start,
                    layer_desc.key_range.end,
                    layer_desc.lsn_range.start,
                    layer_desc.lsn_range.end,
                    layer_desc.is_delta()
                );
                if expect_visible == LayerVisibilityHint::Covered {
                    eprintln!("Covered by:");
                    for other in covered_by {
                        eprintln!(
                            "  {}..{} @ {}",
                            other.get_key_range().start,
                            other.get_key_range().end,
                            other.image_layer_lsn()
                        );
                    }
                    if let Some(range) = coverage.ranges.first() {
                        eprintln!(
                            "Total coverage from contributing layers: {}..{}",
                            range.start, range.end
                        );
                    } else {
                        eprintln!(
                            "Total coverage from contributing layers: {:?}",
                            coverage.ranges
                        );
                    }
                }
            }
            assert_eq!(expect_visible, *visible);
        }

        // Sanity: the layer that holds latest data for the DBDIR key should always be visible
        // (just using this key as a key that will always exist for any layermap fixture)
        let dbdir_layer = layer_map
            .search(DBDIR_KEY, index.metadata.disk_consistent_lsn())
            .unwrap();
        assert!(matches!(
            layer_visibilities.get(&dbdir_layer.layer).unwrap(),
            LayerVisibilityHint::Visible
        ));
    }
}
