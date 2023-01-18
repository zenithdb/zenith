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

mod historic_layer_coverage;
mod layer_coverage;

use crate::keyspace::KeyPartitioning;
use crate::metrics::NUM_ONDISK_LAYERS;
use crate::repository::Key;
use crate::tenant::storage_layer::InMemoryLayer;
use crate::tenant::storage_layer::Layer;
use anyhow::Result;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use utils::lsn::Lsn;

use historic_layer_coverage::BufferedHistoricLayerCoverage;

///
/// LayerMap tracks what layers exist on a timeline.
///
pub struct LayerMap<L: ?Sized> {
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
    historic: BufferedHistoricLayerCoverage<Arc<L>>,

    /// L0 layers have key range Key::MIN..Key::MAX, and locating them using R-Tree search is very inefficient.
    /// So L0 layers are held in l0_delta_layers vector, in addition to the R-tree.
    l0_delta_layers: Vec<Arc<L>>,
}

impl<L: ?Sized> Default for LayerMap<L> {
    fn default() -> Self {
        Self {
            open_layer: None,
            next_open_layer_at: None,
            frozen_layers: VecDeque::default(),
            l0_delta_layers: Vec::default(),
            historic: BufferedHistoricLayerCoverage::default(),
        }
    }
}

/// Return value of LayerMap::search
pub struct SearchResult<L: ?Sized> {
    pub layer: Arc<L>,
    pub lsn_floor: Lsn,
}

impl<L> LayerMap<L>
where
    L: ?Sized + Layer,
{
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
    pub fn search(&self, key: Key, end_lsn: Lsn) -> Option<SearchResult<L>> {
        let version = self.historic.get().unwrap().get_version(end_lsn.0 - 1)?;
        let latest_delta = version.delta_coverage.query(key.to_i128());
        let latest_image = version.image_coverage.query(key.to_i128());

        match (latest_delta, latest_image) {
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
                let image_is_newer = image.get_lsn_range().end > delta.get_lsn_range().end;
                let image_exact_match = Lsn(img_lsn.0 + 1) == end_lsn;
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

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<L>) {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();
        self.historic.insert(
            kr.start.to_i128()..kr.end.to_i128(),
            lr.start.0..lr.end.0,
            Arc::clone(&layer),
            !layer.is_incremental(),
        );

        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            self.l0_delta_layers.push(layer);
        }

        NUM_ONDISK_LAYERS.inc();
    }

    /// Must be called after a batch of insert_historic calls, before querying
    pub fn rebuild_index(&mut self) {
        self.historic.rebuild();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<L>) {
        let kr = layer.get_key_range();
        let lr = layer.get_lsn_range();
        self.historic.remove(
            kr.start.to_i128()..kr.end.to_i128(),
            lr.start.0..lr.end.0,
            !layer.is_incremental(),
        );

        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            let len_before = self.l0_delta_layers.len();

            // FIXME: ptr_eq might fail to return true for 'dyn'
            // references.  Clippy complains about this. In practice it
            // seems to work, the assertion below would be triggered
            // otherwise but this ought to be fixed.
            #[allow(clippy::vtable_address_comparisons)]
            self.l0_delta_layers
                .retain(|other| !Arc::ptr_eq(other, &layer));
            assert_eq!(self.l0_delta_layers.len(), len_before - 1);
        }

        NUM_ONDISK_LAYERS.dec();
    }

    /// Is there a newer image layer for given key- and LSN-range? Or a set
    /// of image layers within the specified lsn range that cover the entire
    /// specified key range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn image_layer_exists(&self, key: &Range<Key>, lsn: &Range<Lsn>) -> Result<bool> {
        if key.is_empty() {
            return Ok(true);
        }

        let version = match self.historic.get().unwrap().get_version(lsn.end.0 - 1) {
            Some(v) => v,
            None => return Ok(false),
        };

        let start = key.start.to_i128();
        let end = key.end.to_i128();

        let layer_covers = |layer: Option<Arc<L>>| match layer {
            Some(layer) => layer.get_lsn_range().start >= lsn.start,
            None => false,
        };

        // Check the start is covered
        if !layer_covers(version.image_coverage.query(start)) {
            return Ok(false);
        }

        // Check after all changes of coverage
        for (_, change_val) in version.image_coverage.range(start..end) {
            if !layer_covers(change_val) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn iter_historic_layers(&self) -> impl '_ + Iterator<Item = Arc<L>> {
        self.historic.iter()
    }

    ///
    /// Divide the whole given range of keys into sub-ranges based on the latest
    /// image layer that covers each range at the specified lsn (inclusive).
    /// This is used when creating  new image layers.
    ///
    // FIXME: clippy complains that the result type is very complex. She's probably
    // right...
    #[allow(clippy::type_complexity)]
    pub fn image_coverage(
        &self,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Result<Vec<(Range<Key>, Option<Arc<L>>)>> {
        let version = match self.historic.get().unwrap().get_version(lsn.0) {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let start = key_range.start.to_i128();
        let end = key_range.end.to_i128();

        // Initialize loop variables
        let mut coverage: Vec<(Range<Key>, Option<Arc<L>>)> = vec![];
        let mut current_key = start;
        let mut current_val = version.image_coverage.query(start);

        // Loop through the change events and push intervals
        for (change_key, change_val) in version.image_coverage.range(start..end) {
            let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
            coverage.push((kr, current_val.take()));
            current_key = change_key;
            current_val = change_val.clone();
        }

        // Add the final interval
        let kr = Key::from_i128(current_key)..Key::from_i128(end);
        coverage.push((kr, current_val.take()));

        Ok(coverage)
    }

    /// Count the height of the tallest stack of deltas in this 2d region.
    ///
    /// If `limit` is provided we don't try to count above that number.
    ///
    /// This number is used to compute the largest number of deltas that
    /// we'll need to visit for any page reconstruction in this region.
    /// We use this heuristic to decide whether to create an image layer.
    pub fn count_deltas(
        &self,
        key: &Range<Key>,
        lsn: &Range<Lsn>,
        limit: Option<usize>,
    ) -> Result<usize> {
        // We get the delta coverage of the region, and for each part of the coverage
        // we recurse right underneath the delta. The recursion depth is limited by
        // the largest result this function could return, which is in practice between
        // 3 and 10 (since we usually try to create an image when the number gets larger).

        if lsn.is_empty() || key.is_empty() || limit == Some(0) {
            return Ok(0);
        }

        let version = match self.historic.get().unwrap().get_version(lsn.end.0 - 1) {
            Some(v) => v,
            None => return Ok(0),
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
            if let Some(val) = current_val {
                if val.get_lsn_range().end > lsn.start {
                    let kr = Key::from_i128(current_key)..Key::from_i128(change_key);
                    let lr = lsn.start..val.get_lsn_range().start;
                    if !kr.is_empty() {
                        let new_limit = limit.map(|l| l - 1);
                        let max_stacked_deltas_underneath =
                            self.count_deltas(&kr, &lr, new_limit)?;
                        max_stacked_deltas =
                            std::cmp::max(max_stacked_deltas, 1 + max_stacked_deltas_underneath);
                    }
                }
            }

            current_key = change_key;
            current_val = change_val.clone();
        }

        // Consider the last part
        if let Some(val) = current_val {
            if val.get_lsn_range().end > lsn.start {
                let kr = Key::from_i128(current_key)..Key::from_i128(end);
                let lr = lsn.start..val.get_lsn_range().start;

                if !kr.is_empty() {
                    let new_limit = limit.map(|l| l - 1);
                    let max_stacked_deltas_underneath = self.count_deltas(&kr, &lr, new_limit)?;
                    max_stacked_deltas =
                        std::cmp::max(max_stacked_deltas, 1 + max_stacked_deltas_underneath);
                }
            }
        }

        Ok(max_stacked_deltas)
    }

    /// Count how many layers we need to visit for given key-lsn pair.
    ///
    /// Used as a helper for correctness checks only. Performance not critical.
    pub fn get_difficulty(&self, lsn: Lsn, key: Key) -> usize {
        match self.search(key, lsn) {
            Some(search_result) => {
                if search_result.layer.is_incremental() {
                    1 + self.get_difficulty(search_result.lsn_floor, key)
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
                            std::cmp::max(difficulty, self.get_difficulty(lsn, range.start));
                    }
                    while let Some(key) = keys_iter.peek() {
                        if key >= &range.end {
                            break;
                        }
                        let key = keys_iter.next().unwrap();
                        if key < range.start {
                            continue;
                        }
                        difficulty = std::cmp::max(difficulty, self.get_difficulty(lsn, key));
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
                    for (img_range, last_img) in self
                        .image_coverage(range, lsn)
                        .expect("why would this err?")
                    {
                        if limit == Some(difficulty) {
                            break;
                        }
                        let img_lsn = if let Some(last_img) = last_img {
                            last_img.get_lsn_range().end
                        } else {
                            Lsn(0)
                        };

                        if img_lsn < lsn {
                            let num_deltas = self
                                .count_deltas(&img_range, &(img_lsn..lsn), limit)
                                .expect("why would this err lol?");
                            difficulty = std::cmp::max(difficulty, num_deltas);
                        }
                    }
                }
                difficulty
            })
            .collect()
    }

    /// Return all L0 delta layers
    pub fn get_level0_deltas(&self) -> Result<Vec<Arc<L>>> {
        Ok(self.l0_delta_layers.clone())
    }

    /// debugging function to print out the contents of the layer map
    #[allow(unused)]
    pub fn dump(&self, verbose: bool) -> Result<()> {
        println!("Begin dump LayerMap");

        println!("open_layer:");
        if let Some(open_layer) = &self.open_layer {
            open_layer.dump(verbose)?;
        }

        println!("frozen_layers:");
        for frozen_layer in self.frozen_layers.iter() {
            frozen_layer.dump(verbose)?;
        }

        println!("historic_layers:");
        for layer in self.iter_historic_layers() {
            layer.dump(verbose)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }
}
