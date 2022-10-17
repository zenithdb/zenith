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

use crate::metrics::NUM_ONDISK_LAYERS;
use crate::repository::Key;
use crate::tenant::inmemory_layer::InMemoryLayer;
use crate::tenant::storage_layer::Layer;
use crate::tenant::storage_layer::{range_eq, range_overlaps};
use anyhow::Result;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use tracing::*;
use utils::lsn::Lsn;

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
struct BTreeKey {
    lsn: Lsn,
    seq: usize,
}

impl BTreeKey {
    fn new(lsn: Lsn) -> BTreeKey {
        BTreeKey { lsn, seq: 0 }
    }
}

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

    /// All the historic layers are kept here
    historic_layers: BTreeMap<BTreeKey, Arc<dyn Layer>>,
    layers_seqno: usize,

    /// Latest stored delta layer
    latest_delta_layer: Option<Arc<dyn Layer>>,
}

/// Return value of LayerMap::search
pub struct SearchResult {
    pub layer: Arc<dyn Layer>,
    pub lsn_floor: Lsn,
}

impl LayerMap {
    ///
    /// Find the latest layer that covers the given 'key', with lsn <
    /// 'end_lsn'.
    ///
    /// Returns the layer, if any, and an 'lsn_floor' value that
    /// indicates which portion of the layer the caller should
    /// check. 'lsn_floor' is normally the start-LSN of the layer, but
    /// can be greater if there is an overlapping layer that might
    /// contain the version, even if it's missing from the returned
    /// layer.
    ///
    pub fn search(&self, key: Key, end_lsn: Lsn) -> Result<Option<SearchResult>> {
        // linear search
        // Find the latest image layer that covers the given key
        let mut latest_img: Option<Arc<dyn Layer>> = None;
        let mut latest_img_lsn = Lsn(0);
        let mut iter = self
            .historic_layers
            .range(BTreeKey::new(Lsn(0))..BTreeKey::new(end_lsn));
        while let Some((_key, l)) = iter.next_back() {
            if l.is_incremental() {
                continue;
            }
            if !l.get_key_range().contains(&key) {
                continue;
            }
            let img_lsn = l.get_lsn_range().start;
            assert!(img_lsn < end_lsn);
            if Lsn(img_lsn.0 + 1) == end_lsn {
                // found exact match
                return Ok(Some(SearchResult {
                    layer: Arc::clone(l),
                    lsn_floor: img_lsn,
                }));
            }
            latest_img = Some(Arc::clone(l));
            latest_img_lsn = img_lsn;
            break;
        }

        // Search the delta layers
        let mut latest_delta: Option<Arc<dyn Layer>> = None;
        let mut iter = self
            .historic_layers
            .range(BTreeKey::new(Lsn(0))..BTreeKey::new(end_lsn));
        while let Some((_key, l)) = iter.next_back() {
            if !l.is_incremental() {
                continue;
            }
            if !l.get_key_range().contains(&key) {
                continue;
            }
            if l.get_lsn_range().start >= end_lsn {
                info!(
                    "Candidate delta layer {}..{} is too new for lsn {}",
                    l.get_lsn_range().start,
                    l.get_lsn_range().end,
                    end_lsn
                );
            }
            assert!(l.get_lsn_range().start < end_lsn);
            if l.get_lsn_range().end <= latest_img_lsn {
                continue;
            }
            if l.get_lsn_range().end >= end_lsn {
                // this layer contains the requested point in the key/lsn space.
                // No need to search any further
                trace!(
                    "found layer {} for request on {key} at {end_lsn}",
                    l.filename().display(),
                );
                latest_delta.replace(Arc::clone(l));
                break;
            }
            // this layer's end LSN is smaller than the requested point. If there's
            // nothing newer, this is what we need to return. Remember this.
            if let Some(old_candidate) = &latest_delta {
                if l.get_lsn_range().end > old_candidate.get_lsn_range().end {
                    latest_delta.replace(Arc::clone(l));
                }
            } else {
                latest_delta.replace(Arc::clone(l));
            }
        }
        if let Some(l) = latest_delta {
            trace!(
                "found (old) layer {} for request on {key} at {end_lsn}",
                l.filename().display(),
            );
            let lsn_floor = std::cmp::max(Lsn(latest_img_lsn.0 + 1), l.get_lsn_range().start);
            Ok(Some(SearchResult {
                lsn_floor,
                layer: l,
            }))
        } else if let Some(l) = latest_img {
            trace!("found img layer and no deltas for request on {key} at {end_lsn}");
            Ok(Some(SearchResult {
                lsn_floor: latest_img_lsn,
                layer: l,
            }))
        } else {
            trace!("no layer found for request on {key} at {end_lsn}");
            Ok(None)
        }
    }

    ///
    /// Insert an on-disk layer
    ///
    pub fn insert_historic(&mut self, layer: Arc<dyn Layer>) {
        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            self.latest_delta_layer = Some(layer.clone());
        } else if !layer.is_incremental() {
            // If latest delta layer is followed by image layers
            // then reset it, preventing generation of partial image layer
            if let Some(latest_delta) = &self.latest_delta_layer {
                // May be it is more correct to use contains() rather than inrestects
                // but one delta layer can be covered by several image layers.
                let kr1 = layer.get_key_range();
                let kr2 = latest_delta.get_key_range();
                if range_overlaps(&kr1, &kr2) {
                    self.latest_delta_layer = None;
                }
            }
        }
        self.historic_layers.insert(
            BTreeKey {
                lsn: layer.get_lsn_range().start,
                seq: self.layers_seqno,
            },
            layer,
        );
        self.layers_seqno += 1;
        NUM_ONDISK_LAYERS.inc();
    }

    ///
    /// Remove an on-disk layer from the map.
    ///
    /// This should be called when the corresponding file on disk has been deleted.
    ///
    pub fn remove_historic(&mut self, layer: Arc<dyn Layer>) {
        if layer.get_key_range() == (Key::MIN..Key::MAX) {
            if let Some(latest_layer) = &self.latest_delta_layer {
                #[allow(clippy::vtable_address_comparisons)]
                if Arc::ptr_eq(&layer, latest_layer) {
                    self.latest_delta_layer = None;
                }
            }
        }
        let len_before = self.historic_layers.len();
        #[allow(clippy::vtable_address_comparisons)]
        self.historic_layers
            .retain(|_key, other| !Arc::ptr_eq(other, &layer));
        if self.historic_layers.len() != len_before - 1 {
            assert!(self.historic_layers.len() == len_before);
            error!(
                "Failed to remove {} layer: {}..{}__{}..{}",
                if layer.is_incremental() {
                    "inremental"
                } else {
                    "image"
                },
                layer.get_key_range().start,
                layer.get_key_range().end,
                layer.get_lsn_range().start,
                layer.get_lsn_range().end
            );
        }
        assert!(self.historic_layers.len() == len_before - 1);
        NUM_ONDISK_LAYERS.dec();
    }

    /// Is there a newer image layer for given key- and LSN-range?
    ///
    /// This is used for garbage collection, to determine if an old layer can
    /// be deleted.
    pub fn image_layer_exists(
        &self,
        key_range: &Range<Key>,
        lsn_range: &Range<Lsn>,
    ) -> Result<bool> {
        let mut range_remain = key_range.clone();

        loop {
            let mut made_progress = false;
            for (_key, l) in self
                .historic_layers
                .range(BTreeKey::new(lsn_range.start)..BTreeKey::new(lsn_range.end))
            {
                if l.is_incremental() {
                    continue;
                }
                let img_lsn = l.get_lsn_range().start;
                if l.get_key_range().contains(&range_remain.start) && lsn_range.contains(&img_lsn) {
                    made_progress = true;
                    let img_key_end = l.get_key_range().end;

                    if img_key_end >= range_remain.end {
                        return Ok(true);
                    }
                    range_remain.start = img_key_end;
                }
            }

            if !made_progress {
                return Ok(false);
            }
        }
    }

    pub fn iter_historic_layers(&self) -> impl '_ + Iterator<Item = Arc<dyn Layer>> {
        self.historic_layers
            .iter()
            .map(|(_key, layer)| layer.clone())
    }

    /// Find the last image layer that covers 'key', ignoring any image layers
    /// newer than 'lsn'.
    fn find_latest_image(&self, key: Key, lsn: Lsn) -> Option<Arc<dyn Layer>> {
        let mut iter = self
            .historic_layers
            .range(BTreeKey::new(Lsn(0))..BTreeKey::new(lsn + 1));
        while let Some((_key, l)) = iter.next_back() {
            if l.is_incremental() {
                continue;
            }

            if !l.get_key_range().contains(&key) {
                continue;
            }
            let this_lsn = l.get_lsn_range().start;
            assert!(this_lsn <= lsn);
            return Some(Arc::clone(l));
        }
        None
    }

    ///
    /// Divide the whole given range of keys into sub-ranges based on the latest
    /// image layer that covers each range. (This is used when creating  new
    /// image layers)
    ///
    // FIXME: clippy complains that the result type is very complex. She's probably
    // right...
    #[allow(clippy::type_complexity)]
    pub fn image_coverage(
        &self,
        key_range: &Range<Key>,
        lsn: Lsn,
    ) -> Result<Vec<(Range<Key>, Option<Arc<dyn Layer>>)>> {
        let mut points = vec![key_range.start];
        for (_lsn, l) in self
            .historic_layers
            .range(BTreeKey::new(Lsn(0))..BTreeKey::new(lsn + 1))
        {
            assert!(l.get_lsn_range().start <= lsn);
            let range = l.get_key_range();
            if key_range.contains(&range.start) {
                points.push(l.get_key_range().start);
            }
            if key_range.contains(&range.end) {
                points.push(l.get_key_range().end);
            }
        }
        points.push(key_range.end);

        points.sort();
        points.dedup();

        // Ok, we now have a list of "interesting" points in the key space

        // For each range between the points, find the latest image
        let mut start = *points.first().unwrap();
        let mut ranges = Vec::new();
        for end in points[1..].iter() {
            let img = self.find_latest_image(start, lsn);

            ranges.push((start..*end, img));

            start = *end;
        }
        Ok(ranges)
    }

    /// Count how many L1 delta layers there are that overlap with the
    /// given key and LSN range.
    pub fn count_deltas(&self, key_range: &Range<Key>, lsn_range: &Range<Lsn>) -> Result<usize> {
        let mut result = 0;
        if lsn_range.start >= lsn_range.end {
            return Ok(0);
        }
        for (_lsn, l) in self
            .historic_layers
            .range(BTreeKey::new(lsn_range.start)..BTreeKey::new(lsn_range.end))
        {
            if !l.is_incremental() {
                continue;
            }
            if !range_overlaps(&l.get_key_range(), key_range) {
                continue;
            }
            assert!(range_overlaps(&l.get_lsn_range(), lsn_range));

            // We ignore level0 delta layers. Unless the whole keyspace fits
            // into one partition
            if !range_eq(key_range, &(Key::MIN..Key::MAX))
                && range_eq(&l.get_key_range(), &(Key::MIN..Key::MAX))
            {
                continue;
            }

            result += 1;
        }
        Ok(result)
    }

    /// Return all L0 delta layers
    pub fn get_latest_delta_layer(&mut self) -> Option<Arc<dyn Layer>> {
        self.latest_delta_layer.take()
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
        for (_key, layer) in self.historic_layers.iter() {
            layer.dump(verbose)?;
        }
        println!("End dump LayerMap");
        Ok(())
    }
}
