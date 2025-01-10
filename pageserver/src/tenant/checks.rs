use std::collections::BTreeSet;

use itertools::Itertools;
use pageserver_compaction::helpers::overlaps_with;

use super::storage_layer::LayerName;

/// Checks whether a layer map is valid (i.e., is a valid result of the current compaction algorithm if nothing goes wrong).
///
/// The function implements a fast path check and a slow path check.
///
/// The fast path checks if we can split the LSN range of a delta layer only at the LSNs of the delta layers. For example,
///
/// ```plain
/// |       |                 |       |
/// |   1   |    |   2   |    |   3   |
/// |       |    |       |    |       |
/// ```
///
/// This is not a valid layer map because the LSN range of layer 1 intersects with the LSN range of layer 2. 1 and 2 should have
/// the same LSN range.
///
/// The exception is that when layer 2 only contains a single key, it could be split over the LSN range. For example,
///
/// ```plain
/// |       |    |   2   |    |       |
/// |   1   |    |-------|    |   3   |
/// |       |    |   4   |    |       |
///
/// If layer 2 and 4 contain the same single key, this is also a valid layer map.
///
/// However, if a partial compaction is still going on, it is possible that we get a layer map not satisfying the above condition.
/// Therefore, we fallback to simply check if any of the two delta layers overlap. (See "A slow path...")
pub fn check_valid_layermap(metadata: &[LayerName]) -> Option<String> {
    let mut lsn_split_point = BTreeSet::new(); // TODO: use a better data structure (range tree / range set?)
    let mut all_delta_layers = Vec::new();
    for name in metadata {
        if let LayerName::Delta(layer) = name {
            all_delta_layers.push(layer.clone());
        }
    }
    for layer in &all_delta_layers {
        if layer.key_range.start.next() != layer.key_range.end {
            let lsn_range = &layer.lsn_range;
            lsn_split_point.insert(lsn_range.start);
            lsn_split_point.insert(lsn_range.end);
        }
    }
    for (idx, layer) in all_delta_layers.iter().enumerate() {
        if layer.key_range.start.next() == layer.key_range.end {
            continue;
        }
        let lsn_range = layer.lsn_range.clone();
        let intersects = lsn_split_point.range(lsn_range).collect_vec();
        if intersects.len() > 1 {
            // A slow path to check if the layer intersects with any other delta layer.
            for (other_idx, other_layer) in all_delta_layers.iter().enumerate() {
                if other_idx == idx {
                    // do not check self intersects with self
                    continue;
                }
                if overlaps_with(&layer.lsn_range, &other_layer.lsn_range)
                    && overlaps_with(&layer.key_range, &other_layer.key_range)
                {
                    let err = format!(
                            "layer violates the layer map LSN split assumption: layer {} intersects with layer {}",
                            layer, other_layer
                        );
                    return Some(err);
                }
            }
        }
    }
    None
}
