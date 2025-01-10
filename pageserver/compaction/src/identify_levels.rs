//! An LSM tree consists of multiple levels, each exponentially larger than the
//! previous level. And each level consists of multiple "tiers". With tiered
//! compaction, a level is compacted when it has accumulated more than N tiers,
//! forming one tier on the next level.
//!
//! In the pageserver, we don't explicitly track the levels and tiers. Instead,
//! we identify them by looking at the shapes of the layers. It's an easy task
//! for a human, but it's not straightforward to come up with the exact
//! rules. Especially if there are cases like interrupted, half-finished
//! compactions, or highly skewed data distributions that have let us "skip"
//! some levels. It's not critical to classify all cases correctly; at worst we
//! delay some compaction work, and suffer from more read amplification, or we
//! perform some unnecessary compaction work.
//!
//! `identify_level` performs that shape-matching.
//!
//! It returns a Level struct, which has `depth()` function to count the number
//! of "tiers" in the level. The tier count is the max depth of stacked layers
//! within the level. That's a good measure, because the point of compacting is
//! to reduce read amplification, and the depth is what determines that.
//!
//! One interesting effect of this is that if we generate very small delta
//! layers at L0, e.g. because the L0 layers are flushed by timeout rather than
//! because they reach the target size, the L0 compaction will combine them to
//! one larger file. But if the combined file is still smaller than the target
//! file size, the file will still be considered to be part of L0 at the next
//! iteration.

use anyhow::bail;
use std::collections::BTreeSet;
use std::ops::Range;
use utils::lsn::Lsn;

use crate::interface::*;

use tracing::{info, trace};

pub struct Level<L> {
    pub lsn_range: Range<Lsn>,
    pub layers: Vec<L>,
}

/// Identify an LSN > `end_lsn` that partitions the LSN space, so that there are
/// no layers that cross the boundary LSN.
///
/// A further restriction is that all layers in the returned partition cover at
/// most 'lsn_max_size' LSN bytes.
pub async fn identify_level<K, L>(
    all_layers: Vec<L>,
    end_lsn: Lsn,
    lsn_max_size: u64,
) -> anyhow::Result<Option<Level<L>>>
where
    K: CompactionKey,
    L: CompactionLayer<K> + Clone,
{
    // filter out layers that are above the `end_lsn`, they are completely irrelevant.
    let mut layers = Vec::new();
    for l in all_layers {
        if l.lsn_range().start < end_lsn && l.lsn_range().end > end_lsn {
            // shouldn't happen. Indicates that the caller passed a bogus
            // end_lsn.
            bail!("identify_level() called with end_lsn that does not partition the LSN space: end_lsn {} intersects with layer {}", end_lsn, l.short_id());
        }
        // include image layers sitting exacty at `end_lsn`.
        let is_image = !l.is_delta();
        if (is_image && l.lsn_range().start > end_lsn)
            || (!is_image && l.lsn_range().start >= end_lsn)
        {
            continue;
        }
        layers.push(l);
    }
    // All the remaining layers either belong to this level, or are below it.
    info!(
        "identify level at {}, size {}, num layers below: {}",
        end_lsn,
        lsn_max_size,
        layers.len()
    );
    if layers.is_empty() {
        return Ok(None);
    }

    // Walk the ranges in LSN order.
    //
    // ----- end_lsn
    //  |
    //  |
    //  v
    //
    layers.sort_by_key(|l| l.lsn_range().end);
    let mut candidate_start_lsn = end_lsn;
    let mut candidate_layers: Vec<L> = Vec::new();
    let mut current_best_start_lsn = end_lsn;
    let mut current_best_layers: Vec<L> = Vec::new();
    let mut iter = layers.into_iter();
    loop {
        let Some(l) = iter.next_back() else {
            // Reached end. Accept the last candidate
            current_best_start_lsn = candidate_start_lsn;
            current_best_layers.extend_from_slice(&std::mem::take(&mut candidate_layers));
            break;
        };
        trace!(
            "inspecting {} for candidate {}, current best {}",
            l.short_id(),
            candidate_start_lsn,
            current_best_start_lsn
        );

        let r = l.lsn_range();

        // Image layers don't restrict our choice of cutoff LSN
        if l.is_delta() {
            // Is this candidate workable? In other words, are there any
            // delta layers that span across this LSN
            //
            // Valid:                 Not valid:
            //  +                     +
            //  |                     | +
            //  +  <- candidate       + |   <- candidate
            //     +                    +
            //     |
            //     +
            if r.end <= candidate_start_lsn {
                // Hooray, there are no crossing LSNs. And we have visited
                // through all the layers within candidate..end_lsn. The
                // current candidate can be accepted.
                current_best_start_lsn = r.end;
                current_best_layers.extend_from_slice(&std::mem::take(&mut candidate_layers));
                candidate_start_lsn = r.start;
            }

            // Is it small enough to be considered part of this level?
            if r.end.0 - r.start.0 > lsn_max_size {
                // Too large, this layer belongs to next level. Stop.
                trace!(
                    "too large {}, size {} vs {}",
                    l.short_id(),
                    r.end.0 - r.start.0,
                    lsn_max_size
                );
                break;
            }

            // If this crosses the candidate lsn, push it down.
            if r.start < candidate_start_lsn {
                trace!(
                    "layer {} prevents from stopping at {}",
                    l.short_id(),
                    candidate_start_lsn
                );
                candidate_start_lsn = r.start;
            }
        }

        // Include this layer in our candidate
        candidate_layers.push(l);
    }

    Ok(if current_best_start_lsn == end_lsn {
        // empty level
        None
    } else {
        Some(Level {
            lsn_range: current_best_start_lsn..end_lsn,
            layers: current_best_layers,
        })
    })
}

impl<L> Level<L> {
    /// Count the number of deltas stacked on each other.
    pub fn depth<K>(&self) -> u64
    where
        K: CompactionKey,
        L: CompactionLayer<K>,
    {
        struct Event<K> {
            key: K,
            layer_idx: usize,
            start: bool,
        }
        let mut events: Vec<Event<K>> = Vec::new();
        for (idx, l) in self.layers.iter().enumerate() {
            let key_range = l.key_range();
            if key_range.end == key_range.start.next() && l.is_delta() {
                // Ignore single-key delta layers as they can be stacked on top of each other
                // as that is the only way to cut further.
                continue;
            }
            events.push(Event {
                key: l.key_range().start,
                layer_idx: idx,
                start: true,
            });
            events.push(Event {
                key: l.key_range().end,
                layer_idx: idx,
                start: false,
            });
        }
        events.sort_by_key(|e| (e.key, e.start));

        // Sweep the key space left to right. Stop at each distinct key, and
        // count the number of deltas on top of the highest image at that key.
        //
        // This is a little inefficient, as we walk through the active_set on
        // every key. We could increment/decrement a counter on each step
        // instead, but that'd require a bit more complex bookkeeping.
        let mut active_set: BTreeSet<(Lsn, bool, usize)> = BTreeSet::new();
        let mut max_depth = 0;
        let mut events_iter = events.iter().peekable();
        while let Some(e) = events_iter.next() {
            let l = &self.layers[e.layer_idx];
            let is_image = !l.is_delta();

            // update the active set
            if e.start {
                active_set.insert((l.lsn_range().end, is_image, e.layer_idx));
            } else {
                active_set.remove(&(l.lsn_range().end, is_image, e.layer_idx));
            }

            // recalculate depth if this was the last event at this point
            let more_events_at_this_key =
                events_iter.peek().is_some_and(|next_e| next_e.key == e.key);
            if !more_events_at_this_key {
                let mut active_depth = 0;
                for (_end_lsn, is_image, _idx) in active_set.iter().rev() {
                    if *is_image {
                        break;
                    }
                    active_depth += 1;
                }
                if active_depth > max_depth {
                    max_depth = active_depth;
                }
            }
        }
        debug_assert_eq!(active_set, BTreeSet::new());
        max_depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulator::{Key, MockDeltaLayer, MockImageLayer, MockLayer};
    use std::sync::{Arc, Mutex};

    fn delta(key_range: Range<Key>, lsn_range: Range<Lsn>) -> MockLayer {
        MockLayer::Delta(Arc::new(MockDeltaLayer {
            key_range,
            lsn_range,
            // identify_level() doesn't pay attention to the rest of the fields
            file_size: 0,
            deleted: Mutex::new(false),
            records: vec![],
        }))
    }

    fn image(key_range: Range<Key>, lsn: Lsn) -> MockLayer {
        MockLayer::Image(Arc::new(MockImageLayer {
            key_range,
            lsn_range: lsn..(lsn + 1),
            // identify_level() doesn't pay attention to the rest of the fields
            file_size: 0,
            deleted: Mutex::new(false),
        }))
    }

    #[tokio::test]
    async fn test_identify_level() -> anyhow::Result<()> {
        let layers = vec![
            delta(Key::MIN..Key::MAX, Lsn(0x8000)..Lsn(0x9000)),
            delta(Key::MIN..Key::MAX, Lsn(0x5000)..Lsn(0x7000)),
            delta(Key::MIN..Key::MAX, Lsn(0x4000)..Lsn(0x5000)),
            delta(Key::MIN..Key::MAX, Lsn(0x3000)..Lsn(0x4000)),
            delta(Key::MIN..Key::MAX, Lsn(0x2000)..Lsn(0x3000)),
            delta(Key::MIN..Key::MAX, Lsn(0x1000)..Lsn(0x2000)),
        ];

        // All layers fit in the max file size
        let level = identify_level(layers.clone(), Lsn(0x10000), 0x2000)
            .await?
            .unwrap();
        assert_eq!(level.depth(), 6);

        // Same LSN with smaller max file size. The second layer from the top is larger
        // and belongs to next level.
        let level = identify_level(layers.clone(), Lsn(0x10000), 0x1000)
            .await?
            .unwrap();
        assert_eq!(level.depth(), 1);

        // Call with a smaller LSN
        let level = identify_level(layers.clone(), Lsn(0x3000), 0x1000)
            .await?
            .unwrap();
        assert_eq!(level.depth(), 2);

        // Call with an LSN that doesn't partition the space
        let result = identify_level(layers, Lsn(0x6000), 0x1000).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_overlapping_lsn_ranges() -> anyhow::Result<()> {
        // The files LSN ranges overlap, so even though there are more files that
        // fit under the file size, they are not included in the level because they
        // overlap so that we'd need to include the oldest file, too, which is
        // larger
        let layers = vec![
            delta(Key::MIN..Key::MAX, Lsn(0x4000)..Lsn(0x5000)),
            delta(Key::MIN..Key::MAX, Lsn(0x3000)..Lsn(0x4000)), // overlap
            delta(Key::MIN..Key::MAX, Lsn(0x2500)..Lsn(0x3500)), // overlap
            delta(Key::MIN..Key::MAX, Lsn(0x2000)..Lsn(0x3000)), // overlap
            delta(Key::MIN..Key::MAX, Lsn(0x1000)..Lsn(0x2500)), // larger
        ];

        let level = identify_level(layers.clone(), Lsn(0x10000), 0x1000)
            .await?
            .unwrap();
        assert_eq!(level.depth(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_depth_nonoverlapping() -> anyhow::Result<()> {
        // The key ranges don't overlap, so depth is only 1.
        let layers = vec![
            delta(4000..5000, Lsn(0x6000)..Lsn(0x7000)),
            delta(3000..4000, Lsn(0x7000)..Lsn(0x8000)),
            delta(1000..2000, Lsn(0x8000)..Lsn(0x9000)),
        ];

        let level = identify_level(layers.clone(), Lsn(0x10000), 0x2000)
            .await?
            .unwrap();
        assert_eq!(level.layers.len(), 3);
        assert_eq!(level.depth(), 1);

        // Staggered. The 1st and 3rd layer don't overlap with each other.
        let layers = vec![
            delta(1000..2000, Lsn(0x8000)..Lsn(0x9000)),
            delta(1500..2500, Lsn(0x7000)..Lsn(0x8000)),
            delta(2000..3000, Lsn(0x6000)..Lsn(0x7000)),
        ];

        let level = identify_level(layers.clone(), Lsn(0x10000), 0x2000)
            .await?
            .unwrap();
        assert_eq!(level.layers.len(), 3);
        assert_eq!(level.depth(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_depth_images() -> anyhow::Result<()> {
        let layers: Vec<MockLayer> = vec![
            delta(1000..2000, Lsn(0x8000)..Lsn(0x9000)),
            delta(1500..2500, Lsn(0x7000)..Lsn(0x8000)),
            delta(2000..3000, Lsn(0x6000)..Lsn(0x7000)),
            // This covers the same key range as the 2nd delta layer. The depth
            // in that key range is therefore 0.
            image(1500..2500, Lsn(0x9000)),
        ];

        let level = identify_level(layers.clone(), Lsn(0x10000), 0x2000)
            .await?
            .unwrap();
        assert_eq!(level.layers.len(), 4);
        assert_eq!(level.depth(), 1);
        Ok(())
    }
}
