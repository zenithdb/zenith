//! Tool for extracting content-dependent metadata about layers. Useful for scanning real project layer files and evaluating the effectiveness of different heuristics on them.
//!
//! Currently it only analyzes holes, which are regions within the layer range that the layer contains no updates for. In the future it might do more analysis (maybe key quantiles?) but it should never return sensitive data.

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use pageserver::context::{DownloadBehavior, RequestContext};
use pageserver::task_mgr::TaskKind;
use pageserver::tenant::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ops::Range;
use std::{fs, str};

use pageserver::page_cache::PAGE_SZ;
use pageserver::repository::{Key, KEY_SIZE};
use pageserver::tenant::block_io::FileBlockReader;
use pageserver::tenant::disk_btree::{DiskBtreeReader, VisitDirection};
use pageserver::tenant::storage_layer::delta_layer::{Summary, DELTA_KEY_SIZE};
use pageserver::tenant::storage_layer::range_overlaps;
use pageserver::virtual_file::VirtualFile;

use utils::{bin_ser::BeSer, lsn::Lsn};

use crate::AnalyzeLayerMapCmd;

const MIN_HOLE_LENGTH: i128 = (128 * 1024 * 1024 / PAGE_SZ) as i128;
const DEFAULT_MAX_HOLES: usize = 10;

/// Wrapper for key range to provide reverse ordering by range length for BinaryHeap
#[derive(PartialEq, Eq)]
pub struct Hole(Range<Key>);

impl Ord for Hole {
    fn cmp(&self, other: &Self) -> Ordering {
        let other_len = other.0.end.to_i128() - other.0.start.to_i128();
        let self_len = self.0.end.to_i128() - self.0.start.to_i128();
        other_len.cmp(&self_len)
    }
}

impl PartialOrd for Hole {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct LayerFile {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,
    pub is_delta: bool,
    pub holes: Vec<Hole>,
}

impl LayerFile {
    fn skips(&self, key_range: &Range<Key>) -> bool {
        if !range_overlaps(&self.key_range, key_range) {
            return false;
        }
        let start = match self
            .holes
            .binary_search_by_key(&key_range.start, |hole| hole.0.start)
        {
            Ok(index) => index,
            Err(index) => {
                if index == 0 {
                    return false;
                }
                index - 1
            }
        };
        self.holes[start].0.end >= key_range.end
    }
}

pub(crate) fn parse_filename(name: &str) -> Option<LayerFile> {
    let split: Vec<&str> = name.split("__").collect();
    if split.len() != 2 {
        return None;
    }
    let keys: Vec<&str> = split[0].split('-').collect();
    let mut lsns: Vec<&str> = split[1].split('-').collect();
    let is_delta = if lsns.len() == 1 {
        lsns.push(lsns[0]);
        false
    } else {
        true
    };

    let key_range = Key::from_hex(keys[0]).unwrap()..Key::from_hex(keys[1]).unwrap();
    let lsn_range = Lsn::from_hex(lsns[0]).unwrap()..Lsn::from_hex(lsns[1]).unwrap();
    let holes = Vec::new();
    Some(LayerFile {
        key_range,
        lsn_range,
        is_delta,
        holes,
    })
}

// Finds the max_holes largest holes, ignoring any that are smaller than MIN_HOLE_LENGTH"
async fn get_holes(path: &Utf8Path, max_holes: usize, ctx: &RequestContext) -> Result<Vec<Hole>> {
    let file = FileBlockReader::new(VirtualFile::open(path).await?);
    let summary_blk = file.read_blk(0, ctx).await?;
    let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;
    let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
        actual_summary.index_start_blk,
        actual_summary.index_root_blk,
        file,
    );
    // min-heap (reserve space for one more element added before eviction)
    let mut heap: BinaryHeap<Hole> = BinaryHeap::with_capacity(max_holes + 1);
    let mut prev_key: Option<Key> = None;
    tree_reader
        .visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |key, _value| {
                let curr = Key::from_slice(&key[..KEY_SIZE]);
                if let Some(prev) = prev_key {
                    if curr.to_i128() - prev.to_i128() >= MIN_HOLE_LENGTH {
                        heap.push(Hole(prev..curr));
                        if heap.len() > max_holes {
                            heap.pop(); // remove smallest hole
                        }
                    }
                }
                prev_key = Some(curr.next());
                true
            },
            ctx,
        )
        .await?;
    let mut holes = heap.into_vec();
    holes.sort_by_key(|hole| hole.0.start);
    Ok(holes)
}

pub(crate) async fn main(cmd: &AnalyzeLayerMapCmd) -> Result<()> {
    let storage_path = &cmd.path;
    let max_holes = cmd.max_holes.unwrap_or(DEFAULT_MAX_HOLES);
    let ctx = RequestContext::new(TaskKind::DebugTool, DownloadBehavior::Error);

    // Initialize virtual_file (file descriptor cache) and page cache which are needed to access layer persistent B-Tree.
    pageserver::virtual_file::init(10);
    pageserver::page_cache::init(100);

    let mut total_delta_layers = 0usize;
    let mut total_image_layers = 0usize;
    let mut total_excess_layers = 0usize;
    for tenant in fs::read_dir(storage_path.join(TENANTS_SEGMENT_NAME))? {
        let tenant = tenant?;
        if !tenant.file_type()?.is_dir() {
            continue;
        }
        for timeline in fs::read_dir(tenant.path().join(TIMELINES_SEGMENT_NAME))? {
            let timeline = timeline?;
            if !timeline.file_type()?.is_dir() {
                continue;
            }
            // Collect sorted vec of layers and count deltas
            let mut layers = Vec::new();
            let mut n_deltas = 0usize;

            for layer in fs::read_dir(timeline.path())? {
                let layer = layer?;
                if let Some(mut layer_file) =
                    parse_filename(&layer.file_name().into_string().unwrap())
                {
                    if layer_file.is_delta {
                        let layer_path =
                            Utf8PathBuf::from_path_buf(layer.path()).expect("non-Unicode path");
                        layer_file.holes = get_holes(&layer_path, max_holes, &ctx).await?;
                        n_deltas += 1;
                    }
                    layers.push(layer_file);
                }
            }
            layers.sort_by_key(|layer| layer.lsn_range.end);

            // Count the number of holes and number of excess layers.
            // Excess layer is image layer generated when holes in delta layers are not considered.
            let mut n_excess_layers = 0usize;
            let mut n_holes = 0usize;

            for i in 0..layers.len() {
                if !layers[i].is_delta {
                    let mut n_deltas_since_last_image = 0usize;
                    let mut n_skipped = 0usize;
                    let img_key_range = &layers[i].key_range;
                    for j in (0..i).rev() {
                        if range_overlaps(img_key_range, &layers[j].key_range) {
                            if layers[j].is_delta {
                                n_deltas_since_last_image += 1;
                                if layers[j].skips(img_key_range) {
                                    n_skipped += 1;
                                }
                            } else {
                                // Image layer is always dense, despite to the fact that it doesn't contain all possible
                                // key values in the specified range: there are may be no keys in the storage belonging
                                // to the image layer range but not present in the image layer.
                                break;
                            }
                        }
                    }
                    if n_deltas_since_last_image >= 3 && n_deltas_since_last_image - n_skipped < 3 {
                        // It is just approximation: it doesn't take in account all image coverage.
                        // Moreover the new layer map doesn't count total deltas, but the max stack of overlapping deltas.
                        n_excess_layers += 1;
                    }
                    n_holes += n_skipped;
                }
            }
            println!(
                "Tenant {} timeline {} delta layers {} image layers {} excess layers {} holes {}",
                tenant.file_name().into_string().unwrap(),
                timeline.file_name().into_string().unwrap(),
                n_deltas,
                layers.len() - n_deltas,
                n_excess_layers,
                n_holes
            );
            total_delta_layers += n_deltas;
            total_image_layers += layers.len() - n_deltas;
            total_excess_layers += n_excess_layers;
        }
    }
    println!(
        "Total delta layers {} image layers {} excess layers {}",
        total_delta_layers, total_image_layers, total_excess_layers
    );
    Ok(())
}
