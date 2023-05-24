use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context};

use crate::context::RequestContext;

use super::Tenant;
use crate::tenant::Timeline;
use utils::id::TimelineId;
use utils::lsn::Lsn;

use tenant_size_model::{Segment, StorageModel};

/// Inputs to the actual tenant sizing model
///
/// Implements [`serde::Serialize`] but is not meant to be part of the public API, instead meant to
/// be a transferrable format between execution environments and developer.
///
/// This tracks more information than the actual StorageModel that calculation
/// needs. We will convert this into a StorageModel when it's time to perform
/// the calculation.
///
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ModelInputs {
    pub segments: Vec<SegmentMeta>,
    pub timeline_inputs: Vec<TimelineInputs>,
}

/// A [`Segment`], with some extra information for display purposes
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SegmentMeta {
    pub segment: Segment,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub timeline_id: TimelineId,
    pub kind: LsnKind,
}

impl SegmentMeta {
    fn size_needed(&self) -> bool {
        match self.kind {
            LsnKind::BranchStart => {
                // If we don't have a later GcCutoff point on this branch, and
                // no ancestor, calculate size for the branch start point.
                self.segment.needed && self.segment.parent.is_none()
            }
            LsnKind::BranchPoint => true,
            LsnKind::GcCutOff => true,
            LsnKind::BranchEnd => false,
        }
    }
}

#[derive(
    Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum LsnKind {
    /// A timeline starting here
    BranchStart,
    /// A child timeline branches off from here
    BranchPoint,
    /// GC cutoff point
    GcCutOff,
    /// Last record LSN
    BranchEnd,
}

/// Collect all relevant LSNs to the inputs. These will only be helpful in the serialized form as
/// part of [`ModelInputs`] from the HTTP api, explaining the inputs.
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TimelineInputs {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub timeline_id: TimelineId,

    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub ancestor_id: Option<TimelineId>,

    #[serde_as(as = "serde_with::DisplayFromStr")]
    ancestor_lsn: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    last_record: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    latest_gc_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    horizon_cutoff: Lsn,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pitr_cutoff: Lsn,

    /// Cutoff point based on GC settings
    #[serde_as(as = "serde_with::DisplayFromStr")]
    next_gc_cutoff: Lsn,

    /// Cutoff point calculated from the user-supplied 'max_retention_period'
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    retention_param_cutoff: Option<Lsn>,
}

/// Gathers the inputs for the tenant sizing model.
///
/// Tenant size does not consider the latest state, but only the state until next_gc_cutoff, which
/// is updated on-demand, during the start of this calculation and separate from the
/// [`Timeline::latest_gc_cutoff`].
///
/// For timelines in general:
///
/// ```ignore
/// 0-----|---------|----|------------| · · · · · |·> lsn
///   initdb_lsn  branchpoints*  next_gc_cutoff  latest
/// ```
///
/// Until gc_horizon_cutoff > `Timeline::last_record_lsn` for any of the tenant's timelines, the
/// tenant size will be zero.
pub(super) async fn gather_inputs(
    tenant: &Tenant,
    max_retention_period: Option<u64>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
    ctx: &RequestContext,
) -> anyhow::Result<ModelInputs> {
    // refresh is needed to update gc related pitr_cutoff and horizon_cutoff
    tenant
        .refresh_gc_info(ctx)
        .await
        .context("Failed to refresh gc_info before gathering inputs")?;

    // Collect information about all the timelines
    let mut timelines = tenant.list_timelines();

    if timelines.is_empty() {
        // perhaps the tenant has just been created, and as such doesn't have any data yet
        return Ok(ModelInputs {
            segments: vec![],
            timeline_inputs: Vec::new(),
        });
    }

    // Filter out timelines that are not active
    //
    // There may be a race when a timeline is dropped,
    // but it is unlikely to cause any issues. In the worst case,
    // the calculation will error out.
    timelines.retain(|t| t.is_active());

    // Build a map of branch points.
    let mut branchpoints: HashMap<TimelineId, HashSet<Lsn>> = HashMap::new();
    for timeline in timelines.iter() {
        if let Some(ancestor_id) = timeline.get_ancestor_timeline_id() {
            branchpoints
                .entry(ancestor_id)
                .or_default()
                .insert(timeline.get_ancestor_lsn());
        }
    }

    // These become the final result.
    let mut timeline_inputs = Vec::with_capacity(timelines.len());
    let mut segments: Vec<SegmentMeta> = Vec::new();

    //
    // Build Segments representing each timeline. As we do that, also remember
    // the branchpoints and branch startpoints in 'branchpoint_segments' and
    // 'branchstart_segments'
    //

    // BranchPoint segments of each timeline
    // (timeline, branchpoint LSN) -> segment_id
    let mut branchpoint_segments: HashMap<(TimelineId, Lsn), usize> = HashMap::new();

    // timeline, Branchpoint seg id, (ancestor, ancestor LSN)
    type BranchStartSegment = (TimelineId, usize, Option<(TimelineId, Lsn)>);
    let mut branchstart_segments: Vec<BranchStartSegment> = Vec::new();

    for timeline in timelines.iter() {
        let timeline_id = timeline.timeline_id;
        let last_record_lsn = timeline.get_last_record_lsn();
        let ancestor_lsn = timeline.get_ancestor_lsn();

        // there's a race between the update (holding tenant.gc_lock) and this read but it
        // might not be an issue, because it's not for Timeline::gc
        let gc_info = timeline.gc_info.read().unwrap();

        // similar to gc, but Timeline::get_latest_gc_cutoff_lsn() will not be updated before a
        // new gc run, which we have no control over. however differently from `Timeline::gc`
        // we don't consider the `Timeline::disk_consistent_lsn` at all, because we are not
        // actually removing files.
        let mut next_gc_cutoff = cmp::min(gc_info.horizon_cutoff, gc_info.pitr_cutoff);

        // If the caller provided a shorter retention period, use that instead of the GC cutoff.
        let retention_param_cutoff = if let Some(max_retention_period) = max_retention_period {
            let param_cutoff = Lsn(last_record_lsn.0.saturating_sub(max_retention_period));
            if next_gc_cutoff < param_cutoff {
                next_gc_cutoff = param_cutoff;
            }
            Some(param_cutoff)
        } else {
            None
        };

        // next_gc_cutoff in parent branch are not of interest (right now at least), nor do we
        // want to query any logical size before initdb_lsn.
        let branch_start_lsn = cmp::max(ancestor_lsn, timeline.initdb_lsn);

        // Build "interesting LSNs" on this timeline
        let mut lsns: Vec<(Lsn, LsnKind)> = gc_info
            .retain_lsns
            .iter()
            .filter(|&&lsn| lsn > ancestor_lsn)
            .copied()
            // this assumes there are no other retain_lsns than the branchpoints
            .map(|lsn| (lsn, LsnKind::BranchPoint))
            .collect::<Vec<_>>();

        // Add branch points we collected earlier, just in case there were any that were
        // not present in retain_lsns. We will remove any duplicates below later.
        if let Some(this_branchpoints) = branchpoints.get(&timeline_id) {
            lsns.extend(
                this_branchpoints
                    .iter()
                    .map(|lsn| (*lsn, LsnKind::BranchPoint)),
            )
        }

        // Add a point for the GC cutoff
        let branch_start_needed = next_gc_cutoff <= branch_start_lsn;
        if !branch_start_needed {
            lsns.push((next_gc_cutoff, LsnKind::GcCutOff));
        }

        lsns.sort_unstable();
        lsns.dedup();

        //
        // Create Segments for the interesting points.
        //

        // Timeline start point
        let ancestor = timeline
            .get_ancestor_timeline_id()
            .map(|ancestor_id| (ancestor_id, ancestor_lsn));
        branchstart_segments.push((timeline_id, segments.len(), ancestor));
        segments.push(SegmentMeta {
            segment: Segment {
                parent: None, // filled in later
                lsn: branch_start_lsn.0,
                size: None, // filled in later
                needed: branch_start_needed,
            },
            timeline_id: timeline.timeline_id,
            kind: LsnKind::BranchStart,
        });

        // GC cutoff point, and any branch points, i.e. points where
        // other timelines branch off from this timeline.
        let mut parent = segments.len() - 1;
        for (lsn, kind) in lsns {
            if kind == LsnKind::BranchPoint {
                branchpoint_segments.insert((timeline_id, lsn), segments.len());
            }
            segments.push(SegmentMeta {
                segment: Segment {
                    parent: Some(parent),
                    lsn: lsn.0,
                    size: None,
                    needed: lsn > next_gc_cutoff,
                },
                timeline_id: timeline.timeline_id,
                kind,
            });
            parent += 1;
        }

        // Current end of the timeline
        segments.push(SegmentMeta {
            segment: Segment {
                parent: Some(parent),
                lsn: last_record_lsn.0,
                size: None, // Filled in later, if necessary
                needed: true,
            },
            timeline_id: timeline.timeline_id,
            kind: LsnKind::BranchEnd,
        });

        timeline_inputs.push(TimelineInputs {
            timeline_id: timeline.timeline_id,
            ancestor_id: timeline.get_ancestor_timeline_id(),
            ancestor_lsn,
            last_record: last_record_lsn,
            // this is not used above, because it might not have updated recently enough
            latest_gc_cutoff: *timeline.get_latest_gc_cutoff_lsn(),
            horizon_cutoff: gc_info.horizon_cutoff,
            pitr_cutoff: gc_info.pitr_cutoff,
            next_gc_cutoff,
            retention_param_cutoff,
        });
    }

    // We now have all segments from the timelines in 'segments'. The timelines
    // haven't been linked to each other yet, though. Do that.
    for (_timeline_id, seg_id, ancestor) in branchstart_segments {
        // Look up the branch point
        if let Some(ancestor) = ancestor {
            let parent_id = *branchpoint_segments.get(&ancestor).unwrap();
            segments[seg_id].segment.parent = Some(parent_id);
        }
    }

    // We left the 'size' field empty in all of the Segments so far.
    // Now find logical sizes for all of the points that might need or benefit from them.
    fill_logical_sizes(&timelines, &mut segments, logical_size_cache, ctx).await?;

    Ok(ModelInputs {
        segments,
        timeline_inputs,
    })
}

/// Augment 'segments' with logical sizes
///
/// this will probably conflict with on-demand downloaded layers, or at least force them all
/// to be downloaded
///
async fn fill_logical_sizes(
    timelines: &[Arc<Timeline>],
    segments: &mut [SegmentMeta],
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    let timeline_hash: HashMap<TimelineId, Arc<Timeline>> = HashMap::from_iter(
        timelines
            .iter()
            .map(|timeline| (timeline.timeline_id, Arc::clone(timeline))),
    );

    // record the used/inserted cache keys here, to remove extras not to start leaking
    // after initial run the cache should be quite stable, but live timelines will eventually
    // require new lsns to be inspected.
    let mut sizes_needed = HashMap::<(TimelineId, Lsn), Option<u64>>::new();

    // with joinset, on drop, all of the tasks will just be de-scheduled, which we can use to
    // our advantage with `?` error handling.

    // For each point that would benefit from having a logical size available,
    // spawn a Task to fetch it, unless we have it cached already.
    for seg in segments.iter() {
        if !seg.size_needed() {
            continue;
        }

        let timeline_id = seg.timeline_id;
        let lsn = Lsn(seg.segment.lsn);

        if let Entry::Vacant(e) = sizes_needed.entry((timeline_id, lsn)) {
            let mut cached_size = logical_size_cache.get(&(timeline_id, lsn)).cloned();
            if cached_size.is_none() {
                let timeline = Arc::clone(timeline_hash.get(&timeline_id).unwrap());
                cached_size = Some(timeline.get_logical_size(lsn, ctx).await?);
            }
            e.insert(cached_size);
        }
    }

    // prune any keys not needed anymore; we record every used key and added key.
    logical_size_cache.retain(|key, _| sizes_needed.contains_key(key));

    // Insert the looked up sizes to the Segments
    for seg in segments.iter_mut() {
        if !seg.size_needed() {
            continue;
        }

        let timeline_id = seg.timeline_id;
        let lsn = Lsn(seg.segment.lsn);

        if let Some(Some(size)) = sizes_needed.get(&(timeline_id, lsn)) {
            seg.segment.size = Some(*size);
        } else {
            bail!("could not find size at {} in timeline {}", lsn, timeline_id);
        }
    }
    Ok(())
}

impl ModelInputs {
    pub fn calculate_model(&self) -> anyhow::Result<tenant_size_model::StorageModel> {
        // Convert SegmentMetas into plain Segments
        let storage = StorageModel {
            segments: self
                .segments
                .iter()
                .map(|seg| seg.segment.clone())
                .collect(),
        };

        Ok(storage)
    }

    // calculate total project size
    pub fn calculate(&self) -> anyhow::Result<u64> {
        let storage = self.calculate_model()?;
        let sizes = storage.calculate();

        Ok(sizes.total_size)
    }
}

#[test]
fn verify_size_for_multiple_branches() {
    // this is generated from integration test test_tenant_size_with_multiple_branches, but this way
    // it has the stable lsn's
    //
    // The timeline_inputs don't participate in the size calculation, and are here just to explain
    // the inputs.
    let doc = r#"
{
  "segments": [
    {
      "segment": {
        "parent": 9,
        "lsn": 26033560,
        "size": null,
        "needed": false
      },
      "timeline_id": "20b129c9b50cff7213e6503a31b2a5ce",
      "kind": "BranchStart"
    },
    {
      "segment": {
        "parent": 0,
        "lsn": 35720400,
        "size": 25206784,
        "needed": false
      },
      "timeline_id": "20b129c9b50cff7213e6503a31b2a5ce",
      "kind": "GcCutOff"
    },
    {
      "segment": {
        "parent": 1,
        "lsn": 35851472,
        "size": null,
        "needed": true
      },
      "timeline_id": "20b129c9b50cff7213e6503a31b2a5ce",
      "kind": "BranchEnd"
    },
    {
      "segment": {
        "parent": 7,
        "lsn": 24566168,
        "size": null,
        "needed": false
      },
      "timeline_id": "454626700469f0a9914949b9d018e876",
      "kind": "BranchStart"
    },
    {
      "segment": {
        "parent": 3,
        "lsn": 25261936,
        "size": 26050560,
        "needed": false
      },
      "timeline_id": "454626700469f0a9914949b9d018e876",
      "kind": "GcCutOff"
    },
    {
      "segment": {
        "parent": 4,
        "lsn": 25393008,
        "size": null,
        "needed": true
      },
      "timeline_id": "454626700469f0a9914949b9d018e876",
      "kind": "BranchEnd"
    },
    {
      "segment": {
        "parent": null,
        "lsn": 23694408,
        "size": null,
        "needed": false
      },
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "kind": "BranchStart"
    },
    {
      "segment": {
        "parent": 6,
        "lsn": 24566168,
        "size": 25739264,
        "needed": false
      },
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "kind": "BranchPoint"
    },
    {
      "segment": {
        "parent": 7,
        "lsn": 25902488,
        "size": 26402816,
        "needed": false
      },
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "kind": "GcCutOff"
    },
    {
      "segment": {
        "parent": 8,
        "lsn": 26033560,
        "size": 26468352,
        "needed": true
      },
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "kind": "BranchPoint"
    },
    {
      "segment": {
        "parent": 9,
        "lsn": 26033560,
        "size": null,
        "needed": true
      },
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "kind": "BranchEnd"
    }
  ],
  "timeline_inputs": [
    {
      "timeline_id": "20b129c9b50cff7213e6503a31b2a5ce",
      "ancestor_lsn": "0/18D3D98",
      "last_record": "0/2230CD0",
      "latest_gc_cutoff": "0/1698C48",
      "horizon_cutoff": "0/2210CD0",
      "pitr_cutoff": "0/2210CD0",
      "next_gc_cutoff": "0/2210CD0",
      "retention_param_cutoff": null
    },
    {
      "timeline_id": "454626700469f0a9914949b9d018e876",
      "ancestor_lsn": "0/176D998",
      "last_record": "0/1837770",
      "latest_gc_cutoff": "0/1698C48",
      "horizon_cutoff": "0/1817770",
      "pitr_cutoff": "0/1817770",
      "next_gc_cutoff": "0/1817770",
      "retention_param_cutoff": null
    },
    {
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "ancestor_lsn": "0/0",
      "last_record": "0/18D3D98",
      "latest_gc_cutoff": "0/1698C48",
      "horizon_cutoff": "0/18B3D98",
      "pitr_cutoff": "0/18B3D98",
      "next_gc_cutoff": "0/18B3D98",
      "retention_param_cutoff": null
    }
  ]
}
"#;
    let inputs: ModelInputs = serde_json::from_str(doc).unwrap();

    assert_eq!(inputs.calculate().unwrap(), 37_851_408);
}

#[test]
fn verify_size_for_one_branch() {
    let doc = r#"
{
  "segments": [
    {
      "segment": {
        "parent": null,
        "lsn": 0,
        "size": null,
        "needed": false
      },
      "timeline_id": "f15ae0cf21cce2ba27e4d80c6709a6cd",
      "kind": "BranchStart"
    },
    {
      "segment": {
        "parent": 0,
        "lsn": 305547335776,
        "size": 220054675456,
        "needed": false
      },
      "timeline_id": "f15ae0cf21cce2ba27e4d80c6709a6cd",
      "kind": "GcCutOff"
    },
    {
      "segment": {
        "parent": 1,
        "lsn": 305614444640,
        "size": null,
        "needed": true
      },
      "timeline_id": "f15ae0cf21cce2ba27e4d80c6709a6cd",
      "kind": "BranchEnd"
    }
  ],
  "timeline_inputs": [
    {
      "timeline_id": "f15ae0cf21cce2ba27e4d80c6709a6cd",
      "ancestor_lsn": "0/0",
      "last_record": "47/280A5860",
      "latest_gc_cutoff": "47/240A5860",
      "horizon_cutoff": "47/240A5860",
      "pitr_cutoff": "47/240A5860",
      "next_gc_cutoff": "47/240A5860",
      "retention_param_cutoff": "0/0"
    }
  ]
}"#;

    let model: ModelInputs = serde_json::from_str(doc).unwrap();

    let res = model.calculate_model().unwrap().calculate();

    println!("calculated synthetic size: {}", res.total_size);
    println!("result: {:?}", serde_json::to_string(&res.segments));

    use utils::lsn::Lsn;
    let latest_gc_cutoff_lsn: Lsn = "47/240A5860".parse().unwrap();
    let last_lsn: Lsn = "47/280A5860".parse().unwrap();
    println!(
        "latest_gc_cutoff lsn 47/240A5860 is {}, last_lsn lsn 47/280A5860 is {}",
        u64::from(latest_gc_cutoff_lsn),
        u64::from(last_lsn)
    );
    assert_eq!(res.total_size, 220121784320);
}
