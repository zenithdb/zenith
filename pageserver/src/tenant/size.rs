use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tenant_size_model::svg::SvgBranchKind;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::context::RequestContext;
use crate::pgdatadir_mapping::CalculateLogicalSizeError;

use super::{GcError, LogicalSizeCalculationCause, TenantShard};
use crate::tenant::{MaybeOffloaded, Timeline};
use utils::id::TimelineId;
use utils::lsn::Lsn;

use tracing::*;

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
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ModelInputs {
    pub segments: Vec<SegmentMeta>,
    pub timeline_inputs: Vec<TimelineInputs>,
}

/// A [`Segment`], with some extra information for display purposes
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SegmentMeta {
    pub segment: Segment,
    pub timeline_id: TimelineId,
    pub kind: LsnKind,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum CalculateSyntheticSizeError {
    /// Something went wrong internally to the calculation of logical size at a particular branch point
    #[error("Failed to calculated logical size on timeline {timeline_id} at {lsn}: {error}")]
    LogicalSize {
        timeline_id: TimelineId,
        lsn: Lsn,
        error: CalculateLogicalSizeError,
    },

    /// Something went wrong internally when calculating GC parameters at start of size calculation
    #[error(transparent)]
    GcInfo(GcError),

    /// Totally unexpected errors, like panics joining a task
    #[error(transparent)]
    Fatal(anyhow::Error),

    /// Tenant shut down while calculating size
    #[error("Cancelled")]
    Cancelled,
}

impl From<GcError> for CalculateSyntheticSizeError {
    fn from(value: GcError) -> Self {
        match value {
            GcError::TenantCancelled | GcError::TimelineCancelled => {
                CalculateSyntheticSizeError::Cancelled
            }
            other => CalculateSyntheticSizeError::GcInfo(other),
        }
    }
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
            LsnKind::LeasePoint => true,
            LsnKind::LeaseStart => false,
            LsnKind::LeaseEnd => false,
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
    /// A LSN lease is granted here.
    LeasePoint,
    /// A lease starts from here.
    LeaseStart,
    /// Last record LSN for the lease (should have the same LSN as the previous [`LsnKind::LeaseStart`]).
    LeaseEnd,
}

impl From<LsnKind> for SvgBranchKind {
    fn from(kind: LsnKind) -> Self {
        match kind {
            LsnKind::LeasePoint | LsnKind::LeaseStart | LsnKind::LeaseEnd => SvgBranchKind::Lease,
            _ => SvgBranchKind::Timeline,
        }
    }
}

/// Collect all relevant LSNs to the inputs. These will only be helpful in the serialized form as
/// part of [`ModelInputs`] from the HTTP api, explaining the inputs.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TimelineInputs {
    pub timeline_id: TimelineId,

    pub ancestor_id: Option<TimelineId>,

    ancestor_lsn: Lsn,
    last_record: Lsn,
    latest_gc_cutoff: Lsn,

    /// Cutoff point based on GC settings
    next_pitr_cutoff: Lsn,

    /// Cutoff point calculated from the user-supplied 'max_retention_period'
    retention_param_cutoff: Option<Lsn>,

    /// Lease points on the timeline
    lease_points: Vec<Lsn>,
}

/// Gathers the inputs for the tenant sizing model.
///
/// Tenant size does not consider the latest state, but only the state until next_pitr_cutoff, which
/// is updated on-demand, during the start of this calculation and separate from the
/// [`TimelineInputs::latest_gc_cutoff`].
///
/// For timelines in general:
///
/// ```text
/// 0-----|---------|----|------------| · · · · · |·> lsn
///   initdb_lsn  branchpoints*  next_pitr_cutoff  latest
/// ```
pub(super) async fn gather_inputs(
    tenant: &TenantShard,
    limit: &Arc<Semaphore>,
    max_retention_period: Option<u64>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
    cause: LogicalSizeCalculationCause,
    cancel: &CancellationToken,
    ctx: &RequestContext,
) -> Result<ModelInputs, CalculateSyntheticSizeError> {
    // refresh is needed to update [`timeline::GcCutoffs`]
    tenant.refresh_gc_info(cancel, ctx).await?;

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
    // Also filter out archived timelines.
    timelines.retain(|t| t.is_archived() != Some(true));

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
        //
        // We only consider [`timeline::GcCutoffs::time`], and not [`timeline::GcCutoffs::space`], because from
        // a user's perspective they have only requested retention up to the time bound (pitr_cutoff), rather
        // than our internal space cutoff.  This means that if someone drops a database and waits for their
        // PITR interval, they will see synthetic size decrease, even if we are still storing data inside
        // the space cutoff.
        let mut next_pitr_cutoff = gc_info.cutoffs.time;

        // If the caller provided a shorter retention period, use that instead of the GC cutoff.
        let retention_param_cutoff = if let Some(max_retention_period) = max_retention_period {
            let param_cutoff = Lsn(last_record_lsn.0.saturating_sub(max_retention_period));
            if next_pitr_cutoff < param_cutoff {
                next_pitr_cutoff = param_cutoff;
            }
            Some(param_cutoff)
        } else {
            None
        };

        let lease_points = gc_info
            .leases
            .keys()
            .filter(|&&lsn| lsn > ancestor_lsn)
            .copied()
            .collect::<Vec<_>>();

        // next_pitr_cutoff in parent branch are not of interest (right now at least), nor do we
        // want to query any logical size before initdb_lsn.
        let branch_start_lsn = cmp::max(ancestor_lsn, timeline.initdb_lsn);

        // Build "interesting LSNs" on this timeline
        let mut lsns: Vec<(Lsn, LsnKind)> = gc_info
            .retain_lsns
            .iter()
            .filter(|(lsn, _child_id, is_offloaded)| {
                lsn > &ancestor_lsn && *is_offloaded == MaybeOffloaded::No
            })
            .copied()
            // this assumes there are no other retain_lsns than the branchpoints
            .map(|(lsn, _child_id, _is_offloaded)| (lsn, LsnKind::BranchPoint))
            .collect::<Vec<_>>();

        lsns.extend(lease_points.iter().map(|&lsn| (lsn, LsnKind::LeasePoint)));

        drop(gc_info);

        // Add branch points we collected earlier, just in case there were any that were
        // not present in retain_lsns. We will remove any duplicates below later.
        if let Some(this_branchpoints) = branchpoints.get(&timeline_id) {
            lsns.extend(
                this_branchpoints
                    .iter()
                    .map(|lsn| (*lsn, LsnKind::BranchPoint)),
            )
        }

        // Add a point for the PITR cutoff
        let branch_start_needed = next_pitr_cutoff <= branch_start_lsn;
        if !branch_start_needed {
            lsns.push((next_pitr_cutoff, LsnKind::GcCutOff));
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
                    needed: lsn > next_pitr_cutoff,
                },
                timeline_id: timeline.timeline_id,
                kind,
            });

            parent = segments.len() - 1;

            if kind == LsnKind::LeasePoint {
                // Needs `LeaseStart` and `LeaseEnd` as well to model lease as a read-only branch that never writes data
                // (i.e. it's lsn has not advanced from ancestor_lsn), and therefore the three segments have the same LSN
                // value. Without the other two segments, the calculation code would not count the leased LSN as a point
                // to be retained.
                // Did not use `BranchStart` or `BranchEnd` so we can differentiate branches and leases during debug.
                //
                // Alt Design: rewrite the entire calculation code to be independent of timeline id. Both leases and
                // branch points can be given a synthetic id so we can unite them.
                let mut lease_parent = parent;

                // Start of a lease.
                segments.push(SegmentMeta {
                    segment: Segment {
                        parent: Some(lease_parent),
                        lsn: lsn.0,
                        size: None,                     // Filled in later, if necessary
                        needed: lsn > next_pitr_cutoff, // only needed if the point is within rentention.
                    },
                    timeline_id: timeline.timeline_id,
                    kind: LsnKind::LeaseStart,
                });
                lease_parent += 1;

                // End of the lease.
                segments.push(SegmentMeta {
                    segment: Segment {
                        parent: Some(lease_parent),
                        lsn: lsn.0,
                        size: None,   // Filled in later, if necessary
                        needed: true, // everything at the lease LSN must be readable => is needed
                    },
                    timeline_id: timeline.timeline_id,
                    kind: LsnKind::LeaseEnd,
                });
            }
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
            next_pitr_cutoff,
            retention_param_cutoff,
            lease_points,
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
    fill_logical_sizes(
        &timelines,
        &mut segments,
        limit,
        logical_size_cache,
        cause,
        ctx,
    )
    .await?;

    if tenant.cancel.is_cancelled() {
        // If we're shutting down, return an error rather than a sparse result that might include some
        // timelines from before we started shutting down
        return Err(CalculateSyntheticSizeError::Cancelled);
    }

    Ok(ModelInputs {
        segments,
        timeline_inputs,
    })
}

/// Augment 'segments' with logical sizes
///
/// This will leave segments' sizes as None if the Timeline associated with the segment is deleted concurrently
/// (i.e. we cannot read its logical size at a particular LSN).
async fn fill_logical_sizes(
    timelines: &[Arc<Timeline>],
    segments: &mut [SegmentMeta],
    limit: &Arc<Semaphore>,
    logical_size_cache: &mut HashMap<(TimelineId, Lsn), u64>,
    cause: LogicalSizeCalculationCause,
    ctx: &RequestContext,
) -> Result<(), CalculateSyntheticSizeError> {
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
    let mut joinset = tokio::task::JoinSet::new();

    // For each point that would benefit from having a logical size available,
    // spawn a Task to fetch it, unless we have it cached already.
    for seg in segments.iter() {
        if !seg.size_needed() {
            continue;
        }

        let timeline_id = seg.timeline_id;
        let lsn = Lsn(seg.segment.lsn);

        if let Entry::Vacant(e) = sizes_needed.entry((timeline_id, lsn)) {
            let cached_size = logical_size_cache.get(&(timeline_id, lsn)).cloned();
            if cached_size.is_none() {
                let timeline = Arc::clone(timeline_hash.get(&timeline_id).unwrap());
                let parallel_size_calcs = Arc::clone(limit);
                let ctx = ctx.attached_child();
                joinset.spawn(
                    calculate_logical_size(parallel_size_calcs, timeline, lsn, cause, ctx)
                        .in_current_span(),
                );
            }
            e.insert(cached_size);
        }
    }

    // Perform the size lookups
    let mut have_any_error = None;
    while let Some(res) = joinset.join_next().await {
        // each of these come with Result<anyhow::Result<_>, JoinError>
        // because of spawn + spawn_blocking
        match res {
            Err(join_error) if join_error.is_cancelled() => {
                unreachable!("we are not cancelling any of the futures, nor should be");
            }
            Err(join_error) => {
                // cannot really do anything, as this panic is likely a bug
                error!("task that calls spawn_ondemand_logical_size_calculation panicked: {join_error:#}");

                have_any_error = Some(CalculateSyntheticSizeError::Fatal(
                    anyhow::anyhow!(join_error)
                        .context("task that calls spawn_ondemand_logical_size_calculation"),
                ));
            }
            Ok(Err(recv_result_error)) => {
                // cannot really do anything, as this panic is likely a bug
                error!("failed to receive logical size query result: {recv_result_error:#}");
                have_any_error = Some(CalculateSyntheticSizeError::Fatal(
                    anyhow::anyhow!(recv_result_error)
                        .context("Receiving logical size query result"),
                ));
            }
            Ok(Ok(TimelineAtLsnSizeResult(timeline, lsn, Err(error)))) => {
                if matches!(error, CalculateLogicalSizeError::Cancelled) {
                    // Skip this: it's okay if one timeline among many is shutting down while we
                    // calculate inputs for the overall tenant.
                    continue;
                } else {
                    warn!(
                        timeline_id=%timeline.timeline_id,
                        "failed to calculate logical size at {lsn}: {error:#}"
                    );
                    have_any_error = Some(CalculateSyntheticSizeError::LogicalSize {
                        timeline_id: timeline.timeline_id,
                        lsn,
                        error,
                    });
                }
            }
            Ok(Ok(TimelineAtLsnSizeResult(timeline, lsn, Ok(size)))) => {
                debug!(timeline_id=%timeline.timeline_id, %lsn, size, "size calculated");

                logical_size_cache.insert((timeline.timeline_id, lsn), size);
                sizes_needed.insert((timeline.timeline_id, lsn), Some(size));
            }
        }
    }

    // prune any keys not needed anymore; we record every used key and added key.
    logical_size_cache.retain(|key, _| sizes_needed.contains_key(key));

    if let Some(error) = have_any_error {
        // we cannot complete this round, because we are missing data.
        // we have however cached all we were able to request calculation on.
        return Err(error);
    }

    // Insert the looked up sizes to the Segments
    for seg in segments.iter_mut() {
        if !seg.size_needed() {
            continue;
        }

        let timeline_id = seg.timeline_id;
        let lsn = Lsn(seg.segment.lsn);

        if let Some(Some(size)) = sizes_needed.get(&(timeline_id, lsn)) {
            seg.segment.size = Some(*size);
        }
    }
    Ok(())
}

impl ModelInputs {
    pub fn calculate_model(&self) -> tenant_size_model::StorageModel {
        // Convert SegmentMetas into plain Segments
        StorageModel {
            segments: self
                .segments
                .iter()
                .map(|seg| seg.segment.clone())
                .collect(),
        }
    }

    // calculate total project size
    pub fn calculate(&self) -> u64 {
        let storage = self.calculate_model();
        let sizes = storage.calculate();
        sizes.total_size
    }
}

/// Newtype around the tuple that carries the timeline at lsn logical size calculation.
struct TimelineAtLsnSizeResult(
    Arc<crate::tenant::Timeline>,
    utils::lsn::Lsn,
    Result<u64, CalculateLogicalSizeError>,
);

#[instrument(skip_all, fields(timeline_id=%timeline.timeline_id, lsn=%lsn))]
async fn calculate_logical_size(
    limit: Arc<tokio::sync::Semaphore>,
    timeline: Arc<crate::tenant::Timeline>,
    lsn: utils::lsn::Lsn,
    cause: LogicalSizeCalculationCause,
    ctx: RequestContext,
) -> Result<TimelineAtLsnSizeResult, RecvError> {
    let _permit = tokio::sync::Semaphore::acquire_owned(limit)
        .await
        .expect("global semaphore should not had been closed");

    let size_res = timeline
        .spawn_ondemand_logical_size_calculation(lsn, cause, ctx)
        .instrument(info_span!("spawn_ondemand_logical_size_calculation"))
        .await?;
    Ok(TimelineAtLsnSizeResult(timeline, lsn, size_res))
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
      "next_pitr_cutoff": "0/2210CD0",
      "retention_param_cutoff": null,
      "lease_points": []
    },
    {
      "timeline_id": "454626700469f0a9914949b9d018e876",
      "ancestor_lsn": "0/176D998",
      "last_record": "0/1837770",
      "latest_gc_cutoff": "0/1698C48",
      "next_pitr_cutoff": "0/1817770",
      "retention_param_cutoff": null,
      "lease_points": []
    },
    {
      "timeline_id": "cb5e3cbe60a4afc00d01880e1a37047f",
      "ancestor_lsn": "0/0",
      "last_record": "0/18D3D98",
      "latest_gc_cutoff": "0/1698C48",
      "next_pitr_cutoff": "0/18B3D98",
      "retention_param_cutoff": null,
      "lease_points": []
    }
  ]
}
"#;
    let inputs: ModelInputs = serde_json::from_str(doc).unwrap();

    assert_eq!(inputs.calculate(), 37_851_408);
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
      "next_pitr_cutoff": "47/240A5860",
      "retention_param_cutoff": "0/0",
      "lease_points": []
    }
  ]
}"#;

    let model: ModelInputs = serde_json::from_str(doc).unwrap();

    let res = model.calculate_model().calculate();

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
