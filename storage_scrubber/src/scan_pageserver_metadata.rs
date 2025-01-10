use std::collections::{HashMap, HashSet};

use crate::checks::{
    branch_cleanup_and_check_errors, list_timeline_blobs, BlobDataParseResult,
    RemoteTimelineBlobData, TenantObjectListing, TimelineAnalysis,
};
use crate::metadata_stream::{stream_tenant_timelines, stream_tenants};
use crate::{init_remote, BucketConfig, NodeKind, RootTarget, TenantShardTimelineId};
use futures_util::{StreamExt, TryStreamExt};
use pageserver::tenant::remote_timeline_client::remote_layer_path;
use pageserver_api::controller_api::MetadataHealthUpdateRequest;
use pageserver_api::shard::TenantShardId;
use remote_storage::GenericRemoteStorage;
use serde::Serialize;
use tracing::{info_span, Instrument};
use utils::id::TenantId;
use utils::shard::ShardCount;

#[derive(Serialize, Default)]
pub struct MetadataSummary {
    tenant_count: usize,
    timeline_count: usize,
    timeline_shard_count: usize,
    /// Tenant-shard timeline (key) mapping to errors. The key has to be a string because it will be serialized to a JSON.
    /// The key is generated using `TenantShardTimelineId::to_string()`.
    with_errors: HashMap<String, Vec<String>>,
    /// Tenant-shard timeline (key) mapping to warnings. The key has to be a string because it will be serialized to a JSON.
    /// The key is generated using `TenantShardTimelineId::to_string()`.
    with_warnings: HashMap<String, Vec<String>>,
    with_orphans: HashSet<TenantShardTimelineId>,
    indices_by_version: HashMap<usize, usize>,

    #[serde(skip)]
    pub(crate) healthy_tenant_shards: HashSet<TenantShardId>,
    #[serde(skip)]
    pub(crate) unhealthy_tenant_shards: HashSet<TenantShardId>,
}

impl MetadataSummary {
    fn new() -> Self {
        Self::default()
    }

    fn update_data(&mut self, data: &RemoteTimelineBlobData) {
        self.timeline_shard_count += 1;
        if let BlobDataParseResult::Parsed {
            index_part,
            index_part_generation: _,
            s3_layers: _,
            index_part_last_modified_time: _,
            index_part_snapshot_time: _,
        } = &data.blob_data
        {
            *self
                .indices_by_version
                .entry(index_part.version())
                .or_insert(0) += 1;
        }
    }

    fn update_analysis(
        &mut self,
        id: &TenantShardTimelineId,
        analysis: &TimelineAnalysis,
        verbose: bool,
    ) {
        if analysis.is_healthy() {
            self.healthy_tenant_shards.insert(id.tenant_shard_id);
        } else {
            self.healthy_tenant_shards.remove(&id.tenant_shard_id);
            self.unhealthy_tenant_shards.insert(id.tenant_shard_id);
        }

        if !analysis.errors.is_empty() {
            let entry = self.with_errors.entry(id.to_string()).or_default();
            if verbose {
                entry.extend(analysis.errors.iter().cloned());
            }
        }

        if !analysis.warnings.is_empty() {
            let entry = self.with_warnings.entry(id.to_string()).or_default();
            if verbose {
                entry.extend(analysis.warnings.iter().cloned());
            }
        }
    }

    fn notify_timeline_orphan(&mut self, ttid: &TenantShardTimelineId) {
        self.with_orphans.insert(*ttid);
    }

    /// Long-form output for printing at end of a scan
    pub fn summary_string(&self) -> String {
        let version_summary: String = itertools::join(
            self.indices_by_version
                .iter()
                .map(|(k, v)| format!("{k}: {v}")),
            ", ",
        );

        format!(
            "Tenants: {}
Timelines: {}
Timeline-shards: {}
With errors: {}
With warnings: {}
With orphan layers: {}
Index versions: {version_summary}
",
            self.tenant_count,
            self.timeline_count,
            self.timeline_shard_count,
            self.with_errors.len(),
            self.with_warnings.len(),
            self.with_orphans.len(),
        )
    }

    pub fn is_fatal(&self) -> bool {
        !self.with_errors.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.timeline_shard_count == 0
    }

    pub fn build_health_update_request(&self) -> MetadataHealthUpdateRequest {
        MetadataHealthUpdateRequest {
            healthy_tenant_shards: self.healthy_tenant_shards.clone(),
            unhealthy_tenant_shards: self.unhealthy_tenant_shards.clone(),
        }
    }
}

/// Scan the pageserver metadata in an S3 bucket, reporting errors and statistics.
pub async fn scan_pageserver_metadata(
    bucket_config: BucketConfig,
    tenant_ids: Vec<TenantShardId>,
    verbose: bool,
) -> anyhow::Result<MetadataSummary> {
    let (remote_client, target) = init_remote(bucket_config, NodeKind::Pageserver).await?;

    let tenants = if tenant_ids.is_empty() {
        futures::future::Either::Left(stream_tenants(&remote_client, &target))
    } else {
        futures::future::Either::Right(futures::stream::iter(tenant_ids.into_iter().map(Ok)))
    };

    // How many tenants to process in parallel.  We need to be mindful of pageservers
    // accessing the same per tenant prefixes, so use a lower setting than pageservers.
    const CONCURRENCY: usize = 32;

    // Generate a stream of TenantTimelineId
    let timelines = tenants.map_ok(|t| stream_tenant_timelines(&remote_client, &target, t));
    let timelines = timelines.try_buffered(CONCURRENCY);
    let timelines = timelines.try_flatten();

    // Generate a stream of S3TimelineBlobData
    async fn report_on_timeline(
        remote_client: &GenericRemoteStorage,
        target: &RootTarget,
        ttid: TenantShardTimelineId,
    ) -> anyhow::Result<(TenantShardTimelineId, RemoteTimelineBlobData)> {
        let data = list_timeline_blobs(remote_client, ttid, target).await?;
        Ok((ttid, data))
    }
    let timelines = timelines.map_ok(|ttid| report_on_timeline(&remote_client, &target, ttid));
    let mut timelines = std::pin::pin!(timelines.try_buffered(CONCURRENCY));

    // We must gather all the TenantShardTimelineId->S3TimelineBlobData for each tenant, because different
    // shards in the same tenant might refer to one anothers' keys if a shard split has happened.

    let mut tenant_id = None;
    let mut tenant_objects = TenantObjectListing::default();
    let mut tenant_timeline_results = Vec::new();

    async fn analyze_tenant(
        remote_client: &GenericRemoteStorage,
        tenant_id: TenantId,
        summary: &mut MetadataSummary,
        mut tenant_objects: TenantObjectListing,
        timelines: Vec<(TenantShardTimelineId, RemoteTimelineBlobData)>,
        highest_shard_count: ShardCount,
        verbose: bool,
    ) {
        summary.tenant_count += 1;

        let mut timeline_ids = HashSet::new();
        let mut timeline_generations = HashMap::new();
        for (ttid, data) in timelines {
            async {
                if ttid.tenant_shard_id.shard_count == highest_shard_count {
                    // Only analyze `TenantShardId`s with highest shard count.

                    // Stash the generation of each timeline, for later use identifying orphan layers
                    if let BlobDataParseResult::Parsed {
                        index_part,
                        index_part_generation,
                        s3_layers: _,
                        index_part_last_modified_time: _,
                        index_part_snapshot_time: _,
                    } = &data.blob_data
                    {
                        if index_part.deleted_at.is_some() {
                            // skip deleted timeline.
                            tracing::info!(
                                "Skip analysis of {} b/c timeline is already deleted",
                                ttid
                            );
                            return;
                        }
                        timeline_generations.insert(ttid, *index_part_generation);
                    }

                    // Apply checks to this timeline shard's metadata, and in the process update `tenant_objects`
                    // reference counts for layers across the tenant.
                    let analysis = branch_cleanup_and_check_errors(
                        remote_client,
                        &ttid,
                        &mut tenant_objects,
                        None,
                        None,
                        Some(data),
                    )
                    .await;
                    summary.update_analysis(&ttid, &analysis, verbose);

                    timeline_ids.insert(ttid.timeline_id);
                } else {
                    tracing::info!(
                        "Skip analysis of {} b/c a lower shard count than {}",
                        ttid,
                        highest_shard_count.0,
                    );
                }
            }
            .instrument(
                info_span!("analyze-timeline", shard = %ttid.tenant_shard_id.shard_slug(), timeline = %ttid.timeline_id),
            )
            .await
        }

        summary.timeline_count += timeline_ids.len();

        // Identifying orphan layers must be done on a tenant-wide basis, because individual
        // shards' layers may be referenced by other shards.
        //
        // Orphan layers are not a corruption, and not an indication of a problem.  They are just
        // consuming some space in remote storage, and may be cleaned up at leisure.
        for (shard_index, timeline_id, layer_file, generation) in tenant_objects.get_orphans() {
            let ttid = TenantShardTimelineId {
                tenant_shard_id: TenantShardId {
                    tenant_id,
                    shard_count: shard_index.shard_count,
                    shard_number: shard_index.shard_number,
                },
                timeline_id,
            };

            if let Some(timeline_generation) = timeline_generations.get(&ttid) {
                if &generation >= timeline_generation {
                    // Candidate orphan layer is in the current or future generation relative
                    // to the index we read for this timeline shard, so its absence from the index
                    // doesn't make it an orphan: more likely, it is a case where the layer was
                    // uploaded, but the index referencing the layer wasn't written yet.
                    continue;
                }
            }

            let orphan_path = remote_layer_path(
                &tenant_id,
                &timeline_id,
                shard_index,
                &layer_file,
                generation,
            );

            tracing::info!("Orphan layer detected: {orphan_path}");

            summary.notify_timeline_orphan(&ttid);
        }
    }

    // Iterate through  all the timeline results.  These are in key-order, so
    // all results for the same tenant will be adjacent.  We accumulate these,
    // and then call `analyze_tenant` to flush, when we see the next tenant ID.
    let mut summary = MetadataSummary::new();
    let mut highest_shard_count = ShardCount::MIN;
    while let Some(i) = timelines.next().await {
        let (ttid, data) = i?;
        summary.update_data(&data);

        match tenant_id {
            Some(prev_tenant_id) => {
                if prev_tenant_id != ttid.tenant_shard_id.tenant_id {
                    // New tenant: analyze this tenant's timelines, clear accumulated tenant_timeline_results
                    let tenant_objects = std::mem::take(&mut tenant_objects);
                    let timelines = std::mem::take(&mut tenant_timeline_results);
                    analyze_tenant(
                        &remote_client,
                        prev_tenant_id,
                        &mut summary,
                        tenant_objects,
                        timelines,
                        highest_shard_count,
                        verbose,
                    )
                    .instrument(info_span!("analyze-tenant", tenant = %prev_tenant_id))
                    .await;
                    tenant_id = Some(ttid.tenant_shard_id.tenant_id);
                    highest_shard_count = ttid.tenant_shard_id.shard_count;
                } else {
                    highest_shard_count = highest_shard_count.max(ttid.tenant_shard_id.shard_count);
                }
            }
            None => {
                tenant_id = Some(ttid.tenant_shard_id.tenant_id);
                highest_shard_count = highest_shard_count.max(ttid.tenant_shard_id.shard_count);
            }
        }

        match &data.blob_data {
            BlobDataParseResult::Parsed {
                index_part: _,
                index_part_generation: _index_part_generation,
                s3_layers,
                index_part_last_modified_time: _,
                index_part_snapshot_time: _,
            } => {
                tenant_objects.push(ttid, s3_layers.clone());
            }
            BlobDataParseResult::Relic => (),
            BlobDataParseResult::Incorrect {
                errors: _,
                s3_layers,
            } => {
                tenant_objects.push(ttid, s3_layers.clone());
            }
        }
        tenant_timeline_results.push((ttid, data));
    }

    if !tenant_timeline_results.is_empty() {
        let tenant_id = tenant_id.expect("Must be set if results are present");
        analyze_tenant(
            &remote_client,
            tenant_id,
            &mut summary,
            tenant_objects,
            tenant_timeline_results,
            highest_shard_count,
            verbose,
        )
        .instrument(info_span!("analyze-tenant", tenant = %tenant_id))
        .await;
    }

    Ok(summary)
}
