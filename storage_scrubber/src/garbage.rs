//! Functionality for finding and purging garbage, as in "garbage collection".
//!
//! Garbage means S3 objects which are either not referenced by any metadata,
//! or are referenced by a control plane tenant/timeline in a deleted state.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use futures_util::TryStreamExt;
use pageserver_api::shard::TenantShardId;
use remote_storage::{GenericRemoteStorage, ListingMode, ListingObject, RemotePath};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use utils::{backoff, id::TenantId};

use crate::{
    cloud_admin_api::{CloudAdminApiClient, MaybeDeleted, ProjectData},
    init_remote, list_objects_with_retries,
    metadata_stream::{stream_tenant_timelines, stream_tenants_maybe_prefix},
    BucketConfig, ConsoleConfig, NodeKind, TenantShardTimelineId, TraversingDepth, MAX_RETRIES,
};

#[derive(Serialize, Deserialize, Debug)]
enum GarbageReason {
    DeletedInConsole,
    MissingInConsole,

    // The remaining data relates to a known deletion issue, and we're sure that purging this
    // will not delete any real data, for example https://github.com/neondatabase/neon/pull/7928 where
    // there is nothing in a tenant path apart from a heatmap file.
    KnownBug,
}

#[derive(Serialize, Deserialize, Debug)]
enum GarbageEntity {
    Tenant(TenantShardId),
    Timeline(TenantShardTimelineId),
}

#[derive(Serialize, Deserialize, Debug)]
struct GarbageItem {
    entity: GarbageEntity,
    reason: GarbageReason,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GarbageList {
    /// Remember what NodeKind we were finding garbage for, so that we can
    /// purge the list without re-stating it.
    node_kind: NodeKind,

    /// Embed the identity of the bucket, so that we do not risk executing
    /// the wrong list against the wrong bucket, and so that the user does not have
    /// to re-state the bucket details when purging.
    bucket_config: BucketConfig,

    items: Vec<GarbageItem>,

    /// Advisory information to enable consumers to do a validation that if we
    /// see garbage, we saw some active tenants too.  This protects against classes of bugs
    /// in the scrubber that might otherwise generate a "deleted all" result.
    active_tenant_count: usize,
    active_timeline_count: usize,
}

impl GarbageList {
    fn new(node_kind: NodeKind, bucket_config: BucketConfig) -> Self {
        Self {
            items: Vec::new(),
            active_tenant_count: 0,
            active_timeline_count: 0,
            node_kind,
            bucket_config,
        }
    }

    /// If an entity has been identified as requiring purge due to a known bug, e.g.
    /// a particular type of object left behind after an incomplete deletion.
    fn append_buggy(&mut self, entity: GarbageEntity) {
        self.items.push(GarbageItem {
            entity,
            reason: GarbageReason::KnownBug,
        });
    }

    /// Return true if appended, false if not.  False means the result was not garbage.
    fn maybe_append<T>(&mut self, entity: GarbageEntity, result: Option<T>) -> bool
    where
        T: MaybeDeleted,
    {
        match result {
            Some(result_item) if result_item.is_deleted() => {
                self.items.push(GarbageItem {
                    entity,
                    reason: GarbageReason::DeletedInConsole,
                });
                true
            }
            Some(_) => false,
            None => {
                self.items.push(GarbageItem {
                    entity,
                    reason: GarbageReason::MissingInConsole,
                });
                true
            }
        }
    }
}

pub async fn find_garbage(
    bucket_config: BucketConfig,
    console_config: ConsoleConfig,
    depth: TraversingDepth,
    node_kind: NodeKind,
    tenant_id_prefix: Option<String>,
    output_path: String,
) -> anyhow::Result<()> {
    let garbage = find_garbage_inner(
        bucket_config,
        console_config,
        depth,
        node_kind,
        tenant_id_prefix,
    )
    .await?;
    let serialized = serde_json::to_vec_pretty(&garbage)?;

    tokio::fs::write(&output_path, &serialized).await?;

    tracing::info!("Wrote garbage report to {output_path}");

    Ok(())
}

// How many concurrent S3 operations to issue (approximately): this is the concurrency
// for things like listing the timelines within tenant prefixes.
const S3_CONCURRENCY: usize = 32;

// How many concurrent API requests to make to the console API.
//
// Be careful increasing this; roughly we shouldn't have more than ~100 rps. It
// would be better to implement real rsp limiter.
const CONSOLE_CONCURRENCY: usize = 16;

struct ConsoleCache {
    /// Set of tenants found in the control plane API
    projects: HashMap<TenantId, ProjectData>,
    /// Set of tenants for which the control plane API returned 404
    not_found: HashSet<TenantId>,
}

async fn find_garbage_inner(
    bucket_config: BucketConfig,
    console_config: ConsoleConfig,
    depth: TraversingDepth,
    node_kind: NodeKind,
    tenant_id_prefix: Option<String>,
) -> anyhow::Result<GarbageList> {
    // Construct clients for S3 and for Console API
    let (remote_client, target) = init_remote(bucket_config.clone(), node_kind).await?;
    let cloud_admin_api_client = Arc::new(CloudAdminApiClient::new(console_config));

    // Build a set of console-known tenants, for quickly eliminating known-active tenants without having
    // to issue O(N) console API requests.
    let console_projects: HashMap<TenantId, ProjectData> = cloud_admin_api_client
        .list_projects()
        .await?
        .into_iter()
        .map(|t| (t.tenant, t))
        .collect();
    tracing::info!(
        "Loaded {} console projects tenant IDs",
        console_projects.len()
    );

    // Because many tenant shards may look up the same TenantId, we maintain a cache.
    let console_cache = Arc::new(std::sync::Mutex::new(ConsoleCache {
        projects: console_projects,
        not_found: HashSet::new(),
    }));

    // Enumerate Tenants in S3, and check if each one exists in Console
    tracing::info!("Finding all tenants in {}...", bucket_config.desc_str());
    let tenants = stream_tenants_maybe_prefix(&remote_client, &target, tenant_id_prefix);
    let tenants_checked = tenants.map_ok(|t| {
        let api_client = cloud_admin_api_client.clone();
        let console_cache = console_cache.clone();
        async move {
            // Check cache before issuing API call
            let project_data = {
                let cache = console_cache.lock().unwrap();
                let result = cache.projects.get(&t.tenant_id).cloned();
                if result.is_none() && cache.not_found.contains(&t.tenant_id) {
                    return Ok((t, None));
                }
                result
            };

            match project_data {
                Some(project_data) => Ok((t, Some(project_data.clone()))),
                None => {
                    let project_data = api_client
                        .find_tenant_project(t.tenant_id)
                        .await
                        .map_err(|e| anyhow::anyhow!(e));

                    // Populate cache with result of API call
                    {
                        let mut cache = console_cache.lock().unwrap();
                        if let Ok(Some(project_data)) = &project_data {
                            cache.projects.insert(t.tenant_id, project_data.clone());
                        } else if let Ok(None) = &project_data {
                            cache.not_found.insert(t.tenant_id);
                        }
                    }

                    project_data.map(|r| (t, r))
                }
            }
        }
    });
    let mut tenants_checked =
        std::pin::pin!(tenants_checked.try_buffer_unordered(CONSOLE_CONCURRENCY));

    // Process the results of Tenant checks.  If a Tenant is garbage, it goes into
    // the `GarbageList`.  Else it goes into `active_tenants` for more detailed timeline
    // checks if they are enabled by the `depth` parameter.
    let mut garbage = GarbageList::new(node_kind, bucket_config);
    let mut active_tenants: Vec<TenantShardId> = vec![];
    let mut counter = 0;
    while let Some(result) = tenants_checked.next().await {
        let (tenant_shard_id, console_result) = result?;

        // Paranoia check
        if let Some(project) = &console_result {
            assert!(project.tenant == tenant_shard_id.tenant_id);
        }

        // Special case: If it's missing in console, check for known bugs that would enable us to conclusively
        // identify it as purge-able anyway
        if console_result.is_none() {
            let timelines = stream_tenant_timelines(&remote_client, &target, tenant_shard_id)
                .await?
                .collect::<Vec<_>>()
                .await;
            if timelines.is_empty() {
                // No timelines, but a heatmap: the deletion bug where we deleted everything but heatmaps
                let tenant_objects = list_objects_with_retries(
                    &remote_client,
                    ListingMode::WithDelimiter,
                    &target.tenant_root(&tenant_shard_id),
                )
                .await?;
                if let Some(object) = tenant_objects.keys.first() {
                    if object.key.get_path().as_str().ends_with("heatmap-v1.json") {
                        tracing::info!("Tenant {tenant_shard_id}: is missing in console and is only a heatmap (known historic deletion bug)");
                        garbage.append_buggy(GarbageEntity::Tenant(tenant_shard_id));
                        continue;
                    } else {
                        tracing::info!("Tenant {tenant_shard_id} is missing in console and contains one object: {}", object.key);
                    }
                } else {
                    tracing::info!("Tenant {tenant_shard_id} is missing in console appears to have been deleted while we ran");
                }
            } else {
                // A console-unknown tenant with timelines: check if these timelines only contain initdb.tar.zst, from the initial
                // rollout of WAL DR in which we never deleted these.
                let mut any_non_initdb = false;

                for timeline_r in timelines {
                    let timeline = timeline_r?;
                    let timeline_objects = list_objects_with_retries(
                        &remote_client,
                        ListingMode::WithDelimiter,
                        &target.timeline_root(&timeline),
                    )
                    .await?;
                    if !timeline_objects.prefixes.is_empty() {
                        // Sub-paths?  Unexpected
                        any_non_initdb = true;
                    } else {
                        let object = timeline_objects.keys.first().unwrap();
                        if object.key.get_path().as_str().ends_with("initdb.tar.zst") {
                            tracing::info!("Timeline {timeline} contains only initdb.tar.zst");
                        } else {
                            any_non_initdb = true;
                        }
                    }
                }

                if any_non_initdb {
                    tracing::info!("Tenant {tenant_shard_id}: is missing in console and contains timelines, one or more of which are more than just initdb");
                } else {
                    tracing::info!("Tenant {tenant_shard_id}: is missing in console and contains only timelines that only contain initdb");
                    garbage.append_buggy(GarbageEntity::Tenant(tenant_shard_id));
                    continue;
                }
            }
        }

        if garbage.maybe_append(GarbageEntity::Tenant(tenant_shard_id), console_result) {
            tracing::debug!("Tenant {tenant_shard_id} is garbage");
        } else {
            tracing::debug!("Tenant {tenant_shard_id} is active");
            active_tenants.push(tenant_shard_id);
            garbage.active_tenant_count = active_tenants.len();
        }

        counter += 1;
        if counter % 1000 == 0 {
            tracing::info!(
                "Progress: {counter} tenants checked, {} active, {} garbage",
                active_tenants.len(),
                garbage.items.len()
            );
        }
    }

    tracing::info!(
        "Found {}/{} garbage tenants",
        garbage.items.len(),
        garbage.items.len() + active_tenants.len()
    );

    // If we are only checking tenant-deep, we are done.  Otherwise we must
    // proceed to check the individual timelines of the active tenants.
    if depth == TraversingDepth::Tenant {
        return Ok(garbage);
    }

    tracing::info!(
        "Checking timelines for {} active tenants",
        active_tenants.len(),
    );

    // Construct a stream of all timelines within active tenants
    let active_tenants = tokio_stream::iter(active_tenants.iter().map(Ok));
    let timelines = active_tenants.map_ok(|t| stream_tenant_timelines(&remote_client, &target, *t));
    let timelines = timelines.try_buffer_unordered(S3_CONCURRENCY);
    let timelines = timelines.try_flatten();

    // For all timelines within active tenants, call into console API to check their existence
    let timelines_checked = timelines.map_ok(|ttid| {
        let api_client = cloud_admin_api_client.clone();
        async move {
            api_client
                .find_timeline_branch(ttid.tenant_shard_id.tenant_id, ttid.timeline_id)
                .await
                .map_err(|e| anyhow::anyhow!(e))
                .map(|r| (ttid, r))
        }
    });
    let mut timelines_checked =
        std::pin::pin!(timelines_checked.try_buffer_unordered(CONSOLE_CONCURRENCY));

    // Update the GarbageList with any timelines which appear not to exist.
    let mut active_timelines: Vec<TenantShardTimelineId> = vec![];
    while let Some(result) = timelines_checked.next().await {
        let (ttid, console_result) = result?;
        if garbage.maybe_append(GarbageEntity::Timeline(ttid), console_result) {
            tracing::debug!("Timeline {ttid} is garbage");
        } else {
            tracing::debug!("Timeline {ttid} is active");
            active_timelines.push(ttid);
            garbage.active_timeline_count = active_timelines.len();
        }
    }

    let num_garbage_timelines = garbage
        .items
        .iter()
        .filter(|g| matches!(g.entity, GarbageEntity::Timeline(_)))
        .count();
    tracing::info!(
        "Found {}/{} garbage timelines in active tenants",
        num_garbage_timelines,
        active_timelines.len(),
    );

    Ok(garbage)
}

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum PurgeMode {
    /// The safest mode: only delete tenants that were explicitly reported as deleted
    /// by Console API.
    DeletedOnly,

    /// Delete all garbage tenants, including those which are only presumed to be deleted,
    /// because the Console API could not find them.
    DeletedAndMissing,
}

impl std::fmt::Display for PurgeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PurgeMode::DeletedOnly => write!(f, "deleted-only"),
            PurgeMode::DeletedAndMissing => write!(f, "deleted-and-missing"),
        }
    }
}

pub async fn get_tenant_objects(
    s3_client: &GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
) -> anyhow::Result<Vec<ListingObject>> {
    tracing::debug!("Listing objects in tenant {tenant_shard_id}");
    let tenant_root = super::remote_tenant_path(&tenant_shard_id);

    // TODO: apply extra validation based on object modification time.  Don't purge
    // tenants where any timeline's index_part.json has been touched recently.

    let cancel = CancellationToken::new();
    let list = backoff::retry(
        || s3_client.list(Some(&tenant_root), ListingMode::NoDelimiter, None, &cancel),
        |_| false,
        3,
        MAX_RETRIES as u32,
        "get_tenant_objects",
        &cancel,
    )
    .await
    .expect("dummy cancellation token")?;
    Ok(list.keys)
}

pub async fn get_timeline_objects(
    s3_client: &GenericRemoteStorage,
    ttid: TenantShardTimelineId,
) -> anyhow::Result<Vec<ListingObject>> {
    tracing::debug!("Listing objects in timeline {ttid}");
    let timeline_root = super::remote_timeline_path_id(&ttid);

    let cancel = CancellationToken::new();
    let list = backoff::retry(
        || {
            s3_client.list(
                Some(&timeline_root),
                ListingMode::NoDelimiter,
                None,
                &cancel,
            )
        },
        |_| false,
        3,
        MAX_RETRIES as u32,
        "get_timeline_objects",
        &cancel,
    )
    .await
    .expect("dummy cancellation token")?;

    Ok(list.keys)
}

/// Drain a buffer of keys into DeleteObjects requests
///
/// If `drain` is true, drains keys completely; otherwise stops when <
/// `max_keys_per_delete`` keys are left.
/// `num_deleted` returns number of deleted keys.
async fn do_delete(
    remote_client: &GenericRemoteStorage,
    keys: &mut Vec<ListingObject>,
    dry_run: bool,
    drain: bool,
    progress_tracker: &mut DeletionProgressTracker,
) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let max_keys_per_delete = remote_client.max_keys_per_delete();
    while (!keys.is_empty() && drain) || (keys.len() >= max_keys_per_delete) {
        let request_keys =
            keys.split_off(keys.len() - (std::cmp::min(max_keys_per_delete, keys.len())));

        let request_keys: Vec<RemotePath> = request_keys.into_iter().map(|o| o.key).collect();

        let num_deleted = request_keys.len();
        if dry_run {
            tracing::info!("Dry-run deletion of objects: ");
            for k in request_keys {
                tracing::info!("  {k:?}");
            }
        } else {
            remote_client
                .delete_objects(&request_keys, &cancel)
                .await
                .context("deletetion request")?;
            progress_tracker.register(num_deleted);
        }
    }

    Ok(())
}

/// Simple tracker reporting each 10k deleted keys.
#[derive(Default)]
struct DeletionProgressTracker {
    num_deleted: usize,
    last_reported_num_deleted: usize,
}

impl DeletionProgressTracker {
    fn register(&mut self, n: usize) {
        self.num_deleted += n;
        if self.num_deleted - self.last_reported_num_deleted > 10000 {
            tracing::info!("progress: deleted {} keys", self.num_deleted);
            self.last_reported_num_deleted = self.num_deleted;
        }
    }
}

pub async fn purge_garbage(
    input_path: String,
    mode: PurgeMode,
    min_age: Duration,
    dry_run: bool,
) -> anyhow::Result<()> {
    let list_bytes = tokio::fs::read(&input_path).await?;
    let garbage_list = serde_json::from_slice::<GarbageList>(&list_bytes)?;
    tracing::info!(
        "Loaded {} items in garbage list from {}",
        garbage_list.items.len(),
        input_path
    );

    let (remote_client, _target) =
        init_remote(garbage_list.bucket_config.clone(), garbage_list.node_kind).await?;

    assert_eq!(
        garbage_list.bucket_config.bucket_name().unwrap(),
        remote_client.bucket_name().unwrap()
    );

    // Sanity checks on the incoming list
    if garbage_list.active_tenant_count == 0 {
        anyhow::bail!("Refusing to purge a garbage list that reports 0 active tenants");
    }
    if garbage_list
        .items
        .iter()
        .any(|g| matches!(g.entity, GarbageEntity::Timeline(_)))
        && garbage_list.active_timeline_count == 0
    {
        anyhow::bail!("Refusing to purge a garbage list containing garbage timelines that reports 0 active timelines");
    }

    let filtered_items = garbage_list
        .items
        .iter()
        .filter(|i| match (&mode, &i.reason) {
            (PurgeMode::DeletedAndMissing, _) => true,
            (PurgeMode::DeletedOnly, GarbageReason::DeletedInConsole) => true,
            (PurgeMode::DeletedOnly, GarbageReason::KnownBug) => true,
            (PurgeMode::DeletedOnly, GarbageReason::MissingInConsole) => false,
        });

    tracing::info!(
        "Filtered down to {} garbage items based on mode {}",
        garbage_list.items.len(),
        mode
    );

    let items = tokio_stream::iter(filtered_items.map(Ok));
    let get_objects_results = items.map_ok(|i| {
        let remote_client = remote_client.clone();
        async move {
            match i.entity {
                GarbageEntity::Tenant(tenant_id) => {
                    get_tenant_objects(&remote_client, tenant_id).await
                }
                GarbageEntity::Timeline(ttid) => get_timeline_objects(&remote_client, ttid).await,
            }
        }
    });
    let mut get_objects_results =
        std::pin::pin!(get_objects_results.try_buffer_unordered(S3_CONCURRENCY));

    let mut objects_to_delete = Vec::new();
    let mut progress_tracker = DeletionProgressTracker::default();
    while let Some(result) = get_objects_results.next().await {
        let mut object_list = result?;

        // Extra safety check: even if a collection of objects is garbage, check max() of modification
        // times before purging, so that if we incorrectly marked a live tenant as garbage then we would
        // notice that its index has been written recently and would omit deleting it.
        if object_list.is_empty() {
            // Simplify subsequent code by ensuring list always has at least one item
            // Usually, this only occurs if there is parallel deletions racing us, as there is no empty prefixes
            continue;
        }
        let max_mtime = object_list.iter().map(|o| o.last_modified).max().unwrap();
        let age = max_mtime.elapsed();
        match age {
            Err(_) => {
                tracing::warn!("Bad last_modified time");
                continue;
            }
            Ok(a) if a < min_age => {
                // Failed age check.  This doesn't mean we did something wrong: a tenant might really be garbage and recently
                // written, but out of an abundance of caution we still don't purge it.
                tracing::info!(
                    "Skipping tenant with young objects {}..{}",
                    object_list.first().as_ref().unwrap().key,
                    object_list.last().as_ref().unwrap().key
                );
                continue;
            }
            Ok(_) => {
                // Passed age check
            }
        }

        objects_to_delete.append(&mut object_list);
        if objects_to_delete.len() >= remote_client.max_keys_per_delete() {
            do_delete(
                &remote_client,
                &mut objects_to_delete,
                dry_run,
                false,
                &mut progress_tracker,
            )
            .await?;
        }
    }

    do_delete(
        &remote_client,
        &mut objects_to_delete,
        dry_run,
        true,
        &mut progress_tracker,
    )
    .await?;

    tracing::info!("{} keys deleted in total", progress_tracker.num_deleted);

    Ok(())
}
