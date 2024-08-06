use std::collections::{HashMap, HashSet};

use anyhow::Context;
use aws_sdk_s3::Client;
use pageserver::tenant::layer_map::LayerMap;
use pageserver::tenant::remote_timeline_client::index::LayerFileMetadata;
use pageserver_api::shard::ShardIndex;
use tracing::{error, info, warn};
use utils::generation::Generation;
use utils::id::TimelineId;

use crate::cloud_admin_api::BranchData;
use crate::metadata_stream::stream_listing;
use crate::{download_object_with_retries, RootTarget, TenantShardTimelineId};
use futures_util::StreamExt;
use pageserver::tenant::remote_timeline_client::{parse_remote_index_path, remote_layer_path};
use pageserver::tenant::storage_layer::LayerName;
use pageserver::tenant::IndexPart;
use remote_storage::RemotePath;

pub(crate) struct TimelineAnalysis {
    /// Anomalies detected
    pub(crate) errors: Vec<String>,

    /// Healthy-but-noteworthy, like old-versioned structures that are readable but
    /// worth reporting for awareness that we must not remove that old version decoding
    /// yet.
    pub(crate) warnings: Vec<String>,

    /// Keys not referenced in metadata: candidates for removal, but NOT NECESSARILY: beware
    /// of races between reading the metadata and reading the objects.
    pub(crate) garbage_keys: Vec<String>,
}

impl TimelineAnalysis {
    fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            garbage_keys: Vec::new(),
        }
    }

    /// Whether a timeline is healthy.
    pub(crate) fn is_healthy(&self) -> bool {
        self.errors.is_empty() && self.warnings.is_empty()
    }
}

pub(crate) async fn branch_cleanup_and_check_errors(
    s3_client: &Client,
    target: &RootTarget,
    id: &TenantShardTimelineId,
    tenant_objects: &mut TenantObjectListing,
    s3_active_branch: Option<&BranchData>,
    console_branch: Option<BranchData>,
    s3_data: Option<S3TimelineBlobData>,
) -> TimelineAnalysis {
    let mut result = TimelineAnalysis::new();

    info!("Checking timeline {id}");

    if let Some(s3_active_branch) = s3_active_branch {
        info!(
            "Checking console status for timeline for branch {:?}/{:?}",
            s3_active_branch.project_id, s3_active_branch.id
        );
        match console_branch {
            Some(_) => {result.errors.push(format!("Timeline has deleted branch data in the console (id = {:?}, project_id = {:?}), recheck whether it got removed during the check",
                s3_active_branch.id, s3_active_branch.project_id))
            },
            None => {
                result.errors.push(format!("Timeline has no branch data in the console (id = {:?}, project_id = {:?}), recheck whether it got removed during the check",
            s3_active_branch.id, s3_active_branch.project_id))
            }
        };
    }

    match s3_data {
        Some(s3_data) => {
            result.garbage_keys.extend(s3_data.unknown_keys);

            match s3_data.blob_data {
                BlobDataParseResult::Parsed {
                    index_part,
                    index_part_generation: _index_part_generation,
                    s3_layers: _s3_layers,
                } => {
                    if !IndexPart::KNOWN_VERSIONS.contains(&index_part.version()) {
                        result
                            .errors
                            .push(format!("index_part.json version: {}", index_part.version()))
                    }

                    let mut newest_versions = IndexPart::KNOWN_VERSIONS.iter().rev().take(3);
                    if !newest_versions.any(|ip| ip == &index_part.version()) {
                        info!(
                            "index_part.json version is not latest: {}",
                            index_part.version()
                        );
                    }

                    if index_part.metadata.disk_consistent_lsn()
                        != index_part.duplicated_disk_consistent_lsn()
                    {
                        // Tech debt: let's get rid of one of these, they are redundant
                        // https://github.com/neondatabase/neon/issues/8343
                        result.errors.push(format!(
                            "Mismatching disk_consistent_lsn in TimelineMetadata ({}) and in the index_part ({})",
                            index_part.metadata.disk_consistent_lsn(),
                            index_part.duplicated_disk_consistent_lsn(),
                        ))
                    }

                    if index_part.layer_metadata.is_empty() {
                        if index_part.metadata.ancestor_timeline().is_none() {
                            // The initial timeline with no ancestor should ALWAYS have layers.
                            result.errors.push(
                                "index_part.json has no layers (ancestor_timeline=None)"
                                    .to_string(),
                            );
                        } else {
                            // Not an error, can happen for branches with zero writes, but notice that
                            info!("index_part.json has no layers (ancestor_timeline exists)");
                        }
                    }

                    for (layer, metadata) in index_part.layer_metadata {
                        if metadata.file_size == 0 {
                            result.errors.push(format!(
                                "index_part.json contains a layer {} that has 0 size in its layer metadata", layer,
                            ))
                        }

                        if !tenant_objects.check_ref(id.timeline_id, &layer, &metadata) {
                            let path = remote_layer_path(
                                &id.tenant_shard_id.tenant_id,
                                &id.timeline_id,
                                metadata.shard,
                                &layer,
                                metadata.generation,
                            );

                            // HEAD request used here to address a race condition  when an index was uploaded concurrently
                            // with our scan. We check if the object is uploaded to S3 after taking the listing snapshot.
                            let response = s3_client
                                .head_object()
                                .bucket(target.bucket_name())
                                .key(path.get_path().as_str())
                                .send()
                                .await;

                            if response.is_err() {
                                // Object is not present.
                                let is_l0 = LayerMap::is_l0(layer.key_range());

                                let msg = format!(
                                    "index_part.json contains a layer {}{} (shard {}) that is not present in remote storage (layer_is_l0: {})",
                                    layer,
                                    metadata.generation.get_suffix(),
                                    metadata.shard,
                                    is_l0,
                                );

                                if is_l0 {
                                    result.warnings.push(msg);
                                } else {
                                    result.errors.push(msg);
                                }
                            }
                        }
                    }
                }
                BlobDataParseResult::Relic => {}
                BlobDataParseResult::Incorrect(parse_errors) => result.errors.extend(
                    parse_errors
                        .into_iter()
                        .map(|error| format!("parse error: {error}")),
                ),
            }
        }
        None => result
            .errors
            .push("Timeline has no data on S3 at all".to_string()),
    }

    if result.errors.is_empty() {
        info!("No check errors found");
    } else {
        warn!("Timeline metadata errors: {0:?}", result.errors);
    }

    if !result.warnings.is_empty() {
        warn!("Timeline metadata warnings: {0:?}", result.warnings);
    }

    if !result.garbage_keys.is_empty() {
        error!(
            "The following keys should be removed from S3: {0:?}",
            result.garbage_keys
        )
    }

    result
}

#[derive(Default)]
pub(crate) struct LayerRef {
    ref_count: usize,
}

/// Top-level index of objects in a tenant.  This may be used by any shard-timeline within
/// the tenant to query whether an object exists.
#[derive(Default)]
pub(crate) struct TenantObjectListing {
    shard_timelines: HashMap<(ShardIndex, TimelineId), HashMap<(LayerName, Generation), LayerRef>>,
}

impl TenantObjectListing {
    /// Having done an S3 listing of the keys within a timeline prefix, merge them into the overall
    /// list of layer keys for the Tenant.
    pub(crate) fn push(
        &mut self,
        ttid: TenantShardTimelineId,
        layers: HashSet<(LayerName, Generation)>,
    ) {
        let shard_index = ShardIndex::new(
            ttid.tenant_shard_id.shard_number,
            ttid.tenant_shard_id.shard_count,
        );
        let replaced = self.shard_timelines.insert(
            (shard_index, ttid.timeline_id),
            layers
                .into_iter()
                .map(|l| (l, LayerRef::default()))
                .collect(),
        );

        assert!(
            replaced.is_none(),
            "Built from an S3 object listing, which should never repeat a key"
        );
    }

    /// Having loaded a timeline index, check if a layer referenced by the index exists.  If it does,
    /// the layer's refcount will be incremented.  Later, after calling this for all references in all indices
    /// in a tenant, orphan layers may be detected by their zero refcounts.
    ///
    /// Returns true if the layer exists
    pub(crate) fn check_ref(
        &mut self,
        timeline_id: TimelineId,
        layer_file: &LayerName,
        metadata: &LayerFileMetadata,
    ) -> bool {
        let Some(shard_tl) = self.shard_timelines.get_mut(&(metadata.shard, timeline_id)) else {
            return false;
        };

        let Some(layer_ref) = shard_tl.get_mut(&(layer_file.clone(), metadata.generation)) else {
            return false;
        };

        layer_ref.ref_count += 1;

        true
    }

    pub(crate) fn get_orphans(&self) -> Vec<(ShardIndex, TimelineId, LayerName, Generation)> {
        let mut result = Vec::new();
        for ((shard_index, timeline_id), layers) in &self.shard_timelines {
            for ((layer_file, generation), layer_ref) in layers {
                if layer_ref.ref_count == 0 {
                    result.push((*shard_index, *timeline_id, layer_file.clone(), *generation))
                }
            }
        }

        result
    }
}

#[derive(Debug)]
pub(crate) struct S3TimelineBlobData {
    pub(crate) blob_data: BlobDataParseResult,

    // Index objects that were not used when loading `blob_data`, e.g. those from old generations
    pub(crate) unused_index_keys: Vec<String>,

    // Objects whose keys were not recognized at all, i.e. not layer files, not indices
    pub(crate) unknown_keys: Vec<String>,
}

#[derive(Debug)]
pub(crate) enum BlobDataParseResult {
    Parsed {
        index_part: Box<IndexPart>,
        index_part_generation: Generation,
        s3_layers: HashSet<(LayerName, Generation)>,
    },
    /// The remains of a deleted Timeline (i.e. an initdb archive only)
    Relic,
    Incorrect(Vec<String>),
}

pub(crate) fn parse_layer_object_name(name: &str) -> Result<(LayerName, Generation), String> {
    match name.rsplit_once('-') {
        // FIXME: this is gross, just use a regex?
        Some((layer_filename, gen)) if gen.len() == 8 => {
            let layer = layer_filename.parse::<LayerName>()?;
            let gen =
                Generation::parse_suffix(gen).ok_or("Malformed generation suffix".to_string())?;
            Ok((layer, gen))
        }
        _ => Ok((name.parse::<LayerName>()?, Generation::none())),
    }
}

pub(crate) async fn list_timeline_blobs(
    s3_client: &Client,
    id: TenantShardTimelineId,
    s3_root: &RootTarget,
) -> anyhow::Result<S3TimelineBlobData> {
    let mut s3_layers = HashSet::new();

    let mut errors = Vec::new();
    let mut unknown_keys = Vec::new();

    let mut timeline_dir_target = s3_root.timeline_root(&id);
    timeline_dir_target.delimiter = String::new();

    let mut index_part_keys: Vec<String> = Vec::new();
    let mut initdb_archive: bool = false;

    let mut stream = std::pin::pin!(stream_listing(s3_client, &timeline_dir_target));
    while let Some(obj) = stream.next().await {
        let obj = obj?;
        let key = obj.key();

        let blob_name = key.strip_prefix(&timeline_dir_target.prefix_in_bucket);
        match blob_name {
            Some(name) if name.starts_with("index_part.json") => {
                tracing::debug!("Index key {key}");
                index_part_keys.push(key.to_owned())
            }
            Some("initdb.tar.zst") => {
                tracing::debug!("initdb archive {key}");
                initdb_archive = true;
            }
            Some("initdb-preserved.tar.zst") => {
                tracing::info!("initdb archive preserved {key}");
            }
            Some(maybe_layer_name) => match parse_layer_object_name(maybe_layer_name) {
                Ok((new_layer, gen)) => {
                    tracing::debug!("Parsed layer key: {} {:?}", new_layer, gen);
                    s3_layers.insert((new_layer, gen));
                }
                Err(e) => {
                    tracing::info!("Error parsing key {maybe_layer_name}");
                    errors.push(
                        format!("S3 list response got an object with key {key} that is not a layer name: {e}"),
                    );
                    unknown_keys.push(key.to_string());
                }
            },
            None => {
                tracing::warn!("Unknown key {}", key);
                errors.push(format!("S3 list response got an object with odd key {key}"));
                unknown_keys.push(key.to_string());
            }
        }
    }

    if index_part_keys.is_empty() && s3_layers.is_empty() && initdb_archive {
        tracing::debug!(
            "Timeline is empty apart from initdb archive: expected post-deletion state."
        );
        return Ok(S3TimelineBlobData {
            blob_data: BlobDataParseResult::Relic,
            unused_index_keys: index_part_keys,
            unknown_keys: Vec::new(),
        });
    }

    // Choose the index_part with the highest generation
    let (index_part_object, index_part_generation) = match index_part_keys
        .iter()
        .filter_map(|key| {
            // Stripping the index key to the last part, because RemotePath doesn't
            // like absolute paths, and depending on prefix_in_bucket it's possible
            // for the keys we read back to start with a slash.
            let basename = key.rsplit_once('/').unwrap().1;
            parse_remote_index_path(RemotePath::from_string(basename).unwrap()).map(|g| (key, g))
        })
        .max_by_key(|i| i.1)
        .map(|(k, g)| (k.clone(), g))
    {
        Some((key, gen)) => (Some(key), gen),
        None => {
            // Legacy/missing case: one or zero index parts, which did not have a generation
            (index_part_keys.pop(), Generation::none())
        }
    };

    match index_part_object.as_ref() {
        Some(selected) => index_part_keys.retain(|k| k != selected),
        None => {
            errors.push("S3 list response got no index_part.json file".to_string());
        }
    }

    if let Some(index_part_object_key) = index_part_object.as_ref() {
        let index_part_bytes = download_object_with_retries(
            s3_client,
            &timeline_dir_target.bucket_name,
            index_part_object_key,
        )
        .await
        .context("index_part.json download")?;

        match serde_json::from_slice(&index_part_bytes) {
            Ok(index_part) => {
                return Ok(S3TimelineBlobData {
                    blob_data: BlobDataParseResult::Parsed {
                        index_part: Box::new(index_part),
                        index_part_generation,
                        s3_layers,
                    },
                    unused_index_keys: index_part_keys,
                    unknown_keys,
                })
            }
            Err(index_parse_error) => errors.push(format!(
                "index_part.json body parsing error: {index_parse_error}"
            )),
        }
    }

    if errors.is_empty() {
        errors.push(
            "Unexpected: no errors did not lead to a successfully parsed blob return".to_string(),
        );
    }

    Ok(S3TimelineBlobData {
        blob_data: BlobDataParseResult::Incorrect(errors),
        unused_index_keys: index_part_keys,
        unknown_keys,
    })
}
