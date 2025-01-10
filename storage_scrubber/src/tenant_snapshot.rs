use std::collections::HashMap;
use std::sync::Arc;

use crate::checks::{list_timeline_blobs, BlobDataParseResult, RemoteTimelineBlobData};
use crate::metadata_stream::{stream_tenant_shards, stream_tenant_timelines};
use crate::{
    download_object_to_file_s3, init_remote, init_remote_s3, BucketConfig, NodeKind, RootTarget,
    TenantShardTimelineId,
};
use anyhow::Context;
use async_stream::stream;
use aws_sdk_s3::Client;
use camino::Utf8PathBuf;
use futures::{StreamExt, TryStreamExt};
use pageserver::tenant::remote_timeline_client::index::LayerFileMetadata;
use pageserver::tenant::storage_layer::LayerName;
use pageserver::tenant::IndexPart;
use pageserver_api::shard::TenantShardId;
use remote_storage::{GenericRemoteStorage, S3Config};
use utils::generation::Generation;
use utils::id::TenantId;

pub struct SnapshotDownloader {
    s3_client: Arc<Client>,
    s3_root: RootTarget,
    bucket_config: BucketConfig,
    bucket_config_s3: S3Config,
    tenant_id: TenantId,
    output_path: Utf8PathBuf,
    concurrency: usize,
}

impl SnapshotDownloader {
    pub async fn new(
        bucket_config: BucketConfig,
        tenant_id: TenantId,
        output_path: Utf8PathBuf,
        concurrency: usize,
    ) -> anyhow::Result<Self> {
        let bucket_config_s3 = match &bucket_config.0.storage {
            remote_storage::RemoteStorageKind::AwsS3(config) => config.clone(),
            _ => panic!("only S3 configuration is supported for snapshot downloading"),
        };
        let (s3_client, s3_root) =
            init_remote_s3(bucket_config_s3.clone(), NodeKind::Pageserver).await?;
        Ok(Self {
            s3_client,
            s3_root,
            bucket_config,
            bucket_config_s3,
            tenant_id,
            output_path,
            concurrency,
        })
    }

    async fn download_layer(
        &self,
        ttid: TenantShardTimelineId,
        layer_name: LayerName,
        layer_metadata: LayerFileMetadata,
    ) -> anyhow::Result<(LayerName, LayerFileMetadata)> {
        // Note this is local as in a local copy of S3 data, not local as in the pageserver's local format.  They use
        // different layer names (remote-style has the generation suffix)
        let local_path = self.output_path.join(format!(
            "{}/timelines/{}/{}{}",
            ttid.tenant_shard_id,
            ttid.timeline_id,
            layer_name,
            layer_metadata.generation.get_suffix()
        ));

        // We should only be called for layers that are owned by the input TTID
        assert_eq!(layer_metadata.shard, ttid.tenant_shard_id.to_index());

        // Assumption: we always write layer files atomically, and layer files are immutable.  Therefore if the file
        // already exists on local disk, we assume it is fully correct and skip it.
        if tokio::fs::try_exists(&local_path).await? {
            tracing::debug!("{} already exists", local_path);
            return Ok((layer_name, layer_metadata));
        } else {
            tracing::debug!("{} requires download...", local_path);

            let timeline_root = self.s3_root.timeline_root(&ttid);
            let remote_layer_path = format!(
                "{}{}{}",
                timeline_root.prefix_in_bucket,
                layer_name,
                layer_metadata.generation.get_suffix()
            );

            // List versions: the object might be deleted.
            let versions = self
                .s3_client
                .list_object_versions()
                .bucket(self.bucket_config_s3.bucket_name.clone())
                .prefix(&remote_layer_path)
                .send()
                .await?;
            let Some(version) = versions.versions.as_ref().and_then(|v| v.first()) else {
                return Err(anyhow::anyhow!("No versions found for {remote_layer_path}"));
            };
            download_object_to_file_s3(
                &self.s3_client,
                &self.bucket_config_s3.bucket_name,
                &remote_layer_path,
                version.version_id.as_deref(),
                &local_path,
            )
            .await?;

            tracing::debug!("Downloaded successfully to {local_path}");
        }

        Ok((layer_name, layer_metadata))
    }

    /// Download many layers belonging to the same TTID, with some concurrency
    async fn download_layers(
        &self,
        ttid: TenantShardTimelineId,
        layers: Vec<(LayerName, LayerFileMetadata)>,
    ) -> anyhow::Result<()> {
        let layer_count = layers.len();
        tracing::info!("Downloading {} layers for timeline {ttid}...", layer_count);
        let layers_stream = stream! {
            for (layer_name, layer_metadata) in layers {
                yield self.download_layer(ttid, layer_name, layer_metadata);
            }
        };

        tokio::fs::create_dir_all(self.output_path.join(format!(
            "{}/timelines/{}",
            ttid.tenant_shard_id, ttid.timeline_id
        )))
        .await?;

        let layer_results = layers_stream.buffered(self.concurrency);
        let mut layer_results = std::pin::pin!(layer_results);

        let mut err = None;
        let mut download_count = 0;
        while let Some(i) = layer_results.next().await {
            download_count += 1;
            match i {
                Ok((layer_name, layer_metadata)) => {
                    tracing::info!(
                        "[{download_count}/{layer_count}] OK: {} bytes {ttid} {}",
                        layer_metadata.file_size,
                        layer_name
                    );
                }
                Err(e) => {
                    // Warn and continue: we will download what we can
                    tracing::warn!("Download error: {e}");
                    err = Some(e);
                }
            }
        }
        if let Some(e) = err {
            tracing::warn!("Some errors occurred downloading {ttid} layers, last error: {e}");
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn download_timeline(
        &self,
        ttid: TenantShardTimelineId,
        index_part: Box<IndexPart>,
        index_part_generation: Generation,
        ancestor_layers: &mut HashMap<TenantShardTimelineId, HashMap<LayerName, LayerFileMetadata>>,
    ) -> anyhow::Result<()> {
        let index_bytes = serde_json::to_string(&index_part).unwrap();

        let layers = index_part
            .layer_metadata
            .into_iter()
            .filter_map(|(layer_name, layer_metadata)| {
                if layer_metadata.shard.shard_count != ttid.tenant_shard_id.shard_count {
                    // Accumulate ancestor layers for later download
                    let ancestor_ttid = TenantShardTimelineId::new(
                        TenantShardId {
                            tenant_id: ttid.tenant_shard_id.tenant_id,
                            shard_number: layer_metadata.shard.shard_number,
                            shard_count: layer_metadata.shard.shard_count,
                        },
                        ttid.timeline_id,
                    );
                    let ancestor_ttid_layers = ancestor_layers.entry(ancestor_ttid).or_default();
                    use std::collections::hash_map::Entry;
                    match ancestor_ttid_layers.entry(layer_name) {
                        Entry::Occupied(entry) => {
                            // Descendent shards that reference a layer from an ancestor should always have matching metadata,
                            // as their siblings, because it is read atomically during a shard split.
                            assert_eq!(entry.get(), &layer_metadata);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(layer_metadata);
                        }
                    }
                    None
                } else {
                    Some((layer_name, layer_metadata))
                }
            })
            .collect();

        let download_result = self.download_layers(ttid, layers).await;

        // Write index last, once all the layers it references are downloaded
        let local_index_path = self.output_path.join(format!(
            "{}/timelines/{}/index_part.json{}",
            ttid.tenant_shard_id,
            ttid.timeline_id,
            index_part_generation.get_suffix()
        ));
        tokio::fs::write(&local_index_path, index_bytes)
            .await
            .context("writing index")?;

        download_result
    }

    pub async fn download(&self) -> anyhow::Result<()> {
        let (remote_client, target) =
            init_remote(self.bucket_config.clone(), NodeKind::Pageserver).await?;

        // Generate a stream of TenantShardId
        let shards = stream_tenant_shards(&remote_client, &target, self.tenant_id).await?;
        let shards: Vec<TenantShardId> = shards.try_collect().await?;

        // Only read from shards that have the highest count: avoids redundantly downloading
        // from ancestor shards.
        let Some(shard_count) = shards.iter().map(|s| s.shard_count).max() else {
            anyhow::bail!("No shards found");
        };

        // We will build a collection of layers in anccestor shards to download (this will only
        // happen if this tenant has been split at some point)
        let mut ancestor_layers: HashMap<
            TenantShardTimelineId,
            HashMap<LayerName, LayerFileMetadata>,
        > = Default::default();

        for shard in shards.into_iter().filter(|s| s.shard_count == shard_count) {
            // Generate a stream of TenantTimelineId
            let timelines = stream_tenant_timelines(&remote_client, &target, shard).await?;

            // Generate a stream of S3TimelineBlobData
            async fn load_timeline_index(
                remote_client: &GenericRemoteStorage,
                target: &RootTarget,
                ttid: TenantShardTimelineId,
            ) -> anyhow::Result<(TenantShardTimelineId, RemoteTimelineBlobData)> {
                let data = list_timeline_blobs(remote_client, ttid, target).await?;
                Ok((ttid, data))
            }
            let timelines =
                timelines.map_ok(|ttid| load_timeline_index(&remote_client, &target, ttid));
            let mut timelines = std::pin::pin!(timelines.try_buffered(8));

            while let Some(i) = timelines.next().await {
                let (ttid, data) = i?;
                match data.blob_data {
                    BlobDataParseResult::Parsed {
                        index_part,
                        index_part_generation,
                        s3_layers: _,
                        index_part_last_modified_time: _,
                        index_part_snapshot_time: _,
                    } => {
                        self.download_timeline(
                            ttid,
                            index_part,
                            index_part_generation,
                            &mut ancestor_layers,
                        )
                        .await
                        .context("Downloading timeline")?;
                    }
                    BlobDataParseResult::Relic => {}
                    BlobDataParseResult::Incorrect { .. } => {
                        tracing::error!("Bad metadata in timeline {ttid}");
                    }
                };
            }
        }

        for (ttid, layers) in ancestor_layers.into_iter() {
            tracing::info!(
                "Downloading {} layers from ancestor timeline {ttid}...",
                layers.len()
            );

            self.download_layers(ttid, layers.into_iter().collect())
                .await?;
        }

        Ok(())
    }
}
