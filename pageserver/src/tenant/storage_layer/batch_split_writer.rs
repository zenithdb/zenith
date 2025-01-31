use std::{future::Future, ops::Range, sync::Arc};

use bytes::Bytes;
use pageserver_api::key::{Key, KEY_SIZE};
use utils::{id::TimelineId, lsn::Lsn, shard::TenantShardId};

use crate::tenant::storage_layer::Layer;
use crate::{config::PageServerConf, context::RequestContext, tenant::Timeline};
use pageserver_api::value::Value;

use super::layer::S3_UPLOAD_LIMIT;
use super::{
    DeltaLayerWriter, ImageLayerWriter, PersistentLayerDesc, PersistentLayerKey, ResidentLayer,
};

pub(crate) enum BatchWriterResult {
    Produced(ResidentLayer),
    Discarded(PersistentLayerKey),
}

#[cfg(test)]
impl BatchWriterResult {
    fn into_resident_layer(self) -> ResidentLayer {
        match self {
            BatchWriterResult::Produced(layer) => layer,
            BatchWriterResult::Discarded(_) => panic!("unexpected discarded layer"),
        }
    }

    fn into_discarded_layer(self) -> PersistentLayerKey {
        match self {
            BatchWriterResult::Produced(_) => panic!("unexpected produced layer"),
            BatchWriterResult::Discarded(layer) => layer,
        }
    }
}

enum LayerWriterWrapper {
    Image(ImageLayerWriter),
    Delta(DeltaLayerWriter),
}

/// An layer writer that takes unfinished layers and finish them atomically.
#[must_use]
pub struct BatchLayerWriter {
    generated_layer_writers: Vec<(LayerWriterWrapper, PersistentLayerKey)>,
    conf: &'static PageServerConf,
}

impl BatchLayerWriter {
    pub async fn new(conf: &'static PageServerConf) -> anyhow::Result<Self> {
        Ok(Self {
            generated_layer_writers: Vec::new(),
            conf,
        })
    }

    pub fn add_unfinished_image_writer(
        &mut self,
        writer: ImageLayerWriter,
        key_range: Range<Key>,
        lsn: Lsn,
    ) {
        self.generated_layer_writers.push((
            LayerWriterWrapper::Image(writer),
            PersistentLayerKey {
                key_range,
                lsn_range: PersistentLayerDesc::image_layer_lsn_range(lsn),
                is_delta: false,
            },
        ));
    }

    pub fn add_unfinished_delta_writer(
        &mut self,
        writer: DeltaLayerWriter,
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
    ) {
        self.generated_layer_writers.push((
            LayerWriterWrapper::Delta(writer),
            PersistentLayerKey {
                key_range,
                lsn_range,
                is_delta: true,
            },
        ));
    }

    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<ResidentLayer>> {
        let res = self
            .finish_with_discard_fn(tline, ctx, |_| async { false })
            .await?;
        let mut output = Vec::new();
        for r in res {
            if let BatchWriterResult::Produced(layer) = r {
                output.push(layer);
            }
        }
        Ok(output)
    }

    pub(crate) async fn finish_with_discard_fn<D, F>(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        discard_fn: D,
    ) -> anyhow::Result<Vec<BatchWriterResult>>
    where
        D: Fn(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        let Self {
            generated_layer_writers,
            ..
        } = self;
        let clean_up_layers = |generated_layers: Vec<BatchWriterResult>| {
            for produced_layer in generated_layers {
                if let BatchWriterResult::Produced(resident_layer) = produced_layer {
                    let layer: Layer = resident_layer.into();
                    layer.delete_on_drop();
                }
            }
        };
        // BEGIN: catch every error and do the recovery in the below section
        let mut generated_layers: Vec<BatchWriterResult> = Vec::new();
        for (inner, layer_key) in generated_layer_writers {
            if discard_fn(&layer_key).await {
                generated_layers.push(BatchWriterResult::Discarded(layer_key));
            } else {
                let res = match inner {
                    LayerWriterWrapper::Delta(writer) => {
                        writer.finish(layer_key.key_range.end, ctx).await
                    }
                    LayerWriterWrapper::Image(writer) => {
                        writer
                            .finish_with_end_key(layer_key.key_range.end, ctx)
                            .await
                    }
                };
                let layer = match res {
                    Ok((desc, path)) => {
                        match Layer::finish_creating(self.conf, tline, desc, &path) {
                            Ok(layer) => layer,
                            Err(e) => {
                                tokio::fs::remove_file(&path).await.ok();
                                clean_up_layers(generated_layers);
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => {
                        // Image/DeltaLayerWriter::finish will clean up the temporary layer if anything goes wrong,
                        // so we don't need to remove the layer we just failed to create by ourselves.
                        clean_up_layers(generated_layers);
                        return Err(e);
                    }
                };
                generated_layers.push(BatchWriterResult::Produced(layer));
            }
        }
        // END: catch every error and do the recovery in the above section
        Ok(generated_layers)
    }
}

/// An image writer that takes images and produces multiple image layers.
#[must_use]
pub struct SplitImageLayerWriter {
    inner: ImageLayerWriter,
    target_layer_size: u64,
    lsn: Lsn,
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    batches: BatchLayerWriter,
    start_key: Key,
}

impl SplitImageLayerWriter {
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        start_key: Key,
        lsn: Lsn,
        target_layer_size: u64,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            target_layer_size,
            inner: ImageLayerWriter::new(
                conf,
                timeline_id,
                tenant_shard_id,
                &(start_key..Key::MAX),
                lsn,
                ctx,
            )
            .await?,
            conf,
            timeline_id,
            tenant_shard_id,
            batches: BatchLayerWriter::new(conf).await?,
            lsn,
            start_key,
        })
    }

    pub async fn put_image(
        &mut self,
        key: Key,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // The current estimation is an upper bound of the space that the key/image could take
        // because we did not consider compression in this estimation. The resulting image layer
        // could be smaller than the target size.
        let addition_size_estimation = KEY_SIZE as u64 + img.len() as u64;
        if self.inner.num_keys() >= 1
            && self.inner.estimated_size() + addition_size_estimation >= self.target_layer_size
        {
            let next_image_writer = ImageLayerWriter::new(
                self.conf,
                self.timeline_id,
                self.tenant_shard_id,
                &(key..Key::MAX),
                self.lsn,
                ctx,
            )
            .await?;
            let prev_image_writer = std::mem::replace(&mut self.inner, next_image_writer);
            self.batches.add_unfinished_image_writer(
                prev_image_writer,
                self.start_key..key,
                self.lsn,
            );
            self.start_key = key;
        }
        self.inner.put_image(key, img, ctx).await
    }

    pub(crate) async fn finish_with_discard_fn<D, F>(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        end_key: Key,
        discard_fn: D,
    ) -> anyhow::Result<Vec<BatchWriterResult>>
    where
        D: Fn(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        let Self {
            mut batches, inner, ..
        } = self;
        if inner.num_keys() != 0 {
            batches.add_unfinished_image_writer(inner, self.start_key..end_key, self.lsn);
        }
        batches.finish_with_discard_fn(tline, ctx, discard_fn).await
    }

    #[cfg(test)]
    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        end_key: Key,
    ) -> anyhow::Result<Vec<BatchWriterResult>> {
        self.finish_with_discard_fn(tline, ctx, end_key, |_| async { false })
            .await
    }
}

/// A delta writer that takes key-lsn-values and produces multiple delta layers.
///
/// Note that if updates of a single key exceed the target size limit, all of the updates will be batched
/// into a single file. This behavior might change in the future. For reference, the legacy compaction algorithm
/// will split them into multiple files based on size.
#[must_use]
pub struct SplitDeltaLayerWriter {
    inner: Option<(Key, DeltaLayerWriter)>,
    target_layer_size: u64,
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    lsn_range: Range<Lsn>,
    last_key_written: Key,
    batches: BatchLayerWriter,
}

impl SplitDeltaLayerWriter {
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        lsn_range: Range<Lsn>,
        target_layer_size: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            target_layer_size,
            inner: None,
            conf,
            timeline_id,
            tenant_shard_id,
            lsn_range,
            last_key_written: Key::MIN,
            batches: BatchLayerWriter::new(conf).await?,
        })
    }

    pub async fn put_value(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // The current estimation is key size plus LSN size plus value size estimation. This is not an accurate
        // number, and therefore the final layer size could be a little bit larger or smaller than the target.
        //
        // Also, keep all updates of a single key in a single file. TODO: split them using the legacy compaction
        // strategy. https://github.com/neondatabase/neon/issues/8837

        if self.inner.is_none() {
            self.inner = Some((
                key,
                DeltaLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_shard_id,
                    key,
                    self.lsn_range.clone(),
                    ctx,
                )
                .await?,
            ));
        }
        let (_, inner) = self.inner.as_mut().unwrap();

        let addition_size_estimation = KEY_SIZE as u64 + 8 /* LSN u64 size */ + 80 /* value size estimation */;
        if inner.num_keys() >= 1
            && inner.estimated_size() + addition_size_estimation >= self.target_layer_size
        {
            if key != self.last_key_written {
                let next_delta_writer = DeltaLayerWriter::new(
                    self.conf,
                    self.timeline_id,
                    self.tenant_shard_id,
                    key,
                    self.lsn_range.clone(),
                    ctx,
                )
                .await?;
                let (start_key, prev_delta_writer) =
                    std::mem::replace(&mut self.inner, Some((key, next_delta_writer))).unwrap();
                self.batches.add_unfinished_delta_writer(
                    prev_delta_writer,
                    start_key..key,
                    self.lsn_range.clone(),
                );
            } else if inner.estimated_size() >= S3_UPLOAD_LIMIT {
                // We have to produce a very large file b/c a key is updated too often.
                anyhow::bail!(
                    "a single key is updated too often: key={}, estimated_size={}, and the layer file cannot be produced",
                    key,
                    inner.estimated_size()
                );
            }
        }
        self.last_key_written = key;
        let (_, inner) = self.inner.as_mut().unwrap();
        inner.put_value(key, lsn, val, ctx).await
    }

    pub(crate) async fn finish_with_discard_fn<D, F>(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
        discard_fn: D,
    ) -> anyhow::Result<Vec<BatchWriterResult>>
    where
        D: Fn(&PersistentLayerKey) -> F,
        F: Future<Output = bool>,
    {
        let Self {
            mut batches, inner, ..
        } = self;
        if let Some((start_key, writer)) = inner {
            if writer.num_keys() != 0 {
                let end_key = self.last_key_written.next();
                batches.add_unfinished_delta_writer(
                    writer,
                    start_key..end_key,
                    self.lsn_range.clone(),
                );
            }
        }
        batches.finish_with_discard_fn(tline, ctx, discard_fn).await
    }

    #[cfg(test)]
    pub(crate) async fn finish(
        self,
        tline: &Arc<Timeline>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<BatchWriterResult>> {
        self.finish_with_discard_fn(tline, ctx, |_| async { false })
            .await
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::{RngCore, SeedableRng};

    use crate::{
        tenant::{
            harness::{TenantHarness, TIMELINE_ID},
            storage_layer::AsLayerDesc,
        },
        DEFAULT_PG_VERSION,
    };

    use super::*;

    fn get_key(id: u32) -> Key {
        let mut key = Key::from_hex("000000000033333333444444445500000000").unwrap();
        key.field6 = id;
        key
    }

    fn get_img(id: u32) -> Bytes {
        format!("{id:064}").into()
    }

    fn get_large_img() -> Bytes {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(42);
        let mut data = vec![0; 8192];
        rng.fill_bytes(&mut data);
        data.into()
    }

    #[tokio::test]
    async fn write_one_image() {
        let harness = TenantHarness::create("split_writer_write_one_image")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        let mut image_writer = SplitImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            get_key(0),
            Lsn(0x18),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x18)..Lsn(0x20),
            4 * 1024 * 1024,
        )
        .await
        .unwrap();

        image_writer
            .put_image(get_key(0), get_img(0), &ctx)
            .await
            .unwrap();
        let layers = image_writer
            .finish(&tline, &ctx, get_key(10))
            .await
            .unwrap();
        assert_eq!(layers.len(), 1);

        delta_writer
            .put_value(get_key(0), Lsn(0x18), Value::Image(get_img(0)), &ctx)
            .await
            .unwrap();
        let layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(
            layers
                .into_iter()
                .next()
                .unwrap()
                .into_resident_layer()
                .layer_desc()
                .key(),
            PersistentLayerKey {
                key_range: get_key(0)..get_key(1),
                lsn_range: Lsn(0x18)..Lsn(0x20),
                is_delta: true
            }
        );
    }

    #[tokio::test]
    async fn write_split() {
        // Test the split writer with retaining all the layers we have produced (discard=false)
        write_split_helper("split_writer_write_split", false).await;
    }

    #[tokio::test]
    async fn write_split_discard() {
        // Test the split writer with discarding all the layers we have produced (discard=true)
        write_split_helper("split_writer_write_split_discard", true).await;
    }

    /// Test the image+delta writer by writing a large number of images and deltas. If discard is
    /// set to true, all layers will be discarded.
    async fn write_split_helper(harness_name: &'static str, discard: bool) {
        let harness = TenantHarness::create(harness_name).await.unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        let mut image_writer = SplitImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            get_key(0),
            Lsn(0x18),
            4 * 1024 * 1024,
            &ctx,
        )
        .await
        .unwrap();
        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x18)..Lsn(0x20),
            4 * 1024 * 1024,
        )
        .await
        .unwrap();
        const N: usize = 2000;
        for i in 0..N {
            let i = i as u32;
            image_writer
                .put_image(get_key(i), get_large_img(), &ctx)
                .await
                .unwrap();
            delta_writer
                .put_value(get_key(i), Lsn(0x20), Value::Image(get_large_img()), &ctx)
                .await
                .unwrap();
        }
        let image_layers = image_writer
            .finish_with_discard_fn(&tline, &ctx, get_key(N as u32), |_| async { discard })
            .await
            .unwrap();
        let delta_layers = delta_writer
            .finish_with_discard_fn(&tline, &ctx, |_| async { discard })
            .await
            .unwrap();
        let image_layers = image_layers
            .into_iter()
            .map(|x| {
                if discard {
                    x.into_discarded_layer()
                } else {
                    x.into_resident_layer().layer_desc().key()
                }
            })
            .collect_vec();
        let delta_layers = delta_layers
            .into_iter()
            .map(|x| {
                if discard {
                    x.into_discarded_layer()
                } else {
                    x.into_resident_layer().layer_desc().key()
                }
            })
            .collect_vec();
        assert_eq!(image_layers.len(), N / 512 + 1);
        assert_eq!(delta_layers.len(), N / 512 + 1);
        assert_eq!(delta_layers.first().unwrap().key_range.start, get_key(0));
        assert_eq!(
            delta_layers.last().unwrap().key_range.end,
            get_key(N as u32)
        );
        for idx in 0..image_layers.len() {
            assert_ne!(image_layers[idx].key_range.start, Key::MIN);
            assert_ne!(image_layers[idx].key_range.end, Key::MAX);
            assert_ne!(delta_layers[idx].key_range.start, Key::MIN);
            assert_ne!(delta_layers[idx].key_range.end, Key::MAX);
            if idx > 0 {
                assert_eq!(
                    image_layers[idx - 1].key_range.end,
                    image_layers[idx].key_range.start
                );
                assert_eq!(
                    delta_layers[idx - 1].key_range.end,
                    delta_layers[idx].key_range.start
                );
            }
        }
    }

    #[tokio::test]
    async fn write_large_img() {
        let harness = TenantHarness::create("split_writer_write_large_img")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        let mut image_writer = SplitImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            get_key(0),
            Lsn(0x18),
            4 * 1024,
            &ctx,
        )
        .await
        .unwrap();

        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x18)..Lsn(0x20),
            4 * 1024,
        )
        .await
        .unwrap();

        image_writer
            .put_image(get_key(0), get_img(0), &ctx)
            .await
            .unwrap();
        image_writer
            .put_image(get_key(1), get_large_img(), &ctx)
            .await
            .unwrap();
        let layers = image_writer
            .finish(&tline, &ctx, get_key(10))
            .await
            .unwrap();
        assert_eq!(layers.len(), 2);

        delta_writer
            .put_value(get_key(0), Lsn(0x18), Value::Image(get_img(0)), &ctx)
            .await
            .unwrap();
        delta_writer
            .put_value(get_key(1), Lsn(0x1A), Value::Image(get_large_img()), &ctx)
            .await
            .unwrap();
        let layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        assert_eq!(layers.len(), 2);
        let mut layers_iter = layers.into_iter();
        assert_eq!(
            layers_iter
                .next()
                .unwrap()
                .into_resident_layer()
                .layer_desc()
                .key(),
            PersistentLayerKey {
                key_range: get_key(0)..get_key(1),
                lsn_range: Lsn(0x18)..Lsn(0x20),
                is_delta: true
            }
        );
        assert_eq!(
            layers_iter
                .next()
                .unwrap()
                .into_resident_layer()
                .layer_desc()
                .key(),
            PersistentLayerKey {
                key_range: get_key(1)..get_key(2),
                lsn_range: Lsn(0x18)..Lsn(0x20),
                is_delta: true
            }
        );
    }

    #[tokio::test]
    async fn write_split_single_key() {
        let harness = TenantHarness::create("split_writer_write_split_single_key")
            .await
            .unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        const N: usize = 2000;
        let mut delta_writer = SplitDeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            Lsn(0x10)..Lsn(N as u64 * 16 + 0x10),
            4 * 1024 * 1024,
        )
        .await
        .unwrap();

        for i in 0..N {
            let i = i as u32;
            delta_writer
                .put_value(
                    get_key(0),
                    Lsn(i as u64 * 16 + 0x10),
                    Value::Image(get_large_img()),
                    &ctx,
                )
                .await
                .unwrap();
        }
        let delta_layers = delta_writer.finish(&tline, &ctx).await.unwrap();
        assert_eq!(delta_layers.len(), 1);
        let delta_layer = delta_layers
            .into_iter()
            .next()
            .unwrap()
            .into_resident_layer();
        assert_eq!(
            delta_layer.layer_desc().key(),
            PersistentLayerKey {
                key_range: get_key(0)..get_key(1),
                lsn_range: Lsn(0x10)..Lsn(N as u64 * 16 + 0x10),
                is_delta: true
            }
        );
    }
}
