//! An ImageLayer represents an image or a snapshot of a key-range at
//! one particular LSN.
//!
//! It contains an image of all key-value pairs in its key-range. Any key
//! that falls into the image layer's range but does not exist in the layer,
//! does not exist.
//!
//! An image layer is stored in a file on disk. The file is stored in
//! timelines/<timeline_id> directory.  Currently, there are no
//! subdirectories, and each image layer file is named like this:
//!
//! ```text
//!    <key start>-<key end>__<LSN>
//! ```
//!
//! For example:
//!
//! ```text
//!    000000067F000032BE0000400000000070B6-000000067F000032BE0000400000000080B6__00000000346BC568
//! ```
//!
//! Every image layer file consists of three parts: "summary",
//! "index", and "values".  The summary is a fixed size header at the
//! beginning of the file, and it contains basic information about the
//! layer, and offsets to the other parts. The "index" is a B-tree,
//! mapping from Key to an offset in the "values" part.  The
//! actual page images are stored in the "values" part.
use crate::config::PageServerConf;
use crate::context::{PageContentKind, RequestContext, RequestContextBuilder};
use crate::page_cache::{self, FileId, PAGE_SZ};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockBuf, FileBlockReader};
use crate::tenant::disk_btree::{
    DiskBtreeBuilder, DiskBtreeIterator, DiskBtreeReader, VisitDirection,
};
use crate::tenant::timeline::GetVectoredError;
use crate::tenant::vectored_blob_io::{
    BlobFlag, BufView, StreamingVectoredReadPlanner, VectoredBlobReader, VectoredRead,
    VectoredReadPlanner,
};
use crate::tenant::PageReconstructError;
use crate::virtual_file::owned_buffers_io::io_buf_ext::IoBufExt;
use crate::virtual_file::IoBufferMut;
use crate::virtual_file::{self, MaybeFatalIo, VirtualFile};
use crate::{IMAGE_FILE_MAGIC, STORAGE_FORMAT_VERSION, TEMP_FILE_SUFFIX};
use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use hex;
use itertools::Itertools;
use pageserver_api::config::MaxVectoredReadBytes;
use pageserver_api::key::DBDIR_KEY;
use pageserver_api::key::{Key, KEY_SIZE};
use pageserver_api::keyspace::KeySpace;
use pageserver_api::shard::{ShardIdentity, TenantShardId};
use pageserver_api::value::Value;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::File;
use std::io::SeekFrom;
use std::ops::Range;
use std::os::unix::prelude::FileExt;
use std::str::FromStr;
use tokio::sync::OnceCell;
use tokio_stream::StreamExt;
use tracing::*;

use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::layer_name::ImageLayerName;
use super::{AsLayerDesc, LayerName, PersistentLayerDesc, ValuesReconstructState};

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Summary {
    /// Magic value to identify this as a neon image file. Always IMAGE_FILE_MAGIC.
    pub magic: u16,
    pub format_version: u16,

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key_range: Range<Key>,
    pub lsn: Lsn,

    /// Block number where the 'index' part of the file begins.
    pub index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    pub index_root_blk: u32,
    // the 'values' part starts after the summary header, on block 1.
}

impl From<&ImageLayer> for Summary {
    fn from(layer: &ImageLayer) -> Self {
        Self::expected(
            layer.desc.tenant_shard_id.tenant_id,
            layer.desc.timeline_id,
            layer.desc.key_range.clone(),
            layer.lsn,
        )
    }
}

impl Summary {
    pub(super) fn expected(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key_range: Range<Key>,
        lsn: Lsn,
    ) -> Self {
        Self {
            magic: IMAGE_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id,
            timeline_id,
            key_range,
            lsn,

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

/// This is used only from `pagectl`. Within pageserver, all layers are
/// [`crate::tenant::storage_layer::Layer`], which can hold an [`ImageLayerInner`].
pub struct ImageLayer {
    path: Utf8PathBuf,
    pub desc: PersistentLayerDesc,
    // This entry contains an image of all pages as of this LSN, should be the same as desc.lsn
    pub lsn: Lsn,
    inner: OnceCell<ImageLayerInner>,
}

impl std::fmt::Debug for ImageLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("ImageLayer")
            .field("key_range", &RangeDisplayDebug(&self.desc.key_range))
            .field("file_size", &self.desc.file_size)
            .field("lsn", &self.lsn)
            .field("inner", &self.inner)
            .finish()
    }
}

/// ImageLayer is the in-memory data structure associated with an on-disk image
/// file.
pub struct ImageLayerInner {
    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,

    key_range: Range<Key>,
    lsn: Lsn,

    file: VirtualFile,
    file_id: FileId,

    max_vectored_read_bytes: Option<MaxVectoredReadBytes>,
}

impl ImageLayerInner {
    pub(crate) fn layer_dbg_info(&self) -> String {
        format!(
            "image {}..{} {}",
            self.key_range().start,
            self.key_range().end,
            self.lsn()
        )
    }
}

impl std::fmt::Debug for ImageLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageLayerInner")
            .field("index_start_blk", &self.index_start_blk)
            .field("index_root_blk", &self.index_root_blk)
            .finish()
    }
}

impl ImageLayerInner {
    pub(super) async fn dump(&self, ctx: &RequestContext) -> anyhow::Result<()> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        tree_reader.dump().await?;

        tree_reader
            .visit(
                &[0u8; KEY_SIZE],
                VisitDirection::Forwards,
                |key, value| {
                    println!("key: {} offset {}", hex::encode(key), value);
                    true
                },
                ctx,
            )
            .await?;

        Ok(())
    }
}

/// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
impl std::fmt::Display for ImageLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for ImageLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

impl ImageLayer {
    pub async fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        self.desc.dump();

        if !verbose {
            return Ok(());
        }

        let inner = self.load(ctx).await?;

        inner.dump(ctx).await?;

        Ok(())
    }

    fn temp_path_for(
        conf: &PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        fname: &ImageLayerName,
    ) -> Utf8PathBuf {
        let rand_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        conf.timeline_path(&tenant_shard_id, &timeline_id)
            .join(format!("{fname}.{rand_string}.{TEMP_FILE_SUFFIX}"))
    }

    ///
    /// Open the underlying file and read the metadata into memory, if it's
    /// not loaded already.
    ///
    async fn load(&self, ctx: &RequestContext) -> Result<&ImageLayerInner> {
        self.inner
            .get_or_try_init(|| self.load_inner(ctx))
            .await
            .with_context(|| format!("Failed to load image layer {}", self.path()))
    }

    async fn load_inner(&self, ctx: &RequestContext) -> Result<ImageLayerInner> {
        let path = self.path();

        let loaded =
            ImageLayerInner::load(&path, self.desc.image_layer_lsn(), None, None, ctx).await?;

        // not production code
        let actual_layer_name = LayerName::from_str(path.file_name().unwrap()).unwrap();
        let expected_layer_name = self.layer_desc().layer_name();

        if actual_layer_name != expected_layer_name {
            println!("warning: filename does not match what is expected from in-file summary");
            println!("actual: {:?}", actual_layer_name.to_string());
            println!("expected: {:?}", expected_layer_name.to_string());
        }

        Ok(loaded)
    }

    /// Create an ImageLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'pagectl' binary.
    pub fn new_for_path(path: &Utf8Path, file: File) -> Result<ImageLayer> {
        let mut summary_buf = vec![0; PAGE_SZ];
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;
        let metadata = file
            .metadata()
            .context("get file metadata to determine size")?;

        // This function is never used for constructing layers in a running pageserver,
        // so it does not need an accurate TenantShardId.
        let tenant_shard_id = TenantShardId::unsharded(summary.tenant_id);

        Ok(ImageLayer {
            path: path.to_path_buf(),
            desc: PersistentLayerDesc::new_img(
                tenant_shard_id,
                summary.timeline_id,
                summary.key_range,
                summary.lsn,
                metadata.len(),
            ), // Now we assume image layer ALWAYS covers the full range. This may change in the future.
            lsn: summary.lsn,
            inner: OnceCell::new(),
        })
    }

    fn path(&self) -> Utf8PathBuf {
        self.path.clone()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RewriteSummaryError {
    #[error("magic mismatch")]
    MagicMismatch,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<std::io::Error> for RewriteSummaryError {
    fn from(e: std::io::Error) -> Self {
        Self::Other(anyhow::anyhow!(e))
    }
}

impl ImageLayer {
    pub async fn rewrite_summary<F>(
        path: &Utf8Path,
        rewrite: F,
        ctx: &RequestContext,
    ) -> Result<(), RewriteSummaryError>
    where
        F: Fn(Summary) -> Summary,
    {
        let mut file = VirtualFile::open_with_options(
            path,
            virtual_file::OpenOptions::new().read(true).write(true),
            ctx,
        )
        .await
        .with_context(|| format!("Failed to open file '{}'", path))?;
        let file_id = page_cache::next_file_id();
        let block_reader = FileBlockReader::new(&file, file_id);
        let summary_blk = block_reader.read_blk(0, ctx).await?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref()).context("deserialize")?;
        if actual_summary.magic != IMAGE_FILE_MAGIC {
            return Err(RewriteSummaryError::MagicMismatch);
        }

        let new_summary = rewrite(actual_summary);

        let mut buf = Vec::with_capacity(PAGE_SZ);
        // TODO: could use smallvec here but it's a pain with Slice<T>
        Summary::ser_into(&new_summary, &mut buf).context("serialize")?;
        file.seek(SeekFrom::Start(0)).await?;
        let (_buf, res) = file.write_all(buf.slice_len(), ctx).await;
        res?;
        Ok(())
    }
}

impl ImageLayerInner {
    pub(crate) fn key_range(&self) -> &Range<Key> {
        &self.key_range
    }

    pub(crate) fn lsn(&self) -> Lsn {
        self.lsn
    }

    pub(super) async fn load(
        path: &Utf8Path,
        lsn: Lsn,
        summary: Option<Summary>,
        max_vectored_read_bytes: Option<MaxVectoredReadBytes>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        let file = VirtualFile::open_v2(path, ctx)
            .await
            .context("open layer file")?;
        let file_id = page_cache::next_file_id();
        let block_reader = FileBlockReader::new(&file, file_id);
        let summary_blk = block_reader
            .read_blk(0, ctx)
            .await
            .context("read first block")?;

        // length is the only way how this could fail, so it's not actually likely at all unless
        // read_blk returns wrong sized block.
        //
        // TODO: confirm and make this into assertion
        let actual_summary =
            Summary::des_prefix(summary_blk.as_ref()).context("deserialize first block")?;

        if let Some(mut expected_summary) = summary {
            // production code path
            expected_summary.index_start_blk = actual_summary.index_start_blk;
            expected_summary.index_root_blk = actual_summary.index_root_blk;
            // mask out the timeline_id, but still require the layers to be from the same tenant
            expected_summary.timeline_id = actual_summary.timeline_id;

            if actual_summary != expected_summary {
                bail!(
                    "in-file summary does not match expected summary. actual = {:?} expected = {:?}",
                    actual_summary,
                    expected_summary
                );
            }
        }

        Ok(ImageLayerInner {
            index_start_blk: actual_summary.index_start_blk,
            index_root_blk: actual_summary.index_root_blk,
            lsn,
            file,
            file_id,
            max_vectored_read_bytes,
            key_range: actual_summary.key_range,
        })
    }

    // Look up the keys in the provided keyspace and update
    // the reconstruct state with whatever is found.
    pub(super) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let reads = self
            .plan_reads(keyspace, None, ctx)
            .await
            .map_err(GetVectoredError::Other)?;

        self.do_reads_and_update_state(reads, reconstruct_state, ctx)
            .await;

        reconstruct_state.on_image_layer_visited(&self.key_range);

        Ok(())
    }

    /// Traverse the layer's index to build read operations on the overlap of the input keyspace
    /// and the keys in this layer.
    ///
    /// If shard_identity is provided, it will be used to filter keys down to those stored on
    /// this shard.
    async fn plan_reads(
        &self,
        keyspace: KeySpace,
        shard_identity: Option<&ShardIdentity>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<VectoredRead>> {
        let mut planner = VectoredReadPlanner::new(
            self.max_vectored_read_bytes
                .expect("Layer is loaded with max vectored bytes config")
                .0
                .into(),
        );

        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader =
            DiskBtreeReader::new(self.index_start_blk, self.index_root_blk, block_reader);

        let ctx = RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::ImageLayerBtreeNode)
            .build();

        for range in keyspace.ranges.iter() {
            let mut range_end_handled = false;
            let mut search_key: [u8; KEY_SIZE] = [0u8; KEY_SIZE];
            range.start.write_to_byte_slice(&mut search_key);

            let index_stream = tree_reader.clone().into_stream(&search_key, &ctx);
            let mut index_stream = std::pin::pin!(index_stream);

            while let Some(index_entry) = index_stream.next().await {
                let (raw_key, offset) = index_entry?;

                let key = Key::from_slice(&raw_key[..KEY_SIZE]);
                assert!(key >= range.start);

                let flag = if let Some(shard_identity) = shard_identity {
                    if shard_identity.is_key_disposable(&key) {
                        BlobFlag::Ignore
                    } else {
                        BlobFlag::None
                    }
                } else {
                    BlobFlag::None
                };

                if key >= range.end {
                    planner.handle_range_end(offset);
                    range_end_handled = true;
                    break;
                } else {
                    planner.handle(key, self.lsn, offset, flag);
                }
            }

            if !range_end_handled {
                let payload_end = self.index_start_blk as u64 * PAGE_SZ as u64;
                planner.handle_range_end(payload_end);
            }
        }

        Ok(planner.finish())
    }

    /// Given a key range, select the parts of that range that should be retained by the ShardIdentity,
    /// then execute vectored GET operations, passing the results of all read keys into the writer.
    pub(super) async fn filter(
        &self,
        shard_identity: &ShardIdentity,
        writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        // Fragment the range into the regions owned by this ShardIdentity
        let plan = self
            .plan_reads(
                KeySpace {
                    // If asked for the total key space, plan_reads will give us all the keys in the layer
                    ranges: vec![Key::MIN..Key::MAX],
                },
                Some(shard_identity),
                ctx,
            )
            .await?;

        let vectored_blob_reader = VectoredBlobReader::new(&self.file);
        let mut key_count = 0;
        for read in plan.into_iter() {
            let buf_size = read.size();

            let buf = IoBufferMut::with_capacity(buf_size);
            let blobs_buf = vectored_blob_reader.read_blobs(&read, buf, ctx).await?;

            let view = BufView::new_slice(&blobs_buf.buf);

            for meta in blobs_buf.blobs.iter() {
                let img_buf = meta.read(&view).await?;

                key_count += 1;
                writer
                    .put_image(meta.meta.key, img_buf.into_bytes(), ctx)
                    .await
                    .context(format!("Storing key {}", meta.meta.key))?;
            }
        }

        Ok(key_count)
    }

    async fn do_reads_and_update_state(
        &self,
        reads: Vec<VectoredRead>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) {
        let max_vectored_read_bytes = self
            .max_vectored_read_bytes
            .expect("Layer is loaded with max vectored bytes config")
            .0
            .into();

        let vectored_blob_reader = VectoredBlobReader::new(&self.file);
        for read in reads.into_iter() {
            let buf_size = read.size();

            if buf_size > max_vectored_read_bytes {
                // If the read is oversized, it should only contain one key.
                let offenders = read
                    .blobs_at
                    .as_slice()
                    .iter()
                    .filter_map(|(_, blob_meta)| {
                        if blob_meta.key.is_rel_dir_key() || blob_meta.key == DBDIR_KEY {
                            // The size of values for these keys is unbounded and can
                            // grow very large in pathological cases.
                            None
                        } else {
                            Some(format!("{}@{}", blob_meta.key, blob_meta.lsn))
                        }
                    })
                    .join(", ");

                if !offenders.is_empty() {
                    tracing::warn!(
                        "Oversized vectored read ({} > {}) for keys {}",
                        buf_size,
                        max_vectored_read_bytes,
                        offenders
                    );
                }
            }

            let buf = IoBufferMut::with_capacity(buf_size);
            let res = vectored_blob_reader.read_blobs(&read, buf, ctx).await;

            match res {
                Ok(blobs_buf) => {
                    let view = BufView::new_slice(&blobs_buf.buf);
                    for meta in blobs_buf.blobs.iter() {
                        let img_buf = meta.read(&view).await;

                        let img_buf = match img_buf {
                            Ok(img_buf) => img_buf,
                            Err(e) => {
                                reconstruct_state.on_key_error(
                                    meta.meta.key,
                                    PageReconstructError::Other(anyhow!(e).context(format!(
                                        "Failed to decompress blob from virtual file {}",
                                        self.file.path(),
                                    ))),
                                );

                                continue;
                            }
                        };
                        reconstruct_state.update_key(
                            &meta.meta.key,
                            self.lsn,
                            Value::Image(img_buf.into_bytes()),
                        );
                    }
                }
                Err(err) => {
                    let kind = err.kind();
                    for (_, blob_meta) in read.blobs_at.as_slice() {
                        reconstruct_state.on_key_error(
                            blob_meta.key,
                            PageReconstructError::from(anyhow!(
                                "Failed to read blobs from virtual file {}: {}",
                                self.file.path(),
                                kind
                            )),
                        );
                    }
                }
            };
        }
    }

    pub(crate) fn iter<'a>(&'a self, ctx: &'a RequestContext) -> ImageLayerIterator<'a> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader =
            DiskBtreeReader::new(self.index_start_blk, self.index_root_blk, block_reader);
        ImageLayerIterator {
            image_layer: self,
            ctx,
            index_iter: tree_reader.iter(&[0; KEY_SIZE], ctx),
            key_values_batch: VecDeque::new(),
            is_end: false,
            planner: StreamingVectoredReadPlanner::new(
                1024 * 8192, // The default value. Unit tests might use a different value. 1024 * 8K = 8MB buffer.
                1024,        // The default value. Unit tests might use a different value
            ),
        }
    }

    /// NB: not super efficient, but not terrible either. Should prob be an iterator.
    //
    // We're reusing the index traversal logical in plan_reads; would be nice to
    // factor that out.
    pub(crate) async fn load_keys(&self, ctx: &RequestContext) -> anyhow::Result<Vec<Key>> {
        let plan = self
            .plan_reads(KeySpace::single(self.key_range.clone()), None, ctx)
            .await?;
        Ok(plan
            .into_iter()
            .flat_map(|read| read.blobs_at)
            .map(|(_, blob_meta)| blob_meta.key)
            .collect())
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every key-value
///    pair in the key range.
///
/// 3. Call `finish`.
///
struct ImageLayerWriterInner {
    conf: &'static PageServerConf,
    path: Utf8PathBuf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,
    key_range: Range<Key>,
    lsn: Lsn,

    // Total uncompressed bytes passed into put_image
    uncompressed_bytes: u64,

    // Like `uncompressed_bytes`,
    // but only of images we might consider for compression
    uncompressed_bytes_eligible: u64,

    // Like `uncompressed_bytes`, but only of images
    // where we have chosen their compressed form
    uncompressed_bytes_chosen: u64,

    // Number of keys in the layer.
    num_keys: usize,

    blob_writer: BlobWriter<false>,
    tree: DiskBtreeBuilder<BlockBuf, KEY_SIZE>,

    #[cfg(feature = "testing")]
    last_written_key: Key,
}

impl ImageLayerWriterInner {
    ///
    /// Start building a new image layer.
    ///
    async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        key_range: &Range<Key>,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        // Create the file initially with a temporary filename.
        // We'll atomically rename it to the final name when we're done.
        let path = ImageLayer::temp_path_for(
            conf,
            timeline_id,
            tenant_shard_id,
            &ImageLayerName {
                key_range: key_range.clone(),
                lsn,
            },
        );
        trace!("creating image layer {}", path);
        let mut file = {
            VirtualFile::open_with_options(
                &path,
                virtual_file::OpenOptions::new()
                    .write(true)
                    .create_new(true),
                ctx,
            )
            .await?
        };
        // make room for the header block
        file.seek(SeekFrom::Start(PAGE_SZ as u64)).await?;
        let blob_writer = BlobWriter::new(file, PAGE_SZ as u64);

        // Initialize the b-tree index builder
        let block_buf = BlockBuf::new();
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        let writer = Self {
            conf,
            path,
            timeline_id,
            tenant_shard_id,
            key_range: key_range.clone(),
            lsn,
            tree: tree_builder,
            blob_writer,
            uncompressed_bytes: 0,
            uncompressed_bytes_eligible: 0,
            uncompressed_bytes_chosen: 0,
            num_keys: 0,
            #[cfg(feature = "testing")]
            last_written_key: Key::MIN,
        };

        Ok(writer)
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    async fn put_image(
        &mut self,
        key: Key,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        ensure!(self.key_range.contains(&key));
        let compression = self.conf.image_compression;
        let uncompressed_len = img.len() as u64;
        self.uncompressed_bytes += uncompressed_len;
        self.num_keys += 1;
        let (_img, res) = self
            .blob_writer
            .write_blob_maybe_compressed(img.slice_len(), ctx, compression)
            .await;
        // TODO: re-use the buffer for `img` further upstack
        let (off, compression_info) = res?;
        if compression_info.compressed_size.is_some() {
            // The image has been considered for compression at least
            self.uncompressed_bytes_eligible += uncompressed_len;
        }
        if compression_info.written_compressed {
            // The image has been compressed
            self.uncompressed_bytes_chosen += uncompressed_len;
        }

        let mut keybuf: [u8; KEY_SIZE] = [0u8; KEY_SIZE];
        key.write_to_byte_slice(&mut keybuf);
        self.tree.append(&keybuf, off)?;

        #[cfg(feature = "testing")]
        {
            self.last_written_key = key;
        }

        Ok(())
    }

    ///
    /// Finish writing the image layer.
    ///
    async fn finish(
        self,
        ctx: &RequestContext,
        end_key: Option<Key>,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        let temp_path = self.path.clone();
        let result = self.finish0(ctx, end_key).await;
        if let Err(ref e) = result {
            tracing::info!(%temp_path, "cleaning up temporary file after error during writing: {e}");
            if let Err(e) = std::fs::remove_file(&temp_path) {
                tracing::warn!(error=%e, %temp_path, "error cleaning up temporary layer file after error during writing");
            }
        }
        result
    }

    ///
    /// Finish writing the image layer.
    ///
    async fn finish0(
        self,
        ctx: &RequestContext,
        end_key: Option<Key>,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        let index_start_blk = self.blob_writer.size().div_ceil(PAGE_SZ as u64) as u32;

        // Calculate compression ratio
        let compressed_size = self.blob_writer.size() - PAGE_SZ as u64; // Subtract PAGE_SZ for header
        crate::metrics::COMPRESSION_IMAGE_INPUT_BYTES.inc_by(self.uncompressed_bytes);
        crate::metrics::COMPRESSION_IMAGE_INPUT_BYTES_CONSIDERED
            .inc_by(self.uncompressed_bytes_eligible);
        crate::metrics::COMPRESSION_IMAGE_INPUT_BYTES_CHOSEN.inc_by(self.uncompressed_bytes_chosen);
        crate::metrics::COMPRESSION_IMAGE_OUTPUT_BYTES.inc_by(compressed_size);

        let mut file = self.blob_writer.into_inner();

        // Write out the index
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))
            .await?;
        let (index_root_blk, block_buf) = self.tree.finish()?;
        for buf in block_buf.blocks {
            let (_buf, res) = file.write_all(buf.slice_len(), ctx).await;
            res?;
        }

        let final_key_range = if let Some(end_key) = end_key {
            self.key_range.start..end_key
        } else {
            self.key_range.clone()
        };

        // Fill in the summary on blk 0
        let summary = Summary {
            magic: IMAGE_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: self.tenant_shard_id.tenant_id,
            timeline_id: self.timeline_id,
            key_range: final_key_range.clone(),
            lsn: self.lsn,
            index_start_blk,
            index_root_blk,
        };

        let mut buf = Vec::with_capacity(PAGE_SZ);
        // TODO: could use smallvec here but it's a pain with Slice<T>
        Summary::ser_into(&summary, &mut buf)?;
        file.seek(SeekFrom::Start(0)).await?;
        let (_buf, res) = file.write_all(buf.slice_len(), ctx).await;
        res?;

        let metadata = file
            .metadata()
            .await
            .context("get metadata to determine file size")?;

        let desc = PersistentLayerDesc::new_img(
            self.tenant_shard_id,
            self.timeline_id,
            final_key_range,
            self.lsn,
            metadata.len(),
        );

        #[cfg(feature = "testing")]
        if let Some(end_key) = end_key {
            assert!(
                self.last_written_key < end_key,
                "written key violates end_key range"
            );
        }

        // Note: Because we open the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.

        // fsync the file
        file.sync_all()
            .await
            .maybe_fatal_err("image_layer sync_all")?;

        trace!("created image layer {}", self.path);

        Ok((desc, self.path))
    }
}

/// A builder object for constructing a new image layer.
///
/// Usage:
///
/// 1. Create the ImageLayerWriter by calling ImageLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_page_image` for every key-value
///    pair in the key range.
///
/// 3. Call `finish`.
///
/// # Note
///
/// As described in <https://github.com/neondatabase/neon/issues/2650>, it's
/// possible for the writer to drop before `finish` is actually called. So this
/// could lead to odd temporary files in the directory, exhausting file system.
/// This structure wraps `ImageLayerWriterInner` and also contains `Drop`
/// implementation that cleans up the temporary file in failure. It's not
/// possible to do this directly in `ImageLayerWriterInner` since `finish` moves
/// out some fields, making it impossible to implement `Drop`.
///
#[must_use]
pub struct ImageLayerWriter {
    inner: Option<ImageLayerWriterInner>,
}

impl ImageLayerWriter {
    ///
    /// Start building a new image layer.
    ///
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        key_range: &Range<Key>,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<ImageLayerWriter> {
        Ok(Self {
            inner: Some(
                ImageLayerWriterInner::new(conf, timeline_id, tenant_shard_id, key_range, lsn, ctx)
                    .await?,
            ),
        })
    }

    ///
    /// Write next value to the file.
    ///
    /// The page versions must be appended in blknum order.
    ///
    pub async fn put_image(
        &mut self,
        key: Key,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.inner.as_mut().unwrap().put_image(key, img, ctx).await
    }

    /// Estimated size of the image layer.
    pub(crate) fn estimated_size(&self) -> u64 {
        let inner = self.inner.as_ref().unwrap();
        inner.blob_writer.size() + inner.tree.borrow_writer().size() + PAGE_SZ as u64
    }

    pub(crate) fn num_keys(&self) -> usize {
        self.inner.as_ref().unwrap().num_keys
    }

    ///
    /// Finish writing the image layer.
    ///
    pub(crate) async fn finish(
        mut self,
        ctx: &RequestContext,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        self.inner.take().unwrap().finish(ctx, None).await
    }

    /// Finish writing the image layer with an end key, used in [`super::batch_split_writer::SplitImageLayerWriter`]. The end key determines the end of the image layer's covered range and is exclusive.
    pub(super) async fn finish_with_end_key(
        mut self,
        end_key: Key,
        ctx: &RequestContext,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        self.inner.take().unwrap().finish(ctx, Some(end_key)).await
    }
}

impl Drop for ImageLayerWriter {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.blob_writer.into_inner().remove();
        }
    }
}

pub struct ImageLayerIterator<'a> {
    image_layer: &'a ImageLayerInner,
    ctx: &'a RequestContext,
    planner: StreamingVectoredReadPlanner,
    index_iter: DiskBtreeIterator<'a>,
    key_values_batch: VecDeque<(Key, Lsn, Value)>,
    is_end: bool,
}

impl ImageLayerIterator<'_> {
    pub(crate) fn layer_dbg_info(&self) -> String {
        self.image_layer.layer_dbg_info()
    }

    /// Retrieve a batch of key-value pairs into the iterator buffer.
    async fn next_batch(&mut self) -> anyhow::Result<()> {
        assert!(self.key_values_batch.is_empty());
        assert!(!self.is_end);

        let plan = loop {
            if let Some(res) = self.index_iter.next().await {
                let (raw_key, offset) = res?;
                if let Some(batch_plan) = self.planner.handle(
                    Key::from_slice(&raw_key[..KEY_SIZE]),
                    self.image_layer.lsn,
                    offset,
                ) {
                    break batch_plan;
                }
            } else {
                self.is_end = true;
                let payload_end = self.image_layer.index_start_blk as u64 * PAGE_SZ as u64;
                if let Some(item) = self.planner.handle_range_end(payload_end) {
                    break item;
                } else {
                    return Ok(()); // TODO: a test case on empty iterator
                }
            }
        };
        let vectored_blob_reader = VectoredBlobReader::new(&self.image_layer.file);
        let mut next_batch = std::collections::VecDeque::new();
        let buf_size = plan.size();
        let buf = IoBufferMut::with_capacity(buf_size);
        let blobs_buf = vectored_blob_reader
            .read_blobs(&plan, buf, self.ctx)
            .await?;
        let view = BufView::new_slice(&blobs_buf.buf);
        for meta in blobs_buf.blobs.iter() {
            let img_buf = meta.read(&view).await?;
            next_batch.push_back((
                meta.meta.key,
                self.image_layer.lsn,
                Value::Image(img_buf.into_bytes()),
            ));
        }
        self.key_values_batch = next_batch;
        Ok(())
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<(Key, Lsn, Value)>> {
        if self.key_values_batch.is_empty() {
            if self.is_end {
                return Ok(None);
            }
            self.next_batch().await?;
        }
        Ok(Some(
            self.key_values_batch
                .pop_front()
                .expect("should not be empty"),
        ))
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use itertools::Itertools;
    use pageserver_api::{
        key::Key,
        shard::{ShardCount, ShardIdentity, ShardNumber, ShardStripeSize},
        value::Value,
    };
    use utils::{
        generation::Generation,
        id::{TenantId, TimelineId},
        lsn::Lsn,
    };

    use crate::{
        context::RequestContext,
        tenant::{
            config::TenantConf,
            harness::{TenantHarness, TIMELINE_ID},
            storage_layer::{Layer, ResidentLayer},
            vectored_blob_io::StreamingVectoredReadPlanner,
            Tenant, Timeline,
        },
        DEFAULT_PG_VERSION,
    };

    use super::{ImageLayerIterator, ImageLayerWriter};

    #[tokio::test]
    async fn image_layer_rewrite() {
        let tenant_conf = TenantConf {
            gc_period: Duration::ZERO,
            compaction_period: Duration::ZERO,
            ..TenantConf::default()
        };
        let tenant_id = TenantId::generate();
        let mut gen = Generation::new(0xdead0001);
        let mut get_next_gen = || {
            let ret = gen;
            gen = gen.next();
            ret
        };
        // The LSN at which we will create an image layer to filter
        let lsn = Lsn(0xdeadbeef0000);
        let timeline_id = TimelineId::generate();

        //
        // Create an unsharded parent with a layer.
        //

        let harness = TenantHarness::create_custom(
            "test_image_layer_rewrite--parent",
            tenant_conf.clone(),
            tenant_id,
            ShardIdentity::unsharded(),
            get_next_gen(),
        )
        .await
        .unwrap();
        let (tenant, ctx) = harness.load().await;
        let timeline = tenant
            .create_test_timeline(timeline_id, lsn, DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        // This key range contains several 0x8000 page stripes, only one of which belongs to shard zero
        let input_start = Key::from_hex("000000067f00000001000000ae0000000000").unwrap();
        let input_end = Key::from_hex("000000067f00000001000000ae0000020000").unwrap();
        let range = input_start..input_end;

        // Build an image layer to filter
        let resident = {
            let mut writer = ImageLayerWriter::new(
                harness.conf,
                timeline_id,
                harness.tenant_shard_id,
                &range,
                lsn,
                &ctx,
            )
            .await
            .unwrap();

            let foo_img = Bytes::from_static(&[1, 2, 3, 4]);
            let mut key = range.start;
            while key < range.end {
                writer.put_image(key, foo_img.clone(), &ctx).await.unwrap();

                key = key.next();
            }
            let (desc, path) = writer.finish(&ctx).await.unwrap();
            Layer::finish_creating(tenant.conf, &timeline, desc, &path).unwrap()
        };
        let original_size = resident.metadata().file_size;

        //
        // Create child shards and do the rewrite, exercising filter().
        // TODO: abstraction in TenantHarness for splits.
        //

        // Filter for various shards: this exercises cases like values at start of key range, end of key
        // range, middle of key range.
        let shard_count = ShardCount::new(4);
        for shard_number in 0..shard_count.count() {
            //
            // mimic the shard split
            //
            let shard_identity = ShardIdentity::new(
                ShardNumber(shard_number),
                shard_count,
                ShardStripeSize(0x8000),
            )
            .unwrap();
            let harness = TenantHarness::create_custom(
                Box::leak(Box::new(format!(
                    "test_image_layer_rewrite--child{}",
                    shard_identity.shard_slug()
                ))),
                tenant_conf.clone(),
                tenant_id,
                shard_identity,
                // NB: in reality, the shards would each fork off their own gen number sequence from the parent.
                // But here, all we care about is that the gen number is unique.
                get_next_gen(),
            )
            .await
            .unwrap();
            let (tenant, ctx) = harness.load().await;
            let timeline = tenant
                .create_test_timeline(timeline_id, lsn, DEFAULT_PG_VERSION, &ctx)
                .await
                .unwrap();

            //
            // use filter() and make assertions
            //

            let mut filtered_writer = ImageLayerWriter::new(
                harness.conf,
                timeline_id,
                harness.tenant_shard_id,
                &range,
                lsn,
                &ctx,
            )
            .await
            .unwrap();

            let wrote_keys = resident
                .filter(&shard_identity, &mut filtered_writer, &ctx)
                .await
                .unwrap();
            let replacement = if wrote_keys > 0 {
                let (desc, path) = filtered_writer.finish(&ctx).await.unwrap();
                let resident = Layer::finish_creating(tenant.conf, &timeline, desc, &path).unwrap();
                Some(resident)
            } else {
                None
            };

            // This exact size and those below will need updating as/when the layer encoding changes, but
            // should be deterministic for a given version of the format, as we used no randomness generating the input.
            assert_eq!(original_size, 1597440);

            match shard_number {
                0 => {
                    // We should have written out just one stripe for our shard identity
                    assert_eq!(wrote_keys, 0x8000);
                    let replacement = replacement.unwrap();

                    // We should have dropped some of the data
                    assert!(replacement.metadata().file_size < original_size);
                    assert!(replacement.metadata().file_size > 0);

                    // Assert that we dropped ~3/4 of the data.
                    assert_eq!(replacement.metadata().file_size, 417792);
                }
                1 => {
                    // Shard 1 has no keys in our input range
                    assert_eq!(wrote_keys, 0x0);
                    assert!(replacement.is_none());
                }
                2 => {
                    // Shard 2 has one stripes in the input range
                    assert_eq!(wrote_keys, 0x8000);
                    let replacement = replacement.unwrap();
                    assert!(replacement.metadata().file_size < original_size);
                    assert!(replacement.metadata().file_size > 0);
                    assert_eq!(replacement.metadata().file_size, 417792);
                }
                3 => {
                    // Shard 3 has two stripes in the input range
                    assert_eq!(wrote_keys, 0x10000);
                    let replacement = replacement.unwrap();
                    assert!(replacement.metadata().file_size < original_size);
                    assert!(replacement.metadata().file_size > 0);
                    assert_eq!(replacement.metadata().file_size, 811008);
                }
                _ => unreachable!(),
            }
        }
    }

    async fn produce_image_layer(
        tenant: &Tenant,
        tline: &Arc<Timeline>,
        mut images: Vec<(Key, Bytes)>,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<ResidentLayer> {
        images.sort();
        let (key_start, _) = images.first().unwrap();
        let (key_last, _) = images.last().unwrap();
        let key_end = key_last.next();
        let key_range = *key_start..key_end;
        let mut writer = ImageLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            &key_range,
            lsn,
            ctx,
        )
        .await?;

        for (key, img) in images {
            writer.put_image(key, img, ctx).await?;
        }
        let (desc, path) = writer.finish(ctx).await?;
        let img_layer = Layer::finish_creating(tenant.conf, tline, desc, &path)?;

        Ok::<_, anyhow::Error>(img_layer)
    }

    async fn assert_img_iter_equal(
        img_iter: &mut ImageLayerIterator<'_>,
        expect: &[(Key, Bytes)],
        expect_lsn: Lsn,
    ) {
        let mut expect_iter = expect.iter();
        loop {
            let o1 = img_iter.next().await.unwrap();
            let o2 = expect_iter.next();
            match (o1, o2) {
                (None, None) => break,
                (Some((k1, l1, v1)), Some((k2, i2))) => {
                    let Value::Image(i1) = v1 else {
                        panic!("expect Value::Image")
                    };
                    assert_eq!(&k1, k2);
                    assert_eq!(l1, expect_lsn);
                    assert_eq!(&i1, i2);
                }
                (o1, o2) => panic!("iterators length mismatch: {:?}, {:?}", o1, o2),
            }
        }
    }

    #[tokio::test]
    async fn image_layer_iterator() {
        let harness = TenantHarness::create("image_layer_iterator").await.unwrap();
        let (tenant, ctx) = harness.load().await;

        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await
            .unwrap();

        fn get_key(id: u32) -> Key {
            let mut key = Key::from_hex("000000000033333333444444445500000000").unwrap();
            key.field6 = id;
            key
        }
        const N: usize = 1000;
        let test_imgs = (0..N)
            .map(|idx| (get_key(idx as u32), Bytes::from(format!("img{idx:05}"))))
            .collect_vec();
        let resident_layer =
            produce_image_layer(&tenant, &tline, test_imgs.clone(), Lsn(0x10), &ctx)
                .await
                .unwrap();
        let img_layer = resident_layer.get_as_image(&ctx).await.unwrap();
        for max_read_size in [1, 1024] {
            for batch_size in [1, 2, 4, 8, 3, 7, 13] {
                println!("running with batch_size={batch_size} max_read_size={max_read_size}");
                // Test if the batch size is correctly determined
                let mut iter = img_layer.iter(&ctx);
                iter.planner = StreamingVectoredReadPlanner::new(max_read_size, batch_size);
                let mut num_items = 0;
                for _ in 0..3 {
                    iter.next_batch().await.unwrap();
                    num_items += iter.key_values_batch.len();
                    if max_read_size == 1 {
                        // every key should be a batch b/c the value is larger than max_read_size
                        assert_eq!(iter.key_values_batch.len(), 1);
                    } else {
                        assert!(iter.key_values_batch.len() <= batch_size);
                    }
                    if num_items >= N {
                        break;
                    }
                    iter.key_values_batch.clear();
                }
                // Test if the result is correct
                let mut iter = img_layer.iter(&ctx);
                iter.planner = StreamingVectoredReadPlanner::new(max_read_size, batch_size);
                assert_img_iter_equal(&mut iter, &test_imgs, Lsn(0x10)).await;
            }
        }
    }
}
