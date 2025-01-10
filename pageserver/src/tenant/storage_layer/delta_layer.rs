//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, and in a range of Keys. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences, in the form of WAL records
//! against a base LSN. However, if a relation extended or a whole new relation
//! is created, there would be no base for the new pages. The entries for them
//! must be page images or WAL records with the 'will_init' flag set, so that
//! they can be replayed without referring to an older page version.
//!
//! The delta files are stored in `timelines/<timeline_id>` directory.  Currently,
//! there are no subdirectories, and each delta file is named like this:
//!
//! ```text
//!    <key start>-<key end>__<start LSN>-<end LSN>
//! ```
//!
//! For example:
//!
//! ```text
//!    000000067F000032BE0000400000000020B6-000000067F000032BE0000400000000030B6__000000578C6B29-0000000057A50051
//! ```
//!
//! Every delta file consists of three parts: "summary", "values", and
//! "index". The summary is a fixed size header at the beginning of the file,
//! and it contains basic information about the layer, and offsets to the other
//! parts. The "index" is a B-tree, mapping from Key and LSN to an offset in the
//! "values" part.  The actual page images and WAL records are stored in the
//! "values" part.
//!
use crate::config::PageServerConf;
use crate::context::{PageContentKind, RequestContext, RequestContextBuilder};
use crate::page_cache::{self, FileId, PAGE_SZ};
use crate::tenant::blob_io::BlobWriter;
use crate::tenant::block_io::{BlockBuf, BlockCursor, BlockLease, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{
    DiskBtreeBuilder, DiskBtreeIterator, DiskBtreeReader, VisitDirection,
};
use crate::tenant::storage_layer::layer::S3_UPLOAD_LIMIT;
use crate::tenant::timeline::GetVectoredError;
use crate::tenant::vectored_blob_io::{
    BlobFlag, BufView, StreamingVectoredReadPlanner, VectoredBlobReader, VectoredRead,
    VectoredReadPlanner,
};
use crate::tenant::PageReconstructError;
use crate::virtual_file::owned_buffers_io::io_buf_ext::{FullSlice, IoBufExt};
use crate::virtual_file::IoBufferMut;
use crate::virtual_file::{self, MaybeFatalIo, VirtualFile};
use crate::TEMP_FILE_SUFFIX;
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{anyhow, bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use futures::StreamExt;
use itertools::Itertools;
use pageserver_api::config::MaxVectoredReadBytes;
use pageserver_api::key::DBDIR_KEY;
use pageserver_api::key::{Key, KEY_SIZE};
use pageserver_api::keyspace::KeySpace;
use pageserver_api::models::ImageCompressionAlgorithm;
use pageserver_api::shard::TenantShardId;
use pageserver_api::value::Value;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::File;
use std::io::SeekFrom;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio_epoll_uring::IoBuf;
use tracing::*;

use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use super::{AsLayerDesc, LayerName, PersistentLayerDesc, ValuesReconstructState};

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Summary {
    /// Magic value to identify this as a neon delta file. Always DELTA_FILE_MAGIC.
    pub magic: u16,
    pub format_version: u16,

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,

    /// Block number where the 'index' part of the file begins.
    pub index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    pub index_root_blk: u32,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self::expected(
            layer.desc.tenant_shard_id.tenant_id,
            layer.desc.timeline_id,
            layer.desc.key_range.clone(),
            layer.desc.lsn_range.clone(),
        )
    }
}

impl Summary {
    pub(super) fn expected(
        tenant_id: TenantId,
        timeline_id: TimelineId,
        keys: Range<Key>,
        lsns: Range<Lsn>,
    ) -> Self {
        Self {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,

            tenant_id,
            timeline_id,
            key_range: keys,
            lsn_range: lsns,

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

// Flag indicating that this version initialize the page
const WILL_INIT: u64 = 1;

/// Struct representing reference to BLOB in layers.
///
/// Reference contains BLOB offset, and for WAL records it also contains
/// `will_init` flag. The flag helps to determine the range of records
/// that needs to be applied, without reading/deserializing records themselves.
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct BlobRef(pub u64);

impl BlobRef {
    pub fn will_init(&self) -> bool {
        (self.0 & WILL_INIT) != 0
    }

    pub fn pos(&self) -> u64 {
        self.0 >> 1
    }

    pub fn new(pos: u64, will_init: bool) -> BlobRef {
        let mut blob_ref = pos << 1;
        if will_init {
            blob_ref |= WILL_INIT;
        }
        BlobRef(blob_ref)
    }
}

pub const DELTA_KEY_SIZE: usize = KEY_SIZE + 8;
struct DeltaKey([u8; DELTA_KEY_SIZE]);

/// This is the key of the B-tree index stored in the delta layer. It consists
/// of the serialized representation of a Key and LSN.
impl DeltaKey {
    fn from_slice(buf: &[u8]) -> Self {
        let mut bytes: [u8; DELTA_KEY_SIZE] = [0u8; DELTA_KEY_SIZE];
        bytes.copy_from_slice(buf);
        DeltaKey(bytes)
    }

    fn from_key_lsn(key: &Key, lsn: Lsn) -> Self {
        let mut bytes: [u8; DELTA_KEY_SIZE] = [0u8; DELTA_KEY_SIZE];
        key.write_to_byte_slice(&mut bytes[0..KEY_SIZE]);
        bytes[KEY_SIZE..].copy_from_slice(&u64::to_be_bytes(lsn.0));
        DeltaKey(bytes)
    }

    fn key(&self) -> Key {
        Key::from_slice(&self.0)
    }

    fn lsn(&self) -> Lsn {
        Lsn(u64::from_be_bytes(self.0[KEY_SIZE..].try_into().unwrap()))
    }

    fn extract_lsn_from_buf(buf: &[u8]) -> Lsn {
        let mut lsn_buf = [0u8; 8];
        lsn_buf.copy_from_slice(&buf[KEY_SIZE..]);
        Lsn(u64::from_be_bytes(lsn_buf))
    }
}

/// This is used only from `pagectl`. Within pageserver, all layers are
/// [`crate::tenant::storage_layer::Layer`], which can hold a [`DeltaLayerInner`].
pub struct DeltaLayer {
    path: Utf8PathBuf,
    pub desc: PersistentLayerDesc,
    inner: OnceCell<Arc<DeltaLayerInner>>,
}

impl std::fmt::Debug for DeltaLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use super::RangeDisplayDebug;

        f.debug_struct("DeltaLayer")
            .field("key_range", &RangeDisplayDebug(&self.desc.key_range))
            .field("lsn_range", &self.desc.lsn_range)
            .field("file_size", &self.desc.file_size)
            .field("inner", &self.inner)
            .finish()
    }
}

/// `DeltaLayerInner` is the in-memory data structure associated with an on-disk delta
/// file.
pub struct DeltaLayerInner {
    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,

    file: VirtualFile,
    file_id: FileId,

    layer_key_range: Range<Key>,
    layer_lsn_range: Range<Lsn>,

    max_vectored_read_bytes: Option<MaxVectoredReadBytes>,
}

impl DeltaLayerInner {
    pub(crate) fn layer_dbg_info(&self) -> String {
        format!(
            "delta {}..{} {}..{}",
            self.key_range().start,
            self.key_range().end,
            self.lsn_range().start,
            self.lsn_range().end
        )
    }
}

impl std::fmt::Debug for DeltaLayerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaLayerInner")
            .field("index_start_blk", &self.index_start_blk)
            .field("index_root_blk", &self.index_root_blk)
            .finish()
    }
}

/// Boilerplate to implement the Layer trait, always use layer_desc for persistent layers.
impl std::fmt::Display for DeltaLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.layer_desc().short_id())
    }
}

impl AsLayerDesc for DeltaLayer {
    fn layer_desc(&self) -> &PersistentLayerDesc {
        &self.desc
    }
}

impl DeltaLayer {
    pub async fn dump(&self, verbose: bool, ctx: &RequestContext) -> Result<()> {
        self.desc.dump();

        if !verbose {
            return Ok(());
        }

        let inner = self.load(ctx).await?;

        inner.dump(ctx).await
    }

    fn temp_path_for(
        conf: &PageServerConf,
        tenant_shard_id: &TenantShardId,
        timeline_id: &TimelineId,
        key_start: Key,
        lsn_range: &Range<Lsn>,
    ) -> Utf8PathBuf {
        let rand_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        conf.timeline_path(tenant_shard_id, timeline_id)
            .join(format!(
                "{}-XXX__{:016X}-{:016X}.{}.{}",
                key_start,
                u64::from(lsn_range.start),
                u64::from(lsn_range.end),
                rand_string,
                TEMP_FILE_SUFFIX,
            ))
    }

    ///
    /// Open the underlying file and read the metadata into memory, if it's
    /// not loaded already.
    ///
    async fn load(&self, ctx: &RequestContext) -> Result<&Arc<DeltaLayerInner>> {
        // Quick exit if already loaded
        self.inner
            .get_or_try_init(|| self.load_inner(ctx))
            .await
            .with_context(|| format!("Failed to load delta layer {}", self.path()))
    }

    async fn load_inner(&self, ctx: &RequestContext) -> anyhow::Result<Arc<DeltaLayerInner>> {
        let path = self.path();

        let loaded = DeltaLayerInner::load(&path, None, None, ctx).await?;

        // not production code
        let actual_layer_name = LayerName::from_str(path.file_name().unwrap()).unwrap();
        let expected_layer_name = self.layer_desc().layer_name();

        if actual_layer_name != expected_layer_name {
            println!("warning: filename does not match what is expected from in-file summary");
            println!("actual: {:?}", actual_layer_name.to_string());
            println!("expected: {:?}", expected_layer_name.to_string());
        }

        Ok(Arc::new(loaded))
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'pagectl' binary.
    pub fn new_for_path(path: &Utf8Path, file: File) -> Result<Self> {
        let mut summary_buf = vec![0; PAGE_SZ];
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;

        let metadata = file
            .metadata()
            .context("get file metadata to determine size")?;

        // This function is never used for constructing layers in a running pageserver,
        // so it does not need an accurate TenantShardId.
        let tenant_shard_id = TenantShardId::unsharded(summary.tenant_id);

        Ok(DeltaLayer {
            path: path.to_path_buf(),
            desc: PersistentLayerDesc::new_delta(
                tenant_shard_id,
                summary.timeline_id,
                summary.key_range,
                summary.lsn_range,
                metadata.len(),
            ),
            inner: OnceCell::new(),
        })
    }

    /// Path to the layer file in pageserver workdir.
    fn path(&self) -> Utf8PathBuf {
        self.path.clone()
    }
}

/// A builder object for constructing a new delta layer.
///
/// Usage:
///
/// 1. Create the DeltaLayerWriter by calling DeltaLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_value` for every page
///    version to store in the layer.
///
/// 3. Call `finish`.
///
struct DeltaLayerWriterInner {
    pub path: Utf8PathBuf,
    timeline_id: TimelineId,
    tenant_shard_id: TenantShardId,

    key_start: Key,
    lsn_range: Range<Lsn>,

    tree: DiskBtreeBuilder<BlockBuf, DELTA_KEY_SIZE>,

    blob_writer: BlobWriter<true>,

    // Number of key-lsns in the layer.
    num_keys: usize,
}

impl DeltaLayerWriterInner {
    ///
    /// Start building a new delta layer.
    ///
    async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        key_start: Key,
        lsn_range: Range<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        // Create the file initially with a temporary filename. We don't know
        // the end key yet, so we cannot form the final filename yet. We will
        // rename it when we're done.
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path =
            DeltaLayer::temp_path_for(conf, &tenant_shard_id, &timeline_id, key_start, &lsn_range);

        let mut file = VirtualFile::create(&path, ctx).await?;
        // make room for the header block
        file.seek(SeekFrom::Start(PAGE_SZ as u64)).await?;
        let blob_writer = BlobWriter::new(file, PAGE_SZ as u64);

        // Initialize the b-tree index builder
        let block_buf = BlockBuf::new();
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        Ok(Self {
            path,
            timeline_id,
            tenant_shard_id,
            key_start,
            lsn_range,
            tree: tree_builder,
            blob_writer,
            num_keys: 0,
        })
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    async fn put_value(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let (_, res) = self
            .put_value_bytes(
                key,
                lsn,
                Value::ser(&val)?.slice_len(),
                val.will_init(),
                ctx,
            )
            .await;
        res
    }

    async fn put_value_bytes<Buf>(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: FullSlice<Buf>,
        will_init: bool,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, anyhow::Result<()>)
    where
        Buf: IoBuf + Send,
    {
        assert!(
            self.lsn_range.start <= lsn,
            "lsn_start={}, lsn={}",
            self.lsn_range.start,
            lsn
        );
        // We don't want to use compression in delta layer creation
        let compression = ImageCompressionAlgorithm::Disabled;
        let (val, res) = self
            .blob_writer
            .write_blob_maybe_compressed(val, ctx, compression)
            .await;
        let off = match res {
            Ok((off, _)) => off,
            Err(e) => return (val, Err(anyhow::anyhow!(e))),
        };

        let blob_ref = BlobRef::new(off, will_init);

        let delta_key = DeltaKey::from_key_lsn(&key, lsn);
        let res = self.tree.append(&delta_key.0, blob_ref.0);

        self.num_keys += 1;

        (val, res.map_err(|e| anyhow::anyhow!(e)))
    }

    fn size(&self) -> u64 {
        self.blob_writer.size() + self.tree.borrow_writer().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    async fn finish(
        self,
        key_end: Key,
        ctx: &RequestContext,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        let temp_path = self.path.clone();
        let result = self.finish0(key_end, ctx).await;
        if let Err(ref e) = result {
            tracing::info!(%temp_path, "cleaning up temporary file after error during writing: {e}");
            if let Err(e) = std::fs::remove_file(&temp_path) {
                tracing::warn!(error=%e, %temp_path, "error cleaning up temporary layer file after error during writing");
            }
        }
        result
    }

    async fn finish0(
        self,
        key_end: Key,
        ctx: &RequestContext,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        let index_start_blk = self.blob_writer.size().div_ceil(PAGE_SZ as u64) as u32;

        let mut file = self.blob_writer.into_inner(ctx).await?;

        // Write out the index
        let (index_root_blk, block_buf) = self.tree.finish()?;
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))
            .await?;
        for buf in block_buf.blocks {
            let (_buf, res) = file.write_all(buf.slice_len(), ctx).await;
            res?;
        }
        assert!(self.lsn_range.start < self.lsn_range.end);
        // Fill in the summary on blk 0
        let summary = Summary {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: self.tenant_shard_id.tenant_id,
            timeline_id: self.timeline_id,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
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
            .context("get file metadata to determine size")?;

        // 5GB limit for objects without multipart upload (which we don't want to use)
        // Make it a little bit below to account for differing GB units
        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
        ensure!(
            metadata.len() <= S3_UPLOAD_LIMIT,
            "Created delta layer file at {} of size {} above limit {S3_UPLOAD_LIMIT}!",
            file.path(),
            metadata.len()
        );

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.

        let desc = PersistentLayerDesc::new_delta(
            self.tenant_shard_id,
            self.timeline_id,
            self.key_start..key_end,
            self.lsn_range.clone(),
            metadata.len(),
        );

        // fsync the file
        file.sync_all()
            .await
            .maybe_fatal_err("delta_layer sync_all")?;

        trace!("created delta layer {}", self.path);

        Ok((desc, self.path))
    }
}

/// A builder object for constructing a new delta layer.
///
/// Usage:
///
/// 1. Create the DeltaLayerWriter by calling DeltaLayerWriter::new(...)
///
/// 2. Write the contents by calling `put_value` for every page
///    version to store in the layer.
///
/// 3. Call `finish`.
///
/// # Note
///
/// As described in <https://github.com/neondatabase/neon/issues/2650>, it's
/// possible for the writer to drop before `finish` is actually called. So this
/// could lead to odd temporary files in the directory, exhausting file system.
/// This structure wraps `DeltaLayerWriterInner` and also contains `Drop`
/// implementation that cleans up the temporary file in failure. It's not
/// possible to do this directly in `DeltaLayerWriterInner` since `finish` moves
/// out some fields, making it impossible to implement `Drop`.
///
#[must_use]
pub struct DeltaLayerWriter {
    inner: Option<DeltaLayerWriterInner>,
}

impl DeltaLayerWriter {
    ///
    /// Start building a new delta layer.
    ///
    pub async fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        key_start: Key,
        lsn_range: Range<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner: Some(
                DeltaLayerWriterInner::new(
                    conf,
                    timeline_id,
                    tenant_shard_id,
                    key_start,
                    lsn_range,
                    ctx,
                )
                .await?,
            ),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.inner.as_ref().unwrap().num_keys == 0
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    pub async fn put_value(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: Value,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        self.inner
            .as_mut()
            .unwrap()
            .put_value(key, lsn, val, ctx)
            .await
    }

    pub async fn put_value_bytes<Buf>(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: FullSlice<Buf>,
        will_init: bool,
        ctx: &RequestContext,
    ) -> (FullSlice<Buf>, anyhow::Result<()>)
    where
        Buf: IoBuf + Send,
    {
        self.inner
            .as_mut()
            .unwrap()
            .put_value_bytes(key, lsn, val, will_init, ctx)
            .await
    }

    pub fn size(&self) -> u64 {
        self.inner.as_ref().unwrap().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    pub(crate) async fn finish(
        mut self,
        key_end: Key,
        ctx: &RequestContext,
    ) -> anyhow::Result<(PersistentLayerDesc, Utf8PathBuf)> {
        self.inner.take().unwrap().finish(key_end, ctx).await
    }

    pub(crate) fn num_keys(&self) -> usize {
        self.inner.as_ref().unwrap().num_keys
    }

    pub(crate) fn estimated_size(&self) -> u64 {
        let inner = self.inner.as_ref().unwrap();
        inner.blob_writer.size() + inner.tree.borrow_writer().size() + PAGE_SZ as u64
    }
}

impl Drop for DeltaLayerWriter {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // We want to remove the virtual file here, so it's fine to not
            // having completely flushed unwritten data.
            let vfile = inner.blob_writer.into_inner_no_flush();
            vfile.remove();
        }
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

impl DeltaLayer {
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
        if actual_summary.magic != DELTA_FILE_MAGIC {
            return Err(RewriteSummaryError::MagicMismatch);
        }

        let new_summary = rewrite(actual_summary);

        let mut buf = Vec::with_capacity(PAGE_SZ);
        // TODO: could use smallvec here, but it's a pain with Slice<T>
        Summary::ser_into(&new_summary, &mut buf).context("serialize")?;
        file.seek(SeekFrom::Start(0)).await?;
        let (_buf, res) = file.write_all(buf.slice_len(), ctx).await;
        res?;
        Ok(())
    }
}

impl DeltaLayerInner {
    pub(crate) fn key_range(&self) -> &Range<Key> {
        &self.layer_key_range
    }

    pub(crate) fn lsn_range(&self) -> &Range<Lsn> {
        &self.layer_lsn_range
    }

    pub(super) async fn load(
        path: &Utf8Path,
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

        // TODO: this should be an assertion instead; see ImageLayerInner::load
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

        Ok(DeltaLayerInner {
            file,
            file_id,
            index_start_blk: actual_summary.index_start_blk,
            index_root_blk: actual_summary.index_root_blk,
            max_vectored_read_bytes,
            layer_key_range: actual_summary.key_range,
            layer_lsn_range: actual_summary.lsn_range,
        })
    }

    // Look up the keys in the provided keyspace and update
    // the reconstruct state with whatever is found.
    //
    // If the key is cached, go no further than the cached Lsn.
    //
    // Currently, the index is visited for each range, but this
    // can be further optimised to visit the index only once.
    pub(super) async fn get_values_reconstruct_data(
        &self,
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> Result<(), GetVectoredError> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let index_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        let planner = VectoredReadPlanner::new(
            self.max_vectored_read_bytes
                .expect("Layer is loaded with max vectored bytes config")
                .0
                .into(),
        );

        let data_end_offset = self.index_start_offset();

        let reads = Self::plan_reads(
            &keyspace,
            lsn_range.clone(),
            data_end_offset,
            index_reader,
            planner,
            reconstruct_state,
            ctx,
        )
        .await
        .map_err(GetVectoredError::Other)?;

        self.do_reads_and_update_state(reads, reconstruct_state, ctx)
            .await;

        reconstruct_state.on_lsn_advanced(&keyspace, lsn_range.start);

        Ok(())
    }

    async fn plan_reads<Reader>(
        keyspace: &KeySpace,
        lsn_range: Range<Lsn>,
        data_end_offset: u64,
        index_reader: DiskBtreeReader<Reader, DELTA_KEY_SIZE>,
        mut planner: VectoredReadPlanner,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) -> anyhow::Result<Vec<VectoredRead>>
    where
        Reader: BlockReader + Clone,
    {
        let ctx = RequestContextBuilder::extend(ctx)
            .page_content_kind(PageContentKind::DeltaLayerBtreeNode)
            .build();

        for range in keyspace.ranges.iter() {
            let mut range_end_handled = false;

            let start_key = DeltaKey::from_key_lsn(&range.start, lsn_range.start);
            let index_stream = index_reader.clone().into_stream(&start_key.0, &ctx);
            let mut index_stream = std::pin::pin!(index_stream);

            while let Some(index_entry) = index_stream.next().await {
                let (raw_key, value) = index_entry?;
                let key = Key::from_slice(&raw_key[..KEY_SIZE]);
                let lsn = DeltaKey::extract_lsn_from_buf(&raw_key);
                let blob_ref = BlobRef(value);

                // Lsns are not monotonically increasing across keys, so we don't assert on them.
                assert!(key >= range.start);

                let outside_lsn_range = !lsn_range.contains(&lsn);
                let below_cached_lsn = reconstruct_state.get_cached_lsn(&key) >= Some(lsn);

                let flag = {
                    if outside_lsn_range || below_cached_lsn {
                        BlobFlag::Ignore
                    } else if blob_ref.will_init() {
                        BlobFlag::ReplaceAll
                    } else {
                        // Usual path: add blob to the read
                        BlobFlag::None
                    }
                };

                if key >= range.end || (key.next() == range.end && lsn >= lsn_range.end) {
                    planner.handle_range_end(blob_ref.pos());
                    range_end_handled = true;
                    break;
                } else {
                    planner.handle(key, lsn, blob_ref.pos(), flag);
                }
            }

            if !range_end_handled {
                tracing::debug!("Handling range end fallback at {}", data_end_offset);
                planner.handle_range_end(data_end_offset);
            }
        }

        Ok(planner.finish())
    }

    fn get_min_read_buffer_size(
        planned_reads: &[VectoredRead],
        read_size_soft_max: usize,
    ) -> usize {
        let Some(largest_read) = planned_reads.iter().max_by_key(|read| read.size()) else {
            return read_size_soft_max;
        };

        let largest_read_size = largest_read.size();
        if largest_read_size > read_size_soft_max {
            // If the read is oversized, it should only contain one key.
            let offenders = largest_read
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
                    largest_read_size,
                    read_size_soft_max,
                    offenders
                );
            }
        }

        largest_read_size
    }

    async fn do_reads_and_update_state(
        &self,
        reads: Vec<VectoredRead>,
        reconstruct_state: &mut ValuesReconstructState,
        ctx: &RequestContext,
    ) {
        let vectored_blob_reader = VectoredBlobReader::new(&self.file);
        let mut ignore_key_with_err = None;

        let max_vectored_read_bytes = self
            .max_vectored_read_bytes
            .expect("Layer is loaded with max vectored bytes config")
            .0
            .into();
        let buf_size = Self::get_min_read_buffer_size(&reads, max_vectored_read_bytes);
        let mut buf = Some(IoBufferMut::with_capacity(buf_size));

        // Note that reads are processed in reverse order (from highest key+lsn).
        // This is the order that `ReconstructState` requires such that it can
        // track when a key is done.
        for read in reads.into_iter().rev() {
            let res = vectored_blob_reader
                .read_blobs(&read, buf.take().expect("Should have a buffer"), ctx)
                .await;

            let blobs_buf = match res {
                Ok(blobs_buf) => blobs_buf,
                Err(err) => {
                    let kind = err.kind();
                    for (_, blob_meta) in read.blobs_at.as_slice() {
                        reconstruct_state.on_key_error(
                            blob_meta.key,
                            PageReconstructError::Other(anyhow!(
                                "Failed to read blobs from virtual file {}: {}",
                                self.file.path(),
                                kind
                            )),
                        );
                    }

                    // We have "lost" the buffer since the lower level IO api
                    // doesn't return the buffer on error. Allocate a new one.
                    buf = Some(IoBufferMut::with_capacity(buf_size));

                    continue;
                }
            };
            let view = BufView::new_slice(&blobs_buf.buf);
            for meta in blobs_buf.blobs.iter().rev() {
                if Some(meta.meta.key) == ignore_key_with_err {
                    continue;
                }
                let blob_read = meta.read(&view).await;
                let blob_read = match blob_read {
                    Ok(buf) => buf,
                    Err(e) => {
                        reconstruct_state.on_key_error(
                            meta.meta.key,
                            PageReconstructError::Other(anyhow!(e).context(format!(
                                "Failed to decompress blob from virtual file {}",
                                self.file.path(),
                            ))),
                        );

                        ignore_key_with_err = Some(meta.meta.key);
                        continue;
                    }
                };

                let value = Value::des(&blob_read);

                let value = match value {
                    Ok(v) => v,
                    Err(e) => {
                        reconstruct_state.on_key_error(
                            meta.meta.key,
                            PageReconstructError::Other(anyhow!(e).context(format!(
                                "Failed to deserialize blob from virtual file {}",
                                self.file.path(),
                            ))),
                        );

                        ignore_key_with_err = Some(meta.meta.key);
                        continue;
                    }
                };

                // Invariant: once a key reaches [`ValueReconstructSituation::Complete`]
                // state, no further updates shall be made to it. The call below will
                // panic if the invariant is violated.
                reconstruct_state.update_key(&meta.meta.key, meta.meta.lsn, value);
            }

            buf = Some(blobs_buf.buf);
        }
    }

    pub(crate) async fn index_entries<'a>(
        &'a self,
        ctx: &RequestContext,
    ) -> Result<Vec<DeltaEntry<'a>>> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        let mut all_keys: Vec<DeltaEntry<'_>> = Vec::new();

        tree_reader
            .visit(
                &[0u8; DELTA_KEY_SIZE],
                VisitDirection::Forwards,
                |key, value| {
                    let delta_key = DeltaKey::from_slice(key);
                    let val_ref = ValueRef {
                        blob_ref: BlobRef(value),
                        layer: self,
                    };
                    let pos = BlobRef(value).pos();
                    if let Some(last) = all_keys.last_mut() {
                        // subtract offset of the current and last entries to get the size
                        // of the value associated with this (key, lsn) tuple
                        let first_pos = last.size;
                        last.size = pos - first_pos;
                    }
                    let entry = DeltaEntry {
                        key: delta_key.key(),
                        lsn: delta_key.lsn(),
                        size: pos,
                        val: val_ref,
                    };
                    all_keys.push(entry);
                    true
                },
                &RequestContextBuilder::extend(ctx)
                    .page_content_kind(PageContentKind::DeltaLayerBtreeNode)
                    .build(),
            )
            .await?;
        if let Some(last) = all_keys.last_mut() {
            // Last key occupies all space till end of value storage,
            // which corresponds to beginning of the index
            last.size = self.index_start_offset() - last.size;
        }
        Ok(all_keys)
    }

    /// Using the given writer, write out a version which has the earlier Lsns than `until`.
    ///
    /// Return the amount of key value records pushed to the writer.
    pub(super) async fn copy_prefix(
        &self,
        writer: &mut DeltaLayerWriter,
        until: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        use crate::tenant::vectored_blob_io::{
            BlobMeta, ChunkedVectoredReadBuilder, VectoredReadExtended,
        };
        use futures::stream::TryStreamExt;

        #[derive(Debug)]
        enum Item {
            Actual(Key, Lsn, BlobRef),
            Sentinel,
        }

        impl From<Item> for Option<(Key, Lsn, BlobRef)> {
            fn from(value: Item) -> Self {
                match value {
                    Item::Actual(key, lsn, blob) => Some((key, lsn, blob)),
                    Item::Sentinel => None,
                }
            }
        }

        impl Item {
            fn offset(&self) -> Option<BlobRef> {
                match self {
                    Item::Actual(_, _, blob) => Some(*blob),
                    Item::Sentinel => None,
                }
            }

            fn is_last(&self) -> bool {
                matches!(self, Item::Sentinel)
            }
        }

        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        let stream = self.stream_index_forwards(tree_reader, &[0u8; DELTA_KEY_SIZE], ctx);
        let stream = stream.map_ok(|(key, lsn, pos)| Item::Actual(key, lsn, pos));
        // put in a sentinel value for getting the end offset for last item, and not having to
        // repeat the whole read part
        let stream = stream.chain(futures::stream::once(futures::future::ready(Ok(
            Item::Sentinel,
        ))));
        let mut stream = std::pin::pin!(stream);

        let mut prev: Option<(Key, Lsn, BlobRef)> = None;

        let mut read_builder: Option<ChunkedVectoredReadBuilder> = None;

        let max_read_size = self
            .max_vectored_read_bytes
            .map(|x| x.0.get())
            .unwrap_or(8192);

        let mut buffer = Some(IoBufferMut::with_capacity(max_read_size));

        // FIXME: buffering of DeltaLayerWriter
        let mut per_blob_copy = Vec::new();

        let mut records = 0;

        while let Some(item) = stream.try_next().await? {
            tracing::debug!(?item, "popped");
            let offset = item
                .offset()
                .unwrap_or(BlobRef::new(self.index_start_offset(), false));

            let actionable = if let Some((key, lsn, start_offset)) = prev.take() {
                let end_offset = offset;

                Some((BlobMeta { key, lsn }, start_offset..end_offset))
            } else {
                None
            };

            let is_last = item.is_last();

            prev = Option::from(item);

            let actionable = actionable.filter(|x| x.0.lsn < until);

            let builder = if let Some((meta, offsets)) = actionable {
                // extend or create a new builder
                if read_builder
                    .as_mut()
                    .map(|x| x.extend(offsets.start.pos(), offsets.end.pos(), meta))
                    .unwrap_or(VectoredReadExtended::No)
                    == VectoredReadExtended::Yes
                {
                    None
                } else {
                    read_builder.replace(ChunkedVectoredReadBuilder::new(
                        offsets.start.pos(),
                        offsets.end.pos(),
                        meta,
                        max_read_size,
                    ))
                }
            } else {
                // nothing to do, except perhaps flush any existing for the last element
                None
            };

            // flush the possible older builder and also the new one if the item was the last one
            let builders = builder.into_iter();
            let builders = if is_last {
                builders.chain(read_builder.take())
            } else {
                builders.chain(None)
            };

            for builder in builders {
                let read = builder.build();

                let reader = VectoredBlobReader::new(&self.file);

                let mut buf = buffer.take().unwrap();

                buf.clear();
                buf.reserve(read.size());
                let res = reader.read_blobs(&read, buf, ctx).await?;

                let view = BufView::new_slice(&res.buf);

                for blob in res.blobs {
                    let key = blob.meta.key;
                    let lsn = blob.meta.lsn;

                    let data = blob.read(&view).await?;

                    #[cfg(debug_assertions)]
                    Value::des(&data)
                        .with_context(|| {
                            format!(
                                "blob failed to deserialize for {}: {:?}",
                                blob,
                                utils::Hex(&data)
                            )
                        })
                        .unwrap();

                    // is it an image or will_init walrecord?
                    // FIXME: this could be handled by threading the BlobRef to the
                    // VectoredReadBuilder
                    let will_init = pageserver_api::value::ValueBytes::will_init(&data)
                        .inspect_err(|_e| {
                            #[cfg(feature = "testing")]
                            tracing::error!(data=?utils::Hex(&data), err=?_e, %key, %lsn, "failed to parse will_init out of serialized value");
                        })
                        .unwrap_or(false);

                    per_blob_copy.clear();
                    per_blob_copy.extend_from_slice(&data);

                    let (tmp, res) = writer
                        .put_value_bytes(
                            key,
                            lsn,
                            std::mem::take(&mut per_blob_copy).slice_len(),
                            will_init,
                            ctx,
                        )
                        .await;
                    per_blob_copy = tmp.into_raw_slice().into_inner();

                    res?;

                    records += 1;
                }

                buffer = Some(res.buf);
            }
        }

        assert!(
            read_builder.is_none(),
            "with the sentinel above loop should had handled all"
        );

        Ok(records)
    }

    pub(super) async fn dump(&self, ctx: &RequestContext) -> anyhow::Result<()> {
        println!(
            "index_start_blk: {}, root {}",
            self.index_start_blk, self.index_root_blk
        );

        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            self.index_start_blk,
            self.index_root_blk,
            block_reader,
        );

        tree_reader.dump().await?;

        let keys = self.index_entries(ctx).await?;

        async fn dump_blob(val: &ValueRef<'_>, ctx: &RequestContext) -> anyhow::Result<String> {
            let buf = val.load_raw(ctx).await?;
            let val = Value::des(&buf)?;
            let desc = match val {
                Value::Image(img) => {
                    format!(" img {} bytes", img.len())
                }
                Value::WalRecord(rec) => {
                    let wal_desc = pageserver_api::record::describe_wal_record(&rec)?;
                    format!(
                        " rec {} bytes will_init: {} {}",
                        buf.len(),
                        rec.will_init(),
                        wal_desc
                    )
                }
            };
            Ok(desc)
        }

        for entry in keys {
            let DeltaEntry { key, lsn, val, .. } = entry;
            let desc = match dump_blob(&val, ctx).await {
                Ok(desc) => desc,
                Err(err) => {
                    format!("ERROR: {err}")
                }
            };
            println!("  key {key} at {lsn}: {desc}");

            // Print more details about CHECKPOINT records. Would be nice to print details
            // of many other record types too, but these are particularly interesting, as
            // have a lot of special processing for them in walingest.rs.
            use pageserver_api::key::CHECKPOINT_KEY;
            use postgres_ffi::CheckPoint;
            if key == CHECKPOINT_KEY {
                let val = val.load(ctx).await?;
                match val {
                    Value::Image(img) => {
                        let checkpoint = CheckPoint::decode(&img)?;
                        println!("   CHECKPOINT: {:?}", checkpoint);
                    }
                    Value::WalRecord(_rec) => {
                        println!("   unexpected walrecord value for checkpoint key");
                    }
                }
            }
        }

        Ok(())
    }

    fn stream_index_forwards<'a, R>(
        &'a self,
        reader: DiskBtreeReader<R, DELTA_KEY_SIZE>,
        start: &'a [u8; DELTA_KEY_SIZE],
        ctx: &'a RequestContext,
    ) -> impl futures::stream::Stream<
        Item = Result<(Key, Lsn, BlobRef), crate::tenant::disk_btree::DiskBtreeError>,
    > + 'a
    where
        R: BlockReader + 'a,
    {
        use futures::stream::TryStreamExt;
        let stream = reader.into_stream(start, ctx);
        stream.map_ok(|(key, value)| {
            let key = DeltaKey::from_slice(&key);
            let (key, lsn) = (key.key(), key.lsn());
            let offset = BlobRef(value);

            (key, lsn, offset)
        })
    }

    /// The file offset to the first block of index.
    ///
    /// The file structure is summary, values, and index. We often need this for the size of last blob.
    fn index_start_offset(&self) -> u64 {
        let offset = self.index_start_blk as u64 * PAGE_SZ as u64;
        let bref = BlobRef(offset);
        tracing::debug!(
            index_start_blk = self.index_start_blk,
            offset,
            pos = bref.pos(),
            "index_start_offset"
        );
        offset
    }

    pub fn iter<'a>(&'a self, ctx: &'a RequestContext) -> DeltaLayerIterator<'a> {
        let block_reader = FileBlockReader::new(&self.file, self.file_id);
        let tree_reader =
            DiskBtreeReader::new(self.index_start_blk, self.index_root_blk, block_reader);
        DeltaLayerIterator {
            delta_layer: self,
            ctx,
            index_iter: tree_reader.iter(&[0; DELTA_KEY_SIZE], ctx),
            key_values_batch: std::collections::VecDeque::new(),
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
        self.index_entries(ctx)
            .await
            .map(|entries| entries.into_iter().map(|entry| entry.key).collect())
    }
}

/// A set of data associated with a delta layer key and its value
pub struct DeltaEntry<'a> {
    pub key: Key,
    pub lsn: Lsn,
    /// Size of the stored value
    pub size: u64,
    /// Reference to the on-disk value
    pub val: ValueRef<'a>,
}

/// Reference to an on-disk value
pub struct ValueRef<'a> {
    blob_ref: BlobRef,
    layer: &'a DeltaLayerInner,
}

impl ValueRef<'_> {
    /// Loads the value from disk
    pub async fn load(&self, ctx: &RequestContext) -> Result<Value> {
        let buf = self.load_raw(ctx).await?;
        let val = Value::des(&buf)?;
        Ok(val)
    }

    async fn load_raw(&self, ctx: &RequestContext) -> Result<Vec<u8>> {
        let reader = BlockCursor::new(crate::tenant::block_io::BlockReaderRef::Adapter(Adapter(
            self.layer,
        )));
        let buf = reader.read_blob(self.blob_ref.pos(), ctx).await?;
        Ok(buf)
    }
}

pub(crate) struct Adapter<T>(T);

impl<T: AsRef<DeltaLayerInner>> Adapter<T> {
    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, std::io::Error> {
        let block_reader = FileBlockReader::new(&self.0.as_ref().file, self.0.as_ref().file_id);
        block_reader.read_blk(blknum, ctx).await
    }
}

impl AsRef<DeltaLayerInner> for DeltaLayerInner {
    fn as_ref(&self) -> &DeltaLayerInner {
        self
    }
}

impl<'a> pageserver_compaction::interface::CompactionDeltaEntry<'a, Key> for DeltaEntry<'a> {
    fn key(&self) -> Key {
        self.key
    }
    fn lsn(&self) -> Lsn {
        self.lsn
    }
    fn size(&self) -> u64 {
        self.size
    }
}

pub struct DeltaLayerIterator<'a> {
    delta_layer: &'a DeltaLayerInner,
    ctx: &'a RequestContext,
    planner: StreamingVectoredReadPlanner,
    index_iter: DiskBtreeIterator<'a>,
    key_values_batch: VecDeque<(Key, Lsn, Value)>,
    is_end: bool,
}

impl DeltaLayerIterator<'_> {
    pub(crate) fn layer_dbg_info(&self) -> String {
        self.delta_layer.layer_dbg_info()
    }

    /// Retrieve a batch of key-value pairs into the iterator buffer.
    async fn next_batch(&mut self) -> anyhow::Result<()> {
        assert!(self.key_values_batch.is_empty());
        assert!(!self.is_end);

        let plan = loop {
            if let Some(res) = self.index_iter.next().await {
                let (raw_key, value) = res?;
                let key = Key::from_slice(&raw_key[..KEY_SIZE]);
                let lsn = DeltaKey::extract_lsn_from_buf(&raw_key);
                let blob_ref = BlobRef(value);
                let offset = blob_ref.pos();
                if let Some(batch_plan) = self.planner.handle(key, lsn, offset) {
                    break batch_plan;
                }
            } else {
                self.is_end = true;
                let data_end_offset = self.delta_layer.index_start_offset();
                if let Some(item) = self.planner.handle_range_end(data_end_offset) {
                    break item;
                } else {
                    return Ok(()); // TODO: test empty iterator
                }
            }
        };
        let vectored_blob_reader = VectoredBlobReader::new(&self.delta_layer.file);
        let mut next_batch = std::collections::VecDeque::new();
        let buf_size = plan.size();
        let buf = IoBufferMut::with_capacity(buf_size);
        let blobs_buf = vectored_blob_reader
            .read_blobs(&plan, buf, self.ctx)
            .await?;
        let view = BufView::new_slice(&blobs_buf.buf);
        for meta in blobs_buf.blobs.iter() {
            let blob_read = meta.read(&view).await?;
            let value = Value::des(&blob_read)?;

            next_batch.push_back((meta.meta.key, meta.meta.lsn, value));
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
pub(crate) mod test {
    use std::collections::BTreeMap;

    use itertools::MinMaxResult;
    use rand::prelude::{SeedableRng, SliceRandom, StdRng};
    use rand::RngCore;

    use super::*;
    use crate::tenant::harness::TIMELINE_ID;
    use crate::tenant::storage_layer::{Layer, ResidentLayer};
    use crate::tenant::vectored_blob_io::StreamingVectoredReadPlanner;
    use crate::tenant::{Tenant, Timeline};
    use crate::{
        context::DownloadBehavior,
        task_mgr::TaskKind,
        tenant::{disk_btree::tests::TestDisk, harness::TenantHarness},
        DEFAULT_PG_VERSION,
    };
    use bytes::Bytes;
    use pageserver_api::value::Value;

    /// Construct an index for a fictional delta layer and and then
    /// traverse in order to plan vectored reads for a query. Finally,
    /// verify that the traversal fed the right index key and value
    /// pairs into the planner.
    #[tokio::test]
    async fn test_delta_layer_index_traversal() {
        let base_key = Key {
            field1: 0,
            field2: 1663,
            field3: 12972,
            field4: 16396,
            field5: 0,
            field6: 246080,
        };

        // Populate the index with some entries
        let entries: BTreeMap<Key, Vec<Lsn>> = BTreeMap::from([
            (base_key, vec![Lsn(1), Lsn(5), Lsn(25), Lsn(26), Lsn(28)]),
            (base_key.add(1), vec![Lsn(2), Lsn(5), Lsn(10), Lsn(50)]),
            (base_key.add(2), vec![Lsn(2), Lsn(5), Lsn(10), Lsn(50)]),
            (base_key.add(5), vec![Lsn(10), Lsn(15), Lsn(16), Lsn(20)]),
        ]);

        let mut disk = TestDisk::default();
        let mut writer = DiskBtreeBuilder::<_, DELTA_KEY_SIZE>::new(&mut disk);

        let mut disk_offset = 0;
        for (key, lsns) in &entries {
            for lsn in lsns {
                let index_key = DeltaKey::from_key_lsn(key, *lsn);
                let blob_ref = BlobRef::new(disk_offset, false);
                writer
                    .append(&index_key.0, blob_ref.0)
                    .expect("In memory disk append should never fail");

                disk_offset += 1;
            }
        }

        // Prepare all the arguments for the call into `plan_reads` below
        let (root_offset, _writer) = writer
            .finish()
            .expect("In memory disk finish should never fail");
        let reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(0, root_offset, disk);
        let planner = VectoredReadPlanner::new(100);
        let mut reconstruct_state = ValuesReconstructState::new();
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        let keyspace = KeySpace {
            ranges: vec![
                base_key..base_key.add(3),
                base_key.add(3)..base_key.add(100),
            ],
        };
        let lsn_range = Lsn(2)..Lsn(40);

        // Plan and validate
        let vectored_reads = DeltaLayerInner::plan_reads(
            &keyspace,
            lsn_range.clone(),
            disk_offset,
            reader,
            planner,
            &mut reconstruct_state,
            &ctx,
        )
        .await
        .expect("Read planning should not fail");

        validate(keyspace, lsn_range, vectored_reads, entries);
    }

    fn validate(
        keyspace: KeySpace,
        lsn_range: Range<Lsn>,
        vectored_reads: Vec<VectoredRead>,
        index_entries: BTreeMap<Key, Vec<Lsn>>,
    ) {
        #[derive(Debug, PartialEq, Eq)]
        struct BlobSpec {
            key: Key,
            lsn: Lsn,
            at: u64,
        }

        let mut planned_blobs = Vec::new();
        for read in vectored_reads {
            for (at, meta) in read.blobs_at.as_slice() {
                planned_blobs.push(BlobSpec {
                    key: meta.key,
                    lsn: meta.lsn,
                    at: *at,
                });
            }
        }

        let mut expected_blobs = Vec::new();
        let mut disk_offset = 0;
        for (key, lsns) in index_entries {
            for lsn in lsns {
                let key_included = keyspace.ranges.iter().any(|range| range.contains(&key));
                let lsn_included = lsn_range.contains(&lsn);

                if key_included && lsn_included {
                    expected_blobs.push(BlobSpec {
                        key,
                        lsn,
                        at: disk_offset,
                    });
                }

                disk_offset += 1;
            }
        }

        assert_eq!(planned_blobs, expected_blobs);
    }

    mod constants {
        use utils::lsn::Lsn;

        /// Offset used by all lsns in this test
        pub(super) const LSN_OFFSET: Lsn = Lsn(0x08);
        /// Number of unique keys including in the test data
        pub(super) const KEY_COUNT: u8 = 60;
        /// Max number of different lsns for each key
        pub(super) const MAX_ENTRIES_PER_KEY: u8 = 20;
        /// Possible value sizes for each key along with a probability weight
        pub(super) const VALUE_SIZES: [(usize, u8); 3] = [(100, 2), (1024, 2), (1024 * 1024, 1)];
        /// Probability that there will be a gap between the current key and the next one (33.3%)
        pub(super) const KEY_GAP_CHANGES: [(bool, u8); 2] = [(true, 1), (false, 2)];
        /// The minimum size of a key range in all the generated reads
        pub(super) const MIN_RANGE_SIZE: i128 = 10;
        /// The number of ranges included in each vectored read
        pub(super) const RANGES_COUNT: u8 = 2;
        /// The number of vectored reads performed
        pub(super) const READS_COUNT: u8 = 100;
        /// Soft max size of a vectored read. Will be violated if we have to read keys
        /// with values larger than the limit
        pub(super) const MAX_VECTORED_READ_BYTES: usize = 64 * 1024;
    }

    struct Entry {
        key: Key,
        lsn: Lsn,
        value: Vec<u8>,
    }

    fn generate_entries(rng: &mut StdRng) -> Vec<Entry> {
        let mut current_key = Key::MIN;

        let mut entries = Vec::new();
        for _ in 0..constants::KEY_COUNT {
            let count = rng.gen_range(1..constants::MAX_ENTRIES_PER_KEY);
            let mut lsns_iter =
                std::iter::successors(Some(Lsn(constants::LSN_OFFSET.0 + 0x08)), |lsn| {
                    Some(Lsn(lsn.0 + 0x08))
                });
            let mut lsns = Vec::new();
            while lsns.len() < count as usize {
                let take = rng.gen_bool(0.5);
                let lsn = lsns_iter.next().unwrap();
                if take {
                    lsns.push(lsn);
                }
            }

            for lsn in lsns {
                let size = constants::VALUE_SIZES
                    .choose_weighted(rng, |item| item.1)
                    .unwrap()
                    .0;
                let mut buf = vec![0; size];
                rng.fill_bytes(&mut buf);

                entries.push(Entry {
                    key: current_key,
                    lsn,
                    value: buf,
                })
            }

            let gap = constants::KEY_GAP_CHANGES
                .choose_weighted(rng, |item| item.1)
                .unwrap()
                .0;
            if gap {
                current_key = current_key.add(2);
            } else {
                current_key = current_key.add(1);
            }
        }

        entries
    }

    struct EntriesMeta {
        key_range: Range<Key>,
        lsn_range: Range<Lsn>,
        index: BTreeMap<(Key, Lsn), Vec<u8>>,
    }

    fn get_entries_meta(entries: &[Entry]) -> EntriesMeta {
        let key_range = match entries.iter().minmax_by_key(|e| e.key) {
            MinMaxResult::MinMax(min, max) => min.key..max.key.next(),
            _ => panic!("More than one entry is always expected"),
        };

        let lsn_range = match entries.iter().minmax_by_key(|e| e.lsn) {
            MinMaxResult::MinMax(min, max) => min.lsn..Lsn(max.lsn.0 + 1),
            _ => panic!("More than one entry is always expected"),
        };

        let mut index = BTreeMap::new();
        for entry in entries.iter() {
            index.insert((entry.key, entry.lsn), entry.value.clone());
        }

        EntriesMeta {
            key_range,
            lsn_range,
            index,
        }
    }

    fn pick_random_keyspace(rng: &mut StdRng, key_range: &Range<Key>) -> KeySpace {
        let start = key_range.start.to_i128();
        let end = key_range.end.to_i128();

        let mut keyspace = KeySpace::default();

        for _ in 0..constants::RANGES_COUNT {
            let mut range: Option<Range<Key>> = Option::default();
            while range.is_none() || keyspace.overlaps(range.as_ref().unwrap()) {
                let range_start = rng.gen_range(start..end);
                let range_end_offset = range_start + constants::MIN_RANGE_SIZE;
                if range_end_offset >= end {
                    range = Some(Key::from_i128(range_start)..Key::from_i128(end));
                } else {
                    let range_end = rng.gen_range((range_start + constants::MIN_RANGE_SIZE)..end);
                    range = Some(Key::from_i128(range_start)..Key::from_i128(range_end));
                }
            }
            keyspace.ranges.push(range.unwrap());
        }

        keyspace
    }

    #[tokio::test]
    async fn test_delta_layer_vectored_read_end_to_end() -> anyhow::Result<()> {
        let harness = TenantHarness::create("test_delta_layer_oversized_vectored_read").await?;
        let (tenant, ctx) = harness.load().await;

        let timeline_id = TimelineId::generate();
        let timeline = tenant
            .create_test_timeline(timeline_id, constants::LSN_OFFSET, DEFAULT_PG_VERSION, &ctx)
            .await?;

        tracing::info!("Generating test data ...");

        let rng = &mut StdRng::seed_from_u64(0);
        let entries = generate_entries(rng);
        let entries_meta = get_entries_meta(&entries);

        tracing::info!("Done generating {} entries", entries.len());

        tracing::info!("Writing test data to delta layer ...");
        let mut writer = DeltaLayerWriter::new(
            harness.conf,
            timeline_id,
            harness.tenant_shard_id,
            entries_meta.key_range.start,
            entries_meta.lsn_range.clone(),
            &ctx,
        )
        .await?;

        for entry in entries {
            let (_, res) = writer
                .put_value_bytes(entry.key, entry.lsn, entry.value.slice_len(), false, &ctx)
                .await;
            res?;
        }

        let (desc, path) = writer.finish(entries_meta.key_range.end, &ctx).await?;
        let resident = Layer::finish_creating(harness.conf, &timeline, desc, &path)?;

        let inner = resident.get_as_delta(&ctx).await?;

        let file_size = inner.file.metadata().await?.len();
        tracing::info!(
            "Done writing test data to delta layer. Resulting file size is: {}",
            file_size
        );

        for i in 0..constants::READS_COUNT {
            tracing::info!("Doing vectored read {}/{}", i + 1, constants::READS_COUNT);

            let block_reader = FileBlockReader::new(&inner.file, inner.file_id);
            let index_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
                inner.index_start_blk,
                inner.index_root_blk,
                block_reader,
            );

            let planner = VectoredReadPlanner::new(constants::MAX_VECTORED_READ_BYTES);
            let mut reconstruct_state = ValuesReconstructState::new();
            let keyspace = pick_random_keyspace(rng, &entries_meta.key_range);
            let data_end_offset = inner.index_start_blk as u64 * PAGE_SZ as u64;

            let vectored_reads = DeltaLayerInner::plan_reads(
                &keyspace,
                entries_meta.lsn_range.clone(),
                data_end_offset,
                index_reader,
                planner,
                &mut reconstruct_state,
                &ctx,
            )
            .await?;

            let vectored_blob_reader = VectoredBlobReader::new(&inner.file);
            let buf_size = DeltaLayerInner::get_min_read_buffer_size(
                &vectored_reads,
                constants::MAX_VECTORED_READ_BYTES,
            );
            let mut buf = Some(IoBufferMut::with_capacity(buf_size));

            for read in vectored_reads {
                let blobs_buf = vectored_blob_reader
                    .read_blobs(&read, buf.take().expect("Should have a buffer"), &ctx)
                    .await?;
                let view = BufView::new_slice(&blobs_buf.buf);
                for meta in blobs_buf.blobs.iter() {
                    let value = meta.read(&view).await?;
                    assert_eq!(
                        &value[..],
                        &entries_meta.index[&(meta.meta.key, meta.meta.lsn)]
                    );
                }

                buf = Some(blobs_buf.buf);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn copy_delta_prefix_smoke() {
        use bytes::Bytes;
        use pageserver_api::record::NeonWalRecord;

        let h = crate::tenant::harness::TenantHarness::create("truncate_delta_smoke")
            .await
            .unwrap();
        let (tenant, ctx) = h.load().await;
        let ctx = &ctx;
        let timeline = tenant
            .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, ctx)
            .await
            .unwrap();

        let initdb_layer = timeline
            .layers
            .read()
            .await
            .likely_resident_layers()
            .next()
            .cloned()
            .unwrap();

        {
            let mut writer = timeline.writer().await;

            let data = [
                (0x20, 12, Value::Image(Bytes::from_static(b"foobar"))),
                (
                    0x30,
                    12,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: false,
                        rec: Bytes::from_static(b"1"),
                    }),
                ),
                (
                    0x40,
                    12,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: true,
                        rec: Bytes::from_static(b"2"),
                    }),
                ),
                // build an oversized value so we cannot extend and existing read over
                // this
                (
                    0x50,
                    12,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: true,
                        rec: {
                            let mut buf =
                                vec![0u8; tenant.conf.max_vectored_read_bytes.0.get() + 1024];
                            buf.iter_mut()
                                .enumerate()
                                .for_each(|(i, slot)| *slot = (i % 256) as u8);
                            Bytes::from(buf)
                        },
                    }),
                ),
                // because the oversized read cannot be extended further, we are sure to exercise the
                // builder created on the last round with this:
                (
                    0x60,
                    12,
                    Value::WalRecord(NeonWalRecord::Postgres {
                        will_init: true,
                        rec: Bytes::from_static(b"3"),
                    }),
                ),
                (
                    0x60,
                    9,
                    Value::Image(Bytes::from_static(b"something for a different key")),
                ),
            ];

            let mut last_lsn = None;

            for (lsn, key, value) in data {
                let key = Key::from_i128(key);
                writer.put(key, Lsn(lsn), &value, ctx).await.unwrap();
                last_lsn = Some(lsn);
            }

            writer.finish_write(Lsn(last_lsn.unwrap()));
        }
        timeline.freeze_and_flush().await.unwrap();

        let new_layer = timeline
            .layers
            .read()
            .await
            .likely_resident_layers()
            .find(|&x| x != &initdb_layer)
            .cloned()
            .unwrap();

        // create a copy for the timeline, so we don't overwrite the file
        let branch = tenant
            .branch_timeline_test(&timeline, TimelineId::generate(), None, ctx)
            .await
            .unwrap();

        assert_eq!(branch.get_ancestor_lsn(), Lsn(0x60));

        // truncating at 0x61 gives us a full copy, otherwise just go backwards until there's just
        // a single key

        for truncate_at in [0x61, 0x51, 0x41, 0x31, 0x21] {
            let truncate_at = Lsn(truncate_at);

            let mut writer = DeltaLayerWriter::new(
                tenant.conf,
                branch.timeline_id,
                tenant.tenant_shard_id,
                Key::MIN,
                Lsn(0x11)..truncate_at,
                ctx,
            )
            .await
            .unwrap();

            let new_layer = new_layer.download_and_keep_resident().await.unwrap();

            new_layer
                .copy_delta_prefix(&mut writer, truncate_at, ctx)
                .await
                .unwrap();

            let (desc, path) = writer.finish(Key::MAX, ctx).await.unwrap();
            let copied_layer = Layer::finish_creating(tenant.conf, &branch, desc, &path).unwrap();

            copied_layer.get_as_delta(ctx).await.unwrap();

            assert_keys_and_values_eq(
                new_layer.get_as_delta(ctx).await.unwrap(),
                copied_layer.get_as_delta(ctx).await.unwrap(),
                truncate_at,
                ctx,
            )
            .await;
        }
    }

    async fn assert_keys_and_values_eq(
        source: &DeltaLayerInner,
        truncated: &DeltaLayerInner,
        truncated_at: Lsn,
        ctx: &RequestContext,
    ) {
        use futures::future::ready;
        use futures::stream::TryStreamExt;

        let start_key = [0u8; DELTA_KEY_SIZE];

        let source_reader = FileBlockReader::new(&source.file, source.file_id);
        let source_tree = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            source.index_start_blk,
            source.index_root_blk,
            &source_reader,
        );
        let source_stream = source.stream_index_forwards(source_tree, &start_key, ctx);
        let source_stream = source_stream.filter(|res| match res {
            Ok((_, lsn, _)) => ready(lsn < &truncated_at),
            _ => ready(true),
        });
        let mut source_stream = std::pin::pin!(source_stream);

        let truncated_reader = FileBlockReader::new(&truncated.file, truncated.file_id);
        let truncated_tree = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            truncated.index_start_blk,
            truncated.index_root_blk,
            &truncated_reader,
        );
        let truncated_stream = truncated.stream_index_forwards(truncated_tree, &start_key, ctx);
        let mut truncated_stream = std::pin::pin!(truncated_stream);

        let mut scratch_left = Vec::new();
        let mut scratch_right = Vec::new();

        loop {
            let (src, truncated) = (source_stream.try_next(), truncated_stream.try_next());
            let (src, truncated) = tokio::try_join!(src, truncated).unwrap();

            if src.is_none() {
                assert!(truncated.is_none());
                break;
            }

            let (src, truncated) = (src.unwrap(), truncated.unwrap());

            // because we've filtered the source with Lsn, we should always have the same keys from both.
            assert_eq!(src.0, truncated.0);
            assert_eq!(src.1, truncated.1);

            // if this is needed for something else, just drop this assert.
            assert!(
                src.2.pos() >= truncated.2.pos(),
                "value position should not go backwards {} vs. {}",
                src.2.pos(),
                truncated.2.pos()
            );

            scratch_left.clear();
            let src_cursor = source_reader.block_cursor();
            let left = src_cursor.read_blob_into_buf(src.2.pos(), &mut scratch_left, ctx);
            scratch_right.clear();
            let trunc_cursor = truncated_reader.block_cursor();
            let right = trunc_cursor.read_blob_into_buf(truncated.2.pos(), &mut scratch_right, ctx);

            tokio::try_join!(left, right).unwrap();

            assert_eq!(utils::Hex(&scratch_left), utils::Hex(&scratch_right));
        }
    }

    pub(crate) fn sort_delta(
        (k1, l1, _): &(Key, Lsn, Value),
        (k2, l2, _): &(Key, Lsn, Value),
    ) -> std::cmp::Ordering {
        (k1, l1).cmp(&(k2, l2))
    }

    #[cfg(feature = "testing")]
    pub(crate) fn sort_delta_value(
        (k1, l1, v1): &(Key, Lsn, Value),
        (k2, l2, v2): &(Key, Lsn, Value),
    ) -> std::cmp::Ordering {
        let order_1 = if v1.is_image() { 0 } else { 1 };
        let order_2 = if v2.is_image() { 0 } else { 1 };
        (k1, l1, order_1).cmp(&(k2, l2, order_2))
    }

    pub(crate) async fn produce_delta_layer(
        tenant: &Tenant,
        tline: &Arc<Timeline>,
        mut deltas: Vec<(Key, Lsn, Value)>,
        ctx: &RequestContext,
    ) -> anyhow::Result<ResidentLayer> {
        deltas.sort_by(sort_delta);
        let (key_start, _, _) = deltas.first().unwrap();
        let (key_max, _, _) = deltas.last().unwrap();
        let lsn_min = deltas.iter().map(|(_, lsn, _)| lsn).min().unwrap();
        let lsn_max = deltas.iter().map(|(_, lsn, _)| lsn).max().unwrap();
        let lsn_end = Lsn(lsn_max.0 + 1);
        let mut writer = DeltaLayerWriter::new(
            tenant.conf,
            tline.timeline_id,
            tenant.tenant_shard_id,
            *key_start,
            (*lsn_min)..lsn_end,
            ctx,
        )
        .await?;
        let key_end = key_max.next();

        for (key, lsn, value) in deltas {
            writer.put_value(key, lsn, value, ctx).await?;
        }

        let (desc, path) = writer.finish(key_end, ctx).await?;
        let delta_layer = Layer::finish_creating(tenant.conf, tline, desc, &path)?;

        Ok::<_, anyhow::Error>(delta_layer)
    }

    async fn assert_delta_iter_equal(
        delta_iter: &mut DeltaLayerIterator<'_>,
        expect: &[(Key, Lsn, Value)],
    ) {
        let mut expect_iter = expect.iter();
        loop {
            let o1 = delta_iter.next().await.unwrap();
            let o2 = expect_iter.next();
            assert_eq!(o1.is_some(), o2.is_some());
            if o1.is_none() && o2.is_none() {
                break;
            }
            let (k1, l1, v1) = o1.unwrap();
            let (k2, l2, v2) = o2.unwrap();
            assert_eq!(&k1, k2);
            assert_eq!(l1, *l2);
            assert_eq!(&v1, v2);
        }
    }

    #[tokio::test]
    async fn delta_layer_iterator() {
        let harness = TenantHarness::create("delta_layer_iterator").await.unwrap();
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
        let test_deltas = (0..N)
            .map(|idx| {
                (
                    get_key(idx as u32 / 10),
                    Lsn(0x10 * ((idx as u64) % 10 + 1)),
                    Value::Image(Bytes::from(format!("img{idx:05}"))),
                )
            })
            .collect_vec();
        let resident_layer = produce_delta_layer(&tenant, &tline, test_deltas.clone(), &ctx)
            .await
            .unwrap();
        let delta_layer = resident_layer.get_as_delta(&ctx).await.unwrap();
        for max_read_size in [1, 1024] {
            for batch_size in [1, 2, 4, 8, 3, 7, 13] {
                println!("running with batch_size={batch_size} max_read_size={max_read_size}");
                // Test if the batch size is correctly determined
                let mut iter = delta_layer.iter(&ctx);
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
                let mut iter = delta_layer.iter(&ctx);
                iter.planner = StreamingVectoredReadPlanner::new(max_read_size, batch_size);
                assert_delta_iter_equal(&mut iter, &test_deltas).await;
            }
        }
    }
}
