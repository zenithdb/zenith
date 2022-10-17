//! A DeltaLayer represents a collection of WAL records or page images in a range of
//! LSNs, and in a range of Keys. It is stored on a file on disk.
//!
//! Usually a delta layer only contains differences, in the form of WAL records
//! against a base LSN. However, if a relation extended or a whole new relation
//! is created, there would be no base for the new pages. The entries for them
//! must be page images or WAL records with the 'will_init' flag set, so that
//! they can be replayed without referring to an older page version.
//!
//! The delta files are stored in timelines/<timeline_id> directory.  Currently,
//! there are no subdirectories, and each delta file is named like this:
//!
//!    <key start>-<key end>__<start LSN>-<end LSN
//!
//! For example:
//!
//!    000000067F000032BE0000400000000020B6-000000067F000032BE0000400000000030B6__000000578C6B29-0000000057A50051
//!
//! Every delta file consists of three parts: "summary", "index", and
//! "values". The summary is a fixed size header at the beginning of the file,
//! and it contains basic information about the layer, and offsets to the other
//! parts. The "index" is a B-tree, mapping from Key and LSN to an offset in the
//! "values" part.  The actual page images and WAL records are stored in the
//! "values" part.
//!
use crate::config::PageServerConf;
use crate::page_cache::{PageReadGuard, PAGE_SZ};
use crate::repository::{Key, Value, KEY_SIZE};
use crate::tenant::blob_io::{BlobCursor, BlobWriter, WriteBlobWriter};
use crate::tenant::block_io::{BlockBuf, BlockCursor, BlockReader, FileBlockReader};
use crate::tenant::disk_btree::{DiskBtreeBuilder, DiskBtreeReader, VisitDirection};
use crate::tenant::filename::{DeltaFileName, PathOrConf};
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use crate::virtual_file::VirtualFile;
use crate::{walrecord, TEMP_FILE_SUFFIX};
use crate::{DELTA_FILE_MAGIC, STORAGE_FORMAT_VERSION};
use anyhow::{bail, ensure, Context, Result};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufWriter, Write};
use std::io::{Seek, SeekFrom};
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::*;

use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

///
/// Header stored in the beginning of the file
///
/// After this comes the 'values' part, starting on block 1. After that,
/// the 'index' starts at the block indicated by 'index_start_blk'
///
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Summary {
    /// Magic value to identify this as a neon delta file. Always DELTA_FILE_MAGIC.
    magic: u16,
    format_version: u16,

    tenant_id: TenantId,
    timeline_id: TimelineId,
    key_range: Range<Key>,
    lsn_range: Range<Lsn>,

    /// Block number where the 'index' part of the file begins.
    index_start_blk: u32,
    /// Block within the 'index', where the B-tree root page is stored
    index_root_blk: u32,
}

impl From<&DeltaLayer> for Summary {
    fn from(layer: &DeltaLayer) -> Self {
        Self {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,

            tenant_id: layer.tenant_id,
            timeline_id: layer.timeline_id,
            key_range: layer.key_range.clone(),
            lsn_range: layer.lsn_range.clone(),

            index_start_blk: 0,
            index_root_blk: 0,
        }
    }
}

// Flag indicating that this version initialize the page
const WILL_INIT: u64 = 1;

// Flag indicating page image
const IS_IMAGE: u64 = 2;

///
/// Struct representing reference to BLOB in layers. Reference contains BLOB
/// offset, and for WAL records it also contains `will_init` flag. The flag
/// helps to determine the range of records that needs to be applied, without
/// reading/deserializing records themselves.
///
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct BlobRef(u64);

impl BlobRef {
    pub fn will_init(&self) -> bool {
        (self.0 & WILL_INIT) != 0
    }

    pub fn is_image(&self) -> bool {
        (self.0 & IS_IMAGE) != 0
    }

    pub fn pos(&self) -> u64 {
        self.0 >> 2
    }

    pub fn new(pos: u64, will_init: bool, is_image: bool) -> BlobRef {
        let mut blob_ref = pos << 2;
        if will_init {
            blob_ref |= WILL_INIT;
        }
        if is_image {
            blob_ref |= IS_IMAGE;
        }
        BlobRef(blob_ref)
    }
}

const DELTA_KEY_SIZE: usize = KEY_SIZE + 8;
struct DeltaKey([u8; DELTA_KEY_SIZE]);

///
/// This is the key of the B-tree index stored in the delta layer. It consists
/// of the serialized representation of a Key and LSN.
///
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

    fn extract_key_from_buf(buf: &[u8]) -> Key {
        Key::from_slice(&buf[..KEY_SIZE])
    }

    fn extract_lsn_from_buf(buf: &[u8]) -> Lsn {
        let mut lsn_buf = [0u8; 8];
        lsn_buf.copy_from_slice(&buf[KEY_SIZE..]);
        Lsn(u64::from_be_bytes(lsn_buf))
    }
}

///
/// DeltaLayer is the in-memory data structure associated with an
/// on-disk delta file.  We keep a DeltaLayer in memory for each
/// file, in the LayerMap. If a layer is in "loaded" state, we have a
/// copy of the index in memory, in 'inner'. Otherwise the struct is
/// just a placeholder for a file that exists on disk, and it needs to
/// be loaded before using it in queries.
///
pub struct DeltaLayer {
    path_or_conf: PathOrConf,

    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,

    inner: RwLock<DeltaLayerInner>,
}

pub struct DeltaLayerInner {
    /// If false, the fields below have not been loaded into memory yet.
    loaded: bool,

    // values copied from summary
    index_start_blk: u32,
    index_root_blk: u32,

    /// Reader object for reading blocks from the file. (None if not loaded yet)
    file: Option<FileBlockReader<VirtualFile>>,
}

impl Layer for DeltaLayer {
    fn get_tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn get_timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

    fn get_key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        self.lsn_range.clone()
    }

    fn filename(&self) -> PathBuf {
        PathBuf::from(self.layer_name().to_string())
    }

    fn local_path(&self) -> Option<PathBuf> {
        Some(self.path())
    }

    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> anyhow::Result<ValueReconstructResult> {
        ensure!(lsn_range.start >= self.lsn_range.start);
        let mut need_image = true;

        ensure!(self.key_range.contains(&key));

        {
            // Open the file and lock the metadata in memory
            let inner = self.load()?;

            // Scan the page versions backwards, starting from `lsn`.
            let file = inner.file.as_ref().unwrap();
            let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
                inner.index_start_blk,
                inner.index_root_blk,
                file,
            );
            let search_key = DeltaKey::from_key_lsn(&key, Lsn(lsn_range.end.0 - 1));

            let mut offsets: Vec<(Lsn, u64)> = Vec::new();

            tree_reader.visit(&search_key.0, VisitDirection::Backwards, |key, value| {
                let blob_ref = BlobRef(value);
                if key[..KEY_SIZE] != search_key.0[..KEY_SIZE] {
                    return false;
                }
                let entry_lsn = DeltaKey::extract_lsn_from_buf(key);
                if entry_lsn < lsn_range.start {
                    return false;
                }
                offsets.push((entry_lsn, blob_ref.pos()));

                !blob_ref.will_init()
            })?;

            // Ok, 'offsets' now contains the offsets of all the entries we need to read
            let mut cursor = file.block_cursor();
            for (entry_lsn, pos) in offsets {
                let buf = cursor.read_blob(pos).with_context(|| {
                    format!(
                        "Failed to read blob from virtual file {}",
                        file.file.path.display()
                    )
                })?;
                let val = Value::des(&buf).with_context(|| {
                    format!(
                        "Failed to deserialize file blob from virtual file {}",
                        file.file.path.display()
                    )
                })?;
                match val {
                    Value::Image(img) => {
                        reconstruct_state.img = Some((entry_lsn, img));
                        need_image = false;
                        break;
                    }
                    Value::WalRecord(rec) => {
                        let will_init = rec.will_init();
                        reconstruct_state.records.push((entry_lsn, rec));
                        if will_init {
                            // This WAL record initializes the page, so no need to go further back
                            need_image = false;
                            break;
                        }
                    }
                }
            }
            // release metadata lock and close the file
        }

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(ValueReconstructResult::Continue)
        } else {
            Ok(ValueReconstructResult::Complete)
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = anyhow::Result<(Key, Lsn, Value)>> + 'a> {
        let inner = match self.load() {
            Ok(inner) => inner,
            Err(e) => panic!("Failed to load a delta layer: {e:?}"),
        };

        match DeltaValueIter::new(inner) {
            Ok(iter) => Box::new(iter),
            Err(err) => Box::new(std::iter::once(Err(err))),
        }
    }

    fn key_iter<'a>(&'a self, skip_images: bool) -> Box<dyn Iterator<Item = (Key, Lsn, u64)> + 'a> {
        let inner = match self.load() {
            Ok(inner) => inner,
            Err(e) => panic!("Failed to load a delta layer: {e:?}"),
        };

        match DeltaKeyIter::new(inner, skip_images) {
            Ok(iter) => Box::new(iter),
            Err(e) => panic!("Layer index is corrupted: {e:?}"),
        }
    }

    fn delete(&self) -> Result<()> {
        // delete underlying file
        fs::remove_file(self.path())?;
        Ok(())
    }

    fn is_incremental(&self) -> bool {
        true
    }

    fn is_in_memory(&self) -> bool {
        false
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self, verbose: bool) -> Result<()> {
        println!(
            "----- delta layer for ten {} tli {} keys {}-{} lsn {}-{} ----",
            self.tenant_id,
            self.timeline_id,
            self.key_range.start,
            self.key_range.end,
            self.lsn_range.start,
            self.lsn_range.end
        );

        if !verbose {
            return Ok(());
        }

        let inner = self.load()?;

        println!(
            "index_start_blk: {}, root {}",
            inner.index_start_blk, inner.index_root_blk
        );

        let file = inner.file.as_ref().unwrap();
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            inner.index_start_blk,
            inner.index_root_blk,
            file,
        );

        tree_reader.dump()?;

        let mut cursor = file.block_cursor();

        // A subroutine to dump a single blob
        let mut dump_blob = |blob_ref: BlobRef| -> anyhow::Result<String> {
            let buf = cursor.read_blob(blob_ref.pos())?;
            let val = Value::des(&buf)?;
            let desc = match val {
                Value::Image(img) => {
                    format!(" img {} bytes", img.len())
                }
                Value::WalRecord(rec) => {
                    let wal_desc = walrecord::describe_wal_record(&rec)?;
                    format!(
                        " rec {} bytes will_init: {} {}",
                        buf.len(),
                        rec.will_init(),
                        wal_desc
                    )
                }
            };
            Ok(desc)
        };

        tree_reader.visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |delta_key, val| {
                let blob_ref = BlobRef(val);
                let key = DeltaKey::extract_key_from_buf(delta_key);
                let lsn = DeltaKey::extract_lsn_from_buf(delta_key);

                let desc = match dump_blob(blob_ref) {
                    Ok(desc) => desc,
                    Err(err) => format!("ERROR: {}", err),
                };
                println!("  key {} at {}: {}", key, lsn, desc);
                true
            },
        )?;

        Ok(())
    }

    fn contains(&self, key: &Key) -> Result<bool> {
        // Open the file and lock the metadata in memory
        let inner = self.load()?;

        // Scan the page versions backwards, starting from `lsn`.
        let file = inner.file.as_ref().unwrap();
        let reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            inner.index_start_blk,
            inner.index_root_blk,
            file,
        );
        let search_key = DeltaKey::from_key_lsn(key, Lsn(0));
        let mut found = false;
        reader.visit(
            &search_key.0,
            VisitDirection::Forwards,
            |delta_key, _val| {
                found = DeltaKey::extract_key_from_buf(delta_key) == *key;
                false
            },
        )?;
        Ok(found)
    }
}

impl DeltaLayer {
    fn path_for(
        path_or_conf: &PathOrConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        fname: &DeltaFileName,
    ) -> PathBuf {
        match path_or_conf {
            PathOrConf::Path(path) => path.clone(),
            PathOrConf::Conf(conf) => conf
                .timeline_path(&timeline_id, &tenant_id)
                .join(fname.to_string()),
        }
    }

    fn temp_path_for(
        conf: &PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        key_start: Key,
        lsn_range: &Range<Lsn>,
    ) -> PathBuf {
        let rand_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        conf.timeline_path(&timeline_id, &tenant_id).join(format!(
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
    fn load(&self) -> Result<RwLockReadGuard<DeltaLayerInner>> {
        loop {
            // Quick exit if already loaded
            let inner = self.inner.read().unwrap();
            if inner.loaded {
                return Ok(inner);
            }

            // Need to open the file and load the metadata. Upgrade our lock to
            // a write lock. (Or rather, release and re-lock in write mode.)
            drop(inner);
            let inner = self.inner.write().unwrap();
            if !inner.loaded {
                self.load_inner(inner).with_context(|| {
                    format!("Failed to load delta layer {}", self.path().display())
                })?;
            } else {
                // Another thread loaded it while we were not holding the lock.
            }

            // We now have the file open and loaded. There's no function to do
            // that in the std library RwLock, so we have to release and re-lock
            // in read mode. (To be precise, the lock guard was moved in the
            // above call to `load_inner`, so it's already been released). And
            // while we do that, another thread could unload again, so we have
            // to re-check and retry if that happens.
        }
    }

    fn load_inner(&self, mut inner: RwLockWriteGuard<DeltaLayerInner>) -> Result<()> {
        let path = self.path();

        // Open the file if it's not open already.
        if inner.file.is_none() {
            let file = VirtualFile::open(&path)
                .with_context(|| format!("Failed to open file '{}'", path.display()))?;
            inner.file = Some(FileBlockReader::new(file));
        }
        let file = inner.file.as_mut().unwrap();
        let summary_blk = file.read_blk(0)?;
        let actual_summary = Summary::des_prefix(summary_blk.as_ref())?;

        match &self.path_or_conf {
            PathOrConf::Conf(_) => {
                let mut expected_summary = Summary::from(self);
                expected_summary.index_start_blk = actual_summary.index_start_blk;
                expected_summary.index_root_blk = actual_summary.index_root_blk;
                if actual_summary != expected_summary {
                    bail!("in-file summary does not match expected summary. actual = {:?} expected = {:?}", actual_summary, expected_summary);
                }
            }
            PathOrConf::Path(path) => {
                let actual_filename = Path::new(path.file_name().unwrap());
                let expected_filename = self.filename();

                if actual_filename != expected_filename {
                    println!(
                        "warning: filename does not match what is expected from in-file summary"
                    );
                    println!("actual: {:?}", actual_filename);
                    println!("expected: {:?}", expected_filename);
                }
            }
        }

        inner.index_start_blk = actual_summary.index_start_blk;
        inner.index_root_blk = actual_summary.index_root_blk;

        debug!("loaded from {}", &path.display());

        inner.loaded = true;
        Ok(())
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    pub fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        filename: &DeltaFileName,
    ) -> DeltaLayer {
        DeltaLayer {
            path_or_conf: PathOrConf::Conf(conf),
            timeline_id,
            tenant_id,
            key_range: filename.key_range.clone(),
            lsn_range: filename.lsn_range.clone(),
            inner: RwLock::new(DeltaLayerInner {
                loaded: false,
                file: None,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        }
    }

    /// Create a DeltaLayer struct representing an existing file on disk.
    ///
    /// This variant is only used for debugging purposes, by the 'pageserver_binutils' binary.
    pub fn new_for_path<F>(path: &Path, file: F) -> Result<Self>
    where
        F: FileExt,
    {
        let mut summary_buf = Vec::new();
        summary_buf.resize(PAGE_SZ, 0);
        file.read_exact_at(&mut summary_buf, 0)?;
        let summary = Summary::des_prefix(&summary_buf)?;

        Ok(DeltaLayer {
            path_or_conf: PathOrConf::Path(path.to_path_buf()),
            timeline_id: summary.timeline_id,
            tenant_id: summary.tenant_id,
            key_range: summary.key_range,
            lsn_range: summary.lsn_range,
            inner: RwLock::new(DeltaLayerInner {
                loaded: false,
                file: None,
                index_start_blk: 0,
                index_root_blk: 0,
            }),
        })
    }

    fn layer_name(&self) -> DeltaFileName {
        DeltaFileName {
            key_range: self.key_range.clone(),
            lsn_range: self.lsn_range.clone(),
        }
    }

    /// Path to the layer file in pageserver workdir.
    pub fn path(&self) -> PathBuf {
        Self::path_for(
            &self.path_or_conf,
            self.timeline_id,
            self.tenant_id,
            &self.layer_name(),
        )
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
pub struct DeltaLayerWriter {
    conf: &'static PageServerConf,
    path: PathBuf,
    timeline_id: TimelineId,
    tenant_id: TenantId,

    key_start: Key,
    lsn_range: Range<Lsn>,

    tree: DiskBtreeBuilder<BlockBuf, DELTA_KEY_SIZE>,

    blob_writer: WriteBlobWriter<BufWriter<VirtualFile>>,
}

impl DeltaLayerWriter {
    ///
    /// Start building a new delta layer.
    ///
    pub fn new(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        key_start: Key,
        lsn_range: Range<Lsn>,
    ) -> Result<DeltaLayerWriter> {
        // Create the file initially with a temporary filename. We don't know
        // the end key yet, so we cannot form the final filename yet. We will
        // rename it when we're done.
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let path = DeltaLayer::temp_path_for(conf, timeline_id, tenant_id, key_start, &lsn_range);

        let mut file = VirtualFile::create(&path)?;
        // make room for the header block
        file.seek(SeekFrom::Start(PAGE_SZ as u64))?;
        let buf_writer = BufWriter::new(file);
        let blob_writer = WriteBlobWriter::new(buf_writer, PAGE_SZ as u64);

        // Initialize the b-tree index builder
        let block_buf = BlockBuf::new();
        let tree_builder = DiskBtreeBuilder::new(block_buf);

        Ok(DeltaLayerWriter {
            conf,
            path,
            timeline_id,
            tenant_id,
            key_start,
            lsn_range,
            tree: tree_builder,
            blob_writer,
        })
    }

    ///
    /// Append a key-value pair to the file.
    ///
    /// The values must be appended in key, lsn order.
    ///
    pub fn put_value(&mut self, key: Key, lsn: Lsn, val: Value) -> Result<()> {
        self.put_value_bytes(
            key,
            lsn,
            &Value::ser(&val)?,
            val.will_init(),
            val.is_image(),
        )
    }

    pub fn put_value_bytes(
        &mut self,
        key: Key,
        lsn: Lsn,
        val: &[u8],
        will_init: bool,
        is_image: bool,
    ) -> Result<()> {
        assert!(self.lsn_range.start <= lsn);

        let off = self.blob_writer.write_blob(val)?;
        let blob_ref = BlobRef::new(off, will_init, is_image);

        let delta_key = DeltaKey::from_key_lsn(&key, lsn);
        self.tree.append(&delta_key.0, blob_ref.0)?;

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.blob_writer.size() + self.tree.borrow_writer().size()
    }

    ///
    /// Finish writing the delta layer.
    ///
    pub fn finish(self, key_end: Key) -> anyhow::Result<DeltaLayer> {
        let index_start_blk =
            ((self.blob_writer.size() + PAGE_SZ as u64 - 1) / PAGE_SZ as u64) as u32;

        let buf_writer = self.blob_writer.into_inner();
        let mut file = buf_writer.into_inner()?;

        // Write out the index
        let (index_root_blk, block_buf) = self.tree.finish()?;
        file.seek(SeekFrom::Start(index_start_blk as u64 * PAGE_SZ as u64))?;
        for buf in block_buf.blocks {
            file.write_all(buf.as_ref())?;
        }
        assert!(self.lsn_range.start < self.lsn_range.end);
        // Fill in the summary on blk 0
        let summary = Summary {
            magic: DELTA_FILE_MAGIC,
            format_version: STORAGE_FORMAT_VERSION,
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            index_start_blk,
            index_root_blk,
        };
        file.seek(SeekFrom::Start(0))?;
        Summary::ser_into(&summary, &mut file)?;

        // Note: Because we opened the file in write-only mode, we cannot
        // reuse the same VirtualFile for reading later. That's why we don't
        // set inner.file here. The first read will have to re-open it.
        let layer = DeltaLayer {
            path_or_conf: PathOrConf::Conf(self.conf),
            tenant_id: self.tenant_id,
            timeline_id: self.timeline_id,
            key_range: self.key_start..key_end,
            lsn_range: self.lsn_range.clone(),
            inner: RwLock::new(DeltaLayerInner {
                loaded: false,
                file: None,
                index_start_blk,
                index_root_blk,
            }),
        };

        // fsync the file
        file.sync_all()?;
        // Rename the file to its final name
        //
        // Note: This overwrites any existing file. There shouldn't be any.
        // FIXME: throw an error instead?
        let final_path = DeltaLayer::path_for(
            &PathOrConf::Conf(self.conf),
            self.timeline_id,
            self.tenant_id,
            &DeltaFileName {
                key_range: self.key_start..key_end,
                lsn_range: self.lsn_range,
            },
        );
        std::fs::rename(self.path, &final_path)?;

        trace!("created delta layer {}", final_path.display());

        Ok(layer)
    }
}

///
/// Iterator over all key-value pairse stored in a delta layer
///
/// FIXME: This creates a Vector to hold the offsets of all key value pairs.
/// That takes up quite a lot of memory. Should do this in a more streaming
/// fashion.
///
struct DeltaValueIter<'a> {
    all_offsets: Vec<(DeltaKey, BlobRef)>,
    next_idx: usize,
    reader: BlockCursor<Adapter<'a>>,
}

struct Adapter<'a>(RwLockReadGuard<'a, DeltaLayerInner>);

impl<'a> BlockReader for Adapter<'a> {
    type BlockLease = PageReadGuard<'static>;

    fn read_blk(&self, blknum: u32) -> Result<Self::BlockLease, std::io::Error> {
        self.0.file.as_ref().unwrap().read_blk(blknum)
    }
}

impl<'a> Iterator for DeltaValueIter<'a> {
    type Item = Result<(Key, Lsn, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_res().transpose()
    }
}

impl<'a> DeltaValueIter<'a> {
    fn new(inner: RwLockReadGuard<'a, DeltaLayerInner>) -> Result<Self> {
        let file = inner.file.as_ref().unwrap();
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            inner.index_start_blk,
            inner.index_root_blk,
            file,
        );

        let mut all_offsets: Vec<(DeltaKey, BlobRef)> = Vec::new();
        tree_reader.visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |key, value| {
                all_offsets.push((DeltaKey::from_slice(key), BlobRef(value)));
                true
            },
        )?;

        let iter = DeltaValueIter {
            all_offsets,
            next_idx: 0,
            reader: BlockCursor::new(Adapter(inner)),
        };

        Ok(iter)
    }

    fn next_res(&mut self) -> Result<Option<(Key, Lsn, Value)>> {
        if self.next_idx < self.all_offsets.len() {
            let (delta_key, blob_ref) = &self.all_offsets[self.next_idx];

            let key = delta_key.key();
            let lsn = delta_key.lsn();

            let buf = self.reader.read_blob(blob_ref.pos())?;
            let val = Value::des(&buf)?;
            self.next_idx += 1;
            Ok(Some((key, lsn, val)))
        } else {
            Ok(None)
        }
    }
}
///
/// Iterator over all keys stored in a delta layer
///
/// FIXME: This creates a Vector to hold all keys.
/// That takes up quite a lot of memory. Should do this in a more streaming
/// fashion.
///
struct DeltaKeyIter {
    all_keys: Vec<(DeltaKey, u64)>,
    next_idx: usize,
}

impl Iterator for DeltaKeyIter {
    type Item = (Key, Lsn, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_idx < self.all_keys.len() {
            let (delta_key, size) = &self.all_keys[self.next_idx];

            let key = delta_key.key();
            let lsn = delta_key.lsn();

            self.next_idx += 1;
            Some((key, lsn, *size))
        } else {
            None
        }
    }
}

impl<'a> DeltaKeyIter {
    fn new(inner: RwLockReadGuard<'a, DeltaLayerInner>, skip_images: bool) -> Result<Self> {
        let file = inner.file.as_ref().unwrap();
        let tree_reader = DiskBtreeReader::<_, DELTA_KEY_SIZE>::new(
            inner.index_start_blk,
            inner.index_root_blk,
            file,
        );

        let mut all_keys: Vec<(DeltaKey, u64)> = Vec::new();
        let mut last_pos = 0u64;
        let mut last_delta: Option<DeltaKey> = None;
        tree_reader.visit(
            &[0u8; DELTA_KEY_SIZE],
            VisitDirection::Forwards,
            |key, value| {
                let blob_ref = BlobRef(value);
                if !blob_ref.is_image() || !skip_images {
                    let next_delta = DeltaKey::from_slice(key);
                    let pos = blob_ref.pos();
                    if let Some(prev_delta) = last_delta.take() {
                        if prev_delta.key() == next_delta.key() {
                            last_delta = Some(next_delta);
                            return true;
                        }
                        all_keys.push((prev_delta, pos - last_pos));
                    }
                    last_delta = Some(next_delta);
                    last_pos = pos;
                }
                true
            },
        )?;
        if let Some(prev_delta) = last_delta.take() {
            // Last key occupies all space till end of layer
            let file_size = std::fs::metadata(&file.file.path)?.len();
            all_keys.push((prev_delta, file_size - last_pos));
        }
        let iter = DeltaKeyIter {
            all_keys,
            next_idx: 0,
        };

        Ok(iter)
    }
}
