//! An in-memory layer stores recently received key-value pairs.
//!
//! The "in-memory" part of the name is a bit misleading: the actual page versions are
//! held in an ephemeral file, not in memory. The metadata for each page version, i.e.
//! its position in the file, is kept in memory, though.
//!
use crate::config::PageServerConf;
use crate::repository::{Key, Value};
use crate::tenant::blob_io::{BlobCursor, BlobWriter};
use crate::tenant::block_io::BlockReader;
use crate::tenant::delta_layer::{DeltaLayer, DeltaLayerWriter};
use crate::tenant::ephemeral_file::EphemeralFile;
use crate::tenant::storage_layer::{Layer, ValueReconstructResult, ValueReconstructState};
use crate::walrecord;
use anyhow::{bail, ensure, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use tracing::*;
use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
    vec_map::VecMap,
};
// avoid binding to Write (conflicts with std::io::Write)
// while being able to use std::fmt::Write's methods
use std::fmt::Write as _;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::RwLock;

thread_local! {
    /// A buffer for serializing object during [`InMemoryLayer::put_value`].
    /// This buffer is reused for each serialization to avoid additional malloc calls.
    static SER_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

pub struct InMemoryLayer {
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    timeline_id: TimelineId,

    ///
    /// This layer contains all the changes from 'start_lsn'. The
    /// start is inclusive.
    ///
    start_lsn: Lsn,

    /// The above fields never change. The parts that do change are in 'inner',
    /// and protected by mutex.
    inner: RwLock<InMemoryLayerInner>,
}

pub struct InMemoryLayerInner {
    /// Frozen layers have an exclusive end LSN.
    /// Writes are only allowed when this is None
    end_lsn: Option<Lsn>,

    ///
    /// All versions of all pages in the layer are kept here.  Indexed
    /// by block number and LSN. The value is an offset into the
    /// ephemeral file where the page version is stored.
    ///
    index: HashMap<Key, VecMap<Lsn, u64>>,

    /// The values are stored in a serialized format in this file.
    /// Each serialized Value is preceded by a 'u32' length field.
    /// PerSeg::page_versions map stores offsets into this file.
    file: EphemeralFile,
}

impl InMemoryLayerInner {
    fn assert_writeable(&self) {
        assert!(self.end_lsn.is_none());
    }
}

impl Layer for InMemoryLayer {
    // An in-memory layer can be spilled to disk into ephemeral file,
    // This function is used only for debugging, so we don't need to be very precise.
    // Construct a filename as if it was a delta layer.
    fn filename(&self) -> PathBuf {
        let inner = self.inner.read().unwrap();

        let end_lsn = inner.end_lsn.unwrap_or(Lsn(u64::MAX));

        PathBuf::from(format!(
            "inmem-{:016X}-{:016X}",
            self.start_lsn.0, end_lsn.0
        ))
    }

    fn local_path(&self) -> Option<PathBuf> {
        None
    }

    fn get_tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    fn get_timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

    fn get_key_range(&self) -> Range<Key> {
        Key::MIN..Key::MAX
    }

    fn get_lsn_range(&self) -> Range<Lsn> {
        let inner = self.inner.read().unwrap();

        let end_lsn = if let Some(end_lsn) = inner.end_lsn {
            end_lsn
        } else {
            Lsn(u64::MAX)
        };
        self.start_lsn..end_lsn
    }

    /// Look up given value in the layer.
    fn get_value_reconstruct_data(
        &self,
        key: Key,
        lsn_range: Range<Lsn>,
        reconstruct_state: &mut ValueReconstructState,
    ) -> anyhow::Result<ValueReconstructResult> {
        ensure!(lsn_range.start >= self.start_lsn);
        let mut need_image = true;

        let inner = self.inner.read().unwrap();

        let mut reader = inner.file.block_cursor();

        // Scan the page versions backwards, starting from `lsn`.
        if let Some(vec_map) = inner.index.get(&key) {
            let slice = vec_map.slice_range(lsn_range);
            for (entry_lsn, pos) in slice.iter().rev() {
                let buf = reader.read_blob(*pos)?;
                let value = Value::des(&buf)?;
                match value {
                    Value::Image(img) => {
                        reconstruct_state.img = Some((*entry_lsn, img));
                        return Ok(ValueReconstructResult::Complete);
                    }
                    Value::WalRecord(rec) => {
                        let will_init = rec.will_init();
                        reconstruct_state.records.push((*entry_lsn, rec));
                        if will_init {
                            // This WAL record initializes the page, so no need to go further back
                            need_image = false;
                            break;
                        }
                    }
                }
            }
        }

        // release lock on 'inner'

        // If an older page image is needed to reconstruct the page, let the
        // caller know.
        if need_image {
            Ok(ValueReconstructResult::Continue)
        } else {
            Ok(ValueReconstructResult::Complete)
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Key, Lsn, Value)>>> {
        todo!();
    }

    /// Nothing to do here. When you drop the last reference to the layer, it will
    /// be deallocated.
    fn delete(&self) -> Result<()> {
        bail!("can't delete an InMemoryLayer")
    }

    fn is_incremental(&self) -> bool {
        // in-memory layer is always considered incremental.
        true
    }

    fn is_in_memory(&self) -> bool {
        true
    }

    /// debugging function to print out the contents of the layer
    fn dump(&self, verbose: bool) -> Result<()> {
        let inner = self.inner.read().unwrap();

        let end_str = inner
            .end_lsn
            .as_ref()
            .map(Lsn::to_string)
            .unwrap_or_default();

        println!(
            "----- in-memory layer for tli {} LSNs {}-{} ----",
            self.timeline_id, self.start_lsn, end_str,
        );

        if !verbose {
            return Ok(());
        }

        let mut cursor = inner.file.block_cursor();
        let mut buf = Vec::new();
        for (key, vec_map) in inner.index.iter() {
            for (lsn, pos) in vec_map.as_slice() {
                let mut desc = String::new();
                cursor.read_blob_into_buf(*pos, &mut buf)?;
                let val = Value::des(&buf);
                match val {
                    Ok(Value::Image(img)) => {
                        write!(&mut desc, " img {} bytes", img.len())?;
                    }
                    Ok(Value::WalRecord(rec)) => {
                        let wal_desc = walrecord::describe_wal_record(&rec).unwrap();
                        write!(
                            &mut desc,
                            " rec {} bytes will_init: {} {}",
                            buf.len(),
                            rec.will_init(),
                            wal_desc
                        )?;
                    }
                    Err(err) => {
                        write!(&mut desc, " DESERIALIZATION ERROR: {}", err)?;
                    }
                }
                println!("  key {} at {}: {}", key, lsn, desc);
            }
        }

        Ok(())
    }

    fn contains(&self, key: &Key) -> Result<bool> {
        let inner = self.inner.read().unwrap();
        Ok(inner.index.get(key).is_some())
    }
}

impl InMemoryLayer {
    ///
    /// Get layer size on the disk
    ///
    pub fn size(&self) -> Result<u64> {
        let inner = self.inner.read().unwrap();
        Ok(inner.file.size)
    }

    ///
    /// Create a new, empty, in-memory layer
    ///
    pub fn create(
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_id: TenantId,
        start_lsn: Lsn,
    ) -> Result<InMemoryLayer> {
        trace!("initializing new empty InMemoryLayer for writing on timeline {timeline_id} at {start_lsn}");

        let file = EphemeralFile::create(conf, tenant_id, timeline_id)?;

        Ok(InMemoryLayer {
            conf,
            timeline_id,
            tenant_id,
            start_lsn,
            inner: RwLock::new(InMemoryLayerInner {
                end_lsn: None,
                index: HashMap::new(),
                file,
            }),
        })
    }

    // Write operations

    /// Common subroutine of the public put_wal_record() and put_page_image() functions.
    /// Adds the page version to the in-memory tree
    pub fn put_value(&self, key: Key, lsn: Lsn, val: &Value) -> Result<()> {
        trace!("put_value key {} at {}/{}", key, self.timeline_id, lsn);
        let mut inner = self.inner.write().unwrap();
        inner.assert_writeable();

        let off = {
            SER_BUFFER.with(|x| -> Result<_> {
                let mut buf = x.borrow_mut();
                buf.clear();
                val.ser_into(&mut (*buf))?;
                let off = inner.file.write_blob(&buf)?;
                Ok(off)
            })?
        };

        let vec_map = inner.index.entry(key).or_default();
        let old = vec_map.append_or_update_last(lsn, off).unwrap().0;
        if old.is_some() {
            // We already had an entry for this LSN. That's odd..
            warn!("Key {} at {} already exists", key, lsn);
        }

        Ok(())
    }

    pub fn put_tombstone(&self, _key_range: Range<Key>, _lsn: Lsn) -> Result<()> {
        // TODO: Currently, we just leak the storage for any deleted keys

        Ok(())
    }

    /// Make the layer non-writeable. Only call once.
    /// Records the end_lsn for non-dropped layers.
    /// `end_lsn` is exclusive
    pub fn freeze(&self, end_lsn: Lsn) {
        let mut inner = self.inner.write().unwrap();

        assert!(self.start_lsn < end_lsn);
        inner.end_lsn = Some(end_lsn);

        for vec_map in inner.index.values() {
            for (lsn, _pos) in vec_map.as_slice() {
                assert!(*lsn < end_lsn);
            }
        }
    }

    /// Write this frozen in-memory layer to disk.
    ///
    /// Returns a new delta layer with all the same data as this in-memory layer
    pub fn write_to_disk(&self) -> Result<DeltaLayer> {
        // Grab the lock in read-mode. We hold it over the I/O, but because this
        // layer is not writeable anymore, no one should be trying to acquire the
        // write lock on it, so we shouldn't block anyone. There's one exception
        // though: another thread might have grabbed a reference to this layer
        // in `get_layer_for_write' just before the checkpointer called
        // `freeze`, and then `write_to_disk` on it. When the thread gets the
        // lock, it will see that it's not writeable anymore and retry, but it
        // would have to wait until we release it. That race condition is very
        // rare though, so we just accept the potential latency hit for now.
        let inner = self.inner.read().unwrap();

        let mut delta_layer_writer = DeltaLayerWriter::new(
            self.conf,
            self.timeline_id,
            self.tenant_id,
            Key::MIN,
            self.start_lsn..inner.end_lsn.unwrap(),
        )?;

        let mut buf = Vec::new();

        let mut cursor = inner.file.block_cursor();

        let mut keys: Vec<(&Key, &VecMap<Lsn, u64>)> = inner.index.iter().collect();
        keys.sort_by_key(|k| k.0);

        for (key, vec_map) in keys.iter() {
            let key = **key;
            // Write all page versions
            for (lsn, pos) in vec_map.as_slice() {
                cursor.read_blob_into_buf(*pos, &mut buf)?;
                let value = Value::des(&buf)?;
                delta_layer_writer.put_value_bytes(
                    key,
                    *lsn,
                    &buf,
                    value.will_init(),
                    value.is_image(),
                )?;
            }
        }

        let delta_layer = delta_layer_writer.finish(Key::MAX)?;
        Ok(delta_layer)
    }
}
