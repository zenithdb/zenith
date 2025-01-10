//! This module implements batch type for serialized [`pageserver_api::value::Value`]
//! instances. Each batch contains a raw buffer (serialized values)
//! and a list of metadata for each (key, LSN) tuple present in the batch.
//!
//! Such batches are created from decoded PG wal records and ingested
//! by the pageserver by writing directly to the ephemeral file.

use std::collections::BTreeSet;

use bytes::{Bytes, BytesMut};
use pageserver_api::key::rel_block_to_key;
use pageserver_api::keyspace::KeySpace;
use pageserver_api::record::NeonWalRecord;
use pageserver_api::reltag::RelTag;
use pageserver_api::shard::ShardIdentity;
use pageserver_api::{key::CompactKey, value::Value};
use postgres_ffi::walrecord::{DecodedBkpBlock, DecodedWALRecord};
use postgres_ffi::{page_is_new, page_set_lsn, pg_constants, BLCKSZ};
use serde::{Deserialize, Serialize};
use utils::bin_ser::BeSer;
use utils::lsn::Lsn;

use pageserver_api::key::Key;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; BLCKSZ as usize]);

/// Accompanying metadata for the batch
/// A value may be serialized and stored into the batch or just "observed".
/// Shard 0 currently "observes" all values in order to accurately track
/// relation sizes. In the case of "observed" values, we only need to know
/// the key and LSN, so two types of metadata are supported to save on network
/// bandwidth.
#[derive(Serialize, Deserialize)]
pub enum ValueMeta {
    Serialized(SerializedValueMeta),
    Observed(ObservedValueMeta),
}

impl ValueMeta {
    pub fn key(&self) -> CompactKey {
        match self {
            Self::Serialized(ser) => ser.key,
            Self::Observed(obs) => obs.key,
        }
    }

    pub fn lsn(&self) -> Lsn {
        match self {
            Self::Serialized(ser) => ser.lsn,
            Self::Observed(obs) => obs.lsn,
        }
    }
}

/// Wrapper around [`ValueMeta`] that implements ordering by
/// (key, LSN) tuples
struct OrderedValueMeta(ValueMeta);

impl Ord for OrderedValueMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.key(), self.0.lsn()).cmp(&(other.0.key(), other.0.lsn()))
    }
}

impl PartialOrd for OrderedValueMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrderedValueMeta {
    fn eq(&self, other: &Self) -> bool {
        (self.0.key(), self.0.lsn()) == (other.0.key(), other.0.lsn())
    }
}

impl Eq for OrderedValueMeta {}

/// Metadata for a [`Value`] serialized into the batch.
#[derive(Serialize, Deserialize)]
pub struct SerializedValueMeta {
    pub key: CompactKey,
    pub lsn: Lsn,
    /// Starting offset of the value for the (key, LSN) tuple
    /// in [`SerializedValueBatch::raw`]
    pub batch_offset: u64,
    pub len: usize,
    pub will_init: bool,
}

/// Metadata for a [`Value`] observed by the batch
#[derive(Serialize, Deserialize)]
pub struct ObservedValueMeta {
    pub key: CompactKey,
    pub lsn: Lsn,
}

/// Batch of serialized [`Value`]s.
#[derive(Serialize, Deserialize)]
pub struct SerializedValueBatch {
    /// [`Value`]s serialized in EphemeralFile's native format,
    /// ready for disk write by the pageserver
    pub raw: Vec<u8>,

    /// Metadata to make sense of the bytes in [`Self::raw`]
    /// and represent "observed" values.
    ///
    /// Invariant: Metadata entries for any given key are ordered
    /// by LSN. Note that entries for a key do not have to be contiguous.
    pub metadata: Vec<ValueMeta>,

    /// The highest LSN of any value in the batch
    pub max_lsn: Lsn,

    /// Number of values encoded by [`Self::raw`]
    pub len: usize,
}

impl Default for SerializedValueBatch {
    fn default() -> Self {
        Self {
            raw: Default::default(),
            metadata: Default::default(),
            max_lsn: Lsn(0),
            len: 0,
        }
    }
}

impl SerializedValueBatch {
    /// Build a batch of serialized values from a decoded PG WAL record
    ///
    /// The batch will only contain values for keys targeting the specifiec
    /// shard. Shard 0 is a special case, where any keys that don't belong to
    /// it are "observed" by the batch (i.e. present in [`SerializedValueBatch::metadata`],
    /// but absent from the raw buffer [`SerializedValueBatch::raw`]).
    pub(crate) fn from_decoded_filtered(
        decoded: DecodedWALRecord,
        shard: &ShardIdentity,
        next_record_lsn: Lsn,
        pg_version: u32,
    ) -> anyhow::Result<SerializedValueBatch> {
        // First determine how big the buffer needs to be and allocate it up-front.
        // This duplicates some of the work below, but it's empirically much faster.
        let estimated_buffer_size = Self::estimate_buffer_size(&decoded, shard, pg_version);
        let mut buf = Vec::<u8>::with_capacity(estimated_buffer_size);

        let mut metadata: Vec<ValueMeta> = Vec::with_capacity(decoded.blocks.len());
        let mut max_lsn: Lsn = Lsn(0);
        let mut len: usize = 0;
        for blk in decoded.blocks.iter() {
            let relative_off = buf.len() as u64;

            let rel = RelTag {
                spcnode: blk.rnode_spcnode,
                dbnode: blk.rnode_dbnode,
                relnode: blk.rnode_relnode,
                forknum: blk.forknum,
            };

            let key = rel_block_to_key(rel, blk.blkno);

            if !key.is_valid_key_on_write_path() {
                anyhow::bail!(
                    "Unsupported key decoded at LSN {}: {}",
                    next_record_lsn,
                    key
                );
            }

            let key_is_local = shard.is_key_local(&key);

            tracing::debug!(
                lsn=%next_record_lsn,
                key=%key,
                "ingest: shard decision {}",
                if !key_is_local { "drop" } else { "keep" },
            );

            if !key_is_local {
                if shard.is_shard_zero() {
                    // Shard 0 tracks relation sizes.  Although we will not store this block, we will observe
                    // its blkno in case it implicitly extends a relation.
                    metadata.push(ValueMeta::Observed(ObservedValueMeta {
                        key: key.to_compact(),
                        lsn: next_record_lsn,
                    }))
                }

                continue;
            }

            // Instead of storing full-page-image WAL record,
            // it is better to store extracted image: we can skip wal-redo
            // in this case. Also some FPI records may contain multiple (up to 32) pages,
            // so them have to be copied multiple times.
            //
            let val = if Self::block_is_image(&decoded, blk, pg_version) {
                // Extract page image from FPI record
                let img_len = blk.bimg_len as usize;
                let img_offs = blk.bimg_offset as usize;
                let mut image = BytesMut::with_capacity(BLCKSZ as usize);
                // TODO(vlad): skip the copy
                image.extend_from_slice(&decoded.record[img_offs..img_offs + img_len]);

                if blk.hole_length != 0 {
                    let tail = image.split_off(blk.hole_offset as usize);
                    image.resize(image.len() + blk.hole_length as usize, 0u8);
                    image.unsplit(tail);
                }
                //
                // Match the logic of XLogReadBufferForRedoExtended:
                // The page may be uninitialized. If so, we can't set the LSN because
                // that would corrupt the page.
                //
                if !page_is_new(&image) {
                    page_set_lsn(&mut image, next_record_lsn)
                }
                assert_eq!(image.len(), BLCKSZ as usize);

                Value::Image(image.freeze())
            } else {
                Value::WalRecord(NeonWalRecord::Postgres {
                    will_init: blk.will_init || blk.apply_image,
                    rec: decoded.record.clone(),
                })
            };

            val.ser_into(&mut buf)
                .expect("Writing into in-memory buffer is infallible");

            let val_ser_size = buf.len() - relative_off as usize;

            metadata.push(ValueMeta::Serialized(SerializedValueMeta {
                key: key.to_compact(),
                lsn: next_record_lsn,
                batch_offset: relative_off,
                len: val_ser_size,
                will_init: val.will_init(),
            }));
            max_lsn = std::cmp::max(max_lsn, next_record_lsn);
            len += 1;
        }

        if cfg!(any(debug_assertions, test)) {
            let batch = Self {
                raw: buf,
                metadata,
                max_lsn,
                len,
            };

            batch.validate_lsn_order();

            return Ok(batch);
        }

        Ok(Self {
            raw: buf,
            metadata,
            max_lsn,
            len,
        })
    }

    /// Look into the decoded PG WAL record and determine
    /// roughly how large the buffer for serialized values needs to be.
    fn estimate_buffer_size(
        decoded: &DecodedWALRecord,
        shard: &ShardIdentity,
        pg_version: u32,
    ) -> usize {
        let mut estimate: usize = 0;

        for blk in decoded.blocks.iter() {
            let rel = RelTag {
                spcnode: blk.rnode_spcnode,
                dbnode: blk.rnode_dbnode,
                relnode: blk.rnode_relnode,
                forknum: blk.forknum,
            };

            let key = rel_block_to_key(rel, blk.blkno);

            if !shard.is_key_local(&key) {
                continue;
            }

            if Self::block_is_image(decoded, blk, pg_version) {
                // 4 bytes for the Value::Image discriminator
                // 8 bytes for encoding the size of the buffer
                // BLCKSZ for the raw image
                estimate += (4 + 8 + BLCKSZ) as usize;
            } else {
                // 4 bytes for the Value::WalRecord discriminator
                // 4 bytes for the NeonWalRecord::Postgres discriminator
                // 1 bytes for NeonWalRecord::Postgres::will_init
                // 8 bytes for encoding the size of the buffer
                // length of the raw record
                estimate += 8 + 1 + 8 + decoded.record.len();
            }
        }

        estimate
    }

    fn block_is_image(decoded: &DecodedWALRecord, blk: &DecodedBkpBlock, pg_version: u32) -> bool {
        blk.apply_image
            && blk.has_image
            && decoded.xl_rmid == pg_constants::RM_XLOG_ID
            && (decoded.xl_info == pg_constants::XLOG_FPI
            || decoded.xl_info == pg_constants::XLOG_FPI_FOR_HINT)
            // compression of WAL is not yet supported: fall back to storing the original WAL record
            && !postgres_ffi::bkpimage_is_compressed(blk.bimg_info, pg_version)
            // do not materialize null pages because them most likely be soon replaced with real data
            && blk.bimg_len != 0
    }

    /// Encode a list of values and metadata into a serialized batch
    ///
    /// This is used by the pageserver ingest code to conveniently generate
    /// batches for metadata writes.
    pub fn from_values(batch: Vec<(CompactKey, Lsn, usize, Value)>) -> Self {
        // Pre-allocate a big flat buffer to write into. This should be large but not huge: it is soft-limited in practice by
        // [`crate::pgdatadir_mapping::DatadirModification::MAX_PENDING_BYTES`]
        let buffer_size = batch.iter().map(|i| i.2).sum::<usize>();
        let mut buf = Vec::<u8>::with_capacity(buffer_size);

        let mut metadata: Vec<ValueMeta> = Vec::with_capacity(batch.len());
        let mut max_lsn: Lsn = Lsn(0);
        let len = batch.len();
        for (key, lsn, val_ser_size, val) in batch {
            let relative_off = buf.len() as u64;

            val.ser_into(&mut buf)
                .expect("Writing into in-memory buffer is infallible");

            metadata.push(ValueMeta::Serialized(SerializedValueMeta {
                key,
                lsn,
                batch_offset: relative_off,
                len: val_ser_size,
                will_init: val.will_init(),
            }));
            max_lsn = std::cmp::max(max_lsn, lsn);
        }

        // Assert that we didn't do any extra allocations while building buffer.
        debug_assert!(buf.len() <= buffer_size);

        if cfg!(any(debug_assertions, test)) {
            let batch = Self {
                raw: buf,
                metadata,
                max_lsn,
                len,
            };

            batch.validate_lsn_order();

            return batch;
        }

        Self {
            raw: buf,
            metadata,
            max_lsn,
            len,
        }
    }

    /// Add one value to the batch
    ///
    /// This is used by the pageserver ingest code to include metadata block
    /// updates for a single key.
    pub fn put(&mut self, key: CompactKey, value: Value, lsn: Lsn) {
        let relative_off = self.raw.len() as u64;
        value.ser_into(&mut self.raw).unwrap();

        let val_ser_size = self.raw.len() - relative_off as usize;
        self.metadata
            .push(ValueMeta::Serialized(SerializedValueMeta {
                key,
                lsn,
                batch_offset: relative_off,
                len: val_ser_size,
                will_init: value.will_init(),
            }));

        self.max_lsn = std::cmp::max(self.max_lsn, lsn);
        self.len += 1;

        if cfg!(any(debug_assertions, test)) {
            self.validate_lsn_order();
        }
    }

    /// Extend with the contents of another batch
    ///
    /// One batch is generated for each decoded PG WAL record.
    /// They are then merged to accumulate reasonably sized writes.
    pub fn extend(&mut self, mut other: SerializedValueBatch) {
        let extend_batch_start_offset = self.raw.len() as u64;

        self.raw.extend(other.raw);

        // Shift the offsets in the batch we are extending with
        other.metadata.iter_mut().for_each(|meta| match meta {
            ValueMeta::Serialized(ser) => {
                ser.batch_offset += extend_batch_start_offset;
                if cfg!(debug_assertions) {
                    let value_end = ser.batch_offset + ser.len as u64;
                    assert!((value_end as usize) <= self.raw.len());
                }
            }
            ValueMeta::Observed(_) => {}
        });
        self.metadata.extend(other.metadata);

        self.max_lsn = std::cmp::max(self.max_lsn, other.max_lsn);

        self.len += other.len;

        if cfg!(any(debug_assertions, test)) {
            self.validate_lsn_order();
        }
    }

    /// Add zero images for the (key, LSN) tuples specified
    ///
    /// PG versions below 16 do not zero out pages before extending
    /// a relation and may leave gaps. Such gaps need to be identified
    /// by the pageserver ingest logic and get patched up here.
    ///
    /// Note that this function does not validate that the gaps have been
    /// identified correctly (it does not know relation sizes), so it's up
    /// to the call-site to do it properly.
    pub fn zero_gaps(&mut self, gaps: Vec<(KeySpace, Lsn)>) {
        // Implementation note:
        //
        // Values within [`SerializedValueBatch::raw`] do not have any ordering requirements,
        // but the metadata entries should be ordered properly (see
        // [`SerializedValueBatch::metadata`]).
        //
        // Exploiting this observation we do:
        // 1. Drain all the metadata entries into an ordered set.
        // The use of a BTreeSet keyed by (Key, Lsn) relies on the observation that Postgres never
        // includes more than one update to the same block in the same WAL record.
        // 2. For each (key, LSN) gap tuple, append a zero image to the raw buffer
        // and add an index entry to the ordered metadata set.
        // 3. Drain the ordered set back into a metadata vector

        let mut ordered_metas = self
            .metadata
            .drain(..)
            .map(OrderedValueMeta)
            .collect::<BTreeSet<_>>();
        for (keyspace, lsn) in gaps {
            self.max_lsn = std::cmp::max(self.max_lsn, lsn);

            for gap_range in keyspace.ranges {
                let mut key = gap_range.start;
                while key != gap_range.end {
                    let relative_off = self.raw.len() as u64;

                    // TODO(vlad): Can we be cheeky and write only one zero image, and
                    // make all index entries requiring a zero page point to it?
                    // Alternatively, we can change the index entry format to represent zero pages
                    // without writing them at all.
                    Value::Image(ZERO_PAGE.clone())
                        .ser_into(&mut self.raw)
                        .unwrap();
                    let val_ser_size = self.raw.len() - relative_off as usize;

                    ordered_metas.insert(OrderedValueMeta(ValueMeta::Serialized(
                        SerializedValueMeta {
                            key: key.to_compact(),
                            lsn,
                            batch_offset: relative_off,
                            len: val_ser_size,
                            will_init: true,
                        },
                    )));

                    self.len += 1;

                    key = key.next();
                }
            }
        }

        self.metadata = ordered_metas.into_iter().map(|ord| ord.0).collect();

        if cfg!(any(debug_assertions, test)) {
            self.validate_lsn_order();
        }
    }

    /// Checks if the batch contains any serialized or observed values
    pub fn is_empty(&self) -> bool {
        !self.has_data() && self.metadata.is_empty()
    }

    /// Checks if the batch contains only observed values
    pub fn is_observed(&self) -> bool {
        !self.has_data() && !self.metadata.is_empty()
    }

    /// Checks if the batch contains data
    ///
    /// Note that if this returns false, it may still contain observed values or
    /// a metadata record.
    pub fn has_data(&self) -> bool {
        let empty = self.raw.is_empty();

        if cfg!(debug_assertions) && empty {
            assert!(self
                .metadata
                .iter()
                .all(|meta| matches!(meta, ValueMeta::Observed(_))));
        }

        !empty
    }

    /// Returns the number of values serialized in the batch
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the size of the buffer wrapped by the batch
    pub fn buffer_size(&self) -> usize {
        self.raw.len()
    }

    pub fn updates_key(&self, key: &Key) -> bool {
        self.metadata.iter().any(|meta| match meta {
            ValueMeta::Serialized(ser) => key.to_compact() == ser.key,
            ValueMeta::Observed(_) => false,
        })
    }

    pub fn validate_lsn_order(&self) {
        use std::collections::HashMap;

        let mut last_seen_lsn_per_key: HashMap<CompactKey, Lsn> = HashMap::default();

        for meta in self.metadata.iter() {
            let lsn = meta.lsn();
            let key = meta.key();

            if let Some(prev_lsn) = last_seen_lsn_per_key.insert(key, lsn) {
                assert!(
                    lsn >= prev_lsn,
                    "Ordering violated by {}: {} < {}",
                    Key::from_compact(key),
                    lsn,
                    prev_lsn
                );
            }
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;

    fn validate_batch(
        batch: &SerializedValueBatch,
        values: &[(CompactKey, Lsn, usize, Value)],
        gaps: Option<&Vec<(KeySpace, Lsn)>>,
    ) {
        // Invariant 1: The metadata for a given entry in the batch
        // is correct and can be used to deserialize back to the original value.
        for (key, lsn, size, value) in values.iter() {
            let meta = batch
                .metadata
                .iter()
                .find(|meta| (meta.key(), meta.lsn()) == (*key, *lsn))
                .unwrap();
            let meta = match meta {
                ValueMeta::Serialized(ser) => ser,
                ValueMeta::Observed(_) => unreachable!(),
            };

            assert_eq!(meta.len, *size);
            assert_eq!(meta.will_init, value.will_init());

            let start = meta.batch_offset as usize;
            let end = meta.batch_offset as usize + meta.len;
            let value_from_batch = Value::des(&batch.raw[start..end]).unwrap();
            assert_eq!(&value_from_batch, value);
        }

        let mut expected_buffer_size: usize = values.iter().map(|(_, _, size, _)| size).sum();
        let mut gap_pages_count: usize = 0;

        // Invariant 2: Zero pages were added for identified gaps and their metadata
        // is correct.
        if let Some(gaps) = gaps {
            for (gap_keyspace, lsn) in gaps {
                for gap_range in &gap_keyspace.ranges {
                    let mut gap_key = gap_range.start;
                    while gap_key != gap_range.end {
                        let meta = batch
                            .metadata
                            .iter()
                            .find(|meta| (meta.key(), meta.lsn()) == (gap_key.to_compact(), *lsn))
                            .unwrap();
                        let meta = match meta {
                            ValueMeta::Serialized(ser) => ser,
                            ValueMeta::Observed(_) => unreachable!(),
                        };

                        let zero_value = Value::Image(ZERO_PAGE.clone());
                        let zero_value_size = zero_value.serialized_size().unwrap() as usize;

                        assert_eq!(meta.len, zero_value_size);
                        assert_eq!(meta.will_init, zero_value.will_init());

                        let start = meta.batch_offset as usize;
                        let end = meta.batch_offset as usize + meta.len;
                        let value_from_batch = Value::des(&batch.raw[start..end]).unwrap();
                        assert_eq!(value_from_batch, zero_value);

                        gap_pages_count += 1;
                        expected_buffer_size += zero_value_size;
                        gap_key = gap_key.next();
                    }
                }
            }
        }

        // Invariant 3: The length of the batch is equal to the number
        // of values inserted, plus the number of gap pages. This extends
        // to the raw buffer size.
        assert_eq!(batch.len(), values.len() + gap_pages_count);
        assert_eq!(expected_buffer_size, batch.buffer_size());

        // Invariant 4: Metadata entries for any given key are sorted in LSN order.
        batch.validate_lsn_order();
    }

    #[test]
    fn test_creation_from_values() {
        const LSN: Lsn = Lsn(0x10);
        let key = Key::from_hex("110000000033333333444444445500000001").unwrap();

        let values = vec![
            (
                key.to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("foo")),
            ),
            (
                key.next().to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("bar")),
            ),
            (
                key.to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("baz")),
            ),
            (
                key.next().next().to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("taz")),
            ),
        ];

        let values = values
            .into_iter()
            .map(|(key, lsn, value)| (key, lsn, value.serialized_size().unwrap() as usize, value))
            .collect::<Vec<_>>();
        let batch = SerializedValueBatch::from_values(values.clone());

        validate_batch(&batch, &values, None);

        assert!(!batch.is_empty());
    }

    #[test]
    fn test_put() {
        const LSN: Lsn = Lsn(0x10);
        let key = Key::from_hex("110000000033333333444444445500000001").unwrap();

        let values = vec![
            (
                key.to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("foo")),
            ),
            (
                key.next().to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("bar")),
            ),
        ];

        let mut values = values
            .into_iter()
            .map(|(key, lsn, value)| (key, lsn, value.serialized_size().unwrap() as usize, value))
            .collect::<Vec<_>>();
        let mut batch = SerializedValueBatch::from_values(values.clone());

        validate_batch(&batch, &values, None);

        let value = (
            key.to_compact(),
            Lsn(LSN.0 + 0x10),
            Value::WalRecord(NeonWalRecord::wal_append("baz")),
        );
        let serialized_size = value.2.serialized_size().unwrap() as usize;
        let value = (value.0, value.1, serialized_size, value.2);
        values.push(value.clone());
        batch.put(value.0, value.3, value.1);

        validate_batch(&batch, &values, None);

        let value = (
            key.next().next().to_compact(),
            LSN,
            Value::WalRecord(NeonWalRecord::wal_append("taz")),
        );
        let serialized_size = value.2.serialized_size().unwrap() as usize;
        let value = (value.0, value.1, serialized_size, value.2);
        values.push(value.clone());
        batch.put(value.0, value.3, value.1);

        validate_batch(&batch, &values, None);
    }

    #[test]
    fn test_extension() {
        const LSN: Lsn = Lsn(0x10);
        let key = Key::from_hex("110000000033333333444444445500000001").unwrap();

        let values = vec![
            (
                key.to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("foo")),
            ),
            (
                key.next().to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("bar")),
            ),
            (
                key.next().next().to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("taz")),
            ),
        ];

        let mut values = values
            .into_iter()
            .map(|(key, lsn, value)| (key, lsn, value.serialized_size().unwrap() as usize, value))
            .collect::<Vec<_>>();
        let mut batch = SerializedValueBatch::from_values(values.clone());

        let other_values = vec![
            (
                key.to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("foo")),
            ),
            (
                key.next().to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("bar")),
            ),
            (
                key.next().next().to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("taz")),
            ),
        ];

        let other_values = other_values
            .into_iter()
            .map(|(key, lsn, value)| (key, lsn, value.serialized_size().unwrap() as usize, value))
            .collect::<Vec<_>>();
        let other_batch = SerializedValueBatch::from_values(other_values.clone());

        values.extend(other_values);
        batch.extend(other_batch);

        validate_batch(&batch, &values, None);
    }

    #[test]
    fn test_gap_zeroing() {
        const LSN: Lsn = Lsn(0x10);
        let rel_foo_base_key = Key::from_hex("110000000033333333444444445500000001").unwrap();

        let rel_bar_base_key = {
            let mut key = rel_foo_base_key;
            key.field4 += 1;
            key
        };

        let values = vec![
            (
                rel_foo_base_key.to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("foo1")),
            ),
            (
                rel_foo_base_key.add(1).to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("foo2")),
            ),
            (
                rel_foo_base_key.add(5).to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("foo3")),
            ),
            (
                rel_foo_base_key.add(1).to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("foo4")),
            ),
            (
                rel_foo_base_key.add(10).to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("foo5")),
            ),
            (
                rel_foo_base_key.add(11).to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("foo6")),
            ),
            (
                rel_foo_base_key.add(12).to_compact(),
                Lsn(LSN.0 + 0x10),
                Value::WalRecord(NeonWalRecord::wal_append("foo7")),
            ),
            (
                rel_bar_base_key.to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("bar1")),
            ),
            (
                rel_bar_base_key.add(4).to_compact(),
                LSN,
                Value::WalRecord(NeonWalRecord::wal_append("bar2")),
            ),
        ];

        let values = values
            .into_iter()
            .map(|(key, lsn, value)| (key, lsn, value.serialized_size().unwrap() as usize, value))
            .collect::<Vec<_>>();

        let mut batch = SerializedValueBatch::from_values(values.clone());

        let gaps = vec![
            (
                KeySpace {
                    ranges: vec![
                        rel_foo_base_key.add(2)..rel_foo_base_key.add(5),
                        rel_bar_base_key.add(1)..rel_bar_base_key.add(4),
                    ],
                },
                LSN,
            ),
            (
                KeySpace {
                    ranges: vec![rel_foo_base_key.add(6)..rel_foo_base_key.add(10)],
                },
                Lsn(LSN.0 + 0x10),
            ),
        ];

        batch.zero_gaps(gaps.clone());
        validate_batch(&batch, &values, Some(&gaps));
    }
}
