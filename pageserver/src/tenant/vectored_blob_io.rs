//!
//! Utilities for vectored reading of variable-sized "blobs".
//!
//! The "blob" api is an abstraction on top of the "block" api,
//! with the main difference being that blobs do not have a fixed
//! size (each blob is prefixed with 1 or 4 byte length field)
//!
//! The vectored apis provided in this module allow for planning
//! and executing disk IO which covers multiple blobs.
//!
//! Reads are planned with [`VectoredReadPlanner`] which will coalesce
//! adjacent blocks into a single disk IO request and exectuted by
//! [`VectoredBlobReader`] which does all the required offset juggling
//! and returns a buffer housing all the blobs and a list of offsets.
//!
//! Note that the vectored blob api does *not* go through the page cache.

use std::collections::BTreeMap;
use std::ops::Deref;

use bytes::Bytes;
use pageserver_api::key::Key;
use tokio::io::AsyncWriteExt;
use tokio_epoll_uring::BoundedBuf;
use utils::lsn::Lsn;
use utils::vec_map::VecMap;

use crate::context::RequestContext;
use crate::tenant::blob_io::{BYTE_UNCOMPRESSED, BYTE_ZSTD, LEN_COMPRESSION_BIT_MASK};
use crate::virtual_file::IoBufferMut;
use crate::virtual_file::{self, VirtualFile};

/// Metadata bundled with the start and end offset of a blob.
#[derive(Copy, Clone, Debug)]
pub struct BlobMeta {
    pub key: Key,
    pub lsn: Lsn,
}

/// A view into the vectored blobs read buffer.
#[derive(Clone, Debug)]
pub(crate) enum BufView<'a> {
    Slice(&'a [u8]),
    Bytes(bytes::Bytes),
}

impl<'a> BufView<'a> {
    /// Creates a new slice-based view on the blob.
    pub fn new_slice(slice: &'a [u8]) -> Self {
        Self::Slice(slice)
    }

    /// Creates a new [`bytes::Bytes`]-based view on the blob.
    pub fn new_bytes(bytes: bytes::Bytes) -> Self {
        Self::Bytes(bytes)
    }

    /// Convert the view into `Bytes`.
    ///
    /// If using slice as the underlying storage, the copy will be an O(n) operation.
    pub fn into_bytes(self) -> Bytes {
        match self {
            BufView::Slice(slice) => Bytes::copy_from_slice(slice),
            BufView::Bytes(bytes) => bytes,
        }
    }

    /// Creates a sub-view of the blob based on the range.
    fn view(&self, range: std::ops::Range<usize>) -> Self {
        match self {
            BufView::Slice(slice) => BufView::Slice(&slice[range]),
            BufView::Bytes(bytes) => BufView::Bytes(bytes.slice(range)),
        }
    }
}

impl Deref for BufView<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            BufView::Slice(slice) => slice,
            BufView::Bytes(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for BufView<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            BufView::Slice(slice) => slice,
            BufView::Bytes(bytes) => bytes.as_ref(),
        }
    }
}

impl<'a> From<&'a [u8]> for BufView<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::new_slice(value)
    }
}

impl From<Bytes> for BufView<'_> {
    fn from(value: Bytes) -> Self {
        Self::new_bytes(value)
    }
}

/// Blob offsets into [`VectoredBlobsBuf::buf`]. The byte ranges is potentially compressed,
/// subject to [`VectoredBlob::compression_bits`].
pub struct VectoredBlob {
    /// Blob metadata.
    pub meta: BlobMeta,
    /// Start offset.
    start: usize,
    /// End offset.
    end: usize,
    /// Compression used on the the blob.
    compression_bits: u8,
}

impl VectoredBlob {
    /// Reads a decompressed view of the blob.
    pub(crate) async fn read<'a>(&self, buf: &BufView<'a>) -> Result<BufView<'a>, std::io::Error> {
        let view = buf.view(self.start..self.end);

        match self.compression_bits {
            BYTE_UNCOMPRESSED => Ok(view),
            BYTE_ZSTD => {
                let mut decompressed_vec = Vec::new();
                let mut decoder =
                    async_compression::tokio::write::ZstdDecoder::new(&mut decompressed_vec);
                decoder.write_all(&view).await?;
                decoder.flush().await?;
                // Zero-copy conversion from `Vec` to `Bytes`
                Ok(BufView::new_bytes(Bytes::from(decompressed_vec)))
            }
            bits => {
                let error = std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to decompress blob for {}@{}, {}..{}: invalid compression byte {bits:x}", self.meta.key, self.meta.lsn, self.start, self.end),
                );
                Err(error)
            }
        }
    }
}

impl std::fmt::Display for VectoredBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}, {}..{}",
            self.meta.key, self.meta.lsn, self.start, self.end
        )
    }
}

/// Return type of [`VectoredBlobReader::read_blobs`]
pub struct VectoredBlobsBuf {
    /// Buffer for all blobs in this read
    pub buf: IoBufferMut,
    /// Offsets into the buffer and metadata for all blobs in this read
    pub blobs: Vec<VectoredBlob>,
}

/// Description of one disk read for multiple blobs.
/// Used as the argument form [`VectoredBlobReader::read_blobs`]
#[derive(Debug)]
pub struct VectoredRead {
    pub start: u64,
    pub end: u64,
    /// Start offset and metadata for each blob in this read
    pub blobs_at: VecMap<u64, BlobMeta>,
}

impl VectoredRead {
    pub(crate) fn size(&self) -> usize {
        (self.end - self.start) as usize
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum VectoredReadExtended {
    Yes,
    No,
}

/// A vectored read builder that tries to coalesce all reads that fits in a chunk.
pub(crate) struct ChunkedVectoredReadBuilder {
    /// Start block number
    start_blk_no: usize,
    /// End block number (exclusive).
    end_blk_no: usize,
    /// Start offset and metadata for each blob in this read
    blobs_at: VecMap<u64, BlobMeta>,
    max_read_size: Option<usize>,
}

impl ChunkedVectoredReadBuilder {
    const CHUNK_SIZE: usize = virtual_file::get_io_buffer_alignment();
    /// Start building a new vectored read.
    ///
    /// Note that by design, this does not check against reading more than `max_read_size` to
    /// support reading larger blobs than the configuration value. The builder will be single use
    /// however after that.
    fn new_impl(
        start_offset: u64,
        end_offset: u64,
        meta: BlobMeta,
        max_read_size: Option<usize>,
    ) -> Self {
        let mut blobs_at = VecMap::default();
        blobs_at
            .append(start_offset, meta)
            .expect("First insertion always succeeds");

        let start_blk_no = start_offset as usize / Self::CHUNK_SIZE;
        let end_blk_no = (end_offset as usize).div_ceil(Self::CHUNK_SIZE);
        Self {
            start_blk_no,
            end_blk_no,
            blobs_at,
            max_read_size,
        }
    }

    pub(crate) fn new(
        start_offset: u64,
        end_offset: u64,
        meta: BlobMeta,
        max_read_size: usize,
    ) -> Self {
        Self::new_impl(start_offset, end_offset, meta, Some(max_read_size))
    }

    pub(crate) fn new_streaming(start_offset: u64, end_offset: u64, meta: BlobMeta) -> Self {
        Self::new_impl(start_offset, end_offset, meta, None)
    }

    /// Attempts to extend the current read with a new blob if the new blob resides in the same or the immediate next chunk.
    ///
    /// The resulting size also must be below the max read size.
    pub(crate) fn extend(&mut self, start: u64, end: u64, meta: BlobMeta) -> VectoredReadExtended {
        tracing::trace!(start, end, "trying to extend");
        let start_blk_no = start as usize / Self::CHUNK_SIZE;
        let end_blk_no = (end as usize).div_ceil(Self::CHUNK_SIZE);

        let not_limited_by_max_read_size = {
            if let Some(max_read_size) = self.max_read_size {
                let coalesced_size = (end_blk_no - self.start_blk_no) * Self::CHUNK_SIZE;
                coalesced_size <= max_read_size
            } else {
                true
            }
        };

        // True if the second block starts in the same block or the immediate next block where the first block ended.
        //
        // Note: This automatically handles the case where two blocks are adjacent to each other,
        // whether they starts on chunk size boundary or not.
        let is_adjacent_chunk_read = {
            // 1. first.end & second.start are in the same block
            self.end_blk_no == start_blk_no + 1 ||
            // 2. first.end ends one block before second.start
            self.end_blk_no == start_blk_no
        };

        if is_adjacent_chunk_read && not_limited_by_max_read_size {
            self.end_blk_no = end_blk_no;
            self.blobs_at
                .append(start, meta)
                .expect("LSNs are ordered within vectored reads");

            return VectoredReadExtended::Yes;
        }

        VectoredReadExtended::No
    }

    pub(crate) fn size(&self) -> usize {
        (self.end_blk_no - self.start_blk_no) * Self::CHUNK_SIZE
    }

    pub(crate) fn build(self) -> VectoredRead {
        let start = (self.start_blk_no * Self::CHUNK_SIZE) as u64;
        let end = (self.end_blk_no * Self::CHUNK_SIZE) as u64;
        VectoredRead {
            start,
            end,
            blobs_at: self.blobs_at,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BlobFlag {
    None,
    Ignore,
    ReplaceAll,
}

/// Planner for vectored blob reads.
///
/// Blob offsets are received via [`VectoredReadPlanner::handle`]
/// and coalesced into disk reads.
///
/// The implementation is very simple:
/// * Collect all blob offsets in an ordered structure
/// * Iterate over the collected blobs and coalesce them into reads at the end
pub struct VectoredReadPlanner {
    // Track all the blob offsets. Start offsets must be ordered.
    blobs: BTreeMap<Key, Vec<(Lsn, u64, u64)>>,
    // Arguments for previous blob passed into [`VectoredReadPlanner::handle`]
    prev: Option<(Key, Lsn, u64, BlobFlag)>,

    max_read_size: usize,
}

impl VectoredReadPlanner {
    pub fn new(max_read_size: usize) -> Self {
        Self {
            blobs: BTreeMap::new(),
            prev: None,
            max_read_size,
        }
    }

    /// Include a new blob in the read plan.
    ///
    /// This function is called from a B-Tree index visitor (see `DeltaLayerInner::plan_reads`
    /// and `ImageLayerInner::plan_reads`). Said visitor wants to collect blob offsets for all
    /// keys in a given keyspace. This function must be called for each key in the desired
    /// keyspace (monotonically continuous). [`Self::handle_range_end`] must
    /// be called after every range in the offset.
    ///
    /// In the event that keys are skipped, the behaviour is undefined and can lead to an
    /// incorrect read plan. We can end up asserting, erroring in wal redo or returning
    /// incorrect data to the user.
    ///
    /// The `flag` argument has two interesting values:
    /// * [`BlobFlag::ReplaceAll`]: The blob for this key should replace all existing blobs.
    ///   This is used for WAL records that `will_init`.
    /// * [`BlobFlag::Ignore`]: This blob should not be included in the read. This happens
    ///   if the blob is cached.
    pub fn handle(&mut self, key: Key, lsn: Lsn, offset: u64, flag: BlobFlag) {
        // Implementation note: internally lag behind by one blob such that
        // we have a start and end offset when initialising [`VectoredRead`]
        let (prev_key, prev_lsn, prev_offset, prev_flag) = match self.prev {
            None => {
                self.prev = Some((key, lsn, offset, flag));
                return;
            }
            Some(prev) => prev,
        };

        self.add_blob(prev_key, prev_lsn, prev_offset, offset, prev_flag);

        self.prev = Some((key, lsn, offset, flag));
    }

    pub fn handle_range_end(&mut self, offset: u64) {
        if let Some((prev_key, prev_lsn, prev_offset, prev_flag)) = self.prev {
            self.add_blob(prev_key, prev_lsn, prev_offset, offset, prev_flag);
        }

        self.prev = None;
    }

    fn add_blob(&mut self, key: Key, lsn: Lsn, start_offset: u64, end_offset: u64, flag: BlobFlag) {
        match flag {
            BlobFlag::None => {
                let blobs_for_key = self.blobs.entry(key).or_default();
                blobs_for_key.push((lsn, start_offset, end_offset));
            }
            BlobFlag::ReplaceAll => {
                let blobs_for_key = self.blobs.entry(key).or_default();
                blobs_for_key.clear();
                blobs_for_key.push((lsn, start_offset, end_offset));
            }
            BlobFlag::Ignore => {}
        }
    }

    pub fn finish(self) -> Vec<VectoredRead> {
        let mut current_read_builder: Option<ChunkedVectoredReadBuilder> = None;
        let mut reads = Vec::new();

        for (key, blobs_for_key) in self.blobs {
            for (lsn, start_offset, end_offset) in blobs_for_key {
                let extended = match &mut current_read_builder {
                    Some(read_builder) => {
                        read_builder.extend(start_offset, end_offset, BlobMeta { key, lsn })
                    }
                    None => VectoredReadExtended::No,
                };

                if extended == VectoredReadExtended::No {
                    let next_read_builder = ChunkedVectoredReadBuilder::new(
                        start_offset,
                        end_offset,
                        BlobMeta { key, lsn },
                        self.max_read_size,
                    );

                    let prev_read_builder = current_read_builder.replace(next_read_builder);

                    // `current_read_builder` is None in the first iteration of the outer loop
                    if let Some(read_builder) = prev_read_builder {
                        reads.push(read_builder.build());
                    }
                }
            }
        }

        if let Some(read_builder) = current_read_builder {
            reads.push(read_builder.build());
        }

        reads
    }
}

/// Disk reader for vectored blob spans (does not go through the page cache)
pub struct VectoredBlobReader<'a> {
    file: &'a VirtualFile,
}

impl<'a> VectoredBlobReader<'a> {
    pub fn new(file: &'a VirtualFile) -> Self {
        Self { file }
    }

    /// Read the requested blobs into the buffer.
    ///
    /// We have to deal with the fact that blobs are not fixed size.
    /// Each blob is prefixed by a size header.
    ///
    /// The success return value is a struct which contains the buffer
    /// filled from disk and a list of offsets at which each blob lies
    /// in the buffer.
    pub async fn read_blobs(
        &self,
        read: &VectoredRead,
        buf: IoBufferMut,
        ctx: &RequestContext,
    ) -> Result<VectoredBlobsBuf, std::io::Error> {
        assert!(read.size() > 0);
        assert!(
            read.size() <= buf.capacity(),
            "{} > {}",
            read.size(),
            buf.capacity()
        );

        if cfg!(debug_assertions) {
            const ALIGN: u64 = virtual_file::get_io_buffer_alignment() as u64;
            debug_assert_eq!(
                read.start % ALIGN,
                0,
                "Read start at {} does not satisfy the required io buffer alignment ({} bytes)",
                read.start,
                ALIGN
            );
        }

        let buf = self
            .file
            .read_exact_at(buf.slice(0..read.size()), read.start, ctx)
            .await?
            .into_inner();

        let blobs_at = read.blobs_at.as_slice();

        let start_offset = read.start;

        let mut metas = Vec::with_capacity(blobs_at.len());
        // Blobs in `read` only provide their starting offset. The end offset
        // of a blob is implicit: the start of the next blob if one exists
        // or the end of the read.

        for (blob_start, meta) in blobs_at {
            let blob_start_in_buf = blob_start - start_offset;
            let first_len_byte = buf[blob_start_in_buf as usize];

            // Each blob is prefixed by a header containing its size and compression information.
            // Extract the size and skip that header to find the start of the data.
            // The size can be 1 or 4 bytes. The most significant bit is 0 in the
            // 1 byte case and 1 in the 4 byte case.
            let (size_length, blob_size, compression_bits) = if first_len_byte < 0x80 {
                (1, first_len_byte as u64, BYTE_UNCOMPRESSED)
            } else {
                let mut blob_size_buf = [0u8; 4];
                let offset_in_buf = blob_start_in_buf as usize;

                blob_size_buf.copy_from_slice(&buf[offset_in_buf..offset_in_buf + 4]);
                blob_size_buf[0] &= !LEN_COMPRESSION_BIT_MASK;

                let compression_bits = first_len_byte & LEN_COMPRESSION_BIT_MASK;
                (
                    4,
                    u32::from_be_bytes(blob_size_buf) as u64,
                    compression_bits,
                )
            };

            let start = (blob_start_in_buf + size_length) as usize;
            let end = start + blob_size as usize;

            metas.push(VectoredBlob {
                start,
                end,
                meta: *meta,
                compression_bits,
            });
        }

        Ok(VectoredBlobsBuf { buf, blobs: metas })
    }
}

/// Read planner used in [`crate::tenant::storage_layer::image_layer::ImageLayerIterator`].
///
/// It provides a streaming API for getting read blobs. It returns a batch when
/// `handle` gets called and when the current key would just exceed the read_size and
/// max_cnt constraints.
pub struct StreamingVectoredReadPlanner {
    read_builder: Option<ChunkedVectoredReadBuilder>,
    // Arguments for previous blob passed into [`StreamingVectoredReadPlanner::handle`]
    prev: Option<(Key, Lsn, u64)>,
    /// Max read size per batch. This is not a strict limit. If there are [0, 100) and [100, 200), while the `max_read_size` is 150,
    /// we will produce a single batch instead of split them.
    max_read_size: u64,
    /// Max item count per batch
    max_cnt: usize,
    /// Size of the current batch
    cnt: usize,
}

impl StreamingVectoredReadPlanner {
    pub fn new(max_read_size: u64, max_cnt: usize) -> Self {
        assert!(max_cnt > 0);
        assert!(max_read_size > 0);
        Self {
            read_builder: None,
            prev: None,
            max_cnt,
            max_read_size,
            cnt: 0,
        }
    }

    pub fn handle(&mut self, key: Key, lsn: Lsn, offset: u64) -> Option<VectoredRead> {
        // Implementation note: internally lag behind by one blob such that
        // we have a start and end offset when initialising [`VectoredRead`]
        let (prev_key, prev_lsn, prev_offset) = match self.prev {
            None => {
                self.prev = Some((key, lsn, offset));
                return None;
            }
            Some(prev) => prev,
        };

        let res = self.add_blob(prev_key, prev_lsn, prev_offset, offset, false);

        self.prev = Some((key, lsn, offset));

        res
    }

    pub fn handle_range_end(&mut self, offset: u64) -> Option<VectoredRead> {
        let res = if let Some((prev_key, prev_lsn, prev_offset)) = self.prev {
            self.add_blob(prev_key, prev_lsn, prev_offset, offset, true)
        } else {
            None
        };

        self.prev = None;

        res
    }

    fn add_blob(
        &mut self,
        key: Key,
        lsn: Lsn,
        start_offset: u64,
        end_offset: u64,
        is_last_blob_in_read: bool,
    ) -> Option<VectoredRead> {
        match &mut self.read_builder {
            Some(read_builder) => {
                let extended = read_builder.extend(start_offset, end_offset, BlobMeta { key, lsn });
                assert_eq!(extended, VectoredReadExtended::Yes);
            }
            None => {
                self.read_builder = {
                    Some(ChunkedVectoredReadBuilder::new_streaming(
                        start_offset,
                        end_offset,
                        BlobMeta { key, lsn },
                    ))
                };
            }
        }
        let read_builder = self.read_builder.as_mut().unwrap();
        self.cnt += 1;
        if is_last_blob_in_read
            || read_builder.size() >= self.max_read_size as usize
            || self.cnt >= self.max_cnt
        {
            let prev_read_builder = self.read_builder.take();
            self.cnt = 0;

            // `current_read_builder` is None in the first iteration
            if let Some(read_builder) = prev_read_builder {
                return Some(read_builder.build());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;

    use crate::context::DownloadBehavior;
    use crate::page_cache::PAGE_SZ;
    use crate::task_mgr::TaskKind;

    use super::super::blob_io::tests::{random_array, write_maybe_compressed};
    use super::*;

    fn validate_read(read: &VectoredRead, offset_range: &[(Key, Lsn, u64, BlobFlag)]) {
        const ALIGN: u64 = virtual_file::get_io_buffer_alignment() as u64;
        assert_eq!(read.start % ALIGN, 0);
        assert_eq!(read.start / ALIGN, offset_range.first().unwrap().2 / ALIGN);

        let expected_offsets_in_read: Vec<_> = offset_range.iter().map(|o| o.2).collect();

        let offsets_in_read: Vec<_> = read
            .blobs_at
            .as_slice()
            .iter()
            .map(|(offset, _)| *offset)
            .collect();

        assert_eq!(expected_offsets_in_read, offsets_in_read);
    }

    #[test]
    fn planner_chunked_coalesce_all_test() {
        use crate::virtual_file;

        const CHUNK_SIZE: u64 = virtual_file::get_io_buffer_alignment() as u64;

        let max_read_size = CHUNK_SIZE as usize * 8;
        let key = Key::MIN;
        let lsn = Lsn(0);

        let blob_descriptions = [
            (key, lsn, CHUNK_SIZE / 8, BlobFlag::None), // Read 1 BEGIN
            (key, lsn, CHUNK_SIZE / 4, BlobFlag::Ignore), // Gap
            (key, lsn, CHUNK_SIZE / 2, BlobFlag::None),
            (key, lsn, CHUNK_SIZE - 2, BlobFlag::Ignore), // Gap
            (key, lsn, CHUNK_SIZE, BlobFlag::None),
            (key, lsn, CHUNK_SIZE * 2 - 1, BlobFlag::None),
            (key, lsn, CHUNK_SIZE * 2 + 1, BlobFlag::Ignore), // Gap
            (key, lsn, CHUNK_SIZE * 3 + 1, BlobFlag::None),
            (key, lsn, CHUNK_SIZE * 5 + 1, BlobFlag::None),
            (key, lsn, CHUNK_SIZE * 6 + 1, BlobFlag::Ignore), // skipped chunk size, but not a chunk: should coalesce.
            (key, lsn, CHUNK_SIZE * 7 + 1, BlobFlag::None),
            (key, lsn, CHUNK_SIZE * 8, BlobFlag::None), // Read 2 BEGIN (b/c max_read_size)
            (key, lsn, CHUNK_SIZE * 9, BlobFlag::Ignore), // ==== skipped a chunk
            (key, lsn, CHUNK_SIZE * 10, BlobFlag::None), // Read 3 BEGIN (cannot coalesce)
        ];

        let ranges = [
            &[
                blob_descriptions[0],
                blob_descriptions[2],
                blob_descriptions[4],
                blob_descriptions[5],
                blob_descriptions[7],
                blob_descriptions[8],
                blob_descriptions[10],
            ],
            &blob_descriptions[11..12],
            &blob_descriptions[13..],
        ];

        let mut planner = VectoredReadPlanner::new(max_read_size);
        for (key, lsn, offset, flag) in blob_descriptions {
            planner.handle(key, lsn, offset, flag);
        }

        planner.handle_range_end(652 * 1024);

        let reads = planner.finish();

        assert_eq!(reads.len(), ranges.len());

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }

    #[test]
    fn planner_max_read_size_test() {
        let max_read_size = 128 * 1024;
        let key = Key::MIN;
        let lsn = Lsn(0);

        let blob_descriptions = vec![
            (key, lsn, 0, BlobFlag::None),
            (key, lsn, 32 * 1024, BlobFlag::None),
            (key, lsn, 96 * 1024, BlobFlag::None), // Last in read 1
            (key, lsn, 128 * 1024, BlobFlag::None), // Last in read 2
            (key, lsn, 198 * 1024, BlobFlag::None), // Last in read 3
            (key, lsn, 268 * 1024, BlobFlag::None), // Last in read 4
            (key, lsn, 396 * 1024, BlobFlag::None), // Last in read 5
            (key, lsn, 652 * 1024, BlobFlag::None), // Last in read 6
        ];

        let ranges = [
            &blob_descriptions[0..3],
            &blob_descriptions[3..4],
            &blob_descriptions[4..5],
            &blob_descriptions[5..6],
            &blob_descriptions[6..7],
            &blob_descriptions[7..],
        ];

        let mut planner = VectoredReadPlanner::new(max_read_size);
        for (key, lsn, offset, flag) in blob_descriptions.clone() {
            planner.handle(key, lsn, offset, flag);
        }

        planner.handle_range_end(652 * 1024);

        let reads = planner.finish();

        assert_eq!(reads.len(), 6);

        // TODO: could remove zero reads to produce 5 reads here

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }

    #[test]
    fn planner_replacement_test() {
        const CHUNK_SIZE: u64 = virtual_file::get_io_buffer_alignment() as u64;
        let max_read_size = 128 * CHUNK_SIZE as usize;
        let first_key = Key::MIN;
        let second_key = first_key.next();
        let lsn = Lsn(0);

        let blob_descriptions = vec![
            (first_key, lsn, 0, BlobFlag::None),          // First in read 1
            (first_key, lsn, CHUNK_SIZE, BlobFlag::None), // Last in read 1
            (second_key, lsn, 2 * CHUNK_SIZE, BlobFlag::ReplaceAll),
            (second_key, lsn, 3 * CHUNK_SIZE, BlobFlag::None),
            (second_key, lsn, 4 * CHUNK_SIZE, BlobFlag::ReplaceAll), // First in read 2
            (second_key, lsn, 5 * CHUNK_SIZE, BlobFlag::None),       // Last in read 2
        ];

        let ranges = [&blob_descriptions[0..2], &blob_descriptions[4..]];

        let mut planner = VectoredReadPlanner::new(max_read_size);
        for (key, lsn, offset, flag) in blob_descriptions.clone() {
            planner.handle(key, lsn, offset, flag);
        }

        planner.handle_range_end(6 * CHUNK_SIZE);

        let reads = planner.finish();
        assert_eq!(reads.len(), 2);

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }

    #[test]
    fn streaming_planner_max_read_size_test() {
        let max_read_size = 128 * 1024;
        let key = Key::MIN;
        let lsn = Lsn(0);

        let blob_descriptions = vec![
            (key, lsn, 0, BlobFlag::None),
            (key, lsn, 32 * 1024, BlobFlag::None),
            (key, lsn, 96 * 1024, BlobFlag::None),
            (key, lsn, 128 * 1024, BlobFlag::None),
            (key, lsn, 198 * 1024, BlobFlag::None),
            (key, lsn, 268 * 1024, BlobFlag::None),
            (key, lsn, 396 * 1024, BlobFlag::None),
            (key, lsn, 652 * 1024, BlobFlag::None),
        ];

        let ranges = [
            &blob_descriptions[0..3],
            &blob_descriptions[3..5],
            &blob_descriptions[5..6],
            &blob_descriptions[6..7],
            &blob_descriptions[7..],
        ];

        let mut planner = StreamingVectoredReadPlanner::new(max_read_size, 1000);
        let mut reads = Vec::new();
        for (key, lsn, offset, _) in blob_descriptions.clone() {
            reads.extend(planner.handle(key, lsn, offset));
        }
        reads.extend(planner.handle_range_end(652 * 1024));

        assert_eq!(reads.len(), ranges.len());

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }

    #[test]
    fn streaming_planner_max_cnt_test() {
        let max_read_size = 1024 * 1024;
        let key = Key::MIN;
        let lsn = Lsn(0);

        let blob_descriptions = vec![
            (key, lsn, 0, BlobFlag::None),
            (key, lsn, 32 * 1024, BlobFlag::None),
            (key, lsn, 96 * 1024, BlobFlag::None),
            (key, lsn, 128 * 1024, BlobFlag::None),
            (key, lsn, 198 * 1024, BlobFlag::None),
            (key, lsn, 268 * 1024, BlobFlag::None),
            (key, lsn, 396 * 1024, BlobFlag::None),
            (key, lsn, 652 * 1024, BlobFlag::None),
        ];

        let ranges = [
            &blob_descriptions[0..2],
            &blob_descriptions[2..4],
            &blob_descriptions[4..6],
            &blob_descriptions[6..],
        ];

        let mut planner = StreamingVectoredReadPlanner::new(max_read_size, 2);
        let mut reads = Vec::new();
        for (key, lsn, offset, _) in blob_descriptions.clone() {
            reads.extend(planner.handle(key, lsn, offset));
        }
        reads.extend(planner.handle_range_end(652 * 1024));

        assert_eq!(reads.len(), ranges.len());

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }

    #[test]
    fn streaming_planner_edge_test() {
        let max_read_size = 1024 * 1024;
        let key = Key::MIN;
        let lsn = Lsn(0);
        {
            let mut planner = StreamingVectoredReadPlanner::new(max_read_size, 1);
            let mut reads = Vec::new();
            reads.extend(planner.handle_range_end(652 * 1024));
            assert!(reads.is_empty());
        }
        {
            let mut planner = StreamingVectoredReadPlanner::new(max_read_size, 1);
            let mut reads = Vec::new();
            reads.extend(planner.handle(key, lsn, 0));
            reads.extend(planner.handle_range_end(652 * 1024));
            assert_eq!(reads.len(), 1);
            validate_read(&reads[0], &[(key, lsn, 0, BlobFlag::None)]);
        }
        {
            let mut planner = StreamingVectoredReadPlanner::new(max_read_size, 1);
            let mut reads = Vec::new();
            reads.extend(planner.handle(key, lsn, 0));
            reads.extend(planner.handle(key, lsn, 128 * 1024));
            reads.extend(planner.handle_range_end(652 * 1024));
            assert_eq!(reads.len(), 2);
            validate_read(&reads[0], &[(key, lsn, 0, BlobFlag::None)]);
            validate_read(&reads[1], &[(key, lsn, 128 * 1024, BlobFlag::None)]);
        }
        {
            let mut planner = StreamingVectoredReadPlanner::new(max_read_size, 2);
            let mut reads = Vec::new();
            reads.extend(planner.handle(key, lsn, 0));
            reads.extend(planner.handle(key, lsn, 128 * 1024));
            reads.extend(planner.handle_range_end(652 * 1024));
            assert_eq!(reads.len(), 1);
            validate_read(
                &reads[0],
                &[
                    (key, lsn, 0, BlobFlag::None),
                    (key, lsn, 128 * 1024, BlobFlag::None),
                ],
            );
        }
    }

    async fn round_trip_test_compressed(blobs: &[Vec<u8>], compression: bool) -> Result<(), Error> {
        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);
        let (_temp_dir, pathbuf, offsets) =
            write_maybe_compressed(blobs, compression, &ctx).await?;

        let file = VirtualFile::open_v2(&pathbuf, &ctx).await?;
        let file_len = std::fs::metadata(&pathbuf)?.len();

        // Multiply by two (compressed data might need more space), and add a few bytes for the header
        let reserved_bytes = blobs.iter().map(|bl| bl.len()).max().unwrap() * 2 + 16;
        let mut buf = IoBufferMut::with_capacity(reserved_bytes);

        let vectored_blob_reader = VectoredBlobReader::new(&file);
        let meta = BlobMeta {
            key: Key::MIN,
            lsn: Lsn(0),
        };

        for (idx, (blob, offset)) in blobs.iter().zip(offsets.iter()).enumerate() {
            let end = offsets.get(idx + 1).unwrap_or(&file_len);
            if idx + 1 == offsets.len() {
                continue;
            }
            let read_builder = ChunkedVectoredReadBuilder::new(*offset, *end, meta, 16 * 4096);
            let read = read_builder.build();
            let result = vectored_blob_reader.read_blobs(&read, buf, &ctx).await?;
            assert_eq!(result.blobs.len(), 1);
            let read_blob = &result.blobs[0];
            let view = BufView::new_slice(&result.buf);
            let read_buf = read_blob.read(&view).await?;
            assert_eq!(
                &blob[..],
                &read_buf[..],
                "mismatch for idx={idx} at offset={offset}"
            );
            buf = result.buf;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_really_big_array() -> Result<(), Error> {
        let blobs = &[
            b"test".to_vec(),
            random_array(10 * PAGE_SZ),
            b"hello".to_vec(),
            random_array(66 * PAGE_SZ),
            vec![0xf3; 24 * PAGE_SZ],
            b"foobar".to_vec(),
        ];
        round_trip_test_compressed(blobs, false).await?;
        round_trip_test_compressed(blobs, true).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_arrays_inc() -> Result<(), Error> {
        let blobs = (0..PAGE_SZ / 8)
            .map(|v| random_array(v * 16))
            .collect::<Vec<_>>();
        round_trip_test_compressed(&blobs, false).await?;
        round_trip_test_compressed(&blobs, true).await?;
        Ok(())
    }
}
