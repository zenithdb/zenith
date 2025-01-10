//!
//! Low-level Block-oriented I/O functions
//!

use super::storage_layer::delta_layer::{Adapter, DeltaLayerInner};
use crate::context::RequestContext;
use crate::page_cache::{self, FileId, PageReadGuard, PageWriteGuard, ReadBufResult, PAGE_SZ};
#[cfg(test)]
use crate::virtual_file::IoBufferMut;
use crate::virtual_file::VirtualFile;
use bytes::Bytes;
use std::ops::Deref;

/// This is implemented by anything that can read 8 kB (PAGE_SZ)
/// blocks, using the page cache
///
/// There are currently two implementations: EphemeralFile, and FileBlockReader
/// below.
pub trait BlockReader {
    ///
    /// Create a new "cursor" for reading from this reader.
    ///
    /// A cursor caches the last accessed page, allowing for faster
    /// access if the same block is accessed repeatedly.
    fn block_cursor(&self) -> BlockCursor<'_>;
}

impl<B> BlockReader for &B
where
    B: BlockReader,
{
    fn block_cursor(&self) -> BlockCursor<'_> {
        (*self).block_cursor()
    }
}

/// Reference to an in-memory copy of an immutable on-disk block.
pub enum BlockLease<'a> {
    PageReadGuard(PageReadGuard<'static>),
    EphemeralFileMutableTail(&'a [u8; PAGE_SZ]),
    Slice(&'a [u8; PAGE_SZ]),
    #[cfg(test)]
    Arc(std::sync::Arc<[u8; PAGE_SZ]>),
    #[cfg(test)]
    IoBufferMut(IoBufferMut),
}

impl From<PageReadGuard<'static>> for BlockLease<'static> {
    fn from(value: PageReadGuard<'static>) -> BlockLease<'static> {
        BlockLease::PageReadGuard(value)
    }
}

#[cfg(test)]
impl From<std::sync::Arc<[u8; PAGE_SZ]>> for BlockLease<'_> {
    fn from(value: std::sync::Arc<[u8; PAGE_SZ]>) -> Self {
        BlockLease::Arc(value)
    }
}

impl Deref for BlockLease<'_> {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        match self {
            BlockLease::PageReadGuard(v) => v.deref(),
            BlockLease::EphemeralFileMutableTail(v) => v,
            BlockLease::Slice(v) => v,
            #[cfg(test)]
            BlockLease::Arc(v) => v.deref(),
            #[cfg(test)]
            BlockLease::IoBufferMut(v) => {
                TryFrom::try_from(&v[..]).expect("caller must ensure that v has PAGE_SZ")
            }
        }
    }
}

/// Provides the ability to read blocks from different sources,
/// similar to using traits for this purpose.
///
/// Unlike traits, we also support the read function to be async though.
pub(crate) enum BlockReaderRef<'a> {
    FileBlockReader(&'a FileBlockReader<'a>),
    Adapter(Adapter<&'a DeltaLayerInner>),
    #[cfg(test)]
    TestDisk(&'a super::disk_btree::tests::TestDisk),
    #[cfg(test)]
    VirtualFile(&'a VirtualFile),
}

impl BlockReaderRef<'_> {
    #[inline(always)]
    async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, std::io::Error> {
        use BlockReaderRef::*;
        match self {
            FileBlockReader(r) => r.read_blk(blknum, ctx).await,
            Adapter(r) => r.read_blk(blknum, ctx).await,
            #[cfg(test)]
            TestDisk(r) => r.read_blk(blknum),
            #[cfg(test)]
            VirtualFile(r) => r.read_blk(blknum, ctx).await,
        }
    }
}

///
/// A "cursor" for efficiently reading multiple pages from a BlockReader
///
/// You can access the last page with `*cursor`. 'read_blk' returns 'self', so
/// that in many cases you can use a BlockCursor as a drop-in replacement for
/// the underlying BlockReader. For example:
///
/// ```no_run
/// # use pageserver::tenant::block_io::{BlockReader, FileBlockReader};
/// # use pageserver::context::RequestContext;
/// # let reader: FileBlockReader = unimplemented!("stub");
/// # let ctx: RequestContext = unimplemented!("stub");
/// let cursor = reader.block_cursor();
/// let buf = cursor.read_blk(1, &ctx);
/// // do stuff with 'buf'
/// let buf = cursor.read_blk(2, &ctx);
/// // do stuff with 'buf'
/// ```
///
pub struct BlockCursor<'a> {
    pub(super) read_compressed: bool,
    reader: BlockReaderRef<'a>,
}

impl<'a> BlockCursor<'a> {
    pub(crate) fn new(reader: BlockReaderRef<'a>) -> Self {
        Self::new_with_compression(reader, false)
    }
    pub(crate) fn new_with_compression(reader: BlockReaderRef<'a>, read_compressed: bool) -> Self {
        BlockCursor {
            read_compressed,
            reader,
        }
    }
    // Needed by cli
    pub fn new_fileblockreader(reader: &'a FileBlockReader) -> Self {
        BlockCursor {
            read_compressed: false,
            reader: BlockReaderRef::FileBlockReader(reader),
        }
    }

    /// Read a block.
    ///
    /// Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    #[inline(always)]
    pub async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, std::io::Error> {
        self.reader.read_blk(blknum, ctx).await
    }
}

/// An adapter for reading a (virtual) file using the page cache.
///
/// The file is assumed to be immutable. This doesn't provide any functions
/// for modifying the file, nor for invalidating the cache if it is modified.
#[derive(Clone)]
pub struct FileBlockReader<'a> {
    pub file: &'a VirtualFile,

    /// Unique ID of this file, used as key in the page cache.
    file_id: page_cache::FileId,

    compressed_reads: bool,
}

impl<'a> FileBlockReader<'a> {
    pub fn new(file: &'a VirtualFile, file_id: FileId) -> Self {
        FileBlockReader {
            file_id,
            file,
            compressed_reads: true,
        }
    }

    /// Read a page from the underlying file into given buffer.
    async fn fill_buffer(
        &self,
        buf: PageWriteGuard<'static>,
        blkno: u32,
        ctx: &RequestContext,
    ) -> Result<PageWriteGuard<'static>, std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        self.file
            .read_exact_at_page(buf, blkno as u64 * PAGE_SZ as u64, ctx)
            .await
    }
    /// Read a block.
    ///
    /// Returns a "lease" object that can be used to
    /// access to the contents of the page. (For the page cache, the
    /// lease object represents a lock on the buffer.)
    pub async fn read_blk<'b>(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease<'b>, std::io::Error> {
        let cache = page_cache::get();
        match cache
            .read_immutable_buf(self.file_id, blknum, ctx)
            .await
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read immutable buf: {e:#}"),
                )
            })? {
            ReadBufResult::Found(guard) => Ok(guard.into()),
            ReadBufResult::NotFound(write_guard) => {
                // Read the page from disk into the buffer
                let write_guard = self.fill_buffer(write_guard, blknum, ctx).await?;
                Ok(write_guard.mark_valid().into())
            }
        }
    }
}

impl BlockReader for FileBlockReader<'_> {
    fn block_cursor(&self) -> BlockCursor<'_> {
        BlockCursor::new_with_compression(
            BlockReaderRef::FileBlockReader(self),
            self.compressed_reads,
        )
    }
}

///
/// Trait for block-oriented output
///
pub trait BlockWriter {
    ///
    /// Write a page to the underlying storage.
    ///
    /// 'buf' must be of size PAGE_SZ. Returns the block number the page was
    /// written to.
    ///
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error>;
}

///
/// A simple in-memory buffer of blocks.
///
pub struct BlockBuf {
    pub blocks: Vec<Bytes>,
}
impl BlockWriter for BlockBuf {
    fn write_blk(&mut self, buf: Bytes) -> Result<u32, std::io::Error> {
        assert!(buf.len() == PAGE_SZ);
        let blknum = self.blocks.len();
        self.blocks.push(buf);
        Ok(blknum as u32)
    }
}

impl BlockBuf {
    pub fn new() -> Self {
        BlockBuf { blocks: Vec::new() }
    }

    pub fn size(&self) -> u64 {
        (self.blocks.len() * PAGE_SZ) as u64
    }
}
impl Default for BlockBuf {
    fn default() -> Self {
        Self::new()
    }
}
