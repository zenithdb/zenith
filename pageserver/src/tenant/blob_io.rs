//!
//! Functions for reading and writing variable-sized "blobs".
//!
//! Each blob begins with a 1- or 4-byte length field, followed by the
//! actual data. If the length is smaller than 128 bytes, the length
//! is written as a one byte. If it's larger than that, the length
//! is written as a four-byte integer, in big-endian, with the high
//! bit set. This way, we can detect whether it's 1- or 4-byte header
//! by peeking at the first byte.
//!
//! len <  128: 0XXXXXXX
//! len >= 128: 1XXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX
//!
use crate::page_cache::PAGE_SZ;
use crate::tenant::block_io::BlockCursor;
use crate::virtual_file::VirtualFile;
use std::cmp::min;
use std::io::{Error, ErrorKind};

impl<'a> BlockCursor<'a> {
    /// Read a blob into a new buffer.
    pub async fn read_blob(&self, offset: u64) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        self.read_blob_into_buf(offset, &mut buf).await?;
        Ok(buf)
    }
    /// Read blob into the given buffer. Any previous contents in the buffer
    /// are overwritten.
    pub async fn read_blob_into_buf(
        &self,
        offset: u64,
        dstbuf: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let mut blknum = (offset / PAGE_SZ as u64) as u32;
        let mut off = (offset % PAGE_SZ as u64) as usize;

        let mut buf = self.read_blk(blknum).await?;

        // peek at the first byte, to determine if it's a 1- or 4-byte length
        let first_len_byte = buf[off];
        let len: usize = if first_len_byte < 0x80 {
            // 1-byte length header
            off += 1;
            first_len_byte as usize
        } else {
            // 4-byte length header
            let mut len_buf = [0u8; 4];
            let thislen = PAGE_SZ - off;
            if thislen < 4 {
                // it is split across two pages
                len_buf[..thislen].copy_from_slice(&buf[off..PAGE_SZ]);
                blknum += 1;
                buf = self.read_blk(blknum).await?;
                len_buf[thislen..].copy_from_slice(&buf[0..4 - thislen]);
                off = 4 - thislen;
            } else {
                len_buf.copy_from_slice(&buf[off..off + 4]);
                off += 4;
            }
            len_buf[0] &= 0x7f;
            u32::from_be_bytes(len_buf) as usize
        };

        dstbuf.clear();
        dstbuf.reserve(len);

        // Read the payload
        let mut remain = len;
        while remain > 0 {
            let mut page_remain = PAGE_SZ - off;
            if page_remain == 0 {
                // continue on next page
                blknum += 1;
                buf = self.read_blk(blknum).await?;
                off = 0;
                page_remain = PAGE_SZ;
            }
            let this_blk_len = min(remain, page_remain);
            dstbuf.extend_from_slice(&buf[off..off + this_blk_len]);
            remain -= this_blk_len;
            off += this_blk_len;
        }
        Ok(())
    }
}

/// A wrapper of `VirtualFile` that allows users to write blobs.
///
/// If a `BlobWriter` is dropped, the internal buffer will be
/// discarded. You need to call [`flush_buffer`](Self::flush_buffer)
/// manually before dropping.
pub struct BlobWriter<const BUFFERED: bool> {
    inner: VirtualFile,
    offset: u64,
    /// A buffer to save on read calls
    buf: [u8; PAGE_SZ],
    /// The number of bytes already occupied in buf
    /// In other words: pointer to the first unwritten byte in buf.
    ///
    /// After each `write_all` call concludes, we maintain the
    /// invariant that buf_offs < buf.len(), so the buffer is
    /// never completely full outside of the `write_all` function.
    buf_offs: usize,
}

impl<const BUFFERED: bool> BlobWriter<BUFFERED> {
    pub fn new(inner: VirtualFile, start_offset: u64) -> Self {
        Self {
            inner,
            offset: start_offset,
            buf: [0; PAGE_SZ],
            buf_offs: 0,
        }
    }

    pub fn size(&self) -> u64 {
        self.offset
    }

    #[inline(always)]
    /// Writes the given buffer directly to the underlying `VirtualFile`.
    /// You need to make sure that the internal buffer is empty, otherwise
    /// data will be written in wrong order.
    async fn write_all_unbuffered(&mut self, src_buf: &[u8]) -> Result<(), Error> {
        self.inner.write_all(src_buf).await?;
        self.offset += src_buf.len() as u64;
        Ok(())
    }

    #[inline(always)]
    /// Flushes the internal buffer to the underlying `VirtualFile`.
    pub async fn flush_buffer(&mut self) -> Result<(), Error> {
        self.inner.write_all(&self.buf).await?;
        self.buf_offs = 0;
        Ok(())
    }

    #[inline(always)]
    /// Writes as much of `src_buf` into the internal buffer as it fits
    fn write_into_buffer(&mut self, src_buf: &[u8]) -> usize {
        let remaining = self.buf.len() - self.buf_offs;
        let to_copy = src_buf.len().min(remaining);
        self.buf[..to_copy].copy_from_slice(src_buf);
        self.buf_offs += to_copy;
        self.offset += src_buf.len() as u64;
        to_copy
    }

    /// Internal, possibly buffered, write function
    async fn write_all(&mut self, mut src_buf: &[u8]) -> Result<(), Error> {
        if !BUFFERED {
            if self.buf_offs > 0 {
                // Flush the buffer. This creates a write call for
                // potentially very small data, but there is no way
                // we can unify it with the data we are writing below
                // without copying it.
                self.flush_buffer().await?;
            }
            self.write_all_unbuffered(src_buf).await?;
            return Ok(());
        }
        let remaining = self.buf.len() - self.buf_offs;
        // First try to copy as much as we can into the buffer
        if src_buf.len() <= remaining {
            let copied = self.write_into_buffer(src_buf);
            src_buf = &src_buf[copied..];
        }
        // Then, if the buffer is full, flush it out
        if self.buf.len() == self.buf_offs {
            self.flush_buffer().await?;
        }
        // Finally, write the tail of src_buf:
        // If it wholly fits into the buffer without
        // completely filling it, then put it there.
        // If not, write it out directly.
        if !src_buf.is_empty() {
            assert_eq!(self.buf_offs, 0);
            if src_buf.len() >= self.buf.len() {
                let copied = self.write_into_buffer(src_buf);
                // We just verified above that src_buf fits into our internal buffer.
                assert_eq!(copied, src_buf.len());
            } else {
                self.write_all_unbuffered(src_buf).await?;
            }
        }
        Ok(())
    }

    /// Write a blob of data. Returns the offset that it was written to,
    /// which can be used to retrieve the data later.
    pub async fn write_blob(&mut self, srcbuf: &[u8]) -> Result<u64, Error> {
        let offset = self.offset;

        if srcbuf.len() < 128 {
            // Short blob. Write a 1-byte length header
            let len_buf = srcbuf.len() as u8;
            self.write_all(&[len_buf]).await?;
        } else {
            // Write a 4-byte length header
            if srcbuf.len() > 0x7fff_ffff {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("blob too large ({} bytes)", srcbuf.len()),
                ));
            }
            let mut len_buf = ((srcbuf.len()) as u32).to_be_bytes();
            len_buf[0] |= 0x80;
            self.write_all(&len_buf).await?;
        }
        self.write_all(srcbuf).await?;
        Ok(offset)
    }
}

impl BlobWriter<true> {
    /// Access the underlying `VirtualFile`.
    ///
    /// This function flushes the internal buffer before giving access
    /// to the underlying `VirtualFile`.
    pub async fn into_inner(mut self) -> Result<VirtualFile, Error> {
        self.flush_buffer().await?;
        Ok(self.inner)
    }

    /// Access the underlying `VirtualFile`.
    ///
    /// Unlike [`into_inner`](Self::into_inner), this doesn't flush
    /// the internal buffer before giving access.
    pub fn into_inner_no_flush(self) -> VirtualFile {
        self.inner
    }
}

impl BlobWriter<false> {
    /// Access the underlying `VirtualFile`.
    pub fn into_inner(self) -> VirtualFile {
        self.inner
    }
}
