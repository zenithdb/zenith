//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache::{self, PAGE_SZ};
use crate::tenant::block_io::{BlockCursor, BlockLease, BlockReader};
use crate::virtual_file::VirtualFile;
use camino::Utf8PathBuf;
use std::cmp::min;
use std::fs::OpenOptions;
use std::io::{self, ErrorKind};
use std::ops::DerefMut;
use std::sync::atomic::AtomicU64;
use tracing::*;
use utils::id::{TenantId, TimelineId};

pub struct EphemeralFile {
    page_cache_file_id: page_cache::FileId,

    _tenant_id: TenantId,
    _timeline_id: TimelineId,
    file: VirtualFile,
    len: u64,
    /// An ephemeral file is append-only.
    /// We keep the last page, which can still be modified, in [`Self::mutable_tail`].
    /// The other pages, which can no longer be modified, are accessed through the page cache.
    mutable_tail: [u8; PAGE_SZ],
}

impl EphemeralFile {
    pub async fn create(
        conf: &PageServerConf,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<EphemeralFile, io::Error> {
        static NEXT_FILENAME: AtomicU64 = AtomicU64::new(1);
        let filename_disambiguator =
            NEXT_FILENAME.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let filename = conf
            .timeline_path(&tenant_id, &timeline_id)
            .join(Utf8PathBuf::from(format!(
                "ephemeral-{filename_disambiguator}"
            )));

        let file = VirtualFile::open_with_options(
            &filename,
            OpenOptions::new().read(true).write(true).create(true),
        )
        .await?;

        Ok(EphemeralFile {
            page_cache_file_id: page_cache::next_file_id(),
            _tenant_id: tenant_id,
            _timeline_id: timeline_id,
            file,
            len: 0,
            mutable_tail: [0u8; PAGE_SZ],
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) async fn read_blk(
        &self,
        blknum: u32,
        ctx: &RequestContext,
    ) -> Result<BlockLease, io::Error> {
        let flushed_blknums = 0..self.len / PAGE_SZ as u64;
        if flushed_blknums.contains(&(blknum as u64)) {
            let cache = page_cache::get();
            loop {
                match cache
                    .read_immutable_buf(self.page_cache_file_id, blknum, ctx)
                    .await
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            // order path before error because error is anyhow::Error => might have many contexts
                            format!(
                                "ephemeral file: read immutable page #{}: {}: {:#}",
                                blknum, self.file.path, e,
                            ),
                        )
                    })? {
                    page_cache::ReadBufResult::Found(guard) => {
                        return Ok(BlockLease::PageReadGuard(guard))
                    }
                    page_cache::ReadBufResult::NotFound(mut write_guard) => {
                        let buf: &mut [u8] = write_guard.deref_mut();
                        debug_assert_eq!(buf.len(), PAGE_SZ);
                        self.file
                            .read_exact_at(&mut buf[..], blknum as u64 * PAGE_SZ as u64)
                            .await?;
                        write_guard.mark_valid();

                        // Swap for read lock
                        continue;
                    }
                };
            }
        } else {
            debug_assert_eq!(blknum as u64, self.len / PAGE_SZ as u64);
            Ok(BlockLease::EphemeralFileMutableTail(&self.mutable_tail))
        }
    }

    pub(crate) async fn write_blob(
        &mut self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> Result<u64, io::Error> {
        struct Writer<'a> {
            ephemeral_file: &'a mut EphemeralFile,
            /// The block to which the next [`push_bytes`] will write.
            blknum: u32,
            /// The offset inside the block identified by [`blknum`] to which [`push_bytes`] will write.
            off: usize,
        }
        impl<'a> Writer<'a> {
            fn new(ephemeral_file: &'a mut EphemeralFile) -> io::Result<Writer<'a>> {
                Ok(Writer {
                    blknum: (ephemeral_file.len / PAGE_SZ as u64) as u32,
                    off: (ephemeral_file.len % PAGE_SZ as u64) as usize,
                    ephemeral_file,
                })
            }
            #[inline(always)]
            async fn push_bytes(
                &mut self,
                src: &[u8],
                ctx: &RequestContext,
            ) -> Result<(), io::Error> {
                let mut src_remaining = src;
                while !src_remaining.is_empty() {
                    let dst_remaining = &mut self.ephemeral_file.mutable_tail[self.off..];
                    let n = min(dst_remaining.len(), src_remaining.len());
                    dst_remaining[..n].copy_from_slice(&src_remaining[..n]);
                    self.off += n;
                    src_remaining = &src_remaining[n..];
                    if self.off == PAGE_SZ {
                        match self
                            .ephemeral_file
                            .file
                            .write_all_at(
                                &self.ephemeral_file.mutable_tail,
                                self.blknum as u64 * PAGE_SZ as u64,
                            )
                            .await
                        {
                            Ok(_) => {
                                // Pre-warm the page cache with what we just wrote.
                                // This isn't necessary for coherency/correctness, but it's how we've always done it.
                                let cache = page_cache::get();
                                match cache
                                    .read_immutable_buf(
                                        self.ephemeral_file.page_cache_file_id,
                                        self.blknum,
                                        ctx,
                                    )
                                    .await
                                {
                                    Ok(page_cache::ReadBufResult::Found(_guard)) => {
                                        // This function takes &mut self, so, it shouldn't be possible to reach this point.
                                        unreachable!("we just wrote blknum {} and this function takes &mut self, so, no concurrent read_blk is possible", self.blknum);
                                    }
                                    Ok(page_cache::ReadBufResult::NotFound(mut write_guard)) => {
                                        let buf: &mut [u8] = write_guard.deref_mut();
                                        debug_assert_eq!(buf.len(), PAGE_SZ);
                                        buf.copy_from_slice(&self.ephemeral_file.mutable_tail);
                                        write_guard.mark_valid();
                                        // pre-warm successful
                                    }
                                    Err(e) => {
                                        error!("ephemeral_file write_blob failed to get immutable buf to pre-warm page cache: {e:?}");
                                        // fail gracefully, it's not the end of the world if we can't pre-warm the cache here
                                    }
                                }
                                // Zero the buffer for reuse.
                                // Zeroing is critical for correcntess because the write_blob code below
                                // and similarly read_blk expect zeroed pages.
                                self.ephemeral_file.mutable_tail.fill(0);
                                // This block is done, move to next one.
                                self.blknum += 1;
                                self.off = 0;
                            }
                            Err(e) => {
                                return Err(std::io::Error::new(
                                    ErrorKind::Other,
                                    // order error before path because path is long and error is short
                                    format!(
                                        "ephemeral_file: write_blob: write-back full tail blk #{}: {:#}: {}",
                                        self.blknum,
                                        e,
                                        self.ephemeral_file.file.path,
                                    ),
                                ));
                            }
                        }
                    }
                }
                Ok(())
            }
        }

        let pos = self.len;
        let mut writer = Writer::new(self)?;

        // Write the length field
        if srcbuf.len() < 0x80 {
            // short one-byte length header
            let len_buf = [srcbuf.len() as u8];
            writer.push_bytes(&len_buf, ctx).await?;
        } else {
            let mut len_buf = u32::to_be_bytes(srcbuf.len() as u32);
            len_buf[0] |= 0x80;
            writer.push_bytes(&len_buf, ctx).await?;
        }

        // Write the payload
        writer.push_bytes(srcbuf, ctx).await?;

        if srcbuf.len() < 0x80 {
            self.len += 1;
        } else {
            self.len += 4;
        }
        self.len += srcbuf.len() as u64;

        Ok(pos)
    }
}

/// Does the given filename look like an ephemeral file?
pub fn is_ephemeral_file(filename: &str) -> bool {
    if let Some(rest) = filename.strip_prefix("ephemeral-") {
        rest.parse::<u32>().is_ok()
    } else {
        false
    }
}

impl Drop for EphemeralFile {
    fn drop(&mut self) {
        // There might still be pages in the [`crate::page_cache`] for this file.
        // We leave them there, [`crate::page_cache::PageCache::find_victim`] will evict them when needed.

        // unlink the file
        let res = std::fs::remove_file(&self.file.path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!(
                    "could not remove ephemeral file '{}': {}",
                    self.file.path, e
                );
            }
        }
    }
}

impl BlockReader for EphemeralFile {
    fn block_cursor(&self) -> super::block_io::BlockCursor<'_> {
        BlockCursor::new(super::block_io::BlockReaderRef::EphemeralFile(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;
    use crate::tenant::block_io::{BlockCursor, BlockReaderRef};
    use rand::{thread_rng, RngCore};
    use std::fs;
    use std::str::FromStr;

    fn harness(
        test_name: &str,
    ) -> Result<
        (
            &'static PageServerConf,
            TenantId,
            TimelineId,
            RequestContext,
        ),
        io::Error,
    > {
        let repo_dir = PageServerConf::test_repo_dir(test_name);
        let _ = fs::remove_dir_all(&repo_dir);
        let conf = PageServerConf::dummy_conf(repo_dir);
        // Make a static copy of the config. This can never be free'd, but that's
        // OK in a test.
        let conf: &'static PageServerConf = Box::leak(Box::new(conf));

        let tenant_id = TenantId::from_str("11000000000000000000000000000000").unwrap();
        let timeline_id = TimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&tenant_id, &timeline_id))?;

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        Ok((conf, tenant_id, timeline_id, ctx))
    }

    #[tokio::test]
    async fn test_ephemeral_blobs() -> Result<(), io::Error> {
        let (conf, tenant_id, timeline_id, ctx) = harness("ephemeral_blobs")?;

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id).await?;

        let pos_foo = file.write_blob(b"foo", &ctx).await?;
        assert_eq!(
            b"foo",
            file.block_cursor()
                .read_blob(pos_foo, &ctx)
                .await?
                .as_slice()
        );
        let pos_bar = file.write_blob(b"bar", &ctx).await?;
        assert_eq!(
            b"foo",
            file.block_cursor()
                .read_blob(pos_foo, &ctx)
                .await?
                .as_slice()
        );
        assert_eq!(
            b"bar",
            file.block_cursor()
                .read_blob(pos_bar, &ctx)
                .await?
                .as_slice()
        );

        let mut blobs = Vec::new();
        for i in 0..10000 {
            let data = Vec::from(format!("blob{}", i).as_bytes());
            let pos = file.write_blob(&data, &ctx).await?;
            blobs.push((pos, data));
        }
        // also test with a large blobs
        for i in 0..100 {
            let data = format!("blob{}", i).as_bytes().repeat(100);
            let pos = file.write_blob(&data, &ctx).await?;
            blobs.push((pos, data));
        }

        let cursor = BlockCursor::new(BlockReaderRef::EphemeralFile(&file));
        for (pos, expected) in blobs {
            let actual = cursor.read_blob(pos, &ctx).await?;
            assert_eq!(actual, expected);
        }

        // Test a large blob that spans multiple pages
        let mut large_data = Vec::new();
        large_data.resize(20000, 0);
        thread_rng().fill_bytes(&mut large_data);
        let pos_large = file.write_blob(&large_data, &ctx).await?;
        let result = file.block_cursor().read_blob(pos_large, &ctx).await?;
        assert_eq!(result, large_data);

        Ok(())
    }
}
