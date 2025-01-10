//! Implementation of append-only file data structure
//! used to keep in-memory layers spilled on disk.

use crate::assert_u64_eq_usize::{U64IsUsize, UsizeIsU64};
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::page_cache;
use crate::tenant::storage_layer::inmemory_layer::vectored_dio_read::File;
use crate::virtual_file::owned_buffers_io::io_buf_aligned::IoBufAlignedMut;
use crate::virtual_file::owned_buffers_io::slice::SliceMutExt;
use crate::virtual_file::owned_buffers_io::write::Buffer;
use crate::virtual_file::{self, owned_buffers_io, IoBufferMut, VirtualFile};
use camino::Utf8PathBuf;
use num_traits::Num;
use pageserver_api::shard::TenantShardId;
use tokio_epoll_uring::{BoundedBuf, Slice};
use tracing::error;

use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use utils::id::TimelineId;

pub struct EphemeralFile {
    _tenant_shard_id: TenantShardId,
    _timeline_id: TimelineId,
    page_cache_file_id: page_cache::FileId,
    bytes_written: u64,
    buffered_writer: owned_buffers_io::write::BufferedWriter<IoBufferMut, VirtualFile>,
    /// Gate guard is held on as long as we need to do operations in the path (delete on drop)
    _gate_guard: utils::sync::gate::GateGuard,
}

const TAIL_SZ: usize = 64 * 1024;

impl EphemeralFile {
    pub async fn create(
        conf: &PageServerConf,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        gate: &utils::sync::gate::Gate,
        ctx: &RequestContext,
    ) -> anyhow::Result<EphemeralFile> {
        static NEXT_FILENAME: AtomicU64 = AtomicU64::new(1);
        let filename_disambiguator =
            NEXT_FILENAME.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let filename = conf
            .timeline_path(&tenant_shard_id, &timeline_id)
            .join(Utf8PathBuf::from(format!(
                "ephemeral-{filename_disambiguator}"
            )));

        let file = Arc::new(
            VirtualFile::open_with_options_v2(
                &filename,
                virtual_file::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true),
                ctx,
            )
            .await?,
        );

        let page_cache_file_id = page_cache::next_file_id(); // XXX get rid, we're not page-caching anymore

        Ok(EphemeralFile {
            _tenant_shard_id: tenant_shard_id,
            _timeline_id: timeline_id,
            page_cache_file_id,
            bytes_written: 0,
            buffered_writer: owned_buffers_io::write::BufferedWriter::new(
                file,
                || IoBufferMut::with_capacity(TAIL_SZ),
                gate.enter()?,
                ctx,
            ),
            _gate_guard: gate.enter()?,
        })
    }
}

impl Drop for EphemeralFile {
    fn drop(&mut self) {
        // unlink the file
        // we are clear to do this, because we have entered a gate
        let path = self.buffered_writer.as_inner().path();
        let res = std::fs::remove_file(path);
        if let Err(e) = res {
            if e.kind() != std::io::ErrorKind::NotFound {
                // just never log the not found errors, we cannot do anything for them; on detach
                // the tenant directory is already gone.
                //
                // not found files might also be related to https://github.com/neondatabase/neon/issues/2442
                error!("could not remove ephemeral file '{path}': {e}");
            }
        }
    }
}

impl EphemeralFile {
    pub(crate) fn len(&self) -> u64 {
        self.bytes_written
    }

    pub(crate) fn page_cache_file_id(&self) -> page_cache::FileId {
        self.page_cache_file_id
    }

    pub(crate) async fn load_to_io_buf(
        &self,
        ctx: &RequestContext,
    ) -> Result<IoBufferMut, io::Error> {
        let size = self.len().into_usize();
        let buf = IoBufferMut::with_capacity(size);
        let (slice, nread) = self.read_exact_at_eof_ok(0, buf.slice_full(), ctx).await?;
        assert_eq!(nread, size);
        let buf = slice.into_inner();
        assert_eq!(buf.len(), nread);
        assert_eq!(buf.capacity(), size, "we shouldn't be reallocating");
        Ok(buf)
    }

    /// Returns the offset at which the first byte of the input was written, for use
    /// in constructing indices over the written value.
    ///
    /// Panics if the write is short because there's no way we can recover from that.
    /// TODO: make upstack handle this as an error.
    pub(crate) async fn write_raw(
        &mut self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<u64> {
        let (pos, control) = self.write_raw_controlled(srcbuf, ctx).await?;
        if let Some(control) = control {
            control.release().await;
        }
        Ok(pos)
    }

    async fn write_raw_controlled(
        &mut self,
        srcbuf: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<(u64, Option<owned_buffers_io::write::FlushControl>)> {
        let pos = self.bytes_written;

        let new_bytes_written = pos.checked_add(srcbuf.len().into_u64()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "write would grow EphemeralFile beyond u64::MAX: len={pos} writen={srcbuf_len}",
                    srcbuf_len = srcbuf.len(),
                ),
            )
        })?;

        // Write the payload
        let (nwritten, control) = self
            .buffered_writer
            .write_buffered_borrowed_controlled(srcbuf, ctx)
            .await?;
        assert_eq!(
            nwritten,
            srcbuf.len(),
            "buffered writer has no short writes"
        );

        self.bytes_written = new_bytes_written;

        Ok((pos, control))
    }
}

impl super::storage_layer::inmemory_layer::vectored_dio_read::File for EphemeralFile {
    async fn read_exact_at_eof_ok<B: IoBufAlignedMut + Send>(
        &self,
        start: u64,
        dst: tokio_epoll_uring::Slice<B>,
        ctx: &RequestContext,
    ) -> std::io::Result<(tokio_epoll_uring::Slice<B>, usize)> {
        let submitted_offset = self.buffered_writer.bytes_submitted();

        let mutable = self.buffered_writer.inspect_mutable();
        let mutable = &mutable[0..mutable.pending()];

        let maybe_flushed = self.buffered_writer.inspect_maybe_flushed();

        let dst_cap = dst.bytes_total().into_u64();
        let end = {
            // saturating_add is correct here because the max file size is u64::MAX, so,
            // if start + dst.len() > u64::MAX, then we know it will be a short read
            let mut end: u64 = start.saturating_add(dst_cap);
            if end > self.bytes_written {
                end = self.bytes_written;
            }
            end
        };

        // inclusive, exclusive
        #[derive(Debug)]
        struct Range<N>(N, N);
        impl<N: Num + Clone + Copy + PartialOrd + Ord> Range<N> {
            fn len(&self) -> N {
                if self.0 > self.1 {
                    N::zero()
                } else {
                    self.1 - self.0
                }
            }
        }

        let (written_range, maybe_flushed_range) = {
            if maybe_flushed.is_some() {
                // [       written       ][ maybe_flushed ][    mutable    ]
                //                        <-   TAIL_SZ   -><-   TAIL_SZ   ->
                //                                         ^
                //                                 `submitted_offset`
                // <++++++ on disk +++++++????????????????>
                (
                    Range(
                        start,
                        std::cmp::min(end, submitted_offset.saturating_sub(TAIL_SZ as u64)),
                    ),
                    Range(
                        std::cmp::max(start, submitted_offset.saturating_sub(TAIL_SZ as u64)),
                        std::cmp::min(end, submitted_offset),
                    ),
                )
            } else {
                // [       written                        ][    mutable    ]
                //                                         <-   TAIL_SZ   ->
                //                                         ^
                //                                 `submitted_offset`
                // <++++++ on disk +++++++++++++++++++++++>
                (
                    Range(start, std::cmp::min(end, submitted_offset)),
                    // zero len
                    Range(submitted_offset, u64::MIN),
                )
            }
        };

        let mutable_range = Range(std::cmp::max(start, submitted_offset), end);

        let dst = if written_range.len() > 0 {
            let file: &VirtualFile = self.buffered_writer.as_inner();
            let bounds = dst.bounds();
            let slice = file
                .read_exact_at(dst.slice(0..written_range.len().into_usize()), start, ctx)
                .await?;
            Slice::from_buf_bounds(Slice::into_inner(slice), bounds)
        } else {
            dst
        };

        let dst = if maybe_flushed_range.len() > 0 {
            let offset_in_buffer = maybe_flushed_range
                .0
                .checked_sub(submitted_offset.saturating_sub(TAIL_SZ as u64))
                .unwrap()
                .into_usize();
            // Checked previously the buffer is Some.
            let maybe_flushed = maybe_flushed.unwrap();
            let to_copy = &maybe_flushed
                [offset_in_buffer..(offset_in_buffer + maybe_flushed_range.len().into_usize())];
            let bounds = dst.bounds();
            let mut view = dst.slice({
                let start = written_range.len().into_usize();
                let end = start
                    .checked_add(maybe_flushed_range.len().into_usize())
                    .unwrap();
                start..end
            });
            view.as_mut_rust_slice_full_zeroed()
                .copy_from_slice(to_copy);
            Slice::from_buf_bounds(Slice::into_inner(view), bounds)
        } else {
            dst
        };

        let dst = if mutable_range.len() > 0 {
            let offset_in_buffer = mutable_range
                .0
                .checked_sub(submitted_offset)
                .unwrap()
                .into_usize();
            let to_copy =
                &mutable[offset_in_buffer..(offset_in_buffer + mutable_range.len().into_usize())];
            let bounds = dst.bounds();
            let mut view = dst.slice({
                let start =
                    written_range.len().into_usize() + maybe_flushed_range.len().into_usize();
                let end = start.checked_add(mutable_range.len().into_usize()).unwrap();
                start..end
            });
            view.as_mut_rust_slice_full_zeroed()
                .copy_from_slice(to_copy);
            Slice::from_buf_bounds(Slice::into_inner(view), bounds)
        } else {
            dst
        };

        // TODO: in debug mode, randomize the remaining bytes in `dst` to catch bugs

        Ok((dst, (end - start).into_usize()))
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

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::context::DownloadBehavior;
    use crate::task_mgr::TaskKind;
    use std::fs;
    use std::str::FromStr;

    fn harness(
        test_name: &str,
    ) -> Result<
        (
            &'static PageServerConf,
            TenantShardId,
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

        let tenant_shard_id = TenantShardId::from_str("11000000000000000000000000000000").unwrap();
        let timeline_id = TimelineId::from_str("22000000000000000000000000000000").unwrap();
        fs::create_dir_all(conf.timeline_path(&tenant_shard_id, &timeline_id))?;

        let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error);

        Ok((conf, tenant_shard_id, timeline_id, ctx))
    }

    #[tokio::test]
    async fn ephemeral_file_holds_gate_open() {
        const FOREVER: std::time::Duration = std::time::Duration::from_secs(5);

        let (conf, tenant_id, timeline_id, ctx) =
            harness("ephemeral_file_holds_gate_open").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &ctx)
            .await
            .unwrap();

        let mut closing = tokio::task::spawn(async move {
            gate.close().await;
        });

        // gate is entered until the ephemeral file is dropped
        // do not start paused tokio-epoll-uring has a sleep loop
        tokio::time::pause();
        tokio::time::timeout(FOREVER, &mut closing)
            .await
            .expect_err("closing cannot complete before dropping");

        // this is a requirement of the reset_tenant functionality: we have to be able to restart a
        // tenant fast, and for that, we need all tenant_dir operations be guarded by entering a gate
        drop(file);

        tokio::time::timeout(FOREVER, &mut closing)
            .await
            .expect("closing completes right away")
            .expect("closing does not panic");
    }

    #[tokio::test]
    async fn test_ephemeral_file_basics() {
        let (conf, tenant_id, timeline_id, ctx) = harness("test_ephemeral_file_basics").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &ctx)
            .await
            .unwrap();

        let mutable = file.buffered_writer.inspect_mutable();
        let cap = mutable.capacity();
        let align = mutable.align();

        let write_nbytes = cap * 2 + cap / 2;

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(write_nbytes)
            .collect();

        let mut value_offsets = Vec::new();
        for range in (0..write_nbytes)
            .step_by(align)
            .map(|start| start..(start + align).min(write_nbytes))
        {
            let off = file.write_raw(&content[range], &ctx).await.unwrap();
            value_offsets.push(off);
        }

        assert_eq!(file.len() as usize, write_nbytes);
        for (i, range) in (0..write_nbytes)
            .step_by(align)
            .map(|start| start..(start + align).min(write_nbytes))
            .enumerate()
        {
            assert_eq!(value_offsets[i], range.start.into_u64());
            let buf = IoBufferMut::with_capacity(range.len());
            let (buf_slice, nread) = file
                .read_exact_at_eof_ok(range.start.into_u64(), buf.slice_full(), &ctx)
                .await
                .unwrap();
            let buf = buf_slice.into_inner();
            assert_eq!(nread, range.len());
            assert_eq!(&buf, &content[range]);
        }

        let file_contents = std::fs::read(file.buffered_writer.as_inner().path()).unwrap();
        assert!(file_contents == content[0..cap * 2]);

        let maybe_flushed_buffer_contents = file.buffered_writer.inspect_maybe_flushed().unwrap();
        assert_eq!(&maybe_flushed_buffer_contents[..], &content[cap..cap * 2]);

        let mutable_buffer_contents = file.buffered_writer.inspect_mutable();
        assert_eq!(mutable_buffer_contents, &content[cap * 2..write_nbytes]);
    }

    #[tokio::test]
    async fn test_flushes_do_happen() {
        let (conf, tenant_id, timeline_id, ctx) = harness("test_flushes_do_happen").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &ctx)
            .await
            .unwrap();

        // mutable buffer and maybe_flushed buffer each has `cap` bytes.
        let cap = file.buffered_writer.inspect_mutable().capacity();

        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(cap * 2 + cap / 2)
            .collect();

        file.write_raw(&content, &ctx).await.unwrap();

        // assert the state is as this test expects it to be
        assert_eq!(
            &file.load_to_io_buf(&ctx).await.unwrap(),
            &content[0..cap * 2 + cap / 2]
        );
        let md = file.buffered_writer.as_inner().path().metadata().unwrap();
        assert_eq!(
            md.len(),
            2 * cap.into_u64(),
            "buffered writer requires one write to be flushed if we write 2.5x buffer capacity"
        );
        assert_eq!(
            &file.buffered_writer.inspect_maybe_flushed().unwrap()[0..cap],
            &content[cap..cap * 2]
        );
        assert_eq!(
            &file.buffered_writer.inspect_mutable()[0..cap / 2],
            &content[cap * 2..cap * 2 + cap / 2]
        );
    }

    #[tokio::test]
    async fn test_read_split_across_file_and_buffer() {
        // This test exercises the logic on the read path that splits the logical read
        // into a read from the flushed part (= the file) and a copy from the buffered writer's buffer.
        //
        // This test build on the assertions in test_flushes_do_happen

        let (conf, tenant_id, timeline_id, ctx) =
            harness("test_read_split_across_file_and_buffer").unwrap();

        let gate = utils::sync::gate::Gate::default();

        let mut file = EphemeralFile::create(conf, tenant_id, timeline_id, &gate, &ctx)
            .await
            .unwrap();

        let mutable = file.buffered_writer.inspect_mutable();
        let cap = mutable.capacity();
        let align = mutable.align();
        let content: Vec<u8> = rand::thread_rng()
            .sample_iter(rand::distributions::Standard)
            .take(cap * 2 + cap / 2)
            .collect();

        let (_, control) = file.write_raw_controlled(&content, &ctx).await.unwrap();

        let test_read = |start: usize, len: usize| {
            let file = &file;
            let ctx = &ctx;
            let content = &content;
            async move {
                let (buf, nread) = file
                    .read_exact_at_eof_ok(
                        start.into_u64(),
                        IoBufferMut::with_capacity(len).slice_full(),
                        ctx,
                    )
                    .await
                    .unwrap();
                assert_eq!(nread, len);
                assert_eq!(&buf.into_inner(), &content[start..(start + len)]);
            }
        };

        let test_read_all_offset_combinations = || {
            async move {
                test_read(align, align).await;
                // border onto edge of file
                test_read(cap - align, align).await;
                // read across file and buffer
                test_read(cap - align, 2 * align).await;
                // stay from start of maybe flushed buffer
                test_read(cap, align).await;
                // completely within maybe flushed buffer
                test_read(cap + align, align).await;
                // border onto edge of maybe flushed buffer.
                test_read(cap * 2 - align, align).await;
                // read across maybe flushed and mutable buffer
                test_read(cap * 2 - align, 2 * align).await;
                // read across three segments
                test_read(cap - align, cap + 2 * align).await;
                // completely within mutable buffer
                test_read(cap * 2 + align, align).await;
            }
        };

        // completely within the file range
        assert!(align < cap, "test assumption");
        assert!(cap % align == 0);

        // test reads at different flush stages.
        let not_started = control.unwrap().into_not_started();
        test_read_all_offset_combinations().await;
        let in_progress = not_started.ready_to_flush();
        test_read_all_offset_combinations().await;
        in_progress.wait_until_flush_is_done().await;
        test_read_all_offset_combinations().await;
    }
}
