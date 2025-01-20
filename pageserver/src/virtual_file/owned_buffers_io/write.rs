mod flush;
use std::sync::Arc;

use bytes::BufMut;
use flush::FlushHandle;
use tokio_epoll_uring::IoBuf;

use crate::{
    context::RequestContext,
    virtual_file::{IoBuffer, IoBufferMut},
};

use super::{
    io_buf_aligned::{IoBufAligned, IoBufAlignedMut},
    io_buf_ext::{FullSlice, IoBufExt},
};

pub(crate) use flush::FlushControl;

pub(crate) trait CheapCloneForRead {
    /// Returns a cheap clone of the buffer.
    fn cheap_clone(&self) -> Self;
}

impl CheapCloneForRead for IoBuffer {
    fn cheap_clone(&self) -> Self {
        // Cheap clone over an `Arc`.
        self.clone()
    }
}

/// A trait for doing owned-buffer write IO.
/// Think [`tokio::io::AsyncWrite`] but with owned buffers.
/// The owned buffers need to be aligned due to Direct IO requirements.
pub trait OwnedAsyncWriter {
    fn write_all_at<Buf: IoBufAligned + Send>(
        &self,
        buf: FullSlice<Buf>,
        offset: u64,
        ctx: &RequestContext,
    ) -> impl std::future::Future<Output = std::io::Result<FullSlice<Buf>>> + Send;
}

/// A wrapper aorund an [`OwnedAsyncWriter`] that uses a [`Buffer`] to batch
/// small writes into larger writes of size [`Buffer::cap`].
// TODO(yuchen): For large write, implementing buffer bypass for aligned parts of the write could be beneficial to throughput,
// since we would avoid copying majority of the data into the internal buffer.
pub struct BufferedWriter<B: Buffer, W> {
    writer: Arc<W>,
    /// invariant: always remains Some(buf) except
    /// - while IO is ongoing => goes back to Some() once the IO completed successfully
    /// - after an IO error => stays `None` forever
    ///
    /// In these exceptional cases, it's `None`.
    tail: Option<B>,
    /// A handle to the background flush task for writting data to disk.
    flush_handle: FlushHandle<B::IoBuf, W>,
    /// The next offset to be submitted to the background task.
    submit_offset: u64,
}

impl<B, Buf, W> BufferedWriter<B, W>
where
    B: IoBufAlignedMut + Buffer<IoBuf = Buf> + Send + 'static,
    Buf: IoBufAligned + Send + Sync + CheapCloneForRead,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
{
    /// Creates a new buffered writer.
    ///
    /// The `buf_new` function provides a way to initialize the owned buffers used by this writer.
    pub fn new(
        writer: Arc<W>,
        start_offset: u64,
        buf_new: impl Fn() -> B,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: &RequestContext,
    ) -> Self {
        Self {
            writer: writer.clone(),
            tail: Some(buf_new()),
            flush_handle: FlushHandle::spawn_new(
                writer,
                buf_new(),
                gate_guard,
                ctx.attached_child(),
            ),
            submit_offset: start_offset,
        }
    }

    pub fn as_inner(&self) -> &W {
        &self.writer
    }

    /// Returns the number of bytes submitted to the background flush task.
    pub fn submit_offset(&self) -> u64 {
        self.submit_offset
    }

    /// Panics if used after any of the write paths returned an error
    pub fn inspect_tail(&self) -> &B {
        self.tail()
    }

    /// Gets a reference to the maybe flushed read-only buffer.
    /// Returns `None` if the writer has not submitted any flush request.
    pub fn inspect_maybe_flushed(&self) -> Option<&FullSlice<Buf>> {
        self.flush_handle.maybe_flushed.as_ref()
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn shutdown(
        self,
        mut handle_tail: impl FnMut(B) -> Option<B>,
    ) -> std::io::Result<(u64, W)> {
        let Self {
            tail,
            writer,
            mut flush_handle,
            submit_offset,
        } = self;

        let ctx = flush_handle.shutdown().await?;
        let buf = tail.expect("must not use after an error");
        let writer = Arc::into_inner(writer).expect("writer is the only strong reference");
        let mut bytes_amount = submit_offset;
        if let Some(buf) = handle_tail(buf) {
            bytes_amount += buf.pending() as u64;
            let _ = writer
                .write_all_at(buf.flush(), submit_offset, &ctx)
                .await?;
        }
        Ok((bytes_amount, writer))
    }

    /// Gets a immutable reference to the tail in-memory buffer.
    #[inline(always)]
    fn tail(&self) -> &B {
        self.tail
            .as_ref()
            .expect("must not use after we returned an error")
    }

    /// Gets a mutable reference to the tail in-memory buffer.
    #[inline(always)]
    fn tail_mut(&mut self) -> &mut B {
        self.tail
            .as_mut()
            .expect("must not use after we returned an error")
    }

    #[cfg_attr(target_os = "macos", allow(dead_code))]
    pub async fn write_buffered_borrowed(
        &mut self,
        chunk: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<usize> {
        let (len, control) = self.write_buffered_borrowed_controlled(chunk, ctx).await?;
        if let Some(control) = control {
            control.release().await;
        }
        Ok(len)
    }

    /// In addition to bytes submitted in this write, also returns a handle that can control the flush behavior.
    pub(crate) async fn write_buffered_borrowed_controlled(
        &mut self,
        mut chunk: &[u8],
        ctx: &RequestContext,
    ) -> std::io::Result<(usize, Option<FlushControl>)> {
        let chunk_len = chunk.len();
        let mut control: Option<FlushControl> = None;
        while !chunk.is_empty() {
            let buf = self.tail_mut();
            let need = buf.cap() - buf.pending();
            let have = chunk.len();
            let n = std::cmp::min(need, have);
            buf.extend_from_slice(&chunk[..n]);
            chunk = &chunk[n..];
            if buf.pending() >= buf.cap() {
                assert_eq!(buf.pending(), buf.cap());
                if let Some(control) = control.take() {
                    control.release().await;
                }
                control = self.flush(ctx).await?;
            }
        }
        Ok((chunk_len, control))
    }

    #[must_use = "caller must explcitly check the flush control"]
    async fn flush(&mut self, _ctx: &RequestContext) -> std::io::Result<Option<FlushControl>> {
        let buf = self.tail.take().expect("must not use after an error");
        let buf_len = buf.pending();
        if buf_len == 0 {
            self.tail = Some(buf);
            return Ok(None);
        }
        let (recycled, flush_control) = self.flush_handle.flush(buf, self.submit_offset).await?;
        self.submit_offset += u64::try_from(buf_len).unwrap();
        self.tail = Some(recycled);
        Ok(Some(flush_control))
    }
}

/// A [`Buffer`] is used by [`BufferedWriter`] to batch smaller writes into larger ones.
pub trait Buffer {
    type IoBuf: IoBuf;

    /// Capacity of the buffer. Must not change over the lifetime `self`.`
    fn cap(&self) -> usize;

    /// Add data to the buffer.
    /// Panics if there is not enough room to accomodate `other`'s content, i.e.,
    /// panics if `other.len() > self.cap() - self.pending()`.
    fn extend_from_slice(&mut self, other: &[u8]);

    /// Add `count` bytes `val` into `self`.
    /// Panics if `count > self.cap() - self.pending()`.
    fn extend_with(&mut self, val: u8, count: usize);

    /// Number of bytes in the buffer.
    fn pending(&self) -> usize;

    /// Turns `self` into a [`FullSlice`] of the pending data
    /// so we can use [`tokio_epoll_uring`] to write it to disk.
    fn flush(self) -> FullSlice<Self::IoBuf>;

    /// After the write to disk is done and we have gotten back the slice,
    /// [`BufferedWriter`] uses this method to re-use the io buffer.
    fn reuse_after_flush(iobuf: Self::IoBuf) -> Self;
}

impl Buffer for IoBufferMut {
    type IoBuf = IoBuffer;

    fn cap(&self) -> usize {
        self.capacity()
    }

    fn extend_from_slice(&mut self, other: &[u8]) {
        if self.len() + other.len() > self.cap() {
            panic!("Buffer capacity exceeded");
        }

        IoBufferMut::extend_from_slice(self, other);
    }

    fn extend_with(&mut self, val: u8, count: usize) {
        if self.len() + count > self.cap() {
            panic!("Buffer capacity exceeded");
        }

        IoBufferMut::put_bytes(self, val, count);
    }

    fn pending(&self) -> usize {
        self.len()
    }

    fn flush(self) -> FullSlice<Self::IoBuf> {
        self.freeze().slice_len()
    }

    /// Caller should make sure that `iobuf` only have one strong reference before invoking this method.
    fn reuse_after_flush(iobuf: Self::IoBuf) -> Self {
        let mut recycled = iobuf
            .into_mut()
            .expect("buffer should only have one strong reference");
        recycled.clear();
        recycled
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::context::{DownloadBehavior, RequestContext};
    use crate::task_mgr::TaskKind;

    #[derive(Default, Debug)]
    struct RecorderWriter {
        /// record bytes and write offsets.
        writes: Mutex<Vec<(Vec<u8>, u64)>>,
    }

    impl RecorderWriter {
        /// Gets recorded bytes and write offsets.
        fn get_writes(&self) -> Vec<Vec<u8>> {
            self.writes
                .lock()
                .unwrap()
                .iter()
                .map(|(buf, _)| buf.clone())
                .collect()
        }
    }

    impl OwnedAsyncWriter for RecorderWriter {
        async fn write_all_at<Buf: IoBufAligned + Send>(
            &self,
            buf: FullSlice<Buf>,
            offset: u64,
            _: &RequestContext,
        ) -> std::io::Result<FullSlice<Buf>> {
            self.writes
                .lock()
                .unwrap()
                .push((Vec::from(&buf[..]), offset));
            Ok(buf)
        }
    }

    fn test_ctx() -> RequestContext {
        RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Error)
    }

    #[tokio::test]
    async fn test_write_all_borrowed_always_goes_through_buffer() -> anyhow::Result<()> {
        let ctx = test_ctx();
        let ctx = &ctx;
        let recorder = Arc::new(RecorderWriter::default());
        let gate = utils::sync::gate::Gate::default();
        let mut writer = BufferedWriter::<_, RecorderWriter>::new(
            recorder,
            0,
            || IoBufferMut::with_capacity(2),
            gate.enter()?,
            ctx,
        );

        writer.write_buffered_borrowed(b"abc", ctx).await?;
        writer.write_buffered_borrowed(b"", ctx).await?;
        writer.write_buffered_borrowed(b"d", ctx).await?;
        writer.write_buffered_borrowed(b"e", ctx).await?;
        writer.write_buffered_borrowed(b"fg", ctx).await?;
        writer.write_buffered_borrowed(b"hi", ctx).await?;
        writer.write_buffered_borrowed(b"j", ctx).await?;
        writer.write_buffered_borrowed(b"klmno", ctx).await?;

        let (_, recorder) = writer.shutdown(|buf| Some(buf)).await?;
        assert_eq!(
            recorder.get_writes(),
            {
                let expect: &[&[u8]] = &[b"ab", b"cd", b"ef", b"gh", b"ij", b"kl", b"mn", b"o"];
                expect
            }
            .iter()
            .map(|v| v[..].to_vec())
            .collect::<Vec<_>>()
        );
        Ok(())
    }
}
