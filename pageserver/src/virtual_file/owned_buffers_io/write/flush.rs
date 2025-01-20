use std::{marker::PhantomData, sync::Arc};

use utils::sync::duplex;

use crate::{
    context::RequestContext,
    virtual_file::owned_buffers_io::{io_buf_aligned::IoBufAligned, io_buf_ext::FullSlice},
};

use super::{Buffer, CheapCloneForRead, OwnedAsyncWriter};

/// A handle to the flush task.
pub struct FlushHandle<Buf, W> {
    inner: Option<FlushHandleInner<Buf, W>>,
    /// Immutable buffer for serving tail reads.
    /// `None` if no flush request has been submitted.
    pub(super) maybe_flushed: Option<FullSlice<Buf>>,
}

pub struct FlushHandleInner<Buf, W> {
    /// A bi-directional channel that sends (buffer, offset) for writes,
    /// and receives recyled buffer.
    channel: duplex::mpsc::Duplex<FlushRequest<Buf>, FullSlice<Buf>>,
    /// Join handle for the background flush task.
    join_handle: tokio::task::JoinHandle<std::io::Result<RequestContext>>,

    _phantom: PhantomData<W>,
}

struct FlushRequest<Buf> {
    slice: FullSlice<Buf>,
    offset: u64,
    #[cfg(test)]
    ready_to_flush_rx: tokio::sync::oneshot::Receiver<()>,
    #[cfg(test)]
    done_flush_tx: tokio::sync::oneshot::Sender<()>,
}

/// Constructs a request and a control object for a new flush operation.
#[cfg(not(test))]
fn new_flush_op<Buf>(slice: FullSlice<Buf>, offset: u64) -> (FlushRequest<Buf>, FlushControl) {
    let request = FlushRequest { slice, offset };
    let control = FlushControl::untracked();

    (request, control)
}

/// Constructs a request and a control object for a new flush operation.
#[cfg(test)]
fn new_flush_op<Buf>(slice: FullSlice<Buf>, offset: u64) -> (FlushRequest<Buf>, FlushControl) {
    let (ready_to_flush_tx, ready_to_flush_rx) = tokio::sync::oneshot::channel();
    let (done_flush_tx, done_flush_rx) = tokio::sync::oneshot::channel();
    let control = FlushControl::not_started(ready_to_flush_tx, done_flush_rx);

    let request = FlushRequest {
        slice,
        offset,
        ready_to_flush_rx,
        done_flush_tx,
    };
    (request, control)
}

/// A handle to a `FlushRequest` that allows unit tests precise control over flush behavior.
#[cfg(test)]
pub(crate) struct FlushControl {
    not_started: FlushNotStarted,
}

#[cfg(not(test))]
pub(crate) struct FlushControl;

impl FlushControl {
    #[cfg(test)]
    fn not_started(
        ready_to_flush_tx: tokio::sync::oneshot::Sender<()>,
        done_flush_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        FlushControl {
            not_started: FlushNotStarted {
                ready_to_flush_tx,
                done_flush_rx,
            },
        }
    }

    #[cfg(not(test))]
    fn untracked() -> Self {
        FlushControl
    }

    /// In tests, turn flush control into a not started state.
    #[cfg(test)]
    pub(crate) fn into_not_started(self) -> FlushNotStarted {
        self.not_started
    }

    /// Release control to the submitted buffer.
    ///
    /// In `cfg(test)` environment, the buffer is guranteed to be flushed to disk after [`FlushControl::release`] is finishes execution.
    pub async fn release(self) {
        #[cfg(test)]
        {
            self.not_started
                .ready_to_flush()
                .wait_until_flush_is_done()
                .await;
        }
    }
}

impl<Buf, W> FlushHandle<Buf, W>
where
    Buf: IoBufAligned + Send + Sync + CheapCloneForRead,
    W: OwnedAsyncWriter + Send + Sync + 'static + std::fmt::Debug,
{
    /// Spawns a new background flush task and obtains a handle.
    ///
    /// Note: The background task so we do not need to explicitly maintain a queue of buffers.
    pub fn spawn_new<B>(
        file: Arc<W>,
        buf: B,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: RequestContext,
    ) -> Self
    where
        B: Buffer<IoBuf = Buf> + Send + 'static,
    {
        // It is fine to buffer up to only 1 message. We only 1 message in-flight at a time.
        let (front, back) = duplex::mpsc::channel(1);

        let join_handle = tokio::spawn(async move {
            FlushBackgroundTask::new(back, file, gate_guard, ctx)
                .run(buf.flush())
                .await
        });

        FlushHandle {
            inner: Some(FlushHandleInner {
                channel: front,
                join_handle,
                _phantom: PhantomData,
            }),
            maybe_flushed: None,
        }
    }

    /// Submits a buffer to be flushed in the background task.
    /// Returns a buffer that completed flushing for re-use, length reset to 0, capacity unchanged.
    /// If `save_buf_for_read` is true, then we save the buffer in `Self::maybe_flushed`, otherwise
    /// clear `maybe_flushed`.
    pub async fn flush<B>(&mut self, buf: B, offset: u64) -> std::io::Result<(B, FlushControl)>
    where
        B: Buffer<IoBuf = Buf> + Send + 'static,
    {
        let slice = buf.flush();

        // Saves a buffer for read while flushing. This also removes reference to the old buffer.
        self.maybe_flushed = Some(slice.cheap_clone());

        let (request, flush_control) = new_flush_op(slice, offset);

        // Submits the buffer to the background task.
        let submit = self.inner_mut().channel.send(request).await;
        if submit.is_err() {
            return self.handle_error().await;
        }

        // Wait for an available buffer from the background flush task.
        // This is the BACKPRESSURE mechanism: if the flush task can't keep up,
        // then the write path will eventually wait for it here.
        let Some(recycled) = self.inner_mut().channel.recv().await else {
            return self.handle_error().await;
        };

        // The only other place that could hold a reference to the recycled buffer
        // is in `Self::maybe_flushed`, but we have already replace it with the new buffer.
        let recycled = Buffer::reuse_after_flush(recycled.into_raw_slice().into_inner());
        Ok((recycled, flush_control))
    }

    pub(super) async fn handle_error<T>(&mut self) -> std::io::Result<T> {
        Err(self
            .shutdown()
            .await
            .expect_err("flush task only disconnects duplex if it exits with an error"))
    }

    /// Cleans up the channel, join the flush task.
    pub async fn shutdown(&mut self) -> std::io::Result<RequestContext> {
        let handle = self
            .inner
            .take()
            .expect("must not use after we returned an error");
        drop(handle.channel.tx);
        handle.join_handle.await.unwrap()
    }

    /// Gets a mutable reference to the inner handle. Panics if [`Self::inner`] is `None`.
    /// This only happens if the handle is used after an error.
    fn inner_mut(&mut self) -> &mut FlushHandleInner<Buf, W> {
        self.inner
            .as_mut()
            .expect("must not use after we returned an error")
    }
}

/// A background task for flushing data to disk.
pub struct FlushBackgroundTask<Buf, W> {
    /// A bi-directional channel that receives (buffer, offset) for writes,
    /// and send back recycled buffer.
    channel: duplex::mpsc::Duplex<FullSlice<Buf>, FlushRequest<Buf>>,
    /// A writter for persisting data to disk.
    writer: Arc<W>,
    ctx: RequestContext,
    /// Prevent timeline from shuting down until the flush background task finishes flushing all remaining buffers to disk.
    _gate_guard: utils::sync::gate::GateGuard,
}

impl<Buf, W> FlushBackgroundTask<Buf, W>
where
    Buf: IoBufAligned + Send + Sync,
    W: OwnedAsyncWriter + Sync + 'static,
{
    /// Creates a new background flush task.
    fn new(
        channel: duplex::mpsc::Duplex<FullSlice<Buf>, FlushRequest<Buf>>,
        file: Arc<W>,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: RequestContext,
    ) -> Self {
        FlushBackgroundTask {
            channel,
            writer: file,
            _gate_guard: gate_guard,
            ctx,
        }
    }

    /// Runs the background flush task.
    /// The passed in slice is immediately sent back to the flush handle through the duplex channel.
    async fn run(mut self, slice: FullSlice<Buf>) -> std::io::Result<RequestContext> {
        // Sends the extra buffer back to the handle.
        self.channel.send(slice).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "flush handle closed early")
        })?;

        //  Exit condition: channel is closed and there is no remaining buffer to be flushed
        while let Some(request) = self.channel.recv().await {
            #[cfg(test)]
            {
                // In test, wait for control to signal that we are ready to flush.
                if request.ready_to_flush_rx.await.is_err() {
                    tracing::debug!("control dropped");
                }
            }

            // Write slice to disk at `offset`.
            let slice = self
                .writer
                .write_all_at(request.slice, request.offset, &self.ctx)
                .await?;

            #[cfg(test)]
            {
                // In test, tell control we are done flushing buffer.
                if request.done_flush_tx.send(()).is_err() {
                    tracing::debug!("control dropped");
                }
            }

            // Sends the buffer back to the handle for reuse. The handle is in charged of cleaning the buffer.
            if self.channel.send(slice).await.is_err() {
                // Although channel is closed. Still need to finish flushing the remaining buffers.
                continue;
            }
        }
        Ok(self.ctx)
    }
}

#[cfg(test)]
pub(crate) struct FlushNotStarted {
    ready_to_flush_tx: tokio::sync::oneshot::Sender<()>,
    done_flush_rx: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct FlushInProgress {
    done_flush_rx: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct FlushDone;

#[cfg(test)]
impl FlushNotStarted {
    /// Signals the background task the buffer is ready to flush to disk.
    pub fn ready_to_flush(self) -> FlushInProgress {
        self.ready_to_flush_tx
            .send(())
            .map(|_| FlushInProgress {
                done_flush_rx: self.done_flush_rx,
            })
            .unwrap()
    }
}

#[cfg(test)]
impl FlushInProgress {
    /// Waits until background flush is done.
    pub async fn wait_until_flush_is_done(self) -> FlushDone {
        let _ = self.done_flush_rx.await;
        FlushDone
    }
}
