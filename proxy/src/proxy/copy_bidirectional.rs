use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::info;

#[derive(Debug)]
enum TransferState {
    Running(CopyBuffer),
    ShuttingDown(u64),
    Done(u64),
}

#[derive(Debug)]
pub(crate) enum ErrorDirection {
    Read(io::Error),
    Write(io::Error),
}

impl ErrorSource {
    fn from_client(err: ErrorDirection) -> ErrorSource {
        match err {
            ErrorDirection::Read(client) => Self::Client(client),
            ErrorDirection::Write(compute) => Self::Compute(compute),
        }
    }
    fn from_compute(err: ErrorDirection) -> ErrorSource {
        match err {
            ErrorDirection::Write(client) => Self::Client(client),
            ErrorDirection::Read(compute) => Self::Compute(compute),
        }
    }
}

#[derive(Debug)]
pub enum ErrorSource {
    Client(io::Error),
    Compute(io::Error),
}

fn transfer_one_direction<A, B>(
    cx: &mut Context<'_>,
    state: &mut TransferState,
    r: &mut A,
    w: &mut B,
) -> Poll<Result<u64, ErrorDirection>>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut r = Pin::new(r);
    let mut w = Pin::new(w);
    loop {
        match state {
            TransferState::Running(buf) => {
                let count = ready!(buf.poll_copy(cx, r.as_mut(), w.as_mut()))?;
                *state = TransferState::ShuttingDown(count);
            }
            TransferState::ShuttingDown(count) => {
                ready!(w.as_mut().poll_shutdown(cx)).map_err(ErrorDirection::Write)?;
                *state = TransferState::Done(*count);
            }
            TransferState::Done(count) => return Poll::Ready(Ok(*count)),
        }
    }
}

#[tracing::instrument(skip_all)]
pub async fn copy_bidirectional_client_compute<Client, Compute>(
    client: &mut Client,
    compute: &mut Compute,
) -> Result<(u64, u64), ErrorSource>
where
    Client: AsyncRead + AsyncWrite + Unpin + ?Sized,
    Compute: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut client_to_compute = TransferState::Running(CopyBuffer::new());
    let mut compute_to_client = TransferState::Running(CopyBuffer::new());

    poll_fn(|cx| {
        let mut client_to_compute_result =
            transfer_one_direction(cx, &mut client_to_compute, client, compute)
                .map_err(ErrorSource::from_client)?;
        let mut compute_to_client_result =
            transfer_one_direction(cx, &mut compute_to_client, compute, client)
                .map_err(ErrorSource::from_compute)?;

        // TODO: 1 info log, with a enum label for close direction.

        // Early termination checks from compute to client.
        if let TransferState::Done(_) = compute_to_client {
            if let TransferState::Running(buf) = &client_to_compute {
                info!("Compute is done, terminate client");
                // Initiate shutdown
                client_to_compute = TransferState::ShuttingDown(buf.amt);
                client_to_compute_result =
                    transfer_one_direction(cx, &mut client_to_compute, client, compute)
                        .map_err(ErrorSource::from_client)?;
            }
        }

        // Early termination checks from client to compute.
        if let TransferState::Done(_) = client_to_compute {
            if let TransferState::Running(buf) = &compute_to_client {
                info!("Client is done, terminate compute");
                // Initiate shutdown
                compute_to_client = TransferState::ShuttingDown(buf.amt);
                compute_to_client_result =
                    transfer_one_direction(cx, &mut compute_to_client, compute, client)
                        .map_err(ErrorSource::from_compute)?;
            }
        }

        // It is not a problem if ready! returns early ... (comment remains the same)
        let client_to_compute = ready!(client_to_compute_result);
        let compute_to_client = ready!(compute_to_client_result);

        Poll::Ready(Ok((client_to_compute, compute_to_client)))
    })
    .await
}

#[derive(Debug)]
pub(super) struct CopyBuffer {
    read_done: bool,
    need_flush: bool,
    pos: usize,
    cap: usize,
    amt: u64,
    buf: Box<[u8]>,
}
const DEFAULT_BUF_SIZE: usize = 1024;

impl CopyBuffer {
    pub(super) fn new() -> Self {
        Self {
            read_done: false,
            need_flush: false,
            pos: 0,
            cap: 0,
            amt: 0,
            buf: vec![0; DEFAULT_BUF_SIZE].into_boxed_slice(),
        }
    }

    fn poll_fill_buf<R>(
        &mut self,
        cx: &mut Context<'_>,
        reader: Pin<&mut R>,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + ?Sized,
    {
        let me = &mut *self;
        let mut buf = ReadBuf::new(&mut me.buf);
        buf.set_filled(me.cap);

        let res = reader.poll_read(cx, &mut buf);
        if let Poll::Ready(Ok(())) = res {
            let filled_len = buf.filled().len();
            me.read_done = me.cap == filled_len;
            me.cap = filled_len;
        }
        res
    }

    fn poll_write_buf<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<usize, ErrorDirection>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        let me = &mut *self;
        match writer.as_mut().poll_write(cx, &me.buf[me.pos..me.cap]) {
            Poll::Pending => {
                // Top up the buffer towards full if we can read a bit more
                // data - this should improve the chances of a large write
                if !me.read_done && me.cap < me.buf.len() {
                    ready!(me.poll_fill_buf(cx, reader.as_mut())).map_err(ErrorDirection::Read)?;
                }
                Poll::Pending
            }
            res @ Poll::Ready(_) => res.map_err(ErrorDirection::Write),
        }
    }

    pub(super) fn poll_copy<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
    ) -> Poll<Result<u64, ErrorDirection>>
    where
        R: AsyncRead + ?Sized,
        W: AsyncWrite + ?Sized,
    {
        loop {
            // If our buffer is empty, then we need to read some data to
            // continue.
            if self.pos == self.cap && !self.read_done {
                self.pos = 0;
                self.cap = 0;

                match self.poll_fill_buf(cx, reader.as_mut()) {
                    Poll::Ready(Ok(())) => (),
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(ErrorDirection::Read(err))),
                    Poll::Pending => {
                        // Try flushing when the reader has no progress to avoid deadlock
                        // when the reader depends on buffered writer.
                        if self.need_flush {
                            ready!(writer.as_mut().poll_flush(cx))
                                .map_err(ErrorDirection::Write)?;
                            self.need_flush = false;
                        }

                        return Poll::Pending;
                    }
                }
            }

            // If our buffer has some data, let's write it out!
            while self.pos < self.cap {
                let i = ready!(self.poll_write_buf(cx, reader.as_mut(), writer.as_mut()))?;
                if i == 0 {
                    return Poll::Ready(Err(ErrorDirection::Write(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write zero byte into writer",
                    ))));
                }
                self.pos += i;
                self.amt += i as u64;
                self.need_flush = true;
            }

            // If pos larger than cap, this loop will never stop.
            // In particular, user's wrong poll_write implementation returning
            // incorrect written length may lead to thread blocking.
            debug_assert!(
                self.pos <= self.cap,
                "writer returned length larger than input slice"
            );

            // If we've written all the data and we've seen EOF, flush out the
            // data and finish the transfer.
            if self.pos == self.cap && self.read_done {
                ready!(writer.as_mut().poll_flush(cx)).map_err(ErrorDirection::Write)?;
                return Poll::Ready(Ok(self.amt));
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_client_to_compute() {
        let (mut client_client, mut client_proxy) = tokio::io::duplex(8); // Create a mock duplex stream
        let (mut compute_proxy, mut compute_client) = tokio::io::duplex(32); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        client_client.write_all(b"hello").await.unwrap();
        client_client.shutdown().await.unwrap();
        compute_client.write_all(b"Neon").await.unwrap();
        compute_client.shutdown().await.unwrap();

        let result = copy_bidirectional_client_compute(&mut client_proxy, &mut compute_proxy)
            .await
            .unwrap();

        // Assert correct transferred amounts
        let (client_to_compute_count, compute_to_client_count) = result;
        assert_eq!(client_to_compute_count, 5); // 'hello' was transferred
        assert_eq!(compute_to_client_count, 4); // response only partially transferred or not at all
    }

    #[tokio::test]
    async fn test_compute_to_client() {
        let (mut client_client, mut client_proxy) = tokio::io::duplex(32); // Create a mock duplex stream
        let (mut compute_proxy, mut compute_client) = tokio::io::duplex(8); // Create a mock duplex stream

        // Simulate 'a' finishing while there's still data for 'b'
        compute_client.write_all(b"hello").await.unwrap();
        compute_client.shutdown().await.unwrap();
        client_client
            .write_all(b"Neon Serverless Postgres")
            .await
            .unwrap();

        let result = copy_bidirectional_client_compute(&mut client_proxy, &mut compute_proxy)
            .await
            .unwrap();

        // Assert correct transferred amounts
        let (client_to_compute_count, compute_to_client_count) = result;
        assert_eq!(compute_to_client_count, 5); // 'hello' was transferred
        assert!(client_to_compute_count <= 8); // response only partially transferred or not at all
    }
}
