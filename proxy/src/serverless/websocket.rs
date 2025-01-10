use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use anyhow::Context as _;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use framed_websockets::{Frame, OpCode, WebSocketServer};
use futures::{Sink, Stream};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use pin_project_lite::pin_project;
use tokio::io::{self, AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};
use tracing::warn;

use crate::cancellation::CancellationHandlerMain;
use crate::config::ProxyConfig;
use crate::context::RequestContext;
use crate::error::{io_error, ReportableError};
use crate::metrics::Metrics;
use crate::proxy::{handle_client, ClientMode, ErrorSource};
use crate::rate_limiter::EndpointRateLimiter;

pin_project! {
    /// This is a wrapper around a [`WebSocketStream`] that
    /// implements [`AsyncRead`] and [`AsyncWrite`].
    pub(crate) struct WebSocketRw<S> {
        #[pin]
        stream: WebSocketServer<S>,
        recv: Bytes,
        send: BytesMut,
    }
}

impl<S> WebSocketRw<S> {
    pub(crate) fn new(stream: WebSocketServer<S>) -> Self {
        Self {
            stream,
            recv: Bytes::new(),
            send: BytesMut::new(),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketRw<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        let mut stream = this.stream;

        ready!(stream.as_mut().poll_ready(cx).map_err(io_error))?;

        this.send.put(buf);
        match stream.as_mut().start_send(Frame::binary(this.send.split())) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(io_error(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.project().stream;
        stream.poll_flush(cx).map_err(io_error)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stream = self.project().stream;
        stream.poll_close(cx).map_err(io_error)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketRw<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let bytes = ready!(self.as_mut().poll_fill_buf(cx))?;
        let len = std::cmp::min(bytes.len(), buf.remaining());
        buf.put_slice(&bytes[..len]);
        self.consume(len);
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncBufRead for WebSocketRw<S> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        // Please refer to poll_fill_buf's documentation.
        const EOF: Poll<io::Result<&[u8]>> = Poll::Ready(Ok(&[]));

        let mut this = self.project();
        loop {
            if !this.recv.chunk().is_empty() {
                let chunk = (*this.recv).chunk();
                return Poll::Ready(Ok(chunk));
            }

            let res = ready!(this.stream.as_mut().poll_next(cx));
            match res.transpose().map_err(io_error)? {
                Some(message) => match message.opcode {
                    OpCode::Ping => {}
                    OpCode::Pong => {}
                    OpCode::Text => {
                        // We expect to see only binary messages.
                        let error = "unexpected text message in the websocket";
                        warn!(length = message.payload.len(), error);
                        return Poll::Ready(Err(io_error(error)));
                    }
                    OpCode::Binary | OpCode::Continuation => {
                        debug_assert!(this.recv.is_empty());
                        *this.recv = message.payload.freeze();
                    }
                    OpCode::Close => return EOF,
                },
                None => return EOF,
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        self.project().recv.advance(amount);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_websocket(
    config: &'static ProxyConfig,
    auth_backend: &'static crate::auth::Backend<'static, ()>,
    ctx: RequestContext,
    websocket: OnUpgrade,
    cancellation_handler: Arc<CancellationHandlerMain>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    hostname: Option<String>,
    cancellations: tokio_util::task::task_tracker::TaskTracker,
) -> anyhow::Result<()> {
    let websocket = websocket.await?;
    let websocket = WebSocketServer::after_handshake(TokioIo::new(websocket));

    let conn_gauge = Metrics::get()
        .proxy
        .client_connections
        .guard(crate::metrics::Protocol::Ws);

    let res = Box::pin(handle_client(
        config,
        auth_backend,
        &ctx,
        cancellation_handler,
        WebSocketRw::new(websocket),
        ClientMode::Websockets { hostname },
        endpoint_rate_limiter,
        conn_gauge,
        cancellations,
    ))
    .await;

    match res {
        Err(e) => {
            // todo: log and push to ctx the error kind
            ctx.set_error_kind(e.get_error_kind());
            Err(e.into())
        }
        Ok(None) => {
            ctx.set_success();
            Ok(())
        }
        Ok(Some(p)) => {
            ctx.set_success();
            ctx.log_connect();
            match p.proxy_pass(&config.connect_to_compute).await {
                Ok(()) => Ok(()),
                Err(ErrorSource::Client(err)) => Err(err).context("client"),
                Err(ErrorSource::Compute(err)) => Err(err).context("compute"),
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::pin::pin;

    use framed_websockets::WebSocketServer;
    use futures::{SinkExt, StreamExt};
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
    use tokio::task::JoinSet;
    use tokio_tungstenite::tungstenite::protocol::Role;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    use super::WebSocketRw;

    #[tokio::test]
    async fn websocket_stream_wrapper_happy_path() {
        let (stream1, stream2) = duplex(1024);

        let mut js = JoinSet::new();

        js.spawn(async move {
            let mut client = WebSocketStream::from_raw_socket(stream1, Role::Client, None).await;

            client
                .send(Message::Binary(b"hello world".to_vec()))
                .await
                .unwrap();

            let message = client.next().await.unwrap().unwrap();
            assert_eq!(message, Message::Binary(b"websockets are cool".to_vec()));

            client.close(None).await.unwrap();
        });

        js.spawn(async move {
            let mut rw = pin!(WebSocketRw::new(WebSocketServer::after_handshake(stream2)));

            let mut buf = vec![0; 1024];
            let n = rw.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"hello world");

            rw.write_all(b"websockets are cool").await.unwrap();
            rw.flush().await.unwrap();

            let n = rw.read_to_end(&mut buf).await.unwrap();
            assert_eq!(n, 0);
        });

        js.join_next().await.unwrap().unwrap();
        js.join_next().await.unwrap().unwrap();
    }
}
