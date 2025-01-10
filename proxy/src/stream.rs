use std::pin::Pin;
use std::sync::Arc;
use std::{io, task};

use bytes::BytesMut;
use pq_proto::framed::{ConnectionError, Framed};
use pq_proto::{BeMessage, FeMessage, FeStartupPacket, ProtocolError};
use rustls::ServerConfig;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::server::TlsStream;
use tracing::debug;

use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::metrics::Metrics;
use crate::tls::TlsServerEndPoint;

/// Stream wrapper which implements libpq's protocol.
///
/// NOTE: This object deliberately doesn't implement [`AsyncRead`]
/// or [`AsyncWrite`] to prevent subtle errors (e.g. trying
/// to pass random malformed bytes through the connection).
pub struct PqStream<S> {
    pub(crate) framed: Framed<S>,
}

impl<S> PqStream<S> {
    /// Construct a new libpq protocol wrapper.
    pub fn new(stream: S) -> Self {
        Self {
            framed: Framed::new(stream),
        }
    }

    /// Extract the underlying stream and read buffer.
    pub fn into_inner(self) -> (S, BytesMut) {
        self.framed.into_inner()
    }

    /// Get a shared reference to the underlying stream.
    pub(crate) fn get_ref(&self) -> &S {
        self.framed.get_ref()
    }
}

fn err_connection() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "connection is lost")
}

impl<S: AsyncRead + Unpin> PqStream<S> {
    /// Receive [`FeStartupPacket`], which is a first packet sent by a client.
    pub async fn read_startup_packet(&mut self) -> io::Result<FeStartupPacket> {
        self.framed
            .read_startup_message()
            .await
            .map_err(ConnectionError::into_io_error)?
            .ok_or_else(err_connection)
    }

    async fn read_message(&mut self) -> io::Result<FeMessage> {
        self.framed
            .read_message()
            .await
            .map_err(ConnectionError::into_io_error)?
            .ok_or_else(err_connection)
    }

    pub(crate) async fn read_password_message(&mut self) -> io::Result<bytes::Bytes> {
        match self.read_message().await? {
            FeMessage::PasswordMessage(msg) => Ok(msg),
            bad => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected message type: {bad:?}"),
            )),
        }
    }
}

#[derive(Debug)]
pub struct ReportedError {
    source: anyhow::Error,
    error_kind: ErrorKind,
}

impl std::fmt::Display for ReportedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for ReportedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

impl ReportableError for ReportedError {
    fn get_error_kind(&self) -> ErrorKind {
        self.error_kind
    }
}

impl<S: AsyncWrite + Unpin> PqStream<S> {
    /// Write the message into an internal buffer, but don't flush the underlying stream.
    pub(crate) fn write_message_noflush(
        &mut self,
        message: &BeMessage<'_>,
    ) -> io::Result<&mut Self> {
        self.framed
            .write_message(message)
            .map_err(ProtocolError::into_io_error)?;
        Ok(self)
    }

    /// Write the message into an internal buffer and flush it.
    pub async fn write_message(&mut self, message: &BeMessage<'_>) -> io::Result<&mut Self> {
        self.write_message_noflush(message)?;
        self.flush().await?;
        Ok(self)
    }

    /// Flush the output buffer into the underlying stream.
    pub(crate) async fn flush(&mut self) -> io::Result<&mut Self> {
        self.framed.flush().await?;
        Ok(self)
    }

    /// Write the error message using [`Self::write_message`], then re-throw it.
    /// Allowing string literals is safe under the assumption they might not contain any runtime info.
    /// This method exists due to `&str` not implementing `Into<anyhow::Error>`.
    pub async fn throw_error_str<T>(
        &mut self,
        msg: &'static str,
        error_kind: ErrorKind,
    ) -> Result<T, ReportedError> {
        // TODO: only log this for actually interesting errors
        tracing::info!(
            kind = error_kind.to_metric_label(),
            msg,
            "forwarding error to user"
        );

        // already error case, ignore client IO error
        self.write_message(&BeMessage::ErrorResponse(msg, None))
            .await
            .inspect_err(|e| debug!("write_message failed: {e}"))
            .ok();

        Err(ReportedError {
            source: anyhow::anyhow!(msg),
            error_kind,
        })
    }

    /// Write the error message using [`Self::write_message`], then re-throw it.
    /// Trait [`UserFacingError`] acts as an allowlist for error types.
    pub(crate) async fn throw_error<T, E>(&mut self, error: E) -> Result<T, ReportedError>
    where
        E: UserFacingError + Into<anyhow::Error>,
    {
        let error_kind = error.get_error_kind();
        let msg = error.to_string_client();
        tracing::info!(
            kind=error_kind.to_metric_label(),
            error=%error,
            msg,
            "forwarding error to user"
        );

        // already error case, ignore client IO error
        self.write_message(&BeMessage::ErrorResponse(&msg, None))
            .await
            .inspect_err(|e| debug!("write_message failed: {e}"))
            .ok();

        Err(ReportedError {
            source: anyhow::anyhow!(error),
            error_kind,
        })
    }
}

/// Wrapper for upgrading raw streams into secure streams.
pub enum Stream<S> {
    /// We always begin with a raw stream,
    /// which may then be upgraded into a secure stream.
    Raw { raw: S },
    Tls {
        /// We box [`TlsStream`] since it can be quite large.
        tls: Box<TlsStream<S>>,
        /// Channel binding parameter
        tls_server_end_point: TlsServerEndPoint,
    },
}

impl<S: Unpin> Unpin for Stream<S> {}

impl<S> Stream<S> {
    /// Construct a new instance from a raw stream.
    pub fn from_raw(raw: S) -> Self {
        Self::Raw { raw }
    }

    /// Return SNI hostname when it's available.
    pub fn sni_hostname(&self) -> Option<&str> {
        match self {
            Stream::Raw { .. } => None,
            Stream::Tls { tls, .. } => tls.get_ref().1.server_name(),
        }
    }

    pub(crate) fn tls_server_end_point(&self) -> TlsServerEndPoint {
        match self {
            Stream::Raw { .. } => TlsServerEndPoint::Undefined,
            Stream::Tls {
                tls_server_end_point,
                ..
            } => *tls_server_end_point,
        }
    }
}

#[derive(Debug, Error)]
#[error("Can't upgrade TLS stream")]
pub enum StreamUpgradeError {
    #[error("Bad state reached: can't upgrade TLS stream")]
    AlreadyTls,

    #[error("Can't upgrade stream: IO error: {0}")]
    Io(#[from] io::Error),
}

impl<S: AsyncRead + AsyncWrite + Unpin> Stream<S> {
    /// If possible, upgrade raw stream into a secure TLS-based stream.
    pub async fn upgrade(
        self,
        cfg: Arc<ServerConfig>,
        record_handshake_error: bool,
    ) -> Result<TlsStream<S>, StreamUpgradeError> {
        match self {
            Stream::Raw { raw } => Ok(tokio_rustls::TlsAcceptor::from(cfg)
                .accept(raw)
                .await
                .inspect_err(|_| {
                    if record_handshake_error {
                        Metrics::get().proxy.tls_handshake_failures.inc();
                    }
                })?),
            Stream::Tls { .. } => Err(StreamUpgradeError::AlreadyTls),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Stream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_read(context, buf),
            Self::Tls { tls, .. } => Pin::new(tls).poll_read(context, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Stream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
        buf: &[u8],
    ) -> task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_write(context, buf),
            Self::Tls { tls, .. } => Pin::new(tls).poll_write(context, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_flush(context),
            Self::Tls { tls, .. } => Pin::new(tls).poll_flush(context),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut task::Context<'_>,
    ) -> task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Raw { raw } => Pin::new(raw).poll_shutdown(context),
            Self::Tls { tls, .. } => Pin::new(tls).poll_shutdown(context),
        }
    }
}
