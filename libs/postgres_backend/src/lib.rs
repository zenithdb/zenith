//! Server-side asynchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.
#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]
use anyhow::Context;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Poll};
use std::{fmt, io};
use std::{future::Future, str::FromStr};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use pq_proto::framed::{ConnectionError, Framed, FramedReader, FramedWriter};
use pq_proto::{
    BeMessage, FeMessage, FeStartupPacket, ProtocolError, SQLSTATE_ADMIN_SHUTDOWN,
    SQLSTATE_INTERNAL_ERROR, SQLSTATE_SUCCESSFUL_COMPLETION,
};

/// An error, occurred during query processing:
/// either during the connection ([`ConnectionError`]) or before/after it.
#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    /// The connection was lost while processing the query.
    #[error(transparent)]
    Disconnected(#[from] ConnectionError),
    /// We were instructed to shutdown while processing the query
    #[error("Shutting down")]
    Shutdown,
    /// Query handler indicated that client should reconnect
    #[error("Server requested reconnect")]
    Reconnect,
    /// Query named an entity that was not found
    #[error("Not found: {0}")]
    NotFound(std::borrow::Cow<'static, str>),
    /// Authentication failure
    #[error("Unauthorized: {0}")]
    Unauthorized(std::borrow::Cow<'static, str>),
    #[error("Simulated Connection Error")]
    SimulatedConnectionError,
    /// Some other error
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<io::Error> for QueryError {
    fn from(e: io::Error) -> Self {
        Self::Disconnected(ConnectionError::Io(e))
    }
}

impl QueryError {
    pub fn pg_error_code(&self) -> &'static [u8; 5] {
        match self {
            Self::Disconnected(_) | Self::SimulatedConnectionError | Self::Reconnect => b"08006", // connection failure
            Self::Shutdown => SQLSTATE_ADMIN_SHUTDOWN,
            Self::Unauthorized(_) | Self::NotFound(_) => SQLSTATE_INTERNAL_ERROR,
            Self::Other(_) => SQLSTATE_INTERNAL_ERROR, // internal error
        }
    }
}

/// Returns true if the given error is a normal consequence of a network issue,
/// or the client closing the connection.
///
/// These errors can happen during normal operations,
/// and don't indicate a bug in our code.
pub fn is_expected_io_error(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    matches!(
        e.kind(),
        BrokenPipe | ConnectionRefused | ConnectionAborted | ConnectionReset | TimedOut
    )
}

pub trait Handler<IO> {
    /// Handle single query.
    /// postgres_backend will issue ReadyForQuery after calling this (this
    /// might be not what we want after CopyData streaming, but currently we don't
    /// care). It will also flush out the output buffer.
    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        query_string: &str,
    ) -> impl Future<Output = Result<(), QueryError>>;

    /// Called on startup packet receival, allows to process params.
    ///
    /// If Ok(false) is returned postgres_backend will skip auth -- that is needed for new users
    /// creation is the proxy code. That is quite hacky and ad-hoc solution, may be we could allow
    /// to override whole init logic in implementations.
    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        _sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    /// Check auth jwt
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        _jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        Err(QueryError::Other(anyhow::anyhow!("JWT auth failed")))
    }
}

/// PostgresBackend protocol state.
/// XXX: The order of the constructors matters.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum ProtoState {
    /// Nothing happened yet.
    Initialization,
    /// Encryption handshake is done; waiting for encrypted Startup message.
    Encrypted,
    /// Waiting for password (auth token).
    Authentication,
    /// Performed handshake and auth, ReadyForQuery is issued.
    Established,
    Closed,
}

#[derive(Clone, Copy)]
pub enum ProcessMsgResult {
    Continue,
    Break,
}

/// Either plain TCP stream or encrypted one, implementing AsyncRead + AsyncWrite.
pub enum MaybeTlsStream<IO> {
    Unencrypted(IO),
    Tls(Box<tokio_rustls::server::TlsStream<IO>>),
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTlsStream<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}
impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTlsStream<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum AuthType {
    Trust,
    // This mimics postgres's AuthenticationCleartextPassword but instead of password expects JWT
    NeonJWT,
}

impl FromStr for AuthType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Trust" => Ok(Self::Trust),
            "NeonJWT" => Ok(Self::NeonJWT),
            _ => anyhow::bail!("invalid value \"{s}\" for auth type"),
        }
    }
}

impl fmt::Display for AuthType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AuthType::Trust => "Trust",
            AuthType::NeonJWT => "NeonJWT",
        })
    }
}

/// Either full duplex Framed or write only half; the latter is left in
/// PostgresBackend after call to `split`. In principle we could always store a
/// pair of splitted handles, but that would force to to pay splitting price
/// (Arc and kinda mutex inside polling) for all uses (e.g. pageserver).
enum MaybeWriteOnly<IO> {
    Full(Framed<MaybeTlsStream<IO>>),
    WriteOnly(FramedWriter<MaybeTlsStream<IO>>),
    Broken, // temporary value palmed off during the split
}

impl<IO: AsyncRead + AsyncWrite + Unpin> MaybeWriteOnly<IO> {
    async fn read_startup_message(&mut self) -> Result<Option<FeStartupPacket>, ConnectionError> {
        match self {
            MaybeWriteOnly::Full(framed) => framed.read_startup_message().await,
            MaybeWriteOnly::WriteOnly(_) => {
                Err(io::Error::new(ErrorKind::Other, "reading from write only half").into())
            }
            MaybeWriteOnly::Broken => panic!("IO on invalid MaybeWriteOnly"),
        }
    }

    async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        match self {
            MaybeWriteOnly::Full(framed) => framed.read_message().await,
            MaybeWriteOnly::WriteOnly(_) => {
                Err(io::Error::new(ErrorKind::Other, "reading from write only half").into())
            }
            MaybeWriteOnly::Broken => panic!("IO on invalid MaybeWriteOnly"),
        }
    }

    fn write_message_noflush(&mut self, msg: &BeMessage<'_>) -> Result<(), ProtocolError> {
        match self {
            MaybeWriteOnly::Full(framed) => framed.write_message(msg),
            MaybeWriteOnly::WriteOnly(framed_writer) => framed_writer.write_message_noflush(msg),
            MaybeWriteOnly::Broken => panic!("IO on invalid MaybeWriteOnly"),
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            MaybeWriteOnly::Full(framed) => framed.flush().await,
            MaybeWriteOnly::WriteOnly(framed_writer) => framed_writer.flush().await,
            MaybeWriteOnly::Broken => panic!("IO on invalid MaybeWriteOnly"),
        }
    }

    /// Cancellation safe as long as the underlying IO is cancellation safe.
    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            MaybeWriteOnly::Full(framed) => framed.shutdown().await,
            MaybeWriteOnly::WriteOnly(framed_writer) => framed_writer.shutdown().await,
            MaybeWriteOnly::Broken => panic!("IO on invalid MaybeWriteOnly"),
        }
    }
}

pub struct PostgresBackend<IO> {
    framed: MaybeWriteOnly<IO>,

    pub state: ProtoState,

    auth_type: AuthType,

    peer_addr: SocketAddr,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
}

pub type PostgresBackendTCP = PostgresBackend<tokio::net::TcpStream>;

/// Cast a byte slice to a string slice, dropping null terminator if there's one.
fn cstr_to_str(bytes: &[u8]) -> anyhow::Result<&str> {
    let without_null = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    std::str::from_utf8(without_null).map_err(|e| e.into())
}

impl PostgresBackend<tokio::net::TcpStream> {
    pub fn new(
        socket: tokio::net::TcpStream,
        auth_type: AuthType,
        tls_config: Option<Arc<rustls::ServerConfig>>,
    ) -> io::Result<Self> {
        let peer_addr = socket.peer_addr()?;
        let stream = MaybeTlsStream::Unencrypted(socket);

        Ok(Self {
            framed: MaybeWriteOnly::Full(Framed::new(stream)),
            state: ProtoState::Initialization,
            auth_type,
            tls_config,
            peer_addr,
        })
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> PostgresBackend<IO> {
    pub fn new_from_io(
        socket: IO,
        peer_addr: SocketAddr,
        auth_type: AuthType,
        tls_config: Option<Arc<rustls::ServerConfig>>,
    ) -> io::Result<Self> {
        let stream = MaybeTlsStream::Unencrypted(socket);

        Ok(Self {
            framed: MaybeWriteOnly::Full(Framed::new(stream)),
            state: ProtoState::Initialization,
            auth_type,
            tls_config,
            peer_addr,
        })
    }

    pub fn get_peer_addr(&self) -> &SocketAddr {
        &self.peer_addr
    }

    /// Read full message or return None if connection is cleanly closed with no
    /// unprocessed data.
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        if let ProtoState::Closed = self.state {
            Ok(None)
        } else {
            match self.framed.read_message().await {
                Ok(m) => {
                    trace!("read msg {:?}", m);
                    Ok(m)
                }
                Err(e) => {
                    // remember not to try to read anymore
                    self.state = ProtoState::Closed;
                    Err(e)
                }
            }
        }
    }

    /// Write message into internal output buffer, doesn't flush it. Technically
    /// error type can be only ProtocolError here (if, unlikely, serialization
    /// fails), but callers typically wrap it anyway.
    pub fn write_message_noflush(
        &mut self,
        message: &BeMessage<'_>,
    ) -> Result<&mut Self, ConnectionError> {
        self.framed.write_message_noflush(message)?;
        trace!("wrote msg {:?}", message);
        Ok(self)
    }

    /// Flush output buffer into the socket.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.framed.flush().await
    }

    /// Polling version of `flush()`, saves the caller need to pin.
    pub fn poll_flush(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let flush_fut = std::pin::pin!(self.flush());
        flush_fut.poll(cx)
    }

    /// Write message into internal output buffer and flush it to the stream.
    pub async fn write_message(
        &mut self,
        message: &BeMessage<'_>,
    ) -> Result<&mut Self, ConnectionError> {
        self.write_message_noflush(message)?;
        self.flush().await?;
        Ok(self)
    }

    /// Returns an AsyncWrite implementation that wraps all the data written
    /// to it in CopyData messages, and writes them to the connection
    ///
    /// The caller is responsible for sending CopyOutResponse and CopyDone messages.
    pub fn copyout_writer(&mut self) -> CopyDataWriter<IO> {
        CopyDataWriter { pgb: self }
    }

    /// Wrapper for run_message_loop() that shuts down socket when we are done
    pub async fn run(
        mut self,
        handler: &mut impl Handler<IO>,
        cancel: &CancellationToken,
    ) -> Result<(), QueryError> {
        let ret = self.run_message_loop(handler, cancel).await;

        tokio::select! {
            _ = cancel.cancelled() => {
                // do nothing; we most likely got already stopped by shutdown and will log it next.
            }
            _ = self.framed.shutdown() => {
                // socket might be already closed, e.g. if previously received error,
                // so ignore result.
            },
        }

        match ret {
            Ok(()) => Ok(()),
            Err(QueryError::Shutdown) => {
                info!("Stopped due to shutdown");
                Ok(())
            }
            Err(QueryError::Reconnect) => {
                // Dropping out of this loop implicitly disconnects
                info!("Stopped due to handler reconnect request");
                Ok(())
            }
            Err(QueryError::Disconnected(e)) => {
                info!("Disconnected ({e:#})");
                // Disconnection is not an error: we just use it that way internally to drop
                // out of loops.
                Ok(())
            }
            e => e,
        }
    }

    async fn run_message_loop(
        &mut self,
        handler: &mut impl Handler<IO>,
        cancel: &CancellationToken,
    ) -> Result<(), QueryError> {
        trace!("postgres backend to {:?} started", self.peer_addr);

        tokio::select!(
            biased;

            _ = cancel.cancelled() => {
                // We were requested to shut down.
                tracing::info!("shutdown request received during handshake");
                return Err(QueryError::Shutdown)
            },

            handshake_r = self.handshake(handler) => {
                handshake_r?;
            }
        );

        // Authentication completed
        let mut query_string = Bytes::new();
        while let Some(msg) = tokio::select!(
            biased;
            _ = cancel.cancelled() => {
                // We were requested to shut down.
                tracing::info!("shutdown request received in run_message_loop");
                return Err(QueryError::Shutdown)
            },
            msg = self.read_message() => { msg },
        )? {
            trace!("got message {:?}", msg);

            let result = self.process_message(handler, msg, &mut query_string).await;
            tokio::select!(
                biased;
                _ = cancel.cancelled() => {
                    // We were requested to shut down.
                    tracing::info!("shutdown request received during response flush");

                    // If we exited process_message with a shutdown error, there may be
                    // some valid response content on in our transmit buffer: permit sending
                    // this within a short timeout.  This is a best effort thing so we don't
                    // care about the result.
                    tokio::time::timeout(std::time::Duration::from_millis(500), self.flush()).await.ok();

                    return Err(QueryError::Shutdown)
                },
                flush_r = self.flush() => {
                    flush_r?;
                }
            );

            match result? {
                ProcessMsgResult::Continue => {
                    continue;
                }
                ProcessMsgResult::Break => break,
            }
        }

        trace!("postgres backend to {:?} exited", self.peer_addr);
        Ok(())
    }

    /// Try to upgrade MaybeTlsStream into actual TLS one, performing handshake.
    async fn tls_upgrade(
        src: MaybeTlsStream<IO>,
        tls_config: Arc<rustls::ServerConfig>,
    ) -> anyhow::Result<MaybeTlsStream<IO>> {
        match src {
            MaybeTlsStream::Unencrypted(s) => {
                let acceptor = TlsAcceptor::from(tls_config);
                let tls_stream = acceptor.accept(s).await?;
                Ok(MaybeTlsStream::Tls(Box::new(tls_stream)))
            }
            MaybeTlsStream::Tls(_) => {
                anyhow::bail!("TLS already started");
            }
        }
    }

    async fn start_tls(&mut self) -> anyhow::Result<()> {
        // temporary replace stream with fake to cook TLS one, Indiana Jones style
        match std::mem::replace(&mut self.framed, MaybeWriteOnly::Broken) {
            MaybeWriteOnly::Full(framed) => {
                let tls_config = self
                    .tls_config
                    .as_ref()
                    .context("start_tls called without conf")?
                    .clone();
                let tls_framed = framed
                    .map_stream(|s| PostgresBackend::tls_upgrade(s, tls_config))
                    .await?;
                // push back ready TLS stream
                self.framed = MaybeWriteOnly::Full(tls_framed);
                Ok(())
            }
            MaybeWriteOnly::WriteOnly(_) => {
                anyhow::bail!("TLS upgrade attempt in split state")
            }
            MaybeWriteOnly::Broken => panic!("TLS upgrade on framed in invalid state"),
        }
    }

    /// Split off owned read part from which messages can be read in different
    /// task/thread.
    pub fn split(&mut self) -> anyhow::Result<PostgresBackendReader<IO>> {
        // temporary replace stream with fake to cook split one, Indiana Jones style
        match std::mem::replace(&mut self.framed, MaybeWriteOnly::Broken) {
            MaybeWriteOnly::Full(framed) => {
                let (reader, writer) = framed.split();
                self.framed = MaybeWriteOnly::WriteOnly(writer);
                Ok(PostgresBackendReader {
                    reader,
                    closed: false,
                })
            }
            MaybeWriteOnly::WriteOnly(_) => {
                anyhow::bail!("PostgresBackend is already split")
            }
            MaybeWriteOnly::Broken => panic!("split on framed in invalid state"),
        }
    }

    /// Join read part back.
    pub fn unsplit(&mut self, reader: PostgresBackendReader<IO>) -> anyhow::Result<()> {
        // temporary replace stream with fake to cook joined one, Indiana Jones style
        match std::mem::replace(&mut self.framed, MaybeWriteOnly::Broken) {
            MaybeWriteOnly::Full(_) => {
                anyhow::bail!("PostgresBackend is not split")
            }
            MaybeWriteOnly::WriteOnly(writer) => {
                let joined = Framed::unsplit(reader.reader, writer);
                self.framed = MaybeWriteOnly::Full(joined);
                // if reader encountered connection error, do not attempt reading anymore
                if reader.closed {
                    self.state = ProtoState::Closed;
                }
                Ok(())
            }
            MaybeWriteOnly::Broken => panic!("unsplit on framed in invalid state"),
        }
    }

    /// Perform handshake with the client, transitioning to Established.
    /// In case of EOF during handshake logs this, sets state to Closed and returns Ok(()).
    async fn handshake(&mut self, handler: &mut impl Handler<IO>) -> Result<(), QueryError> {
        while self.state < ProtoState::Authentication {
            match self.framed.read_startup_message().await? {
                Some(msg) => {
                    self.process_startup_message(handler, msg).await?;
                }
                None => {
                    trace!(
                        "postgres backend to {:?} received EOF during handshake",
                        self.peer_addr
                    );
                    self.state = ProtoState::Closed;
                    return Err(QueryError::Disconnected(ConnectionError::Protocol(
                        ProtocolError::Protocol("EOF during handshake".to_string()),
                    )));
                }
            }
        }

        // Perform auth, if needed.
        if self.state == ProtoState::Authentication {
            match self.framed.read_message().await? {
                Some(FeMessage::PasswordMessage(m)) => {
                    assert!(self.auth_type == AuthType::NeonJWT);

                    let (_, jwt_response) = m.split_last().context("protocol violation")?;

                    if let Err(e) = handler.check_auth_jwt(self, jwt_response) {
                        self.write_message_noflush(&BeMessage::ErrorResponse(
                            &short_error(&e),
                            Some(e.pg_error_code()),
                        ))?;
                        return Err(e);
                    }

                    self.write_message_noflush(&BeMessage::AuthenticationOk)?
                        .write_message_noflush(&BeMessage::CLIENT_ENCODING)?
                        .write_message(&BeMessage::ReadyForQuery)
                        .await?;
                    self.state = ProtoState::Established;
                }
                Some(m) => {
                    return Err(QueryError::Other(anyhow::anyhow!(
                        "Unexpected message {:?} while waiting for handshake",
                        m
                    )));
                }
                None => {
                    trace!(
                        "postgres backend to {:?} received EOF during auth",
                        self.peer_addr
                    );
                    self.state = ProtoState::Closed;
                    return Err(QueryError::Disconnected(ConnectionError::Protocol(
                        ProtocolError::Protocol("EOF during auth".to_string()),
                    )));
                }
            }
        }

        Ok(())
    }

    /// Process startup packet:
    /// - transition to Established if auth type is trust
    /// - transition to Authentication if auth type is NeonJWT.
    /// - or perform TLS handshake -- then need to call this again to receive
    ///   actual startup packet.
    async fn process_startup_message(
        &mut self,
        handler: &mut impl Handler<IO>,
        msg: FeStartupPacket,
    ) -> Result<(), QueryError> {
        assert!(self.state < ProtoState::Authentication);
        let have_tls = self.tls_config.is_some();
        match msg {
            FeStartupPacket::SslRequest { direct } => {
                debug!("SSL requested");

                if !direct {
                    self.write_message(&BeMessage::EncryptionResponse(have_tls))
                        .await?;
                } else if !have_tls {
                    return Err(QueryError::Other(anyhow::anyhow!(
                        "direct SSL negotiation but no TLS support"
                    )));
                }

                if have_tls {
                    self.start_tls().await?;
                    self.state = ProtoState::Encrypted;
                }
            }
            FeStartupPacket::GssEncRequest => {
                debug!("GSS requested");
                self.write_message(&BeMessage::EncryptionResponse(false))
                    .await?;
            }
            FeStartupPacket::StartupMessage { .. } => {
                if have_tls && !matches!(self.state, ProtoState::Encrypted) {
                    self.write_message(&BeMessage::ErrorResponse("must connect with TLS", None))
                        .await?;
                    return Err(QueryError::Other(anyhow::anyhow!(
                        "client did not connect with TLS"
                    )));
                }

                // NB: startup() may change self.auth_type -- we are using that in proxy code
                // to bypass auth for new users.
                handler.startup(self, &msg)?;

                match self.auth_type {
                    AuthType::Trust => {
                        self.write_message_noflush(&BeMessage::AuthenticationOk)?
                            .write_message_noflush(&BeMessage::CLIENT_ENCODING)?
                            .write_message_noflush(&BeMessage::INTEGER_DATETIMES)?
                            // The async python driver requires a valid server_version
                            .write_message_noflush(&BeMessage::server_version("14.1"))?
                            .write_message(&BeMessage::ReadyForQuery)
                            .await?;
                        self.state = ProtoState::Established;
                    }
                    AuthType::NeonJWT => {
                        self.write_message(&BeMessage::AuthenticationCleartextPassword)
                            .await?;
                        self.state = ProtoState::Authentication;
                    }
                }
            }
            FeStartupPacket::CancelRequest { .. } => {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "Unexpected CancelRequest message during handshake"
                )));
            }
        }
        Ok(())
    }

    // Proto looks like this:
    // FeMessage::Query("pagestream_v2{FeMessage::CopyData(PagesetreamFeMessage::GetPage(..))}")

    async fn process_message(
        &mut self,
        handler: &mut impl Handler<IO>,
        msg: FeMessage,
        unnamed_query_string: &mut Bytes,
    ) -> Result<ProcessMsgResult, QueryError> {
        // Allow only startup and password messages during auth. Otherwise client would be able to bypass auth
        // TODO: change that to proper top-level match of protocol state with separate message handling for each state
        assert!(self.state == ProtoState::Established);

        match msg {
            FeMessage::Query(body) => {
                // remove null terminator
                let query_string = cstr_to_str(&body)?;

                trace!("got query {query_string:?}");
                if let Err(e) = handler.process_query(self, query_string).await {
                    match e {
                        QueryError::Shutdown => return Ok(ProcessMsgResult::Break),
                        QueryError::SimulatedConnectionError => {
                            return Err(QueryError::SimulatedConnectionError)
                        }
                        err @ QueryError::Reconnect => {
                            // Instruct the client to reconnect, stop processing messages
                            // from this libpq connection and, finally, disconnect from the
                            // server side (returning an Err achieves the later).
                            //
                            // Note the flushing is done by the caller.
                            let reconnect_error = short_error(&err);
                            self.write_message_noflush(&BeMessage::ErrorResponse(
                                &reconnect_error,
                                Some(err.pg_error_code()),
                            ))?;

                            return Err(err);
                        }
                        e => {
                            log_query_error(query_string, &e);
                            let short_error = short_error(&e);
                            self.write_message_noflush(&BeMessage::ErrorResponse(
                                &short_error,
                                Some(e.pg_error_code()),
                            ))?;
                        }
                    }
                }
                self.write_message_noflush(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Parse(m) => {
                *unnamed_query_string = m.query_string;
                self.write_message_noflush(&BeMessage::ParseComplete)?;
            }

            FeMessage::Describe(_) => {
                self.write_message_noflush(&BeMessage::ParameterDescription)?
                    .write_message_noflush(&BeMessage::NoData)?;
            }

            FeMessage::Bind(_) => {
                self.write_message_noflush(&BeMessage::BindComplete)?;
            }

            FeMessage::Close(_) => {
                self.write_message_noflush(&BeMessage::CloseComplete)?;
            }

            FeMessage::Execute(_) => {
                let query_string = cstr_to_str(unnamed_query_string)?;
                trace!("got execute {query_string:?}");
                if let Err(e) = handler.process_query(self, query_string).await {
                    log_query_error(query_string, &e);
                    self.write_message_noflush(&BeMessage::ErrorResponse(
                        &e.to_string(),
                        Some(e.pg_error_code()),
                    ))?;
                }
                // NOTE there is no ReadyForQuery message. This handler is used
                // for basebackup and it uses CopyOut which doesn't require
                // ReadyForQuery message and backend just switches back to
                // processing mode after sending CopyDone or ErrorResponse.
            }

            FeMessage::Sync => {
                self.write_message_noflush(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Terminate => {
                return Ok(ProcessMsgResult::Break);
            }

            // We prefer explicit pattern matching to wildcards, because
            // this helps us spot the places where new variants are missing
            FeMessage::CopyData(_)
            | FeMessage::CopyDone
            | FeMessage::CopyFail
            | FeMessage::PasswordMessage(_) => {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "unexpected message type: {msg:?}",
                )));
            }
        }

        Ok(ProcessMsgResult::Continue)
    }

    /// - Log as info/error result of handling COPY stream and send back
    ///   ErrorResponse if that makes sense.
    /// - Shutdown the stream if we got Terminate.
    /// - Then close the connection because we don't handle exiting from COPY
    ///   stream normally.
    pub async fn handle_copy_stream_end(&mut self, end: CopyStreamHandlerEnd) {
        use CopyStreamHandlerEnd::*;

        let expected_end = match &end {
            ServerInitiated(_) | CopyDone | CopyFail | Terminate | EOF | Cancelled => true,
            CopyStreamHandlerEnd::Disconnected(ConnectionError::Io(io_error))
                if is_expected_io_error(io_error) =>
            {
                true
            }
            _ => false,
        };
        if expected_end {
            info!("terminated: {:#}", end);
        } else {
            error!("terminated: {:?}", end);
        }

        // Note: no current usages ever send this
        if let CopyDone = &end {
            if let Err(e) = self.write_message(&BeMessage::CopyDone).await {
                error!("failed to send CopyDone: {}", e);
            }
        }

        let err_to_send_and_errcode = match &end {
            ServerInitiated(_) => Some((end.to_string(), SQLSTATE_SUCCESSFUL_COMPLETION)),
            Other(_) => Some((format!("{end:#}"), SQLSTATE_INTERNAL_ERROR)),
            // Note: CopyFail in duplex copy is somewhat unexpected (at least to
            // PG walsender; evidently and per my docs reading client should
            // finish it with CopyDone). It is not a problem to recover from it
            // finishing the stream in both directions like we do, but note that
            // sync rust-postgres client (which we don't use anymore) hangs if
            // socket is not closed here.
            // https://github.com/sfackler/rust-postgres/issues/755
            // https://github.com/neondatabase/neon/issues/935
            //
            // Currently, the version of tokio_postgres replication patch we use
            // sends this when it closes the stream (e.g. pageserver decided to
            // switch conn to another safekeeper and client gets dropped).
            // Moreover, seems like 'connection' task errors with 'unexpected
            // message from server' when it receives ErrorResponse (anything but
            // CopyData/CopyDone) back.
            CopyFail => Some((end.to_string(), SQLSTATE_SUCCESSFUL_COMPLETION)),

            // When cancelled, send no response: we must not risk blocking on sending that response
            Cancelled => None,
            _ => None,
        };
        if let Some((err, errcode)) = err_to_send_and_errcode {
            if let Err(ee) = self
                .write_message(&BeMessage::ErrorResponse(&err, Some(errcode)))
                .await
            {
                error!("failed to send ErrorResponse: {}", ee);
            }
        }

        // Proper COPY stream finishing to continue using the connection is not
        // implemented at the server side (we don't need it so far). To prevent
        // further usages of the connection, close it.
        self.framed.shutdown().await.ok();
        self.state = ProtoState::Closed;
    }
}

pub struct PostgresBackendReader<IO> {
    reader: FramedReader<MaybeTlsStream<IO>>,
    closed: bool, // true if received error closing the connection
}

impl<IO: AsyncRead + AsyncWrite + Unpin> PostgresBackendReader<IO> {
    /// Read full message or return None if connection is cleanly closed with no
    /// unprocessed data.
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        match self.reader.read_message().await {
            Ok(m) => {
                trace!("read msg {:?}", m);
                Ok(m)
            }
            Err(e) => {
                self.closed = true;
                Err(e)
            }
        }
    }

    /// Get CopyData contents of the next message in COPY stream or error
    /// closing it. The error type is wider than actual errors which can happen
    /// here -- it includes 'Other' and 'ServerInitiated', but that's ok for
    /// current callers.
    pub async fn read_copy_message(&mut self) -> Result<Bytes, CopyStreamHandlerEnd> {
        match self.read_message().await? {
            Some(msg) => match msg {
                FeMessage::CopyData(m) => Ok(m),
                FeMessage::CopyDone => Err(CopyStreamHandlerEnd::CopyDone),
                FeMessage::CopyFail => Err(CopyStreamHandlerEnd::CopyFail),
                FeMessage::Terminate => Err(CopyStreamHandlerEnd::Terminate),
                _ => Err(CopyStreamHandlerEnd::from(ConnectionError::Protocol(
                    ProtocolError::Protocol(format!("unexpected message in COPY stream {:?}", msg)),
                ))),
            },
            None => Err(CopyStreamHandlerEnd::EOF),
        }
    }
}

///
/// A futures::AsyncWrite implementation that wraps all data written to it in CopyData
/// messages.
///
pub struct CopyDataWriter<'a, IO> {
    pgb: &'a mut PostgresBackend<IO>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncWrite for CopyDataWriter<'_, IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();

        // It's not strictly required to flush between each message, but makes it easier
        // to view in wireshark, and usually the messages that the callers write are
        // decently-sized anyway.
        if let Err(err) = ready!(this.pgb.poll_flush(cx)) {
            return Poll::Ready(Err(err));
        }

        // CopyData
        // XXX: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        this.pgb
            .write_message_noflush(&BeMessage::CopyData(buf))
            // write_message only writes to the buffer, so it can fail iff the
            // message is invaid, but CopyData can't be invalid.
            .map_err(|_| io::Error::new(ErrorKind::Other, "failed to serialize CopyData"))?;

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        this.pgb.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        this.pgb.poll_flush(cx)
    }
}

pub fn short_error(e: &QueryError) -> String {
    match e {
        QueryError::Disconnected(connection_error) => connection_error.to_string(),
        QueryError::Reconnect => "reconnect".to_string(),
        QueryError::Shutdown => "shutdown".to_string(),
        QueryError::NotFound(_) => "not found".to_string(),
        QueryError::Unauthorized(_e) => "JWT authentication error".to_string(),
        QueryError::SimulatedConnectionError => "simulated connection error".to_string(),
        QueryError::Other(e) => format!("{e:#}"),
    }
}

fn log_query_error(query: &str, e: &QueryError) {
    // If you want to change the log level of a specific error, also re-categorize it in `BasebackupQueryTimeOngoingRecording`.
    match e {
        QueryError::Disconnected(ConnectionError::Io(io_error)) => {
            if is_expected_io_error(io_error) {
                info!("query handler for '{query}' failed with expected io error: {io_error}");
            } else {
                error!("query handler for '{query}' failed with io error: {io_error}");
            }
        }
        QueryError::Disconnected(other_connection_error) => {
            error!("query handler for '{query}' failed with connection error: {other_connection_error}")
        }
        QueryError::SimulatedConnectionError => {
            error!("query handler for query '{query}' failed due to a simulated connection error")
        }
        QueryError::Reconnect => {
            info!("query handler for '{query}' requested client to reconnect")
        }
        QueryError::Shutdown => {
            info!("query handler for '{query}' cancelled during tenant shutdown")
        }
        QueryError::NotFound(reason) => {
            info!("query handler for '{query}' entity not found: {reason}")
        }
        QueryError::Unauthorized(e) => {
            warn!("query handler for '{query}' failed with authentication error: {e}");
        }
        QueryError::Other(e) => {
            error!("query handler for '{query}' failed: {e}");
        }
    }
}

/// Something finishing handling of COPY stream, see handle_copy_stream_end.
/// This is not always a real error, but it allows to use ? and thiserror impls.
#[derive(thiserror::Error, Debug)]
pub enum CopyStreamHandlerEnd {
    /// Handler initiates the end of streaming.
    #[error("{0}")]
    ServerInitiated(String),
    #[error("received CopyDone")]
    CopyDone,
    #[error("received CopyFail")]
    CopyFail,
    #[error("received Terminate")]
    Terminate,
    #[error("EOF on COPY stream")]
    EOF,
    /// The connection was lost
    #[error("connection error: {0}")]
    Disconnected(#[from] ConnectionError),
    #[error("Shutdown")]
    Cancelled,
    /// Some other error
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
