//! Postgres protocol messages serialization-deserialization. See
//! <https://www.postgresql.org/docs/devel/protocol-message-formats.html>
//! on message formats.
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod framed;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fmt, io, str};

// re-export for use in utils pageserver_feedback.rs
pub use postgres_protocol::PG_EPOCH;

pub type Oid = u32;
pub type SystemId = u64;

pub const INT8_OID: Oid = 20;
pub const INT4_OID: Oid = 23;
pub const TEXT_OID: Oid = 25;

#[derive(Debug)]
pub enum FeMessage {
    // Simple query.
    Query(Bytes),
    // Extended query protocol.
    Parse(FeParseMessage),
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Close(FeCloseMessage),
    Sync,
    Terminate,
    CopyData(Bytes),
    CopyDone,
    CopyFail,
    PasswordMessage(Bytes),
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self(((major as u32) << 16) | minor as u32)
    }
    pub const fn minor(self) -> u16 {
        self.0 as u16
    }
    pub const fn major(self) -> u16 {
        (self.0 >> 16) as u16
    }
}

impl fmt::Debug for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entry(&self.major())
            .entry(&self.minor())
            .finish()
    }
}

#[derive(Debug)]
pub enum FeStartupPacket {
    CancelRequest(CancelKeyData),
    SslRequest {
        direct: bool,
    },
    GssEncRequest,
    StartupMessage {
        version: ProtocolVersion,
        params: StartupMessageParams,
    },
}

#[derive(Debug, Clone, Default)]
pub struct StartupMessageParamsBuilder {
    params: BytesMut,
}

impl StartupMessageParamsBuilder {
    /// Set parameter's value by its name.
    /// name and value must not contain a \0 byte
    pub fn insert(&mut self, name: &str, value: &str) {
        self.params.put(name.as_bytes());
        self.params.put(&b"\0"[..]);
        self.params.put(value.as_bytes());
        self.params.put(&b"\0"[..]);
    }

    pub fn freeze(self) -> StartupMessageParams {
        StartupMessageParams {
            params: self.params.freeze(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StartupMessageParams {
    pub params: Bytes,
}

impl StartupMessageParams {
    /// Get parameter's value by its name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.iter().find_map(|(k, v)| (k == name).then_some(v))
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// taking into account all escape sequences but leaving them as-is.
    /// [`None`] means that there's no `options` in [`Self`].
    pub fn options_raw(&self) -> Option<impl Iterator<Item = &str>> {
        self.get("options").map(Self::parse_options_raw)
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// applying all escape sequences (using owned strings as needed).
    /// [`None`] means that there's no `options` in [`Self`].
    pub fn options_escaped(&self) -> Option<impl Iterator<Item = Cow<'_, str>>> {
        self.get("options").map(Self::parse_options_escaped)
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// taking into account all escape sequences but leaving them as-is.
    pub fn parse_options_raw(input: &str) -> impl Iterator<Item = &str> {
        // See `postgres: pg_split_opts`.
        let mut last_was_escape = false;
        input
            .split(move |c: char| {
                // We split by non-escaped whitespace symbols.
                let should_split = c.is_ascii_whitespace() && !last_was_escape;
                last_was_escape = c == '\\' && !last_was_escape;
                should_split
            })
            .filter(|s| !s.is_empty())
    }

    /// Split command-line options according to PostgreSQL's logic,
    /// applying all escape sequences (using owned strings as needed).
    pub fn parse_options_escaped(input: &str) -> impl Iterator<Item = Cow<'_, str>> {
        // See `postgres: pg_split_opts`.
        Self::parse_options_raw(input).map(|s| {
            let mut preserve_next_escape = false;
            let escape = |c| {
                // We should remove '\\' unless it's preceded by '\\'.
                let should_remove = c == '\\' && !preserve_next_escape;
                preserve_next_escape = should_remove;
                should_remove
            };

            match s.contains('\\') {
                true => Cow::Owned(s.replace(escape, "")),
                false => Cow::Borrowed(s),
            }
        })
    }

    /// Iterate through key-value pairs in an arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        let params =
            std::str::from_utf8(&self.params).expect("should be validated as utf8 already");
        params.split_terminator('\0').tuples()
    }

    // This function is mostly useful in tests.
    #[doc(hidden)]
    pub fn new<'a, const N: usize>(pairs: [(&'a str, &'a str); N]) -> Self {
        let mut b = StartupMessageParamsBuilder::default();
        for (k, v) in pairs {
            b.insert(k, v)
        }
        b.freeze()
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct CancelKeyData {
    pub backend_pid: i32,
    pub cancel_key: i32,
}

impl fmt::Display for CancelKeyData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hi = (self.backend_pid as u64) << 32;
        let lo = (self.cancel_key as u64) & 0xffffffff;
        let id = hi | lo;

        // This format is more compact and might work better for logs.
        f.debug_tuple("CancelKeyData")
            .field(&format_args!("{:x}", id))
            .finish()
    }
}

use rand::distributions::{Distribution, Standard};
impl Distribution<CancelKeyData> for Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> CancelKeyData {
        CancelKeyData {
            backend_pid: rng.gen(),
            cancel_key: rng.gen(),
        }
    }
}

// We only support the simple case of Parse on unnamed prepared statement and
// no params
#[derive(Debug)]
pub struct FeParseMessage {
    pub query_string: Bytes,
}

#[derive(Debug)]
pub struct FeDescribeMessage {
    pub kind: u8, // 'S' to describe a prepared statement; or 'P' to describe a portal.
                  // we only support unnamed prepared stmt or portal
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
pub struct FeBindMessage;

// we only support unnamed prepared stmt or portal
#[derive(Debug)]
pub struct FeExecuteMessage {
    /// max # of rows
    pub maxrows: i32,
}

// we only support unnamed prepared stmt and portal
#[derive(Debug)]
pub struct FeCloseMessage;

/// An error occurred while parsing or serializing raw stream into Postgres
/// messages.
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    /// Invalid packet was received from the client (e.g. unexpected message
    /// type or broken len).
    #[error("Protocol error: {0}")]
    Protocol(String),
    /// Failed to parse or, (unlikely), serialize a protocol message.
    #[error("Message parse error: {0}")]
    BadMessage(String),
}

impl ProtocolError {
    /// Proxy stream.rs uses only io::Error; provide it.
    pub fn into_io_error(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, self.to_string())
    }
}

impl FeMessage {
    /// Read and parse one message from the `buf` input buffer. If there is at
    /// least one valid message, returns it, advancing `buf`; redundant copies
    /// are avoided, as thanks to `bytes` crate ptrs in parsed message point
    /// directly into the `buf` (processed data is garbage collected after
    /// parsed message is dropped).
    ///
    /// Returns None if `buf` doesn't contain enough data for a single message.
    /// For efficiency, tries to reserve large enough space in `buf` for the
    /// next message in this case to save the repeated calls.
    ///
    /// Returns Error if message is malformed, the only possible ErrorKind is
    /// InvalidInput.
    //
    // Inspired by rust-postgres Message::parse.
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeMessage>, ProtocolError> {
        // Every message contains message type byte and 4 bytes len; can't do
        // much without them.
        if buf.len() < 5 {
            let to_read = 5 - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        // We shouldn't advance `buf` as probably full message is not there yet,
        // so can't directly use Bytes::get_u32 etc.
        let tag = buf[0];
        let len = (&buf[1..5]).read_u32::<BigEndian>().unwrap();
        if len < 4 {
            return Err(ProtocolError::Protocol(format!(
                "invalid message length {}",
                len
            )));
        }

        // length field includes itself, but not message type.
        let total_len = len as usize + 1;
        if buf.len() < total_len {
            // Don't have full message yet.
            let to_read = total_len - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        // got the message, advance buffer
        let mut msg = buf.split_to(total_len).freeze();
        msg.advance(5); // consume message type and len

        match tag {
            b'Q' => Ok(Some(FeMessage::Query(msg))),
            b'P' => Ok(Some(FeParseMessage::parse(msg)?)),
            b'D' => Ok(Some(FeDescribeMessage::parse(msg)?)),
            b'E' => Ok(Some(FeExecuteMessage::parse(msg)?)),
            b'B' => Ok(Some(FeBindMessage::parse(msg)?)),
            b'C' => Ok(Some(FeCloseMessage::parse(msg)?)),
            b'S' => Ok(Some(FeMessage::Sync)),
            b'X' => Ok(Some(FeMessage::Terminate)),
            b'd' => Ok(Some(FeMessage::CopyData(msg))),
            b'c' => Ok(Some(FeMessage::CopyDone)),
            b'f' => Ok(Some(FeMessage::CopyFail)),
            b'p' => Ok(Some(FeMessage::PasswordMessage(msg))),
            tag => Err(ProtocolError::Protocol(format!(
                "unknown message tag: {tag},'{msg:?}'"
            ))),
        }
    }
}

impl FeStartupPacket {
    /// Read and parse startup message from the `buf` input buffer. It is
    /// different from [`FeMessage::parse`] because startup messages don't have
    /// message type byte; otherwise, its comments apply.
    pub fn parse(buf: &mut BytesMut) -> Result<Option<FeStartupPacket>, ProtocolError> {
        /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L118>
        const MAX_STARTUP_PACKET_LENGTH: usize = 10000;
        const RESERVED_INVALID_MAJOR_VERSION: u16 = 1234;
        /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L132>
        const CANCEL_REQUEST_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5678);
        /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L166>
        const NEGOTIATE_SSL_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5679);
        /// <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/include/libpq/pqcomm.h#L167>
        const NEGOTIATE_GSS_CODE: ProtocolVersion = ProtocolVersion::new(1234, 5680);

        // <https://github.com/postgres/postgres/blob/04bcf9e19a4261fe9c7df37c777592c2e10c32a7/src/backend/tcop/backend_startup.c#L378-L382>
        // First byte indicates standard SSL handshake message
        // (It can't be a Postgres startup length because in network byte order
        // that would be a startup packet hundreds of megabytes long)
        if buf.first() == Some(&0x16) {
            return Ok(Some(FeStartupPacket::SslRequest { direct: true }));
        }

        // need at least 4 bytes with packet len
        if buf.len() < 4 {
            let to_read = 4 - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        // We shouldn't advance `buf` as probably full message is not there yet,
        // so can't directly use Bytes::get_u32 etc.
        let len = (&buf[0..4]).read_u32::<BigEndian>().unwrap() as usize;
        // The proposed replacement is `!(8..=MAX_STARTUP_PACKET_LENGTH).contains(&len)`
        // which is less readable
        #[allow(clippy::manual_range_contains)]
        if len < 8 || len > MAX_STARTUP_PACKET_LENGTH {
            return Err(ProtocolError::Protocol(format!(
                "invalid startup packet message length {}",
                len
            )));
        }

        if buf.len() < len {
            // Don't have full message yet.
            let to_read = len - buf.len();
            buf.reserve(to_read);
            return Ok(None);
        }

        // got the message, advance buffer
        let mut msg = buf.split_to(len).freeze();
        msg.advance(4); // consume len

        let request_code = ProtocolVersion(msg.get_u32());
        // StartupMessage, CancelRequest, SSLRequest etc are differentiated by request code.
        let message = match request_code {
            CANCEL_REQUEST_CODE => {
                if msg.remaining() != 8 {
                    return Err(ProtocolError::BadMessage(
                        "CancelRequest message is malformed, backend PID / secret key missing"
                            .to_owned(),
                    ));
                }
                FeStartupPacket::CancelRequest(CancelKeyData {
                    backend_pid: msg.get_i32(),
                    cancel_key: msg.get_i32(),
                })
            }
            NEGOTIATE_SSL_CODE => {
                // Requested upgrade to SSL (aka TLS)
                FeStartupPacket::SslRequest { direct: false }
            }
            NEGOTIATE_GSS_CODE => {
                // Requested upgrade to GSSAPI
                FeStartupPacket::GssEncRequest
            }
            version if version.major() == RESERVED_INVALID_MAJOR_VERSION => {
                return Err(ProtocolError::Protocol(format!(
                    "Unrecognized request code {}",
                    version.minor()
                )));
            }
            // TODO bail if protocol major_version is not 3?
            version => {
                // StartupMessage

                let s = str::from_utf8(&msg).map_err(|_e| {
                    ProtocolError::BadMessage("StartupMessage params: invalid utf-8".to_owned())
                })?;
                let s = s.strip_suffix('\0').ok_or_else(|| {
                    ProtocolError::Protocol(
                        "StartupMessage params: missing null terminator".to_string(),
                    )
                })?;

                FeStartupPacket::StartupMessage {
                    version,
                    params: StartupMessageParams {
                        params: msg.slice_ref(s.as_bytes()),
                    },
                }
            }
        };
        Ok(Some(message))
    }
}

impl FeParseMessage {
    fn parse(mut buf: Bytes) -> Result<FeMessage, ProtocolError> {
        // FIXME: the rust-postgres driver uses a named prepared statement
        // for copy_out(). We're not prepared to handle that correctly. For
        // now, just ignore the statement name, assuming that the client never
        // uses more than one prepared statement at a time.

        let _pstmt_name = read_cstr(&mut buf)?;
        let query_string = read_cstr(&mut buf)?;
        if buf.remaining() < 2 {
            return Err(ProtocolError::BadMessage(
                "Parse message is malformed, nparams missing".to_string(),
            ));
        }
        let nparams = buf.get_i16();

        if nparams != 0 {
            return Err(ProtocolError::BadMessage(
                "query params not implemented".to_string(),
            ));
        }

        Ok(FeMessage::Parse(FeParseMessage { query_string }))
    }
}

impl FeDescribeMessage {
    fn parse(mut buf: Bytes) -> Result<FeMessage, ProtocolError> {
        let kind = buf.get_u8();
        let _pstmt_name = read_cstr(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        if kind != b'S' {
            return Err(ProtocolError::BadMessage(
                "only prepared statemement Describe is implemented".to_string(),
            ));
        }

        Ok(FeMessage::Describe(FeDescribeMessage { kind }))
    }
}

impl FeExecuteMessage {
    fn parse(mut buf: Bytes) -> Result<FeMessage, ProtocolError> {
        let portal_name = read_cstr(&mut buf)?;
        if buf.remaining() < 4 {
            return Err(ProtocolError::BadMessage(
                "FeExecuteMessage message is malformed, maxrows missing".to_string(),
            ));
        }
        let maxrows = buf.get_i32();

        if !portal_name.is_empty() {
            return Err(ProtocolError::BadMessage(
                "named portals not implemented".to_string(),
            ));
        }
        if maxrows != 0 {
            return Err(ProtocolError::BadMessage(
                "row limit in Execute message not implemented".to_string(),
            ));
        }

        Ok(FeMessage::Execute(FeExecuteMessage { maxrows }))
    }
}

impl FeBindMessage {
    fn parse(mut buf: Bytes) -> Result<FeMessage, ProtocolError> {
        let portal_name = read_cstr(&mut buf)?;
        let _pstmt_name = read_cstr(&mut buf)?;

        // FIXME: see FeParseMessage::parse
        if !portal_name.is_empty() {
            return Err(ProtocolError::BadMessage(
                "named portals not implemented".to_string(),
            ));
        }

        Ok(FeMessage::Bind(FeBindMessage))
    }
}

impl FeCloseMessage {
    fn parse(mut buf: Bytes) -> Result<FeMessage, ProtocolError> {
        let _kind = buf.get_u8();
        let _pstmt_or_portal_name = read_cstr(&mut buf)?;

        // FIXME: we do nothing with Close
        Ok(FeMessage::Close(FeCloseMessage))
    }
}

// Backend

#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    AuthenticationMD5Password([u8; 4]),
    AuthenticationSasl(BeAuthenticationSaslMessage<'a>),
    AuthenticationCleartextPassword,
    BackendKeyData(CancelKeyData),
    BindComplete,
    CommandComplete(&'a [u8]),
    CopyData(&'a [u8]),
    CopyDone,
    CopyFail,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,
    CloseComplete,
    // None means column is NULL
    DataRow(&'a [Option<&'a [u8]>]),
    // None errcode means internal_error will be sent.
    ErrorResponse(&'a str, Option<&'a [u8; 5]>),
    /// Single byte - used in response to SSLRequest/GSSENCRequest.
    EncryptionResponse(bool),
    NoData,
    ParameterDescription,
    ParameterStatus {
        name: &'a [u8],
        value: &'a [u8],
    },
    ParseComplete,
    ReadyForQuery,
    RowDescription(&'a [RowDescriptor<'a>]),
    XLogData(XLogDataBody<'a>),
    NoticeResponse(&'a str),
    NegotiateProtocolVersion {
        version: ProtocolVersion,
        options: &'a [&'a str],
    },
    KeepAlive(WalSndKeepAlive),
    /// Batch of interpreted, shard filtered WAL records,
    /// ready for the pageserver to ingest
    InterpretedWalRecords(InterpretedWalRecordsBody<'a>),

    Raw(u8, &'a [u8]),
}

/// Common shorthands.
impl<'a> BeMessage<'a> {
    /// A [`BeMessage::ParameterStatus`] holding the client encoding, i.e. UTF-8.
    /// This is a sensible default, given that:
    ///  * rust strings only support this encoding out of the box.
    ///  * tokio-postgres, postgres-jdbc (and probably more) mandate it.
    ///
    /// TODO: do we need to report `server_encoding` as well?
    pub const CLIENT_ENCODING: Self = Self::ParameterStatus {
        name: b"client_encoding",
        value: b"UTF8",
    };

    pub const INTEGER_DATETIMES: Self = Self::ParameterStatus {
        name: b"integer_datetimes",
        value: b"on",
    };

    /// Build a [`BeMessage::ParameterStatus`] holding the server version.
    pub fn server_version(version: &'a str) -> Self {
        Self::ParameterStatus {
            name: b"server_version",
            value: version.as_bytes(),
        }
    }
}

#[derive(Debug)]
pub enum BeAuthenticationSaslMessage<'a> {
    Methods(&'a [&'a str]),
    Continue(&'a [u8]),
    Final(&'a [u8]),
}

#[derive(Debug)]
pub enum BeParameterStatusMessage<'a> {
    Encoding(&'a str),
    ServerVersion(&'a str),
}

// One row description in RowDescription packet.
#[derive(Debug)]
pub struct RowDescriptor<'a> {
    pub name: &'a [u8],
    pub tableoid: Oid,
    pub attnum: i16,
    pub typoid: Oid,
    pub typlen: i16,
    pub typmod: i32,
    pub formatcode: i16,
}

impl Default for RowDescriptor<'_> {
    fn default() -> RowDescriptor<'static> {
        RowDescriptor {
            name: b"",
            tableoid: 0,
            attnum: 0,
            typoid: 0,
            typlen: 0,
            typmod: 0,
            formatcode: 0,
        }
    }
}

impl RowDescriptor<'_> {
    /// Convenience function to create a RowDescriptor message for an int8 column
    pub const fn int8_col(name: &[u8]) -> RowDescriptor {
        RowDescriptor {
            name,
            tableoid: 0,
            attnum: 0,
            typoid: INT8_OID,
            typlen: 8,
            typmod: 0,
            formatcode: 0,
        }
    }

    pub const fn text_col(name: &[u8]) -> RowDescriptor {
        RowDescriptor {
            name,
            tableoid: 0,
            attnum: 0,
            typoid: TEXT_OID,
            typlen: -1,
            typmod: 0,
            formatcode: 0,
        }
    }
}

#[derive(Debug)]
pub struct XLogDataBody<'a> {
    pub wal_start: u64,
    pub wal_end: u64, // current end of WAL on the server
    pub timestamp: i64,
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct WalSndKeepAlive {
    pub wal_end: u64, // current end of WAL on the server
    pub timestamp: i64,
    pub request_reply: bool,
}

/// Batch of interpreted WAL records used in the interpreted
/// safekeeper to pageserver protocol.
///
/// Note that the pageserver uses the RawInterpretedWalRecordsBody
/// counterpart of this from the neondatabase/rust-postgres repo.
/// If you're changing this struct, you likely need to change its
/// twin as well.
#[derive(Debug)]
pub struct InterpretedWalRecordsBody<'a> {
    /// End of raw WAL in [`Self::data`]
    pub streaming_lsn: u64,
    /// Current end of WAL on the server
    pub commit_lsn: u64,
    pub data: &'a [u8],
}

pub static HELLO_WORLD_ROW: BeMessage = BeMessage::DataRow(&[Some(b"hello world")]);

// single text column
pub static SINGLE_COL_ROWDESC: BeMessage = BeMessage::RowDescription(&[RowDescriptor {
    name: b"data",
    tableoid: 0,
    attnum: 0,
    typoid: TEXT_OID,
    typlen: -1,
    typmod: 0,
    formatcode: 0,
}]);

/// Call f() to write body of the message and prepend it with 4-byte len as
/// prescribed by the protocol.
fn write_body<R>(buf: &mut BytesMut, f: impl FnOnce(&mut BytesMut) -> R) -> R {
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    let res = f(buf);

    let size = i32::try_from(buf.len() - base).expect("message too big to transmit");
    (&mut buf[base..]).put_slice(&size.to_be_bytes());

    res
}

/// Safe write of s into buf as cstring (String in the protocol).
fn write_cstr(s: impl AsRef<[u8]>, buf: &mut BytesMut) -> Result<(), ProtocolError> {
    let bytes = s.as_ref();
    if bytes.contains(&0) {
        return Err(ProtocolError::BadMessage(
            "string contains embedded null".to_owned(),
        ));
    }
    buf.put_slice(bytes);
    buf.put_u8(0);
    Ok(())
}

/// Read cstring from buf, advancing it.
pub fn read_cstr(buf: &mut Bytes) -> Result<Bytes, ProtocolError> {
    let pos = buf
        .iter()
        .position(|x| *x == 0)
        .ok_or_else(|| ProtocolError::BadMessage("missing cstring terminator".to_owned()))?;
    let result = buf.split_to(pos);
    buf.advance(1); // drop the null terminator
    Ok(result)
}

pub const SQLSTATE_INTERNAL_ERROR: &[u8; 5] = b"XX000";
pub const SQLSTATE_ADMIN_SHUTDOWN: &[u8; 5] = b"57P01";
pub const SQLSTATE_SUCCESSFUL_COMPLETION: &[u8; 5] = b"00000";

impl BeMessage<'_> {
    /// Serialize `message` to the given `buf`.
    /// Apart from smart memory managemet, BytesMut is good here as msg len
    /// precedes its body and it is handy to write it down first and then fill
    /// the length. With Write we would have to either calc it manually or have
    /// one more buffer.
    pub fn write(buf: &mut BytesMut, message: &BeMessage) -> Result<(), ProtocolError> {
        match message {
            BeMessage::Raw(code, data) => {
                buf.put_u8(*code);
                write_body(buf, |b| b.put_slice(data))
            }
            BeMessage::AuthenticationOk => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(0); // Specifies that the authentication was successful.
                });
            }

            BeMessage::AuthenticationCleartextPassword => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(3); // Specifies that clear text password is required.
                });
            }

            BeMessage::AuthenticationMD5Password(salt) => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    buf.put_i32(5); // Specifies that an MD5-encrypted password is required.
                    buf.put_slice(&salt[..]);
                });
            }

            BeMessage::AuthenticationSasl(msg) => {
                buf.put_u8(b'R');
                write_body(buf, |buf| {
                    use BeAuthenticationSaslMessage::*;
                    match msg {
                        Methods(methods) => {
                            buf.put_i32(10); // Specifies that SASL auth method is used.
                            for method in methods.iter() {
                                write_cstr(method, buf)?;
                            }
                            buf.put_u8(0); // zero terminator for the list
                        }
                        Continue(extra) => {
                            buf.put_i32(11); // Continue SASL auth.
                            buf.put_slice(extra);
                        }
                        Final(extra) => {
                            buf.put_i32(12); // Send final SASL message.
                            buf.put_slice(extra);
                        }
                    }
                    Ok(())
                })?;
            }

            BeMessage::BackendKeyData(key_data) => {
                buf.put_u8(b'K');
                write_body(buf, |buf| {
                    buf.put_i32(key_data.backend_pid);
                    buf.put_i32(key_data.cancel_key);
                });
            }

            BeMessage::BindComplete => {
                buf.put_u8(b'2');
                write_body(buf, |_| {});
            }

            BeMessage::CloseComplete => {
                buf.put_u8(b'3');
                write_body(buf, |_| {});
            }

            BeMessage::CommandComplete(cmd) => {
                buf.put_u8(b'C');
                write_body(buf, |buf| write_cstr(cmd, buf))?;
            }

            BeMessage::CopyData(data) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_slice(data);
                });
            }

            BeMessage::CopyDone => {
                buf.put_u8(b'c');
                write_body(buf, |_| {});
            }

            BeMessage::CopyFail => {
                buf.put_u8(b'f');
                write_body(buf, |_| {});
            }

            BeMessage::CopyInResponse => {
                buf.put_u8(b'G');
                write_body(buf, |buf| {
                    buf.put_u8(1); // copy_is_binary
                    buf.put_i16(0); // numAttributes
                });
            }

            BeMessage::CopyOutResponse => {
                buf.put_u8(b'H');
                write_body(buf, |buf| {
                    buf.put_u8(0); // copy_is_binary
                    buf.put_i16(0); // numAttributes
                });
            }

            BeMessage::CopyBothResponse => {
                buf.put_u8(b'W');
                write_body(buf, |buf| {
                    // doesn't matter, used only for replication
                    buf.put_u8(0); // copy_is_binary
                    buf.put_i16(0); // numAttributes
                });
            }

            BeMessage::DataRow(vals) => {
                buf.put_u8(b'D');
                write_body(buf, |buf| {
                    buf.put_u16(vals.len() as u16); // num of cols
                    for val_opt in vals.iter() {
                        if let Some(val) = val_opt {
                            buf.put_u32(val.len() as u32);
                            buf.put_slice(val);
                        } else {
                            buf.put_i32(-1);
                        }
                    }
                });
            }

            // ErrorResponse is a zero-terminated array of zero-terminated fields.
            // First byte of each field represents type of this field. Set just enough fields
            // to satisfy rust-postgres client: 'S' -- severity, 'C' -- error, 'M' -- error
            // message text.
            BeMessage::ErrorResponse(error_msg, pg_error_code) => {
                // 'E' signalizes ErrorResponse messages
                buf.put_u8(b'E');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    buf.put_slice(b"ERROR\0");

                    buf.put_u8(b'C'); // SQLSTATE error code
                    buf.put_slice(&terminate_code(
                        pg_error_code.unwrap_or(SQLSTATE_INTERNAL_ERROR),
                    ));

                    buf.put_u8(b'M'); // the message
                    write_cstr(error_msg, buf)?;

                    buf.put_u8(0); // terminator
                    Ok(())
                })?;
            }

            // NoticeResponse has the same format as ErrorResponse. From doc: "The frontend should display the
            // message but continue listening for ReadyForQuery or ErrorResponse"
            BeMessage::NoticeResponse(error_msg) => {
                // For all the errors set Severity to Error and error code to
                // 'internal error'.

                // 'N' signalizes NoticeResponse messages
                buf.put_u8(b'N');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    buf.put_slice(b"NOTICE\0");

                    buf.put_u8(b'C'); // SQLSTATE error code
                    buf.put_slice(&terminate_code(SQLSTATE_INTERNAL_ERROR));

                    buf.put_u8(b'M'); // the message
                    write_cstr(error_msg.as_bytes(), buf)?;

                    buf.put_u8(0); // terminator
                    Ok(())
                })?;
            }

            BeMessage::NoData => {
                buf.put_u8(b'n');
                write_body(buf, |_| {});
            }

            BeMessage::EncryptionResponse(should_negotiate) => {
                let response = if *should_negotiate { b'S' } else { b'N' };
                buf.put_u8(response);
            }

            BeMessage::ParameterStatus { name, value } => {
                buf.put_u8(b'S');
                write_body(buf, |buf| {
                    write_cstr(name, buf)?;
                    write_cstr(value, buf)
                })?;
            }

            BeMessage::ParameterDescription => {
                buf.put_u8(b't');
                write_body(buf, |buf| {
                    // we don't support params, so always 0
                    buf.put_i16(0);
                });
            }

            BeMessage::ParseComplete => {
                buf.put_u8(b'1');
                write_body(buf, |_| {});
            }

            BeMessage::ReadyForQuery => {
                buf.put_u8(b'Z');
                write_body(buf, |buf| {
                    buf.put_u8(b'I');
                });
            }

            BeMessage::RowDescription(rows) => {
                buf.put_u8(b'T');
                write_body(buf, |buf| {
                    buf.put_i16(rows.len() as i16); // # of fields
                    for row in rows.iter() {
                        write_cstr(row.name, buf)?;
                        buf.put_i32(0); /* table oid */
                        buf.put_i16(0); /* attnum */
                        buf.put_u32(row.typoid);
                        buf.put_i16(row.typlen);
                        buf.put_i32(-1); /* typmod */
                        buf.put_i16(0); /* format code */
                    }
                    Ok(())
                })?;
            }

            BeMessage::XLogData(body) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'w');
                    buf.put_u64(body.wal_start);
                    buf.put_u64(body.wal_end);
                    buf.put_i64(body.timestamp);
                    buf.put_slice(body.data);
                });
            }

            BeMessage::KeepAlive(req) => {
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'k');
                    buf.put_u64(req.wal_end);
                    buf.put_i64(req.timestamp);
                    buf.put_u8(u8::from(req.request_reply));
                });
            }

            BeMessage::NegotiateProtocolVersion { version, options } => {
                buf.put_u8(b'v');
                write_body(buf, |buf| {
                    buf.put_u32(version.0);
                    buf.put_u32(options.len() as u32);
                    for option in options.iter() {
                        write_cstr(option, buf)?;
                    }
                    Ok(())
                })?
            }

            BeMessage::InterpretedWalRecords(rec) => {
                // We use the COPY_DATA_TAG for our custom message
                // since this tag is interpreted as raw bytes.
                buf.put_u8(b'd');
                write_body(buf, |buf| {
                    buf.put_u8(b'0'); // matches INTERPRETED_WAL_RECORD_TAG in postgres-protocol
                                      // dependency
                    buf.put_u64(rec.streaming_lsn);
                    buf.put_u64(rec.commit_lsn);
                    buf.put_slice(rec.data);
                });
            }
        }
        Ok(())
    }
}

fn terminate_code(code: &[u8; 5]) -> [u8; 6] {
    let mut terminated = [0; 6];
    for (i, &elem) in code.iter().enumerate() {
        terminated[i] = elem;
    }

    terminated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_startup_message_params_options_escaped() {
        fn split_options(params: &StartupMessageParams) -> Vec<Cow<'_, str>> {
            params
                .options_escaped()
                .expect("options are None")
                .collect()
        }

        let make_params = |options| StartupMessageParams::new([("options", options)]);

        let params = StartupMessageParams::new([]);
        assert!(params.options_escaped().is_none());

        let params = make_params("");
        assert!(split_options(&params).is_empty());

        let params = make_params("foo");
        assert_eq!(split_options(&params), ["foo"]);

        let params = make_params(" foo  bar ");
        assert_eq!(split_options(&params), ["foo", "bar"]);

        let params = make_params("foo\\ bar \\ \\\\ baz\\  lol");
        assert_eq!(split_options(&params), ["foo bar", " \\", "baz ", "lol"]);
    }

    #[test]
    fn parse_fe_startup_packet_regression() {
        let data = [0, 0, 0, 7, 0, 0, 0, 0];
        FeStartupPacket::parse(&mut BytesMut::from_iter(data)).unwrap_err();
    }

    #[test]
    fn cancel_key_data() {
        let key = CancelKeyData {
            backend_pid: -1817212860,
            cancel_key: -1183897012,
        };
        assert_eq!(format!("{key}"), "CancelKeyData(93af8844b96f2a4c)");
    }
}
