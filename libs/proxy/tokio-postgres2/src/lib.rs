//! An asynchronous, pipelined, PostgreSQL client.
#![warn(clippy::all)]

pub use crate::cancel_token::CancelToken;
pub use crate::client::{Client, SocketConfig};
pub use crate::config::Config;
pub use crate::connect_raw::RawConnection;
pub use crate::connection::Connection;
use crate::error::DbError;
pub use crate::error::Error;
pub use crate::generic_client::GenericClient;
pub use crate::query::RowStream;
pub use crate::row::{Row, SimpleQueryRow};
pub use crate::simple_query::SimpleQueryStream;
pub use crate::statement::{Column, Statement};
pub use crate::tls::NoTls;
pub use crate::to_statement::ToStatement;
pub use crate::transaction::Transaction;
pub use crate::transaction_builder::{IsolationLevel, TransactionBuilder};
use crate::types::ToSql;
use postgres_protocol2::message::backend::ReadyForQueryBody;

/// After executing a query, the connection will be in one of these states
#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(u8)]
pub enum ReadyForQueryStatus {
    /// Connection state is unknown
    Unknown,
    /// Connection is idle (no transactions)
    Idle = b'I',
    /// Connection is in a transaction block
    Transaction = b'T',
    /// Connection is in a failed transaction block
    FailedTransaction = b'E',
}

impl From<ReadyForQueryBody> for ReadyForQueryStatus {
    fn from(value: ReadyForQueryBody) -> Self {
        match value.status() {
            b'I' => Self::Idle,
            b'T' => Self::Transaction,
            b'E' => Self::FailedTransaction,
            _ => Self::Unknown,
        }
    }
}

mod cancel_query;
mod cancel_query_raw;
mod cancel_token;
mod client;
mod codec;
pub mod config;
mod connect;
mod connect_raw;
mod connect_socket;
mod connect_tls;
mod connection;
pub mod error;
mod generic_client;
pub mod maybe_tls_stream;
mod prepare;
mod query;
pub mod row;
mod simple_query;
mod statement;
pub mod tls;
mod to_statement;
mod transaction;
mod transaction_builder;
pub mod types;

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    process_id: i32,
    channel: String,
    payload: String,
}

impl Notification {
    /// The process ID of the notifying backend process.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// The name of the channel that the notify has been raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The "payload" string passed from the notifying process.
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

/// An asynchronous message from the server.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AsyncMessage {
    /// A notice.
    ///
    /// Notices use the same format as errors, but aren't "errors" per-se.
    Notice(DbError),
    /// A notification.
    ///
    /// Connections can subscribe to notifications with the `LISTEN` command.
    Notification(Notification),
}

/// Message returned by the `SimpleQuery` stream.
#[derive(Debug)]
#[non_exhaustive]
pub enum SimpleQueryMessage {
    /// A row of data.
    Row(SimpleQueryRow),
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    CommandComplete(u64),
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a (dyn ToSql + Sync)> + 'a {
    s.iter().map(|s| *s as _)
}
