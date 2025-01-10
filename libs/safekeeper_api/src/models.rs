//! Types used in safekeeper http API. Many of them are also reused internally.

use postgres_ffi::TimestampTz;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::time::Instant;

use utils::{
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
    pageserver_feedback::PageserverFeedback,
};

use crate::{ServerInfo, Term};

#[derive(Debug, Serialize)]
pub struct SafekeeperStatus {
    pub id: NodeId,
}

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub peer_ids: Option<Vec<NodeId>>,
    pub pg_version: u32,
    pub system_id: Option<u64>,
    pub wal_seg_size: Option<u32>,
    pub commit_lsn: Lsn,
    // If not passed, it is assigned to the beginning of commit_lsn segment.
    pub local_start_lsn: Option<Lsn>,
}

/// Same as TermLsn, but serializes LSN using display serializer
/// in Postgres format, i.e. 0/FFFFFFFF. Used only for the API response.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TermSwitchApiEntry {
    pub term: Term,
    pub lsn: Lsn,
}

/// Augment AcceptorState with last_log_term for convenience
#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptorStateStatus {
    pub term: Term,
    pub epoch: Term, // aka last_log_term, old `epoch` name is left for compatibility
    pub term_history: Vec<TermSwitchApiEntry>,
}

/// Things safekeeper should know about timeline state on peers.
/// Used as both model and internally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub sk_id: NodeId,
    pub term: Term,
    /// Term of the last entry.
    pub last_log_term: Term,
    /// LSN of the last record.
    pub flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    /// Since which LSN safekeeper has WAL.
    pub local_start_lsn: Lsn,
    /// When info was received. Serde annotations are not very useful but make
    /// the code compile -- we don't rely on this field externally.
    #[serde(skip)]
    #[serde(default = "Instant::now")]
    pub ts: Instant,
    pub pg_connstr: String,
    pub http_connstr: String,
}

pub type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HotStandbyFeedback {
    pub ts: TimestampTz,
    pub xmin: FullTransactionId,
    pub catalog_xmin: FullTransactionId,
}

pub const INVALID_FULL_TRANSACTION_ID: FullTransactionId = 0;

impl HotStandbyFeedback {
    pub fn empty() -> HotStandbyFeedback {
        HotStandbyFeedback {
            ts: 0,
            xmin: 0,
            catalog_xmin: 0,
        }
    }
}

/// Standby status update
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StandbyReply {
    pub write_lsn: Lsn, // The location of the last WAL byte + 1 received and written to disk in the standby.
    pub flush_lsn: Lsn, // The location of the last WAL byte + 1 flushed to disk in the standby.
    pub apply_lsn: Lsn, // The location of the last WAL byte + 1 applied in the standby.
    pub reply_ts: TimestampTz, // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    pub reply_requested: bool,
}

impl StandbyReply {
    pub fn empty() -> Self {
        StandbyReply {
            write_lsn: Lsn::INVALID,
            flush_lsn: Lsn::INVALID,
            apply_lsn: Lsn::INVALID,
            reply_ts: 0,
            reply_requested: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StandbyFeedback {
    pub reply: StandbyReply,
    pub hs_feedback: HotStandbyFeedback,
}

impl StandbyFeedback {
    pub fn empty() -> Self {
        StandbyFeedback {
            reply: StandbyReply::empty(),
            hs_feedback: HotStandbyFeedback::empty(),
        }
    }
}

/// Receiver is either pageserver or regular standby, which have different
/// feedbacks.
/// Used as both model and internally.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ReplicationFeedback {
    Pageserver(PageserverFeedback),
    Standby(StandbyFeedback),
}

/// Uniquely identifies a WAL service connection. Logged in spans for
/// observability.
pub type ConnectionId = u32;

/// Serialize is used only for json'ing in API response. Also used internally.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSenderState {
    pub ttid: TenantTimelineId,
    pub addr: SocketAddr,
    pub conn_id: ConnectionId,
    // postgres application_name
    pub appname: Option<String>,
    pub feedback: ReplicationFeedback,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalReceiverState {
    /// None means it is recovery initiated by us (this safekeeper).
    pub conn_id: Option<ConnectionId>,
    pub status: WalReceiverStatus,
}

/// Walreceiver status. Currently only whether it passed voting stage and
/// started receiving the stream, but it is easy to add more if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalReceiverStatus {
    Voting,
    Streaming,
}

/// Info about timeline on safekeeper ready for reporting.
#[derive(Debug, Serialize, Deserialize)]
pub struct TimelineStatus {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub acceptor_state: AcceptorStateStatus,
    pub pg_info: ServerInfo,
    pub flush_lsn: Lsn,
    pub timeline_start_lsn: Lsn,
    pub local_start_lsn: Lsn,
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub peer_horizon_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    pub peers: Vec<PeerInfo>,
    pub walsenders: Vec<WalSenderState>,
    pub walreceivers: Vec<WalReceiverState>,
}

fn lsn_invalid() -> Lsn {
    Lsn::INVALID
}

/// Data about safekeeper's timeline, mirrors broker.proto.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SkTimelineInfo {
    /// Term.
    pub term: Option<u64>,
    /// Term of the last entry.
    pub last_log_term: Option<u64>,
    /// LSN of the last record.
    #[serde(default = "lsn_invalid")]
    pub flush_lsn: Lsn,
    /// Up to which LSN safekeeper regards its WAL as committed.
    #[serde(default = "lsn_invalid")]
    pub commit_lsn: Lsn,
    /// LSN up to which safekeeper has backed WAL.
    #[serde(default = "lsn_invalid")]
    pub backup_lsn: Lsn,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde(default = "lsn_invalid")]
    pub remote_consistent_lsn: Lsn,
    #[serde(default = "lsn_invalid")]
    pub peer_horizon_lsn: Lsn,
    #[serde(default = "lsn_invalid")]
    pub local_start_lsn: Lsn,
    /// A connection string to use for WAL receiving.
    #[serde(default)]
    pub safekeeper_connstr: Option<String>,
    #[serde(default)]
    pub http_connstr: Option<String>,
    // Minimum of all active RO replicas flush LSN
    #[serde(default = "lsn_invalid")]
    pub standby_horizon: Lsn,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimelineCopyRequest {
    pub target_timeline_id: TimelineId,
    pub until_lsn: Lsn,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimelineTermBumpRequest {
    /// bump to
    pub term: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimelineTermBumpResponse {
    // before the request
    pub previous_term: u64,
    pub current_term: u64,
}
