//!
//! This module implements JSON_CTRL protocol, which allows exchange
//! JSON messages over psql for testing purposes.
//!
//! Currently supports AppendLogicalMessage, which is used for WAL
//! modifications in tests.
//!

use anyhow::Context;
use postgres_backend::QueryError;
use safekeeper_api::{ServerInfo, Term};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::*;

use crate::handler::SafekeeperPostgresHandler;
use crate::safekeeper::{AcceptorProposerMessage, AppendResponse};
use crate::safekeeper::{
    AppendRequest, AppendRequestHeader, ProposerAcceptorMessage, ProposerElected,
};
use crate::safekeeper::{TermHistory, TermLsn};
use crate::state::TimelinePersistentState;
use crate::timeline::WalResidentTimeline;
use postgres_backend::PostgresBackend;
use postgres_ffi::encode_logical_message;
use postgres_ffi::WAL_SEGMENT_SIZE;
use pq_proto::{BeMessage, RowDescriptor, TEXT_OID};
use utils::lsn::Lsn;

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendLogicalMessage {
    // prefix and message to build LogicalMessage
    pub lm_prefix: String,
    pub lm_message: String,

    // if true, commit_lsn will match flush_lsn after append
    pub set_commit_lsn: bool,

    // if true, ProposerElected will be sent before append
    pub send_proposer_elected: bool,

    // fields from AppendRequestHeader
    pub term: Term,
    #[serde(with = "utils::lsn::serde_as_u64")]
    pub epoch_start_lsn: Lsn,
    #[serde(with = "utils::lsn::serde_as_u64")]
    pub begin_lsn: Lsn,
    #[serde(with = "utils::lsn::serde_as_u64")]
    pub truncate_lsn: Lsn,
    pub pg_version: u32,
}

#[derive(Debug, Serialize)]
struct AppendResult {
    // safekeeper state after append
    state: TimelinePersistentState,
    // info about new record in the WAL
    inserted_wal: InsertedWAL,
}

/// Handles command to craft logical message WAL record with given
/// content, and then append it with specified term and lsn. This
/// function is used to test safekeepers in different scenarios.
pub async fn handle_json_ctrl<IO: AsyncRead + AsyncWrite + Unpin>(
    spg: &SafekeeperPostgresHandler,
    pgb: &mut PostgresBackend<IO>,
    append_request: &AppendLogicalMessage,
) -> Result<(), QueryError> {
    info!("JSON_CTRL request: {append_request:?}");

    // need to init safekeeper state before AppendRequest
    let tli = prepare_safekeeper(spg, append_request.pg_version).await?;

    // if send_proposer_elected is true, we need to update local history
    if append_request.send_proposer_elected {
        send_proposer_elected(&tli, append_request.term, append_request.epoch_start_lsn).await?;
    }

    let inserted_wal = append_logical_message(&tli, append_request).await?;
    let response = AppendResult {
        state: tli.get_state().await.1,
        inserted_wal,
    };
    let response_data = serde_json::to_vec(&response)
        .with_context(|| format!("Response {response:?} is not a json array"))?;

    pgb.write_message_noflush(&BeMessage::RowDescription(&[RowDescriptor {
        name: b"json",
        typoid: TEXT_OID,
        typlen: -1,
        ..Default::default()
    }]))?
    .write_message_noflush(&BeMessage::DataRow(&[Some(&response_data)]))?
    .write_message_noflush(&BeMessage::CommandComplete(b"JSON_CTRL"))?;
    Ok(())
}

/// Prepare safekeeper to process append requests without crashes,
/// by sending ProposerGreeting with default server.wal_seg_size.
async fn prepare_safekeeper(
    spg: &SafekeeperPostgresHandler,
    pg_version: u32,
) -> anyhow::Result<WalResidentTimeline> {
    let tli = spg
        .global_timelines
        .create(
            spg.ttid,
            ServerInfo {
                pg_version,
                wal_seg_size: WAL_SEGMENT_SIZE as u32,
                system_id: 0,
            },
            Lsn::INVALID,
            Lsn::INVALID,
        )
        .await?;

    tli.wal_residence_guard().await
}

async fn send_proposer_elected(
    tli: &WalResidentTimeline,
    term: Term,
    lsn: Lsn,
) -> anyhow::Result<()> {
    // add new term to existing history
    let history = tli.get_state().await.1.acceptor_state.term_history;
    let history = history.up_to(lsn.checked_sub(1u64).unwrap());
    let mut history_entries = history.0;
    history_entries.push(TermLsn { term, lsn });
    let history = TermHistory(history_entries);

    let proposer_elected_request = ProposerAcceptorMessage::Elected(ProposerElected {
        term,
        start_streaming_at: lsn,
        term_history: history,
        timeline_start_lsn: lsn,
    });

    tli.process_msg(&proposer_elected_request).await?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct InsertedWAL {
    begin_lsn: Lsn,
    pub end_lsn: Lsn,
    append_response: AppendResponse,
}

/// Extend local WAL with new LogicalMessage record. To do that,
/// create AppendRequest with new WAL and pass it to safekeeper.
pub async fn append_logical_message(
    tli: &WalResidentTimeline,
    msg: &AppendLogicalMessage,
) -> anyhow::Result<InsertedWAL> {
    let wal_data = encode_logical_message(&msg.lm_prefix, &msg.lm_message);
    let sk_state = tli.get_state().await.1;

    let begin_lsn = msg.begin_lsn;
    let end_lsn = begin_lsn + wal_data.len() as u64;

    let commit_lsn = if msg.set_commit_lsn {
        end_lsn
    } else {
        sk_state.commit_lsn
    };

    let append_request = ProposerAcceptorMessage::AppendRequest(AppendRequest {
        h: AppendRequestHeader {
            term: msg.term,
            term_start_lsn: begin_lsn,
            begin_lsn,
            end_lsn,
            commit_lsn,
            truncate_lsn: msg.truncate_lsn,
            proposer_uuid: [0u8; 16],
        },
        wal_data,
    });

    let response = tli.process_msg(&append_request).await?;

    let append_response = match response {
        Some(AcceptorProposerMessage::AppendResponse(resp)) => resp,
        _ => anyhow::bail!("not AppendResponse"),
    };

    Ok(InsertedWAL {
        begin_lsn,
        end_lsn,
        append_response,
    })
}
