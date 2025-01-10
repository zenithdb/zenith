//! This module implements pulling WAL from peer safekeepers if compute can't
//! provide it, i.e. safekeeper lags too much.

use std::time::SystemTime;
use std::{fmt, pin::pin};

use anyhow::{bail, Context};
use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage;
use safekeeper_api::models::{PeerInfo, TimelineStatus};
use safekeeper_api::Term;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::timeout;
use tokio::{
    select,
    time::sleep,
    time::{self, Duration},
};
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::types::PgLsn;
use tracing::*;
use utils::postgres_client::{ConnectionConfigArgs, PostgresClientProtocol};
use utils::{id::NodeId, lsn::Lsn, postgres_client::wal_stream_connection_config};

use crate::receive_wal::{WalAcceptor, REPLY_QUEUE_SIZE};
use crate::safekeeper::{AppendRequest, AppendRequestHeader};
use crate::timeline::WalResidentTimeline;
use crate::{
    receive_wal::MSG_QUEUE_SIZE,
    safekeeper::{
        AcceptorProposerMessage, ProposerAcceptorMessage, ProposerElected, TermHistory, TermLsn,
        VoteRequest,
    },
    SafeKeeperConf,
};

/// Entrypoint for per timeline task which always runs, checking whether
/// recovery for this safekeeper is needed and starting it if so.
#[instrument(name = "recovery", skip_all, fields(ttid = %tli.ttid))]
pub async fn recovery_main(tli: WalResidentTimeline, conf: SafeKeeperConf) {
    info!("started");

    let cancel = tli.cancel.clone();
    select! {
        _ = recovery_main_loop(tli, conf) => { unreachable!() }
        _ = cancel.cancelled() => {
            info!("stopped");
        }
    }
}

/// Should we start fetching WAL from a peer safekeeper, and if yes, from
/// which? Answer is yes, i.e. .donors is not empty if 1) there is something
/// to fetch, and we can do that without running elections; 2) there is no
/// actively streaming compute, as we don't want to compete with it.
///
/// If donor(s) are choosen, theirs last_log_term is guaranteed to be equal
/// to its last_log_term so we are sure such a leader ever had been elected.
///
/// All possible donors are returned so that we could keep connection to the
/// current one if it is good even if it slightly lags behind.
///
/// Note that term conditions above might be not met, but safekeepers are
/// still not aligned on last flush_lsn. Generally in this case until
/// elections are run it is not possible to say which safekeeper should
/// recover from which one -- history which would be committed is different
/// depending on assembled quorum (e.g. classic picture 8 from Raft paper).
/// Thus we don't try to predict it here.
async fn recovery_needed(
    tli: &WalResidentTimeline,
    heartbeat_timeout: Duration,
) -> RecoveryNeededInfo {
    let ss = tli.read_shared_state().await;
    let term = ss.sk.state().acceptor_state.term;
    let last_log_term = ss.sk.last_log_term();
    let flush_lsn = ss.sk.flush_lsn();
    // note that peers contain myself, but that's ok -- we are interested only in peers which are strictly ahead of us.
    let mut peers = ss.get_peers(heartbeat_timeout);
    // Sort by <last log term, lsn> pairs.
    peers.sort_by(|p1, p2| {
        let tl1 = TermLsn {
            term: p1.last_log_term,
            lsn: p1.flush_lsn,
        };
        let tl2 = TermLsn {
            term: p2.last_log_term,
            lsn: p2.flush_lsn,
        };
        tl2.cmp(&tl1) // desc
    });
    let num_streaming_computes = tli.get_walreceivers().get_num_streaming();
    let donors = if num_streaming_computes > 0 {
        vec![] // If there is a streaming compute, don't try to recover to not intervene.
    } else {
        peers
            .iter()
            .filter_map(|candidate| {
                // Are we interested in this candidate?
                let candidate_tl = TermLsn {
                    term: candidate.last_log_term,
                    lsn: candidate.flush_lsn,
                };
                let my_tl = TermLsn {
                    term: last_log_term,
                    lsn: flush_lsn,
                };
                if my_tl < candidate_tl {
                    // Yes, we are interested. Can we pull from it without
                    // (re)running elections? It is possible if 1) his term
                    // is equal to his last_log_term so we could act on
                    // behalf of leader of this term (we must be sure he was
                    // ever elected) and 2) our term is not higher, or we'll refuse data.
                    if candidate.term == candidate.last_log_term && candidate.term >= term {
                        Some(Donor::from(candidate))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    };
    RecoveryNeededInfo {
        term,
        last_log_term,
        flush_lsn,
        peers,
        num_streaming_computes,
        donors,
    }
}
/// Result of Timeline::recovery_needed, contains donor(s) if recovery needed and
/// fields to explain the choice.
#[derive(Debug)]
pub struct RecoveryNeededInfo {
    /// my term
    pub term: Term,
    /// my last_log_term
    pub last_log_term: Term,
    /// my flush_lsn
    pub flush_lsn: Lsn,
    /// peers from which we can fetch WAL, for observability.
    pub peers: Vec<PeerInfo>,
    /// for observability
    pub num_streaming_computes: usize,
    pub donors: Vec<Donor>,
}

// Custom to omit not important fields from PeerInfo.
impl fmt::Display for RecoveryNeededInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(
            f,
            "term: {}, last_log_term: {}, flush_lsn: {}, peers: {{",
            self.term, self.last_log_term, self.flush_lsn
        )?;
        for p in self.peers.iter() {
            write!(
                f,
                "PeerInfo {{ sk_id: {}, term: {}, last_log_term: {}, flush_lsn: {} }}, ",
                p.sk_id, p.term, p.last_log_term, p.flush_lsn
            )?;
        }
        write!(
            f,
            "}} num_streaming_computes: {}, donors: {:?}",
            self.num_streaming_computes, self.donors
        )
    }
}

#[derive(Clone, Debug)]
pub struct Donor {
    pub sk_id: NodeId,
    /// equals to last_log_term
    pub term: Term,
    pub flush_lsn: Lsn,
    pub pg_connstr: String,
    pub http_connstr: String,
}

impl From<&PeerInfo> for Donor {
    fn from(p: &PeerInfo) -> Self {
        Donor {
            sk_id: p.sk_id,
            term: p.term,
            flush_lsn: p.flush_lsn,
            pg_connstr: p.pg_connstr.clone(),
            http_connstr: p.http_connstr.clone(),
        }
    }
}

const CHECK_INTERVAL_MS: u64 = 2000;

/// Check regularly whether we need to start recovery.
async fn recovery_main_loop(tli: WalResidentTimeline, conf: SafeKeeperConf) {
    let check_duration = Duration::from_millis(CHECK_INTERVAL_MS);
    loop {
        let recovery_needed_info = recovery_needed(&tli, conf.heartbeat_timeout).await;
        match recovery_needed_info.donors.first() {
            Some(donor) => {
                info!(
                    "starting recovery from donor {}: {}",
                    donor.sk_id, recovery_needed_info
                );
                let res = tli.wal_residence_guard().await;
                if let Err(e) = res {
                    warn!("failed to obtain guard: {}", e);
                    continue;
                }
                match recover(res.unwrap(), donor, &conf).await {
                    // Note: 'write_wal rewrites WAL written before' error is
                    // expected here and might happen if compute and recovery
                    // concurrently write the same data. Eventually compute
                    // should win.
                    Err(e) => warn!("recovery failed: {:#}", e),
                    Ok(msg) => info!("recovery finished: {}", msg),
                }
            }
            None => {
                trace!(
                    "recovery not needed or not possible: {}",
                    recovery_needed_info
                );
            }
        }
        sleep(check_duration).await;
    }
}

/// Recover from the specified donor. Returns message explaining normal finish
/// reason or error.
async fn recover(
    tli: WalResidentTimeline,
    donor: &Donor,
    conf: &SafeKeeperConf,
) -> anyhow::Result<String> {
    // Learn donor term switch history to figure out starting point.
    let client = reqwest::Client::new();
    let timeline_info: TimelineStatus = client
        .get(format!(
            "http://{}/v1/tenant/{}/timeline/{}",
            donor.http_connstr, tli.ttid.tenant_id, tli.ttid.timeline_id
        ))
        .send()
        .await?
        .json()
        .await?;
    if timeline_info.acceptor_state.term != donor.term {
        bail!(
            "donor term changed from {} to {}",
            donor.term,
            timeline_info.acceptor_state.term
        );
    }
    // convert from API TermSwitchApiEntry into TermLsn.
    let donor_th = TermHistory(
        timeline_info
            .acceptor_state
            .term_history
            .iter()
            .map(|tl| Into::<TermLsn>::into(*tl))
            .collect(),
    );

    // Now understand our term history.
    let vote_request = ProposerAcceptorMessage::VoteRequest(VoteRequest { term: donor.term });
    let vote_response = match tli
        .process_msg(&vote_request)
        .await
        .context("VoteRequest handling")?
    {
        Some(AcceptorProposerMessage::VoteResponse(vr)) => vr,
        _ => {
            bail!("unexpected VoteRequest response"); // unreachable
        }
    };
    if vote_response.term != donor.term {
        bail!(
            "our term changed from {} to {}",
            donor.term,
            vote_response.term
        );
    }

    let last_common_point = match TermHistory::find_highest_common_point(
        &donor_th,
        &vote_response.term_history,
        vote_response.flush_lsn,
    ) {
        None => bail!(
            "couldn't find common point in histories, donor {:?}, sk {:?}",
            donor_th,
            vote_response.term_history,
        ),
        Some(lcp) => lcp,
    };
    info!("found last common point at {:?}", last_common_point);

    // truncate WAL locally
    let pe = ProposerAcceptorMessage::Elected(ProposerElected {
        term: donor.term,
        start_streaming_at: last_common_point.lsn,
        term_history: donor_th,
        timeline_start_lsn: Lsn::INVALID,
    });
    // Successful ProposerElected handling always returns None. If term changed,
    // we'll find out that during the streaming. Note: it is expected to get
    // 'refusing to overwrite correct WAL' here if walproposer reconnected
    // concurrently, restart helps here.
    tli.process_msg(&pe)
        .await
        .context("ProposerElected handling")?;

    recovery_stream(tli, donor, last_common_point.lsn, conf).await
}

// Pull WAL from donor, assuming handshake is already done.
async fn recovery_stream(
    tli: WalResidentTimeline,
    donor: &Donor,
    start_streaming_at: Lsn,
    conf: &SafeKeeperConf,
) -> anyhow::Result<String> {
    // TODO: pass auth token
    let connection_conf_args = ConnectionConfigArgs {
        protocol: PostgresClientProtocol::Vanilla,
        ttid: tli.ttid,
        shard_number: None,
        shard_count: None,
        shard_stripe_size: None,
        listen_pg_addr_str: &donor.pg_connstr,
        auth_token: None,
        availability_zone: None,
    };
    let cfg = wal_stream_connection_config(connection_conf_args)?;
    let mut cfg = cfg.to_tokio_postgres_config();
    // It will make safekeeper give out not committed WAL (up to flush_lsn).
    cfg.application_name(&format!("safekeeper_{}", conf.my_id));
    cfg.replication_mode(tokio_postgres::config::ReplicationMode::Physical);

    let connect_timeout = Duration::from_millis(10000);
    let (client, connection) = match time::timeout(connect_timeout, cfg.connect(postgres::NoTls))
        .await
    {
        Ok(client_and_conn) => client_and_conn?,
        Err(_elapsed) => {
            bail!("timed out while waiting {connect_timeout:?} for connection to peer safekeeper to open");
        }
    };
    trace!("connected to {:?}", donor);

    // The connection object performs the actual communication with the
    // server, spawn it off to run on its own.
    let ttid = tli.ttid;
    tokio::spawn(async move {
        if let Err(e) = connection
            .instrument(info_span!("recovery task connection poll", ttid = %ttid))
            .await
        {
            // This logging isn't very useful as error is anyway forwarded to client.
            trace!(
                "tokio_postgres connection object finished with error: {}",
                e
            );
        }
    });

    let query = format!(
        "START_REPLICATION PHYSICAL {} (term='{}')",
        start_streaming_at, donor.term
    );

    let copy_stream = client.copy_both_simple(&query).await?;
    let physical_stream = ReplicationStream::new(copy_stream);

    // As in normal walreceiver, do networking and writing to disk in parallel.
    let (msg_tx, msg_rx) = channel(MSG_QUEUE_SIZE);
    let (reply_tx, reply_rx) = channel(REPLY_QUEUE_SIZE);
    let wa = WalAcceptor::spawn(tli.wal_residence_guard().await?, msg_rx, reply_tx, None);

    let res = tokio::select! {
        r = network_io(physical_stream, msg_tx, donor.clone(), tli, conf.clone()) => r,
        r = read_replies(reply_rx, donor.term) => r.map(|()| None),
    };

    // Join the spawned WalAcceptor. At this point chans to/from it passed to
    // network routines are dropped, so it will exit as soon as it touches them.
    match wa.await {
        Ok(Ok(())) => {
            // WalAcceptor finished normally, termination reason is different
            match res {
                Ok(Some(success_desc)) => Ok(success_desc),
                Ok(None) => bail!("unexpected recovery end without error/success"), // can't happen
                Err(e) => Err(e), // network error or term change
            }
        }
        Ok(Err(e)) => Err(e), // error while processing message
        Err(e) => bail!("WalAcceptor panicked: {}", e),
    }
}

// Perform network part of streaming: read data and push it to msg_tx, send KA
// to make sender hear from us. If there is nothing coming for a while, check
// for termination.
// Returns
// - Ok(None) if channel to WalAcceptor closed -- its task should return error.
// - Ok(Some(String)) if recovery successfully completed.
// - Err if error happened while reading/writing to socket.
async fn network_io(
    physical_stream: ReplicationStream,
    msg_tx: Sender<ProposerAcceptorMessage>,
    donor: Donor,
    tli: WalResidentTimeline,
    conf: SafeKeeperConf,
) -> anyhow::Result<Option<String>> {
    let mut physical_stream = pin!(physical_stream);
    let mut last_received_lsn = Lsn::INVALID;
    // tear down connection if no data arrives withing this period
    let no_data_timeout = Duration::from_millis(30000);

    loop {
        let msg = match timeout(no_data_timeout, physical_stream.next()).await {
            Ok(next) => match next {
                None => bail!("unexpected end of replication stream"),
                Some(msg) => msg.context("get replication message")?,
            },
            Err(_) => bail!("no message received within {:?}", no_data_timeout),
        };

        match msg {
            ReplicationMessage::XLogData(xlog_data) => {
                let ar_hdr = AppendRequestHeader {
                    term: donor.term,
                    term_start_lsn: Lsn::INVALID, // unused
                    begin_lsn: Lsn(xlog_data.wal_start()),
                    end_lsn: Lsn(xlog_data.wal_start()) + xlog_data.data().len() as u64,
                    commit_lsn: Lsn::INVALID, // do not attempt to advance, peer communication anyway does it
                    truncate_lsn: Lsn::INVALID, // do not attempt to advance
                    proposer_uuid: [0; 16],
                };
                let ar = AppendRequest {
                    h: ar_hdr,
                    wal_data: xlog_data.into_data(),
                };
                trace!(
                    "processing AppendRequest {}-{}, len {}",
                    ar.h.begin_lsn,
                    ar.h.end_lsn,
                    ar.wal_data.len()
                );
                last_received_lsn = ar.h.end_lsn;
                if msg_tx
                    .send(ProposerAcceptorMessage::AppendRequest(ar))
                    .await
                    .is_err()
                {
                    return Ok(None); // chan closed, WalAcceptor terminated
                }
            }
            ReplicationMessage::PrimaryKeepAlive(_) => {
                // keepalive means nothing is being streamed for a while. Check whether we need to stop.
                let recovery_needed_info = recovery_needed(&tli, conf.heartbeat_timeout).await;
                // do current donors still contain one we currently connected to?
                if !recovery_needed_info
                    .donors
                    .iter()
                    .any(|d| d.sk_id == donor.sk_id)
                {
                    // Most likely it means we are caughtup.
                    // note: just exiting makes tokio_postgres send CopyFail to the far end.
                    return Ok(Some(format!(
                        "terminating at {} as connected safekeeper {} with term {} is not a donor anymore: {}",
                        last_received_lsn, donor.sk_id, donor.term, recovery_needed_info
                    )));
                }
            }
            _ => {}
        }
        // Send reply to each message to keep connection alive. Ideally we
        // should do that once in a while instead, but this again requires
        // stream split or similar workaround, and recovery is anyway not that
        // performance critical.
        //
        // We do not know here real write/flush LSNs (need to take mutex again
        // or check replies which are read in different future), but neither
        // sender much cares about them, so just send last received.
        physical_stream
            .as_mut()
            .standby_status_update(
                PgLsn::from(last_received_lsn.0),
                PgLsn::from(last_received_lsn.0),
                PgLsn::from(last_received_lsn.0),
                SystemTime::now(),
                0,
            )
            .await?;
    }
}

// Read replies from WalAcceptor. We are not interested much in sending them to
// donor safekeeper, so don't route them anywhere. However, we should check if
// term changes and exit if it does.
// Returns Ok(()) if channel closed, Err in case of term change.
async fn read_replies(
    mut reply_rx: Receiver<AcceptorProposerMessage>,
    donor_term: Term,
) -> anyhow::Result<()> {
    loop {
        match reply_rx.recv().await {
            Some(msg) => {
                if let AcceptorProposerMessage::AppendResponse(ar) = msg {
                    if ar.term != donor_term {
                        bail!("donor term changed from {} to {}", donor_term, ar.term);
                    }
                }
            }
            None => return Ok(()), // chan closed, WalAcceptor terminated
        }
    }
}
