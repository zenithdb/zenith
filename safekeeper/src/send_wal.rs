//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message, and registry of walsenders.

use crate::handler::SafekeeperPostgresHandler;
use crate::metrics::RECEIVED_PS_FEEDBACKS;
use crate::receive_wal::WalReceivers;
use crate::safekeeper::TermLsn;
use crate::send_interpreted_wal::InterpretedWalSender;
use crate::timeline::WalResidentTimeline;
use crate::wal_reader_stream::WalReaderStreamBuilder;
use crate::wal_storage::WalReader;
use anyhow::{bail, Context as AnyhowContext};
use bytes::Bytes;
use futures::future::Either;
use parking_lot::Mutex;
use postgres_backend::PostgresBackend;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackendReader, QueryError};
use postgres_ffi::get_current_timestamp;
use postgres_ffi::{TimestampTz, MAX_SEND_SIZE};
use pq_proto::{BeMessage, WalSndKeepAlive, XLogDataBody};
use safekeeper_api::models::{
    ConnectionId, HotStandbyFeedback, ReplicationFeedback, StandbyFeedback, StandbyReply,
    WalSenderState, INVALID_FULL_TRANSACTION_ID,
};
use safekeeper_api::Term;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::failpoint_support;
use utils::id::TenantTimelineId;
use utils::pageserver_feedback::PageserverFeedback;
use utils::postgres_client::PostgresClientProtocol;

use std::cmp::{max, min};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::time::timeout;
use tracing::*;
use utils::{bin_ser::BeSer, lsn::Lsn};

// See: https://www.postgresql.org/docs/13/protocol-replication.html
const HOT_STANDBY_FEEDBACK_TAG_BYTE: u8 = b'h';
const STANDBY_STATUS_UPDATE_TAG_BYTE: u8 = b'r';
// neon extension of replication protocol
const NEON_STATUS_UPDATE_TAG_BYTE: u8 = b'z';

/// WalSenders registry. Timeline holds it (wrapped in Arc).
pub struct WalSenders {
    mutex: Mutex<WalSendersShared>,
    walreceivers: Arc<WalReceivers>,
}

impl WalSenders {
    pub fn new(walreceivers: Arc<WalReceivers>) -> Arc<WalSenders> {
        Arc::new(WalSenders {
            mutex: Mutex::new(WalSendersShared::new()),
            walreceivers,
        })
    }

    /// Register new walsender. Returned guard provides access to the slot and
    /// automatically deregisters in Drop.
    fn register(
        self: &Arc<WalSenders>,
        ttid: TenantTimelineId,
        addr: SocketAddr,
        conn_id: ConnectionId,
        appname: Option<String>,
    ) -> WalSenderGuard {
        let slots = &mut self.mutex.lock().slots;
        let walsender_state = WalSenderState {
            ttid,
            addr,
            conn_id,
            appname,
            feedback: ReplicationFeedback::Pageserver(PageserverFeedback::empty()),
        };
        // find empty slot or create new one
        let pos = if let Some(pos) = slots.iter().position(|s| s.is_none()) {
            slots[pos] = Some(walsender_state);
            pos
        } else {
            let pos = slots.len();
            slots.push(Some(walsender_state));
            pos
        };
        WalSenderGuard {
            id: pos,
            walsenders: self.clone(),
        }
    }

    /// Get state of all walsenders.
    pub fn get_all(self: &Arc<WalSenders>) -> Vec<WalSenderState> {
        self.mutex.lock().slots.iter().flatten().cloned().collect()
    }

    /// Get LSN of the most lagging pageserver receiver. Return None if there are no
    /// active walsenders.
    pub fn laggard_lsn(self: &Arc<WalSenders>) -> Option<Lsn> {
        self.mutex
            .lock()
            .slots
            .iter()
            .flatten()
            .filter_map(|s| match s.feedback {
                ReplicationFeedback::Pageserver(feedback) => Some(feedback.last_received_lsn),
                ReplicationFeedback::Standby(_) => None,
            })
            .min()
    }

    /// Returns total counter of pageserver feedbacks received and last feedback.
    pub fn get_ps_feedback_stats(self: &Arc<WalSenders>) -> (u64, PageserverFeedback) {
        let shared = self.mutex.lock();
        (shared.ps_feedback_counter, shared.last_ps_feedback)
    }

    /// Get aggregated hot standby feedback (we send it to compute).
    pub fn get_hotstandby(self: &Arc<WalSenders>) -> StandbyFeedback {
        self.mutex.lock().agg_standby_feedback
    }

    /// Record new pageserver feedback, update aggregated values.
    fn record_ps_feedback(self: &Arc<WalSenders>, id: WalSenderId, feedback: &PageserverFeedback) {
        let mut shared = self.mutex.lock();
        shared.get_slot_mut(id).feedback = ReplicationFeedback::Pageserver(*feedback);
        shared.last_ps_feedback = *feedback;
        shared.ps_feedback_counter += 1;
        drop(shared);

        RECEIVED_PS_FEEDBACKS.inc();

        // send feedback to connected walproposers
        self.walreceivers.broadcast_pageserver_feedback(*feedback);
    }

    /// Record standby reply.
    fn record_standby_reply(self: &Arc<WalSenders>, id: WalSenderId, reply: &StandbyReply) {
        let mut shared = self.mutex.lock();
        let slot = shared.get_slot_mut(id);
        debug!(
            "Record standby reply: ts={} apply_lsn={}",
            reply.reply_ts, reply.apply_lsn
        );
        match &mut slot.feedback {
            ReplicationFeedback::Standby(sf) => sf.reply = *reply,
            ReplicationFeedback::Pageserver(_) => {
                slot.feedback = ReplicationFeedback::Standby(StandbyFeedback {
                    reply: *reply,
                    hs_feedback: HotStandbyFeedback::empty(),
                })
            }
        }
    }

    /// Record hot standby feedback, update aggregated value.
    fn record_hs_feedback(self: &Arc<WalSenders>, id: WalSenderId, feedback: &HotStandbyFeedback) {
        let mut shared = self.mutex.lock();
        let slot = shared.get_slot_mut(id);
        match &mut slot.feedback {
            ReplicationFeedback::Standby(sf) => sf.hs_feedback = *feedback,
            ReplicationFeedback::Pageserver(_) => {
                slot.feedback = ReplicationFeedback::Standby(StandbyFeedback {
                    reply: StandbyReply::empty(),
                    hs_feedback: *feedback,
                })
            }
        }
        shared.update_reply_feedback();
    }

    /// Get remote_consistent_lsn reported by the pageserver. Returns None if
    /// client is not pageserver.
    pub fn get_ws_remote_consistent_lsn(self: &Arc<WalSenders>, id: WalSenderId) -> Option<Lsn> {
        let shared = self.mutex.lock();
        let slot = shared.get_slot(id);
        match slot.feedback {
            ReplicationFeedback::Pageserver(feedback) => Some(feedback.remote_consistent_lsn),
            _ => None,
        }
    }

    /// Unregister walsender.
    fn unregister(self: &Arc<WalSenders>, id: WalSenderId) {
        let mut shared = self.mutex.lock();
        shared.slots[id] = None;
        shared.update_reply_feedback();
    }
}

struct WalSendersShared {
    // aggregated over all walsenders value
    agg_standby_feedback: StandbyFeedback,
    // last feedback ever received from any pageserver, empty if none
    last_ps_feedback: PageserverFeedback,
    // total counter of pageserver feedbacks received
    ps_feedback_counter: u64,
    slots: Vec<Option<WalSenderState>>,
}

impl WalSendersShared {
    fn new() -> Self {
        WalSendersShared {
            agg_standby_feedback: StandbyFeedback::empty(),
            last_ps_feedback: PageserverFeedback::empty(),
            ps_feedback_counter: 0,
            slots: Vec::new(),
        }
    }

    /// Get content of provided id slot, it must exist.
    fn get_slot(&self, id: WalSenderId) -> &WalSenderState {
        self.slots[id].as_ref().expect("walsender doesn't exist")
    }

    /// Get mut content of provided id slot, it must exist.
    fn get_slot_mut(&mut self, id: WalSenderId) -> &mut WalSenderState {
        self.slots[id].as_mut().expect("walsender doesn't exist")
    }

    /// Update aggregated hot standy and normal reply feedbacks. We just take min of valid xmins
    /// and ts.
    fn update_reply_feedback(&mut self) {
        let mut agg = HotStandbyFeedback::empty();
        let mut reply_agg = StandbyReply::empty();
        for ws_state in self.slots.iter().flatten() {
            if let ReplicationFeedback::Standby(standby_feedback) = ws_state.feedback {
                let hs_feedback = standby_feedback.hs_feedback;
                // doing Option math like op1.iter().chain(op2.iter()).min()
                // would be nicer, but we serialize/deserialize this struct
                // directly, so leave as is for now
                if hs_feedback.xmin != INVALID_FULL_TRANSACTION_ID {
                    if agg.xmin != INVALID_FULL_TRANSACTION_ID {
                        agg.xmin = min(agg.xmin, hs_feedback.xmin);
                    } else {
                        agg.xmin = hs_feedback.xmin;
                    }
                    agg.ts = max(agg.ts, hs_feedback.ts);
                }
                if hs_feedback.catalog_xmin != INVALID_FULL_TRANSACTION_ID {
                    if agg.catalog_xmin != INVALID_FULL_TRANSACTION_ID {
                        agg.catalog_xmin = min(agg.catalog_xmin, hs_feedback.catalog_xmin);
                    } else {
                        agg.catalog_xmin = hs_feedback.catalog_xmin;
                    }
                    agg.ts = max(agg.ts, hs_feedback.ts);
                }
                let reply = standby_feedback.reply;
                if reply.write_lsn != Lsn::INVALID {
                    if reply_agg.write_lsn != Lsn::INVALID {
                        reply_agg.write_lsn = Lsn::min(reply_agg.write_lsn, reply.write_lsn);
                    } else {
                        reply_agg.write_lsn = reply.write_lsn;
                    }
                }
                if reply.flush_lsn != Lsn::INVALID {
                    if reply_agg.flush_lsn != Lsn::INVALID {
                        reply_agg.flush_lsn = Lsn::min(reply_agg.flush_lsn, reply.flush_lsn);
                    } else {
                        reply_agg.flush_lsn = reply.flush_lsn;
                    }
                }
                if reply.apply_lsn != Lsn::INVALID {
                    if reply_agg.apply_lsn != Lsn::INVALID {
                        reply_agg.apply_lsn = Lsn::min(reply_agg.apply_lsn, reply.apply_lsn);
                    } else {
                        reply_agg.apply_lsn = reply.apply_lsn;
                    }
                }
                if reply.reply_ts != 0 {
                    if reply_agg.reply_ts != 0 {
                        reply_agg.reply_ts = TimestampTz::min(reply_agg.reply_ts, reply.reply_ts);
                    } else {
                        reply_agg.reply_ts = reply.reply_ts;
                    }
                }
            }
        }
        self.agg_standby_feedback = StandbyFeedback {
            reply: reply_agg,
            hs_feedback: agg,
        };
    }
}

// id of the occupied slot in WalSenders to access it (and save in the
// WalSenderGuard). We could give Arc directly to the slot, but there is not
// much sense in that as values aggregation which is performed on each feedback
// receival iterates over all walsenders.
pub type WalSenderId = usize;

/// Scope guard to access slot in WalSenders registry and unregister from it in
/// Drop.
pub struct WalSenderGuard {
    id: WalSenderId,
    walsenders: Arc<WalSenders>,
}

impl WalSenderGuard {
    pub fn id(&self) -> WalSenderId {
        self.id
    }

    pub fn walsenders(&self) -> &Arc<WalSenders> {
        &self.walsenders
    }
}

impl Drop for WalSenderGuard {
    fn drop(&mut self) {
        self.walsenders.unregister(self.id);
    }
}

impl SafekeeperPostgresHandler {
    /// Wrapper around handle_start_replication_guts handling result. Error is
    /// handled here while we're still in walsender ttid span; with API
    /// extension, this can probably be moved into postgres_backend.
    pub async fn handle_start_replication<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        start_pos: Lsn,
        term: Option<Term>,
    ) -> Result<(), QueryError> {
        let tli = self
            .global_timelines
            .get(self.ttid)
            .map_err(|e| QueryError::Other(e.into()))?;
        let residence_guard = tli.wal_residence_guard().await?;

        if let Err(end) = self
            .handle_start_replication_guts(pgb, start_pos, term, residence_guard)
            .await
        {
            let info = tli.get_safekeeper_info(&self.conf).await;
            // Log the result and probably send it to the client, closing the stream.
            pgb.handle_copy_stream_end(end)
            .instrument(info_span!("", term=%info.term, last_log_term=%info.last_log_term, flush_lsn=%Lsn(info.flush_lsn), commit_lsn=%Lsn(info.flush_lsn)))
            .await;
        }
        Ok(())
    }

    pub async fn handle_start_replication_guts<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        start_pos: Lsn,
        term: Option<Term>,
        tli: WalResidentTimeline,
    ) -> Result<(), CopyStreamHandlerEnd> {
        let appname = self.appname.clone();

        // Use a guard object to remove our entry from the timeline when we are done.
        let ws_guard = Arc::new(tli.get_walsenders().register(
            self.ttid,
            *pgb.get_peer_addr(),
            self.conn_id,
            self.appname.clone(),
        ));

        // Walsender can operate in one of two modes which we select by
        // application_name: give only committed WAL (used by pageserver) or all
        // existing WAL (up to flush_lsn, used by walproposer or peer recovery).
        // The second case is always driven by a consensus leader which term
        // must be supplied.
        let end_watch = if term.is_some() {
            EndWatch::Flush(tli.get_term_flush_lsn_watch_rx())
        } else {
            EndWatch::Commit(tli.get_commit_lsn_watch_rx())
        };
        // we don't check term here; it will be checked on first waiting/WAL reading anyway.
        let end_pos = end_watch.get();

        if end_pos < start_pos {
            warn!(
                "requested start_pos {} is ahead of available WAL end_pos {}",
                start_pos, end_pos
            );
        }

        info!(
            "starting streaming from {:?}, available WAL ends at {}, recovery={}, appname={:?}, protocol={:?}",
            start_pos,
            end_pos,
            matches!(end_watch, EndWatch::Flush(_)),
            appname,
            self.protocol(),
        );

        // switch to copy
        pgb.write_message(&BeMessage::CopyBothResponse).await?;

        let wal_reader = tli.get_walreader(start_pos).await?;

        // Split to concurrently receive and send data; replies are generally
        // not synchronized with sends, so this avoids deadlocks.
        let reader = pgb.split().context("START_REPLICATION split")?;

        let send_fut = match self.protocol() {
            PostgresClientProtocol::Vanilla => {
                let sender = WalSender {
                    pgb,
                    // should succeed since we're already holding another guard
                    tli: tli.wal_residence_guard().await?,
                    appname,
                    start_pos,
                    end_pos,
                    term,
                    end_watch,
                    ws_guard: ws_guard.clone(),
                    wal_reader,
                    send_buf: vec![0u8; MAX_SEND_SIZE],
                };

                Either::Left(sender.run())
            }
            PostgresClientProtocol::Interpreted {
                format,
                compression,
            } => {
                let pg_version = tli.tli.get_state().await.1.server.pg_version / 10000;
                let end_watch_view = end_watch.view();
                let wal_stream_builder = WalReaderStreamBuilder {
                    tli: tli.wal_residence_guard().await?,
                    start_pos,
                    end_pos,
                    term,
                    end_watch,
                    wal_sender_guard: ws_guard.clone(),
                };

                let sender = InterpretedWalSender {
                    format,
                    compression,
                    pgb,
                    wal_stream_builder,
                    end_watch_view,
                    shard: self.shard.unwrap(),
                    pg_version,
                    appname,
                };

                Either::Right(sender.run())
            }
        };

        let tli_cancel = tli.cancel.clone();

        let mut reply_reader = ReplyReader {
            reader,
            ws_guard: ws_guard.clone(),
            tli,
        };

        let res = tokio::select! {
            // todo: add read|write .context to these errors
            r = send_fut => r,
            r = reply_reader.run() => r,
            _ = tli_cancel.cancelled() => {
                return Err(CopyStreamHandlerEnd::Cancelled);
            }
        };

        let ws_state = ws_guard
            .walsenders
            .mutex
            .lock()
            .get_slot(ws_guard.id)
            .clone();
        info!(
            "finished streaming to {}, feedback={:?}",
            ws_state.addr, ws_state.feedback,
        );

        // Join pg backend back.
        pgb.unsplit(reply_reader.reader)?;

        res
    }
}

/// TODO(vlad): maybe lift this instead
/// Walsender streams either up to commit_lsn (normally) or flush_lsn in the
/// given term (recovery by walproposer or peer safekeeper).
#[derive(Clone)]
pub(crate) enum EndWatch {
    Commit(Receiver<Lsn>),
    Flush(Receiver<TermLsn>),
}

impl EndWatch {
    pub(crate) fn view(&self) -> EndWatchView {
        EndWatchView(self.clone())
    }

    /// Get current end of WAL.
    pub(crate) fn get(&self) -> Lsn {
        match self {
            EndWatch::Commit(r) => *r.borrow(),
            EndWatch::Flush(r) => r.borrow().lsn,
        }
    }

    /// Wait for the update.
    pub(crate) async fn changed(&mut self) -> anyhow::Result<()> {
        match self {
            EndWatch::Commit(r) => r.changed().await?,
            EndWatch::Flush(r) => r.changed().await?,
        }
        Ok(())
    }

    pub(crate) async fn wait_for_lsn(
        &mut self,
        lsn: Lsn,
        client_term: Option<Term>,
    ) -> anyhow::Result<Lsn> {
        loop {
            let end_pos = self.get();
            if end_pos > lsn {
                return Ok(end_pos);
            }
            if let EndWatch::Flush(rx) = &self {
                let curr_term = rx.borrow().term;
                if let Some(client_term) = client_term {
                    if curr_term != client_term {
                        bail!("term changed: requested {}, now {}", client_term, curr_term);
                    }
                }
            }
            self.changed().await?;
        }
    }
}

pub(crate) struct EndWatchView(EndWatch);

impl EndWatchView {
    pub(crate) fn get(&self) -> Lsn {
        self.0.get()
    }
}
/// A half driving sending WAL.
struct WalSender<'a, IO> {
    pgb: &'a mut PostgresBackend<IO>,
    tli: WalResidentTimeline,
    appname: Option<String>,
    // Position since which we are sending next chunk.
    start_pos: Lsn,
    // WAL up to this position is known to be locally available.
    // Usually this is the same as the latest commit_lsn, but in case of
    // walproposer recovery, this is flush_lsn.
    //
    // We send this LSN to the receiver as wal_end, so that it knows how much
    // WAL this safekeeper has. This LSN should be as fresh as possible.
    end_pos: Lsn,
    /// When streaming uncommitted part, the term the client acts as the leader
    /// in. Streaming is stopped if local term changes to a different (higher)
    /// value.
    term: Option<Term>,
    /// Watch channel receiver to learn end of available WAL (and wait for its advancement).
    end_watch: EndWatch,
    ws_guard: Arc<WalSenderGuard>,
    wal_reader: WalReader,
    // buffer for readling WAL into to send it
    send_buf: Vec<u8>,
}

const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);

impl<IO: AsyncRead + AsyncWrite + Unpin> WalSender<'_, IO> {
    /// Send WAL until
    /// - an error occurs
    /// - receiver is caughtup and there is no computes (if streaming up to commit_lsn)
    /// - timeline's cancellation token fires
    ///
    /// Err(CopyStreamHandlerEnd) is always returned; Result is used only for ?
    /// convenience.
    async fn run(mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            // Wait for the next portion if it is not there yet, or just
            // update our end of WAL available for sending value, we
            // communicate it to the receiver.
            self.wait_wal().await?;
            assert!(
                self.end_pos > self.start_pos,
                "nothing to send after waiting for WAL"
            );

            // try to send as much as available, capped by MAX_SEND_SIZE
            let mut chunk_end_pos = self.start_pos + MAX_SEND_SIZE as u64;
            // if we went behind available WAL, back off
            if chunk_end_pos >= self.end_pos {
                chunk_end_pos = self.end_pos;
            } else {
                // If sending not up to end pos, round down to page boundary to
                // avoid breaking WAL record not at page boundary, as protocol
                // demands. See walsender.c (XLogSendPhysical).
                chunk_end_pos = chunk_end_pos
                    .checked_sub(chunk_end_pos.block_offset())
                    .unwrap();
            }
            let send_size = (chunk_end_pos.0 - self.start_pos.0) as usize;
            let send_buf = &mut self.send_buf[..send_size];
            let send_size: usize;
            {
                // If uncommitted part is being pulled, check that the term is
                // still the expected one.
                let _term_guard = if let Some(t) = self.term {
                    Some(self.tli.acquire_term(t).await?)
                } else {
                    None
                };
                // Read WAL into buffer. send_size can be additionally capped to
                // segment boundary here.
                send_size = self.wal_reader.read(send_buf).await?
            };
            let send_buf = &send_buf[..send_size];

            // and send it, while respecting Timeline::cancel
            let msg = BeMessage::XLogData(XLogDataBody {
                wal_start: self.start_pos.0,
                wal_end: self.end_pos.0,
                timestamp: get_current_timestamp(),
                data: send_buf,
            });
            self.pgb.write_message(&msg).await?;

            if let Some(appname) = &self.appname {
                if appname == "replica" {
                    failpoint_support::sleep_millis_async!("sk-send-wal-replica-sleep");
                }
            }
            trace!(
                "sent {} bytes of WAL {}-{}",
                send_size,
                self.start_pos,
                self.start_pos + send_size as u64
            );
            self.start_pos += send_size as u64;
        }
    }

    /// wait until we have WAL to stream, sending keepalives and checking for
    /// exit in the meanwhile
    async fn wait_wal(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            self.end_pos = self.end_watch.get();
            let have_something_to_send = (|| {
                fail::fail_point!(
                    "sk-pause-send",
                    self.appname.as_deref() != Some("pageserver"),
                    |_| { false }
                );
                self.end_pos > self.start_pos
            })();

            if have_something_to_send {
                trace!("got end_pos {:?}, streaming", self.end_pos);
                return Ok(());
            }

            // Wait for WAL to appear, now self.end_pos == self.start_pos.
            if let Some(lsn) = self.wait_for_lsn().await? {
                self.end_pos = lsn;
                trace!("got end_pos {:?}, streaming", self.end_pos);
                return Ok(());
            }

            // Timed out waiting for WAL, check for termination and send KA.
            // Check for termination only if we are streaming up to commit_lsn
            // (to pageserver).
            if let EndWatch::Commit(_) = self.end_watch {
                if let Some(remote_consistent_lsn) = self
                    .ws_guard
                    .walsenders
                    .get_ws_remote_consistent_lsn(self.ws_guard.id)
                {
                    if self.tli.should_walsender_stop(remote_consistent_lsn).await {
                        // Terminate if there is nothing more to send.
                        // Note that "ending streaming" part of the string is used by
                        // pageserver to identify WalReceiverError::SuccessfulCompletion,
                        // do not change this string without updating pageserver.
                        return Err(CopyStreamHandlerEnd::ServerInitiated(format!(
                        "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
                        self.appname, self.start_pos,
                    )));
                    }
                }
            }

            let msg = BeMessage::KeepAlive(WalSndKeepAlive {
                wal_end: self.end_pos.0,
                timestamp: get_current_timestamp(),
                request_reply: true,
            });

            self.pgb.write_message(&msg).await?;
        }
    }

    /// Wait until we have available WAL > start_pos or timeout expires. Returns
    /// - Ok(Some(end_pos)) if needed lsn is successfully observed;
    /// - Ok(None) if timeout expired;
    /// - Err in case of error -- only if 1) term changed while fetching in recovery
    ///   mode 2) watch channel closed, which must never happen.
    async fn wait_for_lsn(&mut self) -> anyhow::Result<Option<Lsn>> {
        let fp = (|| {
            fail::fail_point!(
                "sk-pause-send",
                self.appname.as_deref() != Some("pageserver"),
                |_| { true }
            );
            false
        })();
        if fp {
            tokio::time::sleep(POLL_STATE_TIMEOUT).await;
            return Ok(None);
        }

        let res = timeout(POLL_STATE_TIMEOUT, async move {
            loop {
                let end_pos = self.end_watch.get();
                if end_pos > self.start_pos {
                    return Ok(end_pos);
                }
                if let EndWatch::Flush(rx) = &self.end_watch {
                    let curr_term = rx.borrow().term;
                    if let Some(client_term) = self.term {
                        if curr_term != client_term {
                            bail!("term changed: requested {}, now {}", client_term, curr_term);
                        }
                    }
                }
                self.end_watch.changed().await?;
            }
        })
        .await;

        match res {
            // success
            Ok(Ok(commit_lsn)) => Ok(Some(commit_lsn)),
            // error inside closure
            Ok(Err(err)) => Err(err),
            // timeout
            Err(_) => Ok(None),
        }
    }
}

/// A half driving receiving replies.
struct ReplyReader<IO> {
    reader: PostgresBackendReader<IO>,
    ws_guard: Arc<WalSenderGuard>,
    tli: WalResidentTimeline,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> ReplyReader<IO> {
    async fn run(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            let msg = self.reader.read_copy_message().await?;
            self.handle_feedback(&msg).await?
        }
    }

    async fn handle_feedback(&mut self, msg: &Bytes) -> anyhow::Result<()> {
        match msg.first().cloned() {
            Some(HOT_STANDBY_FEEDBACK_TAG_BYTE) => {
                // Note: deserializing is on m[1..] because we skip the tag byte.
                let mut hs_feedback = HotStandbyFeedback::des(&msg[1..])
                    .context("failed to deserialize HotStandbyFeedback")?;
                // TODO: xmin/catalog_xmin are serialized by walreceiver.c in this way:
                // pq_sendint32(&reply_message, xmin);
                // pq_sendint32(&reply_message, xmin_epoch);
                // So it is two big endian 32-bit words in low endian order!
                hs_feedback.xmin = hs_feedback.xmin.rotate_left(32);
                hs_feedback.catalog_xmin = hs_feedback.catalog_xmin.rotate_left(32);
                self.ws_guard
                    .walsenders
                    .record_hs_feedback(self.ws_guard.id, &hs_feedback);
            }
            Some(STANDBY_STATUS_UPDATE_TAG_BYTE) => {
                let reply =
                    StandbyReply::des(&msg[1..]).context("failed to deserialize StandbyReply")?;
                self.ws_guard
                    .walsenders
                    .record_standby_reply(self.ws_guard.id, &reply);
            }
            Some(NEON_STATUS_UPDATE_TAG_BYTE) => {
                // pageserver sends this.
                // Note: deserializing is on m[9..] because we skip the tag byte and len bytes.
                let buf = Bytes::copy_from_slice(&msg[9..]);
                let ps_feedback = PageserverFeedback::parse(buf);

                trace!("PageserverFeedback is {:?}", ps_feedback);
                self.ws_guard
                    .walsenders
                    .record_ps_feedback(self.ws_guard.id, &ps_feedback);
                self.tli
                    .update_remote_consistent_lsn(ps_feedback.remote_consistent_lsn)
                    .await;
                // in principle new remote_consistent_lsn could allow to
                // deactivate the timeline, but we check that regularly through
                // broker updated, not need to do it here
            }
            _ => warn!("unexpected message {:?}", msg),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use safekeeper_api::models::FullTransactionId;
    use utils::id::{TenantId, TimelineId};

    use super::*;

    fn mock_ttid() -> TenantTimelineId {
        TenantTimelineId {
            tenant_id: TenantId::from_slice(&[0x00; 16]).unwrap(),
            timeline_id: TimelineId::from_slice(&[0x00; 16]).unwrap(),
        }
    }

    fn mock_addr() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    // add to wss specified feedback setting other fields to dummy values
    fn push_feedback(wss: &mut WalSendersShared, feedback: ReplicationFeedback) {
        let walsender_state = WalSenderState {
            ttid: mock_ttid(),
            addr: mock_addr(),
            conn_id: 1,
            appname: None,
            feedback,
        };
        wss.slots.push(Some(walsender_state))
    }

    // form standby feedback with given hot standby feedback ts/xmin and the
    // rest set to dummy values.
    fn hs_feedback(ts: TimestampTz, xmin: FullTransactionId) -> ReplicationFeedback {
        ReplicationFeedback::Standby(StandbyFeedback {
            reply: StandbyReply::empty(),
            hs_feedback: HotStandbyFeedback {
                ts,
                xmin,
                catalog_xmin: 0,
            },
        })
    }

    // test that hs aggregation works as expected
    #[test]
    fn test_hs_feedback_no_valid() {
        let mut wss = WalSendersShared::new();
        push_feedback(&mut wss, hs_feedback(1, INVALID_FULL_TRANSACTION_ID));
        wss.update_reply_feedback();
        assert_eq!(
            wss.agg_standby_feedback.hs_feedback.xmin,
            INVALID_FULL_TRANSACTION_ID
        );
    }

    #[test]
    fn test_hs_feedback() {
        let mut wss = WalSendersShared::new();
        push_feedback(&mut wss, hs_feedback(1, INVALID_FULL_TRANSACTION_ID));
        push_feedback(&mut wss, hs_feedback(1, 42));
        push_feedback(&mut wss, hs_feedback(1, 64));
        wss.update_reply_feedback();
        assert_eq!(wss.agg_standby_feedback.hs_feedback.xmin, 42);
    }
}
