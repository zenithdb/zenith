//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use crate::handler::SafekeeperPostgresHandler;
use crate::metrics::{
    WAL_RECEIVERS, WAL_RECEIVER_QUEUE_DEPTH, WAL_RECEIVER_QUEUE_DEPTH_TOTAL,
    WAL_RECEIVER_QUEUE_SIZE_TOTAL,
};
use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;
use crate::timeline::WalResidentTimeline;
use crate::GlobalTimelines;
use anyhow::{anyhow, Context};
use bytes::BytesMut;
use parking_lot::MappedMutexGuard;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use postgres_backend::CopyStreamHandlerEnd;
use postgres_backend::PostgresBackend;
use postgres_backend::PostgresBackendReader;
use postgres_backend::QueryError;
use pq_proto::BeMessage;
use safekeeper_api::membership::Configuration;
use safekeeper_api::models::{ConnectionId, WalReceiverState, WalReceiverStatus};
use safekeeper_api::ServerInfo;
use std::future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tracing::*;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;
use utils::pageserver_feedback::PageserverFeedback;

const DEFAULT_FEEDBACK_CAPACITY: usize = 8;

/// Registry of WalReceivers (compute connections). Timeline holds it (wrapped
/// in Arc).
pub struct WalReceivers {
    mutex: Mutex<WalReceiversShared>,
    pageserver_feedback_tx: tokio::sync::broadcast::Sender<PageserverFeedback>,

    num_computes_tx: tokio::sync::watch::Sender<usize>,
    num_computes_rx: tokio::sync::watch::Receiver<usize>,
}

/// Id under which walreceiver is registered in shmem.
type WalReceiverId = usize;

impl WalReceivers {
    pub fn new() -> Arc<WalReceivers> {
        let (pageserver_feedback_tx, _) =
            tokio::sync::broadcast::channel(DEFAULT_FEEDBACK_CAPACITY);

        let (num_computes_tx, num_computes_rx) = tokio::sync::watch::channel(0usize);

        Arc::new(WalReceivers {
            mutex: Mutex::new(WalReceiversShared { slots: Vec::new() }),
            pageserver_feedback_tx,
            num_computes_tx,
            num_computes_rx,
        })
    }

    /// Register new walreceiver. Returned guard provides access to the slot and
    /// automatically deregisters in Drop.
    pub fn register(self: &Arc<WalReceivers>, conn_id: Option<ConnectionId>) -> WalReceiverGuard {
        let mut shared = self.mutex.lock();
        let slots = &mut shared.slots;
        let walreceiver = WalReceiverState {
            conn_id,
            status: WalReceiverStatus::Voting,
        };
        // find empty slot or create new one
        let pos = if let Some(pos) = slots.iter().position(|s| s.is_none()) {
            slots[pos] = Some(walreceiver);
            pos
        } else {
            let pos = slots.len();
            slots.push(Some(walreceiver));
            pos
        };

        self.update_num(&shared);
        WAL_RECEIVERS.inc();

        WalReceiverGuard {
            id: pos,
            walreceivers: self.clone(),
        }
    }

    /// Get reference to locked slot contents. Slot must exist (registered
    /// earlier).
    fn get_slot<'a>(
        self: &'a Arc<WalReceivers>,
        id: WalReceiverId,
    ) -> MappedMutexGuard<'a, WalReceiverState> {
        MutexGuard::map(self.mutex.lock(), |locked| {
            locked.slots[id]
                .as_mut()
                .expect("walreceiver doesn't exist")
        })
    }

    /// Get number of walreceivers (compute connections).
    pub fn get_num(self: &Arc<WalReceivers>) -> usize {
        self.mutex.lock().get_num()
    }

    /// Get channel for number of walreceivers.
    pub fn get_num_rx(self: &Arc<WalReceivers>) -> tokio::sync::watch::Receiver<usize> {
        self.num_computes_rx.clone()
    }

    /// Should get called after every update of slots.
    fn update_num(self: &Arc<WalReceivers>, shared: &MutexGuard<WalReceiversShared>) {
        let num = shared.get_num();
        self.num_computes_tx.send_replace(num);
    }

    /// Get state of all walreceivers.
    pub fn get_all(self: &Arc<WalReceivers>) -> Vec<WalReceiverState> {
        self.mutex.lock().slots.iter().flatten().cloned().collect()
    }

    /// Get number of streaming walreceivers (normally 0 or 1) from compute.
    pub fn get_num_streaming(self: &Arc<WalReceivers>) -> usize {
        self.mutex
            .lock()
            .slots
            .iter()
            .flatten()
            // conn_id.is_none skips recovery which also registers here
            .filter(|s| s.conn_id.is_some() && matches!(s.status, WalReceiverStatus::Streaming))
            .count()
    }

    /// Unregister walreceiver.
    fn unregister(self: &Arc<WalReceivers>, id: WalReceiverId) {
        let mut shared = self.mutex.lock();
        shared.slots[id] = None;
        self.update_num(&shared);
        WAL_RECEIVERS.dec();
    }

    /// Broadcast pageserver feedback to connected walproposers.
    pub fn broadcast_pageserver_feedback(&self, feedback: PageserverFeedback) {
        // Err means there is no subscribers, it is fine.
        let _ = self.pageserver_feedback_tx.send(feedback);
    }
}

/// Only a few connections are expected (normally one), so store in Vec.
struct WalReceiversShared {
    slots: Vec<Option<WalReceiverState>>,
}

impl WalReceiversShared {
    /// Get number of walreceivers (compute connections).
    fn get_num(&self) -> usize {
        self.slots.iter().flatten().count()
    }
}

/// Scope guard to access slot in WalReceivers registry and unregister from
/// it in Drop.
pub struct WalReceiverGuard {
    id: WalReceiverId,
    walreceivers: Arc<WalReceivers>,
}

impl WalReceiverGuard {
    /// Get reference to locked shared state contents.
    fn get(&self) -> MappedMutexGuard<WalReceiverState> {
        self.walreceivers.get_slot(self.id)
    }
}

impl Drop for WalReceiverGuard {
    fn drop(&mut self) {
        self.walreceivers.unregister(self.id);
    }
}

pub const MSG_QUEUE_SIZE: usize = 256;
pub const REPLY_QUEUE_SIZE: usize = 16;

impl SafekeeperPostgresHandler {
    /// Wrapper around handle_start_wal_push_guts handling result. Error is
    /// handled here while we're still in walreceiver ttid span; with API
    /// extension, this can probably be moved into postgres_backend.
    pub async fn handle_start_wal_push<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
    ) -> Result<(), QueryError> {
        let mut tli: Option<WalResidentTimeline> = None;
        if let Err(end) = self.handle_start_wal_push_guts(pgb, &mut tli).await {
            // Log the result and probably send it to the client, closing the stream.
            let handle_end_fut = pgb.handle_copy_stream_end(end);
            // If we managed to create the timeline, augment logging with current LSNs etc.
            if let Some(tli) = tli {
                let info = tli.get_safekeeper_info(&self.conf).await;
                handle_end_fut
                    .instrument(info_span!("", term=%info.term, last_log_term=%info.last_log_term, flush_lsn=%Lsn(info.flush_lsn), commit_lsn=%Lsn(info.commit_lsn)))
                    .await;
            } else {
                handle_end_fut.await;
            }
        }
        Ok(())
    }

    pub async fn handle_start_wal_push_guts<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        tli: &mut Option<WalResidentTimeline>,
    ) -> Result<(), CopyStreamHandlerEnd> {
        // The `tli` parameter is only used for passing _out_ a timeline, one should
        // not have been passed in.
        assert!(tli.is_none());

        // Notify the libpq client that it's allowed to send `CopyData` messages
        pgb.write_message(&BeMessage::CopyBothResponse).await?;

        // Experiments [1] confirm that doing network IO in one (this) thread and
        // processing with disc IO in another significantly improves
        // performance; we spawn off WalAcceptor thread for message processing
        // to this end.
        //
        // [1] https://github.com/neondatabase/neon/pull/1318
        let (msg_tx, msg_rx) = channel(MSG_QUEUE_SIZE);
        let (reply_tx, reply_rx) = channel(REPLY_QUEUE_SIZE);
        let mut acceptor_handle: Option<JoinHandle<anyhow::Result<()>>> = None;

        // Concurrently receive and send data; replies are not synchronized with
        // sends, so this avoids deadlocks.
        let mut pgb_reader = pgb.split().context("START_WAL_PUSH split")?;
        let peer_addr = *pgb.get_peer_addr();

        let mut network_reader = NetworkReader {
            ttid: self.ttid,
            conn_id: self.conn_id,
            pgb_reader: &mut pgb_reader,
            peer_addr,
            acceptor_handle: &mut acceptor_handle,
            global_timelines: self.global_timelines.clone(),
        };

        // Read first message and create timeline if needed.
        let res = network_reader.read_first_message().await;

        let network_res = if let Ok((timeline, next_msg)) = res {
            let pageserver_feedback_rx: tokio::sync::broadcast::Receiver<PageserverFeedback> =
                timeline
                    .get_walreceivers()
                    .pageserver_feedback_tx
                    .subscribe();
            *tli = Some(timeline.wal_residence_guard().await?);

            let timeline_cancel = timeline.cancel.clone();
            tokio::select! {
                // todo: add read|write .context to these errors
                r = network_reader.run(msg_tx, msg_rx, reply_tx, timeline, next_msg) => r,
                r = network_write(pgb, reply_rx, pageserver_feedback_rx) => r,
                _ = timeline_cancel.cancelled() => {
                    return Err(CopyStreamHandlerEnd::Cancelled);
                }
            }
        } else {
            res.map(|_| ())
        };

        // Join pg backend back.
        pgb.unsplit(pgb_reader)?;

        // Join the spawned WalAcceptor. At this point chans to/from it passed
        // to network routines are dropped, so it will exit as soon as it
        // touches them.
        match acceptor_handle {
            None => {
                // failed even before spawning; read_network should have error
                Err(network_res.expect_err("no error with WalAcceptor not spawn"))
            }
            Some(handle) => {
                let wal_acceptor_res = handle.await;

                // If there was any network error, return it.
                network_res?;

                // Otherwise, WalAcceptor thread must have errored.
                match wal_acceptor_res {
                    Ok(Ok(_)) => Ok(()), // Clean shutdown
                    Ok(Err(e)) => Err(CopyStreamHandlerEnd::Other(e.context("WAL acceptor"))),
                    Err(_) => Err(CopyStreamHandlerEnd::Other(anyhow!(
                        "WalAcceptor task panicked",
                    ))),
                }
            }
        }
    }
}

struct NetworkReader<'a, IO> {
    ttid: TenantTimelineId,
    conn_id: ConnectionId,
    pgb_reader: &'a mut PostgresBackendReader<IO>,
    peer_addr: SocketAddr,
    // WalAcceptor is spawned when we learn server info from walproposer and
    // create timeline; handle is put here.
    acceptor_handle: &'a mut Option<JoinHandle<anyhow::Result<()>>>,
    global_timelines: Arc<GlobalTimelines>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> NetworkReader<'_, IO> {
    async fn read_first_message(
        &mut self,
    ) -> Result<(WalResidentTimeline, ProposerAcceptorMessage), CopyStreamHandlerEnd> {
        // Receive information about server to create timeline, if not yet.
        let next_msg = read_message(self.pgb_reader).await?;
        let tli = match next_msg {
            ProposerAcceptorMessage::Greeting(ref greeting) => {
                info!(
                    "start handshake with walproposer {} sysid {} timeline {}",
                    self.peer_addr, greeting.system_id, greeting.tli,
                );
                let server_info = ServerInfo {
                    pg_version: greeting.pg_version,
                    system_id: greeting.system_id,
                    wal_seg_size: greeting.wal_seg_size,
                };
                let tli = self
                    .global_timelines
                    .create(
                        self.ttid,
                        Configuration::empty(),
                        server_info,
                        Lsn::INVALID,
                        Lsn::INVALID,
                    )
                    .await
                    .context("create timeline")?;
                tli.wal_residence_guard().await?
            }
            _ => {
                return Err(CopyStreamHandlerEnd::Other(anyhow::anyhow!(
                    "unexpected message {next_msg:?} instead of greeting"
                )))
            }
        };
        Ok((tli, next_msg))
    }

    /// This function is cancellation-safe (only does network I/O and channel read/writes).
    async fn run(
        self,
        msg_tx: Sender<ProposerAcceptorMessage>,
        msg_rx: Receiver<ProposerAcceptorMessage>,
        reply_tx: Sender<AcceptorProposerMessage>,
        tli: WalResidentTimeline,
        next_msg: ProposerAcceptorMessage,
    ) -> Result<(), CopyStreamHandlerEnd> {
        *self.acceptor_handle = Some(WalAcceptor::spawn(
            tli,
            msg_rx,
            reply_tx,
            Some(self.conn_id),
        ));

        // Forward all messages to WalAcceptor
        read_network_loop(self.pgb_reader, msg_tx, next_msg).await
    }
}

/// Read next message from walproposer.
/// TODO: Return Ok(None) on graceful termination.
async fn read_message<IO: AsyncRead + AsyncWrite + Unpin>(
    pgb_reader: &mut PostgresBackendReader<IO>,
) -> Result<ProposerAcceptorMessage, CopyStreamHandlerEnd> {
    let copy_data = pgb_reader.read_copy_message().await?;
    let msg = ProposerAcceptorMessage::parse(copy_data)?;
    Ok(msg)
}

async fn read_network_loop<IO: AsyncRead + AsyncWrite + Unpin>(
    pgb_reader: &mut PostgresBackendReader<IO>,
    msg_tx: Sender<ProposerAcceptorMessage>,
    mut next_msg: ProposerAcceptorMessage,
) -> Result<(), CopyStreamHandlerEnd> {
    /// Threshold for logging slow WalAcceptor sends.
    const SLOW_THRESHOLD: Duration = Duration::from_secs(5);

    loop {
        let started = Instant::now();
        let size = next_msg.size();

        match msg_tx.send_timeout(next_msg, SLOW_THRESHOLD).await {
            Ok(()) => {}
            // Slow send, log a message and keep trying. Log context has timeline ID.
            Err(SendTimeoutError::Timeout(next_msg)) => {
                warn!(
                    "slow WalAcceptor send blocked for {:.3}s",
                    Instant::now().duration_since(started).as_secs_f64()
                );
                if msg_tx.send(next_msg).await.is_err() {
                    return Ok(()); // WalAcceptor terminated
                }
                warn!(
                    "slow WalAcceptor send completed after {:.3}s",
                    Instant::now().duration_since(started).as_secs_f64()
                )
            }
            // WalAcceptor terminated.
            Err(SendTimeoutError::Closed(_)) => return Ok(()),
        }

        // Update metrics. Will be decremented in WalAcceptor.
        WAL_RECEIVER_QUEUE_DEPTH_TOTAL.inc();
        WAL_RECEIVER_QUEUE_SIZE_TOTAL.add(size as i64);

        next_msg = read_message(pgb_reader).await?;
    }
}

/// Read replies from WalAcceptor and pass them back to socket. Returns Ok(())
/// if reply_rx closed; it must mean WalAcceptor terminated, joining it should
/// tell the error.
///
/// This function is cancellation-safe (only does network I/O and channel read/writes).
async fn network_write<IO: AsyncRead + AsyncWrite + Unpin>(
    pgb_writer: &mut PostgresBackend<IO>,
    mut reply_rx: Receiver<AcceptorProposerMessage>,
    mut pageserver_feedback_rx: tokio::sync::broadcast::Receiver<PageserverFeedback>,
) -> Result<(), CopyStreamHandlerEnd> {
    let mut buf = BytesMut::with_capacity(128);

    // storing append_response to inject PageserverFeedback into it
    let mut last_append_response = None;

    loop {
        // trying to read either AcceptorProposerMessage or PageserverFeedback
        let msg = tokio::select! {
            reply = reply_rx.recv() => {
                if let Some(msg) = reply {
                    if let AcceptorProposerMessage::AppendResponse(append_response) = &msg {
                        last_append_response = Some(append_response.clone());
                    }
                    Some(msg)
                } else {
                    return Ok(()); // chan closed, WalAcceptor terminated
                }
            }

            feedback = pageserver_feedback_rx.recv() =>
                match (feedback, &last_append_response) {
                    (Ok(feedback), Some(append_response)) => {
                        // clone AppendResponse and inject PageserverFeedback into it
                        let mut append_response = append_response.clone();
                        append_response.pageserver_feedback = Some(feedback);
                        Some(AcceptorProposerMessage::AppendResponse(append_response))
                    }
                    _ => None,
                },
        };

        let Some(msg) = msg else {
            continue;
        };

        buf.clear();
        msg.serialize(&mut buf)?;
        pgb_writer.write_message(&BeMessage::CopyData(&buf)).await?;
    }
}

/// The WAL flush interval. This ensures we periodically flush the WAL and send AppendResponses to
/// walproposer, even when it's writing a steady stream of messages.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// The metrics computation interval.
///
/// The Prometheus poll interval is 60 seconds at the time of writing. We sample the queue depth
/// every 5 seconds, for 12 samples per poll. This will give a count of up to 12x active timelines.
const METRICS_INTERVAL: Duration = Duration::from_secs(5);

/// Encapsulates a task which takes messages from msg_rx, processes and pushes
/// replies to reply_tx.
///
/// Reading from socket and writing to disk in parallel is beneficial for
/// performance, this struct provides the writing to disk part.
pub struct WalAcceptor {
    tli: WalResidentTimeline,
    msg_rx: Receiver<ProposerAcceptorMessage>,
    reply_tx: Sender<AcceptorProposerMessage>,
    conn_id: Option<ConnectionId>,
}

impl WalAcceptor {
    /// Spawn task with WalAcceptor running, return handle to it. Task returns
    /// Ok(()) if either of channels has closed, and Err if any error during
    /// message processing is encountered.
    ///
    /// conn_id None means WalAcceptor is used by recovery initiated at this safekeeper.
    pub fn spawn(
        tli: WalResidentTimeline,
        msg_rx: Receiver<ProposerAcceptorMessage>,
        reply_tx: Sender<AcceptorProposerMessage>,
        conn_id: Option<ConnectionId>,
    ) -> JoinHandle<anyhow::Result<()>> {
        task::spawn(async move {
            let mut wa = WalAcceptor {
                tli,
                msg_rx,
                reply_tx,
                conn_id,
            };

            let span_ttid = wa.tli.ttid; // satisfy borrow checker
            wa.run()
                .instrument(
                    info_span!("WAL acceptor", cid = %conn_id.unwrap_or(0), ttid = %span_ttid),
                )
                .await
        })
    }

    /// The main loop. Returns Ok(()) if either msg_rx or reply_tx got closed;
    /// it must mean that network thread terminated.
    ///
    /// This function is *not* cancellation safe, it does local disk I/O: it should always
    /// be allowed to run to completion. It respects Timeline::cancel and shuts down cleanly
    /// when that gets triggered.
    async fn run(&mut self) -> anyhow::Result<()> {
        let walreceiver_guard = self.tli.get_walreceivers().register(self.conn_id);

        // Periodically flush the WAL and compute metrics.
        let mut flush_ticker = tokio::time::interval(FLUSH_INTERVAL);
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        flush_ticker.tick().await; // skip the initial, immediate tick

        let mut metrics_ticker = tokio::time::interval(METRICS_INTERVAL);
        metrics_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Tracks whether we have unflushed appends.
        let mut dirty = false;

        while !self.tli.is_cancelled() {
            let reply = tokio::select! {
                // Process inbound message.
                msg = self.msg_rx.recv() => {
                    // If disconnected, break to flush WAL and return.
                    let Some(mut msg) = msg else {
                        break;
                    };

                    // Update gauge metrics.
                    WAL_RECEIVER_QUEUE_DEPTH_TOTAL.dec();
                    WAL_RECEIVER_QUEUE_SIZE_TOTAL.sub(msg.size() as i64);

                    // Update walreceiver state in shmem for reporting.
                    if let ProposerAcceptorMessage::Elected(_) = &msg {
                        walreceiver_guard.get().status = WalReceiverStatus::Streaming;
                    }

                    // Don't flush the WAL on every append, only periodically via flush_ticker.
                    // This batches multiple appends per fsync. If the channel is empty after
                    // sending the reply, we'll schedule an immediate flush.
                    //
                    // Note that a flush can still happen on segment bounds, which will result
                    // in an AppendResponse.
                    if let ProposerAcceptorMessage::AppendRequest(append_request) = msg {
                        msg = ProposerAcceptorMessage::NoFlushAppendRequest(append_request);
                        dirty = true;
                    }

                    self.tli.process_msg(&msg).await?
                }

                // While receiving AppendRequests, flush the WAL periodically and respond with an
                // AppendResponse to let walproposer know we're still alive.
                _ = flush_ticker.tick(), if dirty => {
                    dirty = false;
                    self.tli
                        .process_msg(&ProposerAcceptorMessage::FlushWAL)
                        .await?
                }

                // If there are no pending messages, flush the WAL immediately.
                //
                // TODO: this should be done via flush_ticker.reset_immediately(), but that's always
                // delayed by 1ms due to this bug: https://github.com/tokio-rs/tokio/issues/6866.
                _ = future::ready(()), if dirty && self.msg_rx.is_empty() => {
                    dirty = false;
                    flush_ticker.reset();
                    self.tli
                        .process_msg(&ProposerAcceptorMessage::FlushWAL)
                        .await?
                }

                // Update histogram metrics periodically.
                _ = metrics_ticker.tick() => {
                    WAL_RECEIVER_QUEUE_DEPTH.observe(self.msg_rx.len() as f64);
                    None // no reply
                }

                _ = self.tli.cancel.cancelled() => {
                    break;
                }
            };

            // Send reply, if any.
            if let Some(reply) = reply {
                if self.reply_tx.send(reply).await.is_err() {
                    break; // disconnected, break to flush WAL and return
                }
            }
        }

        // Flush WAL on disconnect, see https://github.com/neondatabase/neon/issues/9259.
        if dirty && !self.tli.cancel.is_cancelled() {
            self.tli
                .process_msg(&ProposerAcceptorMessage::FlushWAL)
                .await?;
        }

        Ok(())
    }
}

/// On drop, drain msg_rx and update metrics to avoid leaks.
impl Drop for WalAcceptor {
    fn drop(&mut self) {
        self.msg_rx.close(); // prevent further sends
        while let Ok(msg) = self.msg_rx.try_recv() {
            WAL_RECEIVER_QUEUE_DEPTH_TOTAL.dec();
            WAL_RECEIVER_QUEUE_SIZE_TOTAL.sub(msg.size() as i64);
        }
    }
}
