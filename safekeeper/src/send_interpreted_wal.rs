use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures::future::Either;
use futures::StreamExt;
use pageserver_api::shard::ShardIdentity;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackend};
use postgres_ffi::waldecoder::WalDecodeError;
use postgres_ffi::{get_current_timestamp, waldecoder::WalStreamDecoder};
use pq_proto::{BeMessage, InterpretedWalRecordsBody, WalSndKeepAlive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{info_span, Instrument};
use utils::lsn::Lsn;
use utils::postgres_client::Compression;
use utils::postgres_client::InterpretedFormat;
use wal_decoder::models::{InterpretedWalRecord, InterpretedWalRecords};
use wal_decoder::wire_format::ToWireFormat;

use crate::send_wal::{EndWatchView, WalSenderGuard};
use crate::timeline::WalResidentTimeline;
use crate::wal_reader_stream::{StreamingWalReader, WalBytes};

/// Shard-aware fan-out interpreted record reader.
/// Reads WAL from disk, decodes it, intepretets it, and sends
/// it to any [`InterpretedWalSender`] connected to it.
/// Each [`InterpretedWalSender`] corresponds to one shard
/// and gets interpreted records concerning that shard only.
pub(crate) struct InterpretedWalReader {
    wal_stream: StreamingWalReader,
    shards: HashMap<ShardIdentity, ShardState>,
    shard_notification_rx: Option<tokio::sync::mpsc::UnboundedReceiver<AttachShardNotification>>,
    state: Arc<std::sync::RwLock<InterpretedWalReaderState>>,
    pg_version: u32,
    cancel: CancellationToken,
}

/// A handle for [`InterpretedWalReader`] which allows for interacting with it
/// when it runs as a separate tokio task.
#[derive(Debug)]
pub(crate) struct InterpretedWalReaderHandle {
    join_handle: JoinHandle<Result<(), InterpretedWalReaderError>>,
    state: Arc<std::sync::RwLock<InterpretedWalReaderState>>,
    shard_notification_tx: tokio::sync::mpsc::UnboundedSender<AttachShardNotification>,
    // TODO: remove or keep
    #[allow(unused)]
    cancel: CancellationToken,
}

struct ShardState {
    tx: tokio::sync::mpsc::Sender<Batch>,
    next_record_lsn: Lsn,
}

/// State of [`InterpretedWalReader`] visible outside of the task running it.
#[derive(Debug)]
pub(crate) enum InterpretedWalReaderState {
    Running { current_position: Lsn },
    Done,
}

pub(crate) struct Batch {
    wal_end_lsn: Lsn,
    available_wal_end_lsn: Lsn,
    records: InterpretedWalRecords,
}

#[derive(thiserror::Error, Debug)]
pub enum InterpretedWalReaderError {
    /// Handler initiates the end of streaming.
    #[error("decode error: {0}")]
    Decode(#[from] WalDecodeError),
    #[error("read or interpret error: {0}")]
    ReadOrInterpret(#[from] anyhow::Error),
    #[error("wal stream closed")]
    WalStreamClosed,
}

impl InterpretedWalReaderState {
    fn current_position(&self) -> Option<Lsn> {
        match self {
            InterpretedWalReaderState::Running {
                current_position, ..
            } => Some(*current_position),
            InterpretedWalReaderState::Done => None,
        }
    }
}

pub(crate) struct AttachShardNotification {
    shard_id: ShardIdentity,
    sender: tokio::sync::mpsc::Sender<Batch>,
    start_pos: Lsn,
}

impl InterpretedWalReader {
    /// Spawn the reader in a separate tokio task and return a handle
    pub(crate) fn spawn(
        wal_stream: StreamingWalReader,
        start_pos: Lsn,
        tx: tokio::sync::mpsc::Sender<Batch>,
        shard: ShardIdentity,
        pg_version: u32,
    ) -> InterpretedWalReaderHandle {
        let cancel = CancellationToken::new();

        let state = Arc::new(std::sync::RwLock::new(InterpretedWalReaderState::Running {
            current_position: start_pos,
        }));

        let (shard_notification_tx, shard_notification_rx) = tokio::sync::mpsc::unbounded_channel();

        let reader = InterpretedWalReader {
            wal_stream,
            shards: HashMap::from([(
                shard,
                ShardState {
                    tx,
                    next_record_lsn: start_pos,
                },
            )]),
            shard_notification_rx: Some(shard_notification_rx),
            state: state.clone(),
            pg_version,
            cancel: cancel.clone(),
        };

        let join_handle = tokio::task::spawn(
            async move {
                let res = reader.run(start_pos).await;
                if let Err(ref err) = res {
                    tracing::error!("Task finished with error: {err}");
                }
                res
            }
            .instrument(info_span!("interpreted wal reader")),
        );

        InterpretedWalReaderHandle {
            join_handle,
            state,
            shard_notification_tx,
            cancel,
        }
    }

    /// Construct the reader without spawning anything
    /// Callers should drive the future returned by [`Self::run`].
    pub(crate) fn new(
        wal_stream: StreamingWalReader,
        start_pos: Lsn,
        tx: tokio::sync::mpsc::Sender<Batch>,
        shard: ShardIdentity,
        pg_version: u32,
    ) -> InterpretedWalReader {
        let cancel = CancellationToken::new();
        let state = Arc::new(std::sync::RwLock::new(InterpretedWalReaderState::Running {
            current_position: start_pos,
        }));

        InterpretedWalReader {
            wal_stream,
            shards: HashMap::from([(
                shard,
                ShardState {
                    tx,
                    next_record_lsn: start_pos,
                },
            )]),
            shard_notification_rx: None,
            state: state.clone(),
            pg_version,
            cancel: cancel.clone(),
        }
    }

    /// Send interpreted WAL to one or more [`InterpretedWalSender`]s
    /// Stops when an error is encountered or when [`InterpretedWalReaderHandle::cancel`]
    /// is called.
    pub(crate) async fn run(mut self, start_pos: Lsn) -> Result<(), InterpretedWalReaderError> {
        let defer_state = self.state.clone();
        scopeguard::defer! {
            *defer_state.write().unwrap() = InterpretedWalReaderState::Done;
        }

        let mut wal_decoder = WalStreamDecoder::new(start_pos, self.pg_version);

        loop {
            tokio::select! {
                // Main branch for reading WAL and forwarding it
                wal = self.wal_stream.next() => {
                    let WalBytes {
                        wal,
                        wal_start_lsn: _,
                        wal_end_lsn,
                        available_wal_end_lsn,
                    } = match wal {
                        Some(some) => some.map_err(InterpretedWalReaderError::ReadOrInterpret)?,
                        None => {
                            // [`StreamingWalReader::next`] is an endless stream of WAL.
                            // It shouldn't ever finish unless it panicked.
                            return Result::Err(InterpretedWalReaderError::WalStreamClosed);
                        }
                    };

                    wal_decoder.feed_bytes(&wal);

                    // Deserialize and interpret WAL records from this batch of WAL.
                    // Interpreted records for each shard are collected separately.
                    let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
                    let mut records_by_shard: HashMap<ShardIdentity, Vec<InterpretedWalRecord>> = HashMap::new();
                    let mut max_next_record_lsn = None;
                    while let Some((next_record_lsn, recdata)) = wal_decoder.poll_decode()?
                    {
                        assert!(next_record_lsn.is_aligned());
                        max_next_record_lsn = Some(next_record_lsn);

                        let interpreted = InterpretedWalRecord::from_bytes_filtered(
                            recdata,
                            &shard_ids,
                            next_record_lsn,
                            self.pg_version,
                        )
                        .with_context(|| "Failed to interpret WAL")?;

                        for (shard, record) in interpreted {
                            if !record.is_empty()
                                && record.next_record_lsn > self.shards.get(&shard).unwrap().next_record_lsn {
                                records_by_shard.entry(shard).or_default().push(record);
                            }
                        }
                    }

                    let max_next_record_lsn = match max_next_record_lsn {
                        Some(lsn) => lsn,
                        None => { continue; }
                    };

                    // Update the current position such that new receivers can decide
                    // whether to attach to us or spawn a new WAL reader.
                    match &mut *self.state.write().unwrap() {
                        InterpretedWalReaderState::Running { current_position, .. } => {
                            *current_position = max_next_record_lsn;
                        },
                        InterpretedWalReaderState::Done => {
                            unreachable!()
                        }
                    }

                    // Send interpreted records downstream. Anything that has already been seen
                    // by a shard is filtered out.
                    let mut shards_to_remove = Vec::new();
                    for (shard, state) in &mut self.shards {
                        let records = records_by_shard.remove(shard).unwrap_or_default();

                        if max_next_record_lsn <= state.next_record_lsn {
                            continue;
                        }

                        let batch = InterpretedWalRecords {
                            records,
                            next_record_lsn: Some(max_next_record_lsn),
                        };

                        let res = state.tx.send(Batch {
                            wal_end_lsn,
                            available_wal_end_lsn,
                            records: batch,
                        }).await;

                        if res.is_err() {
                            shards_to_remove.push(*shard);
                        } else {
                            state.next_record_lsn = max_next_record_lsn;
                        }
                    }

                    // Clean up any shards that have dropped out.
                    for shard in shards_to_remove {
                        let removed = self.shards.remove(&shard);
                        assert!(removed.is_some());
                    }
                },
                // Listen for new shards that want to attach to this reader.
                // If the reader is not running as a task, then this is not supported
                // (see the pending branch below).
                notification = match self.shard_notification_rx.as_mut() {
                        Some(rx) => Either::Left(rx.recv()),
                        None => Either::Right(std::future::pending())
                    } => {
                    if let Some(n) = notification {
                        let AttachShardNotification { shard_id, sender, start_pos } = n;

                        // Update internal and external state, then reset the WAL stream
                        // if required.
                        self.shards.insert(shard_id, ShardState { tx: sender, next_record_lsn: start_pos});
                        let current_pos = self.state.read().unwrap().current_position().unwrap();
                        if start_pos < current_pos {
                            self.wal_stream.reset(start_pos);
                            wal_decoder = WalStreamDecoder::new(start_pos, self.pg_version);
                        }
                    }
                }
                _ = self.cancel.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl InterpretedWalReaderHandle {
    /// Fan-out the reader by attaching a new shard to it
    pub(crate) fn fanout(
        &self,
        shard_id: ShardIdentity,
        sender: tokio::sync::mpsc::Sender<Batch>,
        start_pos: Lsn,
    ) -> Result<(), SendError<AttachShardNotification>> {
        self.shard_notification_tx.send(AttachShardNotification {
            shard_id,
            sender,
            start_pos,
        })
    }

    /// Get the current WAL position of the reader
    pub(crate) fn current_position(&self) -> Option<Lsn> {
        self.state.read().unwrap().current_position()
    }

    pub(crate) fn abort(&self) {
        self.join_handle.abort()
    }
}

impl Drop for InterpretedWalReaderHandle {
    fn drop(&mut self) {
        self.abort()
    }
}

pub(crate) struct InterpretedWalSender<'a, IO> {
    pub(crate) format: InterpretedFormat,
    pub(crate) compression: Option<Compression>,
    pub(crate) appname: Option<String>,

    pub(crate) tli: WalResidentTimeline,
    pub(crate) start_lsn: Lsn,

    pub(crate) pgb: &'a mut PostgresBackend<IO>,
    pub(crate) end_watch_view: EndWatchView,
    pub(crate) wal_sender_guard: Arc<WalSenderGuard>,
    pub(crate) rx: tokio::sync::mpsc::Receiver<Batch>,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> InterpretedWalSender<'_, IO> {
    /// Send interpreted WAL records over the network.
    /// Also manages keep-alives if nothing was sent for a while.
    pub(crate) async fn run(mut self) -> Result<(), CopyStreamHandlerEnd> {
        let mut keepalive_ticker = tokio::time::interval(Duration::from_secs(1));
        keepalive_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        keepalive_ticker.reset();

        let mut wal_position = self.start_lsn;

        loop {
            tokio::select! {
                batch = self.rx.recv() => {
                    let batch = match batch {
                        Some(b) => b,
                        None => {
                            return Result::Err(
                                CopyStreamHandlerEnd::Other(anyhow!("Interpreted WAL reader exited early"))
                            );
                        }
                    };

                    wal_position = batch.wal_end_lsn;

                    let buf = batch
                        .records
                        .to_wire(self.format, self.compression)
                        .await
                        .with_context(|| "Failed to serialize interpreted WAL")
                        .map_err(CopyStreamHandlerEnd::from)?;

                    // Reset the keep alive ticker since we are sending something
                    // over the wire now.
                    keepalive_ticker.reset();

                    self.pgb
                        .write_message(&BeMessage::InterpretedWalRecords(InterpretedWalRecordsBody {
                            streaming_lsn: batch.wal_end_lsn.0,
                            commit_lsn: batch.available_wal_end_lsn.0,
                            data: &buf,
                        })).await?;
                }
                // Send a periodic keep alive when the connection has been idle for a while.
                // Since we've been idle, also check if we can stop streaming.
                _ = keepalive_ticker.tick() => {
                    if let Some(remote_consistent_lsn) = self.wal_sender_guard
                        .walsenders()
                        .get_ws_remote_consistent_lsn(self.wal_sender_guard.id())
                    {
                        if self.tli.should_walsender_stop(remote_consistent_lsn).await {
                            // Stop streaming if the receivers are caught up and
                            // there's no active compute. This causes the loop in
                            // [`crate::send_interpreted_wal::InterpretedWalSender::run`]
                            // to exit and terminate the WAL stream.
                            break;
                        }
                    }

                    self.pgb
                        .write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                            wal_end: self.end_watch_view.get().0,
                            timestamp: get_current_timestamp(),
                            request_reply: true,
                        }))
                        .await?;
                },
            }
        }

        Err(CopyStreamHandlerEnd::ServerInitiated(format!(
            "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
            self.appname, wal_position,
        )))
    }
}
#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr, time::Duration};

    use pageserver_api::shard::{ShardIdentity, ShardStripeSize};
    use postgres_ffi::MAX_SEND_SIZE;
    use tokio::sync::mpsc::error::TryRecvError;
    use utils::{
        id::{NodeId, TenantTimelineId},
        lsn::Lsn,
        shard::{ShardCount, ShardNumber},
    };

    use crate::{
        send_interpreted_wal::{Batch, InterpretedWalReader},
        test_utils::Env,
        wal_reader_stream::StreamingWalReader,
    };

    #[tokio::test]
    async fn test_interpreted_wal_reader_fanout() {
        let _ = env_logger::builder().is_test(true).try_init();

        const SIZE: usize = 8 * 1024;
        const MSG_COUNT: usize = 200;
        const PG_VERSION: u32 = 17;
        const SHARD_COUNT: u8 = 2;

        let start_lsn = Lsn::from_str("0/149FD18").unwrap();
        let env = Env::new(true).unwrap();
        let tli = env
            .make_timeline(NodeId(1), TenantTimelineId::generate(), start_lsn)
            .await
            .unwrap();

        let resident_tli = tli.wal_residence_guard().await.unwrap();
        let end_watch = Env::write_wal(tli, start_lsn, SIZE, MSG_COUNT)
            .await
            .unwrap();
        let end_pos = end_watch.get();

        tracing::info!("Doing first round of reads ...");

        let streaming_wal_reader = StreamingWalReader::new(
            resident_tli,
            None,
            start_lsn,
            end_pos,
            end_watch,
            MAX_SEND_SIZE,
        );

        let shard_0 = ShardIdentity::new(
            ShardNumber(0),
            ShardCount(SHARD_COUNT),
            ShardStripeSize::default(),
        )
        .unwrap();

        let shard_1 = ShardIdentity::new(
            ShardNumber(1),
            ShardCount(SHARD_COUNT),
            ShardStripeSize::default(),
        )
        .unwrap();

        let mut shards = HashMap::new();

        for shard_number in 0..SHARD_COUNT {
            let shard_id = ShardIdentity::new(
                ShardNumber(shard_number),
                ShardCount(SHARD_COUNT),
                ShardStripeSize::default(),
            )
            .unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel::<Batch>(MSG_COUNT * 2);
            shards.insert(shard_id, (Some(tx), Some(rx)));
        }

        let shard_0_tx = shards.get_mut(&shard_0).unwrap().0.take().unwrap();
        let mut shard_0_rx = shards.get_mut(&shard_0).unwrap().1.take().unwrap();

        let handle = InterpretedWalReader::spawn(
            streaming_wal_reader,
            start_lsn,
            shard_0_tx,
            shard_0,
            PG_VERSION,
        );

        tracing::info!("Reading all WAL with only shard 0 attached ...");

        let mut shard_0_interpreted_records = Vec::new();
        while let Some(batch) = shard_0_rx.recv().await {
            shard_0_interpreted_records.push(batch.records);
            if batch.wal_end_lsn == batch.available_wal_end_lsn {
                break;
            }
        }

        let shard_1_tx = shards.get_mut(&shard_1).unwrap().0.take().unwrap();
        let mut shard_1_rx = shards.get_mut(&shard_1).unwrap().1.take().unwrap();

        tracing::info!("Attaching shard 1 to the reader at start of WAL");
        handle.fanout(shard_1, shard_1_tx, start_lsn).unwrap();

        tracing::info!("Reading all WAL with shard 0 and shard 1 attached ...");

        let mut shard_1_interpreted_records = Vec::new();
        while let Some(batch) = shard_1_rx.recv().await {
            shard_1_interpreted_records.push(batch.records);
            if batch.wal_end_lsn == batch.available_wal_end_lsn {
                break;
            }
        }

        // This test uses logical messages. Those only go to shard 0. Check that the
        // filtering worked and shard 1 did not get any.
        assert!(shard_1_interpreted_records
            .iter()
            .all(|recs| recs.records.is_empty()));

        // Shard 0 should not receive anything more since the reader is
        // going through wal that it has already processed.
        let res = shard_0_rx.try_recv();
        if let Ok(ref ok) = res {
            tracing::error!(
                "Shard 0 received batch: wal_end_lsn={} available_wal_end_lsn={}",
                ok.wal_end_lsn,
                ok.available_wal_end_lsn
            );
        }
        assert!(matches!(res, Err(TryRecvError::Empty)));

        // Check that the next records lsns received by the two shards match up.
        let shard_0_next_lsns = shard_0_interpreted_records
            .iter()
            .map(|recs| recs.next_record_lsn)
            .collect::<Vec<_>>();
        let shard_1_next_lsns = shard_1_interpreted_records
            .iter()
            .map(|recs| recs.next_record_lsn)
            .collect::<Vec<_>>();
        assert_eq!(shard_0_next_lsns, shard_1_next_lsns);

        handle.abort();
        let mut done = false;
        for _ in 0..5 {
            if handle.current_position().is_none() {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(done);
    }
}
