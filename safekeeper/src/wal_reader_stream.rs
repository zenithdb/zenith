use std::sync::Arc;

use async_stream::try_stream;
use bytes::Bytes;
use futures::Stream;
use postgres_backend::CopyStreamHandlerEnd;
use safekeeper_api::Term;
use std::time::Duration;
use tokio::time::timeout;
use utils::lsn::Lsn;

use crate::{
    send_wal::{EndWatch, WalSenderGuard},
    timeline::WalResidentTimeline,
};

pub(crate) struct WalReaderStreamBuilder {
    pub(crate) tli: WalResidentTimeline,
    pub(crate) start_pos: Lsn,
    pub(crate) end_pos: Lsn,
    pub(crate) term: Option<Term>,
    pub(crate) end_watch: EndWatch,
    pub(crate) wal_sender_guard: Arc<WalSenderGuard>,
}

impl WalReaderStreamBuilder {
    pub(crate) fn start_pos(&self) -> Lsn {
        self.start_pos
    }
}

pub(crate) struct WalBytes {
    /// Raw PG WAL
    pub(crate) wal: Bytes,
    /// Start LSN of [`Self::wal`]
    #[allow(dead_code)]
    pub(crate) wal_start_lsn: Lsn,
    /// End LSN of [`Self::wal`]
    pub(crate) wal_end_lsn: Lsn,
    /// End LSN of WAL available on the safekeeper.
    ///
    /// For pagservers this will be commit LSN,
    /// while for the compute it will be the flush LSN.
    pub(crate) available_wal_end_lsn: Lsn,
}

impl WalReaderStreamBuilder {
    /// Builds a stream of Postgres WAL starting from [`Self::start_pos`].
    /// The stream terminates when the receiver (pageserver) is fully caught up
    /// and there's no active computes.
    pub(crate) async fn build(
        self,
        buffer_size: usize,
    ) -> anyhow::Result<impl Stream<Item = Result<WalBytes, CopyStreamHandlerEnd>>> {
        // TODO(vlad): The code below duplicates functionality from [`crate::send_wal`].
        // We can make the raw WAL sender use this stream too and remove the duplication.
        let Self {
            tli,
            mut start_pos,
            mut end_pos,
            term,
            mut end_watch,
            wal_sender_guard,
        } = self;
        let mut wal_reader = tli.get_walreader(start_pos).await?;
        let mut buffer = vec![0; buffer_size];

        const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);

        Ok(try_stream! {
            loop {
                let have_something_to_send = end_pos > start_pos;

                if !have_something_to_send {
                    // wait for lsn
                    let res = timeout(POLL_STATE_TIMEOUT, end_watch.wait_for_lsn(start_pos, term)).await;
                    match res {
                        Ok(ok) => {
                            end_pos = ok?;
                        },
                        Err(_) => {
                            if let EndWatch::Commit(_) = end_watch {
                                if let Some(remote_consistent_lsn) = wal_sender_guard
                                    .walsenders()
                                    .get_ws_remote_consistent_lsn(wal_sender_guard.id())
                                {
                                    if tli.should_walsender_stop(remote_consistent_lsn).await {
                                        // Stop streaming if the receivers are caught up and
                                        // there's no active compute. This causes the loop in
                                        // [`crate::send_interpreted_wal::InterpretedWalSender::run`]
                                        // to exit and terminate the WAL stream.
                                        return;
                                    }
                                }
                            }

                            continue;
                        }
                    }
                }


                assert!(
                    end_pos > start_pos,
                    "nothing to send after waiting for WAL"
                );

                // try to send as much as available, capped by the buffer size
                let mut chunk_end_pos = start_pos + buffer_size as u64;
                // if we went behind available WAL, back off
                if chunk_end_pos >= end_pos {
                    chunk_end_pos = end_pos;
                } else {
                    // If sending not up to end pos, round down to page boundary to
                    // avoid breaking WAL record not at page boundary, as protocol
                    // demands. See walsender.c (XLogSendPhysical).
                    chunk_end_pos = chunk_end_pos
                        .checked_sub(chunk_end_pos.block_offset())
                        .unwrap();
                }
                let send_size = (chunk_end_pos.0 - start_pos.0) as usize;
                let buffer = &mut buffer[..send_size];
                let send_size: usize;
                {
                    // If uncommitted part is being pulled, check that the term is
                    // still the expected one.
                    let _term_guard = if let Some(t) = term {
                        Some(tli.acquire_term(t).await?)
                    } else {
                        None
                    };
                    // Read WAL into buffer. send_size can be additionally capped to
                    // segment boundary here.
                    send_size = wal_reader.read(buffer).await?
                };
                let wal = Bytes::copy_from_slice(&buffer[..send_size]);

                yield WalBytes {
                    wal,
                    wal_start_lsn: start_pos,
                    wal_end_lsn: start_pos + send_size as u64,
                    available_wal_end_lsn: end_pos
                };

                start_pos += send_size as u64;
            }
        })
    }
}
