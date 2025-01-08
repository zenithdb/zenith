use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{stream::BoxStream, Stream, StreamExt};
use utils::lsn::Lsn;

use crate::{send_wal::EndWatch, timeline::WalResidentTimeline, wal_storage::WalReader};
use safekeeper_api::Term;

#[derive(PartialEq, Eq, Debug)]
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

struct PositionedWalReader {
    start: Lsn,
    end: Lsn,
    reader: Option<WalReader>,
}

/// A streaming WAL reader wrapper which can be reset while running
pub(crate) struct StreamingWalReader {
    stream: BoxStream<'static, WalOrReset>,
    start_changed_tx: tokio::sync::watch::Sender<Lsn>,
}

pub(crate) enum WalOrReset {
    Wal(anyhow::Result<WalBytes>),
    Reset(Lsn),
}

impl WalOrReset {
    pub(crate) fn get_wal(self) -> Option<anyhow::Result<WalBytes>> {
        match self {
            WalOrReset::Wal(wal) => Some(wal),
            WalOrReset::Reset(_) => None,
        }
    }
}

impl StreamingWalReader {
    pub(crate) fn new(
        tli: WalResidentTimeline,
        term: Option<Term>,
        start: Lsn,
        end: Lsn,
        end_watch: EndWatch,
        buffer_size: usize,
    ) -> Self {
        let (start_changed_tx, start_changed_rx) = tokio::sync::watch::channel(start);

        let state = WalReaderStreamState {
            tli,
            wal_reader: PositionedWalReader {
                start,
                end,
                reader: None,
            },
            term,
            end_watch,
            buffer: vec![0; buffer_size],
            buffer_size,
            start_changed_rx: Some(start_changed_rx),
        };

        // When a change notification is received while polling the internal
        // reader, stop polling the read future and service the change.
        let stream = futures::stream::unfold(state, |mut state| async move {
            let mut change_rx = state.start_changed_rx.take().unwrap();

            let wal_or_reset = tokio::select! {
                read_res = state.read() => { WalOrReset::Wal(read_res) },
                changed_res = change_rx.changed() => {
                    assert!(changed_res.is_ok());
                    let new_start_pos = change_rx.borrow_and_update();
                    WalOrReset::Reset(*new_start_pos)
                }
            };

            state.start_changed_rx = Some(change_rx);

            if let WalOrReset::Reset(lsn) = wal_or_reset {
                state.wal_reader.start = lsn;
                state.wal_reader.reader = None;
            }

            Some((wal_or_reset, state))
        })
        .boxed();

        Self {
            stream,
            start_changed_tx,
        }
    }

    /// Reset the stream to a given position.
    pub(crate) async fn reset(&mut self, start: Lsn) {
        self.start_changed_tx.send(start).unwrap();
        while let Some(wal_or_reset) = self.stream.next().await {
            match wal_or_reset {
                WalOrReset::Reset(at) => {
                    // Stream confirmed the reset.
                    // There may only one ongoing reset at any given time,
                    // hence the assertion.
                    assert_eq!(at, start);
                    break;
                }
                WalOrReset::Wal(_) => {
                    // Ignore wal generated before reset was handled
                }
            }
        }
    }
}

impl Stream for StreamingWalReader {
    type Item = WalOrReset;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

struct WalReaderStreamState {
    tli: WalResidentTimeline,
    wal_reader: PositionedWalReader,
    term: Option<Term>,
    end_watch: EndWatch,
    buffer: Vec<u8>,
    buffer_size: usize,
    start_changed_rx: Option<tokio::sync::watch::Receiver<Lsn>>,
}

impl WalReaderStreamState {
    async fn read(&mut self) -> anyhow::Result<WalBytes> {
        // Create reader if needed
        if self.wal_reader.reader.is_none() {
            self.wal_reader.reader = Some(self.tli.get_walreader(self.wal_reader.start).await?);
        }

        let have_something_to_send = self.wal_reader.end > self.wal_reader.start;
        if !have_something_to_send {
            tracing::debug!(
                "Waiting for wal: start={}, end={}",
                self.wal_reader.end,
                self.wal_reader.start
            );
            self.wal_reader.end = self
                .end_watch
                .wait_for_lsn(self.wal_reader.start, self.term)
                .await?;
            tracing::debug!(
                "Done waiting for wal: start={}, end={}",
                self.wal_reader.end,
                self.wal_reader.start
            );
        }

        assert!(
            self.wal_reader.end > self.wal_reader.start,
            "nothing to send after waiting for WAL"
        );

        // Calculate chunk size
        let mut chunk_end_pos = self.wal_reader.start + self.buffer_size as u64;
        if chunk_end_pos >= self.wal_reader.end {
            chunk_end_pos = self.wal_reader.end;
        } else {
            chunk_end_pos = chunk_end_pos
                .checked_sub(chunk_end_pos.block_offset())
                .unwrap();
        }

        let send_size = (chunk_end_pos.0 - self.wal_reader.start.0) as usize;
        let buffer = &mut self.buffer[..send_size];

        // Read WAL
        let send_size = {
            let _term_guard = if let Some(t) = self.term {
                Some(self.tli.acquire_term(t).await?)
            } else {
                None
            };
            self.wal_reader
                .reader
                .as_mut()
                .unwrap()
                .read(buffer)
                .await?
        };

        let wal = Bytes::copy_from_slice(&buffer[..send_size]);
        let result = WalBytes {
            wal,
            wal_start_lsn: self.wal_reader.start,
            wal_end_lsn: self.wal_reader.start + send_size as u64,
            available_wal_end_lsn: self.wal_reader.end,
        };

        self.wal_reader.start += send_size as u64;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::StreamExt;
    use postgres_ffi::MAX_SEND_SIZE;
    use utils::{
        id::{NodeId, TenantTimelineId},
        lsn::Lsn,
    };

    use crate::{test_utils::Env, wal_reader_stream::StreamingWalReader};

    #[tokio::test]
    async fn test_streaming_wal_reader_reset() {
        let _ = env_logger::builder().is_test(true).try_init();

        const SIZE: usize = 8 * 1024;
        const MSG_COUNT: usize = 200;

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

        let mut streaming_wal_reader = StreamingWalReader::new(
            resident_tli,
            None,
            start_lsn,
            end_pos,
            end_watch,
            MAX_SEND_SIZE,
        );

        let mut before_reset = Vec::new();
        while let Some(wor) = streaming_wal_reader.next().await {
            let wal = wor.get_wal().unwrap().unwrap();
            let stop = wal.available_wal_end_lsn == wal.wal_end_lsn;
            before_reset.push(wal);

            if stop {
                break;
            }
        }

        tracing::info!("Resetting the WAL stream ...");

        streaming_wal_reader.reset(start_lsn).await;

        tracing::info!("Doing second round of reads ...");

        let mut after_reset = Vec::new();
        while let Some(wor) = streaming_wal_reader.next().await {
            let wal = wor.get_wal().unwrap().unwrap();
            let stop = wal.available_wal_end_lsn == wal.wal_end_lsn;
            after_reset.push(wal);

            if stop {
                break;
            }
        }

        assert_eq!(before_reset, after_reset);
    }
}
