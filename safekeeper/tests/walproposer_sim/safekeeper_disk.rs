use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use safekeeper::state::TimelinePersistentState;
use utils::id::TenantTimelineId;

use super::block_storage::BlockStorage;

use std::{ops::Deref, time::Instant};

use anyhow::Result;
use bytes::{Buf, BytesMut};
use futures::future::BoxFuture;
use postgres_ffi::{waldecoder::WalStreamDecoder, XLogSegNo};
use safekeeper::{control_file, metrics::WalStorageMetrics, wal_storage};
use tracing::{debug, info};
use utils::lsn::Lsn;

/// All safekeeper state that is usually saved to disk.
pub struct SafekeeperDisk {
    pub timelines: Mutex<HashMap<TenantTimelineId, Arc<TimelineDisk>>>,
}

impl Default for SafekeeperDisk {
    fn default() -> Self {
        Self::new()
    }
}

impl SafekeeperDisk {
    pub fn new() -> Self {
        SafekeeperDisk {
            timelines: Mutex::new(HashMap::new()),
        }
    }

    pub fn put_state(
        &self,
        ttid: &TenantTimelineId,
        state: TimelinePersistentState,
    ) -> Arc<TimelineDisk> {
        self.timelines
            .lock()
            .entry(*ttid)
            .and_modify(|e| {
                let mut mu = e.state.lock();
                *mu = state.clone();
            })
            .or_insert_with(|| {
                Arc::new(TimelineDisk {
                    state: Mutex::new(state),
                    wal: Mutex::new(BlockStorage::new()),
                })
            })
            .clone()
    }
}

/// Control file state and WAL storage.
pub struct TimelineDisk {
    pub state: Mutex<TimelinePersistentState>,
    pub wal: Mutex<BlockStorage>,
}

/// Implementation of `control_file::Storage` trait.
pub struct DiskStateStorage {
    persisted_state: TimelinePersistentState,
    disk: Arc<TimelineDisk>,
    last_persist_at: Instant,
}

impl DiskStateStorage {
    pub fn new(disk: Arc<TimelineDisk>) -> Self {
        let guard = disk.state.lock();
        let state = guard.clone();
        drop(guard);
        DiskStateStorage {
            persisted_state: state,
            disk,
            last_persist_at: Instant::now(),
        }
    }
}

impl control_file::Storage for DiskStateStorage {
    /// Persist safekeeper state on disk and update internal state.
    async fn persist(&mut self, s: &TimelinePersistentState) -> Result<()> {
        self.persisted_state = s.clone();
        *self.disk.state.lock() = s.clone();
        Ok(())
    }

    /// Timestamp of last persist.
    fn last_persist_at(&self) -> Instant {
        // TODO: don't rely on it in tests
        self.last_persist_at
    }
}

impl Deref for DiskStateStorage {
    type Target = TimelinePersistentState;

    fn deref(&self) -> &Self::Target {
        &self.persisted_state
    }
}

/// Implementation of `wal_storage::Storage` trait.
pub struct DiskWALStorage {
    /// Written to disk, but possibly still in the cache and not fully persisted.
    /// Also can be ahead of record_lsn, if happen to be in the middle of a WAL record.
    write_lsn: Lsn,

    /// The LSN of the last WAL record written to disk. Still can be not fully flushed.
    write_record_lsn: Lsn,

    /// The LSN of the last WAL record flushed to disk.
    flush_record_lsn: Lsn,

    /// Decoder is required for detecting boundaries of WAL records.
    decoder: WalStreamDecoder,

    /// Bytes of WAL records that are not yet written to disk.
    unflushed_bytes: BytesMut,

    /// Contains BlockStorage for WAL.
    disk: Arc<TimelineDisk>,
}

impl DiskWALStorage {
    pub fn new(disk: Arc<TimelineDisk>, state: &TimelinePersistentState) -> Result<Self> {
        let write_lsn = if state.commit_lsn == Lsn(0) {
            Lsn(0)
        } else {
            Self::find_end_of_wal(disk.clone(), state.commit_lsn)?
        };

        let flush_lsn = write_lsn;
        Ok(DiskWALStorage {
            write_lsn,
            write_record_lsn: flush_lsn,
            flush_record_lsn: flush_lsn,
            decoder: WalStreamDecoder::new(flush_lsn, 16),
            unflushed_bytes: BytesMut::new(),
            disk,
        })
    }

    fn find_end_of_wal(disk: Arc<TimelineDisk>, start_lsn: Lsn) -> Result<Lsn> {
        let mut buf = [0; 8192];
        let mut pos = start_lsn.0;
        let mut decoder = WalStreamDecoder::new(start_lsn, 16);
        let mut result = start_lsn;
        loop {
            disk.wal.lock().read(pos, &mut buf);
            pos += buf.len() as u64;
            decoder.feed_bytes(&buf);

            loop {
                match decoder.poll_decode() {
                    Ok(Some(record)) => result = record.0,
                    Err(e) => {
                        debug!(
                            "find_end_of_wal reached end at {:?}, decode error: {:?}",
                            result, e
                        );
                        return Ok(result);
                    }
                    Ok(None) => break, // need more data
                }
            }
        }
    }
}

impl wal_storage::Storage for DiskWALStorage {
    // Last written LSN.
    fn write_lsn(&self) -> Lsn {
        self.write_lsn
    }
    /// LSN of last durably stored WAL record.
    fn flush_record_lsn(&self) -> Lsn {
        self.flush_record_lsn
    }

    async fn initialize_first_segment(&mut self, _init_lsn: Lsn) -> Result<()> {
        Ok(())
    }

    /// Write piece of WAL from buf to disk, but not necessarily sync it.
    async fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        if self.write_lsn != startpos {
            panic!("write_wal called with wrong startpos");
        }

        self.unflushed_bytes.extend_from_slice(buf);
        self.write_lsn += buf.len() as u64;

        if self.decoder.available() != startpos {
            info!(
                "restart decoder from {} to {}",
                self.decoder.available(),
                startpos,
            );
            self.decoder = WalStreamDecoder::new(startpos, 16);
        }
        self.decoder.feed_bytes(buf);
        loop {
            match self.decoder.poll_decode()? {
                None => break, // no full record yet
                Some((lsn, _rec)) => {
                    self.write_record_lsn = lsn;
                }
            }
        }

        Ok(())
    }

    /// Truncate WAL at specified LSN, which must be the end of WAL record.
    async fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        if self.write_lsn != Lsn(0) && end_pos > self.write_lsn {
            panic!(
                "truncate_wal called on non-written WAL, write_lsn={}, end_pos={}",
                self.write_lsn, end_pos
            );
        }

        self.flush_wal().await?;

        // write zeroes to disk from end_pos until self.write_lsn
        let buf = [0; 8192];
        let mut pos = end_pos.0;
        while pos < self.write_lsn.0 {
            self.disk.wal.lock().write(pos, &buf);
            pos += buf.len() as u64;
        }

        self.write_lsn = end_pos;
        self.write_record_lsn = end_pos;
        self.flush_record_lsn = end_pos;
        self.unflushed_bytes.clear();
        self.decoder = WalStreamDecoder::new(end_pos, 16);

        Ok(())
    }

    /// Durably store WAL on disk, up to the last written WAL record.
    async fn flush_wal(&mut self) -> Result<()> {
        if self.flush_record_lsn == self.write_record_lsn {
            // no need to do extra flush
            return Ok(());
        }

        let num_bytes = self.write_record_lsn.0 - self.flush_record_lsn.0;

        self.disk.wal.lock().write(
            self.flush_record_lsn.0,
            &self.unflushed_bytes[..num_bytes as usize],
        );
        self.unflushed_bytes.advance(num_bytes as usize);
        self.flush_record_lsn = self.write_record_lsn;

        Ok(())
    }

    /// Remove all segments <= given segno. Returns function doing that as we
    /// want to perform it without timeline lock.
    fn remove_up_to(&self, _segno_up_to: XLogSegNo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    /// Release resources associated with the storage -- technically, close FDs.
    /// Currently we don't remove timelines until restart (#3146), so need to
    /// spare descriptors. This would be useful for temporary tli detach as
    /// well.
    fn close(&mut self) {}

    /// Get metrics for this timeline.
    fn get_metrics(&self) -> WalStorageMetrics {
        WalStorageMetrics::default()
    }
}
