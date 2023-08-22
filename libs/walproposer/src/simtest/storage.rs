use std::{ops::Deref, sync::Arc};

use anyhow::Result;
use bytes::{Buf, BytesMut};
use log::{debug, info};
use postgres_ffi::{waldecoder::WalStreamDecoder, XLogSegNo};
use safekeeper::{control_file, safekeeper::SafeKeeperState, wal_storage};
use utils::lsn::Lsn;

use super::disk::TimelineDisk;

pub struct DiskStateStorage {
    persisted_state: SafeKeeperState,
    disk: Arc<TimelineDisk>,
}

impl DiskStateStorage {
    pub fn new(disk: Arc<TimelineDisk>) -> Self {
        let guard = disk.state.lock();
        let state = guard.clone();
        drop(guard);
        DiskStateStorage {
            persisted_state: state,
            disk,
        }
    }
}

impl control_file::Storage for DiskStateStorage {
    fn persist(&mut self, s: &SafeKeeperState) -> Result<()> {
        self.persisted_state = s.clone();
        *self.disk.state.lock() = s.clone();
        Ok(())
    }
}

impl Deref for DiskStateStorage {
    type Target = SafeKeeperState;

    fn deref(&self) -> &Self::Target {
        &self.persisted_state
    }
}

pub struct DummyWalStore {
    lsn: Lsn,
}

impl DummyWalStore {
    pub fn new() -> Self {
        DummyWalStore { lsn: Lsn::INVALID }
    }
}

impl wal_storage::Storage for DummyWalStore {
    fn flush_lsn(&self) -> Lsn {
        self.lsn
    }

    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
        self.lsn = startpos + buf.len() as u64;
        Ok(())
    }

    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        self.lsn = end_pos;
        Ok(())
    }

    fn flush_wal(&mut self) -> Result<()> {
        Ok(())
    }

    fn remove_up_to(&self) -> Box<dyn Fn(XLogSegNo) -> Result<()>> {
        Box::new(move |_segno_up_to: XLogSegNo| Ok(()))
    }

    fn get_metrics(&self) -> safekeeper::metrics::WalStorageMetrics {
        safekeeper::metrics::WalStorageMetrics::default()
    }
}

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

    unflushed_bytes: BytesMut,

    disk: Arc<TimelineDisk>,
}

impl DiskWALStorage {
    pub fn new(disk: Arc<TimelineDisk>, state: &SafeKeeperState) -> Result<Self> {
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
            decoder: WalStreamDecoder::new(flush_lsn, 15),
            unflushed_bytes: BytesMut::new(),
            disk,
        })
    }

    fn find_end_of_wal(disk: Arc<TimelineDisk>, start_lsn: Lsn) -> Result<Lsn> {
        let mut buf = [0; 8192];
        let mut pos = start_lsn.0;
        let mut decoder = WalStreamDecoder::new(start_lsn, 15);
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
    fn flush_lsn(&self) -> Lsn {
        self.flush_record_lsn
    }

    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> Result<()> {
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
            self.decoder = WalStreamDecoder::new(startpos, 15);
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

    fn truncate_wal(&mut self, end_pos: Lsn) -> Result<()> {
        if self.write_lsn != Lsn(0) && end_pos > self.write_lsn {
            panic!(
                "truncate_wal called on non-written WAL, write_lsn={}, end_pos={}",
                self.write_lsn, end_pos
            );
        }

        self.flush_wal()?;

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
        self.decoder = WalStreamDecoder::new(end_pos, 15);

        Ok(())
    }

    fn flush_wal(&mut self) -> Result<()> {
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

    fn remove_up_to(&self) -> Box<dyn Fn(XLogSegNo) -> Result<()>> {
        Box::new(move |_segno_up_to: XLogSegNo| Ok(()))
    }

    fn get_metrics(&self) -> safekeeper::metrics::WalStorageMetrics {
        safekeeper::metrics::WalStorageMetrics::default()
    }
}
