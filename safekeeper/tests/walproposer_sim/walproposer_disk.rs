use std::{ffi::CStr, sync::Arc};

use parking_lot::{Mutex, MutexGuard};
use postgres_ffi::v16::wal_generator::{LogicalMessageGenerator, WalGenerator};
use utils::lsn::Lsn;

use super::block_storage::BlockStorage;

/// Simulation implementation of walproposer WAL storage.
pub struct DiskWalProposer {
    state: Mutex<State>,
}

impl DiskWalProposer {
    pub fn new() -> Arc<DiskWalProposer> {
        Arc::new(DiskWalProposer {
            state: Mutex::new(State {
                internal_available_lsn: Lsn(0),
                prev_lsn: Lsn(0),
                disk: BlockStorage::new(),
                wal_generator: WalGenerator::new(LogicalMessageGenerator::new(c"", &[]), Lsn(0)),
            }),
        })
    }

    pub fn lock(&self) -> MutexGuard<State> {
        self.state.lock()
    }
}

pub struct State {
    // flush_lsn
    internal_available_lsn: Lsn,
    // needed for WAL generation
    prev_lsn: Lsn,
    // actual WAL storage
    disk: BlockStorage,
    // WAL record generator
    wal_generator: WalGenerator<LogicalMessageGenerator>,
}

impl State {
    pub fn read(&self, pos: u64, buf: &mut [u8]) {
        self.disk.read(pos, buf);
        // TODO: fail on reading uninitialized data
    }

    pub fn write(&mut self, pos: u64, buf: &[u8]) {
        self.disk.write(pos, buf);
    }

    /// Update the internal available LSN to the given value.
    pub fn reset_to(&mut self, lsn: Lsn) {
        self.internal_available_lsn = lsn;
        self.prev_lsn = Lsn(0); // Safekeeper doesn't care if this is omitted
        self.wal_generator.lsn = self.internal_available_lsn;
        self.wal_generator.prev_lsn = self.prev_lsn;
    }

    /// Get current LSN.
    pub fn flush_rec_ptr(&self) -> Lsn {
        self.internal_available_lsn
    }

    /// Inserts a logical record in the WAL at the current LSN.
    pub fn insert_logical_message(&mut self, prefix: &CStr, msg: &[u8]) {
        let (_, record) = self.wal_generator.append_logical_message(prefix, msg);
        self.disk.write(self.internal_available_lsn.into(), &record);
        self.prev_lsn = self.internal_available_lsn;
        self.internal_available_lsn += record.len() as u64;
    }
}
