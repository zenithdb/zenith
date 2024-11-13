//! This module defines the WAL record format used within the pageserver.

use bytes::Bytes;
use postgres_ffi::walrecord::{describe_postgres_wal_record, MultiXactMember};
use postgres_ffi::{MultiXactId, MultiXactOffset, TimestampTz, TransactionId};
use serde::{Deserialize, Serialize};
use utils::bin_ser::DeserializeError;

/// Each update to a page is represented by a NeonWalRecord. It can be a wrapper
/// around a PostgreSQL WAL record, or a custom neon-specific "record".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NeonWalRecord {
    /// Native PostgreSQL WAL record
    Postgres { will_init: bool, rec: Bytes },

    /// Clear bits in heap visibility map. ('flags' is bitmap of bits to clear)
    ClearVisibilityMapFlags {
        new_heap_blkno: Option<u32>,
        old_heap_blkno: Option<u32>,
        flags: u8,
    },
    /// Mark transaction IDs as committed on a CLOG page
    ClogSetCommitted {
        xids: Vec<TransactionId>,
        timestamp: TimestampTz,
    },
    /// Mark transaction IDs as aborted on a CLOG page
    ClogSetAborted { xids: Vec<TransactionId> },
    /// Extend multixact offsets SLRU
    MultixactOffsetCreate {
        mid: MultiXactId,
        moff: MultiXactOffset,
    },
    /// Extend multixact members SLRU.
    MultixactMembersCreate {
        moff: MultiXactOffset,
        members: Vec<MultiXactMember>,
    },
    /// Update the map of AUX files, either writing or dropping an entry
    AuxFile {
        file_path: String,
        content: Option<Bytes>,
    },

    /// A testing record for unit testing purposes. It supports append data to an existing image, or clear it.
    #[cfg(feature = "testing")]
    Test {
        /// Append a string to the image.
        append: String,
        /// Clear the image before appending.
        clear: bool,
        /// Treat this record as an init record. `clear` should be set to true if this field is set
        /// to true. This record does not need the history WALs to reconstruct. See [`NeonWalRecord::will_init`] and
        /// its references in `timeline.rs`.
        will_init: bool,
    },
}

impl NeonWalRecord {
    /// Does replaying this WAL record initialize the page from scratch, or does
    /// it need to be applied over the previous image of the page?
    pub fn will_init(&self) -> bool {
        // If you change this function, you'll also need to change ValueBytes::will_init
        match self {
            NeonWalRecord::Postgres { will_init, rec: _ } => *will_init,
            #[cfg(feature = "testing")]
            NeonWalRecord::Test { will_init, .. } => *will_init,
            // None of the special neon record types currently initialize the page
            _ => false,
        }
    }

    #[cfg(feature = "testing")]
    pub fn wal_append(s: impl AsRef<str>) -> Self {
        Self::Test {
            append: s.as_ref().to_string(),
            clear: false,
            will_init: false,
        }
    }

    #[cfg(feature = "testing")]
    pub fn wal_clear(s: impl AsRef<str>) -> Self {
        Self::Test {
            append: s.as_ref().to_string(),
            clear: true,
            will_init: false,
        }
    }

    #[cfg(feature = "testing")]
    pub fn wal_init(s: impl AsRef<str>) -> Self {
        Self::Test {
            append: s.as_ref().to_string(),
            clear: true,
            will_init: true,
        }
    }
}

/// Build a human-readable string to describe a WAL record
///
/// For debugging purposes
pub fn describe_wal_record(rec: &NeonWalRecord) -> Result<String, DeserializeError> {
    match rec {
        NeonWalRecord::Postgres { will_init, rec } => Ok(format!(
            "will_init: {}, {}",
            will_init,
            describe_postgres_wal_record(rec)?
        )),
        _ => Ok(format!("{:?}", rec)),
    }
}
