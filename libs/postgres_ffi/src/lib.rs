#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
// bindgen creates some unsafe code with no doc comments.
#![allow(clippy::missing_safety_doc)]
// noted at 1.63 that in many cases there's u32 -> u32 transmutes in bindgen code.
#![allow(clippy::useless_transmute)]
// modules included with the postgres_ffi macro depend on the types of the specific version's
// types, and trigger a too eager lint.
#![allow(clippy::duplicate_mod)]
#![deny(clippy::undocumented_unsafe_blocks)]

use bytes::Bytes;
use utils::bin_ser::SerializeError;
use utils::lsn::Lsn;

macro_rules! postgres_ffi {
    ($version:ident) => {
        #[path = "."]
        pub mod $version {
            pub mod bindings {
                // bindgen generates bindings for a lot of stuff we don't need
                #![allow(dead_code)]
                #![allow(clippy::undocumented_unsafe_blocks)]

                use serde::{Deserialize, Serialize};
                include!(concat!(
                    env!("OUT_DIR"),
                    "/bindings_",
                    stringify!($version),
                    ".rs"
                ));

                include!(concat!("pg_constants_", stringify!($version), ".rs"));
            }
            pub mod controlfile_utils;
            pub mod nonrelfile_utils;
            pub mod wal_craft_test_export;
            pub mod wal_generator;
            pub mod waldecoder_handler;
            pub mod xlog_utils;

            pub const PG_MAJORVERSION: &str = stringify!($version);

            // Re-export some symbols from bindings
            pub use bindings::DBState_DB_SHUTDOWNED;
            pub use bindings::{CheckPoint, ControlFileData, XLogRecord};

            pub const ZERO_CHECKPOINT: bytes::Bytes =
                bytes::Bytes::from_static(&[0u8; xlog_utils::SIZEOF_CHECKPOINT]);
        }
    };
}

#[macro_export]
macro_rules! for_all_postgres_versions {
    ($macro:tt) => {
        $macro!(v14);
        $macro!(v15);
        $macro!(v16);
        $macro!(v17);
    };
}

for_all_postgres_versions! { postgres_ffi }

/// dispatch_pgversion
///
/// Run a code block in a context where the postgres_ffi bindings for a
/// specific (supported) PostgreSQL version are `use`-ed in scope under the pgv
/// identifier.
/// If the provided pg_version is not supported, we panic!(), unless the
/// optional third argument was provided (in which case that code will provide
/// the default handling instead).
///
/// Use like
///
/// dispatch_pgversion!(my_pgversion, { pgv::constants::XLOG_DBASE_CREATE })
/// dispatch_pgversion!(my_pgversion, pgv::constants::XLOG_DBASE_CREATE)
///
/// Other uses are for macro-internal purposes only and strictly unsupported.
///
#[macro_export]
macro_rules! dispatch_pgversion {
    ($version:expr, $code:expr) => {
        dispatch_pgversion!($version, $code, panic!("Unknown PostgreSQL version {}", $version))
    };
    ($version:expr, $code:expr, $invalid_pgver_handling:expr) => {
        dispatch_pgversion!(
            $version => $code,
            default = $invalid_pgver_handling,
            pgversions = [
                14 : v14,
                15 : v15,
                16 : v16,
                17 : v17,
            ]
        )
    };
    ($pgversion:expr => $code:expr,
     default = $default:expr,
     pgversions = [$($sv:literal : $vsv:ident),+ $(,)?]) => {
        match ($pgversion) {
            $($sv => {
                use $crate::$vsv as pgv;
                $code
            },)+
            _ => {
                $default
            }
        }
    };
}

#[macro_export]
macro_rules! enum_pgversion_dispatch {
    ($name:expr, $typ:ident, $bind:ident, $code:block) => {
        enum_pgversion_dispatch!(
            name = $name,
            bind = $bind,
            typ = $typ,
            code = $code,
            pgversions = [
                V14 : v14,
                V15 : v15,
                V16 : v16,
                V17 : v17,
            ]
        )
    };
    (name = $name:expr,
     bind = $bind:ident,
     typ = $typ:ident,
     code = $code:block,
     pgversions = [$($variant:ident : $md:ident),+ $(,)?]) => {
        match $name {
            $(
            self::$typ::$variant($bind) => {
                use $crate::$md as pgv;
                $code
            }
            ),+,
        }
    };
}

#[macro_export]
macro_rules! enum_pgversion {
    {$name:ident, pgv :: $t:ident} => {
        enum_pgversion!{
            name = $name,
            typ = $t,
            pgversions = [
                V14 : v14,
                V15 : v15,
                V16 : v16,
                V17 : v17,
            ]
        }
    };
    {$name:ident, pgv :: $p:ident :: $t:ident} => {
        enum_pgversion!{
            name = $name,
            path = $p,
            typ = $t,
            pgversions = [
                V14 : v14,
                V15 : v15,
                V16 : v16,
                V17 : v17,
            ]
        }
    };
    {name = $name:ident,
     typ = $t:ident,
     pgversions = [$($variant:ident : $md:ident),+ $(,)?]} => {
        pub enum $name {
            $($variant ( $crate::$md::$t )),+
        }
        impl self::$name {
            pub fn pg_version(&self) -> u32 {
                enum_pgversion_dispatch!(self, $name, _ign, {
                    pgv::bindings::PG_MAJORVERSION_NUM
                })
            }
        }
        $(
        impl Into<self::$name> for $crate::$md::$t {
            fn into(self) -> self::$name {
                self::$name::$variant (self)
            }
        }
        )+
    };
    {name = $name:ident,
     path = $p:ident,
     typ = $t:ident,
     pgversions = [$($variant:ident : $md:ident),+ $(,)?]} => {
        pub enum $name {
            $($variant ($crate::$md::$p::$t)),+
        }
        impl $name {
            pub fn pg_version(&self) -> u32 {
                enum_pgversion_dispatch!(self, $name, _ign, {
                    pgv::bindings::PG_MAJORVERSION_NUM
                })
            }
        }
        $(
        impl Into<$name> for $crate::$md::$p::$t {
            fn into(self) -> $name {
                $name::$variant (self)
            }
        }
        )+
    };
}

pub mod pg_constants;
pub mod relfile_utils;
pub mod walrecord;

// Export some widely used datatypes that are unlikely to change across Postgres versions
pub use v14::bindings::RepOriginId;
pub use v14::bindings::{uint32, uint64, Oid};
pub use v14::bindings::{BlockNumber, OffsetNumber};
pub use v14::bindings::{MultiXactId, TransactionId};
pub use v14::bindings::{TimeLineID, TimestampTz, XLogRecPtr, XLogSegNo};

// Likewise for these, although the assumption that these don't change is a little more iffy.
pub use v14::bindings::{MultiXactOffset, MultiXactStatus};
pub use v14::bindings::{PageHeaderData, XLogRecord};
pub use v14::xlog_utils::{
    SIZEOF_CHECKPOINT, XLOG_SIZE_OF_XLOG_LONG_PHD, XLOG_SIZE_OF_XLOG_RECORD,
    XLOG_SIZE_OF_XLOG_SHORT_PHD,
};

pub use v14::bindings::{CheckPoint, ControlFileData};

// from pg_config.h. These can be changed with configure options --with-blocksize=BLOCKSIZE and
// --with-segsize=SEGSIZE, but assume the defaults for now.
pub const BLCKSZ: u16 = 8192;
pub const RELSEG_SIZE: u32 = 1024 * 1024 * 1024 / (BLCKSZ as u32);
pub const XLOG_BLCKSZ: usize = 8192;
pub const WAL_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

pub const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;

// Export some version independent functions that are used outside of this mod
pub use v14::xlog_utils::calculate_walrecord_end_lsn;
pub use v14::xlog_utils::encode_logical_message;
pub use v14::xlog_utils::get_current_timestamp;
pub use v14::xlog_utils::to_pg_timestamp;
pub use v14::xlog_utils::try_from_pg_timestamp;
pub use v14::xlog_utils::XLogFileName;

pub use v14::bindings::DBState_DB_SHUTDOWNED;

pub fn bkpimage_is_compressed(bimg_info: u8, version: u32) -> bool {
    dispatch_pgversion!(version, pgv::bindings::bkpimg_is_compressed(bimg_info))
}

pub fn generate_wal_segment(
    segno: u64,
    system_id: u64,
    pg_version: u32,
    lsn: Lsn,
) -> Result<Bytes, SerializeError> {
    assert_eq!(segno, lsn.segment_number(WAL_SEGMENT_SIZE));

    dispatch_pgversion!(
        pg_version,
        pgv::xlog_utils::generate_wal_segment(segno, system_id, lsn),
        Err(SerializeError::BadInput)
    )
}

pub fn generate_pg_control(
    pg_control_bytes: &[u8],
    checkpoint_bytes: &[u8],
    lsn: Lsn,
    pg_version: u32,
) -> anyhow::Result<(Bytes, u64)> {
    dispatch_pgversion!(
        pg_version,
        pgv::xlog_utils::generate_pg_control(pg_control_bytes, checkpoint_bytes, lsn),
        anyhow::bail!("Unknown version {}", pg_version)
    )
}

// PG timeline is always 1, changing it doesn't have any useful meaning in Neon.
//
// NOTE: this is not to be confused with Neon timelines; different concept!
//
// It's a shaky assumption, that it's always 1. We might import a
// PostgreSQL data directory that has gone through timeline bumps,
// for example. FIXME later.
pub const PG_TLI: u32 = 1;

//  See TransactionIdIsNormal in transam.h
pub const fn transaction_id_is_normal(id: TransactionId) -> bool {
    id > pg_constants::FIRST_NORMAL_TRANSACTION_ID
}

// See TransactionIdPrecedes in transam.c
pub const fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^32 comparison.
     */

    if !(transaction_id_is_normal(id1)) || !transaction_id_is_normal(id2) {
        return id1 < id2;
    }

    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

// Check if page is not yet initialized (port of Postgres PageIsInit() macro)
pub fn page_is_new(pg: &[u8]) -> bool {
    pg[14] == 0 && pg[15] == 0 // pg_upper == 0
}

// ExtractLSN from page header
pub fn page_get_lsn(pg: &[u8]) -> Lsn {
    Lsn(
        ((u32::from_le_bytes(pg[0..4].try_into().unwrap()) as u64) << 32)
            | u32::from_le_bytes(pg[4..8].try_into().unwrap()) as u64,
    )
}

pub fn page_set_lsn(pg: &mut [u8], lsn: Lsn) {
    pg[0..4].copy_from_slice(&((lsn.0 >> 32) as u32).to_le_bytes());
    pg[4..8].copy_from_slice(&(lsn.0 as u32).to_le_bytes());
}

// This is port of function with the same name from freespace.c.
// The only difference is that it does not have "level" parameter because XLogRecordPageWithFreeSpace
// always call it with level=FSM_BOTTOM_LEVEL
pub fn fsm_logical_to_physical(addr: BlockNumber) -> BlockNumber {
    let mut leafno = addr;
    const FSM_TREE_DEPTH: u32 = if pg_constants::SLOTS_PER_FSM_PAGE >= 1626 {
        3
    } else {
        4
    };

    /* Count upper level nodes required to address the leaf page */
    let mut pages: BlockNumber = 0;
    for _l in 0..FSM_TREE_DEPTH {
        pages += leafno + 1;
        leafno /= pg_constants::SLOTS_PER_FSM_PAGE;
    }
    /* Turn the page count into 0-based block number */
    pages - 1
}

pub mod waldecoder {
    use bytes::{Buf, Bytes, BytesMut};
    use std::num::NonZeroU32;
    use thiserror::Error;
    use utils::lsn::Lsn;

    pub enum State {
        WaitingForRecord,
        ReassemblingRecord {
            recordbuf: BytesMut,
            contlen: NonZeroU32,
        },
        SkippingEverything {
            skip_until_lsn: Lsn,
        },
    }

    pub struct WalStreamDecoder {
        pub lsn: Lsn,
        pub pg_version: u32,
        pub inputbuf: BytesMut,
        pub state: State,
    }

    #[derive(Error, Debug, Clone)]
    #[error("{msg} at {lsn}")]
    pub struct WalDecodeError {
        pub msg: String,
        pub lsn: Lsn,
    }

    impl WalStreamDecoder {
        pub fn new(lsn: Lsn, pg_version: u32) -> WalStreamDecoder {
            WalStreamDecoder {
                lsn,
                pg_version,
                inputbuf: BytesMut::new(),
                state: State::WaitingForRecord,
            }
        }

        // The latest LSN position fed to the decoder.
        pub fn available(&self) -> Lsn {
            self.lsn + self.inputbuf.remaining() as u64
        }

        pub fn feed_bytes(&mut self, buf: &[u8]) {
            self.inputbuf.extend_from_slice(buf);
        }

        pub fn poll_decode(&mut self) -> Result<Option<(Lsn, Bytes)>, WalDecodeError> {
            dispatch_pgversion!(
                self.pg_version,
                {
                    use pgv::waldecoder_handler::WalStreamDecoderHandler;
                    self.poll_decode_internal()
                },
                Err(WalDecodeError {
                    msg: format!("Unknown version {}", self.pg_version),
                    lsn: self.lsn,
                })
            )
        }
    }
}
