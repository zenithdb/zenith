//!
//! Misc constants, copied from PostgreSQL headers.
//!
//! Only place version-independent constants here.
//!
//! TODO: These probably should be auto-generated using bindgen,
//! rather than copied by hand. Although on the other hand, it's nice
//! to have them all here in one place, and have the ability to add
//! comments on them.
//!

use crate::PageHeaderData;
use crate::BLCKSZ;

//
// From pg_tablespace_d.h
//
pub const DEFAULTTABLESPACE_OID: u32 = 1663;
pub const GLOBALTABLESPACE_OID: u32 = 1664;

// From storage_xlog.h
pub const XLOG_SMGR_CREATE: u8 = 0x10;
pub const XLOG_SMGR_TRUNCATE: u8 = 0x20;

pub const SMGR_TRUNCATE_HEAP: u32 = 0x0001;
pub const SMGR_TRUNCATE_VM: u32 = 0x0002;
pub const SMGR_TRUNCATE_FSM: u32 = 0x0004;

//
// From bufpage.h
//

// Assumes 8 byte alignment
const SIZEOF_PAGE_HEADER_DATA: usize = size_of::<PageHeaderData>();
pub const MAXALIGN_SIZE_OF_PAGE_HEADER_DATA: usize = (SIZEOF_PAGE_HEADER_DATA + 7) & !7;

//
// constants from clog.h
//
pub const CLOG_XACTS_PER_BYTE: u32 = 4;
pub const CLOG_XACTS_PER_PAGE: u32 = BLCKSZ as u32 * CLOG_XACTS_PER_BYTE;
pub const CLOG_BITS_PER_XACT: u8 = 2;
pub const CLOG_XACT_BITMASK: u8 = (1 << CLOG_BITS_PER_XACT) - 1;

pub const TRANSACTION_STATUS_COMMITTED: u8 = 0x01;
pub const TRANSACTION_STATUS_ABORTED: u8 = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: u8 = 0x03;

pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;

//
// Constants from visibilitymap.h, visibilitymapdefs.h and visibilitymap.c
//
pub const SIZE_OF_PAGE_HEADER: u16 = 24;
pub const BITS_PER_BYTE: u16 = 8;
pub const HEAPBLOCKS_PER_PAGE: u32 =
    (BLCKSZ - SIZE_OF_PAGE_HEADER) as u32 * 8 / BITS_PER_HEAPBLOCK as u32;
pub const HEAPBLOCKS_PER_BYTE: u16 = BITS_PER_BYTE / BITS_PER_HEAPBLOCK;

pub const fn HEAPBLK_TO_MAPBLOCK(x: u32) -> u32 {
    x / HEAPBLOCKS_PER_PAGE
}
pub const fn HEAPBLK_TO_MAPBYTE(x: u32) -> u32 {
    (x % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE as u32
}
pub const fn HEAPBLK_TO_OFFSET(x: u32) -> u32 {
    (x % HEAPBLOCKS_PER_BYTE as u32) * BITS_PER_HEAPBLOCK as u32
}

pub const BITS_PER_HEAPBLOCK: u16 = 2;
pub const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
pub const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;
pub const VISIBILITYMAP_VALID_BITS: u8 = 0x03;

// From xact.h
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;

// From standbydefs.h
pub const XLOG_RUNNING_XACTS: u8 = 0x10;

// From srlu.h
pub const SLRU_PAGES_PER_SEGMENT: u32 = 32;
pub const SLRU_SEG_SIZE: usize = BLCKSZ as usize * SLRU_PAGES_PER_SEGMENT as usize;

/* mask for filtering opcodes out of xl_info */
pub const XLOG_XACT_OPMASK: u8 = 0x70;
pub const XLOG_HEAP_OPMASK: u8 = 0x70;
/* does this record have a 'xinfo' field or not */
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

/*
 * The following flags, stored in xinfo, determine which information is
 * contained in commit/abort records.
 */
pub const XACT_XINFO_HAS_DBINFO: u32 = 1u32 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1u32 << 1;
pub const XACT_XINFO_HAS_RELFILENODES: u32 = 1u32 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1u32 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1u32 << 4;
pub const XACT_XINFO_HAS_ORIGIN: u32 = 1u32 << 5;
// pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1u32 << 6;
// pub const XACT_XINFO_HAS_GID: u32 = 1u32 << 7;

// From pg_control.h and rmgrlist.h
pub const XLOG_NEXTOID: u8 = 0x30;
pub const XLOG_SWITCH: u8 = 0x40;
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
pub const XLOG_FPI: u8 = 0xB0;

// From multixact.h
pub const FIRST_MULTIXACT_ID: u32 = 1;
pub const MAX_MULTIXACT_ID: u32 = 0xFFFFFFFF;
pub const MAX_MULTIXACT_OFFSET: u32 = 0xFFFFFFFF;

pub const XLOG_MULTIXACT_ZERO_OFF_PAGE: u8 = 0x00;
pub const XLOG_MULTIXACT_ZERO_MEM_PAGE: u8 = 0x10;
pub const XLOG_MULTIXACT_CREATE_ID: u8 = 0x20;
pub const XLOG_MULTIXACT_TRUNCATE_ID: u8 = 0x30;

pub const MULTIXACT_OFFSETS_PER_PAGE: u16 = BLCKSZ / 4;
pub const MXACT_MEMBER_BITS_PER_XACT: u16 = 8;
pub const MXACT_MEMBER_FLAGS_PER_BYTE: u16 = 1;
pub const MULTIXACT_FLAGBYTES_PER_GROUP: u16 = 4;
pub const MULTIXACT_MEMBERS_PER_MEMBERGROUP: u16 =
    MULTIXACT_FLAGBYTES_PER_GROUP * MXACT_MEMBER_FLAGS_PER_BYTE;
/* size in bytes of a complete group */
pub const MULTIXACT_MEMBERGROUP_SIZE: u16 =
    4 * MULTIXACT_MEMBERS_PER_MEMBERGROUP + MULTIXACT_FLAGBYTES_PER_GROUP;
pub const MULTIXACT_MEMBERGROUPS_PER_PAGE: u16 = BLCKSZ / MULTIXACT_MEMBERGROUP_SIZE;
pub const MULTIXACT_MEMBERS_PER_PAGE: u16 =
    MULTIXACT_MEMBERGROUPS_PER_PAGE * MULTIXACT_MEMBERS_PER_MEMBERGROUP;

// From heapam_xlog.h
pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;
pub const XLOG_HEAP2_VISIBLE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
pub const XLH_LOCK_ALL_FROZEN_CLEARED: u8 = 0x01;
pub const XLH_INSERT_ALL_FROZEN_SET: u8 = (1 << 5) as u8;
pub const XLH_INSERT_ALL_VISIBLE_CLEARED: u8 = (1 << 0) as u8;
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: u8 = (1 << 0) as u8;
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: u8 = (1 << 1) as u8;
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = (1 << 0) as u8;

// From heapam_xlog.h
pub const XLOG_HEAP2_REWRITE: u8 = 0x00;

// From replication/message.h
pub const XLOG_LOGICAL_MESSAGE: u8 = 0x00;

// From rmgrlist.h
pub const RM_XLOG_ID: u8 = 0;
pub const RM_XACT_ID: u8 = 1;
pub const RM_SMGR_ID: u8 = 2;
pub const RM_CLOG_ID: u8 = 3;
pub const RM_DBASE_ID: u8 = 4;
pub const RM_TBLSPC_ID: u8 = 5;
pub const RM_MULTIXACT_ID: u8 = 6;
pub const RM_RELMAP_ID: u8 = 7;
pub const RM_STANDBY_ID: u8 = 8;
pub const RM_HEAP2_ID: u8 = 9;
pub const RM_HEAP_ID: u8 = 10;
pub const RM_REPLORIGIN_ID: u8 = 19;
pub const RM_LOGICALMSG_ID: u8 = 21;

// from neon_rmgr.h
pub const RM_NEON_ID: u8 = 134;

pub const XLOG_NEON_HEAP_INIT_PAGE: u8 = 0x80;

pub const XLOG_NEON_HEAP_INSERT: u8 = 0x00;
pub const XLOG_NEON_HEAP_DELETE: u8 = 0x10;
pub const XLOG_NEON_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_NEON_HEAP_HOT_UPDATE: u8 = 0x30;
pub const XLOG_NEON_HEAP_LOCK: u8 = 0x40;
pub const XLOG_NEON_HEAP_MULTI_INSERT: u8 = 0x50;

pub const XLOG_NEON_HEAP_VISIBLE: u8 = 0x40;

// from xlogreader.h
pub const XLR_INFO_MASK: u8 = 0x0F;
pub const XLR_RMGR_INFO_MASK: u8 = 0xF0;

pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
pub const XLOG_TBLSPC_DROP: u8 = 0x10;

//
// from xlogrecord.h
//
pub const XLR_MAX_BLOCK_ID: u8 = 32;

pub const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: u8 = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

pub const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub const _BKPBLOCK_FLAG_MASK: u8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: u8 = 0x10; /* block data is an XLogRecordBlockImage */
pub const BKPBLOCK_HAS_DATA: u8 = 0x20;
pub const BKPBLOCK_WILL_INIT: u8 = 0x40; /* redo will re-init the page */
pub const BKPBLOCK_SAME_REL: u8 = 0x80; /* RelFileNode omitted, same as previous */

/* Information stored in bimg_info */
pub const BKPIMAGE_HAS_HOLE: u8 = 0x01; /* page image has "hole" */

/* From transam.h */
pub const FIRST_NORMAL_TRANSACTION_ID: u32 = 3;
pub const INVALID_TRANSACTION_ID: u32 = 0;

/* pg_control.h */
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: u8 = 0x10;
pub const XLOG_PARAMETER_CHANGE: u8 = 0x60;
pub const XLOG_END_OF_RECOVERY: u8 = 0x90;

/* From xlog.h */
pub const XLOG_REPLORIGIN_SET: u8 = 0x00;
pub const XLOG_REPLORIGIN_DROP: u8 = 0x10;

/* xlog_internal.h */
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLP_LONG_HEADER: u16 = 0x0002;

/* From replication/slot.h */
pub const REPL_SLOT_ON_DISK_OFFSETOF_RESTART_LSN: usize = 4*4  /* offset of `slotdata` in ReplicationSlotOnDisk  */
   + 64 /* NameData */  + 4*4;

/* From fsm_internals.h */
const FSM_NODES_PER_PAGE: usize = BLCKSZ as usize - SIZEOF_PAGE_HEADER_DATA - 4;
const FSM_NON_LEAF_NODES_PER_PAGE: usize = BLCKSZ as usize / 2 - 1;
const FSM_LEAF_NODES_PER_PAGE: usize = FSM_NODES_PER_PAGE - FSM_NON_LEAF_NODES_PER_PAGE;
pub const SLOTS_PER_FSM_PAGE: u32 = FSM_LEAF_NODES_PER_PAGE as u32;

/* From visibilitymap.c */

pub const VM_MAPSIZE: usize = BLCKSZ as usize - MAXALIGN_SIZE_OF_PAGE_HEADER_DATA;
pub const VM_BITS_PER_HEAPBLOCK: usize = 2;
pub const VM_HEAPBLOCKS_PER_BYTE: usize = 8 / VM_BITS_PER_HEAPBLOCK;
pub const VM_HEAPBLOCKS_PER_PAGE: usize = VM_MAPSIZE * VM_HEAPBLOCKS_PER_BYTE;

/* From origin.c */
pub const REPLICATION_STATE_MAGIC: u32 = 0x1257DADE;

// Don't include postgresql.conf as it is inconvenient on node start:
// we need postgresql.conf before basebackup to synchronize safekeepers
// so no point in overwriting it during backup restore. Rest of the files
// here are not needed before backup so it is okay to edit them after.
pub const PGDATA_SPECIAL_FILES: [&str; 3] =
    ["pg_hba.conf", "pg_ident.conf", "postgresql.auto.conf"];

pub static PG_HBA: &str = include_str!("../samples/pg_hba.conf");
