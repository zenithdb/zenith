//!
//! Functions for parsing WAL records.
//!

use anyhow::Result;
use bytes::{Buf, Bytes};
use postgres_ffi::pg_constants;
use postgres_ffi::BLCKSZ;
use postgres_ffi::{BlockNumber, OffsetNumber, TimestampTz};
use postgres_ffi::{MultiXactId, MultiXactOffset, MultiXactStatus, Oid, TransactionId};
use postgres_ffi::{XLogRecord, XLOG_SIZE_OF_XLOG_RECORD};
use serde::{Deserialize, Serialize};
use tracing::*;
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
}

impl NeonWalRecord {
    /// Does replaying this WAL record initialize the page from scratch, or does
    /// it need to be applied over the previous image of the page?
    pub fn will_init(&self) -> bool {
        match self {
            NeonWalRecord::Postgres { will_init, rec: _ } => *will_init,

            // None of the special neon record types currently initialize the page
            _ => false,
        }
    }
}

/// DecodedBkpBlock represents per-page data contained in a WAL record.
#[derive(Default)]
pub struct DecodedBkpBlock {
    /* Is this block ref in use? */
    //in_use: bool,

    /* Identify the block this refers to */
    pub rnode_spcnode: u32,
    pub rnode_dbnode: u32,
    pub rnode_relnode: u32,
    // Note that we have a few special forknum values for non-rel files.
    pub forknum: u8,
    pub blkno: u32,

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    pub flags: u8,

    /* Information on full-page image, if any */
    pub has_image: bool,   /* has image, even for consistency checking */
    pub apply_image: bool, /* has image that should be restored */
    pub will_init: bool,   /* record doesn't need previous page version to apply */
    //char	   *bkp_image;
    pub hole_offset: u16,
    pub hole_length: u16,
    pub bimg_offset: u32,
    pub bimg_len: u16,
    pub bimg_info: u8,

    /* Buffer holding the rmgr-specific data associated with this block */
    has_data: bool,
    data_len: u16,
}

impl DecodedBkpBlock {
    pub fn new() -> DecodedBkpBlock {
        Default::default()
    }
}

#[derive(Default)]
pub struct DecodedWALRecord {
    pub xl_xid: TransactionId,
    pub xl_info: u8,
    pub xl_rmid: u8,
    pub record: Bytes, // raw XLogRecord

    pub blocks: Vec<DecodedBkpBlock>,
    pub main_data_offset: usize,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RelFileNode {
    pub spcnode: Oid, /* tablespace */
    pub dbnode: Oid,  /* database */
    pub relnode: Oid, /* relation */
}

#[repr(C)]
#[derive(Debug)]
pub struct XlRelmapUpdate {
    pub dbid: Oid,   /* database ID, or 0 for shared map */
    pub tsid: Oid,   /* database's tablespace, or pg_global */
    pub nbytes: i32, /* size of relmap data */
}

impl XlRelmapUpdate {
    pub fn decode(buf: &mut Bytes) -> XlRelmapUpdate {
        XlRelmapUpdate {
            dbid: buf.get_u32_le(),
            tsid: buf.get_u32_le(),
            nbytes: buf.get_i32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlSmgrCreate {
    pub rnode: RelFileNode,
    // FIXME: This is ForkNumber in storage_xlog.h. That's an enum. Does it have
    // well-defined size?
    pub forknum: u8,
}

impl XlSmgrCreate {
    pub fn decode(buf: &mut Bytes) -> XlSmgrCreate {
        XlSmgrCreate {
            rnode: RelFileNode {
                spcnode: buf.get_u32_le(), /* tablespace */
                dbnode: buf.get_u32_le(),  /* database */
                relnode: buf.get_u32_le(), /* relation */
            },
            forknum: buf.get_u32_le() as u8,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlSmgrTruncate {
    pub blkno: BlockNumber,
    pub rnode: RelFileNode,
    pub flags: u32,
}

impl XlSmgrTruncate {
    pub fn decode(buf: &mut Bytes) -> XlSmgrTruncate {
        XlSmgrTruncate {
            blkno: buf.get_u32_le(),
            rnode: RelFileNode {
                spcnode: buf.get_u32_le(), /* tablespace */
                dbnode: buf.get_u32_le(),  /* database */
                relnode: buf.get_u32_le(), /* relation */
            },
            flags: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlCreateDatabase {
    pub db_id: Oid,
    pub tablespace_id: Oid,
    pub src_db_id: Oid,
    pub src_tablespace_id: Oid,
}

impl XlCreateDatabase {
    pub fn decode(buf: &mut Bytes) -> XlCreateDatabase {
        XlCreateDatabase {
            db_id: buf.get_u32_le(),
            tablespace_id: buf.get_u32_le(),
            src_db_id: buf.get_u32_le(),
            src_tablespace_id: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlDropDatabase {
    pub db_id: Oid,
    pub n_tablespaces: Oid, /* number of tablespace IDs */
    pub tablespace_ids: Vec<Oid>,
}

impl XlDropDatabase {
    pub fn decode(buf: &mut Bytes) -> XlDropDatabase {
        let mut rec = XlDropDatabase {
            db_id: buf.get_u32_le(),
            n_tablespaces: buf.get_u32_le(),
            tablespace_ids: Vec::<Oid>::new(),
        };

        for _i in 0..rec.n_tablespaces {
            let id = buf.get_u32_le();
            rec.tablespace_ids.push(id);
        }

        rec
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapInsert {
    pub offnum: OffsetNumber,
    pub flags: u8,
}

impl XlHeapInsert {
    pub fn decode(buf: &mut Bytes) -> XlHeapInsert {
        XlHeapInsert {
            offnum: buf.get_u16_le(),
            flags: buf.get_u8(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapMultiInsert {
    pub flags: u8,
    pub _padding: u8,
    pub ntuples: u16,
}

impl XlHeapMultiInsert {
    pub fn decode(buf: &mut Bytes) -> XlHeapMultiInsert {
        XlHeapMultiInsert {
            flags: buf.get_u8(),
            _padding: buf.get_u8(),
            ntuples: buf.get_u16_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapDelete {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub _padding: u16,
    pub t_cid: u32,
    pub infobits_set: u8,
    pub flags: u8,
}

impl XlHeapDelete {
    pub fn decode(buf: &mut Bytes) -> XlHeapDelete {
        let neon_format = buf.remaining() == pg_constants::SIZE_OF_HEAP_DELETE;
        let xmax = buf.get_u32_le();
        let offnum = buf.get_u16_le();
        let _padding;
        let t_cid;
        if neon_format {
            _padding = buf.get_u16_le();
            t_cid = buf.get_u32_le();
        } else {
            _padding = 0;
            t_cid = 0;
        }
        let infobits_set = buf.get_u8();
        let flags = buf.get_u8();
        assert!(((flags & pg_constants::XLH_DELETE_STORE_CID) == 0) ^ neon_format);
        XlHeapDelete {
            xmax,
            offnum,
            _padding,
            t_cid,
            infobits_set,
            flags,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapLock {
    pub locking_xid: TransactionId,
    pub offnum: OffsetNumber,
    pub _padding: u16,
    pub t_cid: u32,
    pub infobits_set: u8,
    pub flags: u8,
}

impl XlHeapLock {
    pub fn decode(buf: &mut Bytes) -> XlHeapLock {
        let neon_format = buf.remaining() == pg_constants::SIZE_OF_HEAP_LOCK;
        let locking_xid = buf.get_u32_le();
        let offnum = buf.get_u16_le();
        let _padding;
        let t_cid;
        if neon_format {
            _padding = buf.get_u16_le();
            t_cid = buf.get_u32_le();
        } else {
            _padding = 0;
            t_cid = 0;
        }
        let infobits_set = buf.get_u8();
        let flags = buf.get_u8();
        assert!(((flags & pg_constants::XLH_LOCK_STORE_CID) == 0) ^ neon_format);
        XlHeapLock {
            locking_xid,
            offnum,
            _padding,
            t_cid,
            infobits_set,
            flags,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlHeapUpdate {
    pub old_xmax: TransactionId,
    pub old_offnum: OffsetNumber,
    pub old_infobits_set: u8,
    pub flags: u8,
    pub t_cid: u32,
    pub new_xmax: TransactionId,
    pub new_offnum: OffsetNumber,
}

impl XlHeapUpdate {
    pub fn decode(buf: &mut Bytes) -> XlHeapUpdate {
        let old_xmax = buf.get_u32_le();
        let old_offnum = buf.get_u16_le();
        let old_infobits_set = buf.get_u8();
        let flags = buf.get_u8();
        let t_cid = if (flags & pg_constants::XLH_UPDATE_STORE_CID) != 0 {
            buf.get_u32()
        } else {
            0
        };
        XlHeapUpdate {
            old_xmax,
            old_offnum,
            old_infobits_set,
            flags,
            t_cid,
            new_xmax: buf.get_u32_le(),
            new_offnum: buf.get_u16_le(),
        }
    }
}

///
/// Note: Parsing some fields is missing, because they're not needed.
///
/// This is similar to the xl_xact_parsed_commit and
/// xl_xact_parsed_abort structs in PostgreSQL, but we use the same
/// struct for commits and aborts.
///
#[derive(Debug)]
pub struct XlXactParsedRecord {
    pub xid: TransactionId,
    pub info: u8,
    pub xact_time: TimestampTz,
    pub xinfo: u32,

    pub db_id: Oid, /* MyDatabaseId */
    pub ts_id: Oid, /* MyDatabaseTableSpace */

    pub subxacts: Vec<TransactionId>,

    pub xnodes: Vec<RelFileNode>,
}

impl XlXactParsedRecord {
    /// Decode a XLOG_XACT_COMMIT/ABORT/COMMIT_PREPARED/ABORT_PREPARED
    /// record. This should agree with the ParseCommitRecord and ParseAbortRecord
    /// functions in PostgreSQL (in src/backend/access/rmgr/xactdesc.c)
    pub fn decode(buf: &mut Bytes, mut xid: TransactionId, xl_info: u8) -> XlXactParsedRecord {
        let info = xl_info & pg_constants::XLOG_XACT_OPMASK;
        // The record starts with time of commit/abort
        let xact_time = buf.get_i64_le();
        let xinfo = if xl_info & pg_constants::XLOG_XACT_HAS_INFO != 0 {
            buf.get_u32_le()
        } else {
            0
        };
        let db_id;
        let ts_id;
        if xinfo & pg_constants::XACT_XINFO_HAS_DBINFO != 0 {
            db_id = buf.get_u32_le();
            ts_id = buf.get_u32_le();
        } else {
            db_id = 0;
            ts_id = 0;
        }
        let mut subxacts = Vec::<TransactionId>::new();
        if xinfo & pg_constants::XACT_XINFO_HAS_SUBXACTS != 0 {
            let nsubxacts = buf.get_i32_le();
            for _i in 0..nsubxacts {
                let subxact = buf.get_u32_le();
                subxacts.push(subxact);
            }
        }
        let mut xnodes = Vec::<RelFileNode>::new();
        if xinfo & pg_constants::XACT_XINFO_HAS_RELFILENODES != 0 {
            let nrels = buf.get_i32_le();
            for _i in 0..nrels {
                let spcnode = buf.get_u32_le();
                let dbnode = buf.get_u32_le();
                let relnode = buf.get_u32_le();
                trace!(
                    "XLOG_XACT_COMMIT relfilenode {}/{}/{}",
                    spcnode,
                    dbnode,
                    relnode
                );
                xnodes.push(RelFileNode {
                    spcnode,
                    dbnode,
                    relnode,
                });
            }
        }

        if xinfo & postgres_ffi::v15::bindings::XACT_XINFO_HAS_DROPPED_STATS != 0 {
            let nitems = buf.get_i32_le();
            debug!(
                "XLOG_XACT_COMMIT-XACT_XINFO_HAS_DROPPED_STAT nitems {}",
                nitems
            );
            let sizeof_xl_xact_stats_item = 12;
            buf.advance((nitems * sizeof_xl_xact_stats_item).try_into().unwrap());
        }

        if xinfo & pg_constants::XACT_XINFO_HAS_INVALS != 0 {
            let nmsgs = buf.get_i32_le();
            let sizeof_shared_invalidation_message = 16;
            buf.advance(
                (nmsgs * sizeof_shared_invalidation_message)
                    .try_into()
                    .unwrap(),
            );
        }

        if xinfo & pg_constants::XACT_XINFO_HAS_TWOPHASE != 0 {
            xid = buf.get_u32_le();
            debug!("XLOG_XACT_COMMIT-XACT_XINFO_HAS_TWOPHASE xid {}", xid);
        }

        XlXactParsedRecord {
            xid,
            info,
            xact_time,
            xinfo,
            db_id,
            ts_id,
            subxacts,
            xnodes,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlClogTruncate {
    pub pageno: u32,
    pub oldest_xid: TransactionId,
    pub oldest_xid_db: Oid,
}

impl XlClogTruncate {
    pub fn decode(buf: &mut Bytes) -> XlClogTruncate {
        XlClogTruncate {
            pageno: buf.get_u32_le(),
            oldest_xid: buf.get_u32_le(),
            oldest_xid_db: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiXactMember {
    pub xid: TransactionId,
    pub status: MultiXactStatus,
}

impl MultiXactMember {
    pub fn decode(buf: &mut Bytes) -> MultiXactMember {
        MultiXactMember {
            xid: buf.get_u32_le(),
            status: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlMultiXactCreate {
    pub mid: MultiXactId,      /* new MultiXact's ID */
    pub moff: MultiXactOffset, /* its starting offset in members file */
    pub nmembers: u32,         /* number of member XIDs */
    pub members: Vec<MultiXactMember>,
}

impl XlMultiXactCreate {
    pub fn decode(buf: &mut Bytes) -> XlMultiXactCreate {
        let mid = buf.get_u32_le();
        let moff = buf.get_u32_le();
        let nmembers = buf.get_u32_le();
        let mut members = Vec::new();
        for _ in 0..nmembers {
            members.push(MultiXactMember::decode(buf));
        }
        XlMultiXactCreate {
            mid,
            moff,
            nmembers,
            members,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlMultiXactTruncate {
    pub oldest_multi_db: Oid,
    /* to-be-truncated range of multixact offsets */
    pub start_trunc_off: MultiXactId, /* just for completeness' sake */
    pub end_trunc_off: MultiXactId,

    /* to-be-truncated range of multixact members */
    pub start_trunc_memb: MultiXactOffset,
    pub end_trunc_memb: MultiXactOffset,
}

impl XlMultiXactTruncate {
    pub fn decode(buf: &mut Bytes) -> XlMultiXactTruncate {
        XlMultiXactTruncate {
            oldest_multi_db: buf.get_u32_le(),
            start_trunc_off: buf.get_u32_le(),
            end_trunc_off: buf.get_u32_le(),
            start_trunc_memb: buf.get_u32_le(),
            end_trunc_memb: buf.get_u32_le(),
        }
    }
}

/// Main routine to decode a WAL record and figure out which blocks are modified
//
// See xlogrecord.h for details
// The overall layout of an XLOG record is:
//		Fixed-size header (XLogRecord struct)
//      XLogRecordBlockHeader struct
//          If pg_constants::BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows
//	           If pg_constants::BKPIMAGE_HAS_HOLE and pg_constants::BKPIMAGE_IS_COMPRESSED, an
//	           XLogRecordBlockCompressHeader struct follows.
//          If pg_constants::BKPBLOCK_SAME_REL is not set, a RelFileNode follows
//          BlockNumber follows
//      XLogRecordBlockHeader struct
//      ...
//      XLogRecordDataHeader[Short|Long] struct
//      block data
//      block data
//      ...
//      main data
//
//
// For performance reasons, the caller provides the DecodedWALRecord struct and the function just fills it in.
// It would be more natural for this function to return a DecodedWALRecord as return value,
// but reusing the caller-supplied struct avoids an allocation.
// This code is in the hot path for digesting incoming WAL, and is very performance sensitive.
//
pub fn decode_wal_record(
    record: Bytes,
    decoded: &mut DecodedWALRecord,
    pg_version: u32,
) -> Result<()> {
    let mut rnode_spcnode: u32 = 0;
    let mut rnode_dbnode: u32 = 0;
    let mut rnode_relnode: u32 = 0;
    let mut got_rnode = false;

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf)?;

    trace!(
        "decode_wal_record xl_rmid = {} xl_info = {}",
        xlogrec.xl_rmid,
        xlogrec.xl_info
    );

    let remaining: usize = xlogrec.xl_tot_len as usize - XLOG_SIZE_OF_XLOG_RECORD;

    if buf.remaining() != remaining {
        //TODO error
    }

    let mut max_block_id = 0;
    let mut blocks_total_len: u32 = 0;
    let mut main_data_len = 0;
    let mut datatotal: u32 = 0;
    decoded.blocks.clear();

    // 2. Decode the headers.
    // XLogRecordBlockHeaders if any,
    // XLogRecordDataHeader[Short|Long]
    while buf.remaining() > datatotal as usize {
        let block_id = buf.get_u8();

        match block_id {
            pg_constants::XLR_BLOCK_ID_DATA_SHORT => {
                /* XLogRecordDataHeaderShort */
                main_data_len = buf.get_u8() as u32;
                datatotal += main_data_len;
            }

            pg_constants::XLR_BLOCK_ID_DATA_LONG => {
                /* XLogRecordDataHeaderLong */
                main_data_len = buf.get_u32_le();
                datatotal += main_data_len;
            }

            pg_constants::XLR_BLOCK_ID_ORIGIN => {
                // RepOriginId is uint16
                buf.advance(2);
            }

            pg_constants::XLR_BLOCK_ID_TOPLEVEL_XID => {
                // TransactionId is uint32
                buf.advance(4);
            }

            0..=pg_constants::XLR_MAX_BLOCK_ID => {
                /* XLogRecordBlockHeader */
                let mut blk = DecodedBkpBlock::new();

                if block_id <= max_block_id {
                    // TODO
                    //report_invalid_record(state,
                    //			  "out-of-order block_id %u at %X/%X",
                    //			  block_id,
                    //			  (uint32) (state->ReadRecPtr >> 32),
                    //			  (uint32) state->ReadRecPtr);
                    //    goto err;
                }
                max_block_id = block_id;

                let fork_flags: u8 = buf.get_u8();
                blk.forknum = fork_flags & pg_constants::BKPBLOCK_FORK_MASK;
                blk.flags = fork_flags;
                blk.has_image = (fork_flags & pg_constants::BKPBLOCK_HAS_IMAGE) != 0;
                blk.has_data = (fork_flags & pg_constants::BKPBLOCK_HAS_DATA) != 0;
                blk.will_init = (fork_flags & pg_constants::BKPBLOCK_WILL_INIT) != 0;
                blk.data_len = buf.get_u16_le();

                /* TODO cross-check that the HAS_DATA flag is set iff data_length > 0 */

                datatotal += blk.data_len as u32;
                blocks_total_len += blk.data_len as u32;

                if blk.has_image {
                    blk.bimg_len = buf.get_u16_le();
                    blk.hole_offset = buf.get_u16_le();
                    blk.bimg_info = buf.get_u8();

                    blk.apply_image = if pg_version == 14 {
                        (blk.bimg_info & postgres_ffi::v14::bindings::BKPIMAGE_APPLY) != 0
                    } else {
                        assert_eq!(pg_version, 15);
                        (blk.bimg_info & postgres_ffi::v15::bindings::BKPIMAGE_APPLY) != 0
                    };

                    let blk_img_is_compressed =
                        postgres_ffi::bkpimage_is_compressed(blk.bimg_info, pg_version)?;

                    if blk_img_is_compressed {
                        debug!("compressed block image , pg_version = {}", pg_version);
                    }

                    if blk_img_is_compressed {
                        if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE != 0 {
                            blk.hole_length = buf.get_u16_le();
                        } else {
                            blk.hole_length = 0;
                        }
                    } else {
                        blk.hole_length = BLCKSZ - blk.bimg_len;
                    }
                    datatotal += blk.bimg_len as u32;
                    blocks_total_len += blk.bimg_len as u32;

                    /*
                     * cross-check that hole_offset > 0, hole_length > 0 and
                     * bimg_len < BLCKSZ if the HAS_HOLE flag is set.
                     */
                    if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE != 0
                        && (blk.hole_offset == 0 || blk.hole_length == 0 || blk.bimg_len == BLCKSZ)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "pg_constants::BKPIMAGE_HAS_HOLE set, but hole offset %u length %u block image length %u at %X/%X",
                                      (unsigned int) blk->hole_offset,
                                      (unsigned int) blk->hole_length,
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that hole_offset == 0 and hole_length == 0 if
                     * the HAS_HOLE flag is not set.
                     */
                    if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE == 0
                        && (blk.hole_offset != 0 || blk.hole_length != 0)
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "pg_constants::BKPIMAGE_HAS_HOLE not set, but hole offset %u length %u at %X/%X",
                                      (unsigned int) blk->hole_offset,
                                      (unsigned int) blk->hole_length,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len < BLCKSZ if the IS_COMPRESSED
                     * flag is set.
                     */
                    if !blk_img_is_compressed && blk.bimg_len == BLCKSZ {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "pg_constants::BKPIMAGE_IS_COMPRESSED set, but block image length %u at %X/%X",
                                      (unsigned int) blk->bimg_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }

                    /*
                     * cross-check that bimg_len = BLCKSZ if neither HAS_HOLE nor
                     * IS_COMPRESSED flag is set.
                     */
                    if blk.bimg_info & pg_constants::BKPIMAGE_HAS_HOLE == 0
                        && !blk_img_is_compressed
                        && blk.bimg_len != BLCKSZ
                    {
                        // TODO
                        /*
                        report_invalid_record(state,
                                      "neither pg_constants::BKPIMAGE_HAS_HOLE nor pg_constants::BKPIMAGE_IS_COMPRESSED set, but block image length is %u at %X/%X",
                                      (unsigned int) blk->data_len,
                                      (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                        goto err;
                                     */
                    }
                }
                if fork_flags & pg_constants::BKPBLOCK_SAME_REL == 0 {
                    rnode_spcnode = buf.get_u32_le();
                    rnode_dbnode = buf.get_u32_le();
                    rnode_relnode = buf.get_u32_le();
                    got_rnode = true;
                } else if !got_rnode {
                    // TODO
                    /*
                    report_invalid_record(state,
                                    "pg_constants::BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                                    (uint32) (state->ReadRecPtr >> 32), (uint32) state->ReadRecPtr);
                    goto err;           */
                }

                blk.rnode_spcnode = rnode_spcnode;
                blk.rnode_dbnode = rnode_dbnode;
                blk.rnode_relnode = rnode_relnode;

                blk.blkno = buf.get_u32_le();
                trace!(
                    "this record affects {}/{}/{} blk {}",
                    rnode_spcnode,
                    rnode_dbnode,
                    rnode_relnode,
                    blk.blkno
                );

                decoded.blocks.push(blk);
            }

            _ => {
                // TODO: invalid block_id
            }
        }
    }

    // 3. Decode blocks.
    let mut ptr = record.len() - buf.remaining();
    for blk in decoded.blocks.iter_mut() {
        if blk.has_image {
            blk.bimg_offset = ptr as u32;
            ptr += blk.bimg_len as usize;
        }
        if blk.has_data {
            ptr += blk.data_len as usize;
        }
    }
    // We don't need them, so just skip blocks_total_len bytes
    buf.advance(blocks_total_len as usize);
    assert_eq!(ptr, record.len() - buf.remaining());

    let main_data_offset = (xlogrec.xl_tot_len - main_data_len) as usize;

    // 4. Decode main_data
    if main_data_len > 0 {
        assert_eq!(buf.remaining(), main_data_len as usize);
    }

    decoded.xl_xid = xlogrec.xl_xid;
    decoded.xl_info = xlogrec.xl_info;
    decoded.xl_rmid = xlogrec.xl_rmid;
    decoded.record = record;
    decoded.main_data_offset = main_data_offset;

    Ok(())
}

///
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

fn describe_postgres_wal_record(record: &Bytes) -> Result<String, DeserializeError> {
    // TODO: It would be nice to use the PostgreSQL rmgrdesc infrastructure for this.
    // Maybe use the postgres wal redo process, the same used for replaying WAL records?
    // Or could we compile the rmgrdesc routines into the dump_layer_file() binary directly,
    // without worrying about security?
    //
    // But for now, we have a hand-written code for a few common WAL record types here.

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf)?;

    let unknown_str: String;

    let result: &str = match xlogrec.xl_rmid {
        pg_constants::RM_HEAP2_ID => {
            let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            match info {
                pg_constants::XLOG_HEAP2_MULTI_INSERT => "HEAP2 MULTI_INSERT",
                pg_constants::XLOG_HEAP2_VISIBLE => "HEAP2 VISIBLE",
                _ => {
                    unknown_str = format!("HEAP2 UNKNOWN_0x{:02x}", info);
                    &unknown_str
                }
            }
        }
        pg_constants::RM_HEAP_ID => {
            let info = xlogrec.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            match info {
                pg_constants::XLOG_HEAP_INSERT => "HEAP INSERT",
                pg_constants::XLOG_HEAP_DELETE => "HEAP DELETE",
                pg_constants::XLOG_HEAP_UPDATE => "HEAP UPDATE",
                pg_constants::XLOG_HEAP_HOT_UPDATE => "HEAP HOT_UPDATE",
                _ => {
                    unknown_str = format!("HEAP2 UNKNOWN_0x{:02x}", info);
                    &unknown_str
                }
            }
        }
        pg_constants::RM_XLOG_ID => {
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            match info {
                pg_constants::XLOG_FPI => "XLOG FPI",
                pg_constants::XLOG_FPI_FOR_HINT => "XLOG FPI_FOR_HINT",
                _ => {
                    unknown_str = format!("XLOG UNKNOWN_0x{:02x}", info);
                    &unknown_str
                }
            }
        }
        rmid => {
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

            unknown_str = format!("UNKNOWN_RM_{} INFO_0x{:02x}", rmid, info);
            &unknown_str
        }
    };

    Ok(String::from(result))
}
