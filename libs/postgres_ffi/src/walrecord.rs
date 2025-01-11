//! This module houses types used in decoding of PG WAL
//! records.
//!
//! TODO: Generate separate types for each supported PG version

use crate::pg_constants;
use crate::XLogRecord;
use crate::{
    BlockNumber, MultiXactId, MultiXactOffset, MultiXactStatus, Oid, RepOriginId, TimestampTz,
    TransactionId,
};
use crate::{BLCKSZ, XLOG_SIZE_OF_XLOG_RECORD};
use bytes::{Buf, Bytes};
use serde::{Deserialize, Serialize};
use utils::bin_ser::DeserializeError;
use utils::lsn::Lsn;

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlMultiXactCreate {
    pub mid: MultiXactId,
    /* new MultiXact's ID */
    pub moff: MultiXactOffset,
    /* its starting offset in members file */
    pub nmembers: u32,
    /* number of member XIDs */
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlMultiXactTruncate {
    pub oldest_multi_db: Oid,
    /* to-be-truncated range of multixact offsets */
    pub start_trunc_off: MultiXactId,
    /* just for completeness' sake */
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

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlReploriginDrop {
    pub node_id: RepOriginId,
}

impl XlReploriginDrop {
    pub fn decode(buf: &mut Bytes) -> XlReploriginDrop {
        XlReploriginDrop {
            node_id: buf.get_u16_le(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlReploriginSet {
    pub remote_lsn: Lsn,
    pub node_id: RepOriginId,
}

impl XlReploriginSet {
    pub fn decode(buf: &mut Bytes) -> XlReploriginSet {
        XlReploriginSet {
            remote_lsn: Lsn(buf.get_u64_le()),
            node_id: buf.get_u16_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RelFileNode {
    pub spcnode: Oid, /* tablespace */
    pub dbnode: Oid,  /* database */
    pub relnode: Oid, /* relation */
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
    pub has_image: bool,
    /* has image, even for consistency checking */
    pub apply_image: bool,
    /* has image that should be restored */
    pub will_init: bool,
    /* record doesn't need previous page version to apply */
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
    pub origin_id: u16,
}

impl DecodedWALRecord {
    /// Check if this WAL record represents a legacy "copy" database creation, which populates new relations
    /// by reading other existing relations' data blocks.  This is more complex to apply than new-style database
    /// creations which simply include all the desired blocks in the WAL, so we need a helper function to detect this case.
    pub fn is_dbase_create_copy(&self, pg_version: u32) -> bool {
        if self.xl_rmid == pg_constants::RM_DBASE_ID {
            let info = self.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            match pg_version {
                14 => {
                    // Postgres 14 database creations are always the legacy kind
                    info == crate::v14::bindings::XLOG_DBASE_CREATE
                }
                15 => info == crate::v15::bindings::XLOG_DBASE_CREATE_FILE_COPY,
                16 => info == crate::v16::bindings::XLOG_DBASE_CREATE_FILE_COPY,
                17 => info == crate::v17::bindings::XLOG_DBASE_CREATE_FILE_COPY,
                _ => {
                    panic!("Unsupported postgres version {pg_version}")
                }
            }
        } else {
            false
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
) -> anyhow::Result<()> {
    let mut rnode_spcnode: u32 = 0;
    let mut rnode_dbnode: u32 = 0;
    let mut rnode_relnode: u32 = 0;
    let mut got_rnode = false;
    let mut origin_id: u16 = 0;

    let mut buf = record.clone();

    // 1. Parse XLogRecord struct

    // FIXME: assume little-endian here
    let xlogrec = XLogRecord::from_bytes(&mut buf)?;

    tracing::trace!(
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
                origin_id = buf.get_u16_le();
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

                    blk.apply_image = dispatch_pgversion!(
                        pg_version,
                        (blk.bimg_info & pgv::bindings::BKPIMAGE_APPLY) != 0
                    );

                    let blk_img_is_compressed =
                        crate::bkpimage_is_compressed(blk.bimg_info, pg_version);

                    if blk_img_is_compressed {
                        tracing::debug!("compressed block image , pg_version = {}", pg_version);
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
                tracing::trace!(
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
    decoded.origin_id = origin_id;
    decoded.main_data_offset = main_data_offset;

    Ok(())
}

pub mod v14 {
    use crate::{OffsetNumber, TransactionId};
    use bytes::{Buf, Bytes};

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
            XlHeapDelete {
                xmax: buf.get_u32_le(),
                offnum: buf.get_u16_le(),
                _padding: buf.get_u16_le(),
                t_cid: buf.get_u32_le(),
                infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
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
            XlHeapUpdate {
                old_xmax: buf.get_u32_le(),
                old_offnum: buf.get_u16_le(),
                old_infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
                t_cid: buf.get_u32_le(),
                new_xmax: buf.get_u32_le(),
                new_offnum: buf.get_u16_le(),
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
            XlHeapLock {
                locking_xid: buf.get_u32_le(),
                offnum: buf.get_u16_le(),
                _padding: buf.get_u16_le(),
                t_cid: buf.get_u32_le(),
                infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
            }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapLockUpdated {
        pub xmax: TransactionId,
        pub offnum: OffsetNumber,
        pub infobits_set: u8,
        pub flags: u8,
    }

    impl XlHeapLockUpdated {
        pub fn decode(buf: &mut Bytes) -> XlHeapLockUpdated {
            XlHeapLockUpdated {
                xmax: buf.get_u32_le(),
                offnum: buf.get_u16_le(),
                infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
            }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlParameterChange {
        pub max_connections: i32,
        pub max_worker_processes: i32,
        pub max_wal_senders: i32,
        pub max_prepared_xacts: i32,
        pub max_locks_per_xact: i32,
        pub wal_level: i32,
        pub wal_log_hints: bool,
        pub track_commit_timestamp: bool,
        pub _padding: [u8; 2],
    }

    impl XlParameterChange {
        pub fn decode(buf: &mut Bytes) -> XlParameterChange {
            XlParameterChange {
                max_connections: buf.get_i32_le(),
                max_worker_processes: buf.get_i32_le(),
                max_wal_senders: buf.get_i32_le(),
                max_prepared_xacts: buf.get_i32_le(),
                max_locks_per_xact: buf.get_i32_le(),
                wal_level: buf.get_i32_le(),
                wal_log_hints: buf.get_u8() != 0,
                track_commit_timestamp: buf.get_u8() != 0,
                _padding: [buf.get_u8(), buf.get_u8()],
            }
        }
    }
}

pub mod v15 {
    pub use super::v14::{
        XlHeapDelete, XlHeapInsert, XlHeapLock, XlHeapLockUpdated, XlHeapMultiInsert, XlHeapUpdate,
        XlParameterChange,
    };
}

pub mod v16 {
    pub use super::v14::{XlHeapInsert, XlHeapLockUpdated, XlHeapMultiInsert, XlParameterChange};
    use crate::{OffsetNumber, TransactionId};
    use bytes::{Buf, Bytes};

    pub struct XlHeapDelete {
        pub xmax: TransactionId,
        pub offnum: OffsetNumber,
        pub infobits_set: u8,
        pub flags: u8,
    }

    impl XlHeapDelete {
        pub fn decode(buf: &mut Bytes) -> XlHeapDelete {
            XlHeapDelete {
                xmax: buf.get_u32_le(),
                offnum: buf.get_u16_le(),
                infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
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
        pub new_xmax: TransactionId,
        pub new_offnum: OffsetNumber,
    }

    impl XlHeapUpdate {
        pub fn decode(buf: &mut Bytes) -> XlHeapUpdate {
            XlHeapUpdate {
                old_xmax: buf.get_u32_le(),
                old_offnum: buf.get_u16_le(),
                old_infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
                new_xmax: buf.get_u32_le(),
                new_offnum: buf.get_u16_le(),
            }
        }
    }

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlHeapLock {
        pub locking_xid: TransactionId,
        pub offnum: OffsetNumber,
        pub infobits_set: u8,
        pub flags: u8,
    }

    impl XlHeapLock {
        pub fn decode(buf: &mut Bytes) -> XlHeapLock {
            XlHeapLock {
                locking_xid: buf.get_u32_le(),
                offnum: buf.get_u16_le(),
                infobits_set: buf.get_u8(),
                flags: buf.get_u8(),
            }
        }
    }

    /* Since PG16, we have the Neon RMGR (RM_NEON_ID) to manage Neon-flavored WAL. */
    pub mod rm_neon {
        use crate::{OffsetNumber, TransactionId};
        use bytes::{Buf, Bytes};

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapInsert {
            pub offnum: OffsetNumber,
            pub flags: u8,
        }

        impl XlNeonHeapInsert {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapInsert {
                XlNeonHeapInsert {
                    offnum: buf.get_u16_le(),
                    flags: buf.get_u8(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapMultiInsert {
            pub flags: u8,
            pub _padding: u8,
            pub ntuples: u16,
            pub t_cid: u32,
        }

        impl XlNeonHeapMultiInsert {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapMultiInsert {
                XlNeonHeapMultiInsert {
                    flags: buf.get_u8(),
                    _padding: buf.get_u8(),
                    ntuples: buf.get_u16_le(),
                    t_cid: buf.get_u32_le(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapDelete {
            pub xmax: TransactionId,
            pub offnum: OffsetNumber,
            pub infobits_set: u8,
            pub flags: u8,
            pub t_cid: u32,
        }

        impl XlNeonHeapDelete {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapDelete {
                XlNeonHeapDelete {
                    xmax: buf.get_u32_le(),
                    offnum: buf.get_u16_le(),
                    infobits_set: buf.get_u8(),
                    flags: buf.get_u8(),
                    t_cid: buf.get_u32_le(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapUpdate {
            pub old_xmax: TransactionId,
            pub old_offnum: OffsetNumber,
            pub old_infobits_set: u8,
            pub flags: u8,
            pub t_cid: u32,
            pub new_xmax: TransactionId,
            pub new_offnum: OffsetNumber,
        }

        impl XlNeonHeapUpdate {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapUpdate {
                XlNeonHeapUpdate {
                    old_xmax: buf.get_u32_le(),
                    old_offnum: buf.get_u16_le(),
                    old_infobits_set: buf.get_u8(),
                    flags: buf.get_u8(),
                    t_cid: buf.get_u32(),
                    new_xmax: buf.get_u32_le(),
                    new_offnum: buf.get_u16_le(),
                }
            }
        }

        #[repr(C)]
        #[derive(Debug)]
        pub struct XlNeonHeapLock {
            pub locking_xid: TransactionId,
            pub t_cid: u32,
            pub offnum: OffsetNumber,
            pub infobits_set: u8,
            pub flags: u8,
        }

        impl XlNeonHeapLock {
            pub fn decode(buf: &mut Bytes) -> XlNeonHeapLock {
                XlNeonHeapLock {
                    locking_xid: buf.get_u32_le(),
                    t_cid: buf.get_u32_le(),
                    offnum: buf.get_u16_le(),
                    infobits_set: buf.get_u8(),
                    flags: buf.get_u8(),
                }
            }
        }
    }
}

pub mod v17 {
    pub use super::v14::XlHeapLockUpdated;
    pub use crate::{TimeLineID, TimestampTz};
    use bytes::{Buf, Bytes};

    pub use super::v16::rm_neon;
    pub use super::v16::{
        XlHeapDelete, XlHeapInsert, XlHeapLock, XlHeapMultiInsert, XlHeapUpdate, XlParameterChange,
    };

    #[repr(C)]
    #[derive(Debug)]
    pub struct XlEndOfRecovery {
        pub end_time: TimestampTz,
        pub this_time_line_id: TimeLineID,
        pub prev_time_line_id: TimeLineID,
        pub wal_level: i32,
    }

    impl XlEndOfRecovery {
        pub fn decode(buf: &mut Bytes) -> XlEndOfRecovery {
            XlEndOfRecovery {
                end_time: buf.get_i64_le(),
                this_time_line_id: buf.get_u32_le(),
                prev_time_line_id: buf.get_u32_le(),
                wal_level: buf.get_i32_le(),
            }
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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

///
/// Note: Parsing some fields is missing, because they're not needed.
///
/// This is similar to the xl_xact_parsed_commit and
/// xl_xact_parsed_abort structs in PostgreSQL, but we use the same
/// struct for commits and aborts.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct XlXactParsedRecord {
    pub xid: TransactionId,
    pub info: u8,
    pub xact_time: TimestampTz,
    pub xinfo: u32,

    pub db_id: Oid,
    /* MyDatabaseId */
    pub ts_id: Oid,
    /* MyDatabaseTableSpace */
    pub subxacts: Vec<TransactionId>,

    pub xnodes: Vec<RelFileNode>,
    pub origin_lsn: Lsn,
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
                tracing::trace!(
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

        if xinfo & crate::v15::bindings::XACT_XINFO_HAS_DROPPED_STATS != 0 {
            let nitems = buf.get_i32_le();
            tracing::debug!(
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
            tracing::debug!("XLOG_XACT_COMMIT-XACT_XINFO_HAS_TWOPHASE xid {}", xid);
        }

        let origin_lsn = if xinfo & pg_constants::XACT_XINFO_HAS_ORIGIN != 0 {
            Lsn(buf.get_u64_le())
        } else {
            Lsn::INVALID
        };
        XlXactParsedRecord {
            xid,
            info,
            xact_time,
            xinfo,
            db_id,
            ts_id,
            subxacts,
            xnodes,
            origin_lsn,
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
    pub fn decode(buf: &mut Bytes, pg_version: u32) -> XlClogTruncate {
        XlClogTruncate {
            pageno: if pg_version < 17 {
                buf.get_u32_le()
            } else {
                buf.get_u64_le() as u32
            },
            oldest_xid: buf.get_u32_le(),
            oldest_xid_db: buf.get_u32_le(),
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlLogicalMessage {
    pub db_id: Oid,
    pub transactional: bool,
    pub prefix_size: usize,
    pub message_size: usize,
}

impl XlLogicalMessage {
    pub fn decode(buf: &mut Bytes) -> XlLogicalMessage {
        XlLogicalMessage {
            db_id: buf.get_u32_le(),
            transactional: buf.get_u32_le() != 0, // 4-bytes alignment
            prefix_size: buf.get_u64_le() as usize,
            message_size: buf.get_u64_le() as usize,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct XlRunningXacts {
    pub xcnt: u32,
    pub subxcnt: u32,
    pub subxid_overflow: bool,
    pub next_xid: TransactionId,
    pub oldest_running_xid: TransactionId,
    pub latest_completed_xid: TransactionId,
    pub xids: Vec<TransactionId>,
}

impl XlRunningXacts {
    pub fn decode(buf: &mut Bytes) -> XlRunningXacts {
        let xcnt = buf.get_u32_le();
        let subxcnt = buf.get_u32_le();
        let subxid_overflow = buf.get_u32_le() != 0;
        let next_xid = buf.get_u32_le();
        let oldest_running_xid = buf.get_u32_le();
        let latest_completed_xid = buf.get_u32_le();
        let mut xids = Vec::new();
        for _ in 0..(xcnt + subxcnt) {
            xids.push(buf.get_u32_le());
        }
        XlRunningXacts {
            xcnt,
            subxcnt,
            subxid_overflow,
            next_xid,
            oldest_running_xid,
            latest_completed_xid,
            xids,
        }
    }
}

pub fn describe_postgres_wal_record(record: &Bytes) -> Result<String, DeserializeError> {
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
