//
// This file contains common utilities for dealing with PostgreSQL WAL files and
// LSNs.
//
// Many of these functions have been copied from PostgreSQL, and rewritten in
// Rust. That's why they don't follow the usual Rust naming conventions, they
// have been named the same as the corresponding PostgreSQL functions instead.
//

use super::super::waldecoder::WalStreamDecoder;
use super::bindings::{
    CheckPoint, ControlFileData, DBState_DB_SHUTDOWNED, FullTransactionId, TimeLineID, TimestampTz,
    XLogLongPageHeaderData, XLogPageHeaderData, XLogRecPtr, XLogRecord, XLogSegNo, XLOG_PAGE_MAGIC,
};
use super::wal_generator::LogicalMessageGenerator;
use super::PG_MAJORVERSION;
use crate::pg_constants;
use crate::PG_TLI;
use crate::{uint32, uint64, Oid};
use crate::{WAL_SEGMENT_SIZE, XLOG_BLCKSZ};

use bytes::BytesMut;
use bytes::{Buf, Bytes};

use log::*;

use serde::Serialize;
use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io::prelude::*;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::path::Path;
use std::time::SystemTime;
use utils::bin_ser::DeserializeError;
use utils::bin_ser::SerializeError;

use utils::lsn::Lsn;

pub const XLOG_FNAME_LEN: usize = 24;
pub const XLP_BKP_REMOVABLE: u16 = 0x0004;
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLP_REM_LEN_OFFS: usize = 2 + 2 + 4 + 8;
pub const XLOG_RECORD_CRC_OFFS: usize = 4 + 4 + 8 + 1 + 1 + 2;

pub const XLOG_SIZE_OF_XLOG_SHORT_PHD: usize = size_of::<XLogPageHeaderData>();
pub const XLOG_SIZE_OF_XLOG_LONG_PHD: usize = size_of::<XLogLongPageHeaderData>();
pub const XLOG_SIZE_OF_XLOG_RECORD: usize = size_of::<XLogRecord>();
#[allow(clippy::identity_op)]
pub const SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT: usize = 1 * 2;

/// Interval of checkpointing metadata file. We should store metadata file to enforce
/// predicate that checkpoint.nextXid is larger than any XID in WAL.
/// But flushing checkpoint file for each transaction seems to be too expensive,
/// so XID_CHECKPOINT_INTERVAL is used to forward align nextXid and so perform
/// metadata checkpoint only once per XID_CHECKPOINT_INTERVAL transactions.
/// XID_CHECKPOINT_INTERVAL should not be larger than BLCKSZ*CLOG_XACTS_PER_BYTE
/// in order to let CLOG_TRUNCATE mechanism correctly extend CLOG.
const XID_CHECKPOINT_INTERVAL: u32 = 1024;

pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: usize) -> XLogSegNo {
    (0x100000000u64 / wal_segsz_bytes as u64) as XLogSegNo
}

pub fn XLogSegNoOffsetToRecPtr(
    segno: XLogSegNo,
    offset: u32,
    wal_segsz_bytes: usize,
) -> XLogRecPtr {
    segno * (wal_segsz_bytes as u64) + (offset as u64)
}

pub fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: usize) -> String {
    format!(
        "{:>08X}{:>08X}{:>08X}",
        tli,
        logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes),
        logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)
    )
}

pub fn XLogFromFileName(
    fname: &OsStr,
    wal_seg_size: usize,
) -> anyhow::Result<(XLogSegNo, TimeLineID)> {
    if let Some(fname_str) = fname.to_str() {
        let tli = u32::from_str_radix(&fname_str[0..8], 16)?;
        let log = u32::from_str_radix(&fname_str[8..16], 16)? as XLogSegNo;
        let seg = u32::from_str_radix(&fname_str[16..24], 16)? as XLogSegNo;
        Ok((log * XLogSegmentsPerXLogId(wal_seg_size) + seg, tli))
    } else {
        anyhow::bail!("non-ut8 filename: {:?}", fname);
    }
}

pub fn IsXLogFileName(fname: &OsStr) -> bool {
    if let Some(fname) = fname.to_str() {
        fname.len() == XLOG_FNAME_LEN && fname.chars().all(|c| c.is_ascii_hexdigit())
    } else {
        false
    }
}

pub fn IsPartialXLogFileName(fname: &OsStr) -> bool {
    if let Some(fname) = fname.to_str() {
        fname.ends_with(".partial") && IsXLogFileName(OsStr::new(&fname[0..fname.len() - 8]))
    } else {
        false
    }
}

/// If LSN points to the beginning of the page, then shift it to first record,
/// otherwise align on 8-bytes boundary (required for WAL records)
pub fn normalize_lsn(lsn: Lsn, seg_sz: usize) -> Lsn {
    if lsn.0 % XLOG_BLCKSZ as u64 == 0 {
        let hdr_size = if lsn.0 % seg_sz as u64 == 0 {
            XLOG_SIZE_OF_XLOG_LONG_PHD
        } else {
            XLOG_SIZE_OF_XLOG_SHORT_PHD
        };
        lsn + hdr_size as u64
    } else {
        lsn.align()
    }
}

pub fn generate_pg_control(
    pg_control_bytes: &[u8],
    checkpoint_bytes: &[u8],
    lsn: Lsn,
) -> anyhow::Result<(Bytes, u64)> {
    let mut pg_control = ControlFileData::decode(pg_control_bytes)?;
    let mut checkpoint = CheckPoint::decode(checkpoint_bytes)?;

    // Generate new pg_control needed for bootstrap
    checkpoint.redo = normalize_lsn(lsn, WAL_SEGMENT_SIZE).0;

    //save new values in pg_control
    pg_control.checkPoint = 0;
    pg_control.checkPointCopy = checkpoint;
    pg_control.state = DBState_DB_SHUTDOWNED;

    Ok((pg_control.encode(), pg_control.system_identifier))
}

pub fn get_current_timestamp() -> TimestampTz {
    to_pg_timestamp(SystemTime::now())
}

// Module to reduce the scope of the constants
mod timestamp_conversions {
    use std::time::Duration;

    use anyhow::Context;

    use super::*;

    const UNIX_EPOCH_JDATE: u64 = 2440588; // == date2j(1970, 1, 1)
    const POSTGRES_EPOCH_JDATE: u64 = 2451545; // == date2j(2000, 1, 1)
    const SECS_PER_DAY: u64 = 86400;
    const USECS_PER_SEC: u64 = 1000000;
    const SECS_DIFF_UNIX_TO_POSTGRES_EPOCH: u64 =
        (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;

    pub fn to_pg_timestamp(time: SystemTime) -> TimestampTz {
        match time.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => {
                ((n.as_secs() - SECS_DIFF_UNIX_TO_POSTGRES_EPOCH) * USECS_PER_SEC
                    + n.subsec_micros() as u64) as i64
            }
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        }
    }

    pub fn try_from_pg_timestamp(time: TimestampTz) -> anyhow::Result<SystemTime> {
        let time: u64 = time
            .try_into()
            .context("timestamp before millenium (postgres epoch)")?;
        let since_unix_epoch = time + SECS_DIFF_UNIX_TO_POSTGRES_EPOCH * USECS_PER_SEC;
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_micros(since_unix_epoch))
            .context("SystemTime overflow")
    }
}

pub use timestamp_conversions::{to_pg_timestamp, try_from_pg_timestamp};

// Returns (aligned) end_lsn of the last record in data_dir with WAL segments.
// start_lsn must point to some previously known record boundary (beginning of
// the next record). If no valid record after is found, start_lsn is returned
// back.
pub fn find_end_of_wal(
    data_dir: &Path,
    wal_seg_size: usize,
    start_lsn: Lsn, // start reading WAL at this point; must point at record start_lsn.
) -> anyhow::Result<Lsn> {
    let mut result = start_lsn;
    let mut curr_lsn = start_lsn;
    let mut buf = [0u8; XLOG_BLCKSZ];
    let pg_version = PG_MAJORVERSION[1..3].parse::<u32>().unwrap();
    debug!("find_end_of_wal PG_VERSION: {}", pg_version);

    let mut decoder = WalStreamDecoder::new(start_lsn, pg_version);

    // loop over segments
    loop {
        let segno = curr_lsn.segment_number(wal_seg_size);
        let seg_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
        let seg_file_path = data_dir.join(seg_file_name);
        match open_wal_segment(&seg_file_path)? {
            None => {
                // no more segments
                debug!(
                    "find_end_of_wal reached end at {:?}, segment {:?} doesn't exist",
                    result, seg_file_path
                );
                return Ok(result);
            }
            Some(mut segment) => {
                let seg_offs = curr_lsn.segment_offset(wal_seg_size);
                segment.seek(SeekFrom::Start(seg_offs as u64))?;
                // loop inside segment
                while curr_lsn.segment_number(wal_seg_size) == segno {
                    let bytes_read = segment.read(&mut buf)?;
                    if bytes_read == 0 {
                        debug!(
                            "find_end_of_wal reached end at {:?}, EOF in segment {:?} at offset {}",
                            result,
                            seg_file_path,
                            curr_lsn.segment_offset(wal_seg_size)
                        );
                        return Ok(result);
                    }
                    curr_lsn += bytes_read as u64;
                    decoder.feed_bytes(&buf[0..bytes_read]);

                    // advance result past all completely read records
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
    }
}

// Open .partial or full WAL segment file, if present.
fn open_wal_segment(seg_file_path: &Path) -> anyhow::Result<Option<File>> {
    let mut partial_path = seg_file_path.to_owned();
    partial_path.set_extension("partial");
    match File::open(partial_path) {
        Ok(file) => Ok(Some(file)),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                // .partial not found, try full
                match File::open(seg_file_path) {
                    Ok(file) => Ok(Some(file)),
                    Err(e) => match e.kind() {
                        ErrorKind::NotFound => Ok(None),
                        _ => Err(e.into()),
                    },
                }
            }
            _ => Err(e.into()),
        },
    }
}

impl XLogRecord {
    pub fn from_slice(buf: &[u8]) -> Result<XLogRecord, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogRecord::des(buf)
    }

    pub fn from_bytes<B: Buf>(buf: &mut B) -> Result<XLogRecord, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogRecord::des_from(&mut buf.reader())
    }

    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        Ok(self.ser()?.into())
    }

    // Is this record an XLOG_SWITCH record? They need some special processing,
    pub fn is_xlog_switch_record(&self) -> bool {
        self.xl_info == pg_constants::XLOG_SWITCH && self.xl_rmid == pg_constants::RM_XLOG_ID
    }
}

impl XLogPageHeaderData {
    pub fn from_bytes<B: Buf>(buf: &mut B) -> Result<XLogPageHeaderData, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogPageHeaderData::des_from(&mut buf.reader())
    }

    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        self.ser().map(|b| b.into())
    }
}

impl XLogLongPageHeaderData {
    pub fn from_bytes<B: Buf>(buf: &mut B) -> Result<XLogLongPageHeaderData, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogLongPageHeaderData::des_from(&mut buf.reader())
    }

    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        self.ser().map(|b| b.into())
    }
}

pub const SIZEOF_CHECKPOINT: usize = size_of::<CheckPoint>();

impl CheckPoint {
    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        Ok(self.ser()?.into())
    }

    pub fn decode(buf: &[u8]) -> Result<CheckPoint, DeserializeError> {
        use utils::bin_ser::LeSer;
        CheckPoint::des(buf)
    }

    /// Update next XID based on provided new_xid and stored epoch.
    /// Next XID should be greater than new_xid. This handles 32-bit
    /// XID wraparound correctly.
    ///
    /// Returns 'true' if the XID was updated.
    pub fn update_next_xid(&mut self, xid: u32) -> bool {
        // nextXid should be greater than any XID in WAL, so increment provided XID and check for wraparround.
        let mut new_xid = std::cmp::max(
            xid.wrapping_add(1),
            pg_constants::FIRST_NORMAL_TRANSACTION_ID,
        );
        // To reduce number of metadata checkpoints, we forward align XID on XID_CHECKPOINT_INTERVAL.
        // XID_CHECKPOINT_INTERVAL should not be larger than BLCKSZ*CLOG_XACTS_PER_BYTE
        new_xid =
            new_xid.wrapping_add(XID_CHECKPOINT_INTERVAL - 1) & !(XID_CHECKPOINT_INTERVAL - 1);
        let full_xid = self.nextXid.value;
        let old_xid = full_xid as u32;
        if new_xid.wrapping_sub(old_xid) as i32 > 0 {
            let mut epoch = full_xid >> 32;
            if new_xid < old_xid {
                // wrap-around
                epoch += 1;
            }
            let nextXid = (epoch << 32) | new_xid as u64;

            if nextXid != self.nextXid.value {
                self.nextXid = FullTransactionId { value: nextXid };
                return true;
            }
        }
        false
    }

    /// Advance next multi-XID/offset to those given in arguments.
    ///
    /// It's important that this handles wraparound correctly. This should match the
    /// MultiXactAdvanceNextMXact() logic in PostgreSQL's xlog_redo() function.
    ///
    /// Returns 'true' if the Checkpoint was updated.
    pub fn update_next_multixid(&mut self, multi_xid: u32, multi_offset: u32) -> bool {
        let mut modified = false;

        if multi_xid.wrapping_sub(self.nextMulti) as i32 > 0 {
            self.nextMulti = multi_xid;
            modified = true;
        }

        if multi_offset.wrapping_sub(self.nextMultiOffset) as i32 > 0 {
            self.nextMultiOffset = multi_offset;
            modified = true;
        }

        modified
    }
}

/// Generate new, empty WAL segment, with correct block headers at the first
/// page of the segment and the page that contains the given LSN.
/// We need this segment to start compute node.
pub fn generate_wal_segment(segno: u64, system_id: u64, lsn: Lsn) -> Result<Bytes, SerializeError> {
    let mut seg_buf = BytesMut::with_capacity(WAL_SEGMENT_SIZE);

    let pageaddr = XLogSegNoOffsetToRecPtr(segno, 0, WAL_SEGMENT_SIZE);

    let page_off = lsn.block_offset();
    let seg_off = lsn.segment_offset(WAL_SEGMENT_SIZE);

    let first_page_only = seg_off < XLOG_BLCKSZ;
    // If first records starts in the middle of the page, pretend in page header
    // there is a fake record which ends where first real record starts. This
    // makes pg_waldump etc happy.
    let (shdr_rem_len, infoflags) = if first_page_only && seg_off > 0 {
        assert!(seg_off >= XLOG_SIZE_OF_XLOG_LONG_PHD);
        // xlp_rem_len doesn't include page header, hence the subtraction.
        (
            seg_off - XLOG_SIZE_OF_XLOG_LONG_PHD,
            pg_constants::XLP_FIRST_IS_CONTRECORD,
        )
    } else {
        (0, 0)
    };

    let hdr = XLogLongPageHeaderData {
        std: {
            XLogPageHeaderData {
                xlp_magic: XLOG_PAGE_MAGIC as u16,
                xlp_info: pg_constants::XLP_LONG_HEADER | infoflags,
                xlp_tli: PG_TLI,
                xlp_pageaddr: pageaddr,
                xlp_rem_len: shdr_rem_len as u32,
                ..Default::default() // Put 0 in padding fields.
            }
        },
        xlp_sysid: system_id,
        xlp_seg_size: WAL_SEGMENT_SIZE as u32,
        xlp_xlog_blcksz: XLOG_BLCKSZ as u32,
    };

    let hdr_bytes = hdr.encode()?;
    seg_buf.extend_from_slice(&hdr_bytes);

    //zero out the rest of the file
    seg_buf.resize(WAL_SEGMENT_SIZE, 0);

    if !first_page_only {
        let block_offset = lsn.page_offset_in_segment(WAL_SEGMENT_SIZE) as usize;
        // see comments above about XLP_FIRST_IS_CONTRECORD and xlp_rem_len.
        let (xlp_rem_len, xlp_info) = if page_off > 0 {
            assert!(page_off >= XLOG_SIZE_OF_XLOG_SHORT_PHD as u64);
            (
                (page_off - XLOG_SIZE_OF_XLOG_SHORT_PHD as u64) as u32,
                pg_constants::XLP_FIRST_IS_CONTRECORD,
            )
        } else {
            (0, 0)
        };
        let header = XLogPageHeaderData {
            xlp_magic: XLOG_PAGE_MAGIC as u16,
            xlp_info,
            xlp_tli: PG_TLI,
            xlp_pageaddr: lsn.page_lsn().0,
            xlp_rem_len,
            ..Default::default() // Put 0 in padding fields.
        };
        let hdr_bytes = header.encode()?;

        debug_assert!(seg_buf.len() > block_offset + hdr_bytes.len());
        debug_assert_ne!(block_offset, 0);

        seg_buf[block_offset..block_offset + hdr_bytes.len()].copy_from_slice(&hdr_bytes[..]);
    }

    Ok(seg_buf.freeze())
}

#[repr(C)]
#[derive(Serialize)]
pub struct XlLogicalMessage {
    pub db_id: Oid,
    pub transactional: uint32, // bool, takes 4 bytes due to alignment in C structures
    pub prefix_size: uint64,
    pub message_size: uint64,
}

impl XlLogicalMessage {
    pub fn encode(&self) -> Bytes {
        use utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }
}

/// Create new WAL record for non-transactional logical message.
/// Used for creating artificial WAL for tests, as LogicalMessage
/// record is basically no-op.
pub fn encode_logical_message(prefix: &str, message: &str) -> Bytes {
    // This function can take untrusted input, so discard any NUL bytes in the prefix string.
    let prefix = CString::new(prefix.replace('\0', "")).expect("no NULs");
    let message = message.as_bytes();
    LogicalMessageGenerator::new(&prefix, message)
        .next()
        .unwrap()
        .encode(Lsn(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ts_conversion() {
        let now = SystemTime::now();
        let round_trip = try_from_pg_timestamp(to_pg_timestamp(now)).unwrap();

        let now_since = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let round_trip_since = round_trip.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        assert_eq!(now_since.as_micros(), round_trip_since.as_micros());

        let now_pg = get_current_timestamp();
        let round_trip_pg = to_pg_timestamp(try_from_pg_timestamp(now_pg).unwrap());

        assert_eq!(now_pg, round_trip_pg);
    }

    // If you need to craft WAL and write tests for this module, put it at wal_craft crate.
}
