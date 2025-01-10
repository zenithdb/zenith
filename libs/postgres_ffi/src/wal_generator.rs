use std::ffi::{CStr, CString};

use bytes::{Bytes, BytesMut};
use crc32c::crc32c_append;
use utils::lsn::Lsn;

use super::bindings::{RmgrId, XLogLongPageHeaderData, XLogPageHeaderData, XLOG_PAGE_MAGIC};
use super::xlog_utils::{
    XlLogicalMessage, XLOG_RECORD_CRC_OFFS, XLOG_SIZE_OF_XLOG_RECORD, XLP_BKP_REMOVABLE,
    XLP_FIRST_IS_CONTRECORD,
};
use super::XLogRecord;
use crate::pg_constants::{
    RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, XLP_LONG_HEADER, XLR_BLOCK_ID_DATA_LONG,
    XLR_BLOCK_ID_DATA_SHORT,
};
use crate::{WAL_SEGMENT_SIZE, XLOG_BLCKSZ};

/// A WAL record payload. Will be prefixed by an XLogRecord header when encoded.
pub struct Record {
    pub rmid: RmgrId,
    pub info: u8,
    pub data: Bytes,
}

impl Record {
    /// Encodes the WAL record including an XLogRecord header. prev_lsn is the start position of
    /// the previous record in the WAL -- this is ignored by the Safekeeper, but not Postgres.
    pub fn encode(&self, prev_lsn: Lsn) -> Bytes {
        // Prefix data with block ID and length.
        let data_header = Bytes::from(match self.data.len() {
            0 => vec![],
            1..=255 => vec![XLR_BLOCK_ID_DATA_SHORT, self.data.len() as u8],
            256.. => {
                let len_bytes = (self.data.len() as u32).to_le_bytes();
                [&[XLR_BLOCK_ID_DATA_LONG], len_bytes.as_slice()].concat()
            }
        });

        // Construct the WAL record header.
        let mut header = XLogRecord {
            xl_tot_len: (XLOG_SIZE_OF_XLOG_RECORD + data_header.len() + self.data.len()) as u32,
            xl_xid: 0,
            xl_prev: prev_lsn.into(),
            xl_info: self.info,
            xl_rmid: self.rmid,
            __bindgen_padding_0: [0; 2],
            xl_crc: 0, // see below
        };

        // Compute the CRC checksum for the data, and the header up to the CRC field.
        let mut crc = 0;
        crc = crc32c_append(crc, &data_header);
        crc = crc32c_append(crc, &self.data);
        crc = crc32c_append(crc, &header.encode().unwrap()[0..XLOG_RECORD_CRC_OFFS]);
        header.xl_crc = crc;

        // Encode the final header and record.
        let header = header.encode().unwrap();

        [header, data_header, self.data.clone()].concat().into()
    }
}

/// Generates WAL record payloads.
///
/// TODO: currently only provides LogicalMessageGenerator for trivial noop messages. Add a generator
/// that creates a table and inserts rows.
pub trait RecordGenerator: Iterator<Item = Record> {}

impl<I: Iterator<Item = Record>> RecordGenerator for I {}

/// Generates binary WAL for use in tests and benchmarks. The provided record generator constructs
/// the WAL records. It is used as an iterator which yields encoded bytes for a single WAL record,
/// including internal page headers if it spans pages. Concatenating the bytes will yield a
/// complete, well-formed WAL, which can be chunked at segment boundaries if desired. Not optimized
/// for performance.
///
/// The WAL format is version-dependant (see e.g. `XLOG_PAGE_MAGIC`), so make sure to import this
/// for the appropriate Postgres version (e.g. `postgres_ffi::v17::wal_generator::WalGenerator`).
///
/// A WAL is split into 16 MB segments. Each segment is split into 8 KB pages, with headers.
/// Records are arbitrary length, 8-byte aligned, and may span pages. The layout is e.g.:
///
/// |        Segment 1         |        Segment 2         |        Segment 3         |
/// | Page 1 | Page 2 | Page 3 | Page 4 | Page 5 | Page 6 | Page 7 | Page 8 | Page 9 |
/// | R1 |   R2  |R3|  R4  | R5  |  R6  |                 R7            | R8  |
#[derive(Default)]
pub struct WalGenerator<R: RecordGenerator> {
    /// Generates record payloads for the WAL.
    pub record_generator: R,
    /// Current LSN to append the next record at.
    ///
    /// Callers can modify this (and prev_lsn) to restart generation at a different LSN, but should
    /// ensure that the LSN is on a valid record boundary (i.e. we can't start appending in the
    /// middle on an existing record or header, or beyond the end of the existing WAL).
    pub lsn: Lsn,
    /// The starting LSN of the previous record. Used in WAL record headers. The Safekeeper doesn't
    /// care about this, unlike Postgres, but we include it for completeness.
    pub prev_lsn: Lsn,
}

impl<R: RecordGenerator> WalGenerator<R> {
    // Hardcode the sys and timeline ID. We can make them configurable if we care about them.
    const SYS_ID: u64 = 0;
    const TIMELINE_ID: u32 = 1;

    /// Creates a new WAL generator with the given record generator.
    pub fn new(record_generator: R, start_lsn: Lsn) -> WalGenerator<R> {
        Self {
            record_generator,
            lsn: start_lsn,
            prev_lsn: start_lsn,
        }
    }

    /// Appends a record with an arbitrary payload at the current LSN, then increments the LSN.
    /// Returns the WAL bytes for the record, including page headers and padding, and the start LSN.
    fn append_record(&mut self, record: Record) -> (Lsn, Bytes) {
        let record = record.encode(self.prev_lsn);
        let record = Self::insert_pages(record, self.lsn);
        let record = Self::pad_record(record, self.lsn);
        let lsn = self.lsn;
        self.prev_lsn = self.lsn;
        self.lsn += record.len() as u64;
        (lsn, record)
    }

    /// Inserts page headers on 8KB page boundaries. Takes the current LSN position where the record
    /// is to be appended.
    fn insert_pages(record: Bytes, mut lsn: Lsn) -> Bytes {
        // Fast path: record fits in current page, and the page already has a header.
        if lsn.remaining_in_block() as usize >= record.len() && lsn.block_offset() > 0 {
            return record;
        }

        let mut pages = BytesMut::new();
        let mut remaining = record.clone(); // Bytes::clone() is cheap
        while !remaining.is_empty() {
            // At new page boundary, inject page header.
            if lsn.block_offset() == 0 {
                let mut page_header = XLogPageHeaderData {
                    xlp_magic: XLOG_PAGE_MAGIC as u16,
                    xlp_info: XLP_BKP_REMOVABLE,
                    xlp_tli: Self::TIMELINE_ID,
                    xlp_pageaddr: lsn.0,
                    xlp_rem_len: 0,
                    __bindgen_padding_0: [0; 4],
                };
                // If the record was split across page boundaries, mark as continuation.
                if remaining.len() < record.len() {
                    page_header.xlp_rem_len = remaining.len() as u32;
                    page_header.xlp_info |= XLP_FIRST_IS_CONTRECORD;
                }
                // At start of segment, use a long page header.
                let page_header = if lsn.segment_offset(WAL_SEGMENT_SIZE) == 0 {
                    page_header.xlp_info |= XLP_LONG_HEADER;
                    XLogLongPageHeaderData {
                        std: page_header,
                        xlp_sysid: Self::SYS_ID,
                        xlp_seg_size: WAL_SEGMENT_SIZE as u32,
                        xlp_xlog_blcksz: XLOG_BLCKSZ as u32,
                    }
                    .encode()
                    .unwrap()
                } else {
                    page_header.encode().unwrap()
                };
                pages.extend_from_slice(&page_header);
                lsn += page_header.len() as u64;
            }

            // Append the record up to the next page boundary, if any.
            let page_free = lsn.remaining_in_block() as usize;
            let chunk = remaining.split_to(std::cmp::min(page_free, remaining.len()));
            pages.extend_from_slice(&chunk);
            lsn += chunk.len() as u64;
        }
        pages.freeze()
    }

    /// Records must be 8-byte aligned. Take an encoded record (including any injected page
    /// boundaries), starting at the given LSN, and add any necessary padding at the end.
    fn pad_record(record: Bytes, mut lsn: Lsn) -> Bytes {
        lsn += record.len() as u64;
        let padding = lsn.calc_padding(8u64) as usize;
        if padding == 0 {
            return record;
        }
        [record, Bytes::from(vec![0; padding])].concat().into()
    }
}

/// Generates WAL records as an iterator.
impl<R: RecordGenerator> Iterator for WalGenerator<R> {
    type Item = (Lsn, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let record = self.record_generator.next()?;
        Some(self.append_record(record))
    }
}

/// Generates logical message records (effectively noops) with a fixed message.
pub struct LogicalMessageGenerator {
    prefix: CString,
    message: Vec<u8>,
}

impl LogicalMessageGenerator {
    const DB_ID: u32 = 0; // hardcoded for now
    const RM_ID: RmgrId = RM_LOGICALMSG_ID;
    const INFO: u8 = XLOG_LOGICAL_MESSAGE;

    /// Creates a new LogicalMessageGenerator.
    pub fn new(prefix: &CStr, message: &[u8]) -> Self {
        Self {
            prefix: prefix.to_owned(),
            message: message.to_owned(),
        }
    }

    /// Encodes a logical message.
    fn encode(prefix: &CStr, message: &[u8]) -> Bytes {
        let prefix = prefix.to_bytes_with_nul();
        let header = XlLogicalMessage {
            db_id: Self::DB_ID,
            transactional: 0,
            prefix_size: prefix.len() as u64,
            message_size: message.len() as u64,
        };
        [&header.encode(), prefix, message].concat().into()
    }

    /// Computes how large a value must be to get a record of the given size. Convenience method to
    /// construct records of pre-determined size. Panics if the record size is too small.
    pub fn make_value_size(record_size: usize, prefix: &CStr) -> usize {
        let xlog_header_size = XLOG_SIZE_OF_XLOG_RECORD;
        let lm_header_size = size_of::<XlLogicalMessage>();
        let prefix_size = prefix.to_bytes_with_nul().len();
        let data_header_size = match record_size - xlog_header_size - 2 {
            0..=255 => 2,
            256..=258 => panic!("impossible record_size {record_size}"),
            259.. => 5,
        };
        record_size
            .checked_sub(xlog_header_size + lm_header_size + prefix_size + data_header_size)
            .expect("record_size too small")
    }
}

impl Iterator for LogicalMessageGenerator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Record {
            rmid: Self::RM_ID,
            info: Self::INFO,
            data: Self::encode(&self.prefix, &self.message),
        })
    }
}

impl WalGenerator<LogicalMessageGenerator> {
    /// Convenience method for appending a WAL record with an arbitrary logical message at the
    /// current WAL LSN position. Returns the start LSN and resulting WAL bytes.
    pub fn append_logical_message(&mut self, prefix: &CStr, message: &[u8]) -> (Lsn, Bytes) {
        let record = Record {
            rmid: LogicalMessageGenerator::RM_ID,
            info: LogicalMessageGenerator::INFO,
            data: LogicalMessageGenerator::encode(prefix, message),
        };
        self.append_record(record)
    }
}
