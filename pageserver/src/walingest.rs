//!
//! Parse PostgreSQL WAL records and store them in a neon Timeline.
//!
//! The pipeline for ingesting WAL looks like this:
//!
//! WAL receiver  ->   WalIngest  ->   Repository
//!
//! The WAL receiver receives a stream of WAL from the WAL safekeepers,
//! and decodes it to individual WAL records. It feeds the WAL records
//! to WalIngest, which parses them and stores them in the Repository.
//!
//! The neon Repository can store page versions in two formats: as
//! page images, or a WAL records. WalIngest::ingest_record() extracts
//! page images out of some WAL records, but most it stores as WAL
//! records. If a WAL record modifies multiple pages, WalIngest
//! will call Repository::put_wal_record or put_page_image functions
//! separately for each modified page.
//!
//! To reconstruct a page using a WAL record, the Repository calls the
//! code in walredo.rs. walredo.rs passes most WAL records to the WAL
//! redo Postgres process, but some records it can handle directly with
//! bespoken Rust code.

use pageserver_api::shard::ShardIdentity;
use postgres_ffi::v14::nonrelfile_utils::clogpage_precedes;
use postgres_ffi::v14::nonrelfile_utils::slru_may_delete_clogsegment;
use postgres_ffi::{fsm_logical_to_physical, page_is_new, page_set_lsn};

use anyhow::{bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use tracing::*;
use utils::failpoint_support;

use crate::context::RequestContext;
use crate::metrics::WAL_INGEST;
use crate::pgdatadir_mapping::{DatadirModification, Version};
use crate::tenant::PageReconstructError;
use crate::tenant::Timeline;
use crate::walrecord::*;
use crate::ZERO_PAGE;
use pageserver_api::key::rel_block_to_key;
use pageserver_api::reltag::{BlockNumber, RelTag, SlruKind};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::{FSM_FORKNUM, INIT_FORKNUM, MAIN_FORKNUM, VISIBILITYMAP_FORKNUM};
use postgres_ffi::v14::nonrelfile_utils::mx_offset_to_member_segment;
use postgres_ffi::v14::xlog_utils::*;
use postgres_ffi::v14::CheckPoint;
use postgres_ffi::TransactionId;
use postgres_ffi::BLCKSZ;
use utils::lsn::Lsn;

pub struct WalIngest {
    shard: ShardIdentity,
    checkpoint: CheckPoint,
    checkpoint_modified: bool,
}

impl WalIngest {
    pub async fn new(
        timeline: &Timeline,
        startpoint: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<WalIngest> {
        // Fetch the latest checkpoint into memory, so that we can compare with it
        // quickly in `ingest_record` and update it when it changes.
        let checkpoint_bytes = timeline.get_checkpoint(startpoint, ctx).await?;
        let checkpoint = CheckPoint::decode(&checkpoint_bytes)?;
        trace!("CheckPoint.nextXid = {}", checkpoint.nextXid.value);

        Ok(WalIngest {
            shard: *timeline.get_shard_identity(),
            checkpoint,
            checkpoint_modified: false,
        })
    }

    ///
    /// Decode a PostgreSQL WAL record and store it in the repository, in the given timeline.
    ///
    /// This function updates `lsn` field of `DatadirModification`
    ///
    /// Helper function to parse a WAL record and call the Timeline's PUT functions for all the
    /// relations/pages that the record affects.
    ///
    /// This function returns `true` if the record was ingested, and `false` if it was filtered out
    ///
    pub async fn ingest_record(
        &mut self,
        recdata: Bytes,
        lsn: Lsn,
        modification: &mut DatadirModification<'_>,
        decoded: &mut DecodedWALRecord,
        ctx: &RequestContext,
    ) -> anyhow::Result<bool> {
        WAL_INGEST.records_received.inc();
        let pg_version = modification.tline.pg_version;
        let prev_len = modification.len();

        modification.set_lsn(lsn)?;
        decode_wal_record(recdata, decoded, pg_version)?;

        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);

        assert!(!self.checkpoint_modified);
        if decoded.xl_xid != pg_constants::INVALID_TRANSACTION_ID
            && self.checkpoint.update_next_xid(decoded.xl_xid)
        {
            self.checkpoint_modified = true;
        }

        failpoint_support::sleep_millis_async!("wal-ingest-record-sleep");

        match decoded.xl_rmid {
            pg_constants::RM_HEAP_ID | pg_constants::RM_HEAP2_ID => {
                // Heap AM records need some special handling, because they modify VM pages
                // without registering them with the standard mechanism.
                self.ingest_heapam_record(&mut buf, modification, decoded, ctx)
                    .await?;
            }
            pg_constants::RM_NEON_ID => {
                self.ingest_neonrmgr_record(&mut buf, modification, decoded, ctx)
                    .await?;
            }
            // Handle other special record types
            pg_constants::RM_SMGR_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

                if info == pg_constants::XLOG_SMGR_CREATE {
                    let create = XlSmgrCreate::decode(&mut buf);
                    self.ingest_xlog_smgr_create(modification, &create, ctx)
                        .await?;
                } else if info == pg_constants::XLOG_SMGR_TRUNCATE {
                    let truncate = XlSmgrTruncate::decode(&mut buf);
                    self.ingest_xlog_smgr_truncate(modification, &truncate, ctx)
                        .await?;
                }
            }
            pg_constants::RM_DBASE_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
                debug!(%info, %pg_version, "handle RM_DBASE_ID");

                if pg_version == 14 {
                    if info == postgres_ffi::v14::bindings::XLOG_DBASE_CREATE {
                        let createdb = XlCreateDatabase::decode(&mut buf);
                        debug!("XLOG_DBASE_CREATE v14");

                        self.ingest_xlog_dbase_create(modification, &createdb, ctx)
                            .await?;
                    } else if info == postgres_ffi::v14::bindings::XLOG_DBASE_DROP {
                        let dropdb = XlDropDatabase::decode(&mut buf);
                        for tablespace_id in dropdb.tablespace_ids {
                            trace!("Drop db {}, {}", tablespace_id, dropdb.db_id);
                            modification
                                .drop_dbdir(tablespace_id, dropdb.db_id, ctx)
                                .await?;
                        }
                    }
                } else if pg_version == 15 {
                    if info == postgres_ffi::v15::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                        debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
                    } else if info == postgres_ffi::v15::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                        // The XLOG record was renamed between v14 and v15,
                        // but the record format is the same.
                        // So we can reuse XlCreateDatabase here.
                        debug!("XLOG_DBASE_CREATE_FILE_COPY");
                        let createdb = XlCreateDatabase::decode(&mut buf);
                        self.ingest_xlog_dbase_create(modification, &createdb, ctx)
                            .await?;
                    } else if info == postgres_ffi::v15::bindings::XLOG_DBASE_DROP {
                        let dropdb = XlDropDatabase::decode(&mut buf);
                        for tablespace_id in dropdb.tablespace_ids {
                            trace!("Drop db {}, {}", tablespace_id, dropdb.db_id);
                            modification
                                .drop_dbdir(tablespace_id, dropdb.db_id, ctx)
                                .await?;
                        }
                    }
                } else if pg_version == 16 {
                    if info == postgres_ffi::v16::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                        debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
                    } else if info == postgres_ffi::v16::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                        // The XLOG record was renamed between v14 and v15,
                        // but the record format is the same.
                        // So we can reuse XlCreateDatabase here.
                        debug!("XLOG_DBASE_CREATE_FILE_COPY");
                        let createdb = XlCreateDatabase::decode(&mut buf);
                        self.ingest_xlog_dbase_create(modification, &createdb, ctx)
                            .await?;
                    } else if info == postgres_ffi::v16::bindings::XLOG_DBASE_DROP {
                        let dropdb = XlDropDatabase::decode(&mut buf);
                        for tablespace_id in dropdb.tablespace_ids {
                            trace!("Drop db {}, {}", tablespace_id, dropdb.db_id);
                            modification
                                .drop_dbdir(tablespace_id, dropdb.db_id, ctx)
                                .await?;
                        }
                    }
                }
            }
            pg_constants::RM_TBLSPC_ID => {
                trace!("XLOG_TBLSPC_CREATE/DROP is not handled yet");
            }
            pg_constants::RM_CLOG_ID => {
                let info = decoded.xl_info & !pg_constants::XLR_INFO_MASK;

                if info == pg_constants::CLOG_ZEROPAGE {
                    let pageno = buf.get_u32_le();
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    self.put_slru_page_image(
                        modification,
                        SlruKind::Clog,
                        segno,
                        rpageno,
                        ZERO_PAGE.clone(),
                        ctx,
                    )
                    .await?;
                } else {
                    assert!(info == pg_constants::CLOG_TRUNCATE);
                    let xlrec = XlClogTruncate::decode(&mut buf);
                    self.ingest_clog_truncate_record(modification, &xlrec, ctx)
                        .await?;
                }
            }
            pg_constants::RM_XACT_ID => {
                let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;

                if info == pg_constants::XLOG_XACT_COMMIT || info == pg_constants::XLOG_XACT_ABORT {
                    let parsed_xact =
                        XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
                    self.ingest_xact_record(
                        modification,
                        &parsed_xact,
                        info == pg_constants::XLOG_XACT_COMMIT,
                        decoded.origin_id,
                        lsn,
                        ctx,
                    )
                    .await?;
                } else if info == pg_constants::XLOG_XACT_COMMIT_PREPARED
                    || info == pg_constants::XLOG_XACT_ABORT_PREPARED
                {
                    let parsed_xact =
                        XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
                    self.ingest_xact_record(
                        modification,
                        &parsed_xact,
                        info == pg_constants::XLOG_XACT_COMMIT_PREPARED,
                        decoded.origin_id,
                        lsn,
                        ctx,
                    )
                    .await?;
                    // Remove twophase file. see RemoveTwoPhaseFile() in postgres code
                    trace!(
                        "Drop twophaseFile for xid {} parsed_xact.xid {} here at {}",
                        decoded.xl_xid,
                        parsed_xact.xid,
                        lsn,
                    );
                    modification
                        .drop_twophase_file(parsed_xact.xid, ctx)
                        .await?;
                } else if info == pg_constants::XLOG_XACT_PREPARE {
                    modification
                        .put_twophase_file(decoded.xl_xid, Bytes::copy_from_slice(&buf[..]), ctx)
                        .await?;
                }
            }
            pg_constants::RM_MULTIXACT_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

                if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
                    let pageno = buf.get_u32_le();
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    self.put_slru_page_image(
                        modification,
                        SlruKind::MultiXactOffsets,
                        segno,
                        rpageno,
                        ZERO_PAGE.clone(),
                        ctx,
                    )
                    .await?;
                } else if info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE {
                    let pageno = buf.get_u32_le();
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    self.put_slru_page_image(
                        modification,
                        SlruKind::MultiXactMembers,
                        segno,
                        rpageno,
                        ZERO_PAGE.clone(),
                        ctx,
                    )
                    .await?;
                } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
                    let xlrec = XlMultiXactCreate::decode(&mut buf);
                    self.ingest_multixact_create_record(modification, &xlrec)?;
                } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
                    let xlrec = XlMultiXactTruncate::decode(&mut buf);
                    self.ingest_multixact_truncate_record(modification, &xlrec, ctx)
                        .await?;
                }
            }
            pg_constants::RM_RELMAP_ID => {
                let xlrec = XlRelmapUpdate::decode(&mut buf);
                self.ingest_relmap_page(modification, &xlrec, decoded, ctx)
                    .await?;
            }
            pg_constants::RM_XLOG_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

                if info == pg_constants::XLOG_NEXTOID {
                    let next_oid = buf.get_u32_le();
                    if self.checkpoint.nextOid != next_oid {
                        self.checkpoint.nextOid = next_oid;
                        self.checkpoint_modified = true;
                    }
                } else if info == pg_constants::XLOG_CHECKPOINT_ONLINE
                    || info == pg_constants::XLOG_CHECKPOINT_SHUTDOWN
                {
                    let mut checkpoint_bytes = [0u8; SIZEOF_CHECKPOINT];
                    buf.copy_to_slice(&mut checkpoint_bytes);
                    let xlog_checkpoint = CheckPoint::decode(&checkpoint_bytes)?;
                    trace!(
                        "xlog_checkpoint.oldestXid={}, checkpoint.oldestXid={}",
                        xlog_checkpoint.oldestXid,
                        self.checkpoint.oldestXid
                    );
                    if (self
                        .checkpoint
                        .oldestXid
                        .wrapping_sub(xlog_checkpoint.oldestXid) as i32)
                        < 0
                    {
                        self.checkpoint.oldestXid = xlog_checkpoint.oldestXid;
                    }
                    trace!(
                        "xlog_checkpoint.oldestActiveXid={}, checkpoint.oldestActiveXid={}",
                        xlog_checkpoint.oldestActiveXid,
                        self.checkpoint.oldestActiveXid
                    );

                    // A shutdown checkpoint has `oldestActiveXid == InvalidTransactionid`,
                    // because at shutdown, all in-progress transactions will implicitly
                    // end. Postgres startup code knows that, and allows hot standby to start
                    // immediately from a shutdown checkpoint.
                    //
                    // In Neon, Postgres hot standby startup always behaves as if starting from
                    // an online checkpoint. It needs a valid `oldestActiveXid` value, so
                    // instead of overwriting self.checkpoint.oldestActiveXid with
                    // InvalidTransactionid from the checkpoint WAL record, update it to a
                    // proper value, knowing that there are no in-progress transactions at this
                    // point, except for prepared transactions.
                    //
                    // See also the neon code changes in the InitWalRecovery() function.
                    if xlog_checkpoint.oldestActiveXid == pg_constants::INVALID_TRANSACTION_ID
                        && info == pg_constants::XLOG_CHECKPOINT_SHUTDOWN
                    {
                        let mut oldest_active_xid = self.checkpoint.nextXid.value as u32;
                        for xid in modification.tline.list_twophase_files(lsn, ctx).await? {
                            if (xid.wrapping_sub(oldest_active_xid) as i32) < 0 {
                                oldest_active_xid = xid;
                            }
                        }
                        self.checkpoint.oldestActiveXid = oldest_active_xid;
                    } else {
                        self.checkpoint.oldestActiveXid = xlog_checkpoint.oldestActiveXid;
                    }

                    // Write a new checkpoint key-value pair on every checkpoint record, even
                    // if nothing really changed. Not strictly required, but it seems nice to
                    // have some trace of the checkpoint records in the layer files at the same
                    // LSNs.
                    self.checkpoint_modified = true;
                }
            }
            pg_constants::RM_LOGICALMSG_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

                if info == pg_constants::XLOG_LOGICAL_MESSAGE {
                    let xlrec = crate::walrecord::XlLogicalMessage::decode(&mut buf);
                    let prefix = std::str::from_utf8(&buf[0..xlrec.prefix_size - 1])?;
                    let message = &buf[xlrec.prefix_size..xlrec.prefix_size + xlrec.message_size];
                    if prefix == "neon-test" {
                        // This is a convenient way to make the WAL ingestion pause at
                        // particular point in the WAL. For more fine-grained control,
                        // we could peek into the message and only pause if it contains
                        // a particular string, for example, but this is enough for now.
                        failpoint_support::sleep_millis_async!("wal-ingest-logical-message-sleep");
                    } else if let Some(path) = prefix.strip_prefix("neon-file:") {
                        modification.put_file(path, message, ctx).await?;
                    }
                }
            }
            pg_constants::RM_STANDBY_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
                if info == pg_constants::XLOG_RUNNING_XACTS {
                    let xlrec = crate::walrecord::XlRunningXacts::decode(&mut buf);
                    self.checkpoint.oldestActiveXid = xlrec.oldest_running_xid;
                    self.checkpoint_modified = true;
                }
            }
            pg_constants::RM_REPLORIGIN_ID => {
                let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
                if info == pg_constants::XLOG_REPLORIGIN_SET {
                    let xlrec = crate::walrecord::XlReploriginSet::decode(&mut buf);
                    info!(
                        "Set replication origin_id={}, origin_lsn={}",
                        xlrec.node_id, xlrec.remote_lsn
                    );
                    modification
                        .set_replorigin(xlrec.node_id, xlrec.remote_lsn)
                        .await?
                } else if info == pg_constants::XLOG_REPLORIGIN_DROP {
                    let xlrec = crate::walrecord::XlReploriginDrop::decode(&mut buf);
                    info!("Drop replication origin_id={}", xlrec.node_id);
                    modification.drop_replorigin(xlrec.node_id).await?
                }
            }
            _x => {
                // TODO: should probably log & fail here instead of blindly
                // doing something without understanding the protocol
            }
        }

        // Iterate through all the blocks that the record modifies, and
        // "put" a separate copy of the record for each block.
        for blk in decoded.blocks.iter() {
            let rel = RelTag {
                spcnode: blk.rnode_spcnode,
                dbnode: blk.rnode_dbnode,
                relnode: blk.rnode_relnode,
                forknum: blk.forknum,
            };

            let key = rel_block_to_key(rel, blk.blkno);
            let key_is_local = self.shard.is_key_local(&key);

            tracing::debug!(
                lsn=%lsn,
                key=%key,
                "ingest: shard decision {} (checkpoint={})",
                if !key_is_local { "drop" } else { "keep" },
                self.checkpoint_modified
            );

            if !key_is_local {
                if self.shard.is_shard_zero() {
                    // Shard 0 tracks relation sizes.  Although we will not store this block, we will observe
                    // its blkno in case it implicitly extends a relation.
                    self.observe_decoded_block(modification, blk, ctx).await?;
                }

                continue;
            }
            self.ingest_decoded_block(modification, lsn, decoded, blk, ctx)
                .await?;
        }

        // If checkpoint data was updated, store the new version in the repository
        if self.checkpoint_modified {
            let new_checkpoint_bytes = self.checkpoint.encode()?;

            modification.put_checkpoint(new_checkpoint_bytes)?;
            self.checkpoint_modified = false;
        }

        // Note that at this point this record is only cached in the modification
        // until commit() is called to flush the data into the repository and update
        // the latest LSN.

        Ok(modification.len() > prev_len)
    }

    /// Do not store this block, but observe it for the purposes of updating our relation size state.
    async fn observe_decoded_block(
        &mut self,
        modification: &mut DatadirModification<'_>,
        blk: &DecodedBkpBlock,
        ctx: &RequestContext,
    ) -> Result<(), PageReconstructError> {
        let rel = RelTag {
            spcnode: blk.rnode_spcnode,
            dbnode: blk.rnode_dbnode,
            relnode: blk.rnode_relnode,
            forknum: blk.forknum,
        };
        self.handle_rel_extend(modification, rel, blk.blkno, ctx)
            .await
    }

    async fn ingest_decoded_block(
        &mut self,
        modification: &mut DatadirModification<'_>,
        lsn: Lsn,
        decoded: &DecodedWALRecord,
        blk: &DecodedBkpBlock,
        ctx: &RequestContext,
    ) -> Result<(), PageReconstructError> {
        let rel = RelTag {
            spcnode: blk.rnode_spcnode,
            dbnode: blk.rnode_dbnode,
            relnode: blk.rnode_relnode,
            forknum: blk.forknum,
        };

        //
        // Instead of storing full-page-image WAL record,
        // it is better to store extracted image: we can skip wal-redo
        // in this case. Also some FPI records may contain multiple (up to 32) pages,
        // so them have to be copied multiple times.
        //
        if blk.apply_image
            && blk.has_image
            && decoded.xl_rmid == pg_constants::RM_XLOG_ID
            && (decoded.xl_info == pg_constants::XLOG_FPI
                || decoded.xl_info == pg_constants::XLOG_FPI_FOR_HINT)
            // compression of WAL is not yet supported: fall back to storing the original WAL record
            && !postgres_ffi::bkpimage_is_compressed(blk.bimg_info, modification.tline.pg_version)
            // do not materialize null pages because them most likely be soon replaced with real data
            && blk.bimg_len != 0
        {
            // Extract page image from FPI record
            let img_len = blk.bimg_len as usize;
            let img_offs = blk.bimg_offset as usize;
            let mut image = BytesMut::with_capacity(BLCKSZ as usize);
            image.extend_from_slice(&decoded.record[img_offs..img_offs + img_len]);

            if blk.hole_length != 0 {
                let tail = image.split_off(blk.hole_offset as usize);
                image.resize(image.len() + blk.hole_length as usize, 0u8);
                image.unsplit(tail);
            }
            //
            // Match the logic of XLogReadBufferForRedoExtended:
            // The page may be uninitialized. If so, we can't set the LSN because
            // that would corrupt the page.
            //
            if !page_is_new(&image) {
                page_set_lsn(&mut image, lsn)
            }
            assert_eq!(image.len(), BLCKSZ as usize);
            self.put_rel_page_image(modification, rel, blk.blkno, image.freeze(), ctx)
                .await?;
        } else {
            let rec = NeonWalRecord::Postgres {
                will_init: blk.will_init || blk.apply_image,
                rec: decoded.record.clone(),
            };
            self.put_rel_wal_record(modification, rel, blk.blkno, rec, ctx)
                .await?;
        }
        Ok(())
    }

    async fn ingest_heapam_record(
        &mut self,
        buf: &mut Bytes,
        modification: &mut DatadirModification<'_>,
        decoded: &DecodedWALRecord,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        let mut flags = pg_constants::VISIBILITYMAP_VALID_BITS;

        match modification.tline.pg_version {
            14 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v14::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v14::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v14::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v14::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v14::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v14::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            15 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v15::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v15::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v15::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v15::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v15::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v15::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            16 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v16::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v16::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v16::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v16::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v16::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v16::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            _ => {}
        }

        // Clear the VM bits if required.
        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            let mut new_vm_blk = new_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);
            let mut old_vm_blk = old_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);

            // Sometimes, Postgres seems to create heap WAL records with the
            // ALL_VISIBLE_CLEARED flag set, even though the bit in the VM page is
            // not set. In fact, it's possible that the VM page does not exist at all.
            // In that case, we don't want to store a record to clear the VM bit;
            // replaying it would fail to find the previous image of the page, because
            // it doesn't exist. So check if the VM page(s) exist, and skip the WAL
            // record if it doesn't.
            let vm_size = get_relsize(modification, vm_rel, ctx).await?;
            if let Some(blknum) = new_vm_blk {
                if blknum >= vm_size {
                    new_vm_blk = None;
                }
            }
            if let Some(blknum) = old_vm_blk {
                if blknum >= vm_size {
                    old_vm_blk = None;
                }
            }

            if new_vm_blk.is_some() || old_vm_blk.is_some() {
                if new_vm_blk == old_vm_blk {
                    // An UPDATE record that needs to clear the bits for both old and the
                    // new page, both of which reside on the same VM page.
                    self.put_rel_wal_record(
                        modification,
                        vm_rel,
                        new_vm_blk.unwrap(),
                        NeonWalRecord::ClearVisibilityMapFlags {
                            new_heap_blkno,
                            old_heap_blkno,
                            flags,
                        },
                        ctx,
                    )
                    .await?;
                } else {
                    // Clear VM bits for one heap page, or for two pages that reside on
                    // different VM pages.
                    if let Some(new_vm_blk) = new_vm_blk {
                        self.put_rel_wal_record(
                            modification,
                            vm_rel,
                            new_vm_blk,
                            NeonWalRecord::ClearVisibilityMapFlags {
                                new_heap_blkno,
                                old_heap_blkno: None,
                                flags,
                            },
                            ctx,
                        )
                        .await?;
                    }
                    if let Some(old_vm_blk) = old_vm_blk {
                        self.put_rel_wal_record(
                            modification,
                            vm_rel,
                            old_vm_blk,
                            NeonWalRecord::ClearVisibilityMapFlags {
                                new_heap_blkno: None,
                                old_heap_blkno,
                                flags,
                            },
                            ctx,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn ingest_neonrmgr_record(
        &mut self,
        buf: &mut Bytes,
        modification: &mut DatadirModification<'_>,
        decoded: &DecodedWALRecord,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        let mut flags = pg_constants::VISIBILITYMAP_VALID_BITS;
        let pg_version = modification.tline.pg_version;

        assert_eq!(decoded.xl_rmid, pg_constants::RM_NEON_ID);

        match pg_version {
            16 => {
                let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                match info {
                    pg_constants::XLOG_NEON_HEAP_INSERT => {
                        let xlrec = v16::rm_neon::XlNeonHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_DELETE => {
                        let xlrec = v16::rm_neon::XlNeonHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_UPDATE
                    | pg_constants::XLOG_NEON_HEAP_HOT_UPDATE => {
                        let xlrec = v16::rm_neon::XlNeonHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_MULTI_INSERT => {
                        let xlrec = v16::rm_neon::XlNeonHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_LOCK => {
                        let xlrec = v16::rm_neon::XlNeonHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                    info => bail!("Unknown WAL record type for Neon RMGR: {}", info),
                }
            }
            _ => bail!(
                "Neon RMGR has no known compatibility with PostgreSQL version {}",
                pg_version
            ),
        }

        // Clear the VM bits if required.
        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            let mut new_vm_blk = new_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);
            let mut old_vm_blk = old_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);

            // Sometimes, Postgres seems to create heap WAL records with the
            // ALL_VISIBLE_CLEARED flag set, even though the bit in the VM page is
            // not set. In fact, it's possible that the VM page does not exist at all.
            // In that case, we don't want to store a record to clear the VM bit;
            // replaying it would fail to find the previous image of the page, because
            // it doesn't exist. So check if the VM page(s) exist, and skip the WAL
            // record if it doesn't.
            let vm_size = get_relsize(modification, vm_rel, ctx).await?;
            if let Some(blknum) = new_vm_blk {
                if blknum >= vm_size {
                    new_vm_blk = None;
                }
            }
            if let Some(blknum) = old_vm_blk {
                if blknum >= vm_size {
                    old_vm_blk = None;
                }
            }

            if new_vm_blk.is_some() || old_vm_blk.is_some() {
                if new_vm_blk == old_vm_blk {
                    // An UPDATE record that needs to clear the bits for both old and the
                    // new page, both of which reside on the same VM page.
                    self.put_rel_wal_record(
                        modification,
                        vm_rel,
                        new_vm_blk.unwrap(),
                        NeonWalRecord::ClearVisibilityMapFlags {
                            new_heap_blkno,
                            old_heap_blkno,
                            flags,
                        },
                        ctx,
                    )
                    .await?;
                } else {
                    // Clear VM bits for one heap page, or for two pages that reside on
                    // different VM pages.
                    if let Some(new_vm_blk) = new_vm_blk {
                        self.put_rel_wal_record(
                            modification,
                            vm_rel,
                            new_vm_blk,
                            NeonWalRecord::ClearVisibilityMapFlags {
                                new_heap_blkno,
                                old_heap_blkno: None,
                                flags,
                            },
                            ctx,
                        )
                        .await?;
                    }
                    if let Some(old_vm_blk) = old_vm_blk {
                        self.put_rel_wal_record(
                            modification,
                            vm_rel,
                            old_vm_blk,
                            NeonWalRecord::ClearVisibilityMapFlags {
                                new_heap_blkno: None,
                                old_heap_blkno,
                                flags,
                            },
                            ctx,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_DBASE_CREATE record.
    async fn ingest_xlog_dbase_create(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rec: &XlCreateDatabase,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let db_id = rec.db_id;
        let tablespace_id = rec.tablespace_id;
        let src_db_id = rec.src_db_id;
        let src_tablespace_id = rec.src_tablespace_id;

        let rels = modification
            .tline
            .list_rels(
                src_tablespace_id,
                src_db_id,
                Version::Modified(modification),
                ctx,
            )
            .await?;

        debug!("ingest_xlog_dbase_create: {} rels", rels.len());

        // Copy relfilemap
        let filemap = modification
            .tline
            .get_relmap_file(
                src_tablespace_id,
                src_db_id,
                Version::Modified(modification),
                ctx,
            )
            .await?;
        modification
            .put_relmap_file(tablespace_id, db_id, filemap, ctx)
            .await?;

        let mut num_rels_copied = 0;
        let mut num_blocks_copied = 0;
        for src_rel in rels {
            assert_eq!(src_rel.spcnode, src_tablespace_id);
            assert_eq!(src_rel.dbnode, src_db_id);

            let nblocks = modification
                .tline
                .get_rel_size(src_rel, Version::Modified(modification), ctx)
                .await?;
            let dst_rel = RelTag {
                spcnode: tablespace_id,
                dbnode: db_id,
                relnode: src_rel.relnode,
                forknum: src_rel.forknum,
            };

            modification.put_rel_creation(dst_rel, nblocks, ctx).await?;

            // Copy content
            debug!("copying rel {} to {}, {} blocks", src_rel, dst_rel, nblocks);
            for blknum in 0..nblocks {
                // Sharding:
                //  - src and dst are always on the same shard, because they differ only by dbNode, and
                //    dbNode is not included in the hash inputs for sharding.
                //  - This WAL command is replayed on all shards, but each shard only copies the blocks
                //    that belong to it.
                let src_key = rel_block_to_key(src_rel, blknum);
                if !self.shard.is_key_local(&src_key) {
                    debug!(
                        "Skipping non-local key {} during XLOG_DBASE_CREATE",
                        src_key
                    );
                    continue;
                }
                debug!(
                    "copying block {} from {} ({}) to {}",
                    blknum, src_rel, src_key, dst_rel
                );

                let content = modification
                    .tline
                    .get_rel_page_at_lsn(src_rel, blknum, Version::Modified(modification), ctx)
                    .await?;
                modification.put_rel_page_image(dst_rel, blknum, content)?;
                num_blocks_copied += 1;
            }

            num_rels_copied += 1;
        }

        info!(
            "Created database {}/{}, copied {} blocks in {} rels",
            tablespace_id, db_id, num_blocks_copied, num_rels_copied
        );
        Ok(())
    }

    async fn ingest_xlog_smgr_create(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rec: &XlSmgrCreate,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let rel = RelTag {
            spcnode: rec.rnode.spcnode,
            dbnode: rec.rnode.dbnode,
            relnode: rec.rnode.relnode,
            forknum: rec.forknum,
        };
        self.put_rel_creation(modification, rel, ctx).await?;
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_SMGR_TRUNCATE record.
    ///
    /// This is the same logic as in PostgreSQL's smgr_redo() function.
    async fn ingest_xlog_smgr_truncate(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rec: &XlSmgrTruncate,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let spcnode = rec.rnode.spcnode;
        let dbnode = rec.rnode.dbnode;
        let relnode = rec.rnode.relnode;

        if (rec.flags & pg_constants::SMGR_TRUNCATE_HEAP) != 0 {
            let rel = RelTag {
                spcnode,
                dbnode,
                relnode,
                forknum: MAIN_FORKNUM,
            };
            self.put_rel_truncation(modification, rel, rec.blkno, ctx)
                .await?;
        }
        if (rec.flags & pg_constants::SMGR_TRUNCATE_FSM) != 0 {
            let rel = RelTag {
                spcnode,
                dbnode,
                relnode,
                forknum: FSM_FORKNUM,
            };

            let fsm_logical_page_no = rec.blkno / pg_constants::SLOTS_PER_FSM_PAGE;
            let mut fsm_physical_page_no = fsm_logical_to_physical(fsm_logical_page_no);
            if rec.blkno % pg_constants::SLOTS_PER_FSM_PAGE != 0 {
                // Tail of last remaining FSM page has to be zeroed.
                // We are not precise here and instead of digging in FSM bitmap format just clear the whole page.
                modification.put_rel_page_image(rel, fsm_physical_page_no, ZERO_PAGE.clone())?;
                fsm_physical_page_no += 1;
            }
            let nblocks = get_relsize(modification, rel, ctx).await?;
            if nblocks > fsm_physical_page_no {
                // check if something to do: FSM is larger than truncate position
                self.put_rel_truncation(modification, rel, fsm_physical_page_no, ctx)
                    .await?;
            }
        }
        if (rec.flags & pg_constants::SMGR_TRUNCATE_VM) != 0 {
            let rel = RelTag {
                spcnode,
                dbnode,
                relnode,
                forknum: VISIBILITYMAP_FORKNUM,
            };

            let mut vm_page_no = rec.blkno / pg_constants::VM_HEAPBLOCKS_PER_PAGE;
            if rec.blkno % pg_constants::VM_HEAPBLOCKS_PER_PAGE != 0 {
                // Tail of last remaining vm page has to be zeroed.
                // We are not precise here and instead of digging in VM bitmap format just clear the whole page.
                modification.put_rel_page_image(rel, vm_page_no, ZERO_PAGE.clone())?;
                vm_page_no += 1;
            }
            let nblocks = get_relsize(modification, rel, ctx).await?;
            if nblocks > vm_page_no {
                // check if something to do: VM is larger than truncate position
                self.put_rel_truncation(modification, rel, vm_page_no, ctx)
                    .await?;
            }
        }
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_XACT_* records.
    ///
    async fn ingest_xact_record(
        &mut self,
        modification: &mut DatadirModification<'_>,
        parsed: &XlXactParsedRecord,
        is_commit: bool,
        origin_id: u16,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Record update of CLOG pages
        let mut pageno = parsed.xid / pg_constants::CLOG_XACTS_PER_PAGE;
        let mut segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let mut rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
        let mut page_xids: Vec<TransactionId> = vec![parsed.xid];

        for subxact in &parsed.subxacts {
            let subxact_pageno = subxact / pg_constants::CLOG_XACTS_PER_PAGE;
            if subxact_pageno != pageno {
                // This subxact goes to different page. Write the record
                // for all the XIDs on the previous page, and continue
                // accumulating XIDs on this new page.
                modification.put_slru_wal_record(
                    SlruKind::Clog,
                    segno,
                    rpageno,
                    if is_commit {
                        NeonWalRecord::ClogSetCommitted {
                            xids: page_xids,
                            timestamp: parsed.xact_time,
                        }
                    } else {
                        NeonWalRecord::ClogSetAborted { xids: page_xids }
                    },
                )?;
                page_xids = Vec::new();
            }
            pageno = subxact_pageno;
            segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
            page_xids.push(*subxact);
        }
        modification.put_slru_wal_record(
            SlruKind::Clog,
            segno,
            rpageno,
            if is_commit {
                NeonWalRecord::ClogSetCommitted {
                    xids: page_xids,
                    timestamp: parsed.xact_time,
                }
            } else {
                NeonWalRecord::ClogSetAborted { xids: page_xids }
            },
        )?;

        for xnode in &parsed.xnodes {
            for forknum in MAIN_FORKNUM..=INIT_FORKNUM {
                let rel = RelTag {
                    forknum,
                    spcnode: xnode.spcnode,
                    dbnode: xnode.dbnode,
                    relnode: xnode.relnode,
                };
                if modification
                    .tline
                    .get_rel_exists(rel, Version::Modified(modification), ctx)
                    .await?
                {
                    self.put_rel_drop(modification, rel, ctx).await?;
                }
            }
        }
        if origin_id != 0 {
            info!(
                "Commit at {} origin_id={}, origin_lsn={}",
                lsn, origin_id, parsed.origin_lsn
            );
            modification
                .set_replorigin(origin_id, parsed.origin_lsn)
                .await?;
        }
        Ok(())
    }

    async fn ingest_clog_truncate_record(
        &mut self,
        modification: &mut DatadirModification<'_>,
        xlrec: &XlClogTruncate,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        info!(
            "RM_CLOG_ID truncate pageno {} oldestXid {} oldestXidDB {}",
            xlrec.pageno, xlrec.oldest_xid, xlrec.oldest_xid_db
        );

        // In Postgres, oldestXid and oldestXidDB are updated in memory when the CLOG is
        // truncated, but a checkpoint record with the updated values isn't written until
        // later. In Neon, a server can start at any LSN, not just on a checkpoint record,
        // so we keep the oldestXid and oldestXidDB up-to-date.
        self.checkpoint.oldestXid = xlrec.oldest_xid;
        self.checkpoint.oldestXidDB = xlrec.oldest_xid_db;
        self.checkpoint_modified = true;

        // TODO Treat AdvanceOldestClogXid() or write a comment why we don't need it

        let latest_page_number =
            self.checkpoint.nextXid.value as u32 / pg_constants::CLOG_XACTS_PER_PAGE;

        // Now delete all segments containing pages between xlrec.pageno
        // and latest_page_number.

        // First, make an important safety check:
        // the current endpoint page must not be eligible for removal.
        // See SimpleLruTruncate() in slru.c
        if clogpage_precedes(latest_page_number, xlrec.pageno) {
            info!("could not truncate directory pg_xact apparent wraparound");
            return Ok(());
        }

        // Iterate via SLRU CLOG segments and drop segments that we're ready to truncate
        //
        // We cannot pass 'lsn' to the Timeline.list_nonrels(), or it
        // will block waiting for the last valid LSN to advance up to
        // it. So we use the previous record's LSN in the get calls
        // instead.
        for segno in modification
            .tline
            .list_slru_segments(SlruKind::Clog, Version::Modified(modification), ctx)
            .await?
        {
            let segpage = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;
            if slru_may_delete_clogsegment(segpage, xlrec.pageno) {
                modification
                    .drop_slru_segment(SlruKind::Clog, segno, ctx)
                    .await?;
                trace!("Drop CLOG segment {:>04X}", segno);
            }
        }

        Ok(())
    }

    fn ingest_multixact_create_record(
        &mut self,
        modification: &mut DatadirModification,
        xlrec: &XlMultiXactCreate,
    ) -> Result<()> {
        // Create WAL record for updating the multixact-offsets page
        let pageno = xlrec.mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

        modification.put_slru_wal_record(
            SlruKind::MultiXactOffsets,
            segno,
            rpageno,
            NeonWalRecord::MultixactOffsetCreate {
                mid: xlrec.mid,
                moff: xlrec.moff,
            },
        )?;

        // Create WAL records for the update of each affected multixact-members page
        let mut members = xlrec.members.iter();
        let mut offset = xlrec.moff;
        loop {
            let pageno = offset / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

            // How many members fit on this page?
            let page_remain = pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32
                - offset % pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

            let mut this_page_members: Vec<MultiXactMember> = Vec::new();
            for _ in 0..page_remain {
                if let Some(m) = members.next() {
                    this_page_members.push(m.clone());
                } else {
                    break;
                }
            }
            if this_page_members.is_empty() {
                // all done
                break;
            }
            let n_this_page = this_page_members.len();

            modification.put_slru_wal_record(
                SlruKind::MultiXactMembers,
                pageno / pg_constants::SLRU_PAGES_PER_SEGMENT,
                pageno % pg_constants::SLRU_PAGES_PER_SEGMENT,
                NeonWalRecord::MultixactMembersCreate {
                    moff: offset,
                    members: this_page_members,
                },
            )?;

            // Note: The multixact members can wrap around, even within one WAL record.
            offset = offset.wrapping_add(n_this_page as u32);
        }
        let next_offset = offset;
        assert!(xlrec.moff.wrapping_add(xlrec.nmembers) == next_offset);

        // Update next-multi-xid and next-offset
        //
        // NB: In PostgreSQL, the next-multi-xid stored in the control file is allowed to
        // go to 0, and it's fixed up by skipping to FirstMultiXactId in functions that
        // read it, like GetNewMultiXactId(). This is different from how nextXid is
        // incremented! nextXid skips over < FirstNormalTransactionId when the the value
        // is stored, so it's never 0 in a checkpoint.
        //
        // I don't know why it's done that way, it seems less error-prone to skip over 0
        // when the value is stored rather than when it's read. But let's do it the same
        // way here.
        let next_multi_xid = xlrec.mid.wrapping_add(1);

        if self
            .checkpoint
            .update_next_multixid(next_multi_xid, next_offset)
        {
            self.checkpoint_modified = true;
        }

        // Also update the next-xid with the highest member. According to the comments in
        // multixact_redo(), this shouldn't be necessary, but let's do the same here.
        let max_mbr_xid = xlrec.members.iter().fold(None, |acc, mbr| {
            if let Some(max_xid) = acc {
                if mbr.xid.wrapping_sub(max_xid) as i32 > 0 {
                    Some(mbr.xid)
                } else {
                    acc
                }
            } else {
                Some(mbr.xid)
            }
        });

        if let Some(max_xid) = max_mbr_xid {
            if self.checkpoint.update_next_xid(max_xid) {
                self.checkpoint_modified = true;
            }
        }
        Ok(())
    }

    async fn ingest_multixact_truncate_record(
        &mut self,
        modification: &mut DatadirModification<'_>,
        xlrec: &XlMultiXactTruncate,
        ctx: &RequestContext,
    ) -> Result<()> {
        self.checkpoint.oldestMulti = xlrec.end_trunc_off;
        self.checkpoint.oldestMultiDB = xlrec.oldest_multi_db;
        self.checkpoint_modified = true;

        // PerformMembersTruncation
        let maxsegment: i32 = mx_offset_to_member_segment(pg_constants::MAX_MULTIXACT_OFFSET);
        let startsegment: i32 = mx_offset_to_member_segment(xlrec.start_trunc_memb);
        let endsegment: i32 = mx_offset_to_member_segment(xlrec.end_trunc_memb);
        let mut segment: i32 = startsegment;

        // Delete all the segments except the last one. The last segment can still
        // contain, possibly partially, valid data.
        while segment != endsegment {
            modification
                .drop_slru_segment(SlruKind::MultiXactMembers, segment as u32, ctx)
                .await?;

            /* move to next segment, handling wraparound correctly */
            if segment == maxsegment {
                segment = 0;
            } else {
                segment += 1;
            }
        }

        // Truncate offsets
        // FIXME: this did not handle wraparound correctly

        Ok(())
    }

    async fn ingest_relmap_page(
        &mut self,
        modification: &mut DatadirModification<'_>,
        xlrec: &XlRelmapUpdate,
        decoded: &DecodedWALRecord,
        ctx: &RequestContext,
    ) -> Result<()> {
        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);
        // skip xl_relmap_update
        buf.advance(12);

        modification
            .put_relmap_file(
                xlrec.tsid,
                xlrec.dbid,
                Bytes::copy_from_slice(&buf[..]),
                ctx,
            )
            .await
    }

    async fn put_rel_creation(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rel: RelTag,
        ctx: &RequestContext,
    ) -> Result<()> {
        modification.put_rel_creation(rel, 0, ctx).await?;
        Ok(())
    }

    async fn put_rel_page_image(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rel: RelTag,
        blknum: BlockNumber,
        img: Bytes,
        ctx: &RequestContext,
    ) -> Result<(), PageReconstructError> {
        self.handle_rel_extend(modification, rel, blknum, ctx)
            .await?;
        modification.put_rel_page_image(rel, blknum, img)?;
        Ok(())
    }

    async fn put_rel_wal_record(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rel: RelTag,
        blknum: BlockNumber,
        rec: NeonWalRecord,
        ctx: &RequestContext,
    ) -> Result<()> {
        self.handle_rel_extend(modification, rel, blknum, ctx)
            .await?;
        modification.put_rel_wal_record(rel, blknum, rec)?;
        Ok(())
    }

    async fn put_rel_truncation(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        modification.put_rel_truncation(rel, nblocks, ctx).await?;
        Ok(())
    }

    async fn put_rel_drop(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rel: RelTag,
        ctx: &RequestContext,
    ) -> Result<()> {
        modification.put_rel_drop(rel, ctx).await?;
        Ok(())
    }

    async fn handle_rel_extend(
        &mut self,
        modification: &mut DatadirModification<'_>,
        rel: RelTag,
        blknum: BlockNumber,
        ctx: &RequestContext,
    ) -> Result<(), PageReconstructError> {
        let new_nblocks = blknum + 1;
        // Check if the relation exists. We implicitly create relations on first
        // record.
        // TODO: would be nice if to be more explicit about it

        // Get current size and put rel creation if rel doesn't exist
        //
        // NOTE: we check the cache first even though get_rel_exists and get_rel_size would
        //       check the cache too. This is because eagerly checking the cache results in
        //       less work overall and 10% better performance. It's more work on cache miss
        //       but cache miss is rare.
        let old_nblocks = if let Some(nblocks) = modification
            .tline
            .get_cached_rel_size(&rel, modification.get_lsn())
        {
            nblocks
        } else if !modification
            .tline
            .get_rel_exists(rel, Version::Modified(modification), ctx)
            .await?
        {
            // create it with 0 size initially, the logic below will extend it
            modification
                .put_rel_creation(rel, 0, ctx)
                .await
                .context("Relation Error")?;
            0
        } else {
            modification
                .tline
                .get_rel_size(rel, Version::Modified(modification), ctx)
                .await?
        };

        if new_nblocks > old_nblocks {
            //info!("extending {} {} to {}", rel, old_nblocks, new_nblocks);
            modification.put_rel_extend(rel, new_nblocks, ctx).await?;

            let mut key = rel_block_to_key(rel, blknum);
            // fill the gap with zeros
            for gap_blknum in old_nblocks..blknum {
                key.field6 = gap_blknum;

                if self.shard.get_shard_number(&key) != self.shard.number {
                    continue;
                }

                modification.put_rel_page_image(rel, gap_blknum, ZERO_PAGE.clone())?;
            }
        }
        Ok(())
    }

    async fn put_slru_page_image(
        &mut self,
        modification: &mut DatadirModification<'_>,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        img: Bytes,
        ctx: &RequestContext,
    ) -> Result<()> {
        self.handle_slru_extend(modification, kind, segno, blknum, ctx)
            .await?;
        modification.put_slru_page_image(kind, segno, blknum, img)?;
        Ok(())
    }

    async fn handle_slru_extend(
        &mut self,
        modification: &mut DatadirModification<'_>,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // we don't use a cache for this like we do for relations. SLRUS are explcitly
        // extended with ZEROPAGE records, not with commit records, so it happens
        // a lot less frequently.

        let new_nblocks = blknum + 1;
        // Check if the relation exists. We implicitly create relations on first
        // record.
        // TODO: would be nice if to be more explicit about it
        let old_nblocks = if !modification
            .tline
            .get_slru_segment_exists(kind, segno, Version::Modified(modification), ctx)
            .await?
        {
            // create it with 0 size initially, the logic below will extend it
            modification
                .put_slru_segment_creation(kind, segno, 0, ctx)
                .await?;
            0
        } else {
            modification
                .tline
                .get_slru_segment_size(kind, segno, Version::Modified(modification), ctx)
                .await?
        };

        if new_nblocks > old_nblocks {
            trace!(
                "extending SLRU {:?} seg {} from {} to {} blocks",
                kind,
                segno,
                old_nblocks,
                new_nblocks
            );
            modification.put_slru_extend(kind, segno, new_nblocks)?;

            // fill the gap with zeros
            for gap_blknum in old_nblocks..blknum {
                modification.put_slru_page_image(kind, segno, gap_blknum, ZERO_PAGE.clone())?;
            }
        }
        Ok(())
    }
}

async fn get_relsize(
    modification: &DatadirModification<'_>,
    rel: RelTag,
    ctx: &RequestContext,
) -> Result<BlockNumber, PageReconstructError> {
    let nblocks = if !modification
        .tline
        .get_rel_exists(rel, Version::Modified(modification), ctx)
        .await?
    {
        0
    } else {
        modification
            .tline
            .get_rel_size(rel, Version::Modified(modification), ctx)
            .await?
    };
    Ok(nblocks)
}

#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::*;
    use crate::tenant::remote_timeline_client::{remote_initdb_archive_path, INITDB_PATH};
    use postgres_ffi::RELSEG_SIZE;

    use crate::DEFAULT_PG_VERSION;

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelTag = RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    };

    fn assert_current_logical_size(_timeline: &Timeline, _lsn: Lsn) {
        // TODO
    }

    static ZERO_CHECKPOINT: Bytes = Bytes::from_static(&[0u8; SIZEOF_CHECKPOINT]);

    async fn init_walingest_test(tline: &Timeline, ctx: &RequestContext) -> Result<WalIngest> {
        let mut m = tline.begin_modification(Lsn(0x10));
        m.put_checkpoint(ZERO_CHECKPOINT.clone())?;
        m.put_relmap_file(0, 111, Bytes::from(""), ctx).await?; // dummy relmapper file
        m.commit(ctx).await?;
        let walingest = WalIngest::new(tline, Lsn(0x10), ctx).await?;

        Ok(walingest)
    }

    #[tokio::test]
    async fn test_relsize() -> Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_relsize").await?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let mut walingest = init_walingest_test(&tline, &ctx).await?;

        let mut m = tline.begin_modification(Lsn(0x20));
        walingest.put_rel_creation(&mut m, TESTREL_A, &ctx).await?;
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 0, test_img("foo blk 0 at 2"), &ctx)
            .await?;
        m.commit(&ctx).await?;
        let mut m = tline.begin_modification(Lsn(0x30));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 0, test_img("foo blk 0 at 3"), &ctx)
            .await?;
        m.commit(&ctx).await?;
        let mut m = tline.begin_modification(Lsn(0x40));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 1, test_img("foo blk 1 at 4"), &ctx)
            .await?;
        m.commit(&ctx).await?;
        let mut m = tline.begin_modification(Lsn(0x50));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 2, test_img("foo blk 2 at 5"), &ctx)
            .await?;
        m.commit(&ctx).await?;

        assert_current_logical_size(&tline, Lsn(0x50));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x10)), &ctx)
                .await?,
            false
        );
        assert!(tline
            .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x10)), &ctx)
            .await
            .is_err());
        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            true
        );
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            1
        );
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            3
        );

        // Check page contents at each LSN
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 0, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            test_img("foo blk 0 at 2")
        );

        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 0, Version::Lsn(Lsn(0x30)), &ctx)
                .await?,
            test_img("foo blk 0 at 3")
        );

        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 0, Version::Lsn(Lsn(0x40)), &ctx)
                .await?,
            test_img("foo blk 0 at 3")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 1, Version::Lsn(Lsn(0x40)), &ctx)
                .await?,
            test_img("foo blk 1 at 4")
        );

        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 0, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            test_img("foo blk 0 at 3")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 1, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            test_img("foo blk 1 at 4")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 2, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            test_img("foo blk 2 at 5")
        );

        // Truncate last block
        let mut m = tline.begin_modification(Lsn(0x60));
        walingest
            .put_rel_truncation(&mut m, TESTREL_A, 2, &ctx)
            .await?;
        m.commit(&ctx).await?;
        assert_current_logical_size(&tline, Lsn(0x60));

        // Check reported size and contents after truncation
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x60)), &ctx)
                .await?,
            2
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 0, Version::Lsn(Lsn(0x60)), &ctx)
                .await?,
            test_img("foo blk 0 at 3")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 1, Version::Lsn(Lsn(0x60)), &ctx)
                .await?,
            test_img("foo blk 1 at 4")
        );

        // should still see the truncated block with older LSN
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            3
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 2, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            test_img("foo blk 2 at 5")
        );

        // Truncate to zero length
        let mut m = tline.begin_modification(Lsn(0x68));
        walingest
            .put_rel_truncation(&mut m, TESTREL_A, 0, &ctx)
            .await?;
        m.commit(&ctx).await?;
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x68)), &ctx)
                .await?,
            0
        );

        // Extend from 0 to 2 blocks, leaving a gap
        let mut m = tline.begin_modification(Lsn(0x70));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 1, test_img("foo blk 1"), &ctx)
            .await?;
        m.commit(&ctx).await?;
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x70)), &ctx)
                .await?,
            2
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 0, Version::Lsn(Lsn(0x70)), &ctx)
                .await?,
            ZERO_PAGE
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 1, Version::Lsn(Lsn(0x70)), &ctx)
                .await?,
            test_img("foo blk 1")
        );

        // Extend a lot more, leaving a big gap that spans across segments
        let mut m = tline.begin_modification(Lsn(0x80));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 1500, test_img("foo blk 1500"), &ctx)
            .await?;
        m.commit(&ctx).await?;
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x80)), &ctx)
                .await?,
            1501
        );
        for blk in 2..1500 {
            assert_eq!(
                tline
                    .get_rel_page_at_lsn(TESTREL_A, blk, Version::Lsn(Lsn(0x80)), &ctx)
                    .await?,
                ZERO_PAGE
            );
        }
        assert_eq!(
            tline
                .get_rel_page_at_lsn(TESTREL_A, 1500, Version::Lsn(Lsn(0x80)), &ctx)
                .await?,
            test_img("foo blk 1500")
        );

        Ok(())
    }

    // Test what happens if we dropped a relation
    // and then created it again within the same layer.
    #[tokio::test]
    async fn test_drop_extend() -> Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_drop_extend")
            .await?
            .load()
            .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let mut walingest = init_walingest_test(&tline, &ctx).await?;

        let mut m = tline.begin_modification(Lsn(0x20));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 0, test_img("foo blk 0 at 2"), &ctx)
            .await?;
        m.commit(&ctx).await?;

        // Check that rel exists and size is correct
        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            true
        );
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            1
        );

        // Drop rel
        let mut m = tline.begin_modification(Lsn(0x30));
        walingest.put_rel_drop(&mut m, TESTREL_A, &ctx).await?;
        m.commit(&ctx).await?;

        // Check that rel is not visible anymore
        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x30)), &ctx)
                .await?,
            false
        );

        // FIXME: should fail
        //assert!(tline.get_rel_size(TESTREL_A, Lsn(0x30), false)?.is_none());

        // Re-create it
        let mut m = tline.begin_modification(Lsn(0x40));
        walingest
            .put_rel_page_image(&mut m, TESTREL_A, 0, test_img("foo blk 0 at 4"), &ctx)
            .await?;
        m.commit(&ctx).await?;

        // Check that rel exists and size is correct
        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x40)), &ctx)
                .await?,
            true
        );
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x40)), &ctx)
                .await?,
            1
        );

        Ok(())
    }

    // Test what happens if we truncated a relation
    // so that one of its segments was dropped
    // and then extended it again within the same layer.
    #[tokio::test]
    async fn test_truncate_extend() -> Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_truncate_extend")
            .await?
            .load()
            .await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let mut walingest = init_walingest_test(&tline, &ctx).await?;

        // Create a 20 MB relation (the size is arbitrary)
        let relsize = 20 * 1024 * 1024 / 8192;
        let mut m = tline.begin_modification(Lsn(0x20));
        for blkno in 0..relsize {
            let data = format!("foo blk {} at {}", blkno, Lsn(0x20));
            walingest
                .put_rel_page_image(&mut m, TESTREL_A, blkno, test_img(&data), &ctx)
                .await?;
        }
        m.commit(&ctx).await?;

        // The relation was created at LSN 20, not visible at LSN 1 yet.
        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x10)), &ctx)
                .await?,
            false
        );
        assert!(tline
            .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x10)), &ctx)
            .await
            .is_err());

        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            true
        );
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x20)), &ctx)
                .await?,
            relsize
        );

        // Check relation content
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline
                    .get_rel_page_at_lsn(TESTREL_A, blkno, Version::Lsn(lsn), &ctx)
                    .await?,
                test_img(&data)
            );
        }

        // Truncate relation so that second segment was dropped
        // - only leave one page
        let mut m = tline.begin_modification(Lsn(0x60));
        walingest
            .put_rel_truncation(&mut m, TESTREL_A, 1, &ctx)
            .await?;
        m.commit(&ctx).await?;

        // Check reported size and contents after truncation
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x60)), &ctx)
                .await?,
            1
        );

        for blkno in 0..1 {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline
                    .get_rel_page_at_lsn(TESTREL_A, blkno, Version::Lsn(Lsn(0x60)), &ctx)
                    .await?,
                test_img(&data)
            );
        }

        // should still see all blocks with older LSN
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x50)), &ctx)
                .await?,
            relsize
        );
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline
                    .get_rel_page_at_lsn(TESTREL_A, blkno, Version::Lsn(Lsn(0x50)), &ctx)
                    .await?,
                test_img(&data)
            );
        }

        // Extend relation again.
        // Add enough blocks to create second segment
        let lsn = Lsn(0x80);
        let mut m = tline.begin_modification(lsn);
        for blkno in 0..relsize {
            let data = format!("foo blk {} at {}", blkno, lsn);
            walingest
                .put_rel_page_image(&mut m, TESTREL_A, blkno, test_img(&data), &ctx)
                .await?;
        }
        m.commit(&ctx).await?;

        assert_eq!(
            tline
                .get_rel_exists(TESTREL_A, Version::Lsn(Lsn(0x80)), &ctx)
                .await?,
            true
        );
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(0x80)), &ctx)
                .await?,
            relsize
        );
        // Check relation content
        for blkno in 0..relsize {
            let lsn = Lsn(0x80);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline
                    .get_rel_page_at_lsn(TESTREL_A, blkno, Version::Lsn(Lsn(0x80)), &ctx)
                    .await?,
                test_img(&data)
            );
        }

        Ok(())
    }

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    #[tokio::test]
    async fn test_large_rel() -> Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_large_rel").await?.load().await;
        let tline = tenant
            .create_test_timeline(TIMELINE_ID, Lsn(8), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let mut walingest = init_walingest_test(&tline, &ctx).await?;

        let mut lsn = 0x10;
        for blknum in 0..RELSEG_SIZE + 1 {
            lsn += 0x10;
            let mut m = tline.begin_modification(Lsn(lsn));
            let img = test_img(&format!("foo blk {} at {}", blknum, Lsn(lsn)));
            walingest
                .put_rel_page_image(&mut m, TESTREL_A, blknum as BlockNumber, img, &ctx)
                .await?;
            m.commit(&ctx).await?;
        }

        assert_current_logical_size(&tline, Lsn(lsn));

        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(lsn)), &ctx)
                .await?,
            RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 0x10;
        let mut m = tline.begin_modification(Lsn(lsn));
        walingest
            .put_rel_truncation(&mut m, TESTREL_A, RELSEG_SIZE, &ctx)
            .await?;
        m.commit(&ctx).await?;
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(lsn)), &ctx)
                .await?,
            RELSEG_SIZE
        );
        assert_current_logical_size(&tline, Lsn(lsn));

        // Truncate another block
        lsn += 0x10;
        let mut m = tline.begin_modification(Lsn(lsn));
        walingest
            .put_rel_truncation(&mut m, TESTREL_A, RELSEG_SIZE - 1, &ctx)
            .await?;
        m.commit(&ctx).await?;
        assert_eq!(
            tline
                .get_rel_size(TESTREL_A, Version::Lsn(Lsn(lsn)), &ctx)
                .await?,
            RELSEG_SIZE - 1
        );
        assert_current_logical_size(&tline, Lsn(lsn));

        // Truncate to 1500, and then truncate all the way down to 0, one block at a time
        // This tests the behavior at segment boundaries
        let mut size: i32 = 3000;
        while size >= 0 {
            lsn += 0x10;
            let mut m = tline.begin_modification(Lsn(lsn));
            walingest
                .put_rel_truncation(&mut m, TESTREL_A, size as BlockNumber, &ctx)
                .await?;
            m.commit(&ctx).await?;
            assert_eq!(
                tline
                    .get_rel_size(TESTREL_A, Version::Lsn(Lsn(lsn)), &ctx)
                    .await?,
                size as BlockNumber
            );

            size -= 1;
        }
        assert_current_logical_size(&tline, Lsn(lsn));

        Ok(())
    }

    /// Replay a wal segment file taken directly from safekeepers.
    ///
    /// This test is useful for benchmarking since it allows us to profile only
    /// the walingest code in a single-threaded executor, and iterate more quickly
    /// without waiting for unrelated steps.
    #[tokio::test]
    async fn test_ingest_real_wal() {
        use crate::tenant::harness::*;
        use postgres_ffi::waldecoder::WalStreamDecoder;
        use postgres_ffi::WAL_SEGMENT_SIZE;

        // Define test data path and constants.
        //
        // Steps to reconstruct the data, if needed:
        // 1. Run the pgbench python test
        // 2. Take the first wal segment file from safekeeper
        // 3. Compress it using `zstd --long input_file`
        // 4. Copy initdb.tar.zst from local_fs_remote_storage
        // 5. Grep sk logs for "restart decoder" to get startpoint
        // 6. Run just the decoder from this test to get the endpoint.
        //    It's the last LSN the decoder will output.
        let pg_version = 15; // The test data was generated by pg15
        let path = "test_data/sk_wal_segment_from_pgbench";
        let wal_segment_path = format!("{path}/000000010000000000000001.zst");
        let source_initdb_path = format!("{path}/{INITDB_PATH}");
        let startpoint = Lsn::from_hex("14AEC08").unwrap();
        let _endpoint = Lsn::from_hex("1FFFF98").unwrap();

        let harness = TenantHarness::create("test_ingest_real_wal").await.unwrap();
        let (tenant, ctx) = harness.load().await;

        let remote_initdb_path =
            remote_initdb_archive_path(&tenant.tenant_shard_id().tenant_id, &TIMELINE_ID);
        let initdb_path = harness.remote_fs_dir.join(remote_initdb_path.get_path());

        std::fs::create_dir_all(initdb_path.parent().unwrap())
            .expect("creating test dir should work");
        std::fs::copy(source_initdb_path, initdb_path).expect("copying the initdb.tar.zst works");

        // Bootstrap a real timeline. We can't use create_test_timeline because
        // it doesn't create a real checkpoint, and Walingest::new tries to parse
        // the garbage data.
        let tline = tenant
            .bootstrap_timeline_test(TIMELINE_ID, pg_version, Some(TIMELINE_ID), &ctx)
            .await
            .unwrap();

        // We fully read and decompress this into memory before decoding
        // to get a more accurate perf profile of the decoder.
        let bytes = {
            use async_compression::tokio::bufread::ZstdDecoder;
            let file = tokio::fs::File::open(wal_segment_path).await.unwrap();
            let reader = tokio::io::BufReader::new(file);
            let decoder = ZstdDecoder::new(reader);
            let mut reader = tokio::io::BufReader::new(decoder);
            let mut buffer = Vec::new();
            tokio::io::copy_buf(&mut reader, &mut buffer).await.unwrap();
            buffer
        };

        // TODO start a profiler too
        let started_at = std::time::Instant::now();

        // Initialize walingest
        let xlogoff: usize = startpoint.segment_offset(WAL_SEGMENT_SIZE);
        let mut decoder = WalStreamDecoder::new(startpoint, pg_version);
        let mut walingest = WalIngest::new(tline.as_ref(), startpoint, &ctx)
            .await
            .unwrap();
        let mut modification = tline.begin_modification(startpoint);
        let mut decoded = DecodedWALRecord::default();
        println!("decoding {} bytes", bytes.len() - xlogoff);

        // Decode and ingest wal. We process the wal in chunks because
        // that's what happens when we get bytes from safekeepers.
        for chunk in bytes[xlogoff..].chunks(50) {
            decoder.feed_bytes(chunk);
            while let Some((lsn, recdata)) = decoder.poll_decode().unwrap() {
                walingest
                    .ingest_record(recdata, lsn, &mut modification, &mut decoded, &ctx)
                    .await
                    .unwrap();
            }
            modification.commit(&ctx).await.unwrap();
        }

        let duration = started_at.elapsed();
        println!("done in {:?}", duration);
    }
}
