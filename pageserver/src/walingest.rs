//!
//! Parse PostgreSQL WAL records and store them in a neon Timeline.
//!
//! The pipeline for ingesting WAL looks like this:
//!
//! WAL receiver  -> [`wal_decoder`] ->  WalIngest  ->   Repository
//!
//! The WAL receiver receives a stream of WAL from the WAL safekeepers.
//! Records get decoded and interpreted in the [`wal_decoder`] module
//! and then stored to the Repository by WalIngest.
//!
//! The neon Repository can store page versions in two formats: as
//! page images, or a WAL records. [`wal_decoder::models::InterpretedWalRecord::from_bytes_filtered`]
//! extracts page images out of some WAL records, but mostly it's WAL
//! records. If a WAL record modifies multiple pages, WalIngest
//! will call Repository::put_rel_wal_record or put_rel_page_image functions
//! separately for each modified page.
//!
//! To reconstruct a page using a WAL record, the Repository calls the
//! code in walredo.rs. walredo.rs passes most WAL records to the WAL
//! redo Postgres process, but some records it can handle directly with
//! bespoken Rust code.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use pageserver_api::shard::ShardIdentity;
use postgres_ffi::fsm_logical_to_physical;
use postgres_ffi::walrecord::*;
use postgres_ffi::{dispatch_pgversion, enum_pgversion, enum_pgversion_dispatch, TimestampTz};
use wal_decoder::models::*;

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use tracing::*;
use utils::failpoint_support;
use utils::rate_limit::RateLimit;

use crate::context::RequestContext;
use crate::metrics::WAL_INGEST;
use crate::pgdatadir_mapping::{DatadirModification, Version};
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::PageReconstructError;
use crate::tenant::Timeline;
use crate::ZERO_PAGE;
use pageserver_api::key::rel_block_to_key;
use pageserver_api::record::NeonWalRecord;
use pageserver_api::reltag::{BlockNumber, RelTag, SlruKind};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::{FSM_FORKNUM, INIT_FORKNUM, MAIN_FORKNUM, VISIBILITYMAP_FORKNUM};
use postgres_ffi::TransactionId;
use utils::bin_ser::SerializeError;
use utils::lsn::Lsn;

enum_pgversion! {CheckPoint, pgv::CheckPoint}

impl CheckPoint {
    fn encode(&self) -> Result<Bytes, SerializeError> {
        enum_pgversion_dispatch!(self, CheckPoint, cp, { cp.encode() })
    }

    fn update_next_xid(&mut self, xid: u32) -> bool {
        enum_pgversion_dispatch!(self, CheckPoint, cp, { cp.update_next_xid(xid) })
    }

    pub fn update_next_multixid(&mut self, multi_xid: u32, multi_offset: u32) -> bool {
        enum_pgversion_dispatch!(self, CheckPoint, cp, {
            cp.update_next_multixid(multi_xid, multi_offset)
        })
    }
}

/// Temporary limitation of WAL lag warnings after attach
///
/// After tenant attach, we want to limit WAL lag warnings because
/// we don't look at the WAL until the attach is complete, which
/// might take a while.
pub struct WalLagCooldown {
    /// Until when should this limitation apply at all
    active_until: std::time::Instant,
    /// The maximum lag to suppress. Lags above this limit get reported anyways.
    max_lag: Duration,
}

impl WalLagCooldown {
    pub fn new(attach_start: Instant, attach_duration: Duration) -> Self {
        Self {
            active_until: attach_start + attach_duration * 3 + Duration::from_secs(120),
            max_lag: attach_duration * 2 + Duration::from_secs(60),
        }
    }
}

pub struct WalIngest {
    attach_wal_lag_cooldown: Arc<OnceLock<WalLagCooldown>>,
    shard: ShardIdentity,
    checkpoint: CheckPoint,
    checkpoint_modified: bool,
    warn_ingest_lag: WarnIngestLag,
}

struct WarnIngestLag {
    lag_msg_ratelimit: RateLimit,
    future_lsn_msg_ratelimit: RateLimit,
    timestamp_invalid_msg_ratelimit: RateLimit,
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
        let pgversion = timeline.pg_version;

        let checkpoint = dispatch_pgversion!(pgversion, {
            let checkpoint = pgv::CheckPoint::decode(&checkpoint_bytes)?;
            trace!("CheckPoint.nextXid = {}", checkpoint.nextXid.value);
            <pgv::CheckPoint as Into<CheckPoint>>::into(checkpoint)
        });

        Ok(WalIngest {
            shard: *timeline.get_shard_identity(),
            checkpoint,
            checkpoint_modified: false,
            attach_wal_lag_cooldown: timeline.attach_wal_lag_cooldown.clone(),
            warn_ingest_lag: WarnIngestLag {
                lag_msg_ratelimit: RateLimit::new(std::time::Duration::from_secs(10)),
                future_lsn_msg_ratelimit: RateLimit::new(std::time::Duration::from_secs(10)),
                timestamp_invalid_msg_ratelimit: RateLimit::new(std::time::Duration::from_secs(10)),
            },
        })
    }

    /// Ingest an interpreted PostgreSQL WAL record by doing writes to the underlying key value
    /// storage of a given timeline.
    ///
    /// This function updates `lsn` field of `DatadirModification`
    ///
    /// This function returns `true` if the record was ingested, and `false` if it was filtered out
    pub async fn ingest_record(
        &mut self,
        interpreted: InterpretedWalRecord,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<bool> {
        WAL_INGEST.records_received.inc();
        let prev_len = modification.len();

        modification.set_lsn(interpreted.next_record_lsn)?;

        if matches!(interpreted.flush_uncommitted, FlushUncommittedRecords::Yes) {
            // Records of this type should always be preceded by a commit(), as they
            // rely on reading data pages back from the Timeline.
            assert!(!modification.has_dirty_data());
        }

        assert!(!self.checkpoint_modified);
        if interpreted.xid != pg_constants::INVALID_TRANSACTION_ID
            && self.checkpoint.update_next_xid(interpreted.xid)
        {
            self.checkpoint_modified = true;
        }

        failpoint_support::sleep_millis_async!("wal-ingest-record-sleep");

        match interpreted.metadata_record {
            Some(MetadataRecord::Heapam(rec)) => match rec {
                HeapamRecord::ClearVmBits(clear_vm_bits) => {
                    self.ingest_clear_vm_bits(clear_vm_bits, modification, ctx)
                        .await?;
                }
            },
            Some(MetadataRecord::Neonrmgr(rec)) => match rec {
                NeonrmgrRecord::ClearVmBits(clear_vm_bits) => {
                    self.ingest_clear_vm_bits(clear_vm_bits, modification, ctx)
                        .await?;
                }
            },
            Some(MetadataRecord::Smgr(rec)) => match rec {
                SmgrRecord::Create(create) => {
                    self.ingest_xlog_smgr_create(create, modification, ctx)
                        .await?;
                }
                SmgrRecord::Truncate(truncate) => {
                    self.ingest_xlog_smgr_truncate(truncate, modification, ctx)
                        .await?;
                }
            },
            Some(MetadataRecord::Dbase(rec)) => match rec {
                DbaseRecord::Create(create) => {
                    self.ingest_xlog_dbase_create(create, modification, ctx)
                        .await?;
                }
                DbaseRecord::Drop(drop) => {
                    self.ingest_xlog_dbase_drop(drop, modification, ctx).await?;
                }
            },
            Some(MetadataRecord::Clog(rec)) => match rec {
                ClogRecord::ZeroPage(zero_page) => {
                    self.ingest_clog_zero_page(zero_page, modification, ctx)
                        .await?;
                }
                ClogRecord::Truncate(truncate) => {
                    self.ingest_clog_truncate(truncate, modification, ctx)
                        .await?;
                }
            },
            Some(MetadataRecord::Xact(rec)) => {
                self.ingest_xact_record(rec, modification, ctx).await?;
            }
            Some(MetadataRecord::MultiXact(rec)) => match rec {
                MultiXactRecord::ZeroPage(zero_page) => {
                    self.ingest_multixact_zero_page(zero_page, modification, ctx)
                        .await?;
                }
                MultiXactRecord::Create(create) => {
                    self.ingest_multixact_create(modification, &create)?;
                }
                MultiXactRecord::Truncate(truncate) => {
                    self.ingest_multixact_truncate(modification, &truncate, ctx)
                        .await?;
                }
            },
            Some(MetadataRecord::Relmap(rec)) => match rec {
                RelmapRecord::Update(update) => {
                    self.ingest_relmap_update(update, modification, ctx).await?;
                }
            },
            Some(MetadataRecord::Xlog(rec)) => match rec {
                XlogRecord::Raw(raw) => {
                    self.ingest_raw_xlog_record(raw, modification, ctx).await?;
                }
            },
            Some(MetadataRecord::LogicalMessage(rec)) => match rec {
                LogicalMessageRecord::Put(put) => {
                    self.ingest_logical_message_put(put, modification, ctx)
                        .await?;
                }
                #[cfg(feature = "testing")]
                LogicalMessageRecord::Failpoint => {
                    // This is a convenient way to make the WAL ingestion pause at
                    // particular point in the WAL. For more fine-grained control,
                    // we could peek into the message and only pause if it contains
                    // a particular string, for example, but this is enough for now.
                    failpoint_support::sleep_millis_async!(
                        "pageserver-wal-ingest-logical-message-sleep"
                    );
                }
            },
            Some(MetadataRecord::Standby(rec)) => {
                self.ingest_standby_record(rec).unwrap();
            }
            Some(MetadataRecord::Replorigin(rec)) => {
                self.ingest_replorigin_record(rec, modification).await?;
            }
            None => {
                // There are two cases through which we end up here:
                // 1. The resource manager for the original PG WAL record
                //    is [`pg_constants::RM_TBLSPC_ID`]. This is not a supported
                //    record type within Neon.
                // 2. The resource manager id was unknown to
                //    [`wal_decoder::decoder::MetadataRecord::from_decoded`].
                // TODO(vlad): Tighten this up more once we build confidence
                // that case (2) does not happen in the field.
            }
        }

        modification
            .ingest_batch(interpreted.batch, &self.shard, ctx)
            .await?;

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

    /// This is the same as AdjustToFullTransactionId(xid) in PostgreSQL
    fn adjust_to_full_transaction_id(&self, xid: TransactionId) -> Result<u64> {
        let next_full_xid =
            enum_pgversion_dispatch!(&self.checkpoint, CheckPoint, cp, { cp.nextXid.value });

        let next_xid = (next_full_xid) as u32;
        let mut epoch = (next_full_xid >> 32) as u32;

        if xid > next_xid {
            // Wraparound occurred, must be from a prev epoch.
            if epoch == 0 {
                bail!("apparent XID wraparound with prepared transaction XID {xid}, nextXid is {next_full_xid}");
            }
            epoch -= 1;
        }

        Ok(((epoch as u64) << 32) | xid as u64)
    }

    async fn ingest_clear_vm_bits(
        &mut self,
        clear_vm_bits: ClearVmBits,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let ClearVmBits {
            new_heap_blkno,
            old_heap_blkno,
            flags,
            vm_rel,
        } = clear_vm_bits;
        // Clear the VM bits if required.
        let mut new_vm_blk = new_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);
        let mut old_vm_blk = old_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);

        // Sometimes, Postgres seems to create heap WAL records with the
        // ALL_VISIBLE_CLEARED flag set, even though the bit in the VM page is
        // not set. In fact, it's possible that the VM page does not exist at all.
        // In that case, we don't want to store a record to clear the VM bit;
        // replaying it would fail to find the previous image of the page, because
        // it doesn't exist. So check if the VM page(s) exist, and skip the WAL
        // record if it doesn't.
        //
        // TODO: analyze the metrics and tighten this up accordingly. This logic
        // implicitly assumes that VM pages see explicit WAL writes before
        // implicit ClearVmBits, and will otherwise silently drop updates.
        let Some(vm_size) = get_relsize(modification, vm_rel, ctx).await? else {
            WAL_INGEST
                .clear_vm_bits_unknown
                .with_label_values(&["relation"])
                .inc();
            return Ok(());
        };
        if let Some(blknum) = new_vm_blk {
            if blknum >= vm_size {
                WAL_INGEST
                    .clear_vm_bits_unknown
                    .with_label_values(&["new_page"])
                    .inc();
                new_vm_blk = None;
            }
        }
        if let Some(blknum) = old_vm_blk {
            if blknum >= vm_size {
                WAL_INGEST
                    .clear_vm_bits_unknown
                    .with_label_values(&["old_page"])
                    .inc();
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

        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_DBASE_CREATE record.
    async fn ingest_xlog_dbase_create(
        &mut self,
        create: DbaseCreate,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let DbaseCreate {
            db_id,
            tablespace_id,
            src_db_id,
            src_tablespace_id,
        } = create;

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
                    .get_rel_page_at_lsn(
                        src_rel,
                        blknum,
                        Version::Modified(modification),
                        ctx,
                        crate::tenant::storage_layer::IoConcurrency::sequential(),
                    )
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

    async fn ingest_xlog_dbase_drop(
        &mut self,
        dbase_drop: DbaseDrop,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let DbaseDrop {
            db_id,
            tablespace_ids,
        } = dbase_drop;
        for tablespace_id in tablespace_ids {
            trace!("Drop db {}, {}", tablespace_id, db_id);
            modification.drop_dbdir(tablespace_id, db_id, ctx).await?;
        }

        Ok(())
    }

    async fn ingest_xlog_smgr_create(
        &mut self,
        create: SmgrCreate,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let SmgrCreate { rel } = create;
        self.put_rel_creation(modification, rel, ctx).await?;
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_SMGR_TRUNCATE record.
    ///
    /// This is the same logic as in PostgreSQL's smgr_redo() function.
    async fn ingest_xlog_smgr_truncate(
        &mut self,
        truncate: XlSmgrTruncate,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let XlSmgrTruncate {
            blkno,
            rnode,
            flags,
        } = truncate;

        let spcnode = rnode.spcnode;
        let dbnode = rnode.dbnode;
        let relnode = rnode.relnode;

        if flags & pg_constants::SMGR_TRUNCATE_HEAP != 0 {
            let rel = RelTag {
                spcnode,
                dbnode,
                relnode,
                forknum: MAIN_FORKNUM,
            };

            self.put_rel_truncation(modification, rel, blkno, ctx)
                .await?;
        }
        if flags & pg_constants::SMGR_TRUNCATE_FSM != 0 {
            let rel = RelTag {
                spcnode,
                dbnode,
                relnode,
                forknum: FSM_FORKNUM,
            };

            // Zero out the last remaining FSM page, if this shard owns it. We are not precise here,
            // and instead of digging in the FSM bitmap format we just clear the whole page.
            let fsm_logical_page_no = blkno / pg_constants::SLOTS_PER_FSM_PAGE;
            let mut fsm_physical_page_no = fsm_logical_to_physical(fsm_logical_page_no);
            if blkno % pg_constants::SLOTS_PER_FSM_PAGE != 0
                && self
                    .shard
                    .is_key_local(&rel_block_to_key(rel, fsm_physical_page_no))
            {
                modification.put_rel_page_image_zero(rel, fsm_physical_page_no)?;
                fsm_physical_page_no += 1;
            }
            // Truncate this shard's view of the FSM relation size, if it even has one.
            let nblocks = get_relsize(modification, rel, ctx).await?.unwrap_or(0);
            if nblocks > fsm_physical_page_no {
                self.put_rel_truncation(modification, rel, fsm_physical_page_no, ctx)
                    .await?;
            }
        }
        if flags & pg_constants::SMGR_TRUNCATE_VM != 0 {
            let rel = RelTag {
                spcnode,
                dbnode,
                relnode,
                forknum: VISIBILITYMAP_FORKNUM,
            };

            // last remaining block, byte, and bit
            let mut vm_page_no = blkno / (pg_constants::VM_HEAPBLOCKS_PER_PAGE as u32);
            let trunc_byte = blkno as usize % pg_constants::VM_HEAPBLOCKS_PER_PAGE
                / pg_constants::VM_HEAPBLOCKS_PER_BYTE;
            let trunc_offs = blkno as usize % pg_constants::VM_HEAPBLOCKS_PER_BYTE
                * pg_constants::VM_BITS_PER_HEAPBLOCK;

            // Unless the new size is exactly at a visibility map page boundary, the
            // tail bits in the last remaining map page, representing truncated heap
            // blocks, need to be cleared. This is not only tidy, but also necessary
            // because we don't get a chance to clear the bits if the heap is extended
            // again. Only do this on the shard that owns the page.
            if (trunc_byte != 0 || trunc_offs != 0)
                && self.shard.is_key_local(&rel_block_to_key(rel, vm_page_no))
            {
                modification.put_rel_wal_record(
                    rel,
                    vm_page_no,
                    NeonWalRecord::TruncateVisibilityMap {
                        trunc_byte,
                        trunc_offs,
                    },
                )?;
                vm_page_no += 1;
            }
            // Truncate this shard's view of the VM relation size, if it even has one.
            let nblocks = get_relsize(modification, rel, ctx).await?.unwrap_or(0);
            if nblocks > vm_page_no {
                self.put_rel_truncation(modification, rel, vm_page_no, ctx)
                    .await?;
            }
        }
        Ok(())
    }

    fn warn_on_ingest_lag(
        &mut self,
        conf: &crate::config::PageServerConf,
        wal_timestamp: TimestampTz,
    ) {
        debug_assert_current_span_has_tenant_and_timeline_id();
        let now = SystemTime::now();
        let rate_limits = &mut self.warn_ingest_lag;

        let ts = enum_pgversion_dispatch!(&self.checkpoint, CheckPoint, _cp, {
            pgv::xlog_utils::try_from_pg_timestamp(wal_timestamp)
        });

        match ts {
            Ok(ts) => {
                match now.duration_since(ts) {
                    Ok(lag) => {
                        if lag > conf.wait_lsn_timeout {
                            rate_limits.lag_msg_ratelimit.call2(|rate_limit_stats| {
                                if let Some(cooldown) = self.attach_wal_lag_cooldown.get() {
                                    if std::time::Instant::now() < cooldown.active_until && lag <= cooldown.max_lag {
                                        return;
                                    }
                                } else {
                                    // Still loading? We shouldn't be here
                                }
                                let lag = humantime::format_duration(lag);
                                warn!(%rate_limit_stats, %lag, "ingesting record with timestamp lagging more than wait_lsn_timeout");
                            })
                        }
                    }
                    Err(e) => {
                        let delta_t = e.duration();
                        // determined by prod victoriametrics query: 1000 * (timestamp(node_time_seconds{neon_service="pageserver"}) - node_time_seconds)
                        // => https://www.robustperception.io/time-metric-from-the-node-exporter/
                        const IGNORED_DRIFT: Duration = Duration::from_millis(100);
                        if delta_t > IGNORED_DRIFT {
                            let delta_t = humantime::format_duration(delta_t);
                            rate_limits.future_lsn_msg_ratelimit.call2(|rate_limit_stats| {
                                warn!(%rate_limit_stats, %delta_t, "ingesting record with timestamp from future");
                            })
                        }
                    }
                };
            }
            Err(error) => {
                rate_limits.timestamp_invalid_msg_ratelimit.call2(|rate_limit_stats| {
                    warn!(%rate_limit_stats, %error, "ingesting record with invalid timestamp, cannot calculate lag and will fail find-lsn-for-timestamp type queries");
                })
            }
        }
    }

    /// Subroutine of ingest_record(), to handle an XLOG_XACT_* records.
    ///
    async fn ingest_xact_record(
        &mut self,
        record: XactRecord,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let (xact_common, is_commit, is_prepared) = match record {
            XactRecord::Prepare(XactPrepare { xl_xid, data }) => {
                let xid: u64 = if modification.tline.pg_version >= 17 {
                    self.adjust_to_full_transaction_id(xl_xid)?
                } else {
                    xl_xid as u64
                };
                return modification.put_twophase_file(xid, data, ctx).await;
            }
            XactRecord::Commit(common) => (common, true, false),
            XactRecord::Abort(common) => (common, false, false),
            XactRecord::CommitPrepared(common) => (common, true, true),
            XactRecord::AbortPrepared(common) => (common, false, true),
        };

        let XactCommon {
            parsed,
            origin_id,
            xl_xid,
            lsn,
        } = xact_common;

        // Record update of CLOG pages
        let mut pageno = parsed.xid / pg_constants::CLOG_XACTS_PER_PAGE;
        let mut segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let mut rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
        let mut page_xids: Vec<TransactionId> = vec![parsed.xid];

        self.warn_on_ingest_lag(modification.tline.conf, parsed.xact_time);

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

        // Group relations to drop by dbNode.  This map will contain all relations that _might_
        // exist, we will reduce it to which ones really exist later.  This map can be huge if
        // the transaction touches a huge number of relations (there is no bound on this in
        // postgres).
        let mut drop_relations: HashMap<(u32, u32), Vec<RelTag>> = HashMap::new();

        for xnode in &parsed.xnodes {
            for forknum in MAIN_FORKNUM..=INIT_FORKNUM {
                let rel = RelTag {
                    forknum,
                    spcnode: xnode.spcnode,
                    dbnode: xnode.dbnode,
                    relnode: xnode.relnode,
                };
                drop_relations
                    .entry((xnode.spcnode, xnode.dbnode))
                    .or_default()
                    .push(rel);
            }
        }

        // Execute relation drops in a batch: the number may be huge, so deleting individually is prohibitively expensive
        modification.put_rel_drops(drop_relations, ctx).await?;

        if origin_id != 0 {
            modification
                .set_replorigin(origin_id, parsed.origin_lsn)
                .await?;
        }

        if is_prepared {
            // Remove twophase file. see RemoveTwoPhaseFile() in postgres code
            trace!(
                "Drop twophaseFile for xid {} parsed_xact.xid {} here at {}",
                xl_xid,
                parsed.xid,
                lsn,
            );

            let xid: u64 = if modification.tline.pg_version >= 17 {
                self.adjust_to_full_transaction_id(parsed.xid)?
            } else {
                parsed.xid as u64
            };
            modification.drop_twophase_file(xid, ctx).await?;
        }

        Ok(())
    }

    async fn ingest_clog_truncate(
        &mut self,
        truncate: ClogTruncate,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let ClogTruncate {
            pageno,
            oldest_xid,
            oldest_xid_db,
        } = truncate;

        info!(
            "RM_CLOG_ID truncate pageno {} oldestXid {} oldestXidDB {}",
            pageno, oldest_xid, oldest_xid_db
        );

        // In Postgres, oldestXid and oldestXidDB are updated in memory when the CLOG is
        // truncated, but a checkpoint record with the updated values isn't written until
        // later. In Neon, a server can start at any LSN, not just on a checkpoint record,
        // so we keep the oldestXid and oldestXidDB up-to-date.
        enum_pgversion_dispatch!(&mut self.checkpoint, CheckPoint, cp, {
            cp.oldestXid = oldest_xid;
            cp.oldestXidDB = oldest_xid_db;
        });
        self.checkpoint_modified = true;

        // TODO Treat AdvanceOldestClogXid() or write a comment why we don't need it

        let latest_page_number =
            enum_pgversion_dispatch!(self.checkpoint, CheckPoint, cp, { cp.nextXid.value }) as u32
                / pg_constants::CLOG_XACTS_PER_PAGE;

        // Now delete all segments containing pages between xlrec.pageno
        // and latest_page_number.

        // First, make an important safety check:
        // the current endpoint page must not be eligible for removal.
        // See SimpleLruTruncate() in slru.c
        if dispatch_pgversion!(modification.tline.pg_version, {
            pgv::nonrelfile_utils::clogpage_precedes(latest_page_number, pageno)
        }) {
            info!("could not truncate directory pg_xact apparent wraparound");
            return Ok(());
        }

        // Iterate via SLRU CLOG segments and drop segments that we're ready to truncate
        //
        // We cannot pass 'lsn' to the Timeline.list_nonrels(), or it
        // will block waiting for the last valid LSN to advance up to
        // it. So we use the previous record's LSN in the get calls
        // instead.
        if modification.tline.get_shard_identity().is_shard_zero() {
            for segno in modification
                .tline
                .list_slru_segments(SlruKind::Clog, Version::Modified(modification), ctx)
                .await?
            {
                let segpage = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;

                let may_delete = dispatch_pgversion!(modification.tline.pg_version, {
                    pgv::nonrelfile_utils::slru_may_delete_clogsegment(segpage, pageno)
                });

                if may_delete {
                    modification
                        .drop_slru_segment(SlruKind::Clog, segno, ctx)
                        .await?;
                    trace!("Drop CLOG segment {:>04X}", segno);
                }
            }
        }

        Ok(())
    }

    async fn ingest_clog_zero_page(
        &mut self,
        zero_page: ClogZeroPage,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let ClogZeroPage { segno, rpageno } = zero_page;

        self.put_slru_page_image(
            modification,
            SlruKind::Clog,
            segno,
            rpageno,
            ZERO_PAGE.clone(),
            ctx,
        )
        .await
    }

    fn ingest_multixact_create(
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

    async fn ingest_multixact_truncate(
        &mut self,
        modification: &mut DatadirModification<'_>,
        xlrec: &XlMultiXactTruncate,
        ctx: &RequestContext,
    ) -> Result<()> {
        let (maxsegment, startsegment, endsegment) =
            enum_pgversion_dispatch!(&mut self.checkpoint, CheckPoint, cp, {
                cp.oldestMulti = xlrec.end_trunc_off;
                cp.oldestMultiDB = xlrec.oldest_multi_db;
                let maxsegment: i32 = pgv::nonrelfile_utils::mx_offset_to_member_segment(
                    pg_constants::MAX_MULTIXACT_OFFSET,
                );
                let startsegment: i32 =
                    pgv::nonrelfile_utils::mx_offset_to_member_segment(xlrec.start_trunc_memb);
                let endsegment: i32 =
                    pgv::nonrelfile_utils::mx_offset_to_member_segment(xlrec.end_trunc_memb);
                (maxsegment, startsegment, endsegment)
            });

        self.checkpoint_modified = true;

        // PerformMembersTruncation
        let mut segment: i32 = startsegment;

        // Delete all the segments except the last one. The last segment can still
        // contain, possibly partially, valid data.
        if modification.tline.get_shard_identity().is_shard_zero() {
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
        }

        // Truncate offsets
        // FIXME: this did not handle wraparound correctly

        Ok(())
    }

    async fn ingest_multixact_zero_page(
        &mut self,
        zero_page: MultiXactZeroPage,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> Result<()> {
        let MultiXactZeroPage {
            slru_kind,
            segno,
            rpageno,
        } = zero_page;
        self.put_slru_page_image(
            modification,
            slru_kind,
            segno,
            rpageno,
            ZERO_PAGE.clone(),
            ctx,
        )
        .await
    }

    async fn ingest_relmap_update(
        &mut self,
        update: RelmapUpdate,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> Result<()> {
        let RelmapUpdate { update, buf } = update;

        modification
            .put_relmap_file(update.tsid, update.dbid, buf, ctx)
            .await
    }

    async fn ingest_raw_xlog_record(
        &mut self,
        raw_record: RawXlogRecord,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> Result<()> {
        let RawXlogRecord { info, lsn, mut buf } = raw_record;
        let pg_version = modification.tline.pg_version;

        if info == pg_constants::XLOG_PARAMETER_CHANGE {
            if let CheckPoint::V17(cp) = &mut self.checkpoint {
                let rec = v17::XlParameterChange::decode(&mut buf);
                cp.wal_level = rec.wal_level;
                self.checkpoint_modified = true;
            }
        } else if info == pg_constants::XLOG_END_OF_RECOVERY {
            if let CheckPoint::V17(cp) = &mut self.checkpoint {
                let rec = v17::XlEndOfRecovery::decode(&mut buf);
                cp.wal_level = rec.wal_level;
                self.checkpoint_modified = true;
            }
        }

        enum_pgversion_dispatch!(&mut self.checkpoint, CheckPoint, cp, {
            if info == pg_constants::XLOG_NEXTOID {
                let next_oid = buf.get_u32_le();
                if cp.nextOid != next_oid {
                    cp.nextOid = next_oid;
                    self.checkpoint_modified = true;
                }
            } else if info == pg_constants::XLOG_CHECKPOINT_ONLINE
                || info == pg_constants::XLOG_CHECKPOINT_SHUTDOWN
            {
                let mut checkpoint_bytes = [0u8; pgv::xlog_utils::SIZEOF_CHECKPOINT];
                buf.copy_to_slice(&mut checkpoint_bytes);
                let xlog_checkpoint = pgv::CheckPoint::decode(&checkpoint_bytes)?;
                trace!(
                    "xlog_checkpoint.oldestXid={}, checkpoint.oldestXid={}",
                    xlog_checkpoint.oldestXid,
                    cp.oldestXid
                );
                if (cp.oldestXid.wrapping_sub(xlog_checkpoint.oldestXid) as i32) < 0 {
                    cp.oldestXid = xlog_checkpoint.oldestXid;
                }
                trace!(
                    "xlog_checkpoint.oldestActiveXid={}, checkpoint.oldestActiveXid={}",
                    xlog_checkpoint.oldestActiveXid,
                    cp.oldestActiveXid
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
                    let oldest_active_xid = if pg_version >= 17 {
                        let mut oldest_active_full_xid = cp.nextXid.value;
                        for xid in modification.tline.list_twophase_files(lsn, ctx).await? {
                            if xid < oldest_active_full_xid {
                                oldest_active_full_xid = xid;
                            }
                        }
                        oldest_active_full_xid as u32
                    } else {
                        let mut oldest_active_xid = cp.nextXid.value as u32;
                        for xid in modification.tline.list_twophase_files(lsn, ctx).await? {
                            let narrow_xid = xid as u32;
                            if (narrow_xid.wrapping_sub(oldest_active_xid) as i32) < 0 {
                                oldest_active_xid = narrow_xid;
                            }
                        }
                        oldest_active_xid
                    };
                    cp.oldestActiveXid = oldest_active_xid;
                } else {
                    cp.oldestActiveXid = xlog_checkpoint.oldestActiveXid;
                }
                // NB: We abuse the Checkpoint.redo field:
                //
                // - In PostgreSQL, the Checkpoint struct doesn't store the information
                //   of whether this is an online checkpoint or a shutdown checkpoint. It's
                //   stored in the XLOG info field of the WAL record, shutdown checkpoints
                //   use record type XLOG_CHECKPOINT_SHUTDOWN and online checkpoints use
                //   XLOG_CHECKPOINT_ONLINE. We don't store the original WAL record headers
                //   in the pageserver, however.
                //
                // - In PostgreSQL, the Checkpoint.redo field stores the *start* of the
                //   checkpoint record, if it's a shutdown checkpoint. But when we are
                //   starting from a shutdown checkpoint, the basebackup LSN is the *end*
                //   of the shutdown checkpoint WAL record. That makes it difficult to
                //   correctly detect whether we're starting from a shutdown record or
                //   not.
                //
                // To address both of those issues, we store 0 in the redo field if it's
                // an online checkpoint record, and the record's *end* LSN if it's a
                // shutdown checkpoint. We don't need the original redo pointer in neon,
                // because we don't perform WAL replay at startup anyway, so we can get
                // away with abusing the redo field like this.
                //
                // XXX: Ideally, we would persist the extra information in a more
                // explicit format, rather than repurpose the fields of the Postgres
                // struct like this. However, we already have persisted data like this,
                // so we need to maintain backwards compatibility.
                //
                // NB: We didn't originally have this convention, so there are still old
                // persisted records that didn't do this. Before, we didn't update the
                // persisted redo field at all. That means that old records have a bogus
                // redo pointer that points to some old value, from the checkpoint record
                // that was originally imported from the data directory. If it was a
                // project created in Neon, that means it points to the first checkpoint
                // after initdb. That's OK for our purposes: all such old checkpoints are
                // treated as old online checkpoints when the basebackup is created.
                cp.redo = if info == pg_constants::XLOG_CHECKPOINT_SHUTDOWN {
                    // Store the *end* LSN of the checkpoint record. Or to be precise,
                    // the start LSN of the *next* record, i.e. if the record ends
                    // exactly at page boundary, the redo LSN points to just after the
                    // page header on the next page.
                    lsn.into()
                } else {
                    Lsn::INVALID.into()
                };

                // Write a new checkpoint key-value pair on every checkpoint record, even
                // if nothing really changed. Not strictly required, but it seems nice to
                // have some trace of the checkpoint records in the layer files at the same
                // LSNs.
                self.checkpoint_modified = true;
            }
        });

        Ok(())
    }

    async fn ingest_logical_message_put(
        &mut self,
        put: PutLogicalMessage,
        modification: &mut DatadirModification<'_>,
        ctx: &RequestContext,
    ) -> Result<()> {
        let PutLogicalMessage { path, buf } = put;
        modification.put_file(path.as_str(), &buf, ctx).await
    }

    fn ingest_standby_record(&mut self, record: StandbyRecord) -> Result<()> {
        match record {
            StandbyRecord::RunningXacts(running_xacts) => {
                enum_pgversion_dispatch!(&mut self.checkpoint, CheckPoint, cp, {
                    cp.oldestActiveXid = running_xacts.oldest_running_xid;
                });

                self.checkpoint_modified = true;
            }
        }

        Ok(())
    }

    async fn ingest_replorigin_record(
        &mut self,
        record: ReploriginRecord,
        modification: &mut DatadirModification<'_>,
    ) -> Result<()> {
        match record {
            ReploriginRecord::Set(set) => {
                modification
                    .set_replorigin(set.node_id, set.remote_lsn)
                    .await?;
            }
            ReploriginRecord::Drop(drop) => {
                modification.drop_replorigin(drop.node_id).await?;
            }
        }

        Ok(())
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

    #[cfg(test)]
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
        let old_nblocks = modification.create_relation_if_required(rel, ctx).await?;

        if new_nblocks > old_nblocks {
            //info!("extending {} {} to {}", rel, old_nblocks, new_nblocks);
            modification.put_rel_extend(rel, new_nblocks, ctx).await?;

            let mut key = rel_block_to_key(rel, blknum);

            // fill the gap with zeros
            let mut gap_blocks_filled: u64 = 0;
            for gap_blknum in old_nblocks..blknum {
                key.field6 = gap_blknum;

                if self.shard.get_shard_number(&key) != self.shard.number {
                    continue;
                }

                modification.put_rel_page_image_zero(rel, gap_blknum)?;
                gap_blocks_filled += 1;
            }

            WAL_INGEST
                .gap_blocks_zeroed_on_rel_extend
                .inc_by(gap_blocks_filled);

            // Log something when relation extends cause use to fill gaps
            // with zero pages. Logging is rate limited per pg version to
            // avoid skewing.
            if gap_blocks_filled > 0 {
                use once_cell::sync::Lazy;
                use std::sync::Mutex;
                use utils::rate_limit::RateLimit;

                struct RateLimitPerPgVersion {
                    rate_limiters: [Lazy<Mutex<RateLimit>>; 4],
                }

                impl RateLimitPerPgVersion {
                    const fn new() -> Self {
                        Self {
                            rate_limiters: [const {
                                Lazy::new(|| Mutex::new(RateLimit::new(Duration::from_secs(30))))
                            }; 4],
                        }
                    }

                    const fn rate_limiter(
                        &self,
                        pg_version: u32,
                    ) -> Option<&Lazy<Mutex<RateLimit>>> {
                        const MIN_PG_VERSION: u32 = 14;
                        const MAX_PG_VERSION: u32 = 17;

                        if pg_version < MIN_PG_VERSION || pg_version > MAX_PG_VERSION {
                            return None;
                        }

                        Some(&self.rate_limiters[(pg_version - MIN_PG_VERSION) as usize])
                    }
                }

                static LOGGED: RateLimitPerPgVersion = RateLimitPerPgVersion::new();
                if let Some(rate_limiter) = LOGGED.rate_limiter(modification.tline.pg_version) {
                    if let Ok(mut locked) = rate_limiter.try_lock() {
                        locked.call(|| {
                            info!(
                                lsn=%modification.get_lsn(),
                                pg_version=%modification.tline.pg_version,
                                rel=%rel,
                                "Filled {} gap blocks on rel extend to {} from {}",
                                gap_blocks_filled,
                                new_nblocks,
                                old_nblocks);
                        });
                    }
                }
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
        if !self.shard.is_shard_zero() {
            return Ok(());
        }

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
                modification.put_slru_page_image_zero(kind, segno, gap_blknum)?;
            }
        }
        Ok(())
    }
}

/// Returns the size of the relation as of this modification, or None if the relation doesn't exist.
///
/// This is only accurate on shard 0. On other shards, it will return the size up to the highest
/// page number stored in the shard, or None if the shard does not have any pages for it.
async fn get_relsize(
    modification: &DatadirModification<'_>,
    rel: RelTag,
    ctx: &RequestContext,
) -> Result<Option<BlockNumber>, PageReconstructError> {
    if !modification
        .tline
        .get_rel_exists(rel, Version::Modified(modification), ctx)
        .await?
    {
        return Ok(None);
    }
    modification
        .tline
        .get_rel_size(rel, Version::Modified(modification), ctx)
        .await
        .map(Some)
}

#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::*;
    use crate::tenant::remote_timeline_client::{remote_initdb_archive_path, INITDB_PATH};
    use crate::tenant::storage_layer::IoConcurrency;
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

    #[tokio::test]
    async fn test_zeroed_checkpoint_decodes_correctly() -> Result<()> {
        for i in 14..=16 {
            dispatch_pgversion!(i, {
                pgv::CheckPoint::decode(&pgv::ZERO_CHECKPOINT)?;
            });
        }

        Ok(())
    }

    async fn init_walingest_test(tline: &Timeline, ctx: &RequestContext) -> Result<WalIngest> {
        let mut m = tline.begin_modification(Lsn(0x10));
        m.put_checkpoint(dispatch_pgversion!(
            tline.pg_version,
            pgv::ZERO_CHECKPOINT.clone()
        ))?;
        m.put_relmap_file(0, 111, Bytes::from(""), ctx).await?; // dummy relmapper file
        m.commit(ctx).await?;
        let walingest = WalIngest::new(tline, Lsn(0x10), ctx).await?;

        Ok(walingest)
    }

    #[tokio::test]
    async fn test_relsize() -> Result<()> {
        let (tenant, ctx) = TenantHarness::create("test_relsize").await?.load().await;
        let io_concurrency = IoConcurrency::spawn_for_test();
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

        let test_span = tracing::info_span!(parent: None, "test",
                                            tenant_id=%tline.tenant_shard_id.tenant_id,
                                            shard_id=%tline.tenant_shard_id.shard_slug(),
                                            timeline_id=%tline.timeline_id);

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
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    0,
                    Version::Lsn(Lsn(0x20)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 0 at 2")
        );

        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    0,
                    Version::Lsn(Lsn(0x30)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 0 at 3")
        );

        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    0,
                    Version::Lsn(Lsn(0x40)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 0 at 3")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    1,
                    Version::Lsn(Lsn(0x40)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 1 at 4")
        );

        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    0,
                    Version::Lsn(Lsn(0x50)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 0 at 3")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    1,
                    Version::Lsn(Lsn(0x50)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 1 at 4")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    2,
                    Version::Lsn(Lsn(0x50)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
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
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    0,
                    Version::Lsn(Lsn(0x60)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            test_img("foo blk 0 at 3")
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    1,
                    Version::Lsn(Lsn(0x60)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
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
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    2,
                    Version::Lsn(Lsn(0x50)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
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
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    0,
                    Version::Lsn(Lsn(0x70)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
                .await?,
            ZERO_PAGE
        );
        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    1,
                    Version::Lsn(Lsn(0x70)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
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
                    .get_rel_page_at_lsn(
                        TESTREL_A,
                        blk,
                        Version::Lsn(Lsn(0x80)),
                        &ctx,
                        io_concurrency.clone()
                    )
                    .instrument(test_span.clone())
                    .await?,
                ZERO_PAGE
            );
        }
        assert_eq!(
            tline
                .get_rel_page_at_lsn(
                    TESTREL_A,
                    1500,
                    Version::Lsn(Lsn(0x80)),
                    &ctx,
                    io_concurrency.clone()
                )
                .instrument(test_span.clone())
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
        let mut rel_drops = HashMap::new();
        rel_drops.insert((TESTREL_A.spcnode, TESTREL_A.dbnode), vec![TESTREL_A]);
        m.put_rel_drops(rel_drops, &ctx).await?;
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
        let io_concurrency = IoConcurrency::spawn_for_test();
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

        let test_span = tracing::info_span!(parent: None, "test",
                                            tenant_id=%tline.tenant_shard_id.tenant_id,
                                            shard_id=%tline.tenant_shard_id.shard_slug(),
                                            timeline_id=%tline.timeline_id);

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
                    .get_rel_page_at_lsn(
                        TESTREL_A,
                        blkno,
                        Version::Lsn(lsn),
                        &ctx,
                        io_concurrency.clone()
                    )
                    .instrument(test_span.clone())
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
                    .get_rel_page_at_lsn(
                        TESTREL_A,
                        blkno,
                        Version::Lsn(Lsn(0x60)),
                        &ctx,
                        io_concurrency.clone()
                    )
                    .instrument(test_span.clone())
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
                    .get_rel_page_at_lsn(
                        TESTREL_A,
                        blkno,
                        Version::Lsn(Lsn(0x50)),
                        &ctx,
                        io_concurrency.clone()
                    )
                    .instrument(test_span.clone())
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
                    .get_rel_page_at_lsn(
                        TESTREL_A,
                        blkno,
                        Version::Lsn(Lsn(0x80)),
                        &ctx,
                        io_concurrency.clone()
                    )
                    .instrument(test_span.clone())
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
        let span = harness
            .span()
            .in_scope(|| info_span!("timeline_span", timeline_id=%TIMELINE_ID));
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
        println!("decoding {} bytes", bytes.len() - xlogoff);

        // Decode and ingest wal. We process the wal in chunks because
        // that's what happens when we get bytes from safekeepers.
        for chunk in bytes[xlogoff..].chunks(50) {
            decoder.feed_bytes(chunk);
            while let Some((lsn, recdata)) = decoder.poll_decode().unwrap() {
                let interpreted = InterpretedWalRecord::from_bytes_filtered(
                    recdata,
                    &[*modification.tline.get_shard_identity()],
                    lsn,
                    modification.tline.pg_version,
                )
                .unwrap()
                .remove(modification.tline.get_shard_identity())
                .unwrap();

                walingest
                    .ingest_record(interpreted, &mut modification, &ctx)
                    .instrument(span.clone())
                    .await
                    .unwrap();
            }
            modification.commit(&ctx).await.unwrap();
        }

        let duration = started_at.elapsed();
        println!("done in {:?}", duration);
    }
}
