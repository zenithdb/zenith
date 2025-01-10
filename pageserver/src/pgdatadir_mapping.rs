//!
//! This provides an abstraction to store PostgreSQL relations and other files
//! in the key-value store that implements the Repository interface.
//!
//! (TODO: The line between PUT-functions here and walingest.rs is a bit blurry, as
//! walingest.rs handles a few things like implicit relation creation and extension.
//! Clarify that)
//!
use super::tenant::{PageReconstructError, Timeline};
use crate::aux_file;
use crate::context::RequestContext;
use crate::keyspace::{KeySpace, KeySpaceAccum};
use crate::metrics::{
    RELSIZE_CACHE_ENTRIES, RELSIZE_CACHE_HITS, RELSIZE_CACHE_MISSES, RELSIZE_CACHE_MISSES_OLD,
};
use crate::span::{
    debug_assert_current_span_has_tenant_and_timeline_id,
    debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id,
};
use crate::tenant::timeline::GetVectoredError;
use anyhow::{ensure, Context};
use bytes::{Buf, Bytes, BytesMut};
use enum_map::Enum;
use itertools::Itertools;
use pageserver_api::key::Key;
use pageserver_api::key::{
    dbdir_key_range, rel_block_to_key, rel_dir_to_key, rel_key_range, rel_size_to_key,
    relmap_file_key, repl_origin_key, repl_origin_key_range, slru_block_to_key, slru_dir_to_key,
    slru_segment_key_range, slru_segment_size_to_key, twophase_file_key, twophase_key_range,
    CompactKey, AUX_FILES_KEY, CHECKPOINT_KEY, CONTROLFILE_KEY, DBDIR_KEY, TWOPHASEDIR_KEY,
};
use pageserver_api::keyspace::SparseKeySpace;
use pageserver_api::record::NeonWalRecord;
use pageserver_api::reltag::{BlockNumber, RelTag, SlruKind};
use pageserver_api::shard::ShardIdentity;
use pageserver_api::value::Value;
use postgres_ffi::relfile_utils::{FSM_FORKNUM, VISIBILITYMAP_FORKNUM};
use postgres_ffi::BLCKSZ;
use postgres_ffi::{Oid, RepOriginId, TimestampTz, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, BTreeMap, HashMap, HashSet};
use std::ops::ControlFlow;
use std::ops::Range;
use strum::IntoEnumIterator;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use utils::bin_ser::DeserializeError;
use utils::pausable_failpoint;
use utils::{bin_ser::BeSer, lsn::Lsn};
use wal_decoder::serialized_batch::SerializedValueBatch;

/// Max delta records appended to the AUX_FILES_KEY (for aux v1). The write path will write a full image once this threshold is reached.
pub const MAX_AUX_FILE_DELTAS: usize = 1024;

/// Max number of aux-file-related delta layers. The compaction will create a new image layer once this threshold is reached.
pub const MAX_AUX_FILE_V2_DELTAS: usize = 16;

#[derive(Debug)]
pub enum LsnForTimestamp {
    /// Found commits both before and after the given timestamp
    Present(Lsn),

    /// Found no commits after the given timestamp, this means
    /// that the newest data in the branch is older than the given
    /// timestamp.
    ///
    /// All commits <= LSN happened before the given timestamp
    Future(Lsn),

    /// The queried timestamp is past our horizon we look back at (PITR)
    ///
    /// All commits > LSN happened after the given timestamp,
    /// but any commits < LSN might have happened before or after
    /// the given timestamp. We don't know because no data before
    /// the given lsn is available.
    Past(Lsn),

    /// We have found no commit with a timestamp,
    /// so we can't return anything meaningful.
    ///
    /// The associated LSN is the lower bound value we can safely
    /// create branches on, but no statement is made if it is
    /// older or newer than the timestamp.
    ///
    /// This variant can e.g. be returned right after a
    /// cluster import.
    NoData(Lsn),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CalculateLogicalSizeError {
    #[error("cancelled")]
    Cancelled,

    /// Something went wrong while reading the metadata we use to calculate logical size
    /// Note that cancellation variants of `PageReconstructError` are transformed to [`Self::Cancelled`]
    /// in the `From` implementation for this variant.
    #[error(transparent)]
    PageRead(PageReconstructError),

    /// Something went wrong deserializing metadata that we read to calculate logical size
    #[error("decode error: {0}")]
    Decode(#[from] DeserializeError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CollectKeySpaceError {
    #[error(transparent)]
    Decode(#[from] DeserializeError),
    #[error(transparent)]
    PageRead(PageReconstructError),
    #[error("cancelled")]
    Cancelled,
}

impl From<PageReconstructError> for CollectKeySpaceError {
    fn from(err: PageReconstructError) -> Self {
        match err {
            PageReconstructError::Cancelled => Self::Cancelled,
            err => Self::PageRead(err),
        }
    }
}

impl From<PageReconstructError> for CalculateLogicalSizeError {
    fn from(pre: PageReconstructError) -> Self {
        match pre {
            PageReconstructError::Cancelled => Self::Cancelled,
            _ => Self::PageRead(pre),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RelationError {
    #[error("Relation Already Exists")]
    AlreadyExists,
    #[error("invalid relnode")]
    InvalidRelnode,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

///
/// This impl provides all the functionality to store PostgreSQL relations, SLRUs,
/// and other special kinds of files, in a versioned key-value store. The
/// Timeline struct provides the key-value store.
///
/// This is a separate impl, so that we can easily include all these functions in a Timeline
/// implementation, and might be moved into a separate struct later.
impl Timeline {
    /// Start ingesting a WAL record, or other atomic modification of
    /// the timeline.
    ///
    /// This provides a transaction-like interface to perform a bunch
    /// of modifications atomically.
    ///
    /// To ingest a WAL record, call begin_modification(lsn) to get a
    /// DatadirModification object. Use the functions in the object to
    /// modify the repository state, updating all the pages and metadata
    /// that the WAL record affects. When you're done, call commit() to
    /// commit the changes.
    ///
    /// Lsn stored in modification is advanced by `ingest_record` and
    /// is used by `commit()` to update `last_record_lsn`.
    ///
    /// Calling commit() will flush all the changes and reset the state,
    /// so the `DatadirModification` struct can be reused to perform the next modification.
    ///
    /// Note that any pending modifications you make through the
    /// modification object won't be visible to calls to the 'get' and list
    /// functions of the timeline until you finish! And if you update the
    /// same page twice, the last update wins.
    ///
    pub fn begin_modification(&self, lsn: Lsn) -> DatadirModification
    where
        Self: Sized,
    {
        DatadirModification {
            tline: self,
            pending_lsns: Vec::new(),
            pending_metadata_pages: HashMap::new(),
            pending_data_batch: None,
            pending_deletions: Vec::new(),
            pending_nblocks: 0,
            pending_directory_entries: Vec::new(),
            pending_metadata_bytes: 0,
            lsn,
        }
    }

    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page version.
    pub(crate) async fn get_rel_page_at_lsn(
        &self,
        tag: RelTag,
        blknum: BlockNumber,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        match version {
            Version::Lsn(effective_lsn) => {
                let pages: smallvec::SmallVec<[_; 1]> = smallvec::smallvec![(tag, blknum)];
                let res = self
                    .get_rel_page_at_lsn_batched(
                        pages.iter().map(|(tag, blknum)| (tag, blknum)),
                        effective_lsn,
                        ctx,
                    )
                    .await;
                assert_eq!(res.len(), 1);
                res.into_iter().next().unwrap()
            }
            Version::Modified(modification) => {
                if tag.relnode == 0 {
                    return Err(PageReconstructError::Other(
                        RelationError::InvalidRelnode.into(),
                    ));
                }

                let nblocks = self.get_rel_size(tag, version, ctx).await?;
                if blknum >= nblocks {
                    debug!(
                        "read beyond EOF at {} blk {} at {}, size is {}: returning all-zeros page",
                        tag,
                        blknum,
                        version.get_lsn(),
                        nblocks
                    );
                    return Ok(ZERO_PAGE.clone());
                }

                let key = rel_block_to_key(tag, blknum);
                modification.get(key, ctx).await
            }
        }
    }

    /// Like [`Self::get_rel_page_at_lsn`], but returns a batch of pages.
    ///
    /// The ordering of the returned vec corresponds to the ordering of `pages`.
    pub(crate) async fn get_rel_page_at_lsn_batched(
        &self,
        pages: impl ExactSizeIterator<Item = (&RelTag, &BlockNumber)>,
        effective_lsn: Lsn,
        ctx: &RequestContext,
    ) -> Vec<Result<Bytes, PageReconstructError>> {
        debug_assert_current_span_has_tenant_and_timeline_id();

        let mut slots_filled = 0;
        let page_count = pages.len();

        // Would be nice to use smallvec here but it doesn't provide the spare_capacity_mut() API.
        let mut result = Vec::with_capacity(pages.len());
        let result_slots = result.spare_capacity_mut();

        let mut keys_slots: BTreeMap<Key, smallvec::SmallVec<[usize; 1]>> = BTreeMap::default();
        for (response_slot_idx, (tag, blknum)) in pages.enumerate() {
            if tag.relnode == 0 {
                result_slots[response_slot_idx].write(Err(PageReconstructError::Other(
                    RelationError::InvalidRelnode.into(),
                )));

                slots_filled += 1;
                continue;
            }

            let nblocks = match self
                .get_rel_size(*tag, Version::Lsn(effective_lsn), ctx)
                .await
            {
                Ok(nblocks) => nblocks,
                Err(err) => {
                    result_slots[response_slot_idx].write(Err(err));
                    slots_filled += 1;
                    continue;
                }
            };

            if *blknum >= nblocks {
                debug!(
                    "read beyond EOF at {} blk {} at {}, size is {}: returning all-zeros page",
                    tag, blknum, effective_lsn, nblocks
                );
                result_slots[response_slot_idx].write(Ok(ZERO_PAGE.clone()));
                slots_filled += 1;
                continue;
            }

            let key = rel_block_to_key(*tag, *blknum);

            let key_slots = keys_slots.entry(key).or_default();
            key_slots.push(response_slot_idx);
        }

        let keyspace = {
            // add_key requires monotonicity
            let mut acc = KeySpaceAccum::new();
            for key in keys_slots
                .keys()
                // in fact it requires strong monotonicity
                .dedup()
            {
                acc.add_key(*key);
            }
            acc.to_keyspace()
        };

        match self.get_vectored(keyspace, effective_lsn, ctx).await {
            Ok(results) => {
                for (key, res) in results {
                    let mut key_slots = keys_slots.remove(&key).unwrap().into_iter();
                    let first_slot = key_slots.next().unwrap();

                    for slot in key_slots {
                        let clone = match &res {
                            Ok(buf) => Ok(buf.clone()),
                            Err(err) => Err(match err {
                                PageReconstructError::Cancelled => {
                                    PageReconstructError::Cancelled
                                }

                                x @ PageReconstructError::Other(_) |
                                x @ PageReconstructError::AncestorLsnTimeout(_) |
                                x @ PageReconstructError::WalRedo(_) |
                                x @ PageReconstructError::MissingKey(_) => {
                                    PageReconstructError::Other(anyhow::anyhow!("there was more than one request for this key in the batch, error logged once: {x:?}"))
                                },
                            }),
                        };

                        result_slots[slot].write(clone);
                        slots_filled += 1;
                    }

                    result_slots[first_slot].write(res);
                    slots_filled += 1;
                }
            }
            Err(err) => {
                // this cannot really happen because get_vectored only errors globally on invalid LSN or too large batch size
                // (We enforce the max batch size outside of this function, in the code that constructs the batch request.)
                for slot in keys_slots.values().flatten() {
                    // this whole `match` is a lot like `From<GetVectoredError> for PageReconstructError`
                    // but without taking ownership of the GetVectoredError
                    let err = match &err {
                        GetVectoredError::Cancelled => {
                            Err(PageReconstructError::Cancelled)
                        }
                        // TODO: restructure get_vectored API to make this error per-key
                        GetVectoredError::MissingKey(err) => {
                            Err(PageReconstructError::Other(anyhow::anyhow!("whole vectored get request failed because one or more of the requested keys were missing: {err:?}")))
                        }
                        // TODO: restructure get_vectored API to make this error per-key
                        GetVectoredError::GetReadyAncestorError(err) => {
                            Err(PageReconstructError::Other(anyhow::anyhow!("whole vectored get request failed because one or more key required ancestor that wasn't ready: {err:?}")))
                        }
                        // TODO: restructure get_vectored API to make this error per-key
                        GetVectoredError::Other(err) => {
                            Err(PageReconstructError::Other(
                                anyhow::anyhow!("whole vectored get request failed: {err:?}"),
                            ))
                        }
                        // TODO: we can prevent this error class by moving this check into the type system
                        GetVectoredError::InvalidLsn(e) => {
                            Err(anyhow::anyhow!("invalid LSN: {e:?}").into())
                        }
                        // NB: this should never happen in practice because we limit MAX_GET_VECTORED_KEYS
                        // TODO: we can prevent this error class by moving this check into the type system
                        GetVectoredError::Oversized(err) => {
                            Err(anyhow::anyhow!(
                                "batching oversized: {err:?}"
                            )
                            .into())
                        }
                    };

                    result_slots[*slot].write(err);
                }

                slots_filled += keys_slots.values().map(|slots| slots.len()).sum::<usize>();
            }
        };

        assert_eq!(slots_filled, page_count);
        // SAFETY:
        // 1. `result` and any of its uninint members are not read from until this point
        // 2. The length below is tracked at run-time and matches the number of requested pages.
        unsafe {
            result.set_len(page_count);
        }

        result
    }

    /// Get size of a database in blocks. This is only accurate on shard 0. It will undercount on
    /// other shards, by only accounting for relations the shard has pages for, and only accounting
    /// for pages up to the highest page number it has stored.
    pub(crate) async fn get_db_size(
        &self,
        spcnode: Oid,
        dbnode: Oid,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<usize, PageReconstructError> {
        let mut total_blocks = 0;

        let rels = self.list_rels(spcnode, dbnode, version, ctx).await?;

        for rel in rels {
            let n_blocks = self.get_rel_size(rel, version, ctx).await?;
            total_blocks += n_blocks as usize;
        }
        Ok(total_blocks)
    }

    /// Get size of a relation file. The relation must exist, otherwise an error is returned.
    ///
    /// This is only accurate on shard 0. On other shards, it will return the size up to the highest
    /// page number stored in the shard.
    pub(crate) async fn get_rel_size(
        &self,
        tag: RelTag,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<BlockNumber, PageReconstructError> {
        if tag.relnode == 0 {
            return Err(PageReconstructError::Other(
                RelationError::InvalidRelnode.into(),
            ));
        }

        if let Some(nblocks) = self.get_cached_rel_size(&tag, version.get_lsn()) {
            return Ok(nblocks);
        }

        if (tag.forknum == FSM_FORKNUM || tag.forknum == VISIBILITYMAP_FORKNUM)
            && !self.get_rel_exists(tag, version, ctx).await?
        {
            // FIXME: Postgres sometimes calls smgrcreate() to create
            // FSM, and smgrnblocks() on it immediately afterwards,
            // without extending it.  Tolerate that by claiming that
            // any non-existent FSM fork has size 0.
            return Ok(0);
        }

        let key = rel_size_to_key(tag);
        let mut buf = version.get(self, key, ctx).await?;
        let nblocks = buf.get_u32_le();

        self.update_cached_rel_size(tag, version.get_lsn(), nblocks);

        Ok(nblocks)
    }

    /// Does the relation exist?
    ///
    /// Only shard 0 has a full view of the relations. Other shards only know about relations that
    /// the shard stores pages for.
    pub(crate) async fn get_rel_exists(
        &self,
        tag: RelTag,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<bool, PageReconstructError> {
        if tag.relnode == 0 {
            return Err(PageReconstructError::Other(
                RelationError::InvalidRelnode.into(),
            ));
        }

        // first try to lookup relation in cache
        if let Some(_nblocks) = self.get_cached_rel_size(&tag, version.get_lsn()) {
            return Ok(true);
        }
        // then check if the database was already initialized.
        // get_rel_exists can be called before dbdir is created.
        let buf = version.get(self, DBDIR_KEY, ctx).await?;
        let dbdirs = DbDirectory::des(&buf)?.dbdirs;
        if !dbdirs.contains_key(&(tag.spcnode, tag.dbnode)) {
            return Ok(false);
        }
        // fetch directory listing
        let key = rel_dir_to_key(tag.spcnode, tag.dbnode);
        let buf = version.get(self, key, ctx).await?;

        let dir = RelDirectory::des(&buf)?;
        Ok(dir.rels.contains(&(tag.relnode, tag.forknum)))
    }

    /// Get a list of all existing relations in given tablespace and database.
    ///
    /// Only shard 0 has a full view of the relations. Other shards only know about relations that
    /// the shard stores pages for.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    pub(crate) async fn list_rels(
        &self,
        spcnode: Oid,
        dbnode: Oid,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<HashSet<RelTag>, PageReconstructError> {
        // fetch directory listing
        let key = rel_dir_to_key(spcnode, dbnode);
        let buf = version.get(self, key, ctx).await?;

        let dir = RelDirectory::des(&buf)?;
        let rels: HashSet<RelTag> =
            HashSet::from_iter(dir.rels.iter().map(|(relnode, forknum)| RelTag {
                spcnode,
                dbnode,
                relnode: *relnode,
                forknum: *forknum,
            }));

        Ok(rels)
    }

    /// Get the whole SLRU segment
    pub(crate) async fn get_slru_segment(
        &self,
        kind: SlruKind,
        segno: u32,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        assert!(self.tenant_shard_id.is_shard_zero());
        let n_blocks = self
            .get_slru_segment_size(kind, segno, Version::Lsn(lsn), ctx)
            .await?;
        let mut segment = BytesMut::with_capacity(n_blocks as usize * BLCKSZ as usize);
        for blkno in 0..n_blocks {
            let block = self
                .get_slru_page_at_lsn(kind, segno, blkno, lsn, ctx)
                .await?;
            segment.extend_from_slice(&block[..BLCKSZ as usize]);
        }
        Ok(segment.freeze())
    }

    /// Look up given SLRU page version.
    pub(crate) async fn get_slru_page_at_lsn(
        &self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        assert!(self.tenant_shard_id.is_shard_zero());
        let key = slru_block_to_key(kind, segno, blknum);
        self.get(key, lsn, ctx).await
    }

    /// Get size of an SLRU segment
    pub(crate) async fn get_slru_segment_size(
        &self,
        kind: SlruKind,
        segno: u32,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<BlockNumber, PageReconstructError> {
        assert!(self.tenant_shard_id.is_shard_zero());
        let key = slru_segment_size_to_key(kind, segno);
        let mut buf = version.get(self, key, ctx).await?;
        Ok(buf.get_u32_le())
    }

    /// Get size of an SLRU segment
    pub(crate) async fn get_slru_segment_exists(
        &self,
        kind: SlruKind,
        segno: u32,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<bool, PageReconstructError> {
        assert!(self.tenant_shard_id.is_shard_zero());
        // fetch directory listing
        let key = slru_dir_to_key(kind);
        let buf = version.get(self, key, ctx).await?;

        let dir = SlruSegmentDirectory::des(&buf)?;
        Ok(dir.segments.contains(&segno))
    }

    /// Locate LSN, such that all transactions that committed before
    /// 'search_timestamp' are visible, but nothing newer is.
    ///
    /// This is not exact. Commit timestamps are not guaranteed to be ordered,
    /// so it's not well defined which LSN you get if there were multiple commits
    /// "in flight" at that point in time.
    ///
    pub(crate) async fn find_lsn_for_timestamp(
        &self,
        search_timestamp: TimestampTz,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<LsnForTimestamp, PageReconstructError> {
        pausable_failpoint!("find-lsn-for-timestamp-pausable");

        let gc_cutoff_lsn_guard = self.get_latest_gc_cutoff_lsn();
        // We use this method to figure out the branching LSN for the new branch, but the
        // GC cutoff could be before the branching point and we cannot create a new branch
        // with LSN < `ancestor_lsn`. Thus, pick the maximum of these two to be
        // on the safe side.
        let min_lsn = std::cmp::max(*gc_cutoff_lsn_guard, self.get_ancestor_lsn());
        let max_lsn = self.get_last_record_lsn();

        // LSNs are always 8-byte aligned. low/mid/high represent the
        // LSN divided by 8.
        let mut low = min_lsn.0 / 8;
        let mut high = max_lsn.0 / 8 + 1;

        let mut found_smaller = false;
        let mut found_larger = false;

        while low < high {
            if cancel.is_cancelled() {
                return Err(PageReconstructError::Cancelled);
            }
            // cannot overflow, high and low are both smaller than u64::MAX / 2
            let mid = (high + low) / 2;

            let cmp = match self
                .is_latest_commit_timestamp_ge_than(
                    search_timestamp,
                    Lsn(mid * 8),
                    &mut found_smaller,
                    &mut found_larger,
                    ctx,
                )
                .await
            {
                Ok(res) => res,
                Err(PageReconstructError::MissingKey(e)) => {
                    warn!("Missing key while find_lsn_for_timestamp. Either we might have already garbage-collected that data or the key is really missing. Last error: {:#}", e);
                    // Return that we didn't find any requests smaller than the LSN, and logging the error.
                    return Ok(LsnForTimestamp::Past(min_lsn));
                }
                Err(e) => return Err(e),
            };

            if cmp {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        // If `found_smaller == true`, `low = t + 1` where `t` is the target LSN,
        // so the LSN of the last commit record before or at `search_timestamp`.
        // Remove one from `low` to get `t`.
        //
        // FIXME: it would be better to get the LSN of the previous commit.
        // Otherwise, if you restore to the returned LSN, the database will
        // include physical changes from later commits that will be marked
        // as aborted, and will need to be vacuumed away.
        let commit_lsn = Lsn((low - 1) * 8);
        match (found_smaller, found_larger) {
            (false, false) => {
                // This can happen if no commit records have been processed yet, e.g.
                // just after importing a cluster.
                Ok(LsnForTimestamp::NoData(min_lsn))
            }
            (false, true) => {
                // Didn't find any commit timestamps smaller than the request
                Ok(LsnForTimestamp::Past(min_lsn))
            }
            (true, _) if commit_lsn < min_lsn => {
                // the search above did set found_smaller to true but it never increased the lsn.
                // Then, low is still the old min_lsn, and the subtraction above gave a value
                // below the min_lsn. We should never do that.
                Ok(LsnForTimestamp::Past(min_lsn))
            }
            (true, false) => {
                // Only found commits with timestamps smaller than the request.
                // It's still a valid case for branch creation, return it.
                // And `update_gc_info()` ignores LSN for a `LsnForTimestamp::Future`
                // case, anyway.
                Ok(LsnForTimestamp::Future(commit_lsn))
            }
            (true, true) => Ok(LsnForTimestamp::Present(commit_lsn)),
        }
    }

    /// Subroutine of find_lsn_for_timestamp(). Returns true, if there are any
    /// commits that committed after 'search_timestamp', at LSN 'probe_lsn'.
    ///
    /// Additionally, sets 'found_smaller'/'found_Larger, if encounters any commits
    /// with a smaller/larger timestamp.
    ///
    pub(crate) async fn is_latest_commit_timestamp_ge_than(
        &self,
        search_timestamp: TimestampTz,
        probe_lsn: Lsn,
        found_smaller: &mut bool,
        found_larger: &mut bool,
        ctx: &RequestContext,
    ) -> Result<bool, PageReconstructError> {
        self.map_all_timestamps(probe_lsn, ctx, |timestamp| {
            if timestamp >= search_timestamp {
                *found_larger = true;
                return ControlFlow::Break(true);
            } else {
                *found_smaller = true;
            }
            ControlFlow::Continue(())
        })
        .await
    }

    /// Obtain the possible timestamp range for the given lsn.
    ///
    /// If the lsn has no timestamps, returns None. returns `(min, max, median)` if it has timestamps.
    pub(crate) async fn get_timestamp_for_lsn(
        &self,
        probe_lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Option<TimestampTz>, PageReconstructError> {
        let mut max: Option<TimestampTz> = None;
        self.map_all_timestamps::<()>(probe_lsn, ctx, |timestamp| {
            if let Some(max_prev) = max {
                max = Some(max_prev.max(timestamp));
            } else {
                max = Some(timestamp);
            }
            ControlFlow::Continue(())
        })
        .await?;

        Ok(max)
    }

    /// Runs the given function on all the timestamps for a given lsn
    ///
    /// The return value is either given by the closure, or set to the `Default`
    /// impl's output.
    async fn map_all_timestamps<T: Default>(
        &self,
        probe_lsn: Lsn,
        ctx: &RequestContext,
        mut f: impl FnMut(TimestampTz) -> ControlFlow<T>,
    ) -> Result<T, PageReconstructError> {
        for segno in self
            .list_slru_segments(SlruKind::Clog, Version::Lsn(probe_lsn), ctx)
            .await?
        {
            let nblocks = self
                .get_slru_segment_size(SlruKind::Clog, segno, Version::Lsn(probe_lsn), ctx)
                .await?;
            for blknum in (0..nblocks).rev() {
                let clog_page = self
                    .get_slru_page_at_lsn(SlruKind::Clog, segno, blknum, probe_lsn, ctx)
                    .await?;

                if clog_page.len() == BLCKSZ as usize + 8 {
                    let mut timestamp_bytes = [0u8; 8];
                    timestamp_bytes.copy_from_slice(&clog_page[BLCKSZ as usize..]);
                    let timestamp = TimestampTz::from_be_bytes(timestamp_bytes);

                    match f(timestamp) {
                        ControlFlow::Break(b) => return Ok(b),
                        ControlFlow::Continue(()) => (),
                    }
                }
            }
        }
        Ok(Default::default())
    }

    pub(crate) async fn get_slru_keyspace(
        &self,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<KeySpace, PageReconstructError> {
        let mut accum = KeySpaceAccum::new();

        for kind in SlruKind::iter() {
            let mut segments: Vec<u32> = self
                .list_slru_segments(kind, version, ctx)
                .await?
                .into_iter()
                .collect();
            segments.sort_unstable();

            for seg in segments {
                let block_count = self.get_slru_segment_size(kind, seg, version, ctx).await?;

                accum.add_range(
                    slru_block_to_key(kind, seg, 0)..slru_block_to_key(kind, seg, block_count),
                );
            }
        }

        Ok(accum.to_keyspace())
    }

    /// Get a list of SLRU segments
    pub(crate) async fn list_slru_segments(
        &self,
        kind: SlruKind,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<HashSet<u32>, PageReconstructError> {
        // fetch directory entry
        let key = slru_dir_to_key(kind);

        let buf = version.get(self, key, ctx).await?;
        Ok(SlruSegmentDirectory::des(&buf)?.segments)
    }

    pub(crate) async fn get_relmap_file(
        &self,
        spcnode: Oid,
        dbnode: Oid,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        let key = relmap_file_key(spcnode, dbnode);

        let buf = version.get(self, key, ctx).await?;
        Ok(buf)
    }

    pub(crate) async fn list_dbdirs(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashMap<(Oid, Oid), bool>, PageReconstructError> {
        // fetch directory entry
        let buf = self.get(DBDIR_KEY, lsn, ctx).await?;

        Ok(DbDirectory::des(&buf)?.dbdirs)
    }

    pub(crate) async fn get_twophase_file(
        &self,
        xid: u64,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        let key = twophase_file_key(xid);
        let buf = self.get(key, lsn, ctx).await?;
        Ok(buf)
    }

    pub(crate) async fn list_twophase_files(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashSet<u64>, PageReconstructError> {
        // fetch directory entry
        let buf = self.get(TWOPHASEDIR_KEY, lsn, ctx).await?;

        if self.pg_version >= 17 {
            Ok(TwoPhaseDirectoryV17::des(&buf)?.xids)
        } else {
            Ok(TwoPhaseDirectory::des(&buf)?
                .xids
                .iter()
                .map(|x| u64::from(*x))
                .collect())
        }
    }

    pub(crate) async fn get_control_file(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        self.get(CONTROLFILE_KEY, lsn, ctx).await
    }

    pub(crate) async fn get_checkpoint(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        self.get(CHECKPOINT_KEY, lsn, ctx).await
    }

    async fn list_aux_files_v2(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashMap<String, Bytes>, PageReconstructError> {
        let kv = self
            .scan(KeySpace::single(Key::metadata_aux_key_range()), lsn, ctx)
            .await?;
        let mut result = HashMap::new();
        let mut sz = 0;
        for (_, v) in kv {
            let v = v?;
            let v = aux_file::decode_file_value_bytes(&v)
                .context("value decode")
                .map_err(PageReconstructError::Other)?;
            for (fname, content) in v {
                sz += fname.len();
                sz += content.len();
                result.insert(fname, content);
            }
        }
        self.aux_file_size_estimator.on_initial(sz);
        Ok(result)
    }

    pub(crate) async fn trigger_aux_file_size_computation(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<(), PageReconstructError> {
        self.list_aux_files_v2(lsn, ctx).await?;
        Ok(())
    }

    pub(crate) async fn list_aux_files(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashMap<String, Bytes>, PageReconstructError> {
        self.list_aux_files_v2(lsn, ctx).await
    }

    pub(crate) async fn get_replorigins(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashMap<RepOriginId, Lsn>, PageReconstructError> {
        let kv = self
            .scan(KeySpace::single(repl_origin_key_range()), lsn, ctx)
            .await?;
        let mut result = HashMap::new();
        for (k, v) in kv {
            let v = v?;
            let origin_id = k.field6 as RepOriginId;
            let origin_lsn = Lsn::des(&v).unwrap();
            if origin_lsn != Lsn::INVALID {
                result.insert(origin_id, origin_lsn);
            }
        }
        Ok(result)
    }

    /// Does the same as get_current_logical_size but counted on demand.
    /// Used to initialize the logical size tracking on startup.
    ///
    /// Only relation blocks are counted currently. That excludes metadata,
    /// SLRUs, twophase files etc.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    pub(crate) async fn get_current_logical_size_non_incremental(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<u64, CalculateLogicalSizeError> {
        debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id();

        // Fetch list of database dirs and iterate them
        let buf = self.get(DBDIR_KEY, lsn, ctx).await?;
        let dbdir = DbDirectory::des(&buf)?;

        let mut total_size: u64 = 0;
        for (spcnode, dbnode) in dbdir.dbdirs.keys() {
            for rel in self
                .list_rels(*spcnode, *dbnode, Version::Lsn(lsn), ctx)
                .await?
            {
                if self.cancel.is_cancelled() {
                    return Err(CalculateLogicalSizeError::Cancelled);
                }
                let relsize_key = rel_size_to_key(rel);
                let mut buf = self.get(relsize_key, lsn, ctx).await?;
                let relsize = buf.get_u32_le();

                total_size += relsize as u64;
            }
        }
        Ok(total_size * BLCKSZ as u64)
    }

    /// Get a KeySpace that covers all the Keys that are in use at AND below the given LSN. This is only used
    /// for gc-compaction.
    ///
    /// gc-compaction cannot use the same `collect_keyspace` function as the legacy compaction because it
    /// processes data at multiple LSNs and needs to be aware of the fact that some key ranges might need to
    /// be kept only for a specific range of LSN.
    ///
    /// Consider the case that the user created branches at LSN 10 and 20, where the user created a table A at
    /// LSN 10 and dropped that table at LSN 20. `collect_keyspace` at LSN 10 will return the key range
    /// corresponding to that table, while LSN 20 won't. The keyspace info at a single LSN is not enough to
    /// determine which keys to retain/drop for gc-compaction.
    ///
    /// For now, it only drops AUX-v1 keys. But in the future, the function will be extended to return the keyspace
    /// to be retained for each of the branch LSN.
    ///
    /// The return value is (dense keyspace, sparse keyspace).
    pub(crate) async fn collect_gc_compaction_keyspace(
        &self,
    ) -> Result<(KeySpace, SparseKeySpace), CollectKeySpaceError> {
        let metadata_key_begin = Key::metadata_key_range().start;
        let aux_v1_key = AUX_FILES_KEY;
        let dense_keyspace = KeySpace {
            ranges: vec![Key::MIN..aux_v1_key, aux_v1_key.next()..metadata_key_begin],
        };
        Ok((
            dense_keyspace,
            SparseKeySpace(KeySpace::single(Key::metadata_key_range())),
        ))
    }

    ///
    /// Get a KeySpace that covers all the Keys that are in use at the given LSN.
    /// Anything that's not listed maybe removed from the underlying storage (from
    /// that LSN forwards).
    ///
    /// The return value is (dense keyspace, sparse keyspace).
    pub(crate) async fn collect_keyspace(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<(KeySpace, SparseKeySpace), CollectKeySpaceError> {
        // Iterate through key ranges, greedily packing them into partitions
        let mut result = KeySpaceAccum::new();

        // The dbdir metadata always exists
        result.add_key(DBDIR_KEY);

        // Fetch list of database dirs and iterate them
        let dbdir = self.list_dbdirs(lsn, ctx).await?;
        let mut dbs: Vec<((Oid, Oid), bool)> = dbdir.into_iter().collect();

        dbs.sort_unstable_by(|(k_a, _), (k_b, _)| k_a.cmp(k_b));
        for ((spcnode, dbnode), has_relmap_file) in dbs {
            if has_relmap_file {
                result.add_key(relmap_file_key(spcnode, dbnode));
            }
            result.add_key(rel_dir_to_key(spcnode, dbnode));

            let mut rels: Vec<RelTag> = self
                .list_rels(spcnode, dbnode, Version::Lsn(lsn), ctx)
                .await?
                .into_iter()
                .collect();
            rels.sort_unstable();
            for rel in rels {
                let relsize_key = rel_size_to_key(rel);
                let mut buf = self.get(relsize_key, lsn, ctx).await?;
                let relsize = buf.get_u32_le();

                result.add_range(rel_block_to_key(rel, 0)..rel_block_to_key(rel, relsize));
                result.add_key(relsize_key);
            }
        }

        // Iterate SLRUs next
        if self.tenant_shard_id.is_shard_zero() {
            for kind in [
                SlruKind::Clog,
                SlruKind::MultiXactMembers,
                SlruKind::MultiXactOffsets,
            ] {
                let slrudir_key = slru_dir_to_key(kind);
                result.add_key(slrudir_key);
                let buf = self.get(slrudir_key, lsn, ctx).await?;
                let dir = SlruSegmentDirectory::des(&buf)?;
                let mut segments: Vec<u32> = dir.segments.iter().cloned().collect();
                segments.sort_unstable();
                for segno in segments {
                    let segsize_key = slru_segment_size_to_key(kind, segno);
                    let mut buf = self.get(segsize_key, lsn, ctx).await?;
                    let segsize = buf.get_u32_le();

                    result.add_range(
                        slru_block_to_key(kind, segno, 0)..slru_block_to_key(kind, segno, segsize),
                    );
                    result.add_key(segsize_key);
                }
            }
        }

        // Then pg_twophase
        result.add_key(TWOPHASEDIR_KEY);

        let mut xids: Vec<u64> = self
            .list_twophase_files(lsn, ctx)
            .await?
            .iter()
            .cloned()
            .collect();
        xids.sort_unstable();
        for xid in xids {
            result.add_key(twophase_file_key(xid));
        }

        result.add_key(CONTROLFILE_KEY);
        result.add_key(CHECKPOINT_KEY);

        // Add extra keyspaces in the test cases. Some test cases write keys into the storage without
        // creating directory keys. These test cases will add such keyspaces into `extra_test_dense_keyspace`
        // and the keys will not be garbage-colllected.
        #[cfg(test)]
        {
            let guard = self.extra_test_dense_keyspace.load();
            for kr in &guard.ranges {
                result.add_range(kr.clone());
            }
        }

        let dense_keyspace = result.to_keyspace();
        let sparse_keyspace = SparseKeySpace(KeySpace {
            ranges: vec![Key::metadata_aux_key_range(), repl_origin_key_range()],
        });

        if cfg!(debug_assertions) {
            // Verify if the sparse keyspaces are ordered and non-overlapping.

            // We do not use KeySpaceAccum for sparse_keyspace because we want to ensure each
            // category of sparse keys are split into their own image/delta files. If there
            // are overlapping keyspaces, they will be automatically merged by keyspace accum,
            // and we want the developer to keep the keyspaces separated.

            let ranges = &sparse_keyspace.0.ranges;

            // TODO: use a single overlaps_with across the codebase
            fn overlaps_with<T: Ord>(a: &Range<T>, b: &Range<T>) -> bool {
                !(a.end <= b.start || b.end <= a.start)
            }
            for i in 0..ranges.len() {
                for j in 0..i {
                    if overlaps_with(&ranges[i], &ranges[j]) {
                        panic!(
                            "overlapping sparse keyspace: {}..{} and {}..{}",
                            ranges[i].start, ranges[i].end, ranges[j].start, ranges[j].end
                        );
                    }
                }
            }
            for i in 1..ranges.len() {
                assert!(
                    ranges[i - 1].end <= ranges[i].start,
                    "unordered sparse keyspace: {}..{} and {}..{}",
                    ranges[i - 1].start,
                    ranges[i - 1].end,
                    ranges[i].start,
                    ranges[i].end
                );
            }
        }

        Ok((dense_keyspace, sparse_keyspace))
    }

    /// Get cached size of relation if it not updated after specified LSN
    pub fn get_cached_rel_size(&self, tag: &RelTag, lsn: Lsn) -> Option<BlockNumber> {
        let rel_size_cache = self.rel_size_cache.read().unwrap();
        if let Some((cached_lsn, nblocks)) = rel_size_cache.map.get(tag) {
            if lsn >= *cached_lsn {
                RELSIZE_CACHE_HITS.inc();
                return Some(*nblocks);
            }
            RELSIZE_CACHE_MISSES_OLD.inc();
        }
        RELSIZE_CACHE_MISSES.inc();
        None
    }

    /// Update cached relation size if there is no more recent update
    pub fn update_cached_rel_size(&self, tag: RelTag, lsn: Lsn, nblocks: BlockNumber) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();

        if lsn < rel_size_cache.complete_as_of {
            // Do not cache old values. It's safe to cache the size on read, as long as
            // the read was at an LSN since we started the WAL ingestion. Reasoning: we
            // never evict values from the cache, so if the relation size changed after
            // 'lsn', the new value is already in the cache.
            return;
        }

        match rel_size_cache.map.entry(tag) {
            hash_map::Entry::Occupied(mut entry) => {
                let cached_lsn = entry.get_mut();
                if lsn >= cached_lsn.0 {
                    *cached_lsn = (lsn, nblocks);
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert((lsn, nblocks));
                RELSIZE_CACHE_ENTRIES.inc();
            }
        }
    }

    /// Store cached relation size
    pub fn set_cached_rel_size(&self, tag: RelTag, lsn: Lsn, nblocks: BlockNumber) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        if rel_size_cache.map.insert(tag, (lsn, nblocks)).is_none() {
            RELSIZE_CACHE_ENTRIES.inc();
        }
    }

    /// Remove cached relation size
    pub fn remove_cached_rel_size(&self, tag: &RelTag) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        if rel_size_cache.map.remove(tag).is_some() {
            RELSIZE_CACHE_ENTRIES.dec();
        }
    }
}

/// DatadirModification represents an operation to ingest an atomic set of
/// updates to the repository.
///
/// It is created by the 'begin_record' function. It is called for each WAL
/// record, so that all the modifications by a one WAL record appear atomic.
pub struct DatadirModification<'a> {
    /// The timeline this modification applies to. You can access this to
    /// read the state, but note that any pending updates are *not* reflected
    /// in the state in 'tline' yet.
    pub tline: &'a Timeline,

    /// Current LSN of the modification
    lsn: Lsn,

    // The modifications are not applied directly to the underlying key-value store.
    // The put-functions add the modifications here, and they are flushed to the
    // underlying key-value store by the 'finish' function.
    pending_lsns: Vec<Lsn>,
    pending_deletions: Vec<(Range<Key>, Lsn)>,
    pending_nblocks: i64,

    /// Metadata writes, indexed by key so that they can be read from not-yet-committed modifications
    /// while ingesting subsequent records. See [`Self::is_data_key`] for the definition of 'metadata'.
    pending_metadata_pages: HashMap<CompactKey, Vec<(Lsn, usize, Value)>>,

    /// Data writes, ready to be flushed into an ephemeral layer. See [`Self::is_data_key`] for
    /// which keys are stored here.
    pending_data_batch: Option<SerializedValueBatch>,

    /// For special "directory" keys that store key-value maps, track the size of the map
    /// if it was updated in this modification.
    pending_directory_entries: Vec<(DirectoryKind, usize)>,

    /// An **approximation** of how many metadata bytes will be written to the EphemeralFile.
    pending_metadata_bytes: usize,
}

impl DatadirModification<'_> {
    // When a DatadirModification is committed, we do a monolithic serialization of all its contents.  WAL records can
    // contain multiple pages, so the pageserver's record-based batch size isn't sufficient to bound this allocation: we
    // additionally specify a limit on how much payload a DatadirModification may contain before it should be committed.
    pub(crate) const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

    /// Get the current lsn
    pub(crate) fn get_lsn(&self) -> Lsn {
        self.lsn
    }

    pub(crate) fn approx_pending_bytes(&self) -> usize {
        self.pending_data_batch
            .as_ref()
            .map_or(0, |b| b.buffer_size())
            + self.pending_metadata_bytes
    }

    pub(crate) fn has_dirty_data(&self) -> bool {
        self.pending_data_batch
            .as_ref()
            .is_some_and(|b| b.has_data())
    }

    /// Set the current lsn
    pub(crate) fn set_lsn(&mut self, lsn: Lsn) -> anyhow::Result<()> {
        ensure!(
            lsn >= self.lsn,
            "setting an older lsn {} than {} is not allowed",
            lsn,
            self.lsn
        );

        if lsn > self.lsn {
            self.pending_lsns.push(self.lsn);
            self.lsn = lsn;
        }
        Ok(())
    }

    /// In this context, 'metadata' means keys that are only read by the pageserver internally, and 'data' means
    /// keys that represent literal blocks that postgres can read.  So data includes relation blocks and
    /// SLRU blocks, which are read directly by postgres, and everything else is considered metadata.
    ///
    /// The distinction is important because data keys are handled on a fast path where dirty writes are
    /// not readable until this modification is committed, whereas metadata keys are visible for read
    /// via [`Self::get`] as soon as their record has been ingested.
    fn is_data_key(key: &Key) -> bool {
        key.is_rel_block_key() || key.is_slru_block_key()
    }

    /// Initialize a completely new repository.
    ///
    /// This inserts the directory metadata entries that are assumed to
    /// always exist.
    pub fn init_empty(&mut self) -> anyhow::Result<()> {
        let buf = DbDirectory::ser(&DbDirectory {
            dbdirs: HashMap::new(),
        })?;
        self.pending_directory_entries.push((DirectoryKind::Db, 0));
        self.put(DBDIR_KEY, Value::Image(buf.into()));

        let buf = if self.tline.pg_version >= 17 {
            TwoPhaseDirectoryV17::ser(&TwoPhaseDirectoryV17 {
                xids: HashSet::new(),
            })
        } else {
            TwoPhaseDirectory::ser(&TwoPhaseDirectory {
                xids: HashSet::new(),
            })
        }?;
        self.pending_directory_entries
            .push((DirectoryKind::TwoPhase, 0));
        self.put(TWOPHASEDIR_KEY, Value::Image(buf.into()));

        let buf: Bytes = SlruSegmentDirectory::ser(&SlruSegmentDirectory::default())?.into();
        let empty_dir = Value::Image(buf);

        // Initialize SLRUs on shard 0 only: creating these on other shards would be
        // harmless but they'd just be dropped on later compaction.
        if self.tline.tenant_shard_id.is_shard_zero() {
            self.put(slru_dir_to_key(SlruKind::Clog), empty_dir.clone());
            self.pending_directory_entries
                .push((DirectoryKind::SlruSegment(SlruKind::Clog), 0));
            self.put(
                slru_dir_to_key(SlruKind::MultiXactMembers),
                empty_dir.clone(),
            );
            self.pending_directory_entries
                .push((DirectoryKind::SlruSegment(SlruKind::Clog), 0));
            self.put(slru_dir_to_key(SlruKind::MultiXactOffsets), empty_dir);
            self.pending_directory_entries
                .push((DirectoryKind::SlruSegment(SlruKind::MultiXactOffsets), 0));
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn init_empty_test_timeline(&mut self) -> anyhow::Result<()> {
        self.init_empty()?;
        self.put_control_file(bytes::Bytes::from_static(
            b"control_file contents do not matter",
        ))
        .context("put_control_file")?;
        self.put_checkpoint(bytes::Bytes::from_static(
            b"checkpoint_file contents do not matter",
        ))
        .context("put_checkpoint_file")?;
        Ok(())
    }

    /// Creates a relation if it is not already present.
    /// Returns the current size of the relation
    pub(crate) async fn create_relation_if_required(
        &mut self,
        rel: RelTag,
        ctx: &RequestContext,
    ) -> Result<u32, PageReconstructError> {
        // Get current size and put rel creation if rel doesn't exist
        //
        // NOTE: we check the cache first even though get_rel_exists and get_rel_size would
        //       check the cache too. This is because eagerly checking the cache results in
        //       less work overall and 10% better performance. It's more work on cache miss
        //       but cache miss is rare.
        if let Some(nblocks) = self.tline.get_cached_rel_size(&rel, self.get_lsn()) {
            Ok(nblocks)
        } else if !self
            .tline
            .get_rel_exists(rel, Version::Modified(self), ctx)
            .await?
        {
            // create it with 0 size initially, the logic below will extend it
            self.put_rel_creation(rel, 0, ctx)
                .await
                .context("Relation Error")?;
            Ok(0)
        } else {
            self.tline
                .get_rel_size(rel, Version::Modified(self), ctx)
                .await
        }
    }

    /// Given a block number for a relation (which represents a newly written block),
    /// the previous block count of the relation, and the shard info, find the gaps
    /// that were created by the newly written block if any.
    fn find_gaps(
        rel: RelTag,
        blkno: u32,
        previous_nblocks: u32,
        shard: &ShardIdentity,
    ) -> Option<KeySpace> {
        let mut key = rel_block_to_key(rel, blkno);
        let mut gap_accum = None;

        for gap_blkno in previous_nblocks..blkno {
            key.field6 = gap_blkno;

            if shard.get_shard_number(&key) != shard.number {
                continue;
            }

            gap_accum
                .get_or_insert_with(KeySpaceAccum::new)
                .add_key(key);
        }

        gap_accum.map(|accum| accum.to_keyspace())
    }

    pub async fn ingest_batch(
        &mut self,
        mut batch: SerializedValueBatch,
        // TODO(vlad): remove this argument and replace the shard check with is_key_local
        shard: &ShardIdentity,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let mut gaps_at_lsns = Vec::default();

        for meta in batch.metadata.iter() {
            let (rel, blkno) = Key::from_compact(meta.key()).to_rel_block()?;
            let new_nblocks = blkno + 1;

            let old_nblocks = self.create_relation_if_required(rel, ctx).await?;
            if new_nblocks > old_nblocks {
                self.put_rel_extend(rel, new_nblocks, ctx).await?;
            }

            if let Some(gaps) = Self::find_gaps(rel, blkno, old_nblocks, shard) {
                gaps_at_lsns.push((gaps, meta.lsn()));
            }
        }

        if !gaps_at_lsns.is_empty() {
            batch.zero_gaps(gaps_at_lsns);
        }

        match self.pending_data_batch.as_mut() {
            Some(pending_batch) => {
                pending_batch.extend(batch);
            }
            None if batch.has_data() => {
                self.pending_data_batch = Some(batch);
            }
            None => {
                // Nothing to initialize the batch with
            }
        }

        Ok(())
    }

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// NOTE: this will *not* implicitly extend the relation, if the page is beyond the
    /// current end-of-file. It's up to the caller to check that the relation size
    /// matches the blocks inserted!
    pub fn put_rel_wal_record(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
        rec: NeonWalRecord,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        self.put(rel_block_to_key(rel, blknum), Value::WalRecord(rec));
        Ok(())
    }

    // Same, but for an SLRU.
    pub fn put_slru_wal_record(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        rec: NeonWalRecord,
    ) -> anyhow::Result<()> {
        if !self.tline.tenant_shard_id.is_shard_zero() {
            return Ok(());
        }

        self.put(
            slru_block_to_key(kind, segno, blknum),
            Value::WalRecord(rec),
        );
        Ok(())
    }

    /// Like put_wal_record, but with ready-made image of the page.
    pub fn put_rel_page_image(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
        img: Bytes,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        let key = rel_block_to_key(rel, blknum);
        if !key.is_valid_key_on_write_path() {
            anyhow::bail!(
                "the request contains data not supported by pageserver at {}",
                key
            );
        }
        self.put(rel_block_to_key(rel, blknum), Value::Image(img));
        Ok(())
    }

    pub fn put_slru_page_image(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        img: Bytes,
    ) -> anyhow::Result<()> {
        assert!(self.tline.tenant_shard_id.is_shard_zero());

        let key = slru_block_to_key(kind, segno, blknum);
        if !key.is_valid_key_on_write_path() {
            anyhow::bail!(
                "the request contains data not supported by pageserver at {}",
                key
            );
        }
        self.put(key, Value::Image(img));
        Ok(())
    }

    pub(crate) fn put_rel_page_image_zero(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        let key = rel_block_to_key(rel, blknum);
        if !key.is_valid_key_on_write_path() {
            anyhow::bail!(
                "the request contains data not supported by pageserver: {} @ {}",
                key,
                self.lsn
            );
        }

        let batch = self
            .pending_data_batch
            .get_or_insert_with(SerializedValueBatch::default);

        batch.put(key.to_compact(), Value::Image(ZERO_PAGE.clone()), self.lsn);

        Ok(())
    }

    pub(crate) fn put_slru_page_image_zero(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
    ) -> anyhow::Result<()> {
        assert!(self.tline.tenant_shard_id.is_shard_zero());
        let key = slru_block_to_key(kind, segno, blknum);
        if !key.is_valid_key_on_write_path() {
            anyhow::bail!(
                "the request contains data not supported by pageserver: {} @ {}",
                key,
                self.lsn
            );
        }

        let batch = self
            .pending_data_batch
            .get_or_insert_with(SerializedValueBatch::default);

        batch.put(key.to_compact(), Value::Image(ZERO_PAGE.clone()), self.lsn);

        Ok(())
    }

    /// Store a relmapper file (pg_filenode.map) in the repository
    pub async fn put_relmap_file(
        &mut self,
        spcnode: Oid,
        dbnode: Oid,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Add it to the directory (if it doesn't exist already)
        let buf = self.get(DBDIR_KEY, ctx).await?;
        let mut dbdir = DbDirectory::des(&buf)?;

        let r = dbdir.dbdirs.insert((spcnode, dbnode), true);
        if r.is_none() || r == Some(false) {
            // The dbdir entry didn't exist, or it contained a
            // 'false'. The 'insert' call already updated it with
            // 'true', now write the updated 'dbdirs' map back.
            let buf = DbDirectory::ser(&dbdir)?;
            self.put(DBDIR_KEY, Value::Image(buf.into()));
        }
        if r.is_none() {
            // Create RelDirectory
            let buf = RelDirectory::ser(&RelDirectory {
                rels: HashSet::new(),
            })?;
            self.pending_directory_entries.push((DirectoryKind::Rel, 0));
            self.put(
                rel_dir_to_key(spcnode, dbnode),
                Value::Image(Bytes::from(buf)),
            );
        }

        self.put(relmap_file_key(spcnode, dbnode), Value::Image(img));
        Ok(())
    }

    pub async fn put_twophase_file(
        &mut self,
        xid: u64,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Add it to the directory entry
        let dirbuf = self.get(TWOPHASEDIR_KEY, ctx).await?;
        let newdirbuf = if self.tline.pg_version >= 17 {
            let mut dir = TwoPhaseDirectoryV17::des(&dirbuf)?;
            if !dir.xids.insert(xid) {
                anyhow::bail!("twophase file for xid {} already exists", xid);
            }
            self.pending_directory_entries
                .push((DirectoryKind::TwoPhase, dir.xids.len()));
            Bytes::from(TwoPhaseDirectoryV17::ser(&dir)?)
        } else {
            let xid = xid as u32;
            let mut dir = TwoPhaseDirectory::des(&dirbuf)?;
            if !dir.xids.insert(xid) {
                anyhow::bail!("twophase file for xid {} already exists", xid);
            }
            self.pending_directory_entries
                .push((DirectoryKind::TwoPhase, dir.xids.len()));
            Bytes::from(TwoPhaseDirectory::ser(&dir)?)
        };
        self.put(TWOPHASEDIR_KEY, Value::Image(newdirbuf));

        self.put(twophase_file_key(xid), Value::Image(img));
        Ok(())
    }

    pub async fn set_replorigin(
        &mut self,
        origin_id: RepOriginId,
        origin_lsn: Lsn,
    ) -> anyhow::Result<()> {
        let key = repl_origin_key(origin_id);
        self.put(key, Value::Image(origin_lsn.ser().unwrap().into()));
        Ok(())
    }

    pub async fn drop_replorigin(&mut self, origin_id: RepOriginId) -> anyhow::Result<()> {
        self.set_replorigin(origin_id, Lsn::INVALID).await
    }

    pub fn put_control_file(&mut self, img: Bytes) -> anyhow::Result<()> {
        self.put(CONTROLFILE_KEY, Value::Image(img));
        Ok(())
    }

    pub fn put_checkpoint(&mut self, img: Bytes) -> anyhow::Result<()> {
        self.put(CHECKPOINT_KEY, Value::Image(img));
        Ok(())
    }

    pub async fn drop_dbdir(
        &mut self,
        spcnode: Oid,
        dbnode: Oid,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let total_blocks = self
            .tline
            .get_db_size(spcnode, dbnode, Version::Modified(self), ctx)
            .await?;

        // Remove entry from dbdir
        let buf = self.get(DBDIR_KEY, ctx).await?;
        let mut dir = DbDirectory::des(&buf)?;
        if dir.dbdirs.remove(&(spcnode, dbnode)).is_some() {
            let buf = DbDirectory::ser(&dir)?;
            self.pending_directory_entries
                .push((DirectoryKind::Db, dir.dbdirs.len()));
            self.put(DBDIR_KEY, Value::Image(buf.into()));
        } else {
            warn!(
                "dropped dbdir for spcnode {} dbnode {} did not exist in db directory",
                spcnode, dbnode
            );
        }

        // Update logical database size.
        self.pending_nblocks -= total_blocks as i64;

        // Delete all relations and metadata files for the spcnode/dnode
        self.delete(dbdir_key_range(spcnode, dbnode));
        Ok(())
    }

    /// Create a relation fork.
    ///
    /// 'nblocks' is the initial size.
    pub async fn put_rel_creation(
        &mut self,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> Result<(), RelationError> {
        if rel.relnode == 0 {
            return Err(RelationError::InvalidRelnode);
        }
        // It's possible that this is the first rel for this db in this
        // tablespace.  Create the reldir entry for it if so.
        let mut dbdir = DbDirectory::des(&self.get(DBDIR_KEY, ctx).await.context("read db")?)
            .context("deserialize db")?;
        let rel_dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let mut rel_dir =
            if let hash_map::Entry::Vacant(e) = dbdir.dbdirs.entry((rel.spcnode, rel.dbnode)) {
                // Didn't exist. Update dbdir
                e.insert(false);
                let buf = DbDirectory::ser(&dbdir).context("serialize db")?;
                self.pending_directory_entries
                    .push((DirectoryKind::Db, dbdir.dbdirs.len()));
                self.put(DBDIR_KEY, Value::Image(buf.into()));

                // and create the RelDirectory
                RelDirectory::default()
            } else {
                // reldir already exists, fetch it
                RelDirectory::des(&self.get(rel_dir_key, ctx).await.context("read db")?)
                    .context("deserialize db")?
            };

        // Add the new relation to the rel directory entry, and write it back
        if !rel_dir.rels.insert((rel.relnode, rel.forknum)) {
            return Err(RelationError::AlreadyExists);
        }

        self.pending_directory_entries
            .push((DirectoryKind::Rel, rel_dir.rels.len()));

        self.put(
            rel_dir_key,
            Value::Image(Bytes::from(
                RelDirectory::ser(&rel_dir).context("serialize")?,
            )),
        );

        // Put size
        let size_key = rel_size_to_key(rel);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

        self.pending_nblocks += nblocks as i64;

        // Update relation size cache
        self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

        // Even if nblocks > 0, we don't insert any actual blocks here. That's up to the
        // caller.
        Ok(())
    }

    /// Truncate relation
    pub async fn put_rel_truncation(
        &mut self,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        if self
            .tline
            .get_rel_exists(rel, Version::Modified(self), ctx)
            .await?
        {
            let size_key = rel_size_to_key(rel);
            // Fetch the old size first
            let old_size = self.get(size_key, ctx).await?.get_u32_le();

            // Update the entry with the new size.
            let buf = nblocks.to_le_bytes();
            self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            // Update logical database size.
            self.pending_nblocks -= old_size as i64 - nblocks as i64;
        }
        Ok(())
    }

    /// Extend relation
    /// If new size is smaller, do nothing.
    pub async fn put_rel_extend(
        &mut self,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);

        // Put size
        let size_key = rel_size_to_key(rel);
        let old_size = self.get(size_key, ctx).await?.get_u32_le();

        // only extend relation here. never decrease the size
        if nblocks > old_size {
            let buf = nblocks.to_le_bytes();
            self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            self.pending_nblocks += nblocks as i64 - old_size as i64;
        }
        Ok(())
    }

    /// Drop some relations
    pub(crate) async fn put_rel_drops(
        &mut self,
        drop_relations: HashMap<(u32, u32), Vec<RelTag>>,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        for ((spc_node, db_node), rel_tags) in drop_relations {
            let dir_key = rel_dir_to_key(spc_node, db_node);
            let buf = self.get(dir_key, ctx).await?;
            let mut dir = RelDirectory::des(&buf)?;

            let mut dirty = false;
            for rel_tag in rel_tags {
                if dir.rels.remove(&(rel_tag.relnode, rel_tag.forknum)) {
                    dirty = true;

                    // update logical size
                    let size_key = rel_size_to_key(rel_tag);
                    let old_size = self.get(size_key, ctx).await?.get_u32_le();
                    self.pending_nblocks -= old_size as i64;

                    // Remove entry from relation size cache
                    self.tline.remove_cached_rel_size(&rel_tag);

                    // Delete size entry, as well as all blocks
                    self.delete(rel_key_range(rel_tag));
                }
            }

            if dirty {
                self.put(dir_key, Value::Image(Bytes::from(RelDirectory::ser(&dir)?)));
                self.pending_directory_entries
                    .push((DirectoryKind::Rel, dir.rels.len()));
            }
        }

        Ok(())
    }

    pub async fn put_slru_segment_creation(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        assert!(self.tline.tenant_shard_id.is_shard_zero());

        // Add it to the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key, ctx).await?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.insert(segno) {
            anyhow::bail!("slru segment {kind:?}/{segno} already exists");
        }
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(kind), dir.segments.len()));
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

        // even if nblocks > 0, we don't insert any actual blocks here

        Ok(())
    }

    /// Extend SLRU segment
    pub fn put_slru_extend(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
    ) -> anyhow::Result<()> {
        assert!(self.tline.tenant_shard_id.is_shard_zero());

        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub async fn drop_slru_segment(
        &mut self,
        kind: SlruKind,
        segno: u32,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Remove it from the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key, ctx).await?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.remove(&segno) {
            warn!("slru segment {:?}/{} does not exist", kind, segno);
        }
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(kind), dir.segments.len()));
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Delete size entry, as well as all blocks
        self.delete(slru_segment_key_range(kind, segno));

        Ok(())
    }

    /// Drop a relmapper file (pg_filenode.map)
    pub fn drop_relmap_file(&mut self, _spcnode: Oid, _dbnode: Oid) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub async fn drop_twophase_file(
        &mut self,
        xid: u64,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Remove it from the directory entry
        let buf = self.get(TWOPHASEDIR_KEY, ctx).await?;
        let newdirbuf = if self.tline.pg_version >= 17 {
            let mut dir = TwoPhaseDirectoryV17::des(&buf)?;

            if !dir.xids.remove(&xid) {
                warn!("twophase file for xid {} does not exist", xid);
            }
            self.pending_directory_entries
                .push((DirectoryKind::TwoPhase, dir.xids.len()));
            Bytes::from(TwoPhaseDirectoryV17::ser(&dir)?)
        } else {
            let xid: u32 = u32::try_from(xid)?;
            let mut dir = TwoPhaseDirectory::des(&buf)?;

            if !dir.xids.remove(&xid) {
                warn!("twophase file for xid {} does not exist", xid);
            }
            self.pending_directory_entries
                .push((DirectoryKind::TwoPhase, dir.xids.len()));
            Bytes::from(TwoPhaseDirectory::ser(&dir)?)
        };
        self.put(TWOPHASEDIR_KEY, Value::Image(newdirbuf));

        // Delete it
        self.delete(twophase_key_range(xid));

        Ok(())
    }

    pub async fn put_file(
        &mut self,
        path: &str,
        content: &[u8],
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let key = aux_file::encode_aux_file_key(path);
        // retrieve the key from the engine
        let old_val = match self.get(key, ctx).await {
            Ok(val) => Some(val),
            Err(PageReconstructError::MissingKey(_)) => None,
            Err(e) => return Err(e.into()),
        };
        let files: Vec<(&str, &[u8])> = if let Some(ref old_val) = old_val {
            aux_file::decode_file_value(old_val)?
        } else {
            Vec::new()
        };
        let mut other_files = Vec::with_capacity(files.len());
        let mut modifying_file = None;
        for file @ (p, content) in files {
            if path == p {
                assert!(
                    modifying_file.is_none(),
                    "duplicated entries found for {}",
                    path
                );
                modifying_file = Some(content);
            } else {
                other_files.push(file);
            }
        }
        let mut new_files = other_files;
        match (modifying_file, content.is_empty()) {
            (Some(old_content), false) => {
                self.tline
                    .aux_file_size_estimator
                    .on_update(old_content.len(), content.len());
                new_files.push((path, content));
            }
            (Some(old_content), true) => {
                self.tline
                    .aux_file_size_estimator
                    .on_remove(old_content.len());
                // not adding the file key to the final `new_files` vec.
            }
            (None, false) => {
                self.tline.aux_file_size_estimator.on_add(content.len());
                new_files.push((path, content));
            }
            (None, true) => warn!("removing non-existing aux file: {}", path),
        }
        let new_val = aux_file::encode_file_value(&new_files)?;
        self.put(key, Value::Image(new_val.into()));

        Ok(())
    }

    ///
    /// Flush changes accumulated so far to the underlying repository.
    ///
    /// Usually, changes made in DatadirModification are atomic, but this allows
    /// you to flush them to the underlying repository before the final `commit`.
    /// That allows to free up the memory used to hold the pending changes.
    ///
    /// Currently only used during bulk import of a data directory. In that
    /// context, breaking the atomicity is OK. If the import is interrupted, the
    /// whole import fails and the timeline will be deleted anyway.
    /// (Or to be precise, it will be left behind for debugging purposes and
    /// ignored, see <https://github.com/neondatabase/neon/pull/1809>)
    ///
    /// Note: A consequence of flushing the pending operations is that they
    /// won't be visible to subsequent operations until `commit`. The function
    /// retains all the metadata, but data pages are flushed. That's again OK
    /// for bulk import, where you are just loading data pages and won't try to
    /// modify the same pages twice.
    pub(crate) async fn flush(&mut self, ctx: &RequestContext) -> anyhow::Result<()> {
        // Unless we have accumulated a decent amount of changes, it's not worth it
        // to scan through the pending_updates list.
        let pending_nblocks = self.pending_nblocks;
        if pending_nblocks < 10000 {
            return Ok(());
        }

        let mut writer = self.tline.writer().await;

        // Flush relation and  SLRU data blocks, keep metadata.
        if let Some(batch) = self.pending_data_batch.take() {
            tracing::debug!(
                "Flushing batch with max_lsn={}. Last record LSN is {}",
                batch.max_lsn,
                self.tline.get_last_record_lsn()
            );

            // This bails out on first error without modifying pending_updates.
            // That's Ok, cf this function's doc comment.
            writer.put_batch(batch, ctx).await?;
        }

        if pending_nblocks != 0 {
            writer.update_current_logical_size(pending_nblocks * i64::from(BLCKSZ));
            self.pending_nblocks = 0;
        }

        for (kind, count) in std::mem::take(&mut self.pending_directory_entries) {
            writer.update_directory_entries_count(kind, count as u64);
        }

        Ok(())
    }

    ///
    /// Finish this atomic update, writing all the updated keys to the
    /// underlying timeline.
    /// All the modifications in this atomic update are stamped by the specified LSN.
    ///
    pub async fn commit(&mut self, ctx: &RequestContext) -> anyhow::Result<()> {
        let mut writer = self.tline.writer().await;

        let pending_nblocks = self.pending_nblocks;
        self.pending_nblocks = 0;

        // Ordering: the items in this batch do not need to be in any global order, but values for
        // a particular Key must be in Lsn order relative to one another.  InMemoryLayer relies on
        // this to do efficient updates to its index.  See [`wal_decoder::serialized_batch`] for
        // more details.

        let metadata_batch = {
            let pending_meta = self
                .pending_metadata_pages
                .drain()
                .flat_map(|(key, values)| {
                    values
                        .into_iter()
                        .map(move |(lsn, value_size, value)| (key, lsn, value_size, value))
                })
                .collect::<Vec<_>>();

            if pending_meta.is_empty() {
                None
            } else {
                Some(SerializedValueBatch::from_values(pending_meta))
            }
        };

        let data_batch = self.pending_data_batch.take();

        let maybe_batch = match (data_batch, metadata_batch) {
            (Some(mut data), Some(metadata)) => {
                data.extend(metadata);
                Some(data)
            }
            (Some(data), None) => Some(data),
            (None, Some(metadata)) => Some(metadata),
            (None, None) => None,
        };

        if let Some(batch) = maybe_batch {
            tracing::debug!(
                "Flushing batch with max_lsn={}. Last record LSN is {}",
                batch.max_lsn,
                self.tline.get_last_record_lsn()
            );

            // This bails out on first error without modifying pending_updates.
            // That's Ok, cf this function's doc comment.
            writer.put_batch(batch, ctx).await?;
        }

        if !self.pending_deletions.is_empty() {
            writer.delete_batch(&self.pending_deletions, ctx).await?;
            self.pending_deletions.clear();
        }

        self.pending_lsns.push(self.lsn);
        for pending_lsn in self.pending_lsns.drain(..) {
            // TODO(vlad): pretty sure the comment below is not valid anymore
            // and we can call finish write with the latest LSN
            //
            // Ideally, we should be able to call writer.finish_write() only once
            // with the highest LSN. However, the last_record_lsn variable in the
            // timeline keeps track of the latest LSN and the immediate previous LSN
            // so we need to record every LSN to not leave a gap between them.
            writer.finish_write(pending_lsn);
        }

        if pending_nblocks != 0 {
            writer.update_current_logical_size(pending_nblocks * i64::from(BLCKSZ));
        }

        for (kind, count) in std::mem::take(&mut self.pending_directory_entries) {
            writer.update_directory_entries_count(kind, count as u64);
        }

        self.pending_metadata_bytes = 0;

        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.pending_metadata_pages.len()
            + self.pending_data_batch.as_ref().map_or(0, |b| b.len())
            + self.pending_deletions.len()
    }

    /// Read a page from the Timeline we are writing to.  For metadata pages, this passes through
    /// a cache in Self, which makes writes earlier in this modification visible to WAL records later
    /// in the modification.
    ///
    /// For data pages, reads pass directly to the owning Timeline: any ingest code which reads a data
    /// page must ensure that the pages they read are already committed in Timeline, for example
    /// DB create operations are always preceded by a call to commit().  This is special cased because
    /// it's rare: all the 'normal' WAL operations will only read metadata pages such as relation sizes,
    /// and not data pages.
    async fn get(&self, key: Key, ctx: &RequestContext) -> Result<Bytes, PageReconstructError> {
        if !Self::is_data_key(&key) {
            // Have we already updated the same key? Read the latest pending updated
            // version in that case.
            //
            // Note: we don't check pending_deletions. It is an error to request a
            // value that has been removed, deletion only avoids leaking storage.
            if let Some(values) = self.pending_metadata_pages.get(&key.to_compact()) {
                if let Some((_, _, value)) = values.last() {
                    return if let Value::Image(img) = value {
                        Ok(img.clone())
                    } else {
                        // Currently, we never need to read back a WAL record that we
                        // inserted in the same "transaction". All the metadata updates
                        // work directly with Images, and we never need to read actual
                        // data pages. We could handle this if we had to, by calling
                        // the walredo manager, but let's keep it simple for now.
                        Err(PageReconstructError::Other(anyhow::anyhow!(
                            "unexpected pending WAL record"
                        )))
                    };
                }
            }
        } else {
            // This is an expensive check, so we only do it in debug mode. If reading a data key,
            // this key should never be present in pending_data_pages. We ensure this by committing
            // modifications before ingesting DB create operations, which are the only kind that reads
            // data pages during ingest.
            if cfg!(debug_assertions) {
                assert!(!self
                    .pending_data_batch
                    .as_ref()
                    .is_some_and(|b| b.updates_key(&key)));
            }
        }

        // Metadata page cache miss, or we're reading a data page.
        let lsn = Lsn::max(self.tline.get_last_record_lsn(), self.lsn);
        self.tline.get(key, lsn, ctx).await
    }

    fn put(&mut self, key: Key, val: Value) {
        if Self::is_data_key(&key) {
            self.put_data(key.to_compact(), val)
        } else {
            self.put_metadata(key.to_compact(), val)
        }
    }

    fn put_data(&mut self, key: CompactKey, val: Value) {
        let batch = self
            .pending_data_batch
            .get_or_insert_with(SerializedValueBatch::default);
        batch.put(key, val, self.lsn);
    }

    fn put_metadata(&mut self, key: CompactKey, val: Value) {
        let values = self.pending_metadata_pages.entry(key).or_default();
        // Replace the previous value if it exists at the same lsn
        if let Some((last_lsn, last_value_ser_size, last_value)) = values.last_mut() {
            if *last_lsn == self.lsn {
                // Update the pending_metadata_bytes contribution from this entry, and update the serialized size in place
                self.pending_metadata_bytes -= *last_value_ser_size;
                *last_value_ser_size = val.serialized_size().unwrap() as usize;
                self.pending_metadata_bytes += *last_value_ser_size;

                // Use the latest value, this replaces any earlier write to the same (key,lsn), such as much
                // have been generated by synthesized zero page writes prior to the first real write to a page.
                *last_value = val;
                return;
            }
        }

        let val_serialized_size = val.serialized_size().unwrap() as usize;
        self.pending_metadata_bytes += val_serialized_size;
        values.push((self.lsn, val_serialized_size, val));

        if key == CHECKPOINT_KEY.to_compact() {
            tracing::debug!("Checkpoint key added to pending with size {val_serialized_size}");
        }
    }

    fn delete(&mut self, key_range: Range<Key>) {
        trace!("DELETE {}-{}", key_range.start, key_range.end);
        self.pending_deletions.push((key_range, self.lsn));
    }
}

/// This struct facilitates accessing either a committed key from the timeline at a
/// specific LSN, or the latest uncommitted key from a pending modification.
///
/// During WAL ingestion, the records from multiple LSNs may be batched in the same
/// modification before being flushed to the timeline. Hence, the routines in WalIngest
/// need to look up the keys in the modification first before looking them up in the
/// timeline to not miss the latest updates.
#[derive(Clone, Copy)]
pub enum Version<'a> {
    Lsn(Lsn),
    Modified(&'a DatadirModification<'a>),
}

impl Version<'_> {
    async fn get(
        &self,
        timeline: &Timeline,
        key: Key,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        match self {
            Version::Lsn(lsn) => timeline.get(key, *lsn, ctx).await,
            Version::Modified(modification) => modification.get(key, ctx).await,
        }
    }

    fn get_lsn(&self) -> Lsn {
        match self {
            Version::Lsn(lsn) => *lsn,
            Version::Modified(modification) => modification.lsn,
        }
    }
}

//--- Metadata structs stored in key-value pairs in the repository.

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DbDirectory {
    // (spcnode, dbnode) -> (do relmapper and PG_VERSION files exist)
    pub(crate) dbdirs: HashMap<(Oid, Oid), bool>,
}

// The format of TwoPhaseDirectory changed in PostgreSQL v17, because the filenames of
// pg_twophase files was expanded from 32-bit XIDs to 64-bit XIDs.  Previously, the files
// were named like "pg_twophase/000002E5", now they're like
// "pg_twophsae/0000000A000002E4".

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TwoPhaseDirectory {
    pub(crate) xids: HashSet<TransactionId>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TwoPhaseDirectoryV17 {
    xids: HashSet<u64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct RelDirectory {
    // Set of relations that exist. (relfilenode, forknum)
    //
    // TODO: Store it as a btree or radix tree or something else that spans multiple
    // key-value pairs, if you have a lot of relations
    pub(crate) rels: HashSet<(Oid, u8)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RelSizeEntry {
    nblocks: u32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct SlruSegmentDirectory {
    // Set of SLRU segments that exist.
    pub(crate) segments: HashSet<u32>,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, enum_map::Enum)]
#[repr(u8)]
pub(crate) enum DirectoryKind {
    Db,
    TwoPhase,
    Rel,
    AuxFiles,
    SlruSegment(SlruKind),
}

impl DirectoryKind {
    pub(crate) const KINDS_NUM: usize = <DirectoryKind as Enum>::LENGTH;
    pub(crate) fn offset(&self) -> usize {
        self.into_usize()
    }
}

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; BLCKSZ as usize]);

#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use hex_literal::hex;
    use pageserver_api::{models::ShardParameters, shard::ShardStripeSize};
    use utils::{
        id::TimelineId,
        shard::{ShardCount, ShardNumber},
    };

    use super::*;

    use crate::{tenant::harness::TenantHarness, DEFAULT_PG_VERSION};

    /// Test a round trip of aux file updates, from DatadirModification to reading back from the Timeline
    #[tokio::test]
    async fn aux_files_round_trip() -> anyhow::Result<()> {
        let name = "aux_files_round_trip";
        let harness = TenantHarness::create(name).await?;

        pub const TIMELINE_ID: TimelineId =
            TimelineId::from_array(hex!("11223344556677881122334455667788"));

        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0x10), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let tline = tline.raw_timeline().unwrap();

        // First modification: insert two keys
        let mut modification = tline.begin_modification(Lsn(0x1000));
        modification.put_file("foo/bar1", b"content1", &ctx).await?;
        modification.set_lsn(Lsn(0x1008))?;
        modification.put_file("foo/bar2", b"content2", &ctx).await?;
        modification.commit(&ctx).await?;
        let expect_1008 = HashMap::from([
            ("foo/bar1".to_string(), Bytes::from_static(b"content1")),
            ("foo/bar2".to_string(), Bytes::from_static(b"content2")),
        ]);

        let readback = tline.list_aux_files(Lsn(0x1008), &ctx).await?;
        assert_eq!(readback, expect_1008);

        // Second modification: update one key, remove the other
        let mut modification = tline.begin_modification(Lsn(0x2000));
        modification.put_file("foo/bar1", b"content3", &ctx).await?;
        modification.set_lsn(Lsn(0x2008))?;
        modification.put_file("foo/bar2", b"", &ctx).await?;
        modification.commit(&ctx).await?;
        let expect_2008 =
            HashMap::from([("foo/bar1".to_string(), Bytes::from_static(b"content3"))]);

        let readback = tline.list_aux_files(Lsn(0x2008), &ctx).await?;
        assert_eq!(readback, expect_2008);

        // Reading back in time works
        let readback = tline.list_aux_files(Lsn(0x1008), &ctx).await?;
        assert_eq!(readback, expect_1008);

        Ok(())
    }

    #[test]
    fn gap_finding() {
        let rel = RelTag {
            spcnode: 1663,
            dbnode: 208101,
            relnode: 2620,
            forknum: 0,
        };
        let base_blkno = 1;

        let base_key = rel_block_to_key(rel, base_blkno);
        let before_base_key = rel_block_to_key(rel, base_blkno - 1);

        let shard = ShardIdentity::unsharded();

        let mut previous_nblocks = 0;
        for i in 0..10 {
            let crnt_blkno = base_blkno + i;
            let gaps = DatadirModification::find_gaps(rel, crnt_blkno, previous_nblocks, &shard);

            previous_nblocks = crnt_blkno + 1;

            if i == 0 {
                // The first block we write is 1, so we should find the gap.
                assert_eq!(gaps.unwrap(), KeySpace::single(before_base_key..base_key));
            } else {
                assert!(gaps.is_none());
            }
        }

        // This is an update to an already existing block. No gaps here.
        let update_blkno = 5;
        let gaps = DatadirModification::find_gaps(rel, update_blkno, previous_nblocks, &shard);
        assert!(gaps.is_none());

        // This is an update past the current end block.
        let after_gap_blkno = 20;
        let gaps = DatadirModification::find_gaps(rel, after_gap_blkno, previous_nblocks, &shard);

        let gap_start_key = rel_block_to_key(rel, previous_nblocks);
        let after_gap_key = rel_block_to_key(rel, after_gap_blkno);
        assert_eq!(
            gaps.unwrap(),
            KeySpace::single(gap_start_key..after_gap_key)
        );
    }

    #[test]
    fn sharded_gap_finding() {
        let rel = RelTag {
            spcnode: 1663,
            dbnode: 208101,
            relnode: 2620,
            forknum: 0,
        };

        let first_blkno = 6;

        // This shard will get the even blocks
        let shard = ShardIdentity::from_params(
            ShardNumber(0),
            &ShardParameters {
                count: ShardCount(2),
                stripe_size: ShardStripeSize(1),
            },
        );

        // Only keys belonging to this shard are considered as gaps.
        let mut previous_nblocks = 0;
        let gaps =
            DatadirModification::find_gaps(rel, first_blkno, previous_nblocks, &shard).unwrap();
        assert!(!gaps.ranges.is_empty());
        for gap_range in gaps.ranges {
            let mut k = gap_range.start;
            while k != gap_range.end {
                assert_eq!(shard.get_shard_number(&k), shard.number);
                k = k.next();
            }
        }

        previous_nblocks = first_blkno;

        let update_blkno = 2;
        let gaps = DatadirModification::find_gaps(rel, update_blkno, previous_nblocks, &shard);
        assert!(gaps.is_none());
    }

    /*
        fn assert_current_logical_size<R: Repository>(timeline: &DatadirTimeline<R>, lsn: Lsn) {
            let incremental = timeline.get_current_logical_size();
            let non_incremental = timeline
                .get_current_logical_size_non_incremental(lsn)
                .unwrap();
            assert_eq!(incremental, non_incremental);
        }
    */

    /*
    ///
    /// Test list_rels() function, with branches and dropped relations
    ///
    #[test]
    fn test_list_rels_drop() -> Result<()> {
        let repo = RepoHarness::create("test_list_rels_drop")?.load();
        let tline = create_empty_timeline(repo, TIMELINE_ID)?;
        const TESTDB: u32 = 111;

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        let mut writer = tline.begin_record(Lsn(0x10));
        writer.put_checkpoint(ZERO_CHECKPOINT.clone())?;
        writer.finish()?;

        // Create a relation on the timeline
        let mut writer = tline.begin_record(Lsn(0x20));
        writer.put_rel_page_image(TESTREL_A, 0, TEST_IMG("foo blk 0 at 2"))?;
        writer.finish()?;

        let writer = tline.begin_record(Lsn(0x00));
        writer.finish()?;

        // Check that list_rels() lists it after LSN 2, but no before it
        assert!(!tline.list_rels(0, TESTDB, Lsn(0x10))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x20))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x30))?.contains(&TESTREL_A));

        // Create a branch, check that the relation is visible there
        repo.branch_timeline(&tline, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
            Some(timeline) => timeline,
            None => panic!("Should have a local timeline"),
        };
        let newtline = DatadirTimelineImpl::new(newtline);
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));

        // Drop it on the branch
        let mut new_writer = newtline.begin_record(Lsn(0x40));
        new_writer.drop_relation(TESTREL_A)?;
        new_writer.finish()?;

        // Check that it's no longer listed on the branch after the point where it was dropped
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));
        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        // Run checkpoint and garbage collection and check that it's still not visible
        newtline.checkpoint(CheckpointConfig::Forced)?;
        repo.gc_iteration(Some(NEW_TIMELINE_ID), 0, true)?;

        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        Ok(())
    }
     */

    /*
    #[test]
    fn test_read_beyond_eof() -> Result<()> {
        let repo = RepoHarness::create("test_read_beyond_eof")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;

        make_some_layers(&tline, Lsn(0x20))?;
        let mut writer = tline.begin_record(Lsn(0x60));
        walingest.put_rel_page_image(
            &mut writer,
            TESTREL_A,
            0,
            TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x60))),
        )?;
        writer.finish()?;

        // Test read before rel creation. Should error out.
        assert!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x10), false).is_err());

        // Read block beyond end of relation at different points in time.
        // These reads should fall into different delta, image, and in-memory layers.
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x20), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x25), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x30), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x35), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x40), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x45), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x50), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x55), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x60), false)?, ZERO_PAGE);

        // Test on an in-memory layer with no preceding layer
        let mut writer = tline.begin_record(Lsn(0x70));
        walingest.put_rel_page_image(
            &mut writer,
            TESTREL_B,
            0,
            TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x70))),
        )?;
        writer.finish()?;

        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_B, 1, Lsn(0x70), false)?6, ZERO_PAGE);

        Ok(())
    }
     */
}
