//!
//! This provides an abstraction to store PostgreSQL relations and other files
//! in the key-value store that implements the Repository interface.
//!
//! (TODO: The line between PUT-functions here and walingest.rs is a bit blurry, as
//! walingest.rs handles a few things like implicit relation creation and extension.
//! Clarify that)
//!
use crate::keyspace::{KeySpace, KeySpaceAccum};
use crate::repository::*;
use crate::tenant::Timeline;
use crate::walrecord::NeonWalRecord;
use anyhow::{bail, ensure, Result};
use bytes::{Buf, Bytes};
use pageserver_api::reltag::{RelTag, SlruKind};
use postgres_ffi::relfile_utils::{FSM_FORKNUM, VISIBILITYMAP_FORKNUM};
use postgres_ffi::BLCKSZ;
use postgres_ffi::{Oid, TimestampTz, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap, HashSet};
use std::ops::Range;
use tracing::{debug, trace, warn};
use utils::{bin_ser::BeSer, lsn::Lsn};

/// Block number within a relation or SLRU. This matches PostgreSQL's BlockNumber type.
pub type BlockNumber = u32;

#[derive(Debug)]
pub enum LsnForTimestamp {
    Present(Lsn),
    Future(Lsn),
    Past(Lsn),
    NoData(Lsn),
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
            pending_updates: HashMap::new(),
            pending_deletions: Vec::new(),
            pending_nblocks: 0,
            lsn,
        }
    }

    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page version.
    pub fn get_rel_page_at_lsn(
        &self,
        tag: RelTag,
        blknum: BlockNumber,
        lsn: Lsn,
        latest: bool,
    ) -> Result<Bytes> {
        ensure!(tag.relnode != 0, "invalid relnode");

        let nblocks = self.get_rel_size(tag, lsn, latest)?;
        if blknum >= nblocks {
            debug!(
                "read beyond EOF at {} blk {} at {}, size is {}: returning all-zeros page",
                tag, blknum, lsn, nblocks
            );
            return Ok(ZERO_PAGE.clone());
        }

        let key = rel_block_to_key(tag, blknum);
        self.get(key, lsn)
    }

    // Get size of a database in blocks
    pub fn get_db_size(&self, spcnode: Oid, dbnode: Oid, lsn: Lsn, latest: bool) -> Result<usize> {
        let mut total_blocks = 0;

        let rels = self.list_rels(spcnode, dbnode, lsn)?;

        for rel in rels {
            let n_blocks = self.get_rel_size(rel, lsn, latest)?;
            total_blocks += n_blocks as usize;
        }
        Ok(total_blocks)
    }

    /// Get size of a relation file
    pub fn get_rel_size(&self, tag: RelTag, lsn: Lsn, latest: bool) -> Result<BlockNumber> {
        ensure!(tag.relnode != 0, "invalid relnode");

        if let Some(nblocks) = self.get_cached_rel_size(&tag, lsn) {
            return Ok(nblocks);
        }

        if (tag.forknum == FSM_FORKNUM || tag.forknum == VISIBILITYMAP_FORKNUM)
            && !self.get_rel_exists(tag, lsn, latest)?
        {
            // FIXME: Postgres sometimes calls smgrcreate() to create
            // FSM, and smgrnblocks() on it immediately afterwards,
            // without extending it.  Tolerate that by claiming that
            // any non-existent FSM fork has size 0.
            return Ok(0);
        }

        let key = rel_size_to_key(tag);
        let mut buf = self.get(key, lsn)?;
        let nblocks = buf.get_u32_le();

        if latest {
            // Update relation size cache only if "latest" flag is set.
            // This flag is set by compute when it is working with most recent version of relation.
            // Typically master compute node always set latest=true.
            // Please notice, that even if compute node "by mistake" specifies old LSN but set
            // latest=true, then it can not cause cache corruption, because with latest=true
            // pageserver choose max(request_lsn, last_written_lsn) and so cached value will be
            // associated with most recent value of LSN.
            self.update_cached_rel_size(tag, lsn, nblocks);
        }
        Ok(nblocks)
    }

    /// Does relation exist?
    pub fn get_rel_exists(&self, tag: RelTag, lsn: Lsn, _latest: bool) -> Result<bool> {
        ensure!(tag.relnode != 0, "invalid relnode");

        // first try to lookup relation in cache
        if let Some(_nblocks) = self.get_cached_rel_size(&tag, lsn) {
            return Ok(true);
        }
        // fetch directory listing
        let key = rel_dir_to_key(tag.spcnode, tag.dbnode);
        let buf = self.get(key, lsn)?;
        let dir = RelDirectory::des(&buf)?;

        let exists = dir.rels.get(&(tag.relnode, tag.forknum)).is_some();

        Ok(exists)
    }

    /// Get a list of all existing relations in given tablespace and database.
    pub fn list_rels(&self, spcnode: Oid, dbnode: Oid, lsn: Lsn) -> Result<HashSet<RelTag>> {
        // fetch directory listing
        let key = rel_dir_to_key(spcnode, dbnode);
        let buf = self.get(key, lsn)?;
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

    /// Look up given SLRU page version.
    pub fn get_slru_page_at_lsn(
        &self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        lsn: Lsn,
    ) -> Result<Bytes> {
        let key = slru_block_to_key(kind, segno, blknum);
        self.get(key, lsn)
    }

    /// Get size of an SLRU segment
    pub fn get_slru_segment_size(
        &self,
        kind: SlruKind,
        segno: u32,
        lsn: Lsn,
    ) -> Result<BlockNumber> {
        let key = slru_segment_size_to_key(kind, segno);
        let mut buf = self.get(key, lsn)?;
        Ok(buf.get_u32_le())
    }

    /// Get size of an SLRU segment
    pub fn get_slru_segment_exists(&self, kind: SlruKind, segno: u32, lsn: Lsn) -> Result<bool> {
        // fetch directory listing
        let key = slru_dir_to_key(kind);
        let buf = self.get(key, lsn)?;
        let dir = SlruSegmentDirectory::des(&buf)?;

        let exists = dir.segments.get(&segno).is_some();
        Ok(exists)
    }

    /// Locate LSN, such that all transactions that committed before
    /// 'search_timestamp' are visible, but nothing newer is.
    ///
    /// This is not exact. Commit timestamps are not guaranteed to be ordered,
    /// so it's not well defined which LSN you get if there were multiple commits
    /// "in flight" at that point in time.
    ///
    pub fn find_lsn_for_timestamp(&self, search_timestamp: TimestampTz) -> Result<LsnForTimestamp> {
        let gc_cutoff_lsn_guard = self.get_latest_gc_cutoff_lsn();
        let min_lsn = *gc_cutoff_lsn_guard;
        let max_lsn = self.get_last_record_lsn();

        // LSNs are always 8-byte aligned. low/mid/high represent the
        // LSN divided by 8.
        let mut low = min_lsn.0 / 8;
        let mut high = max_lsn.0 / 8 + 1;

        let mut found_smaller = false;
        let mut found_larger = false;
        while low < high {
            // cannot overflow, high and low are both smaller than u64::MAX / 2
            let mid = (high + low) / 2;

            let cmp = self.is_latest_commit_timestamp_ge_than(
                search_timestamp,
                Lsn(mid * 8),
                &mut found_smaller,
                &mut found_larger,
            )?;

            if cmp {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        match (found_smaller, found_larger) {
            (false, false) => {
                // This can happen if no commit records have been processed yet, e.g.
                // just after importing a cluster.
                Ok(LsnForTimestamp::NoData(max_lsn))
            }
            (true, false) => {
                // Didn't find any commit timestamps larger than the request
                Ok(LsnForTimestamp::Future(max_lsn))
            }
            (false, true) => {
                // Didn't find any commit timestamps smaller than the request
                Ok(LsnForTimestamp::Past(max_lsn))
            }
            (true, true) => {
                // low is the LSN of the first commit record *after* the search_timestamp,
                // Back off by one to get to the point just before the commit.
                //
                // FIXME: it would be better to get the LSN of the previous commit.
                // Otherwise, if you restore to the returned LSN, the database will
                // include physical changes from later commits that will be marked
                // as aborted, and will need to be vacuumed away.
                Ok(LsnForTimestamp::Present(Lsn((low - 1) * 8)))
            }
        }
    }

    ///
    /// Subroutine of find_lsn_for_timestamp(). Returns true, if there are any
    /// commits that committed after 'search_timestamp', at LSN 'probe_lsn'.
    ///
    /// Additionally, sets 'found_smaller'/'found_Larger, if encounters any commits
    /// with a smaller/larger timestamp.
    ///
    pub fn is_latest_commit_timestamp_ge_than(
        &self,
        search_timestamp: TimestampTz,
        probe_lsn: Lsn,
        found_smaller: &mut bool,
        found_larger: &mut bool,
    ) -> Result<bool> {
        for segno in self.list_slru_segments(SlruKind::Clog, probe_lsn)? {
            let nblocks = self.get_slru_segment_size(SlruKind::Clog, segno, probe_lsn)?;
            for blknum in (0..nblocks).rev() {
                let clog_page =
                    self.get_slru_page_at_lsn(SlruKind::Clog, segno, blknum, probe_lsn)?;

                if clog_page.len() == BLCKSZ as usize + 8 {
                    let mut timestamp_bytes = [0u8; 8];
                    timestamp_bytes.copy_from_slice(&clog_page[BLCKSZ as usize..]);
                    let timestamp = TimestampTz::from_be_bytes(timestamp_bytes);

                    if timestamp >= search_timestamp {
                        *found_larger = true;
                        return Ok(true);
                    } else {
                        *found_smaller = true;
                    }
                }
            }
        }
        Ok(false)
    }

    /// Get a list of SLRU segments
    pub fn list_slru_segments(&self, kind: SlruKind, lsn: Lsn) -> Result<HashSet<u32>> {
        // fetch directory entry
        let key = slru_dir_to_key(kind);

        let buf = self.get(key, lsn)?;
        let dir = SlruSegmentDirectory::des(&buf)?;

        Ok(dir.segments)
    }

    pub fn get_relmap_file(&self, spcnode: Oid, dbnode: Oid, lsn: Lsn) -> Result<Bytes> {
        let key = relmap_file_key(spcnode, dbnode);

        let buf = self.get(key, lsn)?;
        Ok(buf)
    }

    pub fn list_dbdirs(&self, lsn: Lsn) -> Result<HashMap<(Oid, Oid), bool>> {
        // fetch directory entry
        let buf = self.get(DBDIR_KEY, lsn)?;
        let dir = DbDirectory::des(&buf)?;

        Ok(dir.dbdirs)
    }

    pub fn get_twophase_file(&self, xid: TransactionId, lsn: Lsn) -> Result<Bytes> {
        let key = twophase_file_key(xid);
        let buf = self.get(key, lsn)?;
        Ok(buf)
    }

    pub fn list_twophase_files(&self, lsn: Lsn) -> Result<HashSet<TransactionId>> {
        // fetch directory entry
        let buf = self.get(TWOPHASEDIR_KEY, lsn)?;
        let dir = TwoPhaseDirectory::des(&buf)?;

        Ok(dir.xids)
    }

    pub fn get_control_file(&self, lsn: Lsn) -> Result<Bytes> {
        self.get(CONTROLFILE_KEY, lsn)
    }

    pub fn get_checkpoint(&self, lsn: Lsn) -> Result<Bytes> {
        self.get(CHECKPOINT_KEY, lsn)
    }

    /// Does the same as get_current_logical_size but counted on demand.
    /// Used to initialize the logical size tracking on startup.
    ///
    /// Only relation blocks are counted currently. That excludes metadata,
    /// SLRUs, twophase files etc.
    pub fn get_current_logical_size_non_incremental(&self, lsn: Lsn) -> Result<u64> {
        // Fetch list of database dirs and iterate them
        let buf = self.get(DBDIR_KEY, lsn)?;
        let dbdir = DbDirectory::des(&buf)?;

        let mut total_size: u64 = 0;
        for (spcnode, dbnode) in dbdir.dbdirs.keys() {
            for rel in self.list_rels(*spcnode, *dbnode, lsn)? {
                let relsize_key = rel_size_to_key(rel);
                let mut buf = self.get(relsize_key, lsn)?;
                let relsize = buf.get_u32_le();

                total_size += relsize as u64;
            }
        }
        Ok(total_size * BLCKSZ as u64)
    }

    ///
    /// Get a KeySpace that covers all the Keys that are in use at the given LSN.
    /// Anything that's not listed maybe removed from the underlying storage (from
    /// that LSN forwards).
    pub fn collect_keyspace(&self, lsn: Lsn) -> Result<KeySpace> {
        // Iterate through key ranges, greedily packing them into partitions
        let mut result = KeySpaceAccum::new();

        // The dbdir metadata always exists
        result.add_key(DBDIR_KEY);

        // Fetch list of database dirs and iterate them
        let buf = self.get(DBDIR_KEY, lsn)?;
        let dbdir = DbDirectory::des(&buf)?;

        let mut dbs: Vec<(Oid, Oid)> = dbdir.dbdirs.keys().cloned().collect();
        dbs.sort_unstable();
        for (spcnode, dbnode) in dbs {
            result.add_key(relmap_file_key(spcnode, dbnode));
            result.add_key(rel_dir_to_key(spcnode, dbnode));

            let mut rels: Vec<RelTag> = self
                .list_rels(spcnode, dbnode, lsn)?
                .iter()
                .cloned()
                .collect();
            rels.sort_unstable();
            for rel in rels {
                let relsize_key = rel_size_to_key(rel);
                let mut buf = self.get(relsize_key, lsn)?;
                let relsize = buf.get_u32_le();

                result.add_range(rel_block_to_key(rel, 0)..rel_block_to_key(rel, relsize));
                result.add_key(relsize_key);
            }
        }

        // Iterate SLRUs next
        for kind in [
            SlruKind::Clog,
            SlruKind::MultiXactMembers,
            SlruKind::MultiXactOffsets,
        ] {
            let slrudir_key = slru_dir_to_key(kind);
            result.add_key(slrudir_key);
            let buf = self.get(slrudir_key, lsn)?;
            let dir = SlruSegmentDirectory::des(&buf)?;
            let mut segments: Vec<u32> = dir.segments.iter().cloned().collect();
            segments.sort_unstable();
            for segno in segments {
                let segsize_key = slru_segment_size_to_key(kind, segno);
                let mut buf = self.get(segsize_key, lsn)?;
                let segsize = buf.get_u32_le();

                result.add_range(
                    slru_block_to_key(kind, segno, 0)..slru_block_to_key(kind, segno, segsize),
                );
                result.add_key(segsize_key);
            }
        }

        // Then pg_twophase
        result.add_key(TWOPHASEDIR_KEY);
        let buf = self.get(TWOPHASEDIR_KEY, lsn)?;
        let twophase_dir = TwoPhaseDirectory::des(&buf)?;
        let mut xids: Vec<TransactionId> = twophase_dir.xids.iter().cloned().collect();
        xids.sort_unstable();
        for xid in xids {
            result.add_key(twophase_file_key(xid));
        }

        result.add_key(CONTROLFILE_KEY);
        result.add_key(CHECKPOINT_KEY);

        Ok(result.to_keyspace())
    }

    /// Get cached size of relation if it not updated after specified LSN
    pub fn get_cached_rel_size(&self, tag: &RelTag, lsn: Lsn) -> Option<BlockNumber> {
        let rel_size_cache = self.rel_size_cache.read().unwrap();
        if let Some((cached_lsn, nblocks)) = rel_size_cache.get(tag) {
            if lsn >= *cached_lsn {
                return Some(*nblocks);
            }
        }
        None
    }

    /// Update cached relation size if there is no more recent update
    pub fn update_cached_rel_size(&self, tag: RelTag, lsn: Lsn, nblocks: BlockNumber) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        match rel_size_cache.entry(tag) {
            hash_map::Entry::Occupied(mut entry) => {
                let cached_lsn = entry.get_mut();
                if lsn >= cached_lsn.0 {
                    *cached_lsn = (lsn, nblocks);
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert((lsn, nblocks));
            }
        }
    }

    /// Store cached relation size
    pub fn set_cached_rel_size(&self, tag: RelTag, lsn: Lsn, nblocks: BlockNumber) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        rel_size_cache.insert(tag, (lsn, nblocks));
    }

    /// Remove cached relation size
    pub fn remove_cached_rel_size(&self, tag: &RelTag) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        rel_size_cache.remove(tag);
    }
}

/// DatadirModification represents an operation to ingest an atomic set of
/// updates to the repository. It is created by the 'begin_record'
/// function. It is called for each WAL record, so that all the modifications
/// by a one WAL record appear atomic.
pub struct DatadirModification<'a> {
    /// The timeline this modification applies to. You can access this to
    /// read the state, but note that any pending updates are *not* reflected
    /// in the state in 'tline' yet.
    pub tline: &'a Timeline,

    /// Lsn assigned by begin_modification
    pub lsn: Lsn,

    // The modifications are not applied directly to the underlying key-value store.
    // The put-functions add the modifications here, and they are flushed to the
    // underlying key-value store by the 'finish' function.
    pending_updates: HashMap<Key, Value>,
    pending_deletions: Vec<Range<Key>>,
    pending_nblocks: i64,
}

impl<'a> DatadirModification<'a> {
    /// Initialize a completely new repository.
    ///
    /// This inserts the directory metadata entries that are assumed to
    /// always exist.
    pub fn init_empty(&mut self) -> Result<()> {
        let buf = DbDirectory::ser(&DbDirectory {
            dbdirs: HashMap::new(),
        })?;
        self.put(DBDIR_KEY, Value::Image(buf.into()));

        let buf = TwoPhaseDirectory::ser(&TwoPhaseDirectory {
            xids: HashSet::new(),
        })?;
        self.put(TWOPHASEDIR_KEY, Value::Image(buf.into()));

        let buf: Bytes = SlruSegmentDirectory::ser(&SlruSegmentDirectory::default())?.into();
        let empty_dir = Value::Image(buf);
        self.put(slru_dir_to_key(SlruKind::Clog), empty_dir.clone());
        self.put(
            slru_dir_to_key(SlruKind::MultiXactMembers),
            empty_dir.clone(),
        );
        self.put(slru_dir_to_key(SlruKind::MultiXactOffsets), empty_dir);

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
    ) -> Result<()> {
        ensure!(rel.relnode != 0, "invalid relnode");
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
    ) -> Result<()> {
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
    ) -> Result<()> {
        ensure!(rel.relnode != 0, "invalid relnode");
        self.put(rel_block_to_key(rel, blknum), Value::Image(img));
        Ok(())
    }

    pub fn put_slru_page_image(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        img: Bytes,
    ) -> Result<()> {
        self.put(slru_block_to_key(kind, segno, blknum), Value::Image(img));
        Ok(())
    }

    /// Store a relmapper file (pg_filenode.map) in the repository
    pub fn put_relmap_file(&mut self, spcnode: Oid, dbnode: Oid, img: Bytes) -> Result<()> {
        // Add it to the directory (if it doesn't exist already)
        let buf = self.get(DBDIR_KEY)?;
        let mut dbdir = DbDirectory::des(&buf)?;

        let r = dbdir.dbdirs.insert((spcnode, dbnode), true);
        if r == None || r == Some(false) {
            // The dbdir entry didn't exist, or it contained a
            // 'false'. The 'insert' call already updated it with
            // 'true', now write the updated 'dbdirs' map back.
            let buf = DbDirectory::ser(&dbdir)?;
            self.put(DBDIR_KEY, Value::Image(buf.into()));
        }
        if r == None {
            // Create RelDirectory
            let buf = RelDirectory::ser(&RelDirectory {
                rels: HashSet::new(),
            })?;
            self.put(
                rel_dir_to_key(spcnode, dbnode),
                Value::Image(Bytes::from(buf)),
            );
        }

        self.put(relmap_file_key(spcnode, dbnode), Value::Image(img));
        Ok(())
    }

    pub fn put_twophase_file(&mut self, xid: TransactionId, img: Bytes) -> Result<()> {
        // Add it to the directory entry
        let buf = self.get(TWOPHASEDIR_KEY)?;
        let mut dir = TwoPhaseDirectory::des(&buf)?;
        if !dir.xids.insert(xid) {
            bail!("twophase file for xid {} already exists", xid);
        }
        self.put(
            TWOPHASEDIR_KEY,
            Value::Image(Bytes::from(TwoPhaseDirectory::ser(&dir)?)),
        );

        self.put(twophase_file_key(xid), Value::Image(img));
        Ok(())
    }

    pub fn put_control_file(&mut self, img: Bytes) -> Result<()> {
        self.put(CONTROLFILE_KEY, Value::Image(img));
        Ok(())
    }

    pub fn put_checkpoint(&mut self, img: Bytes) -> Result<()> {
        self.put(CHECKPOINT_KEY, Value::Image(img));
        Ok(())
    }

    pub fn drop_dbdir(&mut self, spcnode: Oid, dbnode: Oid) -> Result<()> {
        let req_lsn = self.tline.get_last_record_lsn();

        let total_blocks = self.tline.get_db_size(spcnode, dbnode, req_lsn, true)?;

        // Remove entry from dbdir
        let buf = self.get(DBDIR_KEY)?;
        let mut dir = DbDirectory::des(&buf)?;
        if dir.dbdirs.remove(&(spcnode, dbnode)).is_some() {
            let buf = DbDirectory::ser(&dir)?;
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
    pub fn put_rel_creation(&mut self, rel: RelTag, nblocks: BlockNumber) -> Result<()> {
        ensure!(rel.relnode != 0, "invalid relnode");
        // It's possible that this is the first rel for this db in this
        // tablespace.  Create the reldir entry for it if so.
        let mut dbdir = DbDirectory::des(&self.get(DBDIR_KEY)?)?;
        let rel_dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let mut rel_dir = if dbdir.dbdirs.get(&(rel.spcnode, rel.dbnode)).is_none() {
            // Didn't exist. Update dbdir
            dbdir.dbdirs.insert((rel.spcnode, rel.dbnode), false);
            let buf = DbDirectory::ser(&dbdir)?;
            self.put(DBDIR_KEY, Value::Image(buf.into()));

            // and create the RelDirectory
            RelDirectory::default()
        } else {
            // reldir already exists, fetch it
            RelDirectory::des(&self.get(rel_dir_key)?)?
        };

        // Add the new relation to the rel directory entry, and write it back
        if !rel_dir.rels.insert((rel.relnode, rel.forknum)) {
            bail!("rel {} already exists", rel);
        }
        self.put(
            rel_dir_key,
            Value::Image(Bytes::from(RelDirectory::ser(&rel_dir)?)),
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
    pub fn put_rel_truncation(&mut self, rel: RelTag, nblocks: BlockNumber) -> Result<()> {
        ensure!(rel.relnode != 0, "invalid relnode");
        let last_lsn = self.tline.get_last_record_lsn();
        if self.tline.get_rel_exists(rel, last_lsn, true)? {
            let size_key = rel_size_to_key(rel);
            // Fetch the old size first
            let old_size = self.get(size_key)?.get_u32_le();

            // Update the entry with the new size.
            let buf = nblocks.to_le_bytes();
            self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            // Update logical database size.
            self.pending_nblocks -= old_size as i64 - nblocks as i64;
        }
        Ok(())
    }

    /// Extend relation
    /// If new size is smaller, do nothing.
    pub fn put_rel_extend(&mut self, rel: RelTag, nblocks: BlockNumber) -> Result<()> {
        ensure!(rel.relnode != 0, "invalid relnode");

        // Put size
        let size_key = rel_size_to_key(rel);
        let old_size = self.get(size_key)?.get_u32_le();

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

    /// Drop a relation.
    pub fn put_rel_drop(&mut self, rel: RelTag) -> Result<()> {
        ensure!(rel.relnode != 0, "invalid relnode");

        // Remove it from the directory entry
        let dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let buf = self.get(dir_key)?;
        let mut dir = RelDirectory::des(&buf)?;

        if dir.rels.remove(&(rel.relnode, rel.forknum)) {
            self.put(dir_key, Value::Image(Bytes::from(RelDirectory::ser(&dir)?)));
        } else {
            warn!("dropped rel {} did not exist in rel directory", rel);
        }

        // update logical size
        let size_key = rel_size_to_key(rel);
        let old_size = self.get(size_key)?.get_u32_le();
        self.pending_nblocks -= old_size as i64;

        // Remove enty from relation size cache
        self.tline.remove_cached_rel_size(&rel);

        // Delete size entry, as well as all blocks
        self.delete(rel_key_range(rel));

        Ok(())
    }

    pub fn put_slru_segment_creation(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
    ) -> Result<()> {
        // Add it to the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key)?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.insert(segno) {
            bail!("slru segment {:?}/{} already exists", kind, segno);
        }
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
    ) -> Result<()> {
        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub fn drop_slru_segment(&mut self, kind: SlruKind, segno: u32) -> Result<()> {
        // Remove it from the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key)?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.remove(&segno) {
            warn!("slru segment {:?}/{} does not exist", kind, segno);
        }
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Delete size entry, as well as all blocks
        self.delete(slru_segment_key_range(kind, segno));

        Ok(())
    }

    /// Drop a relmapper file (pg_filenode.map)
    pub fn drop_relmap_file(&mut self, _spcnode: Oid, _dbnode: Oid) -> Result<()> {
        // TODO
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub fn drop_twophase_file(&mut self, xid: TransactionId) -> Result<()> {
        // Remove it from the directory entry
        let buf = self.get(TWOPHASEDIR_KEY)?;
        let mut dir = TwoPhaseDirectory::des(&buf)?;

        if !dir.xids.remove(&xid) {
            warn!("twophase file for xid {} does not exist", xid);
        }
        self.put(
            TWOPHASEDIR_KEY,
            Value::Image(Bytes::from(TwoPhaseDirectory::ser(&dir)?)),
        );

        // Delete it
        self.delete(twophase_key_range(xid));

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
    /// ignored, see https://github.com/neondatabase/neon/pull/1809)
    ///
    /// Note: A consequence of flushing the pending operations is that they
    /// won't be visible to subsequent operations until `commit`. The function
    /// retains all the metadata, but data pages are flushed. That's again OK
    /// for bulk import, where you are just loading data pages and won't try to
    /// modify the same pages twice.
    pub fn flush(&mut self) -> Result<()> {
        // Unless we have accumulated a decent amount of changes, it's not worth it
        // to scan through the pending_updates list.
        let pending_nblocks = self.pending_nblocks;
        if pending_nblocks < 10000 {
            return Ok(());
        }

        let writer = self.tline.writer();

        // Flush relation and  SLRU data blocks, keep metadata.
        let mut result: Result<()> = Ok(());
        self.pending_updates.retain(|&key, value| {
            if result.is_ok() && (is_rel_block_key(key) || is_slru_block_key(key)) {
                result = writer.put(key, self.lsn, value);
                false
            } else {
                true
            }
        });
        result?;

        if pending_nblocks != 0 {
            writer.update_current_logical_size(pending_nblocks * i64::from(BLCKSZ));
            self.pending_nblocks = 0;
        }

        Ok(())
    }

    ///
    /// Finish this atomic update, writing all the updated keys to the
    /// underlying timeline.
    /// All the modifications in this atomic update are stamped by the specified LSN.
    ///
    pub fn commit(&mut self) -> anyhow::Result<()> {
        let writer = self.tline.writer();
        let lsn = self.lsn;
        let pending_nblocks = self.pending_nblocks;
        self.pending_nblocks = 0;

        for (key, value) in self.pending_updates.drain() {
            writer.put(key, lsn, &value)?;
        }
        for key_range in self.pending_deletions.drain(..) {
            writer.delete(key_range, lsn)?;
        }

        writer.finish_write(lsn);

        if pending_nblocks != 0 {
            writer.update_current_logical_size(pending_nblocks * i64::from(BLCKSZ));
        }

        Ok(())
    }

    // Internal helper functions to batch the modifications

    fn get(&self, key: Key) -> Result<Bytes> {
        // Have we already updated the same key? Read the pending updated
        // version in that case.
        //
        // Note: we don't check pending_deletions. It is an error to request a
        // value that has been removed, deletion only avoids leaking storage.
        if let Some(value) = self.pending_updates.get(&key) {
            if let Value::Image(img) = value {
                Ok(img.clone())
            } else {
                // Currently, we never need to read back a WAL record that we
                // inserted in the same "transaction". All the metadata updates
                // work directly with Images, and we never need to read actual
                // data pages. We could handle this if we had to, by calling
                // the walredo manager, but let's keep it simple for now.
                bail!("unexpected pending WAL record");
            }
        } else {
            let lsn = Lsn::max(self.tline.get_last_record_lsn(), self.lsn);
            self.tline.get(key, lsn)
        }
    }

    fn put(&mut self, key: Key, val: Value) {
        self.pending_updates.insert(key, val);
    }

    fn delete(&mut self, key_range: Range<Key>) {
        trace!("DELETE {}-{}", key_range.start, key_range.end);
        self.pending_deletions.push(key_range);
    }
}

//--- Metadata structs stored in key-value pairs in the repository.

#[derive(Debug, Serialize, Deserialize)]
struct DbDirectory {
    // (spcnode, dbnode) -> (do relmapper and PG_VERSION files exist)
    dbdirs: HashMap<(Oid, Oid), bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TwoPhaseDirectory {
    xids: HashSet<TransactionId>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RelDirectory {
    // Set of relations that exist. (relfilenode, forknum)
    //
    // TODO: Store it as a btree or radix tree or something else that spans multiple
    // key-value pairs, if you have a lot of relations
    rels: HashSet<(Oid, u8)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RelSizeEntry {
    nblocks: u32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SlruSegmentDirectory {
    // Set of SLRU segments that exist.
    segments: HashSet<u32>,
}

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; BLCKSZ as usize]);

// Layout of the Key address space
//
// The Key struct, used to address the underlying key-value store, consists of
// 18 bytes, split into six fields. See 'Key' in repository.rs. We need to map
// all the data and metadata keys into those 18 bytes.
//
// Principles for the mapping:
//
// - Things that are often accessed or modified together, should be close to
//   each other in the key space. For example, if a relation is extended by one
//   block, we create a new key-value pair for the block data, and update the
//   relation size entry. Because of that, the RelSize key comes after all the
//   RelBlocks of a relation: the RelSize and the last RelBlock are always next
//   to each other.
//
// The key space is divided into four major sections, identified by the first
// byte, and the form a hierarchy:
//
// 00 Relation data and metadata
//
//   DbDir    () -> (dbnode, spcnode)
//   Filenodemap
//   RelDir   -> relnode forknum
//       RelBlocks
//       RelSize
//
// 01 SLRUs
//
//   SlruDir  kind
//   SlruSegBlocks segno
//   SlruSegSize
//
// 02 pg_twophase
//
// 03 misc
//    controlfile
//    checkpoint
//    pg_version
//
// Below is a full list of the keyspace allocation:
//
// DbDir:
// 00 00000000 00000000 00000000 00   00000000
//
// Filenodemap:
// 00 SPCNODE  DBNODE   00000000 00   00000000
//
// RelDir:
// 00 SPCNODE  DBNODE   00000000 00   00000001 (Postgres never uses relfilenode 0)
//
// RelBlock:
// 00 SPCNODE  DBNODE   RELNODE  FORK BLKNUM
//
// RelSize:
// 00 SPCNODE  DBNODE   RELNODE  FORK FFFFFFFF
//
// SlruDir:
// 01 kind     00000000 00000000 00   00000000
//
// SlruSegBlock:
// 01 kind     00000001 SEGNO    00   BLKNUM
//
// SlruSegSize:
// 01 kind     00000001 SEGNO    00   FFFFFFFF
//
// TwoPhaseDir:
// 02 00000000 00000000 00000000 00   00000000
//
// TwoPhaseFile:
// 02 00000000 00000000 00000000 00   XID
//
// ControlFile:
// 03 00000000 00000000 00000000 00   00000000
//
// Checkpoint:
// 03 00000000 00000000 00000000 00   00000001
//-- Section 01: relation data and metadata

const DBDIR_KEY: Key = Key {
    field1: 0x00,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

fn dbdir_key_range(spcnode: Oid, dbnode: Oid) -> Range<Key> {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 0,
    }..Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0xffffffff,
        field5: 0xff,
        field6: 0xffffffff,
    }
}

fn relmap_file_key(spcnode: Oid, dbnode: Oid) -> Key {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 0,
    }
}

fn rel_dir_to_key(spcnode: Oid, dbnode: Oid) -> Key {
    Key {
        field1: 0x00,
        field2: spcnode,
        field3: dbnode,
        field4: 0,
        field5: 0,
        field6: 1,
    }
}

pub fn rel_block_to_key(rel: RelTag, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: blknum,
    }
}

fn rel_size_to_key(rel: RelTag) -> Key {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: 0xffffffff,
    }
}

fn rel_key_range(rel: RelTag) -> Range<Key> {
    Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum,
        field6: 0,
    }..Key {
        field1: 0x00,
        field2: rel.spcnode,
        field3: rel.dbnode,
        field4: rel.relnode,
        field5: rel.forknum + 1,
        field6: 0,
    }
}

//-- Section 02: SLRUs

fn slru_dir_to_key(kind: SlruKind) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 0,
        field4: 0,
        field5: 0,
        field6: 0,
    }
}

fn slru_block_to_key(kind: SlruKind, segno: u32, blknum: BlockNumber) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 1,
        field4: segno,
        field5: 0,
        field6: blknum,
    }
}

fn slru_segment_size_to_key(kind: SlruKind, segno: u32) -> Key {
    Key {
        field1: 0x01,
        field2: match kind {
            SlruKind::Clog => 0x00,
            SlruKind::MultiXactMembers => 0x01,
            SlruKind::MultiXactOffsets => 0x02,
        },
        field3: 1,
        field4: segno,
        field5: 0,
        field6: 0xffffffff,
    }
}

fn slru_segment_key_range(kind: SlruKind, segno: u32) -> Range<Key> {
    let field2 = match kind {
        SlruKind::Clog => 0x00,
        SlruKind::MultiXactMembers => 0x01,
        SlruKind::MultiXactOffsets => 0x02,
    };

    Key {
        field1: 0x01,
        field2,
        field3: segno,
        field4: 0,
        field5: 0,
        field6: 0,
    }..Key {
        field1: 0x01,
        field2,
        field3: segno,
        field4: 0,
        field5: 1,
        field6: 0,
    }
}

//-- Section 03: pg_twophase

const TWOPHASEDIR_KEY: Key = Key {
    field1: 0x02,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

fn twophase_file_key(xid: TransactionId) -> Key {
    Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: 0,
        field6: xid,
    }
}

fn twophase_key_range(xid: TransactionId) -> Range<Key> {
    let (next_xid, overflowed) = xid.overflowing_add(1);

    Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: 0,
        field6: xid,
    }..Key {
        field1: 0x02,
        field2: 0,
        field3: 0,
        field4: 0,
        field5: if overflowed { 1 } else { 0 },
        field6: next_xid,
    }
}

//-- Section 03: Control file
const CONTROLFILE_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 0,
};

const CHECKPOINT_KEY: Key = Key {
    field1: 0x03,
    field2: 0,
    field3: 0,
    field4: 0,
    field5: 0,
    field6: 1,
};

// Reverse mappings for a few Keys.
// These are needed by WAL redo manager.

pub fn key_to_rel_block(key: Key) -> Result<(RelTag, BlockNumber)> {
    Ok(match key.field1 {
        0x00 => (
            RelTag {
                spcnode: key.field2,
                dbnode: key.field3,
                relnode: key.field4,
                forknum: key.field5,
            },
            key.field6,
        ),
        _ => bail!("unexpected value kind 0x{:02x}", key.field1),
    })
}

fn is_rel_block_key(key: Key) -> bool {
    key.field1 == 0x00 && key.field4 != 0
}

pub fn is_rel_fsm_block_key(key: Key) -> bool {
    key.field1 == 0x00 && key.field4 != 0 && key.field5 == FSM_FORKNUM && key.field6 != 0xffffffff
}

pub fn is_rel_vm_block_key(key: Key) -> bool {
    key.field1 == 0x00
        && key.field4 != 0
        && key.field5 == VISIBILITYMAP_FORKNUM
        && key.field6 != 0xffffffff
}

pub fn key_to_slru_block(key: Key) -> Result<(SlruKind, u32, BlockNumber)> {
    Ok(match key.field1 {
        0x01 => {
            let kind = match key.field2 {
                0x00 => SlruKind::Clog,
                0x01 => SlruKind::MultiXactMembers,
                0x02 => SlruKind::MultiXactOffsets,
                _ => bail!("unrecognized slru kind 0x{:02x}", key.field2),
            };
            let segno = key.field4;
            let blknum = key.field6;

            (kind, segno, blknum)
        }
        _ => bail!("unexpected value kind 0x{:02x}", key.field1),
    })
}

fn is_slru_block_key(key: Key) -> bool {
    key.field1 == 0x01                // SLRU-related
        && key.field3 == 0x00000001   // but not SlruDir
        && key.field6 != 0xffffffff // and not SlruSegSize
}

#[cfg(test)]
pub fn create_test_timeline(
    tenant: &crate::tenant::Tenant,
    timeline_id: utils::id::TimelineId,
    pg_version: u32,
) -> Result<std::sync::Arc<Timeline>> {
    let tline = tenant
        .create_empty_timeline(timeline_id, Lsn(8), pg_version)?
        .initialize()?;
    let mut m = tline.begin_modification(Lsn(8));
    m.init_empty()?;
    m.commit()?;
    Ok(tline)
}

#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    //use super::repo_harness::*;
    //use super::*;

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
        repo.branch_timeline(TIMELINE_ID, NEW_TIMELINE_ID, Lsn(0x30))?;
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
