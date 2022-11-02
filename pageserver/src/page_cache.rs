//!
//! Global page cache
//!
//! The page cache uses up most of the memory in the page server. It is shared
//! by all tenants, and it is used to store different kinds of pages. Sharing
//! the cache allows memory to be dynamically allocated where it's needed the
//! most.
//!
//! The page cache consists of fixed-size buffers, 8 kB each to match the
//! PostgreSQL buffer size, and a Slot struct for each buffer to contain
//! information about what's stored in the buffer.
//!
//! # Locking
//!
//! There are two levels of locking involved: There's one lock for the "mapping"
//! from page identifier (tenant ID, timeline ID, rel, block, LSN) to the buffer
//! slot, and a separate lock on each slot. To read or write the contents of a
//! slot, you must hold the lock on the slot in read or write mode,
//! respectively. To change the mapping of a slot, i.e. to evict a page or to
//! assign a buffer for a page, you must hold the mapping lock and the lock on
//! the slot at the same time.
//!
//! Whenever you need to hold both locks simultaneously, the slot lock must be
//! acquired first. This consistent ordering avoids deadlocks. To look up a page
//! in the cache, you would first look up the mapping, while holding the mapping
//! lock, and then lock the slot. You must release the mapping lock in between,
//! to obey the lock ordering and avoid deadlock.
//!
//! A slot can momentarily have invalid contents, even if it's already been
//! inserted to the mapping, but you must hold the write-lock on the slot until
//! the contents are valid. If you need to release the lock without initializing
//! the contents, you must remove the mapping first. We make that easy for the
//! callers with PageWriteGuard: when lock_for_write() returns an uninitialized
//! page, the caller must explicitly call guard.mark_valid() after it has
//! initialized it. If the guard is dropped without calling mark_valid(), the
//! mapping is automatically removed and the slot is marked free.
//!

use std::{
    collections::{hash_map::Entry, HashMap},
    convert::TryInto,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError,
    },
};

use anyhow::Context;
use once_cell::sync::OnceCell;
use tracing::error;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::repository::Key;
use crate::tenant::writeback_ephemeral_file;

static PAGE_CACHE: OnceCell<PageCache> = OnceCell::new();
const TEST_PAGE_CACHE_SIZE: usize = 50;

///
/// Initialize the page cache. This must be called once at page server startup.
///
pub fn init(size: usize) {
    if PAGE_CACHE.set(PageCache::new(size)).is_err() {
        panic!("page cache already initialized");
    }
}

///
/// Get a handle to the page cache.
///
pub fn get() -> &'static PageCache {
    //
    // In unit tests, page server startup doesn't happen and no one calls
    // page_cache::init(). Initialize it here with a tiny cache, so that the
    // page cache is usable in unit tests.
    //
    if cfg!(test) {
        PAGE_CACHE.get_or_init(|| PageCache::new(TEST_PAGE_CACHE_SIZE))
    } else {
        PAGE_CACHE.get().expect("page cache not initialized")
    }
}

pub const PAGE_SZ: usize = postgres_ffi::BLCKSZ as usize;
const MAX_USAGE_COUNT: u8 = 5;

///
/// CacheKey uniquely identifies a "thing" to cache in the page cache.
///
#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::enum_variant_names)]
enum CacheKey {
    MaterializedPage {
        hash_key: MaterializedPageHashKey,
        lsn: Lsn,
    },
    EphemeralPage {
        file_id: u64,
        blkno: u32,
    },
    ImmutableFilePage {
        file_id: u64,
        blkno: u32,
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct MaterializedPageHashKey {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub key: Key,
}

#[derive(Clone)]
struct Version {
    lsn: Lsn,
    slot_idx: usize,
}

struct Slot {
    inner: RwLock<SlotInner>,
    usage_count: AtomicU8,
}

struct SlotInner {
    key: Option<CacheKey>,
    buf: &'static mut [u8; PAGE_SZ],
    dirty: bool,
}

impl Slot {
    /// Increment usage count on the buffer, with ceiling at MAX_USAGE_COUNT.
    fn inc_usage_count(&self) {
        let _ = self
            .usage_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                if val == MAX_USAGE_COUNT {
                    None
                } else {
                    Some(val + 1)
                }
            });
    }

    /// Decrement usage count on the buffer, unless it's already zero.  Returns
    /// the old usage count.
    fn dec_usage_count(&self) -> u8 {
        let count_res =
            self.usage_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                    if val == 0 {
                        None
                    } else {
                        Some(val - 1)
                    }
                });

        match count_res {
            Ok(usage_count) => usage_count,
            Err(usage_count) => usage_count,
        }
    }
}

pub struct PageCache {
    /// This contains the mapping from the cache key to buffer slot that currently
    /// contains the page, if any.
    ///
    /// TODO: This is protected by a single lock. If that becomes a bottleneck,
    /// this HashMap can be replaced with a more concurrent version, there are
    /// plenty of such crates around.
    ///
    /// If you add support for caching different kinds of objects, each object kind
    /// can have a separate mapping map, next to this field.
    materialized_page_map: RwLock<HashMap<MaterializedPageHashKey, Vec<Version>>>,

    ephemeral_page_map: RwLock<HashMap<(u64, u32), usize>>,

    immutable_page_map: RwLock<HashMap<(u64, u32), usize>>,

    /// The actual buffers with their metadata.
    slots: Box<[Slot]>,

    /// Index of the next candidate to evict, for the Clock replacement algorithm.
    /// This is interpreted modulo the page cache size.
    next_evict_slot: AtomicUsize,
}

///
/// PageReadGuard is a "lease" on a buffer, for reading. The page is kept locked
/// until the guard is dropped.
///
pub struct PageReadGuard<'i>(RwLockReadGuard<'i, SlotInner>);

impl std::ops::Deref for PageReadGuard<'_> {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        self.0.buf
    }
}

impl AsRef<[u8; PAGE_SZ]> for PageReadGuard<'_> {
    fn as_ref(&self) -> &[u8; PAGE_SZ] {
        self.0.buf
    }
}

///
/// PageWriteGuard is a lease on a buffer for modifying it. The page is kept locked
/// until the guard is dropped.
///
/// Counterintuitively, this is used even for a read, if the requested page is not
/// currently found in the page cache. In that case, the caller of lock_for_read()
/// is expected to fill in the page contents and call mark_valid(). Similarly
/// lock_for_write() can return an invalid buffer that the caller is expected to
/// to initialize.
///
pub struct PageWriteGuard<'i> {
    inner: RwLockWriteGuard<'i, SlotInner>,

    // Are the page contents currently valid?
    valid: bool,
}

impl std::ops::DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.buf
    }
}

impl std::ops::Deref for PageWriteGuard<'_> {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        self.inner.buf
    }
}

impl AsMut<[u8; PAGE_SZ]> for PageWriteGuard<'_> {
    fn as_mut(&mut self) -> &mut [u8; PAGE_SZ] {
        self.inner.buf
    }
}

impl PageWriteGuard<'_> {
    /// Mark that the buffer contents are now valid.
    pub fn mark_valid(&mut self) {
        assert!(self.inner.key.is_some());
        assert!(
            !self.valid,
            "mark_valid called on a buffer that was already valid"
        );
        self.valid = true;
    }
    pub fn mark_dirty(&mut self) {
        // only ephemeral pages can be dirty ATM.
        assert!(matches!(
            self.inner.key,
            Some(CacheKey::EphemeralPage { .. })
        ));
        self.inner.dirty = true;
    }
}

impl Drop for PageWriteGuard<'_> {
    ///
    /// If the buffer was allocated for a page that was not already in the
    /// cache, but the lock_for_read/write() caller dropped the buffer without
    /// initializing it, remove the mapping from the page cache.
    ///
    fn drop(&mut self) {
        assert!(self.inner.key.is_some());
        if !self.valid {
            let self_key = self.inner.key.as_ref().unwrap();
            PAGE_CACHE.get().unwrap().remove_mapping(self_key);
            self.inner.key = None;
            self.inner.dirty = false;
        }
    }
}

/// lock_for_read() return value
pub enum ReadBufResult<'a> {
    Found(PageReadGuard<'a>),
    NotFound(PageWriteGuard<'a>),
}

/// lock_for_write() return value
pub enum WriteBufResult<'a> {
    Found(PageWriteGuard<'a>),
    NotFound(PageWriteGuard<'a>),
}

impl PageCache {
    //
    // Section 1.1: Public interface functions for looking up and memorizing materialized page
    // versions in the page cache
    //

    /// Look up a materialized page version.
    ///
    /// The 'lsn' is an upper bound, this will return the latest version of
    /// the given block, but not newer than 'lsn'. Returns the actual LSN of the
    /// returned page.
    pub fn lookup_materialized_page(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key: &Key,
        lsn: Lsn,
    ) -> Option<(Lsn, PageReadGuard)> {
        let mut cache_key = CacheKey::MaterializedPage {
            hash_key: MaterializedPageHashKey {
                tenant_id,
                timeline_id,
                key: *key,
            },
            lsn,
        };

        if let Some(guard) = self.try_lock_for_read(&mut cache_key) {
            if let CacheKey::MaterializedPage { hash_key: _, lsn } = cache_key {
                Some((lsn, guard))
            } else {
                panic!("unexpected key type in slot");
            }
        } else {
            None
        }
    }

    ///
    /// Store an image of the given page in the cache.
    ///
    pub fn memorize_materialized_page(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key: Key,
        lsn: Lsn,
        img: &[u8],
    ) -> anyhow::Result<()> {
        let cache_key = CacheKey::MaterializedPage {
            hash_key: MaterializedPageHashKey {
                tenant_id,
                timeline_id,
                key,
            },
            lsn,
        };

        match self.lock_for_write(&cache_key)? {
            WriteBufResult::Found(write_guard) => {
                // We already had it in cache. Another thread must've put it there
                // concurrently. Check that it had the same contents that we
                // replayed.
                assert!(*write_guard == img);
            }
            WriteBufResult::NotFound(mut write_guard) => {
                write_guard.copy_from_slice(img);
                write_guard.mark_valid();
            }
        }

        Ok(())
    }

    // Section 1.2: Public interface functions for working with Ephemeral pages.

    pub fn read_ephemeral_buf(&self, file_id: u64, blkno: u32) -> anyhow::Result<ReadBufResult> {
        let mut cache_key = CacheKey::EphemeralPage { file_id, blkno };

        self.lock_for_read(&mut cache_key)
    }

    pub fn write_ephemeral_buf(&self, file_id: u64, blkno: u32) -> anyhow::Result<WriteBufResult> {
        let cache_key = CacheKey::EphemeralPage { file_id, blkno };

        self.lock_for_write(&cache_key)
    }

    /// Immediately drop all buffers belonging to given file, without writeback
    pub fn drop_buffers_for_ephemeral(&self, drop_file_id: u64) {
        for slot_idx in 0..self.slots.len() {
            let slot = &self.slots[slot_idx];

            let mut inner = slot.inner.write().unwrap();
            if let Some(key) = &inner.key {
                match key {
                    CacheKey::EphemeralPage { file_id, blkno: _ } if *file_id == drop_file_id => {
                        // remove mapping for old buffer
                        self.remove_mapping(key);
                        inner.key = None;
                        inner.dirty = false;
                    }
                    _ => {}
                }
            }
        }
    }

    // Section 1.3: Public interface functions for working with immutable file pages.

    pub fn read_immutable_buf(&self, file_id: u64, blkno: u32) -> anyhow::Result<ReadBufResult> {
        let mut cache_key = CacheKey::ImmutableFilePage { file_id, blkno };

        self.lock_for_read(&mut cache_key)
    }

    /// Immediately drop all buffers belonging to given file, without writeback
    pub fn drop_buffers_for_immutable(&self, drop_file_id: u64) {
        for slot_idx in 0..self.slots.len() {
            let slot = &self.slots[slot_idx];

            let mut inner = slot.inner.write().unwrap();
            if let Some(key) = &inner.key {
                match key {
                    CacheKey::ImmutableFilePage { file_id, blkno: _ }
                        if *file_id == drop_file_id =>
                    {
                        // remove mapping for old buffer
                        self.remove_mapping(key);
                        inner.key = None;
                        inner.dirty = false;
                    }
                    _ => {}
                }
            }
        }
    }

    //
    // Section 2: Internal interface functions for lookup/update.
    //
    // To add support for a new kind of "thing" to cache, you will need
    // to add public interface routines above, and code to deal with the
    // "mappings" after this section. But the routines in this section should
    // not require changes.

    /// Look up a page in the cache.
    ///
    /// If the search criteria is not exact, *cache_key is updated with the key
    /// for exact key of the returned page. (For materialized pages, that means
    /// that the LSN in 'cache_key' is updated with the LSN of the returned page
    /// version.)
    ///
    /// If no page is found, returns None and *cache_key is left unmodified.
    ///
    fn try_lock_for_read(&self, cache_key: &mut CacheKey) -> Option<PageReadGuard> {
        let cache_key_orig = cache_key.clone();
        if let Some(slot_idx) = self.search_mapping(cache_key) {
            // The page was found in the mapping. Lock the slot, and re-check
            // that it's still what we expected (because we released the mapping
            // lock already, another thread could have evicted the page)
            let slot = &self.slots[slot_idx];
            let inner = slot.inner.read().unwrap();
            if inner.key.as_ref() == Some(cache_key) {
                slot.inc_usage_count();
                return Some(PageReadGuard(inner));
            } else {
                // search_mapping might have modified the search key; restore it.
                *cache_key = cache_key_orig;
            }
        }
        None
    }

    /// Return a locked buffer for given block.
    ///
    /// Like try_lock_for_read(), if the search criteria is not exact and the
    /// page is already found in the cache, *cache_key is updated.
    ///
    /// If the page is not found in the cache, this allocates a new buffer for
    /// it. The caller may then initialize the buffer with the contents, and
    /// call mark_valid().
    ///
    /// Example usage:
    ///
    /// ```ignore
    /// let cache = page_cache::get();
    ///
    /// match cache.lock_for_read(&key) {
    ///     ReadBufResult::Found(read_guard) => {
    ///         // The page was found in cache. Use it
    ///     },
    ///     ReadBufResult::NotFound(write_guard) => {
    ///         // The page was not found in cache. Read it from disk into the
    ///         // buffer.
    ///         //read_my_page_from_disk(write_guard);
    ///
    ///         // The buffer contents are now valid. Tell the page cache.
    ///         write_guard.mark_valid();
    ///     },
    /// }
    /// ```
    ///
    fn lock_for_read(&self, cache_key: &mut CacheKey) -> anyhow::Result<ReadBufResult> {
        loop {
            // First check if the key already exists in the cache.
            if let Some(read_guard) = self.try_lock_for_read(cache_key) {
                return Ok(ReadBufResult::Found(read_guard));
            }

            // Not found. Find a victim buffer
            let (slot_idx, mut inner) =
                self.find_victim().context("Failed to find evict victim")?;

            // Insert mapping for this. At this point, we may find that another
            // thread did the same thing concurrently. In that case, we evicted
            // our victim buffer unnecessarily. Put it into the free list and
            // continue with the slot that the other thread chose.
            if let Some(_existing_slot_idx) = self.try_insert_mapping(cache_key, slot_idx) {
                // TODO: put to free list

                // We now just loop back to start from beginning. This is not
                // optimal, we'll perform the lookup in the mapping again, which
                // is not really necessary because we already got
                // 'existing_slot_idx'.  But this shouldn't happen often enough
                // to matter much.
                continue;
            }

            // Make the slot ready
            let slot = &self.slots[slot_idx];
            inner.key = Some(cache_key.clone());
            inner.dirty = false;
            slot.usage_count.store(1, Ordering::Relaxed);

            return Ok(ReadBufResult::NotFound(PageWriteGuard {
                inner,
                valid: false,
            }));
        }
    }

    /// Look up a page in the cache and lock it in write mode. If it's not
    /// found, returns None.
    ///
    /// When locking a page for writing, the search criteria is always "exact".
    fn try_lock_for_write(&self, cache_key: &CacheKey) -> Option<PageWriteGuard> {
        if let Some(slot_idx) = self.search_mapping_for_write(cache_key) {
            // The page was found in the mapping. Lock the slot, and re-check
            // that it's still what we expected (because we don't released the mapping
            // lock already, another thread could have evicted the page)
            let slot = &self.slots[slot_idx];
            let inner = slot.inner.write().unwrap();
            if inner.key.as_ref() == Some(cache_key) {
                slot.inc_usage_count();
                return Some(PageWriteGuard { inner, valid: true });
            }
        }
        None
    }

    /// Return a write-locked buffer for given block.
    ///
    /// Similar to lock_for_read(), but the returned buffer is write-locked and
    /// may be modified by the caller even if it's already found in the cache.
    fn lock_for_write(&self, cache_key: &CacheKey) -> anyhow::Result<WriteBufResult> {
        loop {
            // First check if the key already exists in the cache.
            if let Some(write_guard) = self.try_lock_for_write(cache_key) {
                return Ok(WriteBufResult::Found(write_guard));
            }

            // Not found. Find a victim buffer
            let (slot_idx, mut inner) =
                self.find_victim().context("Failed to find evict victim")?;

            // Insert mapping for this. At this point, we may find that another
            // thread did the same thing concurrently. In that case, we evicted
            // our victim buffer unnecessarily. Put it into the free list and
            // continue with the slot that the other thread chose.
            if let Some(_existing_slot_idx) = self.try_insert_mapping(cache_key, slot_idx) {
                // TODO: put to free list

                // We now just loop back to start from beginning. This is not
                // optimal, we'll perform the lookup in the mapping again, which
                // is not really necessary because we already got
                // 'existing_slot_idx'.  But this shouldn't happen often enough
                // to matter much.
                continue;
            }

            // Make the slot ready
            let slot = &self.slots[slot_idx];
            inner.key = Some(cache_key.clone());
            inner.dirty = false;
            slot.usage_count.store(1, Ordering::Relaxed);

            return Ok(WriteBufResult::NotFound(PageWriteGuard {
                inner,
                valid: false,
            }));
        }
    }

    //
    // Section 3: Mapping functions
    //

    /// Search for a page in the cache using the given search key.
    ///
    /// Returns the slot index, if any. If the search criteria is not exact,
    /// *cache_key is updated with the actual key of the found page.
    ///
    /// NOTE: We don't hold any lock on the mapping on return, so the slot might
    /// get recycled for an unrelated page immediately after this function
    /// returns.  The caller is responsible for re-checking that the slot still
    /// contains the page with the same key before using it.
    ///
    fn search_mapping(&self, cache_key: &mut CacheKey) -> Option<usize> {
        match cache_key {
            CacheKey::MaterializedPage { hash_key, lsn } => {
                let map = self.materialized_page_map.read().unwrap();
                let versions = map.get(hash_key)?;

                let version_idx = match versions.binary_search_by_key(lsn, |v| v.lsn) {
                    Ok(version_idx) => version_idx,
                    Err(0) => return None,
                    Err(version_idx) => version_idx - 1,
                };
                let version = &versions[version_idx];
                *lsn = version.lsn;
                Some(version.slot_idx)
            }
            CacheKey::EphemeralPage { file_id, blkno } => {
                let map = self.ephemeral_page_map.read().unwrap();
                Some(*map.get(&(*file_id, *blkno))?)
            }
            CacheKey::ImmutableFilePage { file_id, blkno } => {
                let map = self.immutable_page_map.read().unwrap();
                Some(*map.get(&(*file_id, *blkno))?)
            }
        }
    }

    /// Search for a page in the cache using the given search key.
    ///
    /// Like 'search_mapping, but performs an "exact" search. Used for
    /// allocating a new buffer.
    fn search_mapping_for_write(&self, key: &CacheKey) -> Option<usize> {
        match key {
            CacheKey::MaterializedPage { hash_key, lsn } => {
                let map = self.materialized_page_map.read().unwrap();
                let versions = map.get(hash_key)?;

                if let Ok(version_idx) = versions.binary_search_by_key(lsn, |v| v.lsn) {
                    Some(versions[version_idx].slot_idx)
                } else {
                    None
                }
            }
            CacheKey::EphemeralPage { file_id, blkno } => {
                let map = self.ephemeral_page_map.read().unwrap();
                Some(*map.get(&(*file_id, *blkno))?)
            }
            CacheKey::ImmutableFilePage { file_id, blkno } => {
                let map = self.immutable_page_map.read().unwrap();
                Some(*map.get(&(*file_id, *blkno))?)
            }
        }
    }

    ///
    /// Remove mapping for given key.
    ///
    fn remove_mapping(&self, old_key: &CacheKey) {
        match old_key {
            CacheKey::MaterializedPage {
                hash_key: old_hash_key,
                lsn: old_lsn,
            } => {
                let mut map = self.materialized_page_map.write().unwrap();
                if let Entry::Occupied(mut old_entry) = map.entry(old_hash_key.clone()) {
                    let versions = old_entry.get_mut();

                    if let Ok(version_idx) = versions.binary_search_by_key(old_lsn, |v| v.lsn) {
                        versions.remove(version_idx);
                        if versions.is_empty() {
                            old_entry.remove_entry();
                        }
                    }
                } else {
                    panic!("could not find old key in mapping")
                }
            }
            CacheKey::EphemeralPage { file_id, blkno } => {
                let mut map = self.ephemeral_page_map.write().unwrap();
                map.remove(&(*file_id, *blkno))
                    .expect("could not find old key in mapping");
            }
            CacheKey::ImmutableFilePage { file_id, blkno } => {
                let mut map = self.immutable_page_map.write().unwrap();
                map.remove(&(*file_id, *blkno))
                    .expect("could not find old key in mapping");
            }
        }
    }

    ///
    /// Insert mapping for given key.
    ///
    /// If a mapping already existed for the given key, returns the slot index
    /// of the existing mapping and leaves it untouched.
    fn try_insert_mapping(&self, new_key: &CacheKey, slot_idx: usize) -> Option<usize> {
        match new_key {
            CacheKey::MaterializedPage {
                hash_key: new_key,
                lsn: new_lsn,
            } => {
                let mut map = self.materialized_page_map.write().unwrap();
                let versions = map.entry(new_key.clone()).or_default();
                match versions.binary_search_by_key(new_lsn, |v| v.lsn) {
                    Ok(version_idx) => Some(versions[version_idx].slot_idx),
                    Err(version_idx) => {
                        versions.insert(
                            version_idx,
                            Version {
                                lsn: *new_lsn,
                                slot_idx,
                            },
                        );
                        None
                    }
                }
            }
            CacheKey::EphemeralPage { file_id, blkno } => {
                let mut map = self.ephemeral_page_map.write().unwrap();
                match map.entry((*file_id, *blkno)) {
                    Entry::Occupied(entry) => Some(*entry.get()),
                    Entry::Vacant(entry) => {
                        entry.insert(slot_idx);
                        None
                    }
                }
            }
            CacheKey::ImmutableFilePage { file_id, blkno } => {
                let mut map = self.immutable_page_map.write().unwrap();
                match map.entry((*file_id, *blkno)) {
                    Entry::Occupied(entry) => Some(*entry.get()),
                    Entry::Vacant(entry) => {
                        entry.insert(slot_idx);
                        None
                    }
                }
            }
        }
    }

    //
    // Section 4: Misc internal helpers
    //

    /// Find a slot to evict.
    ///
    /// On return, the slot is empty and write-locked.
    fn find_victim(&self) -> anyhow::Result<(usize, RwLockWriteGuard<SlotInner>)> {
        let iter_limit = self.slots.len() * 10;
        let mut iters = 0;
        loop {
            iters += 1;
            let slot_idx = self.next_evict_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();

            let slot = &self.slots[slot_idx];

            if slot.dec_usage_count() == 0 {
                let mut inner = match slot.inner.try_write() {
                    Ok(inner) => inner,
                    Err(TryLockError::Poisoned(err)) => {
                        anyhow::bail!("buffer lock was poisoned: {err:?}")
                    }
                    Err(TryLockError::WouldBlock) => {
                        // If we have looped through the whole buffer pool 10 times
                        // and still haven't found a victim buffer, something's wrong.
                        // Maybe all the buffers were in locked. That could happen in
                        // theory, if you have more threads holding buffers locked than
                        // there are buffers in the pool. In practice, with a reasonably
                        // large buffer pool it really shouldn't happen.
                        if iters > iter_limit {
                            anyhow::bail!("exceeded evict iter limit");
                        }
                        continue;
                    }
                };
                if let Some(old_key) = &inner.key {
                    if inner.dirty {
                        if let Err(err) = Self::writeback(old_key, inner.buf) {
                            // Writing the page to disk failed.
                            //
                            // FIXME: What to do here, when? We could propagate the error to the
                            // caller, but victim buffer is generally unrelated to the original
                            // call. It can even belong to a different tenant. Currently, we
                            // report the error to the log and continue the clock sweep to find
                            // a different victim. But if the problem persists, the page cache
                            // could fill up with dirty pages that we cannot evict, and we will
                            // loop retrying the writebacks indefinitely.
                            error!("writeback of buffer {:?} failed: {}", old_key, err);
                            continue;
                        }
                    }

                    // remove mapping for old buffer
                    self.remove_mapping(old_key);
                    inner.dirty = false;
                    inner.key = None;
                }
                return Ok((slot_idx, inner));
            }
        }
    }

    fn writeback(cache_key: &CacheKey, buf: &[u8]) -> Result<(), std::io::Error> {
        match cache_key {
            CacheKey::MaterializedPage {
                hash_key: _,
                lsn: _,
            } => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unexpected dirty materialized page",
            )),
            CacheKey::EphemeralPage { file_id, blkno } => {
                writeback_ephemeral_file(*file_id, *blkno, buf)
            }
            CacheKey::ImmutableFilePage {
                file_id: _,
                blkno: _,
            } => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unexpected dirty immutable page",
            )),
        }
    }

    /// Initialize a new page cache
    ///
    /// This should be called only once at page server startup.
    fn new(num_pages: usize) -> Self {
        assert!(num_pages > 0, "page cache size must be > 0");

        let page_buffer = Box::leak(vec![0u8; num_pages * PAGE_SZ].into_boxed_slice());

        let slots = page_buffer
            .chunks_exact_mut(PAGE_SZ)
            .map(|chunk| {
                let buf: &mut [u8; PAGE_SZ] = chunk.try_into().unwrap();

                Slot {
                    inner: RwLock::new(SlotInner {
                        key: None,
                        buf,
                        dirty: false,
                    }),
                    usage_count: AtomicU8::new(0),
                }
            })
            .collect();

        Self {
            materialized_page_map: Default::default(),
            ephemeral_page_map: Default::default(),
            immutable_page_map: Default::default(),
            slots,
            next_evict_slot: AtomicUsize::new(0),
        }
    }
}
