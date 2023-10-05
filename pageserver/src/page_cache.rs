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
//! # Types Of Pages
//!
//! [`PageCache`] only supports immutable pages.
//! Hence there is no need to worry about coherency.
//!
//! Two types of pages are supported:
//!
//! * **Materialized pages**, filled & used by page reconstruction
//! * **Immutable File pages**, filled & used by [`crate::tenant::block_io`] and [`crate::tenant::ephemeral_file`].
//!
//! Note that [`crate::tenant::ephemeral_file::EphemeralFile`] is generally mutable, but, it's append-only.
//! It uses the page cache only for the blocks that are already fully written and immutable.
//!
//! # Filling The Page Cache
//!
//! Page cache maps from a cache key to a buffer slot.
//! The cache key uniquely identifies the piece of data that is being cached.
//!
//! The cache key for **materialized pages** is  [`TenantId`], [`TimelineId`], [`Key`], and [`Lsn`].
//! Use [`PageCache::memorize_materialized_page`] and [`PageCache::lookup_materialized_page`] for fill & access.
//!
//! The cache key for **immutable file** pages is [`FileId`] and a block number.
//! Users of page cache that wish to page-cache an arbitrary (immutable!) on-disk file do the following:
//! * Have a mechanism to deterministically associate the on-disk file with a [`FileId`].
//! * Get a [`FileId`] using [`next_file_id`].
//! * Use the mechanism to associate the on-disk file with the returned [`FileId`].
//! * Use [`PageCache::read_immutable_buf`] to get a [`ReadBufResult`].
//! * If the page was already cached, it'll be the [`ReadBufResult::Found`] variant that contains
//!   a read guard for the page. Just use it.
//! * If the page was not cached, it'll be the [`ReadBufResult::NotFound`] variant that contains
//!   a write guard for the page. Fill the page with the contents of the on-disk file.
//!   Then call [`PageWriteGuard::mark_valid`] to mark the page as valid.
//!   Then try again to [`PageCache::read_immutable_buf`].
//!   Unless there's high cache pressure, the page should now be cached.
//!   (TODO: allow downgrading the write guard to a read guard to ensure forward progress.)
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
        atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use anyhow::Context;
use once_cell::sync::OnceCell;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::{context::RequestContext, metrics::PageCacheSizeMetrics, repository::Key};

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

/// See module-level comment.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct FileId(u64);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// See module-level comment.
pub fn next_file_id() -> FileId {
    FileId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

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
    ImmutableFilePage {
        file_id: FileId,
        blkno: u32,
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct MaterializedPageHashKey {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    key: Key,
}

#[derive(Clone)]
struct Version {
    lsn: Lsn,
    slot_idx: usize,
}

struct Slot {
    inner: tokio::sync::RwLock<SlotInner>,
    usage_count: AtomicU8,
}

struct SlotInner {
    key: Option<CacheKey>,
    // for `coalesce_readers_permit`
    permit: std::sync::Mutex<Weak<PinnedSlotsPermit>>,
    buf: &'static mut [u8; PAGE_SZ],
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

    /// Sets the usage count to a specific value.
    fn set_usage_count(&self, count: u8) {
        self.usage_count.store(count, Ordering::Relaxed);
    }
}

impl SlotInner {
    /// If there is already a reader, drop our permit and share its permit, just like we share read access.
    fn coalesce_readers_permit(&self, permit: PinnedSlotsPermit) -> Arc<PinnedSlotsPermit> {
        let mut guard = self.permit.lock().unwrap();
        if let Some(existing_permit) = guard.upgrade() {
            drop(guard);
            drop(permit);
            existing_permit
        } else {
            let permit = Arc::new(permit);
            *guard = Arc::downgrade(&permit);
            permit
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
    materialized_page_map: std::sync::RwLock<HashMap<MaterializedPageHashKey, Vec<Version>>>,

    immutable_page_map: std::sync::RwLock<HashMap<(FileId, u32), usize>>,

    /// The actual buffers with their metadata.
    slots: Box<[Slot]>,

    pinned_slots: Arc<tokio::sync::Semaphore>,

    /// Index of the next candidate to evict, for the Clock replacement algorithm.
    /// This is interpreted modulo the page cache size.
    next_evict_slot: AtomicUsize,

    size_metrics: &'static PageCacheSizeMetrics,
}

struct PinnedSlotsPermit(tokio::sync::OwnedSemaphorePermit);

///
/// PageReadGuard is a "lease" on a buffer, for reading. The page is kept locked
/// until the guard is dropped.
///
pub struct PageReadGuard<'i> {
    _permit: Arc<PinnedSlotsPermit>,
    slot_guard: tokio::sync::RwLockReadGuard<'i, SlotInner>,
}

impl std::ops::Deref for PageReadGuard<'_> {
    type Target = [u8; PAGE_SZ];

    fn deref(&self) -> &Self::Target {
        self.slot_guard.buf
    }
}

impl AsRef<[u8; PAGE_SZ]> for PageReadGuard<'_> {
    fn as_ref(&self) -> &[u8; PAGE_SZ] {
        self.slot_guard.buf
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
    inner: tokio::sync::RwLockWriteGuard<'i, SlotInner>,

    _permit: PinnedSlotsPermit,

    // Are the page contents currently valid?
    // Used to mark pages as invalid that are assigned but not yet filled with data.
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
    pub async fn lookup_materialized_page(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        key: &Key,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Option<(Lsn, PageReadGuard)> {
        let Ok(permit) = self.try_get_pinned_slot_permit().await else {
            return None;
        };

        crate::metrics::PAGE_CACHE
            .for_ctx(ctx)
            .read_accesses_materialized_page
            .inc();

        let mut cache_key = CacheKey::MaterializedPage {
            hash_key: MaterializedPageHashKey {
                tenant_id,
                timeline_id,
                key: *key,
            },
            lsn,
        };

        if let Some(guard) = self
            .try_lock_for_read(&mut cache_key, &mut Some(permit))
            .await
        {
            if let CacheKey::MaterializedPage {
                hash_key: _,
                lsn: available_lsn,
            } = cache_key
            {
                if available_lsn == lsn {
                    crate::metrics::PAGE_CACHE
                        .for_ctx(ctx)
                        .read_hits_materialized_page_exact
                        .inc();
                } else {
                    crate::metrics::PAGE_CACHE
                        .for_ctx(ctx)
                        .read_hits_materialized_page_older_lsn
                        .inc();
                }
                Some((available_lsn, guard))
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
    pub async fn memorize_materialized_page(
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

        match self.lock_for_write(&cache_key).await? {
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

    // Section 1.2: Public interface functions for working with immutable file pages.

    pub async fn read_immutable_buf(
        &self,
        file_id: FileId,
        blkno: u32,
        ctx: &RequestContext,
    ) -> anyhow::Result<ReadBufResult> {
        let mut cache_key = CacheKey::ImmutableFilePage { file_id, blkno };

        self.lock_for_read(&mut cache_key, ctx).await
    }

    //
    // Section 2: Internal interface functions for lookup/update.
    //
    // To add support for a new kind of "thing" to cache, you will need
    // to add public interface routines above, and code to deal with the
    // "mappings" after this section. But the routines in this section should
    // not require changes.

    async fn try_get_pinned_slot_permit(&self) -> anyhow::Result<PinnedSlotsPermit> {
        let timer = crate::metrics::PAGE_CACHE_ACQUIRE_PINNED_SLOT_TIME.start_timer();
        match tokio::time::timeout(
            // Choose small timeout, neon_smgr does its own retries.
            // https://neondb.slack.com/archives/C04DGM6SMTM/p1694786876476869
            Duration::from_secs(10),
            Arc::clone(&self.pinned_slots).acquire_owned(),
        )
        .await
        {
            Ok(res) => Ok(PinnedSlotsPermit(
                res.expect("this semaphore is never closed"),
            )),
            Err(_timeout) => {
                timer.stop_and_discard();
                crate::metrics::page_cache_errors_inc(
                    crate::metrics::PageCacheErrorKind::AcquirePinnedSlotTimeout,
                );
                anyhow::bail!("timeout: there were page guards alive for all page cache slots")
            }
        }
    }

    /// Look up a page in the cache.
    ///
    /// If the search criteria is not exact, *cache_key is updated with the key
    /// for exact key of the returned page. (For materialized pages, that means
    /// that the LSN in 'cache_key' is updated with the LSN of the returned page
    /// version.)
    ///
    /// If no page is found, returns None and *cache_key is left unmodified.
    ///
    async fn try_lock_for_read(
        &self,
        cache_key: &mut CacheKey,
        permit: &mut Option<PinnedSlotsPermit>,
    ) -> Option<PageReadGuard> {
        let cache_key_orig = cache_key.clone();
        if let Some(slot_idx) = self.search_mapping(cache_key) {
            // The page was found in the mapping. Lock the slot, and re-check
            // that it's still what we expected (because we released the mapping
            // lock already, another thread could have evicted the page)
            let slot = &self.slots[slot_idx];
            let inner = slot.inner.read().await;
            if inner.key.as_ref() == Some(cache_key) {
                slot.inc_usage_count();
                return Some(PageReadGuard {
                    _permit: inner.coalesce_readers_permit(permit.take().unwrap()),
                    slot_guard: inner,
                });
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
    async fn lock_for_read(
        &self,
        cache_key: &mut CacheKey,
        ctx: &RequestContext,
    ) -> anyhow::Result<ReadBufResult> {
        let mut permit = Some(self.try_get_pinned_slot_permit().await?);

        let (read_access, hit) = match cache_key {
            CacheKey::MaterializedPage { .. } => {
                unreachable!("Materialized pages use lookup_materialized_page")
            }
            CacheKey::ImmutableFilePage { .. } => (
                &crate::metrics::PAGE_CACHE
                    .for_ctx(ctx)
                    .read_accesses_immutable,
                &crate::metrics::PAGE_CACHE.for_ctx(ctx).read_hits_immutable,
            ),
        };
        read_access.inc();

        let mut is_first_iteration = true;
        loop {
            // First check if the key already exists in the cache.
            if let Some(read_guard) = self.try_lock_for_read(cache_key, &mut permit).await {
                debug_assert!(permit.is_none());
                if is_first_iteration {
                    hit.inc();
                }
                return Ok(ReadBufResult::Found(read_guard));
            }
            debug_assert!(permit.is_some());
            is_first_iteration = false;

            // Not found. Find a victim buffer
            let (slot_idx, mut inner) = self
                .find_victim(permit.as_ref().unwrap())
                .await
                .context("Failed to find evict victim")?;

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
            slot.set_usage_count(1);

            debug_assert!(
                {
                    let guard = inner.permit.lock().unwrap();
                    guard.upgrade().is_none()
                },
                "we hold a write lock, so, no one else should have a permit"
            );

            return Ok(ReadBufResult::NotFound(PageWriteGuard {
                _permit: permit.take().unwrap(),
                inner,
                valid: false,
            }));
        }
    }

    /// Look up a page in the cache and lock it in write mode. If it's not
    /// found, returns None.
    ///
    /// When locking a page for writing, the search criteria is always "exact".
    async fn try_lock_for_write(
        &self,
        cache_key: &CacheKey,
        permit: &mut Option<PinnedSlotsPermit>,
    ) -> Option<PageWriteGuard> {
        if let Some(slot_idx) = self.search_mapping_for_write(cache_key) {
            // The page was found in the mapping. Lock the slot, and re-check
            // that it's still what we expected (because we don't released the mapping
            // lock already, another thread could have evicted the page)
            let slot = &self.slots[slot_idx];
            let inner = slot.inner.write().await;
            if inner.key.as_ref() == Some(cache_key) {
                slot.inc_usage_count();
                debug_assert!(
                    {
                        let guard = inner.permit.lock().unwrap();
                        guard.upgrade().is_none()
                    },
                    "we hold a write lock, so, no one else should have a permit"
                );
                return Some(PageWriteGuard {
                    _permit: permit.take().unwrap(),
                    inner,
                    valid: true,
                });
            }
        }
        None
    }

    /// Return a write-locked buffer for given block.
    ///
    /// Similar to lock_for_read(), but the returned buffer is write-locked and
    /// may be modified by the caller even if it's already found in the cache.
    async fn lock_for_write(&self, cache_key: &CacheKey) -> anyhow::Result<WriteBufResult> {
        let mut permit = Some(self.try_get_pinned_slot_permit().await?);
        loop {
            // First check if the key already exists in the cache.
            if let Some(write_guard) = self.try_lock_for_write(cache_key, &mut permit).await {
                debug_assert!(permit.is_none());
                return Ok(WriteBufResult::Found(write_guard));
            }
            debug_assert!(permit.is_some());

            // Not found. Find a victim buffer
            let (slot_idx, mut inner) = self
                .find_victim(permit.as_ref().unwrap())
                .await
                .context("Failed to find evict victim")?;

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
            slot.set_usage_count(1);

            debug_assert!(
                {
                    let guard = inner.permit.lock().unwrap();
                    guard.upgrade().is_none()
                },
                "we hold a write lock, so, no one else should have a permit"
            );

            return Ok(WriteBufResult::NotFound(PageWriteGuard {
                _permit: permit.take().unwrap(),
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
                        self.size_metrics
                            .current_bytes_materialized_page
                            .sub_page_sz(1);
                        if versions.is_empty() {
                            old_entry.remove_entry();
                        }
                    }
                } else {
                    panic!("could not find old key in mapping")
                }
            }
            CacheKey::ImmutableFilePage { file_id, blkno } => {
                let mut map = self.immutable_page_map.write().unwrap();
                map.remove(&(*file_id, *blkno))
                    .expect("could not find old key in mapping");
                self.size_metrics.current_bytes_immutable.sub_page_sz(1);
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
                        self.size_metrics
                            .current_bytes_materialized_page
                            .add_page_sz(1);
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
                        self.size_metrics.current_bytes_immutable.add_page_sz(1);
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
    async fn find_victim(
        &self,
        _permit_witness: &PinnedSlotsPermit,
    ) -> anyhow::Result<(usize, tokio::sync::RwLockWriteGuard<SlotInner>)> {
        let iter_limit = self.slots.len() * 10;
        let mut iters = 0;
        loop {
            iters += 1;
            let slot_idx = self.next_evict_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();

            let slot = &self.slots[slot_idx];

            if slot.dec_usage_count() == 0 {
                let mut inner = match slot.inner.try_write() {
                    Ok(inner) => inner,
                    Err(_err) => {
                        if iters > iter_limit {
                            // NB: Even with the permits, there's no hard guarantee that we will find a slot with
                            // any particular number of iterations: other threads might race ahead and acquire and
                            // release pins just as we're scanning the array.
                            //
                            // Imagine that nslots is 2, and as starting point, usage_count==1 on all
                            // slots. There are two threads running concurrently, A and B. A has just
                            // acquired the permit from the semaphore.
                            //
                            //   A: Look at slot 1. Its usage_count == 1, so decrement it to zero, and continue the search
                            //   B: Acquire permit.
                            //   B: Look at slot 2, decrement its usage_count to zero and continue the search
                            //   B: Look at slot 1. Its usage_count is zero, so pin it and bump up its usage_count to 1.
                            //   B: Release pin and permit again
                            //   B: Acquire permit.
                            //   B: Look at slot 2. Its usage_count is zero, so pin it and bump up its usage_count to 1.
                            //   B: Release pin and permit again
                            //
                            // Now we're back in the starting situation that both slots have
                            // usage_count 1, but A has now been through one iteration of the
                            // find_victim() loop. This can repeat indefinitely and on each
                            // iteration, A's iteration count increases by one.
                            //
                            // So, even though the semaphore for the permits is fair, the victim search
                            // itself happens in parallel and is not fair.
                            // Hence even with a permit, a task can theoretically be starved.
                            // To avoid this, we'd need tokio to give priority to tasks that are holding
                            // permits for longer.
                            // Note that just yielding to tokio during iteration without such
                            // priority boosting is likely counter-productive. We'd just give more opportunities
                            // for B to bump usage count, further starving A.
                            crate::metrics::page_cache_errors_inc(
                                crate::metrics::PageCacheErrorKind::EvictIterLimit,
                            );
                            anyhow::bail!("exceeded evict iter limit");
                        }
                        continue;
                    }
                };
                if let Some(old_key) = &inner.key {
                    // remove mapping for old buffer
                    self.remove_mapping(old_key);
                    inner.key = None;
                }
                crate::metrics::PAGE_CACHE_FIND_VICTIMS_ITERS_TOTAL.inc_by(iters as u64);
                return Ok((slot_idx, inner));
            }
        }
    }

    /// Initialize a new page cache
    ///
    /// This should be called only once at page server startup.
    fn new(num_pages: usize) -> Self {
        assert!(num_pages > 0, "page cache size must be > 0");

        // We could use Vec::leak here, but that potentially also leaks
        // uninitialized reserved capacity. With into_boxed_slice and Box::leak
        // this is avoided.
        let page_buffer = Box::leak(vec![0u8; num_pages * PAGE_SZ].into_boxed_slice());

        let size_metrics = &crate::metrics::PAGE_CACHE_SIZE;
        size_metrics.max_bytes.set_page_sz(num_pages);
        size_metrics.current_bytes_immutable.set_page_sz(0);
        size_metrics.current_bytes_materialized_page.set_page_sz(0);

        let slots = page_buffer
            .chunks_exact_mut(PAGE_SZ)
            .map(|chunk| {
                let buf: &mut [u8; PAGE_SZ] = chunk.try_into().unwrap();

                Slot {
                    inner: tokio::sync::RwLock::new(SlotInner {
                        key: None,
                        buf,
                        permit: std::sync::Mutex::new(Weak::new()),
                    }),
                    usage_count: AtomicU8::new(0),
                }
            })
            .collect();

        Self {
            materialized_page_map: Default::default(),
            immutable_page_map: Default::default(),
            slots,
            next_evict_slot: AtomicUsize::new(0),
            size_metrics,
            pinned_slots: Arc::new(tokio::sync::Semaphore::new(num_pages)),
        }
    }
}

trait PageSzBytesMetric {
    fn set_page_sz(&self, count: usize);
    fn add_page_sz(&self, count: usize);
    fn sub_page_sz(&self, count: usize);
}

#[inline(always)]
fn count_times_page_sz(count: usize) -> u64 {
    u64::try_from(count).unwrap() * u64::try_from(PAGE_SZ).unwrap()
}

impl PageSzBytesMetric for metrics::UIntGauge {
    fn set_page_sz(&self, count: usize) {
        self.set(count_times_page_sz(count));
    }
    fn add_page_sz(&self, count: usize) {
        self.add(count_times_page_sz(count));
    }
    fn sub_page_sz(&self, count: usize) {
        self.sub(count_times_page_sz(count));
    }
}
