//!
//! Global page image cache
//!
//! Unlike page_cache it holds only most recent version of reconstructed page images.
//! And it uses invalidation mechanism to avoid layer ap lookups.

use crate::page_cache::MaterializedPageHashKey;
use crate::pgdatadir_mapping::{rel_block_to_key, BlockNumber};
use crate::repository::Key;
use crate::tenant::Timeline;
use anyhow::{bail, Result};
use bytes::Bytes;
use once_cell::sync::OnceCell;
use pageserver_api::reltag::RelTag;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Condvar, Mutex};
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

static PAGE_CACHE: OnceCell<Mutex<PageImageCache>> = OnceCell::new();
const TEST_PAGE_CACHE_SIZE: usize = 50;

enum PageImageState {
    Vacant,                        // entry is not used
    Loaded(Option<Bytes>),         // page is loaded or has failed
    Loading(Option<Arc<Condvar>>), // page in process of loading, Condvar is created on demand when some thread need to wait load completion
}

struct CacheEntry {
    key: MaterializedPageHashKey,

    // next+prev are used for LRU L2-list and next is also used for L1 free pages list
    next: usize,
    prev: usize,

    collision: usize, // L1 hash collision chain

    state: PageImageState,
}

pub struct PageImageCache {
    free_list: usize, // L1 list of free entries
    pages: Vec<CacheEntry>,
    hash_table: Vec<usize>, // indexes in pages array
}

///
/// Initialize the page cache. This must be called once at page server startup.
///
pub fn init(size: usize) {
    if PAGE_CACHE
        .set(Mutex::new(PageImageCache::new(size)))
        .is_err()
    {
        panic!("page cache already initialized");
    }
}

///
/// Get a handle to the page cache.
///
pub fn get() -> &'static Mutex<PageImageCache> {
    //
    // In unit tests, page server startup doesn't happen and no one calls
    // page_image_cache::init(). Initialize it here with a tiny cache, so that the
    // page cache is usable in unit tests.
    //
    if cfg!(test) {
        PAGE_CACHE.get_or_init(|| Mutex::new(PageImageCache::new(TEST_PAGE_CACHE_SIZE)))
    } else {
        PAGE_CACHE.get().expect("page cache not initialized")
    }
}

fn hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}

impl PageImageCache {
    fn new(size: usize) -> Self {
        let mut pages: Vec<CacheEntry> = Vec::with_capacity(size + 1);
        let hash_table = vec![0usize; size];

        // Dummy key
        let dummy_key = MaterializedPageHashKey {
            key: Key::MIN,
            tenant_id: TenantId::from([0u8; 16]),
            timeline_id: TimelineId::from([0u8; 16]),
        };

        // LRU list head
        pages.push(CacheEntry {
            key: dummy_key.clone(),
            next: 0,
            prev: 0,
            collision: 0,
            state: PageImageState::Vacant,
        });

        // Construct L1 free page list
        for i in 0..size {
            pages.push(CacheEntry {
                key: dummy_key.clone(),
                next: i + 2, // build L1-list of free pages
                prev: 0,
                collision: 0,
                state: PageImageState::Vacant,
            });
        }
        pages[size - 1].next = 0; // en of free page list

        PageImageCache {
            free_list: 1,
            pages,
            hash_table,
        }
    }

    // Unlink from L2-list
    fn unlink(&mut self, index: usize) {
        let next = self.pages[index].next;
        let prev = self.pages[index].prev;
        self.pages[next].prev = prev;
        self.pages[prev].next = next;
    }

    // Link in L2-list after specified element
    fn link_after(&mut self, after: usize, index: usize) {
        let next = self.pages[after].next;
        self.pages[index].prev = after;
        self.pages[index].next = next;
        self.pages[next].prev = index;
        self.pages[after].next = index;
    }

    fn prune(&mut self, index: usize) {
        self.pages[index].prev = index;
        self.pages[index].next = index;
    }

    fn is_empty(&self, index: usize) -> bool {
        self.pages[index].next == index
    }
}

// Remove entry from cache: o page invalidation or drop relation
pub fn remove(key: Key, tenant_id: TenantId, timeline_id: TimelineId) {
    let key = MaterializedPageHashKey {
        key,
        tenant_id,
        timeline_id,
    };
    let this = get();
    let mut cache = this.lock().unwrap();
    let h = hash(&key) % cache.hash_table.len();
    let mut index = cache.hash_table[h];
    let mut prev = 0usize;
    while index != 0 {
        if cache.pages[index].key == key {
            if !cache.is_empty(index) {
                cache.pages[index].state = PageImageState::Vacant;
                // Remove from LRU list
                cache.unlink(index);
                // Insert entry in free list
                cache.pages[index].next = cache.free_list;
                cache.free_list = index;
            } else {
                // Page is process of loading: we can not remove it righ now,
                // so just mark for deletion
                cache.pages[index].next = 0; // make is_empty == false
            }
            // Remove from hash table
            if prev == 0 {
                cache.hash_table[h] = cache.pages[index].collision;
            } else {
                cache.pages[prev].collision = cache.pages[index].collision;
            }
            break;
        }
        prev = index;
        index = cache.pages[index].collision;
    }
    // It's Ok if image not found
}

// Find or load page image in the cache
pub fn lookup(timeline: &Timeline, rel: RelTag, blkno: BlockNumber, lsn: Lsn) -> Result<Bytes> {
    let key = MaterializedPageHashKey {
        key: rel_block_to_key(rel, blkno),
        tenant_id: timeline.tenant_id,
        timeline_id: timeline.timeline_id,
    };
    let this = get();
    let mut cache = this.lock().unwrap();
    let h = hash(&key) % cache.hash_table.len();

    'lookup: loop {
        let mut index = cache.hash_table[h];
        while index != 0 {
            if cache.pages[index].key == key {
                // cache hit
                match &cache.pages[index].state {
                    PageImageState::Loaded(cached_page) => {
                        // Move to the head of LRU list
                        let page = cached_page.clone();
                        cache.unlink(index);
                        cache.link_after(0, index);
                        return page.ok_or_else(|| anyhow::anyhow!("page loading failed earlier"));
                    }
                    PageImageState::Loading(event) => {
                        // Create event on which to sleep if not yet assigned
                        let cv = match event {
                            None => {
                                let cv = Arc::new(Condvar::new());
                                cache.pages[index].state =
                                    PageImageState::Loading(Some(cv.clone()));
                                cv
                            }
                            Some(cv) => cv.clone(),
                        };
                        cache = cv.wait(cache).unwrap();
                        // Retry lookup
                        continue 'lookup;
                    }
                    PageImageState::Vacant => bail!("Vacant entry is not expected here"),
                };
            }
            index = cache.pages[index].collision;
        }
        // Cache miss
        index = cache.free_list;
        if index == 0 {
            // no free items
            let victim = cache.pages[0].prev; // take least recently used element from the tail of LRU list
            assert!(victim != 0);
            // Remove victim from hash table
            let h = hash(&cache.pages[victim].key) % cache.hash_table.len();
            index = cache.hash_table[h];
            let mut prev = 0usize;
            while index != victim {
                assert!(index != 0);
                prev = index;
                index = cache.pages[index].collision;
            }
            if prev == 0 {
                cache.hash_table[h] = cache.pages[victim].collision;
            } else {
                cache.pages[prev].collision = cache.pages[victim].collision;
            }
            // and from LRU list
            cache.unlink(victim);

            index = victim;
        } else {
            // Use next free item
            cache.free_list = cache.pages[index].next;
        }
        // Make is_empty(index) == true. If entry is removed in process of loaded,
        // it will be updated so that !is_empty(index)
        cache.prune(index);

        // Insert in hash table
        cache.pages[index].collision = cache.hash_table[h];
        cache.hash_table[h] = index;

        cache.pages[index].key = key;
        cache.pages[index].state = PageImageState::Loading(None);
        drop(cache); //release lock

        // Load page
        let res = timeline.get_rel_page_at_lsn(rel, blkno, lsn, true);

        cache = this.lock().unwrap();
        if let PageImageState::Loading(event) = &cache.pages[index].state {
            // Are there soMe waiting threads?
            if let Some(cv) = event {
                // If so, then wakeup them
                cv.notify_all();
            }
        } else {
            bail!("Loading state is expected");
        }
        if cache.is_empty(index) {
            // entry was not marked as deleted {
            // Page is loaded

            // match &res { ... } is same as `res.as_ref().ok().cloned()`
            cache.pages[index].state = PageImageState::Loaded(match &res {
                Ok(page) => Some(page.clone()),
                Err(_) => None,
            });
            // Link the page to the head of LRU list
            cache.link_after(0, index);
        } else {
            cache.pages[index].state = PageImageState::Vacant;
            // Return page to free list
            cache.pages[index].next = cache.free_list;
            cache.free_list = index;
        }
        // only the first one gets the full error from `get_rel_page_at_lsn`
        return res;
    }
}
