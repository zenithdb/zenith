//! This file contains generic utility functions over the interface types,
//! which could be handy for any compaction implementation.
use crate::interface::*;

use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use pageserver_api::shard::ShardIdentity;
use pin_project_lite::pin_project;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::fmt::Display;
use std::future::Future;
use std::ops::{DerefMut, Range};
use std::pin::Pin;
use std::task::{ready, Poll};
use utils::lsn::Lsn;

pub const PAGE_SZ: u64 = 8192;

pub fn keyspace_total_size<K>(
    keyspace: &CompactionKeySpace<K>,
    shard_identity: &ShardIdentity,
) -> u64
where
    K: CompactionKey,
{
    keyspace
        .iter()
        .map(|r| K::key_range_size(r, shard_identity) as u64)
        .sum()
}

pub fn overlaps_with<T: Ord>(a: &Range<T>, b: &Range<T>) -> bool {
    !(a.end <= b.start || b.end <= a.start)
}

/// Whether a fully contains b, example as below
/// ```plain
/// |      a       |
///       |  b  |
/// ```
pub fn fully_contains<T: Ord>(a: &Range<T>, b: &Range<T>) -> bool {
    a.start <= b.start && a.end >= b.end
}

pub fn union_to_keyspace<K: Ord>(a: &mut CompactionKeySpace<K>, b: CompactionKeySpace<K>) {
    let x = std::mem::take(a);
    let mut all_ranges_iter = [x.into_iter(), b.into_iter()]
        .into_iter()
        .kmerge_by(|a, b| a.start < b.start);
    let mut ranges = Vec::new();
    if let Some(first) = all_ranges_iter.next() {
        let (mut start, mut end) = (first.start, first.end);

        for r in all_ranges_iter {
            assert!(r.start >= start);
            if r.start > end {
                ranges.push(start..end);
                start = r.start;
                end = r.end;
            } else if r.end > end {
                end = r.end;
            }
        }
        ranges.push(start..end);
    }
    *a = ranges
}

pub fn intersect_keyspace<K: Ord + Clone + Copy>(
    a: &CompactionKeySpace<K>,
    r: &Range<K>,
) -> CompactionKeySpace<K> {
    let mut ranges: Vec<Range<K>> = Vec::new();

    for x in a.iter() {
        if x.end <= r.start {
            continue;
        }
        if x.start >= r.end {
            break;
        }
        ranges.push(x.clone())
    }

    // trim the ends
    if let Some(first) = ranges.first_mut() {
        first.start = std::cmp::max(first.start, r.start);
    }
    if let Some(last) = ranges.last_mut() {
        last.end = std::cmp::min(last.end, r.end);
    }
    ranges
}

/// Create a stream that iterates through all DeltaEntrys among all input
/// layers, in key-lsn order.
///
/// This is public because the create_delta() implementation likely wants to use this too
/// TODO: move to a more shared place
pub fn merge_delta_keys<'a, E: CompactionJobExecutor>(
    layers: &'a [E::DeltaLayer],
    ctx: &'a E::RequestContext,
) -> MergeDeltaKeys<'a, E> {
    // Use a binary heap to merge the layers. Each input layer is initially
    // represented by a LazyLoadLayer::Unloaded element, which uses the start of
    // the layer's key range as the key. The first time a layer reaches the top
    // of the heap, all the keys of the layer are loaded into a sorted vector.
    //
    // This helps to keep the memory usage reasonable: we only need to hold in
    // memory the DeltaEntrys of the layers that overlap with the "current" key.
    let mut heap: BinaryHeap<LazyLoadLayer<'a, E>> = BinaryHeap::new();
    for l in layers {
        heap.push(LazyLoadLayer::Unloaded(l));
    }
    MergeDeltaKeys {
        heap,
        ctx,
        load_future: None,
    }
}

pub async fn merge_delta_keys_buffered<'a, E: CompactionJobExecutor + 'a>(
    layers: &'a [E::DeltaLayer],
    ctx: &'a E::RequestContext,
) -> anyhow::Result<impl Stream<Item = <E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>>>
{
    let mut keys = Vec::new();
    for l in layers {
        // Boxing and casting to LoadFuture is required to obtain the right Sync bound.
        // If we do l.load_keys(ctx).await? directly, there is a compilation error.
        let load_future: LoadFuture<'a, _> = Box::pin(l.load_keys(ctx));
        keys.extend(load_future.await?.into_iter());
    }
    keys.sort_by_key(|k| (k.key(), k.lsn()));
    let stream = futures::stream::iter(keys.into_iter());
    Ok(stream)
}

enum LazyLoadLayer<'a, E: CompactionJobExecutor> {
    Loaded(VecDeque<<E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>>),
    Unloaded(&'a E::DeltaLayer),
}
impl<E: CompactionJobExecutor> LazyLoadLayer<'_, E> {
    fn min_key(&self) -> E::Key {
        match self {
            Self::Loaded(entries) => entries.front().unwrap().key(),
            Self::Unloaded(dl) => dl.key_range().start,
        }
    }
    fn min_lsn(&self) -> Lsn {
        match self {
            Self::Loaded(entries) => entries.front().unwrap().lsn(),
            Self::Unloaded(dl) => dl.lsn_range().start,
        }
    }
}
impl<E: CompactionJobExecutor> PartialOrd for LazyLoadLayer<'_, E> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<E: CompactionJobExecutor> Ord for LazyLoadLayer<'_, E> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reverse order so that we get a min-heap
        (other.min_key(), other.min_lsn()).cmp(&(self.min_key(), self.min_lsn()))
    }
}
impl<E: CompactionJobExecutor> PartialEq for LazyLoadLayer<'_, E> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl<E: CompactionJobExecutor> Eq for LazyLoadLayer<'_, E> {}

type LoadFuture<'a, E> = BoxFuture<'a, anyhow::Result<Vec<E>>>;

// Stream returned by `merge_delta_keys`
pin_project! {
#[allow(clippy::type_complexity)]
pub struct MergeDeltaKeys<'a, E: CompactionJobExecutor> {
    heap: BinaryHeap<LazyLoadLayer<'a, E>>,

    #[pin]
    load_future: Option<LoadFuture<'a, <E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>>>,

    ctx: &'a E::RequestContext,
}
}

impl<'a, E> Stream for MergeDeltaKeys<'a, E>
where
    E: CompactionJobExecutor + 'a,
{
    type Item = anyhow::Result<<E::DeltaLayer as CompactionDeltaLayer<E>>::DeltaEntry<'a>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::option::Option<<Self as futures::Stream>::Item>> {
        let mut this = self.project();
        loop {
            if let Some(mut load_future) = this.load_future.as_mut().as_pin_mut() {
                // We are waiting for loading the keys to finish
                match ready!(load_future.as_mut().poll(cx)) {
                    Ok(entries) => {
                        this.load_future.set(None);
                        *this.heap.peek_mut().unwrap() =
                            LazyLoadLayer::Loaded(VecDeque::from(entries));
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            }

            // If the topmost layer in the heap hasn't been loaded yet, start
            // loading it. Otherwise return the next entry from it and update
            // the layer's position in the heap (this decreaseKey operation is
            // performed implicitly when `top` is dropped).
            if let Some(mut top) = this.heap.peek_mut() {
                match top.deref_mut() {
                    LazyLoadLayer::Unloaded(ref mut l) => {
                        let fut = l.load_keys(this.ctx);
                        this.load_future.set(Some(Box::pin(fut)));
                        continue;
                    }
                    LazyLoadLayer::Loaded(ref mut entries) => {
                        let result = entries.pop_front().unwrap();
                        if entries.is_empty() {
                            std::collections::binary_heap::PeekMut::pop(top);
                        }
                        return Poll::Ready(Some(Ok(result)));
                    }
                }
            } else {
                return Poll::Ready(None);
            }
        }
    }
}

// Accumulate values at key boundaries
pub struct KeySize<K> {
    pub key: K,
    pub num_values: u64,
    pub size: u64,
    /// The lsns to partition at (if empty then no per-lsn partitioning)
    pub partition_lsns: Vec<(Lsn, u64)>,
}

pub fn accum_key_values<'a, I, K, D, E>(
    input: I,
    target_size: u64,
) -> impl Stream<Item = Result<KeySize<K>, E>>
where
    K: Eq + PartialOrd + Display + Copy,
    I: Stream<Item = Result<D, E>>,
    D: CompactionDeltaEntry<'a, K>,
{
    async_stream::try_stream! {
        // Initialize the state from the first value
        let mut input = std::pin::pin!(input);

        if let Some(first) = input.next().await {
            let first = first?;
            let mut part_size = first.size();
            let mut accum: KeySize<K> = KeySize {
                key: first.key(),
                num_values: 1,
                size: part_size,
                partition_lsns: Vec::new(),
            };
            let mut last_key = accum.key;
            while let Some(this) = input.next().await {
                let this = this?;
                if this.key() == accum.key {
                    let add_size = this.size();
                    if part_size + add_size > target_size {
                        accum.partition_lsns.push((this.lsn(), part_size));
                        part_size = 0;
                    }
                    part_size += add_size;
                    accum.size += add_size;
                    accum.num_values += 1;
                } else {
                    assert!(last_key <= accum.key, "last_key={last_key} <= accum.key={}", accum.key);
                    last_key = accum.key;
                    yield accum;
                    part_size = this.size();
                    accum = KeySize {
                        key: this.key(),
                        num_values: 1,
                        size: part_size,
                        partition_lsns: Vec::new(),
                    };
                }
            }
            assert!(last_key <= accum.key, "last_key={last_key} <= accum.key={}", accum.key);
            yield accum;
        }
    }
}
