//! This is what the compaction implementation needs to know about
//! layers, keyspace etc.
//!
//! All the heavy lifting is done by the create_image and create_delta
//! functions that the implementor provides.
use futures::Future;
use pageserver_api::{key::Key, keyspace::ShardedRange, shard::ShardIdentity};
use std::ops::Range;
use utils::lsn::Lsn;

/// Public interface. This is the main thing that the implementor needs to provide
pub trait CompactionJobExecutor {
    // Type system.
    //
    // We assume that there are two kinds of layers, deltas and images. The
    // compaction doesn't distinguish whether they are stored locally or
    // remotely.
    //
    // The keyspace is defined by the CompactionKey trait.
    type Key: CompactionKey;

    type Layer: CompactionLayer<Self::Key> + Clone;
    type DeltaLayer: CompactionDeltaLayer<Self> + Clone;
    type ImageLayer: CompactionImageLayer<Self> + Clone;

    // This is passed through to all the interface functions. The compaction
    // implementation doesn't do anything with it, but it might be useful for
    // the interface implementation.
    type RequestContext: CompactionRequestContext;

    // ----
    // Functions that the planner uses to support its decisions
    // ----

    fn get_shard_identity(&self) -> &ShardIdentity;

    /// Return all layers that overlap the given bounding box.
    fn get_layers(
        &mut self,
        key_range: &Range<Self::Key>,
        lsn_range: &Range<Lsn>,
        ctx: &Self::RequestContext,
    ) -> impl Future<Output = anyhow::Result<Vec<Self::Layer>>> + Send;

    fn get_keyspace(
        &mut self,
        key_range: &Range<Self::Key>,
        lsn: Lsn,
        ctx: &Self::RequestContext,
    ) -> impl Future<Output = anyhow::Result<CompactionKeySpace<Self::Key>>> + Send;

    /// NB: This is a pretty expensive operation. In the real pageserver
    /// implementation, it downloads the layer, and keeps it resident
    /// until the DeltaLayer is dropped.
    fn downcast_delta_layer(
        &self,
        layer: &Self::Layer,
    ) -> impl Future<Output = anyhow::Result<Option<Self::DeltaLayer>>> + Send;

    // ----
    // Functions to execute the plan
    // ----

    /// Create a new image layer, materializing all the values in the key range,
    /// at given 'lsn'.
    fn create_image(
        &mut self,
        lsn: Lsn,
        key_range: &Range<Self::Key>,
        ctx: &Self::RequestContext,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Create a new delta layer, containing all the values from 'input_layers'
    /// in the given key and LSN range.
    fn create_delta(
        &mut self,
        lsn_range: &Range<Lsn>,
        key_range: &Range<Self::Key>,
        input_layers: &[Self::DeltaLayer],
        ctx: &Self::RequestContext,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Delete a layer. The compaction implementation will call this only after
    /// all the create_image() or create_delta() calls that deletion of this
    /// layer depends on have finished. But if the implementor has extra lazy
    /// background tasks, like uploading the index json file to remote storage.
    /// it is the implementation's responsibility to track those.
    fn delete_layer(
        &mut self,
        layer: &Self::Layer,
        ctx: &Self::RequestContext,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub trait CompactionKey: std::cmp::Ord + Clone + Copy + std::fmt::Display {
    const MIN: Self;
    const MAX: Self;

    /// Calculate distance between key_range.start and key_range.end.
    ///
    /// This returns u32, for compatibility with Repository::key. If the
    /// distance is larger, return u32::MAX.
    fn key_range_size(key_range: &Range<Self>, shard_identity: &ShardIdentity) -> u32;

    // return "self + 1"
    fn next(&self) -> Self;

    // return "self + <some decent amount to skip>". The amount to skip
    // is left to the implementation.
    // FIXME: why not just "add(u32)" ?  This is hard to use
    fn skip_some(&self) -> Self;
}

impl CompactionKey for Key {
    const MIN: Self = Self::MIN;
    const MAX: Self = Self::MAX;

    fn key_range_size(r: &std::ops::Range<Self>, shard_identity: &ShardIdentity) -> u32 {
        ShardedRange::new(r.clone(), shard_identity).page_count()
    }
    fn next(&self) -> Key {
        (self as &Key).next()
    }
    fn skip_some(&self) -> Key {
        self.add(128)
    }
}

/// Contiguous ranges of keys that belong to the key space. In key order, and
/// with no overlap.
pub type CompactionKeySpace<K> = Vec<Range<K>>;

/// Functions needed from all layers.
pub trait CompactionLayer<K: CompactionKey> {
    fn key_range(&self) -> &Range<K>;
    fn lsn_range(&self) -> &Range<Lsn>;

    fn file_size(&self) -> u64;

    /// For debugging, short human-readable representation of the layer. E.g. filename.
    fn short_id(&self) -> String;

    fn is_delta(&self) -> bool;
}
pub trait CompactionDeltaLayer<E: CompactionJobExecutor + ?Sized>: CompactionLayer<E::Key> {
    type DeltaEntry<'a>: CompactionDeltaEntry<'a, E::Key>
    where
        Self: 'a;

    /// Return all keys in this delta layer.
    fn load_keys(
        &self,
        ctx: &E::RequestContext,
    ) -> impl Future<Output = anyhow::Result<Vec<Self::DeltaEntry<'_>>>> + Send;
}

pub trait CompactionImageLayer<E: CompactionJobExecutor + ?Sized>: CompactionLayer<E::Key> {}

pub trait CompactionDeltaEntry<'a, K> {
    fn key(&self) -> K;
    fn lsn(&self) -> Lsn;
    fn size(&self) -> u64;
}

pub trait CompactionRequestContext {}
