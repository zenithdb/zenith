mod draw;

use draw::{LayerTraceEvent, LayerTraceFile, LayerTraceOp};

use futures::StreamExt;
use pageserver_api::shard::ShardIdentity;
use rand::Rng;
use tracing::info;

use utils::lsn::Lsn;

use std::fmt::Write;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;

use crate::helpers::PAGE_SZ;
use crate::helpers::{merge_delta_keys, overlaps_with};

use crate::interface;
use crate::interface::CompactionLayer;

//
// Implementation for the CompactionExecutor interface
//
pub struct MockTimeline {
    // Parameters for the compaction algorithm
    pub target_file_size: u64,
    tiers_per_level: u64,

    num_l0_flushes: u64,
    last_compact_at_flush: u64,
    last_flush_lsn: Lsn,

    // In-memory layer
    records: Vec<MockRecord>,
    total_len: u64,
    start_lsn: Lsn,
    end_lsn: Lsn,

    // Current keyspace at `end_lsn`. This is updated on every ingested record.
    keyspace: KeySpace,

    // historic keyspaces
    old_keyspaces: Vec<(Lsn, KeySpace)>,

    // "on-disk" layers
    pub live_layers: Vec<MockLayer>,

    num_deleted_layers: u64,

    // Statistics
    wal_ingested: u64,
    bytes_written: u64,
    bytes_deleted: u64,
    layers_created: u64,
    layers_deleted: u64,

    // All the events - creation and deletion of files - are collected
    // in 'history'. It is used to draw the SVG animation at the end.
    time: u64,
    history: Vec<draw::LayerTraceEvent>,
}

type KeySpace = interface::CompactionKeySpace<Key>;

pub struct MockRequestContext {}
impl interface::CompactionRequestContext for MockRequestContext {}

pub type Key = u64;

impl interface::CompactionKey for Key {
    const MIN: Self = u64::MIN;
    const MAX: Self = u64::MAX;

    fn key_range_size(key_range: &Range<Self>, _shard_identity: &ShardIdentity) -> u32 {
        std::cmp::min(key_range.end - key_range.start, u32::MAX as u64) as u32
    }

    fn next(&self) -> Self {
        self + 1
    }
    fn skip_some(&self) -> Self {
        // round up to next xx
        self + 100
    }
}

#[derive(Clone)]
pub struct MockRecord {
    lsn: Lsn,
    key: Key,
    len: u64,
}

impl interface::CompactionDeltaEntry<'_, Key> for MockRecord {
    fn key(&self) -> Key {
        self.key
    }
    fn lsn(&self) -> Lsn {
        self.lsn
    }
    fn size(&self) -> u64 {
        self.len
    }
}

pub struct MockDeltaLayer {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,

    pub file_size: u64,

    pub deleted: Mutex<bool>,

    pub records: Vec<MockRecord>,
}

impl interface::CompactionLayer<Key> for Arc<MockDeltaLayer> {
    fn key_range(&self) -> &Range<Key> {
        &self.key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.lsn_range
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn short_id(&self) -> String {
        format!(
            "{:016X}-{:016X}__{:08X}-{:08X}",
            self.key_range.start, self.key_range.end, self.lsn_range.start.0, self.lsn_range.end.0
        )
    }

    fn is_delta(&self) -> bool {
        true
    }
}

impl interface::CompactionDeltaLayer<MockTimeline> for Arc<MockDeltaLayer> {
    type DeltaEntry<'a> = MockRecord;

    async fn load_keys(&self, _ctx: &MockRequestContext) -> anyhow::Result<Vec<MockRecord>> {
        Ok(self.records.clone())
    }
}

pub struct MockImageLayer {
    pub key_range: Range<Key>,
    pub lsn_range: Range<Lsn>,

    pub file_size: u64,

    pub deleted: Mutex<bool>,
}

impl interface::CompactionImageLayer<MockTimeline> for Arc<MockImageLayer> {}

impl interface::CompactionLayer<Key> for Arc<MockImageLayer> {
    fn key_range(&self) -> &Range<Key> {
        &self.key_range
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        &self.lsn_range
    }

    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn short_id(&self) -> String {
        format!(
            "{:016X}-{:016X}__{:08X}",
            self.key_range.start, self.key_range.end, self.lsn_range.start.0,
        )
    }

    fn is_delta(&self) -> bool {
        false
    }
}

impl MockTimeline {
    pub fn new() -> Self {
        MockTimeline {
            target_file_size: 256 * 1024 * 1024,
            tiers_per_level: 4,

            num_l0_flushes: 0,
            last_compact_at_flush: 0,
            last_flush_lsn: Lsn(0),

            records: Vec::new(),
            total_len: 0,
            start_lsn: Lsn(1000),
            end_lsn: Lsn(1000),
            keyspace: KeySpace::new(),

            old_keyspaces: vec![],

            live_layers: vec![],

            num_deleted_layers: 0,

            wal_ingested: 0,
            bytes_written: 0,
            bytes_deleted: 0,
            layers_created: 0,
            layers_deleted: 0,

            time: 0,
            history: Vec::new(),
        }
    }

    pub async fn compact(&mut self) -> anyhow::Result<()> {
        let ctx = MockRequestContext {};

        crate::compact_tiered::compact_tiered(
            self,
            self.last_flush_lsn,
            self.target_file_size,
            self.tiers_per_level,
            &ctx,
        )
        .await?;

        Ok(())
    }

    // Ingest one record to the timeline
    pub fn ingest_record(&mut self, key: Key, len: u64) {
        self.records.push(MockRecord {
            lsn: self.end_lsn,
            key,
            len,
        });
        self.total_len += len;
        self.end_lsn += len;

        if self.total_len > self.target_file_size {
            self.flush_l0();
        }
    }

    pub async fn compact_if_needed(&mut self) -> anyhow::Result<()> {
        if self.num_l0_flushes - self.last_compact_at_flush >= self.tiers_per_level {
            self.compact().await?;
            self.last_compact_at_flush = self.num_l0_flushes;
        }
        Ok(())
    }

    pub fn flush_l0(&mut self) {
        if self.records.is_empty() {
            return;
        }

        let mut records = std::mem::take(&mut self.records);
        records.sort_by_key(|rec| rec.key);

        let lsn_range = self.start_lsn..self.end_lsn;
        let new_layer = Arc::new(MockDeltaLayer {
            key_range: Key::MIN..Key::MAX,
            lsn_range: lsn_range.clone(),
            file_size: self.total_len,
            records,
            deleted: Mutex::new(false),
        });
        info!("flushed L0 layer {}", new_layer.short_id());
        self.live_layers.push(MockLayer::from(&new_layer));

        // reset L0
        self.start_lsn = self.end_lsn;
        self.total_len = 0;
        self.records = Vec::new();

        self.layers_created += 1;
        self.bytes_written += new_layer.file_size;

        self.time += 1;
        self.history.push(LayerTraceEvent {
            time_rel: self.time,
            op: LayerTraceOp::Flush,
            file: LayerTraceFile {
                filename: new_layer.short_id(),
                key_range: new_layer.key_range.clone(),
                lsn_range: new_layer.lsn_range.clone(),
            },
        });

        self.num_l0_flushes += 1;
        self.last_flush_lsn = self.end_lsn;
    }

    // Ingest `num_records' records to the timeline, with random keys
    // uniformly distributed in `key_range`
    pub fn ingest_uniform(
        &mut self,
        num_records: u64,
        len: u64,
        key_range: &Range<Key>,
    ) -> anyhow::Result<()> {
        crate::helpers::union_to_keyspace(&mut self.keyspace, vec![key_range.clone()]);
        let mut rng = rand::thread_rng();
        for _ in 0..num_records {
            self.ingest_record(rng.gen_range(key_range.clone()), len);
            self.wal_ingested += len;
        }
        Ok(())
    }

    pub fn stats(&self) -> anyhow::Result<String> {
        let mut s = String::new();

        writeln!(s, "STATISTICS:")?;
        writeln!(
            s,
            "WAL ingested:   {:>10} MB",
            self.wal_ingested / (1024 * 1024)
        )?;
        writeln!(
            s,
            "size created:   {:>10} MB",
            self.bytes_written / (1024 * 1024)
        )?;
        writeln!(
            s,
            "size deleted:   {:>10} MB",
            self.bytes_deleted / (1024 * 1024)
        )?;
        writeln!(s, "files created:     {:>10}", self.layers_created)?;
        writeln!(s, "files deleted:     {:>10}", self.layers_deleted)?;
        writeln!(
            s,
            "write amp:         {:>10.2}",
            self.bytes_written as f64 / self.wal_ingested as f64
        )?;
        writeln!(
            s,
            "storage amp:       {:>10.2}",
            (self.bytes_written - self.bytes_deleted) as f64 / self.wal_ingested as f64
        )?;

        Ok(s)
    }

    pub fn draw_history<W: std::io::Write>(&self, output: W) -> anyhow::Result<()> {
        draw::draw_history(&self.history, output)
    }
}

impl Default for MockTimeline {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub enum MockLayer {
    Delta(Arc<MockDeltaLayer>),
    Image(Arc<MockImageLayer>),
}

impl interface::CompactionLayer<Key> for MockLayer {
    fn key_range(&self) -> &Range<Key> {
        match self {
            MockLayer::Delta(this) => this.key_range(),
            MockLayer::Image(this) => this.key_range(),
        }
    }
    fn lsn_range(&self) -> &Range<Lsn> {
        match self {
            MockLayer::Delta(this) => this.lsn_range(),
            MockLayer::Image(this) => this.lsn_range(),
        }
    }
    fn file_size(&self) -> u64 {
        match self {
            MockLayer::Delta(this) => this.file_size,
            MockLayer::Image(this) => this.file_size,
        }
    }
    fn short_id(&self) -> String {
        match self {
            MockLayer::Delta(this) => this.short_id(),
            MockLayer::Image(this) => this.short_id(),
        }
    }

    fn is_delta(&self) -> bool {
        match self {
            MockLayer::Delta(_) => true,
            MockLayer::Image(_) => false,
        }
    }
}

impl MockLayer {
    fn is_deleted(&self) -> bool {
        let guard = match self {
            MockLayer::Delta(this) => this.deleted.lock().unwrap(),
            MockLayer::Image(this) => this.deleted.lock().unwrap(),
        };
        *guard
    }
    fn mark_deleted(&self) {
        let mut deleted_guard = match self {
            MockLayer::Delta(this) => this.deleted.lock().unwrap(),
            MockLayer::Image(this) => this.deleted.lock().unwrap(),
        };
        assert!(!*deleted_guard, "layer already deleted");
        *deleted_guard = true;
    }
}

impl From<&Arc<MockDeltaLayer>> for MockLayer {
    fn from(l: &Arc<MockDeltaLayer>) -> Self {
        MockLayer::Delta(l.clone())
    }
}

impl From<&Arc<MockImageLayer>> for MockLayer {
    fn from(l: &Arc<MockImageLayer>) -> Self {
        MockLayer::Image(l.clone())
    }
}

impl interface::CompactionJobExecutor for MockTimeline {
    type Key = Key;
    type Layer = MockLayer;
    type DeltaLayer = Arc<MockDeltaLayer>;
    type ImageLayer = Arc<MockImageLayer>;
    type RequestContext = MockRequestContext;

    fn get_shard_identity(&self) -> &ShardIdentity {
        static IDENTITY: ShardIdentity = ShardIdentity::unsharded();
        &IDENTITY
    }

    async fn get_layers(
        &mut self,
        key_range: &Range<Self::Key>,
        lsn_range: &Range<Lsn>,
        _ctx: &Self::RequestContext,
    ) -> anyhow::Result<Vec<Self::Layer>> {
        // Clear any deleted layers from our vec
        self.live_layers.retain(|l| !l.is_deleted());

        let layers: Vec<MockLayer> = self
            .live_layers
            .iter()
            .filter(|l| {
                overlaps_with(l.lsn_range(), lsn_range) && overlaps_with(l.key_range(), key_range)
            })
            .cloned()
            .collect();

        Ok(layers)
    }

    async fn get_keyspace(
        &mut self,
        key_range: &Range<Self::Key>,
        _lsn: Lsn,
        _ctx: &Self::RequestContext,
    ) -> anyhow::Result<interface::CompactionKeySpace<Key>> {
        // find it in the levels
        if self.old_keyspaces.is_empty() {
            Ok(crate::helpers::intersect_keyspace(
                &self.keyspace,
                key_range,
            ))
        } else {
            // not implemented

            // The mock implementation only allows requesting the
            // keyspace at the level's end LSN. That's all that the
            // current implementation needs.
            panic!("keyspace not available for requested lsn");
        }
    }

    async fn downcast_delta_layer(
        &self,
        layer: &MockLayer,
    ) -> anyhow::Result<Option<Arc<MockDeltaLayer>>> {
        Ok(match layer {
            MockLayer::Delta(l) => Some(l.clone()),
            MockLayer::Image(_) => None,
        })
    }

    async fn create_image(
        &mut self,
        lsn: Lsn,
        key_range: &Range<Key>,
        ctx: &MockRequestContext,
    ) -> anyhow::Result<()> {
        let keyspace = self.get_keyspace(key_range, lsn, ctx).await?;

        let mut accum_size: u64 = 0;
        for r in keyspace {
            accum_size += r.end - r.start;
        }

        let new_layer = Arc::new(MockImageLayer {
            key_range: key_range.clone(),
            lsn_range: lsn..lsn,
            file_size: accum_size * PAGE_SZ,
            deleted: Mutex::new(false),
        });
        info!(
            "created image layer, size {}: {}",
            new_layer.file_size,
            new_layer.short_id()
        );
        self.live_layers.push(MockLayer::Image(new_layer.clone()));

        // update stats
        self.bytes_written += new_layer.file_size;
        self.layers_created += 1;

        self.time += 1;
        self.history.push(LayerTraceEvent {
            time_rel: self.time,
            op: LayerTraceOp::CreateImage,
            file: LayerTraceFile {
                filename: new_layer.short_id(),
                key_range: new_layer.key_range.clone(),
                lsn_range: new_layer.lsn_range.clone(),
            },
        });

        Ok(())
    }

    async fn create_delta(
        &mut self,
        lsn_range: &Range<Lsn>,
        key_range: &Range<Key>,
        input_layers: &[Arc<MockDeltaLayer>],
        ctx: &MockRequestContext,
    ) -> anyhow::Result<()> {
        let mut key_value_stream =
            std::pin::pin!(merge_delta_keys::<MockTimeline>(input_layers, ctx));
        let mut records: Vec<MockRecord> = Vec::new();
        let mut total_len = 2;
        while let Some(delta_entry) = key_value_stream.next().await {
            let delta_entry: MockRecord = delta_entry?;
            if key_range.contains(&delta_entry.key) && lsn_range.contains(&delta_entry.lsn) {
                total_len += delta_entry.len;
                records.push(delta_entry);
            }
        }
        let total_records = records.len();
        let new_layer = Arc::new(MockDeltaLayer {
            key_range: key_range.clone(),
            lsn_range: lsn_range.clone(),
            file_size: total_len,
            records,
            deleted: Mutex::new(false),
        });
        info!(
            "created delta layer, recs {}, size {}: {}",
            total_records,
            total_len,
            new_layer.short_id()
        );
        self.live_layers.push(MockLayer::Delta(new_layer.clone()));

        // update stats
        self.bytes_written += total_len;
        self.layers_created += 1;

        self.time += 1;
        self.history.push(LayerTraceEvent {
            time_rel: self.time,
            op: LayerTraceOp::CreateDelta,
            file: LayerTraceFile {
                filename: new_layer.short_id(),
                key_range: new_layer.key_range.clone(),
                lsn_range: new_layer.lsn_range.clone(),
            },
        });

        Ok(())
    }

    async fn delete_layer(
        &mut self,
        layer: &Self::Layer,
        _ctx: &MockRequestContext,
    ) -> anyhow::Result<()> {
        let layer = std::pin::pin!(layer);
        info!("deleting layer: {}", layer.short_id());
        self.num_deleted_layers += 1;
        self.bytes_deleted += layer.file_size();
        layer.mark_deleted();

        self.time += 1;
        self.history.push(LayerTraceEvent {
            time_rel: self.time,
            op: LayerTraceOp::Delete,
            file: LayerTraceFile {
                filename: layer.short_id(),
                key_range: layer.key_range().clone(),
                lsn_range: layer.lsn_range().clone(),
            },
        });

        Ok(())
    }
}
