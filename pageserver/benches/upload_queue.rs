//! Upload queue benchmarks.

use std::str::FromStr as _;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use pageserver::tenant::metadata::TimelineMetadata;
use pageserver::tenant::remote_timeline_client::index::LayerFileMetadata;
use pageserver::tenant::storage_layer::LayerName;
use pageserver::tenant::upload_queue::{Delete, UploadOp, UploadQueue, UploadTask};
use pageserver::tenant::IndexPart;
use pprof::criterion::{Output, PProfProfiler};
use utils::generation::Generation;
use utils::shard::{ShardCount, ShardIndex, ShardNumber};

// Register benchmarks with Criterion.
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_upload_queue_next_ready,
);
criterion_main!(benches);

/// Benchmarks the cost of UploadQueue::next_ready() with the given number of in-progress tasks
/// (which is equivalent to tasks ahead of it in the queue). This has linear cost, and the upload
/// queue as a whole is thus quadratic.
///
/// UploadOp::UploadLayer requires an entire tenant and timeline to construct, so we just test
/// Delete and UploadMetadata instead. This is incidentally the most expensive case.
fn bench_upload_queue_next_ready(c: &mut Criterion) {
    let mut g = c.benchmark_group("upload_queue_next_ready");
    for inprogress in [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000] {
        g.bench_function(format!("inprogress={inprogress}"), |b| {
            run_bench(b, inprogress).unwrap()
        });
    }

    fn run_bench(b: &mut Bencher, inprogress: usize) -> anyhow::Result<()> {
        // Construct two layers. layer0 is in the indexes, layer1 will be deleted.
        let layer0 = LayerName::from_str("000000000000000000000000000000000000-100000000000000000000000000000000000__00000000016B59D8-00000000016B5A51").expect("invalid name");
        let layer1 = LayerName::from_str("100000000000000000000000000000000001-200000000000000000000000000000000000__00000000016B59D8-00000000016B5A51").expect("invalid name");

        let metadata = LayerFileMetadata {
            shard: ShardIndex::new(ShardNumber(1), ShardCount(2)),
            generation: Generation::Valid(1),
            file_size: 0,
        };

        // Construct the (initial and uploaded) index with layer0.
        let mut index = IndexPart::empty(TimelineMetadata::example());
        index.layer_metadata.insert(layer0, metadata.clone());

        // Construct the queue.
        let mut queue = UploadQueue::Uninitialized;
        let queue = queue.initialize_with_current_remote_index_part(&index)?;

        // Populate inprogress_tasks with a bunch of layer1 deletions.
        let delete = UploadOp::Delete(Delete {
            layers: vec![(layer1, metadata)],
        });

        for task_id in 0..(inprogress as u64) {
            queue.inprogress_tasks.insert(
                task_id,
                Arc::new(UploadTask {
                    task_id,
                    retries: AtomicU32::new(0),
                    op: delete.clone(),
                    coalesced_ops: Vec::new(),
                }),
            );
        }

        // Benchmark index upload scheduling.
        let index_upload = UploadOp::UploadMetadata {
            uploaded: Box::new(index),
        };

        b.iter(|| {
            queue.queued_operations.push_front(index_upload.clone());
            assert!(queue.next_ready().is_some());
        });

        Ok(())
    }
}
