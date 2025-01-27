use once_cell::sync::OnceCell;
use pageserver_compaction::interface::CompactionLayer;
use pageserver_compaction::simulator::MockTimeline;
use utils::logging;

static LOG_HANDLE: OnceCell<()> = OnceCell::new();

pub(crate) fn setup_logging() {
    LOG_HANDLE.get_or_init(|| {
        logging::init(
            logging::LogFormat::Test,
            logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
            utils::logging::OtelEnablement::Disabled,
            logging::Output::Stdout,
        )
        .expect("Failed to init test logging")
    });
}

/// Test the extreme case that there are so many updates for a single key that
/// even if we produce an extremely narrow delta layer, spanning just that one
/// key, we still too many records to fit in the target file size. We need to
/// split in the LSN dimension too in that case.
#[tokio::test]
async fn test_many_updates_for_single_key() {
    setup_logging();
    let mut executor = MockTimeline::new();
    executor.target_file_size = 1_000_000; // 1 MB

    // Ingest 10 MB of updates to a single key.
    for _ in 1..1000 {
        executor.ingest_uniform(100, 10, &(0..100_000)).unwrap();
        executor.ingest_uniform(1000, 10, &(0..1)).unwrap();
        executor.compact().await.unwrap();
    }

    // Check that all the layers are smaller than the target size (with some slop)
    for l in executor.live_layers.iter() {
        println!("layer {}: {}", l.short_id(), l.file_size());
    }
    for l in executor.live_layers.iter() {
        assert!(l.file_size() < executor.target_file_size * 2);
        // Sanity check that none of the delta layers are empty either.
        if l.is_delta() {
            assert!(l.file_size() > 0);
        }
    }
}

#[tokio::test]
async fn test_simple_updates() {
    setup_logging();
    let mut executor = MockTimeline::new();
    executor.target_file_size = 500_000; // 500 KB

    // Ingest some traffic.
    for _ in 1..400 {
        executor.ingest_uniform(100, 500, &(0..100_000)).unwrap();
    }

    for l in executor.live_layers.iter() {
        println!("layer {}: {}", l.short_id(), l.file_size());
    }

    println!("Running compaction...");
    executor.compact().await.unwrap();

    for l in executor.live_layers.iter() {
        println!("layer {}: {}", l.short_id(), l.file_size());
    }
}
