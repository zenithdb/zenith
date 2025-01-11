use anyhow::Context;
use criterion::{criterion_group, criterion_main, Criterion};
use futures::{stream::FuturesUnordered, StreamExt};
use pageserver_api::shard::{ShardIdentity, ShardStripeSize};
use postgres_ffi::{waldecoder::WalStreamDecoder, MAX_SEND_SIZE, WAL_SEGMENT_SIZE};
use pprof::criterion::{Output, PProfProfiler};
use serde::Deserialize;
use std::{env, num::NonZeroUsize, sync::Arc};

use camino::{Utf8Path, Utf8PathBuf};
use camino_tempfile::Utf8TempDir;
use remote_storage::{
    DownloadOpts, GenericRemoteStorage, ListingMode, RemoteStorageConfig, RemoteStorageKind,
    S3Config,
};
use tokio_util::sync::CancellationToken;
use utils::{
    lsn::Lsn,
    shard::{ShardCount, ShardNumber},
};
use wal_decoder::models::InterpretedWalRecord;

const S3_BUCKET: &str = "neon-github-public-dev";
const S3_REGION: &str = "eu-central-1";
const BUCKET_PREFIX: &str = "wal-snapshots/bulk-insert/";
const METADATA_FILENAME: &str = "metadata.json";

/// Use jemalloc, and configure it to sample allocations for profiles every 1 MB.
/// This mirrors the configuration in bin/safekeeper.rs.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:20\0";

async fn create_s3_client() -> anyhow::Result<Arc<GenericRemoteStorage>> {
    let remote_storage_config = RemoteStorageConfig {
        storage: RemoteStorageKind::AwsS3(S3Config {
            bucket_name: S3_BUCKET.to_string(),
            bucket_region: S3_REGION.to_string(),
            prefix_in_bucket: Some(BUCKET_PREFIX.to_string()),
            endpoint: None,
            concurrency_limit: NonZeroUsize::new(100).unwrap(),
            max_keys_per_list_response: None,
            upload_storage_class: None,
        }),
        timeout: RemoteStorageConfig::DEFAULT_TIMEOUT,
        small_timeout: RemoteStorageConfig::DEFAULT_SMALL_TIMEOUT,
    };
    Ok(Arc::new(
        GenericRemoteStorage::from_config(&remote_storage_config)
            .await
            .context("remote storage init")?,
    ))
}

async fn download_bench_data(
    client: Arc<GenericRemoteStorage>,
    cancel: &CancellationToken,
) -> anyhow::Result<Utf8TempDir> {
    let temp_dir_parent: Utf8PathBuf = env::current_dir().unwrap().try_into()?;
    let temp_dir = camino_tempfile::tempdir_in(temp_dir_parent)?;

    eprintln!("Downloading benchmark data to {:?}", temp_dir);

    let listing = client
        .list(None, ListingMode::NoDelimiter, None, cancel)
        .await?;

    let mut downloads = listing
        .keys
        .into_iter()
        .map(|obj| {
            let client = client.clone();
            let temp_dir_path = temp_dir.path().to_owned();

            async move {
                let remote_path = obj.key;
                let download = client
                    .download(&remote_path, &DownloadOpts::default(), cancel)
                    .await?;
                let mut body = tokio_util::io::StreamReader::new(download.download_stream);

                let file_name = remote_path.object_name().unwrap();
                let file_path = temp_dir_path.join(file_name);
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&file_path)
                    .await?;

                let mut writer = tokio::io::BufWriter::new(file);
                tokio::io::copy_buf(&mut body, &mut writer).await?;

                Ok::<(), anyhow::Error>(())
            }
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(download) = downloads.next().await {
        download?;
    }

    Ok(temp_dir)
}

struct BenchmarkData {
    wal: Vec<u8>,
    meta: BenchmarkMetadata,
}

#[derive(Deserialize)]
struct BenchmarkMetadata {
    pg_version: u32,
    start_lsn: Lsn,
}

async fn load_bench_data(path: &Utf8Path, input_size: usize) -> anyhow::Result<BenchmarkData> {
    eprintln!("Loading benchmark data from {:?}", path);

    let mut entries = tokio::fs::read_dir(path).await?;
    let mut ordered_segment_paths = Vec::new();
    let mut metadata = None;

    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name() == METADATA_FILENAME {
            let bytes = tokio::fs::read(entry.path()).await?;
            metadata = Some(
                serde_json::from_slice::<BenchmarkMetadata>(&bytes)
                    .context("failed to deserialize metadata.json")?,
            );
        } else {
            ordered_segment_paths.push(entry.path());
        }
    }

    ordered_segment_paths.sort();

    let mut buffer = Vec::new();
    for path in ordered_segment_paths {
        if buffer.len() >= input_size {
            break;
        }

        use async_compression::tokio::bufread::ZstdDecoder;
        let file = tokio::fs::File::open(path).await?;
        let reader = tokio::io::BufReader::new(file);
        let decoder = ZstdDecoder::new(reader);
        let mut reader = tokio::io::BufReader::new(decoder);
        tokio::io::copy_buf(&mut reader, &mut buffer).await?;
    }

    buffer.truncate(input_size);

    Ok(BenchmarkData {
        wal: buffer,
        meta: metadata.unwrap(),
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    const INPUT_SIZE: usize = 128 * 1024 * 1024;

    let setup_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let (_temp_dir, bench_data) = setup_runtime.block_on(async move {
        let cancel = CancellationToken::new();
        let client = create_s3_client().await.unwrap();
        let temp_dir = download_bench_data(client, &cancel).await.unwrap();
        let bench_data = load_bench_data(temp_dir.path(), INPUT_SIZE).await.unwrap();

        (temp_dir, bench_data)
    });

    eprintln!(
        "Benchmarking against {} MiB of WAL",
        INPUT_SIZE / 1024 / 1024
    );

    let mut group = c.benchmark_group("decode-interpret-wal");
    group.throughput(criterion::Throughput::Bytes(bench_data.wal.len() as u64));
    group.sample_size(10);

    group.bench_function("unsharded", |b| {
        b.iter(|| decode_interpret_main(&bench_data, &[ShardIdentity::unsharded()]))
    });

    let eight_shards = (0..8)
        .map(|i| ShardIdentity::new(ShardNumber(i), ShardCount(8), ShardStripeSize(8)).unwrap())
        .collect::<Vec<_>>();

    group.bench_function("8/8-shards", |b| {
        b.iter(|| decode_interpret_main(&bench_data, &eight_shards))
    });

    let four_shards = eight_shards
        .into_iter()
        .filter(|s| s.number.0 % 2 == 0)
        .collect::<Vec<_>>();
    group.bench_function("4/8-shards", |b| {
        b.iter(|| decode_interpret_main(&bench_data, &four_shards))
    });

    let two_shards = four_shards
        .into_iter()
        .filter(|s| s.number.0 % 4 == 0)
        .collect::<Vec<_>>();
    group.bench_function("2/8-shards", |b| {
        b.iter(|| decode_interpret_main(&bench_data, &two_shards))
    });
}

fn decode_interpret_main(bench: &BenchmarkData, shards: &[ShardIdentity]) {
    let r = decode_interpret(bench, shards);
    if let Err(e) = r {
        panic!("{e:?}");
    }
}

fn decode_interpret(bench: &BenchmarkData, shard: &[ShardIdentity]) -> anyhow::Result<()> {
    let mut decoder = WalStreamDecoder::new(bench.meta.start_lsn, bench.meta.pg_version);
    let xlogoff: usize = bench.meta.start_lsn.segment_offset(WAL_SEGMENT_SIZE);

    for chunk in bench.wal[xlogoff..].chunks(MAX_SEND_SIZE) {
        decoder.feed_bytes(chunk);
        while let Some((lsn, recdata)) = decoder.poll_decode().unwrap() {
            assert!(lsn.is_aligned());
            let _ = InterpretedWalRecord::from_bytes_filtered(
                recdata,
                shard,
                lsn,
                bench.meta.pg_version,
            )
            .unwrap();
        }
    }

    Ok(())
}
criterion_group!(
    name=benches;
    config=Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets=criterion_benchmark
);
criterion_main!(benches);
