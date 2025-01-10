use anyhow::Context;
use camino::Utf8PathBuf;
use pageserver_api::key::Key;
use pageserver_api::keyspace::KeySpaceAccum;
use pageserver_api::models::{PagestreamGetPageRequest, PagestreamRequest};

use pageserver_api::shard::TenantShardId;
use tokio_util::sync::CancellationToken;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;

use rand::prelude::*;
use tokio::task::JoinSet;
use tracing::info;

use std::collections::HashSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::util::tokio_thread_local_stats::AllThreadLocalStats;
use crate::util::{request_stats, tokio_thread_local_stats};

/// GetPage@LatestLSN, uniformly distributed across the compute-accessible keyspace.
#[derive(clap::Parser)]
pub(crate) struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    #[clap(long)]
    pageserver_jwt: Option<String>,
    #[clap(long, default_value = "1")]
    num_clients: NonZeroUsize,
    #[clap(long)]
    runtime: Option<humantime::Duration>,
    /// Each client sends requests at the given rate.
    ///
    /// If a request takes too long and we should be issuing a new request already,
    /// we skip that request and account it as `MISSED`.
    #[clap(long)]
    per_client_rate: Option<usize>,
    /// Probability for sending `latest=true` in the request (uniform distribution).
    #[clap(long, default_value = "1")]
    req_latest_probability: f64,
    #[clap(long)]
    limit_to_first_n_targets: Option<usize>,
    /// For large pageserver installations, enumerating the keyspace takes a lot of time.
    /// If specified, the specified path is used to maintain a cache of the keyspace enumeration result.
    /// The cache is tagged and auto-invalided by the tenant/timeline ids only.
    /// It doesn't get invalidated if the keyspace changes under the hood, e.g., due to new ingested data or compaction.
    #[clap(long)]
    keyspace_cache: Option<Utf8PathBuf>,
    /// Before starting the benchmark, live-reconfigure the pageserver to use the given
    /// [`pageserver_api::models::virtual_file::IoEngineKind`].
    #[clap(long)]
    set_io_engine: Option<pageserver_api::models::virtual_file::IoEngineKind>,

    /// Before starting the benchmark, live-reconfigure the pageserver to use specified io mode (buffered vs. direct).
    #[clap(long)]
    set_io_mode: Option<pageserver_api::models::virtual_file::IoMode>,

    targets: Option<Vec<TenantTimelineId>>,
}

#[derive(Debug, Default)]
struct LiveStats {
    completed_requests: AtomicU64,
    missed: AtomicU64,
}

impl LiveStats {
    fn request_done(&self) {
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
    fn missed(&self, n: u64) {
        self.missed.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct KeyRange {
    timeline: TenantTimelineId,
    timeline_lsn: Lsn,
    start: i128,
    end: i128,
}

impl KeyRange {
    fn len(&self) -> i128 {
        self.end - self.start
    }
}

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
struct WorkerId {
    timeline: TenantTimelineId,
    num_client: usize, // from 0..args.num_clients
}

#[derive(serde::Serialize)]
struct Output {
    total: request_stats::Output,
}

tokio_thread_local_stats::declare!(STATS: request_stats::Stats);

pub(crate) fn main(args: Args) -> anyhow::Result<()> {
    tokio_thread_local_stats::main!(STATS, move |thread_local_stats| {
        main_impl(args, thread_local_stats)
    })
}

async fn main_impl(
    args: Args,
    all_thread_local_stats: AllThreadLocalStats<request_stats::Stats>,
) -> anyhow::Result<()> {
    let args: &'static Args = Box::leak(Box::new(args));

    let mgmt_api_client = Arc::new(pageserver_client::mgmt_api::Client::new(
        args.mgmt_api_endpoint.clone(),
        args.pageserver_jwt.as_deref(),
    ));

    if let Some(engine_str) = &args.set_io_engine {
        mgmt_api_client.put_io_engine(engine_str).await?;
    }

    if let Some(mode) = &args.set_io_mode {
        mgmt_api_client.put_io_mode(mode).await?;
    }

    // discover targets
    let timelines: Vec<TenantTimelineId> = crate::util::cli::targets::discover(
        &mgmt_api_client,
        crate::util::cli::targets::Spec {
            limit_to_first_n_targets: args.limit_to_first_n_targets,
            targets: args.targets.clone(),
        },
    )
    .await?;

    #[derive(serde::Deserialize)]
    struct KeyspaceCacheDe {
        tag: Vec<TenantTimelineId>,
        data: Vec<KeyRange>,
    }
    #[derive(serde::Serialize)]
    struct KeyspaceCacheSer<'a> {
        tag: &'a [TenantTimelineId],
        data: &'a [KeyRange],
    }
    let cache = args
        .keyspace_cache
        .as_ref()
        .map(|keyspace_cache_file| {
            let contents = match std::fs::read(keyspace_cache_file) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return anyhow::Ok(None);
                }
                x => x.context("read keyspace cache file")?,
            };
            let cache: KeyspaceCacheDe =
                serde_json::from_slice(&contents).context("deserialize cache file")?;
            let tag_ok = HashSet::<TenantTimelineId>::from_iter(cache.tag.into_iter())
                == HashSet::from_iter(timelines.iter().cloned());
            info!("keyspace cache file matches tag: {tag_ok}");
            anyhow::Ok(if tag_ok { Some(cache.data) } else { None })
        })
        .transpose()?
        .flatten();
    let all_ranges: Vec<KeyRange> = if let Some(cached) = cache {
        info!("using keyspace cache file");
        cached
    } else {
        let mut js = JoinSet::new();
        for timeline in &timelines {
            js.spawn({
                let mgmt_api_client = Arc::clone(&mgmt_api_client);
                let timeline = *timeline;
                async move {
                    let partitioning = mgmt_api_client
                        .keyspace(
                            TenantShardId::unsharded(timeline.tenant_id),
                            timeline.timeline_id,
                        )
                        .await?;
                    let lsn = partitioning.at_lsn;
                    let start = Instant::now();
                    let mut filtered = KeySpaceAccum::new();
                    // let's hope this is inlined and vectorized...
                    // TODO: turn this loop into a is_rel_block_range() function.
                    for r in partitioning.keys.ranges.iter() {
                        let mut i = r.start;
                        while i != r.end {
                            if i.is_rel_block_key() {
                                filtered.add_key(i);
                            }
                            i = i.next();
                        }
                    }
                    let filtered = filtered.to_keyspace();
                    let filter_duration = start.elapsed();

                    anyhow::Ok((
                        filter_duration,
                        filtered.ranges.into_iter().map(move |r| KeyRange {
                            timeline,
                            timeline_lsn: lsn,
                            start: r.start.to_i128(),
                            end: r.end.to_i128(),
                        }),
                    ))
                }
            });
        }
        let mut total_filter_duration = Duration::from_secs(0);
        let mut all_ranges: Vec<KeyRange> = Vec::new();
        while let Some(res) = js.join_next().await {
            let (filter_duration, range) = res.unwrap().unwrap();
            all_ranges.extend(range);
            total_filter_duration += filter_duration;
        }
        info!("filter duration: {}", total_filter_duration.as_secs_f64());
        if let Some(cachefile) = args.keyspace_cache.as_ref() {
            let cache = KeyspaceCacheSer {
                tag: &timelines,
                data: &all_ranges,
            };
            let bytes = serde_json::to_vec(&cache).context("serialize keyspace for cache file")?;
            std::fs::write(cachefile, bytes).context("write keyspace cache file to disk")?;
            info!("successfully wrote keyspace cache file");
        }
        all_ranges
    };

    let live_stats = Arc::new(LiveStats::default());

    let num_live_stats_dump = 1;
    let num_work_sender_tasks = args.num_clients.get() * timelines.len();
    let num_main_impl = 1;

    let start_work_barrier = Arc::new(tokio::sync::Barrier::new(
        num_live_stats_dump + num_work_sender_tasks + num_main_impl,
    ));

    tokio::spawn({
        let stats = Arc::clone(&live_stats);
        let start_work_barrier = Arc::clone(&start_work_barrier);
        async move {
            start_work_barrier.wait().await;
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                let missed = stats.missed.swap(0, Ordering::Relaxed);
                let elapsed = start.elapsed();
                info!(
                    "RPS: {:.0}   MISSED: {:.0}",
                    completed_requests as f64 / elapsed.as_secs_f64(),
                    missed as f64 / elapsed.as_secs_f64()
                );
            }
        }
    });

    let cancel = CancellationToken::new();

    let rps_period = args
        .per_client_rate
        .map(|rps_limit| Duration::from_secs_f64(1.0 / (rps_limit as f64)));
    let make_worker: &dyn Fn(WorkerId) -> Pin<Box<dyn Send + Future<Output = ()>>> = &|worker_id| {
        let live_stats = live_stats.clone();
        let start_work_barrier = start_work_barrier.clone();
        let ranges: Vec<KeyRange> = all_ranges
            .iter()
            .filter(|r| r.timeline == worker_id.timeline)
            .cloned()
            .collect();
        let weights =
            rand::distributions::weighted::WeightedIndex::new(ranges.iter().map(|v| v.len()))
                .unwrap();

        let cancel = cancel.clone();
        Box::pin(async move {
            let client =
                pageserver_client::page_service::Client::new(args.page_service_connstring.clone())
                    .await
                    .unwrap();
            let mut client = client
                .pagestream(worker_id.timeline.tenant_id, worker_id.timeline.timeline_id)
                .await
                .unwrap();

            start_work_barrier.wait().await;
            let client_start = Instant::now();
            let mut ticks_processed = 0;
            while !cancel.is_cancelled() {
                // Detect if a request took longer than the RPS rate
                if let Some(period) = &rps_period {
                    let periods_passed_until_now =
                        usize::try_from(client_start.elapsed().as_micros() / period.as_micros())
                            .unwrap();

                    if periods_passed_until_now > ticks_processed {
                        live_stats.missed((periods_passed_until_now - ticks_processed) as u64);
                    }
                    ticks_processed = periods_passed_until_now;
                }

                let start = Instant::now();
                let req = {
                    let mut rng = rand::thread_rng();
                    let r = &ranges[weights.sample(&mut rng)];
                    let key: i128 = rng.gen_range(r.start..r.end);
                    let key = Key::from_i128(key);
                    assert!(key.is_rel_block_key());
                    let (rel_tag, block_no) = key
                        .to_rel_block()
                        .expect("we filter non-rel-block keys out above");
                    PagestreamGetPageRequest {
                        hdr: PagestreamRequest {
                            reqid: 0,
                            request_lsn: if rng.gen_bool(args.req_latest_probability) {
                                Lsn::MAX
                            } else {
                                r.timeline_lsn
                            },
                            not_modified_since: r.timeline_lsn,
                        },
                        rel: rel_tag,
                        blkno: block_no,
                    }
                };
                client.getpage(req).await.unwrap();
                let end = Instant::now();
                live_stats.request_done();
                ticks_processed += 1;
                STATS.with(|stats| {
                    stats
                        .borrow()
                        .lock()
                        .unwrap()
                        .observe(end.duration_since(start))
                        .unwrap();
                });

                if let Some(period) = &rps_period {
                    let next_at = client_start
                        + Duration::from_micros(
                            (ticks_processed) as u64 * u64::try_from(period.as_micros()).unwrap(),
                        );
                    tokio::time::sleep_until(next_at.into()).await;
                }
            }
        })
    };

    info!("spawning workers");
    let mut workers = JoinSet::new();
    for timeline in timelines.iter().cloned() {
        for num_client in 0..args.num_clients.get() {
            let worker_id = WorkerId {
                timeline,
                num_client,
            };
            workers.spawn(make_worker(worker_id));
        }
    }
    let workers = async move {
        while let Some(res) = workers.join_next().await {
            res.unwrap();
        }
    };

    info!("waiting for everything to become ready");
    start_work_barrier.wait().await;
    info!("work started");
    if let Some(runtime) = args.runtime {
        tokio::time::sleep(runtime.into()).await;
        info!("runtime over, signalling cancellation");
        cancel.cancel();
        workers.await;
        info!("work sender exited");
    } else {
        workers.await;
        unreachable!("work sender never terminates");
    }

    let output = Output {
        total: {
            let mut agg_stats = request_stats::Stats::new();
            for stats in all_thread_local_stats.lock().unwrap().iter() {
                let stats = stats.lock().unwrap();
                agg_stats.add(&stats);
            }
            agg_stats.output()
        },
    };

    let output = serde_json::to_string_pretty(&output).unwrap();
    println!("{output}");

    anyhow::Ok(())
}
