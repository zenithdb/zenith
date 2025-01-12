//! WAL ingestion benchmarks.

use std::io::Write as _;

use bytes::BytesMut;
use camino_tempfile::tempfile;
use criterion::{criterion_group, criterion_main, BatchSize, Bencher, Criterion};
use itertools::Itertools as _;
use postgres_ffi::v17::wal_generator::{LogicalMessageGenerator, WalGenerator};
use pprof::criterion::{Output, PProfProfiler};
use safekeeper::receive_wal::{self, WalAcceptor};
use safekeeper::safekeeper::{
    AcceptorProposerMessage, AppendRequest, AppendRequestHeader, ProposerAcceptorMessage,
};
use safekeeper::test_utils::Env;
use tokio::io::AsyncWriteExt as _;
use utils::id::{NodeId, TenantTimelineId};
use utils::lsn::Lsn;

const KB: usize = 1024;
const MB: usize = 1024 * KB;
const GB: usize = 1024 * MB;

/// Use jemalloc and enable profiling, to mirror bin/safekeeper.rs.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:21\0";

// Register benchmarks with Criterion.
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_process_msg,
    bench_wal_acceptor,
    bench_wal_acceptor_throughput,
    bench_file_write,
    bench_bytes_reserve,
);
criterion_main!(benches);

/// Benchmarks SafeKeeper::process_msg() as time per message and throughput. Each message is an
/// AppendRequest with a single WAL record containing an XlLogicalMessage of varying size. When
/// measuring throughput, only the logical message payload is considered, excluding
/// segment/page/record headers.
fn bench_process_msg(c: &mut Criterion) {
    let mut g = c.benchmark_group("process_msg");
    for fsync in [false, true] {
        for commit in [false, true] {
            for size in [8, KB, 8 * KB, 128 * KB, MB] {
                // Kind of weird to change the group throughput per benchmark, but it's the only way
                // to vary it per benchmark. It works.
                g.throughput(criterion::Throughput::Bytes(size as u64));
                g.bench_function(format!("fsync={fsync}/commit={commit}/size={size}"), |b| {
                    run_bench(b, size, fsync, commit).unwrap()
                });
            }
        }
    }

    // The actual benchmark. If commit is true, advance the commit LSN on every message.
    fn run_bench(b: &mut Bencher, size: usize, fsync: bool, commit: bool) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread() // single is fine, sync IO only
            .enable_all()
            .build()?;

        // Construct the payload. The prefix counts towards the payload (including NUL terminator).
        let prefix = c"p";
        let prefixlen = prefix.to_bytes_with_nul().len();
        assert!(size >= prefixlen);
        let message = vec![0; size - prefixlen];

        let walgen = &mut WalGenerator::new(LogicalMessageGenerator::new(prefix, &message), Lsn(0));

        // Set up the Safekeeper.
        let env = Env::new(fsync)?;
        let mut safekeeper = runtime.block_on(env.make_safekeeper(
            NodeId(1),
            TenantTimelineId::generate(),
            Lsn(0),
        ))?;

        b.iter_batched_ref(
            // Pre-construct WAL records and requests. Criterion will batch them.
            || {
                let (lsn, record) = walgen.next().expect("endless WAL");
                ProposerAcceptorMessage::AppendRequest(AppendRequest {
                    h: AppendRequestHeader {
                        term: 1,
                        term_start_lsn: Lsn(0),
                        begin_lsn: lsn,
                        end_lsn: lsn + record.len() as u64,
                        commit_lsn: if commit { lsn } else { Lsn(0) }, // commit previous record
                        truncate_lsn: Lsn(0),
                        proposer_uuid: [0; 16],
                    },
                    wal_data: record,
                })
            },
            // Benchmark message processing (time per message).
            |msg| {
                runtime
                    .block_on(safekeeper.process_msg(msg))
                    .expect("message failed")
            },
            BatchSize::SmallInput, // automatically determine a batch size
        );
        Ok(())
    }
}

/// Benchmarks WalAcceptor message processing time by sending it a batch of WAL records and waiting
/// for it to confirm that the last LSN has been flushed to storage. We pipeline a bunch of messages
/// instead of measuring each individual message to amortize costs (e.g. fsync), which is more
/// realistic. Records are XlLogicalMessage with a tiny payload (~64 bytes per record including
/// headers). Records are pre-constructed to avoid skewing the benchmark.
///
/// TODO: add benchmarks with in-memory storage, see comment on `Env::make_safekeeper()`:
fn bench_wal_acceptor(c: &mut Criterion) {
    let mut g = c.benchmark_group("wal_acceptor");
    for fsync in [false, true] {
        for n in [1, 100, 10000] {
            g.bench_function(format!("fsync={fsync}/n={n}"), |b| {
                run_bench(b, n, fsync).unwrap()
            });
        }
    }

    /// The actual benchmark. n is the number of WAL records to send in a pipelined batch.
    fn run_bench(b: &mut Bencher, n: usize, fsync: bool) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?; // needs multithreaded

        let env = Env::new(fsync)?;
        let walgen =
            &mut WalGenerator::new(LogicalMessageGenerator::new(c"prefix", b"message"), Lsn(0));

        // Create buffered channels that can fit all requests, to avoid blocking on channels.
        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(n);
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel(n);

        // Spawn the WalAcceptor task.
        runtime.block_on(async {
            // TODO: WalAcceptor doesn't actually need a full timeline, only
            // Safekeeper::process_msg(). Consider decoupling them to simplify the setup.
            let tli = env
                .make_timeline(NodeId(1), TenantTimelineId::generate(), Lsn(0))
                .await?
                .wal_residence_guard()
                .await?;
            WalAcceptor::spawn(tli, msg_rx, reply_tx, Some(0));
            anyhow::Ok(())
        })?;

        b.iter_batched(
            // Pre-construct a batch of WAL records and requests.
            || {
                walgen
                    .take(n)
                    .map(|(lsn, record)| AppendRequest {
                        h: AppendRequestHeader {
                            term: 1,
                            term_start_lsn: Lsn(0),
                            begin_lsn: lsn,
                            end_lsn: lsn + record.len() as u64,
                            commit_lsn: Lsn(0),
                            truncate_lsn: Lsn(0),
                            proposer_uuid: [0; 16],
                        },
                        wal_data: record,
                    })
                    .collect_vec()
            },
            // Benchmark batch ingestion (time per batch).
            |reqs| {
                runtime.block_on(async {
                    let final_lsn = reqs.last().unwrap().h.end_lsn;
                    // Stuff all the messages into the buffered channel to pipeline them.
                    for req in reqs {
                        let msg = ProposerAcceptorMessage::AppendRequest(req);
                        msg_tx.send(msg).await.expect("send failed");
                    }
                    // Wait for the last message to get flushed.
                    while let Some(reply) = reply_rx.recv().await {
                        if let AcceptorProposerMessage::AppendResponse(resp) = reply {
                            if resp.flush_lsn >= final_lsn {
                                return;
                            }
                        }
                    }
                    panic!("disconnected")
                })
            },
            BatchSize::PerIteration, // only run one request batch at a time
        );
        Ok(())
    }
}

/// Benchmarks WalAcceptor throughput by sending 1 GB of data with varying message sizes and waiting
/// for the last LSN to be flushed to storage. Only the actual message payload counts towards
/// throughput, headers are excluded and considered overhead. Records are XlLogicalMessage.
///
/// To avoid running out of memory, messages are constructed during the benchmark.
fn bench_wal_acceptor_throughput(c: &mut Criterion) {
    const VOLUME: usize = GB; // NB: excludes message/page/segment headers and padding

    let mut g = c.benchmark_group("wal_acceptor_throughput");
    g.sample_size(10);
    g.throughput(criterion::Throughput::Bytes(VOLUME as u64));

    for fsync in [false, true] {
        for commit in [false, true] {
            for size in [KB, 8 * KB, 128 * KB, MB] {
                assert_eq!(VOLUME % size, 0, "volume must be divisible by size");
                let count = VOLUME / size;
                g.bench_function(format!("fsync={fsync}/commit={commit}/size={size}"), |b| {
                    run_bench(b, count, size, fsync, commit).unwrap()
                });
            }
        }
    }

    /// The actual benchmark. size is the payload size per message, count is the number of messages.
    /// If commit is true, advance the commit LSN on each message.
    fn run_bench(
        b: &mut Bencher,
        count: usize,
        size: usize,
        fsync: bool,
        commit: bool,
    ) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?; // needs multithreaded

        // Construct the payload. The prefix counts towards the payload (including NUL terminator).
        let prefix = c"p";
        let prefixlen = prefix.to_bytes_with_nul().len();
        assert!(size >= prefixlen);
        let message = vec![0; size - prefixlen];

        let walgen = &mut WalGenerator::new(LogicalMessageGenerator::new(prefix, &message), Lsn(0));

        // Construct and spawn the WalAcceptor task.
        let env = Env::new(fsync)?;

        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(receive_wal::MSG_QUEUE_SIZE);
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel(receive_wal::REPLY_QUEUE_SIZE);

        runtime.block_on(async {
            let tli = env
                .make_timeline(NodeId(1), TenantTimelineId::generate(), Lsn(0))
                .await?
                .wal_residence_guard()
                .await?;
            WalAcceptor::spawn(tli, msg_rx, reply_tx, Some(0));
            anyhow::Ok(())
        })?;

        // Ingest the WAL.
        b.iter(|| {
            runtime.block_on(async {
                let reqgen = walgen.take(count).map(|(lsn, record)| AppendRequest {
                    h: AppendRequestHeader {
                        term: 1,
                        term_start_lsn: Lsn(0),
                        begin_lsn: lsn,
                        end_lsn: lsn + record.len() as u64,
                        commit_lsn: if commit { lsn } else { Lsn(0) }, // commit previous record
                        truncate_lsn: Lsn(0),
                        proposer_uuid: [0; 16],
                    },
                    wal_data: record,
                });

                // Send requests.
                for req in reqgen {
                    _ = reply_rx.try_recv(); // discard any replies, to avoid blocking
                    let msg = ProposerAcceptorMessage::AppendRequest(req);
                    msg_tx.send(msg).await.expect("send failed");
                }

                // Wait for last message to get flushed.
                while let Some(reply) = reply_rx.recv().await {
                    if let AcceptorProposerMessage::AppendResponse(resp) = reply {
                        if resp.flush_lsn >= walgen.lsn {
                            return;
                        }
                    }
                }
                panic!("disconnected")
            })
        });
        Ok(())
    }
}

/// Benchmarks OS write throughput by appending blocks of a given size to a file. This is intended
/// to compare Tokio and stdlib writes, and give a baseline for optimal WAL throughput.
fn bench_file_write(c: &mut Criterion) {
    let mut g = c.benchmark_group("file_write");

    for kind in ["stdlib", "tokio"] {
        for fsync in [false, true] {
            for size in [8, KB, 8 * KB, 128 * KB, MB] {
                // Kind of weird to change the group throughput per benchmark, but it's the only way to
                // vary it per benchmark. It works.
                g.throughput(criterion::Throughput::Bytes(size as u64));
                g.bench_function(
                    format!("{kind}/fsync={fsync}/size={size}"),
                    |b| match kind {
                        "stdlib" => run_bench_stdlib(b, size, fsync).unwrap(),
                        "tokio" => run_bench_tokio(b, size, fsync).unwrap(),
                        name => panic!("unknown kind {name}"),
                    },
                );
            }
        }
    }

    fn run_bench_stdlib(b: &mut Bencher, size: usize, fsync: bool) -> anyhow::Result<()> {
        let mut file = tempfile()?;
        let buf = vec![0u8; size];

        b.iter(|| {
            file.write_all(&buf).unwrap();
            file.flush().unwrap();
            if fsync {
                file.sync_data().unwrap();
            }
        });

        Ok(())
    }

    fn run_bench_tokio(b: &mut Bencher, size: usize, fsync: bool) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Runtime::new()?; // needs multithreaded

        let mut file = tokio::fs::File::from_std(tempfile()?);
        let buf = vec![0u8; size];

        b.iter(|| {
            runtime.block_on(async {
                file.write_all(&buf).await.unwrap();
                file.flush().await.unwrap();
                if fsync {
                    file.sync_data().await.unwrap();
                }
            })
        });

        Ok(())
    }
}

/// Benchmarks the cost of memory allocations when receiving WAL messages. This emulates the logic
/// in FeMessage::parse, which extends the read buffer. It is primarily intended to test jemalloc.
fn bench_bytes_reserve(c: &mut Criterion) {
    let mut g = c.benchmark_group("bytes_reserve");
    for size in [1, 64, KB, 8 * KB, 128 * KB] {
        g.throughput(criterion::Throughput::Bytes(size as u64));
        g.bench_function(format!("size={size}"), |b| run_bench(b, size).unwrap());
    }

    fn run_bench(b: &mut Bencher, size: usize) -> anyhow::Result<()> {
        let mut bytes = BytesMut::new();
        let data = vec![0; size];

        b.iter(|| {
            bytes.reserve(size);
            bytes.extend_from_slice(&data);
            bytes.split_to(size).freeze();
        });

        Ok(())
    }
}
