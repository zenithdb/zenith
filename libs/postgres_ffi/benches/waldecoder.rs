use std::ffi::CStr;

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use postgres_ffi::v17::wal_generator::LogicalMessageGenerator;
use postgres_ffi::v17::waldecoder_handler::WalStreamDecoderHandler;
use postgres_ffi::waldecoder::WalStreamDecoder;
use pprof::criterion::{Output, PProfProfiler};
use utils::lsn::Lsn;

const KB: usize = 1024;

// Register benchmarks with Criterion.
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_complete_record,
);
criterion_main!(benches);

/// Benchmarks WalStreamDecoder::complete_record() for a logical message of varying size.
fn bench_complete_record(c: &mut Criterion) {
    let mut g = c.benchmark_group("complete_record");
    for size in [64, KB, 8 * KB, 128 * KB] {
        // Kind of weird to change the group throughput per benchmark, but it's the only way
        // to vary it per benchmark. It works.
        g.throughput(criterion::Throughput::Bytes(size as u64));
        g.bench_function(format!("size={size}"), |b| run_bench(b, size).unwrap());
    }

    fn run_bench(b: &mut Bencher, size: usize) -> anyhow::Result<()> {
        const PREFIX: &CStr = c"";
        let value_size = LogicalMessageGenerator::make_value_size(size, PREFIX);
        let value = vec![1; value_size];

        let mut decoder = WalStreamDecoder::new(Lsn(0), 170000);
        let msg = LogicalMessageGenerator::new(PREFIX, &value)
            .next()
            .unwrap()
            .encode(Lsn(0));
        assert_eq!(msg.len(), size);

        b.iter(|| {
            let msg = msg.clone(); // Bytes::clone() is cheap
            decoder.complete_record(msg).unwrap();
        });

        Ok(())
    }
}
