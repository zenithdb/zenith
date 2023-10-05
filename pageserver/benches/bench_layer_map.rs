use pageserver::keyspace::{KeyPartitioning, KeySpace};
use pageserver::repository::Key;
use pageserver::tenant::layer_map::LayerMap;
use pageserver::tenant::storage_layer::LayerFileName;
use pageserver::tenant::storage_layer::PersistentLayerDesc;
use rand::prelude::{SeedableRng, SliceRandom, StdRng};
use std::cmp::{max, min};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use utils::id::{TenantId, TimelineId};

use utils::lsn::Lsn;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn build_layer_map(filename_dump: PathBuf) -> LayerMap {
    let mut layer_map = LayerMap::default();

    let mut min_lsn = Lsn(u64::MAX);
    let mut max_lsn = Lsn(0);

    let filenames = BufReader::new(File::open(filename_dump).unwrap()).lines();

    let mut updates = layer_map.batch_update();
    for fname in filenames {
        let fname = fname.unwrap();
        let fname = LayerFileName::from_str(&fname).unwrap();
        let layer = PersistentLayerDesc::from(fname);

        let lsn_range = layer.get_lsn_range();
        min_lsn = min(min_lsn, lsn_range.start);
        max_lsn = max(max_lsn, Lsn(lsn_range.end.0 - 1));

        updates.insert_historic(layer);
    }

    println!("min: {min_lsn}, max: {max_lsn}");

    updates.flush();
    layer_map
}

/// Construct a layer map query pattern for benchmarks
fn uniform_query_pattern(layer_map: &LayerMap) -> Vec<(Key, Lsn)> {
    // For each image layer we query one of the pages contained, at LSN right
    // before the image layer was created. This gives us a somewhat uniform
    // coverage of both the lsn and key space because image layers have
    // approximately equal sizes and cover approximately equal WAL since
    // last image.
    layer_map
        .iter_historic_layers()
        .filter_map(|l| {
            if l.is_incremental() {
                None
            } else {
                let kr = l.get_key_range();
                let lr = l.get_lsn_range();

                let key_inside = kr.start.next();
                let lsn_before = Lsn(lr.start.0 - 1);

                Some((key_inside, lsn_before))
            }
        })
        .collect()
}

// Construct a partitioning for testing get_difficulty map when we
// don't have an exact result of `collect_keyspace` to work with.
fn uniform_key_partitioning(layer_map: &LayerMap, _lsn: Lsn) -> KeyPartitioning {
    let mut parts = Vec::new();

    // We add a partition boundary at the start of each image layer,
    // no matter what lsn range it covers. This is just the easiest
    // thing to do. A better thing to do would be to get a real
    // partitioning from some database. Even better, remove the need
    // for key partitions by deciding where to create image layers
    // directly based on a coverage-based difficulty map.
    let mut keys: Vec<_> = layer_map
        .iter_historic_layers()
        .filter_map(|l| {
            if l.is_incremental() {
                None
            } else {
                let kr = l.get_key_range();
                Some(kr.start.next())
            }
        })
        .collect();
    keys.sort();

    let mut current_key = Key::from_hex("000000000000000000000000000000000000").unwrap();
    for key in keys {
        parts.push(KeySpace {
            ranges: vec![current_key..key],
        });
        current_key = key;
    }

    KeyPartitioning { parts }
}

// Benchmark using metadata extracted from our performance test environment, from
// a project where we have run pgbench many timmes. The pgbench database was initialized
// between each test run.
fn bench_from_captest_env(c: &mut Criterion) {
    // TODO consider compressing this file
    let layer_map = build_layer_map(PathBuf::from("benches/odd-brook-layernames.txt"));
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map);

    // Test with uniform query pattern
    c.bench_function("captest_uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box(layer_map.search(q.0, q.1));
            }
        });
    });

    // test with a key that corresponds to the RelDir entry. See pgdatadir_mapping.rs.
    c.bench_function("captest_rel_dir_query", |b| {
        b.iter(|| {
            let result = black_box(layer_map.search(
                Key::from_hex("000000067F00008000000000000000000001").unwrap(),
                // This LSN is higher than any of the LSNs in the tree
                Lsn::from_str("D0/80208AE1").unwrap(),
            ));
            result.unwrap();
        });
    });
}

// Benchmark using metadata extracted from a real project that was taknig
// too long processing layer map queries.
fn bench_from_real_project(c: &mut Criterion) {
    // Init layer map
    let now = Instant::now();
    let layer_map = build_layer_map(PathBuf::from("benches/odd-brook-layernames.txt"));
    println!("Finished layer map init in {:?}", now.elapsed());

    // Choose uniformly distributed queries
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map);

    // Choose inputs for get_difficulty_map
    let latest_lsn = layer_map
        .iter_historic_layers()
        .map(|l| l.get_lsn_range().end)
        .max()
        .unwrap();
    let partitioning = uniform_key_partitioning(&layer_map, latest_lsn);

    // Check correctness of get_difficulty_map
    // TODO put this in a dedicated test outside of this mod
    {
        println!("running correctness check");

        let now = Instant::now();
        let result_bruteforce = layer_map.get_difficulty_map_bruteforce(latest_lsn, &partitioning);
        assert!(result_bruteforce.len() == partitioning.parts.len());
        println!("Finished bruteforce in {:?}", now.elapsed());

        let now = Instant::now();
        let result_fast = layer_map.get_difficulty_map(latest_lsn, &partitioning, None);
        assert!(result_fast.len() == partitioning.parts.len());
        println!("Finished fast in {:?}", now.elapsed());

        // Assert results are equal. Manually iterate for easier debugging.
        let zip = std::iter::zip(
            &partitioning.parts,
            std::iter::zip(result_bruteforce, result_fast),
        );
        for (_part, (bruteforce, fast)) in zip {
            assert_eq!(bruteforce, fast);
        }

        println!("No issues found");
    }

    // Define and name the benchmark function
    let mut group = c.benchmark_group("real_map");
    group.bench_function("uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box(layer_map.search(q.0, q.1));
            }
        });
    });
    group.bench_function("get_difficulty_map", |b| {
        b.iter(|| {
            layer_map.get_difficulty_map(latest_lsn, &partitioning, Some(3));
        });
    });
    group.finish();
}

// Benchmark using synthetic data. Arrange image layers on stacked diagonal lines.
fn bench_sequential(c: &mut Criterion) {
    // Init layer map. Create 100_000 layers arranged in 1000 diagonal lines.
    //
    // TODO This code is pretty slow and runs even if we're only running other
    //      benchmarks. It needs to be somewhere else, but it's not clear where.
    //      Putting it inside the `bench_function` closure is not a solution
    //      because then it runs multiple times during warm-up.
    let now = Instant::now();
    let mut layer_map = LayerMap::default();
    let mut updates = layer_map.batch_update();
    for i in 0..100_000 {
        let i32 = (i as u32) % 100;
        let zero = Key::from_hex("000000000000000000000000000000000000").unwrap();
        let layer = PersistentLayerDesc::new_img(
            TenantId::generate(),
            TimelineId::generate(),
            zero.add(10 * i32)..zero.add(10 * i32 + 1),
            Lsn(i),
            0,
        );
        updates.insert_historic(layer);
    }
    updates.flush();
    println!("Finished layer map init in {:?}", now.elapsed());

    // Choose 100 uniformly random queries
    let rng = &mut StdRng::seed_from_u64(1);
    let queries: Vec<(Key, Lsn)> = uniform_query_pattern(&layer_map)
        .choose_multiple(rng, 100)
        .copied()
        .collect();

    // Define and name the benchmark function
    let mut group = c.benchmark_group("sequential");
    group.bench_function("uniform_queries", |b| {
        b.iter(|| {
            for q in queries.clone().into_iter() {
                black_box(layer_map.search(q.0, q.1));
            }
        });
    });
    group.finish();
}

criterion_group!(group_1, bench_from_captest_env);
criterion_group!(group_2, bench_from_real_project);
criterion_group!(group_3, bench_sequential);
criterion_main!(group_1, group_2, group_3);
