use criterion::*;
use itertools::kmerge;
use rayon::prelude::*;
use rayon::{join, ThreadPoolBuilder};
use task_splitting::*;
const N: usize = 100000;

/*
 * Split single-threaded in N parts and them merg them
 */
fn merge_n_bench(c: &mut Criterion) {
    let v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    let mut group = c.benchmark_group("merge_n");
    for size in [1, 2, 3, 4, 5, 6, 7, 8].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut x = v.clone(); // we shouldn't re-use that thing
                merge_n(black_box(&mut x), &mut buffer, size);
                assert_eq!(checksum, x.iter().sum::<u64>());
            });
        });
    }
    group.finish();
}

/*
 * Split multithreaded (also calculate how many splits are required
 */
fn parallel_merge_n_bench(c: &mut Criterion) {
    let split = 4;
    let pool = ThreadPoolBuilder::new()
        .num_threads(split)
        .build()
        .expect("failed creating pool");

    let v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    let mut group = c.benchmark_group("parallel_merge_n");
    for size in [2, 3, 4, 5, 6, 7, 8].iter() {
        let levels = (split as f64).log(*size as f64).ceil() as u64;
        println!("Need {} recursive splits for {}", levels, size);
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                pool.install(|| {
                    let mut x = v.clone(); // we shouldn't re-use that thing
                    parallel_merge_n(black_box(&mut x), &mut buffer, size, levels);
                    assert_eq!(checksum, x.iter().sum::<u64>());
                });
            });
        });
    }
    group.finish();
}

fn merge_kmerge(c: &mut Criterion) {
    let v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    c.bench_function("kmerge", |b| {
        b.iter(|| {
            let mut x = v.clone(); // we shouldn't re-use that thing
            merge_n(black_box(&mut x), &mut buffer, 2);
            assert_eq!(checksum, x.iter().sum::<u64>());
        })
    });
    c.bench_function("merge", |b| {
        b.iter(|| {
            let mut x = v.clone(); // we shouldn't re-use that thing
            merge_2(black_box(&mut x), &mut buffer);
            assert_eq!(checksum, x.iter().sum::<u64>());
        })
    });
}

//criterion_group!(benches, merge_kmerge);
criterion_group!(benches, parallel_merge_n_bench);
// criterion_group!(benches, parallel_merge_n_bench, merge_n_bench);
criterion_main!(benches);
