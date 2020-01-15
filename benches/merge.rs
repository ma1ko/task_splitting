// extern crate rayon_logs as rayon;
use criterion::*;
use itertools::kmerge;
use rayon::prelude::*;
use rayon::{join, ThreadPoolBuilder};
use task_splitting::*;

/*
 * Split multithreaded (also calculate how many splits are required
 */
fn mergesort_n_bench(c: &mut Criterion) {
    const N: usize = 20000;
    /*
    let split = 4;
    let pool = ThreadPoolBuilder::new()
        .num_threads(split)
        .build()
        .expect("failed creating pool");
        */

    let v: Vec<u64> = std::iter::repeat_with(rand::random)
        .take(N)
        .map(|x: u64| x % 10)
        .collect();
    let buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    let mut group = c.benchmark_group("mergesort_n");
    for size in [1, 2, 3, 4, 5, 6, 7, 8].iter() {
        // let levels = (split as f64).log(*size as f64).ceil() as u64;
        // println!("Need {} recursive splits for {}", levels, size);
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || (v.clone(), buffer.clone()),
                |(mut v, mut buffer)| {
                    if size == 1 {
                        mergesort_2(black_box(&mut v), &mut buffer);
                    } else {
                        mergesort_n(black_box(&mut v), &mut buffer, size);
                    }
                    assert_eq!(checksum, v.iter().sum::<u64>());
                    assert!(v.windows(2).all(|w| w[0] <= w[1]));
                    v
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn merge_kmerge_bench(c: &mut Criterion) {
    let N = 500000;
    let v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    c.bench_function("kmerge", |b| {
        b.iter_batched(
            || v.clone(),
            |mut v| {
                merge_n(black_box(&mut v), &mut buffer, 2);
                assert_eq!(checksum, v.iter().sum::<u64>());
                v
            },
            BatchSize::SmallInput,
        )
    });
    c.bench_function("merge", |b| {
        b.iter_batched(
            || v.clone(),
            |mut v| {
                merge_2(black_box(&mut v), &mut buffer);
                assert_eq!(checksum, v.iter().sum::<u64>());
                v
            },
            BatchSize::SmallInput,
        )
    });
}
/*
 * Split single-threaded in N parts and them merg them
 */
fn kmerge_n_bench(c: &mut Criterion) {
    let n = 100000;
    let v: Vec<u64> = std::iter::repeat_with(rand::random).take(n).collect();
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    let mut group = c.benchmark_group("merge_n");
    for size in [1, 2, 3, 4, 5, 6, 7, 8].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || v.clone(),
                |mut v| {
                    merge_n(black_box(&mut v), &mut buffer, size);
                    //assert_eq!(checksum, v.iter().sum::<u64>());
                    v
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn sort_bench(c: &mut Criterion) {
    let n = 50000;
    let v: Vec<u64> = std::iter::repeat_with(rand::random).take(n).collect();
    let checksum: u64 = v.iter().sum();

    let mut group = c.benchmark_group("mergesort_efficient");

    group.bench_function("2", |b| {
        b.iter_batched(
            || v.clone(),
            |mut v| {
                let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
                    .take(v.len())
                    .collect();
                mergesort_2_stop(black_box(&mut v), &mut buffer, 2);
                assert_eq!(checksum, v.iter().sum::<u64>());
                assert!(v.windows(2).all(|w| w[0] <= w[1]));
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("std", |b| {
        b.iter_batched(
            || v.clone(),
            |mut v| {
                v.sort();
                assert_eq!(checksum, v.iter().sum::<u64>());
                assert!(v.windows(2).all(|w| w[0] <= w[1]));
            },
            BatchSize::SmallInput,
        )
    });
}
fn mergesort_par_n_bench(c: &mut Criterion) {
    const N: usize = 500000;

    let v: Vec<u64> = std::iter::repeat_with(rand::random)
        .take(N)
        .map(|x: u64| x % 10)
        .collect();
    let buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    let mut group = c.benchmark_group("mergesort_par_n");
    group.sample_size(10);
    for size in [0, 1, 2, 3, 4, 5, 6, 7, 8].iter() {
        let pool = ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .expect("failed creating pool");

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || (v.clone(), buffer.clone()),
                |(mut v, mut buffer)| {
                    pool.install(|| {
                        if size == 0 {
                            parallel_mergesort_rayon(black_box(&mut v), &mut buffer);
                        } else if size == 1 {
                            parallel_mergesort_2(black_box(&mut v), &mut buffer, 2);
                        } else {
                            parallel_mergesort_n(black_box(&mut v), &mut buffer, size, 2);
                        }
                    });
                    assert_eq!(checksum, v.iter().sum::<u64>());
                    assert!(v.windows(2).all(|w| w[0] <= w[1]));
                    v
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}
fn logs(c: &mut Criterion) {
    const N: usize = 50000000;

    let v: Vec<u64> = std::iter::repeat_with(rand::random)
        .take(N)
        .map(|x: u64| x % 10)
        .collect();
    let buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(v.len())
        .collect();
    let checksum: u64 = v.iter().sum();
    for size in [0, 1, 2, 3, 4, 5, 6, 7, 8].iter() {
        let pool = rayon_logs::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .expect("failed creating pool");
        let mut v = v.clone();
        let mut buffer = buffer.clone();
        let (_, log) = pool.logging_install(|| {
            if *size == 0 {
                parallel_mergesort_rayon(black_box(&mut v), &mut buffer);
            } else if *size == 1 {
                parallel_mergesort_2(black_box(&mut v), &mut buffer, 2);
            } else {
                parallel_mergesort_n(black_box(&mut v), &mut buffer, *size, 2);
            }
        });
        assert_eq!(checksum, v.iter().sum::<u64>());
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
        log.save_svg(format!("merge_sort_{}.svg", size))
            .expect("failed saving svg");
    }
}

/*
criterion_group!(
    benches,
    merge_kmerge_bench,
    kmerge_n_bench,
    mergesort_n_bench,
    mergesort_par_n_bench
);
*/
//criterion_group!(benches, mergesort_n_bench);
// criterion_group!(benches, merge_kmerge_bench);
// criterion_group!(benches, mergesort_par_n_bench);
// criterion_group!(benches, logs);
criterion_group!(benches, mergesort_par_n_bench); //, merge_n_bench);
criterion_main!(benches);
