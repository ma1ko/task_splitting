use criterion::*;
use itertools::kmerge;
use rayon::prelude::*;
use rayon::{join, ThreadPoolBuilder};
use task_splitting::*;

/*
 * Split multithreaded (also calculate how many splits are required
 */
fn mergesort_n_bench(c: &mut Criterion) {
    const N: usize = 19683;
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
                    // assert_eq!(checksum, v.iter().sum::<u64>());
                    // assert!(v.windows(2).all(|w| w[0] <= w[1]));
                    v
                },
                BatchSize::SmallInput,
            );
        });
    }
   
    group.finish();
}

fn merge_kmerge(c: &mut Criterion) {
    let N = 100000;
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
                // assert_eq!(checksum, v.iter().sum::<u64>());
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
                // assert_eq!(checksum, v.iter().sum::<u64>());
                v
            },
            BatchSize::SmallInput,
        )
    });
}
/*
 * Split single-threaded in N parts and them merg them
 */
fn kmerge_n(c: &mut Criterion) {
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
    let n = 30000;
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

//criterion_group!(benches, merge_kmerge);
//criterion_group!(benches, mergesort_n_bench);
criterion_group!(benches, sort_bench);
// criterion_group!(benches, parallel_merge_n_bench, merge_n_bench);
criterion_main!(benches);
