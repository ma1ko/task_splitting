use itertools::kmerge;
use rayon::{join, ThreadPoolBuilder};
use rayon::prelude::*;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use task_splitting::*;
const N: usize = 100_000;

fn threeway(c : &mut Criterion) {
        let pool = ThreadPoolBuilder::new()
        .num_threads(3)
        .build()
        .expect("failed creating pool");

        let mut v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
        c.bench_function("ThreeWay", |b| b.iter(|| {
            pool.install(||
            sort_threeway(&mut v)
            );
        }));
}
fn twoway(c : &mut Criterion) {
        let pool = ThreadPoolBuilder::new()
        .num_threads(3)
        .build()
        .expect("failed creating pool");

        let mut v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
        c.bench_function("TwoWay", |b| b.iter(|| {
                  pool.install(||
                  sort_twoway(&mut v)
                  );
                }));

}
criterion_group!(benches, twoway, threeway);
criterion_main!(benches);




