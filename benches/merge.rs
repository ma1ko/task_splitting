use itertools::kmerge;
use rayon::{join, ThreadPoolBuilder};
use rayon::prelude::*;
use criterion::*;
use task_splitting::*;
const N: usize = 1 << 16;


fn merge_n_bench(c : &mut Criterion) {
        println!("{}", N);

        let v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
        let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default).take(v.len()).collect();
        let checksum: u64 = v.iter().sum();
        let mut group = c.benchmark_group("merge_n");
        for size in [2, 3, 4, 5, 6 , 7, 8].iter() {
          group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut x = v.clone();  // we shouldn't re-use that thing
                merge_n(black_box(&mut x), &mut buffer, size);
            assert_eq!(checksum, x.iter().sum::<u64>());
            });
          });
        }
      group.finish();
}



fn _parallel_merge_n_bench(c : &mut Criterion) {
        println!("{}", N);
        let pool = ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("failed creating pool");


        let v: Vec<u64> = std::iter::repeat_with(rand::random).take(N).collect();
        let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default).take(v.len()).collect();
        let checksum: u64 = v.iter().sum();
        c.bench_function("parallel_merge_n", |b| b.iter(|| {
            let mut x = v.clone(); 
                  pool.install(||
                      parallel_merge_n(black_box(&mut x), &mut buffer, 2, 2)
                  );
            assert_eq!(checksum, x.iter().sum::<u64>());
        }));
}
criterion_group!(benches, merge_n_bench);
criterion_main!(benches);




