use itertools::Itertools;
use itertools::{kmerge, merge};
use rayon::prelude::*;
use rayon::{join, ThreadPool, ThreadPoolBuilder};

pub fn sort_twoway(input: &mut Vec<u64>) {
    let checksum: u64 = input.iter().sum();
    //let (_, log) = pool.logging_install(|| {
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(input.len())
        .collect();
    let mid = input.len() / 2;

    let (input1, input3) = input.split_at_mut(mid);
    let (buffer1, buffer2) = buffer.split_at_mut(mid);

    join(
        || {
            let (input1, input2) = input1.split_at_mut(mid / 2);
            join(|| input1.sort(), || input2.sort());
            buffer1
                .iter_mut()
                .zip(kmerge(vec![input1.iter(), input2.iter()]))
                .for_each(|(o, i)| *o = *i);
        },
        || {
            let (input3, input4) = input3.split_at_mut(mid / 2);
            join(|| input3.sort(), || input4.sort());
            buffer2
                .iter_mut()
                .zip(kmerge(vec![input3.iter(), input4.iter()]))
                .for_each(|(o, i)| *o = *i);
        },
    );
    input
        .iter_mut()
        .zip(kmerge(vec![buffer1.iter(), buffer2.iter()]))
        .for_each(|(o, i)| *o = *i);

    //log.save_svg("merge_sort.svg").expect("failed saving svg");
    assert_eq!(checksum, input.iter().sum::<u64>());
    assert!(input.windows(2).all(|w| w[0] <= w[1]));
}

/// pre-condition: we need an even number of levels
/// and not more than log(n) levels
pub fn sort_threeway(input: &mut [u64]) {
    let checksum: u64 = input.iter().sum();
    //let (_, log) = pool.logging_install(|| {
    let mut buffer: Vec<u64> = std::iter::repeat_with(Default::default)
        .take(input.len())
        .collect();
    let third = input.len() / 3;

    let (input1, input2) = input.split_at_mut(third);
    let (input2, input3) = input2.split_at_mut(third);

    join(
        || join(|| input1.sort(), || input2.sort()),
        || input3.sort(),
    );
    buffer
        .iter_mut()
        .zip(kmerge(vec![input1.iter(), input2.iter(), input3.iter()]))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = i);

    assert_eq!(checksum, input.iter().sum::<u64>());
    assert!(input.windows(2).all(|w| w[0] <= w[1]));
}

pub fn parallel_merge_n(input: &mut [u64], buffer: &mut [u64], n: usize, level: u64) {
    if level == 0 {
        input.sort();
        return;
    }

    let mut chunksize = input.len() / n;
    if chunksize == 0 {
        println!("Damn! {} {} {}", input.len(), n, level);
    }

    // assert!(input.len() % chunksize == 0); Need a solution for that
    if input.len() % chunksize != 0 {
        chunksize += 1;
    };
    let mut inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();
    let buffers: Vec<&mut [u64]> = buffer.chunks_mut(chunksize).collect();

    inputs.par_iter_mut().zip(buffers).for_each(|(i, b)| {
        parallel_merge_n(i, b, n, level - 1);
    });
    if n != 2 {
        buffer
            .iter_mut()
            .zip(kmerge(inputs))
            .for_each(|(o, i)| *o = *i);
    } else {
        // user "normal" merge for two parts
        buffer
            .iter_mut()
            .zip(inputs[0].iter().merge(inputs[1].iter()))
            .for_each(|(o, i)| *o = *i);
    }
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i); // write back
}
pub fn merge_n(input: &mut [u64], buffer: &mut [u64], n: usize) {
    //if n != 2 {
    let chunksize = input.len() / n;
    let inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();
    buffer
        .iter_mut()
        .zip(kmerge(inputs))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i);
    /*
    } else {
        merge_2(input, buffer);
    }
    */
}

pub fn merge_2(input: &mut [u64], buffer: &mut [u64]) {
    let chunksize = input.len() / 2;

    // assert!(input.len() % chunksize == 0);
    let inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();

    buffer
        .iter_mut()
        .zip(inputs[0].iter().merge(inputs[1].iter()))
        .for_each(|(o, i)| *o = *i);

    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i);
}
