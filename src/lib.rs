// extern crate rayon_logs as rayon;
use itertools::Itertools;
use itertools::{kmerge, merge};
use rayon::prelude::*;
use rayon::{join, join_context, ThreadPool, ThreadPoolBuilder};

pub fn merge_n(input: &mut [u64], buffer: &mut [u64], n: usize) {
    let chunksize = input.len() / n;
    let inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();
    buffer
        .iter_mut()
        .zip(kmerge(inputs))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i);
}

pub fn merge_2(input: &mut [u64], buffer: &mut [u64]) {
    let chunksize = input.len() / 2;

    let inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();

    buffer
        .iter_mut()
        .zip(inputs[0].iter().merge(inputs[1].iter()))
        .for_each(|(o, i)| *o = *i);

    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i);
}

pub fn mergesort_n(input: &mut [u64], buffer: &mut [u64], split: usize) {
    if input.len() == 0 || input.len() == 1 {
        // those are sorted by default
        return;
    }

    let mut chunksize = input.len() / split;

    if chunksize == 0 || input.len() % split != 0 {
        // if we have less elements than tasks (chunsize == 0)
        // just use a few less tasks
        // if we can't evently divide input on tasks, we give the first tasks a bit more
        chunksize = chunksize + 1;
    }

    let mut inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();
    let buffers: Vec<&mut [u64]> = buffer.chunks_mut(chunksize).collect();
    inputs.iter_mut().zip(buffers).for_each(|(i, b)| {
        mergesort_n(i, b, split);
    });
    buffer
        .iter_mut()
        .zip(kmerge(inputs))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i); // write back
}

pub fn mergesort_2(input: &mut [u64], buffer: &mut [u64]) {
    if input.len() == 0 || input.len() == 1 {
        // those are sorted by default
        return;
    }
    let (input1, input2) = input.split_at_mut(input.len() / 2);
    let (buffer1, buffer2) = buffer.split_at_mut(buffer.len() / 2);
    mergesort_2(input1, buffer1);
    mergesort_2(input2, buffer2);
    buffer
        .iter_mut()
        .zip(input1.iter().merge(input2.iter()))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i); // write back
}
pub fn mergesort_n_stop(input: &mut [u64], buffer: &mut [u64], split: usize, level: u64) {
    if level == 0 {
        input.sort();
        return;
    }
    let mut chunksize = input.len() / split;
    if chunksize == 0 || input.len() % split != 0 {
        // if we have less elements than tasks (chunsize == 0)
        // just use a few less tasks
        // if we can't evently divide input on tasks, we give the first tasks a bit more
        chunksize = chunksize + 1;
    }
    let mut inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();
    let buffers: Vec<&mut [u64]> = buffer.chunks_mut(chunksize).collect();
    // inputs.iter().zip(buffers).collect().par_iter();

    inputs
        .iter_mut()
        .zip(buffers)
        .collect::<Vec<(&mut &mut [u64], &mut [u64])>>()
        .par_iter_mut()
        .for_each(|(i, b)| {
            //inputs.par_iter_mut().zip(buffers).for_each(|(i, b)| {
            mergesort_n(i, b, split);
        });

    buffer
        .iter_mut()
        .zip(kmerge(inputs))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i); // write back
}

pub fn mergesort_2_stop(input: &mut [u64], buffer: &mut [u64], level: u64) {
    if level == 0 {
        input.sort();
        return;
    }
    let (input1, input2) = input.split_at_mut(input.len() / 2);
    let (buffer1, buffer2) = buffer.split_at_mut(buffer.len() / 2);
    mergesort_2_stop(buffer1, input1, level - 1);
    mergesort_2_stop(buffer2, input2, level - 1);
    input
        .iter_mut()
        .zip(buffer1.iter().merge(buffer2.iter()))
        .for_each(|(o, i)| *o = *i);
}
pub fn parallel_mergesort_2(input: &mut [u64], buffer: &mut [u64], level: u64) {
    if level == 0 {
        input.sort();
        return;
    }
    let (input1, input2) = input.split_at_mut(input.len() / 2);
    let (buffer1, buffer2) = buffer.split_at_mut(buffer.len() / 2);
    join_context(
        |_| parallel_mergesort_2(input1, buffer1, level - 1),
        |c| {
            let level = if c.migrated() { 2 } else { level - 1 };
            parallel_mergesort_2(input2, buffer2, level);
        },
    );
    buffer
        .iter_mut()
        .zip(input1.iter().merge(input2.iter()))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i); // write back
}
pub fn parallel_mergesort_n(input: &mut [u64], buffer: &mut [u64], split: usize, level: u64) {
    if level == 0 {
        input.sort();
        return;
    }

    let mut chunksize = input.len() / split;

    if chunksize == 0 || input.len() % split != 0 {
        // if we have less elements than tasks (chunsize == 0)
        // just use a few less tasks
        // if we can't evently divide input on tasks, we give the first tasks a bit more
        chunksize = chunksize + 1;
    }

    let mut inputs: Vec<&mut [u64]> = input.chunks_mut(chunksize).collect();
    let buffers: Vec<&mut [u64]> = buffer.chunks_mut(chunksize).collect();
    /*
     inputs.par_iter_mut().zip(buffers).for_each(|(i, b)| {
         parallel_mergesort_n(i, b, split, level - 1);
     });
    */
    // join can only do 2 tasks, we need n. par_iter can't check if a process is migrated, so we do
    // that manually with current_thread_index()
    let idx = rayon::current_thread_index().unwrap();
    inputs.par_iter_mut().zip(buffers).for_each(|(i, b)| {
        let level = if rayon::current_thread_index().unwrap() == idx {
            level - 1
        } else {
            2
        };
        parallel_mergesort_n(i, b, split, level);
    });

    buffer
        .iter_mut()
        .zip(kmerge(inputs))
        .for_each(|(o, i)| *o = *i);
    input.iter_mut().zip(buffer).for_each(|(o, i)| *o = *i); // write back
}
