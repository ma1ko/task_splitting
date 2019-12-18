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
pub fn mergesort_2_stop(input: &mut [u64], buffer: &mut [u64], level: u64) {
    if level == 0{
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



