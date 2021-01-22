use std::{io::{Result, Read, Write}, ops::Deref};

pub trait NetBufRead {

}


pub trait NetBufWrite {

}


#[derive(Debug)]
pub struct Block {
    b: Box<[u8]>,
    read_idx: usize,
    write_idx: usize,
}

impl Block {
    pub fn new(block_size: usize) -> Self {
        Self{
            b: vec![0; block_size].into_boxed_slice(),
            read_idx: 0,
            write_idx: 0,
        }
    }

    pub fn clear(&mut self) {
        self.read_idx = 0;
        self.write_idx = 0;
    }
}

impl Deref for Block {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.b
    }
}

#[derive(Debug)]
pub struct VecBuf {
    bs: Vec<Block>,
    read_idx: usize,
    write_idx: usize,
}

impl VecBuf {

    // create a new VecBuf
    pub fn new() -> Self {
        Self::with_initial_size(32 * 1024)
    }

    // create a new VecBuf with given size
    pub fn with_initial_size(block_size: usize) -> Self {
        Self {
            bs: vec![Block::new(block_size)],
            read_idx: 0,
            write_idx: 0,
        }
    }

    // reset truncates vec to size 1
    pub fn reset(&mut self) {
        self.bs.truncate(1);
        self.read_idx = 0;
        self.write_idx = 0;
    }

    // grow will append a new block of double size 
    // of last block at end of vec
    pub fn grow(&mut self) {
        self.bs.push(Block::new(self.last_block_size()));
    }

    fn last_block_size(&self) -> usize {
        self.bs.last().unwrap().len()
    }
}

// conn will implements AsyncRead / AsyncWrite based on VecBuf's Read / Write

impl Write for VecBuf {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

impl Read for VecBuf {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        todo!()
    }
}