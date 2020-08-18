//! essential parsing of bytes
//! 
//! inspired by nom parser combinator (https://github.com/Geal/nom)
pub mod error;
pub mod number;
pub mod number_async;
pub mod my;
pub mod util;
pub mod bytes;

pub use error::*;
use std::ops::Deref;

/// global empty byte array as place holder
pub const EMPTY_BYTE_ARRAY: [u8;0] = [];

// pub trait ToBytes {
//     fn to_bytes(&self) -> Vec<u8>;
// }

// impl ToBytes for Vec<u8> {
//     fn to_bytes(&self) -> Vec<u8> {
//         self.clone()
//     }
// }

// pub trait WriteToBytes {

//     /// write anything that impls ToBytes
//     fn write_to_bytes<T: ToBytes>(&mut self, tb: T) -> Result<()>;
// }

// impl WriteToBytes for Vec<u8> {

//     fn write_to_bytes<T: ToBytes>(&mut self, tb: T) -> Result<()> {
//         self.extend(tb.to_bytes());
//         Ok(())
//     }
// }

pub trait ReadWithContext<'a, 'c, T: 'a> {
    type Context: 'c;

    /// generic method to read object from input with given context
    fn read_with_ctx(&'a self, offset: usize, ctx: Self::Context) -> Result<(usize, T)>;
}

pub trait ReadFrom<'a, T> where Self: 'a, T: 'a {
    
    /// generic method to read object from input
    fn read_from(&'a self, offset: usize) -> Result<(usize, T)>;
}

impl<'a, R: 'a, T: 'a> ReadWithContext<'a, 'static, T> for R where R: ReadFrom<'a, T> {
    type Context = ();

    fn read_with_ctx(&'a self, offset: usize, _ctx: Self::Context) -> Result<(usize, T)> {
        self.read_from(offset)
    }
}

pub trait WriteWithContext<'a, 'c, T: 'a> {
    type Context: 'c;

    /// generic method to write object to output
    fn write_with_ctx(&mut self, val: T, ctx: Self::Context) -> Result<usize>;
}

pub trait WriteTo<'a, T: 'a> {
    fn write_to(&mut self, val: T) -> Result<usize>;
}

impl<'a, W, T: 'a> WriteWithContext<'a, 'static, T> for W where W: WriteTo<'a, T> {
    type Context = ();

    fn write_with_ctx(&mut self, val: T, _ctx: Self::Context) -> Result<usize> {
        self.write_to(val)
    }
}

impl<'a, T> WriteTo<'a, &'a T> for Vec<u8> where T: Deref<Target=[u8]> + 'a {
    fn write_to(&mut self, val: &'a T) -> Result<usize> {
        self.extend(val.deref());
        Ok(val.len())
    }
}
