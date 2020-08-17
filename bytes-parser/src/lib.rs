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

/// global empty byte array as place holder
pub const EMPTY_BYTE_ARRAY: [u8;0] = [];

pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }
}

pub trait WriteToBytes {

    /// write anything that impls ToBytes
    fn write_to_bytes<T: ToBytes>(&mut self, tb: T) -> Result<()>;
}

impl WriteToBytes for Vec<u8> {

    fn write_to_bytes<T: ToBytes>(&mut self, tb: T) -> Result<()> {
        self.extend(tb.to_bytes());
        Ok(())
    }
}

pub trait ReadWithContext<'a, 'c, T: 'a> {
    type Context: 'c;

    /// generic method to read object from input with given context
    fn read_with_ctx(&'a self, offset: usize, ctx: Self::Context) -> Result<(usize, T)>;
}

pub trait ReadAs<'a, T> where Self: 'a, T: 'a {
    
    /// generic method to read object from input
    fn read_as(&'a self, offset: usize) -> Result<(usize, T)>;
}

impl<'a, R: 'a, T: 'a> ReadWithContext<'a, 'static, T> for R where R: ReadAs<'a, T> {
    type Context = ();

    fn read_with_ctx(&'a self, offset: usize, _ctx: Self::Context) -> Result<(usize, T)> {
        self.read_as(offset)
    }
}
