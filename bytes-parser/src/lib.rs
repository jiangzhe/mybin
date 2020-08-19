//! essential parsing of bytes
//!
//! inspired by nom parser combinator (https://github.com/Geal/nom)
pub mod take;
pub mod error;
pub mod my;
pub mod number;
pub mod number_async;
pub mod util;

pub use error::*;
use bytes::{Buf, Bytes, BufMut, BytesMut};

/// global empty byte array as place holder
pub const EMPTY_BYTE_ARRAY: [u8; 0] = [];

pub trait ReadFromBytesWithContext<'c> 
where
    Self: Sized,
{
    type Context: 'c;

    fn read_with_ctx(input: &mut Bytes, ctx: Self::Context) -> Result<Self>;
}

pub trait ReadFromBytes
where
    Self: Sized,
{
    fn read_from(input: &mut Bytes) -> Result<Self>;
}

pub trait ReadBytesExt {

    fn read_u8(&mut self) -> Result<u8>;

    fn read_i8(&mut self) -> Result<i8> {
        self.read_u8().map(|n| n as i8)
    }

    fn read_le_u16(&mut self) -> Result<u16>;

    fn read_le_i16(&mut self) -> Result<i16> {
        self.read_le_u16().map(|n| n as i16)
    }

    fn read_le_u24(&mut self) -> Result<u32>;

    fn read_le_i24(&mut self) -> Result<i32> {
        self.read_le_u32().map(|n| {
            if n & 0x80_0000_u32 != 0 {
                (n | 0xff00_0000_u32) as i32
            } else {
                n as i32
            }
        })
    }

    fn read_le_u32(&mut self) -> Result<u32>;

    fn read_le_i32(&mut self) -> Result<i32> {
        self.read_le_u32().map(|n| n as i32)
    }

    fn read_le_u48(&mut self) -> Result<u64>;

    fn read_le_i48(&mut self) -> Result<i64> {
        self.read_le_u48().map(|n| {
            if n & 0x8000_0000_0000_u64 != 0 {
                (n | 0xffff_0000_0000_0000_u64) as i64
            } else {
                n as i64
            }
        })
    }

    fn read_le_u64(&mut self) -> Result<u64>;

    fn read_le_i64(&mut self) -> Result<i64> {
        self.read_le_u64().map(|n| n as i64)
    }

    fn read_le_u128(&mut self) -> Result<u128>;

    fn read_le_i128(&mut self) -> Result<i128> {
        self.read_le_u128().map(|n| n as i128)
    }

    fn read_le_f32(&mut self) -> Result<f32>;

    fn read_le_f64(&mut self) -> Result<f64>;

    fn read_len(&mut self, len: usize) -> Result<Bytes>;

    fn read_until(&mut self, b: u8, inclusive: bool) -> Result<Bytes>;
}

impl ReadBytesExt for Bytes {

    fn read_u8(&mut self) -> Result<u8> {
        if self.remaining() < 1 {
            return Err(Error::InputIncomplete(Needed::Size(1)));
        }
        Ok(self.get_u8())
    }

    fn read_le_u16(&mut self) -> Result<u16> {
        if self.remaining() < 2 {
            return Err(Error::InputIncomplete(Needed::Size(2 - self.remaining())));
        }
        Ok(self.get_u16_le())
    }

    fn read_le_u24(&mut self) -> Result<u32> {
        if self.remaining() < 3 {
            return Err(Error::InputIncomplete(Needed::Size(3 - self.remaining())));
        }
        let bs = self.bytes();
        let r = bs[0] as u32
            + ((bs[1] as u32) << 8)
            + ((bs[2] as u32) << 16);
        self.advance(3);
        Ok(r)
    }

    fn read_le_u32(&mut self) -> Result<u32> {
        if self.remaining() < 4 {
            return Err(Error::InputIncomplete(Needed::Size(4 - self.remaining())));
        }
        Ok(self.get_u32_le())
    }

    fn read_le_u48(&mut self) -> Result<u64> {
        if self.remaining() < 6 {
            return Err(Error::InputIncomplete(Needed::Size(6 - self.remaining())));
        }
        let bs = self.bytes();
        let r = bs[0] as u64
            + ((bs[1] as u64) << 8)
            + ((bs[2] as u64) << 16)
            + ((bs[3] as u64) << 24)
            + ((bs[4] as u64) << 32)
            + ((bs[5] as u64) << 40);
        self.advance(6);
        Ok(r)
    }

    fn read_le_u64(&mut self) -> Result<u64> {
        if self.remaining() < 8 {
            return Err(Error::InputIncomplete(Needed::Size(8 - self.remaining())));
        }
        Ok(self.get_u64_le())
    }

    fn read_le_u128(&mut self) -> Result<u128> {
        if self.remaining() < 16 {
            return Err(Error::InputIncomplete(Needed::Size(16 - self.remaining())));
        }
        Ok(self.get_u128_le())
    }

    fn read_le_f32(&mut self) -> Result<f32> {
        if self.remaining() < 4 {
            return Err(Error::InputIncomplete(Needed::Size(4 - self.remaining())));
        }
        Ok(self.get_f32_le())
    }

    fn read_le_f64(&mut self) -> Result<f64> {
        if self.remaining() < 8 {
            return Err(Error::InputIncomplete(Needed::Size(4 - self.remaining())));
        }
        Ok(self.get_f64_le())
    }

    fn read_len(&mut self, len: usize) -> Result<Bytes> {
        if self.remaining() < len {
            return Err(Error::InputIncomplete(Needed::Size(len - self.remaining())));
        }
        Ok(self.split_to(len))
    }

    fn read_until(&mut self, b: u8, inclusive: bool) -> Result<Bytes> {
        if let Some(pos) = self.bytes().iter().position(|&x| x == b) {
            let end = pos + 1;
            let bs = if inclusive {
                self.split_to(end)
            } else {
                let bs = self.split_to(end-1);
                self.advance(1);
                bs
            };
            return Ok(bs);
        }
        Err(Error::InputIncomplete(Needed::Unknown))
    }
}


pub trait WriteToBytes {
    fn write_to(self, out: &mut BytesMut) -> Result<usize>;
}

pub trait WriteToBytesWithContext<'c> {
    type Context: 'c;

    fn write_with_ctx(self, out: &mut BytesMut, ctx: Self::Context) -> Result<usize>;
}

pub trait WriteBytesExt {

    fn write_u8(&mut self, n: u8) -> Result<usize>;

    fn write_le_u16(&mut self, n: u16) -> Result<usize>;

    fn write_le_u24(&mut self, n: u32) -> Result<usize>;

    fn write_le_u32(&mut self, n: u32) -> Result<usize>;

    fn write_le_u48(&mut self, n: u32) -> Result<usize>;

    fn write_le_u64(&mut self, n: u64) -> Result<usize>;

    fn write_le_u128(&mut self, n: u128) -> Result<usize>;

    fn write_le_f32(&mut self, n: f32) -> Result<usize>;

    fn write_le_f64(&mut self, n: f64) -> Result<usize>;

    fn write_bytes(&mut self, bs: &[u8]) -> Result<usize>;
}

impl WriteBytesExt for BytesMut {

    fn write_u8(&mut self, n: u8) -> Result<usize> {
        self.put_u8(n);
        Ok(1)
    }

    fn write_le_u16(&mut self, n: u16) -> Result<usize> {
        self.put_u16_le(n);
        Ok(2)
    }

    fn write_le_u24(&mut self, n: u32) -> Result<usize> {
        self.put(&n.to_le_bytes()[..3]);
        Ok(3)
    }

    fn write_le_u32(&mut self, n: u32) -> Result<usize> {
        self.put_u32_le(n);
        Ok(4)
    }

    fn write_le_u48(&mut self, n: u32) -> Result<usize> {
        self.put(&n.to_le_bytes()[..6]);
        Ok(6)
    }

    fn write_le_u64(&mut self, n: u64) -> Result<usize> {
        self.put_u64_le(n);
        Ok(8)
    }

    fn write_le_u128(&mut self, n: u128) -> Result<usize> {
        self.put_u128_le(n);
        Ok(16)
    }

    fn write_le_f32(&mut self, n: f32) -> Result<usize> {
        self.put_f32_le(n);
        Ok(4)
    }

    fn write_le_f64(&mut self, n: f64) -> Result<usize> {
        self.put_f64_le(n);
        Ok(8)
    }

    fn write_bytes(&mut self, bs: &[u8]) -> Result<usize> {
        self.put(bs);
        Ok(bs.len())
    }
}