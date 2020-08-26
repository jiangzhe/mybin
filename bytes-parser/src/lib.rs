//! essential parsing of bytes
//!
//! inspired by nom parser combinator (https://github.com/Geal/nom)
pub mod error;
pub mod future;
pub mod my;
pub mod util;

use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use error::*;

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
        self.read_le_u24().map(|n| {
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
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Size(1)));
        }
        Ok(self.get_u8())
    }

    fn read_le_u16(&mut self) -> Result<u16> {
        if self.remaining() < 2 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(2 - self.remaining()),
            ));
        }
        Ok(self.get_u16_le())
    }

    fn read_le_u24(&mut self) -> Result<u32> {
        if self.remaining() < 3 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(3 - self.remaining()),
            ));
        }
        let bs = self.bytes();
        let r = bs[0] as u32 + ((bs[1] as u32) << 8) + ((bs[2] as u32) << 16);
        self.advance(3);
        Ok(r)
    }

    fn read_le_u32(&mut self) -> Result<u32> {
        if self.remaining() < 4 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(4 - self.remaining()),
            ));
        }
        Ok(self.get_u32_le())
    }

    fn read_le_u48(&mut self) -> Result<u64> {
        if self.remaining() < 6 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(6 - self.remaining()),
            ));
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
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(8 - self.remaining()),
            ));
        }
        Ok(self.get_u64_le())
    }

    fn read_le_u128(&mut self) -> Result<u128> {
        if self.remaining() < 16 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(16 - self.remaining()),
            ));
        }
        Ok(self.get_u128_le())
    }

    fn read_le_f32(&mut self) -> Result<f32> {
        if self.remaining() < 4 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(4 - self.remaining()),
            ));
        }
        Ok(self.get_f32_le())
    }

    fn read_le_f64(&mut self) -> Result<f64> {
        if self.remaining() < 8 {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(4 - self.remaining()),
            ));
        }
        Ok(self.get_f64_le())
    }

    fn read_len(&mut self, len: usize) -> Result<Bytes> {
        if self.remaining() < len {
            return Err(Error::InputIncomplete(
                Bytes::new(),
                Needed::Size(len - self.remaining()),
            ));
        }
        Ok(self.split_to(len))
    }

    fn read_until(&mut self, b: u8, inclusive: bool) -> Result<Bytes> {
        if let Some(pos) = self.bytes().iter().position(|&x| x == b) {
            let end = pos + 1;
            let bs = if inclusive {
                self.split_to(end)
            } else {
                let bs = self.split_to(end - 1);
                self.advance(1);
                bs
            };
            return Ok(bs);
        }
        Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown))
    }
}

pub trait WriteToBytes {
    fn write_to(self, out: &mut BytesMut) -> Result<usize>;
}

impl WriteToBytes for &[u8] {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let len = self.len();
        out.put(self);
        Ok(len)
    }
}

impl WriteToBytes for Bytes {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let len = self.remaining();
        out.put(self.bytes());
        Ok(len)
    }
}

pub trait WriteToBytesWithContext<'c> {
    type Context: 'c;

    fn write_with_ctx(self, out: &mut BytesMut, ctx: Self::Context) -> Result<usize>;
}

pub trait WriteBytesExt {
    fn write_u8(&mut self, n: u8) -> Result<usize>;

    fn write_i8(&mut self, n: i8) -> Result<usize>;

    fn write_le_u16(&mut self, n: u16) -> Result<usize>;

    fn write_le_i16(&mut self, n: i16) -> Result<usize>;

    fn write_le_u24(&mut self, n: u32) -> Result<usize>;

    fn write_le_i24(&mut self, n: i32) -> Result<usize>;

    fn write_le_u32(&mut self, n: u32) -> Result<usize>;

    fn write_le_i32(&mut self, n: i32) -> Result<usize>;

    fn write_le_u48(&mut self, n: u64) -> Result<usize>;

    fn write_le_i48(&mut self, n: i64) -> Result<usize>;

    fn write_le_u64(&mut self, n: u64) -> Result<usize>;

    fn write_le_i64(&mut self, n: i64) -> Result<usize>;

    fn write_le_u128(&mut self, n: u128) -> Result<usize>;

    fn write_le_i128(&mut self, n: i128) -> Result<usize>;

    fn write_le_f32(&mut self, n: f32) -> Result<usize>;

    fn write_le_f64(&mut self, n: f64) -> Result<usize>;

    fn write_bytes<T>(&mut self, val: T) -> Result<usize>
    where
        T: WriteToBytes;
}

impl WriteBytesExt for BytesMut {
    fn write_u8(&mut self, n: u8) -> Result<usize> {
        self.put_u8(n);
        Ok(1)
    }

    fn write_i8(&mut self, n: i8) -> Result<usize> {
        self.put_i8(n);
        Ok(1)
    }

    fn write_le_u16(&mut self, n: u16) -> Result<usize> {
        self.put_u16_le(n);
        Ok(2)
    }

    fn write_le_i16(&mut self, n: i16) -> Result<usize> {
        self.put_i16_le(n);
        Ok(2)
    }

    fn write_le_u24(&mut self, n: u32) -> Result<usize> {
        self.put(&n.to_le_bytes()[..3]);
        Ok(3)
    }

    fn write_le_i24(&mut self, n: i32) -> Result<usize> {
        let n = if n < 0 {
            (n as u32) | 0xff80_0000
        } else {
            n as u32
        };
        self.put(&n.to_le_bytes()[..3]);
        Ok(3)
    }

    fn write_le_u32(&mut self, n: u32) -> Result<usize> {
        self.put_u32_le(n);
        Ok(4)
    }

    fn write_le_i32(&mut self, n: i32) -> Result<usize> {
        self.put_i32_le(n);
        Ok(4)
    }

    fn write_le_u48(&mut self, n: u64) -> Result<usize> {
        self.put(&n.to_le_bytes()[..6]);
        Ok(6)
    }

    fn write_le_i48(&mut self, n: i64) -> Result<usize> {
        let n = if n < 0 {
            (n as u64) | 0xffff_8000_0000_0000_u64
        } else {
            n as u64
        };
        self.put(&n.to_le_bytes()[..6]);
        Ok(6)
    }

    fn write_le_u64(&mut self, n: u64) -> Result<usize> {
        self.put_u64_le(n);
        Ok(8)
    }

    fn write_le_i64(&mut self, n: i64) -> Result<usize> {
        self.put_i64_le(n);
        Ok(8)
    }

    fn write_le_u128(&mut self, n: u128) -> Result<usize> {
        self.put_u128_le(n);
        Ok(16)
    }

    fn write_le_i128(&mut self, n: i128) -> Result<usize> {
        self.put_i128_le(n);
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

    fn write_bytes<T>(&mut self, val: T) -> Result<usize>
    where
        T: WriteToBytes,
    {
        val.write_to(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use bytes::Buf;

    #[test]
    fn test_u8() -> Result<()> {
        // read
        let orig = vec![1u8];
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_u8()?;
        assert_eq!(1, success);
        let fail = input.read_u8();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_u8(success).unwrap();
        assert_eq!(vec![1], v);
        Ok(())
    }


    #[test]
    fn test_i8() -> Result<()> {
        // read
        let orig = vec![-20i8 as u8];
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_i8()?;
        assert_eq!(-20, success);
        let fail = input.read_u8();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_i8(success).unwrap();
        assert_eq!(vec![-20i8 as u8], v);
        Ok(())
    }

    #[test]
    fn test_le_u16() -> Result<()> {
        // read
        let orig = vec![1u8, 2, 3];
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_u16()?;
        assert_eq!(1 + (2u16 << 8), success);
        let fail = input.read_le_u16();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_u16(success).unwrap();
        assert_eq!(vec![1, 2], v);
        Ok(())
    }

    #[test]
    fn test_le_i16() -> Result<()> {
        // read
        let orig = Vec::from((-200i16 as u16).to_le_bytes());
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_i16()?;
        assert_eq!(-200, success);
        let fail = input.read_le_i16();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_i16(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }

    #[test]
    fn test_le_u24() -> Result<()> {
        // read
        let orig = vec![1, 2, 3, 4];
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_u24()?;
        assert_eq!(1u32 + (2u32 << 8) + (3u32 << 16), success);
        let fail = input.read_le_u24();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_u24(success).unwrap();
        assert_eq!(vec![1, 2, 3], v);
        Ok(())
    }

    #[test]
    fn test_le_i24() -> Result<()> {
        // read
        let orig = Vec::from(&(-200000i32 as u32 | 0xff80_0000).to_le_bytes()[..3]);
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_i24()?;
        assert_eq!(-200000, success);
        let fail = input.read_le_i24();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_i24(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }

    #[test]
    fn test_le_u32() -> Result<()> {
        // read
        let orig = vec![1u8, 2, 3, 4, 5];
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_u32()?;
        assert_eq!(1u32 + (2u32 << 8) + (3u32 << 16) + (4u32 << 24), success);
        let fail = input.read_le_u32();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_u32(success).unwrap();
        assert_eq!(vec![1, 2, 3, 4], v);
        Ok(())
    }

    #[test]
    fn test_le_i32() -> Result<()> {
        // read
        let orig = Vec::from((-20000000i32 as u32).to_le_bytes());
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_i32()?;
        assert_eq!(-20000000, success);
        let fail = input.read_le_i32();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_i32(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }

    #[test]
    fn test_le_u48() -> Result<()> {
        // read
        let input = vec![1u8, 2, 3, 4, 1, 2, 3, 4];
        let mut input = &input[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_u48()?;
        assert_eq!(
            1u64 + (2u64 << 8) + (3u64 << 16) + (4u64 << 24) + (1u64 << 32) + (2u64 << 40),
            success
        );
        let fail = input.read_le_u48();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_u48(success).unwrap();
        assert_eq!(vec![1, 2, 3, 4, 1, 2], v);
        Ok(())
    }

    #[test]
    fn test_le_i48() -> Result<()> {
        // read
        let orig = Vec::from(&(-2000000000i64 as u64 | 0xffff_8000_0000_0000).to_le_bytes()[..6]);
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_i48()?;
        assert_eq!(-2000000000i64, success);
        let fail = input.read_le_i48();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_i48(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }

    #[test]
    fn test_le_u64() -> Result<()> {
        // read
        let orig = vec![1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4];
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_u64()?;
        assert_eq!(
            1u64 + (2u64 << 8)
                + (3u64 << 16)
                + (4u64 << 24)
                + (1u64 << 32)
                + (2u64 << 40)
                + (3u64 << 48)
                + (4u64 << 56),
            success
        );
        let fail = input.read_le_u64();
        dbg!(fail.unwrap_err());
        // write
        // write
        let mut v = BytesMut::new();
        v.write_le_u64(success).unwrap();
        assert_eq!(vec![1, 2, 3, 4, 1, 2, 3, 4], v);
        Ok(())
    }

    #[test]
    fn test_le_i64() -> Result<()> {
        // read
        let orig = Vec::from((-200000000000i64 as u64).to_le_bytes());
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_i64()?;
        assert_eq!(-200000000000i64, success);
        let fail = input.read_le_i64();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_i64(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }

    #[test]
    fn test_le_u128() -> Result<()> {
        // read
        let orig = Vec::from(200000000000200000000000_u128.to_le_bytes());
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_u128()?;
        assert_eq!(200000000000200000000000_u128, success);
        let fail = input.read_le_u128();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_u128(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }

    #[test]
    fn test_le_i128() -> Result<()> {
        // read
        let orig = Vec::from((-200000000000200000000000_i128 as u128).to_le_bytes());
        let mut input = &orig[..];
        let input = &mut input.to_bytes();
        let success = input.read_le_i128()?;
        assert_eq!(-200000000000200000000000_i128, success);
        let fail = input.read_le_i128();
        dbg!(fail.unwrap_err());
        // write
        let mut v = BytesMut::new();
        v.write_le_i128(success).unwrap();
        assert_eq!(orig, v);
        Ok(())
    }
}
