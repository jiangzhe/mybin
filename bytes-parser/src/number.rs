use crate::error::{Result, Error, Needed};

pub trait ReadNumber {

    /// convert 1 byte starting at offset to u8
    fn read_u8(&self, offset: usize) -> Result<(usize, u8)>;

    /// convert 2 bytes starting at offset to u16
    fn read_le_u16(&self, offset: usize) -> Result<(usize, u16)>;

    /// convert 3 bytes starting at offset to u32
    fn read_le_u24(&self, offset: usize) -> Result<(usize, u32)>;

    /// convert 4 bytes starting at offset to u32
    fn read_le_u32(&self, offset: usize) -> Result<(usize, u32)>;

    /// convert 6 bytes starting at offset to u64
    fn read_le_u48(&self, offset: usize) -> Result<(usize, u64)>;

    /// convert 8 bytes starting at offset to u64
    fn read_le_u64(&self, offset: usize) -> Result<(usize, u64)>;

}

impl ReadNumber for [u8] {
    fn read_u8(&self, offset: usize) -> Result<(usize, u8)> {
        if self.len() < offset + 1 {
            return Err(Error::InputIncomplete(Needed::Size(offset + 1 - self.len())));
        }
        Ok((offset+1, self[offset]))
    }

    fn read_le_u16(&self, offset: usize) -> Result<(usize, u16)> {
        if self.len() < offset + 2 {
            return Err(Error::InputIncomplete(Needed::Size(offset + 2 - self.len())));
        }
        let r = self[offset] as u16 + ((self[offset+1] as u16) << 8);
        Ok((offset+2, r))
    }

    fn read_le_u24(&self, offset: usize) -> Result<(usize, u32)> {
        if self.len() < offset + 3 {
            return Err(Error::InputIncomplete(Needed::Size(offset + 3 - self.len())));
        }
        let r = self[offset] as u32 + ((self[offset+1] as u32) << 8) + ((self[offset+2] as u32) << 16);
        Ok((offset+3, r))
    }

    fn read_le_u32(&self, offset: usize) -> Result<(usize, u32)> {
        if self.len() < offset + 4 {
            return Err(Error::InputIncomplete(Needed::Size(offset + 4 - self.len())));
        }
        let r = self[offset] as u32 + ((self[offset+1] as u32) << 8) 
            + ((self[offset+2] as u32) << 16) + ((self[offset+3] as u32) << 24);
        Ok((offset+4, r))
    }

    fn read_le_u48(&self, offset: usize) -> Result<(usize, u64)> {
        if self.len() < offset + 6 {
            return Err(Error::InputIncomplete(Needed::Size(offset + 6 - self.len())));
        }
        let r = self[offset] as u64 + ((self[offset+1] as u64) << 8) 
            + ((self[offset+2] as u64) << 16) + ((self[offset+3] as u64) << 24)
            + ((self[offset+4] as u64) << 32) + ((self[offset+5] as u64) << 40);
        Ok((offset+6, r))
    }

    fn read_le_u64(&self, offset: usize) -> Result<(usize, u64)> {
        if self.len() < offset + 8 {
            return Err(Error::InputIncomplete(Needed::Size(offset + 8 - self.len())));
        }
        let r = self[offset] as u64 + ((self[offset+1] as u64) << 8) 
            + ((self[offset+2] as u64) << 16) + ((self[offset+3] as u64) << 24)
            + ((self[offset+4] as u64) << 32) + ((self[offset+5] as u64) << 40)
            + ((self[offset+6] as u64) << 48) + ((self[offset+7] as u64) << 56);
        Ok((offset+8, r))
    }
}

pub trait WriteNumber {

    /// write single byte
    fn write_u8(&mut self, n: u8) -> Result<()>;

    /// write u16 as 2 bytes in little endian byte order
    fn write_le_u16(&mut self, n: u16) -> Result<()>;

    /// write u24 as 3 bytes in little endian byte order
    fn write_le_u24(&mut self, n: u32) -> Result<()>;

    /// write u32 as 4 bytes in little endian byte order
    fn write_le_u32(&mut self, n: u32) -> Result<()>;

    /// write u48 as 6 bytes in little endian byte order
    fn write_le_u48(&mut self, n: u64) -> Result<()>;

    /// write u64 as 8 bytes in little endian byte order
    fn write_le_u64(&mut self, n: u64) -> Result<()>;

}

impl WriteNumber for Vec<u8> {

    fn write_u8(&mut self, n: u8) -> Result<()> {
        self.push(n);
        Ok(())
    }

    fn write_le_u16(&mut self, n: u16) -> Result<()> {
        self.extend(&n.to_le_bytes());
        Ok(())
    }

    fn write_le_u24(&mut self, n: u32) -> Result<()> {
        debug_assert!(n <= 0xff_ffff);
        self.extend(&n.to_le_bytes()[..3]);
        Ok(())
    }

    fn write_le_u32(&mut self, n: u32) -> Result<()> {
        self.extend(&n.to_le_bytes());
        Ok(())
    }

    fn write_le_u48(&mut self, n: u64) -> Result<()> {
        debug_assert!(n <= 0xffff_ffff_ffffu64);
        self.extend(&n.to_le_bytes()[..6]);
        Ok(())
    }

    fn write_le_u64(&mut self, n: u64) -> Result<()> {
        self.extend(&n.to_le_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use super::*;

    #[test]
    fn test_u8() -> Result<()> {
        // read
        let input = vec![1];
        let (offset, success) = input.read_u8(0)?;
        assert_eq!(1, success);
        let fail = input.read_u8(offset);
        assert!(fail.is_err());
        dbg!(fail);
        // write
        let mut v = vec![];
        v.write_u8(success).unwrap();
        assert_eq!(vec![1], v);
        Ok(())
    }

    #[test]
    fn test_le_u16() -> Result<()> {
        // read
        let input = vec![1,2,3];
        let (offset, success) = input.read_le_u16(0)?;
        assert_eq!(1 + (2u16 << 8), success);
        let fail = input.read_le_u16(offset);
        assert!(fail.is_err());
        dbg!(fail);
        // write
        let mut v = vec![];
        v.write_le_u16(success).unwrap();
        assert_eq!(vec![1,2], v);
        Ok(())
    }

    #[test]
    fn test_le_u24() -> Result<()> {
        // read
        let input = vec![1,2,3,4];
        let (offset, success) = input.read_le_u24(0)?;
        assert_eq!(1u32 + (2u32 << 8) + (3u32 << 16), success);
        let fail = input.read_le_u24(offset);
        assert!(fail.is_err());
        dbg!(fail);
        // write
        let mut v = vec![];
        v.write_le_u24(success).unwrap();
        assert_eq!(vec![1,2,3], v);
        Ok(())
    }

    #[test]
    fn test_le_u32() -> Result<()> {
        // read
        let input = vec![1,2,3,4,5];
        let (offset, success) = input.read_le_u32(0)?;
        assert_eq!(1u32 + (2u32 << 8) + (3u32 << 16) + (4u32 << 24), success);
        let fail = input.read_le_u32(offset);
        assert!(fail.is_err());
        dbg!(fail);
        // write
        let mut v = vec![];
        v.write_le_u32(success).unwrap();
        assert_eq!(vec![1,2,3,4], v);
        Ok(())
    }

    #[test]
    fn test_le_u48() -> Result<()> {
        // read
        let input = vec![1,2,3,4,1,2,3,4];
        let (offset, success) = input.read_le_u48(0)?;
        assert_eq!(1u64 + (2u64 << 8) + (3u64 << 16) + (4u64 << 24) + (1u64 << 32) + (2u64 << 40), success);
        let fail = input.read_le_u48(offset);
        assert!(fail.is_err());
        dbg!(fail);
        // write
        let mut v = vec![];
        v.write_le_u48(success).unwrap();
        assert_eq!(vec![1,2,3,4,1,2], v);
        Ok(())
    }

    #[test]
    fn test_le_u64() -> Result<()> {
        // read
        let input = vec![1,2,3,4,1,2,3,4,1,2,3,4];
        let (offset, success) = input.read_le_u64(0)?;
        assert_eq!(1u64 + (2u64 << 8) + (3u64 << 16) + (4u64 << 24) 
            + (1u64 << 32) + (2u64 << 40) + (3u64 << 48) + (4u64 << 56), success);
        let fail = input.read_le_u64(offset);
        assert!(fail.is_err());
        dbg!(fail);
        // write
        // write
        let mut v = vec![];
        v.write_le_u64(success).unwrap();
        assert_eq!(vec![1,2,3,4,1,2,3,4], v);
        Ok(())
    }
}