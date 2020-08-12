use crate::error::{Result, Error, Needed};

pub trait BytesNumberParser {

    /// convert 1 byte starting at offset to u8
    fn le_u8(&self, offset: usize) -> Result<(usize, u8)>;

    /// convert 2 bytes starting at offset to u16
    fn le_u16(&self, offset: usize) -> Result<(usize, u16)>;

    /// convert 3 bytes starting at offset to u32
    fn le_u24(&self, offset: usize) -> Result<(usize, u32)>;

    /// convert 4 bytes starting at offset to u32
    fn le_u32(&self, offset: usize) -> Result<(usize, u32)>;

    /// convert 6 bytes starting at offset to u64
    fn le_u48(&self, offset: usize) -> Result<(usize, u64)>;

    /// convert 8 bytes starting at offset to u64
    fn le_u64(&self, offset: usize) -> Result<(usize, u64)>;

}

impl BytesNumberParser for [u8] {
    fn le_u8(&self, offset: usize) -> Result<(usize, u8)> {
        if self.len() < offset + 1 {
            return Err(Error::Incomplete(Needed::Size(offset + 1 - self.len())));
        }
        Ok((offset+1, self[offset]))
    }

    fn le_u16(&self, offset: usize) -> Result<(usize, u16)> {
        if self.len() < offset + 2 {
            return Err(Error::Incomplete(Needed::Size(offset + 2 - self.len())));
        }
        let r = self[offset] as u16 + ((self[offset+1] as u16) << 8);
        Ok((offset+2, r))
    }

    fn le_u24(&self, offset: usize) -> Result<(usize, u32)> {
        if self.len() < offset + 3 {
            return Err(Error::Incomplete(Needed::Size(offset + 3 - self.len())));
        }
        let r = self[offset] as u32 + ((self[offset+1] as u32) << 8) + ((self[offset+2] as u32) << 16);
        Ok((offset+3, r))
    }

    fn le_u32(&self, offset: usize) -> Result<(usize, u32)> {
        if self.len() < offset + 4 {
            return Err(Error::Incomplete(Needed::Size(offset + 4 - self.len())));
        }
        let r = self[offset] as u32 + ((self[offset+1] as u32) << 8) 
            + ((self[offset+2] as u32) << 16) + ((self[offset+3] as u32) << 24);
        Ok((offset+4, r))
    }

    fn le_u48(&self, offset: usize) -> Result<(usize, u64)> {
        if self.len() < offset + 6 {
            return Err(Error::Incomplete(Needed::Size(offset + 6 - self.len())));
        }
        let r = self[offset] as u64 + ((self[offset+1] as u64) << 8) 
            + ((self[offset+2] as u64) << 16) + ((self[offset+3] as u64) << 24)
            + ((self[offset+4] as u64) << 32) + ((self[offset+5] as u64) << 40);
        Ok((offset+6, r))
    }

    fn le_u64(&self, offset: usize) -> Result<(usize, u64)> {
        if self.len() < offset + 8 {
            return Err(Error::Incomplete(Needed::Size(offset + 8 - self.len())));
        }
        let r = self[offset] as u64 + ((self[offset+1] as u64) << 8) 
            + ((self[offset+2] as u64) << 16) + ((self[offset+3] as u64) << 24)
            + ((self[offset+4] as u64) << 32) + ((self[offset+5] as u64) << 40)
            + ((self[offset+6] as u64) << 48) + ((self[offset+7] as u64) << 56);
        Ok((offset+8, r))
    }
}


#[cfg(test)]
mod tests {
    use crate::error::Result;
    use super::*;

    #[test]
    fn test_le_u8() -> Result<()> {
        let input = vec![1,2,3];
        let (offset, rst) = input.le_u8(0)?;
        assert_eq!(1, rst);
        let (offset, rst) = input.le_u8(offset)?;
        assert_eq!(2, rst);
        let (offset, rst) = input.le_u8(offset)?;
        assert_eq!(3, rst);
        let rst = input.le_u8(offset);
        assert!(rst.is_err());
        dbg!(rst);
        Ok(())
    }

    #[test]
    fn test_le_u16() -> Result<()> {
        let input = vec![1,2,3];
        let (offset, rst) = input.le_u16(0)?;
        assert_eq!(1 + (2u16 << 8), rst);
        let rst = input.le_u16(offset);
        assert!(rst.is_err());
        dbg!(rst);
        Ok(())
    }

    #[test]
    fn test_le_u24() -> Result<()> {
        let input = vec![1,2,3,4];
        let (offset, rst) = input.le_u24(0)?;
        assert_eq!(1u32 + (2u32 << 8) + (3u32 << 16), rst);
        let rst = input.le_u24(offset);
        assert!(rst.is_err());
        dbg!(rst);
        Ok(())
    }

    #[test]
    fn test_le_u32() -> Result<()> {
        let input = vec![1,2,3,4,5];
        let (offset, rst) = input.le_u32(0)?;
        assert_eq!(1u32 + (2u32 << 8) + (3u32 << 16) + (4u32 << 24), rst);
        let rst = input.le_u32(offset);
        assert!(rst.is_err());
        dbg!(rst);
        Ok(())
    }
}