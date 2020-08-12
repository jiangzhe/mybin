use crate::error::{Error, Needed, Result};
use crate::number::ReadNumber;
use crate::WriteTo;

/// read MySQL encoded types
pub trait ReadMyEncoding {
    // read a MySQL length encoded integer
    fn read_len_enc_int(&self, offset: usize) -> Result<(usize, LenEncInt)>;

    fn read_len_enc_str<'a>(&'a self, offset: usize) -> Result<(usize, LenEncStr<'a>)>;
}

impl ReadMyEncoding for [u8] {
    fn read_len_enc_int(&self, offset: usize) -> Result<(usize, LenEncInt)> {
        if self.len() < offset + 1 {
            return Err(Error::InputIncomplete(Needed::Size(
                offset + 1 - self.len(),
            )));
        }
        let (offset, len) = self.read_u8(offset)?;
        match len {
            0xfb => Ok((offset, LenEncInt::Null)),
            0xfc => {
                let (offset, n) = self.read_le_u16(offset)?;
                Ok((offset, LenEncInt::Len3(n)))
            }
            0xfd => {
                let (offset, n) = self.read_le_u24(offset)?;
                Ok((offset, LenEncInt::Len4(n)))
            }
            0xfe => {
                let (offset, n) = self.read_le_u64(offset)?;
                Ok((offset, LenEncInt::Len9(n)))
            }
            0xff => Ok((offset, LenEncInt::Err)),
            _ => Ok((offset, LenEncInt::Len1(len))),
        }
    }

    fn read_len_enc_str(&self, offset: usize) -> Result<(usize, LenEncStr)> {
        let (offset, lei) = self.read_len_enc_int(offset)?;
        match lei {
            LenEncInt::Err => Ok((offset, LenEncStr::Err)),
            LenEncInt::Null => Ok((offset, LenEncStr::Null)),
            _ => {
                let len = lei.to_u64().unwrap() as usize;
                let end = offset + len;
                if self.len() < end {
                    return Err(Error::InputIncomplete(Needed::Size(end - self.len())));
                }
                Ok((end, LenEncStr::Ref(&self[offset..end])))
            }
        }
    }
}

/// MySQL length encoded integer
#[derive(Debug, Clone, PartialEq)]
pub enum LenEncInt {
    Null,
    Err,
    Len1(u8),
    Len3(u16),
    Len4(u32),
    Len9(u64),
}

impl LenEncInt {
    pub fn to_u64(&self) -> Option<u64> {
        match self {
            LenEncInt::Len1(n) => Some(*n as u64),
            LenEncInt::Len3(n) => Some(*n as u64),
            LenEncInt::Len4(n) => Some(*n as u64),
            LenEncInt::Len9(n) => Some(*n as u64),
            _ => None,
        }
    }

    pub fn to_u32(&self) -> Option<u32> {
        match self {
            LenEncInt::Len1(n) => Some(*n as u32),
            LenEncInt::Len3(n) => Some(*n as u32),
            LenEncInt::Len4(n) => Some(*n as u32),
            LenEncInt::Len9(n) => Some(*n as u32),
            _ => None,
        }
    }
}

impl WriteTo<'_, LenEncInt> for Vec<u8> {
    fn write_to(&mut self, val: LenEncInt) -> Result<usize> {
        let len = match val {
            LenEncInt::Null => {
                self.push(0xfb);
                1
            }
            LenEncInt::Err => {
                self.push(0xff);
                1
            }
            LenEncInt::Len1(n) => {
                self.push(n);
                1
            }
            LenEncInt::Len3(n) => {
                self.reserve(3);
                self.push(0xfc);
                self.extend(&n.to_le_bytes());
                3
            }
            LenEncInt::Len4(n) => {
                self.reserve(4);
                self.push(0xfd);
                self.extend(&n.to_le_bytes()[..3]);
                4
            }
            LenEncInt::Len9(n) => {
                self.reserve(9);
                self.push(0xfe);
                self.extend(&n.to_le_bytes());
                9
            }
        };
        Ok(len)
    }
}

/// MySQL length encoded string
#[derive(Debug, Clone, PartialEq)]
pub enum LenEncStr<'a> {
    Null,
    Ref(&'a [u8]),
    Owned(Vec<u8>),
    Err,
}

impl<'a> LenEncStr<'a> {
    pub fn new_ref(bs: &'a [u8]) -> Self {
        LenEncStr::Ref(bs)
    }

    pub fn new_owned(bs: Vec<u8>) -> Self {
        LenEncStr::Owned(bs)
    }

    pub fn to_utf8_string(&self) -> Option<String> {
        match self {
            LenEncStr::Ref(r) => Some(String::from_utf8_lossy(r).to_string()),
            LenEncStr::Owned(o) => Some(String::from_utf8_lossy(o).to_string()),
            _ => None,
        }
    }

    pub fn into_ref(self) -> Option<&'a [u8]> {
        match self {
            LenEncStr::Ref(r) => Some(r),
            _ => None,
        }
    }

    pub fn into_owned(self) -> Option<Vec<u8>> {
        match self {
            LenEncStr::Owned(v) => Some(v),
            _ => None,
        }
    }

    /// convert either ref or owned bytes into Vec<u8>
    pub fn into_inner_bytes(self) -> Option<Vec<u8>> {
        match self {
            LenEncStr::Owned(v) => Some(v),
            LenEncStr::Ref(r) => {
                let mut bs = Vec::with_capacity(r.len());
                bs.extend(r);
                Some(bs)
            }
            _ => None,
        }
    }
}

impl<'a> WriteTo<'a, LenEncStr<'a>> for Vec<u8> {
    fn write_to(&mut self, val: LenEncStr) -> Result<usize> {
        let len = match val {
            LenEncStr::Null => {
                self.push(0xfb);
                1
            }
            LenEncStr::Err => {
                self.push(0xff);
                1
            }
            LenEncStr::Ref(r) => {
                let len = r.len() as u64;
                let lei: LenEncInt = len.into();
                let lei_len = self.write_to(lei)?;
                self.extend(&r[..]);
                lei_len + len as usize
            }
            LenEncStr::Owned(o) => {
                let len = o.len() as u64;
                let lei: LenEncInt = len.into();
                let lei_len = self.write_to(lei)?;
                self.extend(&o[..]);
                lei_len + len as usize
            }
        };
        Ok(len)
    }
}

/// convert u64 to len-enc-int
impl From<u64> for LenEncInt {
    fn from(src: u64) -> Self {
        if src <= 0xfb {
            LenEncInt::Len1(src as u8)
        } else if src <= 0xffff {
            LenEncInt::Len3(src as u16)
        } else if src <= 0xffffff {
            LenEncInt::Len4(src as u32)
        } else {
            LenEncInt::Len9(src)
        }
    }
}

/// convert u8 to len-enc-int
impl From<u8> for LenEncInt {
    fn from(src: u8) -> Self {
        Self::from(src as u64)
    }
}

/// convert u16 to len-enc-int
impl From<u16> for LenEncInt {
    fn from(src: u16) -> Self {
        Self::from(src as u64)
    }
}

/// convert u32 to len-enc-int
impl From<u32> for LenEncInt {
    fn from(src: u32) -> Self {
        Self::from(src as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_len_enc_int_1() {
        // read
        let bs = vec![0x0a_u8];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Len1(0x0a), lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);
    }

    #[test]
    fn test_len_enc_int_3() {
        // read
        let bs = vec![0xfc_u8, 0xfd, 0x00];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Len3(0xfd_u16), lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);

        // read
        let bs = vec![0xfc_u8, 0x1d, 0x05];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Len3(0x051d_u16), lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);
    }

    #[test]
    fn test_len_enc_int_4() {
        // read
        let bs = vec![0xfd_u8, 0xc2, 0xb2, 0xa2];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Len4(0xa2b2c2_u32), lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);
    }

    #[test]
    fn test_len_enc_int_8() {
        // read
        let bs = vec![0xfe, 0x0d, 0x0c, 0x0b, 0x0a, 0x04, 0x03, 0x02, 0x01];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Len9(0x010203040a0b0c0d_u64), lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);
    }

    #[test]
    fn test_len_enc_int_err() {
        // read
        let bs = vec![0xff_u8];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Err, lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);
    }

    #[test]
    fn test_len_enc_int_null() {
        // read
        let bs = vec![0xfb_u8];
        let lei = bs.read_len_enc_int(0).unwrap().1;
        assert_eq!(LenEncInt::Null, lei);
        // write
        let mut encoded = Vec::new();
        encoded.write_to(lei).unwrap();
        assert_eq!(bs, encoded);
    }

    #[test]
    fn test_len_enc_str() {
        // read
        let bs = Vec::from(&b"\x05hello"[..]);
        let (_, les) = bs.read_len_enc_str(0).unwrap();
        assert_eq!(b"hello", les.to_utf8_string().unwrap().as_bytes());
        // write
        let mut encoded = Vec::new();
        encoded.write_to(les).unwrap();
        assert_eq!(bs, encoded);
    }
}
