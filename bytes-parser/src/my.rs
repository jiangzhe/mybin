use crate::error::{Error, Needed, Result};
use crate::{ReadBytesExt, ReadFromBytes, WriteToBytes};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// read MySQL encoded types
pub trait ReadMyEnc {
    fn read_len_enc_int(&mut self) -> Result<LenEncInt>;

    fn read_len_enc_str(&mut self) -> Result<LenEncStr>;
}

impl ReadMyEnc for Bytes {
    fn read_len_enc_int(&mut self) -> Result<LenEncInt> {
        if self.remaining() < 1 {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        let len = self.read_u8()?;
        match len {
            0xfb => Ok(LenEncInt::Null),
            0xfc => {
                let n = self.read_le_u16()?;
                Ok(LenEncInt::Len3(n))
            }
            0xfd => {
                let n = self.read_le_u24()?;
                Ok(LenEncInt::Len4(n))
            }
            0xfe => {
                let n = self.read_le_u64()?;
                Ok(LenEncInt::Len9(n))
            }
            0xff => Ok(LenEncInt::Err),
            _ => Ok(LenEncInt::Len1(len)),
        }
    }

    fn read_len_enc_str(&mut self) -> Result<LenEncStr> {
        let lei = self.read_len_enc_int()?;
        match lei {
            LenEncInt::Err => Ok(LenEncStr::Err),
            LenEncInt::Null => Ok(LenEncStr::Null),
            _ => {
                let len = lei.to_u64().unwrap() as usize;
                if self.remaining() < len {
                    return Err(Error::InputIncomplete(
                        Bytes::new(),
                        Needed::Size(len - self.remaining()),
                    ));
                }
                let bs = self.split_to(len);
                Ok(LenEncStr::Bytes(bs))
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

impl ReadFromBytes for LenEncInt {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        input.read_len_enc_int()
    }
}

impl WriteToBytes for LenEncInt {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let len = match self {
            LenEncInt::Null => {
                out.put_u8(0xfb);
                1
            }
            LenEncInt::Err => {
                out.put_u8(0xff);
                1
            }
            LenEncInt::Len1(n) => {
                out.put_u8(n);
                1
            }
            LenEncInt::Len3(n) => {
                out.put_u8(0xfc);
                out.put(&n.to_le_bytes()[..]);
                3
            }
            LenEncInt::Len4(n) => {
                out.put_u8(0xfd);
                out.put(&n.to_le_bytes()[..3]);
                4
            }
            LenEncInt::Len9(n) => {
                out.put_u8(0xfe);
                out.put(&n.to_le_bytes()[..]);
                9
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

#[derive(Debug, Clone)]
pub enum LenEncStr {
    Null,
    Err,
    Bytes(Bytes),
}

impl LenEncStr {
    pub fn null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }

    pub fn err(&self) -> bool {
        match self {
            Self::Err => true,
            _ => false,
        }
    }

    pub fn bytes(&self) -> Option<&Bytes> {
        match self {
            Self::Bytes(bs) => Some(bs),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Option<Bytes> {
        match self {
            Self::Bytes(bs) => Some(bs),
            _ => None,
        }
    }

    /// convert into str, returns empty str if null or err
    pub fn into_str(&self) -> std::result::Result<&str, std::str::Utf8Error> {
        match self {
            Self::Bytes(bs) => std::str::from_utf8(bs.as_ref()),
            _ => Ok(""),
        }
    }

    /// convert into owned string, returns empty string if null or err
    pub fn into_string(self) -> std::result::Result<String, std::string::FromUtf8Error> {
        match self {
            Self::Bytes(bs) => String::from_utf8(Vec::from(bs.as_ref())),
            _ => Ok(String::new()),
        }
    }
}

impl ReadFromBytes for LenEncStr {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        input.read_len_enc_str()
    }
}

impl WriteToBytes for LenEncStr {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let len = match self {
            LenEncStr::Null => {
                out.put_u8(0xfb);
                1
            }
            LenEncStr::Err => {
                out.put_u8(0xff);
                1
            }
            LenEncStr::Bytes(bs) => {
                let len = bs.remaining();
                let lei = LenEncInt::from(len as u64);
                let lei_len = lei.write_to(out)?;
                out.put(bs);
                lei_len + len
            }
        };
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_len_enc_int_1() {
        // read
        let orig = vec![0x0a_u8];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Len1(0x0a), lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_len_enc_int_3() {
        // read
        let orig = vec![0xfc_u8, 0xfd, 0x00];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Len3(0xfd_u16), lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());

        // read
        let orig = vec![0xfc_u8, 0x1d, 0x05];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Len3(0x051d_u16), lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_len_enc_int_4() {
        // read
        let orig = vec![0xfd_u8, 0xc2, 0xb2, 0xa2];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Len4(0xa2b2c2_u32), lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_len_enc_int_8() {
        // read
        let orig = vec![0xfe, 0x0d, 0x0c, 0x0b, 0x0a, 0x04, 0x03, 0x02, 0x01];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Len9(0x010203040a0b0c0d_u64), lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_len_enc_int_err() {
        // read
        let orig = vec![0xff_u8];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Err, lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_len_enc_int_null() {
        // read
        let orig = vec![0xfb_u8];
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Null, lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_len_enc_str() {
        // read
        let orig = b"\x05hello";
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let les = bs.read_len_enc_str().unwrap();
        assert_eq!(b"hello", les.bytes().unwrap().as_ref());
        // write
        let mut encoded = BytesMut::new();
        les.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_bytes_len_enc_int_null() {
        // read
        let orig = b"\xfb";
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let lei = bs.read_len_enc_int().unwrap();
        assert_eq!(LenEncInt::Null, lei);
        // write
        let mut encoded = BytesMut::new();
        lei.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_bytes_len_enc_str_valid() {
        // read
        let orig = b"\x05hello";
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let les = bs.read_len_enc_str().unwrap();
        assert_eq!("hello", les.clone().into_str().unwrap());
        // write
        let mut encoded = BytesMut::new();
        les.write_to(&mut encoded).unwrap();
        assert_eq!(orig, encoded.as_ref());
    }

    #[test]
    fn test_bytes_len_enc_str_invalid() {
        // realet orig = b"\x05hello";
        let orig = b"\x05hell";
        let mut bs = Bytes::copy_from_slice(&orig[..]);
        let fail = bs.read_len_enc_str();
        assert!(fail.is_err());
    }
}
