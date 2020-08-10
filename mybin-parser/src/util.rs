//! implements parsing of mysql length encoded types
use nom::bytes::streaming::take;
use nom::error::ParseError;
use nom::number::streaming::{le_u16, le_u24, le_u64, le_u8};
use nom::IResult;
use serde_derive::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LenEncInt {
    Null,
    Len1(u8),
    Len3(u16),
    // actual 3-byte integer
    Len4(u32),
    Len9(u64),
    Err,
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

/// https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
pub(crate) fn len_enc_int<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], LenEncInt, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, len) = le_u8(input)?;
    match len {
        0xfb => Ok((input, LenEncInt::Null)),
        0xfc => {
            let (input, n) = le_u16(input)?;
            Ok((input, LenEncInt::Len3(n)))
        }
        0xfd => {
            let (input, n) = le_u24(input)?;
            Ok((input, LenEncInt::Len4(n)))
        }
        0xfe => {
            let (input, n) = le_u64(input)?;
            Ok((input, LenEncInt::Len9(n)))
        }
        0xff => Ok((input, LenEncInt::Err)),
        _ => Ok((input, LenEncInt::Len1(len))),
    }
}

impl From<LenEncInt> for Vec<u8> {
    fn from(src: LenEncInt) -> Self {
        match src {
            LenEncInt::Null => vec![0xfb],
            LenEncInt::Err => vec![0xff],
            LenEncInt::Len1(n) => vec![n],
            LenEncInt::Len3(n) => {
                let mut vec = Vec::with_capacity(3);
                vec.push(0xfc);
                vec.extend_from_slice(&n.to_le_bytes());
                vec
            }
            LenEncInt::Len4(n) => {
                let mut vec = Vec::with_capacity(4);
                vec.push(0xfd);
                // only extend 3 bytes
                vec.extend_from_slice(&n.to_le_bytes()[..3]);
                vec
            }
            LenEncInt::Len9(n) => {
                let mut vec = Vec::with_capacity(9);
                vec.push(0xfe);
                vec.extend_from_slice(&n.to_le_bytes());
                vec
            }
        }
    }
}

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

impl From<u8> for LenEncInt {
    fn from(src: u8) -> Self {
        Self::from(src as u64)
    }
}

impl From<u16> for LenEncInt {
    fn from(src: u16) -> Self {
        Self::from(src as u64)
    }
}

impl From<u32> for LenEncInt {
    fn from(src: u32) -> Self {
        Self::from(src as u64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LenEncStr<'a> {
    Null,
    Ref(&'a [u8]),
    Err,
}

impl<'a> LenEncStr<'a> {
    pub fn to_owned_string(&self) -> Option<String> {
        match self {
            LenEncStr::Ref(r) => Some(String::from_utf8_lossy(r).to_string()),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self {
            LenEncStr::Ref(r) => Some(r),
            _ => None,
        }
    }
}

// https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
pub(crate) fn len_enc_str<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], LenEncStr<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, lei) = len_enc_int(input)?;
    match lei {
        LenEncInt::Null => Ok((input, LenEncStr::Null)),
        LenEncInt::Err => Ok((input, LenEncStr::Err)),
        LenEncInt::Len1(i) => {
            let (input, s) = take(i)(input)?;
            Ok((input, LenEncStr::Ref(s)))
        }
        LenEncInt::Len3(i) => {
            let (input, s) = take(i)(input)?;
            Ok((input, LenEncStr::Ref(s)))
        }
        LenEncInt::Len4(i) => {
            let (input, s) = take(i)(input)?;
            Ok((input, LenEncStr::Ref(s)))
        }
        LenEncInt::Len9(i) => {
            let (input, s) = take(i)(input)?;
            Ok((input, LenEncStr::Ref(s)))
        }
    }
}

/// encode int into length-encoded-int
pub fn encode_int(src: u64) -> Vec<u8> {
    let len: LenEncInt = src.into();
    len.into()
}

/// encode string into length-encoded-string
pub fn encode_string(src: &str) -> Vec<u8> {
    let len: LenEncInt = (src.len() as u64).into();
    let mut bs: Vec<u8> = len.into();
    bs.extend_from_slice(src.as_bytes());
    bs
}

/// helper function to get 6-byte unsigned integer from input
/// nom does not have le_u48, so make it
#[inline]
pub(crate) fn streaming_le_u48<'a, E>(i: &'a [u8]) -> IResult<&'a [u8], u64, E>
where
    E: ParseError<&'a [u8]>,
{
    if i.len() < 6 {
        Err(nom::Err::Incomplete(nom::Needed::Size(6)))
    } else {
        let res = (i[0] as u64)
            + ((i[1] as u64) << 8)
            + ((i[2] as u64) << 16)
            + ((i[3] as u64) << 24)
            + ((i[4] as u64) << 32)
            + ((i[5] as u64) << 40);
        Ok((&i[6..], res))
    }
}

/// helper function to get indexed bool value from bitmap
#[inline]
pub(crate) fn bitmap_index(bitmap: &[u8], idx: usize) -> bool {
    let bucket = idx >> 3;
    let offset = idx & 7;
    let bit = 1 << offset;
    bit & bitmap[bucket] == bit
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::error::VerboseError;

    #[test]
    fn test_len_enc_int_1() {
        let n = 10u8;
        let encoded: LenEncInt = n.into();
        assert_eq!(LenEncInt::Len1(n), encoded);
        let bs: Vec<u8> = encoded.clone().into();
        assert_eq!(vec![n], bs);
        assert_eq!(encoded, len_enc_int::<VerboseError<_>>(&bs).unwrap().1);
    }

    #[test]
    fn test_len_enc_int_3() {
        let n = 0xfd_u16;
        let encoded: LenEncInt = n.into();
        assert_eq!(LenEncInt::Len3(n), encoded);
        let bs: Vec<u8> = encoded.clone().into();
        assert_eq!(vec![0xfc, 0xfd, 0x00], bs);
        assert_eq!(encoded, len_enc_int::<VerboseError<_>>(&bs).unwrap().1);

        let n = 0x1d05_u16;
        let encoded: LenEncInt = n.into();
        assert_eq!(LenEncInt::Len3(n), encoded);
        let bs: Vec<u8> = encoded.clone().into();
        assert_eq!(vec![0xfc, 0x05, 0x1d], bs);
        assert_eq!(encoded, len_enc_int::<VerboseError<_>>(&bs).unwrap().1);
    }
    
    #[test]
    fn test_len_enc_int_4() {
        let n = 0xa2b2c2_u32;
        let encoded: LenEncInt = n.into();
        assert_eq!(LenEncInt::Len4(n), encoded);
        let bs: Vec<u8> = encoded.clone().into();
        assert_eq!(vec![0xfd, 0xc2, 0xb2, 0xa2], bs);
        assert_eq!(encoded, len_enc_int::<VerboseError<_>>(&bs).unwrap().1);
    }

    #[test]
    fn test_len_enc_int_8() {
        let n = 0x010203040a0b0c0d_u64;
        let encoded: LenEncInt = n.into();
        assert_eq!(LenEncInt::Len9(n), encoded);
        let bs: Vec<u8> = encoded.clone().into();
        assert_eq!(vec![0xfe, 0x0d, 0x0c, 0x0b, 0x0a, 0x04, 0x03, 0x02, 0x01], bs);
        assert_eq!(encoded, len_enc_int::<VerboseError<_>>(&bs).unwrap().1);
    }

    #[test]
    fn test_encode_string() {
        let expected = b"\x05hello";
        assert_eq!(Vec::from(&expected[..]), encode_string("hello"));
    }
}