use bytes::{Buf, Bytes};
use bytes_parser::error::Result;
use bytes_parser::ReadBytesExt;
use std::fmt;

const DIG_PER_DEC1: u8 = 9;
const DIG_TO_BYTES: [u32; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
const POWERS_10: [u32; 10] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];

#[derive(Debug, Clone, PartialEq)]
pub struct MyDecimal {
    // intg is the number of *decimal* digits before the points
    pub intg: u8,
    // frac is the number of decimal digits after the points
    pub frac: u8,
    pub negative: bool,
    // array of a segment of decimal digits, stored as u32, range from 0 to (1000_000_000 -1)
    pub buf: Vec<u32>,
}

impl MyDecimal {
    pub fn zero(intg: u8, frac: u8) -> Self {
        Self {
            intg,
            frac,
            negative: false,
            buf: vec![],
        }
    }

    pub fn read_from(input: &mut Bytes, intg: u8, frac: u8) -> Result<Self> {
        // number of main integral fragments
        let intg0 = intg / DIG_PER_DEC1;
        // number of main fractional fragments
        let frac0 = frac / DIG_PER_DEC1;
        // digit number of extra integral fragment
        let intg0x = intg - intg0 * DIG_PER_DEC1;
        // digit number of extra fractional fragment
        let frac0x = frac - frac0 * DIG_PER_DEC1;
        // total byte length
        let bin_size = intg0 as u32 * 4
            + DIG_TO_BYTES[intg0x as usize]
            + frac0 as u32 * 4
            + DIG_TO_BYTES[frac0x as usize];
        if (bin_size as usize) < input.remaining() {
            log::debug!(
                "decimal length mismatch: intg={}, frac={}, bin_len={}, actual_len={}",
                intg,
                frac,
                bin_size,
                input.remaining()
            );
        }
        if !input.has_remaining() {
            return Ok(Self::zero(intg, frac));
        }
        let mut buf = vec![];
        // positive number will have first bit 1, this is MySQL decimal encoding
        let negative = input[0] & 0x80 != 0x80;
        if intg0x > 0 {
            let len = DIG_TO_BYTES[intg0x as usize];
            let frag = read_extra_fragment(input, true, len as usize, negative)?;
            buf.push(frag);
        }
        for _ in 0..intg0 {
            let frag = read_fragment(input, negative)?;
            buf.push(frag);
        }
        for _ in 0..frac0 {
            let frag = read_fragment(input, negative)?;
            buf.push(frag);
        }
        if frac0x > 0 {
            let len = DIG_TO_BYTES[frac0x as usize];
            let frag = read_extra_fragment(input, false, len as usize, negative)?;
            buf.push(frag * POWERS_10[(DIG_PER_DEC1 - frac0x) as usize]);
        }
        Ok(Self {
            intg,
            frac,
            negative,
            buf,
        })
    }
}

impl fmt::Display for MyDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.buf.is_empty() {
            write!(f, "0")?;
            return Ok(());
        }
        let intg0 = self.intg / DIG_PER_DEC1;
        let intg0x = self.intg - intg0 * DIG_PER_DEC1;
        let frac0 = self.frac / DIG_PER_DEC1;
        let frac0x = self.frac - frac0 * DIG_PER_DEC1;
        let mut i = 0;
        if self.negative {
            write!(f, "-")?;
        }
        if intg0x > 0 {
            write!(f, "{}", self.buf[i])?;
            i += 1;
        }
        for _ in 0..intg0 {
            write!(f, "{}", self.buf[i])?;
            i += 1;
        }
        if self.frac == 0 {
            return Ok(());
        }
        write!(f, ".")?;
        for _ in 0..frac0 {
            write!(f, "{}", self.buf[i])?;
            i += 1;
        }
        if frac0x > 0 {
            let x = self.buf[i] / POWERS_10[(DIG_PER_DEC1 - frac0x) as usize];
            write!(f, "{}", x)?;
            // i += 1;
        }
        Ok(())
    }
}

/// read the extra fragment at beginning
/// the fragment may contains 1, 2, 3, or 4 bytes
/// according to the intg and scale
/// if negative, all bits of the result needs to be reversed
fn read_extra_fragment(
    input: &mut Bytes,
    rev_first_bit: bool,
    len: usize,
    negative: bool,
) -> Result<u32> {
    let frag = match len {
        1 => {
            let mut n = input.read_u8()?;
            if rev_first_bit {
                n ^= 0x80;
            }
            if negative {
                n ^= 0xff;
            }
            n as u32
        }
        2 => {
            let mut n = input.read_be_u16()?;
            if rev_first_bit {
                n ^= 0x8000;
            }
            if negative {
                n ^= 0xffff;
            }
            n as u32
        }
        3 => {
            let mut n = input.read_be_u24()?;
            if rev_first_bit {
                n ^= 0x80_0000;
            }
            if negative {
                n ^= 0xff_ffff;
            }
            n & 0x00ff_ffff
        }
        4 => {
            let mut n = input.read_be_u32()?;
            if rev_first_bit {
                n ^= 0x8000_0000;
            }
            if negative {
                n ^= 0xffff_ffff
            }
            n
        }
        _ => unreachable!("unexpected decimal fragment length {}", len),
    };
    Ok(frag)
}

/// read fragment with fixed length of 4
/// if negative, all bits of the result needs to be reversed
fn read_fragment(input: &mut Bytes, negative: bool) -> Result<u32> {
    // decimal encoding using BigEndian
    let mut n = input.read_be_u32()?;
    if negative {
        n ^= 0xffff_ffff;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_read_decimal_positive() {
        // 1 234567890 . 123400000
        let mut bs = Bytes::from(vec![0x81, 0x0d, 0xfb, 0x38, 0xd2, 0x04, 0xd2]);
        let d1 = MyDecimal::read_from(&mut bs, 10, 4).unwrap();
        assert_eq!(10, d1.intg);
        assert_eq!(4, d1.frac);
        assert!(!d1.negative);
        assert_eq!(vec![1, 234567890, 123400000], d1.buf);
        assert_eq!("1234567890.1234", d1.to_string());
    }

    #[test]
    fn test_read_decimal_negative() {
        // - 1 234567890 . 123400000
        let mut bs = Bytes::from(vec![0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D]);
        let d1 = MyDecimal::read_from(&mut bs, 10, 4).unwrap();
        assert_eq!(10, d1.intg);
        assert_eq!(4, d1.frac);
        assert!(d1.negative);
        assert_eq!(vec![1, 234567890, 123400000], d1.buf);
        assert_eq!("-1234567890.1234", d1.to_string());
    }
}
