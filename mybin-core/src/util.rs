use crc_any::CRCu32;

/// helper function to get indexed bool value from bitmap
#[inline]
pub(crate) fn bitmap_index(bitmap: &[u8], idx: usize) -> bool {
    let bucket = idx >> 3;
    let offset = idx & 7;
    let bit = 1 << offset;
    bit & bitmap[bucket] == bit
}

#[inline]
pub(crate) fn bitmap_mark(bitmap: &mut [u8], idx: usize, mark: bool) {
    let bucket = idx >> 3;
    let offset = idx & 7;
    if mark {
        let bit = 1u8 << offset;
        bitmap[bucket] |= bit;
    } else {
        let mut bit = 1u8 << offset;
        bit = !bit;
        bitmap[bucket] &= bit;
    }
}

pub fn bitmap_iter(bits: &[u8]) -> BitmapIter {
    BitmapIter { bits, idx: 0 }
}

pub struct BitmapIter<'a> {
    bits: &'a [u8],
    idx: usize,
}

impl Iterator for BitmapIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bits.len() << 3 == self.idx {
            return None;
        }
        let bucket = self.idx >> 3;
        let offset = self.idx & 7;
        let flag = (self.bits[bucket] & (1 << offset)) != 0;
        self.idx += 1;
        Some(flag)
    }
}

pub(crate) fn checksum_crc32(bytes: &[u8]) -> u32 {
    let mut hasher = CRCu32::crc32();
    hasher.digest(bytes);
    hasher.get_crc()
}

#[macro_export]
macro_rules! try_from_text_column_value {
    ($($struct_name:ident),*) => {
        $(
            impl $crate::resultset::FromColumnValue<$crate::col::TextColumnValue> for $struct_name {
                fn from_value(value: $crate::col::TextColumnValue) -> Result<Option<Self>> {
                    use bytes::Buf;

                    match value {
                        TextColumnValue::Null => Ok(None),
                        TextColumnValue::Bytes(bs) => {
                            let s = std::str::from_utf8(bs.bytes())?;
                            Ok(Some(s.parse()?))
                        }
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! try_number_from_binary_column_value {
    ($num_type:ident, $($enum_variant:ident => $inter_type:ty),+) => {
        impl $crate::resultset::FromColumnValue<$crate::col::BinaryColumnValue> for $num_type {
            fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
                match value {
                    BinaryColumnValue::Null => Ok(None),
                    $(
                        BinaryColumnValue::$enum_variant(v) => Ok(Some(v as $inter_type as $num_type)),
                    )+
                    _ => Err(Error::column_type_mismatch(stringify!($num_type), &value))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_iso_3309() {
        assert_eq!(907060870, checksum_crc32(b"hello"));
        assert_eq!(980881731, checksum_crc32(b"world"));
    }

    #[test]
    fn test_bitmap_iter() {
        let single_zeros = [0u8];
        let sum = bitmap_iter(&single_zeros)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(0, sum);
        let single_ones = [0xff_u8];
        let sum = bitmap_iter(&single_ones)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(8, sum);
        let multi_zeros = [0u8, 0, 0];
        let sum = bitmap_iter(&multi_zeros)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(0, sum);
        let multi_ones = [0xff_u8, 0xff, 0xff];
        let sum = bitmap_iter(&multi_ones)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(24, sum);
    }
}
