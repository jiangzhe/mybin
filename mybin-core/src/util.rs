use crc_any::CRCu32;

/// helper function to get indexed bool value from bitmap
#[inline]
pub(crate) fn bitmap_index(bitmap: &[u8], idx: usize) -> bool {
    let bucket = idx >> 3;
    let offset = idx & 7;
    let bit = 1 << offset;
    bit & bitmap[bucket] == bit
}

pub(crate) fn checksum_crc32(bytes: &[u8]) -> u32 {
    let mut hasher = CRCu32::crc32();
    hasher.digest(bytes);
    hasher.get_crc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_iso_3309() {
        assert_eq!(907060870, checksum_crc32(b"hello"));
        assert_eq!(980881731, checksum_crc32(b"world"));
    }
}
