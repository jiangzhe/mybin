/// helper function to get indexed bool value from bitmap
#[inline]
pub(crate) fn index(bitmap: &[u8], idx: usize) -> bool {
    let bucket = idx >> 3;
    let offset = idx & 7;
    let bit = 1 << offset;
    bit & bitmap[bucket] == bit
}

#[inline]
pub(crate) fn mark(bitmap: &mut [u8], idx: usize, mark: bool) {
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

pub(crate) fn to_iter(bits: &[u8], offset: usize) -> ToIter {
    ToIter { bits, idx: offset }
}

/// construct bitmap from iterator
/// the first "offset" bits are set to zero
/// generated bitmap is at least 1 byte
pub(crate) fn from_iter<I>(iter: I, mut offset: usize) -> Vec<u8>
where
    I: IntoIterator<Item = bool>,
{
    let mut bm = Vec::new();
    for _ in 0..(offset >> 3) + 1 {
        bm.push(0u8);
    }
    for b in iter {
        if (offset >> 3) == bm.len() {
            bm.push(0u8);
        }
        if b {
            *bm.last_mut().unwrap() |= 1 << (offset & 7);
        }
        offset += 1;
    }
    bm
}

pub struct ToIter<'a> {
    bits: &'a [u8],
    idx: usize,
}

impl Iterator for ToIter<'_> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_to_iter() {
        let single_zeros = [0u8];
        let sum = to_iter(&single_zeros, 0)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(0, sum);
        let single_ones = [0xff_u8];
        let sum = to_iter(&single_ones, 0)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(8, sum);
        let multi_zeros = [0u8, 0, 0];
        let sum = to_iter(&multi_zeros, 0)
            .map(|b| if b { 1 } else { 0 })
            .sum();
        assert_eq!(0, sum);
        let multi_ones = [0xff_u8, 0xff, 0xff];
        let sum = to_iter(&multi_ones, 0).map(|b| if b { 1 } else { 0 }).sum();
        assert_eq!(24, sum);
    }

    #[test]
    fn test_bitmap_from_iter() {
        let bools = vec![true, false, true, false];
        let bm1 = from_iter(bools.clone(), 0);
        assert_eq!(vec![0b00000101_u8], bm1);

        let bm2 = from_iter(bools, 2);
        assert_eq!(vec![0b00010100_u8], bm2);
    }
}
