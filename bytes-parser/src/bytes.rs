use crate::error::{Error, Needed, Result};

pub trait ReadBytes<'a> {
    /// take a byte slice given offset and len
    fn take_len(&'a self, offset: usize, len: usize) -> Result<(usize, &'a [u8])>
    where
        Self: 'a;

    fn take_out_len(&'a self, offset: usize, len: usize, out: &mut Vec<u8>) -> Result<usize>
    where
        Self: 'a,
    {
        let (offset, bs) = self.take_len(offset, len)?;
        out.extend(bs);
        Ok(offset)
    }

    /// take a byte slice until encountering given byte
    ///
    /// if inclusive flag is set, the end byte will be
    /// included in the returned slice.
    fn take_until(&'a self, offset: usize, b: u8, inclusive: bool) -> Result<(usize, &'a [u8])>;

    fn take_out_until(
        &'a self,
        offset: usize,
        b: u8,
        inclusive: bool,
        out: &mut Vec<u8>,
    ) -> Result<usize> {
        let (offset, bs) = self.take_until(offset, b, inclusive)?;
        out.extend(bs);
        Ok(offset)
    }
}

impl<'a> ReadBytes<'a> for [u8] {
    fn take_len(&'a self, offset: usize, len: usize) -> Result<(usize, &'a [u8])>
    where
        Self: 'a,
    {
        let end = offset + len;
        if self.len() < end {
            return Err(Error::InputIncomplete(Needed::Size(end - self.len())));
        }
        Ok((end, &self[offset..end]))
    }

    fn take_until(&self, offset: usize, b: u8, inclusive: bool) -> Result<(usize, &[u8])> {
        if self.len() <= offset {
            return Err(Error::InputIncomplete(Needed::Unknown));
        }
        if let Some(pos) = self[offset..].iter().position(|&x| x == b) {
            let end = offset + pos + 1;
            let r = if inclusive {
                &self[offset..end]
            } else {
                &self[offset..end - 1]
            };
            return Ok((end, r));
        }
        Err(Error::InputIncomplete(Needed::Unknown))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_len() {
        let bs = vec![1, 2, 3, 4, 5];
        let (offset, r) = bs.take_len(0, 3).unwrap();
        assert_eq!(vec![1, 2, 3], r);
        assert_eq!(3, offset);
        let r = bs.take_len(0, 6);
        assert!(r.is_err());
    }

    #[test]
    fn test_take_until() {
        let bs = vec![1u8, 2, 3, 4, 5];
        let (offset, r) = bs.take_until(0, 3, false).unwrap();
        assert_eq!(vec![1, 2], r);
        assert_eq!(3, offset);
        let (_, r) = bs.take_until(0, 3, true).unwrap();
        assert_eq!(vec![1, 2, 3], r);
        let r = bs.take_until(0, 6, false);
        assert!(r.is_err());
    }
}
