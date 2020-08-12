use crate::error::{Result, Error, Needed};

/// special parser to support MySQL len-enc-int
pub trait BytesMyLenEncIntParser {

    fn len_enc_int(&self, offset: usize) -> Result<(usize, LenEncInt)>;
}

impl BytesMyLenEncIntParser for [u8] {
    
    fn len_enc_int(&self, offset: usize) -> Result<(usize, LenEncInt)> {
        if self.len() < offset + 1 {
            return Err(Error::Incomplete(Needed::Size(offset + 1 - self.len())));
        }
        todo!()
    }
}

#[derive(Debug)]
pub enum LenEncInt {
    Null,
    Err,
    Len1(u8),
    Len3(u16),
    Len4(u32),
    Len9(u64),
}

