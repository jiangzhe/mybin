use bytes_parser::ReadFromBytesWithContext;
use bytes_parser::my::{LenEncStr, ReadMyEnc};
use bytes_parser::Result;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct TextRow(pub Vec<LenEncStr>);

impl<'c> ReadFromBytesWithContext<'c> for TextRow {
    type Context = usize;

    fn read_with_ctx(input: &mut Bytes, col_cnt: usize) -> Result<Self> {
        let mut tcvs = Vec::with_capacity(col_cnt as usize);
        for _ in 0..col_cnt {
            let s = input.read_len_enc_str()?;
            tcvs.push(s);
        }
        Ok(TextRow(tcvs))
    }
}
