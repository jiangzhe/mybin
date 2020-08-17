use bytes_parser::ReadAs;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::error::Result;

/// Data of RotateEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/rotate-event.html
#[derive(Debug, Clone)]
pub struct RotateData<'a> {
    pub position: u64,
    // below is variable part
    pub next_binlog_filename: &'a [u8],
}

impl<'a> ReadAs<'a, RotateData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, RotateData<'a>)> {
        let (offset, position) = self.read_le_u64(offset)?;
        // let (input, s) = take(len as usize - post_header_len as usize)(input)?;
        let (offset, next_binlog_filename) = self.take_len(offset, self.len() - offset)?;
        Ok((
            offset,
            RotateData {
                position,
                next_binlog_filename,
            },
        ))
    }
}
