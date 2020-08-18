use bytes_parser::ReadFrom;
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

impl<'a> ReadFrom<'a, RotateData<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, RotateData<'a>)> {
        let (offset, position) = self.read_le_u64(offset)?;
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
