use crate::col::ColumnDefinition;
use crate::packet::{EofPacket, ErrPacket, OkPacket};
use crate::row::TextRow;
use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComQuery {
    pub cmd: Command,
    pub query: String,
}

impl ComQuery {
    pub fn new<S: Into<String>>(query: S) -> Self {
        ComQuery {
            cmd: Command::Query,
            query: query.into(),
        }
    }
}

impl WriteToBytes for ComQuery {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.query.as_bytes())?;
        Ok(len)
    }
}

/// response of COM_QUERY
///
/// reference: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
///
/// todo: support local_infile_request
#[derive(Debug, Clone)]
pub enum ComQueryResponse {
    Ok(OkPacket),
    Err(ErrPacket),
    // below are result set related packets
    ColCnt(u64),
    ColDef(ColumnDefinition),
    Eof(EofPacket),
    Row(TextRow),
}
