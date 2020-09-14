use crate::col::ColumnDefinition;
use crate::flag::CapabilityFlags;
use crate::packet::{EofPacket, ErrPacket};
use crate::Command;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::{WriteBytesExt, WriteToBytes};

/// get column definitions of a table
///
/// deprecated, use SHOW COLUMNS instead
#[derive(Debug)]
pub struct ComFieldList {
    pub cmd: Command,
    pub table: String,
    pub field_wildcard: String,
}

impl ComFieldList {
    pub fn new<T: Into<String>, U: Into<String>>(table: T, field_wildcard: U) -> Self {
        ComFieldList {
            cmd: Command::FieldList,
            table: table.into(),
            field_wildcard: field_wildcard.into(),
        }
    }
}

impl WriteToBytes for ComFieldList {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.table.as_bytes())?;
        len += out.write_u8(0)?;
        len += out.write_bytes(self.field_wildcard.as_bytes())?;
        Ok(len)
    }
}

#[derive(Debug)]
pub enum ComFieldListResponse {
    ColDef(ColumnDefinition),
    Err(ErrPacket),
    Eof(EofPacket),
}

impl ComFieldListResponse {
    pub fn read_from(input: &mut Bytes, cap_flags: &CapabilityFlags, sql: bool) -> Result<Self> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0xff => {
                let err = ErrPacket::read_from(input, cap_flags, sql)?;
                Ok(ComFieldListResponse::Err(err))
            }
            0xfe => {
                let eof = EofPacket::read_from(input, cap_flags)?;
                Ok(ComFieldListResponse::Eof(eof))
            }
            _ => {
                let col_def = ColumnDefinition::read_from(input, true)?;
                Ok(ComFieldListResponse::ColDef(col_def))
            }
        }
    }
}
