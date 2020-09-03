use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComTableDump {
    pub cmd: Command,
    pub db: String,
    pub table: String,
}

impl ComTableDump {
    pub fn new<D, T>(db: D, table: T) -> Self
    where
        D: Into<String>,
        T: Into<String>,
    {
        Self {
            cmd: Command::TableDump,
            db: db.into(),
            table: table.into(),
        }
    }
}

impl WriteToBytes for ComTableDump {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_u8(self.db.len() as u8)?;
        len += out.write_bytes(self.db.as_bytes())?;
        len += out.write_u8(self.table.len() as u8)?;
        len += out.write_bytes(self.table.as_bytes())?;
        Ok(len)
    }
}
