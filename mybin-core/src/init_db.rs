use crate::Command;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};
use bytes::BytesMut;

#[derive(Debug, Clone)]
pub struct ComInitDB {
    cmd: Command,
    schema_name: String,
}

impl ComInitDB {
    pub fn new<T: AsRef<str>>(db_name: T) -> Self {
        ComInitDB{
            cmd: Command::InitDB,
            schema_name: db_name.as_ref().to_string(),
        }
    }
}

impl WriteToBytes for ComInitDB {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.schema_name.as_bytes())?;
        Ok(len)
    }
}
