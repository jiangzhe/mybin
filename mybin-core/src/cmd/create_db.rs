use crate::resp::ComResponse;
use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComCreateDB {
    pub cmd: Command,
    pub schema_name: String,
}

impl ComCreateDB {
    pub fn new<T: Into<String>>(db_name: T) -> Self {
        ComCreateDB {
            cmd: Command::CreateDB,
            schema_name: db_name.into(),
        }
    }
}

impl WriteToBytes for ComCreateDB {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.schema_name.as_bytes())?;
        Ok(len)
    }
}

pub type ComCreateDBResponse = ComResponse;
